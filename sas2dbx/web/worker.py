"""MigrationWorker — executa o pipeline SAS→Databricks em background.

Cada migração roda em uma thread daemon independente para não bloquear o
servidor FastAPI. O progresso é persistido via MigrationStorage, permitindo
que o polling do frontend acompanhe em tempo real.

Pipeline por migração:
  1. Extrai upload.zip → input/
  2. Scan de arquivos .sas no input/
  3. DependencyAnalyzer → grafo de dependências
  4. TranspilationEngine → notebooks .py em output/
  5. JobDocumentor (LLM, opcional) → README.md por job em docs/jobs/
  6. ArchitectureDocumentor → ARCHITECTURE.md em docs/
  7. ArchitectureExplorer → explorer.html na raiz da migração

Decisões arquiteturais aplicadas:
  R20: status "done" = pipeline correu até o fim; "failed" = exceção de
       infraestrutura que impediu o pipeline de completar.
  R22: LLMConfig injetado via construtor — worker nunca lê os.environ.
"""

from __future__ import annotations

import logging
import threading
import zipfile
from pathlib import Path

from sas2dbx.transpile.llm.client import LLMConfig
from sas2dbx.web.storage import MigrationStorage

logger = logging.getLogger(__name__)


class MigrationWorker:
    """Executa o pipeline de migração em background thread por migração.

    Args:
        storage: MigrationStorage compartilhado com o servidor.
        llm_config: Configuração LLM a ser injetada no DocumentEngine.
            Se api_key for None, JobDocumentor usará o stub embutido no LLMClient.
    """

    def __init__(self, storage: MigrationStorage, llm_config: LLMConfig) -> None:
        self._storage = storage
        self._llm_config = llm_config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, migration_id: str) -> None:
        """Dispara a thread de processamento para uma migração.

        Args:
            migration_id: UUID da migração criada pelo endpoint POST.
        """
        thread = threading.Thread(
            target=self._run,
            args=(migration_id,),
            daemon=True,
            name=f"worker-{migration_id[:8]}",
        )
        thread.start()
        logger.info("MigrationWorker: thread iniciada para %s", migration_id)

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    def _run(self, migration_id: str) -> None:
        """Ponto de entrada da thread — captura exceções de infraestrutura."""
        try:
            self._storage.update_status(migration_id, "processing")
            self._execute_pipeline(migration_id)
            self._storage.update_status(migration_id, "done")
            logger.info("MigrationWorker: migração %s concluída com sucesso", migration_id)
        except Exception as exc:  # noqa: BLE001
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.exception("MigrationWorker: falha na migração %s", migration_id)
            self._storage.update_status(migration_id, "failed", error=error_msg)

    def _execute_pipeline(self, migration_id: str) -> None:
        """Executa as etapas do pipeline em sequência."""
        from sas2dbx.analyze.classifier import classify_block
        from sas2dbx.analyze.dependency import DependencyAnalyzer
        from sas2dbx.analyze.parser import extract_block_deps
        from sas2dbx.document.architecture import ArchitectureDocumentor
        from sas2dbx.document.job_doc import JobDocumentor
        from sas2dbx.document.visual import ArchitectureExplorer
        from sas2dbx.ingest.reader import read_sas_file, split_blocks
        from sas2dbx.ingest.scanner import scan_directory
        from sas2dbx.models.migration_result import MigrationResult
        from sas2dbx.transpile.engine import TranspilationEngine
        from sas2dbx.transpile.llm.client import LLMClient

        migration_dir = self._storage.get_migration_dir(migration_id)
        input_dir = migration_dir / "input"
        output_dir = migration_dir / "output"
        docs_dir = migration_dir / "docs"

        # 1 — Extrai zip → input/
        zip_path = migration_dir / "upload.zip"
        _extract_zip(zip_path, input_dir)
        logger.info("MigrationWorker [%s]: zip extraído para %s", migration_id, input_dir)

        # 2 — Scan
        sas_files = scan_directory(input_dir)
        if not sas_files:
            logger.warning("MigrationWorker [%s]: nenhum .sas encontrado", migration_id)
            return

        logger.info(
            "MigrationWorker [%s]: %d arquivo(s) .sas encontrado(s)",
            migration_id,
            len(sas_files),
        )

        # 3 — Analyze
        meta = self._storage.get_meta(migration_id)
        autoexec_filename = meta.get("config", {}).get("autoexec_filename", "autoexec.sas")
        autoexec_path = input_dir / autoexec_filename

        analyzer = DependencyAnalyzer(
            autoexec_path=autoexec_path if autoexec_path.exists() else None,
        )
        graph = analyzer.analyze(sas_files)
        execution_order = graph.get_execution_order()

        # 4 — Transpilação
        engine = TranspilationEngine(output_dir=output_dir, resume=False)
        migration_results: list[MigrationResult] = engine.run(sas_files, execution_order)

        # 5 — Documentação por job (LLM)
        jobs_code: dict[str, str] = {}
        jobs_block_deps: dict[str, list] = {}
        jobs_classifications: dict[str, list] = {}

        for sas_file in sas_files:
            job_name = sas_file.path.stem
            code, _ = read_sas_file(sas_file.path)
            jobs_code[job_name] = code
            blocks = split_blocks(code, source_file=sas_file.path)
            jobs_block_deps[job_name] = [extract_block_deps(b) for b in blocks]
            jobs_classifications[job_name] = [classify_block(b.raw_code) for b in blocks]

        llm_client = LLMClient(self._llm_config)
        doc_engine = JobDocumentor(llm_client=llm_client)
        jobs_dir = docs_dir / "jobs"
        job_docs: dict[str, str] = {}

        for sas_file in sas_files:
            job_name = sas_file.path.stem
            try:
                doc_result = doc_engine.generate_doc_sync(
                    job_name=job_name,
                    sas_code=jobs_code[job_name],
                    block_deps=jobs_block_deps.get(job_name, []),
                    classification_results=jobs_classifications.get(job_name, []),
                    graph=graph,
                )
                doc_engine.write_doc(doc_result, jobs_dir)
                job_docs[job_name] = doc_result.content
                logger.debug("MigrationWorker [%s]: doc gerada para %s", migration_id, job_name)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MigrationWorker [%s]: falha ao documentar %s: %s",
                    migration_id,
                    job_name,
                    exc,
                )
                job_docs[job_name] = f"# {job_name}\n\n*Documentação não gerada: {exc}*\n"

        # 6 — ARCHITECTURE.md
        arch_doc = ArchitectureDocumentor()
        arch_md = arch_doc.generate_architecture_md(graph, migration_results, job_docs)
        arch_doc.write(arch_md, docs_dir)
        logger.info("MigrationWorker [%s]: ARCHITECTURE.md gerado", migration_id)

        # 7 — Architecture Explorer HTML
        project_name = meta.get("config", {}).get("original_filename", migration_id)
        explorer = ArchitectureExplorer(project_name=project_name)
        html = explorer.generate_html(graph, migration_results, job_docs)
        html_path = migration_dir / "explorer.html"
        html_path.write_text(html, encoding="utf-8")
        logger.info("MigrationWorker [%s]: explorer.html gerado", migration_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_zip(zip_path: Path, dest: Path) -> None:
    """Extrai zip_path para dest com proteção contra zip-slip.

    Raises:
        ValueError: Se algum entry tentasse escrever fora de dest (zip-slip).
        zipfile.BadZipFile: Se o arquivo não é um zip válido.
    """
    dest.mkdir(parents=True, exist_ok=True)
    dest_resolved = dest.resolve()

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            # Proteção zip-slip: resolve o caminho e verifica que está dentro de dest
            target = (dest / member.filename).resolve()
            try:
                target.relative_to(dest_resolved)
            except ValueError:
                raise ValueError(
                    f"Zip-slip detectado: entry '{member.filename}' aponta para fora de dest"
                ) from None
            zf.extract(member, dest)
