"""Engine de transpilação SAS → PySpark/Databricks.

Orquestra o pipeline: analyze → transpile por bloco → write output.
Suporta checkpointing via MigrationStateManager e flag --resume.

T.1: Engine integration — wiring com KnowledgeStore, LLMClient e NotebookGenerator.
H.3: Auto-merge — executa build-mappings após on-demand harvests.
"""

from __future__ import annotations

import datetime
import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING

from sas2dbx.generate.notebook import Cell, CellModel, CellType, NotebookGenerator
from sas2dbx.models.migration_result import JobStatus, MigrationResult, SASOrigin
from sas2dbx.models.sas_ast import SASBlock, SASFile, Tier
from sas2dbx.transpile.llm.validator import validate_pyspark
from sas2dbx.transpile.state import MigrationStateManager

if TYPE_CHECKING:
    from sas2dbx.knowledge.store import KnowledgeStore
    from sas2dbx.transpile.llm.client import LLMClient

logger = logging.getLogger(__name__)

# Palavras-chave SAS que não são nomes de funções — excluídas da extração de func_names
_SAS_KEYWORD_SKIP: frozenset[str] = frozenset({
    "DATA", "PROC", "FROM", "WHERE", "SELECT", "CREATE", "INSERT",
    "UPDATE", "INTO", "SET", "OUTPUT", "MERGE", "RETAIN", "KEEP",
    "DROP", "IF", "THEN", "ELSE", "DO", "END", "BY", "ORDER",
})


class TranspilationEngine:
    """Orquestra a transpilação de um conjunto de jobs SAS.

    Args:
        output_dir: Diretório onde notebooks e state file serão gravados.
        resume: Se True, carrega estado anterior e pula jobs já concluídos.
        knowledge_store: KnowledgeStore para enriquecer prompts com contexto.
        llm_client: LLMClient para transpilação Tier 1/2 via LLM.
        catalog: Unity Catalog de destino (default: "main").
        schema: Schema de destino (default: "migrated").
        notebook_format: "py" (Databricks) ou "ipynb" (Jupyter).
    """

    def __init__(
        self,
        output_dir: Path,
        resume: bool = False,
        knowledge_store: KnowledgeStore | None = None,
        llm_client: LLMClient | None = None,
        catalog: str = "main",
        schema: str = "migrated",
        notebook_format: str = "py",
    ) -> None:
        self._output_dir = output_dir
        self._resume = resume
        self._state = MigrationStateManager(output_dir)
        self._knowledge_store = knowledge_store
        self._llm_client = llm_client
        self._catalog = catalog
        self._schema = schema
        self._notebook_format = notebook_format
        self._generator = NotebookGenerator(notebook_format=notebook_format)
        self._ks_harvest_baseline: int = 0  # H.3: baseline de _harvest_attempted antes de run()

    def run(self, sas_files: list[SASFile], execution_order: list[str]) -> list[MigrationResult]:
        """Executa a transpilação de todos os jobs na ordem fornecida.

        Falha em job N não interrompe jobs N+1..M.

        Args:
            sas_files: Lista de SASFiles a transpilar.
            execution_order: Ordem topológica de execução dos jobs.

        Returns:
            Lista de MigrationResult, um por job na execution_order.
        """
        file_map = {f.path.stem: f for f in sas_files}

        if self._resume:
            loaded = self._state.load()
            if not loaded:
                logger.warning(
                    "Engine: --resume solicitado mas state não encontrado — iniciando do zero"
                )
                self._state.init_fresh(execution_order)
        else:
            self._state.init_fresh(execution_order)

        # H.3: captura baseline para detectar on-demand harvests reais ao longo do run
        if self._knowledge_store is not None:
            self._ks_harvest_baseline = len(self._knowledge_store._harvest_attempted)

        jobs_to_run = self._state.get_pending_jobs(execution_order)

        logger.info(
            "Engine: %d job(s) para processar (de %d total)",
            len(jobs_to_run),
            len(execution_order),
        )

        results: list[MigrationResult] = []

        for job_id in execution_order:
            if job_id not in jobs_to_run:
                result = MigrationResult(
                    job_id=job_id,
                    status=JobStatus.DONE,
                    confidence=1.0
                    if self._state.get_job_status(job_id) == JobStatus.DONE
                    else 0.0,
                )
                results.append(result)
                continue

            sas_file = file_map.get(job_id)
            if sas_file is None:
                logger.warning("Engine: job '%s' não encontrado em sas_files — pulando", job_id)
                continue

            self._state.mark_started(job_id)
            result = self._transpile_job(job_id, sas_file)
            results.append(result)

            if result.status == JobStatus.DONE:
                self._state.mark_done(job_id, result)
            else:
                self._state.mark_failed(job_id, result.error or "erro desconhecido")

        # H.3 — Auto-merge: se on-demand harvests ocorreram, reconstruir merged/
        self._maybe_rebuild_mappings()

        return results

    # -------------------------------------------------------------------------
    # Internal — transpilação de um job
    # -------------------------------------------------------------------------

    def _transpile_job(self, job_id: str, sas_file: SASFile) -> MigrationResult:
        """Transpila um único job SAS.

        Fluxo por bloco:
          Tier.RULE / Tier.LLM → enriquece contexto do KS + chama LLM
          Tier.MANUAL          → flag com SAS original como comentário
          sem LLMClient        → stub com SAS original comentado
        """
        try:
            from sas2dbx.analyze.classifier import classify_block
            from sas2dbx.ingest.reader import read_sas_file, split_blocks
            from sas2dbx.transpile.llm.context import format_context_for_prompt
            from sas2dbx.transpile.llm.prompts import build_transpile_prompt, strip_code_fences

            raw_code, encoding = read_sas_file(sas_file.path)
            sas_file.encoding = encoding

            blocks = split_blocks(raw_code, source_file=sas_file.path)
            if not blocks:
                blocks = [SASBlock(
                    raw_code=raw_code or "/* empty */",
                    start_line=1,
                    end_line=1,
                    source_file=sas_file.path,
                )]

            cells: list[Cell] = []
            confidence_scores: list[float] = []
            warnings: list[str] = []

            for order_idx, block in enumerate(blocks):
                classification = classify_block(block.raw_code)
                block.classification = classification

                if classification.tier == Tier.MANUAL:
                    pyspark_code = _flag_manual_block(
                        block.raw_code,
                        classification.construct_type,
                        sas_file.path.name,
                        block.start_line,
                    )
                    confidence_scores.append(0.0)
                    warnings.append(
                        f"{classification.construct_type} (linha {block.start_line})"
                        " requer revisão manual"
                    )

                elif self._llm_client is not None:
                    context_text = ""
                    if self._knowledge_store is not None:
                        ctx = self._build_ks_context(
                            block.raw_code, classification.construct_type
                        )
                        context_text = format_context_for_prompt(ctx)

                    prompt = build_transpile_prompt(
                        sas_code=block.raw_code,
                        construct_type=classification.construct_type,
                        catalog=self._catalog,
                        schema=self._schema,
                        context_text=context_text,
                    )
                    response = self._llm_client.complete_sync(prompt)
                    pyspark_code = strip_code_fences(response.content)
                    confidence_scores.append(classification.confidence)

                else:
                    pyspark_code = _stub_block(
                        block.raw_code,
                        classification.construct_type,
                        sas_file.path.name,
                    )
                    confidence_scores.append(0.0)
                    warnings.append(
                        f"LLM não configurado — bloco {classification.construct_type}"
                        f" (linha {block.start_line}) não transpilado"
                    )

                cells.append(Cell(
                    cell_type=CellType.CODE,
                    source=pyspark_code,
                    sas_origin=SASOrigin(
                        job_name=job_id,
                        start_line=block.start_line,
                        end_line=block.end_line,
                        construct_type=classification.construct_type,
                    ),
                    order=order_idx,
                ))

            avg_confidence = (
                sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
            )

            model = CellModel(
                job_name=job_id,
                sas_source_file=sas_file.path.name,
                migration_date=datetime.date.today().isoformat(),
                confidence=avg_confidence,
                warnings=warnings,
                cells=cells,
            )

            output_path = self._output_dir / job_id
            actual_path = self._generator.generate(model, output_path)

            all_code = "\n".join(c.source for c in cells if c.cell_type == CellType.CODE)
            validation = validate_pyspark(all_code)

            if not validation.is_valid:
                logger.warning(
                    "Engine: job '%s' com erros de validação: %s",
                    job_id,
                    [e.code for e in validation.errors],
                )

            logger.info(
                "Engine: job '%s' transpilado [%d bloco(s), conf=%.2f] → %s",
                job_id, len(blocks), avg_confidence, actual_path,
            )

            return MigrationResult(
                job_id=job_id,
                status=JobStatus.DONE,
                confidence=avg_confidence,
                output_path=str(actual_path),
                warnings=warnings,
                validation_result=validation,
            )

        except Exception as exc:
            logger.error("Engine: falha ao transpilar '%s': %s", job_id, exc)
            return MigrationResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error=str(exc),
            )

    def _build_ks_context(self, raw_code: str, construct_type: str) -> dict:
        """Extrai keys relevantes do bloco e consulta o KnowledgeStore."""
        from sas2dbx.transpile.llm.context import build_context

        proc_names: list[str] = []
        if construct_type.startswith("PROC_"):
            proc_names = [construct_type[len("PROC_"):]]

        sql_constructs: list[str] = []
        if construct_type == "PROC_SQL":
            for kw in ("CALCULATED", "MONOTONIC", "DATEPART", "DATETIME_LITERALS"):
                if kw in raw_code.upper():
                    sql_constructs.append(kw)

        sas_func_re = re.compile(r"\b([A-Z][A-Z0-9_]{1,})\s*\(", re.IGNORECASE)
        func_names = list({
            m.group(1).upper()
            for m in sas_func_re.finditer(raw_code)
            if m.group(1).upper() not in _SAS_KEYWORD_SKIP
        })[:15]

        return build_context(
            self._knowledge_store,
            func_names=func_names,
            proc_names=proc_names,
            sql_constructs=sql_constructs,
        )

    def _maybe_rebuild_mappings(self) -> None:
        """H.3 — Auto-merge: reconstrói merged/ se on-demand harvests reais ocorreram.

        Usa `_harvest_attempted` do KnowledgeStore como proxy correto: qualquer
        nova entrada adicionada ao set durante run() indica que o engine tentou
        lookups além do cache inicial — sinal de que generated/ pode ter crescido.
        """
        if self._knowledge_store is None:
            return

        current = len(self._knowledge_store._harvest_attempted)
        new_harvests = current - self._ks_harvest_baseline
        if new_harvests <= 0:
            return

        logger.info(
            "Engine: %d on-demand harvest(s) detectados — reconstruindo merged/...",
            new_harvests,
        )
        try:
            from sas2dbx.knowledge.populate.normalizer import build_mappings

            base_path = self._knowledge_store.base_path
            result = build_mappings(base_path=str(base_path))
            total = sum(result.values())
            logger.info("Engine: merged/ atualizado — %d entradas totais", total)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Engine: falha ao reconstruir merged/: %s", exc)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _flag_manual_block(
    raw_code: str, construct_type: str, source_name: str, start_line: int
) -> str:
    """Produz Python com flag Tier.MANUAL e SAS original como comentário."""
    lines = [
        f"# WARNING: {construct_type} — requer revisão manual",
        f"# Origem: {source_name} (linha {start_line})",
        "#",
        "# Código SAS original preservado abaixo:",
    ]
    lines.extend(f"# {line}" for line in raw_code.splitlines())
    return "\n".join(lines)


def _stub_block(raw_code: str, construct_type: str, source_name: str) -> str:
    """Produz stub comentado quando LLM não está configurado."""
    lines = [
        f"# TODO: transpilar {construct_type} de {source_name}",
        "# Configure llm_client no TranspilationEngine para transpilação automática.",
        "#",
        "# Código SAS original:",
    ]
    lines.extend(f"# {line}" for line in raw_code.splitlines())
    return "\n".join(lines)
