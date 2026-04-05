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
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING

from sas2dbx.analyze.classifier import classify_block
from sas2dbx.generate.notebook import Cell, CellModel, CellType, NotebookGenerator
from sas2dbx.ingest.reader import read_sas_file, split_blocks
from sas2dbx.models.migration_result import JobStatus, MigrationResult, SASOrigin
from sas2dbx.models.sas_ast import SASBlock, SASFile, Tier
from sas2dbx.transpile.llm.context import (
    build_context,
    extract_macro_resolutions,
    format_context_for_prompt,
    format_schema_context,
    load_libnames,
    load_table_schemas,
)
from sas2dbx.transpile.llm.prompts import (
    build_system_prompt,
    build_transpile_prompt,
    build_user_message,
    strip_code_fences,
)
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

_SAS_FUNC_RE = re.compile(r"\b([A-Z][A-Z0-9_]{1,})\s*\(", re.IGNORECASE)


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
        on_progress: Callable[[str, str], None] | None = None,
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
        self._on_progress = on_progress
        self._ks_harvest_baseline: int = 0  # H.3: baseline de _harvest_attempted antes de run()
        # PP2-04: system prompt pré-calculado uma vez por engine instance — cacheado pela Anthropic
        self._system_prompt: str = build_system_prompt(catalog, schema)
        # Schema das tabelas de origem — carregado de custom/schemas.yaml se existir.
        # Injeta nomes de coluna reais no prompt para evitar que o LLM invente nomes.
        # _schemas_raw mantém o dict para filtragem por bloco; nunca injetar schemas de
        # tabelas de output para evitar contaminação cruzada entre jobs.
        self._schemas_raw: dict[str, list[str]] = self._load_schema_raw(output_dir)
        # Libnames para resolução de macro params (LIB.table → catalog.schema.table)
        self._libnames: dict[str, dict] = self._load_libnames(output_dir)

    def _load_schema_raw(self, output_dir: Path) -> dict[str, list[str]]:
        """Carrega schemas de custom/schemas.yaml e migration/schemas.yaml como dict bruto.

        Retorna dict table_fqn → list[column_name] para filtragem por bloco.
        """
        candidates = [
            output_dir.parent / "custom" / "schemas.yaml",  # global do projeto
            output_dir / "schemas.yaml",                     # específico da migração
        ]
        schemas: dict[str, list[str]] = {}
        for path in candidates:
            loaded = load_table_schemas(path)
            schemas.update(loaded)
            if loaded:
                logger.info(
                    "TranspilationEngine: %d schema(s) carregado(s) de %s",
                    len(loaded), path,
                )
        return schemas

    def _load_libnames(self, output_dir: Path) -> dict[str, dict]:
        """Carrega knowledge/custom/libnames.yaml para resolução de macro params."""
        candidates = [
            output_dir.parent / "custom" / "libnames.yaml",
            output_dir.parent.parent / "knowledge" / "custom" / "libnames.yaml",
        ]
        for path in candidates:
            data = load_libnames(path)
            if data:
                logger.info(
                    "TranspilationEngine: %d libname(s) carregado(s) de %s",
                    len(data), path,
                )
                return data
        return {}

    def _macro_resolution_for_block(self, sas_code: str) -> str:
        """Resolve parâmetros de macros paramétricas para tabelas e colunas reais.

        Ex: %rfm_score(ds_vendas=TELCO.vendas_raw) → resolve &ds_vendas →
        telcostar.operacional.vendas_raw → columns: id_venda, valor_bruto, ...

        Retorna string formatada para injeção no prompt, ou "" se não aplicável.
        """
        return extract_macro_resolutions(sas_code, self._schemas_raw, self._libnames)

    # Regex para extrair nomes de tabela do código SAS (forma lib.table ou apenas table)
    _RE_SAS_TABLE = re.compile(r'\b([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+)\b')

    def _schema_context_for_block(self, sas_code: str) -> str:
        """Retorna schema context filtrado para as tabelas referenciadas no bloco SAS.

        Evita contaminação cruzada: se schemas.yaml tiver rfm_clientes (output de
        run anterior) e o bloco SAS não referenciar essa tabela, ela não é injetada.

        Heurística: compara o nome curto de cada tabela em schemas.yaml contra
        identificadores pontilhados e palavras do código SAS.
        """
        if not self._schemas_raw:
            return ""

        # Extrai candidatos a nome de tabela do SAS: lib.table, lib.table.col ignorado
        sas_upper = sas_code.upper()
        # Coleta nomes curtos (após último ponto) de tabelas no schemas
        relevant: dict[str, list[str]] = {}
        for fqn, cols in self._schemas_raw.items():
            short = fqn.split(".")[-1].lower()
            # Inclui se o nome curto aparece em qualquer parte do código SAS
            if short in sas_upper.lower():
                relevant[fqn] = cols
            else:
                # Tenta também os segmentos intermediários (ex: "operacional" em lib.operacional.table)
                parts = fqn.split(".")
                if any(p.lower() in sas_upper.lower() for p in parts):
                    relevant[fqn] = cols

        if not relevant:
            # Fallback: injeta tudo se nenhuma tabela foi identificada no bloco
            # (bloco muito curto ou sem referência explícita de tabela)
            logger.debug(
                "TranspilationEngine: nenhuma tabela do schema identificada no bloco — injetando todos os %d schemas",
                len(self._schemas_raw),
            )
            relevant = self._schemas_raw

        logger.debug(
            "TranspilationEngine: schema filtrado para bloco: %d/%d tabela(s)",
            len(relevant), len(self._schemas_raw),
        )
        return format_schema_context(relevant)

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

        # Resultados em dict para reordenar de acordo com execution_order
        results_map: dict[str, MigrationResult] = {}
        _progress_lock = threading.Lock()  # protege on_progress (pode ser não thread-safe)

        def _run_job(job_id: str) -> MigrationResult:
            sas_file = file_map.get(job_id)
            if sas_file is None:
                logger.warning("Engine: job '%s' não encontrado em sas_files — pulando", job_id)
                return MigrationResult(job_id=job_id, status=JobStatus.FAILED, error="arquivo não encontrado")

            with _progress_lock:
                if self._on_progress:
                    self._on_progress(job_id, "running")
            self._state.mark_started(job_id)

            result = self._transpile_job(job_id, sas_file)

            if result.status == JobStatus.DONE:
                self._state.mark_done(job_id, result)
                with _progress_lock:
                    if self._on_progress:
                        self._on_progress(job_id, "done")
            else:
                self._state.mark_failed(job_id, result.error or "erro desconhecido")
                with _progress_lock:
                    if self._on_progress:
                        self._on_progress(job_id, "failed")
            return result

        # PP2-06: jobs são independentes durante transpilação (dependências são de execução,
        # não de geração de código) — paralelizar com ThreadPoolExecutor
        # Usa max 4 workers para não saturar a API do LLM
        max_workers = min(4, len(jobs_to_run)) if jobs_to_run else 1

        # Jobs já concluídos (resume) — não entram no pool
        for job_id in execution_order:
            if job_id not in jobs_to_run:
                results_map[job_id] = MigrationResult(
                    job_id=job_id,
                    status=JobStatus.DONE,
                    confidence=1.0
                    if self._state.get_job_status(job_id) == JobStatus.DONE
                    else 0.0,
                )
                with _progress_lock:
                    if self._on_progress:
                        self._on_progress(job_id, "skipped")

        if jobs_to_run:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                future_to_job = {pool.submit(_run_job, jid): jid for jid in jobs_to_run}
                for future in as_completed(future_to_job):
                    result = future.result()
                    results_map[result.job_id] = result

        # Reordena para preservar execution_order na saída
        results = [results_map[jid] for jid in execution_order if jid in results_map]

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
                    all_func_names: list[str] = []
                    ctx: dict = {}
                    if self._knowledge_store is not None:
                        all_func_names = _extract_sas_func_names(block.raw_code)
                        ctx = self._build_ks_context(
                            block.raw_code, classification.construct_type, all_func_names
                        )
                        context_text = format_context_for_prompt(ctx)

                    # PP2-04: user message dinâmico + system prompt estático (cacheado)
                    # Schema filtrado por bloco + resolução de macro params para evitar
                    # invenção de colunas via variáveis &param que ocultam a tabela real.
                    block_schema_ctx = self._schema_context_for_block(block.raw_code)
                    macro_res = self._macro_resolution_for_block(block.raw_code)
                    user_msg = build_user_message(
                        sas_code=block.raw_code,
                        construct_type=classification.construct_type,
                        context_text=context_text,
                        schema_context=block_schema_ctx,
                        macro_resolution=macro_res,
                    )
                    response = self._llm_client.complete_sync(
                        user_msg, system=self._system_prompt
                    )
                    pyspark_code = strip_code_fences(response.content)
                    confidence_scores.append(classification.confidence)

                    # Gap 3 — enriquece KB com funções não encontradas antes da transpilação
                    if self._knowledge_store is not None and all_func_names:
                        _known = set(ctx.get("function_mappings", {}).keys())
                        _unknown = [f for f in all_func_names if f not in _known]
                        if _unknown:
                            self._knowledge_store.enrich_from_transpilation(
                                unknown_funcs=_unknown,
                                sas_code=block.raw_code,
                                pyspark_code=pyspark_code,
                            )

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
            # PP2-07: gera notebook e captura conteúdo em memória para analyzers
            actual_path, notebook_content = self._generator.generate_with_content(model, output_path)

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
                notebook_content=notebook_content,
            )

        except Exception as exc:
            logger.error("Engine: falha ao transpilar '%s': %s", job_id, exc)
            return MigrationResult(
                job_id=job_id,
                status=JobStatus.FAILED,
                error=str(exc),
            )

    def _build_ks_context(
        self, raw_code: str, construct_type: str, func_names: list[str] | None = None
    ) -> dict:
        """Extrai keys relevantes do bloco e consulta o KnowledgeStore.

        Args:
            raw_code: Código SAS do bloco.
            construct_type: Tipo de construto classificado.
            func_names: Lista pré-computada de funções SAS. Se None, extrai de raw_code.
                Receber pré-computado evita extração duplicada quando _transpile_job()
                já precisa da lista para Gap 3.
        """
        if func_names is None:
            func_names = _extract_sas_func_names(raw_code)

        proc_names: list[str] = []
        if construct_type.startswith("PROC_"):
            proc_names = [construct_type[len("PROC_"):]]

        sql_constructs: list[str] = []
        if construct_type == "PROC_SQL":
            for kw in ("CALCULATED", "MONOTONIC", "DATEPART", "DATETIME_LITERALS"):
                if kw in raw_code.upper():
                    sql_constructs.append(kw)

        # Passa todas as funções encontradas — build_context aplica token budget via _fits()
        return build_context(
            self._knowledge_store,
            func_names=func_names,
            proc_names=proc_names,
            sql_constructs=sql_constructs,
            sas_code=raw_code,  # PP2-01: contexto para batch harvest de funções desconhecidas
        )

    def _maybe_rebuild_mappings(self) -> None:
        """H.3 — Auto-merge: reconstrói merged/ se on-demand harvests reais ocorreram.

        Usa `_harvest_attempted` do KnowledgeStore como proxy correto: qualquer
        nova entrada adicionada ao set durante run() indica que o engine tentou
        lookups além do cache inicial — sinal de que generated/ pode ter crescido.
        """
        if self._knowledge_store is None:
            return

        new_harvests = len(self._knowledge_store._harvest_attempted) - self._ks_harvest_baseline
        new_enrichments = getattr(self._knowledge_store, "_enriched_count", 0)
        if new_harvests <= 0 and new_enrichments <= 0:
            return
        total_new = new_harvests + new_enrichments

        logger.info(
            "Engine: %d on-demand harvest(s) + enriquecimento(s) detectados — reconstruindo merged/...",
            total_new,
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


def _extract_sas_func_names(raw_code: str) -> list[str]:
    """Extrai nomes únicos de funções SAS de um bloco (sem keywords SAS)."""
    return list({
        u
        for m in _SAS_FUNC_RE.finditer(raw_code)
        if (u := m.group(1).upper()) not in _SAS_KEYWORD_SKIP
    })


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
