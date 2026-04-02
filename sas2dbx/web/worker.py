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

from sas2dbx.knowledge.global_catalog import GlobalCatalog
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
        self._catalog = GlobalCatalog(storage.work_dir)

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

    def start_healing(
        self,
        migration_id: str,
        healing_id: str,
        config,
        body,
    ) -> None:
        """Dispara o pipeline de self-healing em background thread.

        Args:
            migration_id: UUID da migração.
            healing_id: UUID gerado para este processo de healing.
            config: DatabricksConfig com credenciais.
            body: HealRequest com notebook_name, execution_result e max_iterations.
        """
        thread = threading.Thread(
            target=self._run_healing,
            args=(migration_id, healing_id, config, body),
            daemon=True,
            name=f"heal-{healing_id[:8]}",
        )
        thread.start()
        logger.info(
            "MigrationWorker: healing %s iniciado para migração %s",
            healing_id, migration_id,
        )

    def start_validation(
        self,
        migration_id: str,
        config,
        tables: list[str] | None = None,
        deploy_only: bool = False,
        collect_only: bool = False,
    ) -> None:
        """Dispara o pipeline de validação Databricks em background thread.

        Args:
            migration_id: UUID da migração a validar.
            config: DatabricksConfig com credenciais e parâmetros.
            tables: Tabelas a coletar após execução.
            deploy_only: Se True, não executa o workflow.
            collect_only: Se True, não faz deploy — apenas coleta tabelas.
        """
        thread = threading.Thread(
            target=self._run_validation,
            args=(migration_id, config, tables or [], deploy_only, collect_only),
            daemon=True,
            name=f"validate-{migration_id[:8]}",
        )
        thread.start()
        logger.info("MigrationWorker: validação iniciada para %s", migration_id)

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    def _run_healing(
        self,
        migration_id: str,
        healing_id: str,
        config,
        body,
    ) -> None:
        """Pipeline de self-healing em background."""
        from sas2dbx.validate.executor import ExecutionResult
        from sas2dbx.validate.heal.pipeline import SelfHealingPipeline

        migration_dir = self._storage.get_migration_dir(migration_id)
        output_dir = migration_dir / "output"
        notebook_path = output_dir / f"{body.notebook_name}.py"

        def _save_healing(data: dict) -> None:
            meta = self._storage.get_meta(migration_id)
            if "healings" not in meta:
                meta["healings"] = {}
            meta["healings"][healing_id] = data
            self._storage.save_meta(migration_id, meta)

        # Persiste estado inicial para que o polling não receba 404
        _save_healing({"status": "running", "healed": False, "iterations": 0,
                       "strategy": "none", "description": "", "error": None})

        try:
            exec_result = ExecutionResult(
                run_id=body.execution_result.run_id,
                status=body.execution_result.status,
                duration_ms=body.execution_result.duration_ms,
                error=body.execution_result.error,
            )

            healing_history = self._storage.get_meta(migration_id).get("healing_history", [])
            pipeline = SelfHealingPipeline(
                config=config,
                max_iterations=body.max_iterations,
                healing_history=healing_history,
            )
            report = pipeline.heal(notebook_path, exec_result)

            healing_data = {
                "status": "done",
                "healed": report.healed,
                "iterations": report.iterations,
                "strategy": report.suggestion.strategy,
                "description": report.suggestion.description,
                "static_fixes_applied": report.static_fixes_applied,
                "upstream_fix_needed": report.upstream_fix_needed,
                "error": None,
            }
            _save_healing(healing_data)

            # GAP-6: acumula pattern_key no healing_history para uso proativo
            # pelo StaticNotebookValidator na próxima sessão de validação
            if report.diagnostic is not None:
                meta = self._storage.get_meta(migration_id)
                history = meta.setdefault("healing_history", [])
                history.append({
                    "fix_key": report.diagnostic.pattern_key,
                    "category": report.diagnostic.category,
                    "notebook": body.notebook_name,
                    "healed": report.healed,
                    "static_fixes_applied": report.static_fixes_applied,
                })
                if report.upstream_fix_needed:
                    logger.warning(
                        "MigrationWorker[heal %s]: upstream dependency detectada — "
                        "notebook '%s' deveria ter criado a tabela ausente",
                        healing_id, report.upstream_fix_needed,
                    )
                self._storage.save_meta(migration_id, meta)

                # GAP-G: alimenta catálogo global cross-migração
                self._catalog.record_error(
                    pattern_key=report.diagnostic.pattern_key,
                    symptom=report.diagnostic.category,
                    category=report.diagnostic.category,
                    auto_fixed=report.healed,
                    notebook=body.notebook_name,
                )

            logger.info(
                "MigrationWorker[heal %s]: concluído healed=%s iterations=%d static_fixes=%d",
                healing_id, report.healed, report.iterations, report.static_fixes_applied,
            )

        except Exception as exc:  # noqa: BLE001
            logger.exception("MigrationWorker[heal %s]: falha", healing_id)
            _save_healing({
                "status": "failed",
                "healed": False,
                "iterations": 0,
                "strategy": "none",
                "description": "",
                "error": f"{type(exc).__name__}: {exc}",
            })

    def _run_validation(
        self,
        migration_id: str,
        config,
        tables: list[str],
        deploy_only: bool,
        collect_only: bool,
    ) -> None:
        """Pipeline de validação Databricks em background."""
        from sas2dbx.validate.collector import DatabricksCollector
        from sas2dbx.validate.deployer import DatabricksDeployer
        from sas2dbx.validate.executor import WorkflowExecutor
        from sas2dbx.validate.heal.static_validator import StaticNotebookValidator
        from sas2dbx.validate.report import generate_validation_report

        logger.info("MigrationWorker[validate %s]: thread iniciada", migration_id)

        migration_dir = self._storage.get_migration_dir(migration_id)
        output_dir = migration_dir / "output"
        notebooks = sorted(output_dir.glob("*.py"))

        # H1 — Pré-validação estática: corrige padrões conhecidos antes do deploy
        # GAP-6: passa healing_history para que fixes conhecidos de sessões anteriores
        # sejam aplicados proativamente sem esperar o erro ocorrer novamente
        healing_history = self._storage.get_meta(migration_id).get("healing_history", [])
        validator = StaticNotebookValidator(
            catalog=config.catalog,
            schema=config.schema,
            healing_history=healing_history,
        )
        if not collect_only and notebooks:
            static_reports = validator.validate_directory(output_dir)
            if static_reports:
                logger.info(
                    "MigrationWorker[validate %s]: pré-validação aplicou %d correção(ões) estáticas",
                    migration_id, sum(len(r.fixes) for r in static_reports),
                )

        logger.info(
            "MigrationWorker[validate %s]: %d notebook(s) encontrado(s) em %s",
            migration_id, len(notebooks), output_dir,
        )

        notebook_results: list[dict] = []
        table_validations: list = []

        def _save_progress() -> None:
            """Persiste resultados parciais para polling do frontend."""
            meta = self._storage.get_meta(migration_id)
            meta["validation"] = {
                "status": "running",
                "notebook_results": notebook_results,
            }
            self._storage.save_meta(migration_id, meta)

        try:
            deployer = DatabricksDeployer(config) if not collect_only else None
            executor = WorkflowExecutor(config) if not (deploy_only or collect_only) else None
            total = len(notebooks)
            exec_result = None  # inicializado para que a guarda de falha seja segura

            # GAP-A: preflight — detecta fontes fantasma antes do deploy
            if not collect_only and notebooks:
                from sas2dbx.validate.preflight import PreflightChecker
                preflight = PreflightChecker(config)
                preflight_report = preflight.check(output_dir)
                logger.info(
                    "MigrationWorker[validate %s]: preflight — %s",
                    migration_id, preflight_report.summary(),
                )
                # Persiste resultado e alimenta catálogo global
                meta = self._storage.get_meta(migration_id)
                meta.setdefault("validation", {})["preflight"] = {
                    "summary": preflight_report.summary(),
                    "ghost_sources": [
                        {"table": g.table_name, "notebooks": g.notebooks, "suggestion": g.suggestion}
                        for g in preflight_report.ghost_sources
                    ],
                    "skipped": preflight_report.skipped,
                }
                self._storage.save_meta(migration_id, meta)
                for ghost in preflight_report.ghost_sources:
                    self._catalog.record_ghost_source(
                        table_name=ghost.table_name,
                        migration_id=migration_id,
                        notebook=ghost.notebooks[0] if ghost.notebooks else "",
                    )

            for idx, nb in enumerate(notebooks):
                nb_name = nb.stem
                logger.info(
                    "MigrationWorker[validate %s]: [%d/%d] processando %s",
                    migration_id, idx + 1, total, nb_name,
                )
                nb_result: dict = {"name": nb_name, "index": idx + 1, "total": total}

                # Deploy
                if not collect_only:
                    deploy_result = deployer.deploy(nb, nb_name)
                    nb_result["job_id"] = deploy_result.job_id
                    nb_result["workspace_path"] = deploy_result.workspace_path
                    logger.info(
                        "MigrationWorker[validate %s]: [%d/%d] deploy ok — job_id=%d",
                        migration_id, idx + 1, total, deploy_result.job_id,
                    )

                # Execução
                if not deploy_only and not collect_only:
                    exec_result = executor.execute(deploy_result.job_id)
                    deploy_result.run_id = exec_result.run_id
                    nb_result["run_id"] = exec_result.run_id
                    nb_result["status"] = exec_result.status
                    nb_result["duration_ms"] = exec_result.duration_ms
                    nb_result["error"] = exec_result.error
                    logger.info(
                        "MigrationWorker[validate %s]: [%d/%d] execução %s — run_id=%d",
                        migration_id, idx + 1, total, exec_result.status, exec_result.run_id,
                    )

                notebook_results.append(nb_result)
                _save_progress()

                # GAP-2: antes de interromper, tenta static healing inline.
                # Aplica somente quando temos deployer + executor ativos (não em
                # deploy_only / collect_only) e o notebook acabou de falhar.
                exec_failed = (
                    not deploy_only
                    and not collect_only
                    and exec_result is not None
                    and exec_result.status != "SUCCESS"
                )
                if exec_failed:
                    static_report = validator.validate_notebook(nb)
                    if static_report.changed:
                        logger.info(
                            "MigrationWorker[validate %s]: [%d/%d] static healing aplicou %d fix(es) em %s "
                            "— re-tentando execução",
                            migration_id, idx + 1, total, len(static_report.fixes), nb_name,
                        )
                        nb_result["static_fixes"] = static_report.fixes
                        # Re-deploy + re-execução com notebook corrigido (UMA tentativa)
                        try:
                            deploy_result = deployer.deploy(nb, nb_name)
                            nb_result["job_id"] = deploy_result.job_id
                            exec_result = executor.execute(deploy_result.job_id)
                            deploy_result.run_id = exec_result.run_id
                            nb_result["run_id"] = exec_result.run_id
                            nb_result["status"] = exec_result.status
                            nb_result["duration_ms"] = exec_result.duration_ms
                            nb_result["error"] = exec_result.error
                            logger.info(
                                "MigrationWorker[validate %s]: [%d/%d] re-execução após static healing: %s",
                                migration_id, idx + 1, total, exec_result.status,
                            )
                        except Exception as retry_exc:  # noqa: BLE001
                            logger.warning(
                                "MigrationWorker[validate %s]: [%d/%d] falha no re-deploy após static healing: %s",
                                migration_id, idx + 1, total, retry_exc,
                            )

                if exec_failed and exec_result.status != "SUCCESS":
                    logger.warning(
                        "MigrationWorker[validate %s]: [%d/%d] falha em %s — interrompendo sequência",
                        migration_id, idx + 1, total, nb_name,
                    )
                    break

            # Coleta de tabelas (após todos os notebooks)
            if tables:
                collector = DatabricksCollector(config)
                table_validations = collector.collect(tables)

            # Relatório final
            succeeded = sum(1 for r in notebook_results if r.get("status") == "SUCCESS")
            failed = sum(1 for r in notebook_results if r.get("status") not in ("SUCCESS", None))
            overall = "success" if succeeded == total else ("partial" if succeeded > 0 else "failed")

            report = {
                "summary": {
                    "total_notebooks": total,
                    "notebooks_ok": succeeded,
                    "notebooks_failed": failed,
                    "notebooks_skipped": total - len(notebook_results),
                    "total_rows_collected": sum(
                        tv.row_count for tv in table_validations if tv.error is None
                    ),
                    "overall_status": overall,
                },
                "notebooks": notebook_results,
                "tables": [
                    {
                        "table_name": tv.table_name,
                        "row_count": tv.row_count,
                        "error": tv.error,
                        "status": "ok" if tv.error is None else "error",
                    }
                    for tv in table_validations
                ],
            }

            meta = self._storage.get_meta(migration_id)
            meta["validation"] = {"status": "done", "report": report}
            self._storage.save_meta(migration_id, meta)
            logger.info(
                "MigrationWorker[validate %s]: concluído — %d/%d ok",
                migration_id, succeeded, total,
            )

        except Exception as exc:  # noqa: BLE001
            logger.exception("MigrationWorker[validate %s]: falha", migration_id)
            meta = self._storage.get_meta(migration_id)
            meta["validation"] = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "notebook_results": notebook_results,
            }
            self._storage.save_meta(migration_id, meta)

    def _run(self, migration_id: str) -> None:
        """Ponto de entrada da thread — captura exceções de infraestrutura."""
        try:
            self._storage.update_status(migration_id, "processing")
            self._execute_pipeline(migration_id)
            self._storage.update_status(migration_id, "done")
            self._catalog.record_migration(migration_id)
            logger.info("MigrationWorker: migração %s concluída com sucesso", migration_id)
        except Exception as exc:  # noqa: BLE001
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.exception("MigrationWorker: falha na migração %s", migration_id)
            # Marca etapa em execução como falha para o frontend refletir
            try:
                meta = self._storage.get_meta(migration_id)
                for step in meta.get("pipeline_steps", []):
                    if step["status"] == "running":
                        step["status"] = "failed"
                self._storage.save_meta(migration_id, meta)
            except Exception:  # noqa: BLE001
                pass
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

        def step(step_id: str, status: str, detail: str = "") -> None:
            self._storage.update_pipeline_step(migration_id, step_id, status, detail)

        migration_dir = self._storage.get_migration_dir(migration_id)
        input_dir = migration_dir / "input"
        output_dir = migration_dir / "output"
        docs_dir = migration_dir / "docs"

        # Inicializa etapas no meta.json
        self._storage.init_pipeline_steps(migration_id)

        # 1 — Extrai zip → input/
        zip_path = migration_dir / "upload.zip"
        step("extract", "running")
        _extract_zip(zip_path, input_dir)
        step("extract", "done")
        logger.info("MigrationWorker [%s]: zip extraído para %s", migration_id, input_dir)

        # 2 — Scan
        step("scan", "running")
        sas_files = scan_directory(input_dir)
        if not sas_files:
            step("scan", "failed", "Nenhum .sas encontrado")
            logger.warning("MigrationWorker [%s]: nenhum .sas encontrado", migration_id)
            return
        step("scan", "done", f"{len(sas_files)} arquivo(s) .sas")
        logger.info(
            "MigrationWorker [%s]: %d arquivo(s) .sas encontrado(s)",
            migration_id,
            len(sas_files),
        )

        # 3 — Analyze
        step("analyze", "running")
        meta = self._storage.get_meta(migration_id)
        autoexec_filename = meta.get("config", {}).get("autoexec_filename", "autoexec.sas")
        autoexec_path = input_dir / autoexec_filename

        analyzer = DependencyAnalyzer(
            autoexec_path=autoexec_path if autoexec_path.exists() else None,
        )
        graph = analyzer.analyze(sas_files)
        execution_order = graph.get_execution_order()
        step("analyze", "done", f"{len(graph.jobs)} job(s) · {len(graph.implicit_edges)} dep(s) implícita(s)")

        # 4 — Transpilação (com LLM para Tier 2 se configurado)
        step("transpile", "running", f"0 / {len(sas_files)} jobs")
        llm_client = LLMClient(self._llm_config)
        engine = TranspilationEngine(
            output_dir=output_dir,
            resume=False,
            llm_client=llm_client if llm_client._primary.is_available() else None,
            catalog=meta.get("config", {}).get("catalog", "main"),
            schema=meta.get("config", {}).get("schema", "migrated"),
        )
        migration_results: list[MigrationResult] = engine.run(sas_files, execution_order)
        done_count = sum(1 for r in migration_results if r.status.value == "done")
        step("transpile", "done", f"{done_count} / {len(sas_files)} jobs concluídos")

        # 4b — GAP-F: verifica risco de encoding nos arquivos SAS
        from sas2dbx.ingest.reader import check_encoding_risk
        encoding_warnings: list[str] = []
        for sas_file in sas_files:
            _, enc_used = read_sas_file(sas_file.path)
            w = check_encoding_risk(sas_file.path, enc_used)
            if w:
                encoding_warnings.append(f"{sas_file.path.name}: {w}")
        if encoding_warnings:
            meta = self._storage.get_meta(migration_id)
            meta.setdefault("warnings", []).extend(encoding_warnings)
            self._storage.save_meta(migration_id, meta)
            logger.warning(
                "MigrationWorker [%s]: %d aviso(s) de encoding detectado(s)",
                migration_id, len(encoding_warnings),
            )

        # 4c — GAP-C: análise de idempotência dos notebooks gerados
        from sas2dbx.analyze.idempotency import IdempotencyAnalyzer
        idempotency_reports = IdempotencyAnalyzer().analyze_directory(output_dir)
        idempotency_issues = [
            {"notebook": r.notebook, "summary": r.summary(),
             "issues": [{"type": i.issue_type, "description": i.description,
                         "recommendation": i.recommendation, "line": i.line}
                        for i in r.issues]}
            for r in idempotency_reports if not r.is_idempotent
        ]
        if idempotency_issues:
            meta = self._storage.get_meta(migration_id)
            meta["idempotency"] = {"issues": idempotency_issues}
            self._storage.save_meta(migration_id, meta)
            logger.info(
                "MigrationWorker [%s]: %d notebook(s) com problemas de idempotência",
                migration_id, len(idempotency_issues),
            )

        # 4d — GAP-E: riscos semânticos nos notebooks gerados
        from sas2dbx.validate.semantic_risk import SemanticRiskAnalyzer
        semantic_reports = SemanticRiskAnalyzer().analyze_directory(output_dir)
        semantic_risks = [
            {"notebook": r.notebook, "summary": r.summary(),
             "risks": [{"code": risk.code, "description": risk.description,
                        "mitigation": risk.mitigation, "line": risk.line}
                       for risk in r.risks]}
            for r in semantic_reports
        ]
        if semantic_risks:
            meta = self._storage.get_meta(migration_id)
            meta["semantic_risks"] = {"reports": semantic_risks}
            self._storage.save_meta(migration_id, meta)
            logger.info(
                "MigrationWorker [%s]: %d notebook(s) com riscos semânticos",
                migration_id, len(semantic_risks),
            )

        # 4e — GAP-B: gera notebooks de reconciliação
        from sas2dbx.generate.reconciliation import ReconciliationGenerator, ReconciliationConfig
        recon_gen = ReconciliationGenerator(
            ReconciliationConfig(
                catalog=meta.get("config", {}).get("catalog", "main"),
                schema=meta.get("config", {}).get("schema", "migrated"),
            )
        )
        recon_notebooks = recon_gen.generate_directory(output_dir)
        if recon_notebooks:
            meta = self._storage.get_meta(migration_id)
            meta["reconciliation"] = {
                "notebooks": [nb.name for nb in recon_notebooks],
                "count": len(recon_notebooks),
            }
            self._storage.save_meta(migration_id, meta)
            logger.info(
                "MigrationWorker [%s]: %d notebook(s) de reconciliação gerado(s)",
                migration_id, len(recon_notebooks),
            )

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

        doc_engine = JobDocumentor(llm_client=llm_client)
        jobs_dir = docs_dir / "jobs"
        job_docs: dict[str, str] = {}
        n_jobs = len(sas_files)

        has_llm = self._llm_config.api_key or self._llm_config.khon_token
        logger.info(
            "MigrationWorker [%s]: iniciando documentação — %d jobs, LLM=%s",
            migration_id, n_jobs, "sim" if has_llm else "stub",
        )

        for idx, sas_file in enumerate(sas_files):
            job_name = sas_file.path.stem
            step("document", "running", f"{job_name} ({idx + 1}/{n_jobs})")
            try:
                logger.info("MigrationWorker [%s]: documentando %s...", migration_id, job_name)
                doc_result = doc_engine.generate_doc_sync(
                    job_name=job_name,
                    sas_code=jobs_code[job_name],
                    block_deps=jobs_block_deps.get(job_name, []),
                    classification_results=jobs_classifications.get(job_name, []),
                    graph=graph,
                )
                doc_engine.write_doc(doc_result, jobs_dir)
                job_docs[job_name] = doc_result.content
                logger.info(
                    "MigrationWorker [%s]: doc gerada para %s (tokens=%d)",
                    migration_id, job_name, doc_result.tokens_used,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MigrationWorker [%s]: falha ao documentar %s: %s",
                    migration_id,
                    job_name,
                    exc,
                )
                job_docs[job_name] = f"# {job_name}\n\n*Documentação não gerada: {exc}*\n"

        step("document", "done", f"{n_jobs} job(s) documentados")

        # 6 — ARCHITECTURE.md
        step("architecture", "running")
        arch_doc = ArchitectureDocumentor()
        arch_md = arch_doc.generate_architecture_md(graph, migration_results, job_docs)
        arch_doc.write(arch_md, docs_dir)
        step("architecture", "done")
        logger.info("MigrationWorker [%s]: ARCHITECTURE.md gerado", migration_id)

        # 7 — Architecture Explorer HTML
        step("explorer", "running")
        project_name = meta.get("config", {}).get("original_filename", migration_id)
        explorer = ArchitectureExplorer(project_name=project_name)
        html = explorer.generate_html(graph, migration_results, job_docs)
        html_path = migration_dir / "explorer.html"
        html_path.write_text(html, encoding="utf-8")
        step("explorer", "done")
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
