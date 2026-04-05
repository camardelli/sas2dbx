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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
        # C1/C2: EvolutionEngine singleton — construído uma vez, compartilhado entre heals
        # Evita reconectar LLMClient e re-carregar HealthMonitor a cada _try_evolve()
        self._evolution_engine = None  # lazy init na primeira chamada (api_key pode não estar disponível no boot)
        # HealingKnowledgeBase singleton — compartilhado entre heals e evolution engine
        # Persiste em catalog_dir/healing_kb.json para sobreviver entre sessões
        from sas2dbx.validate.heal.knowledge_base import HealingKnowledgeBase
        _catalog_dir = storage.work_dir / "catalog"
        self._healing_kb = HealingKnowledgeBase(_catalog_dir / "healing_kb.json")
        # Cancelamento cooperativo: cada migration_id tem um Event que a thread verifica
        self._cancel_events: dict[str, threading.Event] = {}

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

    def cancel(self, migration_id: str) -> None:
        """Sinaliza cancelamento cooperativo para a thread da migração.

        A thread verifica o evento entre cada notebook no loop de validação
        e entre etapas do pipeline principal. Não mata a thread imediatamente.
        """
        event = self._cancel_events.get(migration_id)
        if event:
            event.set()
            logger.info("MigrationWorker: sinal de cancelamento enviado para %s", migration_id)
        else:
            logger.warning("MigrationWorker: sem thread ativa para cancelar %s", migration_id)

    def _is_cancelled(self, migration_id: str) -> bool:
        """Verifica se o cancelamento foi solicitado para esta migração."""
        event = self._cancel_events.get(migration_id)
        return event is not None and event.is_set()

    def _register_cancel(self, migration_id: str) -> threading.Event:
        """Cria e registra um Event de cancelamento para a migração."""
        event = threading.Event()
        self._cancel_events[migration_id] = event
        return event

    def _unregister_cancel(self, migration_id: str) -> None:
        """Remove o Event de cancelamento após término da thread."""
        self._cancel_events.pop(migration_id, None)

    def start_validation(
        self,
        migration_id: str,
        config,
        tables: list[str] | None = None,
        deploy_only: bool = False,
        collect_only: bool = False,
        force_deploy: bool = False,
    ) -> None:
        """Dispara o pipeline de validação Databricks em background thread.

        Args:
            migration_id: UUID da migração a validar.
            config: DatabricksConfig com credenciais e parâmetros.
            tables: Tabelas a coletar após execução.
            deploy_only: Se True, não executa o workflow.
            collect_only: Se True, não faz deploy — apenas coleta tabelas.
            force_deploy: Se True, ignora fontes ausentes e faz deploy com placeholders.
        """
        thread = threading.Thread(
            target=self._run_validation,
            args=(migration_id, config, tables or [], deploy_only, collect_only, force_deploy),
            daemon=True,
            name=f"validate-{migration_id[:8]}",
        )
        thread.start()
        logger.info("MigrationWorker: validação iniciada para %s (force=%s)", migration_id, force_deploy)

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
                kb=self._healing_kb,
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

            # Sprint 10 — Self-Evolution: se healing falhou, aciona EvolutionEngine
            if not report.healed and self._llm_config.api_key:
                self._try_evolve(
                    migration_id=migration_id,
                    healing_id=healing_id,
                    notebook_path=notebook_path,
                    healing_report=report,
                    notebook_name=body.notebook_name,
                    healing_data=healing_data,
                    save_fn=_save_healing,
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
        force_deploy: bool = False,
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
        notebooks = sorted(
            nb for nb in output_dir.glob("*.py")
            if not nb.stem.endswith("_reconciliation")
            and not nb.stem.lower().startswith("autoexec")
        )

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
                # Lê execution_order persistido durante a transpilação para classificação correta
                meta = self._storage.get_meta(migration_id)
                execution_order = meta.get("execution_order") or []
                preflight_report = preflight.check(output_dir, execution_order=execution_order or None)
                logger.info(
                    "MigrationWorker[validate %s]: preflight — %s",
                    migration_id, preflight_report.summary(),
                )
                # Persiste resultado e alimenta catálogo global
                meta = self._storage.get_meta(migration_id)
                meta.setdefault("validation", {})["preflight"] = {
                    "summary": preflight_report.summary(),
                    "ghost_sources": [
                        {
                            "table": g.table_name,
                            "notebooks": g.notebooks,
                            "suggestion": g.suggestion,
                            "category": g.category.value,
                        }
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

                # Auto-force: se todos os MISSING_SOURCE pertencem a schemas configurados
                # em auto_force_schemas, promove force_deploy automaticamente sem bloquear.
                auto_force_schemas: list[str] = getattr(config, "auto_force_schemas", [])
                if preflight_report.missing_source and not force_deploy and auto_force_schemas:
                    auto_forced = all(
                        any(
                            g.table_name.startswith(schema + ".")
                            for schema in auto_force_schemas
                        )
                        for g in preflight_report.missing_source
                    )
                    if auto_forced:
                        logger.info(
                            "MigrationWorker[validate %s]: auto_force_deploy — "
                            "%d tabela(s) pertencem a schemas configurados (%s), "
                            "procedendo com bootstrap automático",
                            migration_id,
                            len(preflight_report.missing_source),
                            ", ".join(auto_force_schemas),
                        )
                        force_deploy = True

                # MISSING_SOURCE → bloquear (requer dados reais) — a não ser que force_deploy
                if preflight_report.missing_source and not force_deploy:
                    logger.warning(
                        "MigrationWorker[validate %s]: %d tabela(s) de origem ausente(s) — "
                        "bloqueando deploy até ingestão manual",
                        migration_id, len(preflight_report.missing_source),
                    )
                    meta = self._storage.get_meta(migration_id)
                    meta["validation"] = {
                        "status": "awaiting_tables",
                        "preflight": meta.get("validation", {}).get("preflight", {}),
                        "missing_source": [
                            {
                                "table": g.table_name,
                                "notebooks": g.notebooks,
                                "suggestion": g.suggestion,
                            }
                            for g in preflight_report.missing_source
                        ],
                    }
                    self._storage.save_meta(migration_id, meta)
                    return  # Interrompe — aguarda criação das tabelas de origem

                # Bootstrap: MISSING_UPSTREAM sempre; MISSING_SOURCE apenas em force_deploy
                ghosts_to_bootstrap = list(preflight_report.missing_upstream)
                if force_deploy and preflight_report.missing_source:
                    logger.warning(
                        "MigrationWorker[validate %s]: force_deploy=True — "
                        "injetando placeholders para %d tabela(s) de origem ausente(s)",
                        migration_id, len(preflight_report.missing_source),
                    )
                    ghosts_to_bootstrap.extend(preflight_report.missing_source)

                if ghosts_to_bootstrap:
                    total_bootstrapped = 0
                    for nb in notebooks:
                        relevant = [
                            g for g in ghosts_to_bootstrap
                            if nb.stem in g.notebooks
                        ]
                        if relevant:
                            created = preflight.inject_placeholder_bootstrap(nb, relevant)
                            total_bootstrapped += len(created)
                    if total_bootstrapped:
                        logger.info(
                            "MigrationWorker[validate %s]: preflight-bootstrap — "
                            "%d placeholder(s) injetados (force=%s)",
                            migration_id, total_bootstrapped, force_deploy,
                        )
                        meta = self._storage.get_meta(migration_id)
                        meta.setdefault("validation", {}).setdefault("preflight", {})[
                            "bootstrap_injected"
                        ] = total_bootstrapped
                        self._storage.save_meta(migration_id, meta)

            for idx, nb in enumerate(notebooks):
                # Verifica cancelamento antes de cada notebook (evita deploys desnecessários)
                if self._is_cancelled(migration_id):
                    logger.info(
                        "MigrationWorker[validate %s]: cancelamento detectado antes de [%d/%d] — abortando loop",
                        migration_id, idx + 1, total,
                    )
                    break

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

                # C2: checkpoint do HealthMonitor a cada 50 notebooks para alimentar
                # /evolution/health sem esperar o fim da validação inteira
                if self._evolution_engine is not None and (idx + 1) % 50 == 0:
                    try:
                        qp = self._evolution_engine.quarantine.count_pending()
                        self._evolution_engine.health.checkpoint(
                            jobs_processed=idx + 1,
                            jobs_total=total,
                            quarantine_pending=qp,
                            human_review_pending=qp,
                        )
                    except Exception:  # noqa: BLE001
                        pass

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

            validation_status = "cancelled" if self._is_cancelled(migration_id) else "done"
            meta = self._storage.get_meta(migration_id)
            meta["validation"] = {"status": validation_status, "report": report}
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

    # ------------------------------------------------------------------
    # Evolution Engine
    # ------------------------------------------------------------------

    def _get_evolution_engine(self):
        """Retorna EvolutionEngine singleton (lazy init na primeira chamada).

        C1: Instância única compartilhada — evita reconectar LLMClient e
        re-carregar HealthMonitor do disco a cada _try_evolve().
        """
        if self._evolution_engine is not None:
            return self._evolution_engine

        from sas2dbx.evolve.engine import EvolutionEngine
        from sas2dbx.evolve.health import HealthMonitor
        from sas2dbx.transpile.llm.client import LLMClient
        import dataclasses

        # Evolução precisa de mais tokens e mais tempo — resposta inclui código de teste completo
        # timeout=180s: 8192 tokens de output pode levar 90-120s em Claude Sonnet
        evolution_config = dataclasses.replace(self._llm_config, max_tokens=8192, timeout=180.0)
        llm = LLMClient(evolution_config)
        project_root = Path(__file__).resolve().parents[2]
        health_path = self._storage.work_dir / "catalog" / "health_snapshots.json"
        health = HealthMonitor(health_path)
        self._evolution_engine = EvolutionEngine(
            llm_client=llm,
            project_root=project_root,
            health_monitor=health,
            unresolved_dir=self._storage.work_dir / "catalog" / "unresolved",
            catalog_dir=self._storage.work_dir / "catalog",
            kb=self._healing_kb,
        )
        return self._evolution_engine

    def _try_evolve(
        self,
        migration_id: str,
        healing_id: str,
        notebook_path: Path,
        healing_report,
        notebook_name: str,
        healing_data: dict,
        save_fn,
    ) -> None:
        """Aciona o EvolutionEngine quando healing falha. Não bloqueia — fire-and-forget."""
        from sas2dbx.evolve.unresolved import UnresolvedError

        logger.info(
            "MigrationWorker[heal %s]: healing falhou — acionando EvolutionEngine",
            healing_id,
        )

        try:
            error = UnresolvedError.from_healing_report(
                job_id=notebook_name,
                migration_id=migration_id,
                notebook_path=notebook_path,
                healing_report=healing_report,
                construct_type="UNKNOWN",
            )

            engine = self._get_evolution_engine()
            result = engine.process(error)

            # Atualiza healing_data com resultado da evolução
            healing_data["evolution"] = {
                "proposal_generated": result.proposal_generated,
                "gate_decision": result.gate_decision,
                "applied": result.applied,
                "quarantine_entry_id": result.quarantine_entry_id,
                "fix_type": result.fix_type,
                "risk_level": result.risk_level,
                "description": result.description,
                "elapsed_seconds": round(result.elapsed_seconds, 2),
            }
            save_fn(healing_data)

            logger.info(
                "MigrationWorker[heal %s]: evolution concluído — gate=%s applied=%s",
                healing_id, result.gate_decision, result.applied,
            )

            # C2: checkpoint após cada decisão do EvolutionEngine para que
            # /evolution/health reflita o estado atual (sem checkpoint o arquivo nunca é criado)
            try:
                engine = self._get_evolution_engine()
                quarantine_pending = engine.quarantine.count_pending()
                engine.health.checkpoint(
                    jobs_processed=1,
                    jobs_total=1,
                    quarantine_pending=quarantine_pending,
                    human_review_pending=quarantine_pending,
                )
            except Exception:  # noqa: BLE001
                pass  # checkpoint é melhor esforço — não bloqueia healing

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "MigrationWorker[heal %s]: EvolutionEngine falhou (não crítico): %s",
                healing_id, exc,
            )
            healing_data["evolution"] = {"error": str(exc)}
            save_fn(healing_data)

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    def _run(self, migration_id: str) -> None:
        """Ponto de entrada da thread — captura exceções de infraestrutura."""
        self._register_cancel(migration_id)
        try:
            self._storage.update_status(migration_id, "processing")
            self._execute_pipeline(migration_id)
            if self._is_cancelled(migration_id):
                self._storage.update_status(migration_id, "cancelled")
                logger.info("MigrationWorker: migração %s cancelada", migration_id)
            else:
                self._storage.update_status(migration_id, "done")
                self._catalog.record_migration(migration_id)
                logger.info("MigrationWorker: migração %s concluída com sucesso", migration_id)
        except Exception as exc:  # noqa: BLE001
            if self._is_cancelled(migration_id):
                self._storage.update_status(migration_id, "cancelled")
                logger.info("MigrationWorker: migração %s cancelada (interrompeu exceção)", migration_id)
            else:
                error_msg = f"{type(exc).__name__}: {exc}"
                logger.exception("MigrationWorker: falha na migração %s", migration_id)
                try:
                    meta = self._storage.get_meta(migration_id)
                    for step in meta.get("pipeline_steps", []):
                        if step["status"] == "running":
                            step["status"] = "failed"
                    self._storage.save_meta(migration_id, meta)
                except Exception:  # noqa: BLE001
                    pass
                self._storage.update_status(migration_id, "failed", error=error_msg)
        finally:
            self._unregister_cancel(migration_id)

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
        # Persiste execution_order para uso no preflight durante a validação
        meta = self._storage.get_meta(migration_id)
        meta["execution_order"] = execution_order
        self._storage.save_meta(migration_id, meta)

        # 3b — Source Inventory (ANTES da transpilação)
        # Verifica tabelas SOURCE no Databricks e gera schemas.yaml tipado.
        # schemas.yaml gerado aqui é lido pelo TranspilationEngine na primeira call LLM,
        # eliminando o problema de timing que causava invenção de colunas (job_107).
        inventory_mode = meta.get("config", {}).get("inventory_mode", "STRICT")
        # Tenta obter credenciais do environment (DATABRICKS_HOST + DATABRICKS_TOKEN)
        try:
            from sas2dbx.validate.config import DatabricksConfig as _DBC
            _dbx_config = _DBC.from_env()
        except (ValueError, ImportError):
            _dbx_config = None
        if _dbx_config and _dbx_config.is_complete():
            try:
                from sas2dbx.inventory.extractor import TableExtractor, classify_roles, map_libnames
                from sas2dbx.inventory.checker import DatabricksChecker
                from sas2dbx.inventory.gate import InventoryGate
                from sas2dbx.inventory.reporter import InventoryReporter
                from sas2dbx.transpile.llm.context import load_libnames as _load_libnames

                step("inventory", "running")
                all_entries: list = []
                extractor = TableExtractor()
                for sas_file in sas_files:
                    from sas2dbx.ingest.reader import read_sas_file, split_blocks as _split_blocks
                    try:
                        code, _ = read_sas_file(sas_file.path)
                        blocks = _split_blocks(code, source_file=sas_file.path)
                        all_entries.extend(extractor.extract(blocks, source_file=sas_file.path))
                    except Exception as _e:
                        logger.warning("Inventory: erro ao extrair tabelas de %s — %s", sas_file.path.name, _e)

                # Resolve LIBNAMEs → FQN
                _libnames_path = input_dir / "libnames.yaml"
                _libnames_map = _load_libnames(_libnames_path) if _libnames_path.exists() else {}
                map_libnames(all_entries, _libnames_map)
                classify_roles(all_entries)

                # Verifica no Databricks via UC REST API
                checker = DatabricksChecker(_dbx_config)
                check_result = checker.check_all(all_entries)

                # Gate
                gate_result = InventoryGate().evaluate(check_result, mode=inventory_mode)

                # Reporter
                reporter = InventoryReporter()
                inv_report = reporter.build_report(all_entries, check_result, gate_result)
                reporter.save_report(inv_report, output_dir)
                reporter.print_summary(inv_report)

                # Salva schemas.yaml ANTES da transpilação
                reporter.save_schemas(inv_report, output_dir)

                step("inventory", "done", f"gate={gate_result.decision} sources={check_result.total} missing={gate_result.missing_count}")
                meta = self._storage.get_meta(migration_id)
                meta["inventory"] = {
                    "gate": gate_result.decision,
                    "missing_count": gate_result.missing_count,
                    "schemas_captured": len(inv_report.schemas),
                }
                self._storage.save_meta(migration_id, meta)

                if gate_result.decision == "BLOCK":
                    step("transpile", "skipped", f"Inventory BLOCK: {gate_result.missing_count} tabela(s) SOURCE ausente(s)")
                    logger.error(
                        "MigrationWorker [%s]: inventory BLOCK — transpilação cancelada. %s",
                        migration_id, gate_result.message,
                    )
                    return
            except Exception as _inv_exc:
                logger.warning(
                    "MigrationWorker [%s]: inventory falhou com exceção — prosseguindo sem verificação: %s",
                    migration_id, _inv_exc,
                )
                step("inventory", "skipped", f"erro: {_inv_exc}")
        else:
            step("inventory", "skipped", "sem credenciais Databricks — modo offline")
            logger.info("MigrationWorker [%s]: inventory sem credenciais — schemas.yaml não pré-gerado", migration_id)

        # 4 — Transpilação (com LLM para Tier 2 se configurado)
        step("transpile", "running", f"0 / {len(sas_files)} jobs")
        llm_client = LLMClient(self._llm_config)

        # Auto-resume: se o state file já contém jobs DONE, retoma de onde parou
        # (interrupção por cancel ou crash anterior)
        from sas2dbx.transpile.state import MigrationStateManager
        from sas2dbx.models.migration_result import JobStatus as _JS
        _state_path = output_dir / ".sas2dbx_state.json"
        _auto_resume = False
        if _state_path.exists():
            _sm = MigrationStateManager(output_dir)
            if _sm.load():
                _done = [j for j in execution_order if _sm.get_job_status(j) == _JS.DONE]
                if _done:
                    _auto_resume = True
                    logger.info(
                        "MigrationWorker [%s]: auto-resume — %d job(s) já concluído(s), retomando",
                        migration_id,
                        len(_done),
                    )

        engine = TranspilationEngine(
            output_dir=output_dir,
            resume=_auto_resume,
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
        # PP2-07: usa conteúdo em memória dos MigrationResults para evitar re-leitura de disco
        from sas2dbx.analyze.idempotency import IdempotencyAnalyzer
        _idp_analyzer = IdempotencyAnalyzer()
        _content_map = {r.job_id: r.notebook_content for r in migration_results if r.notebook_content}
        if _content_map:
            idempotency_reports = [
                _idp_analyzer.analyze_content(name, content)
                for name, content in _content_map.items()
            ]
        else:
            idempotency_reports = _idp_analyzer.analyze_directory(output_dir)
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
        # PP2-07: idem — conteúdo em memória quando disponível
        from sas2dbx.validate.semantic_risk import SemanticRiskAnalyzer
        _sr_analyzer = SemanticRiskAnalyzer()
        if _content_map:
            semantic_reports = [
                r for r in (
                    _sr_analyzer.analyze_content(name, content)
                    for name, content in _content_map.items()
                )
                if r.has_risks
            ]
        else:
            semantic_reports = _sr_analyzer.analyze_directory(output_dir)
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

        # PP2-05: Geração de documentação paralela — cada chamada ao LLM é independente
        step("document", "running", f"0/{n_jobs} jobs documentados")

        def _doc_one(sas_file_item):  # noqa: ANN001
            jname = sas_file_item.path.stem
            try:
                logger.info("MigrationWorker [%s]: documentando %s...", migration_id, jname)
                result = doc_engine.generate_doc_sync(
                    job_name=jname,
                    sas_code=jobs_code[jname],
                    block_deps=jobs_block_deps.get(jname, []),
                    classification_results=jobs_classifications.get(jname, []),
                    graph=graph,
                )
                doc_engine.write_doc(result, jobs_dir)
                logger.info(
                    "MigrationWorker [%s]: doc gerada para %s (tokens=%d)",
                    migration_id, jname, result.tokens_used,
                )
                return jname, result.content, None
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MigrationWorker [%s]: falha ao documentar %s: %s",
                    migration_id, jname, exc,
                )
                return jname, f"# {jname}\n\n*Documentação não gerada: {exc}*\n", exc

        completed_count = 0
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_doc_one, sf): sf for sf in sas_files}
            for fut in as_completed(futures):
                jname, content, _err = fut.result()
                job_docs[jname] = content
                completed_count += 1
                step("document", "running", f"{completed_count}/{n_jobs} jobs documentados")

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
