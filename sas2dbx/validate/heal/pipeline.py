"""SelfHealingPipeline — orquestrador de alto nível do ciclo de self-healing."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.executor import ExecutionResult
from sas2dbx.validate.heal.advisor import FixSuggestion, HealingAdvisor
from sas2dbx.validate.heal.diagnostics import ErrorDiagnostic
from sas2dbx.validate.heal.knowledge_base import HealingKnowledgeBase

logger = logging.getLogger(__name__)


@dataclass
class HealingReport:
    """Relatório consolidado do ciclo de self-healing.

    Attributes:
        original_result: ExecutionResult original com o erro.
        diagnostic: ErrorDiagnostic produzido na primeira tentativa.
        suggestion: FixSuggestion com o resultado final.
        healed: True se o retest final foi SUCCESS.
        iterations: Número de tentativas de correção realizadas.
        upstream_fix_needed: Nome do notebook upstream que deveria ter criado a
            tabela ausente (preenchido quando category='missing_table' e a tabela
            pertence a outro job). None se não aplicável.
        static_fixes_applied: Total de fixes estáticos aplicados nas iterações.
    """

    original_result: ExecutionResult
    diagnostic: ErrorDiagnostic | None
    suggestion: FixSuggestion
    healed: bool
    iterations: int
    upstream_fix_needed: str | None = None
    static_fixes_applied: int = 0


class SelfHealingPipeline:
    """Orquestra o ciclo completo de self-healing.

    Combina DiagnosticsEngine + NotebookFixer + RetestEngine em um pipeline
    com limite configurável de iterações.

    Args:
        config: DatabricksConfig para retest.
        llm_config: Se fornecido, cria LLMClient interno para diagnóstico e sugestão.
        max_iterations: Número máximo de tentativas de correção. Default 2
            (alinhado com coderabbit-integration.md: max_iterations: 2).
        on_progress: Callback repassado para HealingAdvisor.
    """

    def __init__(
        self,
        config: DatabricksConfig,
        llm_config: object | None = None,
        max_iterations: int = 2,
        on_progress: Callable[..., None] | None = None,
        healing_history: list[dict] | None = None,
        kb: HealingKnowledgeBase | None = None,
    ) -> None:
        self._config = config
        self._llm_config = llm_config
        self._max_iterations = max_iterations
        self._on_progress = on_progress
        self._healing_history: list[dict] = healing_history or []
        self._kb = kb

    def heal(
        self,
        notebook_path: Path,
        execution_result: ExecutionResult,
    ) -> HealingReport:
        """Executa o ciclo completo de self-healing.

        Fluxo:
          1. Cria HealingAdvisor com LLMClient (se llm_config disponível)
          2. Loop até max_iterations:
             a. suggestion = advisor.suggest_fix_sync(notebook_path, current_result)
             b. Se retest melhorou: para (healed=True)
             c. Se strategy == "none": para (sem mais opções)
             d. Se retest não melhorou: usa novo execution_result e continua
          3. Retorna HealingReport consolidado

        Args:
            notebook_path: Path local do notebook com erro.
            execution_result: ExecutionResult com status="FAILED".

        Returns:
            HealingReport com diagnóstico, sugestão e flag healed.

        Raises:
            ValueError: Se execution_result.status != "FAILED".
        """
        if execution_result.status != "FAILED":
            raise ValueError(
                f"SelfHealingPipeline.heal() requer status='FAILED', "
                f"recebeu '{execution_result.status}'"
            )

        from sas2dbx.validate.heal.static_validator import StaticNotebookValidator

        llm_client = self._build_llm_client()
        advisor = HealingAdvisor(
            config=self._config,
            llm_client=llm_client,
            on_progress=self._on_progress,
        )

        # Pré-validação estática — passa healing_history para que fixes conhecidos
        # de sessões anteriores sejam aplicados proativamente em cada iteração.
        static_validator = StaticNotebookValidator(
            catalog=self._config.catalog,
            schema=self._config.schema,
            healing_history=self._healing_history,
        )

        current_result = execution_result
        last_suggestion: FixSuggestion | None = None
        iterations = 0
        total_static_fixes = 0
        # Memoiza upstream para evitar dupla leitura de disco (chamado nos dois paths de saída)
        _upstream_cache: dict[str, str | None] = {}

        for _i in range(self._max_iterations):
            iterations += 1
            logger.info(
                "SelfHealingPipeline: iteração %d/%d para %s",
                iterations,
                self._max_iterations,
                notebook_path.stem,
            )

            # GAP-1: re-executa o StaticValidator no início de CADA iteração.
            # Erros residuais de iterações anteriores (ex: novos broken writes gerados
            # pelo LLM ao tentar corrigir o notebook) são eliminados antes do retest.
            static_report = static_validator.validate_notebook(notebook_path)
            if static_report.changed:
                total_static_fixes += len(static_report.fixes)
                logger.info(
                    "SelfHealingPipeline: static pre-fix aplicou %d fix(es) na iteração %d: %s",
                    len(static_report.fixes),
                    iterations,
                    "; ".join(static_report.fixes),
                )

            suggestion = advisor.suggest_fix_sync(notebook_path, current_result)
            last_suggestion = suggestion

            # Registra tentativa no KB
            if self._kb is not None and suggestion.diagnostic is not None:
                _error_key = self._kb.compute_error_key(suggestion.diagnostic)
                # Usa description truncada como identificador do fix (único campo descritivo disponível)
                _fix_name = (suggestion.description or suggestion.strategy or "none")[:80]
                _retest_ok = (
                    suggestion.retest_result is not None
                    and suggestion.retest_result.improved
                )
                self._kb.record_attempt(
                    error_category=suggestion.diagnostic.category,
                    error_key=_error_key,
                    fix_name=_fix_name,
                    job_id=notebook_path.stem,
                    result="success" if _retest_ok else "failed",
                    reason=str(current_result.error or "")[:300],
                )
                if self._kb.is_stuck(suggestion.diagnostic.category, _error_key):
                    logger.warning(
                        "SelfHealingPipeline: KB detectou loop para %s/%s — interrompendo iterações",
                        suggestion.diagnostic.category, _error_key,
                    )
                    # Interrompe o loop: continuar re-aplicando o mesmo fix que falhou
                    # N vezes é desperdício de recursos e não converge.
                    last_suggestion = suggestion
                    break

            # Retest foi bem-sucedido?
            if suggestion.retest_result is not None and suggestion.retest_result.improved:
                logger.info("SelfHealingPipeline: notebook curado em %d iteração(ões)", iterations)
                cache_key = str(notebook_path)
                if cache_key not in _upstream_cache:
                    _upstream_cache[cache_key] = self._detect_upstream_dependency(
                        notebook_path, suggestion
                    )
                return HealingReport(
                    original_result=execution_result,
                    diagnostic=suggestion.diagnostic,
                    suggestion=suggestion,
                    healed=True,
                    iterations=iterations,
                    upstream_fix_needed=_upstream_cache[cache_key],
                    static_fixes_applied=total_static_fixes,
                )

            # Sem mais opções de fix
            if suggestion.strategy == "none":
                logger.info("SelfHealingPipeline: sem mais opções de fix automático")
                break

            # Retest existiu mas não melhorou — tenta próxima iteração
            if suggestion.retest_result is not None:
                current_result = suggestion.retest_result.execution_result
                logger.info(
                    "SelfHealingPipeline: retest falhou (%s) — tentando próxima iteração",
                    current_result.status,
                )

        # Esgotou iterações sem cura — reutiliza cache se já computado no path healed=True
        cache_key = str(notebook_path)
        if cache_key not in _upstream_cache:
            _upstream_cache[cache_key] = self._detect_upstream_dependency(
                notebook_path, last_suggestion
            )
        return HealingReport(
            original_result=execution_result,
            diagnostic=last_suggestion.diagnostic if last_suggestion else None,
            suggestion=last_suggestion or FixSuggestion(
                strategy="none",
                description="Nenhuma correção automática disponível.",
            ),
            healed=False,
            iterations=iterations,
            upstream_fix_needed=_upstream_cache[cache_key],
            static_fixes_applied=total_static_fixes,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _detect_upstream_dependency(
        self,
        notebook_path: Path,
        suggestion: FixSuggestion | None,
    ) -> str | None:
        """GAP-2: Detecta se o erro é causado por tabela que deveria ter sido
        criada por um notebook upstream (cross-notebook dependency).

        Estratégia:
          - Se diagnostic.category == 'missing_table' e a tabela não é placeholder,
            varre os notebooks irmãos em busca de saveAsTable("...table_name...")
          - Se encontrado: retorna o nome do notebook upstream (sem extensão)
          - Se não: retorna None

        Args:
            notebook_path: Notebook que falhou.
            suggestion: FixSuggestion da última iteração (pode ser None).

        Returns:
            Nome do notebook upstream (sem extensão) ou None.
        """
        if suggestion is None or suggestion.diagnostic is None:
            return None
        if suggestion.diagnostic.category != "missing_table":
            return None

        table_name = suggestion.diagnostic.entities.get("table_name", "")
        if not table_name:
            return None

        # Extrai apenas o nome da tabela (última parte do catalog.schema.table)
        table_short = table_name.split(".")[-1]
        if not table_short:
            return None

        # Varre notebooks irmãos à procura de saveAsTable que escreve para essa tabela
        output_dir = notebook_path.parent
        save_pattern = re.compile(
            rf'saveAsTable\(["\'].*{re.escape(table_short)}["\']',
            re.IGNORECASE,
        )

        for sibling in sorted(output_dir.glob("*.py")):
            if sibling == notebook_path:
                continue
            try:
                sibling_content = sibling.read_text(encoding="utf-8")
                if save_pattern.search(sibling_content):
                    logger.info(
                        "SelfHealingPipeline: tabela '%s' deveria ser criada por '%s' (upstream dep)",
                        table_short,
                        sibling.stem,
                    )
                    return sibling.stem
            except OSError:
                continue

        return None

    def _build_llm_client(self) -> object | None:
        """Constrói LLMClient se llm_config foi fornecido."""
        if self._llm_config is None:
            return None
        try:
            from sas2dbx.transpile.llm.client import LLMClient
            return LLMClient(self._llm_config)
        except Exception as exc:  # noqa: BLE001
            logger.warning("SelfHealingPipeline: falha ao criar LLMClient: %s", exc)
            return None
