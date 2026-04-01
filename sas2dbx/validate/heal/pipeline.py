"""SelfHealingPipeline — orquestrador de alto nível do ciclo de self-healing."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.executor import ExecutionResult
from sas2dbx.validate.heal.advisor import FixSuggestion, HealingAdvisor
from sas2dbx.validate.heal.diagnostics import ErrorDiagnostic

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
    """

    original_result: ExecutionResult
    diagnostic: ErrorDiagnostic | None
    suggestion: FixSuggestion
    healed: bool
    iterations: int


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
    ) -> None:
        self._config = config
        self._llm_config = llm_config
        self._max_iterations = max_iterations
        self._on_progress = on_progress

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

        llm_client = self._build_llm_client()
        advisor = HealingAdvisor(
            config=self._config,
            llm_client=llm_client,
            on_progress=self._on_progress,
        )

        current_result = execution_result
        last_suggestion: FixSuggestion | None = None
        iterations = 0

        for _i in range(self._max_iterations):
            iterations += 1
            logger.info(
                "SelfHealingPipeline: iteração %d/%d para %s",
                iterations,
                self._max_iterations,
                notebook_path.stem,
            )

            suggestion = advisor.suggest_fix_sync(notebook_path, current_result)
            last_suggestion = suggestion

            # Retest foi bem-sucedido?
            if suggestion.retest_result is not None and suggestion.retest_result.improved:
                logger.info("SelfHealingPipeline: notebook curado em %d iteração(ões)", iterations)
                return HealingReport(
                    original_result=execution_result,
                    diagnostic=suggestion.diagnostic,
                    suggestion=suggestion,
                    healed=True,
                    iterations=iterations,
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

        # Esgotou iterações sem cura
        return HealingReport(
            original_result=execution_result,
            diagnostic=last_suggestion.diagnostic if last_suggestion else None,
            suggestion=last_suggestion or FixSuggestion(
                strategy="none",
                description="Nenhuma correção automática disponível.",
            ),
            healed=False,
            iterations=iterations,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

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
