"""HealingAdvisor — coordena diagnóstico, fix determinístico e sugestão LLM."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.executor import ExecutionResult
from sas2dbx.validate.heal.diagnostics import DiagnosticsEngine, ErrorDiagnostic
from sas2dbx.validate.heal.fixer import NotebookFixer
from sas2dbx.validate.heal.retest import RetestEngine, RetestResult

logger = logging.getLogger(__name__)


@dataclass
class FixSuggestion:
    """Sugestão de correção gerada pelo advisor.

    Attributes:
        strategy: "deterministic" | "llm" | "none"
        description: Descrição humana da sugestão.
        patch_applied: True se o patch foi aplicado no notebook.
        retest_result: RetestResult se retest foi executado (None se não).
        llm_suggestion: Texto com sugestão LLM (None se strategy != "llm").
        diagnostic: ErrorDiagnostic que gerou a sugestão.
    """

    strategy: str
    description: str
    patch_applied: bool = False
    retest_result: RetestResult | None = None
    llm_suggestion: str | None = None
    diagnostic: ErrorDiagnostic | None = None


class HealingAdvisor:
    """Coordena o ciclo de diagnóstico → fix → retest.

    Estratégia:
      1. Diagnóstico determinístico via ErrorDiagnostic (catálogo de patterns)
      2. Fix determinístico via NotebookFixer (se disponível)
      3. Retest via RetestEngine (verifica se o fix resolveu)
      4. Se fix determinístico não disponível: sugestão LLM

    Args:
        config: DatabricksConfig para retest (node_type_id, spark_version — sem cluster_id).
        llm_client: LLMClient opcional para diagnóstico e sugestão LLM.
        on_progress: Callback opcional chamado com (stage: str, detail: str).
    """

    def __init__(
        self,
        config: DatabricksConfig,
        llm_client: object | None = None,
        on_progress: Callable[..., None] | None = None,
    ) -> None:
        self._config = config
        self._llm = llm_client
        self._on_progress = on_progress

    def suggest_fix_sync(
        self,
        notebook_path: Path,
        execution_result: ExecutionResult,
    ) -> FixSuggestion:
        """Versão síncrona de suggest_fix().

        Usa asyncio.run() — mesmo padrão de LLMClient.complete_sync().

        Returns:
            FixSuggestion com o resultado.
        """
        return asyncio.run(self.suggest_fix(notebook_path, execution_result))

    @staticmethod
    def _build_patch_diff(notebook_path: Path) -> str:
        """Gera diff resumido entre .py.bak e o notebook atual.

        Retorna string formatada para injeção no prompt LLM, ou "" se .py.bak
        não existir (primeira iteração sem backup).
        """
        bak_path = notebook_path.with_suffix(".py.bak")
        if not bak_path.exists():
            return ""
        try:
            import difflib
            original = bak_path.read_text(encoding="utf-8").splitlines()
            current = notebook_path.read_text(encoding="utf-8").splitlines()
            diff_lines = list(
                difflib.unified_diff(original, current, lineterm="", n=2)
            )
            if not diff_lines:
                return ""
            # Limita a 60 linhas para não explodir o contexto
            truncated = diff_lines[:60]
            if len(diff_lines) > 60:
                truncated.append(f"... ({len(diff_lines) - 60} linhas omitidas)")
            return "\n".join(truncated)
        except Exception:  # noqa: BLE001
            return ""

    async def suggest_fix(
        self,
        notebook_path: Path,
        execution_result: ExecutionResult,
    ) -> FixSuggestion:
        """Ciclo completo: diagnóstico + fix determinístico ou LLM.

        Fluxo:
          1. Diagnostica o erro via DiagnosticsEngine
          2. Tenta fix determinístico + retest
          3. Se não disponível e LLM presente: sugestão LLM
          4. Se nenhum: retorna strategy="none"

        Args:
            notebook_path: Path local do notebook com erro.
            execution_result: ExecutionResult com status="FAILED".

        Returns:
            FixSuggestion.
        """
        error_raw = execution_result.error or "Unknown error"
        self._notify_progress("diagnosing", "analisando erro...")

        engine = DiagnosticsEngine(llm_client=self._llm)
        diagnostic = engine.diagnose(
            error_raw=error_raw,
            job_name=notebook_path.stem,
        )
        self._notify_progress("diagnosing", diagnostic.category or "unknown")
        logger.info(
            "HealingAdvisor: diagnóstico — categoria=%s, fix=%s",
            diagnostic.category,
            diagnostic.deterministic_fix,
        )

        # Tenta fix determinístico
        det_fix = self._try_deterministic_fix(notebook_path, diagnostic)
        if det_fix is not None:
            det_fix.diagnostic = diagnostic
            return det_fix

        # Fallback: LLM
        if self._llm is not None:
            patch_diff = self._build_patch_diff(notebook_path)
            llm_fix = self._llm_suggest_fix(diagnostic, patch_diff=patch_diff)
            llm_fix.diagnostic = diagnostic
            return llm_fix

        return FixSuggestion(
            strategy="none",
            description="Nenhuma sugestão de correção automática disponível.",
            diagnostic=diagnostic,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _try_deterministic_fix(
        self,
        notebook_path: Path,
        diagnostic: ErrorDiagnostic,
    ) -> FixSuggestion | None:
        """Tenta aplicar patch determinístico e re-executar.

        Args:
            notebook_path: Path do notebook a corrigir.
            diagnostic: ErrorDiagnostic com resultado do diagnóstico.

        Returns:
            FixSuggestion se patch aplicado, None se inaplicável.
        """
        if not diagnostic.deterministic_fix:
            return None

        fixer = NotebookFixer(
            correct_catalog=self._config.catalog,
            correct_schema=self._config.schema,
        )
        patch_result = fixer.apply_fix(notebook_path, diagnostic)

        if not patch_result.patched:
            logger.info(
                "HealingAdvisor: fix determinístico inaplicável — %s",
                patch_result.description,
            )
            return None

        self._notify_progress("retesting", patch_result.description)
        logger.info("HealingAdvisor: patch aplicado, iniciando retest...")

        try:
            retest_engine = RetestEngine(self._config)
            retest_result = retest_engine.retest(notebook_path, notebook_path.stem)
        except ImportError:
            logger.warning("HealingAdvisor: databricks-sdk não instalado — retest ignorado")
            return None

        return FixSuggestion(
            strategy="deterministic",
            description=patch_result.description,
            patch_applied=True,
            retest_result=retest_result,
        )

    def _llm_suggest_fix(self, diagnostic: ErrorDiagnostic, patch_diff: str = "") -> FixSuggestion:
        """Solicita sugestão de correção ao LLM.

        Não propaga LLMProviderError — loga warning e retorna strategy="none".

        Args:
            diagnostic: ErrorDiagnostic preenchido.

        Returns:
            FixSuggestion com strategy="llm" ou "none".
        """
        try:
            diff_section = (
                f"\nPATCH JÁ APLICADO (diff do backup → versão atual):\n```diff\n{patch_diff}\n```\n"
                if patch_diff
                else ""
            )
            prompt = (
                "You are a Databricks/PySpark expert helping fix notebook errors.\n"
                f"Error category: {diagnostic.category or 'unknown'}\n"
                f"Error message: {diagnostic.error_raw[:1000]}\n"
                f"LLM analysis: {diagnostic.llm_analysis or 'N/A'}\n"
                f"Entities: {diagnostic.entities}\n"
                f"{diff_section}\n"
                "Provide a specific, actionable fix as a code snippet or step-by-step "
                "instructions. Keep it under 200 words."
            )
            response = self._llm.complete_sync(prompt)
            logger.info("HealingAdvisor: sugestão LLM obtida (%d chars)", len(response.content))
            return FixSuggestion(
                strategy="llm",
                description="Sugestão gerada pelo LLM",
                llm_suggestion=response.content,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("HealingAdvisor: LLM suggest_fix falhou: %s", exc)
            return FixSuggestion(
                strategy="none",
                description="LLM indisponível — sem sugestão automática.",
            )

    def _notify_progress(self, stage: str, detail: str) -> None:
        """Chama on_progress callback se disponível. Não propaga exceções."""
        if self._on_progress is None:
            return
        try:
            self._on_progress(stage, detail)
        except Exception:  # noqa: BLE001
            pass
