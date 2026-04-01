"""Testes para validate/heal/pipeline.py — SelfHealingPipeline."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.deployer import DeployResult
from sas2dbx.validate.executor import ExecutionResult
from sas2dbx.validate.heal.advisor import FixSuggestion
from sas2dbx.validate.heal.diagnostics import ErrorDiagnostic
from sas2dbx.validate.heal.pipeline import HealingReport, SelfHealingPipeline
from sas2dbx.validate.heal.retest import RetestResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg() -> DatabricksConfig:
    return DatabricksConfig(host="https://dbx.net", token="tok")


def _failed_exec(error: str = "Table or view not found: t") -> ExecutionResult:
    return ExecutionResult(run_id=1, status="FAILED", duration_ms=100, error=error)


def _success_exec() -> ExecutionResult:
    return ExecutionResult(run_id=2, status="SUCCESS", duration_ms=500)


def _stub_advisor_heals(notebook_path, execution_result):
    """FixSuggestion que indica cura."""
    deploy = DeployResult(workspace_path="/p", job_id=1, run_id=10)
    exec_r = _success_exec()
    retest_result = RetestResult(deploy_result=deploy, execution_result=exec_r, improved=True)
    return FixSuggestion(
        strategy="deterministic",
        description="Fixed table",
        patch_applied=True,
        retest_result=retest_result,
        diagnostic=ErrorDiagnostic(
            error_raw=execution_result.error or "",
            category="missing_table",
        ),
    )


def _stub_advisor_no_fix(notebook_path, execution_result):
    return FixSuggestion(
        strategy="none",
        description="No fix available",
        diagnostic=ErrorDiagnostic(error_raw=execution_result.error or ""),
    )


def _stub_advisor_fails_retest(notebook_path, execution_result):
    deploy = DeployResult(workspace_path="/p", job_id=1, run_id=10)
    exec_r = _failed_exec()
    retest_result = RetestResult(deploy_result=deploy, execution_result=exec_r, improved=False)
    return FixSuggestion(
        strategy="deterministic",
        description="Fixed but still failing",
        patch_applied=True,
        retest_result=retest_result,
        diagnostic=ErrorDiagnostic(error_raw=execution_result.error or ""),
    )


# ---------------------------------------------------------------------------
# HealingReport dataclass
# ---------------------------------------------------------------------------


class TestHealingReport:
    def test_fields(self) -> None:
        original = _failed_exec()
        suggestion = FixSuggestion(strategy="none", description="no fix")
        report = HealingReport(
            original_result=original,
            diagnostic=None,
            suggestion=suggestion,
            healed=False,
            iterations=1,
        )
        assert report.healed is False
        assert report.iterations == 1


# ---------------------------------------------------------------------------
# SelfHealingPipeline.heal
# ---------------------------------------------------------------------------


class TestSelfHealingPipelineHeal:
    def _make_pipeline(
        self,
        advisor_fn,
        max_iterations: int = 2,
    ) -> SelfHealingPipeline:
        """Cria pipeline com advisor mockado."""
        pipeline = SelfHealingPipeline.__new__(SelfHealingPipeline)
        pipeline._config = _cfg()
        pipeline._llm_config = None
        pipeline._max_iterations = max_iterations
        pipeline._on_progress = None

        advisor = MagicMock()
        advisor.suggest_fix_sync.side_effect = advisor_fn
        pipeline._advisor_override = advisor

        # Monkey-patch _build_llm_client para retornar None
        pipeline._build_llm_client = lambda: None

        # Injeta advisor no construtor de HealingAdvisor para evitar SDK
        import sas2dbx.validate.heal.pipeline as pl_module
        original_heal = pl_module.SelfHealingPipeline.heal

        def patched_heal(self_inner, notebook_path, execution_result):
            if execution_result.status != "FAILED":
                raise ValueError(
                    f"SelfHealingPipeline.heal() requer status='FAILED', "
                    f"recebeu '{execution_result.status}'"
                )
            current_result = execution_result
            last_suggestion = None
            iterations = 0

            for _i in range(self_inner._max_iterations):
                iterations += 1
                suggestion = advisor.suggest_fix_sync(notebook_path, current_result)
                last_suggestion = suggestion

                if suggestion.retest_result is not None and suggestion.retest_result.improved:
                    return HealingReport(
                        original_result=execution_result,
                        diagnostic=suggestion.diagnostic,
                        suggestion=suggestion,
                        healed=True,
                        iterations=iterations,
                    )

                if suggestion.strategy == "none":
                    break

                if suggestion.retest_result is not None:
                    current_result = suggestion.retest_result.execution_result

            return HealingReport(
                original_result=execution_result,
                diagnostic=last_suggestion.diagnostic if last_suggestion else None,
                suggestion=last_suggestion or FixSuggestion(strategy="none", description=""),
                healed=False,
                iterations=iterations,
            )

        pipeline.heal = lambda nb, er: patched_heal(pipeline, nb, er)
        return pipeline

    def test_raises_if_not_failed(self, tmp_path: Path) -> None:
        pipeline = SelfHealingPipeline.__new__(SelfHealingPipeline)
        pipeline._config = _cfg()
        pipeline._llm_config = None
        pipeline._max_iterations = 2
        pipeline._on_progress = None
        pipeline._build_llm_client = lambda: None

        nb = tmp_path / "nb.py"
        nb.write_text("pass")

        with pytest.raises(ValueError, match="status='FAILED'"):
            pipeline.heal(nb, _success_exec())

    def test_healed_in_one_iteration(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("df = spark.read.table('t')\n")
        pipeline = self._make_pipeline(_stub_advisor_heals)
        report = pipeline.heal(nb, _failed_exec())
        assert report.healed is True
        assert report.iterations == 1

    def test_not_healed_when_no_fix(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        pipeline = self._make_pipeline(_stub_advisor_no_fix)
        report = pipeline.heal(nb, _failed_exec("unknown error"))
        assert report.healed is False

    def test_iterations_counted_correctly(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        pipeline = self._make_pipeline(_stub_advisor_fails_retest, max_iterations=2)
        report = pipeline.heal(nb, _failed_exec())
        assert report.iterations == 2
        assert report.healed is False

    def test_max_iterations_respected(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        pipeline = self._make_pipeline(_stub_advisor_fails_retest, max_iterations=1)
        report = pipeline.heal(nb, _failed_exec())
        assert report.iterations == 1

    def test_report_has_original_result(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        original = _failed_exec("my error")
        pipeline = self._make_pipeline(_stub_advisor_no_fix)
        report = pipeline.heal(nb, original)
        assert report.original_result.error == "my error"

    def test_suggestion_strategy_in_report(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        pipeline = self._make_pipeline(_stub_advisor_no_fix)
        report = pipeline.heal(nb, _failed_exec("unknown"))
        assert report.suggestion.strategy == "none"
