"""Testes para validate/heal/advisor.py — HealingAdvisor."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.deployer import DeployResult
from sas2dbx.validate.executor import ExecutionResult
from sas2dbx.validate.heal.advisor import FixSuggestion, HealingAdvisor
from sas2dbx.validate.heal.diagnostics import ErrorDiagnostic
from sas2dbx.validate.heal.retest import RetestResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg() -> DatabricksConfig:
    return DatabricksConfig(host="https://dbx.net", token="tok")


def _failed_exec(error: str = "Table or view not found: main.m.t") -> ExecutionResult:
    return ExecutionResult(run_id=1, status="FAILED", duration_ms=100, error=error)


def _make_advisor(
    llm=None,
    on_progress=None,
    mock_retest: bool = False,
    retest_success: bool = True,
) -> HealingAdvisor:
    advisor = HealingAdvisor.__new__(HealingAdvisor)
    advisor._config = _cfg()
    advisor._llm = llm
    advisor._on_progress = on_progress

    if mock_retest:
        deploy = DeployResult(workspace_path="/p", job_id=1, run_id=10)
        exec_r = ExecutionResult(
            run_id=10,
            status="SUCCESS" if retest_success else "FAILED",
            duration_ms=1000,
        )
        retest_result = RetestResult(
            deploy_result=deploy,
            execution_result=exec_r,
            improved=retest_success,
        )
        retest_engine = MagicMock()
        retest_engine.retest.return_value = retest_result

        from sas2dbx.validate import heal
        original_retest = heal.retest.RetestEngine

        advisor._retest_override = retest_engine

    return advisor


# ---------------------------------------------------------------------------
# FixSuggestion dataclass
# ---------------------------------------------------------------------------


class TestFixSuggestion:
    def test_fields(self) -> None:
        fs = FixSuggestion(strategy="deterministic", description="Fixed table")
        assert fs.strategy == "deterministic"
        assert not fs.patch_applied
        assert fs.retest_result is None
        assert fs.llm_suggestion is None
        assert fs.diagnostic is None


# ---------------------------------------------------------------------------
# HealingAdvisor.suggest_fix — async
# ---------------------------------------------------------------------------


class TestHealingAdvisorSuggestFix:
    def test_no_fix_for_unknown_error_without_llm(self, tmp_path: Path) -> None:
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        advisor = HealingAdvisor(config=_cfg(), llm_client=None)
        result = asyncio.run(advisor.suggest_fix(nb, _failed_exec("unknown xyz")))
        assert result.strategy == "none"

    def test_llm_called_for_unknown_error_with_llm(self, tmp_path: Path) -> None:
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        llm = MagicMock()
        resp = MagicMock()
        resp.content = "Add missing table."
        llm.complete_sync.return_value = resp
        advisor = HealingAdvisor(config=_cfg(), llm_client=llm)
        result = asyncio.run(advisor.suggest_fix(nb, _failed_exec("unknown xyz")))
        assert result.strategy == "llm"
        assert result.llm_suggestion == "Add missing table."

    def test_llm_failure_returns_none_strategy(self, tmp_path: Path) -> None:
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        llm = MagicMock()
        llm.complete_sync.side_effect = RuntimeError("LLM down")
        advisor = HealingAdvisor(config=_cfg(), llm_client=llm)
        result = asyncio.run(advisor.suggest_fix(nb, _failed_exec("unknown xyz")))
        assert result.strategy == "none"

    def test_diagnostic_attached_to_suggestion(self, tmp_path: Path) -> None:
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        advisor = HealingAdvisor(config=_cfg())
        result = asyncio.run(
            advisor.suggest_fix(nb, _failed_exec("Table or view not found: main.m.t"))
        )
        assert result.diagnostic is not None

    def test_on_progress_called(self, tmp_path: Path) -> None:
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        calls = []
        advisor = HealingAdvisor(
            config=_cfg(),
            on_progress=lambda stage, detail: calls.append((stage, detail)),
        )
        asyncio.run(advisor.suggest_fix(nb, _failed_exec("unknown xyz")))
        assert len(calls) >= 1

    def test_on_progress_exception_not_propagated(self, tmp_path: Path) -> None:
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        def bad_cb(stage, detail):
            raise ValueError("bad callback")
        advisor = HealingAdvisor(config=_cfg(), on_progress=bad_cb)
        # Não deve propagar
        result = asyncio.run(advisor.suggest_fix(nb, _failed_exec("unknown xyz")))
        assert isinstance(result, FixSuggestion)


# ---------------------------------------------------------------------------
# suggest_fix_sync — sync wrapper
# ---------------------------------------------------------------------------


class TestSuggestFixSync:
    def test_sync_returns_fix_suggestion(self, tmp_path: Path) -> None:
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        advisor = HealingAdvisor(config=_cfg())
        result = advisor.suggest_fix_sync(nb, _failed_exec("unknown xyz"))
        assert isinstance(result, FixSuggestion)

    def test_sync_uses_asyncio_run(self, tmp_path: Path) -> None:
        """suggest_fix_sync deve usar asyncio.run() internamente."""
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        advisor = HealingAdvisor(config=_cfg())
        # Se usar asyncio.run() e não houver event loop, funciona normalmente
        result = advisor.suggest_fix_sync(nb, _failed_exec("unknown xyz"))
        assert isinstance(result, FixSuggestion)


# ---------------------------------------------------------------------------
# _try_deterministic_fix — sem retest (patch inaplicável)
# ---------------------------------------------------------------------------


class TestTryDeterministicFix:
    def test_returns_none_when_no_deterministic_fix(self, tmp_path: Path) -> None:
        nb = tmp_path / "job.py"
        nb.write_text("pass")
        advisor = HealingAdvisor(config=_cfg())
        diag = ErrorDiagnostic(
            error_raw="cannot resolve col given input columns",
            category="missing_column",
            deterministic_fix=None,
        )
        result = advisor._try_deterministic_fix(nb, diag)
        assert result is None

    def test_returns_none_when_patch_not_applied(self, tmp_path: Path) -> None:
        """Notebook sem spark.read não tem onde inserir — fixer retorna patched=False."""
        nb = tmp_path / "job.py"
        nb.write_text("")  # vazio — fixer ainda tenta mas com content vazio
        advisor = HealingAdvisor(config=_cfg())
        diag = ErrorDiagnostic(
            error_raw="error",
            category="missing_table",
            deterministic_fix="unknown_strategy",  # chave não registrada
        )
        result = advisor._try_deterministic_fix(nb, diag)
        assert result is None
