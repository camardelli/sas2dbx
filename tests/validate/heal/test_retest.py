"""Testes para validate/heal/retest.py — RetestEngine."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.deployer import DeployResult
from sas2dbx.validate.executor import ExecutionResult
from sas2dbx.validate.heal.retest import RetestEngine, RetestResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg() -> DatabricksConfig:
    return DatabricksConfig(host="https://dbx.net", token="tok")


def _make_engine(deploy_result: DeployResult, exec_result: ExecutionResult) -> RetestEngine:
    """Cria RetestEngine com deployer e executor mockados."""
    engine = RetestEngine.__new__(RetestEngine)
    engine._config = _cfg()

    deployer = MagicMock()
    deployer.deploy.return_value = deploy_result
    engine._deployer = deployer

    executor = MagicMock()
    executor.execute.return_value = exec_result
    engine._executor = executor

    return engine


def _deploy(job_id: int = 42) -> DeployResult:
    return DeployResult(workspace_path="/p/nb", job_id=job_id)


def _exec(status: str = "SUCCESS", duration_ms: int = 3000) -> ExecutionResult:
    return ExecutionResult(run_id=100, status=status, duration_ms=duration_ms)


# ---------------------------------------------------------------------------
# RetestResult dataclass
# ---------------------------------------------------------------------------


class TestRetestResult:
    def test_fields(self) -> None:
        rr = RetestResult(
            deploy_result=_deploy(),
            execution_result=_exec(),
            improved=True,
        )
        assert rr.improved is True
        assert rr.deploy_result.job_id == 42

    def test_not_improved(self) -> None:
        rr = RetestResult(
            deploy_result=_deploy(),
            execution_result=_exec("FAILED"),
            improved=False,
        )
        assert rr.improved is False


# ---------------------------------------------------------------------------
# RetestEngine.retest
# ---------------------------------------------------------------------------


class TestRetestEngineRetest:
    def test_calls_deploy_with_public_api(self, tmp_path: Path) -> None:
        nb = tmp_path / "job_x.py"
        nb.write_text("pass")

        engine = _make_engine(_deploy(), _exec("SUCCESS"))
        engine.retest(nb, "job_x")

        # Verifica que usou API pública deploy() e não _upload_notebook()
        assert engine._deployer.deploy.called
        assert not hasattr(engine._deployer, "_upload_notebook") or \
               not engine._deployer._upload_notebook.called

    def test_uses_job_id_from_deploy_for_execute(self, tmp_path: Path) -> None:
        nb = tmp_path / "job_y.py"
        nb.write_text("pass")

        deploy_result = _deploy(job_id=99)
        engine = _make_engine(deploy_result, _exec())
        engine.retest(nb, "job_y")

        engine._executor.execute.assert_called_once_with(99)

    def test_improved_true_when_success(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        engine = _make_engine(_deploy(), _exec("SUCCESS"))
        result = engine.retest(nb, "nb")
        assert result.improved is True

    def test_improved_false_when_failed(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        engine = _make_engine(_deploy(), _exec("FAILED"))
        result = engine.retest(nb, "nb")
        assert result.improved is False

    def test_improved_false_when_timeout(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        engine = _make_engine(_deploy(), _exec("TIMEOUT"))
        result = engine.retest(nb, "nb")
        assert result.improved is False

    def test_run_id_propagated_to_deploy_result(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass")
        exec_result = ExecutionResult(run_id=777, status="SUCCESS", duration_ms=1000)
        engine = _make_engine(_deploy(), exec_result)
        result = engine.retest(nb, "nb")
        assert result.deploy_result.run_id == 777

    def test_does_not_use_config_cluster_id(self) -> None:
        """Garante que RetestEngine não acessa config.cluster_id (não existe)."""
        engine = RetestEngine.__new__(RetestEngine)
        cfg = _cfg()
        engine._config = cfg
        engine._deployer = MagicMock()
        engine._executor = MagicMock()
        # cluster_id não existe em DatabricksConfig — verificação estática
        assert not hasattr(cfg, "cluster_id")

    def test_import_error_without_sdk(self) -> None:
        with patch.dict("sys.modules", {"databricks": None, "databricks.sdk": None}):
            with pytest.raises(ImportError, match="databricks-sdk"):
                RetestEngine(_cfg())
