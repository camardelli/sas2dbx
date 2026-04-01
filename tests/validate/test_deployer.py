"""Testes para validate/deployer.py — DatabricksDeployer."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.deployer import DeployResult, DatabricksDeployer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg() -> DatabricksConfig:
    return DatabricksConfig(host="https://dbx.net", token="tok")


def _stub_client():
    """Cria um mock do WorkspaceClient."""
    client = MagicMock()
    # jobs.list retorna iterável vazio por padrão
    client.jobs.list.return_value = iter([])
    # jobs.create retorna objeto com job_id
    create_resp = MagicMock()
    create_resp.job_id = 42
    client.jobs.create.return_value = create_resp
    return client


# ---------------------------------------------------------------------------
# DeployResult dataclass
# ---------------------------------------------------------------------------


class TestDeployResult:
    def test_fields(self) -> None:
        dr = DeployResult(workspace_path="/path/nb", job_id=10)
        assert dr.workspace_path == "/path/nb"
        assert dr.job_id == 10
        assert dr.run_id is None

    def test_run_id_optional(self) -> None:
        dr = DeployResult(workspace_path="/p", job_id=1, run_id=99)
        assert dr.run_id == 99


# ---------------------------------------------------------------------------
# DatabricksDeployer — upload e criação de job
# ---------------------------------------------------------------------------


class TestDatabricksDeployer:
    def _make_deployer(self, client=None) -> DatabricksDeployer:
        """Cria deployer com client mockado (não chama databricks-sdk real)."""
        deployer = DatabricksDeployer.__new__(DatabricksDeployer)
        deployer._config = _cfg()
        deployer._client = client or _stub_client()
        return deployer

    def test_deploy_uploads_notebook(self, tmp_path: Path) -> None:
        nb = tmp_path / "job_x.py"
        nb.write_text("df = spark.read.table('t')", encoding="utf-8")

        client = _stub_client()
        deployer = self._make_deployer(client)
        result = deployer.deploy(nb, "job_x")

        assert client.workspace.import_.called
        call_kwargs = client.workspace.import_.call_args
        assert call_kwargs.kwargs["path"] == "/sas2dbx_migrations/job_x"
        assert call_kwargs.kwargs["overwrite"] is True

    def test_deploy_creates_job_if_not_exists(self, tmp_path: Path) -> None:
        nb = tmp_path / "job_y.py"
        nb.write_text("pass", encoding="utf-8")

        client = _stub_client()
        client.jobs.list.return_value = iter([])
        deployer = self._make_deployer(client)

        result = deployer.deploy(nb, "job_y")
        assert client.jobs.create.called
        assert result.job_id == 42

    def test_deploy_resets_existing_job(self, tmp_path: Path) -> None:
        nb = tmp_path / "job_z.py"
        nb.write_text("pass", encoding="utf-8")

        existing_job = MagicMock()
        existing_job.job_id = 77
        client = _stub_client()
        client.jobs.list.return_value = iter([existing_job])

        deployer = self._make_deployer(client)
        result = deployer.deploy(nb, "job_z")

        assert client.jobs.reset.called
        assert not client.jobs.create.called
        assert result.job_id == 77

    def test_deploy_returns_workspace_path(self, tmp_path: Path) -> None:
        nb = tmp_path / "nb.py"
        nb.write_text("pass", encoding="utf-8")
        deployer = self._make_deployer()
        result = deployer.deploy(nb, "nb")
        assert result.workspace_path == "/sas2dbx_migrations/nb"

    def test_build_job_settings_uses_config_cluster(self) -> None:
        cfg = DatabricksConfig(
            host="h",
            token="t",
            node_type_id="c5.xlarge",
            spark_version="14.0.x-scala2.12",
        )
        deployer = DatabricksDeployer.__new__(DatabricksDeployer)
        deployer._config = cfg
        deployer._client = MagicMock()

        settings = deployer._build_job_settings("/p/nb", "nb")
        cluster = settings["tasks"][0]["new_cluster"]
        assert cluster["node_type_id"] == "c5.xlarge"
        assert cluster["spark_version"] == "14.0.x-scala2.12"

    def test_import_error_without_sdk(self) -> None:
        """_build_client lança ImportError se databricks-sdk não instalado."""
        with patch.dict("sys.modules", {"databricks": None, "databricks.sdk": None}):
            with pytest.raises(ImportError, match="databricks-sdk"):
                DatabricksDeployer(_cfg())
