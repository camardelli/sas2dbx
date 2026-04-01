"""Testes para validate/config.py — DatabricksConfig."""

from __future__ import annotations

import os

import pytest

from sas2dbx.validate.config import DatabricksConfig


class TestDatabricksConfigDefaults:
    def test_required_fields(self) -> None:
        cfg = DatabricksConfig(host="https://adb-123.net", token="dapi-abc")
        assert cfg.host == "https://adb-123.net"
        assert cfg.token == "dapi-abc"

    def test_default_catalog_schema(self) -> None:
        cfg = DatabricksConfig(host="h", token="t")
        assert cfg.catalog == "main"
        assert cfg.schema == "migrated"

    def test_default_node_type(self) -> None:
        cfg = DatabricksConfig(host="h", token="t")
        assert cfg.node_type_id == "i3.xlarge"

    def test_default_spark_version(self) -> None:
        cfg = DatabricksConfig(host="h", token="t")
        assert cfg.spark_version == "13.3.x-scala2.12"

    def test_default_warehouse_id_none(self) -> None:
        cfg = DatabricksConfig(host="h", token="t")
        assert cfg.warehouse_id is None

    def test_custom_fields(self) -> None:
        cfg = DatabricksConfig(
            host="h",
            token="t",
            catalog="prod",
            schema="gold",
            node_type_id="m5.large",
            spark_version="14.3.x-scala2.12",
            warehouse_id="wh-123",
        )
        assert cfg.catalog == "prod"
        assert cfg.spark_version == "14.3.x-scala2.12"
        assert cfg.warehouse_id == "wh-123"


class TestDatabricksConfigFromEnv:
    def test_reads_host_and_token(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "https://dbx.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok-xyz")
        cfg = DatabricksConfig.from_env()
        assert cfg.host == "https://dbx.net"
        assert cfg.token == "tok-xyz"

    def test_strips_trailing_slash_from_host(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "https://dbx.net/")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        cfg = DatabricksConfig.from_env()
        assert not cfg.host.endswith("/")

    def test_raises_if_host_missing(self, monkeypatch) -> None:
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        with pytest.raises(ValueError, match="DATABRICKS_HOST"):
            DatabricksConfig.from_env()

    def test_raises_if_token_missing(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "https://dbx.net")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        with pytest.raises(ValueError, match="DATABRICKS_TOKEN"):
            DatabricksConfig.from_env()

    def test_optional_catalog_schema(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "https://dbx.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.setenv("DATABRICKS_CATALOG", "staging")
        monkeypatch.setenv("DATABRICKS_SCHEMA", "silver")
        cfg = DatabricksConfig.from_env()
        assert cfg.catalog == "staging"
        assert cfg.schema == "silver"

    def test_optional_node_type(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "https://dbx.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.setenv("DATABRICKS_NODE_TYPE_ID", "c5.xlarge")
        cfg = DatabricksConfig.from_env()
        assert cfg.node_type_id == "c5.xlarge"

    def test_optional_spark_version(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "https://dbx.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.setenv("DATABRICKS_SPARK_VERSION", "14.0.x-scala2.12")
        cfg = DatabricksConfig.from_env()
        assert cfg.spark_version == "14.0.x-scala2.12"

    def test_optional_warehouse_id(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "https://dbx.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh-999")
        cfg = DatabricksConfig.from_env()
        assert cfg.warehouse_id == "wh-999"

    def test_warehouse_id_empty_string_becomes_none(self, monkeypatch) -> None:
        monkeypatch.setenv("DATABRICKS_HOST", "https://dbx.net")
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "")
        cfg = DatabricksConfig.from_env()
        assert cfg.warehouse_id is None


class TestDatabricksConfigIsComplete:
    def test_complete_with_host_and_token(self) -> None:
        assert DatabricksConfig(host="h", token="t").is_complete() is True

    def test_incomplete_without_host(self) -> None:
        assert DatabricksConfig(host="", token="t").is_complete() is False

    def test_incomplete_without_token(self) -> None:
        assert DatabricksConfig(host="h", token="").is_complete() is False


class TestDatabricksConfigToDict:
    def test_does_not_include_token(self) -> None:
        cfg = DatabricksConfig(host="h", token="secret")
        d = cfg.to_dict()
        assert "token" not in d

    def test_includes_host_and_catalog(self) -> None:
        cfg = DatabricksConfig(host="h", token="t", catalog="prod")
        d = cfg.to_dict()
        assert d["host"] == "h"
        assert d["catalog"] == "prod"

    def test_includes_is_complete(self) -> None:
        cfg = DatabricksConfig(host="h", token="t")
        assert cfg.to_dict()["is_complete"] is True
