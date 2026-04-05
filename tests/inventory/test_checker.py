"""Testes para DatabricksChecker — mock de WorkspaceClient via sys.modules."""

from __future__ import annotations

import sys
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from sas2dbx.inventory import InventoryEntry
from sas2dbx.inventory.checker import CheckResult, DatabricksChecker, check_offline
from sas2dbx.validate.config import DatabricksConfig


def _config() -> DatabricksConfig:
    return DatabricksConfig(host="https://test.azuredatabricks.net", token="token123")


def _entry(table_short: str, role: str = "SOURCE", fqn: str | None = None) -> InventoryEntry:
    return InventoryEntry(
        table_short=table_short,
        libname="TELCO",
        role=role,
        sas_context="FROM",
        referenced_in=["job_test"],
        table_fqn=fqn or f"main.raw.{table_short}",
    )


def _col(name: str, type_text: str = "STRING") -> MagicMock:
    col = MagicMock()
    col.name = name
    col.type_text = type_text
    return col


def _make_sdk_mock(workspace_client_cls: MagicMock) -> dict:
    """Cria fake databricks.sdk no sys.modules para evitar ImportError."""
    sdk_mod = ModuleType("databricks.sdk")
    sdk_mod.WorkspaceClient = workspace_client_cls  # type: ignore[attr-defined]
    dbx_mod = ModuleType("databricks")
    dbx_mod.sdk = sdk_mod  # type: ignore[attr-defined]
    return {"databricks": dbx_mod, "databricks.sdk": sdk_mod}


class TestCheckTable:
    def test_existing_table_returns_true_and_columns(self) -> None:
        checker = DatabricksChecker(_config())
        table_info = MagicMock()
        table_info.columns = [_col("id_venda", "BIGINT"), _col("valor", "DOUBLE")]
        MockClient = MagicMock()
        MockClient.return_value.tables.get.return_value = table_info

        with patch.dict(sys.modules, _make_sdk_mock(MockClient)):
            exists, columns = checker.check_table("main.raw.vendas")

        assert exists is True
        assert columns == [{"name": "id_venda", "type": "BIGINT"}, {"name": "valor", "type": "DOUBLE"}]

    def test_missing_table_returns_false_none(self) -> None:
        checker = DatabricksChecker(_config())
        MockClient = MagicMock()
        MockClient.return_value.tables.get.side_effect = Exception("table not found")

        with patch.dict(sys.modules, _make_sdk_mock(MockClient)):
            exists, columns = checker.check_table("main.raw.nao_existe")

        assert exists is False
        assert columns is None

    def test_table_with_no_columns_returns_none_columns(self) -> None:
        checker = DatabricksChecker(_config())
        table_info = MagicMock()
        table_info.columns = []
        MockClient = MagicMock()
        MockClient.return_value.tables.get.return_value = table_info

        with patch.dict(sys.modules, _make_sdk_mock(MockClient)):
            exists, columns = checker.check_table("main.raw.vazia")

        assert exists is True
        assert columns is None  # empty columns → None

    def test_col_without_name_skipped(self) -> None:
        checker = DatabricksChecker(_config())
        col_ok = _col("id_cliente")
        col_bad = MagicMock()
        col_bad.name = None
        table_info = MagicMock()
        table_info.columns = [col_ok, col_bad]
        MockClient = MagicMock()
        MockClient.return_value.tables.get.return_value = table_info

        with patch.dict(sys.modules, _make_sdk_mock(MockClient)):
            exists, columns = checker.check_table("main.raw.t")

        assert columns == [{"name": "id_cliente", "type": "STRING"}]


class TestCheckAll:
    def test_check_all_found_initializes_to_zero(self) -> None:
        """Garante que CheckResult.found começa em 0 — bug regression test."""
        checker = DatabricksChecker(_config())
        entries = [_entry("vendas")]

        with patch.object(checker, "check_table", return_value=(True, [{"name": "id", "type": "STRING"}])):
            result = checker.check_all(entries)

        assert result.found == 1
        assert result.total == 1
        assert result.missing == []

    def test_check_all_missing_table(self) -> None:
        checker = DatabricksChecker(_config())
        entry = _entry("ausente")
        with patch.object(checker, "check_table", return_value=(False, None)):
            result = checker.check_all([entry])

        assert result.found == 0
        assert result.total == 1
        assert len(result.missing) == 1
        assert result.missing[0] is entry

    def test_check_all_skips_non_source(self) -> None:
        checker = DatabricksChecker(_config())
        target = _entry("output", role="TARGET")
        with patch.object(checker, "check_table") as mock_check:
            result = checker.check_all([target])

        mock_check.assert_not_called()
        assert result.total == 0

    def test_check_all_skips_unresolved(self) -> None:
        checker = DatabricksChecker(_config())
        unresolved = InventoryEntry(
            table_short="t", libname="X", role="SOURCE", sas_context="SET",
            referenced_in=["j"], table_fqn=None,
        )
        with patch.object(checker, "check_table") as mock_check:
            result = checker.check_all([unresolved])

        mock_check.assert_not_called()
        assert result.total == 0

    def test_check_all_schemas_captured(self) -> None:
        checker = DatabricksChecker(_config())
        entry = _entry("clientes", fqn="main.raw.clientes")
        cols = [{"name": "id", "type": "BIGINT"}]

        with patch.object(checker, "check_table", return_value=(True, cols)):
            result = checker.check_all([entry])

        assert "main.raw.clientes" in result.schemas
        assert result.schemas["main.raw.clientes"] == cols

    def test_check_all_mode_default_online(self) -> None:
        checker = DatabricksChecker(_config())
        with patch.object(checker, "check_table", return_value=(True, None)):
            result = checker.check_all([_entry("t")])
        assert result.mode == "online"


class TestCheckOffline:
    def test_offline_returns_skip_mode(self) -> None:
        result = check_offline()
        assert result.mode == "skip"
        assert result.total == 0
        assert result.found == 0
        assert result.missing == []
