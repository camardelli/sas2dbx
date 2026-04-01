"""Testes para validate/collector.py — DatabricksCollector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from sas2dbx.validate.collector import DatabricksCollector, TableValidation
from sas2dbx.validate.config import DatabricksConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg(warehouse_id: str | None = None) -> DatabricksConfig:
    return DatabricksConfig(host="https://dbx.net", token="tok", warehouse_id=warehouse_id)


def _stub_execution_result(rows: list[list], col_names: list[str] | None = None):
    """Cria mock de StatementExecutionResult."""
    result = MagicMock()
    result.result.data_array = rows

    if col_names:
        schema = MagicMock()
        cols = [MagicMock(name=n) for n in col_names]
        # Configura .name em cada coluna mock
        for col_mock, name in zip(cols, col_names):
            col_mock.name = name
        schema.columns = cols
        result.manifest.schema = schema
    else:
        result.manifest = None

    return result


def _make_collector(config: DatabricksConfig = None) -> DatabricksCollector:
    collector = DatabricksCollector.__new__(DatabricksCollector)
    collector._config = config or _cfg()
    collector._client = MagicMock()
    return collector


# ---------------------------------------------------------------------------
# TableValidation dataclass
# ---------------------------------------------------------------------------


class TestTableValidation:
    def test_fields(self) -> None:
        tv = TableValidation(table_name="t", row_count=10, column_count=3)
        assert tv.table_name == "t"
        assert tv.row_count == 10
        assert tv.error is None

    def test_error_field(self) -> None:
        tv = TableValidation(table_name="t", row_count=0, column_count=0, error="not found")
        assert tv.error == "not found"


# ---------------------------------------------------------------------------
# _get_or_create_warehouse — D2 fixes
# ---------------------------------------------------------------------------


class TestGetOrCreateWarehouse:
    def test_uses_config_warehouse_id_if_provided(self) -> None:
        collector = _make_collector(_cfg(warehouse_id="wh-configured"))
        result = collector._get_or_create_warehouse()
        assert result == "wh-configured"
        # Não deve consultar a API
        assert not collector._client.warehouses.list.called

    def test_finds_running_warehouse(self) -> None:
        collector = _make_collector()
        wh = MagicMock()
        wh.state = MagicMock()
        wh.state.value = "RUNNING"
        wh.id = "wh-running"
        collector._client.warehouses.list.return_value = [wh]

        result = collector._get_or_create_warehouse()
        assert result == "wh-running"

    def test_skips_stopped_warehouse(self) -> None:
        collector = _make_collector()
        stopped = MagicMock()
        stopped.state = MagicMock()
        stopped.state.value = "STOPPED"
        stopped.id = "wh-stopped"

        running = MagicMock()
        running.state = MagicMock()
        running.state.value = "RUNNING"
        running.id = "wh-running-2"

        collector._client.warehouses.list.return_value = [stopped, running]
        result = collector._get_or_create_warehouse()
        assert result == "wh-running-2"

    def test_creates_warehouse_if_none_running(self) -> None:
        collector = _make_collector()
        # Nenhum warehouse RUNNING
        stopped = MagicMock()
        stopped.state = MagicMock()
        stopped.state.value = "STOPPED"
        collector._client.warehouses.list.return_value = [stopped]

        create_resp = MagicMock()
        create_resp.id = "wh-new"
        collector._client.warehouses.create.return_value = create_resp

        result = collector._get_or_create_warehouse()
        assert result == "wh-new"
        assert collector._client.warehouses.create.called

    def test_creates_warehouse_if_list_empty(self) -> None:
        collector = _make_collector()
        collector._client.warehouses.list.return_value = []
        create_resp = MagicMock()
        create_resp.id = "wh-fresh"
        collector._client.warehouses.create.return_value = create_resp

        result = collector._get_or_create_warehouse()
        assert result == "wh-fresh"


# ---------------------------------------------------------------------------
# _qualify — table name resolution
# ---------------------------------------------------------------------------


class TestQualify:
    def test_three_part_name_unchanged(self) -> None:
        collector = _make_collector(_cfg())
        assert collector._qualify("cat.sch.tbl") == "cat.sch.tbl"

    def test_two_part_adds_catalog(self) -> None:
        collector = _make_collector(_cfg())
        result = collector._qualify("sch.tbl")
        assert result == "main.sch.tbl"

    def test_one_part_adds_catalog_schema(self) -> None:
        cfg = DatabricksConfig(host="h", token="t", catalog="prod", schema="gold")
        collector = _make_collector(cfg)
        result = collector._qualify("tbl")
        assert result == "prod.gold.tbl"


# ---------------------------------------------------------------------------
# collect — full flow
# ---------------------------------------------------------------------------


class TestCollect:
    def test_collect_returns_list_of_validations(self) -> None:
        collector = _make_collector(_cfg(warehouse_id="wh-123"))

        # Mock _collect_table diretamente
        collector._collect_table = lambda wh, fqn: TableValidation(
            table_name=fqn, row_count=100, column_count=5
        )

        results = collector.collect(["t1", "t2"])
        assert len(results) == 2
        assert all(isinstance(r, TableValidation) for r in results)

    def test_collect_handles_error_per_table(self) -> None:
        collector = _make_collector(_cfg(warehouse_id="wh-123"))

        def mock_collect(wh, fqn):
            if "bad" in fqn:
                return TableValidation(
                    table_name=fqn, row_count=0, column_count=0, error="not found"
                )
            return TableValidation(table_name=fqn, row_count=50, column_count=3)

        collector._collect_table = mock_collect
        results = collector.collect(["good_table", "bad_table"])

        good = next(r for r in results if "good" in r.table_name)
        bad = next(r for r in results if "bad" in r.table_name)

        assert good.error is None
        assert bad.error is not None

    def test_import_error_without_sdk(self) -> None:
        with patch.dict("sys.modules", {"databricks": None, "databricks.sdk": None}):
            with pytest.raises(ImportError, match="databricks-sdk"):
                DatabricksCollector(_cfg())
