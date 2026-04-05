"""Testes para inventory/reporter.py — InventoryReporter."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sas2dbx.inventory import GateResult, InventoryEntry, InventoryReport
from sas2dbx.inventory.checker import CheckResult
from sas2dbx.inventory.reporter import InventoryReporter


def _entry(short: str, role: str, fqn: str | None = None, exists: bool | None = None) -> InventoryEntry:
    e = InventoryEntry(
        table_short=short,
        libname="TELCO",
        role=role,
        sas_context="FROM",
        table_fqn=fqn,
        exists_in_databricks=exists,
    )
    e.referenced_in = ["job_test"]
    return e


def _report(entries: list[InventoryEntry], schemas: dict, decision: str = "PASS") -> InventoryReport:
    return InventoryReport(
        gate=GateResult(decision=decision, message="test", missing_count=0),
        entries=entries,
        schemas=schemas,
    )


class TestSaveReport:
    def test_creates_json_file(self, tmp_path: Path) -> None:
        report = _report([], {})
        reporter = InventoryReporter()
        path = reporter.save_report(report, tmp_path)
        assert path.exists()
        assert path.suffix == ".json"

    def test_json_content_valid(self, tmp_path: Path) -> None:
        entries = [
            _entry("vendas_raw", "SOURCE", "telcostar.operacional.vendas_raw", exists=True),
            _entry("clientes_raw", "SOURCE", "telcostar.operacional.clientes_raw", exists=False),
        ]
        report = _report(entries, {}, decision="BLOCK")
        reporter = InventoryReporter()
        path = reporter.save_report(report, tmp_path)
        data = json.loads(path.read_text())
        assert data["gate_result"] == "BLOCK"
        assert data["summary"]["source_tables"] == 2
        assert data["summary"]["source_missing"] == 1
        assert len(data["missing_tables"]) == 1

    def test_missing_tables_have_action(self, tmp_path: Path) -> None:
        entries = [_entry("missing", "SOURCE", "telcostar.op.missing", exists=False)]
        report = _report(entries, {}, decision="BLOCK")
        reporter = InventoryReporter()
        path = reporter.save_report(report, tmp_path)
        data = json.loads(path.read_text())
        assert "action_required" in data["missing_tables"][0]


class TestSaveSchemas:
    def test_saves_yaml_with_schemas(self, tmp_path: Path) -> None:
        schemas = {
            "telcostar.operacional.vendas_raw": [
                {"name": "id_venda", "type": "STRING"},
                {"name": "valor_bruto", "type": "DOUBLE"},
            ]
        }
        report = _report([], schemas, decision="PASS")
        reporter = InventoryReporter()
        path = reporter.save_schemas(report, tmp_path)
        assert path is not None
        assert path.exists()

    def test_yaml_format_is_list_dict(self, tmp_path: Path) -> None:
        """schemas.yaml deve ter formato list[dict] com name e type."""
        import yaml
        schemas = {
            "telcostar.operacional.vendas_raw": [{"name": "id_venda", "type": "STRING"}]
        }
        report = _report([], schemas)
        reporter = InventoryReporter()
        path = reporter.save_schemas(report, tmp_path)
        assert path is not None
        raw = yaml.safe_load(path.read_text())
        cols = raw["telcostar.operacional.vendas_raw"]["columns"]
        assert isinstance(cols[0], dict)
        assert "name" in cols[0]
        assert "type" in cols[0]

    def test_empty_schemas_returns_none(self, tmp_path: Path) -> None:
        report = _report([], {})
        reporter = InventoryReporter()
        path = reporter.save_schemas(report, tmp_path)
        assert path is None

    def test_existing_schemas_merged_incrementally(self, tmp_path: Path) -> None:
        """Tabelas existentes em schemas.yaml são preservadas (curadoria manual tem precedência)."""
        import yaml
        existing = {"telcostar.operacional.old_table": {"columns": [{"name": "col_old", "type": "STRING"}]}}
        (tmp_path / "schemas.yaml").write_text(yaml.dump(existing), encoding="utf-8")

        schemas = {"telcostar.operacional.new_table": [{"name": "col_new", "type": "DOUBLE"}]}
        report = _report([], schemas)
        reporter = InventoryReporter()
        reporter.save_schemas(report, tmp_path)

        raw = yaml.safe_load((tmp_path / "schemas.yaml").read_text())
        assert "telcostar.operacional.old_table" in raw
        assert "telcostar.operacional.new_table" in raw


class TestBuildReport:
    def test_build_report_has_all_fields(self) -> None:
        entries = [_entry("t", "SOURCE", "cat.sc.t", exists=True)]
        schemas = {"cat.sc.t": [{"name": "col", "type": "STRING"}]}
        check = CheckResult(total=1, found=1, schemas=schemas)
        gate = GateResult(decision="PASS", message="ok")
        reporter = InventoryReporter()
        report = reporter.build_report(entries, check, gate)
        assert report.gate.decision == "PASS"
        assert len(report.entries) == 1
        assert "cat.sc.t" in report.schemas


class TestInventoryReportToDict:
    def test_summary_counts(self) -> None:
        entries = [
            _entry("s1", "SOURCE", fqn="c.s.s1", exists=True),
            _entry("s2", "SOURCE", fqn="c.s.s2", exists=False),
            _entry("t1", "TARGET", fqn="c.s.t1"),
            _entry("i1", "INTERMEDIATE", fqn="c.s.i1"),
            _entry("u1", "UNRESOLVED"),
        ]
        report = _report(entries, {}, decision="BLOCK")
        d = report.to_dict()
        s = d["summary"]
        assert s["source_tables"] == 2
        assert s["source_found"] == 1
        assert s["source_missing"] == 1
        assert s["target_tables"] == 1
        assert s["intermediate_tables"] == 1
        assert s["unresolved_tables"] == 1
