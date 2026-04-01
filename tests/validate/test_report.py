"""Testes para validate/report.py — generate_validation_report."""

from __future__ import annotations

import pytest

from sas2dbx.validate.collector import TableValidation
from sas2dbx.validate.deployer import DeployResult
from sas2dbx.validate.executor import ExecutionResult
from sas2dbx.validate.report import generate_validation_report


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _deploy() -> DeployResult:
    return DeployResult(workspace_path="/sas2dbx_migrations/job1", job_id=10, run_id=200)


def _exec_success() -> ExecutionResult:
    return ExecutionResult(run_id=200, status="SUCCESS", duration_ms=5000)


def _exec_failed() -> ExecutionResult:
    return ExecutionResult(run_id=200, status="FAILED", duration_ms=1000, error="OOM")


def _table_ok(name: str = "main.m.t1") -> TableValidation:
    return TableValidation(
        table_name=name,
        row_count=100,
        column_count=5,
        sample_rows=[{"id": 1, "name": "row1"}],
    )


def _table_err(name: str = "main.m.t2") -> TableValidation:
    return TableValidation(table_name=name, row_count=0, column_count=0, error="not found")


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------


class TestReportStructure:
    def test_returns_dict(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [_table_ok()])
        assert isinstance(r, dict)

    def test_has_generated_at(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [])
        assert "generated_at" in r
        assert "T" in r["generated_at"]  # ISO 8601

    def test_has_pipeline_section(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [])
        assert "pipeline" in r
        assert "deploy" in r["pipeline"]
        assert "execution" in r["pipeline"]

    def test_has_summary_section(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [])
        assert "summary" in r

    def test_has_tables_list(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [_table_ok()])
        assert "tables" in r
        assert isinstance(r["tables"], list)


# ---------------------------------------------------------------------------
# Pipeline section
# ---------------------------------------------------------------------------


class TestReportPipeline:
    def test_deploy_fields(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [])
        d = r["pipeline"]["deploy"]
        assert d["workspace_path"] == "/sas2dbx_migrations/job1"
        assert d["job_id"] == 10
        assert d["run_id"] == 200

    def test_execution_fields(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [])
        e = r["pipeline"]["execution"]
        assert e["status"] == "SUCCESS"
        assert e["duration_ms"] == 5000
        assert e["error"] is None

    def test_execution_error_field(self) -> None:
        r = generate_validation_report(_deploy(), _exec_failed(), [])
        assert r["pipeline"]["execution"]["error"] == "OOM"


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


class TestReportSummary:
    def test_total_tables(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [_table_ok(), _table_ok("t2")])
        assert r["summary"]["total_tables"] == 2

    def test_tables_ok_count(self) -> None:
        r = generate_validation_report(
            _deploy(), _exec_success(), [_table_ok(), _table_ok("t2"), _table_err()]
        )
        assert r["summary"]["tables_ok"] == 2
        assert r["summary"]["tables_error"] == 1

    def test_total_rows_only_ok_tables(self) -> None:
        ok = TableValidation(table_name="t1", row_count=50, column_count=3)
        err = TableValidation(table_name="t2", row_count=0, column_count=0, error="fail")
        r = generate_validation_report(_deploy(), _exec_success(), [ok, err])
        assert r["summary"]["total_rows_collected"] == 50

    def test_overall_status_success(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [_table_ok()])
        assert r["summary"]["overall_status"] == "success"

    def test_overall_status_partial(self) -> None:
        r = generate_validation_report(
            _deploy(), _exec_success(), [_table_ok(), _table_err()]
        )
        assert r["summary"]["overall_status"] == "partial"

    def test_overall_status_failed_execution(self) -> None:
        r = generate_validation_report(_deploy(), _exec_failed(), [_table_ok()])
        assert r["summary"]["overall_status"] == "failed"

    def test_overall_status_failed_all_tables_error(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [_table_err()])
        assert r["summary"]["overall_status"] == "failed"

    def test_empty_tables_success(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [])
        assert r["summary"]["overall_status"] == "success"


# ---------------------------------------------------------------------------
# Tables list
# ---------------------------------------------------------------------------


class TestReportTables:
    def test_table_ok_status(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [_table_ok()])
        assert r["tables"][0]["status"] == "ok"
        assert r["tables"][0]["error"] is None

    def test_table_error_status(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [_table_err()])
        assert r["tables"][0]["status"] == "error"
        assert r["tables"][0]["error"] == "not found"

    def test_table_fields(self) -> None:
        r = generate_validation_report(_deploy(), _exec_success(), [_table_ok()])
        t = r["tables"][0]
        assert "table_name" in t
        assert "row_count" in t
        assert "column_count" in t
        assert "sample_rows" in t
