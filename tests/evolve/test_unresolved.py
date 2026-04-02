"""Testes para UnresolvedError — captura e empacotamento de contexto."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sas2dbx.evolve.unresolved import UnresolvedError, HealingAttempt


def _make_error(**kwargs) -> UnresolvedError:
    defaults = dict(
        job_id="job_test_001",
        migration_id="mig-abc123",
        notebook_path="/tmp/test_notebook.py",
        sas_original="PROC SQL; SELECT * FROM lib.tabela; QUIT;",
        pyspark_generated="spark.sql('SELECT * FROM main.raw.tabela')",
        databricks_error="Table or view not found: main.raw.tabela",
        error_category="missing_table",
    )
    defaults.update(kwargs)
    return UnresolvedError(**defaults)


class TestUnresolvedError:
    def test_basic_creation(self):
        err = _make_error()
        assert err.job_id == "job_test_001"
        assert err.error_category == "missing_table"
        assert err.construct_type == "UNKNOWN"
        assert err.timestamp  # preenchido automaticamente

    def test_to_json_roundtrip(self):
        err = _make_error(
            healing_attempts=[
                HealingAttempt(
                    iteration=1,
                    strategy="deterministic",
                    fix_applied="create_placeholder_table",
                    retest_status="FAILED",
                    error_after="UNRESOLVED_COLUMN",
                )
            ]
        )
        raw = err.to_json()
        data = json.loads(raw)
        assert data["job_id"] == "job_test_001"
        assert len(data["healing_attempts"]) == 1
        assert data["healing_attempts"][0]["strategy"] == "deterministic"

    def test_save_and_load(self, tmp_path):
        err = _make_error()
        saved = err.save(tmp_path)
        assert saved.exists()
        loaded = json.loads(saved.read_text())
        assert loaded["migration_id"] == "mig-abc123"

    def test_from_healing_report_minimal(self):
        """from_healing_report com mock mínimo de HealingReport."""

        class MockDiagnostic:
            category = "missing_table"
            entities = {"table_name": "main.raw.tabela"}

        class MockSuggestion:
            strategy = "deterministic"
            description = "create_placeholder_table"
            diagnostic = MockDiagnostic()
            retest_result = None

        class MockReport:
            original_result = type("R", (), {"error": "Table not found", "status": "FAILED"})()
            suggestion = MockSuggestion()
            iterations = 2
            healed = False

        err = UnresolvedError.from_healing_report(
            job_id="test_job",
            migration_id="mig-001",
            notebook_path=Path("/nonexistent/notebook.py"),
            healing_report=MockReport(),
            sas_original="PROC SQL; QUIT;",
            construct_type="PROC_SQL",
        )
        assert err.job_id == "test_job"
        assert err.error_category == "missing_table"
        assert err.construct_type == "PROC_SQL"
        assert len(err.healing_attempts) == 2

    def test_similar_jobs_default_empty(self):
        err = _make_error()
        assert err.similar_jobs_affected == []

    def test_knowledge_context_default_empty(self):
        err = _make_error()
        assert err.knowledge_context == {}
