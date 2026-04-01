"""Testes para validate/heal/diagnostics.py — DiagnosticsEngine."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sas2dbx.validate.heal.diagnostics import DiagnosticsEngine, ErrorDiagnostic


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _engine(llm=None) -> DiagnosticsEngine:
    return DiagnosticsEngine(llm_client=llm)


def _stub_llm(content: str = "LLM analysis here"):
    llm = MagicMock()
    resp = MagicMock()
    resp.content = content
    llm.complete_sync.return_value = resp
    return llm


# ---------------------------------------------------------------------------
# ErrorDiagnostic dataclass
# ---------------------------------------------------------------------------


class TestErrorDiagnostic:
    def test_defaults(self) -> None:
        d = ErrorDiagnostic(error_raw="error")
        assert d.category is None
        assert d.entities == {}
        assert d.deterministic_fix is None
        assert d.severity == "UNKNOWN"
        assert d.llm_analysis is None
        assert d.pattern_key is None


# ---------------------------------------------------------------------------
# diagnose — deterministic path
# ---------------------------------------------------------------------------


class TestDiagnoseDetermanistic:
    def test_table_not_found(self) -> None:
        engine = _engine()
        d = engine.diagnose("Table or view not found: main.migrated.dim_clientes")
        assert d.category == "missing_table"
        assert d.pattern_key == "table_not_found"
        assert d.deterministic_fix == "create_placeholder_table"
        assert d.severity == "HIGH"

    def test_column_not_found(self) -> None:
        engine = _engine()
        d = engine.diagnose("cannot resolve `col_xyz` given input columns: [id, name]")
        assert d.category == "missing_column"
        assert d.deterministic_fix is None

    def test_import_error(self) -> None:
        engine = _engine()
        d = engine.diagnose("ModuleNotFoundError: No module named 'great_expectations'")
        assert d.category == "missing_import"
        assert d.deterministic_fix == "add_missing_import"

    def test_permission_denied(self) -> None:
        engine = _engine()
        d = engine.diagnose("AccessDeniedException: User does not have privilege")
        assert d.category == "permissions"
        assert d.deterministic_fix is None

    def test_out_of_memory(self) -> None:
        engine = _engine()
        d = engine.diagnose("java.lang.OutOfMemoryError: GC overhead limit exceeded")
        assert d.deterministic_fix == "increase_cluster_config"

    def test_raw_error_preserved(self) -> None:
        engine = _engine()
        raw = "Table or view not found: main.schema.t"
        d = engine.diagnose(raw)
        assert d.error_raw == raw

    def test_job_name_ignored_in_category(self) -> None:
        engine = _engine()
        d = engine.diagnose("Table or view not found: t", job_name="job_007")
        assert d.category == "missing_table"


# ---------------------------------------------------------------------------
# diagnose — entity extraction
# ---------------------------------------------------------------------------


class TestEntityExtraction:
    def test_extracts_table_name(self) -> None:
        engine = _engine()
        d = engine.diagnose("Table or view not found: main.migrated.dim_clientes")
        assert d.entities.get("table_name") == "main.migrated.dim_clientes"

    def test_extracts_catalog_from_table(self) -> None:
        engine = _engine()
        d = engine.diagnose("Table or view not found: main.migrated.my_table")
        assert d.entities.get("catalog") == "main"

    def test_extracts_column_name(self) -> None:
        engine = _engine()
        d = engine.diagnose("cannot resolve `cod_cliente` given input columns: [id]")
        assert d.entities.get("column_name") == "cod_cliente"

    def test_extracts_module_name(self) -> None:
        engine = _engine()
        d = engine.diagnose("ModuleNotFoundError: No module named 'great_expectations'")
        assert d.entities.get("module_name") == "great_expectations"

    def test_function_name_extraction(self) -> None:
        engine = _engine()
        d = engine.diagnose("Undefined function: INTCK_SAS")
        assert d.entities.get("function_name") == "INTCK_SAS"


# ---------------------------------------------------------------------------
# diagnose — LLM fallback
# ---------------------------------------------------------------------------


class TestDiagnoseLLMFallback:
    def test_unknown_error_calls_llm(self) -> None:
        llm = _stub_llm("The root cause is...")
        engine = _engine(llm=llm)
        d = engine.diagnose("This is a completely unknown error xyz123")
        assert d.category is None
        assert d.llm_analysis == "The root cause is..."
        assert llm.complete_sync.called

    def test_unknown_error_without_llm_returns_none_category(self) -> None:
        engine = _engine()
        d = engine.diagnose("This is a completely unknown error xyz123")
        assert d.category is None
        assert d.llm_analysis is None

    def test_known_error_does_not_call_llm(self) -> None:
        llm = _stub_llm()
        engine = _engine(llm=llm)
        engine.diagnose("Table or view not found: t")
        assert not llm.complete_sync.called

    def test_llm_error_does_not_propagate(self) -> None:
        llm = MagicMock()
        llm.complete_sync.side_effect = RuntimeError("LLM down")
        engine = _engine(llm=llm)
        # Não deve lançar
        d = engine.diagnose("unknown error xyz")
        assert d.llm_analysis is None


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_removes_java_stack_trace_lines(self) -> None:
        engine = _engine()
        raw = "Error message\n\tat com.databricks.foo.bar(foo.java:42)\n\tat xyz\n"
        normalized = engine._normalize(raw)
        assert "\tat" not in normalized
        assert "Error message" in normalized

    def test_removes_timestamps(self) -> None:
        engine = _engine()
        raw = "2024-01-15 12:00:00 ERROR Table or view not found: t"
        normalized = engine._normalize(raw)
        assert "2024-01-15" not in normalized
        assert "Table or view not found" in normalized

    def test_normalizes_whitespace(self) -> None:
        engine = _engine()
        raw = "Table   or   view   not   found"
        normalized = engine._normalize(raw)
        assert "  " not in normalized
