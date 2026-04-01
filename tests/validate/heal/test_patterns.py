"""Testes para validate/heal/patterns.py."""

from __future__ import annotations

import pytest

from sas2dbx.validate.heal.patterns import ERROR_PATTERNS, match_pattern


class TestErrorPatternsStructure:
    def test_all_entries_have_required_keys(self) -> None:
        for key, entry in ERROR_PATTERNS.items():
            assert "pattern" in entry, f"{key}: missing 'pattern'"
            assert "category" in entry, f"{key}: missing 'category'"
            assert "severity" in entry, f"{key}: missing 'severity'"

    def test_severity_values_valid(self) -> None:
        valid = {"CRITICAL", "HIGH", "MEDIUM"}
        for key, entry in ERROR_PATTERNS.items():
            assert entry["severity"] in valid, f"{key}: severity '{entry['severity']}' inválido"

    def test_deterministic_fix_is_str_or_none(self) -> None:
        for key, entry in ERROR_PATTERNS.items():
            fix = entry.get("deterministic_fix")
            assert fix is None or isinstance(fix, str), f"{key}: deterministic_fix inválido"

    def test_at_least_10_patterns(self) -> None:
        assert len(ERROR_PATTERNS) >= 10


class TestMatchPattern:
    def test_table_not_found(self) -> None:
        key, entry = match_pattern("Table or view not found: main.schema.my_table")
        assert key == "table_not_found"
        assert entry["category"] == "missing_table"

    def test_table_not_found_variant(self) -> None:
        key, _ = match_pattern("org.apache.spark.sql.AnalysisException: Table or view not found: t")
        assert key == "table_not_found"

    def test_column_not_found(self) -> None:
        key, entry = match_pattern(
            "cannot resolve `col_xyz` given input columns: [id, name]"
        )
        assert key == "column_not_found"
        assert entry["category"] == "missing_column"

    def test_unresolved_column(self) -> None:
        key, _ = match_pattern("UNRESOLVED_COLUMN: col_abc")
        assert key == "column_not_found"

    def test_type_mismatch(self) -> None:
        key, entry = match_pattern("cannot cast 'abc' to INT")
        assert key == "type_mismatch"
        assert entry["category"] == "type_error"

    def test_import_error(self) -> None:
        key, entry = match_pattern("ModuleNotFoundError: No module named 'great_expectations'")
        assert key == "import_error"
        assert entry["category"] == "missing_import"
        assert entry["deterministic_fix"] == "add_missing_import"

    def test_permission_denied(self) -> None:
        key, entry = match_pattern("AccessDeniedException: User does not have SELECT privilege")
        assert key == "permission_denied"
        assert entry["category"] == "permissions"
        assert entry["deterministic_fix"] is None

    def test_out_of_memory(self) -> None:
        key, entry = match_pattern("java.lang.OutOfMemoryError: GC overhead limit exceeded")
        assert key == "out_of_memory"
        assert entry["deterministic_fix"] == "increase_cluster_config"

    def test_cluster_error(self) -> None:
        key, _ = match_pattern("ClusterNotReadyException: Cluster was terminated")
        assert key == "cluster_error"

    def test_syntax_error(self) -> None:
        key, entry = match_pattern("ParseException: mismatched input 'FROM' expecting")
        assert key == "syntax_error"
        assert entry["category"] == "syntax"
        assert entry["severity"] == "CRITICAL"

    def test_null_value_error(self) -> None:
        key, _ = match_pattern("java.lang.NullPointerException")
        assert key == "null_value_error"

    def test_division_by_zero(self) -> None:
        key, _ = match_pattern("ArithmeticException: divide by zero")
        assert key == "division_by_zero"

    def test_function_not_found(self) -> None:
        key, entry = match_pattern("Undefined function: INTCK")
        assert key == "function_not_found"
        assert entry["category"] == "missing_function"

    def test_unknown_returns_none_none(self) -> None:
        key, entry = match_pattern("This is a completely unknown message xyz123")
        assert key is None
        assert entry is None

    def test_case_insensitive(self) -> None:
        key, _ = match_pattern("table or view not found: my_table")
        assert key == "table_not_found"
