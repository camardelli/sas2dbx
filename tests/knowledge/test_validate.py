"""Testes para validate_knowledge_store."""

from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.validate import validate_knowledge_store
from sas2dbx.models.sas_ast import ValidationReport

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def empty_knowledge(tmp_path: Path) -> Path:
    """Knowledge store completamente vazio (sem merged/)."""
    return tmp_path


@pytest.fixture
def knowledge_with_mappings(tmp_path: Path) -> Path:
    """Knowledge store com merged/ populado minimamente."""
    merged = tmp_path / "mappings" / "merged"
    merged.mkdir(parents=True)

    (merged / "functions_map.yaml").write_text(
        yaml.dump({
            "INTCK": {"pyspark": "months_between(end, start)", "confidence": 0.85},
            "SUBSTR": {"pyspark": "substring(col, pos, len)", "confidence": 0.95},
        }),
        encoding="utf-8",
    )
    (merged / "proc_map.yaml").write_text(
        yaml.dump({
            "SQL": {"approach": "rule", "confidence": 0.9},
            "SORT": {"approach": "rule", "confidence": 0.95},
        }),
        encoding="utf-8",
    )
    (merged / "sql_dialect_map.yaml").write_text(
        yaml.dump({
            "CALCULATED": {"sas": "CALCULATED x", "spark": "WITH t AS ...", "notes": "usar CTE"},
        }),
        encoding="utf-8",
    )
    (merged / "formats_map.yaml").write_text(yaml.dump({}), encoding="utf-8")
    (merged / "informats_map.yaml").write_text(yaml.dump({}), encoding="utf-8")
    (merged / "options_map.yaml").write_text(yaml.dump({}), encoding="utf-8")

    return tmp_path


# ---------------------------------------------------------------------------
# Store vazio
# ---------------------------------------------------------------------------

class TestEmptyStore:
    def test_empty_store_is_invalid(self, empty_knowledge: Path) -> None:
        report = validate_knowledge_store(base_path=empty_knowledge)
        assert report.is_valid is False

    def test_empty_store_has_warnings(self, empty_knowledge: Path) -> None:
        report = validate_knowledge_store(base_path=empty_knowledge)
        assert len(report.warnings) > 0

    def test_returns_validation_report_instance(self, empty_knowledge: Path) -> None:
        report = validate_knowledge_store(base_path=empty_knowledge)
        assert isinstance(report, ValidationReport)

    def test_empty_store_coverage_is_zero(self, empty_knowledge: Path) -> None:
        report = validate_knowledge_store(base_path=empty_knowledge)
        assert report.coverage == 0.0


# ---------------------------------------------------------------------------
# Store populado
# ---------------------------------------------------------------------------

class TestPopulatedStore:
    def test_valid_store_is_valid(self, knowledge_with_mappings: Path) -> None:
        report = validate_knowledge_store(base_path=knowledge_with_mappings)
        assert report.is_valid is True

    def test_returns_entry_counts(self, knowledge_with_mappings: Path) -> None:
        report = validate_knowledge_store(base_path=knowledge_with_mappings)
        assert report.total_entries["functions_map.yaml"] == 2
        assert report.total_entries["proc_map.yaml"] == 2
        assert report.total_entries["sql_dialect_map.yaml"] == 1

    def test_coverage_above_zero(self, knowledge_with_mappings: Path) -> None:
        report = validate_knowledge_store(base_path=knowledge_with_mappings)
        assert report.coverage > 0.0

    def test_high_confidence_entries_give_high_coverage(
        self, knowledge_with_mappings: Path
    ) -> None:
        """Todas as entradas têm confidence >= 0.7 → coverage = 1.0."""
        report = validate_knowledge_store(base_path=knowledge_with_mappings)
        assert report.coverage == 1.0


# ---------------------------------------------------------------------------
# Missing references
# ---------------------------------------------------------------------------

class TestMissingReferences:
    def test_proc_rule_without_md_is_in_missing_refs(self, knowledge_with_mappings: Path) -> None:
        """PROCs approach=rule sem .md em sas_reference/procs/ aparecem em missing_references."""
        report = validate_knowledge_store(base_path=knowledge_with_mappings)
        # SQL e SORT são rule mas não têm .md → devem aparecer
        assert len(report.missing_references) >= 2
        assert any("proc_sql" in r for r in report.missing_references)

    def test_proc_with_md_not_in_missing_refs(self, knowledge_with_mappings: Path) -> None:
        """PROC com .md correspondente não aparece em missing_references."""
        procs_dir = knowledge_with_mappings / "sas_reference" / "procs"
        procs_dir.mkdir(parents=True)
        (procs_dir / "proc_sql.md").write_text("# PROC SQL\n...", encoding="utf-8")
        (procs_dir / "proc_sort.md").write_text("# PROC SORT\n...", encoding="utf-8")

        report = validate_knowledge_store(base_path=knowledge_with_mappings)
        assert not any("proc_sql" in r for r in report.missing_references)
        assert not any("proc_sort" in r for r in report.missing_references)


# ---------------------------------------------------------------------------
# Validação de campos obrigatórios
# ---------------------------------------------------------------------------

class TestRequiredFields:
    def test_function_without_pyspark_field_generates_warning(self, tmp_path: Path) -> None:
        merged = tmp_path / "mappings" / "merged"
        merged.mkdir(parents=True)
        for f in ["formats_map.yaml", "informats_map.yaml", "options_map.yaml",
                  "proc_map.yaml", "sql_dialect_map.yaml"]:
            (merged / f).write_text(yaml.dump({}), encoding="utf-8")
        (merged / "functions_map.yaml").write_text(
            yaml.dump({"MYFUNCTION": {"notes": "sem pyspark"}}),
            encoding="utf-8",
        )
        report = validate_knowledge_store(base_path=tmp_path)
        assert any("pyspark" in w for w in report.warnings)

    def test_sql_dialect_without_spark_field_generates_warning(self, tmp_path: Path) -> None:
        merged = tmp_path / "mappings" / "merged"
        merged.mkdir(parents=True)
        for f in ["formats_map.yaml", "informats_map.yaml", "options_map.yaml",
                  "functions_map.yaml", "proc_map.yaml"]:
            (merged / f).write_text(yaml.dump({}), encoding="utf-8")
        (merged / "sql_dialect_map.yaml").write_text(
            yaml.dump({"RULE": {"sas": "x", "notes": "y"}}),  # sem 'spark'
            encoding="utf-8",
        )
        report = validate_knowledge_store(base_path=tmp_path)
        assert any("spark" in w for w in report.warnings)
