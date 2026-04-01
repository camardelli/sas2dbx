"""Testes para validate/heal/fixer.py — NotebookFixer."""

from __future__ import annotations

from pathlib import Path

import pytest

from sas2dbx.validate.heal.diagnostics import ErrorDiagnostic
from sas2dbx.validate.heal.fixer import NotebookFixer, PatchResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _notebook(tmp_path: Path, content: str, name: str = "job_test.py") -> Path:
    nb = tmp_path / name
    nb.write_text(content, encoding="utf-8")
    return nb


def _diag(deterministic_fix: str | None, entities: dict | None = None) -> ErrorDiagnostic:
    return ErrorDiagnostic(
        error_raw="error",
        deterministic_fix=deterministic_fix,
        entities=entities or {},
    )


# ---------------------------------------------------------------------------
# PatchResult dataclass
# ---------------------------------------------------------------------------


class TestPatchResult:
    def test_fields(self) -> None:
        pr = PatchResult(patched=True, description="done")
        assert pr.patched is True
        assert pr.backup_path is None
        assert pr.error is None


# ---------------------------------------------------------------------------
# apply_fix — no fix available
# ---------------------------------------------------------------------------


class TestApplyFixNoHandler:
    def test_returns_patched_false_when_no_fix_key(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "pass")
        fixer = NotebookFixer()
        result = fixer.apply_fix(nb, _diag(None))
        assert result.patched is False

    def test_returns_patched_false_when_unknown_fix_key(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "pass")
        fixer = NotebookFixer()
        result = fixer.apply_fix(nb, _diag("unknown_strategy"))
        assert result.patched is False

    def test_notebook_not_exists_returns_error(self, tmp_path: Path) -> None:
        nb = tmp_path / "nonexistent.py"
        fixer = NotebookFixer()
        result = fixer.apply_fix(nb, _diag("add_missing_import"))
        assert result.patched is False
        assert result.error is not None


# ---------------------------------------------------------------------------
# apply_fix — create_placeholder_table
# ---------------------------------------------------------------------------


class TestFixCreatePlaceholderTable:
    def test_creates_backup(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "df = spark.read.table('t')\n")
        fixer = NotebookFixer()
        fixer.apply_fix(nb, _diag("create_placeholder_table", {"table_name": "main.raw.t"}))
        assert (tmp_path / "job_test.py.bak").exists()

    def test_inserts_create_statement(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "df = spark.read.table('t')\n")
        fixer = NotebookFixer()
        result = fixer.apply_fix(
            nb, _diag("create_placeholder_table", {"table_name": "main.raw.my_table"})
        )
        assert result.patched is True
        content = nb.read_text(encoding="utf-8")
        assert "CREATE TABLE IF NOT EXISTS main.raw.my_table" in content

    def test_placeholder_inserted_before_spark_code(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "df = spark.read.table('t')\ndf.show()\n")
        fixer = NotebookFixer()
        fixer.apply_fix(nb, _diag("create_placeholder_table", {"table_name": "t"}))
        content = nb.read_text(encoding="utf-8")
        create_pos = content.find("CREATE TABLE")
        read_pos = content.find("spark.read")
        assert create_pos < read_pos

    def test_description_contains_table_name(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "df = spark.read.table('t')\n")
        fixer = NotebookFixer()
        result = fixer.apply_fix(
            nb, _diag("create_placeholder_table", {"table_name": "main.gold.clients"})
        )
        assert "main.gold.clients" in result.description

    def test_backup_not_overwritten_on_second_fix(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "df = spark.read.table('t')\n")
        fixer = NotebookFixer()
        fixer.apply_fix(nb, _diag("create_placeholder_table", {"table_name": "t1"}))
        backup1_content = (tmp_path / "job_test.py.bak").read_text()
        fixer.apply_fix(nb, _diag("create_placeholder_table", {"table_name": "t2"}))
        backup2_content = (tmp_path / "job_test.py.bak").read_text()
        # Backup do original preservado (não sobrescrito)
        assert backup1_content == backup2_content


# ---------------------------------------------------------------------------
# apply_fix — add_missing_import
# ---------------------------------------------------------------------------


class TestFixAddMissingImport:
    def test_adds_import_to_notebook(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "import pyspark\ndf = spark.sql('SELECT 1')\n")
        fixer = NotebookFixer()
        result = fixer.apply_fix(
            nb, _diag("add_missing_import", {"module_name": "great_expectations"})
        )
        assert result.patched is True
        assert "import great_expectations" in nb.read_text(encoding="utf-8")

    def test_skips_if_already_imported(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "import great_expectations\ndf = spark.sql('SELECT 1')\n")
        fixer = NotebookFixer()
        result = fixer.apply_fix(
            nb, _diag("add_missing_import", {"module_name": "great_expectations"})
        )
        assert result.patched is True
        assert "already present" in result.description

    def test_handles_no_module_name_in_entities(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "pass\n")
        fixer = NotebookFixer()
        result = fixer.apply_fix(nb, _diag("add_missing_import", {}))
        assert result.patched is True
        assert "No module name" in result.description


# ---------------------------------------------------------------------------
# apply_fix — increase_cluster_config
# ---------------------------------------------------------------------------


class TestFixIncreaseClusterConfig:
    def test_adds_memory_config(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, "df = spark.read.table('t')\n")
        fixer = NotebookFixer()
        result = fixer.apply_fix(nb, _diag("increase_cluster_config"))
        assert result.patched is True
        content = nb.read_text(encoding="utf-8")
        assert "spark.executor.memory" in content

    def test_skips_if_already_configured(self, tmp_path: Path) -> None:
        nb = _notebook(
            tmp_path,
            'spark.conf.set("spark.executor.memory", "2g")\ndf = spark.read.table("t")\n',
        )
        fixer = NotebookFixer()
        result = fixer.apply_fix(nb, _diag("increase_cluster_config"))
        assert result.patched is True
        assert "already present" in result.description
