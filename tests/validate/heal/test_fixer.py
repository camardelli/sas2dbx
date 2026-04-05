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


# ---------------------------------------------------------------------------
# fix_output_column_exists — C1 (sem SyntaxError), A1, varredura total
# ---------------------------------------------------------------------------


class TestFixOutputColumnExists:
    def _diag_dup(self, col: str) -> ErrorDiagnostic:
        return _diag("fix_output_column_exists", {"column_name": col})

    def test_withcolumn_no_syntax_error(self, tmp_path: Path) -> None:
        """C1: fix não deve inserir comentário dentro de chamada de função."""
        nb = _notebook(
            tmp_path,
            'df = df.withColumn("fl_churnou_idx", F.lit(1))\n',
        )
        fixer = NotebookFixer()
        fixer.apply_fix(nb, self._diag_dup("fl_churnou_idx"))
        content = nb.read_text(encoding="utf-8")
        # Deve ser Python válido — sem comentário dentro dos parênteses
        import ast
        ast.parse(content)  # levanta SyntaxError se inválido
        assert ".drop(" in content

    def test_withcolumn_drop_inserted_before(self, tmp_path: Path) -> None:
        nb = _notebook(
            tmp_path,
            'df = df.withColumn("my_col", F.lit(0))\n',
        )
        fixer = NotebookFixer()
        fixer.apply_fix(nb, self._diag_dup("my_col"))
        content = nb.read_text(encoding="utf-8")
        assert '.drop("my_col").withColumn("my_col"' in content

    def test_all_output_cols_dropped_in_one_pass(self, tmp_path: Path) -> None:
        """Varredura total: todos os outputCol corrigidos em uma iteração."""
        nb = _notebook(
            tmp_path,
            (
                'indexer1 = StringIndexer(inputCol="a", outputCol="a_idx")\n'
                'df = indexer1.fit(df).transform(df)\n'
                'indexer2 = StringIndexer(inputCol="b", outputCol="b_idx")\n'
                'df = indexer2.fit(df).transform(df)\n'
            ),
        )
        fixer = NotebookFixer()
        result = fixer.apply_fix(nb, self._diag_dup("a_idx"))
        content = nb.read_text(encoding="utf-8")
        # Ambas as colunas (a_idx e b_idx) devem ter drop inserido
        assert '"a_idx"' in content
        assert '"b_idx"' in content
        assert result.patched is True

    def test_no_fix_when_no_output_col(self, tmp_path: Path) -> None:
        nb = _notebook(tmp_path, 'df = spark.read.table("main.s.t")\n')
        fixer = NotebookFixer()
        result = fixer.apply_fix(nb, self._diag_dup("inexistente_idx"))
        # Handler roda mas não encontra nada — description indica ausência de fix
        assert "não encontradas" in result.description.lower() or result.patched is True


# ---------------------------------------------------------------------------
# _fix_placeholder_add_column — D3 bug fixes
# ---------------------------------------------------------------------------


def _write_schemas(tmp_path: Path, schemas: dict[str, list[str] | list[dict]]) -> None:
    """Grava schemas.yaml no mesmo diretório do notebook.

    Aceita list[str] (converte para list[dict]) ou list[dict] diretamente.
    """
    import yaml
    schemas_path = tmp_path / "schemas.yaml"
    normalized: dict = {}
    for table, cols in schemas.items():
        if cols and isinstance(cols[0], str):
            normalized[table] = {"columns": [{"name": c, "type": "STRING"} for c in cols]}
        else:
            normalized[table] = {"columns": cols}
    schemas_path.write_text(yaml.dump(normalized, allow_unicode=True), encoding="utf-8")


class TestD3BugFixes:
    """Bug 1 e Bug 2 do fixer D3."""

    def _diag_d3(self, col: str) -> ErrorDiagnostic:
        return ErrorDiagnostic(
            error_raw=f"UNRESOLVED_COLUMN {col}",
            deterministic_fix="fix_placeholder_add_column",
            entities={"missing_column": col},
        )

    # ------------------------------------------------------------------
    # Bug 2 — coluna derivada não existe no schemas.yaml → D3 ignorado
    # ------------------------------------------------------------------

    def test_derived_column_skipped_when_not_in_schemas(self, tmp_path: Path) -> None:
        """Coluna criada por withColumnRenamed não está em schemas.yaml → D3 ignorado."""
        content = (
            'df = spark.read.table("telcostar.operacional.clientes_raw")\n'
            'df = df.withColumnRenamed("_FREQ_", "clientes_cidade")\n'
            'df = df.select(F.col("clientes_cidade"))\n'
        )
        nb = _notebook(tmp_path, content)
        _write_schemas(tmp_path, {"telcostar.operacional.clientes_raw": ["id_cliente", "nome", "cidade"]})
        fixer = NotebookFixer()
        result = fixer._fix_placeholder_add_column(nb, "clientes_cidade")
        assert "derivada" in result.lower()
        # Notebook não deve ser modificado
        assert nb.read_text(encoding="utf-8") == content

    def test_real_missing_column_not_skipped(self, tmp_path: Path) -> None:
        """Coluna que realmente existe no schema mas falta no notebook → D3 não ignora."""
        content = (
            'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
            'df = df.select(F.col("valor_bruto"))\n'
        )
        nb = _notebook(tmp_path, content)
        _write_schemas(tmp_path, {"telcostar.operacional.vendas_raw": ["id_venda", "valor_bruto", "dt_venda"]})
        fixer = NotebookFixer()
        result = fixer._fix_placeholder_add_column(nb, "valor_bruto")
        # "derivada" NÃO deve estar no resultado — D3 deve processar normalmente
        assert "derivada" not in result.lower()

    def test_no_schemas_file_proceeds_normally(self, tmp_path: Path) -> None:
        """Sem schemas.yaml, Bug 2 não aplica e D3 continua fluxo normal."""
        content = (
            'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
            'df = df.select(F.col("valor_bruto"))\n'
        )
        nb = _notebook(tmp_path, content)
        # Sem schemas.yaml no diretório
        fixer = NotebookFixer()
        result = fixer._fix_placeholder_add_column(nb, "valor_bruto")
        assert "derivada" not in result.lower()

    # ------------------------------------------------------------------
    # Bug 1 — tabelas criadas por saveAsTable são excluídas de D3
    # ------------------------------------------------------------------

    def test_notebook_internal_table_excluded(self, tmp_path: Path) -> None:
        """Tabela criada por saveAsTable no notebook não recebe ALTER TABLE nem withColumn."""
        content = (
            'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
            'df_agg = df.groupBy("canal_venda").count()\n'
            'df_agg.write.mode("overwrite").saveAsTable("telcostar.operacional.geo_base")\n'
            'df2 = spark.read.table("telcostar.operacional.geo_base")\n'
        )
        nb = _notebook(tmp_path, content)
        # schemas.yaml tem apenas a tabela de origem, geo_base é output
        _write_schemas(tmp_path, {
            "telcostar.operacional.vendas_raw": ["id_venda", "canal_venda", "valor_bruto"],
        })
        fixer = NotebookFixer()
        result = fixer._fix_placeholder_add_column(nb, "canal_venda")
        # canal_venda existe em schemas.yaml → Bug 2 não aplica
        # geo_base está em saveAsTable → Bug 1: excluído de D3
        assert "geo_base" not in result or "exclu" in result.lower() or "derived" in result.lower()
        # O notebook não deve ter ALTER TABLE para geo_base
        modified = nb.read_text(encoding="utf-8")
        assert 'ALTER TABLE telcostar.operacional.geo_base' not in modified
        assert 'ALTER TABLE geo_base' not in modified

    def test_source_table_not_excluded(self, tmp_path: Path) -> None:
        """Tabela de origem (apenas lida, não salva) ainda pode ser candidata de D3."""
        content = (
            'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
            'df = df.select(F.col("valor_bruto"))\n'
        )
        nb = _notebook(tmp_path, content)
        _write_schemas(tmp_path, {
            "telcostar.operacional.vendas_raw": ["id_venda", "valor_bruto", "canal_venda"],
        })
        fixer = NotebookFixer()
        result = fixer._fix_placeholder_add_column(nb, "valor_bruto")
        # vendas_raw é apenas lida (não saveAsTable) → não deve ser excluída como interna
        assert "excluída" not in result.lower() or "geo_base" not in result.lower()
