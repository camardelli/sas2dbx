"""Testes para transpile/llm/validator.py — PySparkValidator."""

from __future__ import annotations

from sas2dbx.transpile.llm.validator import validate_pyspark

# ---------------------------------------------------------------------------
# Código PySpark válido
# ---------------------------------------------------------------------------

class TestValidCode:
    def test_valid_pyspark_returns_is_valid(self) -> None:
        code = """
import pyspark.sql.functions as F

df = spark.read.table("main.raw.clientes")
result = df.filter(F.col("status") == "A")
result.write.mode("overwrite").saveAsTable("main.migrated.clientes")
"""
        result = validate_pyspark(code)
        assert result.is_valid
        assert result.syntax_ok
        assert result.errors == []

    def test_valid_no_warnings_for_prefixed_table(self) -> None:
        code = """
import pyspark.sql.functions as F
df = spark.read.table("main.raw.t")
df.write.mode("overwrite").saveAsTable("main.migrated.output")
"""
        result = validate_pyspark(code)
        assert not any(w.code == "UC_PREFIX" for w in result.warnings)

    def test_simple_spark_sql_no_errors(self) -> None:
        code = "result = spark.sql('SELECT * FROM main.raw.t')"
        result = validate_pyspark(code)
        assert result.syntax_ok


# ---------------------------------------------------------------------------
# AC-3: sintaxe Python — falha rápida
# ---------------------------------------------------------------------------

class TestSyntaxCheck:
    def test_syntax_error_detected(self) -> None:
        code = "def foo(:\n    pass"
        result = validate_pyspark(code)
        assert not result.syntax_ok
        assert not result.is_valid
        assert any(e.code == "SYNTAX_ERROR" for e in result.errors)

    def test_syntax_error_no_semantic_checks_run(self) -> None:
        """Quando há erro de sintaxe, checagens semânticas não são executadas."""
        code = "def foo(:\n    F.col('x')"
        result = validate_pyspark(code)
        # Deve ter só SYNTAX_ERROR, não MISSING_F_IMPORT
        codes = [e.code for e in result.errors]
        assert "SYNTAX_ERROR" in codes
        assert "MISSING_F_IMPORT" not in codes


# ---------------------------------------------------------------------------
# AC-1: import F ausente
# ---------------------------------------------------------------------------

class TestImportChecks:
    def test_f_usage_without_import_is_error(self) -> None:
        code = "result = df.filter(F.col('status') == 'A')"
        result = validate_pyspark(code)
        assert not result.is_valid
        assert any(e.code == "MISSING_F_IMPORT" for e in result.errors)

    def test_f_import_present_no_error(self) -> None:
        code = "import pyspark.sql.functions as F\nresult = df.filter(F.col('x') == 1)"
        result = validate_pyspark(code)
        assert not any(e.code == "MISSING_F_IMPORT" for e in result.errors)

    def test_f_import_from_style_accepted(self) -> None:
        code = "from pyspark.sql import functions as F\nresult = df.select(F.col('x'))"
        result = validate_pyspark(code)
        assert not any(e.code == "MISSING_F_IMPORT" for e in result.errors)

    def test_window_usage_without_import_is_error(self) -> None:
        # Window sem import correto
        code_no_window = (
            "import pyspark.sql.functions as F\n"
            "w = Window.partitionBy('id')\n"
        )
        result = validate_pyspark(code_no_window)
        assert any(e.code == "MISSING_WINDOW_IMPORT" for e in result.errors)

    def test_window_import_present_no_error(self) -> None:
        code = (
            "import pyspark.sql.functions as F\n"
            "from pyspark.sql.window import Window\n"
            "w = Window.partitionBy('id')\n"
            "result = df.withColumn('rn', F.row_number().over(w))\n"
        )
        result = validate_pyspark(code)
        assert not any(e.code == "MISSING_WINDOW_IMPORT" for e in result.errors)


# ---------------------------------------------------------------------------
# AC-2: Unity Catalog prefix — WARNING não erro
# ---------------------------------------------------------------------------

class TestUnityCatalogWarning:
    def test_save_as_table_without_prefix_is_warning(self) -> None:
        code = (
            "import pyspark.sql.functions as F\n"
            "df.write.mode('overwrite').saveAsTable('clientes')\n"
        )
        result = validate_pyspark(code)
        assert result.is_valid  # warning, não erro
        assert any(w.code == "UC_PREFIX" for w in result.warnings)

    def test_save_as_table_schema_only_is_warning(self) -> None:
        code = (
            "import pyspark.sql.functions as F\n"
            "df.write.saveAsTable('raw.clientes')\n"
        )
        result = validate_pyspark(code)
        assert any(w.code == "UC_PREFIX" for w in result.warnings)

    def test_save_as_table_full_prefix_no_warning(self) -> None:
        code = (
            "import pyspark.sql.functions as F\n"
            "df.write.saveAsTable('main.migrated.clientes')\n"
        )
        result = validate_pyspark(code)
        assert not any(w.code == "UC_PREFIX" for w in result.warnings)


# ---------------------------------------------------------------------------
# Estilo de colunas — WARNING
# ---------------------------------------------------------------------------

class TestColumnStyleWarning:
    def test_mixed_col_style_is_warning(self) -> None:
        code = (
            "import pyspark.sql.functions as F\n"
            "result = df.filter(df['status'] == 'A').select(F.col('id'))\n"
        )
        result = validate_pyspark(code)
        assert any(w.code == "MIXED_COL_STYLE" for w in result.warnings)

    def test_consistent_fcol_no_warning(self) -> None:
        code = (
            "import pyspark.sql.functions as F\n"
            "result = df.filter(F.col('status') == 'A').select(F.col('id'))\n"
        )
        result = validate_pyspark(code)
        assert not any(w.code == "MIXED_COL_STYLE" for w in result.warnings)

    def test_consistent_bracket_no_warning(self) -> None:
        code = "result = df.filter(df['status'] == 'A').select(df['id'])\n"
        result = validate_pyspark(code)
        assert not any(w.code == "MIXED_COL_STYLE" for w in result.warnings)


# ---------------------------------------------------------------------------
# ValidationResult estrutura
# ---------------------------------------------------------------------------

class TestValidationResultStructure:
    def test_warnings_not_in_errors(self) -> None:
        code = (
            "import pyspark.sql.functions as F\n"
            "df.write.saveAsTable('output')\n"
        )
        result = validate_pyspark(code)
        assert result.warnings  # tem warning
        assert result.errors == []  # mas não tem erro
        assert result.is_valid  # portanto é válido

    def test_multiple_errors_accumulated(self) -> None:
        # F sem import + Window sem import
        code = (
            "result = df.filter(F.col('x') == 1)\n"
            "w = Window.partitionBy('id')\n"
        )
        result = validate_pyspark(code)
        codes = [e.code for e in result.errors]
        assert "MISSING_F_IMPORT" in codes
        assert "MISSING_WINDOW_IMPORT" in codes
