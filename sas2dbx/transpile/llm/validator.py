"""Validador semântico de código PySpark gerado pelo transpiler.

Checagens em cascata (ordem: falha rápida em sintaxe, depois semântica):
  1. Sintaxe Python — ast.parse()
  2. Imports necessários — F, Window, SparkSession
  3. Referência a spark — uso de spark.read/sql sem instanciação (ok em Databricks)
  4. Unity Catalog prefix — saveAsTable sem catalog.schema.table gera WARNING
  5. Estilo de acesso a colunas — mistura df["col"] + F.col() gera WARNING
"""

from __future__ import annotations

import ast
import logging
import re

from sas2dbx.models.migration_result import ValidationError, ValidationResult, ValidationWarning

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Padrões de detecção
# ---------------------------------------------------------------------------

# Uso de pyspark.sql.functions via alias F
_RE_F_USAGE = re.compile(r"\bF\s*\.\s*\w+\s*\(")

# Import correto de F
_RE_F_IMPORT = re.compile(
    r"import\s+pyspark\.sql\.functions\s+as\s+F"
    r"|from\s+pyspark\.sql\s+import\s+functions\s+as\s+F",
    re.MULTILINE,
)

# Uso de Window
_RE_WINDOW_USAGE = re.compile(r"\bWindow\s*\.")
_RE_WINDOW_IMPORT = re.compile(
    r"from\s+pyspark\.sql\.window\s+import\s+Window"
    r"|from\s+pyspark\.sql\s+import\s+Window",
    re.MULTILINE,
)

# Uso de spark (injetado no Databricks — ok sem instanciação)
_RE_SPARK_USAGE = re.compile(r"\bspark\s*\.\s*(read|sql|createDataFrame|table)\b")
_RE_SPARK_INSTANTIATION = re.compile(
    r"SparkSession\s*\.\s*builder|spark\s*=\s*SparkSession"
)

# saveAsTable sem Unity Catalog prefix (catalog.schema.table = 2 pontos)
_RE_SAVE_AS_TABLE = re.compile(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)')

# Mistura de df["col"] e F.col() — estilo inconsistente
_RE_BRACKET_COL = re.compile(r'\bdf\s*\[\s*["\']')
_RE_FCOL_USAGE = re.compile(r"\bF\.col\s*\(")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def validate_pyspark(code: str) -> ValidationResult:
    """Valida código PySpark gerado semanticamente.

    Args:
        code: Código PySpark a validar.

    Returns:
        ValidationResult com is_valid, syntax_ok, warnings e errors.
    """
    errors: list[ValidationError] = []
    warnings: list[ValidationWarning] = []

    # 1 — Sintaxe Python
    syntax_ok = _check_syntax(code, errors)
    if not syntax_ok:
        return ValidationResult(is_valid=False, syntax_ok=False, errors=errors)

    # 2 — Imports necessários
    _check_imports(code, errors)

    # 3 — Unity Catalog prefix em saveAsTable
    _check_unity_catalog(code, warnings)

    # 4 — Estilo inconsistente de acesso a colunas
    _check_column_style(code, warnings)

    is_valid = len(errors) == 0
    return ValidationResult(
        is_valid=is_valid,
        syntax_ok=True,
        warnings=warnings,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# Internal checkers
# ---------------------------------------------------------------------------


def _check_syntax(code: str, errors: list[ValidationError]) -> bool:
    """Retorna True se ast.parse() passa sem erro."""
    try:
        ast.parse(code)
        return True
    except SyntaxError as exc:
        errors.append(ValidationError(
            code="SYNTAX_ERROR",
            message=f"Erro de sintaxe Python: {exc.msg} (linha {exc.lineno})",
            line=exc.lineno,
        ))
        return False


def _check_imports(code: str, errors: list[ValidationError]) -> None:
    """Verifica que imports necessários estão presentes."""
    if _RE_F_USAGE.search(code) and not _RE_F_IMPORT.search(code):
        errors.append(ValidationError(
            code="MISSING_F_IMPORT",
            message=(
                "Código usa F.col() / F.* mas não importa "
                "'import pyspark.sql.functions as F'"
            ),
        ))

    if _RE_WINDOW_USAGE.search(code) and not _RE_WINDOW_IMPORT.search(code):
        errors.append(ValidationError(
            code="MISSING_WINDOW_IMPORT",
            message=(
                "Código usa Window.* mas não importa "
                "'from pyspark.sql.window import Window'"
            ),
        ))

    # spark sem instanciação é ok em Databricks — apenas loga debug
    if _RE_SPARK_USAGE.search(code) and not _RE_SPARK_INSTANTIATION.search(code):
        logger.debug(
            "Validator: spark sem instanciação — ok para Databricks (injetado automaticamente)"
        )


def _check_unity_catalog(code: str, warnings: list[ValidationWarning]) -> None:
    """Avisa sobre saveAsTable sem Unity Catalog prefix (catalog.schema.table)."""
    for m in _RE_SAVE_AS_TABLE.finditer(code):
        table_name = m.group(1)
        if table_name.count(".") < 2:
            warnings.append(ValidationWarning(
                code="UC_PREFIX",
                message=(
                    f"'.saveAsTable(\"{table_name}\")' sem Unity Catalog prefix — "
                    f"use 'catalog.schema.{table_name}' para evitar ambiguidade"
                ),
            ))


def _check_column_style(code: str, warnings: list[ValidationWarning]) -> None:
    """Avisa sobre uso misto de df['col'] e F.col() no mesmo bloco."""
    has_bracket = bool(_RE_BRACKET_COL.search(code))
    has_fcol = bool(_RE_FCOL_USAGE.search(code))
    if has_bracket and has_fcol:
        warnings.append(ValidationWarning(
            code="MIXED_COL_STYLE",
            message=(
                "Mistura de df['col'] e F.col() no mesmo bloco — "
                "prefira F.col() para consistência com o estilo PySpark"
            ),
        ))
