"""Padrões de erro conhecidos para diagnóstico determinístico.

Catálogo inline — sem leitura de YAML externo.

Estrutura de cada entrada:
  pattern: regex compilado para matching contra a mensagem de erro
  category: categoria semântica do erro
  deterministic_fix: chave da ação de correção (None = apenas LLM)
  severity: "CRITICAL" | "HIGH" | "MEDIUM"
"""

from __future__ import annotations

import re

# Tipo da entrada do catálogo
ErrorPattern = dict  # keys: pattern, category, deterministic_fix, severity

ERROR_PATTERNS: dict[str, ErrorPattern] = {
    "table_not_found": {
        "pattern": re.compile(
            r"Table or view not found[:\s]+([`'\"]?[\w.]+[`'\"]?)"
            r"|TABLE_OR_VIEW_NOT_FOUND"
            r"|AnalysisException[:\s]+.*not found",
            re.IGNORECASE,
        ),
        "category": "missing_table",
        "deterministic_fix": "create_placeholder_table",
        "severity": "HIGH",
    },
    "column_not_found_with_suggestion": {
        "pattern": re.compile(
            r"UNRESOLVED_COLUMN\.WITH_SUGGESTION"
            r"|(?:column|variable|function parameter) with name\s+`?(?P<table_alias>[\w]+)`?\.`?(?P<bad_column>[\w]+)`?\s+cannot be resolved"
            r"|cannot be resolved\. Did you mean one of the following\?\s*\[(?P<suggestions>[^\]]+)\]",
            re.IGNORECASE,
        ),
        "category": "unresolved_column_suggestion",
        "deterministic_fix": "fix_unresolved_column",
        "severity": "HIGH",
        "entity_extractors": {
            "bad_column": re.compile(
                r"with name\s+`?[\w]*`?\.`?([\w]+)`?\s+cannot be resolved",
                re.IGNORECASE,
            ),
            "table_alias": re.compile(
                r"with name\s+`?([\w]+)`?\.[`\w]+\s+cannot be resolved",
                re.IGNORECASE,
            ),
            "suggestions": re.compile(
                r"Did you mean one of the following\?\s*\[([^\]]+)\]",
                re.IGNORECASE,
            ),
        },
    },
    "column_not_found": {
        "pattern": re.compile(
            r"cannot resolve[`'\s]+([\w.]+)[`'\s]+given input columns"
            r"|UNRESOLVED_COLUMN(?!\.WITH_SUGGESTION)"
            r"|AnalysisException[:\s]+.*cannot resolve",
            re.IGNORECASE,
        ),
        "category": "missing_column",
        "deterministic_fix": None,
        "severity": "HIGH",
    },
    "cast_invalid_input": {
        "pattern": re.compile(
            r"CAST_INVALID_INPUT"
            r"|cannot be cast to .BIGINT. because it is malformed",
            re.IGNORECASE,
        ),
        "category": "cast_type_mismatch",
        "deterministic_fix": "fix_when_otherwise_type",
        "severity": "HIGH",
    },
    "stack_type_mismatch": {
        "pattern": re.compile(
            r"DATATYPE_MISMATCH\.STACK_COLUMN_DIFF_TYPES",
            re.IGNORECASE,
        ),
        "category": "stack_type_mismatch",
        "deterministic_fix": "fix_stack_type_mismatch",
        "severity": "HIGH",
    },
    "type_mismatch": {
        "pattern": re.compile(
            r"cannot cast"
            r"|data type mismatch"
            r"|TYPE_MISMATCH"
            r"|incompatible types",
            re.IGNORECASE,
        ),
        "category": "type_error",
        "deterministic_fix": None,
        "severity": "HIGH",
    },
    "date_format_error": {
        "pattern": re.compile(
            r"DateTimeException"
            r"|java\.time\.format"
            r"|Unparseable date",
            re.IGNORECASE,
        ),
        "category": "date_format",
        "deterministic_fix": None,
        "severity": "HIGH",
    },
    "function_not_found": {
        "pattern": re.compile(
            r"Undefined function[:\s]+([`'\"]?[\w]+[`'\"]?)"
            r"|UNRESOLVED_ROUTINE"
            r"|is not a registered function",
            re.IGNORECASE,
        ),
        "category": "missing_function",
        "deterministic_fix": "fix_function_not_found",
        "severity": "HIGH",
    },
    "parse_syntax_exists": {
        "pattern": re.compile(
            r"PARSE_SYNTAX_ERROR.*EXISTS"
            r"|Syntax error at or near 'EXISTS'",
            re.IGNORECASE,
        ),
        "category": "parse_syntax_if_not_exists",
        "deterministic_fix": "fix_parse_syntax_if_not_exists",
        "severity": "CRITICAL",
    },
    "import_error": {
        "pattern": re.compile(
            r"ModuleNotFoundError: No module named '([\w.]+)'"
            r"|ImportError",
            re.IGNORECASE,
        ),
        "category": "missing_import",
        "deterministic_fix": "add_missing_import",
        "severity": "MEDIUM",
    },
    "permission_denied": {
        "pattern": re.compile(
            r"AccessDeniedException"
            r"|PERMISSION_DENIED"
            r"|User does not have",
            re.IGNORECASE,
        ),
        "category": "permissions",
        "deterministic_fix": None,
        "severity": "CRITICAL",
    },
    "cluster_error": {
        "pattern": re.compile(
            r"ClusterNotReadyException"
            r"|RESOURCE_DOES_NOT_EXIST.*cluster"
            r"|Cluster.*terminated",
            re.IGNORECASE,
        ),
        "category": "cluster",
        "deterministic_fix": None,
        "severity": "CRITICAL",
    },
    "null_value_error": {
        "pattern": re.compile(
            r"NullPointerException"
            r"|can't assign null"
            r"|NULL.*not allowed",
            re.IGNORECASE,
        ),
        "category": "null_value",
        "deterministic_fix": None,
        "severity": "MEDIUM",
    },
    "division_by_zero": {
        "pattern": re.compile(
            r"ArithmeticException.*divide by zero"
            r"|Division by zero",
            re.IGNORECASE,
        ),
        "category": "division_by_zero",
        "deterministic_fix": None,
        "severity": "MEDIUM",
    },
    "out_of_memory": {
        "pattern": re.compile(
            r"OutOfMemoryError"
            r"|SparkOutOfMemory"
            r"|java\.lang\.OutOfMemoryError",
            re.IGNORECASE,
        ),
        "category": "resource",
        "deterministic_fix": "increase_cluster_config",
        "severity": "CRITICAL",
    },
    "syntax_error": {
        "pattern": re.compile(
            r"ParseException"
            r"|SyntaxError"
            r"|mismatched input",
            re.IGNORECASE,
        ),
        "category": "syntax",
        "deterministic_fix": None,
        "severity": "CRITICAL",
    },
    "config_not_available": {
        "pattern": re.compile(
            r"CONFIG_NOT_AVAILABLE"
            r"|Configuration .* is not available"
            r"|spark\.conf\.get.*not found",
            re.IGNORECASE,
        ),
        "category": "spark_conf_missing",
        "deterministic_fix": "fix_spark_conf_get",
        "severity": "HIGH",
    },
    "catalog_not_found": {
        "pattern": re.compile(
            r"NO_SUCH_CATALOG_EXCEPTION"
            r"|Catalog '[\w]+' was not found"
            r"|CATALOG_NOT_FOUND",
            re.IGNORECASE,
        ),
        "category": "wrong_catalog",
        "deterministic_fix": "fix_wrong_catalog",
        "severity": "CRITICAL",
    },
    "schema_mismatch": {
        "pattern": re.compile(
            r"_LEGACY_ERROR_TEMP_DELTA_0007"
            r"|schema mismatch detected when writing"
            r"|A schema mismatch detected",
            re.IGNORECASE,
        ),
        "category": "schema_mismatch",
        "deterministic_fix": "fix_overwrite_schema",
        "severity": "HIGH",
    },
    "output_column_exists": {
        "pattern": re.compile(
            r"IllegalArgumentException.*requirement failed.*Output column\s+([\w]+)\s+already exists"
            r"|Output column\s+([\w]+)\s+already exists",
            re.IGNORECASE,
        ),
        "category": "output_column_exists",
        "deterministic_fix": "fix_output_column_exists",
        "severity": "HIGH",
    },
    "kwarg_as_string": {
        # Macro SAS convertida com argumento keyword embutido como string literal
        # Ex: func("param=18") em vez de func(param=18)
        # Detectado em runtime como: TypeError: bad operand type for unary -: 'str'
        "pattern": re.compile(
            r"TypeError: bad operand type for unary -: 'str'"
            r"|bad operand type for unary -.*str",
            re.IGNORECASE,
        ),
        "category": "kwarg_as_string",
        "deterministic_fix": "fix_kwarg_as_string",
        "severity": "HIGH",
    },
    "rdd_not_allowed_serverless": {
        "pattern": re.compile(
            r"NOT_IMPLEMENTED.*PySpark RDDs"
            r"|Using custom code using PySpark RDDs is not allowed on serverless"
            r"|RDDs.*not.*allowed.*serverless",
            re.IGNORECASE,
        ),
        "category": "rdd_not_allowed",
        "deterministic_fix": "fix_rdd_flatmap",
        "severity": "HIGH",
    },
}


def match_pattern(error_message: str) -> tuple[str, ErrorPattern] | tuple[None, None]:
    """Retorna (pattern_key, entry) para o primeiro padrão que casar com error_message.

    Args:
        error_message: Mensagem de erro retornada pelo Databricks.

    Returns:
        Tuple (key, entry) se encontrado, (None, None) se nenhum padrão casar.
    """
    for key, entry in ERROR_PATTERNS.items():
        if entry["pattern"].search(error_message):
            return key, entry
    return None, None
