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
    "column_not_found": {
        "pattern": re.compile(
            r"cannot resolve[`'\s]+([\w.]+)[`'\s]+given input columns"
            r"|UNRESOLVED_COLUMN"
            r"|AnalysisException[:\s]+.*cannot resolve",
            re.IGNORECASE,
        ),
        "category": "missing_column",
        "deterministic_fix": None,
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
        "deterministic_fix": None,
        "severity": "HIGH",
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
