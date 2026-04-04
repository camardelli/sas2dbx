"""Classificador de constructs SAS — allowlist explícita de escopo suportado.

Qualquer construct fora do SUPPORTED_CONSTRUCTS é classificado como UNKNOWN/Tier.MANUAL
sem tentativa de parsing, evitando silently wrong behavior.
"""

from __future__ import annotations

import logging
import re

from sas2dbx.models.sas_ast import ClassificationResult, Tier

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allowlist de constructs suportados com seus tiers
# ---------------------------------------------------------------------------

SUPPORTED_CONSTRUCTS: dict[str, Tier] = {
    # Tier 1 — rule-based (transpilação determinística)
    "DATA_STEP_SIMPLE": Tier.RULE,   # SET, WHERE, KEEP, DROP, RENAME — sem arrays/retain
    "PROC_SQL": Tier.RULE,
    "PROC_SORT": Tier.RULE,
    "PROC_EXPORT": Tier.RULE,
    "PROC_IMPORT": Tier.RULE,
    "LIBNAME": Tier.RULE,
    # Tier 2 — LLM-assisted (lógica complexa requer geração)
    "DATA_STEP_COMPLEX": Tier.LLM,   # arrays, retain, by-group processing
    "PROC_MEANS": Tier.LLM,
    "PROC_SUMMARY": Tier.LLM,
    "PROC_FREQ": Tier.LLM,
    "MACRO_SIMPLE": Tier.LLM,        # %macro/%mend sem recursão/geração dinâmica
    # Invocação de macro do usuário (standalone %name(...))
    "MACRO_INVOCATION": Tier.LLM,
    # Tier 2 — LLM-assisted (adicionados)
    "PROC_TRANSPOSE": Tier.LLM,      # pivot/unpivot → stack() / selectExpr()
    "PROC_REPORT": Tier.MANUAL,       # relatório tabular — flag manual (sugestão: SQL Dashboard)
    "HASH_OBJECT": Tier.MANUAL,       # lookup rápido — flag manual (sugestão: broadcast join)
    # Tier 3 — manual flag (sem transpilação automática, preserva SAS original)
    "PROC_FORMAT": Tier.MANUAL,
    "PROC_TABULATE": Tier.MANUAL,
    "MACRO_DYNAMIC": Tier.MANUAL,    # CALL EXECUTE, %SYSFUNC, geração dinâmica
    "UNKNOWN": Tier.MANUAL,          # default para qualquer construct não reconhecido
}

_CONFIDENCE: dict[Tier, float] = {
    Tier.RULE: 1.0,
    Tier.LLM: 0.8,
    Tier.MANUAL: 0.0,
}

# ---------------------------------------------------------------------------
# Padrões de detecção (case-insensitive)
# ---------------------------------------------------------------------------

# Keywords SAS % que não são invocações de macro do usuário
_MACRO_INVOCATION_KEYWORDS = frozenset({
    "MACRO", "MEND", "IF", "THEN", "ELSE", "DO", "END", "LET", "PUT",
    "INCLUDE", "GLOBAL", "LOCAL", "SYSFUNC", "SYSEVALF", "SYSCALL",
    "STR", "NRSTR", "QUOTE", "NRQUOTE", "BQUOTE", "NRBQUOTE",
    "SUPERQ", "UNQUOTE", "EVAL", "NREVAL", "SCAN", "SUBSTR",
    "UPCASE", "LOWCASE", "TRIM", "LEFT", "RETURN", "GOTO",
    "ABORT", "STOP", "TO", "BY", "WHILE", "UNTIL",
})

# Indicadores de complexidade num DATA step
_DATA_COMPLEX_PATTERNS = re.compile(
    r"\b(array|retain|hash\s+\w+\s*\(|call\s+execute|%sysfunc)\b",
    re.IGNORECASE,
)

# Indicadores de macro dinâmica
_MACRO_DYNAMIC_PATTERNS = re.compile(
    r"\bcall\s+execute\b"        # CALL EXECUTE
    r"|%sysfunc\s*\("            # %SYSFUNC(
    r"|%do\s+%while\b"           # %DO %WHILE
    r"|%do\s+%until\b"           # %DO %UNTIL
    r"|%macro\s+\w+[^;]*;\s*(?:[^%]|%(?!mend))*%macro\s+\w+",  # macro dentro de macro
    re.IGNORECASE,
)

# Hash object detection
_HASH_OBJECT_PATTERN = re.compile(
    r"\bdeclare\s+hash\b|\bhash\s+\w+\s*\(",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_block(sas_block: str) -> ClassificationResult:
    """Classifica um bloco de código SAS e retorna o tier de transpilação.

    A classificação usa pattern matching leve — sem parsing completo de AST.
    Constructs não reconhecidos são classificados como UNKNOWN/Tier.MANUAL com
    um log de WARNING, nunca são tentados de parsear silenciosamente.

    Args:
        sas_block: Código SAS a classificar (pode ser um bloco parcial).

    Returns:
        ClassificationResult com construct_type, tier e confidence.
    """
    construct_type = _detect_construct(sas_block)
    tier = SUPPORTED_CONSTRUCTS[construct_type]
    confidence = _CONFIDENCE[tier]

    if construct_type == "UNKNOWN":
        logger.warning(
            "Construct SAS não reconhecido — classificado como UNKNOWN/Tier.MANUAL. "
            "Bloco será preservado sem transpilação automática. "
            "Trecho: %.80s",
            sas_block.strip(),
        )

    return ClassificationResult(
        construct_type=construct_type,
        tier=tier,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Internal detection logic
# ---------------------------------------------------------------------------

def _detect_construct(block: str) -> str:
    """Identifica o tipo de construct SAS com pattern matching.

    Retorna sempre uma chave válida de SUPPORTED_CONSTRUCTS.
    """
    stripped = block.strip()

    # LIBNAME (antes de PROC/DATA para evitar colisão)
    if re.match(r"^\s*LIBNAME\s+", stripped, re.IGNORECASE):
        return "LIBNAME"

    # DATA step
    if re.match(r"^\s*DATA\s+", stripped, re.IGNORECASE):
        return _classify_data_step(stripped)

    # PROC variants
    proc_match = re.match(r"^\s*PROC\s+(\w+)", stripped, re.IGNORECASE)
    if proc_match:
        proc_name = proc_match.group(1).upper()
        return _classify_proc(proc_name, stripped)

    # Macro definition
    if re.match(r"^\s*%MACRO\s+", stripped, re.IGNORECASE):
        return _classify_macro(stripped)

    # Standalone macro invocation: %name(...) ou %name;
    m = re.match(r"^\s*%(\w+)", stripped, re.IGNORECASE)
    if m and m.group(1).upper() not in _MACRO_INVOCATION_KEYWORDS:
        return "MACRO_INVOCATION"

    return "UNKNOWN"


def _classify_data_step(block: str) -> str:
    """Determina se é DATA_STEP_SIMPLE ou DATA_STEP_COMPLEX."""
    if _HASH_OBJECT_PATTERN.search(block):
        return "HASH_OBJECT"
    if _DATA_COMPLEX_PATTERNS.search(block):
        return "DATA_STEP_COMPLEX"
    return "DATA_STEP_SIMPLE"


def _classify_proc(proc_name: str, block: str) -> str:
    """Mapeia nome do PROC para construct type."""
    mapping: dict[str, str] = {
        "SQL": "PROC_SQL",
        "SORT": "PROC_SORT",
        "EXPORT": "PROC_EXPORT",
        "IMPORT": "PROC_IMPORT",
        "MEANS": "PROC_MEANS",
        "SUMMARY": "PROC_SUMMARY",
        "FREQ": "PROC_FREQ",
        "FORMAT": "PROC_FORMAT",
        "REPORT": "PROC_REPORT",
        "TABULATE": "PROC_TABULATE",
        "TRANSPOSE": "PROC_TRANSPOSE",
    }
    return mapping.get(proc_name, "UNKNOWN")


def _classify_macro(block: str) -> str:
    """Determina se é MACRO_SIMPLE ou MACRO_DYNAMIC."""
    if _MACRO_DYNAMIC_PATTERNS.search(block):
        return "MACRO_DYNAMIC"
    return "MACRO_SIMPLE"
