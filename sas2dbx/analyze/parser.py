"""Parser de blocos SAS — extrai dependências de datasets, libnames e macros.

Não tenta fazer um parse completo de AST: usa pattern matching focado nos
constructs necessários para o grafo de dependências. Constructs não reconhecidos
são ignorados silenciosamente (o classifier já trata a classificação de tier).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from sas2dbx.models.sas_ast import SASBlock

# ---------------------------------------------------------------------------
# Modelo de dependências de um bloco
# ---------------------------------------------------------------------------


@dataclass
class BlockDeps:
    """Dependências extraídas de um bloco SAS.

    Attributes:
        inputs: Datasets lidos (formato normalizado UPPER ou LIB.UPPER).
        outputs: Datasets criados/sobrescritos.
        libnames_declared: Nomes de bibliotecas declaradas neste bloco.
        macros_called: Nomes de macros invocadas (%nome).
        macros_defined: Nomes de macros definidas neste bloco (%MACRO nome).
    """

    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    libnames_declared: list[str] = field(default_factory=list)
    macros_called: list[str] = field(default_factory=list)
    macros_defined: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Padrões regex
# ---------------------------------------------------------------------------

# Dataset name: lib.ds ou apenas ds
_DS_NAME = r"((?:\w+\.)?\w+)"

# LIBNAME declaration
_RE_LIBNAME = re.compile(r"^\s*LIBNAME\s+(\w+)", re.IGNORECASE | re.MULTILINE)

# DATA step header: captura tudo entre "DATA" e ";" incluindo opções inline
# Ex: "DATA work.out(keep=id) work.err;" → "work.out(keep=id) work.err"
# _normalize_ds remove as opções por token individualmente
_RE_DATA_HEADER = re.compile(
    r"^\s*DATA\s+([^;]+);",
    re.IGNORECASE | re.MULTILINE,
)

# DATA step inputs: SET, MERGE, UPDATE — PP2-09: combined single-pass regex
_RE_SET = re.compile(r"\bSET\s+" + _DS_NAME, re.IGNORECASE)
_RE_MERGE = re.compile(r"\bMERGE\s+" + _DS_NAME, re.IGNORECASE)
_RE_UPDATE = re.compile(r"\bUPDATE\s+" + _DS_NAME, re.IGNORECASE)
_RE_DATA_INPUT = re.compile(r"\b(?:SET|MERGE|UPDATE)\s+" + _DS_NAME, re.IGNORECASE)

# PROC: DATA= parameter (source dataset in proc header)
_RE_PROC_DATA = re.compile(r"\bDATA\s*=\s*" + _DS_NAME, re.IGNORECASE)

# PROC output: OUT= parameter
_RE_PROC_OUT = re.compile(r"\bOUT\s*=\s*" + _DS_NAME, re.IGNORECASE)

# PROC SQL: FROM / JOIN
_RE_SQL_FROM = re.compile(r"\bFROM\s+" + _DS_NAME, re.IGNORECASE)
_RE_SQL_JOIN = re.compile(r"\bJOIN\s+" + _DS_NAME, re.IGNORECASE)

# PROC SQL: CREATE TABLE
_RE_SQL_CREATE = re.compile(
    r"\bCREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+" + _DS_NAME, re.IGNORECASE
)

# PROC SQL: INSERT INTO
_RE_SQL_INSERT = re.compile(r"\bINSERT\s+INTO\s+" + _DS_NAME, re.IGNORECASE)

# Macro definition
_RE_MACRO_DEF = re.compile(r"^\s*%MACRO\s+(\w+)", re.IGNORECASE | re.MULTILINE)

# Macro calls: %name( ou %name; ou %name seguido de espaço — exclui keywords SAS
_SAS_MACRO_KEYWORDS = frozenset({
    "MACRO", "MEND", "IF", "THEN", "ELSE", "DO", "END", "LET", "PUT",
    "INCLUDE", "GLOBAL", "LOCAL", "SYSFUNC", "SYSEVALF", "STR", "NRSTR",
    "QUOTE", "BQUOTE", "NRBQUOTE", "SUPERQ", "SCAN", "SUBSTR", "UPCASE",
    "LOWCASE", "TRIM", "LEFT", "EVAL", "NREVAL", "UNQUOTE",
})
_RE_MACRO_CALL = re.compile(r"%(\w+)\s*[(\s;]", re.IGNORECASE)

# Nomes que não são datasets SAS reais (keywords, especiais)
_PSEUDO_DATASETS = frozenset({
    "_NULL_", "_ALL_", "_LAST_", "_DATA_", "DICTIONARY", "SASHELP",
})


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_block_deps(block: SASBlock | str) -> BlockDeps:
    """Extrai dependências de um bloco SAS.

    Args:
        block: SASBlock ou string de código SAS.

    Returns:
        BlockDeps com inputs, outputs, libnames, macros_called, macros_defined.
    """
    code = block.raw_code if isinstance(block, SASBlock) else block
    deps = BlockDeps()

    # Libnames declarados
    for m in _RE_LIBNAME.finditer(code):
        name = m.group(1).upper()
        if name not in deps.libnames_declared:
            deps.libnames_declared.append(name)

    # Macros definidas
    for m in _RE_MACRO_DEF.finditer(code):
        name = m.group(1).upper()
        if name not in deps.macros_defined:
            deps.macros_defined.append(name)

    # Macros chamadas (exclui keywords e macros definidas neste bloco)
    for m in _RE_MACRO_CALL.finditer(code):
        name = m.group(1).upper()
        if name not in _SAS_MACRO_KEYWORDS and name not in deps.macros_defined:
            if name not in deps.macros_called:
                deps.macros_called.append(name)

    stripped_code = code.strip()
    is_proc_sql = bool(re.match(r"^\s*PROC\s+SQL\b", stripped_code, re.IGNORECASE))
    is_macro_invocation = (
        bool(re.match(r"^\s*%\w+", stripped_code, re.IGNORECASE))
        and not bool(re.match(r"^\s*%MACRO\b", stripped_code, re.IGNORECASE))
    )

    if is_proc_sql:
        _extract_sql_deps(code, deps)
    elif is_macro_invocation:
        _extract_macro_invocation_deps(code, deps)
    else:
        _extract_datastep_proc_deps(code, deps)

    return deps


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_sql_deps(code: str, deps: BlockDeps) -> None:
    """Extrai inputs/outputs de um bloco PROC SQL."""
    for m in _RE_SQL_CREATE.finditer(code):
        ds = _normalize_ds(m.group(1))
        if ds and ds not in deps.outputs:
            deps.outputs.append(ds)

    for m in _RE_SQL_INSERT.finditer(code):
        ds = _normalize_ds(m.group(1))
        if ds and ds not in deps.outputs:
            deps.outputs.append(ds)

    for m in _RE_SQL_FROM.finditer(code):
        ds = _normalize_ds(m.group(1))
        if ds and ds not in deps.outputs and ds not in deps.inputs:
            deps.inputs.append(ds)

    for m in _RE_SQL_JOIN.finditer(code):
        ds = _normalize_ds(m.group(1))
        if ds and ds not in deps.outputs and ds not in deps.inputs:
            deps.inputs.append(ds)


def _extract_datastep_proc_deps(code: str, deps: BlockDeps) -> None:
    """Extrai inputs/outputs de DATA steps e PROCs não-SQL."""
    is_data_step = bool(re.match(r"^\s*DATA\b", code.strip(), re.IGNORECASE))

    if is_data_step:
        # Outputs: captura header completo até ";" e extrai nomes por token.
        # _normalize_ds remove opções inline: "work.out(keep=x)" → "WORK.OUT"
        m = _RE_DATA_HEADER.match(code.strip())
        if m:
            for ds_raw in m.group(1).split():
                ds = _normalize_ds(ds_raw)
                if ds and ds not in deps.outputs:
                    deps.outputs.append(ds)

        # Inputs: SET, MERGE, UPDATE — PP2-09: single-pass regex
        for m in _RE_DATA_INPUT.finditer(code):
            ds = _normalize_ds(m.group(1))
            if ds and ds not in deps.inputs:
                deps.inputs.append(ds)
    else:
        # PROC: DATA= → input, OUT= → output
        for m in _RE_PROC_DATA.finditer(code):
            ds = _normalize_ds(m.group(1))
            if ds and ds not in deps.inputs:
                deps.inputs.append(ds)

        for m in _RE_PROC_OUT.finditer(code):
            ds = _normalize_ds(m.group(1))
            if ds and ds not in deps.outputs:
                deps.outputs.append(ds)


def _extract_macro_invocation_deps(code: str, deps: BlockDeps) -> None:
    """Extrai inputs de uma invocação de macro standalone.

    Extrai referências lib.member presentes nos argumentos como inputs potenciais.
    Nomes sem qualificador (ex: `valor`, `depto`) são ignorados.
    """
    for m in re.finditer(r"\b(\w+\.\w+)\b", code, re.IGNORECASE):
        ds = _normalize_ds(m.group(1))
        if ds and ds not in deps.inputs:
            deps.inputs.append(ds)


def _normalize_ds(raw: str) -> str | None:
    """Normaliza nome de dataset para comparação (UPPER.UPPER).

    Retorna None se o nome for uma keyword SAS ou inválido.
    """
    name = raw.strip().upper()
    # Remove opções inline: DATASET(KEEP=x) → DATASET
    name = re.sub(r"\(.*", "", name).strip()
    if not name or name in _PSEUDO_DATASETS:
        return None
    # Deve conter apenas letras, dígitos, _ e opcionalmente um ponto
    if not re.match(r"^\w+(\.\w+)?$", name):
        return None
    return name
