"""ContextBuilder — monta contexto do Knowledge Store para injeção no prompt LLM.

Usa os métodos *_or_harvest() do KnowledgeStore para enriquecer o contexto:
quando um mapeamento não existe em merged/, tenta harvest on-demand via LLM
(se llm_client foi fornecido ao KnowledgeStore).

Apenas context.py usa os métodos *_or_harvest(). Outros módulos (validate.py,
report.py) continuam usando os lookups simples — validação não deve disparar harvest.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sas2dbx.knowledge.store import KnowledgeStore

logger = logging.getLogger(__name__)

# Limite de tokens para contexto injetado no prompt (configurável via sas2dbx.yaml)
DEFAULT_MAX_CONTEXT_TOKENS = 2000

# ---------------------------------------------------------------------------
# Schema context — carrega custom/schemas.yaml e formata para o prompt
# ---------------------------------------------------------------------------

# Regex para extrair sugestões de coluna de erros Databricks:
# "Did you mean one of the following? [`col1`, `col2`, `col3`]"
_RE_COLUMN_SUGGESTIONS = re.compile(
    r"Did you mean one of the following\?\s*\[([^\]]+)\]",
    re.IGNORECASE,
)
# Extrai nomes de tabela de spark.read.table("cat.sch.table") no notebook
_RE_TABLE_REF = re.compile(
    r'spark\.(?:read\.table|table)\(\s*["\']([a-zA-Z0-9_.]+)["\']\s*\)',
)
# Macro call com parâmetros: %macro_name(param1=val1, param2=LIB.table, ...)
_RE_MACRO_CALL = re.compile(
    r'%([A-Za-z_]\w*)\s*\(([^)]*)\)',
    re.IGNORECASE | re.DOTALL,
)
# Parâmetro que referencia tabela SAS: param=LIBNAME.dataset
_RE_PARAM_TABLE = re.compile(
    r'([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w+)\.([A-Za-z_]\w+)',
    re.IGNORECASE,
)


def load_table_schemas(schemas_path: Path) -> dict[str, list[dict]]:
    """Carrega schemas de tabela de custom/schemas.yaml (ou migration schemas.yaml).

    Formato YAML esperado (novo, com tipos):
        telcostar.operacional.vendas_raw:
          columns:
            - {name: id_venda, type: STRING}
            - {name: valor_bruto, type: DOUBLE}

    Backward compatible com formato antigo (apenas nomes):
        telcostar.operacional.vendas_raw:
          columns: [id_venda, id_cliente, ...]  # convertido para list[dict] automaticamente

    Args:
        schemas_path: Caminho para o arquivo schemas.yaml.

    Returns:
        Dict table_fqn → list[{"name": str, "type": str}]. Vazio se arquivo não existe.
    """
    if not schemas_path.exists():
        return {}
    try:
        import yaml  # type: ignore[import]
        raw = yaml.safe_load(schemas_path.read_text(encoding="utf-8")) or {}
        result: dict[str, list[dict]] = {}
        for table, meta in raw.items():
            if isinstance(meta, dict):
                cols_raw = meta.get("columns") or meta.get("cols") or []
            elif isinstance(meta, list):
                cols_raw = meta
            else:
                continue
            if not cols_raw:
                continue
            # Normaliza: strings antigas → {"name": col, "type": "STRING"}
            cols: list[dict] = []
            for c in cols_raw:
                if isinstance(c, dict):
                    cols.append({"name": str(c.get("name", "")), "type": str(c.get("type", "STRING"))})
                else:
                    cols.append({"name": str(c), "type": "STRING"})
            if cols:
                result[str(table).lower()] = cols
        return result
    except Exception as exc:  # noqa: BLE001
        logger.warning("load_table_schemas: erro ao ler %s — %s", schemas_path, exc)
        return {}


def enrich_schemas_from_errors(
    schemas: dict[str, list[dict]],
    error_messages: list[str],
    notebook_content: str = "",
) -> dict[str, list[dict]]:
    """Enriquece o dicionário de schemas com colunas extraídas de erros Databricks.

    Quando Databricks retorna UNRESOLVED_COLUMN.WITH_SUGGESTION, ele lista as colunas
    disponíveis na tabela. Extrai essas sugestões e as associa às tabelas referenciadas
    no notebook, complementando o schemas.yaml existente.

    Args:
        schemas: Schemas existentes (modificado in-place).
        error_messages: Lista de mensagens de erro Databricks.
        notebook_content: Código do notebook (para extrair nomes de tabela).

    Returns:
        schemas enriquecido (mesmo objeto modificado in-place).
    """
    # Extrai tabelas referenciadas no notebook
    tables_in_notebook = [m.lower() for m in _RE_TABLE_REF.findall(notebook_content)]

    for error in error_messages:
        matches = _RE_COLUMN_SUGGESTIONS.findall(error)
        for match in matches:
            # Normaliza: remove backticks e espaços
            suggested_cols = [
                c.strip().strip("`").strip("'\"")
                for c in match.split(",")
                if c.strip().strip("`").strip("'\"")
            ]
            if not suggested_cols:
                continue
            # Associa às tabelas do notebook (melhor esforço — sem schema info do Databricks)
            # Usa a primeira tabela como proxy; schemas existentes têm precedência
            for table in tables_in_notebook:
                existing = schemas.get(table, [])
                existing_names = {c["name"] for c in existing}
                new_dicts = [
                    {"name": c, "type": "STRING"}
                    for c in suggested_cols
                    if c not in existing_names
                ]
                schemas[table] = existing + new_dicts
                logger.debug(
                    "enrich_schemas_from_errors: %s ← %d colunas extraídas do erro",
                    table, len(suggested_cols),
                )

    return schemas


def format_schema_context(schemas: dict[str, list[dict]]) -> str:
    """Formata schemas de tabela para injeção no prompt de transpilação.

    Args:
        schemas: Dict table_fqn → list[{"name": str, "type": str}].

    Returns:
        String formatada, ex:
            - telcostar.operacional.vendas_raw: id_venda, id_cliente, valor_bruto, ...
            - telcostar.operacional.clientes_raw: id_cliente, nome, status, ...
    """
    if not schemas:
        return ""
    lines = []
    for table, cols in sorted(schemas.items()):
        cols_str = ", ".join(c["name"] for c in cols)
        lines.append(f"- {table}: {cols_str}")
    return "\n".join(lines)

def load_libnames(libnames_path: Path) -> dict[str, dict]:
    """Carrega libnames.yaml → dict LIBNAME_UPPER → {catalog, schema}.

    Args:
        libnames_path: Caminho para knowledge/custom/libnames.yaml.

    Returns:
        Dict normalizado com chaves em maiúsculas. Vazio se arquivo não existe.
    """
    if not libnames_path.exists():
        return {}
    try:
        import yaml  # type: ignore[import]
        raw = yaml.safe_load(libnames_path.read_text(encoding="utf-8")) or {}
        result: dict[str, dict] = {}
        for lib, meta in raw.items():
            if isinstance(meta, dict):
                result[str(lib).upper()] = {
                    "catalog": str(meta.get("catalog", "")),
                    "schema": str(meta.get("schema", "")),
                }
        return result
    except Exception as exc:  # noqa: BLE001
        logger.warning("load_libnames: erro ao ler %s — %s", libnames_path, exc)
        return {}


def extract_macro_resolutions(
    sas_code: str,
    schemas: dict[str, list[dict]],
    libnames: dict[str, dict],
) -> str:
    """Gera seção MACRO RESOLUTION para o prompt quando o bloco SAS usa macros paramétricas.

    Problema que resolve: o LLM recebe `&ds_vendas` como referência de coluna/tabela
    e não sabe que esse parâmetro foi instanciado com `ds_vendas=TELCO.vendas_raw`.
    Sem essa resolução explícita, o LLM "normaliza" colunas (inventa `vl_venda` em vez
    de usar `valor_bruto` do schema real).

    Para cada chamada %macro_name(param=LIB.table), resolve:
      LIB → catalog.schema  (via libnames.yaml)
      catalog.schema.table → colunas reais  (via schemas.yaml)

    Args:
        sas_code: Código SAS do bloco a transpilar.
        schemas: Dict FQN → colunas (de load_table_schemas / schemas.yaml).
        libnames: Dict LIBNAME_UPPER → {catalog, schema} (de load_libnames).

    Returns:
        String formatada para injeção no prompt, ou "" se não houver macros resolvíveis.
    """
    if not sas_code.strip():
        return ""

    lines: list[str] = []

    for macro_match in _RE_MACRO_CALL.finditer(sas_code):
        macro_name = macro_match.group(1)
        params_str = macro_match.group(2)

        resolutions: list[str] = []
        for param_match in _RE_PARAM_TABLE.finditer(params_str):
            param_name = param_match.group(1)
            lib_name = param_match.group(2).upper()
            table_name = param_match.group(3).lower()

            # Resolve LIBNAME → catalog.schema
            lib_info = libnames.get(lib_name, {})
            catalog = lib_info.get("catalog", "")
            schema_name = lib_info.get("schema", "")

            # Busca colunas: FQN completo primeiro, depois short name como fallback
            cols_dicts: list[dict] = []
            resolved_fqn = ""
            if catalog and schema_name:
                resolved_fqn = f"{catalog}.{schema_name}.{table_name}"
                cols_dicts = schemas.get(resolved_fqn, [])

            if not cols_dicts:
                for fqn_key, fqn_cols in schemas.items():
                    if fqn_key.split(".")[-1].lower() == table_name:
                        cols_dicts = fqn_cols
                        resolved_fqn = fqn_key
                        break

            if cols_dicts:
                cols_str = ", ".join(c["name"] for c in cols_dicts)
                resolutions.append(
                    f"  &{param_name} = {lib_name}.{table_name} "
                    f"→ {resolved_fqn}\n"
                    f"  columns available: {cols_str}"
                )
                logger.debug(
                    "extract_macro_resolutions: %%%s &%s → %s (%d cols)",
                    macro_name, param_name, resolved_fqn, len(cols_dicts),
                )

        if resolutions:
            lines.append(f"%{macro_name} parameter resolution:")
            lines.extend(resolutions)

    if not lines:
        return ""

    return (
        "MACRO RESOLUTION — parâmetros resolvidos para tabelas reais:\n"
        + "\n".join(lines)
        + "\n"
    )


# PP2-03: Claude usa ~3.5 chars/token (cl100k_base medido empiricamente)
_CHARS_PER_TOKEN = 3.5


def build_context(
    ks: KnowledgeStore,
    func_names: list[str] | None = None,
    proc_names: list[str] | None = None,
    sql_constructs: list[str] | None = None,
    format_names: list[str] | None = None,
    sas_reference_keys: list[tuple[str, str]] | None = None,
    pyspark_reference_keys: list[tuple[str, str]] | None = None,
    max_tokens: int = DEFAULT_MAX_CONTEXT_TOKENS,
    sas_code: str = "",  # PP2-01: usado para batch harvest de funções desconhecidas
) -> dict[str, Any]:
    """Monta o contexto do Knowledge Store para o prompt de transpilação.

    PP2-01: Two-pass para funções — lookup rápido (sem LLM) depois batch harvest
    para desconhecidas (1 chamada LLM em vez de N).
    PP2-03: Budget calibrado para Claude (~3.5 chars/token).

    Args:
        ks: KnowledgeStore configurado (com ou sem llm_client).
        func_names: Funções SAS encontradas no bloco a transpilar.
        proc_names: PROCs SAS encontrados no bloco.
        sql_constructs: Construtos de dialeto SQL SAS encontrados.
        format_names: Formatos SAS encontrados.
        sas_reference_keys: Lista de (category, name) para docs SAS.
        pyspark_reference_keys: Lista de (category, name) para docs PySpark.
        max_tokens: Limite estimado de tokens para o contexto total.
        sas_code: Código SAS do bloco — melhora precisão do batch harvest.

    Returns:
        Dict com chaves: function_mappings, proc_mappings, sql_dialect_notes,
        format_mappings, sas_references, pyspark_references.
    """
    context: dict[str, Any] = {
        "function_mappings": {},
        "proc_mappings": {},
        "sql_dialect_notes": {},
        "format_mappings": {},
        "sas_references": [],
        "pyspark_references": [],
    }

    # PP2-03: budget calibrado — Claude ≈ 3.5 chars/token
    _budget = int(max_tokens * _CHARS_PER_TOKEN)

    def _fits(value: Any) -> bool:
        """Verifica se `value` cabe no budget restante e desconta se sim."""
        nonlocal _budget
        cost = len(str(value))
        if cost <= _budget:
            _budget -= cost
            return True
        logger.debug("ContextBuilder: budget esgotado, entrada omitida")
        return False

    # PP2-01: funções — two-pass (lookup rápido + batch harvest dos desconhecidos)
    if func_names:
        unknown_funcs: list[str] = []
        for func in func_names:
            result = ks.lookup_function(func)  # sem LLM
            if result is not None and _fits(result):
                context["function_mappings"][func.upper()] = result
            elif result is None:
                unknown_funcs.append(func)

        # Batch harvest: 1 chamada LLM para todos os desconhecidos
        if unknown_funcs:
            batch = ks.batch_harvest_functions_sync(unknown_funcs, sas_code)
            for func_upper, entry in batch.items():
                if entry is not None and _fits(entry):
                    context["function_mappings"][func_upper] = entry

    # PROCs SAS (com on-demand harvest individual — PROCs são raros, custo baixo)
    for proc in proc_names or []:
        result = ks.lookup_proc_or_harvest(proc)
        if result is not None and _fits(result):
            context["proc_mappings"][proc.upper()] = result

    # Construtos de dialeto SQL (com on-demand harvest)
    for construct in sql_constructs or []:
        result = ks.lookup_sql_dialect_or_harvest(construct)
        if result is not None and _fits(result):
            context["sql_dialect_notes"][construct.upper()] = result

    # Formatos SAS (com on-demand harvest)
    for fmt in format_names or []:
        result = ks.lookup_format_or_harvest(fmt)
        if result is not None and _fits(result):
            context["format_mappings"][fmt.upper()] = result

    # Docs de referência SAS (lookup simples — sem on-demand)
    for category, name in sas_reference_keys or []:
        doc = ks.get_reference("sas", category, name)
        if doc and _fits(doc):
            context["sas_references"].append({"category": category, "name": name, "content": doc})

    # Docs de referência PySpark (lookup simples — sem on-demand)
    for category, name in pyspark_reference_keys or []:
        doc = ks.get_reference("pyspark", category, name)
        if doc and _fits(doc):
            context["pyspark_references"].append(
                {"category": category, "name": name, "content": doc}
            )

    logger.debug(
        "ContextBuilder: %d funções, %d procs, %d SQL constructs, %d formatos "
        "(budget restante: ~%d tokens)",
        len(context["function_mappings"]),
        len(context["proc_mappings"]),
        len(context["sql_dialect_notes"]),
        len(context["format_mappings"]),
        int(_budget / _CHARS_PER_TOKEN),
    )

    return context


def format_context_for_prompt(context: dict[str, Any]) -> str:
    """Formata o contexto como texto para injeção no prompt LLM.

    Args:
        context: Dict retornado por build_context().

    Returns:
        String formatada para inserção no prompt de transpilação.
    """
    parts: list[str] = []

    if context.get("function_mappings"):
        parts.append("## Mapeamentos de Funções SAS → PySpark")
        for name, mapping in context["function_mappings"].items():
            pyspark = mapping.get("pyspark", "N/A")
            notes = mapping.get("notes", "")
            line = f"- {name}: `{pyspark}`"
            if notes:
                line += f" — {notes}"
            parts.append(line)

    if context.get("proc_mappings"):
        parts.append("\n## Mapeamentos de PROCs SAS")
        for name, mapping in context["proc_mappings"].items():
            notes = mapping.get("notes", "")
            approach = mapping.get("approach", "?")
            parts.append(f"- {name}: approach={approach} — {notes}")

    if context.get("sql_dialect_notes"):
        parts.append("\n## Diferenças de Dialeto SQL SAS → Spark SQL")
        for name, mapping in context["sql_dialect_notes"].items():
            sas_ex = mapping.get("sas", "")
            spark_ex = mapping.get("spark", "")
            notes = mapping.get("notes", "")
            parts.append(f"- {name}: SAS=`{sas_ex}` → Spark=`{spark_ex}` ({notes})")

    if context.get("format_mappings"):
        parts.append("\n## Mapeamentos de Formatos SAS")
        for name, mapping in context["format_mappings"].items():
            pyspark = mapping.get("pyspark", "N/A")
            parts.append(f"- {name}: `{pyspark}`")

    for ref in context.get("sas_references", []):
        parts.append(f"\n## Referência SAS: {ref['name']}\n{ref['content']}")

    for ref in context.get("pyspark_references", []):
        parts.append(f"\n## Referência PySpark: {ref['name']}\n{ref['content']}")

    return "\n".join(parts)
