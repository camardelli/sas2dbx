"""ContextBuilder — monta contexto do Knowledge Store para injeção no prompt LLM.

Usa os métodos *_or_harvest() do KnowledgeStore para enriquecer o contexto:
quando um mapeamento não existe em merged/, tenta harvest on-demand via LLM
(se llm_client foi fornecido ao KnowledgeStore).

Apenas context.py usa os métodos *_or_harvest(). Outros módulos (validate.py,
report.py) continuam usando os lookups simples — validação não deve disparar harvest.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sas2dbx.knowledge.store import KnowledgeStore

logger = logging.getLogger(__name__)

# Limite de tokens para contexto injetado no prompt (configurável via sas2dbx.yaml)
DEFAULT_MAX_CONTEXT_TOKENS = 2000

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
