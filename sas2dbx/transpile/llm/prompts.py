"""Templates de prompt para transpilação SAS → PySpark por tipo de construto."""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Template principal de transpilação
# ---------------------------------------------------------------------------

_TRANSPILE_TEMPLATE = """\
Você é um especialista em migração SAS para PySpark/Databricks.

TAREFA: Converter o bloco SAS abaixo para código PySpark equivalente.

REGRAS:
1. Use DataFrame API nativa (NUNCA RDDs ou UDFs desnecessárias)
2. Use spark.sql() para PROC SQL — adapte sintaxe SAS SQL para Spark SQL
3. Preserve a lógica exata — mesmos filtros, joins, transformações
4. Adicione comentários explicando cada transformação principal
5. Use F.col() para referências de colunas (import pyspark.sql.functions as F)
6. Para datasets SAS, use spark.read.table("{catalog}.{schema}.<tabela>")
7. Para output datasets, use .write.mode("overwrite").saveAsTable("{catalog}.{schema}.<tabela>")
8. Se encontrar construto que NÃO consegue converter com certeza, retorne:
   # WARNING: [tipo_construto] na linha [N] requer revisão manual
   # SAS original: [código]

CONTEXTO DO AMBIENTE:
- Plataforma alvo: Databricks com Unity Catalog
- Catalog: {catalog}
- Schema: {schema}

{context_section}\
BLOCO SAS ({construct_type}):
```sas
{sas_code}
```

Responda APENAS com o código PySpark. Sem explicações fora dos comentários inline.\
"""

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_transpile_prompt(
    sas_code: str,
    construct_type: str,
    catalog: str = "main",
    schema: str = "migrated",
    context_text: str = "",
) -> str:
    """Monta prompt completo para transpilação de um bloco SAS.

    Args:
        sas_code: Código SAS do bloco a transpilar.
        construct_type: Tipo do construct (ex: "PROC_SQL", "DATA_STEP_SIMPLE").
        catalog: Unity Catalog de destino.
        schema: Schema de destino.
        context_text: Contexto formatado do Knowledge Store (pode ser vazio).

    Returns:
        Prompt completo pronto para envio ao LLM.
    """
    context_section = ""
    if context_text.strip():
        context_section = (
            "REFERÊNCIA TÉCNICA (do Knowledge Store — use como ground truth):\n"
            f"{context_text}\n\n"
        )

    # Usar .replace() em vez de .format() para evitar KeyError/IndexError
    # quando código SAS contém { } (macros, strings literais, hash objects, etc.)
    return (
        _TRANSPILE_TEMPLATE
        .replace("{catalog}", catalog)
        .replace("{schema}", schema)
        .replace("{context_section}", context_section)
        .replace("{construct_type}", construct_type)
        .replace("{sas_code}", sas_code)
    )


def strip_code_fences(text: str) -> str:
    """Remove cercas de código (```python ... ```) da resposta do LLM.

    Args:
        text: Texto bruto da resposta do LLM.

    Returns:
        Código limpo sem cercas.
    """
    text = text.strip()
    # Remove abertura: ```python\n ou ```\n
    text = re.sub(r"^```(?:python|pyspark)?\n?", "", text)
    # Remove fechamento: \n```
    text = re.sub(r"\n?```$", "", text)
    return text.strip()
