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
5. SEMPRE inclua os imports necessários no topo do código gerado:
   import pyspark.sql.functions as F
   from pyspark.sql.window import Window  (somente se usar Window)
6. Use F.col() para referências de colunas, NUNCA df["col"]
7. Para datasets SAS, use spark.read.table("{catalog}.{schema}.<tabela>")
8. Para output datasets, use .write.mode("overwrite").saveAsTable("{catalog}.{schema}.<tabela>")
9. Para macro-variáveis SAS (&VAR): converta como variável Python com valor padrão hardcoded.
   NUNCA use spark.conf.get() sem prefixo "spark." — isso causa CONFIG_NOT_AVAILABLE no serverless.
   Correto: DT_REFERENCIA = "2024-01-01"  # ajustar conforme necessário
   Errado:  DT_REFERENCIA = spark.conf.get("DT_REFERENCIA")
10. Se encontrar construto que NÃO consegue converter com certeza, retorne:
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
# Dicas especializadas por construct type
# ---------------------------------------------------------------------------

_CONSTRUCT_HINTS: dict[str, str] = {
    "PROC_TRANSPOSE": """\
DICA PROC TRANSPOSE: Converta para PySpark usando stack() + selectExpr() ou melt().
- PROC TRANSPOSE BY var → groupBy(var) antes do pivot
- VAR lista → colunas a pivotar
- ID col → nomes das colunas novas (stack → nome da coluna _NAME_ vira coluna)
- Sem ID → resultado tem colunas COL1, COL2... → renomear depois
- Padrão unpivot (wide→long): df.selectExpr("id", "stack(N, 'col1', col1, 'col2', col2) as (variable, value)")
- Padrão pivot (long→wide): df.groupBy("id").pivot("variable").agg(F.first("value"))
""",
    "PROC_REPORT": """\
DICA PROC REPORT: Converta para agregação PySpark + display/saveAsTable.
- COLUMN lista → colunas a exibir; sem estatística = passthrough (select)
- DEFINE var / ANALYSIS MEAN|SUM|N → .groupBy().agg(F.mean(), F.sum(), F.count())
- DEFINE var / GROUP → chave de agrupamento (groupBy)
- DEFINE var / DISPLAY → coluna de detalhe (sem agregação)
- ORDER= → .orderBy()
- Output: salvar com saveAsTable(); resultado pode ter subtotais → union com totais se necessário
""",
    "HASH_OBJECT": """\
DICA HASH OBJECT: Converta para broadcast join no PySpark.
- hash h() → instanciar dataset pequeno em memória para lookup rápido
- h.defineKey('key_col') + h.defineData('val_col') → chave e valor do lookup
- h.find() → equivale a LEFT JOIN no DataFrame (ou quando em loop, broadcast join)
- Padrão: df_main.join(F.broadcast(df_lookup), on="key_col", how="left")
- Se hash usado dentro de loop/array: considerar criar dict via collectAsMap() para lookup local
""",
    "DATA_STEP_COMPLEX": """\
DICA DATA STEP COMPLEX: Construto com ARRAY, RETAIN ou BY-group processing.
- ARRAY arr{n} vars → list de colunas; iterar com loop → usar F.col() em sequência ou stack
- RETAIN var → WindowSpec com rowsBetween para carry-forward; ou acumulador com Window.unboundedPreceding
- BY group (first./last.) → Window partitionBy + row_number() para detectar first/last
- OUTPUT dentro de loop → union de DataFrames ou explode
""",
    "MACRO_SIMPLE": """\
DICA MACRO SIMPLE: Converta %macro/%mend para função Python def.
- Parâmetros %macro name(p1, p2) → def name(p1, p2, spark=None):
- %let var = valor → variável local Python
- &var → f-string ou parâmetro da função
- Chamadas a PROC/DATA dentro da macro → chamar sub-funções Python
- Retorne o DataFrame resultante ou None se a macro apenas persiste dados
""",
    "PROC_MEANS": """\
DICA PROC MEANS/SUMMARY: Converta para groupBy().agg().
- VAR lista → colunas a agregar
- CLASS vars → groupBy(vars)
- Sem CLASS → agregação global: df.agg(...)
- OUTPUT OUT= com stat= → adicionar .withColumnRenamed() para nomear métricas
- Estatísticas: N→count, MEAN→mean, STD→stddev, MIN→min, MAX→max, SUM→sum, MEDIAN→percentile_approx(col, 0.5)
""",
}

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

    # Injetar dica especializada por construct type, se disponível
    hint = _CONSTRUCT_HINTS.get(construct_type, "")
    if hint:
        context_section += hint + "\n"

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
