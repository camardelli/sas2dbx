"""Templates de prompt para transpilação SAS → PySpark por tipo de construto."""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Template principal de transpilação
# ---------------------------------------------------------------------------

# PP2-04: Parte estática do prompt — cacheada pelo Anthropic prompt caching.
# Separada da parte dinâmica para maximizar cache hits entre chamadas da mesma migração.
_SYSTEM_TEMPLATE = """\
Você é um especialista em migração SAS para PySpark/Databricks.

TAREFA: Converter o bloco SAS fornecido para código PySpark equivalente.

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
   NUNCA adicione .orderBy() imediatamente antes de .write.saveAsTable() — Delta Lake não preserva
   ordem de linha; o sort é descartado e custa um shuffle global desnecessário.
   Exceção: .orderBy() dentro de Window functions (partitionBy/orderBy) é necessário e correto.
9. Para contar linhas após gravar, use SEMPRE spark.table("<tabela>").count() — NUNCA df.count()
   após saveAsTable(), pois o DataFrame original é lazy e re-executa toda a cadeia do zero.
   Correto:  df.write.mode("overwrite").saveAsTable("cat.sch.tab")
             print(spark.table("cat.sch.tab").count())
   Errado:   df.write.mode("overwrite").saveAsTable("cat.sch.tab")
             print(df.count())  # re-executa toda a query!
10. Para macro-variáveis SAS (&VAR): converta como variável Python com valor padrão hardcoded.
   NUNCA use spark.conf.get() sem prefixo "spark." — isso causa CONFIG_NOT_AVAILABLE no serverless.
   Correto: DT_REFERENCIA = "2024-01-01"  # ajustar conforme necessário
   Errado:  DT_REFERENCIA = spark.conf.get("DT_REFERENCIA")
11. Se encontrar construto que NÃO consegue converter com certeza, retorne:
   # WARNING: [tipo_construto] na linha [N] requer revisão manual
   # SAS original: [código]

CONTEXTO DO AMBIENTE:
- Plataforma alvo: Databricks com Unity Catalog
- Catalog: {catalog}
- Schema: {schema}

Responda APENAS com o código PySpark. Sem explicações fora dos comentários inline.\
"""

# Parte dinâmica: contexto do Knowledge Store + DICA + bloco SAS
_USER_TEMPLATE = """\
{context_section}\
BLOCO SAS ({construct_type}):
```sas
{sas_code}
```\
"""

# Template legado unificado — mantido para compatibilidade com paths que não usam caching
_TRANSPILE_TEMPLATE = _SYSTEM_TEMPLATE + "\n\n" + _USER_TEMPLATE

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
- ORDER= → .orderBy() APENAS para display() no notebook; NUNCA antes de saveAsTable()
  (Delta Lake não preserva ordem — o sort seria descartado e custaria um shuffle global)
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


def build_system_prompt(catalog: str = "main", schema: str = "migrated") -> str:
    """PP2-04: Retorna a parte estática do prompt — candidata ao Anthropic prompt caching.

    Deve ser enviada no campo `system` da API. Por ser idêntica entre blocos da mesma
    migração, o Anthropic cache hit elimina o custo de input tokens do trecho estático.

    Args:
        catalog: Unity Catalog de destino.
        schema: Schema de destino.

    Returns:
        System prompt pronto para uso.
    """
    return _SYSTEM_TEMPLATE.replace("{catalog}", catalog).replace("{schema}", schema)


def build_user_message(
    sas_code: str,
    construct_type: str,
    context_text: str = "",
) -> str:
    """PP2-04: Retorna a parte dinâmica do prompt (user message).

    Contém contexto do Knowledge Store, dica de construct type e o bloco SAS.

    Args:
        sas_code: Código SAS do bloco a transpilar.
        construct_type: Tipo do construct (ex: "PROC_SQL", "DATA_STEP_SIMPLE").
        context_text: Contexto formatado do Knowledge Store (pode ser vazio).

    Returns:
        User message pronta para envio.
    """
    context_section = ""
    if context_text.strip():
        context_section = (
            "REFERÊNCIA TÉCNICA (do Knowledge Store — use como ground truth):\n"
            f"{context_text}\n\n"
        )

    hint = _CONSTRUCT_HINTS.get(construct_type, "")
    if hint:
        context_section += hint + "\n"

    return (
        _USER_TEMPLATE
        .replace("{context_section}", context_section)
        .replace("{construct_type}", construct_type)
        .replace("{sas_code}", sas_code)
    )


def build_transpile_prompt(
    sas_code: str,
    construct_type: str,
    catalog: str = "main",
    schema: str = "migrated",
    context_text: str = "",
) -> str:
    """Monta prompt completo para transpilação de um bloco SAS.

    Mantido para compatibilidade. Para novos usos prefira build_system_prompt()
    + build_user_message() para aproveitar prompt caching (PP2-04).

    Args:
        sas_code: Código SAS do bloco a transpilar.
        construct_type: Tipo do construct (ex: "PROC_SQL", "DATA_STEP_SIMPLE").
        catalog: Unity Catalog de destino.
        schema: Schema de destino.
        context_text: Contexto formatado do Knowledge Store (pode ser vazio).

    Returns:
        Prompt completo pronto para envio ao LLM.
    """
    system = build_system_prompt(catalog, schema)
    user = build_user_message(sas_code, construct_type, context_text)
    return system + "\n\n" + user


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
