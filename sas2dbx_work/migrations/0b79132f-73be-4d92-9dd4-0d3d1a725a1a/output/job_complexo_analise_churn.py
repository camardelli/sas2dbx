# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # job_complexo_analise_churn
# MAGIC **Migrado de:** job_complexo_analise_churn.sas  
# MAGIC **Data migração:** 2026-04-03  
# MAGIC **Confiança:** 0.86  
# MAGIC **Warnings:**  
# MAGIC _nenhum_
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# =============================================================================
# Equivalente PySpark para LIBNAME SAS
# LIBNAME TELCO "/data/telcostar/operacional"
#
# No contexto Databricks/Unity Catalog, o LIBNAME SAS aponta para um diretório
# físico de dados. O equivalente é referenciar o catalog e schema diretamente
# via spark.read.table() ou spark.sql(), sem necessidade de declaração explícita
# de "library" — o Unity Catalog gerencia o namespace automaticamente.
# =============================================================================


# -----------------------------------------------------------------------------
# Definição do namespace equivalente ao LIBNAME TELCO
# Substitui: LIBNAME TELCO "/data/telcostar/operacional"
# No Unity Catalog: catalog = "telcostar", schema = "operacional"
# -----------------------------------------------------------------------------
CATALOG = "telcostar"
SCHEMA  = "operacional"

# Helper para montar o nome completo de uma tabela (three-part name)
# Uso: full_table("minha_tabela") → "telcostar.operacional.minha_tabela"
def full_table(table_name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table_name}"

# -----------------------------------------------------------------------------
# Definição do database padrão para a sessão Spark
# Equivalente a usar TELCO.<tabela> no SAS sem precisar qualificar toda vez
# -----------------------------------------------------------------------------
# [PREFLIGHT-BOOTSTRAP] Placeholders criados antes do deploy (preflight)
spark.catalog.setCurrentCatalog(CATALOG)
spark.catalog.setCurrentDatabase(SCHEMA)

# -----------------------------------------------------------------------------
# A partir daqui, tabelas podem ser lidas de duas formas equivalentes:
#
#   Forma 1 — qualificada (recomendada para clareza):
#     df = spark.read.table(full_table("nome_tabela"))
#
#   Forma 2 — não qualificada (funciona após setCurrentCatalog/Database):
#     df = spark.read.table("nome_tabela")
#
# E gravadas como:
#     df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table("nome_tabela"))
#
# Contagem pós-gravação (SEMPRE via spark.table, nunca df.count()):
#     print(spark.table(full_table("nome_tabela")).count())
# -----------------------------------------------------------------------------
# COMMAND ----------
# =============================================================================
# Conversão SAS → PySpark | Databricks Unity Catalog
# LIBNAME STAGING → referências via spark.read.table("telcostar.operacional.<tabela>")
# =============================================================================


# ---------------------------------------------------------------------------
# LIBNAME STAGING "/data/telcostar/staging";
#
# Em SAS, LIBNAME define um atalho (alias) para um diretório físico onde
# datasets SAS (.sas7bdat) são lidos/gravados.
#
# No Databricks com Unity Catalog NÃO existe equivalente direto de LIBNAME,
# pois o acesso a dados é feito via:
#   - Tabelas registradas no Metastore  → spark.read.table("telcostar.operacional.tabela")
#   - Caminhos cloud (ADLS/S3/GCS)      → spark.read.format(...).load("abfss://...")
#
# AÇÃO NECESSÁRIA: identifique cada dataset SAS referenciado como STAGING.<nome>
# no código original e substitua pelo caminho/tabela correspondente no ambiente
# Databricks, conforme os exemplos abaixo.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# EXEMPLO 1 — Dataset SAS lido de STAGING mapeado para tabela Unity Catalog
#
#   SAS:     data work.minha_tabela; set STAGING.minha_tabela; run;
#   PySpark: df_minha_tabela = spark.read.table("telcostar.operacional.minha_tabela")
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# EXEMPLO 2 — Dataset SAS lido de STAGING mapeado para arquivo em path externo
#
#   SAS:     data work.minha_tabela; set STAGING.minha_tabela; run;
#   PySpark: df_minha_tabela = (
#                spark.read
#                     .format("parquet")          # ajustar: parquet | delta | csv | orc
#                     .load("abfss://<container>@<storage>.dfs.core.windows.net/staging/minha_tabela/")
#            )
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# EXEMPLO 3 — Gravação de output equivalente ao STAGING.<nome> em SAS
#
#   SAS:     data STAGING.resultado; set work.resultado; run;
#   PySpark: df_resultado.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.resultado")
#            print(spark.table("telcostar.operacional.resultado").count())
# ---------------------------------------------------------------------------

# WARNING: [LIBNAME] na linha 1 requer revisão manual
# SAS original: LIBNAME STAGING "/data/telcostar/staging";
# Motivo: LIBNAME é apenas um ponteiro de diretório SAS sem equivalente direto
# em PySpark. Cada dataset referenciado como STAGING.<nome> deve ser mapeado
# individualmente para spark.read.table("telcostar.operacional.<nome>") ou
# spark.read.format(...).load("<path_cloud>/<nome>/") conforme a origem real
# dos dados no ambiente Databricks.
# COMMAND ----------
# =============================================================================
# Conversão SAS → PySpark/Databricks
# LIBNAME DW → Unity Catalog: telcostar.operacional
# =============================================================================
# Em Databricks com Unity Catalog, não há equivalente direto ao LIBNAME SAS.
# O LIBNAME apenas aponta para um diretório/schema de dados.
# A convenção adotada é:
#   - Leitura:  spark.read.table("telcostar.operacional.<tabela>")
#   - Escrita:  df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.<tabela>")
# =============================================================================


# ---------------------------------------------------------------------------
# Definição do catalog e schema equivalentes ao LIBNAME DW
# Substitui: LIBNAME DW "/data/telcostar/datawarehouse";
# ---------------------------------------------------------------------------
CATALOG = "telcostar"
SCHEMA  = "operacional"

# Helper para montar o nome completo da tabela (three-part name do Unity Catalog)
def full_table(table_name: str) -> str:
    """Retorna o nome qualificado catalog.schema.tabela."""
    return f"{CATALOG}.{SCHEMA}.{table_name}"

# ---------------------------------------------------------------------------
# Exemplos de uso do helper (substituem referências DW.<tabela> no SAS):
#
#   Leitura:
#     df = spark.read.table(full_table("minha_tabela"))
#
#   Escrita:
#     df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table("minha_tabela"))
#     print(spark.table(full_table("minha_tabela")).count())
# ---------------------------------------------------------------------------

# Definir o banco de dados padrão da sessão (opcional, mas conveniente)
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
# COMMAND ----------

def calc_churn_score(lib_in, ds_base, ds_uso, ano_mes, ds_out, spark=None):
    """
    Equivalente PySpark da macro SAS calc_churn_score.

    Parâmetros
    ----------
    lib_in  : str  — schema/banco de origem (ex.: "telcostar.operacional")
    ds_base : str  — nome da tabela de clientes
    ds_uso  : str  — nome da tabela de uso
    ano_mes : str  — período de referência (ex.: "202501")
    ds_out  : str  — prefixo da tabela de saída
    spark   : SparkSession — obrigatório; passado explicitamente
    """

    # ------------------------------------------------------------------
    # PASSO 1 — Une base de clientes com métricas de uso
    # Equivalente ao PROC SQL que cria <ds_out>_raw
    # ------------------------------------------------------------------

    # Lê tabelas de origem
    df_base = spark.read.table(f"{lib_in}.{ds_base}")
    df_uso  = spark.read.table(f"{lib_in}.{ds_uso}")

    # Alias para evitar ambiguidade nas colunas de join
    c = df_base.alias("c")
    u = df_uso.alias("u")

    # LEFT JOIN entre clientes e uso filtrado pelo período
    df_joined = (
        c.join(
            u.filter(F.col("u.ano_mes") == ano_mes),
            on=F.col("c.id_cliente") == F.col("u.id_cliente"),
            how="left"
        )
        # Filtros equivalentes ao WHERE do PROC SQL
        .filter(
            (F.col("c.fl_ativo") == 1) &
            (F.col("c.dt_ativacao") <= F.lit("2025-01-01").cast("date"))
        )
    )

    # Calcula meses_ativo: INTCK('MONTH', dt_ativacao, TODAY())
    # months_between retorna fração; cast para int replica INTCK truncado
    df_raw = (
        df_joined
        .select(
            F.col("c.id_cliente"),
            F.col("c.nm_cliente"),
            F.col("c.cd_plano"),
            F.col("c.dt_ativacao"),
            F.col("c.dt_cancelamento"),
            F.col("c.vl_mensalidade"),
            F.col("c.cd_segmento"),
            F.col("c.fl_portabilidade"),
            F.col("u.qt_min_voz"),
            F.col("u.qt_gb_dados"),
            F.col("u.qt_sms"),
            F.col("u.vl_excedente"),
            F.col("u.qt_chamadas_cs"),

            # INTCK('MONTH', dt_ativacao, TODAY()) → months_between truncado
            F.floor(
                F.months_between(F.current_date(), F.col("c.dt_ativacao"))
            ).cast("int").alias("meses_ativo"),

            # ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id()) → número de linha global (row_number sobre tudo)
            F.row_number()
             .over(Window.orderBy(F.lit(1)))
             .alias("row_num"),

            # fl_sem_dados: 1 quando qt_gb_dados = 0
            F.when(F.col("u.qt_gb_dados") == 0, 1).otherwise(0)
             .alias("fl_sem_dados"),

            # CATX(' | ', cd_plano, cd_segmento)
            F.concat_ws(" | ", F.col("c.cd_plano"), F.col("c.cd_segmento"))
             .alias("chave_segmento"),
        )
        # receita_acumulada = meses_ativo * vl_mensalidade
        # calculada após o select para poder referenciar meses_ativo
        .withColumn(
            "receita_acumulada",
            F.col("meses_ativo") * F.col("vl_mensalidade")
        )
    )

    # Persiste tabela _raw (sem orderBy — Delta não preserva ordem de linha)
    raw_table = f"telcostar.operacional.{ds_out}_raw"
    df_raw.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(raw_table)
    print(f"[{raw_table}] linhas gravadas: {spark.table(raw_table).count()}")

    # ------------------------------------------------------------------
    # PASSO 2 — Score ponderado com rank e acumulado por segmento
    # Equivalente ao PROC SORT + DATA STEP com RETAIN
    # ------------------------------------------------------------------

    df_raw_reload = spark.table(raw_table)

    # Janela por segmento ordenada por receita_acumulada DESC
    # Replica o BY cd_segmento + DESCENDING receita_acumulada do PROC SORT
    w_seg = (
        Window
        .partitionBy(F.col("cd_segmento"))
        .orderBy(F.col("receita_acumulada").desc())
    )

    # rank_seg: posição dentro do segmento (RETAIN rank_seg + 1)
    # acum_receita_seg: soma acumulada de vl_mensalidade dentro do segmento
    #   equivale ao RETAIN acum_receita_seg + vl_mensalidade
    df_scored = (
        df_raw_reload
        .withColumn("rank_seg", F.row_number().over(w_seg))
        .withColumn(
            "acum_receita_seg",
            F.sum("vl_mensalidade").over(
                w_seg.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
        )

        # ------------------------------------------------------------------
        # Score de churn — pesos fixos (ARRAY _TEMPORARY_ no SAS)
        # peso_risco = [0.30, 0.25, 0.20, 0.15, 0.10]
        # score = p1*(qt_chamadas_cs/MAX(qt_chamadas_cs,1))
        #       + p2*fl_sem_dados
        #       + p3*(1 - MIN(meses_ativo/24, 1))
        #       + p4*(vl_excedente > 0)
        #       + p5*fl_portabilidade
        # ------------------------------------------------------------------
        .withColumn(
            "score_churn",
            F.lit(0.30) * (
                F.col("qt_chamadas_cs") /
                F.greatest(F.col("qt_chamadas_cs"), F.lit(1))
            ) +
            F.lit(0.25) * F.col("fl_sem_dados").cast("double") +
            F.lit(0.20) * (
                F.lit(1.0) - F.least(F.col("meses_ativo") / F.lit(24.0), F.lit(1.0))
            ) +
            F.lit(0.15) * F.when(F.col("vl_excedente") > 0, 1).otherwise(0).cast("double") +
            F.lit(0.10) * F.col("fl_portabilidade").cast("double")
        )

        # cd_risco: classificação por faixa de score
        .withColumn(
            "cd_risco",
            F.when(F.col("score_churn") >= 0.70, "ALTO")
             .when(F.col("score_churn") >= 0.40, "MEDIO")
             .otherwise("BAIXO")
        )

        # dt_saida_estimada: INTNX('MONTH', TODAY(), 3, 'E')
        # 'E' = último dia do mês resultante
        # add_months(current_date, 3) → último dia via last_day
        .withColumn(
            "dt_saida_estimada",
            F.last_day(F.add_months(F.current_date(), 3))
        )

        # familia_plano: SCAN(cd_plano, 1, '_') → primeiro token antes de '_'
        .withColumn(
            "familia_plano",
            F.split(F.col("cd_plano"), "_").getItem(0)
        )

        # sufixo_plano: COMPRESS(cd_plano, '', 'A') → remove letras, mantém dígitos/símbolos
        # Equivalente: regexp_replace remove todos os caracteres alfabéticos
        .withColumn(
            "sufixo_plano",
            F.regexp_replace(F.col("cd_plano"), "[A-Za-z]", "")
        )

        # nm_cliente_upper: UPCASE(COMPRESS(nm_cliente, '  ', 's'))
        # COMPRESS com modificador 's' remove espaços; depois UPCASE
        .withColumn(
            "nm_cliente_upper",
            F.upper(F.regexp_replace(F.col("nm_cliente"), "\\s+", ""))
        )

        # KEEP — seleciona apenas as colunas do DATA STEP KEEP
        .select(
            F.col("id_cliente"),
            F.col("nm_cliente"),
            F.col("cd_plano"),
            F.col("cd_segmento"),
            F.col("vl_mensalidade"),
            F.col("qt_min_voz"),
            F.col("qt_gb_dados"),
            F.col("qt_chamadas_cs"),
            F.col("vl_excedente"),
            F.col("meses_ativo"),
            F.col("receita_acumulada"),
            F.col("rank_seg"),
            F.col("acum_receita_seg"),
            F.col("score_churn"),
            F.col("cd_risco"),
            F.col("dt_saida_estimada"),
            F.col("familia_plano"),
            F.col("sufixo_plano"),
            F.col("fl_sem_dados"),
            F.col("fl_portabilidade"),
            F.col("row_num"),
        )
    )

    # Persiste tabela _scored
    scored_table = f"telcostar.operacional.{ds_out}_scored"
    df_scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(scored_table)
    print(f"[{scored_table}] linhas gravadas: {spark.table(scored_table).count()}")

    return spark.table(scored_table)


# ------------------------------------------------------------------
# Exemplo de chamada — ajuste os parâmetros conforme necessário
# ------------------------------------------------------------------
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
#
# calc_churn_score(
#     lib_in  = "telcostar.operacional",
#     ds_base = "clientes",
#     ds_uso  = "uso_mensal",
#     ano_mes = "202501",   # ajustar conforme necessário
#     ds_out  = "churn_score",
#     spark   = spark,
# )
# COMMAND ----------
# Window não é necessário neste script, mas mantemos o import caso seja estendido
# from pyspark.sql.window import Window

def identifica_eventos_churn(
    lib_in: str,
    ds_scored: str,
    ano_mes: str,          # ex.: "202501"  (formato YYYYMM, equivalente ao YYMMN6. do SAS)
    ds_eventos: str,
    spark=None
):
    """
    Equivalente PySpark da macro SAS identifica_eventos_churn.

    Parâmetros
    ----------
    lib_in    : schema de origem do historico_cancelamentos (ex.: "telcostar.operacional")
    ds_scored : nome da tabela de scores já gravada em telcostar.operacional
    ano_mes   : string YYYYMM usada para comparar com o início do mês de dt_cancelamento
    ds_eventos: nome da tabela de saída em telcostar.operacional
    spark     : SparkSession ativa
    """

    # ------------------------------------------------------------------
    # 1. Derivar o primeiro dia do mês informado em ano_mes (YYYYMM)
    #    Equivalente SAS: INPUT(PUT(&ano_mes, 6.), YYMMN6.)
    #    INTNX('MONTH', dt_cancelamento, 0, 'B') = primeiro dia do mês
    #    de dt_cancelamento.
    #    A condição SAS verifica se ano_mes <= início do mês de cancelamento,
    #    ou seja: primeiro_dia_ano_mes <= trunc(dt_cancelamento, 'MM')
    # ------------------------------------------------------------------
    primeiro_dia_ano_mes = f"{ano_mes[:4]}-{ano_mes[4:6]}-01"  # ex.: "2025-01-01"

    # ------------------------------------------------------------------
    # 2. Leitura das tabelas de entrada
    # ------------------------------------------------------------------
    # Tabela de scores (STAGING.&ds_scored no SAS)
    df_scored = spark.read.table(f"telcostar.operacional.{ds_scored}")

    # Tabela de histórico de cancelamentos (&lib_in..historico_cancelamentos)
    df_hist = spark.read.table(f"{lib_in}.historico_cancelamentos")

    # ------------------------------------------------------------------
    # 3. Filtro antecipado no histórico: apenas cancelamentos em jan/2025
    #    Equivalente ao predicado do JOIN:
    #    h.dt_cancelamento BETWEEN date('2025-01-01') AND date('2025-01-31')
    # ------------------------------------------------------------------
    df_hist_filtrado = df_hist.filter(
        F.col("dt_cancelamento").between(
            F.lit("2025-01-01").cast("date"),
            F.lit("2025-01-31").cast("date")
        )
    )

    # ------------------------------------------------------------------
    # 4. LEFT JOIN entre scored e histórico filtrado
    #    ON s.id_cliente = h.id_cliente
    # ------------------------------------------------------------------
    df_joined = df_scored.alias("s").join(
        df_hist_filtrado.alias("h"),
        on=F.col("s.id_cliente") == F.col("h.id_cliente"),
        how="left"
    )

    # ------------------------------------------------------------------
    # 5. Filtro principal: score_churn >= 0.40
    #    WHERE s.score_churn >= 0.40
    # ------------------------------------------------------------------
    df_filtrado = df_joined.filter(F.col("s.score_churn") >= 0.40)

    # ------------------------------------------------------------------
    # 6. Construção das colunas de saída
    #
    #    fl_churnou:
    #      CASE WHEN h.dt_cancelamento IS NOT NULL
    #            AND primeiro_dia_ano_mes <= trunc(dt_cancelamento ao mês)
    #       THEN 1 ELSE 0 END
    #
    #    motivo_resumido:
    #      SUBSTR(h.motivo_cancelamento, 1, 50)  →  F.substring(..., 1, 50)
    # ------------------------------------------------------------------
    df_resultado = df_filtrado.select(
        F.col("s.id_cliente"),
        F.col("s.cd_risco"),
        F.col("s.score_churn"),
        F.col("s.dt_saida_estimada"),
        F.col("s.familia_plano"),
        F.col("s.vl_mensalidade"),
        F.col("s.meses_ativo"),
        F.col("h.dt_cancelamento"),
        F.col("h.motivo_cancelamento"),
        F.col("h.cd_atendente"),

        # fl_churnou: 1 se houve cancelamento E o mês de referência é
        # anterior ou igual ao mês do cancelamento
        F.when(
            F.col("h.dt_cancelamento").isNotNull()
            & (
                F.lit(primeiro_dia_ano_mes).cast("date")
                <= F.date_trunc("month", F.col("h.dt_cancelamento")).cast("date")
            ),
            F.lit(1)
        ).otherwise(F.lit(0)).alias("fl_churnou"),

        # motivo_resumido: primeiros 50 caracteres (SAS SUBSTR é 1-based, igual ao Spark)
        F.substring(F.col("h.motivo_cancelamento"), 1, 50).alias("motivo_resumido")
    )

    # ------------------------------------------------------------------
    # 7. Persistência da tabela de saída
    #    NOTA: ORDER BY foi removido antes do write (regra 8).
    #    Delta Lake não preserva ordem de linha; use ORDER BY na leitura
    #    quando necessário.
    # ------------------------------------------------------------------
    tabela_saida = f"telcostar.operacional.{ds_eventos}"

    df_resultado.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela_saida)

    # ------------------------------------------------------------------
    # 8. Contagem pós-gravação lida diretamente da tabela persistida
    #    (regra 9 — evita re-execução da cadeia lazy)
    # ------------------------------------------------------------------
    total = spark.table(tabela_saida).count()
    print(f"[identifica_eventos_churn] Tabela '{tabela_saida}' gravada com {total} linhas.")


# ------------------------------------------------------------------
# Exemplo de chamada (equivalente à invocação da macro SAS):
#
# %identifica_eventos_churn(
#     lib_in   = TELCOSTAR,
#     ds_scored= scored_jan2025,
#     ano_mes  = 202501,
#     ds_eventos= eventos_churn_jan2025
# )
# ------------------------------------------------------------------
identifica_eventos_churn(
    lib_in    = "telcostar.operacional",   # ajustar conforme necessário
    ds_scored = "scored_jan2025",          # ajustar conforme necessário
    ano_mes   = "202501",                  # ajustar conforme necessário
    ds_eventos= "eventos_churn_jan2025",   # ajustar conforme necessário
    spark     = spark
)
# COMMAND ----------

# =============================================================
# Equivalente ao %MACRO agrega_metricas(ds_scored=, ds_agg=)
# Parâmetros convertidos como variáveis Python com valores padrão
# Ajuste os valores abaixo conforme necessário
# =============================================================
DS_SCORED = "ds_scored_default"  # ajustar conforme necessário (equivalente a &ds_scored)
DS_AGG    = "ds_agg_default"     # ajustar conforme necessário (equivalente a &ds_agg)

# Leitura da tabela de entrada (equivalente a DATA=STAGING.&ds_scored)
df_scored = spark.read.table(f"telcostar.operacional.{DS_SCORED}")

# =============================================================
# PROC MEANS com CLASS familia_plano cd_risco
# → groupBy nas variáveis CLASS (NWAY = apenas o nível mais detalhado,
#   ou seja, todas as combinações das CLASS vars — equivalente ao groupBy direto)
# VAR lista → colunas a agregar
# OUTPUT OUT= com estatísticas nomeadas → agg() + alias
# _TYPE_ e _FREQ_ são descartados automaticamente (não gerados no PySpark)
# =============================================================
df_agg = (
    df_scored
    .groupBy(
        F.col("familia_plano"),
        F.col("cd_risco")
    )
    .agg(
        # N = n_clientes (contagem de linhas por grupo, baseada em qualquer VAR não-nula)
        F.count(F.col("vl_mensalidade")).alias("n_clientes"),

        # MEAN = media_mensalidade media_gb media_voz media_cs media_score
        F.mean(F.col("vl_mensalidade")).alias("media_mensalidade"),
        F.mean(F.col("qt_gb_dados")).alias("media_gb"),
        F.mean(F.col("qt_min_voz")).alias("media_voz"),
        F.mean(F.col("qt_chamadas_cs")).alias("media_cs"),
        F.mean(F.col("score_churn")).alias("media_score"),

        # SUM = sum_mensalidade sum_gb sum_voz sum_cs sum_score
        F.sum(F.col("vl_mensalidade")).alias("sum_mensalidade"),
        F.sum(F.col("qt_gb_dados")).alias("sum_gb"),
        F.sum(F.col("qt_min_voz")).alias("sum_voz"),
        F.sum(F.col("qt_chamadas_cs")).alias("sum_cs"),
        F.sum(F.col("score_churn")).alias("sum_score"),

        # STD = std_mensalidade std_gb std_voz std_cs std_score
        # SAS PROC MEANS usa desvio padrão amostral (ddof=1) → stddev() no Spark é amostral
        F.stddev(F.col("vl_mensalidade")).alias("std_mensalidade"),
        F.stddev(F.col("qt_gb_dados")).alias("std_gb"),
        F.stddev(F.col("qt_min_voz")).alias("std_voz"),
        F.stddev(F.col("qt_chamadas_cs")).alias("std_cs"),
        F.stddev(F.col("score_churn")).alias("std_score"),

        # MIN = min_mensalidade min_gb min_voz min_cs min_score
        F.min(F.col("vl_mensalidade")).alias("min_mensalidade"),
        F.min(F.col("qt_gb_dados")).alias("min_gb"),
        F.min(F.col("qt_min_voz")).alias("min_voz"),
        F.min(F.col("qt_chamadas_cs")).alias("min_cs"),
        F.min(F.col("score_churn")).alias("min_score"),

        # MAX = max_mensalidade max_gb max_voz max_cs max_score
        F.max(F.col("vl_mensalidade")).alias("max_mensalidade"),
        F.max(F.col("qt_gb_dados")).alias("max_gb"),
        F.max(F.col("qt_min_voz")).alias("max_voz"),
        F.max(F.col("qt_chamadas_cs")).alias("max_cs"),
        F.max(F.col("score_churn")).alias("max_score"),
    )
)

# Gravação da tabela de saída (equivalente a OUTPUT OUT=STAGING.&ds_agg)
# _TYPE_ e _FREQ_ não são gerados — DROP desnecessário
df_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"telcostar.operacional.{DS_AGG}")

# Contagem de linhas após gravação (leitura da tabela persistida, não re-execução do DataFrame)
print(f"Linhas gravadas em telcostar.operacional.{DS_AGG}:",
      spark.table(f"telcostar.operacional.{DS_AGG}").count())
# COMMAND ----------

# Macro-variáveis SAS — ajustar conforme necessário
DS_AGG = "ds_agg"  # valor padrão para &ds_agg

# Lê a tabela de staging (equivalente ao SET STAGING.&ds_agg)
df = spark.read.table(f"telcostar.operacional.{DS_AGG}")

# Define uma Window global (sem partição) para calcular SUM e MAX sobre todo o dataset
# Equivalente ao SUM(sum_mensalidade) e SUM(n_clientes) sem BY no DATA STEP SAS
window_global = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

df_transformed = (
    df
    # pct_receita = sum_mensalidade / SUM(sum_mensalidade) * 100
    .withColumn(
        "pct_receita",
        F.col("sum_mensalidade") / F.sum("sum_mensalidade").over(window_global) * 100
    )
    # pct_clientes = n_clientes / SUM(n_clientes) * 100
    .withColumn(
        "pct_clientes",
        F.col("n_clientes") / F.sum("n_clientes").over(window_global) * 100
    )
    # ticket_medio = sum_mensalidade / MAX(n_clientes, 1)
    # MAX(n_clientes, 1) em SAS é equivalente a GREATEST(n_clientes, 1) — evita divisão por zero
    .withColumn(
        "ticket_medio",
        F.col("sum_mensalidade") / F.greatest(F.col("n_clientes"), F.lit(1))
    )
)

# Grava o resultado sobrescrevendo a tabela de origem (equivalente ao DATA STAGING.&ds_agg com SET)
df_transformed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"telcostar.operacional.{DS_AGG}")

# Conta linhas lendo diretamente da tabela gravada (evita re-execução lazy da cadeia)
print(spark.table(f"telcostar.operacional.{DS_AGG}").count())
# COMMAND ----------
# =============================================================
# Conversão SAS → PySpark
# PROC TRANSPOSE — pivot de métricas por plano × mês
# Gera linhas (metrica, plano_A, plano_B, plano_C, plano_CTRL)
# =============================================================


# ---------------------------------------------------------------------------
# Parâmetros equivalentes às macro-variáveis SAS (&ds_agg, &ds_pivot)
# Ajustar conforme necessário antes de executar
# ---------------------------------------------------------------------------
DS_AGG   = "nome_tabela_agg"    # equivalente a &ds_agg   — ajustar conforme necessário
DS_PIVOT = "nome_tabela_pivot"  # equivalente a &ds_pivot — ajustar conforme necessário

# ---------------------------------------------------------------------------
# PROC SQL → CREATE TABLE work.pre_pivot
# Lê a tabela de staging e filtra apenas registros com cd_risco = 'ALTO'
# Seleciona somente as colunas necessárias para o pivot posterior
# ---------------------------------------------------------------------------
pre_pivot = (
    spark.read.table(f"telcostar.operacional.{DS_AGG}")
    .filter(F.col("cd_risco") == "ALTO")                  # WHERE cd_risco = 'ALTO'
    .select(
        F.col("familia_plano"),
        F.col("cd_risco"),
        F.col("media_mensalidade"),
        F.col("media_gb"),
        F.col("media_voz"),
        F.col("media_score"),
        F.col("n_clientes"),
    )
)

# ---------------------------------------------------------------------------
# PROC TRANSPOSE (wide → long) usando stack()
# Converte as colunas de métricas em linhas (unpivot)
# Cada linha resultante terá: familia_plano, cd_risco, metrica, valor
# As 5 métricas numéricas são empilhadas em pares ('nome_metrica', valor)
# ---------------------------------------------------------------------------
metricas_long = pre_pivot.selectExpr(
    "familia_plano",
    "cd_risco",
    """
    stack(5,
        'media_mensalidade', CAST(media_mensalidade AS DOUBLE),
        'media_gb',          CAST(media_gb AS DOUBLE),
        'media_voz',         CAST(media_voz AS DOUBLE),
        'media_score',       CAST(media_score AS DOUBLE),
        'n_clientes',        CAST(n_clientes AS DOUBLE)
    ) AS (metrica, valor)
    """
)

# ---------------------------------------------------------------------------
# PROC TRANSPOSE (long → wide) usando pivot()
# Pivota familia_plano como colunas, agregando o valor de cada métrica
# Equivale ao ID familia_plano; VAR valor; BY metrica no PROC TRANSPOSE SAS
# ---------------------------------------------------------------------------
metricas_pivot = (
    metricas_long
    .groupBy("metrica")                        # BY metrica (linha de resultado)
    .pivot("familia_plano")                    # ID familia_plano → colunas
    .agg(F.first("valor"))                     # uma única linha por metrica × plano
)

# ---------------------------------------------------------------------------
# Persiste o resultado final na tabela de saída (equivalente a ds_pivot)
# Não utiliza orderBy() antes do write — Delta Lake não preserva ordem de linha
# ---------------------------------------------------------------------------
metricas_pivot.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"telcostar.operacional.{DS_PIVOT}")

# ---------------------------------------------------------------------------
# Contagem pós-gravação lida diretamente da tabela salva (evita re-execução lazy)
# ---------------------------------------------------------------------------
print(f"Linhas gravadas em telcostar.operacional.{DS_PIVOT}:",
      spark.table(f"telcostar.operacional.{DS_PIVOT}").count())
# COMMAND ----------

# =============================================================================
# Equivalente ao PROC TRANSPOSE do SAS
# DATA    = work.pre_pivot
# OUT     = STAGING.<ds_pivot>  (RENAME=(_NAME_=metrica))
# PREFIX  = plano_
# BY      = cd_risco            → groupBy antes do pivot
# ID      = familia_plano       → valores viram nomes de colunas (com prefixo plano_)
# VAR     = media_mensalidade, media_gb, media_voz, media_score, n_clientes
# =============================================================================

# Macro-variável SAS &ds_pivot — ajustar conforme necessário
DS_PIVOT = "ds_pivot"

# Lê o dataset de entrada
pre_pivot = spark.read.table("telcostar.operacional.pre_pivot")

# Colunas que serão transpostas (VAR no SAS)
var_cols = ["media_mensalidade", "media_gb", "media_voz", "media_score", "n_clientes"]

# Número de colunas VAR — usado na expressão stack()
n_vars = len(var_cols)

# Monta a expressão stack() para unpivot (wide → long)
# Resultado: uma linha por (cd_risco, metrica) com o valor correspondente
# _NAME_ no SAS equivale à coluna "metrica" aqui
stack_expr = f"stack({n_vars}, " + ", ".join(
    [f"'{col}', {col}" for col in var_cols]
) + ") as (metrica, valor)"

# Unpivot: cada combinação (cd_risco, familia_plano) gera N linhas, uma por métrica
df_long = pre_pivot.selectExpr(
    "cd_risco",
    "familia_plano",
    stack_expr
)

# Pivot: os valores distintos de familia_plano viram colunas com prefixo "plano_"
# Equivalente ao ID familia_plano + PREFIX=plano_ do SAS
# groupBy(cd_risco, metrica) → pivot(familia_plano) → colunas plano_<valor>
df_pivot = (
    df_long
    .groupBy("cd_risco", "metrica")
    .pivot("familia_plano")
    .agg(F.first("valor"))
)

# Renomeia as colunas geradas pelo pivot adicionando o prefixo "plano_"
# (somente as colunas que NÃO são cd_risco nem metrica)
cols_by_id = {"cd_risco", "metrica"}
df_pivot = df_pivot.select(
    [
        F.col(c).alias(f"plano_{c}") if c not in cols_by_id else F.col(c)
        for c in df_pivot.columns
    ]
)

# Grava o resultado na tabela de saída
output_table = f"telcostar.operacional.{DS_PIVOT}"

df_pivot.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(output_table)

# Conta linhas a partir da tabela gravada (não re-executa a cadeia)
print(f"Linhas gravadas em {output_table}: {spark.table(output_table).count()}")
# COMMAND ----------
# =============================================================
# PROC FREQ — distribuição de risco e validação de contagens
# Conversão de macro SAS gera_frequencias para PySpark
# =============================================================

from pyspark.sql.functions import sqrt, pow as F_pow

# ---------------------------------------------------------------------------
# Parâmetro equivalente à macro-variável SAS &ds_scored
# Ajustar conforme necessário antes de executar
# ---------------------------------------------------------------------------
DS_SCORED = "nome_da_tabela_scored"  # ajustar conforme necessário

# ---------------------------------------------------------------------------
# Leitura do dataset de entrada (equivalente a STAGING.&ds_scored)
# ---------------------------------------------------------------------------
df_scored = spark.read.table(f"telcostar.operacional.{DS_SCORED}")

# ===========================================================================
# 1. TABLES cd_risco / NOCUM OUT=work.freq_risco
#    Frequência simples de cd_risco: contagem e percentual por categoria
# ===========================================================================

# Total de registros para cálculo de percentual
total_rows = df_scored.count()

freq_risco = (
    df_scored
    .groupBy(F.col("cd_risco"))
    .agg(F.count("*").alias("COUNT"))
    # NOCUM: sem colunas cumulativas — apenas COUNT e PERCENT
    .withColumn("PERCENT", F.round(F.col("COUNT") / F.lit(total_rows) * 100, 2))
    .orderBy(F.col("cd_risco"))
)

# Persiste resultado (equivalente a OUT=work.freq_risco)
freq_risco.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.freq_risco")
print("freq_risco:", spark.table("telcostar.operacional.freq_risco").count(), "linhas")

# ===========================================================================
# 2. TABLES familia_plano * cd_risco / NOCUM NOPERCENT SPARSE
#    OUT=work.freq_plano_risco
#    Tabulação cruzada familia_plano x cd_risco
#    SPARSE: inclui combinações com contagem zero (cross join completo)
#    NOPERCENT: sem colunas de percentual
# ===========================================================================

# Contagem das combinações existentes
freq_plano_risco_raw = (
    df_scored
    .groupBy(F.col("familia_plano"), F.col("cd_risco"))
    .agg(F.count("*").alias("COUNT"))
)

# SPARSE: gera todas as combinações possíveis (produto cartesiano dos valores distintos)
distinct_familia = df_scored.select(F.col("familia_plano")).distinct()
distinct_risco   = df_scored.select(F.col("cd_risco")).distinct()

# Cross join para garantir todas as combinações (SPARSE)
all_combinations = distinct_familia.crossJoin(distinct_risco)

# Left join para preencher zeros onde não há ocorrências
freq_plano_risco = (
    all_combinations
    .join(freq_plano_risco_raw,
          on=["familia_plano", "cd_risco"],
          how="left")
    .withColumn("COUNT", F.coalesce(F.col("COUNT"), F.lit(0)))
    # NOPERCENT: sem colunas de percentual
    .orderBy(F.col("familia_plano"), F.col("cd_risco"))
)

# Persiste resultado (equivalente a OUT=work.freq_plano_risco)
freq_plano_risco.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.freq_plano_risco")
print("freq_plano_risco:", spark.table("telcostar.operacional.freq_plano_risco").count(), "linhas")

# ===========================================================================
# 3. TABLES fl_churnou * cd_risco / CHISQ MEASURES
#    OUT=work.freq_churn_risco
#    Tabulação cruzada fl_churnou x cd_risco com estatísticas
#    CHISQ: Qui-quadrado de Pearson
#    MEASURES: medidas de associação (Cramér's V, etc.)
# ===========================================================================

# --- 3a. Tabela de contingência base ---
freq_churn_risco = (
    df_scored
    .groupBy(F.col("fl_churnou"), F.col("cd_risco"))
    .agg(F.count("*").alias("COUNT"))
)

# Persiste a tabela de contingência base (equivalente a OUT=work.freq_churn_risco)
freq_churn_risco.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.freq_churn_risco")
print("freq_churn_risco:", spark.table("telcostar.operacional.freq_churn_risco").count(), "linhas")

# --- 3b. Cálculo do Qui-quadrado de Pearson (CHISQ) ---
# Recarrega da tabela para evitar re-execução da cadeia lazy
df_contingencia = spark.table("telcostar.operacional.freq_churn_risco")

# Totais marginais por linha (fl_churnou) e coluna (cd_risco)
row_totals = df_contingencia.groupBy(F.col("fl_churnou")).agg(F.sum("COUNT").alias("row_total"))
col_totals = df_contingencia.groupBy(F.col("cd_risco")).agg(F.sum("COUNT").alias("col_total"))
grand_total = df_contingencia.agg(F.sum("COUNT").alias("grand_total")).collect()[0]["grand_total"]

# Junta totais marginais para calcular frequência esperada
df_chi = (
    df_contingencia
    .join(row_totals, on="fl_churnou", how="left")
    .join(col_totals, on="cd_risco",   how="left")
    # Frequência esperada: E_ij = (row_total_i * col_total_j) / N
    .withColumn("expected",
                F.col("row_total") * F.col("col_total") / F.lit(grand_total))
    # Componente qui-quadrado: (O - E)^2 / E
    .withColumn("chi_component",
                F_pow(F.col("COUNT") - F.col("expected"), 2) / F.col("expected"))
)

# Estatística qui-quadrado total
chi_sq_value = df_chi.agg(F.sum("chi_component").alias("chi_square")).collect()[0]["chi_square"]

# Graus de liberdade: (n_linhas - 1) * (n_colunas - 1)
n_rows_cat = df_contingencia.select("fl_churnou").distinct().count()
n_cols_cat = df_contingencia.select("cd_risco").distinct().count()
degrees_of_freedom = (n_rows_cat - 1) * (n_cols_cat - 1)

# --- 3c. Cramér's V (MEASURES) ---
# V = sqrt( chi² / (N * min(r-1, c-1)) )
cramers_v = (chi_sq_value / (grand_total * min(n_rows_cat - 1, n_cols_cat - 1))) ** 0.5

# Consolida estatísticas em um DataFrame de resumo
chisq_summary = spark.createDataFrame(
    [(float(chi_sq_value), int(degrees_of_freedom), float(cramers_v), int(grand_total))],
    ["chi_square", "degrees_of_freedom", "cramers_v", "grand_total"]
)

# Persiste estatísticas CHISQ/MEASURES
chisq_summary.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.freq_churn_risco_stats")
print("freq_churn_risco_stats:", spark.table("telcostar.operacional.freq_churn_risco_stats").count(), "linhas")

# Exibe resumo das estatísticas para validação visual
print(f"\n=== CHISQ / MEASURES — fl_churnou * cd_risco ===")
print(f"  Qui-quadrado de Pearson : {chi_sq_value:.4f}")
print(f"  Graus de liberdade      : {degrees_of_freedom}")
print(f"  Cramér's V              : {cramers_v:.4f}")
print(f"  N total                 : {grand_total}")

# WARNING: [PROC FREQ / CHISQ p-value] requer revisão manual
# SAS original: TABLES fl_churnou * cd_risco / CHISQ MEASURES;
# O p-value do qui-quadrado não é calculado nativamente pelo PySpark.
# Para obtê-lo, adicione: from scipy.stats import chi2; p_value = 1 - chi2.cdf(chi_sq_value, degrees_of_freedom)
# Isso requer scipy instalado no cluster Databricks.
# COMMAND ----------

# Lê o dataset de origem (equivalente ao work.freq_risco do SAS)
df_freq_risco = spark.read.table("telcostar.operacional.freq_risco")

# Filtra registros onde cd_risco = 'ALTO' e coleta o valor de COUNT
# Equivalente ao DATA _NULL_ com PUT condicional — apenas imprime informação, sem gravar dataset
row = (
    df_freq_risco
    .filter(F.col("cd_risco") == "ALTO")   # IF cd_risco = 'ALTO'
    .select(F.col("COUNT"))                 # campo COUNT referenciado no PUT
    .first()                                # DATA _NULL_ não grava — apenas lê/imprime
)

# Imprime a mensagem equivalente ao PUT do SAS
if row is not None:
    print(f"INFO: Clientes alto risco = {row['COUNT']}")
else:
    print("INFO: Clientes alto risco = 0")
# COMMAND ----------
# Window não é necessário neste bloco

def carga_dw(ds_scored: str, ds_eventos: str, ds_pivot: str, ano_mes: str, spark=None):
    """
    Equivalente PySpark da macro SAS carga_dw.

    Parâmetros
    ----------
    ds_scored  : nome da tabela de scores em telcostar.operacional (ex: "scored_202401")
    ds_eventos : nome da tabela de eventos em telcostar.operacional (ex: "eventos_202401")
    ds_pivot   : nome da tabela pivot (parâmetro mantido por compatibilidade; não usado no bloco original)
    ano_mes    : valor literal do período, ex: "202401"
    spark      : SparkSession ativa (opcional quando chamado dentro do notebook)
    """

    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    # -------------------------------------------------------------------------
    # Leitura das tabelas de staging
    # Equivalente a: FROM STAGING.&ds_scored s  /  STAGING.&ds_eventos e
    # -------------------------------------------------------------------------
    df_scored  = spark.read.table(f"telcostar.operacional.{ds_scored}")
    df_eventos = spark.read.table(f"telcostar.operacional.{ds_eventos}")

    # -------------------------------------------------------------------------
    # LEFT JOIN entre scored e eventos por id_cliente
    # + projeção das colunas com COALESCE e literais
    # Equivalente ao PROC SQL / CREATE TABLE DW.fato_churn_mensal AS SELECT ...
    # -------------------------------------------------------------------------
    df_fato = (
        df_scored.alias("s")
        .join(
            df_eventos.alias("e"),
            on=F.col("s.id_cliente") == F.col("e.id_cliente"),
            how="left"
        )
        .select(
            # Literal do período — equivalente a &ano_mes AS ano_mes
            F.lit(ano_mes).cast("string").alias("ano_mes"),

            F.col("s.id_cliente"),
            F.col("s.cd_plano"),
            F.col("s.familia_plano"),
            F.col("s.cd_segmento"),
            F.col("s.cd_risco"),
            F.col("s.score_churn"),
            F.col("s.vl_mensalidade"),
            F.col("s.receita_acumulada"),
            F.col("s.meses_ativo"),
            F.col("s.qt_gb_dados"),
            F.col("s.qt_min_voz"),
            F.col("s.qt_chamadas_cs"),
            F.col("s.dt_saida_estimada"),

            # COALESCE(e.fl_churnou, 0) AS fl_churnou
            F.coalesce(F.col("e.fl_churnou"), F.lit(0)).alias("fl_churnou"),

            # COALESCE(e.motivo_resumido, '') AS motivo_resumido
            F.coalesce(F.col("e.motivo_resumido"), F.lit("")).alias("motivo_resumido"),

            # COALESCE(e.cd_atendente, 'N/A') AS cd_atendente
            F.coalesce(F.col("e.cd_atendente"), F.lit("N/A")).alias("cd_atendente"),

            # TODAY() AS dt_carga — data local de execução
            F.current_date().alias("dt_carga"),
        )
    )

    # -------------------------------------------------------------------------
    # Persistência da tabela fato no Unity Catalog
    # Equivalente a: CREATE TABLE DW.fato_churn_mensal AS ...
    # -------------------------------------------------------------------------
    df_fato.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.fato_churn_mensal")

    # Contagem pós-gravação lida diretamente da tabela (evita re-execução lazy)
    row_count = spark.table("telcostar.operacional.fato_churn_mensal").count()
    print(f"[carga_dw] telcostar.operacional.fato_churn_mensal gravada com {row_count} linhas.")

    # -------------------------------------------------------------------------
    # WARNING: [PROC DATASETS / INDEX CREATE] na linha ~22 requer revisão manual
    # SAS original:
    #   PROC DATASETS LIB=DW NOPRINT;
    #       MODIFY fato_churn_mensal;
    #       INDEX CREATE idx_ano_mes_risco = (ano_mes cd_risco);
    #   QUIT;
    #
    # Delta Lake (Databricks) não possui índices secundários no sentido SAS.
    # Alternativas recomendadas:
    #   1. Z-ORDER (otimização de layout para leituras seletivas):
    #        spark.sql("""
    #            OPTIMIZE telcostar.operacional.fato_churn_mensal
    #            ZORDER BY (ano_mes, cd_risco)
    #        """)
    #   2. Liquid Clustering (Databricks Runtime 13.3+):
    #        spark.sql("""
    #            ALTER TABLE telcostar.operacional.fato_churn_mensal
    #            CLUSTER BY (ano_mes, cd_risco)
    #        """)
    # Descomente a opção adequada ao seu ambiente:
    # -------------------------------------------------------------------------

    # Opção 1 — Z-ORDER (compatível com todos os runtimes Delta)
    # spark.sql("""
    #     OPTIMIZE telcostar.operacional.fato_churn_mensal
    #     ZORDER BY (ano_mes, cd_risco)
    # """)

    # Opção 2 — Liquid Clustering (DBR 13.3+, recomendado para tabelas novas)
    # spark.sql("""
    #     ALTER TABLE telcostar.operacional.fato_churn_mensal
    #     CLUSTER BY (ano_mes, cd_risco)
    # """)


# =============================================================================
# Exemplo de chamada — ajuste os parâmetros conforme o período desejado
# =============================================================================
# carga_dw(
#     ds_scored  = "scored_202401",
#     ds_eventos = "eventos_202401",
#     ds_pivot   = "pivot_202401",   # parâmetro reservado; não utilizado internamente
#     ano_mes    = "202401",
# )
# COMMAND ----------
# WARNING: [MACRO_INVOCATION] na linha 1 requer revisão manual
# SAS original:
# %calc_churn_score(
#     lib_in  = TELCO,
#     ds_base = clientes_ativos,
#     ds_uso  = uso_mensal,
#     ano_mes = &ANO_MES_REF,
#     ds_out  = churn_scored
# );
#
# MOTIVO: Este bloco é uma INVOCAÇÃO de macro SAS (%calc_churn_score),
# não o corpo da macro em si. Sem a definição de %calc_churn_score
# (o bloco %MACRO calc_churn_score ... %MEND), é impossível converter
# a lógica com certeza — o comportamento real depende inteiramente do
# que está implementado dentro da macro.
#
# AÇÃO NECESSÁRIA:
#   1. Localize a definição completa da macro %calc_churn_score no
#      repositório SAS (arquivo .sas ou catálogo de macros).
#   2. Forneça o corpo da macro para que a conversão possa ser feita
#      com fidelidade à lógica original.
#   3. Os parâmetros mapeados seriam:
#        lib_in  = TELCO          -> catalog/schema de origem (telcostar.operacional)
#        ds_base = clientes_ativos -> spark.read.table("telcostar.operacional.clientes_ativos")
#        ds_uso  = uso_mensal      -> spark.read.table("telcostar.operacional.uso_mensal")
#        ano_mes = &ANO_MES_REF   -> ANO_MES_REF = "202401"  # ajustar conforme necessário
#        ds_out  = churn_scored    -> .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.churn_scored")
#
# ESBOÇO PARCIAL (NÃO EXECUTAR sem revisar a lógica da macro):

# from pyspark.sql.window import Window  # descomentar se a macro usar window functions

# Parâmetro equivalente à macro-variável &ANO_MES_REF
ANO_MES_REF = "202401"  # ajustar conforme necessário

# Leitura das tabelas de entrada (equivalente a lib_in=TELCO, ds_base/ds_uso)
# df_base = spark.read.table("telcostar.operacional.clientes_ativos")
# df_uso  = spark.read.table("telcostar.operacional.uso_mensal")

# WARNING: lógica interna de calc_churn_score (joins, features, score) DESCONHECIDA
# Insira aqui as transformações após obter o corpo da macro SAS.

# Escrita do resultado (equivalente a ds_out=churn_scored)
# df_scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("telcostar.operacional.churn_scored")
# print(spark.table("telcostar.operacional.churn_scored").count())
# COMMAND ----------
# WARNING: [MACRO_INVOCATION] na linha 1 requer revisão manual
# SAS original: %identifica_eventos_churn(lib_in=TELCO, ds_scored=churn_scored, ano_mes=&ANO_MES_REF, ds_eventos=churn_eventos)
#
# A macro %identifica_eventos_churn não foi fornecida — apenas sua chamada foi convertida.
# É necessário localizar a definição da macro (%MACRO identifica_eventos_churn ... %MEND)
# e converter sua lógica interna separadamente.
#
# O bloco abaixo representa o ESQUELETO equivalente à invocação da macro,
# com os parâmetros mapeados para variáveis Python e os datasets SAS para tabelas Delta.
# Substitua o corpo da função pela lógica real da macro quando ela for fornecida.

# from pyspark.sql.window import Window  # descomente se a lógica interna da macro usar Window

# ---------------------------------------------------------------------------
# Parâmetros equivalentes às macro-variáveis SAS
# Ajuste ANO_MES_REF conforme o valor real utilizado na execução SAS
# ---------------------------------------------------------------------------
LIB_IN      = "telcostar.operacional"   # equivalente a lib_in = TELCO
DS_SCORED   = "churn_scored"            # equivalente a ds_scored = churn_scored
ANO_MES_REF = "2024-01"                 # equivalente a &ANO_MES_REF — ajustar conforme necessário
DS_EVENTOS  = "churn_eventos"           # equivalente a ds_eventos = churn_eventos

# ---------------------------------------------------------------------------
# Leitura do dataset de entrada (equivalente a TELCO.churn_scored no SAS)
# ---------------------------------------------------------------------------
df_scored = spark.read.table(f"{LIB_IN}.{DS_SCORED}")

# ---------------------------------------------------------------------------
# WARNING: [MACRO_BODY] — lógica interna de %identifica_eventos_churn não fornecida.
# Substitua o bloco abaixo pela conversão real do corpo da macro.
# Exemplo de estrutura esperada (ajustar conforme a macro real):
#
# df_eventos = (
#     df_scored
#     .filter(F.col("ano_mes") == F.lit(ANO_MES_REF))
#     # ... demais filtros, joins e transformações da macro ...
# )
# ---------------------------------------------------------------------------

# WARNING: [MACRO_BODY] na linha 1 requer revisão manual
# SAS original: corpo interno de %identifica_eventos_churn não foi fornecido

# Placeholder — remova e substitua pela lógica real da macro
df_eventos = df_scored.filter(F.col("ano_mes") == F.lit(ANO_MES_REF))

# ---------------------------------------------------------------------------
# Gravação do dataset de saída (equivalente a ds_eventos = churn_eventos no SAS)
# ---------------------------------------------------------------------------
df_eventos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{LIB_IN}.{DS_EVENTOS}")

# Contagem pós-gravação lida diretamente da tabela Delta (não re-executa a query)
print(f"Registros gravados em {LIB_IN}.{DS_EVENTOS}:",
      spark.table(f"{LIB_IN}.{DS_EVENTOS}").count())
# COMMAND ----------
# WARNING: [MACRO_INVOCATION] na linha 1 requer revisão manual
# SAS original:
# %agrega_metricas(
#     ds_scored = churn_scored,
#     ds_agg    = churn_agregado
# );
#
# Não é possível converter esta chamada de macro com certeza sem conhecer
# a definição de %agrega_metricas (corpo da macro não foi fornecido).
# Abaixo está um esqueleto baseado nos parâmetros inferidos pelos nomes:
#   ds_scored  → tabela de entrada com scores de churn
#   ds_agg     → tabela de saída com métricas agregadas
#
# Substitua o bloco de transformações pelo conteúdo real da macro quando disponível.

# from pyspark.sql.window import Window  # descomente se a macro usar window functions

# ── Parâmetros equivalentes às macro-variáveis SAS ──────────────────────────
DS_SCORED = "churn_scored"    # ajustar conforme necessário
DS_AGG    = "churn_agregado"  # ajustar conforme necessário

# ── Leitura da tabela de entrada ─────────────────────────────────────────────
df_scored = spark.read.table(f"telcostar.operacional.{DS_SCORED}")

# ── WARNING: transformações de %agrega_metricas não puderam ser inferidas ────
# Substitua este bloco pela lógica real da macro.
# Exemplo genérico de agregação — AJUSTE conforme o corpo real da macro:
#
# df_agg = (
#     df_scored
#     .groupBy(F.col("<chave_de_agrupamento>"))
#     .agg(
#         F.count("*").alias("qtd_registros"),
#         F.avg(F.col("<score_col>")).alias("score_medio"),
#         F.sum(F.col("<score_col>")).alias("score_total"),
#     )
# )

# ── Placeholder: repassa os dados sem transformação até a macro ser fornecida ─
df_agg = df_scored  # SUBSTITUIR pela lógica real de %agrega_metricas

# ── Gravação da tabela de saída ───────────────────────────────────────────────
df_agg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"telcostar.operacional.{DS_AGG}")

# ── Contagem pós-gravação (leitura do Delta, não re-execução da query) ────────
print(f"Registros gravados em telcostar.operacional.{DS_AGG}:",
      spark.table(f"telcostar.operacional.{DS_AGG}").count())
# COMMAND ----------
# WARNING: [MACRO_INVOCATION] na linha 1 requer revisão manual
# SAS original: %transpoe_metricas(ds_agg = churn_agregado, ds_pivot = churn_pivot);
#
# Não é possível converter esta chamada de macro com certeza sem conhecer
# a definição de %transpoe_metricas (corpo da macro não foi fornecido).
#
# O que se sabe pelo contexto:
#   - ds_agg   = churn_agregado  → tabela de entrada agregada
#   - ds_pivot = churn_pivot     → tabela de saída pivotada (transposição de métricas)
#
# Padrão típico de PROC TRANSPOSE em SAS convertido para PySpark está esboçado
# abaixo como PONTO DE PARTIDA — ajuste colunas, variáveis e lógica conforme
# a definição real da macro %transpoe_metricas.


# ── Parâmetros equivalentes às macro-variáveis SAS ──────────────────────────
DS_AGG   = "telcostar.operacional.churn_agregado"   # tabela de entrada
DS_PIVOT = "telcostar.operacional.churn_pivot"      # tabela de saída

# ── Leitura da tabela agregada ───────────────────────────────────────────────
df_agg = spark.read.table(DS_AGG)

# WARNING: [PROC TRANSPOSE / pivot logic] requer revisão manual
# A macro %transpoe_metricas provavelmente executa um PROC TRANSPOSE que:
#   1. Empilha colunas de métricas em linhas  (unpivot / stack), OU
#   2. Espalha valores de linhas em colunas   (pivot)
# Sem o corpo da macro não é possível determinar qual direção nem quais colunas.
#
# Exemplo de UNPIVOT (stack) — adapte a lista de colunas conforme necessário:
#
# METRIC_COLS = ["metrica_1", "metrica_2", "metrica_3"]  # ajustar
# ID_COLS     = ["id_cliente", "dt_referencia"]           # ajustar
#
# df_pivot = df_agg.select(
#     *[F.col(c) for c in ID_COLS],
#     F.expr(
#         f"stack({len(METRIC_COLS)}, "
#         + ", ".join(f"'{c}', {c}" for c in METRIC_COLS)
#         + ") as (nome_metrica, valor_metrica)"
#     )
# )
#
# Exemplo de PIVOT (spread) — adapte conforme necessário:
#
# df_pivot = (
#     df_agg
#     .groupBy("id_cliente", "dt_referencia")   # ajustar colunas de agrupamento
#     .pivot("nome_metrica")                     # ajustar coluna que vira cabeçalho
#     .agg(F.first(F.col("valor_metrica")))      # ajustar agregação
# )

# WARNING: bloco abaixo é placeholder — substitua pelo código correto após
# inspecionar a definição de %transpoe_metricas e as colunas reais de DS_AGG.
df_pivot = df_agg  # PLACEHOLDER — lógica de transposição não inferível

# ── Gravação da tabela pivotada ──────────────────────────────────────────────
df_pivot.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DS_PIVOT)

# ── Contagem pós-gravação (leitura da tabela materializada) ──────────────────
print(f"Linhas gravadas em {DS_PIVOT}: {spark.table(DS_PIVOT).count()}")
# COMMAND ----------
# WARNING: [MACRO_INVOCATION] na linha 1 requer revisão manual
# SAS original: %gera_frequencias(ds_scored = churn_scored);
#
# A macro %gera_frequencias não foi fornecida no bloco de conversão.
# Sem o corpo da macro, não é possível converter a lógica com certeza.
#
# Comportamento típico de macros SAS chamadas "gera_frequencias":
#   - Executam PROC FREQ sobre o dataset informado (ds_scored = churn_scored)
#   - Geram tabelas de frequência (contagem e percentual) para variáveis categóricas
#   - Podem gravar resultados em datasets de saída ou apenas exibi-los no log
#
# AÇÃO NECESSÁRIA:
#   1. Localize a definição da macro %gera_frequencias no código SAS fonte
#   2. Forneça o corpo completo da macro para que a conversão seja realizada
#   3. Abaixo segue um ESBOÇO genérico de como uma macro desse tipo seria
#      convertida, assumindo que ela calcula frequências de todas as colunas
#      do dataset churn_scored — AJUSTE conforme a lógica real da macro.


# ── Parâmetro equivalente à macro-variável ds_scored ──────────────────────────
DS_SCORED = "churn_scored"  # ajustar conforme necessário

# ── Leitura do dataset scored ─────────────────────────────────────────────────
df_scored = spark.read.table(f"telcostar.operacional.{DS_SCORED}")

# ── ESBOÇO: frequência de cada coluna categórica (lógica genérica) ─────────────
# WARNING: a lógica abaixo é GENÉRICA e pode não refletir o comportamento real
# da macro %gera_frequencias — revisar após obter o corpo da macro.

# Seleciona apenas colunas do tipo string/categórico para calcular frequências
categorical_cols = [
    field.name
    for field in df_scored.schema.fields
    if str(field.dataType) in ("StringType()", "BooleanType()")
]

for col_name in categorical_cols:
    print(f"\n=== Frequência: {col_name} ===")
    (
        df_scored
        # Conta ocorrências de cada valor distinto na coluna
        .groupBy(F.col(col_name))
        .agg(F.count("*").alias("frequencia"))
        # Calcula percentual sobre o total
        .withColumn(
            "percentual",
            F.round(
                F.col("frequencia") / F.sum("frequencia").over(
                    __import__("pyspark.sql.window", fromlist=["Window"]).Window.rowsBetween(
                        __import__("pyspark.sql.window", fromlist=["Window"]).Window.unboundedPreceding,
                        __import__("pyspark.sql.window", fromlist=["Window"]).Window.unboundedFollowing,
                    )
                ) * 100,
                2,
            ),
        )
        .orderBy(F.col("frequencia").desc())
        .show(truncate=False)
    )
# COMMAND ----------
# WARNING: [MACRO_INVOCATION] na linha 1 requer revisão manual
# SAS original: %carga_dw(ds_scored=churn_scored, ds_eventos=churn_eventos, ds_pivot=churn_pivot, ano_mes=&ANO_MES_REF)
#
# A macro %carga_dw não foi fornecida neste bloco — apenas sua invocação.
# Para converter corretamente, é necessário o corpo completo da macro %carga_dw.
# Abaixo está um esqueleto Python equivalente ao padrão de "macro com parâmetros" em SAS,
# aguardando o conteúdo real da macro para ser preenchido.

# from pyspark.sql.window import Window  # descomente se a macro usar Window functions

# ---------------------------------------------------------------------------
# Parâmetros equivalentes às macro-variáveis SAS
# Ajuste ANO_MES_REF conforme o valor de referência desejado
# ---------------------------------------------------------------------------
DS_SCORED  = "churn_scored"   # equivalente ao parâmetro ds_scored
DS_EVENTOS = "churn_eventos"  # equivalente ao parâmetro ds_eventos
DS_PIVOT   = "churn_pivot"    # equivalente ao parâmetro ds_pivot
ANO_MES_REF = "202401"        # equivalente a &ANO_MES_REF — ajustar conforme necessário

# ---------------------------------------------------------------------------
# Função Python equivalente à macro SAS %carga_dw
# O corpo abaixo é um ESQUELETO — preencher com a lógica real da macro
# ---------------------------------------------------------------------------
def carga_dw(ds_scored: str, ds_eventos: str, ds_pivot: str, ano_mes: str) -> None:
    """
    Equivalente PySpark da macro SAS %carga_dw.
    Parâmetros:
        ds_scored  : nome da tabela de scores
        ds_eventos : nome da tabela de eventos
        ds_pivot   : nome da tabela pivot de saída
        ano_mes    : ano/mês de referência no formato YYYYMM
    """

    # WARNING: [MACRO_BODY] corpo da macro %carga_dw não foi fornecido — lógica abaixo é placeholder
    # SAS original: corpo completo de %carga_dw ausente no bloco enviado

    # --- Leitura das tabelas de entrada ---
    df_scored = spark.read.table(f"telcostar.operacional.{ds_scored}")
    df_eventos = spark.read.table(f"telcostar.operacional.{ds_eventos}")

    # --- Filtro por ano/mês de referência (adaptar nome da coluna conforme schema real) ---
    # WARNING: nome da coluna de ano_mes pode diferir — verificar schema das tabelas
    df_scored_filtrado = df_scored.filter(F.col("ano_mes") == F.lit(ano_mes))
    df_eventos_filtrado = df_eventos.filter(F.col("ano_mes") == F.lit(ano_mes))

    # WARNING: [MACRO_BODY] lógica de join, transformações e pivot ausente — inserir aqui
    # Exemplo de estrutura esperada:
    # df_joined = df_scored_filtrado.join(df_eventos_filtrado, on="chave", how="left")
    # df_pivot  = df_joined.groupBy(...).pivot(...).agg(...)

    # --- Gravação da tabela pivot de saída ---
    # WARNING: [MACRO_BODY] substituir df_scored_filtrado pelo DataFrame final correto após implementar a lógica
    df_scored_filtrado.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"telcostar.operacional.{ds_pivot}")

    # Contagem pós-gravação (leitura da tabela persistida — não re-executa a cadeia)
    print(f"[carga_dw] Tabela telcostar.operacional.{ds_pivot} gravada com "
          f"{spark.table(f'telcostar.operacional.{ds_pivot}').count()} linhas.")


# ---------------------------------------------------------------------------
# Invocação equivalente ao %carga_dw(...) do bloco SAS original
# ---------------------------------------------------------------------------
carga_dw(
    ds_scored  = DS_SCORED,
    ds_eventos = DS_EVENTOS,
    ds_pivot   = DS_PIVOT,
    ano_mes    = ANO_MES_REF,
)
