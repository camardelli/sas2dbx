# Databricks notebook source
# MAGIC %md
# MAGIC # Reconciliação: job_complexo_analise_churn
# MAGIC
# MAGIC Notebook gerado automaticamente pelo sas2dbx para validação de equivalência.
# MAGIC
# MAGIC **Como usar:**
# MAGIC 1. Exporte a saída do job SAS para uma tabela Delta (`sas_baseline_table`)
# MAGIC 2. Execute o notebook migrado (`job_complexo_analise_churn.py`) para gerar `dbx_table`
# MAGIC 3. Execute este notebook para comparar os resultados
# MAGIC
# MAGIC **Critério de aprovação:** divergência < 0.01% em row count e somas

from pyspark.sql import functions as F

# Parâmetros — ajuste conforme seu ambiente
dbutils.widgets.text("sas_baseline_table", "", "Tabela baseline (saída SAS exportada)")
dbutils.widgets.text("dbx_table", "", "Tabela Databricks (saída do notebook migrado)")

sas_table = dbutils.widgets.get("sas_baseline_table")
dbx_table  = dbutils.widgets.get("dbx_table")

results = []  # acumula resultados para sumário final


# COMMAND ----------

# MAGIC %md ## 1. Row Count — telcostar.operacional.resultado

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {pct_diff:.4f}%"
    print(f"Row count — SAS: {sas_count:,} | DBX: {dbx_count:,} | Diff: {diff:+,} | {status}")
    results.append({"check": "row_count", "table": "telcostar.operacional.resultado", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")


# COMMAND ----------

# MAGIC %md ## 2. Somas Numéricas — telcostar.operacional.resultado

if sas_table and dbx_table:
    agg_cols = [
    F.sum(F.col("vl_mensalidade")).alias("vl_mensalidade"),
    F.sum(F.col("qt_chamadas_cs")).alias("qt_chamadas_cs"),
    F.sum(F.col("vl_excedente")).alias("vl_excedente"),
    F.sum(F.col("qt_min_voz")).alias("qt_min_voz"),
    F.sum(F.col("qt_gb_dados")).alias("qt_gb_dados"),
    F.sum(F.col("media_mensalidade")).alias("media_mensalidade"),
    F.sum(F.col("media_gb")).alias("media_gb"),
    F.sum(F.col("media_voz")).alias("media_voz"),
    F.sum(F.col("media_cs")).alias("media_cs"),
    F.sum(F.col("media_score")).alias("media_score"),
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
    sas_v = sas_agg["vl_mensalidade"] or 0
    dbx_v = dbx_agg["vl_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_mensalidade", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["qt_chamadas_cs"] or 0
    dbx_v = dbx_agg["qt_chamadas_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_chamadas_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_chamadas_cs", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["vl_excedente"] or 0
    dbx_v = dbx_agg["vl_excedente"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_excedente) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_excedente", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["qt_min_voz"] or 0
    dbx_v = dbx_agg["qt_min_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_min_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_min_voz", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["qt_gb_dados"] or 0
    dbx_v = dbx_agg["qt_gb_dados"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_gb_dados) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_gb_dados", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["media_mensalidade"] or 0
    dbx_v = dbx_agg["media_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_mensalidade", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["media_gb"] or 0
    dbx_v = dbx_agg["media_gb"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_gb) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_gb", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["media_voz"] or 0
    dbx_v = dbx_agg["media_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_voz", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["media_cs"] or 0
    dbx_v = dbx_agg["media_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_cs", "table": "telcostar.operacional.resultado", "status": st})
    sas_v = sas_agg["media_score"] or 0
    dbx_v = dbx_agg["media_score"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_score) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_score", "table": "telcostar.operacional.resultado", "status": st})
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {e}")


# COMMAND ----------

# MAGIC %md ## 3. Distinct Keys — telcostar.operacional.resultado

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        dbx_dist = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {diff:+,} chaves"
        print(f"Distinct keys {["chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente"]} — SAS: {sas_dist:,} | DBX: {dbx_dist:,} | {status}")
        results.append({"check": "distinct_keys", "table": "telcostar.operacional.resultado", "status": status})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {e}")


# COMMAND ----------

# MAGIC %md ## 4. Anti-Join (linhas divergentes) — telcostar.operacional.resultado

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("sas")
        dbx_df = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {in_sas_not_dbx:,} | Em DBX mas não SAS: {in_dbx_not_sas:,} | {status}")
        results.append({"check": "antijoin", "table": "telcostar.operacional.resultado", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {e}")


# COMMAND ----------

# MAGIC %md ## 5. Integridade de Caracteres Especiais — telcostar.operacional.resultado

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {corrupt} registro(s) com encoding corrompido"
            print(f"Charset check (col: {sample_col}) — {status}")
            results.append({"check": "charset", "table": "telcostar.operacional.resultado", "status": status})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {e}")


# COMMAND ----------

# MAGIC %md ## Sumário — telcostar.operacional.resultado

print("\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — telcostar.operacional.resultado")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {len(ok)}")
print(f"  ✗ Falhas    : {len(failed)}")
if failed:
    for r in failed:
        print(f"    - {r['check']}: {r['status']}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\nResultado final: {overall}")
print("="*60)


# COMMAND ----------

# MAGIC %md ## 1. Row Count — telcostar.operacional.<tabela>

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {pct_diff:.4f}%"
    print(f"Row count — SAS: {sas_count:,} | DBX: {dbx_count:,} | Diff: {diff:+,} | {status}")
    results.append({"check": "row_count", "table": "telcostar.operacional.<tabela>", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")


# COMMAND ----------

# MAGIC %md ## 2. Somas Numéricas — telcostar.operacional.<tabela>

if sas_table and dbx_table:
    agg_cols = [
    F.sum(F.col("vl_mensalidade")).alias("vl_mensalidade"),
    F.sum(F.col("qt_chamadas_cs")).alias("qt_chamadas_cs"),
    F.sum(F.col("vl_excedente")).alias("vl_excedente"),
    F.sum(F.col("qt_min_voz")).alias("qt_min_voz"),
    F.sum(F.col("qt_gb_dados")).alias("qt_gb_dados"),
    F.sum(F.col("media_mensalidade")).alias("media_mensalidade"),
    F.sum(F.col("media_gb")).alias("media_gb"),
    F.sum(F.col("media_voz")).alias("media_voz"),
    F.sum(F.col("media_cs")).alias("media_cs"),
    F.sum(F.col("media_score")).alias("media_score"),
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
    sas_v = sas_agg["vl_mensalidade"] or 0
    dbx_v = dbx_agg["vl_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_mensalidade", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["qt_chamadas_cs"] or 0
    dbx_v = dbx_agg["qt_chamadas_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_chamadas_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_chamadas_cs", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["vl_excedente"] or 0
    dbx_v = dbx_agg["vl_excedente"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_excedente) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_excedente", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["qt_min_voz"] or 0
    dbx_v = dbx_agg["qt_min_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_min_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_min_voz", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["qt_gb_dados"] or 0
    dbx_v = dbx_agg["qt_gb_dados"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_gb_dados) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_gb_dados", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["media_mensalidade"] or 0
    dbx_v = dbx_agg["media_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_mensalidade", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["media_gb"] or 0
    dbx_v = dbx_agg["media_gb"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_gb) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_gb", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["media_voz"] or 0
    dbx_v = dbx_agg["media_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_voz", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["media_cs"] or 0
    dbx_v = dbx_agg["media_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_cs", "table": "telcostar.operacional.<tabela>", "status": st})
    sas_v = sas_agg["media_score"] or 0
    dbx_v = dbx_agg["media_score"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_score) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_score", "table": "telcostar.operacional.<tabela>", "status": st})
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {e}")


# COMMAND ----------

# MAGIC %md ## 3. Distinct Keys — telcostar.operacional.<tabela>

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        dbx_dist = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {diff:+,} chaves"
        print(f"Distinct keys {["chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente"]} — SAS: {sas_dist:,} | DBX: {dbx_dist:,} | {status}")
        results.append({"check": "distinct_keys", "table": "telcostar.operacional.<tabela>", "status": status})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {e}")


# COMMAND ----------

# MAGIC %md ## 4. Anti-Join (linhas divergentes) — telcostar.operacional.<tabela>

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("sas")
        dbx_df = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {in_sas_not_dbx:,} | Em DBX mas não SAS: {in_dbx_not_sas:,} | {status}")
        results.append({"check": "antijoin", "table": "telcostar.operacional.<tabela>", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {e}")


# COMMAND ----------

# MAGIC %md ## 5. Integridade de Caracteres Especiais — telcostar.operacional.<tabela>

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {corrupt} registro(s) com encoding corrompido"
            print(f"Charset check (col: {sample_col}) — {status}")
            results.append({"check": "charset", "table": "telcostar.operacional.<tabela>", "status": status})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {e}")


# COMMAND ----------

# MAGIC %md ## Sumário — telcostar.operacional.<tabela>

print("\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — telcostar.operacional.<tabela>")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {len(ok)}")
print(f"  ✗ Falhas    : {len(failed)}")
if failed:
    for r in failed:
        print(f"    - {r['check']}: {r['status']}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\nResultado final: {overall}")
print("="*60)


# COMMAND ----------

# MAGIC %md ## 1. Row Count — telcostar.operacional.freq_risco

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {pct_diff:.4f}%"
    print(f"Row count — SAS: {sas_count:,} | DBX: {dbx_count:,} | Diff: {diff:+,} | {status}")
    results.append({"check": "row_count", "table": "telcostar.operacional.freq_risco", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")


# COMMAND ----------

# MAGIC %md ## 2. Somas Numéricas — telcostar.operacional.freq_risco

if sas_table and dbx_table:
    agg_cols = [
    F.sum(F.col("vl_mensalidade")).alias("vl_mensalidade"),
    F.sum(F.col("qt_chamadas_cs")).alias("qt_chamadas_cs"),
    F.sum(F.col("vl_excedente")).alias("vl_excedente"),
    F.sum(F.col("qt_min_voz")).alias("qt_min_voz"),
    F.sum(F.col("qt_gb_dados")).alias("qt_gb_dados"),
    F.sum(F.col("media_mensalidade")).alias("media_mensalidade"),
    F.sum(F.col("media_gb")).alias("media_gb"),
    F.sum(F.col("media_voz")).alias("media_voz"),
    F.sum(F.col("media_cs")).alias("media_cs"),
    F.sum(F.col("media_score")).alias("media_score"),
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
    sas_v = sas_agg["vl_mensalidade"] or 0
    dbx_v = dbx_agg["vl_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_mensalidade", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["qt_chamadas_cs"] or 0
    dbx_v = dbx_agg["qt_chamadas_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_chamadas_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_chamadas_cs", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["vl_excedente"] or 0
    dbx_v = dbx_agg["vl_excedente"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_excedente) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_excedente", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["qt_min_voz"] or 0
    dbx_v = dbx_agg["qt_min_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_min_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_min_voz", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["qt_gb_dados"] or 0
    dbx_v = dbx_agg["qt_gb_dados"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_gb_dados) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_gb_dados", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["media_mensalidade"] or 0
    dbx_v = dbx_agg["media_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_mensalidade", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["media_gb"] or 0
    dbx_v = dbx_agg["media_gb"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_gb) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_gb", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["media_voz"] or 0
    dbx_v = dbx_agg["media_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_voz", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["media_cs"] or 0
    dbx_v = dbx_agg["media_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_cs", "table": "telcostar.operacional.freq_risco", "status": st})
    sas_v = sas_agg["media_score"] or 0
    dbx_v = dbx_agg["media_score"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_score) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_score", "table": "telcostar.operacional.freq_risco", "status": st})
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {e}")


# COMMAND ----------

# MAGIC %md ## 3. Distinct Keys — telcostar.operacional.freq_risco

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        dbx_dist = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {diff:+,} chaves"
        print(f"Distinct keys {["chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente"]} — SAS: {sas_dist:,} | DBX: {dbx_dist:,} | {status}")
        results.append({"check": "distinct_keys", "table": "telcostar.operacional.freq_risco", "status": status})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {e}")


# COMMAND ----------

# MAGIC %md ## 4. Anti-Join (linhas divergentes) — telcostar.operacional.freq_risco

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("sas")
        dbx_df = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {in_sas_not_dbx:,} | Em DBX mas não SAS: {in_dbx_not_sas:,} | {status}")
        results.append({"check": "antijoin", "table": "telcostar.operacional.freq_risco", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {e}")


# COMMAND ----------

# MAGIC %md ## 5. Integridade de Caracteres Especiais — telcostar.operacional.freq_risco

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {corrupt} registro(s) com encoding corrompido"
            print(f"Charset check (col: {sample_col}) — {status}")
            results.append({"check": "charset", "table": "telcostar.operacional.freq_risco", "status": status})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {e}")


# COMMAND ----------

# MAGIC %md ## Sumário — telcostar.operacional.freq_risco

print("\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — telcostar.operacional.freq_risco")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {len(ok)}")
print(f"  ✗ Falhas    : {len(failed)}")
if failed:
    for r in failed:
        print(f"    - {r['check']}: {r['status']}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\nResultado final: {overall}")
print("="*60)


# COMMAND ----------

# MAGIC %md ## 1. Row Count — telcostar.operacional.freq_plano_risco

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {pct_diff:.4f}%"
    print(f"Row count — SAS: {sas_count:,} | DBX: {dbx_count:,} | Diff: {diff:+,} | {status}")
    results.append({"check": "row_count", "table": "telcostar.operacional.freq_plano_risco", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")


# COMMAND ----------

# MAGIC %md ## 2. Somas Numéricas — telcostar.operacional.freq_plano_risco

if sas_table and dbx_table:
    agg_cols = [
    F.sum(F.col("vl_mensalidade")).alias("vl_mensalidade"),
    F.sum(F.col("qt_chamadas_cs")).alias("qt_chamadas_cs"),
    F.sum(F.col("vl_excedente")).alias("vl_excedente"),
    F.sum(F.col("qt_min_voz")).alias("qt_min_voz"),
    F.sum(F.col("qt_gb_dados")).alias("qt_gb_dados"),
    F.sum(F.col("media_mensalidade")).alias("media_mensalidade"),
    F.sum(F.col("media_gb")).alias("media_gb"),
    F.sum(F.col("media_voz")).alias("media_voz"),
    F.sum(F.col("media_cs")).alias("media_cs"),
    F.sum(F.col("media_score")).alias("media_score"),
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
    sas_v = sas_agg["vl_mensalidade"] or 0
    dbx_v = dbx_agg["vl_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_mensalidade", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["qt_chamadas_cs"] or 0
    dbx_v = dbx_agg["qt_chamadas_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_chamadas_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_chamadas_cs", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["vl_excedente"] or 0
    dbx_v = dbx_agg["vl_excedente"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_excedente) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_excedente", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["qt_min_voz"] or 0
    dbx_v = dbx_agg["qt_min_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_min_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_min_voz", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["qt_gb_dados"] or 0
    dbx_v = dbx_agg["qt_gb_dados"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_gb_dados) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_gb_dados", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["media_mensalidade"] or 0
    dbx_v = dbx_agg["media_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_mensalidade", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["media_gb"] or 0
    dbx_v = dbx_agg["media_gb"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_gb) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_gb", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["media_voz"] or 0
    dbx_v = dbx_agg["media_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_voz", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["media_cs"] or 0
    dbx_v = dbx_agg["media_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_cs", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    sas_v = sas_agg["media_score"] or 0
    dbx_v = dbx_agg["media_score"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_score) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_score", "table": "telcostar.operacional.freq_plano_risco", "status": st})
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {e}")


# COMMAND ----------

# MAGIC %md ## 3. Distinct Keys — telcostar.operacional.freq_plano_risco

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        dbx_dist = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {diff:+,} chaves"
        print(f"Distinct keys {["chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente"]} — SAS: {sas_dist:,} | DBX: {dbx_dist:,} | {status}")
        results.append({"check": "distinct_keys", "table": "telcostar.operacional.freq_plano_risco", "status": status})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {e}")


# COMMAND ----------

# MAGIC %md ## 4. Anti-Join (linhas divergentes) — telcostar.operacional.freq_plano_risco

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("sas")
        dbx_df = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {in_sas_not_dbx:,} | Em DBX mas não SAS: {in_dbx_not_sas:,} | {status}")
        results.append({"check": "antijoin", "table": "telcostar.operacional.freq_plano_risco", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {e}")


# COMMAND ----------

# MAGIC %md ## 5. Integridade de Caracteres Especiais — telcostar.operacional.freq_plano_risco

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {corrupt} registro(s) com encoding corrompido"
            print(f"Charset check (col: {sample_col}) — {status}")
            results.append({"check": "charset", "table": "telcostar.operacional.freq_plano_risco", "status": status})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {e}")


# COMMAND ----------

# MAGIC %md ## Sumário — telcostar.operacional.freq_plano_risco

print("\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — telcostar.operacional.freq_plano_risco")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {len(ok)}")
print(f"  ✗ Falhas    : {len(failed)}")
if failed:
    for r in failed:
        print(f"    - {r['check']}: {r['status']}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\nResultado final: {overall}")
print("="*60)


# COMMAND ----------

# MAGIC %md ## 1. Row Count — telcostar.operacional.freq_churn_risco

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {pct_diff:.4f}%"
    print(f"Row count — SAS: {sas_count:,} | DBX: {dbx_count:,} | Diff: {diff:+,} | {status}")
    results.append({"check": "row_count", "table": "telcostar.operacional.freq_churn_risco", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")


# COMMAND ----------

# MAGIC %md ## 2. Somas Numéricas — telcostar.operacional.freq_churn_risco

if sas_table and dbx_table:
    agg_cols = [
    F.sum(F.col("vl_mensalidade")).alias("vl_mensalidade"),
    F.sum(F.col("qt_chamadas_cs")).alias("qt_chamadas_cs"),
    F.sum(F.col("vl_excedente")).alias("vl_excedente"),
    F.sum(F.col("qt_min_voz")).alias("qt_min_voz"),
    F.sum(F.col("qt_gb_dados")).alias("qt_gb_dados"),
    F.sum(F.col("media_mensalidade")).alias("media_mensalidade"),
    F.sum(F.col("media_gb")).alias("media_gb"),
    F.sum(F.col("media_voz")).alias("media_voz"),
    F.sum(F.col("media_cs")).alias("media_cs"),
    F.sum(F.col("media_score")).alias("media_score"),
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
    sas_v = sas_agg["vl_mensalidade"] or 0
    dbx_v = dbx_agg["vl_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_mensalidade", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["qt_chamadas_cs"] or 0
    dbx_v = dbx_agg["qt_chamadas_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_chamadas_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_chamadas_cs", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["vl_excedente"] or 0
    dbx_v = dbx_agg["vl_excedente"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_excedente) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_excedente", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["qt_min_voz"] or 0
    dbx_v = dbx_agg["qt_min_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_min_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_min_voz", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["qt_gb_dados"] or 0
    dbx_v = dbx_agg["qt_gb_dados"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_gb_dados) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_gb_dados", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["media_mensalidade"] or 0
    dbx_v = dbx_agg["media_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_mensalidade", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["media_gb"] or 0
    dbx_v = dbx_agg["media_gb"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_gb) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_gb", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["media_voz"] or 0
    dbx_v = dbx_agg["media_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_voz", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["media_cs"] or 0
    dbx_v = dbx_agg["media_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_cs", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    sas_v = sas_agg["media_score"] or 0
    dbx_v = dbx_agg["media_score"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_score) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_score", "table": "telcostar.operacional.freq_churn_risco", "status": st})
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {e}")


# COMMAND ----------

# MAGIC %md ## 3. Distinct Keys — telcostar.operacional.freq_churn_risco

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        dbx_dist = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {diff:+,} chaves"
        print(f"Distinct keys {["chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente"]} — SAS: {sas_dist:,} | DBX: {dbx_dist:,} | {status}")
        results.append({"check": "distinct_keys", "table": "telcostar.operacional.freq_churn_risco", "status": status})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {e}")


# COMMAND ----------

# MAGIC %md ## 4. Anti-Join (linhas divergentes) — telcostar.operacional.freq_churn_risco

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("sas")
        dbx_df = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {in_sas_not_dbx:,} | Em DBX mas não SAS: {in_dbx_not_sas:,} | {status}")
        results.append({"check": "antijoin", "table": "telcostar.operacional.freq_churn_risco", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {e}")


# COMMAND ----------

# MAGIC %md ## 5. Integridade de Caracteres Especiais — telcostar.operacional.freq_churn_risco

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {corrupt} registro(s) com encoding corrompido"
            print(f"Charset check (col: {sample_col}) — {status}")
            results.append({"check": "charset", "table": "telcostar.operacional.freq_churn_risco", "status": status})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {e}")


# COMMAND ----------

# MAGIC %md ## Sumário — telcostar.operacional.freq_churn_risco

print("\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — telcostar.operacional.freq_churn_risco")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {len(ok)}")
print(f"  ✗ Falhas    : {len(failed)}")
if failed:
    for r in failed:
        print(f"    - {r['check']}: {r['status']}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\nResultado final: {overall}")
print("="*60)


# COMMAND ----------

# MAGIC %md ## 1. Row Count — telcostar.operacional.freq_churn_risco_stats

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {pct_diff:.4f}%"
    print(f"Row count — SAS: {sas_count:,} | DBX: {dbx_count:,} | Diff: {diff:+,} | {status}")
    results.append({"check": "row_count", "table": "telcostar.operacional.freq_churn_risco_stats", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")


# COMMAND ----------

# MAGIC %md ## 2. Somas Numéricas — telcostar.operacional.freq_churn_risco_stats

if sas_table and dbx_table:
    agg_cols = [
    F.sum(F.col("vl_mensalidade")).alias("vl_mensalidade"),
    F.sum(F.col("qt_chamadas_cs")).alias("qt_chamadas_cs"),
    F.sum(F.col("vl_excedente")).alias("vl_excedente"),
    F.sum(F.col("qt_min_voz")).alias("qt_min_voz"),
    F.sum(F.col("qt_gb_dados")).alias("qt_gb_dados"),
    F.sum(F.col("media_mensalidade")).alias("media_mensalidade"),
    F.sum(F.col("media_gb")).alias("media_gb"),
    F.sum(F.col("media_voz")).alias("media_voz"),
    F.sum(F.col("media_cs")).alias("media_cs"),
    F.sum(F.col("media_score")).alias("media_score"),
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
    sas_v = sas_agg["vl_mensalidade"] or 0
    dbx_v = dbx_agg["vl_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_mensalidade", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["qt_chamadas_cs"] or 0
    dbx_v = dbx_agg["qt_chamadas_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_chamadas_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_chamadas_cs", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["vl_excedente"] or 0
    dbx_v = dbx_agg["vl_excedente"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_excedente) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_excedente", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["qt_min_voz"] or 0
    dbx_v = dbx_agg["qt_min_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_min_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_min_voz", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["qt_gb_dados"] or 0
    dbx_v = dbx_agg["qt_gb_dados"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_gb_dados) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_gb_dados", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["media_mensalidade"] or 0
    dbx_v = dbx_agg["media_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_mensalidade", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["media_gb"] or 0
    dbx_v = dbx_agg["media_gb"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_gb) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_gb", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["media_voz"] or 0
    dbx_v = dbx_agg["media_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_voz", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["media_cs"] or 0
    dbx_v = dbx_agg["media_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_cs", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    sas_v = sas_agg["media_score"] or 0
    dbx_v = dbx_agg["media_score"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_score) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_score", "table": "telcostar.operacional.freq_churn_risco_stats", "status": st})
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {e}")


# COMMAND ----------

# MAGIC %md ## 3. Distinct Keys — telcostar.operacional.freq_churn_risco_stats

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        dbx_dist = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {diff:+,} chaves"
        print(f"Distinct keys {["chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente"]} — SAS: {sas_dist:,} | DBX: {dbx_dist:,} | {status}")
        results.append({"check": "distinct_keys", "table": "telcostar.operacional.freq_churn_risco_stats", "status": status})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {e}")


# COMMAND ----------

# MAGIC %md ## 4. Anti-Join (linhas divergentes) — telcostar.operacional.freq_churn_risco_stats

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("sas")
        dbx_df = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {in_sas_not_dbx:,} | Em DBX mas não SAS: {in_dbx_not_sas:,} | {status}")
        results.append({"check": "antijoin", "table": "telcostar.operacional.freq_churn_risco_stats", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {e}")


# COMMAND ----------

# MAGIC %md ## 5. Integridade de Caracteres Especiais — telcostar.operacional.freq_churn_risco_stats

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {corrupt} registro(s) com encoding corrompido"
            print(f"Charset check (col: {sample_col}) — {status}")
            results.append({"check": "charset", "table": "telcostar.operacional.freq_churn_risco_stats", "status": status})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {e}")


# COMMAND ----------

# MAGIC %md ## Sumário — telcostar.operacional.freq_churn_risco_stats

print("\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — telcostar.operacional.freq_churn_risco_stats")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {len(ok)}")
print(f"  ✗ Falhas    : {len(failed)}")
if failed:
    for r in failed:
        print(f"    - {r['check']}: {r['status']}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\nResultado final: {overall}")
print("="*60)


# COMMAND ----------

# MAGIC %md ## 1. Row Count — telcostar.operacional.fato_churn_mensal

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {pct_diff:.4f}%"
    print(f"Row count — SAS: {sas_count:,} | DBX: {dbx_count:,} | Diff: {diff:+,} | {status}")
    results.append({"check": "row_count", "table": "telcostar.operacional.fato_churn_mensal", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")


# COMMAND ----------

# MAGIC %md ## 2. Somas Numéricas — telcostar.operacional.fato_churn_mensal

if sas_table and dbx_table:
    agg_cols = [
    F.sum(F.col("vl_mensalidade")).alias("vl_mensalidade"),
    F.sum(F.col("qt_chamadas_cs")).alias("qt_chamadas_cs"),
    F.sum(F.col("vl_excedente")).alias("vl_excedente"),
    F.sum(F.col("qt_min_voz")).alias("qt_min_voz"),
    F.sum(F.col("qt_gb_dados")).alias("qt_gb_dados"),
    F.sum(F.col("media_mensalidade")).alias("media_mensalidade"),
    F.sum(F.col("media_gb")).alias("media_gb"),
    F.sum(F.col("media_voz")).alias("media_voz"),
    F.sum(F.col("media_cs")).alias("media_cs"),
    F.sum(F.col("media_score")).alias("media_score"),
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
    sas_v = sas_agg["vl_mensalidade"] or 0
    dbx_v = dbx_agg["vl_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_mensalidade", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["qt_chamadas_cs"] or 0
    dbx_v = dbx_agg["qt_chamadas_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_chamadas_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_chamadas_cs", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["vl_excedente"] or 0
    dbx_v = dbx_agg["vl_excedente"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_excedente) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_excedente", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["qt_min_voz"] or 0
    dbx_v = dbx_agg["qt_min_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_min_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_min_voz", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["qt_gb_dados"] or 0
    dbx_v = dbx_agg["qt_gb_dados"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_gb_dados) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_gb_dados", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["media_mensalidade"] or 0
    dbx_v = dbx_agg["media_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_mensalidade", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["media_gb"] or 0
    dbx_v = dbx_agg["media_gb"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_gb) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_gb", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["media_voz"] or 0
    dbx_v = dbx_agg["media_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_voz", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["media_cs"] or 0
    dbx_v = dbx_agg["media_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_cs", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    sas_v = sas_agg["media_score"] or 0
    dbx_v = dbx_agg["media_score"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_score) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_score", "table": "telcostar.operacional.fato_churn_mensal", "status": st})
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {e}")


# COMMAND ----------

# MAGIC %md ## 3. Distinct Keys — telcostar.operacional.fato_churn_mensal

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        dbx_dist = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {diff:+,} chaves"
        print(f"Distinct keys {["chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente"]} — SAS: {sas_dist:,} | DBX: {dbx_dist:,} | {status}")
        results.append({"check": "distinct_keys", "table": "telcostar.operacional.fato_churn_mensal", "status": status})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {e}")


# COMMAND ----------

# MAGIC %md ## 4. Anti-Join (linhas divergentes) — telcostar.operacional.fato_churn_mensal

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("sas")
        dbx_df = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {in_sas_not_dbx:,} | Em DBX mas não SAS: {in_dbx_not_sas:,} | {status}")
        results.append({"check": "antijoin", "table": "telcostar.operacional.fato_churn_mensal", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {e}")


# COMMAND ----------

# MAGIC %md ## 5. Integridade de Caracteres Especiais — telcostar.operacional.fato_churn_mensal

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {corrupt} registro(s) com encoding corrompido"
            print(f"Charset check (col: {sample_col}) — {status}")
            results.append({"check": "charset", "table": "telcostar.operacional.fato_churn_mensal", "status": status})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {e}")


# COMMAND ----------

# MAGIC %md ## Sumário — telcostar.operacional.fato_churn_mensal

print("\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — telcostar.operacional.fato_churn_mensal")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {len(ok)}")
print(f"  ✗ Falhas    : {len(failed)}")
if failed:
    for r in failed:
        print(f"    - {r['check']}: {r['status']}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\nResultado final: {overall}")
print("="*60)


# COMMAND ----------

# MAGIC %md ## 1. Row Count — telcostar.operacional.churn_scored

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {pct_diff:.4f}%"
    print(f"Row count — SAS: {sas_count:,} | DBX: {dbx_count:,} | Diff: {diff:+,} | {status}")
    results.append({"check": "row_count", "table": "telcostar.operacional.churn_scored", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")


# COMMAND ----------

# MAGIC %md ## 2. Somas Numéricas — telcostar.operacional.churn_scored

if sas_table and dbx_table:
    agg_cols = [
    F.sum(F.col("vl_mensalidade")).alias("vl_mensalidade"),
    F.sum(F.col("qt_chamadas_cs")).alias("qt_chamadas_cs"),
    F.sum(F.col("vl_excedente")).alias("vl_excedente"),
    F.sum(F.col("qt_min_voz")).alias("qt_min_voz"),
    F.sum(F.col("qt_gb_dados")).alias("qt_gb_dados"),
    F.sum(F.col("media_mensalidade")).alias("media_mensalidade"),
    F.sum(F.col("media_gb")).alias("media_gb"),
    F.sum(F.col("media_voz")).alias("media_voz"),
    F.sum(F.col("media_cs")).alias("media_cs"),
    F.sum(F.col("media_score")).alias("media_score"),
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
    sas_v = sas_agg["vl_mensalidade"] or 0
    dbx_v = dbx_agg["vl_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_mensalidade", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["qt_chamadas_cs"] or 0
    dbx_v = dbx_agg["qt_chamadas_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_chamadas_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_chamadas_cs", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["vl_excedente"] or 0
    dbx_v = dbx_agg["vl_excedente"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(vl_excedente) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_vl_excedente", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["qt_min_voz"] or 0
    dbx_v = dbx_agg["qt_min_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_min_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_min_voz", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["qt_gb_dados"] or 0
    dbx_v = dbx_agg["qt_gb_dados"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(qt_gb_dados) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_qt_gb_dados", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["media_mensalidade"] or 0
    dbx_v = dbx_agg["media_mensalidade"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_mensalidade) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_mensalidade", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["media_gb"] or 0
    dbx_v = dbx_agg["media_gb"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_gb) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_gb", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["media_voz"] or 0
    dbx_v = dbx_agg["media_voz"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_voz) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_voz", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["media_cs"] or 0
    dbx_v = dbx_agg["media_cs"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_cs) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_cs", "table": "telcostar.operacional.churn_scored", "status": st})
    sas_v = sas_agg["media_score"] or 0
    dbx_v = dbx_agg["media_score"] or 0
    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0
    st = "✓" if pct < 0.01 else f"✗ {pct:.4f}%"
    print(f"  SUM(media_score) — SAS: {sas_v:,.2f} | DBX: {dbx_v:,.2f} | {st}")
    results.append({"check": "sum_media_score", "table": "telcostar.operacional.churn_scored", "status": st})
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {e}")


# COMMAND ----------

# MAGIC %md ## 3. Distinct Keys — telcostar.operacional.churn_scored

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        dbx_dist = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {diff:+,} chaves"
        print(f"Distinct keys {["chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente"]} — SAS: {sas_dist:,} | DBX: {dbx_dist:,} | {status}")
        results.append({"check": "distinct_keys", "table": "telcostar.operacional.churn_scored", "status": status})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {e}")


# COMMAND ----------

# MAGIC %md ## 4. Anti-Join (linhas divergentes) — telcostar.operacional.churn_scored

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("sas")
        dbx_df = spark.table(dbx_table).select("chave_segmento", "cd_segmento", "cd_risco", "cd_plano", "id_cliente").alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {in_sas_not_dbx:,} | Em DBX mas não SAS: {in_dbx_not_sas:,} | {status}")
        results.append({"check": "antijoin", "table": "telcostar.operacional.churn_scored", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, F.col("sas.chave_segmento") == F.col("dbx.chave_segmento") & F.col("sas.cd_segmento") == F.col("dbx.cd_segmento") & F.col("sas.cd_risco") == F.col("dbx.cd_risco") & F.col("sas.cd_plano") == F.col("dbx.cd_plano") & F.col("sas.id_cliente") == F.col("dbx.id_cliente"), "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {e}")


# COMMAND ----------

# MAGIC %md ## 5. Integridade de Caracteres Especiais — telcostar.operacional.churn_scored

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {corrupt} registro(s) com encoding corrompido"
            print(f"Charset check (col: {sample_col}) — {status}")
            results.append({"check": "charset", "table": "telcostar.operacional.churn_scored", "status": status})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {e}")


# COMMAND ----------

# MAGIC %md ## Sumário — telcostar.operacional.churn_scored

print("\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — telcostar.operacional.churn_scored")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {len(ok)}")
print(f"  ✗ Falhas    : {len(failed)}")
if failed:
    for r in failed:
        print(f"    - {r['check']}: {r['status']}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\nResultado final: {overall}")
print("="*60)
