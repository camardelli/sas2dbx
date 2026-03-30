# CLAUDE.md — SAS Query Migrator para Databricks

## Identidade do Projeto

- **Nome:** SAS2DBX — SAS Query to Databricks Migrator
- **Natureza:** Ferramenta CLI + biblioteca Python para transpilação automatizada de jobs SAS (SAS Query / SAS DI Studio / SAS Base) para notebooks e workflows Databricks, potencializada por LLM.
- **Stack alvo:** Python 3.11+ · PySpark · Databricks Workflows · Claude API (via Khon.ai gateway quando disponível) · Typer (CLI) · Rich (progress/output)
- **Repositório:** Monorepo com Turborepo (padrão Overlabs)

---

## Contexto Estratégico

Este projeto nasce da necessidade real de migrar jobs SAS Query para Databricks. Não existe hoje uma solução open source robusta para esse caminho específico. As ferramentas comerciais existentes (T1A Alchemist, SAS2PY, WiseWithData SPROCKET, EXL Code Harbor) são proprietárias e caras. O objetivo é construir um migrador AI-first que:

1. Sirva à migração interna imediata
2. Possa evoluir para um módulo comercializável dentro do stack Overlabs (conectável ao Prime Gate DI como acelerador de modernização)
3. Demonstre o modelo AIOX + Claude Code em ação (case real, não demo)

---

## Escopo — O Que Este Projeto FAZ e NÃO FAZ

### FAZ (MVP — Sprint 1 a 5)

- Parsing de código SAS exportado (`.sas` files) — DATA steps, PROC SQL, PROC SORT, PROC MEANS/SUMMARY, PROC FREQ, macros simples
- Análise de dependências — identificar datasets de entrada/saída, bibliotecas (LIBNAMEs), macros referenciadas, ordem de execução
- Transpilação SAS → PySpark/SparkSQL — usando LLM (Claude) com prompts especializados por tipo de construto, com fallback rule-based para padrões conhecidos
- Geração de notebooks Databricks (`.py` ou `.ipynb`) — um notebook por job SAS, com células organizadas por etapa lógica
- Geração de manifesto de workflow — arquivo JSON/YAML mapeando a DAG de execução dos jobs para Databricks Workflows
- Relatório de migração — por job: status (migrado/parcial/manual), construtos encontrados, warnings, confiança da tradução

### NÃO FAZ (fora do MVP)

- Migração de dados (`sas7bdat` → Delta Lake) — usar ferramentas existentes (spark-sas7bdat, saspy)
- Conversão de SAS Macros complexas com recursão/dinâmicas avançadas
- Conversão de PROC steps estatísticos avançados (PROC MIXED, PROC GENMOD, etc.)
- UI web — é CLI-first
- Execução dos notebooks no Databricks — apenas gera os artefatos

---

## Arquitetura de Alto Nível

```
┌──────────────────────────────────────────────────────────────────┐
│                        SAS2DBX Pipeline                          │
│                                                                  │
│  ┌──────────┐   ┌───────────┐   ┌──────────────────┐            │
│  │  INGEST  │──▶│  ANALYZE  │──▶│    TRANSPILE      │           │
│  │          │   │           │   │                  │            │
│  │  .sas    │   │ AST/Parse │   │  Rule Engine +   │            │
│  │  files   │   │ Dependency│   │  LLM (Claude)    │            │
│  │ dir scan │   │   Graph   │   │        ▲         │            │
│  └──────────┘   └───────────┘   └────────┼─────────┘            │
│                                          │ context injection     │
│                                          │                       │
│                          ┌───────────────┴────────┐             │
│                          │     KNOWLEDGE STORE     │             │
│                          │                         │             │
│                          │  sas_reference/         │             │
│                          │  pyspark_reference/     │             │
│                          │  mappings/ (YAML)       │             │
│                          │  custom/ (ambiente)     │             │
│                          └───────────────┬─────────┘             │
│                                          │ retrieval             │
│                                          ▼                       │
│                          ┌──────────────────┐                   │
│                          │     GENERATE      │                   │
│                          │                  │                    │
│                          │  .py notebooks   │                    │
│                          │  workflow.yaml   │                    │
│                          │  migration_report│                    │
│                          └──────────────────┘                   │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                   KNOWLEDGE STORE POPULATION                     │
│                                                                  │
│  ┌────────────┐   ┌────────────┐   ┌───────────────────┐        │
│  │  HARVEST   │──▶│  PROCESS   │──▶│   INDEX & STORE   │        │
│  │            │   │            │   │                   │        │
│  │  SAS docs  │   │ chunk by   │   │  YAML mappings    │        │
│  │ Spark docs │   │ construct  │   │  .md references   │        │
│  │ Databricks │   │ normalize  │   │  function maps    │        │
│  │ docs/APIs  │   │ deduplicate│   │  version-tagged   │        │
│  └────────────┘   └────────────┘   └───────────────────┘        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Módulos

```
sas2dbx/
├── cli.py                    # Entry point CLI (Typer + Rich — NOT click)
├── ingest/
│   ├── scanner.py            # Scan de diretório, detecção de .sas files
│   └── reader.py             # Leitura e normalização do código SAS
├── analyze/
│   ├── parser.py             # Parser SAS → AST simplificado (escopo: constructs suportados apenas)
│   ├── dependency.py         # Grafo de dependências (datasets, macros, libs, autoexec.sas)
│   └── classifier.py         # Classifica blocos por SUPPORTED_CONSTRUCTS (Story 0.2)
│                             # UNKNOWN → Tier.MANUAL sem tentativa de parse (nunca silently wrong)
├── knowledge/
│   ├── store.py              # API de acesso ao knowledge store (lookup + retrieval)
│   ├── populate/
│   │   ├── harvester.py      # Orquestrador offline-first de coleta por fonte
│   │   ├── sas_docs.py       # Harvester SAS: parse docs HTML/PDF offline
│   │   ├── spark_docs.py     # Harvester PySpark: extração de assinaturas e exemplos
│   │   ├── dbx_docs.py       # Harvester Databricks: Unity Catalog, DLT, Workflows
│   │   ├── chunker.py        # Quebra docs em chunks por construto/função/PROC
│   │   └── normalizer.py     # Normaliza formato, deduplica, valida integridade
│   ├── sas_reference/        # Docs SAS processados (gerado pelo populate)
│   │   ├── procs/            # Um .md por PROC (proc_sql.md, proc_sort.md, etc.)
│   │   ├── statements/       # DATA step statements (set.md, merge.md, retain.md)
│   │   ├── functions/        # Funções SAS (intck.md, put.md, input.md, etc.)
│   │   └── options/          # Dataset options e system options relevantes
│   ├── pyspark_reference/    # Docs PySpark processados (gerado pelo populate)
│   │   ├── dataframe_api/    # Métodos DataFrame (.filter, .join, .groupBy, etc.)
│   │   ├── functions/        # pyspark.sql.functions (months_between, when, etc.)
│   │   ├── spark_sql/        # Diferenças de dialeto Spark SQL vs ANSI/SAS SQL
│   │   └── delta/            # Operações Delta Lake (MERGE INTO, time travel, etc.)
│   ├── mappings/             # Tabelas de mapeamento — estrutura em 3 camadas (Story 0.1)
│   │   ├── generated/        # SOBRESCRITO pelo build-mappings — nunca editar manualmente
│   │   │   ├── functions_map.yaml
│   │   │   ├── formats_map.yaml
│   │   │   ├── informats_map.yaml
│   │   │   ├── options_map.yaml
│   │   │   ├── proc_map.yaml
│   │   │   └── sql_dialect_map.yaml
│   │   ├── curated/          # NUNCA sobrescrito — curadoria manual tem precedência
│   │   │   └── (mesmo schema de generated/, sobreposição por chave)
│   │   └── merged/           # Ground truth: curated > generated (gerado pelo build-mappings)
│   └── custom/               # Padrões do ambiente específico do cliente
│       ├── libnames.yaml     # LIBNAME → catalog.schema (suporta depends_on_jobs)
│       ├── macros.yaml       # Macros corporativas documentadas (nome, params, lógica)
│       └── conventions.yaml  # Naming conventions, regras de negócio, exceções
├── transpile/
│   ├── engine.py             # Orquestrador de transpilação (com checkpointing via state.py)
│   ├── state.py              # MigrationStateManager — migration_state.json + --resume (Story 3.1)
│   ├── rules/                # Regras determinísticas por construto
│   │   ├── data_step.py
│   │   ├── proc_sql.py
│   │   ├── proc_sort.py
│   │   └── proc_summary.py
│   ├── llm/
│   │   ├── client.py         # LLMClient — abstração de provider (Story 3.2)
│   │   ├── providers/
│   │   │   ├── anthropic.py  # AnthropicProvider (SDK direto)
│   │   │   └── khon.py       # KhonGatewayProvider (HTTP gateway)
│   │   ├── prompts.py        # Templates de prompt por tipo de construto
│   │   ├── context.py        # Monta contexto do Knowledge Store para injeção no prompt
│   │   └── validator.py      # Validação PySpark: sintaxe + semântica básica (Story 3.3)
│   └── fallback.py           # Estratégia quando rule + LLM falham
├── generate/
│   ├── notebook.py           # Gera notebooks via CellModel intermediário (Story 4.1)
│   ├── workflow.py           # Gera workflow YAML/JSON
│   └── report.py             # Gera relatório de migração
├── models/
│   ├── sas_ast.py            # Dataclasses do AST SAS (inclui Tier enum, ClassificationResult)
│   ├── dependency_graph.py   # Modelo do grafo (inclui get_implicit_dependencies())
│   └── migration_result.py   # Modelo do resultado (inclui JobStatus, ValidationResult)
└── utils/
    ├── sas_patterns.py       # Regex e padrões comuns SAS
    └── pyspark_templates.py  # Templates de código PySpark
```

---

## Knowledge Store — Memória Técnica do Migrador

O Knowledge Store é o módulo que dá ao transpiler acesso a documentação técnica precisa de SAS e Databricks/PySpark, eliminando dependência exclusiva do conhecimento embutido no LLM. Funciona como um RAG local file-based — retrieval por lookup direto (mappings YAML) e por construto (docs markdown), sem necessidade de vector DB no MVP.

### Por que é necessário

1. SAS tem ~400 PROCs e milhares de opções — o LLM conhece os 30 mais comuns; o resto precisa de referência documental
2. Funções SAS ↔ PySpark não são 1:1 — INTCK, INTNX, PUT, INPUT, CATX têm nuances que o LLM pode simplificar sem referência
3. Databricks evolui mais rápido que o training cutoff — Unity Catalog, Liquid Clustering, Delta Live Tables mudam a cada quarter
4. Cada ambiente tem macros e convenções únicas — nenhum modelo sabe que `%MACRO_SCD2` da sua empresa faz SCD Type 2 com merge condicional

### Como o Knowledge Store é consumido

```python
# Em transpile/llm/context.py — antes de montar o prompt para Claude
from knowledge.store import KnowledgeStore

ks = KnowledgeStore(base_path="./knowledge")

# 1. Lookup determinístico (mappings/merged/) — resolve sem LLM
pyspark_equiv = ks.lookup_function("INTCK")
# → {"pyspark": "months_between(dt_fim, dt_inicio)", "notes": "ordem dos args invertida"}

# 2. Retrieval de referência (docs markdown) — injeta no prompt
reference_context = ks.get_reference("sas", "procs", "proc_means")
pyspark_context = ks.get_reference("pyspark", "dataframe_api", "groupby_agg")

# 3. Contexto do ambiente (custom/) — resolve LIBNAMEs e macros locais
libname_map = ks.get_custom("libnames")
# → {"SASDATA": "main.raw", "SASTEMP": "main.staging", ...}

# 4. Monta prompt enriquecido
prompt = build_prompt(
    sas_block=sas_code,
    function_mappings=relevant_mappings,
    sas_reference=reference_context,
    pyspark_reference=pyspark_context,
    environment=libname_map
)
```

---

## Processo de População do Knowledge Store

A população é um pipeline separado, executado antes da primeira migração e re-executado quando as fontes de documentação mudam (novo release do Spark, atualização da doc SAS, novas macros corporativas).

### Fase 1 — Harvest (coleta de fontes brutas)

```bash
# CLI dedicado para população
sas2dbx knowledge harvest --source sas --version 9.4
sas2dbx knowledge harvest --source pyspark --version 3.5
sas2dbx knowledge harvest --source databricks --topics "unity-catalog,workflows,delta"
sas2dbx knowledge harvest --source custom --path ./meu_ambiente/
```

**Fontes e métodos de coleta:**

| Fonte | Modo padrão | Método | O que coleta |
|-------|-------------|--------|--------------|
| SAS 9.4 Language Reference | offline | Parse HTML/PDF local de `raw_input/sas/` | PROCs, statements, functions, formats, informats, options |
| SAS Viya (se aplicável) | offline | Idem, foco em CAS actions | CAS equivalents |
| PySpark 3.5 docs | offline | Parse HTML local de `raw_input/pyspark/` | DataFrame API, pyspark.sql.functions, SparkSQL reference |
| Databricks docs | offline | Parse HTML local de `raw_input/databricks/` | Unity Catalog, Delta Lake, Workflows API, SQL functions |
| Ambiente custom | N/A | Leitura de arquivos YAML/JSON fornecidos pelo usuário | LIBNAMEs, macros, naming conventions |

> **IMPORTANTE:** O modo padrão é sempre `--mode=offline`. Scraping online (`--mode=online`) é opt-in explícito — depende da estrutura dos sites e pode quebrar sem aviso.

**Implementação do harvester (`knowledge/populate/harvester.py`):**

```python
class KnowledgeHarvester:
    """Orquestra coleta de todas as fontes de documentação."""

    def harvest_sas(self, version="9.4", output_dir="./knowledge/raw/sas", mode="offline"):
        """
        Coleta documentação SAS.
        MODO PADRÃO: offline (determinístico, sem dependência de conectividade)

        Estratégia offline (default):
          1. Lê HTMLs/PDFs locais de ./knowledge/raw_input/sas/
          2. Parse com BeautifulSoup / pdfplumber
          3. Para cada PROC/função: gera um arquivo raw markdown

        Estratégia online (--mode=online, explícito):
          1. Fetch HTML de documentation.sas.com
          2. Parse e extrai seções relevantes

        NOTA: modo online depende de estrutura do site — pode quebrar sem aviso (Story 0.1)
        """
        pass

    def harvest_pyspark(self, version="3.5", output_dir="./knowledge/raw/pyspark", mode="offline"):
        """
        Coleta documentação PySpark.
        Estratégia offline (default): lê HTMLs locais de raw_input/pyspark/
        Foco em: pyspark.sql.functions, DataFrame methods, SparkSQL
        """
        pass

    def harvest_databricks(self, topics: list, output_dir="./knowledge/raw/databricks", mode="offline"):
        """
        Coleta documentação Databricks.
        Foco em: Unity Catalog DDL, Delta Lake operations,
        Workflows API schema, built-in SQL functions
        """
        pass

    def harvest_custom(self, input_path: str, output_dir="./knowledge/custom"):
        """
        Processa arquivos do ambiente do cliente.
        Aceita:
          - libnames.yaml: mapeamento LIBNAME → catalog.schema (+ depends_on_jobs)
          - macros.yaml: documentação de macros corporativas
          - conventions.yaml: regras de naming, exceções
          - *.sas: macros para auto-documentação (parse e extrai params/lógica)
        """
        pass
```

### Fase 2 — Process (chunking e normalização)

```bash
sas2dbx knowledge process --source sas
sas2dbx knowledge process --source pyspark
sas2dbx knowledge process --source databricks
```

O que o processamento faz:

1. **Chunking por construto** — cada PROC, função, statement vira um arquivo `.md` independente com estrutura padronizada:

```markdown
# PROC MEANS / PROC SUMMARY

## Sintaxe SAS
PROC MEANS DATA=dataset <options>;
  VAR var-list;
  CLASS class-vars;
  OUTPUT OUT=output-dataset <stat-keyword=name>;
RUN;

## Opções principais
- N, NMISS, MEAN, STD, MIN, MAX, SUM, USS, CSS, RANGE, Q1, MEDIAN, Q3
- NWAY: apenas combinações completas de CLASS vars
- NOPRINT: suprime output no log

## Equivalência PySpark
DataFrame.groupBy("class_vars").agg(
    F.count("var").alias("N"),
    F.mean("var").alias("MEAN"),
    F.stddev("var").alias("STD"),
    ...
)

## Gotchas na conversão
- PROC MEANS sem CLASS = agregação global → df.agg() sem groupBy
- OUTPUT OUT= com _TYPE_ e _FREQ_ → requer lógica extra no PySpark
- NWAY → equivale a não incluir subtotais (comportamento padrão do groupBy)

## Exemplos
[pares SAS → PySpark cobrindo variações comuns]
```

2. **Normalização** — formato consistente, encoding UTF-8, remoção de HTML artifacts
3. **Deduplicação** — merge de informações redundantes entre fontes
4. **Versionamento** — cada arquivo processado carrega metadata de versão da fonte

### Fase 3 — Build Mappings (geração de tabelas de lookup)

```bash
sas2dbx knowledge build-mappings
```

Gera arquivos em `mappings/generated/` e faz merge para `mappings/merged/` (curated > generated):

```yaml
# mappings/generated/functions_map.yaml
# Formato: sas_function → {pyspark, notes, confidence}
INTCK:
  pyspark: "months_between(end, start)"  # para MONTH interval
  notes: "Ordem dos argumentos invertida. Para intervalos != MONTH, usar lógica custom."
  variants:
    MONTH: "months_between({arg2}, {arg1})"
    DAY: "datediff({arg2}, {arg1})"
    YEAR: "floor(months_between({arg2}, {arg1}) / 12)"
  confidence: 0.9

INTNX:
  pyspark: "add_months({arg1_date}, {arg2_n})"
  notes: "Apenas para MONTH. Alignment (B/M/E) requer ajuste manual."
  confidence: 0.7

PUT:
  pyspark: "cast / format_string / date_format"
  notes: "Depende do formato. Numeric → cast. Date → date_format. Custom → lookup formats_map."
  confidence: 0.6

INPUT:
  pyspark: "cast({col} as {type})"
  notes: "Para informats numéricos. Para date informats, usar to_date com pattern."
  confidence: 0.7

CATX:
  pyspark: "concat_ws({delimiter}, {args})"
  notes: "CATX remove leading/trailing spaces automaticamente — adicionar trim() se necessário."
  confidence: 0.95

COMPRESS:
  pyspark: "regexp_replace({col}, '[pattern]', '')"
  notes: "Sem segundo arg = remove spaces. Com modifiers (kd, ka) requer regex específico."
  confidence: 0.8

SUBSTR:
  pyspark: "substring({col}, {pos}, {len})"
  notes: "SAS é 1-indexed, PySpark substring() também é 1-indexed — sem ajuste necessário."
  confidence: 0.95

SCAN:
  pyspark: "split({col}, {delimiter})[{n-1}]"
  notes: "SAS SCAN é 1-indexed, array PySpark é 0-indexed — subtrair 1 do índice."
  confidence: 0.85
```

```yaml
# mappings/generated/sql_dialect_map.yaml
CALCULATED:
  sas: "SELECT *, (col1 + col2) AS total, CALCULATED total * 0.1 AS tax"
  spark: "WITH t AS (SELECT *, col1+col2 AS total) SELECT *, total*0.1 AS tax FROM t"
  notes: "CALCULATED não existe em Spark SQL — usar subquery ou CTE"

MONOTONIC:
  sas: "SELECT MONOTONIC() AS row_num, * FROM table"
  spark: "SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num, * FROM table"
  notes: "Ou usar F.monotonically_increasing_id() no DataFrame API (não sequencial)"

CREATE_TABLE_AS:
  sas: "CREATE TABLE lib.output AS SELECT ..."
  spark: "CREATE OR REPLACE TABLE catalog.schema.output AS SELECT ..."
  notes: "Adicionar catalog.schema prefix conforme libnames.yaml"

DATEPART:
  sas: "DATEPART(datetime_col)"
  spark: "CAST(datetime_col AS DATE)"

DATETIME_LITERALS:
  sas: "'01JAN2025'd / '01JAN2025:00:00:00'dt"
  spark: "DATE '2025-01-01' / TIMESTAMP '2025-01-01 00:00:00'"
```

### Fase 4 — Validate (verificação de integridade)

```bash
sas2dbx knowledge validate
```

- Verifica que todo PROC referenciado no `proc_map.yaml` tem um `.md` correspondente em `sas_reference/procs/`
- Verifica que toda função no `functions_map.yaml` tem equivalência PySpark documentada
- Verifica que `libnames.yaml` cobre todas as bibliotecas encontradas nos jobs SAS (cross-check com `analyze`)
- Gera relatório de cobertura: _"Knowledge store cobre 87% dos construtos encontrados nos jobs"_

### Manutenção e Evolução

```yaml
# knowledge/manifest.yaml — metadata do knowledge store
version: "1.0.0"
last_populated: "2026-03-30T14:00:00Z"
sources:
  sas:
    version: "9.4 M8"
    docs_url: "https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/lrdict/titlepage.htm"
    procs_covered: 42
    functions_covered: 156
    last_harvested: "2026-03-30"
  pyspark:
    version: "3.5.1"
    docs_url: "https://spark.apache.org/docs/3.5.1/api/python/"
    functions_covered: 203
    last_harvested: "2026-03-30"
  databricks:
    topics: ["unity-catalog", "workflows", "delta-lake", "sql-functions"]
    last_harvested: "2026-03-30"
  custom:
    libnames_count: 12
    macros_documented: 8
    last_updated: "2026-03-30"
coverage:
  functions_mapped: 156
  procs_documented: 42
  sql_dialect_rules: 15
  confidence_avg: 0.82
```

**Regras de re-população:**

- `sas_reference/` e `pyspark_reference/` — re-harvest quando muda a versão alvo (ex: Spark 3.5 → 4.0)
- `mappings/curated/` — ground truth manual, nunca sobrescrito; `mappings/generated/` é sobrescrito a cada `build-mappings`
- `custom/` — atualizar sempre que o ambiente do cliente mudar (novas libs, novas macros)
- `sas2dbx knowledge validate` deve rodar como pre-check antes de cada migração

---

## Inputs Esperados

### Input Primário: Diretório de jobs SAS

```
/caminho/para/sas_jobs/
├── job_001_carga_clientes.sas
├── job_002_transform_vendas.sas
├── job_003_report_mensal.sas
├── macros/
│   ├── macro_scd2.sas
│   └── macro_utils.sas
└── autoexec.sas          # (opcional) configurações globais de LIBNAME e macros
```

### Input Secundário: Arquivo de configuração

```yaml
# sas2dbx.yaml
source:
  path: ./sas_jobs
  encoding: latin1          # SAS frequentemente usa latin1/cp1252
  autoexec: ./sas_jobs/autoexec.sas

target:
  platform: databricks
  spark_version: "3.5"
  catalog: main             # Unity Catalog
  schema: migrated
  notebook_format: py       # py | ipynb (ver Story 4.1 — CellModel intermediário)

knowledge:
  base_path: ./knowledge    # raiz do knowledge store
  harvest:
    sas_version: "9.4"
    sas_docs_source: offline          # offline (HTMLs/PDFs locais) | online (scrape docs)
    sas_docs_path: ./knowledge/raw_input/sas/
    pyspark_version: "3.5.1"
    databricks_topics:
      - unity-catalog
      - workflows
      - delta-lake
      - sql-functions
  custom:
    libnames: ./knowledge/custom/libnames.yaml
    macros: ./knowledge/custom/macros.yaml
    conventions: ./knowledge/custom/conventions.yaml
  context_injection:
    max_tokens: 2000          # limite de tokens do Knowledge Store no prompt
    include_examples: true    # incluir exemplos SAS→PySpark nos docs de referência
    include_gotchas: true     # incluir gotchas/warnings de conversão

llm:
  provider: anthropic         # anthropic | khon (via gateway)
  model: claude-sonnet-4-20250514
  max_tokens_per_call: 4096
  temperature: 0.0            # determinístico para code generation
  retry_attempts: 3

migration:
  strategy: hybrid            # hybrid (rules + LLM) | llm_only | rules_only
  confidence_threshold: 0.8   # abaixo disso, marca como revisão manual
  generate_tests: true        # gera asserts básicos de validação
  preserve_comments: true     # mantém comentários SAS como docstring
```

---

## Outputs Esperados

### 1. Notebooks Databricks (um por job SAS)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # job_001_carga_clientes
# MAGIC **Migrado de:** job_001_carga_clientes.sas
# MAGIC **Data migração:** 2026-03-30
# MAGIC **Confiança:** 0.92
# MAGIC **Warnings:** PROC FORMAT não convertido (linha 45)

# COMMAND ----------
# Configuração e imports
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# [Bloco 1] DATA step: carga_clientes
# Origem SAS linhas 12-38
df_clientes = (
    spark.read.table("main.raw.clientes_raw")
    .filter(F.col("dt_cadastro") >= "2024-01-01")
    .withColumn("nome_upper", F.upper(F.col("nome")))
    .withColumn("flag_ativo", F.when(F.col("status") == "A", 1).otherwise(0))
)

# COMMAND ----------
# [Bloco 2] PROC SQL: join com vendas
# Origem SAS linhas 40-65
df_resultado = spark.sql("""
    SELECT c.*, v.total_vendas, v.ultima_compra
    FROM main.migrated.clientes c
    LEFT JOIN main.raw.vendas v ON c.id_cliente = v.id_cliente
    WHERE v.ano = 2025
""")

# COMMAND ----------
# Output
df_resultado.write.mode("overwrite").saveAsTable("main.migrated.clientes_enriquecido")
```

### 2. Workflow Definition

```yaml
# workflow.yaml
name: sas_migration_pipeline
tasks:
  - task_key: job_001_carga_clientes
    notebook_task:
      notebook_path: /Repos/migrated/job_001_carga_clientes
    depends_on: []
  - task_key: job_002_transform_vendas
    notebook_task:
      notebook_path: /Repos/migrated/job_002_transform_vendas
    depends_on:
      - task_key: job_001_carga_clientes
  - task_key: job_003_report_mensal
    notebook_task:
      notebook_path: /Repos/migrated/job_003_report_mensal
    depends_on:
      - task_key: job_001_carga_clientes
      - task_key: job_002_transform_vendas
```

### 3. Relatório de Migração

```json
{
  "summary": {
    "total_jobs": 3,
    "fully_migrated": 2,
    "partial": 1,
    "manual_required": 0,
    "total_sas_lines": 420,
    "total_pyspark_lines": 285,
    "avg_confidence": 0.89
  },
  "jobs": [
    {
      "source": "job_001_carga_clientes.sas",
      "output": "job_001_carga_clientes.py",
      "status": "migrated",
      "confidence": 0.92,
      "constructs": {
        "data_steps": 2,
        "proc_sql": 1,
        "proc_sort": 1,
        "macros_resolved": 0
      },
      "warnings": [
        {
          "line": 45,
          "type": "unsupported_proc",
          "message": "PROC FORMAT: custom formats não convertidos automaticamente",
          "suggestion": "Implementar como UDF ou lookup table no Databricks"
        }
      ]
    }
  ]
}
```

---

## Estratégia de Transpilação

### Tier 1 — Rule-Based (alta confiança, sem LLM)

| Construto SAS | Equivalente PySpark |
|---------------|---------------------|
| DATA step simples (SET, WHERE, KEEP, DROP, RENAME) | DataFrame API (read, filter, select, withColumnRenamed) |
| PROC SQL | `spark.sql()` com adaptação de sintaxe |
| PROC SORT | `.orderBy()` / `.dropDuplicates()` |
| LIBNAME ... path | `spark.read.table()` / Unity Catalog reference |
| PROC EXPORT CSV | `.write.csv()` |
| PROC IMPORT CSV | `spark.read.csv()` |

### Tier 2 — LLM-Assisted (confiança média)

| Construto SAS | Abordagem |
|---------------|-----------|
| DATA step com arrays, retain, by-group processing | Prompt especializado com exemplos few-shot |
| PROC MEANS/SUMMARY/FREQ | Prompt com mapeamento para `.groupBy().agg()` |
| Macros simples (`%macro/%mend` com parâmetros) | Prompt para converter em funções Python |
| Expressões condicionais complexas (SELECT/WHEN) | Prompt com `F.when().when().otherwise()` |

### Tier 3 — Manual Flag (baixa confiança)

| Construto SAS | Ação |
|---------------|------|
| PROC FORMAT com formatos customizados | Flag + sugestão de lookup table |
| Macros com `%SYSFUNC`, `CALL EXECUTE`, geração dinâmica | Flag + código SAS original como comentário |
| PROC REPORT / PROC TABULATE | Flag + sugestão de Databricks SQL Dashboard |
| Hash objects | Flag + sugestão de broadcast join |
| I/O não-standard (mainframe, EBCDIC) | Flag + requer análise manual |

---

## Prompt Engineering — Template Base para Claude

```
Você é um especialista em migração SAS para PySpark/Databricks.

TAREFA: Converter o bloco SAS abaixo para código PySpark equivalente.

REGRAS:
1. Use DataFrame API nativa (NUNCA RDDs ou UDFs desnecessárias)
2. Use spark.sql() para PROC SQL — adapte sintaxe SAS SQL para Spark SQL
3. Preserve a lógica exata — mesmos filtros, joins, transformações
4. Adicione comentários explicando cada transformação
5. Use F.col() para referências de colunas (import pyspark.sql.functions as F)
6. Para datasets SAS, use spark.read.table("catalog.schema.tabela")
7. Para output datasets, use .write.mode("overwrite").saveAsTable()
8. Se encontrar construto que NÃO consegue converter com certeza, retorne:
   # WARNING: [tipo_construto] na linha [N] requer revisão manual
   # SAS original: [código]

CONTEXTO DO AMBIENTE:
- Plataforma alvo: Databricks com Unity Catalog
- Spark version: 3.5
- Catalog: {catalog}
- Schema: {schema}
- Datasets disponíveis: {available_datasets}
- Macros já resolvidas: {resolved_macros}
- Mapeamento de bibliotecas: {libname_mappings}

REFERÊNCIA TÉCNICA (do Knowledge Store — use como ground truth):
{knowledge_context}

MAPEAMENTOS DE FUNÇÕES RELEVANTES:
{function_mappings}

DIFERENÇAS DE DIALETO SQL RELEVANTES:
{sql_dialect_notes}

BLOCO SAS:
```sas
{sas_code_block}
```

Responda APENAS com o código PySpark. Sem explicações fora dos comentários inline.
```

**Nota sobre o context injection:** O módulo `transpile/llm/context.py` é responsável por selecionar o contexto relevante:

1. `classifier.py` identifica os tipos de construto no bloco (DATA step, PROC SQL, etc.)
2. `context.py` faz lookup em `mappings/merged/` para os construtos encontrados
3. `context.py` carrega os `.md` de referência correspondentes (SAS + PySpark)
4. `context.py` carrega regras de dialeto SQL se o bloco contém PROC SQL
5. O prompt é montado com o contexto mínimo necessário, respeitando o limite de tokens

---

## Decisões Arquiteturais (revisão @architect — 2026-03-30)

As seguintes decisões foram tomadas após revisão arquitetural e estão refletidas nas stories em `docs/stories/`:

| ID | Decisão | Módulo impactado | Story |
|----|---------|------------------|-------|
| R1 | `classifier.py` usa `SUPPORTED_CONSTRUCTS` allowlist — UNKNOWN → Tier.MANUAL direto, nunca silently parsed | `analyze/` | 0.2 |
| R2 | Harvester offline-first — `--mode=offline` como padrão; `--mode=online` é opt-in explícito | `knowledge/` | 0.1 |
| R3 | Checkpointing em `engine.py` via `state.py` + `migration_state.json` + flag `--resume` | `transpile/` | 3.1 |
| R4 | `validator.py` além de `ast.parse()`: imports necessários, Unity Catalog prefix, referências F/spark | `transpile/` | 3.3 |
| R5 | `dependency.py` analisa `autoexec.sas` e suporta campo `depends_on_jobs` em `libnames.yaml` | `analyze/` | 2.1 |
| R6 | `mappings/` dividido em `generated/` + `curated/` + `merged/` — curated nunca sobrescrito pelo build-mappings | `knowledge/` | 0.1 |
| R7 | CLI usa **Typer** (não Click) — melhor integração com type hints Python 3.11+ e auto-completion | `cli.py` | 1.1 |
| R8 | **Rich** para progress bars, tabelas e output colorizado — obrigatório em todos os comandos batch | `cli.py` | 1.1 |
| R9 | `LLMClient` abstrai provider (Anthropic direto / Khon gateway) com fallback automático e retry | `transpile/` | 3.2 |
| R10 | `notebook.py` usa `CellModel` intermediário agnóstico de formato — renderiza para `.py` ou `.ipynb` sem duplicar lógica | `generate/` | 4.1 |

### Tecnologias Confirmadas

- **CLI**: Typer (`typer[all]`) — **não usar click**
- **Progress/Output**: Rich — obrigatório em todos os comandos com processamento em batch
- **LLM**: Interface unificada `LLMClient` — nunca chamar SDK Anthropic diretamente no engine
- **Mappings**: Sempre ler de `mappings/merged/` — nunca de `generated/` diretamente
- **Parser escopo**: Qualquer construct não listado em `SUPPORTED_CONSTRUCTS` é Tier.MANUAL
- **Notebook format**: Lógica de geração em `CellModel`; renderizadores separados para `.py` e `.ipynb`

---

## Plano de Sprints

### Sprint 0 — Knowledge Store Foundation (pré-requisito)

- [ ] **[Story 0.1]** Harvester offline-first + estrutura `mappings/generated/curated/merged/`
- [ ] **[Story 0.2]** `classifier.py` com `SUPPORTED_CONSTRUCTS` allowlist e `Tier` enum
- [ ] Estrutura de pastas `knowledge/` com subdiretórios
- [ ] `knowledge/store.py` — API de lookup via `mappings/merged/` e retrieval (`get_reference`)
- [ ] `knowledge/populate/harvester.py` — orquestrador offline-first por fonte
- [ ] `knowledge/populate/sas_docs.py` — harvester SAS: parse docs HTML/PDF offline
- [ ] `knowledge/populate/spark_docs.py` — harvester PySpark: extração de assinaturas e exemplos
- [ ] `knowledge/populate/chunker.py` + `normalizer.py` — pipeline raw → `.md`, grava em `generated/`
- [ ] Seed inicial dos `mappings/curated/` — curadoria manual das 50 funções SAS mais frequentes
- [ ] `knowledge/populate/dbx_docs.py` — harvester Databricks: Unity Catalog DDL, Delta operações
- [ ] `knowledge/manifest.yaml` — metadata de versão e cobertura
- [ ] CLI: `sas2dbx knowledge harvest [--mode offline|online]`, `process`, `build-mappings`, `validate`
- [ ] Testes: validate retorna cobertura > 80% para PROCs Tier 1 e funções top-50

- **Entregável:** Knowledge Store populado e validado, pronto para consumo pelo transpiler

### Sprint 1 — Foundation

- [ ] **[Story 1.1]** CLI com Typer + Rich (progress bars, `--verbose`/`--quiet`, `--resume` stub)
- [ ] Setup do monorepo (pyproject.toml com `typer[all]`, `rich`, estrutura de pastas)
- [ ] Módulo `ingest` completo (scanner + reader)
- [ ] Parser SAS básico — tokenização usando `SUPPORTED_CONSTRUCTS` do classifier
- [ ] `knowledge/custom/` — template de `libnames.yaml` (com `depends_on_jobs`), `macros.yaml`, `conventions.yaml`
- [ ] Testes unitários com 3 arquivos SAS de exemplo

- **Entregável:** CLI que lê diretório SAS e imprime inventário de blocos

### Sprint 2 — Analysis

- [ ] **[Story 2.1]** Dependências implícitas + análise de `autoexec.sas`
- [ ] Extração de dependências (datasets in/out por bloco)
- [ ] Grafo de dependências com resolução de ordem
- [ ] Resolução básica de macros (inline substitution para macros simples)
- [ ] Classificador de complexidade por bloco (Tier 1/2/3)
- [ ] Cross-check: `sas2dbx knowledge validate` cruza construtos encontrados nos jobs com cobertura do Knowledge Store

- **Entregável:** CLI que gera dependency graph + relatório de complexidade + gap analysis do Knowledge Store

### Sprint 3 — Transpilation Core

- [ ] **[Story 3.1]** Checkpointing em `engine.py` + `state.py` + `--resume` funcional
- [ ] **[Story 3.2]** `LLMClient` com abstração de provider (Anthropic / Khon gateway + fallback automático)
- [ ] **[Story 3.3]** `validator.py` expandido: imports, Unity Catalog prefix, referências F/spark
- [ ] `transpile/llm/context.py` — montagem de contexto do Knowledge Store (lê de `mappings/merged/`)
- [ ] Rule engine Tier 1 (DATA step simples, PROC SQL, PROC SORT)
- [ ] Integração Claude API para Tier 2 via `LLMClient` — prompts enriquecidos com Knowledge Store
- [ ] Prompt templates por tipo de construto

- **Entregável:** CLI que transpila jobs Tier 1 e 2 end-to-end, com resume e validação semântica

### Sprint 4 — Generation

- [ ] **[Story 4.1]** `CellModel` intermediário + renderizadores `.py` e `.ipynb` para `notebook.py`
- [ ] Gerador de workflow YAML
- [ ] Gerador de relatório de migração (JSON + markdown summary)
- [ ] Tratamento de erros e retry na chamada LLM
- [ ] Relatório inclui cobertura do Knowledge Store por job (quais funções/PROCs tinham referência disponível)

- **Entregável:** Pipeline completa ingest → analyze → transpile → generate

### Sprint 5 — Polish & Validation

- [ ] Validação cruzada: executar notebook gerado em Databricks sandbox
- [ ] Ajuste de prompts baseado em resultados reais
- [ ] Feedback loop: resultados de validação alimentam curadoria dos `mappings/curated/`
- [ ] Documentação de uso (README)
- [ ] Empacotamento (`pip install sas2dbx`)
- [ ] CLI: `sas2dbx knowledge update` — re-harvest seletivo + reprocessamento

- **Entregável:** v0.1.0 release candidate

---

## Princípios de Desenvolvimento

1. **Uma story por sessão** — padrão Overlabs de execução com Claude Code
2. **Testes primeiro** — cada módulo nasce com seus testes
3. **Código gerado é código auditável** — todo output inclui rastreabilidade ao SAS original (número de linha, bloco de origem)
4. **LLM é copiloto, não piloto** — rules engine primeiro, LLM para o que rules não cobre
5. **Fail loud** — quando não conseguir converter, diz claramente e preserva o SAS original como comentário
6. **Zero alucinação** — confidence score em cada bloco, threshold configurável para flag manual (`confidence_threshold`)
7. **Knowledge Store é ground truth** — o LLM recebe referência documental verificada, não depende apenas de treinamento
8. **Knowledge Store antes de tudo** — Sprint 0 existe porque o transpiler sem referência documental alucinaria nas funções menos comuns
9. **Feedback loop fecha o ciclo** — resultados de validação (Sprint 5) retroalimentam a curadoria de `mappings/curated/`

---

## Referências Técnicas

- Databricks Lakebridge (open source): https://github.com/databrickslabs/lakebridge
- Intro Databricks para SAS devs: https://www.databricks.com/blog/2021/12/07/introduction-to-databricks-for-sas-users
- Migração SAS DW para Lakehouse (4 steps): https://www.databricks.com/blog/four-steps-migrating-sas-workloads-databricks
- spark-sas7bdat (leitura de datasets SAS): https://github.com/saurfang/spark-sas7bdat
- SAS Language Reference (para parser): https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/lrdict/titlepage.htm
