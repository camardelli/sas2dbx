# SAS2DBX — SAS Query to Databricks Migrator

Ferramenta CLI + biblioteca Python para transpilação automatizada de jobs SAS para notebooks e workflows Databricks, potencializada por LLM (Claude).

## Instalação

```bash
pip install sas2dbx
```

Ou em modo desenvolvimento:

```bash
git clone https://github.com/camardelli/sas2dbx.git
cd sas2dbx
pip install -e ".[dev]"
```

**Requisitos:** Python 3.11+

## Início Rápido

```bash
# Migrar um diretório de jobs SAS
sas2dbx migrate ./meus_jobs/ --output ./notebooks/

# Retomar migração interrompida
sas2dbx migrate ./meus_jobs/ --output ./notebooks/ --resume

# Ver status de uma migração em andamento
sas2dbx status --output ./notebooks/
```

## Pipeline de Migração

```
.sas files → INGEST → ANALYZE → TRANSPILE → GENERATE → notebooks + workflow + report
```

| Fase | O que faz |
|------|-----------|
| **Ingest** | Scan de diretório, leitura e normalização dos arquivos `.sas` |
| **Analyze** | Parser SAS → AST, grafo de dependências, classificação por tier |
| **Transpile** | Rule engine (Tier 1) + LLM Claude (Tier 2), checkpointing com `--resume` |
| **Generate** | Notebooks `.py`/`.ipynb`, `workflow.yaml`, `migration_report.json/.md` |

## Estratégia de Transpilação

| Tier | Construtos | Abordagem |
|------|-----------|-----------|
| **1 — Rule** | DATA step simples, PROC SQL, PROC SORT | Determinístico, sem LLM |
| **2 — LLM** | DATA step complexo, PROC MEANS/FREQ, macros | Claude com prompts especializados |
| **3 — Manual** | PROC FORMAT, macros dinâmicas, PROC REPORT | Flag para revisão humana, código SAS preservado como comentário |

## Configuração

Crie um `sas2dbx.yaml` na raiz do projeto:

```yaml
source:
  path: ./sas_jobs
  encoding: latin1

target:
  platform: databricks
  catalog: main
  schema: migrated
  notebook_format: py   # py | ipynb

llm:
  provider: anthropic   # anthropic | khon
  model: claude-sonnet-4-6
  temperature: 0.0

migration:
  confidence_threshold: 0.8
```

### Providers LLM

**Anthropic (direto):**
```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

**Khon.ai gateway:**
```yaml
llm:
  provider: khon
  khon_url: https://gateway.khon.ai
  khon_token: seu-token
  api_key: sk-ant-...   # fallback automático se gateway falhar
```

## Saídas

Após a migração, o diretório `--output` conterá:

```
notebooks/
├── job_001_carga_clientes.py      # notebook Databricks
├── job_002_transform_vendas.py
├── job_003_report_mensal.py
├── workflow.yaml                  # Databricks Workflows definition
├── migration_report.json          # relatório estruturado
├── migration_report.md            # sumário legível
└── .sas2dbx_state.json            # checkpoint para --resume
```

### Exemplo de notebook gerado

```python
# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # job_001_carga_clientes
# MAGIC **Migrado de:** job_001_carga_clientes.sas
# MAGIC **Confiança:** 0.92

# COMMAND ----------
import pyspark.sql.functions as F

# COMMAND ----------
df_clientes = (
    spark.read.table("main.raw.clientes_raw")
    .filter(F.col("dt_cadastro") >= "2024-01-01")
    .withColumn("flag_ativo", F.when(F.col("status") == "A", 1).otherwise(0))
)
df_clientes.write.mode("overwrite").saveAsTable("main.migrated.clientes")
```

### Exemplo de workflow gerado

```yaml
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
```

## Comandos CLI

```bash
# Migração completa
sas2dbx migrate <source_dir> --output <output_dir> [--resume] [--verbose]

# Status de migração existente
sas2dbx status --output <output_dir>

# Validar código PySpark gerado
sas2dbx validate <notebook.py>
```

## Desenvolvimento

```bash
# Instalar dependências de dev
pip install -e ".[dev]"

# Rodar testes
pytest

# Lint
ruff check sas2dbx/ tests/

# Build do pacote
pip install hatch
hatch build
```

## Limitações do MVP (v0.1.0)

- Macros com recursão ou geração dinâmica (`CALL EXECUTE`, `%SYSFUNC`) → Tier 3 (flag manual)
- PROCs estatísticos avançados (`PROC MIXED`, `PROC GENMOD`) → Tier 3
- Migração de dados `sas7bdat` → Delta Lake não incluída (usar [spark-sas7bdat](https://github.com/saurfang/spark-sas7bdat))
- Knowledge Store harvesters em modo stub — população manual de `knowledge/custom/` recomendada

## Licença

MIT
