---
title: "SAS2DBX — Manual do Usuário"
subtitle: "Migração Automatizada de Jobs SAS para Databricks"
version: "v0.1.0"
date: "Março 2026"
author: "Equipe Overlabs"
lang: pt-BR
toc: true
toc-depth: 3
numbersections: true
---

# Visão Geral

## O que é o sas2dbx

**sas2dbx** é uma ferramenta CLI e biblioteca Python para transpilação automatizada de jobs SAS (SAS Query / SAS DI Studio / SAS Base) para notebooks e workflows Databricks, potencializada por LLM (Claude API).

O projeto nasce da necessidade real de migrar ambientes SAS para o Databricks Lakehouse, oferecendo uma alternativa open source às ferramentas comerciais existentes (T1A Alchemist, SAS2PY, WiseWithData SPROCKET, EXL Code Harbor), que são proprietárias e de alto custo.

## Casos de uso

- **Migração interna**: Equipes de dados que precisam modernizar pipelines SAS legados para Databricks
- **Aceleração de projetos**: Reduz o esforço manual de reescrita de centenas de jobs SAS
- **Auditoria de complexidade**: Inventaria e classifica o esforço de migração antes de iniciá-la
- **Validação automatizada**: Deploy e execução de notebooks gerados diretamente no workspace Databricks

## Escopo do MVP (v0.1.0)

### O que a ferramenta FAZ

- Parsing de código SAS exportado (`.sas`) — DATA steps, PROC SQL, PROC SORT, PROC MEANS/SUMMARY, PROC FREQ, macros simples
- Análise de dependências — datasets de entrada/saída, bibliotecas (LIBNAMEs), macros, ordem de execução
- Transpilação SAS → PySpark/SparkSQL — via LLM (Claude) com prompts especializados, com fallback rule-based
- Geração de notebooks Databricks (`.py` ou `.ipynb`) — um notebook por job SAS
- Geração de manifesto de workflow — YAML/JSON com a DAG para Databricks Workflows
- Relatório de migração — por job: status, construtos encontrados, warnings, confiança
- Validação Databricks — deploy de notebooks, execução via Workflow, coleta de resultados de tabelas
- Self-healing — diagnóstico automático de falhas de execução e aplicação de fixes determinísticos

### O que NÃO está no escopo

- Migração de dados (`sas7bdat` → Delta Lake) — use ferramentas como `spark-sas7bdat`, `saspy`
- Conversão de macros com recursão/geração dinâmica avançada
- Conversão de PROCs estatísticos avançados (PROC MIXED, PROC GENMOD, etc.)
- Interface web autônoma (a ferramenta é CLI-first; o servidor web é auxiliar)

---

# Arquitetura

## Diagrama do Pipeline

```
┌----------------------------------------------------------┐
|                    SAS2DBX Pipeline                       |
|                                                           |
|   INGEST → ANALYZE → TRANSPILE → GENERATE → VALIDATE     |
|                                                           |
|   .sas     AST/Deps   Rules +    Notebooks   Deploy +     |
|   files    Graph      LLM        Workflow     Execute     |
\-----------------------------------------------------------┘
```

## Estratégia de três tiers de transpilação

O transpilador classifica cada bloco SAS em um dos três tiers antes de processá-lo:

| Tier | Estratégia | Exemplos de construtos |
|------|-----------|----------------------|
| **RULE** | Regras determinísticas, sem LLM | DATA_STEP_SIMPLE, PROC_SQL, PROC_SORT, PROC_EXPORT, PROC_IMPORT, LIBNAME |
| **LLM** | Claude API com prompt especializado | DATA_STEP_COMPLEX, PROC_MEANS, PROC_SUMMARY, PROC_FREQ, MACRO_SIMPLE |
| **MANUAL** | Flagged para revisão humana | PROC_FORMAT, PROC_REPORT, HASH_OBJECT, MACRO_DYNAMIC, UNKNOWN |

Blocos MANUAL são preservados como comentário no notebook gerado, com anotação explícita `# WARNING: revisão manual necessária`.

## Componentes principais

| Módulo | Responsabilidade |
|--------|-----------------|
| `sas2dbx/ingest/` | Scan de diretório, leitura e split de blocos SAS |
| `sas2dbx/analyze/` | Classificação de construtos, grafo de dependências |
| `sas2dbx/transpile/` | Engine de transpilação, integração LLM, checkpointing |
| `sas2dbx/generate/` | Geração de notebooks, workflow YAML, relatório de migração |
| `sas2dbx/validate/` | Deploy Databricks, execução de workflows, coleta de resultados |
| `sas2dbx/validate/heal/` | Self-healing: diagnóstico, fix determinístico, retest |
| `sas2dbx/knowledge/` | Knowledge Store: mappings, referências SAS/PySpark, harvester |
| `sas2dbx/web/` | API REST FastAPI para uso via interface gráfica |

## Knowledge Store

O Knowledge Store é a memória técnica do transpilador. Ele armazena:

- **Mappings de funções SAS → PySpark** (`knowledge/mappings/merged/functions_map.yaml`)
- **Documentação de referência** SAS e PySpark em Markdown (`knowledge/sas_reference/`, `knowledge/pyspark_reference/`)
- **Configurações do ambiente** do cliente (`knowledge/custom/libnames.yaml`, `macros.yaml`)

O Knowledge Store é consumido pelo transpilador para enriquecer os prompts enviados ao Claude, reduzindo alucinações nas funções menos comuns.

---

# Instalação e Configuração

## Pré-requisitos

- Python **3.11 ou superior**
- pip 23+
- (Opcional) Conta Anthropic com `ANTHROPIC_API_KEY` para transpilação via LLM
- (Opcional) Workspace Databricks com `DATABRICKS_HOST` e `DATABRICKS_TOKEN` para validação

## Modos de instalação

### Instalação base (apenas CLI de migração)

```bash
pip install sas2dbx
```

### Com servidor web

```bash
pip install "sas2dbx[web]"
```

Inclui: `fastapi`, `uvicorn`, `python-multipart`

### Com suporte Databricks (validação e healing)

```bash
pip install "sas2dbx[databricks]"
```

Inclui: `databricks-sdk>=0.30`

### Instalação completa

```bash
pip install "sas2dbx[all]"
```

### Modo desenvolvimento

```bash
git clone https://github.com/camardelli/sas2dbx.git
cd sas2dbx
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Arquivo de configuração `sas2dbx.yaml`

Crie um arquivo `sas2dbx.yaml` na raiz do projeto para personalizar o comportamento:

```yaml
source:
  path: ./sas_jobs
  encoding: latin1          # latin1 | utf-8 | auto (detecção automática)
  autoexec: ./sas_jobs/autoexec.sas

target:
  platform: databricks
  spark_version: "3.5"
  catalog: main
  schema: migrated
  notebook_format: py       # py | ipynb

knowledge:
  base_path: ./knowledge
  custom:
    libnames: ./knowledge/custom/libnames.yaml
    macros: ./knowledge/custom/macros.yaml
    conventions: ./knowledge/custom/conventions.yaml

llm:
  provider: anthropic       # anthropic | khon
  model: claude-sonnet-4-20250514
  max_tokens_per_call: 4096
  temperature: 0.0
  retry_attempts: 3
  # Para gateway Khon.ai:
  # khon_url: https://gateway.khon.ai
  # khon_token: seu-token-aqui

migration:
  strategy: hybrid          # hybrid | llm_only | rules_only
  confidence_threshold: 0.8
```

## Variáveis de ambiente

| Variável | Obrigatória | Padrão | Descrição |
|----------|-------------|--------|-----------|
| `ANTHROPIC_API_KEY` | Para LLM | — | Chave da API Anthropic |
| `DATABRICKS_HOST` | Para validação | — | URL do workspace (ex: `https://adb-123.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | Para validação | — | Personal Access Token |
| `DATABRICKS_CATALOG` | Não | `main` | Unity Catalog de destino |
| `DATABRICKS_SCHEMA` | Não | `migrated` | Schema de destino |
| `DATABRICKS_NODE_TYPE_ID` | Não | `i3.xlarge` | Tipo de nó do cluster |
| `DATABRICKS_SPARK_VERSION` | Não | `13.3.x-scala2.12` | Databricks Runtime |
| `DATABRICKS_WAREHOUSE_ID` | Não | — | ID do SQL Warehouse existente |
| `SAS2DBX_WORK_DIR` | Não | `./sas2dbx_work` | Diretório de trabalho do servidor web |
| `MAX_UPLOAD_MB` | Não | `100` | Limite de upload de arquivos .zip |

---

# CLI — Referência Completa

## Opções globais

Disponíveis em todos os comandos:

```
sas2dbx [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --verbose     Ativar logging DEBUG
  -q, --quiet       Suprimir output de progresso
  --config PATH     Path para sas2dbx.yaml
  --help            Exibir ajuda
```

## `sas2dbx migrate`

Migra jobs SAS para notebooks Databricks. Exibe inventário de blocos classificados por tier antes de iniciar a transpilação.

```
sas2dbx migrate [OPTIONS] SOURCE_DIR
```

| Argumento/Opção | Tipo | Padrão | Descrição |
|----------------|------|--------|-----------|
| `SOURCE_DIR` | Path | — | Diretório com arquivos `.sas` (ou arquivo único) |
| `--output`, `-o` | Path | — | Diretório de saída dos notebooks gerados |
| `--resume` | Flag | False | Retomar migração interrompida (requer `--output`) |
| `--recursive/--no-recursive` | Flag | True | Busca recursiva de arquivos `.sas` |

**Saída exibida:**

```
Encontrados 3 arquivo(s) .sas

 Inventário de Blocos SAS
┌-----------------------------┬----------------------┬----------┬-------┐
| Arquivo                     | Construct            | Tier     | Conf. |
|------------------------------┼----------------------┼----------┼-------┤
| job_001_clientes.sas        | DATA_STEP_SIMPLE     | RULE     |  0.9  |
| job_001_clientes.sas        | PROC_SQL             | RULE     |  1.0  |
| job_002_vendas.sas          | DATA_STEP_COMPLEX    | LLM      |  0.7  |
| job_003_report.sas          | PROC_REPORT          | MANUAL   |  0.5  |
\------------------------------┴----------------------┴----------┴-------┘

Resumo: 2 RULE · 1 LLM · 1 MANUAL de 4 bloco(s)
```

**Exemplos:**

```bash
# Apenas inventário (sem transpilação)
sas2dbx migrate ./sas_jobs/

# Inventário + transpilação
sas2dbx migrate ./sas_jobs/ --output ./notebooks/

# Retomar migração interrompida
sas2dbx migrate ./sas_jobs/ --output ./notebooks/ --resume
```

## `sas2dbx analyze`

Analisa dependências entre jobs SAS e exibe o grafo de execução e ordem topológica.

```
sas2dbx analyze [OPTIONS] SOURCE_DIR
```

| Argumento/Opção | Tipo | Padrão | Descrição |
|----------------|------|--------|-----------|
| `SOURCE_DIR` | Path | — | Diretório com arquivos `.sas` |
| `--autoexec` | Path | — | Path para `autoexec.sas` com LIBNAMEs globais |
| `--libnames` | Path | — | Path para `libnames.yaml` com `depends_on_jobs` |
| `--recursive/--no-recursive` | Flag | True | Busca recursiva |
| `--order/--no-order` | Flag | True | Exibir ordem de execução sugerida |

**Saída exibida:**

```
Analisando 3 job(s)...

 Jobs SAS — Dependências
┌-----------------┬------------------┬--------------┬--------┬---------┐
| Job             | Inputs           | Outputs      | Macros | Libs    |
|------------------┼------------------┼--------------┼--------┼---------┤
| job_001         | SASDATA.clientes | WORK.output  |        | SASDATA |
| job_002         | WORK.output      | SASDATA.res  |        | SASDATA |
\------------------┴------------------┴--------------┴--------┴---------┘

Dependências implícitas detectadas:
  WARNING job_002 ← WORK.output ← job_001

Ordem de execução sugerida:
  |-- 1. job_001
  \-- 2. job_002
```

**Exemplo:**

```bash
sas2dbx analyze ./sas_jobs/ \
  --autoexec ./sas_jobs/autoexec.sas \
  --libnames ./knowledge/custom/libnames.yaml
```

## `sas2dbx document`

Gera documentação técnica dos jobs SAS via LLM: `README.md` por job, `ARCHITECTURE.md` e `architecture_explorer.html`.

```
sas2dbx document [OPTIONS] SOURCE_DIR
```

| Argumento/Opção | Tipo | Padrão | Descrição |
|----------------|------|--------|-----------|
| `SOURCE_DIR` | Path | — | Diretório com arquivos `.sas` |
| `--output`, `-o` | Path | `./docs` | Diretório de saída da documentação |
| `--format` | str | `all` | Formato de saída: `md`, `html` ou `all` |
| `--provider` | str | `anthropic` | Provider LLM: `anthropic` ou `khon` |
| `--model` | str | — | Modelo LLM (usa padrão do provider se omitido) |
| `--api-key` | str | — | Chave API (usa variável de ambiente se omitida) |
| `--recursive/--no-recursive` | Flag | True | Busca recursiva |

**Saídas geradas:**

```
docs/
|--- ARCHITECTURE.md          # Visão geral da arquitetura do projeto SAS
|--- jobs/
|   |--- job_001_clientes.md  # Documentação de cada job
|   \--- job_002_vendas.md
\--- architecture_explorer.html  # Explorador visual interativo
```

## `sas2dbx serve`

Inicia o servidor web FastAPI para uso via API REST ou integração com frontends.

```
sas2dbx serve [OPTIONS]
```

| Opção | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `--port`, `-p` | int | `8000` | Porta TCP do servidor |
| `--work-dir` | str | `./sas2dbx_work` | Diretório de trabalho para armazenar migrações |
| `--reload` | Flag | False | Auto-reload do servidor (apenas desenvolvimento) |

**Exemplo:**

```bash
sas2dbx serve --port 8080 --work-dir /data/sas2dbx
```

Após iniciar, acesse:
- API: `http://localhost:8000/api/migrations`
- Documentação interativa (Swagger): `http://localhost:8000/api/docs`

## `sas2dbx validate-deploy`

Valida notebooks gerados via deploy no Databricks, execução de workflow e coleta de resultados de tabelas.

```
sas2dbx validate-deploy [OPTIONS] OUTPUT_DIR
```

| Argumento/Opção | Tipo | Env | Padrão | Descrição |
|----------------|------|-----|--------|-----------|
| `OUTPUT_DIR` | Path | — | — | Diretório com notebooks `.py` gerados |
| `--host` | str | `DATABRICKS_HOST` | — | URL do workspace Databricks |
| `--token` | str | `DATABRICKS_TOKEN` | — | Personal Access Token |
| `--catalog` | str | — | `main` | Unity Catalog de destino |
| `--schema` | str | — | `migrated` | Schema de destino |
| `--node-type` | str | — | `i3.xlarge` | Tipo de nó do cluster |
| `--spark-version` | str | — | `13.3.x-scala2.12` | Databricks Runtime |
| `--warehouse-id` | str | `DATABRICKS_WAREHOUSE_ID` | — | SQL Warehouse ID existente |
| `--deploy-only` | Flag | — | False | Apenas deploy, sem execução |
| `--collect-only` | Flag | — | False | Apenas coleta tabelas existentes |
| `--table`, `-t` | str | — | — | Tabela a validar (repita para múltiplas) |
| `--report`, `-r` | Path | — | — | Salvar relatório JSON em arquivo |

**Exemplo:**

```bash
sas2dbx validate-deploy ./notebooks/ \
  --host https://adb-123.azuredatabricks.net \
  --token dapi... \
  --catalog main \
  --schema migrated \
  --table main.migrated.clientes \
  --table main.migrated.vendas \
  --report ./validation_report.json
```

## `sas2dbx knowledge`

Grupo de subcomandos para popular e gerenciar o Knowledge Store.

### `sas2dbx knowledge harvest`

Coleta documentação técnica de fontes externas.

```
sas2dbx knowledge harvest [OPTIONS] SOURCE
```

| Argumento/Opção | Valores | Padrão | Descrição |
|----------------|---------|--------|-----------|
| `SOURCE` | `sas`, `pyspark`, `databricks`, `custom` | — | Fonte a coletar |
| `--mode` | `offline`, `online` | `offline` | Modo de coleta |
| `--version` | str | — | Versão da documentação (ex: `9.4`, `3.5`) |
| `--path`, `-p` | Path | — | Diretório de entrada (modo offline) |
| `--base-path` | Path | `./knowledge` | Raiz do Knowledge Store |

```bash
# Coletar docs SAS 9.4 de HTMLs locais
sas2dbx knowledge harvest sas --mode offline \
  --path ./knowledge/raw_input/sas/

# Coletar configurações do ambiente do cliente
sas2dbx knowledge harvest custom \
  --path ./meu_ambiente/
```

### `sas2dbx knowledge build-mappings`

Gera `mappings/generated/` e faz merge para `mappings/merged/` (curated prevalece sobre generated).

```
sas2dbx knowledge build-mappings [OPTIONS]
```

```bash
sas2dbx knowledge build-mappings --base-path ./knowledge
```

### `sas2dbx knowledge validate`

Verifica integridade do Knowledge Store e gera relatório de cobertura.

```bash
sas2dbx knowledge validate
```

### `sas2dbx knowledge status`

Exibe estatísticas e cobertura atual do Knowledge Store.

```bash
sas2dbx knowledge status
```

### `sas2dbx knowledge update`

Pipeline completo: harvest + build-mappings + validate em sequência.

```
sas2dbx knowledge update [OPTIONS] [SOURCES]...
```

| Opção | Descrição |
|-------|-----------|
| `SOURCES` | Fontes a atualizar: `sas`, `pyspark`, `databricks`, `custom` |
| `--mode` | `offline` ou `online` |
| `--custom-path` | Path para arquivos do ambiente do cliente |
| `--skip-validate` | Pular validação após update |

```bash
# Atualizar todas as fontes
sas2dbx knowledge update sas pyspark --mode offline

# Atualizar apenas configs do cliente
sas2dbx knowledge update custom --custom-path ./novo_ambiente/
```

## `sas2dbx status`

Exibe o status de uma migração em andamento.

```
sas2dbx status MIGRATION_ID
```

---

# API REST — Referência Completa

O servidor web expõe uma API REST no prefixo `/api`. A documentação interativa (Swagger UI) está disponível em `/api/docs`.

## Endpoints de migração

### `POST /api/migrations`

Cria uma nova migração a partir de um arquivo `.zip` contendo os jobs `.sas`.

**Request:** `multipart/form-data`

| Campo | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `file` | File | — | Arquivo `.zip` com os jobs `.sas` |
| `autoexec_filename` | str | `autoexec.sas` | Nome do arquivo autoexec dentro do zip |
| `encoding` | str | `auto` | Encoding dos arquivos SAS |
| `catalog` | str | `main` | Unity Catalog de destino |
| `db_schema` | str | `migrated` | Schema de destino |

**Response `201`:**

```json
{
  "migration_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "created_at": "2026-03-31T10:00:00Z"
}
```

### `GET /api/migrations/`

Lista todas as migrações (mais recente primeiro).

**Response `200`:** Array de `MigrationSummary`

```json
[
  {
    "migration_id": "550e8400...",
    "status": "done",
    "created_at": "2026-03-31T10:00:00Z"
  }
]
```

### `GET /api/migrations/{migration_id}`

Retorna status e progresso por job. Use para polling durante o processamento (sugerido: a cada 3 segundos).

**Response `200`:**

```json
{
  "migration_id": "550e8400...",
  "status": "processing",
  "created_at": "2026-03-31T10:00:00Z",
  "progress": {
    "total": 3, "done": 1, "failed": 0,
    "pending": 2, "in_progress": 0
  },
  "jobs": [
    {"job_id": "job_001", "status": "done", "confidence": 0.92}
  ]
}
```

### `GET /api/migrations/{migration_id}/results`

Retorna resultados completos. Disponível apenas quando `status == "done"`.

### `GET /api/migrations/{migration_id}/explorer`

Retorna o HTML do Architecture Explorer (visualização interativa do grafo de dependências).

### `GET /api/migrations/{migration_id}/download`

Retorna um `.zip` com todos os artefatos gerados: notebooks `.py`, documentação `.md` e `explorer.html`.

## Endpoints de validação Databricks

### `POST /api/config/databricks`

Configura as credenciais Databricks para o pipeline de validação. As credenciais são armazenadas em memória (não persistidas em disco).

**Request:**

```json
{
  "host": "https://adb-123.azuredatabricks.net",
  "token": "dapi...",
  "catalog": "main",
  "schema": "migrated",
  "node_type_id": "i3.xlarge",
  "spark_version": "13.3.x-scala2.12",
  "warehouse_id": null
}
```

**Response `200`:** `DatabricksConfigStatus` (sem o token)

### `GET /api/config/databricks/status`

Retorna o status da configuração atual (sem o token).

**Erro `404`:** Se `POST /api/config/databricks` ainda não foi chamado.

### `POST /api/migrations/{migration_id}/validate`

Dispara o pipeline de validação em background: deploy → execução → coleta de tabelas.

Requer configuração Databricks prévia (`POST /api/config/databricks`) e migração concluída (`status == "done"`).

**Request:**

```json
{
  "tables": ["main.migrated.clientes", "main.migrated.vendas"],
  "deploy_only": false,
  "collect_only": false
}
```

**Response `202`:**

```json
{
  "migration_id": "550e8400...",
  "validation_status": "running"
}
```

### `GET /api/migrations/{migration_id}/validation`

Retorna o status atual da validação (polling).

**Status possíveis:** `pending` | `running` | `done` | `failed`

### `GET /api/migrations/{migration_id}/validation/report`

Retorna o relatório completo de validação. Disponível após `validation_status == "done"`.

```json
{
  "pipeline": {
    "deploy": {"workspace_path": "/Repos/...", "job_id": 123},
    "execution": {"run_id": 456, "status": "SUCCESS", "duration_ms": 5000}
  },
  "summary": {
    "total_tables": 2,
    "tables_ok": 2,
    "tables_error": 0,
    "total_rows_collected": 1500,
    "overall_status": "success"
  },
  "tables": [...]
}
```

## Endpoints de Self-Healing

### `POST /api/migrations/{migration_id}/heal`

Dispara o pipeline de self-healing para um notebook com execução falha.

**Pré-requisitos:**
- Configuração Databricks configurada (`POST /api/config/databricks`)
- `execution_result.status` deve ser `"FAILED"`

**Request:**

```json
{
  "notebook_name": "job_001_clientes",
  "execution_result": {
    "run_id": 456,
    "status": "FAILED",
    "duration_ms": 1200,
    "error": "Table or view not found: main.migrated.clientes_raw"
  },
  "max_iterations": 2
}
```

**Response `202`:**

```json
{
  "healing_id": "6ba7b810...",
  "migration_id": "550e8400...",
  "status": "running"
}
```

### `GET /api/migrations/{migration_id}/heal/{healing_id}`

Retorna o status de um processo de healing (polling).

**Response `200`:**

```json
{
  "healing_id": "6ba7b810...",
  "migration_id": "550e8400...",
  "status": "done",
  "healed": true,
  "iterations": 1,
  "strategy": "deterministic",
  "description": "Created placeholder table: main.migrated.clientes_raw"
}
```

---

# Módulos Internos

## Ingest (`sas2dbx/ingest/`)

### `scanner.py`

Varre recursivamente diretórios em busca de arquivos `.sas`, retornando uma lista de `SasFile`.

### `reader.py`

Lê arquivos `.sas` com detecção automática de encoding (tenta `utf-8-sig` → `utf-8` → `latin-1`, com fallback `replace`). A função `split_blocks()` divide o código em blocos lógicos (DATA, PROC, LIBNAME, %MACRO).

## Analyze (`sas2dbx/analyze/`)

### `classifier.py`

Classifica cada bloco SAS via allowlist `SUPPORTED_CONSTRUCTS`. Blocos não reconhecidos são automaticamente atribuídos a `Tier.MANUAL` — nunca são parseados silenciosamente.

### `dependency.py`

`DependencyAnalyzer` constrói o grafo de dependências entre jobs, resolvendo:
- Entradas/saídas de datasets (DATA step e PROC SQL)
- LIBNAMEs declarados (incluindo `autoexec.sas`)
- Dependências implícitas: job A produz `WORK.X` → job B consome `WORK.X` → A precede B
- Campo `depends_on_jobs` em `libnames.yaml` (dependências explícitas do ambiente)

## Transpile (`sas2dbx/transpile/`)

### `engine.py`

`TranspilationEngine` orquestra a transpilação com checkpointing via `MigrationStateManager`. Suporta `--resume` para retomar migrações interrompidas. Em caso de falha por falta de referência no Knowledge Store, pode acionar on-demand harvest via LLM.

### `llm/client.py`

`LLMClient` abstrai o provider LLM com:
- `AnthropicProvider` — SDK direto
- `KhonGatewayProvider` — HTTP gateway com fallback automático para Anthropic
- Retry com exponential backoff (`retry_attempts` configurável)
- Método síncrono `complete_sync()` para uso em contextos não-assíncronos

### `llm/prompts.py`

Templates de prompt especializados por tipo de construto SAS (DATA step, PROC SQL, PROC MEANS, etc.), com injeção de contexto do Knowledge Store.

## Generate (`sas2dbx/generate/`)

### `notebook.py`

`NotebookGenerator` usa `CellModel` como representação intermediária agnóstica de formato. Renderizadores separados:
- `DatabricksPyRenderer` → formato `.py` com comentários `# COMMAND ----------`
- `JupyterIpynbRenderer` → formato `.ipynb` com cells JSON

### `workflow.py`

Gera manifesto `workflow.yaml`/`workflow.json` para Databricks Workflows, incluindo a DAG de dependências entre tasks.

## Validate (`sas2dbx/validate/`)

### `config.py`

```python
from sas2dbx.validate.config import DatabricksConfig

# A partir de variáveis de ambiente
cfg = DatabricksConfig.from_env()

# Construção manual
cfg = DatabricksConfig(
    host="https://adb-123.azuredatabricks.net",
    token="dapi...",
    catalog="main",
    schema="migrated",
)
```

### `deployer.py`

`DatabricksDeployer.deploy(notebook_path, job_name)` — faz upload do notebook para o workspace e cria/atualiza o Databricks Job. Retorna `DeployResult` com `workspace_path`, `job_id` e `run_id`.

### `executor.py`

`WorkflowExecutor.execute(job_id)` — dispara o job e aguarda conclusão com polling (timeout padrão: 30 minutos). Suporta callback `on_progress(stage, detail)`.

### `collector.py`

`DatabricksCollector.collect(table_names)` — coleta metadados e amostras de tabelas Delta via SQL Warehouse. Se `warehouse_id` não for configurado, busca o primeiro RUNNING ou cria um novo.

## Self-Healing (`sas2dbx/validate/heal/`)

### Pipeline de healing

O Self-Healing Pipeline opera em ciclos iterativos (máx. `max_iterations`, padrão: 2):

```
ExecutionResult (FAILED)
  |
  v
DiagnosticsEngine.diagnose()
  |-- Padrão reconhecido → ErrorDiagnostic com deterministic_fix
  |-- Padrão desconhecido → LLM análise → ErrorDiagnostic
  |
  v
HealingAdvisor.suggest_fix()
  |-- Fix determinístico disponível:
  |     NotebookFixer.apply_fix() → patch no notebook
  |     RetestEngine.retest() → deploy + execute
  |     improved? → FixSuggestion(strategy="deterministic", healed=True)
  |-- Sem fix determinístico + LLM disponível:
  |     FixSuggestion(strategy="llm", llm_suggestion="...")
  \--- Sem fix:
        FixSuggestion(strategy="none")
  |
  v
HealingReport(healed, iterations, suggestion, original_result)
```

### Padrões de erro reconhecidos

| Categoria | Exemplo de erro | Fix determinístico |
|-----------|----------------|-------------------|
| `missing_table` | `Table or view not found: main.schema.t` | `create_placeholder_table` |
| `missing_import` | `ModuleNotFoundError: No module named 'x'` | `add_missing_import` |
| `out_of_memory` | `java.lang.OutOfMemoryError` | `increase_cluster_config` |
| `missing_column` | `cannot resolve col given input columns` | — (LLM fallback) |
| `permissions` | `AccessDeniedException` | — (LLM fallback) |
| `syntax_error` | `ParseException: extraneous input` | — (LLM fallback) |

### Fixes determinísticos

- **`create_placeholder_table`**: Insere `spark.sql("CREATE TABLE IF NOT EXISTS ...")` antes do primeiro bloco de leitura. Cria backup `.py.bak` do notebook original.
- **`add_missing_import`**: Insere `import <module>` após os imports existentes.
- **`increase_cluster_config`**: Adiciona `spark.conf.set("spark.executor.memory", "4g")` ao início do notebook.

## Knowledge Store (`sas2dbx/knowledge/`)

### Estrutura de diretórios

```
knowledge/
|--- sas_reference/           # Documentação SAS processada (.md por PROC/função)
|--- pyspark_reference/       # Documentação PySpark processada
|--- mappings/
|   |--- generated/           # Gerado automaticamente pelo build-mappings (não editar)
|   |--- curated/             # Curadoria manual — NUNCA sobrescrito
|   \--- merged/              # Ground truth: curated > generated (lido pelo transpilador)
|--- custom/
|   |--- libnames.yaml        # LIBNAME → catalog.schema
|   |--- macros.yaml          # Macros corporativas documentadas
|   \--- conventions.yaml     # Naming conventions do ambiente
|--- raw_input/               # Docs brutos para harvest offline
\--- manifest.yaml            # Metadata de versão e cobertura
```

### Arquivo `libnames.yaml`

```yaml
SASDATA:
  catalog: main
  schema: raw
  depends_on_jobs: []    # jobs que devem ser executados antes

SASTEMP:
  catalog: main
  schema: staging
  depends_on_jobs: ["job_001_carga"]
```

### Arquivo `macros.yaml`

```yaml
MACRO_SCD2:
  description: "SCD Type 2 com merge condicional"
  parameters:
    - name: in_ds
      description: "Dataset de entrada"
    - name: out_ds
      description: "Dataset de saída com histórico"
  logic: |
    Realiza merge entre in_ds e out_ds comparando chaves de negócio.
    Fecha registros antigos (dt_fim = today) e insere novos.
```

---

# Fluxo End-to-End

## Fluxo via CLI

```bash
# 1. Preparar estrutura
mkdir -p sas_jobs knowledge/custom

# 2. Configurar variáveis de ambiente
export ANTHROPIC_API_KEY=sk-ant-...
export DATABRICKS_HOST=https://adb-123.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...

# 3. Configurar mapeamentos do ambiente
cat > knowledge/custom/libnames.yaml << EOF
SASDATA:
  catalog: main
  schema: raw
SASTEMP:
  catalog: main
  schema: staging
EOF

# 4. Popular Knowledge Store
sas2dbx knowledge harvest sas --mode offline --path ./knowledge/raw_input/sas/
sas2dbx knowledge build-mappings

# 5. Analisar dependências
sas2dbx analyze ./sas_jobs/ --autoexec ./sas_jobs/autoexec.sas

# 6. Migrar jobs
sas2dbx migrate ./sas_jobs/ --output ./notebooks/

# 7. Validar no Databricks
sas2dbx validate-deploy ./notebooks/ \
  --table main.raw.clientes \
  --report ./validation_report.json
```

## Fluxo via API REST

```bash
# 1. Iniciar servidor
sas2dbx serve --port 8000

# 2. Configurar Databricks
curl -X POST http://localhost:8000/api/config/databricks \
  -H "Content-Type: application/json" \
  -d '{
    "host": "https://adb-123.azuredatabricks.net",
    "token": "dapi...",
    "catalog": "main",
    "schema": "migrated"
  }'

# 3. Upload do zip com jobs SAS
curl -X POST http://localhost:8000/api/migrations \
  -F "file=@./sas_jobs.zip"
# Retorna: {"migration_id": "550e8400...", "status": "pending"}

# 4. Polling até status=done
curl http://localhost:8000/api/migrations/550e8400...

# 5. Disparar validação
curl -X POST http://localhost:8000/api/migrations/550e8400.../validate \
  -H "Content-Type: application/json" \
  -d '{"tables": ["main.migrated.clientes"]}'

# 6. Polling da validação
curl http://localhost:8000/api/migrations/550e8400.../validation

# 7. Download dos artefatos
curl -O http://localhost:8000/api/migrations/550e8400.../download
```

## Saídas geradas

### Notebooks Databricks (`.py`)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # job_001_carga_clientes
# MAGIC Migrado de: job_001_carga_clientes.sas | Confiança: 0.92

# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
# [Bloco 1] DATA step: carga_clientes (linhas 12-38)
df_clientes = (
    spark.read.table("main.raw.clientes_raw")
    .filter(F.col("dt_cadastro") >= "2024-01-01")
    .withColumn("nome_upper", F.upper(F.col("nome")))
)

# COMMAND ----------
df_clientes.write.mode("overwrite").saveAsTable("main.migrated.clientes")
```

### Workflow definition (`workflow.yaml`)

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

### Relatório de migração (`migration_report.json`)

```json
{
  "summary": {
    "total_jobs": 3,
    "fully_migrated": 2,
    "partial": 1,
    "avg_confidence": 0.89
  },
  "jobs": [
    {
      "source": "job_001_carga_clientes.sas",
      "output": "job_001_carga_clientes.py",
      "status": "migrated",
      "confidence": 0.92,
      "warnings": []
    }
  ]
}
```

---

# Implantação

## Docker

O projeto inclui `docker-compose.yml` para implantação containerizada:

```yaml
services:
  sas2dbx:
    build: .
    ports:
      - "8000:8000"
    environment:
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
      DATABRICKS_HOST: ${DATABRICKS_HOST}
      DATABRICKS_TOKEN: ${DATABRICKS_TOKEN}
      MAX_UPLOAD_MB: "200"
      WORK_DIR: /data/sas2dbx_work
    volumes:
      - sas2dbx_work:/data/sas2dbx_work
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/api/openapi.json"]
      interval: 30s

volumes:
  sas2dbx_work:
```

```bash
# Iniciar com Docker Compose
docker-compose up -d

# Ver logs
docker-compose logs -f sas2dbx
```

## Estrutura do diretório de trabalho

Cada migração cria a seguinte estrutura em `{work_dir}/migrations/{uuid}/`:

```
{uuid}/
|--- meta.json           # Metadados: status, config, progresso, validação, healing
|--- upload.zip          # Arquivo zip original
|--- input/              # Arquivos .sas extraídos do zip
|--- output/             # Notebooks .py gerados
|--- docs/
|   |--- ARCHITECTURE.md
|   \--- jobs/           # Documentação por job
\--- explorer.html       # Architecture Explorer interativo
```

---

# Limitações e Roadmap

## Limitações do MVP (v0.1.0)

| Limitação | Impacto | Alternativa |
|-----------|---------|-------------|
| Macros com recursão ou `CALL EXECUTE` | MANUAL flag | Reescrita manual |
| PROC REPORT, PROC TABULATE | MANUAL flag | Databricks SQL Dashboard |
| Hash objects | MANUAL flag | Broadcast join no PySpark |
| Migração de dados (`.sas7bdat`) | Fora do escopo | `spark-sas7bdat`, `saspy` |
| Harvesters offline dependem de docs locais | Knowledge Store incompleto sem docs locais | Fornecer HTMLs de documentação em `raw_input/` |
| LLM necessário para transpilação Tier 2 | Sem `ANTHROPIC_API_KEY`, apenas Tier 1 é transpilado | Adicionar chave API ou usar `--strategy rules_only` |

## Roadmap

- **v0.2.0**: Suporte a PROC TABULATE e PROC REPORT (geração de SQL Dashboard Databricks)
- **v0.3.0**: Migração de dados `sas7bdat` → Delta Lake (integração `spark-sas7bdat`)
- **v1.0.0**: Interface web completa, multi-usuário, com autenticação

---

# Apêndice A — Constructs SAS Suportados

| Construct SAS | Tier | Confidence | Equivalente PySpark |
|---------------|------|-----------|---------------------|
| `DATA step` simples (SET, WHERE, KEEP, DROP) | RULE | 0.9 | DataFrame API (filter, select) |
| `PROC SQL` | RULE | 1.0 | `spark.sql()` |
| `PROC SORT` | RULE | 1.0 | `.orderBy()`, `.dropDuplicates()` |
| `LIBNAME` | RULE | 0.9 | `spark.read.table()` (Unity Catalog) |
| `PROC EXPORT` (CSV) | RULE | 0.9 | `.write.csv()` |
| `PROC IMPORT` (CSV) | RULE | 0.9 | `spark.read.csv()` |
| `DATA step` complexo (arrays, retain, by-group) | LLM | 0.7 | DataFrame API + Window |
| `PROC MEANS` / `PROC SUMMARY` | LLM | 0.7 | `.groupBy().agg()` |
| `PROC FREQ` | LLM | 0.7 | `.groupBy().count()` |
| `%MACRO` simples (parâmetros fixos) | LLM | 0.6 | Função Python |
| Invocação de macro (`%MY_MACRO`) | LLM | 0.6 | Chamada de função Python |
| `PROC FORMAT` | MANUAL | 0.5 | Lookup table / UDF |
| `PROC REPORT` | MANUAL | 0.5 | SQL Dashboard |
| Hash objects | MANUAL | 0.5 | Broadcast join |
| Macros dinâmicas (`%SYSFUNC`, `CALL EXECUTE`) | MANUAL | 0.3 | Reescrita manual |

# Apêndice B — Mapeamento de funções SAS → PySpark

| Função SAS | Equivalente PySpark | Observações |
|-----------|---------------------|-------------|
| `INTCK('MONTH', d1, d2)` | `months_between(d2, d1)` | Ordem dos argumentos invertida |
| `INTNX('MONTH', d, n)` | `add_months(d, n)` | Apenas interval MONTH |
| `PUT(col, format)` | `cast()` / `date_format()` | Depende do formato |
| `INPUT(col, informat)` | `cast(col as type)` | Para informats numéricos |
| `CATX(delim, ...)` | `concat_ws(delim, ...)` | CATX remove espaços automaticamente |
| `COMPRESS(col)` | `regexp_replace(col, ' ', '')` | Sem 2º arg = remove espaços |
| `SUBSTR(col, pos, len)` | `substring(col, pos, len)` | Ambos são 1-indexed |
| `SCAN(col, n, delim)` | `split(col, delim)[n-1]` | SAS 1-indexed, array 0-indexed |
| `MONOTONIC()` | `row_number() OVER (ORDER BY ...)` | Ou `monotonically_increasing_id()` |
| `DATEPART(dt)` | `cast(dt as date)` | |

# Apêndice C — Exemplo de `sas2dbx.yaml` completo

```yaml
source:
  path: ./sas_jobs
  encoding: latin1
  autoexec: ./sas_jobs/autoexec.sas

target:
  platform: databricks
  spark_version: "3.5"
  catalog: main
  schema: migrated
  notebook_format: py

knowledge:
  base_path: ./knowledge
  harvest:
    sas_version: "9.4"
    sas_docs_source: offline
    sas_docs_path: ./knowledge/raw_input/sas/
    pyspark_version: "3.5.1"
    databricks_topics:
      - unity-catalog
      - workflows
      - delta-lake
  custom:
    libnames: ./knowledge/custom/libnames.yaml
    macros: ./knowledge/custom/macros.yaml
    conventions: ./knowledge/custom/conventions.yaml
  context_injection:
    max_tokens: 2000
    include_examples: true
    include_gotchas: true

llm:
  provider: anthropic
  model: claude-sonnet-4-20250514
  max_tokens_per_call: 4096
  temperature: 0.0
  retry_attempts: 3

migration:
  strategy: hybrid
  confidence_threshold: 0.8
  generate_tests: true
  preserve_comments: true
```
