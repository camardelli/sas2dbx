# Knowledge Store — Curadoria e Ambiente do Cliente

## Estrutura de Mappings

O Knowledge Store usa uma estrutura de 3 camadas para mapeamentos YAML:

```
knowledge/mappings/
├── generated/    ← SOBRESCRITO pelo build-mappings (nunca editar manualmente)
├── curated/      ← NUNCA sobrescrito — curadoria manual tem precedência absoluta
└── merged/       ← Ground truth: curated > generated (gerado automaticamente)
```

### Regra fundamental

> **`curated/` nunca é sobrescrito.** Qualquer chave presente em `curated/` vence a
> correspondente em `generated/` no arquivo `merged/`. Execute `sas2dbx knowledge build-mappings`
> para re-gerar `merged/` após editar `curated/`.

### Fluxo de curadoria

1. Rode `sas2dbx knowledge build-mappings` para popular `generated/` e `merged/`
2. Para corrigir um mapeamento: edite (ou crie) o arquivo correspondente em `curated/`
3. Rode `sas2dbx knowledge build-mappings` novamente — `merged/` será atualizado com sua curadoria
4. Valide: `sas2dbx knowledge validate`

**Exemplo:** para corrigir o mapeamento de `INTCK`:

```yaml
# curated/functions_map.yaml
INTCK:
  pyspark: "months_between({arg2}, {arg1})"
  notes: "Ordem dos args invertida. Para DAY usar datediff, para YEAR usar floor(months/12)."
  variants:
    MONTH: "months_between({arg2}, {arg1})"
    DAY: "datediff({arg2}, {arg1})"
    YEAR: "floor(months_between({arg2}, {arg1}) / 12)"
  confidence: 1.0
  curated: true
```

---

## Arquivos do Ambiente do Cliente

### `libnames.yaml` — Mapeamento de Bibliotecas SAS

```yaml
# Formato básico
SASDATA:
  catalog: "main"
  schema: "raw"

SASTEMP:
  catalog: "main"
  schema: "staging"

# Com dependências explícitas entre jobs (Story 2.1)
SASMIGRATED:
  catalog: "main"
  schema: "migrated"
  depends_on_jobs:
    - "job_001_carga_clientes"
```

### `macros.yaml` — Macros Corporativas

```yaml
MACRO_SCD2:
  description: "Aplica SCD Type 2 via merge condicional"
  params:
    - name: "src_table"
      type: "string"
      description: "Tabela de origem"
    - name: "tgt_table"
      type: "string"
      description: "Tabela de destino"
  pyspark_equivalent: "Ver docs/patterns/scd2_pattern.py"
  tier: "manual"
```

### `conventions.yaml` — Convenções e Regras

```yaml
naming:
  dataset_prefix:
    raw: "raw_"
    staging: "stg_"
    final: ""
  job_prefix: "job_"

encoding: "latin1"  # ou "utf-8"

date_format: "%d%b%Y"  # formato SAS padrão da empresa
```

---

## Adicionar Arquivos do Cliente

```bash
# Copiar arquivos de configuração para knowledge/custom/
sas2dbx knowledge harvest --source custom --path ./meu_ambiente/

# Ou copiar manualmente para esta pasta
cp libnames.yaml knowledge/custom/libnames.yaml
```
