# Arquitetura do Projeto SAS Migrado
## Inventário de Jobs

| Job | Status | Confiança |
|-----|--------|-----------|
| `job_complexo_analise_churn` | done | 86% |
## Grafo de Dependências

```mermaid
graph LR
  job_complexo_analise_churn[job_complexo_analise_churn]
```
## Mapa de Datasets

| Dataset | Produzido por | Consumido por |
|---------|--------------|---------------|
| `STAGING` | `job_complexo_analise_churn` | `job_complexo_analise_churn` |
| `WORK.FREQ_CHURN_RISCO` | `job_complexo_analise_churn` | — |
| `WORK.FREQ_PLANO_RISCO` | `job_complexo_analise_churn` | — |
| `WORK.FREQ_RISCO` | `job_complexo_analise_churn` | `job_complexo_analise_churn` |
| `WORK.PRE_PIVOT` | — | `job_complexo_analise_churn` |
## Mapa de Macros

| Macro | Definida em | Chamada em |
|-------|------------|------------|
| `AGREGA_METRICAS` | `job_complexo_analise_churn` | — |
| `CALC_CHURN_SCORE` | `job_complexo_analise_churn` | — |
| `CARGA_DW` | `job_complexo_analise_churn` | — |
| `GERA_FREQUENCIAS` | `job_complexo_analise_churn` | — |
| `IDENTIFICA_EVENTOS_CHURN` | `job_complexo_analise_churn` | — |
| `TRANSPOE_METRICAS` | `job_complexo_analise_churn` | — |
## Cobertura de Migração

| Métrica | Valor |
|---------|-------|
| Total de jobs | 1 |
| Migrados automaticamente | 1 (100%) |
| Falharam | 0 |
| Não migrados | 0 |
| Confiança média (jobs DONE) | 86% |