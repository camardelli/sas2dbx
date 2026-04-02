"""ReconciliationGenerator — gera notebooks de validação de equivalência.

Seção 3 + 10.3 do roteiro humano: cada migração deve deixar testes de
reconciliação prontos para comparar a saída SAS com a saída Databricks.

Para cada job migrado, gera {job_name}_reconciliation.py com:
  - Row count comparison
  - Soma de colunas numéricas
  - Distinct key counts
  - Anti-join (linhas presentes em um lado mas não no outro)
  - Verificação de integridade de caracteres especiais

Os notebooks gerados são parametrizáveis via widgets Databricks:
  - sas_table: tabela SAS exportada para Delta (baseline)
  - dbx_table: tabela gerada pelo notebook migrado
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ReconciliationConfig:
    """Configuração do gerador de reconciliação."""

    catalog: str = "main"
    schema: str = "migrated"
    include_antijoin: bool = True
    include_charset_check: bool = True


class ReconciliationGenerator:
    """Gera notebooks de reconciliação para validação de equivalência pós-migração."""

    def __init__(self, config: ReconciliationConfig | None = None) -> None:
        self._config = config or ReconciliationConfig()

    def generate(
        self,
        notebook_path: Path,
        output_dir: Path,
    ) -> Path | None:
        """Gera notebook de reconciliação para um notebook migrado.

        Analisa o notebook migrado para extrair tabelas de saída (saveAsTable)
        e gera queries de validação correspondentes.

        Args:
            notebook_path: Notebook .py migrado.
            output_dir: Diretório onde gravar o notebook de reconciliação.

        Returns:
            Path do notebook gerado, ou None se não houver tabelas de saída.
        """
        try:
            content = notebook_path.read_text(encoding="utf-8")
        except OSError:
            return None

        output_tables = self._extract_output_tables(content)
        numeric_cols = self._extract_numeric_columns(content)
        key_cols = self._extract_key_columns(content)

        if not output_tables:
            return None

        recon_code = self._build_notebook(
            job_name=notebook_path.stem,
            output_tables=output_tables,
            numeric_cols=numeric_cols,
            key_cols=key_cols,
        )

        out_path = output_dir / f"{notebook_path.stem}_reconciliation.py"
        out_path.write_text(recon_code, encoding="utf-8")
        return out_path

    def generate_directory(self, output_dir: Path) -> list[Path]:
        """Gera notebooks de reconciliação para todos os notebooks em output_dir."""
        generated = []
        for nb in sorted(output_dir.glob("*.py")):
            if nb.stem.endswith("_reconciliation"):
                continue
            result = self.generate(nb, output_dir)
            if result:
                generated.append(result)
        return generated

    # ------------------------------------------------------------------
    # Internal — extraction
    # ------------------------------------------------------------------

    def _extract_output_tables(self, content: str) -> list[str]:
        """Extrai tabelas escritas via saveAsTable."""
        pattern = re.compile(
            r'\.saveAsTable\(\s*["\']([^"\']+)["\']\s*\)',
            re.IGNORECASE,
        )
        return list(dict.fromkeys(m.group(1) for m in pattern.finditer(content)))

    def _extract_numeric_columns(self, content: str) -> list[str]:
        """Extrai colunas numéricas inferidas pelo nome (vl_, qt_, nr_)."""
        pattern = re.compile(
            r'["\'](\b(?:vl|qt|nr|valor|total|sum|avg|media|count)_\w+)["\']',
            re.IGNORECASE,
        )
        return list(dict.fromkeys(m.group(1) for m in pattern.finditer(content)))[:10]

    def _extract_key_columns(self, content: str) -> list[str]:
        """Extrai colunas candidatas a chave (id_, cd_, nr_)."""
        pattern = re.compile(
            r'["\'](\b(?:id|cd|nr|key|chave)_\w+)["\']',
            re.IGNORECASE,
        )
        return list(dict.fromkeys(m.group(1) for m in pattern.finditer(content)))[:5]

    # ------------------------------------------------------------------
    # Internal — notebook generation
    # ------------------------------------------------------------------

    def _build_notebook(
        self,
        job_name: str,
        output_tables: list[str],
        numeric_cols: list[str],
        key_cols: list[str],
    ) -> str:
        cfg = self._config
        sections = [self._header(job_name)]

        for table in output_tables:
            sections.append(self._section_row_count(table))
            if numeric_cols:
                sections.append(self._section_numeric_sums(table, numeric_cols))
            if key_cols:
                sections.append(self._section_distinct_keys(table, key_cols))
            if cfg.include_antijoin and key_cols:
                sections.append(self._section_antijoin(table, key_cols))
            if cfg.include_charset_check:
                sections.append(self._section_charset(table))
            sections.append(self._section_summary(table))

        return "\n\n# COMMAND ----------\n\n".join(sections)

    def _header(self, job_name: str) -> str:
        return f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Reconciliação: {job_name}
# MAGIC
# MAGIC Notebook gerado automaticamente pelo sas2dbx para validação de equivalência.
# MAGIC
# MAGIC **Como usar:**
# MAGIC 1. Exporte a saída do job SAS para uma tabela Delta (`sas_baseline_table`)
# MAGIC 2. Execute o notebook migrado (`{job_name}.py`) para gerar `dbx_table`
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
'''

    def _section_row_count(self, table: str) -> str:
        return f'''# MAGIC %md ## 1. Row Count — {table}

if sas_table and dbx_table:
    sas_count = spark.table(sas_table).count()
    dbx_count = spark.table(dbx_table).count()
    diff      = dbx_count - sas_count
    pct_diff  = abs(diff / sas_count * 100) if sas_count > 0 else 0

    status = "✓ OK" if pct_diff < 0.01 else f"✗ DIVERGÊNCIA {{pct_diff:.4f}}%"
    print(f"Row count — SAS: {{sas_count:,}} | DBX: {{dbx_count:,}} | Diff: {{diff:+,}} | {{status}}")
    results.append({{"check": "row_count", "table": "{table}", "status": status,
                     "sas": sas_count, "dbx": dbx_count, "pct_diff": pct_diff}})
else:
    print("AVISO: sas_baseline_table ou dbx_table não configurados — pulando row count")
'''

    def _section_numeric_sums(self, table: str, cols: list[str]) -> str:
        col_lines = "\n".join(
            f'    F.sum(F.col("{c}")).alias("{c}"),' for c in cols
        )
        col_checks = "\n".join(
            f'    sas_v = sas_agg["{c}"] or 0\n'
            f'    dbx_v = dbx_agg["{c}"] or 0\n'
            f'    pct = abs((dbx_v - sas_v) / sas_v * 100) if sas_v != 0 else 0\n'
            f'    st = "✓" if pct < 0.01 else f"✗ {{pct:.4f}}%"\n'
            f'    print(f"  SUM({c}) — SAS: {{sas_v:,.2f}} | DBX: {{dbx_v:,.2f}} | {{st}}")\n'
            f'    results.append({{"check": "sum_{c}", "table": "{table}", "status": st}})'
            for c in cols
        )
        return f'''# MAGIC %md ## 2. Somas Numéricas — {table}

if sas_table and dbx_table:
    agg_cols = [
{col_lines}
    ]
    try:
        sas_agg = spark.table(sas_table).agg(*agg_cols).first().asDict()
        dbx_agg = spark.table(dbx_table).agg(*agg_cols).first().asDict()
{col_checks}
    except Exception as e:
        print(f"AVISO: erro ao calcular somas — {{e}}")
'''

    def _section_distinct_keys(self, table: str, key_cols: list[str]) -> str:
        key_list = ", ".join(f'"{c}"' for c in key_cols)
        return f'''# MAGIC %md ## 3. Distinct Keys — {table}

if sas_table and dbx_table:
    try:
        sas_dist = spark.table(sas_table).select({key_list}).distinct().count()
        dbx_dist = spark.table(dbx_table).select({key_list}).distinct().count()
        diff = dbx_dist - sas_dist
        status = "✓ OK" if diff == 0 else f"✗ DIVERGÊNCIA {{diff:+,}} chaves"
        print(f"Distinct keys {{[{key_list}]}} — SAS: {{sas_dist:,}} | DBX: {{dbx_dist:,}} | {{status}}")
        results.append({{"check": "distinct_keys", "table": "{table}", "status": status}})
    except Exception as e:
        print(f"AVISO: erro ao verificar distinct keys — {{e}}")
'''

    def _section_antijoin(self, table: str, key_cols: list[str]) -> str:
        join_cond = " & ".join(
            f'F.col("sas.{c}") == F.col("dbx.{c}")' for c in key_cols
        )
        key_list = ", ".join(f'"{c}"' for c in key_cols)
        return f'''# MAGIC %md ## 4. Anti-Join (linhas divergentes) — {table}

if sas_table and dbx_table:
    try:
        sas_df = spark.table(sas_table).select({key_list}).alias("sas")
        dbx_df = spark.table(dbx_table).select({key_list}).alias("dbx")

        in_sas_not_dbx = sas_df.join(dbx_df, {join_cond}, "left_anti").count()
        in_dbx_not_sas = dbx_df.join(sas_df, {join_cond}, "left_anti").count()

        status = "✓ OK" if in_sas_not_dbx == 0 and in_dbx_not_sas == 0 else "✗ DIVERGÊNCIA"
        print(f"Anti-join — Em SAS mas não DBX: {{in_sas_not_dbx:,}} | Em DBX mas não SAS: {{in_dbx_not_sas:,}} | {{status}}")
        results.append({{"check": "antijoin", "table": "{table}", "status": status,
                         "in_sas_not_dbx": in_sas_not_dbx, "in_dbx_not_sas": in_dbx_not_sas}})

        if in_sas_not_dbx > 0:
            print("Amostra de linhas em SAS mas não em DBX:")
            sas_df.join(dbx_df, {join_cond}, "left_anti").show(5)
    except Exception as e:
        print(f"AVISO: erro no anti-join — {{e}}")
'''

    def _section_charset(self, table: str) -> str:
        return f'''# MAGIC %md ## 5. Integridade de Caracteres Especiais — {table}

if dbx_table:
    try:
        # Detecta colunas STRING com potencial corrupção de encoding
        df = spark.table(dbx_table)
        str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"][:5]

        if str_cols:
            sample_col = str_cols[0]
            # Verifica presença de caracteres de substituição (U+FFFD = encoding corrompido)
            corrupt = df.filter(F.col(sample_col).contains("\\ufffd")).count()
            status = "✓ OK" if corrupt == 0 else f"✗ {{corrupt}} registro(s) com encoding corrompido"
            print(f"Charset check (col: {{sample_col}}) — {{status}}")
            results.append({{"check": "charset", "table": "{table}", "status": status}})
        else:
            print("Charset check — nenhuma coluna STRING encontrada")
    except Exception as e:
        print(f"AVISO: erro no charset check — {{e}}")
'''

    def _section_summary(self, table: str) -> str:
        return f'''# MAGIC %md ## Sumário — {table}

print("\\n" + "="*60)
print(f"SUMÁRIO DE RECONCILIAÇÃO — {table}")
print("="*60)
ok     = [r for r in results if "✓" in str(r.get("status", ""))]
failed = [r for r in results if "✗" in str(r.get("status", ""))]
print(f"  ✓ Aprovados : {{len(ok)}}")
print(f"  ✗ Falhas    : {{len(failed)}}")
if failed:
    for r in failed:
        print(f"    - {{r['check']}}: {{r['status']}}")
overall = "APROVADO" if not failed else "REPROVADO"
print(f"\\nResultado final: {{overall}}")
print("="*60)
'''
