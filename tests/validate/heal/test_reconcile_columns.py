"""Testes para StaticNotebookValidator._reconcile_columns_with_schema().

Cobre:
- Alias SAS determinístico (vl_ → valor_, nm_ → nome, tp_ → tipo_, cd_ → id_)
- Similaridade de string >= 0.85 (typos óbvios)
- Preservação de colunas derivadas (withColumn outputs)
- Não-substituição com similaridade baixa
- Múltiplas tabelas de origem
- SQL strings (spark.sql())
- Zero false-positives em colunas que já existem no schema
"""

from __future__ import annotations

import pytest

from sas2dbx.validate.heal.static_validator import StaticNotebookValidator


# Helper para instanciar o validator sem config de catálogo
def _validator() -> StaticNotebookValidator:
    return StaticNotebookValidator(catalog="telcostar", schema="operacional")


def _schema(*names: str) -> list[dict]:
    """Helper: converte nomes de coluna em list[dict] no novo formato."""
    return [{"name": n, "type": "STRING"} for n in names]


# Schema mínimo que replica o ambiente TelcoStar
VENDAS_SCHEMA = _schema(
    "id_venda", "id_cliente", "id_plano", "id_vendedor",
    "dt_venda", "valor_bruto", "desconto_pct",
    "canal_venda", "tipo_transacao", "dt_primeira_compra",
)

CLIENTES_SCHEMA = _schema(
    "id_cliente", "nome", "cpf", "email",
    "telefone", "cidade", "uf", "dt_adesao",
    "dt_nascimento", "status", "id_vendedor_responsavel",
)


def _reconcile(code: str, schemas: dict) -> tuple[str, str | None]:
    """Chama o método interno diretamente."""
    v = _validator()
    return v._reconcile_columns_with_schema(code, schemas)


# ----------------------------------------------------------------
# Alias SAS — substituições determinísticas
# ----------------------------------------------------------------

def test_vl_prefix_no_match_warns_only():
    """vl_venda: alias expande para valor_venda (não existe) → sem substituição.
    O caso vl_→valor_ correto requer que o schema tenha valor_venda.
    """
    code = (
        'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df = df.withColumn("x", F.col("vl_venda") * 2)\n'
    )
    flat_schemas = {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA}
    result, fix = _reconcile(code, flat_schemas)
    # vl_venda → valor_venda (não existe), similaridade 0.75 < 0.85 → não substitui
    assert 'F.col("vl_venda")' in result
    assert fix is None


def test_nm_prefix_expands_to_nome():
    """nm_cliente → nome via alias nm_ → 'nome'."""
    code = (
        'df = spark.read.table("telcostar.operacional.clientes_raw")\n'
        'df = df.select(F.col("nm_cliente"))\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.clientes_raw": CLIENTES_SCHEMA})
    assert 'F.col("nome")' in result
    assert 'F.col("nm_cliente")' not in result
    assert fix is not None
    assert "nm_cliente" in fix


def test_tp_prefix_expands_to_tipo():
    """tp_transacao → tipo_transacao via alias tp_ → tipo_."""
    code = (
        'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df = df.filter(F.col("tp_transacao") == "CREDITO")\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    assert 'F.col("tipo_transacao")' in result
    assert 'F.col("tp_transacao")' not in result
    assert fix is not None


def test_cd_prefix_expands_to_id():
    """cd_cliente → id_cliente via alias cd_ → id_."""
    code = (
        'df = spark.read.table("telcostar.operacional.clientes_raw")\n'
        'df = df.select(F.col("cd_cliente"))\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.clientes_raw": CLIENTES_SCHEMA})
    assert 'F.col("id_cliente")' in result
    assert fix is not None


# ----------------------------------------------------------------
# Similaridade de string
# ----------------------------------------------------------------

def test_similarity_typo_corrected():
    """desconto_prc → desconto_pct (score >= 0.85) → substitui."""
    code = (
        'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df = df.select(F.col("desconto_prc"))\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    assert 'F.col("desconto_pct")' in result
    assert fix is not None


def test_similarity_low_score_no_substitution():
    """xyz_abc: similaridade muito baixa → não substitui."""
    code = (
        'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df = df.select(F.col("xyz_abc"))\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    assert 'F.col("xyz_abc")' in result
    assert fix is None


# ----------------------------------------------------------------
# Exceções — não reconciliar
# ----------------------------------------------------------------

def test_existing_columns_untouched():
    """Colunas que existem no schema não devem ser alteradas."""
    code = (
        'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df = df.select(F.col("valor_bruto"), F.col("dt_venda"))\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    assert fix is None
    assert result == code


def test_withcolumn_output_not_reconciled():
    """Colunas criadas por withColumn são outputs — não reconciliar."""
    code = (
        'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df = df.withColumn("score_rfm", F.col("valor_bruto") * 2)\n'
        'df = df.select(F.col("score_rfm"))\n'   # score_rfm é output — não existe no schema
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    # score_rfm foi registrado como output do withColumn — não deve ser reconciliado
    assert 'F.col("score_rfm")' in result


def test_alias_output_not_reconciled():
    """Colunas renomeadas por alias não são inputs — não reconciliar."""
    code = (
        'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df = df.select(F.col("valor_bruto").alias("vl_venda_alias"))\n'
        'df2 = df.select(F.col("vl_venda_alias"))\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    # vl_venda_alias foi criado pelo alias() — não deve ser substituído
    assert 'F.col("vl_venda_alias")' in result


def test_underscore_prefix_columns_skipped():
    """Colunas com _ no início são internas do Spark — não reconciliar."""
    code = (
        'df = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df = df.select(F.col("_rn"), F.col("_seq"))\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    assert fix is None


# ----------------------------------------------------------------
# Múltiplas tabelas
# ----------------------------------------------------------------

def test_multiple_tables_reconcile():
    """Reconcilia colunas de múltiplas tabelas de origem."""
    code = (
        'df1 = spark.read.table("telcostar.operacional.vendas_raw")\n'
        'df2 = spark.read.table("telcostar.operacional.clientes_raw")\n'
        'df = df1.join(df2, "id_cliente")\n'
        'df = df.select(F.col("nm_cliente"), F.col("tp_transacao"))\n'
    )
    schemas = {
        "telcostar.operacional.vendas_raw": VENDAS_SCHEMA,
        "telcostar.operacional.clientes_raw": CLIENTES_SCHEMA,
    }
    result, fix = _reconcile(code, schemas)
    assert 'F.col("nome")' in result           # nm_cliente → nome
    assert 'F.col("tipo_transacao")' in result  # tp_transacao → tipo_transacao
    assert fix is not None
    assert "nm_cliente" in fix
    assert "tp_transacao" in fix


# ----------------------------------------------------------------
# SQL strings
# ----------------------------------------------------------------

def test_sql_string_reconciliation():
    """Reconcilia colunas dentro de spark.sql() triple-quoted strings."""
    code = (
        'df = spark.sql("""\n'
        '    SELECT id_cliente, tp_transacao\n'
        '    FROM telcostar.operacional.vendas_raw\n'
        '""")\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    assert "tipo_transacao" in result
    assert "tp_transacao" not in result


def test_sql_qualified_column_not_replaced():
    """Nomes qualificados com tabela (v.col) não são substituídos."""
    code = (
        'df = spark.sql("""\n'
        '    SELECT v.id_cliente, v.tp_transacao\n'
        '    FROM telcostar.operacional.vendas_raw v\n'
        '""")\n'
    )
    result, fix = _reconcile(code, {"telcostar.operacional.vendas_raw": VENDAS_SCHEMA})
    # v.tp_transacao → qualificado, o regex exige que não haja ponto antes → não substitui
    # mas tp_transacao sem qualificação seria substituído
    # Este teste verifica que v.tp_transacao permanece intacto
    assert "v.tp_transacao" in result


# ----------------------------------------------------------------
# Sem schemas → no-op
# ----------------------------------------------------------------

def test_empty_schemas_noop():
    """Sem schemas → retorna conteúdo original sem alterações."""
    code = 'df = df.select(F.col("nm_cliente"))\n'
    result, fix = _reconcile(code, {})
    assert result == code
    assert fix is None


def test_no_tables_in_notebook_noop():
    """Sem spark.read.table() → nenhuma tabela de origem → noop."""
    code = 'df = df.select(F.col("nm_cliente"))\n'
    schemas = {"telcostar.operacional.clientes_raw": CLIENTES_SCHEMA}
    result, fix = _reconcile(code, schemas)
    # Sem read.table no notebook, não há tabelas de origem identificadas
    assert result == code
    assert fix is None
