"""Pré-validação estática de notebooks antes do deploy.

Detecta e corrige padrões problemáticos conhecidos sem necessidade de
executar no Databricks — zero custo de cluster, zero latência de polling.

Padrões corrigidos:
  - spark.conf.get() sem default → valor padrão hardcoded (CONFIG_NOT_AVAILABLE)
  - Catálogo errado (diverge do DatabricksConfig) → replace em massa
  - Blocos AUTO-FIX acumulados de iterações anteriores → remoção
  - Imports duplicados → deduplica
  - catalog.schema.mode("overwrite").saveAsTable() → df_var.write.mode() (LLM bug)
  - WARNING blocks de macro invocação → chamada Python quando função já definida
  - .write.mode("overwrite") sem overwriteSchema → adiciona option preventivo
  - ORDER_COL placeholder → coluna inferida do groupBy/orderBy do notebook
  - stack() com tipos mistos → CAST(col AS DOUBLE) preventivo
  - F.lit(int).otherwise(F.col(str)) → F.lit("int") para unificação de tipo
  - MONOTONIC() → ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id())
  - F.col("flag_col") == 1 → F.col("flag_col") == "1" (colunas categóricas)
  - Literais de data SAS '01JAN2025'd → date('2025-01-01')
  - Literais datetime SAS '01JAN2025:00:00:00'dt → timestamp('2025-01-01 00:00:00')
  - func("param=valor") → func(param=valor) (kwarg serializado como string pelo LLM)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Regex para padrão broken write: catalog.schema.mode(...) — detecta o prefixo
_RE_BROKEN_WRITE = re.compile(
    r'^(\s*)(\w+)\.(\w+)\.mode\(["\'](\w+)["\']\)\.saveAsTable\(',
    re.MULTILINE,
)
# Falsos positivos conhecidos do _fix_broken_write_calls
_BROKEN_WRITE_FALSE_POSITIVES = frozenset(
    ("F", "spark", "df", "Window", "pyspark", "re", "os", "logging",
     "write", "read", "sql", "conf", "streams")
)
# Regex para encontrar último assignment de variável (linha top-level)
_RE_VAR_ASSIGN = re.compile(r'^([A-Za-z_]\w*)\s*=\s*')
# Regex para detectar WARNING blocks de macro invocação não resolvida
_RE_MACRO_WARNING = re.compile(
    r'# WARNING: \[MACRO_INVOCATION\][^\n]*\n'
    r'# SAS original: %(\w+)\(([^)]+)\);',
)
# Regex para extrair definições de funções Python no notebook
_RE_FUNC_DEF = re.compile(r'^def (\w+)\s*\(', re.MULTILINE)

# Regex para spark.conf.get com default
_RE_CONF_GET_DEFAULT = re.compile(
    r'spark\.conf\.get\(\s*["\'][\w.]+["\']\s*,\s*(["\'][^"\']+["\'])\s*\)'
)
# Regex para spark.conf.get sem default
_RE_CONF_GET_NO_DEFAULT = re.compile(
    r'spark\.conf\.get\(\s*["\'][\w.]+["\']\s*\)'
)
# Linhas de AUTO-FIX e CREATE TABLE/SCHEMA adicionados pelo healing.
# NOTA: # [AUTO-FIX D3] é preservado — contém withColumn em memória (seguro e necessário).
# Somente blocos ALTER TABLE acumulados (tag [AUTO-FIX] simples) são removidos.
_RE_AUTOFIX_LINE = re.compile(
    r"^\s*(?:# \[AUTO-FIX\](?! D3)|spark\.sql\(\"CREATE (?:TABLE|SCHEMA) IF NOT EXISTS)"
)


@dataclass
class StaticFixReport:
    """Relatório de correções aplicadas pelo validador estático.

    Attributes:
        notebook: Nome do notebook corrigido.
        fixes: Lista de descrições das correções aplicadas.
    """

    notebook: str
    fixes: list[str] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return len(self.fixes) > 0


# Mapeamento SAS month abbreviations → número (para fix_sas_date_literals)
_SAS_MONTH_MAP: dict[str, str] = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}
# Padrão de literal de data SAS: '01JAN2025'd ou "01JAN2025"d
_RE_SAS_DATE_LIT = re.compile(
    r"""['"](0?\d|[12]\d|3[01])([A-Za-z]{3})(\d{4})['"]d""",
    re.IGNORECASE,
)
# Padrão de literal datetime SAS: '01JAN2025:00:00:00'dt
_RE_SAS_DATETIME_LIT = re.compile(
    r"""['"](0?\d|[12]\d|3[01])([A-Za-z]{3})(\d{4}):(\d{2}:\d{2}:\d{2})['"]dt""",
    re.IGNORECASE,
)
# Padrão MONOTONIC() em SQL — função SAS não suportada pelo Spark
_RE_MONOTONIC = re.compile(r"\bMONOTONIC\(\)", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Column reconciliation — constantes de módulo
# ---------------------------------------------------------------------------

# Prefixos de nomenclatura SAS → equivalentes Databricks/PySpark.
# Formato:
#   "sas_prefix_": "expanded_prefix_"  — substitui prefixo, mantém sufixo
#   "sas_prefix_": "name"              — substitui pelo nome inteiro (sem sufixo)
# Extensível: adicionar entradas conforme convenções do cliente.
SAS_COLUMN_ALIASES: dict[str, str] = {
    "vl_":  "valor_",       # vl_venda    → valor_venda,  vl_total → valor_total
    "nm_":  "nome",         # nm_cliente  → nome  (tenta tb nome_<sufixo>)
    "cd_":  "id_",          # cd_cliente  → id_cliente
    "nr_":  "num_",         # nr_pedido   → num_pedido
    "ds_":  "descricao_",   # ds_plano    → descricao_plano
    "fl_":  "flag_",        # fl_ativo    → flag_ativo
    "qt_":  "qtd_",         # qt_vendas   → qtd_vendas
    "tp_":  "tipo_",        # tp_transacao → tipo_transacao
    "sg_":  "sigla_",       # sg_uf       → sigla_uf
    "pc_":  "pct_",         # pc_desconto → pct_desconto
    "in_":  "ind_",         # in_ativo    → ind_ativo
    "sk_":  "id_",          # sk_cliente  → id_cliente  (surrogate keys)
}

# Coluna referenciada via DataFrame API: F.col("name") ou col("name")
_RE_FCOL_REF = re.compile(r'\bF\.col\(\s*["\']([A-Za-z_]\w*)["\']')
_RE_COL_REF  = re.compile(r'(?<![.\w])col\(\s*["\']([A-Za-z_]\w*)["\']')

# Colunas de OUTPUT: criadas pelo notebook, não são inputs a reconciliar
_RE_WITHCOL_OUT  = re.compile(r'\.withColumn\(\s*["\']([A-Za-z_]\w*)["\']')
_RE_ALIAS_OUT    = re.compile(r'\.alias\(\s*["\']([A-Za-z_]\w*)["\']')
_RE_RENAME_NEW   = re.compile(
    r'\.withColumnRenamed\(\s*["\'][^"\']+["\']\s*,\s*["\']([A-Za-z_]\w*)["\']'
)

# Literais que não são nomes de coluna (F.lit("valor"))
_RE_LIT_STR = re.compile(r'\bF\.lit\(\s*["\']([^"\']*)["\']')

# Tabelas lidas no notebook: spark.read.table / spark.table
_RE_NB_READ_TABLE = re.compile(
    r'spark\.(?:read\.table|table)\(\s*["\']([a-zA-Z0-9_.]+)["\']\s*\)'
)
# Tabelas em SQL inline: FROM / JOIN fqn
_RE_NB_SQL_TABLE = re.compile(
    r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+){1,2})',
    re.IGNORECASE,
)

# Colunas em SQL strings (spark.sql): SELECT col1, col2 FROM ...
# Captura identificadores sem ponto antes de FROM/WHERE/GROUP/ORDER/HAVING/LIMIT
_RE_SQL_SELECT_COL = re.compile(
    r'SELECT\s+(.*?)\s+FROM',
    re.IGNORECASE | re.DOTALL,
)
_RE_SQL_COL_TOKEN = re.compile(r'\b([A-Za-z_]\w*)(?:\s+AS\s+\w+)?(?=\s*(?:,|\s+FROM))', re.IGNORECASE)


class StaticNotebookValidator:
    """Valida e corrige notebooks .py antes do deploy no Databricks.

    Deve ser invocado uma vez por sessão de validação, antes do primeiro
    deploy, para eliminar erros previsíveis sem custo de cluster.

    Args:
        catalog: Catálogo Unity Catalog correto (ex: "telcostar").
        schema: Schema correto (ex: "operacional").
        healing_history: Lista de entradas de histórico de healing de sessões
            anteriores (lida de meta.json → "healing_history"). Quando fornecida,
            permite que o validator aplique proativamente fixes que já foram
            necessários em runs anteriores, sem esperar o erro ocorrer novamente.
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        healing_history: list[dict] | None = None,
    ) -> None:
        self._catalog = catalog
        self._schema = schema
        # GAP-6: mapa fix_key → notebooks onde esse fix já foi necessário.
        # Usado em validate_notebook para priorizar a ordem de aplicação dos
        # fixers e emitir logs diagnósticos proativos por notebook.
        self._history_fix_keys: dict[str, set[str]] = {}
        for entry in (healing_history or []):
            fix_key = entry.get("fix_key")
            notebook = entry.get("notebook", "")
            if fix_key:
                self._history_fix_keys.setdefault(fix_key, set()).add(notebook)

    def validate_directory(self, notebook_dir: Path) -> list[StaticFixReport]:
        """Valida e corrige todos os notebooks .py em um diretório.

        Args:
            notebook_dir: Diretório contendo notebooks gerados.

        Returns:
            Lista de StaticFixReport, um por notebook modificado.
        """
        reports = []
        for nb in sorted(notebook_dir.glob("*.py")):
            report = self.validate_notebook(nb)
            if report.changed:
                reports.append(report)
                logger.info(
                    "StaticValidator: %s — %d correção(ões): %s",
                    nb.name,
                    len(report.fixes),
                    "; ".join(report.fixes),
                )
            else:
                logger.debug("StaticValidator: %s — sem correções necessárias", nb.name)
        return reports

    def validate_notebook(self, notebook_path: Path) -> StaticFixReport:
        """Valida e corrige um único notebook.

        Args:
            notebook_path: Caminho do arquivo .py.

        Returns:
            StaticFixReport com as correções aplicadas (vazio se nenhuma).
        """
        report = StaticFixReport(notebook=notebook_path.name)
        content = notebook_path.read_text(encoding="utf-8")
        original = content

        # GAP-6: emite log diagnóstico quando padrões de erro conhecidos do histórico
        # de healing se aplicam a este notebook — orienta o revisor sobre o que esperar.
        notebook_stem = notebook_path.stem
        known_for_this_nb = [
            fix_key
            for fix_key, notebooks in self._history_fix_keys.items()
            if not notebooks or notebook_stem in notebooks
        ]
        if known_for_this_nb:
            logger.info(
                "StaticValidator: %s — histórico de healing indica padrões conhecidos: %s",
                notebook_path.name,
                ", ".join(known_for_this_nb),
            )

        _fixers = [
            self._remove_autofix_blocks,
            self._fix_kwarg_as_string,           # deve rodar cedo — afeta SQL gerado
            self._fix_spark_conf_get,
            self._fix_wrong_catalog,
            self._fix_broken_write_calls,
            self._fix_macro_invocation_blocks,
            self._fix_overwrite_schema,
            self._fix_order_col_placeholder,
            self._fix_stack_type_cast,
            self._fix_when_otherwise_type,
            self._fix_monotonic_function,       # GAP-8a
            self._fix_numeric_string_comparison, # GAP-8b
            self._fix_sas_date_literals,         # GAP-8c
            self._fix_alter_table_if_not_exists, # sintaxe inválida no Databricks SQL
            self._deduplicate_imports,
        ]
        for fixer_fn in _fixers:
            try:
                content, fix = fixer_fn(content)
                if fix:
                    report.fixes.append(fix)
            except Exception as exc:  # noqa: BLE001
                # Um fixer quebrado não deve interromper os demais.
                # O notebook permanece no estado anterior a essa tentativa.
                logger.warning(
                    "StaticValidator: fixer %s falhou em %s — ignorado: %s",
                    fixer_fn.__name__,
                    notebook_path.name,
                    exc,
                )

        # Reconciliação de colunas com schema real — rodada após todos os outros fixers.
        # Auto-carrega schemas.yaml da mesma pasta do notebook (gerado pelo preflight).
        schemas = self._load_schemas_for_notebook(notebook_path)
        if schemas:
            content, fix = self._reconcile_columns_with_schema(content, schemas)
            if fix:
                report.fixes.append(fix)

        if content != original:
            notebook_path.write_text(content, encoding="utf-8")

        return report

    def _load_schemas_for_notebook(
        self, notebook_path: Path
    ) -> dict[str, list[str]]:
        """Carrega schemas.yaml co-localizado com os notebooks (gerado pelo preflight)."""
        schemas_path = notebook_path.parent / "schemas.yaml"
        if not schemas_path.exists():
            return {}
        try:
            from sas2dbx.transpile.llm.context import load_table_schemas
            return load_table_schemas(schemas_path)
        except Exception as exc:  # noqa: BLE001
            logger.debug("StaticValidator: não foi possível carregar schemas.yaml: %s", exc)
            return {}

    # ------------------------------------------------------------------
    # Fixers internos
    # ------------------------------------------------------------------

    # Padrão: func("param=valor") — kwarg serializado como string pelo LLM.
    # Exemplos problemáticos:
    #   gera_cohorts("meses_retro=18", spark=spark)  → gera_cohorts(meses_retro=18, spark=spark)
    #   calcular_saldo("taxa=0.05", spark=spark)     → calcular_saldo(taxa=0.05, spark=spark)
    # Causa TypeError: bad operand type ou UNRESOLVED_COLUMN quando o valor é
    # usado em f-strings SQL (Spark lê o nome do parâmetro como nome de coluna).
    _RE_KWARG_AS_STRING = re.compile(
        r'(\w+)\(\s*["\']([A-Za-z_]\w*=[\d.]+)["\']\s*(,)',
    )

    def _fix_kwarg_as_string(self, content: str) -> tuple[str, str | None]:
        """Converte func("param=valor") → func(param=valor).

        O LLM transpilador às vezes converte `%macro(param=18)` para
        `func("param=18", spark=spark)` — o argumento keyword é embutido
        em uma string em vez de ser passado como kwarg Python.

        Quando esse valor é depois usado em um f-string SQL (ex:
        `f"SELECT ... -{meses_retro} ..."`) o Spark recebe a string
        `"meses_retro=18"` como valor e ao tentar interpolá-la em SQL
        a expressa como literal ou coluna não resolvida.

        Fix: desembala o par key=value da string e o injeta como kwarg.
        """
        matches = list(self._RE_KWARG_AS_STRING.finditer(content))
        if not matches:
            return content, None

        fixes: list[str] = []

        def _unwrap(m: re.Match) -> str:
            func_name = m.group(1)
            kwarg_pair = m.group(2)   # ex: "meses_retro=18"
            trailing_comma = m.group(3)
            fixes.append(f'{func_name}("{kwarg_pair}") → {func_name}({kwarg_pair})')
            return f'{func_name}({kwarg_pair}{trailing_comma}'

        new_content = self._RE_KWARG_AS_STRING.sub(_unwrap, content)

        if new_content == content:
            return content, None

        return new_content, f"kwarg-como-string corrigido em {len(fixes)} chamada(s): {'; '.join(fixes)}"

    def _remove_autofix_blocks(self, content: str) -> tuple[str, str | None]:
        """Remove blocos AUTO-FIX acumulados de iterações de healing anteriores."""
        lines = content.split("\n")
        clean: list[str] = []
        removed = 0
        skip_next_blank = False

        for line in lines:
            if _RE_AUTOFIX_LINE.match(line):
                removed += 1
                skip_next_blank = True
                continue
            if skip_next_blank and line.strip() == "":
                skip_next_blank = False
                continue
            skip_next_blank = False
            clean.append(line)

        if removed:
            return "\n".join(clean), f"Removidos {removed} bloco(s) AUTO-FIX"
        return content, None

    # ------------------------------------------------------------------
    # Column reconciliation
    # ------------------------------------------------------------------

    def _extract_referenced_tables(self, content: str) -> list[str]:
        """Extrai FQNs de tabelas lidas no notebook (não escritas)."""
        # Tabelas em saveAsTable / CREATE TABLE — são writes, excluir
        write_tables: set[str] = set()
        for m in re.finditer(r'saveAsTable\(\s*["\']([^"\']+)["\']\s*\)', content):
            t = m.group(1).lower()
            write_tables.add(t)
            write_tables.add(t.split(".")[-1])
        for m in re.finditer(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?'
            r'([`"\']?[\w]+(?:\.[\w]+){0,2}[`"\']?)',
            content, re.IGNORECASE,
        ):
            t = m.group(1).strip("`\"'").lower()
            write_tables.add(t)
            write_tables.add(t.split(".")[-1])

        read_tables: set[str] = set()
        for m in _RE_NB_READ_TABLE.finditer(content):
            t = m.group(1).lower()
            if t not in write_tables and t.split(".")[-1] not in write_tables:
                read_tables.add(t)
        for m in _RE_NB_SQL_TABLE.finditer(content):
            t = m.group(1).lower()
            if t not in write_tables and t.split(".")[-1] not in write_tables and "." in t:
                read_tables.add(t)
        return list(read_tables)

    def _expand_sas_prefix(self, col: str, real_columns: set[str]) -> str | None:
        """Tenta expandir prefixo SAS e encontrar match exato no schema.

        Ex: vl_venda → valor_venda (não encontrado) → None
            nm_cliente → nome (encontrado!) → 'nome'
            tp_transacao → tipo_transacao (encontrado!) → 'tipo_transacao'
        """
        col_lower = col.lower()
        for sas_prefix, expanded in SAS_COLUMN_ALIASES.items():
            if not col_lower.startswith(sas_prefix):
                continue
            suffix = col_lower[len(sas_prefix):]
            # Caso "expanded_": substituição de prefixo, mantém sufixo
            if expanded.endswith("_"):
                candidate = expanded + suffix
                if candidate in real_columns:
                    return candidate
            else:
                # Caso "name" sem trailing _: tenta nome direto e nome_sufixo
                if expanded in real_columns:
                    return expanded
                if suffix and (expanded + "_" + suffix) in real_columns:
                    return expanded + "_" + suffix
        return None

    def _find_best_column_match(
        self, col: str, real_columns: set[str]
    ) -> tuple[str, str] | None:
        """Retorna (replacement, method) ou None.

        Precedência:
        1. Expansão de prefixo SAS (determinística — alta confiança)
        2. SequenceMatcher >= 0.85 (conservador — só typos óbvios)
        Avisa mas não substitui para 0.65–0.85.
        """
        from difflib import SequenceMatcher

        # 1. Alias de prefixo SAS
        expanded = self._expand_sas_prefix(col, real_columns)
        if expanded:
            return expanded, "alias SAS"

        # 2. Similaridade de string (threshold conservador)
        col_lower = col.lower()
        best_score = 0.0
        best_match = ""
        for real_col in real_columns:
            score = SequenceMatcher(None, col_lower, real_col.lower()).ratio()
            if score > best_score:
                best_score = score
                best_match = real_col

        if best_score >= 0.85:
            return best_match, f"similaridade={best_score:.2f}"
        if best_score >= 0.65:
            logger.warning(
                "StaticValidator: coluna suspeita '%s' (melhor match: '%s' score=%.2f)"
                " — não substituída automaticamente, revisão manual recomendada",
                col, best_match, best_score,
            )
        return None

    def _reconcile_columns_with_schema(
        self, content: str, schemas: dict[str, list[dict]]
    ) -> tuple[str, str | None]:
        """Reconcilia nomes de coluna inventados pelo LLM contra o schema real.

        Roda após todos os outros fixers, antes do deploy.
        Substitui apenas colunas referenciadas em F.col() / col() e SELECT SQL
        que não existem no schema — nunca toca colunas derivadas (withColumn outputs).

        Args:
            content: Conteúdo do notebook .py.
            schemas: Dict FQN → list[col_name] (de schemas.yaml).

        Returns:
            (new_content, fix_description) onde fix_description é None se sem alterações.
        """
        if not schemas:
            return content, None

        # 1. Tabelas referenciadas no notebook
        referenced_tables = self._extract_referenced_tables(content)
        if not referenced_tables:
            return content, None

        # 2. Pool de colunas reais (de todas as tabelas de origem)
        real_columns: set[str] = set()
        col_to_table: dict[str, str] = {}
        for table_fqn in referenced_tables:
            short = table_fqn.split(".")[-1].lower()
            cols = schemas.get(table_fqn, schemas.get(short, []))
            for col in cols:
                col_name = col["name"] if isinstance(col, dict) else str(col)
                real_columns.add(col_name.lower())
                col_to_table[col_name.lower()] = table_fqn.split(".")[-1]

        if not real_columns:
            return content, None

        # 3. Colunas de output (não reconciliar — são outputs criados pelo notebook)
        output_columns: set[str] = set()
        for m in _RE_WITHCOL_OUT.finditer(content):
            output_columns.add(m.group(1).lower())
        for m in _RE_ALIAS_OUT.finditer(content):
            output_columns.add(m.group(1).lower())
        for m in _RE_RENAME_NEW.finditer(content):
            output_columns.add(m.group(1).lower())
        # Literais F.lit("string") não são nomes de coluna
        lit_values: set[str] = {m.group(1).lower() for m in _RE_LIT_STR.finditer(content)}

        # 4. Colunas de input referenciadas via DataFrame API e SQL strings
        input_refs: set[str] = set()
        for m in _RE_FCOL_REF.finditer(content):
            input_refs.add(m.group(1))
        for m in _RE_COL_REF.finditer(content):
            input_refs.add(m.group(1))
        # Extrai colunas de SELECT dentro de spark.sql("""...""")
        _SQL_KEYWORDS = frozenset({
            "select", "from", "where", "join", "on", "and", "or", "not",
            "group", "by", "order", "having", "limit", "as", "distinct",
            "inner", "left", "right", "outer", "cross", "case", "when",
            "then", "else", "end", "null", "true", "false", "in", "is",
            "between", "like", "cast", "coalesce", "count", "sum", "avg",
            "min", "max", "over", "partition", "rows", "unbounded",
            "preceding", "following", "current", "row",
        })
        for sql_m in re.finditer(r'spark\.sql\(\s*f?"""(.*?)"""\s*\)', content, re.DOTALL):
            sql_body = sql_m.group(1)
            # Identifica colunas não-qualificadas (sem ponto antes): palavras que não
            # são keywords SQL, funções conhecidas, ou nomes de tabela com ponto
            for tok_m in re.finditer(r'(?<![.\w])([A-Za-z_]\w*)(?!\s*\()', sql_body):
                tok = tok_m.group(1)
                if tok.lower() not in _SQL_KEYWORDS and len(tok) >= 3:
                    input_refs.add(tok)

        # 5. Determinar substituições
        replacements: dict[str, str] = {}  # original_case → replacement
        for col in input_refs:
            col_lower = col.lower()
            if (col_lower in real_columns          # já existe
                    or col_lower in output_columns  # é output derivado
                    or col_lower in lit_values       # é literal string
                    or col.startswith("_")           # coluna interna Spark
                    or len(col) < 3):                # muito curto / ambíguo
                continue
            match = self._find_best_column_match(col, real_columns)
            if match:
                replacements[col] = match[0]  # (replacement, method)
                # Guarda method para o log — precisamos recalcular
                # (reutiliza a tupla completa via closure abaixo)

        # Recalcula com método para log estruturado
        replacements_full: dict[str, tuple[str, str]] = {}
        for col in input_refs:
            col_lower = col.lower()
            if (col_lower in real_columns
                    or col_lower in output_columns
                    or col_lower in lit_values
                    or col.startswith("_")
                    or len(col) < 3):
                continue
            match = self._find_best_column_match(col, real_columns)
            if match:
                replacements_full[col] = match  # (replacement, method)

        if not replacements_full:
            return content, None

        # 6. Aplicar substituições em F.col() e col() patterns
        new_content = content
        applied: list[str] = []
        for original, (replacement, method) in sorted(replacements_full.items()):
            original_re = re.escape(original)
            # F.col("original") → F.col("replacement")
            pattern_f = re.compile(r'(\bF\.col\(\s*["\'])' + original_re + r'(["\'])')
            new_content, n1 = pattern_f.subn(
                lambda m, r=replacement: m.group(1) + r + m.group(2),
                new_content,
            )
            # col("original") → col("replacement")
            pattern_c = re.compile(r'((?<![.\w])col\(\s*["\'])' + original_re + r'(["\'])')
            new_content, n2 = pattern_c.subn(
                lambda m, r=replacement: m.group(1) + r + m.group(2),
                new_content,
            )
            # SQL strings: SELECT/WHERE unqualified identifiers
            new_content, n3 = self._reconcile_in_sql_strings(
                new_content, original, replacement, real_columns
            )
            if n1 + n2 + n3 > 0:
                table_hint = col_to_table.get(replacement.lower(), "?")
                applied.append(
                    f"'{original}' → '{replacement}' ({method}, tabela: {table_hint})"
                )

        if not applied:
            return content, None

        logger.info(
            "StaticValidator: reconciliação de colunas — %d substituição(ões): %s",
            len(applied), "; ".join(applied),
        )
        return new_content, f"reconciliação de colunas ({len(applied)}): {'; '.join(applied)}"

    def _reconcile_in_sql_strings(
        self,
        content: str,
        original: str,
        replacement: str,
        real_columns: set[str],
    ) -> tuple[str, int]:
        """Substitui `original` por `replacement` dentro de spark.sql() strings.

        Só substitui identificadores não-qualificados (sem ponto antes) para evitar
        falsos positivos em nomes de tabelas, aliases SQL, e keywords.
        """
        # Encontra blocos spark.sql("""...""") e spark.sql("...")
        _RE_SQL_BLOCK = re.compile(
            r'(spark\.sql\(\s*(?:f?""")(.*?)(?:""")\s*\))',
            re.DOTALL,
        )
        count = 0
        original_re = re.escape(original)
        # Substitui apenas se a palavra não é precedida por ponto (qualificação de tabela)
        col_in_sql = re.compile(r'(?<![.\w])' + original_re + r'\b')

        def _replace_sql_block(m: re.Match) -> str:
            nonlocal count
            prefix = m.group(1)[: m.start(2) - m.start(1)]   # "spark.sql(f\"\"\""
            sql_body = m.group(2)
            new_body, n = col_in_sql.subn(replacement, sql_body)
            count += n
            suffix = m.group(1)[m.end(2) - m.start(1):]       # "\"\"\"\)"
            return prefix + new_body + suffix

        new_content = _RE_SQL_BLOCK.sub(_replace_sql_block, content)
        return new_content, count

    def _fix_spark_conf_get(self, content: str) -> tuple[str, str | None]:
        """Substitui spark.conf.get() por valor padrão (evita CONFIG_NOT_AVAILABLE)."""
        # Com default: spark.conf.get("KEY", "val") → "val"
        matches_default = _RE_CONF_GET_DEFAULT.findall(content)
        if matches_default:
            content = _RE_CONF_GET_DEFAULT.sub(lambda m: m.group(1), content)
            return content, f"spark.conf.get() com default substituído ({len(matches_default)}x)"

        # Sem default: spark.conf.get("KEY") → "" com WARNING
        if _RE_CONF_GET_NO_DEFAULT.search(content):
            content = _RE_CONF_GET_NO_DEFAULT.sub(
                '"" # WARNING: valor não disponível — definir manualmente', content
            )
            return content, "spark.conf.get() sem default substituído por string vazia"

        return content, None

    def _fix_wrong_catalog(self, content: str) -> tuple[str, str | None]:
        """Substitui referências ao catálogo/schema errado pelo correto.

        Só inspeciona padrões dentro de strings entre aspas para evitar
        falsos positivos com código Python (ex: df2.write.mode).
        """
        fixes: list[str] = []

        # Detecta catalog.schema DENTRO de strings (aspas simples ou duplas)
        # Padrão: "catalog.schema.table" ou 'catalog.schema.table'
        string_pattern = re.compile(r'["\']([a-zA-Z]\w*)\.([a-zA-Z]\w*)\.\w')

        wrong_pairs: set[tuple[str, str]] = set()
        for m in string_pattern.finditer(content):
            cat, sch = m.group(1), m.group(2)
            if (cat, sch) == (self._catalog, self._schema):
                continue
            # Filtra falsos positivos conhecidos
            if cat in ("F", "spark", "df", "Window", "pyspark", "re", "os", "logging", "write", "read"):
                continue
            if len(cat) > 2:
                wrong_pairs.add((cat, sch))

        for wrong_cat, wrong_sch in wrong_pairs:
            # Substitui somente dentro de strings (entre aspas)
            old = f"{wrong_cat}.{wrong_sch}."
            new = f"{self._catalog}.{self._schema}."
            # Usa regex para substituir apenas quando precedido por aspas
            sub_pattern = re.compile(
                r'(?<=["\'])' + re.escape(wrong_cat) + r'\.' + re.escape(wrong_sch) + r'\.'
            )
            new_content, count = sub_pattern.subn(f"{self._catalog}.{self._schema}.", content)
            if count:
                content = new_content
                fixes.append(f"{old}* → {new}* ({count}x)")

        return content, ("; ".join(fixes) if fixes else None)

    def _fix_macro_invocation_blocks(self, content: str) -> tuple[str, str | None]:
        """Substitui WARNING blocks de macro por chamada real à função Python já definida.

        O LLM às vezes define a função Python corretamente mas ao encontrar a
        invocação da macro (`%func_name(LIB.t1, LIB.t2)`) gera um bloco WARNING
        comentado em vez de chamar a função que acabou de definir.

        Detecta: `# WARNING: [MACRO_INVOCATION]` + `# SAS original: %func_name(...)`.
        Se `func_name` está definido no notebook como `def func_name(`, substitui
        o bloco (até o próximo `# COMMAND`) por `func_name("t1", "t2", spark=spark)`.
        """
        # Funções definidas no notebook
        defined_funcs = set(_RE_FUNC_DEF.findall(content))
        if not defined_funcs:
            return content, None

        fixed: list[str] = []
        cell_sep = "# COMMAND ----------"

        for m in _RE_MACRO_WARNING.finditer(content):
            func_name = m.group(1)
            if func_name not in defined_funcs:
                continue

            # Extrai parâmetros: "STAGING.vendas_running, STAGING.vendas_enriquecidas"
            raw_params = [p.strip() for p in m.group(2).split(",")]
            # Remove prefixo de library SAS (ex: STAGING., LIB.)
            clean_params = []
            for p in raw_params:
                clean_params.append(p.split(".")[-1] if "." in p else p)

            # Determina extensão do bloco: do início do WARNING até o próximo COMMAND
            block_start = m.start()
            # Retrocede até o início da linha
            line_start = content.rfind("\n", 0, block_start) + 1

            # Avança até o próximo # COMMAND (célula seguinte)
            next_cmd = content.find(cell_sep, m.end())
            if next_cmd == -1:
                continue
            block_end = next_cmd  # o COMMAND pertence à próxima célula

            params_str = ", ".join(f'"{p}"' for p in clean_params)
            call_str = (
                f"# [AUTO-FIX] Invocação da macro %{func_name} convertida\n"
                f"{func_name}({params_str}, spark=spark)\n"
            )

            content = content[:line_start] + call_str + content[block_end:]
            fixed.append(f"Macro %{func_name}({', '.join(clean_params)}) → chamada Python")
            # Reprocess após substituição (só um WARNING por vez para simplicidade)
            break

        return content, ("; ".join(fixed) if fixed else None)

    def _fix_broken_write_calls(self, content: str) -> tuple[str, str | None]:
        """Corrige padrão LLM onde catalog.schema.mode() foi gerado em vez de df.write.mode().

        Detecta: `catalog.schema.mode("overwrite").saveAsTable("catalog.schema.table")`
        Corrige: `last_assigned_var.write.mode("overwrite").saveAsTable("catalog.schema.table")`

        A variável correta é inferida olhando para trás na busca pelo último assignment
        de variável no escopo top-level.
        """
        lines = content.splitlines(keepends=True)
        fixed_count = 0

        for i, line in enumerate(lines):
            m = _RE_BROKEN_WRITE.match(line)
            if not m:
                continue
            indent, cat, sch, mode = m.groups()

            # Filtra falsos positivos: variáveis Python legítimas ou métodos write legítimos
            if cat in _BROKEN_WRITE_FALSE_POSITIVES or sch in _BROKEN_WRITE_FALSE_POSITIVES:
                continue

            # Confirma que saveAsTable referencia o mesmo catalog.schema
            # (cobre literal "cat.sch.table" e f-string f"cat.sch.{var}")
            cat_sch_prefix = f"{cat}.{sch}."
            if cat_sch_prefix not in line:
                continue

            # Busca backwards pelo último assignment de variável
            last_var: str | None = None
            for j in range(i - 1, -1, -1):
                stripped = lines[j].lstrip()
                am = _RE_VAR_ASSIGN.match(stripped)
                if am and not lines[j].startswith(" " * 8):
                    last_var = am.group(1)
                    break

            if not last_var:
                continue

            old_prefix = f"{cat}.{sch}.mode("
            new_prefix = f"{last_var}.write.mode("
            if old_prefix in lines[i]:
                lines[i] = lines[i].replace(old_prefix, new_prefix, 1)
                fixed_count += 1
                logger.debug(
                    "StaticValidator: corrigido write call na linha %d: %s → %s",
                    i + 1, old_prefix, new_prefix,
                )

        if fixed_count:
            return "".join(lines), f"Corrigido {fixed_count} write call(s) incorreto(s) (catalog.schema.mode → df.write.mode)"
        return content, None

    def _fix_order_col_placeholder(self, content: str) -> tuple[str, str | None]:
        """Substitui placeholders de coluna de ordenação gerados pelo LLM.

        O LLM gera `ORDER_COL = "coluna_ordem"` quando não sabe qual coluna
        usar para ordenar o LAG/Window. Detecta esse padrão e infere a coluna
        correta olhando para o groupBy da tabela de origem no mesmo notebook.

        Padrão detectado: variável com valor contendo "coluna_ordem" ou
        qualquer bloco com comentário `# SUBSTITUA "coluna_xxx"`.
        """
        # Detecta placeholder: ORDER_COL = "coluna_ordem" com comentário SUBSTITUA
        placeholder_pattern = re.compile(
            r'(ORDER_COL\s*=\s*["\'])coluna_\w+(["\'])',
        )
        substitua_pattern = re.compile(
            r'#\s*SUBSTITUA\s*["\']?(coluna_\w+)["\']?',
        )

        has_placeholder = placeholder_pattern.search(content) or substitua_pattern.search(content)
        if not has_placeholder:
            return content, None

        # Infere a coluna de ordenação: procura o groupBy mais recente no notebook
        # "coluna de ordem" em tabelas mensais/temporais → tipicamente a coluna de período
        groupby_pattern = re.compile(
            r'\.groupBy\(\s*F\.col\(["\'](\w+)["\']\)|\.groupBy\(["\'](\w+)["\']\)',
        )
        orderby_pattern = re.compile(
            r'\.orderBy\(\s*F\.col\(["\'](\w+)["\']|\.orderBy\(["\'](\w+)["\']\)',
        )

        candidates: list[str] = []
        for m in groupby_pattern.finditer(content):
            col = m.group(1) or m.group(2)
            if col:
                candidates.append(col)
        for m in orderby_pattern.finditer(content):
            col = m.group(1) or m.group(2)
            if col and col not in candidates:
                candidates.append(col)

        if not candidates:
            return content, None

        # Usa a primeira coluna encontrada (tipicamente a mais relevante para ordenação)
        inferred_col = candidates[0]

        # Aplica substituições
        fixes: list[str] = []
        new_content = placeholder_pattern.sub(
            lambda m: f'{m.group(1)}{inferred_col}{m.group(2)}', content
        )
        if new_content != content:
            fixes.append(f"ORDER_COL placeholder → \"{inferred_col}\"")
            content = new_content

        # Também substitui literais "coluna_ordem" / 'coluna_ordem' restantes
        for m in substitua_pattern.finditer(content):
            placeholder = m.group(1)
            old_q = f'"{placeholder}"'
            old_sq = f"'{placeholder}'"
            if old_q in content or old_sq in content:
                count = content.count(old_q) + content.count(old_sq)
                content = content.replace(old_q, f'"{inferred_col}"').replace(old_sq, f"'{inferred_col}'")
                fixes.append(f'"{placeholder}" → "{inferred_col}" ({count}x)')
                break  # uma passagem por vez

        return content, ("; ".join(fixes) if fixes else None)

    def _fix_stack_type_cast(self, content: str) -> tuple[str, str | None]:
        """Adiciona CAST(col AS DOUBLE) nas colunas de valor de expressões stack().

        stack() do Spark SQL (usado em PROC TRANSPOSE) falha com
        DATATYPE_MISMATCH.STACK_COLUMN_DIFF_TYPES quando as colunas de valor
        têm tipos diferentes (ex: DOUBLE e BIGINT).

        Solução preventiva: casteia todas as colunas de valor para DOUBLE.
        """
        if "stack(" not in content:
            return content, None

        # Detecta blocos stack(...) em strings multiline (selectExpr, spark.sql)
        stack_block_pattern = re.compile(
            r'(stack\(\d+,)(.*?)(\)\s*as\s*\([^)]+\))',
            re.DOTALL | re.IGNORECASE,
        )
        # Padrão de par label,value dentro do stack: 'label', col_name
        pair_pattern = re.compile(
            r"('[^'\"]+'\s*,\s*)(\b(?!CAST\b)([a-zA-Z_]\w*))\b"
        )

        def add_cast(m: re.Match) -> str:
            label_part = m.group(1)
            col_name = m.group(2)
            return f"{label_part}CAST({col_name} AS DOUBLE)"

        def fix_block(sm: re.Match) -> str:
            return sm.group(1) + pair_pattern.sub(add_cast, sm.group(2)) + sm.group(3)

        new_content = stack_block_pattern.sub(fix_block, content)

        if new_content != content:
            return new_content, "CAST(... AS DOUBLE) adicionado em stack() (PROC TRANSPOSE type uniformization)"
        return content, None

    def _fix_when_otherwise_type(self, content: str) -> tuple[str, str | None]:
        """Corrige type mismatch em F.when(..., F.lit(N)).otherwise(F.col(...)).

        Quando F.lit(integer) e F.col(string_col) são combinados em when().otherwise(),
        Spark tenta unificar tipos casteando a string para BIGINT → CAST_INVALID_INPUT.

        Fix: converte F.lit(N) → F.lit("N") (string) para type compatibility.
        Aplica APENAS quando o literal inteiro aparece como valor em when().otherwise()
        seguido diretamente de .otherwise(F.col ou F.lit(string).
        """
        pattern = re.compile(
            r'(F\.lit\()(\d+)(\)\)\.otherwise\((?:F\.col|F\.lit)\()'
        )
        matches = list(pattern.finditer(content))
        if not matches:
            return content, None

        new_content = pattern.sub(
            lambda m: f'{m.group(1)}"{m.group(2)}"{m.group(3)}', content
        )
        return new_content, f"F.lit(integer) → F.lit(\"str\") em {len(matches)} when().otherwise() (CAST_INVALID_INPUT prevention)"

    def _fix_overwrite_schema(self, content: str) -> tuple[str, str | None]:
        """Adiciona .option("overwriteSchema", "true") em writes de overwrite.

        Evita _LEGACY_ERROR_TEMP_DELTA_0007 quando a tabela já existe com schema
        diferente (ex: placeholder criado pelo auto-healing com (id, _placeholder)).

        Transforma:
          df.write.mode("overwrite").saveAsTable(...)
        em:
          df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(...)
        """
        pattern = re.compile(
            r'\.write\.mode\(["\']overwrite["\']\)\.saveAsTable\(',
        )
        option_marker = '.option("overwriteSchema", "true")'

        if option_marker in content:
            return content, None  # já aplicado

        matches = list(pattern.finditer(content))
        if not matches:
            return content, None

        # Substitui inserindo a option antes do saveAsTable
        new_content = pattern.sub(
            '.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(',
            content,
        )
        return new_content, f"Adicionado overwriteSchema=true em {len(matches)} write(s)"

    def _fix_monotonic_function(self, content: str) -> tuple[str, str | None]:
        """GAP-8a: Substitui MONOTONIC() por ROW_NUMBER() OVER (...).

        MONOTONIC() é uma função SAS DI Studio que retorna o número de linha
        sequencial da observação. No Spark SQL não existe equivalente direto —
        ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id()) é o mais próximo
        para preservar o comportamento de geração de índice único crescente.

        Cobre tanto o uso em PROC SQL quanto em strings de spark.sql().
        """
        if not _RE_MONOTONIC.search(content):
            return content, None

        replacement = "ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id())"
        count = len(_RE_MONOTONIC.findall(content))
        new_content = _RE_MONOTONIC.sub(replacement, content)
        return new_content, f"MONOTONIC() → ROW_NUMBER() OVER (...) em {count} ocorrência(s)"

    def _fix_numeric_string_comparison(self, content: str) -> tuple[str, str | None]:
        """GAP-8b: Detecta e corrige comparações entre literal numérico e coluna string.

        Heurística estática: detecta padrões como `F.col("col") == 1` (ou != 0, > 0)
        onde 1/0 são flags booleanas usadas com colunas que tipicamente são STRING
        (ex: "status", "flag_ativo"). Substitui o literal inteiro por sua versão string.

        Padrão detectado:
            F.col("flag_col") == 1   →   F.col("flag_col") == "1"
            F.col("flag_col") != 0   →   F.col("flag_col") != "0"

        Limitação: não tem informação de schema — aplica APENAS para colunas cujo
        nome sugere ser categorical (flag_, status_, tipo_, cd_, ds_, tp_).
        """
        # Padrão: F.col("flag_xxx") == 0 ou 1 (sem aspas no literal)
        pattern = re.compile(
            r'(F\.col\(["\'](?:flag_|status_|tipo_|cd_|ds_|tp_|nm_|vl_flag)[^"\']*["\']\))'
            r'(\s*[=!<>]=?\s*)(\b[01]\b)(?!["\'])',
        )
        matches = list(pattern.finditer(content))
        if not matches:
            return content, None

        def quote_literal(m: re.Match) -> str:
            return f'{m.group(1)}{m.group(2)}"{m.group(3)}"'

        new_content = pattern.sub(quote_literal, content)
        return new_content, f"Literal numérico → string em {len(matches)} comparação(ões) de coluna categórica"

    def _fix_sas_date_literals(self, content: str) -> tuple[str, str | None]:
        """GAP-8c: Converte literais de data/datetime SAS para formato Spark SQL.

        SAS usa literais no formato '01JAN2025'd e '01JAN2025:00:00:00'dt.
        Spark não reconhece essa sintaxe — converte para date('YYYY-MM-DD') e
        timestamp('YYYY-MM-DD HH:MM:SS').

        Exemplos:
            '01JAN2025'd          → date('2025-01-01')
            '15MAR2024:08:30:00'dt → timestamp('2024-03-15 08:30:00')
        """
        fixes: list[str] = []

        # Datetime literals primeiro (mais específico)
        def replace_datetime(m: re.Match) -> str:
            day = m.group(1).zfill(2)
            mon = _SAS_MONTH_MAP.get(m.group(2).upper(), "01")
            year = m.group(3)
            time_part = m.group(4)
            return f"timestamp('{year}-{mon}-{day} {time_part}')"

        dt_count = len(_RE_SAS_DATETIME_LIT.findall(content))
        if dt_count:
            content = _RE_SAS_DATETIME_LIT.sub(replace_datetime, content)
            fixes.append(f"Literal datetime SAS → timestamp() em {dt_count} ocorrência(s)")

        # Date literals
        def replace_date(m: re.Match) -> str:
            day = m.group(1).zfill(2)
            mon = _SAS_MONTH_MAP.get(m.group(2).upper(), "01")
            year = m.group(3)
            return f"date('{year}-{mon}-{day}')"

        d_count = len(_RE_SAS_DATE_LIT.findall(content))
        if d_count:
            content = _RE_SAS_DATE_LIT.sub(replace_date, content)
            fixes.append(f"Literal data SAS → date() em {d_count} ocorrência(s)")

        return content, ("; ".join(fixes) if fixes else None)

    def _fix_alter_table_if_not_exists(self, content: str) -> tuple[str, str | None]:
        """Converte ALTER TABLE ... ADD COLUMN IF NOT EXISTS para try/except.

        Databricks SQL não suporta a cláusula IF NOT EXISTS em ADD COLUMN/COLUMNS.
        Substitui por bloco try/except Python que absorve o erro quando a coluna
        já existe, tornando a operação idempotente.

        Também filtra nomes de tabela inválidos (placeholders como <tabela>) que
        o LLM gera quando não consegue resolver o nome real — esses ALTER TABLE
        nunca funcionariam e devem ser removidos.

        Padrões detectados:
            spark.sql("ALTER TABLE t ADD COLUMN IF NOT EXISTS col STRING")
            spark.sql("ALTER TABLE t ADD COLUMN IF NOT EXISTS `col` STRING")
        """
        # Regex flexível: sem/com backticks, ADD COLUMN ou ADD COLUMNS
        # Nomes de tabela: qualquer sequência sem aspas duplas (inclui <placeholder>)
        pattern = re.compile(
            r'spark\.sql\("ALTER TABLE ([^"]+?) ADD COLUMNS? IF NOT EXISTS [`]?(\w+)[`]? (\w+)"\)',
            re.IGNORECASE,
        )

        valid_replacements = []
        invalid_removals = []

        def _replace(m: re.Match) -> str:
            table, col, dtype = m.group(1).strip(), m.group(2), m.group(3)
            # Remove placeholders inválidos gerados pelo LLM (<tabela>, <nome_da_tabela>)
            if "<" in table or ">" in table or table in ("minha_tabela", "nome_tabela", "tabela"):
                invalid_removals.append(f"{table}.{col}")
                return f"# [REMOVIDO] ALTER TABLE inválido: tabela placeholder '{table}' não existe"
            valid_replacements.append(f"{table}.{col}")
            return (
                f'try:\n'
                f'    spark.sql("ALTER TABLE {table} ADD COLUMNS (`{col}` {dtype})")\n'
                f'except Exception:\n'
                f'    pass  # coluna já existe'
            )

        new_content = pattern.sub(_replace, content)
        count = len(valid_replacements) + len(invalid_removals)
        if not count:
            return content, None

        parts = []
        if valid_replacements:
            parts.append(f"ALTER TABLE IF NOT EXISTS → try/except em {len(valid_replacements)} ocorrência(s)")
        if invalid_removals:
            parts.append(f"removidos {len(invalid_removals)} ALTER TABLE com tabela placeholder inválida")

        return new_content, "; ".join(parts)

    def _deduplicate_imports(self, content: str) -> tuple[str, str | None]:
        """Remove linhas de import duplicadas mantendo a primeira ocorrência."""
        lines = content.split("\n")
        seen_imports: set[str] = set()
        clean: list[str] = []
        removed = 0

        for line in lines:
            stripped = line.strip()
            if stripped.startswith(("import ", "from ")) and stripped in seen_imports:
                removed += 1
                continue
            if stripped.startswith(("import ", "from ")):
                seen_imports.add(stripped)
            clean.append(line)

        if removed:
            return "\n".join(clean), f"Removidos {removed} import(s) duplicado(s)"
        return content, None
