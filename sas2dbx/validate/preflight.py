"""PreflightChecker — detecta fontes fantasma antes do deploy.

Seção 2.5 do roteiro humano: valida que todas as tabelas de entrada
referenciadas nos notebooks gerados existem no catálogo Databricks.

Executa ANTES do deploy, eliminando a cascata:
  TABLE_NOT_FOUND → placeholder → UNRESOLVED_COLUMN → ciclos de healing.

Resultado exposto no relatório de validação como seção "ghost_sources".
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


class TableCategory(str, Enum):
    """Categoria de uma tabela ausente no catálogo Databricks."""

    EXISTS = "exists"
    MISSING_SOURCE = "missing_source"
    """Tabela de origem (LIBNAME SAS) — requer ingestão de dados manual."""
    MISSING_UPSTREAM = "missing_upstream"
    """Tabela intermediária — deve ser criada por notebook upstream na DAG."""

# Padrões de leitura de tabelas nos notebooks gerados
_RE_READ_TABLE = re.compile(
    r'spark\.read\.table\(\s*["\']([^"\']+)["\']\s*\)',
    re.IGNORECASE,
)
# f-string: spark.read.table(f"prefix.{VAR}") ou spark.read.table(f"{VAR}.suffix.{VAR2}")
_RE_READ_TABLE_FSTRING = re.compile(
    r'spark\.read\.table\(\s*f["\']([^"\']+)["\']\s*\)',
    re.IGNORECASE,
)
# Atribuição de variável de string simples: VAR = "valor" ou var = 'valor' (case insensitive)
_RE_VAR_ASSIGN = re.compile(
    r'^[ \t]*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*["\']([^"\']+)["\']',
    re.MULTILINE,
)
_RE_SPARK_SQL_FROM = re.compile(
    r'FROM\s+([`"\']?[\w.]+[`"\']?)',
    re.IGNORECASE,
)
_RE_SPARK_SQL_JOIN = re.compile(
    r'JOIN\s+([`"\']?[\w.]+[`"\']?)',
    re.IGNORECASE,
)


@dataclass
class GhostSource:
    """Tabela referenciada mas inexistente no catálogo Databricks."""

    table_name: str
    notebooks: list[str] = field(default_factory=list)
    suggestion: str = ""
    category: TableCategory = TableCategory.MISSING_SOURCE
    upstream_notebook: str | None = None
    """Nome do notebook upstream que deveria criar esta tabela (sem extensão)."""


@dataclass
class PreflightReport:
    """Resultado do preflight check."""

    ghost_sources: list[GhostSource] = field(default_factory=list)
    existing_tables: list[str] = field(default_factory=list)
    checked_tables: int = 0
    skipped: bool = False
    skip_reason: str = ""

    @property
    def has_ghosts(self) -> bool:
        return len(self.ghost_sources) > 0

    @property
    def missing_source(self) -> list[GhostSource]:
        return [g for g in self.ghost_sources if g.category == TableCategory.MISSING_SOURCE]

    @property
    def missing_upstream(self) -> list[GhostSource]:
        return [g for g in self.ghost_sources if g.category == TableCategory.MISSING_UPSTREAM]

    def summary(self) -> str:
        if self.skipped:
            return f"Preflight ignorado: {self.skip_reason}"
        if not self.has_ghosts:
            return f"{self.checked_tables} tabela(s) verificada(s) — nenhuma ausente"
        parts = []
        if self.missing_source:
            parts.append(f"{len(self.missing_source)} source ausente(s)")
        if self.missing_upstream:
            parts.append(f"{len(self.missing_upstream)} upstream ausente(s)")
        return (
            f"{self.checked_tables} verificada(s) · "
            f"{len(self.existing_tables)} existente(s) · "
            + " · ".join(parts)
        )


class PreflightChecker:
    """Verifica existência de tabelas de entrada no catálogo Databricks.

    Args:
        config: DatabricksConfig com credenciais e catálogo alvo.
    """

    def __init__(self, config=None) -> None:
        self._config = config

    def check(self, output_dir: Path) -> PreflightReport:
        """Varre notebooks em output_dir e verifica tabelas de entrada.

        Args:
            output_dir: Diretório com notebooks .py gerados.

        Returns:
            PreflightReport com fontes fantasma identificadas.
        """
        notebooks = sorted(output_dir.glob("*.py"))
        if not notebooks:
            return PreflightReport(skipped=True, skip_reason="nenhum notebook encontrado")

        # Coleta todas as tabelas referenciadas nos notebooks
        table_to_notebooks: dict[str, list[str]] = {}
        for nb in notebooks:
            try:
                content = nb.read_text(encoding="utf-8")
                tables = self._extract_input_tables(content)
                for t in tables:
                    table_to_notebooks.setdefault(t, []).append(nb.stem)
            except OSError as exc:
                logger.warning("PreflightChecker: erro ao ler %s: %s", nb.name, exc)

        if not table_to_notebooks:
            return PreflightReport(skipped=True, skip_reason="nenhuma tabela de entrada detectada")

        # Verifica existência via Databricks API
        report = PreflightReport(checked_tables=len(table_to_notebooks))
        try:
            from databricks.sdk import WorkspaceClient
            client = WorkspaceClient(
                host=self._config.host,
                token=self._config.token,
            )
            for table_name, nbs in sorted(table_to_notebooks.items()):
                exists = self._table_exists(client, table_name)
                if exists:
                    report.existing_tables.append(table_name)
                else:
                    suggestion = self._suggest_action(table_name)
                    report.ghost_sources.append(
                        GhostSource(
                            table_name=table_name,
                            notebooks=nbs,
                            suggestion=suggestion,
                        )
                    )
                    logger.info(
                        "PreflightChecker: fonte fantasma detectada — %s (usada em: %s)",
                        table_name,
                        ", ".join(nbs),
                    )
        except ImportError:
            return PreflightReport(
                skipped=True,
                skip_reason="databricks-sdk não disponível",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("PreflightChecker: falha ao conectar ao Databricks: %s", exc)
            return PreflightReport(
                skipped=True,
                skip_reason=f"falha na conexão Databricks: {type(exc).__name__}",
            )

        # Categoriza tabelas ausentes: upstream (criadas por notebook irmão) vs source
        if report.ghost_sources:
            self._categorize_ghosts(report.ghost_sources, notebooks)

        return report

    def _categorize_ghosts(
        self, ghosts: list[GhostSource], notebooks: list[Path]
    ) -> None:
        """Classifica cada tabela ausente como MISSING_SOURCE ou MISSING_UPSTREAM.

        Uma tabela é MISSING_UPSTREAM se algum notebook irmão contiver
        saveAsTable("...{table_short}...") — ou seja, deveria tê-la criado.
        """
        # Monta mapa: table_short → notebook que a cria
        _save_pattern_tmpl = re.compile(
            r'saveAsTable\(\s*["\']([^"\']+)["\']\s*\)',
            re.IGNORECASE,
        )
        writes: dict[str, str] = {}  # table_short → notebook stem
        for nb in notebooks:
            try:
                content = nb.read_text(encoding="utf-8")
            except OSError:
                continue
            for m in _save_pattern_tmpl.finditer(content):
                full_name = m.group(1).strip("`\"'")
                short = full_name.split(".")[-1].lower()
                writes[short] = nb.stem
                writes[full_name.lower()] = nb.stem

        for ghost in ghosts:
            short = ghost.table_name.split(".")[-1].lower()
            upstream = writes.get(short) or writes.get(ghost.table_name.lower())
            if upstream:
                ghost.category = TableCategory.MISSING_UPSTREAM
                ghost.upstream_notebook = upstream
                ghost.suggestion = (
                    f"Tabela intermediária — execute '{upstream}' antes deste job"
                )
            else:
                ghost.category = TableCategory.MISSING_SOURCE

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_fstring_tables(self, content: str) -> list[str]:
        """Resolve f-strings em spark.read.table(f"...{VAR}...") usando atribuições do notebook."""
        # Coleta todas as atribuições simples: VAR = "valor"
        var_map: dict[str, str] = {}
        for m in _RE_VAR_ASSIGN.finditer(content):
            var_map[m.group(1)] = m.group(2)

        resolved: list[str] = []
        for m in _RE_READ_TABLE_FSTRING.finditer(content):
            template = m.group(1)  # ex: "telcostar.operacional.{DS_AGG}"
            # Substitui todas as ocorrências de {VAR} pelos valores conhecidos
            try:
                result = re.sub(
                    r'\{([A-Za-z_][A-Za-z0-9_]*)\}',
                    lambda mm: var_map.get(mm.group(1), mm.group(0)),
                    template,
                )
                # Só adiciona se todos os placeholders foram resolvidos (sem { restante)
                # e o nome tem exatamente 3 segmentos (catalog.schema.table)
                if "{" not in result and result.count(".") == 2:
                    resolved.append(result)
            except Exception:  # noqa: BLE001
                pass
        return resolved

    def _extract_input_tables(self, content: str) -> list[str]:
        """Extrai tabelas de entrada (leitura) de um notebook.

        Exclui tabelas que aparecem apenas em saveAsTable (escrita).
        Inclui f-strings resolvidas via atribuições de variável no notebook.
        """
        read_tables: set[str] = set()
        write_tables: set[str] = set()

        # spark.read.table("...") — sempre leitura (string literal)
        for m in _RE_READ_TABLE.finditer(content):
            read_tables.add(m.group(1).strip("`\"'"))

        # spark.read.table(f"...{VAR}...") — f-string resolvida via variáveis do notebook
        for t in self._resolve_fstring_tables(content):
            read_tables.add(t.strip("`\"'"))

        # FROM/JOIN em spark.sql — pode ser leitura ou escrita (CREATE TABLE ... AS SELECT)
        # Heurística: se a tabela aparece em FROM/JOIN mas não em saveAsTable → leitura
        for m in _RE_SPARK_SQL_FROM.finditer(content):
            t = m.group(1).strip("`\"'")
            if "." in t and not t.startswith("("):
                read_tables.add(t)
        for m in _RE_SPARK_SQL_JOIN.finditer(content):
            t = m.group(1).strip("`\"'")
            if "." in t:
                read_tables.add(t)

        # saveAsTable — escrita
        for m in re.finditer(r'saveAsTable\(\s*["\']([^"\']+)["\']\s*\)', content):
            write_tables.add(m.group(1).strip("`\"'"))
        # CREATE TABLE ... AS / CREATE OR REPLACE TABLE
        for m in re.finditer(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`"\']?[\w.]+)',
            content,
            re.IGNORECASE,
        ):
            write_tables.add(m.group(1).strip("`\"'"))

        # Tabelas de entrada = lidas mas não escritas
        input_tables = read_tables - write_tables

        # Filtra nomes claramente não-tabela:
        # 1. Sem ponto ou muito curtos
        # 2. Keywords SQL
        # 3. Nomes terminados em '.' (fragmentos como "STAGING." ou "telcostar.operacional.")
        # 4. Nomes com '<' ou '>' (placeholders de template como "<nome>")
        # 5. Prefixos de módulos Python conhecidos (pyspark.sql, scipy.stats, etc.)
        #    — o regex FROM captura 'from pyspark.sql import' como "pyspark.sql"
        _SQL_KEYWORDS = {"dual", "values", "lateral", "unnest"}
        _PYTHON_MODULE_PREFIXES = {
            "pyspark", "scipy", "numpy", "pandas", "matplotlib",
            "sklearn", "tensorflow", "torch", "seaborn", "statsmodels",
            "importlib", "collections", "functools", "itertools", "os",
            "sys", "re", "json", "datetime", "pathlib", "typing",
        }
        return [
            t for t in input_tables
            if "." in t
            and len(t) > 3
            and not t.endswith(".")
            and "<" not in t
            and ">" not in t
            and t.lower() not in _SQL_KEYWORDS
            and t.split(".")[0].lower() not in _PYTHON_MODULE_PREFIXES
        ]

    def _table_exists(self, client, table_name: str) -> bool:
        """Verifica se a tabela existe no catálogo Databricks."""
        try:
            client.tables.get(table_name)
            return True
        except Exception:  # noqa: BLE001
            return False

    def inject_placeholder_bootstrap(
        self, notebook_path: Path, ghosts: list[GhostSource]
    ) -> list[str]:
        """Injeta bloco CREATE TABLE IF NOT EXISTS para todas as fontes fantasma.

        Inserido ANTES do primeiro spark.read.table() ou spark.sql() no notebook.
        Evita o ciclo deploy→falha→heal(1 tabela)→repeat que esgota o cap de iterações.

        Args:
            notebook_path: Notebook .py a modificar.
            ghosts: Lista de GhostSource referenciadas neste notebook.

        Returns:
            Lista de nomes de tabela para as quais o placeholder foi injetado.
        """
        if not ghosts:
            return []

        try:
            content = notebook_path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.warning("PreflightChecker: erro ao ler %s: %s", notebook_path.name, exc)
            return []

        # Não re-injeta se já existe bloco de bootstrap (idempotente)
        if "# [PREFLIGHT-BOOTSTRAP]" in content:
            logger.debug(
                "PreflightChecker: bootstrap já presente em %s — ignorando", notebook_path.name
            )
            return []

        # Extrai nomes de colunas do notebook para criar schema rico na placeholder.
        # Evita UNRESOLVED_COLUMN imediatamente após o CREATE TABLE com schema mínimo.
        col_names = self._extract_column_names_from_notebook(content)
        if col_names:
            col_defs = ", ".join(f"`{c}` STRING" for c in col_names)
            ddl_columns = f"id BIGINT, {col_defs}"
        else:
            ddl_columns = "id BIGINT, _placeholder BOOLEAN"

        lines: list[str] = []
        lines.append("# [PREFLIGHT-BOOTSTRAP] Placeholders criados antes do deploy (preflight)\n")
        injected: list[str] = []
        for ghost in ghosts:
            parts = ghost.table_name.split(".")
            if len(parts) == 3:
                catalog, schema, _ = parts
                lines.append(
                    f'spark.sql("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")\n'
                )
            lines.append(
                f'spark.sql("CREATE TABLE IF NOT EXISTS {ghost.table_name} '
                f'({ddl_columns}) USING DELTA")\n'
            )
            injected.append(ghost.table_name)

        lines.append("\n")
        bootstrap_block = "".join(lines)

        # Insere antes da primeira linha de código real (não-comentário) que use
        # spark.read.table / spark.sql / df = spark.
        # Itera linha por linha para evitar match dentro de comentários Python (#).
        insert_pos: int | None = None
        offset = 0
        # Padrão: qualquer chamada spark. ou df = spark que não seja comentário
        _CODE_PATTERN = re.compile(r"^\s*spark\.", re.MULTILINE)
        for line in content.splitlines(keepends=True):
            stripped = line.lstrip()
            if not stripped.startswith("#") and _CODE_PATTERN.match(line):
                insert_pos = offset
                break
            offset += len(line)

        if insert_pos is not None:
            new_content = content[:insert_pos] + bootstrap_block + content[insert_pos:]
        else:
            new_content = bootstrap_block + content

        try:
            notebook_path.write_text(new_content, encoding="utf-8")
            logger.info(
                "PreflightChecker: bootstrap injetado em %s — %d placeholder(s): %s",
                notebook_path.name,
                len(injected),
                ", ".join(injected),
            )
        except OSError as exc:
            logger.warning("PreflightChecker: falha ao gravar bootstrap em %s: %s", notebook_path.name, exc)
            return []

        return injected

    def _extract_column_names_from_notebook(self, content: str) -> list[str]:
        """Extrai nomes de colunas referenciadas no notebook para schema rico do placeholder.

        Reutiliza os mesmos padrões do NotebookFixer._extract_column_names() para garantir
        que a placeholder tenha as colunas que o notebook vai tentar usar.
        """
        _RE_COL_REFS = re.compile(
            r"""(?:F\.col\(\s*["'](\w+)["']\s*\)|\.col\(\s*["'](\w+)["']\s*\)"""
            r"""|\bcol\(\s*["'](\w+)["']\s*\)|\[["'](\w+)["']\])""",
            re.VERBOSE,
        )
        _RE_SELECT_ARGS = re.compile(
            r'\.(?:select|drop|withColumnRenamed|groupBy|orderBy|partitionBy)\s*\(([^)]+)\)'
        )
        _SKIP = {
            "true", "false", "null", "overwrite", "append", "delta",
            "string", "bigint", "double", "boolean", "date", "timestamp",
        }

        cols: dict[str, None] = {}
        for m in _RE_COL_REFS.finditer(content):
            name = next(g for g in m.groups() if g)
            cols[name] = None
        for m in _RE_SELECT_ARGS.finditer(content):
            for tok in re.findall(r'["\']([\w]+)["\']', m.group(1)):
                cols[tok] = None

        return [c for c in cols if len(c) > 1 and c.lower() not in _SKIP]

    def _suggest_action(self, table_name: str) -> str:
        """Sugere ação para fonte fantasma baseada no nome da tabela."""
        parts = table_name.split(".")
        if len(parts) == 3:
            catalog, schema, table = parts
            if any(kw in catalog.lower() for kw in ("staging", "temp", "tmp", "work")):
                return f"Tabela intermediária — verificar se job upstream deve criá-la primeiro"
            if any(kw in schema.lower() for kw in ("raw", "bronze", "source", "operacional")):
                return f"Tabela de origem — verificar ingestão de dados ou remapear LIBNAME '{catalog}.{schema}'"
            return f"Tabela não encontrada no catálogo — verificar mapeamento de LIBNAME"
        return "Tabela não encontrada — verificar configuração de catalog/schema"
