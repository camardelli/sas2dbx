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
# Padrão para primeira linha de código spark. não-comentário (usado em inject_placeholder_bootstrap)
_RE_SPARK_CODE_LINE = re.compile(r"^\s*spark\.")


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

    def check(
        self,
        output_dir: Path,
        execution_order: list[str] | None = None,
    ) -> PreflightReport:
        """Varre notebooks em output_dir e verifica tabelas de entrada.

        Args:
            output_dir: Diretório com notebooks .py gerados.
            execution_order: Lista de nomes de job na ordem de execução da DAG.
                Quando fornecido, permite distinguir tabelas intermediárias
                (criadas por jobs anteriores) de fontes externas reais.

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
        # Schemas coletados via DESCRIBE TABLE (tabelas que existem) → salvo em schemas.yaml
        discovered_schemas: dict[str, list[str]] = {}
        try:
            from databricks.sdk import WorkspaceClient
            client = WorkspaceClient(
                host=self._config.host,
                token=self._config.token,
            )
            for table_name, nbs in sorted(table_to_notebooks.items()):
                # Busca TableInfo uma única vez — evita segunda chamada de rede para schema
                table_info = self._get_table_info(client, table_name)
                exists = table_info is not None
                if exists:
                    report.existing_tables.append(table_name)
                    # Coleta schema da tabela existente para injetar no prompt de transpilação
                    cols = [
                        col.name for col in (getattr(table_info, "columns", None) or [])
                        if getattr(col, "name", None)
                    ]
                    if cols:
                        discovered_schemas[table_name] = cols
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

        # Persiste schemas descobertos em schemas.yaml para uso no próximo ciclo de transpilação.
        # Exclui tabelas de output (criadas pelos próprios notebooks via saveAsTable/CREATE TABLE)
        # para evitar contaminação cruzada: schemas de saídas de runs anteriores com colunas
        # inventadas pelo LLM não devem influenciar prompts de transpilação futuros.
        if discovered_schemas:
            output_tables = self._extract_output_tables(notebooks)
            source_schemas = {
                t: cols
                for t, cols in discovered_schemas.items()
                if t.split(".")[-1].lower() not in output_tables
                and t.lower() not in output_tables
            }
            if source_schemas:
                self._save_schemas(output_dir, source_schemas)
            skipped = set(discovered_schemas) - set(source_schemas)
            if skipped:
                logger.info(
                    "PreflightChecker: %d schema(s) de tabela(s) de output excluído(s) de schemas.yaml: %s",
                    len(skipped),
                    ", ".join(sorted(skipped)),
                )

        # Categoriza tabelas ausentes: upstream (criadas por notebook irmão) vs source
        if report.ghost_sources:
            self._categorize_ghosts(report.ghost_sources, notebooks, execution_order)

        return report

    def _extract_output_tables(self, notebooks: list[Path]) -> set[str]:
        """Retorna conjunto de nomes de tabela (normalizados) criados pelos notebooks.

        Coleta tanto o nome completo (catalog.schema.table) quanto o nome curto (table)
        para permitir filtragem independente do catalog/schema prefix nos schemas.
        """
        _save_pat = re.compile(r'saveAsTable\(\s*["\']([^"\']+)["\']\s*\)', re.IGNORECASE)
        _create_pat = re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?'
            r'([`"\']?[\w]+(?:\.[\w]+){0,2}[`"\']?)',
            re.IGNORECASE,
        )
        output_tables: set[str] = set()
        for nb in notebooks:
            try:
                content = nb.read_text(encoding="utf-8")
            except OSError:
                continue
            for pat in (_save_pat, _create_pat):
                for m in pat.finditer(content):
                    full = m.group(1).strip("`\"'")
                    if not full or len(full) < 3:
                        continue
                    output_tables.add(full.lower())
                    output_tables.add(full.split(".")[-1].lower())
        return output_tables

    def _categorize_ghosts(
        self,
        ghosts: list[GhostSource],
        notebooks: list[Path],
        execution_order: list[str] | None = None,
    ) -> None:
        """Classifica cada tabela ausente como MISSING_SOURCE ou MISSING_UPSTREAM.

        Regra: uma tabela é MISSING_UPSTREAM se for criada por algum notebook
        da pipeline (via saveAsTable ou CREATE TABLE) que roda ANTES dos notebooks
        que a consomem, segundo execution_order.

        Sem execution_order, usa comportamento legado (qualquer notebook irmão).
        """
        _save_pattern = re.compile(
            r'saveAsTable\(\s*["\']([^"\']+)["\']\s*\)',
            re.IGNORECASE,
        )
        _create_pattern = re.compile(
            r'CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?'
            r'([`"\']?[\w]+(?:\.[\w]+){1,2}[`"\']?)',
            re.IGNORECASE,
        )

        # Mapa: nome_normalizado → notebook stem que cria a tabela
        writes: dict[str, str] = {}
        for nb in notebooks:
            try:
                content = nb.read_text(encoding="utf-8")
            except OSError:
                continue
            for pattern in (_save_pattern, _create_pattern):
                for m in pattern.finditer(content):
                    full_name = m.group(1).strip("`\"'")
                    if not full_name or len(full_name) < 3:
                        continue
                    short = full_name.split(".")[-1].lower()
                    # Não sobrescreve se já mapeado (primeiro escritor vence)
                    writes.setdefault(short, nb.stem)
                    writes.setdefault(full_name.lower(), nb.stem)

        # Índice de posição na execução: stem → int (menor = roda antes)
        order_index: dict[str, int] = {}
        if execution_order:
            for pos, job_name in enumerate(execution_order):
                order_index[job_name] = pos

        for ghost in ghosts:
            short = ghost.table_name.split(".")[-1].lower()
            upstream_nb = writes.get(short) or writes.get(ghost.table_name.lower())

            if not upstream_nb:
                ghost.category = TableCategory.MISSING_SOURCE
                continue

            if not execution_order:
                # Sem ordem conhecida — trata qualquer notebook irmão como upstream
                ghost.category = TableCategory.MISSING_UPSTREAM
                ghost.upstream_notebook = upstream_nb
                ghost.suggestion = f"Tabela intermediária — execute '{upstream_nb}' antes deste job"
                continue

            # Verifica se o notebook criador vem ANTES de todos os consumidores
            upstream_pos = order_index.get(upstream_nb, len(execution_order))
            consumer_positions = [
                order_index.get(nb_stem, len(execution_order))
                for nb_stem in ghost.notebooks
            ]
            earliest_consumer = min(consumer_positions, default=len(execution_order))

            if upstream_pos < earliest_consumer:
                ghost.category = TableCategory.MISSING_UPSTREAM
                ghost.upstream_notebook = upstream_nb
                ghost.suggestion = (
                    f"Tabela intermediária — criada por '{upstream_nb}' "
                    f"(job {upstream_pos + 1} na ordem de execução)"
                )
            else:
                # Criador vem depois ou na mesma posição: não resolve a dependência
                ghost.category = TableCategory.MISSING_SOURCE
                ghost.suggestion = (
                    f"Tabela de origem externa — '{upstream_nb}' cria uma tabela "
                    f"com este nome mas é posterior na execução"
                )

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
        # 6. Nomes que começam com '.' — são métodos Python (.drop, .select, .filter, etc.)
        #    capturados quando o LLM gera SQL com JOIN/FROM na última linha e o método
        #    Python fica na linha seguinte (ex: "JOIN\n.drop(...)").
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
            and not t.startswith(".")
            and not t.endswith(".")
            and "<" not in t
            and ">" not in t
            and t.lower() not in _SQL_KEYWORDS
            and t.split(".")[0].lower() not in _PYTHON_MODULE_PREFIXES
        ]

    def _get_table_info(self, client, table_name: str):
        """Retorna TableInfo da tabela ou None se não existir.

        Unifica a verificação de existência + coleta de schema em uma única
        chamada de rede, evitando o double-fetch de _table_exists + _fetch_columns.
        """
        try:
            return client.tables.get(table_name)
        except Exception:  # noqa: BLE001
            return None

    def _table_exists(self, client, table_name: str) -> bool:
        """Verifica se a tabela existe no catálogo Databricks."""
        return self._get_table_info(client, table_name) is not None

    def _save_schemas(self, output_dir: Path, schemas: dict[str, list[str]]) -> None:
        """Persiste schemas descobertos em output_dir/schemas.yaml.

        O TranspilationEngine lê este arquivo via _load_schema_raw() e injeta
        os nomes de coluna reais no prompt de transpilação, evitando que o LLM
        invente colunas baseado em convenções genéricas.

        O arquivo é criado/atualizado de forma incremental: entradas existentes
        são preservadas e novas tabelas são adicionadas (mas nunca removidas).

        Args:
            output_dir: Diretório de saída dos notebooks da migração.
            schemas: Dict table_fqn → list[column_name] coletado do Databricks.
        """
        schemas_path = output_dir / "schemas.yaml"
        try:
            import yaml  # type: ignore[import]

            # Carrega existente (se houver) para merge incremental
            existing: dict = {}
            if schemas_path.exists():
                existing = yaml.safe_load(schemas_path.read_text(encoding="utf-8")) or {}

            # Merge: novas tabelas adicionadas; existentes preservadas (curadoria manual tem precedência)
            for table, cols in schemas.items():
                if table not in existing:
                    existing[table] = {"columns": cols}

            schemas_path.write_text(
                yaml.dump(existing, allow_unicode=True, default_flow_style=False),
                encoding="utf-8",
            )
            logger.info(
                "PreflightChecker: schemas de %d tabela(s) salvo(s) em %s",
                len(schemas), schemas_path,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "PreflightChecker: falha ao salvar schemas.yaml — %s", exc
            )

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
        for line in content.splitlines(keepends=True):
            stripped = line.lstrip()
            if not stripped.startswith("#") and _RE_SPARK_CODE_LINE.match(line):
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
