"""Fixer — aplica patches nos notebooks gerados com base no diagnóstico."""

from __future__ import annotations

import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.validate.heal.diagnostics import ErrorDiagnostic

logger = logging.getLogger(__name__)

# Registro de handlers: deterministic_fix_key → nome do método interno
_HANDLERS: dict[str, str] = {
    "create_placeholder_table": "_fix_create_placeholder_table",
    "add_missing_import": "_fix_add_missing_import",
    "increase_cluster_config": "_fix_increase_cluster_config",
    "fix_spark_conf_get": "_fix_spark_conf_get",
    "fix_wrong_catalog": "_fix_wrong_catalog",
    "fix_overwrite_schema": "_fix_overwrite_schema",
    "fix_unresolved_column": "_fix_unresolved_column",
    "fix_stack_type_mismatch": "_fix_stack_type_mismatch",
    "fix_when_otherwise_type": "_fix_when_otherwise_type",
    "fix_function_not_found": "_fix_function_not_found",
    "fix_parse_syntax_if_not_exists": "_fix_parse_syntax_if_not_exists",
}

# Mapeamento de funções SQL SAS/não-suportadas → equivalente Spark SQL.
# Inclua apenas funções com substituição 1:1 segura e sintaticamente válida em Python/SQL.
# YRDIF foi excluído: sem equivalente direto — requer revisão manual.
_SQL_FUNCTION_MAP: dict[str, tuple[str, str]] = {
    "MONOTONIC": (
        r"\bMONOTONIC\(\)",
        "ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id())",
    ),
    "DATDIF": (
        r"\bDATDIF\(",
        "datediff(",
    ),
}


@dataclass
class PatchResult:
    """Resultado da tentativa de patch de um notebook.

    Attributes:
        patched: True se o patch foi aplicado com sucesso.
        backup_path: Path do backup criado antes do patch (None se patch não aplicado).
        description: Descrição da ação tomada.
        error: Mensagem de erro se o patch falhou.
    """

    patched: bool
    backup_path: Path | None = None
    description: str = ""
    error: str | None = None


class NotebookFixer:
    """Aplica patches determinísticos em notebooks .py com base no diagnóstico.

    O notebook original é preservado como .py.bak antes de qualquer modificação.

    Args:
        correct_catalog: Catálogo Unity Catalog correto (usado pelo fix_wrong_catalog).
        correct_schema: Schema correto.
    """

    def __init__(self, correct_catalog: str = "", correct_schema: str = "") -> None:
        self._correct_catalog = correct_catalog
        self._correct_schema = correct_schema

    def apply_fix(self, notebook_path: Path, diagnostic: ErrorDiagnostic) -> PatchResult:
        """Aplica patch no notebook com base no diagnóstico.

        Fluxo:
          1. Verifica se diagnostic.deterministic_fix tem handler registrado
          2. Se não tem: retorna PatchResult(patched=False)
          3. Cria backup (.py.bak) via shutil.copy2()
          4. Chama o handler específico
          5. Se handler falhar: restaura backup, retorna PatchResult(patched=False, error=...)
          6. Retorna PatchResult(patched=True, ...)

        Args:
            notebook_path: Path local do notebook .py a ser modificado.
            diagnostic: ErrorDiagnostic com deterministic_fix preenchido.

        Returns:
            PatchResult indicando sucesso ou falha.
        """
        fix_key = diagnostic.deterministic_fix
        if not fix_key or fix_key not in _HANDLERS:
            # Tenta extrair entidades da mensagem de erro bruta antes de desistir
            raw_msg = getattr(diagnostic, "raw_error", "") or ""
            if raw_msg and fix_key == "fix_unresolved_column":
                logger.debug("Fixer: tentando extrair entidades de raw_error para fix_unresolved_column")
            else:
                return PatchResult(
                    patched=False,
                    description=f"Sem fix determinístico disponível para '{fix_key or 'None'}'",
                )

        if not notebook_path.exists():
            return PatchResult(
                patched=False,
                description="",
                error=f"Notebook não encontrado: {notebook_path}",
            )

        backup_path = notebook_path.with_suffix(".py.bak")
        try:
            if not backup_path.exists():
                shutil.copy2(notebook_path, backup_path)
            logger.debug("Fixer: backup criado em %s", backup_path)

            method_name = _HANDLERS[fix_key]
            handler = getattr(self, method_name)
            description = handler(notebook_path, diagnostic.entities)

            logger.info("Fixer: patch '%s' aplicado em %s — %s", fix_key, notebook_path, description)
            return PatchResult(patched=True, backup_path=backup_path, description=description)

        except Exception as exc:  # noqa: BLE001
            logger.warning("Fixer: falha ao aplicar '%s': %s", fix_key, exc)
            # Restaura backup se existir
            if backup_path.exists():
                shutil.copy2(backup_path, notebook_path)
                logger.info("Fixer: backup restaurado após falha")
            return PatchResult(patched=False, backup_path=None, error=str(exc))

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    # Padrões para extração de nomes de colunas do notebook gerado
    _RE_COL_REFS = re.compile(
        r"""
        (?:
          F\.col\(\s*["'](\w+)["']\s*\)   # F.col("col") ou F.col('col')
          | \.col\(\s*["'](\w+)["']\s*\)  # .col("col")
          | \bcol\(\s*["'](\w+)["']\s*\)  # col("col") sem F.
          | \[["'](\w+)["']\]             # df["col"] ou df['col']
        )
        """,
        re.VERBOSE,
    )
    _RE_SELECT_ARGS = re.compile(
        r'\.(?:select|drop|withColumnRenamed|groupBy|orderBy|partitionBy)\s*\(([^)]+)\)',
    )

    @classmethod
    def _extract_column_names(cls, content: str) -> list[str]:
        """Extrai todos os nomes de colunas referenciados no notebook."""
        cols: dict[str, None] = {}  # dict preserva inserção e deduplica

        # Padrão F.col / col / df["col"]
        for m in cls._RE_COL_REFS.finditer(content):
            name = next(g for g in m.groups() if g)
            cols[name] = None

        # Strings dentro de .select(), .groupBy(), etc.
        for m in cls._RE_SELECT_ARGS.finditer(content):
            for tok in re.findall(r'["\']([\w]+)["\']', m.group(1)):
                cols[tok] = None

        # Filtra nomes reservados / muito curtos / claramente não-coluna
        _SKIP = {"true", "false", "null", "overwrite", "append", "delta",
                 "string", "bigint", "double", "boolean", "date", "timestamp"}
        return [c for c in cols if len(c) > 1 and c.lower() not in _SKIP]

    def _fix_create_placeholder_table(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='missing_table'.

        Insere CREATE TABLE IF NOT EXISTS antes do primeiro bloco de código.
        O schema da placeholder é construído a partir de TODAS as colunas
        referenciadas no notebook, evitando ciclos de UNRESOLVED_COLUMN.
        """
        table_name = entities.get("table_name", "unknown_table")
        parts = table_name.split(".")
        schema_stmt = ""
        if len(parts) == 3:
            schema_stmt = f'spark.sql("CREATE SCHEMA IF NOT EXISTS {parts[0]}.{parts[1]}")\n'

        content = notebook_path.read_text(encoding="utf-8")

        # Extrai colunas referenciadas no notebook para criar schema rico
        col_names = self._extract_column_names(content)
        if col_names:
            col_defs = ", ".join(f"`{c}` STRING" for c in col_names)
            ddl = f"id BIGINT, {col_defs}"
        else:
            ddl = "id BIGINT, _placeholder BOOLEAN"

        create_stmt = (
            f'# [AUTO-FIX] Placeholder criado pelo self-healing pipeline\n'
            f'{schema_stmt}'
            f'spark.sql("CREATE TABLE IF NOT EXISTS {table_name} ({ddl}) USING DELTA")\n\n'
        )

        content = notebook_path.read_text(encoding="utf-8")

        # Insere antes do primeiro spark.sql() ou # COMMAND ou df = spark
        insert_match = re.search(
            r"(spark\.sql\(|# COMMAND|df\s*=\s*spark)", content, re.MULTILINE
        )
        if insert_match:
            pos = insert_match.start()
            content = content[:pos] + create_stmt + content[pos:]
        else:
            content = create_stmt + content

        # GAP-3: placeholder table tem schema (id, _placeholder) — todos os writes
        # no mesmo notebook DEVEM usar overwriteSchema para evitar schema mismatch
        # na próxima iteração (_LEGACY_ERROR_TEMP_DELTA_0007)
        write_pattern = re.compile(
            r'\.write\.mode\(["\']overwrite["\']\)\.saveAsTable\('
        )
        if write_pattern.search(content) and '.option("overwriteSchema", "true")' not in content:
            content = write_pattern.sub(
                '.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(',
                content,
            )
            logger.debug("Fixer: overwriteSchema adicionado preventivamente (placeholder criado)")

        notebook_path.write_text(content, encoding="utf-8")
        return f"Created placeholder table: {table_name} (+ overwriteSchema preventivo)"

    def _fix_add_missing_import(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='missing_import'.

        Insere import do módulo ausente após os imports existentes.
        """
        module_name = entities.get("module_name", "")
        if not module_name:
            return "No module name found in error — import not added"

        import_line = f"import {module_name}\n"
        content = notebook_path.read_text(encoding="utf-8")

        # Verifica se já importado
        if import_line.strip() in content:
            return f"Import already present: {module_name}"

        # Localiza última linha de import
        lines = content.splitlines(keepends=True)
        last_import_idx = -1
        for i, line in enumerate(lines):
            if re.match(r"^(import |from )", line.strip()):
                last_import_idx = i

        if last_import_idx >= 0:
            lines.insert(last_import_idx + 1, import_line)
        else:
            lines.insert(0, import_line)

        notebook_path.write_text("".join(lines), encoding="utf-8")
        return f"Added import: {module_name}"

    def _fix_increase_cluster_config(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='resource'.

        Adiciona configurações de memória do Spark ao notebook.
        """
        memory_conf = (
            '# [AUTO-FIX] Configuração de memória aumentada pelo self-healing pipeline\n'
            'spark.conf.set("spark.executor.memory", "4g")\n'
            'spark.conf.set("spark.driver.memory", "4g")\n\n'
        )

        content = notebook_path.read_text(encoding="utf-8")

        # Verifica se já configurado
        if 'spark.executor.memory' in content:
            return "Memory config already present — skipped"

        # Insere após imports, antes do primeiro bloco de código
        insert_match = re.search(
            r"(spark\.read|df\s*=\s*spark|spark\.sql\()", content, re.MULTILINE
        )
        if insert_match:
            pos = insert_match.start()
            content = content[:pos] + memory_conf + content[pos:]
        else:
            content = memory_conf + content

        notebook_path.write_text(content, encoding="utf-8")
        return "Increased memory config: executor=4g, driver=4g"

    def _fix_spark_conf_get(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='spark_conf_missing'.

        Substitui spark.conf.get("KEY", "default") por "default" diretamente,
        evitando CONFIG_NOT_AVAILABLE no Databricks serverless.
        """
        content = notebook_path.read_text(encoding="utf-8")

        # spark.conf.get("KEY", "default") → "default"
        pattern = re.compile(
            r'spark\.conf\.get\(\s*["\'][\w.]+["\']\s*,\s*(["\'][^"\']+["\'])\s*\)'
        )
        matches = pattern.findall(content)
        if matches:
            new_content = pattern.sub(lambda m: m.group(1), content)
            notebook_path.write_text(new_content, encoding="utf-8")
            return f"Substituído spark.conf.get() por valor padrão em {len(matches)} ocorrência(s)"

        # spark.conf.get("KEY") sem default → substitui por string vazia com WARNING
        pattern_no_default = re.compile(r'spark\.conf\.get\(\s*["\'][\w.]+["\']\s*\)')
        if pattern_no_default.search(content):
            new_content = pattern_no_default.sub('"" # WARNING: valor não disponível — definir manualmente', content)
            notebook_path.write_text(new_content, encoding="utf-8")
            return "Substituído spark.conf.get() sem default por string vazia"

        return "Nenhum spark.conf.get() encontrado para substituir"

    def _fix_wrong_catalog(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='wrong_catalog'.

        Substitui o catálogo errado pelo catálogo correto registrado em DatabricksConfig.
        O catálogo correto é inferido do catalog/schema usados na migração,
        extraídos do path do notebook (via convenção de diretório) ou das entidades.

        Estratégia: encontra todos os pares wrong_catalog.schema. no notebook
        e substitui pelo catalog/schema correto detectado via uso majoritário no arquivo.
        """
        wrong_catalog = entities.get("wrong_catalog")
        if not wrong_catalog:
            return "Catálogo errado não identificado na mensagem de erro"

        content = notebook_path.read_text(encoding="utf-8")

        # Detecta o catálogo/schema correto já presente no notebook (uso majoritário)
        # Padrão: "catalog.schema.table" dentro de strings
        catalog_pattern = re.compile(r'["\s`(]([a-zA-Z]\w+)\.([a-zA-Z]\w+)\.\w+')
        catalog_counts: dict[tuple[str, str], int] = {}
        for m in catalog_pattern.finditer(content):
            cat, sch = m.group(1), m.group(2)
            if cat not in ("F", "spark", "df", "Window", "pyspark"):
                catalog_counts[(cat, sch)] = catalog_counts.get((cat, sch), 0) + 1

        # Remove o catálogo errado da contagem para encontrar o correto
        catalog_counts.pop((wrong_catalog, ""), None)
        for key in list(catalog_counts):
            if key[0] == wrong_catalog:
                del catalog_counts[key]

        if not catalog_counts:
            # Fallback: usa catálogo passado no construtor (do DatabricksConfig)
            if self._correct_catalog:
                correct_catalog = self._correct_catalog
                correct_schema = self._correct_schema
            else:
                return f"Não foi possível inferir catálogo correto para substituir '{wrong_catalog}'"
        else:
            correct_catalog, correct_schema = max(catalog_counts, key=lambda k: catalog_counts[k])

        # Substitui wrong_catalog. → correct_catalog. em todo o notebook
        old_prefix = f"{wrong_catalog}."
        new_prefix = f"{correct_catalog}."
        if old_prefix not in content:
            return f"Catálogo '{wrong_catalog}' não encontrado no notebook"

        count = content.count(old_prefix)
        new_content = content.replace(old_prefix, new_prefix)
        notebook_path.write_text(new_content, encoding="utf-8")
        return f"Catálogo '{wrong_catalog}' → '{correct_catalog}' em {count} ocorrência(s)"

    def _fix_overwrite_schema(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='schema_mismatch'.

        Adiciona .option("overwriteSchema", "true") em todos os writes de overwrite
        para permitir que o Delta Lake sobrescreva a schema da tabela existente.

        Evita _LEGACY_ERROR_TEMP_DELTA_0007 quando a tabela tem schema diferente
        (ex: placeholder criado pelo auto-healing com (id, _placeholder)).
        """
        content = notebook_path.read_text(encoding="utf-8")

        option = '.option("overwriteSchema", "true")'
        if option in content:
            return "overwriteSchema já presente no notebook"

        pattern = re.compile(
            r'\.write\.mode\(["\']overwrite["\']\)\.saveAsTable\('
        )
        matches = pattern.findall(content)
        if not matches:
            return "Nenhum write.mode('overwrite').saveAsTable() encontrado"

        new_content = pattern.sub(
            '.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(',
            content,
        )
        notebook_path.write_text(new_content, encoding="utf-8")
        return f"Adicionado overwriteSchema=true em {len(matches)} write(s)"

    # Regex para extrair coluna ruim e sugestões diretamente da mensagem de erro
    _RE_BAD_COL_MSG = re.compile(
        r"with name\s+`?(?P<alias>[\w]+)`?\.`?(?P<col>[\w]+)`?\s+cannot be resolved",
        re.IGNORECASE,
    )
    _RE_SUGGESTIONS_MSG = re.compile(
        r"Did you mean one of the following\?\s*\[(?P<suggestions>[^\]]+)\]",
        re.IGNORECASE,
    )
    # Extrai itens individuais da lista de sugestões: [`alias`.`col`, ...]
    _RE_SUGGESTION_ITEM = re.compile(
        r"`?(?:[\w]+)`?\.`?([\w]+)`?",
        re.IGNORECASE,
    )

    def _fix_unresolved_column(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='unresolved_column_suggestion'.

        Substitui a referência à coluna não resolvida pela sugestão do Databricks.
        Cobre o padrão LLM de placeholder: ORDER_COL = "coluna_xxx" quando o LLM
        não sabia qual coluna usar para ordenação.

        Usa a sugestão extraída da mensagem de erro do Databricks para garantir
        que a substituição seja para uma coluna que realmente existe na tabela.
        """
        unresolved = entities.get("unresolved_column")
        suggested = entities.get("suggested_column")

        if not unresolved or not suggested:
            return "Coluna não resolvida ou sugestão não identificada na mensagem de erro"

        # Caso especial: Databricks sugere '_placeholder' → a tabela foi criada pelo
        # self-healing com schema mínimo (id, _placeholder). Adiciona a coluna
        # faltante via ALTER TABLE em todas as placeholders encontradas no notebook.
        if suggested == "_placeholder":
            return self._fix_placeholder_add_column(notebook_path, unresolved)

        # Rejeita substituições perigosas: sugestão muito curta (≤3 chars) ou
        # nome genérico que indica falso positivo do "Did you mean" do Databricks.
        # Sugestão genérica como "id" quase sempre significa que a tabela placeholder
        # foi criada com schema mínimo — a coluna faltante deve ser ADICIONADA via
        # ALTER TABLE, não substituída.
        _GENERIC_COLUMNS = {"id", "key", "row", "idx", "num", "val", "col", "name"}
        if len(suggested) <= 3 or suggested.lower() in _GENERIC_COLUMNS:
            logger.warning(
                "NotebookFixer._fix_unresolved_column: sugestão '%s' rejeitada "
                "(muito genérica para substituir '%s' com segurança) — tentando ALTER TABLE",
                suggested,
                unresolved,
            )
            return self._fix_placeholder_add_column(notebook_path, unresolved)

        # Rejeita quando a sugestão remove semântica de negócio:
        # se o nome original tem prefixo de domínio (cd_, fl_, vl_, nm_, qt_, dt_)
        # e a sugestão não, é um falso positivo. Tenta ALTER TABLE como fallback.
        _DOMAIN_PREFIXES = ("cd_", "fl_", "vl_", "nm_", "qt_", "dt_", "id_", "tp_", "ds_")
        original_has_domain = any(unresolved.lower().startswith(p) for p in _DOMAIN_PREFIXES)
        suggested_has_domain = any(suggested.lower().startswith(p) for p in _DOMAIN_PREFIXES)
        if original_has_domain and not suggested_has_domain:
            logger.warning(
                "NotebookFixer._fix_unresolved_column: sugestão '%s' rejeitada "
                "(perde prefixo de domínio de '%s') — tentando ALTER TABLE",
                suggested,
                unresolved,
            )
            return self._fix_placeholder_add_column(notebook_path, unresolved)

        content = notebook_path.read_text(encoding="utf-8")

        # Substitui todas as ocorrências da coluna não resolvida entre aspas
        # e como referência F.col() no notebook
        old_quoted = f'"{unresolved}"'
        new_quoted = f'"{suggested}"'
        old_quoted_single = f"'{unresolved}'"
        new_quoted_single = f"'{suggested}'"

        if old_quoted not in content and old_quoted_single not in content:
            return f"Coluna '{unresolved}' não encontrada no notebook"

        count = content.count(old_quoted) + content.count(old_quoted_single)
        new_content = content.replace(old_quoted, new_quoted).replace(old_quoted_single, new_quoted_single)
        notebook_path.write_text(new_content, encoding="utf-8")
        return f"Coluna '{unresolved}' → '{suggested}' em {count} ocorrência(s) (sugestão Databricks)"

    def _fix_placeholder_add_column(
        self, notebook_path: Path, missing_column: str
    ) -> str:
        """Adiciona coluna faltante a todas as tabelas placeholder do notebook.

        Chamado quando Databricks sugere '_placeholder' para uma coluna não resolvida,
        indicando que a tabela foi criada pelo self-healing com schema mínimo
        (id BIGINT, _placeholder BOOLEAN). Injeta ALTER TABLE ADD COLUMN IF NOT EXISTS
        para cada placeholder encontrada no notebook.
        """
        content = notebook_path.read_text(encoding="utf-8")

        # Localiza todas as tabelas criadas como placeholder neste notebook
        placeholder_pattern = re.compile(
            r'spark\.sql\("CREATE TABLE IF NOT EXISTS\s+([\w.]+)\s+\(id BIGINT,\s*_placeholder',
            re.IGNORECASE,
        )
        tables = placeholder_pattern.findall(content)

        # Fallback: a tabela placeholder pode já existir no Databricks de uma sessão
        # anterior (IF NOT EXISTS faz o CREATE TABLE não aparecer no notebook novo).
        # Nesse caso, varre os spark.read.table() para descobrir quais tabelas
        # são candidatas e emite ALTER TABLE para todas.
        # Inclui f-strings resolvendo variáveis simples do notebook.
        if not tables:
            read_pattern = re.compile(
                r'spark\.read\.table\(["\']([^"\']+)["\']\)',
                re.IGNORECASE,
            )
            fstring_pattern = re.compile(
                r'spark\.read\.table\(f["\']([^"\']+)["\']\)',
                re.IGNORECASE,
            )
            # Filtra nomes de tabela inválidos (LLM placeholders) antes de emitir ALTER TABLE.
            # Tabelas com <>, sufixos genéricos ou sem schema qualificado são ruído.
            _INVALID_TABLE_TOKENS = frozenset({
                "minha_tabela", "nome_tabela", "tabela", "table_name",
                "schema_name", "catalog_name", "nome_da_tabela",
            })
            raw_tables = read_pattern.findall(content)

            # Resolve f-strings: extrai padrões {var} e substitui por valores
            # encontrados no notebook via `var = "valor"` ou `var = 'valor'`.
            fstring_templates = fstring_pattern.findall(content)
            for tmpl in fstring_templates:
                var_names = re.findall(r'\{([\w]+)\}', tmpl)
                resolved = tmpl
                for var in var_names:
                    m = re.search(
                        rf'\b{re.escape(var)}\s*=\s*["\']([^"\']+)["\']',
                        content,
                    )
                    if m:
                        resolved = resolved.replace('{' + var + '}', m.group(1))
                if '{' not in resolved:  # todas as variáveis foram resolvidas
                    raw_tables.append(resolved)
                    logger.debug(
                        "Fixer._fix_placeholder_add_column: f-string resolvida: %s → %s",
                        tmpl, resolved,
                    )

            tables = []
            for t in dict.fromkeys(raw_tables):  # preserva ordem, deduplica
                table_simple = t.split(".")[-1].lower()
                if "<" in t or ">" in t or "{" in t:
                    logger.debug("Fixer: tabela placeholder inválida ignorada: %s", t)
                    continue
                if table_simple in _INVALID_TABLE_TOKENS:
                    logger.debug("Fixer: nome genérico de tabela ignorado: %s", t)
                    continue
                tables.append(t)
            logger.info(
                "Fixer._fix_placeholder_add_column: CREATE TABLE não encontrado — "
                "usando %d/%d tabela(s) de read.table como candidatas (após filtro)",
                len(tables), len(raw_tables),
            )

        if not tables:
            return (
                f"Coluna '{missing_column}' não resolvida e nenhuma tabela candidata "
                "encontrada no notebook — requer revisão manual"
            )

        # Extrai TODAS as colunas do notebook — adiciona de uma vez para evitar
        # ciclos de UNRESOLVED_COLUMN (uma coluna por execução → N execuções para N colunas)
        all_cols = self._extract_column_names(content)
        if missing_column not in all_cols:
            all_cols.insert(0, missing_column)

        # Monta ALTER TABLE para cada tabela candidata, adicionando todas as colunas.
        # Databricks não suporta "ADD COLUMN IF NOT EXISTS" — usa try/except por coluna.
        alter_stmts = ""
        for t in tables:
            for col in all_cols:
                alter_stmts += (
                    f'try:\n'
                    f'    spark.sql("ALTER TABLE {t} ADD COLUMNS (`{col}` STRING)")\n'
                    f'except Exception:\n'
                    f'    pass  # coluna já existe\n'
                )
        alter_block = (
            f"# [AUTO-FIX] Adiciona {len(all_cols)} coluna(s) faltante(s) à placeholder\n"
            f"{alter_stmts}\n"
        )

        # Insere após o último CREATE TABLE placeholder (se existir) ou no início
        last_match = None
        for m in placeholder_pattern.finditer(content):
            last_match = m
        if last_match:
            end_of_line = content.find("\n", last_match.end())
            if end_of_line == -1:
                end_of_line = len(content)
            insert_pos = end_of_line + 1
            content = content[:insert_pos] + alter_block + content[insert_pos:]
        else:
            # Insere antes do primeiro spark.read.table em linha NÃO comentada
            # para garantir que a coluna existe antes de qualquer leitura.
            # Importante: ignora matches dentro de linhas de comentário (#)
            # para não quebrar a linha e expor fragmentos como código executável.
            insert_pos = None
            for m in re.finditer(r'spark\.read\.table\(', content):
                # Verifica se a linha do match começa com # (comentário)
                line_start = content.rfind("\n", 0, m.start()) + 1
                line_text = content[line_start:m.start()]
                if not line_text.lstrip().startswith("#"):
                    insert_pos = m.start()
                    break
            if insert_pos is None:
                insert_pos = 0
            content = content[:insert_pos] + alter_block + content[insert_pos:]

        notebook_path.write_text(content, encoding="utf-8")
        return (
            f"Coluna '{missing_column}' adicionada via ALTER TABLE em "
            f"{len(tables)} placeholder(s): {', '.join(tables)}"
        )

    def _fix_parse_syntax_if_not_exists(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para PARSE_SYNTAX_ERROR causado por ADD COLUMN IF NOT EXISTS.

        Delega ao static_validator que tem o fixer completo para esse padrão,
        garantindo que o notebook seja corrigido antes do próximo retest.
        """
        from sas2dbx.validate.heal.static_validator import StaticNotebookValidator
        validator = StaticNotebookValidator(
            catalog=entities.get("catalog", ""),
            schema=entities.get("schema", ""),
        )
        report = validator.validate_notebook(notebook_path)
        if report.changed:
            return f"PARSE_SYNTAX_ERROR/EXISTS corrigido via static_validator: {'; '.join(report.fixes)}"
        return "PARSE_SYNTAX_ERROR/EXISTS: padrão IF NOT EXISTS não encontrado no notebook — verificar manualmente"

    def _fix_stack_type_mismatch(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='stack_type_mismatch'.

        Adiciona CAST(col AS DOUBLE) em todas as colunas de valor dentro de
        expressões stack() geradas pelo PROC TRANSPOSE, garantindo tipo uniforme.

        DATATYPE_MISMATCH.STACK_COLUMN_DIFF_TYPES ocorre quando stack() recebe
        colunas de tipos diferentes (ex: DOUBLE e BIGINT misturados).
        """
        content = notebook_path.read_text(encoding="utf-8")

        # Padrão: 'label_string', column_name dentro de stack(...)
        # Substitui column_name por CAST(column_name AS DOUBLE)
        stack_value_pattern = re.compile(
            r"('[^'\"]+'\s*,\s*)(\b(?!CAST\b)([a-zA-Z_]\w*))\b(?!\s*AS\b)"
        )

        def add_cast(m: re.Match) -> str:
            label_part = m.group(1)
            col_name = m.group(2)
            # Não casteia se já é uma string literal ou CAST()
            if col_name.startswith(("'", '"')) or col_name.upper() == "CAST":
                return m.group(0)
            return f"{label_part}CAST({col_name} AS DOUBLE)"

        # Aplica apenas dentro de expressões stack(...)
        stack_block_pattern = re.compile(
            r'(stack\(\d+,)(.*?)(\)\s*as\s*\([^)]+\))',
            re.DOTALL | re.IGNORECASE,
        )

        def fix_stack_block(sm: re.Match) -> str:
            prefix = sm.group(1)
            body = sm.group(2)
            suffix = sm.group(3)
            new_body = stack_value_pattern.sub(add_cast, body)
            return prefix + new_body + suffix

        new_content = stack_block_pattern.sub(fix_stack_block, content)

        if new_content == content:
            return "Nenhuma expressão stack() encontrada para corrigir"

        notebook_path.write_text(new_content, encoding="utf-8")
        return "CAST(... AS DOUBLE) adicionado nas colunas de stack() para uniformizar tipos"

    def _fix_function_not_found(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='missing_function'.

        Substitui funções SQL SAS não suportadas pelo Spark por equivalentes.
        Usa o catálogo _SQL_FUNCTION_MAP para matching case-insensitive.

        Suporte atual:
          MONOTONIC() → ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id())
          DATDIF(     → datediff(

        Args:
            notebook_path: Path local do notebook.
            entities: Deve conter "function_name" extraído da mensagem de erro.
        """
        func_name = entities.get("function_name", "").upper().rstrip("()")
        content = notebook_path.read_text(encoding="utf-8")

        # Tenta match direto pelo nome da função extraído da mensagem de erro
        entry = _SQL_FUNCTION_MAP.get(func_name)

        # Se não encontrou pelo nome extraído, tenta todos os padrões (scan full content)
        if entry is None:
            for candidate_name, (pattern_str, replacement) in _SQL_FUNCTION_MAP.items():
                if re.search(pattern_str, content, re.IGNORECASE):
                    func_name = candidate_name
                    entry = (pattern_str, replacement)
                    break

        if entry is None:
            return f"Função '{func_name}' sem mapeamento determinístico — requer fix manual"

        pattern_str, replacement = entry
        pattern = re.compile(pattern_str, re.IGNORECASE)
        matches = list(pattern.finditer(content))
        if not matches:
            return f"Padrão para '{func_name}' não encontrado no notebook"

        new_content = pattern.sub(replacement, content)
        notebook_path.write_text(new_content, encoding="utf-8")
        replacement_display = replacement[:60] + ("..." if len(replacement) > 60 else "")
        return f"Função '{func_name}' → '{replacement_display}' em {len(matches)} ocorrência(s)"

    def _fix_when_otherwise_type(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='cast_type_mismatch'.

        Corrige F.when(..., F.lit(N)).otherwise(F.col(...)) onde N é inteiro
        e a coluna otherwise é STRING. Spark tenta unificar INTEGER e STRING
        casteando a string para BIGINT → CAST_INVALID_INPUT.

        Fix: converte F.lit(N) → F.lit("N") para type compatibility.
        """
        content = notebook_path.read_text(encoding="utf-8")

        # Detecta F.lit(integer).otherwise(F.col ou F.lit(string)
        pattern = re.compile(
            r'(F\.lit\()(\d+)(\)\)\.otherwise\((?:F\.col|F\.lit)\()'
        )

        matches = list(pattern.finditer(content))
        if not matches:
            return "Nenhum F.lit(integer).otherwise(F.col()) encontrado"

        new_content = pattern.sub(lambda m: f'{m.group(1)}"{m.group(2)}"{m.group(3)}', content)
        notebook_path.write_text(new_content, encoding="utf-8")
        return f"F.lit(integer) → F.lit(\"string\") em {len(matches)} expressão(ões) when().otherwise()"
