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
    "fix_rdd_flatmap": "_fix_rdd_flatmap",
    "fix_output_column_exists": "_fix_output_column_exists",
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
            raw_msg = getattr(diagnostic, "error_raw", "") or ""
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
          F\.col\(\s*["'](?:\w+\.)?(\w+)["']\s*\)   # F.col("alias.col") ou F.col('col')
          | \.col\(\s*["'](?:\w+\.)?(\w+)["']\s*\)  # .col("alias.col")
          | \bcol\(\s*["'](?:\w+\.)?(\w+)["']\s*\)  # col("alias.col") sem F.
          | \[["'](\w+)["']\]                         # df["col"] ou df['col']
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

    # Tokens que indicam que um nome de tabela é um placeholder LLM, não um nome real.
    # CREATE TABLE para esses nomes cria tabelas vazias que causam erros downstream
    # (ex: chi_sq_value = None → TypeError: unsupported format string passed to NoneType.__format__).
    _PLACEHOLDER_TABLE_TOKENS: frozenset[str] = frozenset({
        "nome_da_tabela", "nome_tabela", "table_name", "my_table",
        "ajustar", "scored_default", "ds_scored_default", "minha_tabela",
        "your_table", "example_table", "placeholder", "nome_da_tabela_scored",
        "nome_tabela_scored", "default", "nome_tabela_agg", "ds_agg",
        "ds_pivot", "nome_tabela_pivot",
    })

    def _looks_like_placeholder_name(self, simple_name: str) -> bool:
        """True se simple_name parece ser um placeholder LLM (não um nome real de tabela)."""
        low = simple_name.lower()
        return any(tok in low for tok in self._PLACEHOLDER_TABLE_TOKENS)

    def _fix_placeholder_table_name(
        self, notebook_path: Path, table_name: str
    ) -> str | None:
        """Tenta substituir o valor placeholder de uma variável pelo nome real da tabela.

        Procura no notebook por outras atribuições da mesma variável com valor não-placeholder.
        Retorna descrição do fix se bem-sucedido, None se não encontrou substituto.
        """
        content = notebook_path.read_text(encoding="utf-8")
        simple_name = table_name.split(".")[-1]

        # Encontra qual variável Python aponta para simple_name
        var_pattern = re.compile(
            rf'\b(\w+)\s*=\s*["\']({re.escape(simple_name)})["\']',
            re.IGNORECASE,
        )
        var_name: str | None = None
        for m in var_pattern.finditer(content):
            var_name = m.group(1)
            break

        if not var_name:
            return None

        # Coleta outros valores atribuídos à mesma variável que não são placeholder
        other_values_pattern = re.compile(
            rf'\b{re.escape(var_name)}\s*=\s*["\']([^"\']+)["\']',
        )
        real_values: list[str] = []
        for m in other_values_pattern.finditer(content):
            val = m.group(1)
            if not self._looks_like_placeholder_name(val):
                real_values.append(val)

        if not real_values:
            return None

        # Usa o valor mais frequente como substituto
        from collections import Counter
        real_value = Counter(real_values).most_common(1)[0][0]

        # Substitui a atribuição placeholder (aspas duplas primeiro, depois simples)
        new_content = content.replace(
            f'{var_name} = "{simple_name}"',
            f'{var_name} = "{real_value}"  # [AUTO-FIX] placeholder substituído',
            1,
        )
        if new_content == content:
            new_content = content.replace(
                f"{var_name} = '{simple_name}'",
                f'{var_name} = "{real_value}"  # [AUTO-FIX] placeholder substituído',
                1,
            )

        if new_content == content:
            return None

        notebook_path.write_text(new_content, encoding="utf-8")
        logger.info(
            "Fixer: placeholder '%s' → '%s' substituído para variável '%s'",
            simple_name, real_value, var_name,
        )
        return f"Placeholder '{simple_name}' substituído por '{real_value}' em {var_name}"

    def _fix_create_placeholder_table(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='missing_table'.

        Insere CREATE TABLE IF NOT EXISTS antes do primeiro bloco de código.
        O schema da placeholder é construído a partir de TODAS as colunas
        referenciadas no notebook, evitando ciclos de UNRESOLVED_COLUMN.

        Se o nome da tabela parece um placeholder LLM (ex: 'nome_da_tabela_scored'),
        tenta substituir a variável pelo nome real em vez de criar tabela vazia.
        Tabelas vazias causam erros downstream (ex: chi_sq_value=None → TypeError).
        """
        table_name = entities.get("table_name", "unknown_table")
        parts = table_name.split(".")

        # Tenta substituir placeholder por nome real antes de criar tabela vazia
        simple_name = parts[-1]
        if self._looks_like_placeholder_name(simple_name):
            fix_result = self._fix_placeholder_table_name(notebook_path, table_name)
            if fix_result:
                logger.info("Fixer: placeholder resolvido — %s", fix_result)
                return fix_result
            logger.warning(
                "Fixer: nome placeholder '%s' detectado mas sem substituto encontrado — "
                "criando tabela vazia (pode causar erros downstream)",
                simple_name,
            )
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

    # Regex para extrair pares (alias, colname) de uma lista de sugestões Databricks
    # Ex: "`c`.`meses_ativo`, `u`.`id`" → [("c", "meses_ativo"), ("u", "id")]
    _SUGGESTION_RE = re.compile(r'`?(\w+)`?\s*\.\s*`?(\w+)`?')

    def _pick_best_suggestion(
        self, suggestions_raw: str, table_alias: str, unresolved: str
    ) -> str | None:
        """Escolhe a melhor sugestão da lista fornecida pelo Databricks.

        Prefere sugestões do mesmo alias de tabela que a coluna não resolvida.
        Rejeita sugestões genéricas (id, key, num, etc.) e muito curtas.
        """
        _GENERIC_COLUMNS = {"id", "key", "row", "idx", "num", "val", "col", "name"}

        candidates: list[str] = []
        for m in self._SUGGESTION_RE.finditer(suggestions_raw):
            alias, colname = m.group(1), m.group(2)
            if len(colname) <= 3 or colname.lower() in _GENERIC_COLUMNS:
                continue
            if colname.lower() == unresolved.lower():
                continue  # não substituir por ela mesma
            # Prioriza sugestões do mesmo alias
            if alias == table_alias:
                candidates.insert(0, colname)
            else:
                candidates.append(colname)

        return candidates[0] if candidates else None

    def _fix_unresolved_column(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='unresolved_column_suggestion'.

        Substitui a referência à coluna não resolvida pela sugestão do Databricks.
        Chaves de entidade produzidas por patterns.py:
          - bad_column: nome da coluna não resolvida (sem alias)
          - table_alias: alias da tabela (ex: "c" em "c.fl_ativo")
          - suggestions: lista de sugestões do Databricks (string raw)
        """
        # Suporta tanto as chaves de patterns.py quanto as legadas
        unresolved = entities.get("bad_column") or entities.get("unresolved_column")
        table_alias = entities.get("table_alias", "")
        suggestions_raw = entities.get("suggestions", "")
        suggested = (
            entities.get("suggested_column")
            or self._pick_best_suggestion(suggestions_raw, table_alias, unresolved or "")
        )

        if not unresolved:
            return "Coluna não resolvida não identificada na mensagem de erro"

        if not suggested:
            return f"Nenhuma sugestão válida para '{unresolved}' — revisão manual necessária"

        # Caso especial: Databricks sugere '_placeholder' → schema mínimo
        if suggested == "_placeholder":
            return self._fix_placeholder_add_column(notebook_path, unresolved)

        # Rejeita substituições genéricas (id, key, etc.)
        _GENERIC_COLUMNS = {"id", "key", "row", "idx", "num", "val", "col", "name"}
        if len(suggested) <= 3 or suggested.lower() in _GENERIC_COLUMNS:
            logger.warning(
                "NotebookFixer._fix_unresolved_column: sugestão '%s' rejeitada "
                "(muito genérica para substituir '%s') — tentando ALTER TABLE",
                suggested,
                unresolved,
            )
            return self._fix_placeholder_add_column(notebook_path, unresolved)

        content = notebook_path.read_text(encoding="utf-8")

        # Tenta substituição simples (coluna sem alias prefix): "fl_ativo" → "meses_ativo"
        old_quoted = f'"{unresolved}"'
        new_quoted = f'"{suggested}"'
        old_quoted_single = f"'{unresolved}'"
        new_quoted_single = f"'{suggested}'"

        if old_quoted in content or old_quoted_single in content:
            # Segurança: não substituir se a sugestão já existe no notebook
            if f'"{suggested}"' in content or f"'{suggested}'" in content:
                logger.warning(
                    "NotebookFixer._fix_unresolved_column: sugestão '%s' já existe "
                    "— redirecionando para D3",
                    suggested,
                )
                return self._fix_placeholder_add_column(notebook_path, unresolved)
            count = content.count(old_quoted) + content.count(old_quoted_single)
            new_content = (
                content.replace(old_quoted, new_quoted).replace(old_quoted_single, new_quoted_single)
            )
            notebook_path.write_text(new_content, encoding="utf-8")
            return f"Coluna '{unresolved}' → '{suggested}' em {count} ocorrência(s)"

        # Tenta substituição com alias prefix: "alias.fl_ativo" → "alias.meses_ativo"
        # Cobre referências do tipo F.col("c.fl_ativo") geradas pelo LLM com alias de JOIN
        alias_col_re = re.compile(
            rf'(["\'])(?P<alias>\w+)\.{re.escape(unresolved)}\1'
        )
        if alias_col_re.search(content):
            alias_for_sub = table_alias or alias_col_re.search(content).group("alias")
            old_alias_d = f'"{alias_for_sub}.{unresolved}"'
            old_alias_s = f"'{alias_for_sub}.{unresolved}'"
            new_alias_d = f'"{alias_for_sub}.{suggested}"'
            new_alias_s = f"'{alias_for_sub}.{suggested}'"
            count = content.count(old_alias_d) + content.count(old_alias_s)
            new_content = (
                content.replace(old_alias_d, new_alias_d).replace(old_alias_s, new_alias_s)
            )
            notebook_path.write_text(new_content, encoding="utf-8")
            return (
                f"Coluna '{alias_for_sub}.{unresolved}' → '{alias_for_sub}.{suggested}'"
                f" em {count} ocorrência(s) (sugestão Databricks)"
            )

        return f"Coluna '{unresolved}' não encontrada no notebook"

    @staticmethod
    def _is_table_owned(content: str, table_name: str) -> bool:
        """Retorna True se o notebook escreve na tabela via saveAsTable.

        D3 — Schema Mutation Gate: tabelas que o notebook não escreve são
        consideradas externas; ALTER TABLE nelas é arriscado (permissões,
        dados físicos que não têm a coluna). Use withColumn como alternativa.
        """
        table_short = table_name.split(".")[-1]
        # Verifica se há saveAsTable(...table_short...) no notebook
        pattern = re.compile(
            rf'saveAsTable\s*\(\s*["\'].*{re.escape(table_short)}["\']',
            re.IGNORECASE,
        )
        return bool(pattern.search(content))

    @classmethod
    def _inject_withcolumn_for_external(
        cls, content: str, missing_column: str, all_cols: list[str]
    ) -> str:
        """Injeta withColumn(col, NULL) após cada spark.read.table() assignment.

        Usado para tabelas externas (D3): sem ALTER TABLE, a coluna é adicionada
        em memória ao DataFrame. O guard `if col not in df.columns` garante
        idempotência — re-execuções não duplicam colunas.

        Regras do regex (evita falsos positivos):
          - Captura apenas SIMPLE assignments: `df = spark.read.table(...)` ou
            `df = spark.table(...)` sem chaining adicional (.count(), .first(), etc.)
          - Usa [^)]* em vez de .*? para impedir expansão até o último ) da linha
            (que quebraria `count = spark.table(t).count()` pois count é int)
          - Usa F.lit(None) em vez de _F.lit(None) — F já está importado no notebook
        """
        # [^)]* garante que não há parênteses aninhados/chaining no argumento.
        # Isso exclui `count = spark.table(t).count()` porque após o primeiro `)`
        # vem `.count()`, não fim de linha.
        read_assign_re = re.compile(
            r'^( *)(\w+)\s*=\s*spark\.(?:read\.table|table)\([^)]*\)\s*$',
            re.MULTILINE,
        )
        matches = list(read_assign_re.finditer(content))
        if not matches:
            return content

        # Injeta a coluna faltante + TODAS as colunas de negócio do notebook.
        # Inclui tanto colunas com prefixo de domínio (fl_, vl_, qt_, etc.)
        # quanto colunas snake_case de negócio (familia_plano, score_churn, etc.).
        # Exclui nomes ALL_CAPS (aliases de agregação SQL como COUNT, PERCENT),
        # nomes com ≤3 chars (aliases curtos), e colunas de controle Spark.
        # O guard no notebook garante idempotência (cast apenas se necessário).
        _SQL_AGG_PATTERN = re.compile(r'^[A-Z_][A-Z0-9_]*$')  # ALL_CAPS ou ALL_CAPS_
        _EXCLUDE_NAMES = {
            "count", "total", "row_num", "rank", "sum", "avg", "min", "max",
            "row_number", "dense_rank", "first", "last",
        }
        business_cols = [
            c for c in all_cols
            if c != missing_column
            and len(c) > 3
            and not _SQL_AGG_PATTERN.match(c)  # exclui ALL_CAPS
            and c.lower() not in _EXCLUDE_NAMES
            and "_" in c  # nomes snake_case — provavelmente colunas de negócio
        ]
        cols_to_add: list[str] = [missing_column] + business_cols
        cols_repr = "[" + ", ".join(f'"{c}"' for c in cols_to_add) + "]"

        # Infere tipo PySpark pelo prefixo do nome da coluna para evitar TYPE_MISMATCH.
        # Gera _d3_type_map (col → type string) para o notebook.
        # O código gerado faz cast em DOIS cenários:
        #   1. Coluna ausente → adiciona como NULL com tipo correto (resolve UNRESOLVED_COLUMN)
        #   2. Coluna presente mas tipo errado (ex: STRING onde SAS exportou numero como texto)
        #      → recast para tipo correto (resolve DATATYPE_MISMATCH)
        def _col_type(col: str) -> str:
            c = col.lower()
            if c.startswith(("vl_", "pct_", "perc_", "score_", "prob_", "pred_", "media_", "acum_", "receita_", "sum_", "chi_")):
                return "double"
            if c.startswith(("qt_", "nr_", "num_", "n_", "meses_", "rank_", "frequencia")):
                return "long"
            if c.startswith(("fl_", "ind_")):
                return "int"
            if c.startswith("dt_"):
                return "date"
            if c.startswith(("id_", "sk_", "seq_")):
                return "long"
            # cd_, nm_, ds_, tp_, sufixo_, familia_, motivo_, e desconhecidos → string
            return "string"

        # Constrói mapa col → type string para injeção inline
        type_map = "{" + ", ".join(f'"{c}": "{_col_type(c)}"' for c in cols_to_add) + "}"

        # Insere da última para a primeira para preservar posições
        for m in reversed(matches):
            indent = m.group(1)
            var_name = m.group(2)
            # Gera código com if/else:
            #   - col ausente → adiciona como NULL com tipo correto
            #   - col presente → recast para tipo correto (corrige STRING→LONG etc.)
            patch = (
                f"\n{indent}# [AUTO-FIX D3] Tabela externa — colunas ausentes/tipo errado via withColumn\n"
                f"{indent}_d3_type_map = {type_map}\n"
                f"{indent}for _c, _t in _d3_type_map.items():\n"
                f"{indent}    if _c in {var_name}.columns:\n"
                f"{indent}        {var_name} = {var_name}.withColumn(_c, F.col(_c).cast(_t))\n"
                f"{indent}    else:\n"
                f"{indent}        {var_name} = {var_name}.withColumn(_c, F.lit(None).cast(_t))"
            )
            insert_pos = m.end()
            content = content[:insert_pos] + patch + content[insert_pos:]

        return content

    def _fix_placeholder_add_column(
        self, notebook_path: Path, missing_column: str
    ) -> str:
        """Adiciona coluna faltante a tabelas do notebook.

        D3 — Schema Mutation Gate:
          - Tabelas OWNED (saveAsTable no notebook): ALTER TABLE + REFRESH TABLE.
          - Tabelas EXTERNAS (apenas read): withColumn em memória após cada read.

        Chamado quando Databricks sugere '_placeholder' ou quando a coluna
        não existe na tabela referenciada (falso positivo de sugestão genérica).
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
                # Nomes que claramente são variáveis Python usadas como templates:
                "nome_tabela_scored", "nome_tabela_agg", "nome_tabela_pre",
                "nome_tabela_out", "nome_tabela_in", "nome_tabela_resultado",
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
                # Databricks suporta no máximo 3 partes (catalog.schema.table).
                # Nomes com 4+ partes são invariavelmente erros de resolução de f-string
                # (ex: lib_in="telcostar.operacional" + catalog="telcostar" → 4 partes).
                if t.count(".") > 2:
                    logger.debug("Fixer: tabela com nome inválido (>3 partes) ignorada: %s", t)
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

        # D3 — Schema Mutation Gate: separa tabelas owned vs external.
        # Owned (saveAsTable no notebook): ALTER TABLE é seguro — schema evolution Delta.
        # External (apenas leitura): ALTER TABLE pode falhar por falta de permissão DDL
        # ou não propagar em tabelas Parquet externas. Usa withColumn em memória.
        owned_tables = [t for t in tables if self._is_table_owned(content, t)]
        external_tables = [t for t in tables if not self._is_table_owned(content, t)]

        logger.info(
            "Fixer D3: %d tabela(s) owned (ALTER TABLE), %d external (withColumn): owned=%s ext=%s",
            len(owned_tables), len(external_tables), owned_tables, external_tables,
        )

        # --- OWNED: ALTER TABLE + REFRESH TABLE ---
        if owned_tables:
            # Detecta a indentação do ponto de inserção para não quebrar blocos
            # de função. Se o ALTER TABLE for inserido dentro de uma função com
            # indentação 4, o try:/except: deve também ter indentação 4 — caso
            # contrário, o try: em coluna 0 ENCERRA a função prematuramente.
            #
            # REGRA: só usa spark.read.table() em nível de módulo (não dentro de defs).
            # Linhas com indentação inicial (spaces/tabs antes de "spark") estão em
            # função/class e DEVEM ser ignoradas como ponto de inserção.
            insert_pos_detect = None
            for m in re.finditer(r'spark\.read\.table\(', content):
                line_start = content.rfind("\n", 0, m.start()) + 1
                line_text = content[line_start:m.start()]
                # Pula linhas comentadas OU com indentação (dentro de função/class)
                if line_text.lstrip().startswith("#"):
                    continue
                if line_text.startswith((" ", "\t")):
                    continue
                insert_pos_detect = line_start
                break
            if insert_pos_detect is None:
                insert_pos_detect = 0
            # Extrai o indent da linha alvo (leading spaces)
            target_line_end = content.find("\n", insert_pos_detect)
            target_line = content[insert_pos_detect:target_line_end] if target_line_end != -1 else content[insert_pos_detect:]
            ctx_indent = " " * (len(target_line) - len(target_line.lstrip()))

            alter_stmts = ""
            for t in owned_tables:
                for col in all_cols:
                    alter_stmts += (
                        f'{ctx_indent}try:\n'
                        f'{ctx_indent}    spark.sql("ALTER TABLE {t} ADD COLUMNS (`{col}` STRING)")\n'
                        f'{ctx_indent}except Exception as _e:\n'
                        f'{ctx_indent}    if "already exists" not in str(_e).lower():\n'
                        f'{ctx_indent}        print(f"[AUTO-FIX] ALTER TABLE {t}.{col} falhou: {{_e}}")\n'
                    )
                alter_stmts += (
                    f'{ctx_indent}try:\n'
                    f'{ctx_indent}    spark.sql("REFRESH TABLE {t}")\n'
                    f'{ctx_indent}except Exception:\n'
                    f'{ctx_indent}    pass\n'
                )

            alter_block = (
                f"{ctx_indent}# [AUTO-FIX] Adiciona {len(all_cols)} coluna(s) a {len(owned_tables)} tabela(s) owned\n"
                f"{alter_stmts}\n"
            )
            # Insere após o último CREATE TABLE placeholder (se existir) ou no início da linha alvo
            last_match = None
            for m in placeholder_pattern.finditer(content):
                last_match = m
            if last_match:
                end_of_line = content.find("\n", last_match.end())
                if end_of_line == -1:
                    end_of_line = len(content)
                insert_pos = end_of_line + 1
            else:
                insert_pos = insert_pos_detect
            content = content[:insert_pos] + alter_block + content[insert_pos:]

        # --- EXTERNAL: withColumn em memória após cada spark.read.table() ---
        if external_tables:
            content = self._inject_withcolumn_for_external(content, missing_column, all_cols)

        # Se não houve nenhuma ação (sem owned nem external com read pattern detectável)
        if not owned_tables and not external_tables:
            notebook_path.write_text(content, encoding="utf-8")
            return (
                f"Coluna '{missing_column}' — nenhuma tabela owned ou external detectável; "
                "notebook não modificado"
            )

        notebook_path.write_text(content, encoding="utf-8")

        parts = []
        if owned_tables:
            parts.append(f"ALTER TABLE em {len(owned_tables)} owned: {', '.join(owned_tables)}")
        if external_tables:
            parts.append(f"withColumn em {len(external_tables)} external: {', '.join(external_tables)}")
        return f"Coluna '{missing_column}' — D3: " + "; ".join(parts)

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

        # Padrão 2: stack_expr dinâmico via join/f-string (gerado pelo LLM para PROC TRANSPOSE).
        # Ex: stack_expr = ", ".join([f"'{c}', `{c}`" for c in var_cols])
        # Fix: substitui `{c}` por CAST(`{c}` AS DOUBLE) para uniformizar tipos.
        if new_content == content:
            # Detecta variantes comuns do padrão f"'{var}', `{var}`"
            dynamic_re = re.compile(
                r"""(stack_expr\s*=\s*["'][^"']*["']\.join\(\[f["']'[^']+',\s*)`(\{[^}]+\})`"""
            )
            new_content = dynamic_re.sub(
                lambda m: m.group(0).replace(
                    f"`{m.group(2)}`", f"CAST(`{m.group(2)}` AS DOUBLE)"
                ),
                new_content,
            )

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

    def _fix_rdd_flatmap(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='rdd_not_allowed' (NOT_IMPLEMENTED em serverless).

        Substitui o padrão .rdd.flatMap(lambda x: x).collect() por uma
        list comprehension equivalente compatível com Databricks Serverless.

        Padrão detectado:
            .rdd.flatMap(lambda x: x)
            .collect()

        Substituído por:
            # via collect() + list comprehension (sem RDD — compatível com serverless)
        """
        content = notebook_path.read_text(encoding="utf-8")

        # Regex para capturar o bloco DataFrame + .rdd.flatMap(lambda x: x)\n    .collect()
        rdd_pattern = re.compile(
            r"(\.select\([^)]+\)\s*\n?\s*\.distinct\(\)\s*\n?\s*\.orderBy\([^)]+\))\s*\n?\s*"
            r"\.rdd\.flatMap\(lambda x: x\)\s*\n?\s*\.collect\(\)",
            re.MULTILINE,
        )
        matches = list(rdd_pattern.finditer(content))
        if not matches:
            # Fallback: replace any .rdd.flatMap(lambda x: x) followed by .collect()
            simple_pattern = re.compile(
                r"\.rdd\.flatMap\(lambda x: x\)\s*\n?\s*\.collect\(\)",
                re.MULTILINE,
            )
            matches_simple = list(simple_pattern.finditer(content))
            if not matches_simple:
                return "Nenhum padrão .rdd.flatMap(lambda x: x).collect() encontrado"
            new_content = simple_pattern.sub(
                ".collect()  # [AUTO-FIX] .rdd removido (não suportado em serverless)",
                content,
            )
            notebook_path.write_text(new_content, encoding="utf-8")
            # Agora precisamos envolver o collect() em list comprehension
            # Localiza os locais que agora têm apenas .collect() (sem rdd) e ajusta atribuição
            # Busca padrão: VAR = (\n  df\n  .select(...)\n  ...\n  .collect()\n)
            assign_pattern = re.compile(
                r"(\w+\s*=\s*\([^)]*?)\.collect\(\s*\)\s*\n?\s*\)",
                re.MULTILINE | re.DOTALL,
            )
            content2 = notebook_path.read_text(encoding="utf-8")
            new_content2 = assign_pattern.sub(
                lambda m: m.group(0).replace(
                    ".collect()",
                    ".collect()  # [AUTO-FIX] serverless-safe collect"
                ),
                content2,
            )
            notebook_path.write_text(new_content2, encoding="utf-8")
            return f".rdd.flatMap substituído em {len(matches_simple)} local(is) — serverless-safe"

        # Substituição precisa: mantém o DataFrame chain, mas troca .rdd.flatMap.collect()
        # por [row[0] for row in ....collect()]
        def replace_rdd(m: re.Match) -> str:
            chain = m.group(1)
            return f"{chain}\n    .collect()  # [AUTO-FIX] .rdd.flatMap removido (serverless)"

        new_content = rdd_pattern.sub(replace_rdd, content)
        # Agora ajusta a atribuição para usar list comprehension
        lc_pattern = re.compile(
            r"(\w+)\s*=\s*\(\s*\n"
            r"([^\n]+\n(?:[^\n]*\n)*?)"
            r"    \.collect\(\)\s*# \[AUTO-FIX\] \.rdd\.flatMap removido \(serverless\)\s*\n"
            r"\)",
            re.MULTILINE,
        )
        def replace_with_lc(m: re.Match) -> str:
            var = m.group(1)
            chain = m.group(2)
            return (
                f"{var} = [\n"
                f"    row[0]\n"
                f"    for row in (\n"
                f"{chain}"
                f"        .collect()  # [AUTO-FIX] serverless-safe (sem .rdd)\n"
                f"    )\n"
                f"]"
            )

        new_content2 = lc_pattern.sub(replace_with_lc, new_content)
        if new_content2 == new_content:
            # Se não conseguiu fazer o lc_pattern, usa a versão simples
            notebook_path.write_text(new_content, encoding="utf-8")
            return f"Removido .rdd.flatMap em {len(matches)} local(is) — collect() mantido"

        notebook_path.write_text(new_content2, encoding="utf-8")
        return f"Substituído .rdd.flatMap por list comprehension em {len(matches)} local(is) — serverless-safe"

    def _fix_output_column_exists(
        self, notebook_path: Path, entities: dict[str, str]
    ) -> str:
        """Handler para category='output_column_exists'.

        Erro: IllegalArgumentException: Output column <col> already exists.
        Causa: StringIndexer/VectorAssembler ou withColumn tentando criar coluna
        que já existe no DataFrame.

        Fix:
          - withColumn("col", ...) → drop("col").withColumn("col", ...)
          - .transform(df) onde outputCol=col → insere df = df.drop("col") antes
        """
        col = entities.get("column_name", "")
        if not col:
            return "Nenhum nome de coluna extraído do erro — sem fix determinístico"

        content = notebook_path.read_text(encoding="utf-8")
        fixes = 0

        # Fix 1: withColumn("col", ...) → drop("col").withColumn("col", ...)
        with_col_pattern = re.compile(
            r'(\.withColumn\(\s*["\']' + re.escape(col) + r'["\'])',
            re.IGNORECASE,
        )
        if with_col_pattern.search(content):
            content = with_col_pattern.sub(
                f'.drop("{col}")\\1  # [AUTO-FIX] drop col duplicada',
                content,
            )
            fixes += 1

        # Fix 2: .transform(df) imediatamente após StringIndexer com outputCol=col
        # Insere df = df.drop("col") na linha anterior ao .fit() ou .transform()
        transform_pattern = re.compile(
            r'([ \t]*)(\w+\s*=\s*\w+\.fit\(\w+\)\.transform\(\w+\))',
            re.MULTILINE,
        )

        def _insert_drop(m: re.Match) -> str:
            indent = m.group(1)
            stmt = m.group(2)
            # Extrai o DataFrame passado para transform
            df_match = re.search(r'\.transform\((\w+)\)', stmt)
            df_var = df_match.group(1) if df_match else None
            if df_var:
                return (
                    f'{indent}{df_var} = {df_var}.drop("{col}")  '
                    f'# [AUTO-FIX] remove col duplicada antes do transform\n'
                    f'{indent}{stmt}'
                )
            return m.group(0)

        # Só aplica Fix 2 se o outputCol=col estiver nas linhas próximas
        if f'outputCol="{col}"' in content or f"outputCol='{col}'" in content:
            new_content = transform_pattern.sub(_insert_drop, content)
            if new_content != content:
                content = new_content
                fixes += 1

        if fixes == 0:
            return f"Padrão de coluna duplicada '{col}' não encontrado para fix determinístico"

        notebook_path.write_text(content, encoding="utf-8")
        return f"Coluna duplicada '{col}' corrigida em {fixes} local(is)"
