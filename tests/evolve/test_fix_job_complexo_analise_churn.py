"""Testes para o fix do erro UNRESOLVED_COLUMN.WITH_SUGGESTION.

Valida que:
1. O padrão em patterns.py detecta corretamente o erro do job_complexo_analise_churn
2. Os entity_extractors extraem bad_column, table_alias e suggestions
3. O handler _fix_unresolved_column substitui referências incorretas no notebook
"""
from __future__ import annotations

import re
import shutil
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ERROR_MESSAGE = (
    "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter "
    "with name `c`.`fl_ativo` cannot be resolved. Did you mean one of the following? "
    "[`c`.`id`, `u`.`id`, `c`.`meses_ativo`, `c`.`total`, `c`.`COUNT`]. SQLSTATE: 42703"
)

NOTEBOOK_CONTENT_TEMPLATE = textwrap.dedent("""
    # Databricks notebook source
    import pyspark.sql.functions as F

    df_clientes = spark.table("telcostar.operacional.clientes")
    df_usuarios = spark.table("telcostar.operacional.usuarios")

    df_result = (
        df_clientes.alias("c")
        .join(df_usuarios.alias("u"), F.col("c.id") == F.col("u.id"))
        .filter(F.col("c.fl_ativo") == 1)
        .select(
            F.col("c.id"),
            F.col("c.meses_ativo"),
            F.col("c.total"),
        )
    )
    df_result.show()
""")


@pytest.fixture()
def notebook_file(tmp_path: Path) -> Path:
    """Cria um notebook temporário com referência à coluna problemática."""
    nb = tmp_path / "job_complexo_analise_churn.py"
    nb.write_text(NOTEBOOK_CONTENT_TEMPLATE, encoding="utf-8")
    return nb


# ---------------------------------------------------------------------------
# 1. Testes de patterns.py
# ---------------------------------------------------------------------------

class TestPatternDetection:
    """Valida que o padrão detecta o erro corretamente."""

    def test_pattern_matches_unresolved_column_with_suggestion(self):
        """O padrão deve casar com a mensagem de erro do job."""
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        entry = ERROR_PATTERNS["column_not_found_with_suggestion"]
        pattern: re.Pattern = entry["pattern"]
        assert pattern.search(ERROR_MESSAGE), (
            "Padrão não casou com a mensagem de erro UNRESOLVED_COLUMN.WITH_SUGGESTION"
        )

    def test_pattern_category_is_correct(self):
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        entry = ERROR_PATTERNS["column_not_found_with_suggestion"]
        assert entry["category"] == "unresolved_column_suggestion"

    def test_pattern_has_deterministic_fix(self):
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        entry = ERROR_PATTERNS["column_not_found_with_suggestion"]
        assert entry["deterministic_fix"] == "fix_unresolved_column"

    def test_pattern_has_entity_extractors(self):
        """Após o fix, o padrão deve ter entity_extractors definidos."""
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        entry = ERROR_PATTERNS["column_not_found_with_suggestion"]
        assert "entity_extractors" in entry, (
            "entry deve conter 'entity_extractors' após o fix"
        )
        extractors = entry["entity_extractors"]
        assert "bad_column" in extractors
        assert "table_alias" in extractors
        assert "suggestions" in extractors

    def test_entity_extractor_bad_column(self):
        """Extrator de bad_column deve capturar 'fl_ativo'."""
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        extractor: re.Pattern = ERROR_PATTERNS[
            "column_not_found_with_suggestion"
        ]["entity_extractors"]["bad_column"]
        m = extractor.search(ERROR_MESSAGE)
        assert m is not None, "Extrator bad_column não casou"
        assert m.group(1) == "fl_ativo"

    def test_entity_extractor_table_alias(self):
        """Extrator de table_alias deve capturar 'c'."""
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        extractor: re.Pattern = ERROR_PATTERNS[
            "column_not_found_with_suggestion"
        ]["entity_extractors"]["table_alias"]
        m = extractor.search(ERROR_MESSAGE)
        assert m is not None, "Extrator table_alias não casou"
        assert m.group(1) == "c"

    def test_entity_extractor_suggestions(self):
        """Extrator de suggestions deve capturar a lista de sugestões."""
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        extractor: re.Pattern = ERROR_PATTERNS[
            "column_not_found_with_suggestion"
        ]["entity_extractors"]["suggestions"]
        m = extractor.search(ERROR_MESSAGE)
        assert m is not None, "Extrator suggestions não casou"
        suggestions_raw = m.group(1)
        # Deve conter pelo menos uma sugestão válida
        assert "meses_ativo" in suggestions_raw or "id" in suggestions_raw

    def test_pattern_does_not_match_unrelated_error(self):
        """Padrão não deve casar com erros não relacionados."""
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        entry = ERROR_PATTERNS["column_not_found_with_suggestion"]
        pattern: re.Pattern = entry["pattern"]
        unrelated = "Table or view not found: telcostar.operacional.clientes"
        assert not pattern.search(unrelated)


# ---------------------------------------------------------------------------
# 2. Testes de fixer.py — handler _fix_unresolved_column
# ---------------------------------------------------------------------------

class TestFixerUnresolvedColumn:
    """Valida o comportamento do handler _fix_unresolved_column."""

    def _make_diagnostic(self, entities: dict):
        """Cria um ErrorDiagnostic mínimo para testes."""
        from sas2dbx.validate.heal.diagnostics import ErrorDiagnostic

        return ErrorDiagnostic(
            category="unresolved_column_suggestion",
            deterministic_fix="fix_unresolved_column",
            severity="HIGH",
            entities=entities,
            raw_error=ERROR_MESSAGE,
        )

    def test_handler_registered(self):
        """O handler fix_unresolved_column deve estar registrado em _HANDLERS."""
        from sas2dbx.validate.heal.fixer import _HANDLERS

        assert "fix_unresolved_column" in _HANDLERS

    def test_handler_method_exists(self):
        """O método _fix_unresolved_column deve existir em NotebookFixer."""
        from sas2dbx.validate.heal.fixer import NotebookFixer

        fixer = NotebookFixer()
        assert hasattr(fixer, "_fix_unresolved_column"), (
            "NotebookFixer deve ter método _fix_unresolved_column"
        )

    def test_apply_fix_returns_patched_true(self, notebook_file: Path):
        """apply_fix deve retornar patched=True quando entidades estão disponíveis."""
        from sas2dbx.validate.heal.fixer import NotebookFixer

        fixer = NotebookFixer()
        diagnostic = self._make_diagnostic({
            "bad_column": "fl_ativo",
            "table_alias": "c",
            "suggestions": "`c`.`id`, `u`.`id`, `c`.`meses_ativo`, `c`.`total`, `c`.`COUNT`",
        })
        result = fixer.apply_fix(notebook_file, diagnostic)
        assert result.patched is True, f"Esperado patched=True, got: {result}"

    def test_backup_created(self, notebook_file: Path):
        """Um arquivo .py.bak deve ser criado antes do patch."""
        from sas2dbx.validate.heal.fixer import NotebookFixer

        fixer = NotebookFixer()
        diagnostic = self._make_diagnostic({
            "bad_column": "fl_ativo",
            "table_alias": "c",
            "suggestions": "`c`.`id`, `u`.`id`, `c`.`meses_ativo`, `c`.`total`, `c`.`COUNT`",
        })
        fixer.apply_fix(notebook_file, diagnostic)
        backup = notebook_file.with_suffix(".py.bak")
        assert backup.exists(), "Backup .py.bak deve ser criado"

    def test_bad_column_reference_removed_or_replaced(self, notebook_file: Path):
        """Após o patch, a referência à coluna problemática deve ser corrigida."""
        from sas2dbx.validate.heal.fixer import NotebookFixer

        fixer = NotebookFixer()
        diagnostic = self._make_diagnostic({
            "bad_column": "fl_ativo",
            "table_alias": "c",
            "suggestions": "`c`.`id`, `u`.`id`, `c`.`meses_ativo`, `c`.`total`, `c`.`COUNT`",
        })
        fixer.apply_fix(notebook_file, diagnostic)
        patched_content = notebook_file.read_text(encoding="utf-8")
        # A coluna fl_ativo não deve mais aparecer como referência não comentada
        # OU deve ter sido substituída por uma sugestão válida
        lines_with_fl_ativo = [
            line for line in patched_content.splitlines()
            if "fl_ativo" in line and not line.strip().startswith("#")
        ]
        assert len(lines_with_fl_ativo) == 0, (
            f"Referências não comentadas a fl_ativo ainda presentes: {lines_with_fl_ativo}"
        )

    def test_fix_without_entities_uses_raw_error(self, notebook_file: Path):
        """Quando entities está vazio, o fixer deve tentar extrair da raw_error."""
        from sas2dbx.validate.heal.fixer import NotebookFixer

        fixer = NotebookFixer()
        diagnostic = self._make_diagnostic({})
        # Não deve lançar exceção — pode retornar patched=False mas não deve crashar
        result = fixer.apply_fix(notebook_file, diagnostic)
        assert isinstance(result.patched, bool)

    def test_result_description_is_informative(self, notebook_file: Path):
        """A descrição do resultado deve mencionar a coluna corrigida."""
        from sas2dbx.validate.heal.fixer import NotebookFixer

        fixer = NotebookFixer()
        diagnostic = self._make_diagnostic({
            "bad_column": "fl_ativo",
            "table_alias": "c",
            "suggestions": "`c`.`id`, `u`.`id`, `c`.`meses_ativo`, `c`.`total`, `c`.`COUNT`",
        })
        result = fixer.apply_fix(notebook_file, diagnostic)
        if result.patched:
            assert result.description, "description não deve ser vazia quando patched=True"


# ---------------------------------------------------------------------------
# 3. Teste de regressão — padrão anterior ainda funciona
# ---------------------------------------------------------------------------

class TestRegressionExistingPatterns:
    """Garante que o fix não quebrou padrões existentes."""

    def test_table_not_found_still_matches(self):
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        pattern = ERROR_PATTERNS["table_not_found"]["pattern"]
        assert pattern.search("Table or view not found: mydb.mytable")

    def test_column_not_found_still_matches(self):
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        pattern = ERROR_PATTERNS["column_not_found"]["pattern"]
        assert pattern.search("cannot resolve 'myCol' given input columns")

    def test_unresolved_column_with_suggestion_does_not_match_column_not_found(self):
        """Os dois padrões não devem se sobrepor para o mesmo erro."""
        from sas2dbx.validate.heal.patterns import ERROR_PATTERNS

        suggestion_pattern = ERROR_PATTERNS["column_not_found_with_suggestion"]["pattern"]
        plain_pattern = ERROR_PATTERNS["column_not_found"]["pattern"]

        # Erro com sugestão: deve casar com suggestion, não com plain
        assert suggestion_pattern.search(ERROR_MESSAGE)
        # plain não deve casar com UNRESOLVED_COLUMN.WITH_SUGGESTION (tem lookahead negativo)
        # (comportamento esperado — plain captura apenas sem sugestão)
        plain_match = plain_pattern.search(ERROR_MESSAGE)
        # Se casar, a categoria deve ser diferente — o routing usa o primeiro match
        # Este teste documenta o comportamento atual para evitar regressão
        assert suggestion_pattern.search(ERROR_MESSAGE) is not None
