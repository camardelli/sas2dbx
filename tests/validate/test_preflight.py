"""Testes para validate/preflight.py — PreflightChecker."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sas2dbx.validate.preflight import PreflightChecker, TableCategory


# ---------------------------------------------------------------------------
# _extract_input_tables — literais e f-strings
# ---------------------------------------------------------------------------


class TestExtractInputTables:
    def _checker(self) -> PreflightChecker:
        return PreflightChecker(config=None)

    def test_literal_read_table(self) -> None:
        content = 'df = spark.read.table("main.schema.clientes")'
        result = self._checker()._extract_input_tables(content)
        assert "main.schema.clientes" in result

    def test_literal_excluded_if_save_as_table(self) -> None:
        content = (
            'df = spark.read.table("main.s.tab")\n'
            'df.write.mode("overwrite").saveAsTable("main.s.tab")\n'
        )
        result = self._checker()._extract_input_tables(content)
        assert "main.s.tab" not in result

    def test_fstring_resolved_from_variable(self) -> None:
        content = (
            'DS_AGG = "ds_agg_placeholder"\n'
            'df = spark.read.table(f"telcostar.operacional.{DS_AGG}")\n'
        )
        result = self._checker()._extract_input_tables(content)
        assert "telcostar.operacional.ds_agg_placeholder" in result

    def test_fstring_with_multiple_vars_resolved(self) -> None:
        content = (
            'CAT = "main"\n'
            'SCH = "raw"\n'
            'df = spark.read.table(f"{CAT}.{SCH}.vendas")\n'
        )
        result = self._checker()._extract_input_tables(content)
        assert "main.raw.vendas" in result

    def test_fstring_unresolved_not_included(self) -> None:
        # Variável não definida no notebook → não deve ser incluída
        content = 'df = spark.read.table(f"main.raw.{undefined_var}")\n'
        result = self._checker()._extract_input_tables(content)
        assert not any("{" in t for t in result)

    def test_fstring_partial_resolution_not_included(self) -> None:
        content = (
            'CAT = "main"\n'
            'df = spark.read.table(f"{CAT}.raw.{undefined}")\n'
        )
        result = self._checker()._extract_input_tables(content)
        assert not any("undefined" in t or "{" in t for t in result)

    def test_fstring_not_included_without_dot(self) -> None:
        content = (
            'TB = "tabela_sem_schema"\n'
            'df = spark.read.table(f"{TB}")\n'
        )
        result = self._checker()._extract_input_tables(content)
        assert "tabela_sem_schema" not in result

    def test_from_clause_extracted(self) -> None:
        content = 'spark.sql("SELECT * FROM main.s.pedidos")'
        result = self._checker()._extract_input_tables(content)
        assert "main.s.pedidos" in result


# ---------------------------------------------------------------------------
# _resolve_fstring_tables — unidade isolada
# ---------------------------------------------------------------------------


class TestResolveFstringTables:
    def _checker(self) -> PreflightChecker:
        return PreflightChecker(config=None)

    def test_resolves_single_var(self) -> None:
        content = (
            'DS = "minha_tabela"\n'
            'df = spark.read.table(f"cat.sch.{DS}")\n'
        )
        result = self._checker()._resolve_fstring_tables(content)
        assert "cat.sch.minha_tabela" in result

    def test_skips_if_var_not_found(self) -> None:
        content = 'df = spark.read.table(f"cat.sch.{MISSING}")\n'
        result = self._checker()._resolve_fstring_tables(content)
        assert result == []

    def test_skips_double_qualified_name(self) -> None:
        # Variável já contém catalog.schema — f-string adiciona outro prefixo
        # Resultado seria "cat.sch.cat.sch.tabela" (5 segmentos) — deve ser ignorado
        content = (
            'TABLE = "cat.sch.tabela"\n'
            'df = spark.read.table(f"cat.sch.{TABLE}")\n'
        )
        result = self._checker()._resolve_fstring_tables(content)
        assert result == []


# ---------------------------------------------------------------------------
# check() — integração com mock do WorkspaceClient
# ---------------------------------------------------------------------------


class TestPreflightCheckerIntegration:
    def _write_notebook(self, tmp_path: Path, content: str) -> None:
        (tmp_path / "job.py").write_text(content, encoding="utf-8")

    def _mock_client(self, existing: list[str]):
        """Retorna mock do WorkspaceClient onde tabelas em `existing` existem."""
        client = MagicMock()

        def _get(table_name):
            if table_name in existing:
                return MagicMock()
            raise Exception(f"Table not found: {table_name}")

        client.tables.get.side_effect = _get
        return client

    def _patch_sdk(self, client):
        """Mocka databricks.sdk no namespace do preflight."""
        sdk_mock = MagicMock()
        sdk_mock.WorkspaceClient.return_value = client
        return patch.dict("sys.modules", {"databricks": MagicMock(), "databricks.sdk": sdk_mock})

    def test_fstring_table_detected_as_ghost(self, tmp_path: Path) -> None:
        content = (
            'DS_AGG = "ds_agg_placeholder"\n'
            'df = spark.read.table(f"telcostar.operacional.{DS_AGG}")\n'
        )
        self._write_notebook(tmp_path, content)

        client = self._mock_client(existing=[])  # tabela não existe

        checker = PreflightChecker(config=MagicMock(host="h", token="t"))
        with self._patch_sdk(client):
            report = checker.check(tmp_path)

        assert report.has_ghosts
        assert any(
            "ds_agg_placeholder" in g.table_name
            for g in report.ghost_sources
        )

    def test_fstring_table_existing_not_flagged(self, tmp_path: Path) -> None:
        content = (
            'DS_AGG = "minha_tabela"\n'
            'df = spark.read.table(f"telcostar.operacional.{DS_AGG}")\n'
        )
        self._write_notebook(tmp_path, content)

        client = self._mock_client(existing=["telcostar.operacional.minha_tabela"])

        checker = PreflightChecker(config=MagicMock(host="h", token="t"))
        with self._patch_sdk(client):
            report = checker.check(tmp_path)

        assert not report.has_ghosts

    def test_literal_ghost_still_detected(self, tmp_path: Path) -> None:
        content = 'df = spark.read.table("main.raw.ausente")\n'
        self._write_notebook(tmp_path, content)

        client = self._mock_client(existing=[])

        checker = PreflightChecker(config=MagicMock(host="h", token="t"))
        with self._patch_sdk(client):
            report = checker.check(tmp_path)

        assert report.has_ghosts
        assert any("main.raw.ausente" in g.table_name for g in report.ghost_sources)
