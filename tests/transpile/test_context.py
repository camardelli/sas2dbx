"""Testes para transpile/llm/context.py — build_context e format_context_for_prompt."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.store import KnowledgeStore
from sas2dbx.transpile.llm.context import (
    DEFAULT_MAX_CONTEXT_TOKENS,
    build_context,
    format_context_for_prompt,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ks_empty(tmp_path: Path) -> KnowledgeStore:
    """KnowledgeStore vazio, sem llm_client."""
    return KnowledgeStore(base_path=tmp_path)


@pytest.fixture
def ks_populated(tmp_path: Path) -> KnowledgeStore:
    """KnowledgeStore com alguns mapeamentos em merged/."""
    merged = tmp_path / "mappings" / "merged"
    merged.mkdir(parents=True)
    (merged / "functions_map.yaml").write_text(
        yaml.dump({
            "INTCK": {
                "pyspark": "months_between(end, start)",
                "notes": "args invertidos",
                "confidence": 0.9,
            },
            "CATX": {
                "pyspark": "concat_ws(delim, args)",
                "notes": "remove spaces",
                "confidence": 0.95,
            },
        }),
        encoding="utf-8",
    )
    (merged / "proc_map.yaml").write_text(
        yaml.dump({
            "SORT": {"approach": "rule", "notes": ".orderBy()", "confidence": 1.0},
        }),
        encoding="utf-8",
    )
    (merged / "sql_dialect_map.yaml").write_text(
        yaml.dump({
            "CALCULATED": {
                "sas": "SELECT CALCULATED x",
                "spark": "WITH t AS (...) ...",
                "notes": "use CTE",
                "confidence": 0.9,
            },
        }),
        encoding="utf-8",
    )
    (merged / "formats_map.yaml").write_text(
        yaml.dump({
            "DATE9.": {
                "pyspark": "date_format(col, 'ddMMMyyyy')",
                "notes": "ok",
                "confidence": 0.9,
            },
        }),
        encoding="utf-8",
    )
    # Referência SAS em markdown
    sas_ref = tmp_path / "sas_reference" / "procs"
    sas_ref.mkdir(parents=True)
    (sas_ref / "proc_sort.md").write_text("# PROC SORT\nOrders a dataset.", encoding="utf-8")

    return KnowledgeStore(base_path=tmp_path)


# ---------------------------------------------------------------------------
# build_context — estrutura de saída
# ---------------------------------------------------------------------------

class TestBuildContextStructure:
    def test_returns_all_keys(self, ks_empty: KnowledgeStore) -> None:
        ctx = build_context(ks_empty)
        assert "function_mappings" in ctx
        assert "proc_mappings" in ctx
        assert "sql_dialect_notes" in ctx
        assert "format_mappings" in ctx
        assert "sas_references" in ctx
        assert "pyspark_references" in ctx

    def test_empty_store_empty_context(self, ks_empty: KnowledgeStore) -> None:
        ctx = build_context(ks_empty, func_names=["INTCK"], proc_names=["SORT"])
        assert ctx["function_mappings"] == {}
        assert ctx["proc_mappings"] == {}

    def test_none_inputs_return_empty(self, ks_empty: KnowledgeStore) -> None:
        ctx = build_context(ks_empty)
        assert ctx["function_mappings"] == {}
        assert ctx["sas_references"] == []


# ---------------------------------------------------------------------------
# build_context — lookups com KS populado
# ---------------------------------------------------------------------------

class TestBuildContextLookups:
    def test_known_function_included(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, func_names=["INTCK"])
        assert "INTCK" in ctx["function_mappings"]
        assert ctx["function_mappings"]["INTCK"]["pyspark"] == "months_between(end, start)"

    def test_multiple_functions(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, func_names=["INTCK", "CATX"])
        assert "INTCK" in ctx["function_mappings"]
        assert "CATX" in ctx["function_mappings"]

    def test_unknown_function_not_included(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, func_names=["PRXMATCH"])
        assert "PRXMATCH" not in ctx["function_mappings"]

    def test_func_names_uppercased(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, func_names=["intck"])
        assert "INTCK" in ctx["function_mappings"]

    def test_known_proc_included(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, proc_names=["SORT"])
        assert "SORT" in ctx["proc_mappings"]

    def test_sql_construct_included(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, sql_constructs=["CALCULATED"])
        assert "CALCULATED" in ctx["sql_dialect_notes"]

    def test_format_included(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, format_names=["DATE9."])
        assert "DATE9." in ctx["format_mappings"]

    def test_sas_reference_included(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(
            ks_populated,
            sas_reference_keys=[("procs", "proc_sort")],
        )
        assert len(ctx["sas_references"]) == 1
        assert ctx["sas_references"][0]["name"] == "proc_sort"
        assert "PROC SORT" in ctx["sas_references"][0]["content"]

    def test_missing_sas_reference_not_included(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(
            ks_populated,
            sas_reference_keys=[("procs", "proc_nonexistent")],
        )
        assert ctx["sas_references"] == []


# ---------------------------------------------------------------------------
# max_tokens — budget enforcement (M4)
# ---------------------------------------------------------------------------

class TestMaxTokensBudget:
    def test_default_budget_allows_normal_entries(self, ks_populated: KnowledgeStore) -> None:
        """Com budget padrão, entradas normais devem entrar."""
        ctx = build_context(
            ks_populated,
            func_names=["INTCK", "CATX"],
            max_tokens=DEFAULT_MAX_CONTEXT_TOKENS,
        )
        assert len(ctx["function_mappings"]) == 2

    def test_tiny_budget_limits_entries(self, ks_populated: KnowledgeStore) -> None:
        """Com budget mínimo (1 token = 4 chars), apenas a primeira entrada cabe."""
        ctx = build_context(
            ks_populated,
            func_names=["INTCK", "CATX"],
            max_tokens=1,  # ~4 chars — não cabe nenhuma entrada real
        )
        # Nenhuma entrada deve ser incluída com budget de 4 chars
        assert len(ctx["function_mappings"]) == 0

    def test_budget_shared_across_sections(self, ks_populated: KnowledgeStore) -> None:
        """Budget é compartilhado entre todas as seções."""
        ctx = build_context(
            ks_populated,
            func_names=["INTCK"],
            proc_names=["SORT"],
            max_tokens=DEFAULT_MAX_CONTEXT_TOKENS,
        )
        # Ambos devem caber com budget normal
        assert "INTCK" in ctx["function_mappings"]
        assert "SORT" in ctx["proc_mappings"]


# ---------------------------------------------------------------------------
# format_context_for_prompt
# ---------------------------------------------------------------------------

class TestFormatContextForPrompt:
    def test_empty_context_returns_empty_string(self, ks_empty: KnowledgeStore) -> None:
        ctx = build_context(ks_empty)
        result = format_context_for_prompt(ctx)
        assert result == ""

    def test_functions_section_present(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, func_names=["INTCK"])
        result = format_context_for_prompt(ctx)
        assert "Funções SAS" in result
        assert "INTCK" in result
        assert "months_between" in result

    def test_notes_included_in_output(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, func_names=["INTCK"])
        result = format_context_for_prompt(ctx)
        assert "args invertidos" in result

    def test_proc_section_present(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, proc_names=["SORT"])
        result = format_context_for_prompt(ctx)
        assert "PROCs" in result
        assert "SORT" in result

    def test_sql_dialect_section_present(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, sql_constructs=["CALCULATED"])
        result = format_context_for_prompt(ctx)
        assert "Dialeto SQL" in result
        assert "CALCULATED" in result

    def test_format_section_present(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, format_names=["DATE9."])
        result = format_context_for_prompt(ctx)
        assert "Formatos SAS" in result
        assert "DATE9." in result

    def test_sas_reference_content_present(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(
            ks_populated,
            sas_reference_keys=[("procs", "proc_sort")],
        )
        result = format_context_for_prompt(ctx)
        assert "proc_sort" in result
        assert "Orders a dataset" in result

    def test_returns_string(self, ks_populated: KnowledgeStore) -> None:
        ctx = build_context(ks_populated, func_names=["INTCK"])
        assert isinstance(format_context_for_prompt(ctx), str)
