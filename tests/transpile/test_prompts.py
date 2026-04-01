"""Testes para transpile/llm/prompts.py — build_transpile_prompt e strip_code_fences."""

from __future__ import annotations

import pytest

from sas2dbx.transpile.llm.prompts import build_transpile_prompt, strip_code_fences


# ---------------------------------------------------------------------------
# build_transpile_prompt — estrutura básica
# ---------------------------------------------------------------------------


class TestBuildTranspilePromptStructure:
    def test_returns_string(self) -> None:
        result = build_transpile_prompt("DATA x; SET y; RUN;", "DATA_STEP_SIMPLE")
        assert isinstance(result, str)

    def test_contains_sas_code(self) -> None:
        sas = "PROC SORT DATA=ds; BY col; RUN;"
        result = build_transpile_prompt(sas, "PROC_SORT")
        assert "PROC SORT DATA=ds" in result

    def test_contains_construct_type(self) -> None:
        result = build_transpile_prompt("x;", "PROC_SQL")
        assert "PROC_SQL" in result

    def test_contains_catalog_and_schema(self) -> None:
        result = build_transpile_prompt("x;", "PROC_SQL", catalog="cat", schema="sch")
        assert "cat" in result
        assert "sch" in result

    def test_no_context_section_when_empty(self) -> None:
        result = build_transpile_prompt("x;", "DATA_STEP_SIMPLE", context_text="")
        assert "REFERÊNCIA TÉCNICA" not in result

    def test_context_section_included_when_present(self) -> None:
        result = build_transpile_prompt("x;", "DATA_STEP_SIMPLE", context_text="INTCK → months_between")
        assert "REFERÊNCIA TÉCNICA" in result
        assert "INTCK" in result

    def test_whitespace_only_context_not_included(self) -> None:
        result = build_transpile_prompt("x;", "DATA_STEP_SIMPLE", context_text="   \n  ")
        assert "REFERÊNCIA TÉCNICA" not in result


# ---------------------------------------------------------------------------
# build_transpile_prompt — C1: resistência a { e } no código SAS
# ---------------------------------------------------------------------------


class TestBraceEscaping:
    def test_curly_braces_in_sas_code_do_not_raise(self) -> None:
        """C1: { } em código SAS não devem causar KeyError/IndexError."""
        sas_with_braces = "data _null_; x = 'test {}'; run;"
        # Antes do fix, isso levantava IndexError
        result = build_transpile_prompt(sas_with_braces, "DATA_STEP_SIMPLE")
        assert isinstance(result, str)
        # O conteúdo original ainda aparece (sem as barras de escape)
        assert "test {}" in result

    def test_macro_braces_in_sas_code_do_not_raise(self) -> None:
        sas = "%macro test; %if &x = {val} %then %do; %end; %mend;"
        result = build_transpile_prompt(sas, "MACRO_SIMPLE")
        assert isinstance(result, str)
        assert "{val}" in result

    def test_multiple_braces_pairs_handled(self) -> None:
        sas = "x = '{a} and {b}';"
        result = build_transpile_prompt(sas, "DATA_STEP_SIMPLE")
        assert "{a} and {b}" in result

    def test_unmatched_brace_does_not_raise(self) -> None:
        sas = "x = 'open { brace';"
        result = build_transpile_prompt(sas, "DATA_STEP_SIMPLE")
        assert isinstance(result, str)

    def test_braces_in_context_text_do_not_raise(self) -> None:
        ctx = "INTCK: months_between({end}, {start})"
        result = build_transpile_prompt("x;", "DATA_STEP_SIMPLE", context_text=ctx)
        assert isinstance(result, str)
        # Conteúdo do contexto preservado no output
        assert "INTCK" in result


# ---------------------------------------------------------------------------
# strip_code_fences
# ---------------------------------------------------------------------------


class TestStripCodeFences:
    def test_strips_python_fence(self) -> None:
        raw = "```python\ndf = spark.read.table('t')\n```"
        assert strip_code_fences(raw) == "df = spark.read.table('t')"

    def test_strips_pyspark_fence(self) -> None:
        raw = "```pyspark\ndf = spark.sql('SELECT 1')\n```"
        assert strip_code_fences(raw) == "df = spark.sql('SELECT 1')"

    def test_strips_generic_fence(self) -> None:
        raw = "```\ndf = spark.table('t')\n```"
        assert strip_code_fences(raw) == "df = spark.table('t')"

    def test_no_fence_returns_unchanged(self) -> None:
        raw = "df = spark.read.table('t')"
        assert strip_code_fences(raw) == raw

    def test_strips_surrounding_whitespace(self) -> None:
        raw = "  \n```python\ndf = spark.table('t')\n```\n  "
        assert strip_code_fences(raw) == "df = spark.table('t')"

    def test_empty_string(self) -> None:
        assert strip_code_fences("") == ""

    def test_multiline_code_preserved(self) -> None:
        raw = "```python\na = 1\nb = 2\nc = 3\n```"
        result = strip_code_fences(raw)
        assert "a = 1" in result
        assert "b = 2" in result
        assert "c = 3" in result
