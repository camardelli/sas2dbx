"""Testes para classifier.py — allowlist SUPPORTED_CONSTRUCTS e classify_block()."""

import logging

import pytest

from sas2dbx.analyze.classifier import (
    SUPPORTED_CONSTRUCTS,
    classify_block,
)
from sas2dbx.models.sas_ast import ClassificationResult, Tier


# ---------------------------------------------------------------------------
# AC-1: SUPPORTED_CONSTRUCTS documenta todos os constructs de Tier 1 e Tier 2
# ---------------------------------------------------------------------------

class TestSupportedConstructs:
    def test_tier1_constructs_present(self) -> None:
        tier1 = {k for k, v in SUPPORTED_CONSTRUCTS.items() if v == Tier.RULE}
        assert "DATA_STEP_SIMPLE" in tier1
        assert "PROC_SQL" in tier1
        assert "PROC_SORT" in tier1
        assert "PROC_EXPORT" in tier1
        assert "PROC_IMPORT" in tier1
        assert "LIBNAME" in tier1

    def test_tier2_constructs_present(self) -> None:
        tier2 = {k for k, v in SUPPORTED_CONSTRUCTS.items() if v == Tier.LLM}
        assert "DATA_STEP_COMPLEX" in tier2
        assert "PROC_MEANS" in tier2
        assert "PROC_SUMMARY" in tier2
        assert "PROC_FREQ" in tier2
        assert "MACRO_SIMPLE" in tier2

    def test_tier3_constructs_include_unknown(self) -> None:
        assert SUPPORTED_CONSTRUCTS["UNKNOWN"] == Tier.MANUAL
        assert SUPPORTED_CONSTRUCTS["PROC_FORMAT"] == Tier.MANUAL
        assert SUPPORTED_CONSTRUCTS["PROC_REPORT"] == Tier.MANUAL
        assert SUPPORTED_CONSTRUCTS["PROC_TABULATE"] == Tier.MANUAL
        assert SUPPORTED_CONSTRUCTS["MACRO_DYNAMIC"] == Tier.MANUAL
        assert SUPPORTED_CONSTRUCTS["HASH_OBJECT"] == Tier.MANUAL

    def test_all_values_are_tier_instances(self) -> None:
        for construct, tier in SUPPORTED_CONSTRUCTS.items():
            assert isinstance(tier, Tier), f"{construct} should map to a Tier instance"


# ---------------------------------------------------------------------------
# AC-2: Construct não reconhecido → UNKNOWN / Tier.MANUAL (nunca silently parsed)
# ---------------------------------------------------------------------------

class TestUnknownConstruct:
    def test_unknown_source_returns_manual(self) -> None:
        result = classify_block("SOME COMPLETELY UNKNOWN SYNTAX;")
        assert result.construct_type == "UNKNOWN"
        assert result.tier == Tier.MANUAL

    def test_unknown_source_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="sas2dbx.analyze.classifier"):
            classify_block("TOTALLY_UNKNOWN_CONSTRUCT;")
        assert any("UNKNOWN" in r.message or "não reconhecido" in r.message for r in caplog.records)

    def test_proc_nonexistent_returns_unknown(self) -> None:
        result = classify_block("PROC NONEXISTENT_PROC; RUN;")
        assert result.construct_type == "UNKNOWN"
        assert result.tier == Tier.MANUAL


# ---------------------------------------------------------------------------
# AC-3: ClassificationResult com construct_type, tier, confidence
# ---------------------------------------------------------------------------

class TestClassificationResult:
    def test_returns_classification_result_instance(self) -> None:
        result = classify_block("PROC SORT DATA=mydata; BY id; RUN;")
        assert isinstance(result, ClassificationResult)

    def test_result_has_required_fields(self) -> None:
        result = classify_block("PROC SQL; SELECT * FROM table; QUIT;")
        assert hasattr(result, "construct_type")
        assert hasattr(result, "tier")
        assert hasattr(result, "confidence")

    def test_tier1_confidence_is_1(self) -> None:
        result = classify_block("PROC SORT DATA=ds; BY id; RUN;")
        assert result.tier == Tier.RULE
        assert result.confidence == 1.0

    def test_tier2_confidence_is_0_8(self) -> None:
        result = classify_block("PROC MEANS DATA=ds; VAR age; RUN;")
        assert result.tier == Tier.LLM
        assert result.confidence == 0.8

    def test_tier3_confidence_is_0(self) -> None:
        result = classify_block("PROC FORMAT; VALUE $fmt 'A'='Active'; RUN;")
        assert result.tier == Tier.MANUAL
        assert result.confidence == 0.0

    def test_unknown_confidence_is_0(self) -> None:
        result = classify_block("GARBAGE SYNTAX;")
        assert result.confidence == 0.0


# ---------------------------------------------------------------------------
# AC-4: Testes cobrem categorias requeridas
# ---------------------------------------------------------------------------

class TestDataStep:
    def test_simple_data_step(self) -> None:
        sas = """
        DATA output;
            SET input;
            WHERE age > 18;
            KEEP id name age;
        RUN;
        """
        result = classify_block(sas)
        assert result.construct_type == "DATA_STEP_SIMPLE"
        assert result.tier == Tier.RULE

    def test_data_step_with_rename(self) -> None:
        sas = "DATA out; SET inp; RENAME old_name=new_name; RUN;"
        result = classify_block(sas)
        assert result.construct_type == "DATA_STEP_SIMPLE"

    def test_data_step_with_drop(self) -> None:
        sas = "DATA out (DROP=temp_var); SET inp; x = 1; RUN;"
        result = classify_block(sas)
        assert result.construct_type == "DATA_STEP_SIMPLE"

    def test_complex_data_step_with_array(self) -> None:
        sas = """
        DATA output;
            SET input;
            ARRAY nums{3} a b c;
            DO i = 1 TO 3;
                nums{i} = nums{i} * 2;
            END;
        RUN;
        """
        result = classify_block(sas)
        assert result.construct_type == "DATA_STEP_COMPLEX"
        assert result.tier == Tier.LLM

    def test_complex_data_step_with_retain(self) -> None:
        sas = """
        DATA output;
            SET input;
            RETAIN running_total 0;
            running_total = running_total + amount;
        RUN;
        """
        result = classify_block(sas)
        assert result.construct_type == "DATA_STEP_COMPLEX"
        assert result.tier == Tier.LLM


class TestProcSQL:
    def test_proc_sql_basic(self) -> None:
        sas = """
        PROC SQL;
            SELECT id, name FROM mylib.customers WHERE active = 1;
        QUIT;
        """
        result = classify_block(sas)
        assert result.construct_type == "PROC_SQL"
        assert result.tier == Tier.RULE

    def test_proc_sql_case_insensitive(self) -> None:
        result = classify_block("proc sql; select * from t; quit;")
        assert result.construct_type == "PROC_SQL"


class TestProcSort:
    def test_proc_sort(self) -> None:
        sas = "PROC SORT DATA=ds OUT=sorted; BY id name; RUN;"
        result = classify_block(sas)
        assert result.construct_type == "PROC_SORT"
        assert result.tier == Tier.RULE


class TestProcFormat:
    def test_proc_format_is_manual(self) -> None:
        sas = """
        PROC FORMAT;
            VALUE $status_fmt
                'A' = 'Active'
                'I' = 'Inactive';
        RUN;
        """
        result = classify_block(sas)
        assert result.construct_type == "PROC_FORMAT"
        assert result.tier == Tier.MANUAL
        assert result.confidence == 0.0


class TestMacros:
    def test_simple_macro(self) -> None:
        sas = """
        %MACRO calc_total(dataset=, var=);
            PROC MEANS DATA=&dataset;
                VAR &var;
            RUN;
        %MEND calc_total;
        """
        result = classify_block(sas)
        assert result.construct_type == "MACRO_SIMPLE"
        assert result.tier == Tier.LLM

    def test_macro_with_call_execute_is_dynamic(self) -> None:
        sas = """
        %MACRO gen_reports;
            %DO i = 1 %TO 5;
                CALL EXECUTE('%do_report(' || &i || ')');
            %END;
        %MEND gen_reports;
        """
        result = classify_block(sas)
        assert result.construct_type == "MACRO_DYNAMIC"
        assert result.tier == Tier.MANUAL

    def test_macro_with_sysfunc_is_dynamic(self) -> None:
        sas = """
        %MACRO get_date;
            %LET today = %SYSFUNC(TODAY(), date9.);
        %MEND get_date;
        """
        result = classify_block(sas)
        assert result.construct_type == "MACRO_DYNAMIC"
        assert result.tier == Tier.MANUAL


# ---------------------------------------------------------------------------
# AC-5: Log WARNING para UNKNOWN — já coberto em TestUnknownConstruct
# (separamos test extra para garantir que WARNING aparece exatamente)
# ---------------------------------------------------------------------------

class TestWarningLogging:
    def test_no_warning_for_known_construct(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="sas2dbx.analyze.classifier"):
            classify_block("PROC SQL; SELECT 1; QUIT;")
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 0

    def test_warning_for_unknown_contains_hint(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING, logger="sas2dbx.analyze.classifier"):
            classify_block("XYZ UNKNOWN_BLOCK foo bar;")
        assert len(caplog.records) >= 1


# ---------------------------------------------------------------------------
# Additional: Tier enum and Libname
# ---------------------------------------------------------------------------

class TestTierEnum:
    def test_tier_values(self) -> None:
        assert Tier.RULE == "rule"
        assert Tier.LLM == "llm"
        assert Tier.MANUAL == "manual"


class TestLibname:
    def test_libname_statement(self) -> None:
        result = classify_block("LIBNAME mylib '/data/sas/datasets';")
        assert result.construct_type == "LIBNAME"
        assert result.tier == Tier.RULE

    def test_libname_case_insensitive(self) -> None:
        result = classify_block("libname mylib '/path';")
        assert result.construct_type == "LIBNAME"


class TestHashObject:
    def test_hash_object_in_data_step(self) -> None:
        sas = """
        DATA _null_;
            DECLARE HASH h(dataset: 'lookup');
            h.defineKey('id');
            h.defineData('value');
            h.defineDone();
        RUN;
        """
        result = classify_block(sas)
        assert result.construct_type == "HASH_OBJECT"
        assert result.tier == Tier.MANUAL
