"""Testes para ingest/reader.py — read_sas_file e split_blocks."""

import logging
from pathlib import Path

import pytest

from sas2dbx.ingest.reader import read_sas_file, split_blocks
from sas2dbx.models.sas_ast import SASBlock

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sas"


# ---------------------------------------------------------------------------
# read_sas_file — encoding
# ---------------------------------------------------------------------------

class TestReadSasFile:
    def test_reads_utf8_file(self, tmp_path: Path) -> None:
        f = tmp_path / "test.sas"
        f.write_text("DATA x; SET y; RUN;", encoding="utf-8")
        code, enc = read_sas_file(f)
        assert "DATA x" in code
        assert enc in ("utf-8", "utf-8-sig")

    def test_reads_latin1_file(self, tmp_path: Path) -> None:
        f = tmp_path / "test.sas"
        f.write_bytes("/* Ação */\nDATA x; SET y; RUN;".encode("latin-1"))
        code, enc = read_sas_file(f)
        assert "DATA x" in code

    def test_forced_encoding(self, tmp_path: Path) -> None:
        f = tmp_path / "test.sas"
        f.write_text("PROC SQL; QUIT;", encoding="latin-1")
        code, enc = read_sas_file(f, encoding="latin-1")
        assert enc == "latin-1"

    def test_raises_for_nonexistent_file(self) -> None:
        with pytest.raises(FileNotFoundError):
            read_sas_file("/nao/existe.sas")

    def test_reads_real_fixture(self) -> None:
        code, enc = read_sas_file(FIXTURES_DIR / "job_001_carga_clientes.sas")
        assert "LIBNAME" in code
        assert len(code) > 0


# ---------------------------------------------------------------------------
# split_blocks — estrutura básica
# ---------------------------------------------------------------------------

class TestSplitBlocks:
    def test_returns_list_of_sas_blocks(self) -> None:
        code = "DATA x; SET y; RUN;"
        blocks = split_blocks(code)
        assert isinstance(blocks, list)
        for b in blocks:
            assert isinstance(b, SASBlock)

    def test_empty_code_returns_empty(self) -> None:
        assert split_blocks("") == []
        assert split_blocks("   \n  \n  ") == []

    def test_single_data_step(self) -> None:
        code = "DATA out;\n    SET inp;\n    WHERE age > 18;\nRUN;"
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert "DATA out" in blocks[0].raw_code

    def test_single_proc_sql(self) -> None:
        code = "PROC SQL;\n    SELECT * FROM t;\nQUIT;"
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert "PROC SQL" in blocks[0].raw_code

    def test_libname_is_single_block(self) -> None:
        code = "LIBNAME mylib '/data/sas';"
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert "LIBNAME" in blocks[0].raw_code

    def test_macro_block(self) -> None:
        code = "%MACRO mytest(param=);\n    %PUT &param;\n%MEND mytest;"
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert "%MACRO" in blocks[0].raw_code

    def test_macro_invocation_single_line(self) -> None:
        code = "%calc_totals(dataset=sasdata.vendas, var=valor);"
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert "%calc_totals" in blocks[0].raw_code.lower()

    def test_macro_invocation_multiline(self) -> None:
        code = "%my_macro(\n    arg1=sasdata.a,\n    arg2=foo\n);"
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert "%my_macro" in blocks[0].raw_code.lower()

    def test_macro_invocation_and_data_step(self) -> None:
        code = "%init_libs();\nDATA out;\n    SET inp;\nRUN;"
        blocks = split_blocks(code)
        assert len(blocks) == 2

    def test_sas_macro_keywords_not_captured_as_blocks(self) -> None:
        """Statements %LET, %IF, %DO fora de blocos não viram blocos isolados."""
        code = "DATA x;\n    SET y;\nRUN;\n%PUT done;"
        blocks = split_blocks(code)
        assert len(blocks) == 1  # só o DATA step

    def test_multiple_blocks(self) -> None:
        code = (
            "DATA x; SET y; RUN;\n"
            "PROC SORT DATA=x; BY id; RUN;\n"
            "PROC SQL; SELECT * FROM x; QUIT;\n"
        )
        blocks = split_blocks(code)
        assert len(blocks) == 3


# ---------------------------------------------------------------------------
# split_blocks — line numbers
# ---------------------------------------------------------------------------

class TestSplitBlocksLineNumbers:
    def test_start_line_is_1indexed(self) -> None:
        code = "DATA x;\n    SET y;\nRUN;"
        blocks = split_blocks(code)
        assert blocks[0].start_line == 1

    def test_end_line_is_correct(self) -> None:
        code = "DATA x;\n    SET y;\nRUN;"
        blocks = split_blocks(code)
        assert blocks[0].end_line == 3

    def test_second_block_start_line(self) -> None:
        code = "DATA x; SET y; RUN;\nPROC SORT DATA=x; BY id; RUN;"
        blocks = split_blocks(code)
        assert len(blocks) == 2
        assert blocks[1].start_line == 2

    def test_source_file_propagated(self, tmp_path: Path) -> None:
        p = tmp_path / "test.sas"
        blocks = split_blocks("DATA x; SET y; RUN;", source_file=p)
        assert blocks[0].source_file == p


# ---------------------------------------------------------------------------
# split_blocks — blocos incompletos
# ---------------------------------------------------------------------------

class TestIncompleteBlocks:
    def test_incomplete_block_preserved(self, caplog: pytest.LogCaptureFixture) -> None:
        code = "DATA x;\n    SET y;\n    WHERE age > 0;"  # sem RUN;
        with caplog.at_level(logging.WARNING):
            blocks = split_blocks(code)
        assert len(blocks) == 1
        assert any("incompleto" in r.message.lower() or "terminador" in r.message.lower()
                   for r in caplog.records)


# ---------------------------------------------------------------------------
# Fixtures reais
# ---------------------------------------------------------------------------

class TestMacroMerge:
    """Testa a mesclagem de %MACRO...%MEND com invocações consecutivas."""

    def test_macro_def_merged_with_single_invocation(self) -> None:
        """%MACRO...%MEND + %macro_call() devem virar um único bloco."""
        code = (
            "%MACRO calc(ds_in=, ds_out=);\n"
            "    DATA &ds_out; SET &ds_in; RUN;\n"
            "%MEND calc;\n"
            "\n"
            "%calc(ds_in=TELCO.vendas, ds_out=DW.result);\n"
        )
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert "%MACRO" in blocks[0].raw_code.upper()
        assert "%calc(" in blocks[0].raw_code.lower()
        assert "TELCO.vendas" in blocks[0].raw_code

    def test_macro_def_merged_with_multiple_invocations(self) -> None:
        """Múltiplas invocações consecutivas da mesma macro devem ser mescladas."""
        code = (
            "%MACRO transform(ds=);\n"
            "    PROC SQL; SELECT * FROM &ds; QUIT;\n"
            "%MEND transform;\n"
            "\n"
            "%transform(ds=TELCO.a);\n"
            "%transform(ds=TELCO.b);\n"
        )
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert "TELCO.a" in blocks[0].raw_code
        assert "TELCO.b" in blocks[0].raw_code

    def test_macro_def_not_merged_with_different_macro_call(self) -> None:
        """Definição de macro_a NÃO deve mesclar com invocação de macro_b."""
        code = (
            "%MACRO macro_a(x=);\n"
            "    DATA out; SET &x; RUN;\n"
            "%MEND macro_a;\n"
            "\n"
            "%macro_b(y=TELCO.z);\n"
        )
        blocks = split_blocks(code)
        assert len(blocks) == 2
        assert "%MACRO" in blocks[0].raw_code.upper()
        assert "%macro_b" in blocks[1].raw_code.lower()

    def test_macro_def_followed_by_proc_not_merged(self) -> None:
        """Definição de macro seguida de PROC SQL não deve ser mesclada."""
        code = (
            "%MACRO score(ds=);\n"
            "    DATA &ds; x = 1; RUN;\n"
            "%MEND score;\n"
            "\n"
            "PROC SQL;\n"
            "    SELECT * FROM DW.result;\n"
            "QUIT;\n"
        )
        blocks = split_blocks(code)
        assert len(blocks) == 2
        assert "%MEND" in blocks[0].raw_code.upper()
        assert "PROC SQL" in blocks[1].raw_code.upper()

    def test_macro_merge_preserves_start_line(self) -> None:
        """start_line do bloco mesclado deve ser da definição da macro."""
        code = (
            "\n\n"
            "%MACRO f(x=);\n"   # linha 3
            "    DATA &x; RUN;\n"
            "%MEND f;\n"
            "\n"
            "%f(x=TELCO.a);\n"
        )
        blocks = split_blocks(code)
        assert len(blocks) == 1
        assert blocks[0].start_line == 3

    def test_rfm_scoring_pattern(self) -> None:
        """Replica o padrão real do job_107: macro + invocação + PROC SQL separado."""
        code = (
            "%MACRO rfm_score(ds_vendas=, ds_clientes=, ds_out=);\n"
            "    PROC SQL;\n"
            "        CREATE TABLE _base AS SELECT valor_bruto FROM &ds_vendas;\n"
            "    QUIT;\n"
            "    DATA &ds_out; SET _base; RUN;\n"
            "%MEND rfm_score;\n"
            "\n"
            "%rfm_score(\n"
            "    ds_vendas=TELCO.vendas_raw,\n"
            "    ds_clientes=TELCO.clientes_raw,\n"
            "    ds_out=DW.rfm_clientes\n"
            ");\n"
            "\n"
            "PROC SQL;\n"
            "    CREATE TABLE DW.rfm_resumo AS\n"
            "    SELECT rfm_segmento, COUNT(*) FROM DW.rfm_clientes\n"
            "    GROUP BY rfm_segmento;\n"
            "QUIT;\n"
        )
        blocks = split_blocks(code)
        # Deve gerar 2 blocos: [macro+invocação, PROC SQL]
        assert len(blocks) == 2
        # Bloco 1: contém definição E invocação com parâmetros reais
        assert "%MEND" in blocks[0].raw_code.upper()
        assert "TELCO.vendas_raw" in blocks[0].raw_code
        # Bloco 2: só o PROC SQL de resumo
        assert "rfm_resumo" in blocks[1].raw_code.lower()


class TestRealFixtures:
    def test_job001_has_libname_data_sort_sql(self) -> None:
        code, _ = read_sas_file(FIXTURES_DIR / "job_001_carga_clientes.sas")
        blocks = split_blocks(code)
        construct_starters = [b.raw_code.strip().split()[0].upper() for b in blocks]
        assert "LIBNAME" in construct_starters
        assert "DATA" in construct_starters
        assert "PROC" in construct_starters

    def test_job002_has_macro_and_means(self) -> None:
        code, _ = read_sas_file(FIXTURES_DIR / "job_002_transform_vendas.sas")
        blocks = split_blocks(code)
        raw_codes = " ".join(b.raw_code.upper() for b in blocks)
        assert "%MACRO" in raw_codes
        assert "PROC MEANS" in raw_codes

    def test_job003_has_format_and_report(self) -> None:
        code, _ = read_sas_file(FIXTURES_DIR / "job_003_report_mensal.sas")
        blocks = split_blocks(code)
        raw_codes = " ".join(b.raw_code.upper() for b in blocks)
        assert "PROC FORMAT" in raw_codes
        assert "PROC REPORT" in raw_codes

    def test_all_blocks_have_nonempty_code(self) -> None:
        for fixture in FIXTURES_DIR.glob("*.sas"):
            code, _ = read_sas_file(fixture)
            blocks = split_blocks(code)
            for b in blocks:
                assert b.raw_code.strip(), f"Bloco vazio em {fixture.name}"
