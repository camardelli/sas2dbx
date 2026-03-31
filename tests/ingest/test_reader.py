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
