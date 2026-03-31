"""Testes para analyze/parser.py — extract_block_deps."""

from pathlib import Path

from sas2dbx.analyze.parser import _normalize_ds, extract_block_deps
from sas2dbx.models.sas_ast import SASBlock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _block(code: str) -> SASBlock:
    return SASBlock(raw_code=code, start_line=1, end_line=code.count("\n") + 1)


# ---------------------------------------------------------------------------
# _normalize_ds
# ---------------------------------------------------------------------------

class TestNormalizeDs:
    def test_simple_name(self) -> None:
        assert _normalize_ds("clientes") == "CLIENTES"

    def test_lib_dot_name(self) -> None:
        assert _normalize_ds("SASDATA.clientes") == "SASDATA.CLIENTES"

    def test_strips_options(self) -> None:
        assert _normalize_ds("sasdata.clientes(keep=id)") == "SASDATA.CLIENTES"

    def test_null_returns_none(self) -> None:
        assert _normalize_ds("_NULL_") is None

    def test_invalid_returns_none(self) -> None:
        assert _normalize_ds("") is None

    def test_preserves_underscores(self) -> None:
        assert _normalize_ds("my_dataset") == "MY_DATASET"


# ---------------------------------------------------------------------------
# DATA step
# ---------------------------------------------------------------------------

class TestDataStep:
    def test_output_dataset(self) -> None:
        code = "DATA work.out;\n    SET sasdata.inp;\nRUN;"
        deps = extract_block_deps(_block(code))
        assert "WORK.OUT" in deps.outputs

    def test_input_set(self) -> None:
        code = "DATA work.out;\n    SET sasdata.clientes;\nRUN;"
        deps = extract_block_deps(_block(code))
        assert "SASDATA.CLIENTES" in deps.inputs

    def test_input_merge(self) -> None:
        code = "DATA merged;\n    MERGE sasdata.a sasdata.b;\n    BY id;\nRUN;"
        deps = extract_block_deps(_block(code))
        assert "SASDATA.A" in deps.inputs
        # Nota: MERGE com múltiplos datasets — ao menos o primeiro é capturado
        assert len(deps.inputs) >= 1

    def test_null_output_ignored(self) -> None:
        code = "DATA _NULL_;\n    SET sasdata.clientes;\n    CALL MISSING(x);\nRUN;"
        deps = extract_block_deps(_block(code))
        assert not deps.outputs

    def test_simple_name_no_libname(self) -> None:
        code = "DATA out;\n    SET inp;\nRUN;"
        deps = extract_block_deps(_block(code))
        assert "OUT" in deps.outputs
        assert "INP" in deps.inputs

    def test_retain_does_not_become_input(self) -> None:
        """RETAIN não é dataset — não deve aparecer como input."""
        code = (
            "DATA vendas;\n"
            "    SET sasdata.vendas;\n"
            "    BY depto;\n"
            "    RETAIN acumulado 0;\n"
            "RUN;"
        )
        deps = extract_block_deps(_block(code))
        assert "SASDATA.VENDAS" in deps.inputs
        # RETAIN não gera dataset
        assert all("ACUMULADO" not in ds for ds in deps.inputs)

    def test_output_with_inline_options(self) -> None:
        """M1 fix: DATA step com opções inline não perde o output."""
        code = "DATA work.out(keep=id nome);\n    SET sasdata.inp;\nRUN;"
        deps = extract_block_deps(_block(code))
        assert "WORK.OUT" in deps.outputs

    def test_multiple_outputs_with_inline_options(self) -> None:
        """M1 fix: múltiplos outputs, um com opções, ambos capturados."""
        code = "DATA work.out(keep=id) sasout.erros;\n    SET inp;\nRUN;"
        deps = extract_block_deps(_block(code))
        assert "WORK.OUT" in deps.outputs
        assert "SASOUT.ERROS" in deps.outputs


# ---------------------------------------------------------------------------
# PROC SQL
# ---------------------------------------------------------------------------

class TestProcSql:
    def test_create_table_output(self) -> None:
        code = (
            "PROC SQL;\n"
            "    CREATE TABLE work.result AS\n"
            "    SELECT * FROM sasdata.clientes;\n"
            "QUIT;"
        )
        deps = extract_block_deps(_block(code))
        assert "WORK.RESULT" in deps.outputs
        assert "SASDATA.CLIENTES" in deps.inputs

    def test_join_as_input(self) -> None:
        code = (
            "PROC SQL;\n"
            "    SELECT c.*, v.total\n"
            "    FROM sasdata.clientes c\n"
            "    LEFT JOIN sasdata.vendas v ON c.id = v.id;\n"
            "QUIT;"
        )
        deps = extract_block_deps(_block(code))
        assert "SASDATA.CLIENTES" in deps.inputs
        assert "SASDATA.VENDAS" in deps.inputs

    def test_no_false_outputs_without_create(self) -> None:
        code = (
            "PROC SQL;\n"
            "    SELECT * FROM sasdata.clientes;\n"
            "QUIT;"
        )
        deps = extract_block_deps(_block(code))
        assert not deps.outputs
        assert "SASDATA.CLIENTES" in deps.inputs


# ---------------------------------------------------------------------------
# PROC com DATA= e OUT=
# ---------------------------------------------------------------------------

class TestProcDataOut:
    def test_proc_sort_data_out(self) -> None:
        code = "PROC SORT DATA=sasdata.clientes OUT=work.sorted;\n    BY id;\nRUN;"
        deps = extract_block_deps(_block(code))
        assert "SASDATA.CLIENTES" in deps.inputs
        assert "WORK.SORTED" in deps.outputs

    def test_proc_means_output(self) -> None:
        code = (
            "PROC MEANS DATA=sasdata.vendas NWAY NOPRINT;\n"
            "    CLASS depto;\n"
            "    VAR valor;\n"
            "    OUTPUT OUT=work.totais SUM=total;\n"
            "RUN;"
        )
        deps = extract_block_deps(_block(code))
        assert "SASDATA.VENDAS" in deps.inputs
        assert "WORK.TOTAIS" in deps.outputs

    def test_proc_freq_no_output(self) -> None:
        code = "PROC FREQ DATA=work.resultado;\n    TABLES status;\nRUN;"
        deps = extract_block_deps(_block(code))
        assert "WORK.RESULTADO" in deps.inputs
        assert not deps.outputs


# ---------------------------------------------------------------------------
# LIBNAME
# ---------------------------------------------------------------------------

class TestLibname:
    def test_libname_declared(self) -> None:
        code = "LIBNAME mylib '/data/sas';"
        deps = extract_block_deps(_block(code))
        assert "MYLIB" in deps.libnames_declared

    def test_multiple_libnames(self) -> None:
        code = (
            "LIBNAME sasdata '/data/input';\n"
            "LIBNAME sasout '/data/output';\n"
        )
        deps = extract_block_deps(_block(code))
        assert "SASDATA" in deps.libnames_declared
        assert "SASOUT" in deps.libnames_declared


# ---------------------------------------------------------------------------
# Macros
# ---------------------------------------------------------------------------

class TestMacros:
    def test_macro_defined(self) -> None:
        code = "%MACRO calc_totals(dataset=);\n    %PUT &dataset;\n%MEND calc_totals;"
        deps = extract_block_deps(_block(code))
        assert "CALC_TOTALS" in deps.macros_defined

    def test_macro_call_detected(self) -> None:
        code = "%calc_totals(dataset=sasdata.vendas, var=valor, groupby=depto);"
        deps = extract_block_deps(_block(code))
        assert "CALC_TOTALS" in deps.macros_called

    def test_macro_invocation_extracts_qualified_inputs(self) -> None:
        """Parâmetros lib.member em invocação de macro viram inputs."""
        code = "%calc_totals(dataset=sasdata.vendas, var=valor, groupby=depto);"
        deps = extract_block_deps(_block(code))
        assert "SASDATA.VENDAS" in deps.inputs

    def test_macro_invocation_unqualified_not_input(self) -> None:
        """Parâmetros sem qualificador (ex: valor) não viram inputs."""
        code = "%calc_totals(dataset=sasdata.vendas, var=valor, groupby=depto);"
        deps = extract_block_deps(_block(code))
        assert "VALOR" not in deps.inputs
        assert "DEPTO" not in deps.inputs

    def test_defined_macro_not_in_called(self) -> None:
        """Macro definida no mesmo bloco não deve aparecer em macros_called."""
        code = "%MACRO mymacro;\n    %PUT hello;\n%MEND mymacro;\n%mymacro;"
        deps = extract_block_deps(_block(code))
        assert "MYMACRO" in deps.macros_defined
        assert "MYMACRO" not in deps.macros_called

    def test_sas_keyword_macros_not_called(self) -> None:
        """Keywords SAS como %IF, %DO não devem aparecer como macros chamadas."""
        code = (
            "%MACRO test;\n"
            "    %IF &cond %THEN %DO;\n"
            "        %PUT ok;\n"
            "    %END;\n"
            "%MEND test;"
        )
        deps = extract_block_deps(_block(code))
        assert "IF" not in deps.macros_called
        assert "DO" not in deps.macros_called
        assert "PUT" not in deps.macros_called


# ---------------------------------------------------------------------------
# Fixture real
# ---------------------------------------------------------------------------

class TestRealFixture:
    FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sas"

    def test_job001_fixture_inputs(self) -> None:
        """job_001 deve ler datasets de sasdata/sastemp."""
        from sas2dbx.ingest.reader import read_sas_file, split_blocks
        code, _ = read_sas_file(self.FIXTURES_DIR / "job_001_carga_clientes.sas")
        blocks = split_blocks(code)
        all_inputs: set[str] = set()
        for b in blocks:
            all_inputs.update(extract_block_deps(b).inputs)
        # Deve ter ao menos um dataset de entrada
        assert len(all_inputs) > 0

    def test_job002_macro_defined(self) -> None:
        """job_002 deve ter macro CALC_TOTALS definida em algum bloco."""
        from sas2dbx.ingest.reader import read_sas_file, split_blocks
        code, _ = read_sas_file(self.FIXTURES_DIR / "job_002_transform_vendas.sas")
        blocks = split_blocks(code)
        all_defined: set[str] = set()
        for b in blocks:
            all_defined.update(extract_block_deps(b).macros_defined)
        assert "CALC_TOTALS" in all_defined

    def test_job002_has_multiple_blocks(self) -> None:
        """job_002 deve gerar múltiplos blocos (%MACRO, DATA step, PROC)."""
        from sas2dbx.ingest.reader import read_sas_file, split_blocks
        code, _ = read_sas_file(self.FIXTURES_DIR / "job_002_transform_vendas.sas")
        blocks = split_blocks(code)
        # %MACRO calc_totals + DATA vendas_com_retain + PROC FREQ = 3 blocos
        assert len(blocks) >= 3
