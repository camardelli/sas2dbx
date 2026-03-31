"""Testes para analyze/dependency.py — DependencyAnalyzer e DependencyGraph."""

from pathlib import Path

import pytest
import yaml

from sas2dbx.analyze.dependency import DependencyAnalyzer
from sas2dbx.models.dependency_graph import DependencyGraph, JobNode
from sas2dbx.models.sas_ast import SASFile

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "sas"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sas_file(tmp_path: Path, name: str, code: str) -> SASFile:
    p = tmp_path / name
    p.write_text(code, encoding="utf-8")
    return SASFile(path=p, size_bytes=len(code))


# ---------------------------------------------------------------------------
# Estrutura básica do grafo
# ---------------------------------------------------------------------------

class TestDependencyGraphStructure:
    def test_analyze_returns_dependency_graph(self, tmp_path: Path) -> None:
        f = _sas_file(tmp_path, "job_a.sas", "DATA out; SET inp; RUN;")
        analyzer = DependencyAnalyzer()
        graph = analyzer.analyze([f])
        assert isinstance(graph, DependencyGraph)

    def test_jobs_populated(self, tmp_path: Path) -> None:
        f = _sas_file(tmp_path, "job_a.sas", "DATA out; SET inp; RUN;")
        graph = DependencyAnalyzer().analyze([f])
        assert "job_a" in graph.jobs

    def test_job_node_type(self, tmp_path: Path) -> None:
        f = _sas_file(tmp_path, "job_a.sas", "DATA out; SET inp; RUN;")
        graph = DependencyAnalyzer().analyze([f])
        assert isinstance(graph.jobs["job_a"], JobNode)

    def test_empty_file_list(self) -> None:
        graph = DependencyAnalyzer().analyze([])
        assert graph.jobs == {}
        assert graph.explicit_edges == []
        assert graph.implicit_edges == []

    def test_multiple_jobs(self, tmp_path: Path) -> None:
        files = [
            _sas_file(tmp_path, "job_a.sas", "DATA out_a; SET inp; RUN;"),
            _sas_file(tmp_path, "job_b.sas", "DATA out_b; SET inp; RUN;"),
        ]
        graph = DependencyAnalyzer().analyze(files)
        assert len(graph.jobs) == 2


# ---------------------------------------------------------------------------
# Inputs e outputs
# ---------------------------------------------------------------------------

class TestJobNodeDeps:
    def test_inputs_populated(self, tmp_path: Path) -> None:
        f = _sas_file(tmp_path, "job_a.sas", "DATA work.out;\n    SET sasdata.clientes;\nRUN;")
        graph = DependencyAnalyzer().analyze([f])
        assert "SASDATA.CLIENTES" in graph.jobs["job_a"].inputs

    def test_outputs_populated(self, tmp_path: Path) -> None:
        f = _sas_file(tmp_path, "job_a.sas", "DATA sasout.result;\n    SET inp;\nRUN;")
        graph = DependencyAnalyzer().analyze([f])
        assert "SASOUT.RESULT" in graph.jobs["job_a"].outputs

    def test_libname_declared(self, tmp_path: Path) -> None:
        code = "LIBNAME mylib '/data';\nDATA out; SET mylib.inp; RUN;"
        f = _sas_file(tmp_path, "job_a.sas", code)
        graph = DependencyAnalyzer().analyze([f])
        assert "MYLIB" in graph.jobs["job_a"].libnames_declared


# ---------------------------------------------------------------------------
# Autoexec
# ---------------------------------------------------------------------------

class TestAutoexec:
    def test_autoexec_libnames_loaded(self, tmp_path: Path) -> None:
        autoexec = tmp_path / "autoexec.sas"
        autoexec.write_text(
            "LIBNAME SASDATA '/data/input';\n"
            "LIBNAME SASTEMP '/data/temp';\n"
            "LIBNAME SASOUT  '/data/output';\n",
            encoding="utf-8",
        )
        analyzer = DependencyAnalyzer(autoexec_path=autoexec)
        graph = analyzer.analyze([])
        assert "SASDATA" in graph.global_libnames
        assert "SASTEMP" in graph.global_libnames
        assert "SASOUT" in graph.global_libnames

    def test_autoexec_three_libnames(self, tmp_path: Path) -> None:
        autoexec = tmp_path / "autoexec.sas"
        autoexec.write_text(
            "LIBNAME A '/path/a';\n"
            "LIBNAME B '/path/b';\n"
            "LIBNAME C '/path/c';\n",
            encoding="utf-8",
        )
        analyzer = DependencyAnalyzer(autoexec_path=autoexec)
        graph = analyzer.analyze([])
        assert len(graph.global_libnames) == 3

    def test_missing_autoexec_does_not_raise(self, tmp_path: Path) -> None:
        analyzer = DependencyAnalyzer(autoexec_path=tmp_path / "nao_existe.sas")
        graph = analyzer.analyze([])
        assert graph.global_libnames == {}

    def test_real_autoexec_fixture(self) -> None:
        autoexec = FIXTURES_DIR / "autoexec.sas"
        analyzer = DependencyAnalyzer(autoexec_path=autoexec)
        graph = analyzer.analyze([])
        assert len(graph.global_libnames) >= 3
        assert "SASDATA" in graph.global_libnames

    def test_autoexec_libname_overlap_warning(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Job que declara LIBNAME presente no autoexec deve gerar aviso."""
        import logging
        autoexec = tmp_path / "autoexec.sas"
        autoexec.write_text("LIBNAME SASDATA '/global/path';\n", encoding="utf-8")
        job = _sas_file(
            tmp_path, "job_a.sas",
            "LIBNAME SASDATA '/local/path';\nDATA out; SET sasdata.inp; RUN;"
        )
        analyzer = DependencyAnalyzer(autoexec_path=autoexec)
        with caplog.at_level(logging.WARNING):
            analyzer.analyze([job])
        assert any("SASDATA" in r.message and "sobreposto" in r.message
                   for r in caplog.records)


# ---------------------------------------------------------------------------
# Dependências explícitas (libnames.yaml)
# ---------------------------------------------------------------------------

class TestExplicitDependencies:
    def test_depends_on_jobs_creates_edge(self, tmp_path: Path) -> None:
        libnames_yaml = tmp_path / "libnames.yaml"
        libnames_yaml.write_text(
            yaml.dump({
                "SASOUT": {
                    "catalog_schema": "main.migrated",
                    "depends_on_jobs": ["job_a"],
                }
            }),
            encoding="utf-8",
        )
        files = [
            _sas_file(tmp_path, "job_a.sas", "DATA sasout.result; SET inp; RUN;"),
            _sas_file(tmp_path, "job_b.sas",
                      "LIBNAME SASOUT '/data';\nDATA out; SET sasout.result; RUN;"),
        ]
        analyzer = DependencyAnalyzer(libnames_yaml=libnames_yaml)
        graph = analyzer.analyze(files)
        assert ("job_b", "job_a") in graph.explicit_edges

    def test_no_self_loop(self, tmp_path: Path) -> None:
        libnames_yaml = tmp_path / "libnames.yaml"
        libnames_yaml.write_text(
            yaml.dump({"MYLIB": {"catalog_schema": "main.raw", "depends_on_jobs": ["job_a"]}}),
            encoding="utf-8",
        )
        job = _sas_file(tmp_path, "job_a.sas", "LIBNAME MYLIB '/data';\nDATA out; SET inp; RUN;")
        analyzer = DependencyAnalyzer(libnames_yaml=libnames_yaml)
        graph = analyzer.analyze([job])
        # job_a não deve depender de si mesmo
        assert ("job_a", "job_a") not in graph.explicit_edges

    def test_missing_libnames_yaml_does_not_raise(self, tmp_path: Path) -> None:
        analyzer = DependencyAnalyzer(libnames_yaml=tmp_path / "nao_existe.yaml")
        graph = analyzer.analyze([])
        assert graph.explicit_edges == []


# ---------------------------------------------------------------------------
# Dependências implícitas
# ---------------------------------------------------------------------------

class TestImplicitDependencies:
    def test_implicit_dependency_detected(self, tmp_path: Path) -> None:
        """job_b lê sasdata.clientes que job_a cria → dependência implícita."""
        files = [
            _sas_file(tmp_path, "job_a.sas",
                      "DATA sasdata.clientes;\n    SET raw.src;\nRUN;"),
            _sas_file(tmp_path, "job_b.sas",
                      "DATA sasdata.out;\n    SET sasdata.clientes;\nRUN;"),
        ]
        graph = DependencyAnalyzer().analyze(files)
        implicit = graph.get_implicit_dependencies()
        assert any(
            dep == "job_b" and prereq == "job_a" and ds == "SASDATA.CLIENTES"
            for dep, prereq, ds in implicit
        )

    def test_work_datasets_excluded_from_implicit(self, tmp_path: Path) -> None:
        """Datasets WORK não devem criar dependências implícitas entre jobs."""
        files = [
            _sas_file(tmp_path, "job_a.sas",
                      "DATA work.tmp;\n    SET raw.src;\nRUN;"),
            _sas_file(tmp_path, "job_b.sas",
                      "DATA sasdata.out;\n    SET work.tmp;\nRUN;"),
        ]
        graph = DependencyAnalyzer().analyze(files)
        implicit = graph.get_implicit_dependencies()
        assert not any(ds == "WORK.TMP" for _, _, ds in implicit)

    def test_implicit_dep_generates_warning(self, tmp_path: Path) -> None:
        files = [
            _sas_file(tmp_path, "job_a.sas",
                      "DATA sasdata.clientes;\n    SET raw.src;\nRUN;"),
            _sas_file(tmp_path, "job_b.sas",
                      "DATA out;\n    SET sasdata.clientes;\nRUN;"),
        ]
        graph = DependencyAnalyzer().analyze(files)
        assert len(graph.warnings) > 0
        assert any("SASDATA.CLIENTES" in w for w in graph.warnings)

    def test_no_implicit_dep_no_overlap(self, tmp_path: Path) -> None:
        files = [
            _sas_file(tmp_path, "job_a.sas", "DATA sasdata.a;\n    SET raw.x;\nRUN;"),
            _sas_file(tmp_path, "job_b.sas", "DATA sasdata.b;\n    SET raw.y;\nRUN;"),
        ]
        graph = DependencyAnalyzer().analyze(files)
        assert graph.get_implicit_dependencies() == []


# ---------------------------------------------------------------------------
# Ordem de execução
# ---------------------------------------------------------------------------

class TestExecutionOrder:
    def test_execution_order_no_deps(self, tmp_path: Path) -> None:
        files = [
            _sas_file(tmp_path, "job_b.sas", "DATA out; SET inp_b; RUN;"),
            _sas_file(tmp_path, "job_a.sas", "DATA out; SET inp_a; RUN;"),
        ]
        graph = DependencyAnalyzer().analyze(files)
        order = graph.get_execution_order()
        assert set(order) == {"job_a", "job_b"}

    def test_execution_order_with_implicit_dep(self, tmp_path: Path) -> None:
        """job_a cria sasdata.clientes → job_b deve vir depois."""
        files = [
            _sas_file(tmp_path, "job_b.sas",
                      "DATA out;\n    SET sasdata.clientes;\nRUN;"),
            _sas_file(tmp_path, "job_a.sas",
                      "DATA sasdata.clientes;\n    SET raw.src;\nRUN;"),
        ]
        graph = DependencyAnalyzer().analyze(files)
        order = graph.get_execution_order()
        assert order.index("job_a") < order.index("job_b")

    def test_execution_order_returns_all_jobs(self, tmp_path: Path) -> None:
        files = [
            _sas_file(tmp_path, "job_a.sas", "DATA sasdata.a; SET raw.x; RUN;"),
            _sas_file(tmp_path, "job_b.sas", "DATA out; SET sasdata.a; RUN;"),
            _sas_file(tmp_path, "job_c.sas", "DATA out2; SET raw.y; RUN;"),
        ]
        graph = DependencyAnalyzer().analyze(files)
        order = graph.get_execution_order()
        assert len(order) == 3


# ---------------------------------------------------------------------------
# Fixtures reais
# ---------------------------------------------------------------------------

class TestRealFixtures:
    def test_analyze_three_fixtures(self) -> None:
        from sas2dbx.ingest.scanner import scan_directory
        sas_files = scan_directory(FIXTURES_DIR)
        # Exclui autoexec da análise (não é job)
        sas_files = [f for f in sas_files if f.path.name != "autoexec.sas"]
        graph = DependencyAnalyzer(
            autoexec_path=FIXTURES_DIR / "autoexec.sas"
        ).analyze(sas_files)
        assert len(graph.jobs) == 3

    def test_autoexec_loaded_in_real_analysis(self) -> None:
        from sas2dbx.ingest.scanner import scan_directory
        sas_files = [
            f for f in scan_directory(FIXTURES_DIR)
            if f.path.name != "autoexec.sas"
        ]
        graph = DependencyAnalyzer(
            autoexec_path=FIXTURES_DIR / "autoexec.sas"
        ).analyze(sas_files)
        assert len(graph.global_libnames) >= 3

    def test_execution_order_contains_all_jobs(self) -> None:
        from sas2dbx.ingest.scanner import scan_directory
        sas_files = [
            f for f in scan_directory(FIXTURES_DIR)
            if f.path.name != "autoexec.sas"
        ]
        graph = DependencyAnalyzer().analyze(sas_files)
        order = graph.get_execution_order()
        job_names = {f.path.stem for f in sas_files}
        assert set(order) == job_names
