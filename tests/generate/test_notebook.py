"""Testes para generate/notebook.py — CellModel, renderers e NotebookGenerator."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sas2dbx.generate.notebook import (
    Cell,
    CellModel,
    CellType,
    DatabricksPyRenderer,
    JupyterIpynbRenderer,
    NotebookGenerator,
)
from sas2dbx.models.migration_result import SASOrigin

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_model(
    *,
    warnings: list[str] | None = None,
    cells: list[Cell] | None = None,
) -> CellModel:
    return CellModel(
        job_name="job_test",
        sas_source_file="job_test.sas",
        migration_date="2026-03-31",
        confidence=0.85,
        warnings=warnings or [],
        cells=cells or [],
    )


def _code_cell(source: str, order: int = 0, origin: SASOrigin | None = None) -> Cell:
    return Cell(cell_type=CellType.CODE, source=source, order=order, sas_origin=origin)


def _md_cell(source: str, order: int = 0) -> Cell:
    return Cell(cell_type=CellType.MARKDOWN, source=source, order=order)


# ---------------------------------------------------------------------------
# AC-1: CellModel com SASOrigin
# ---------------------------------------------------------------------------


class TestCellModel:
    def test_cell_preserves_sas_origin(self) -> None:
        origin = SASOrigin(
            job_name="job_test",
            start_line=10,
            end_line=25,
            construct_type="PROC_SQL",
        )
        cell = _code_cell("df = spark.sql('SELECT 1')", origin=origin)
        assert cell.sas_origin is not None
        assert cell.sas_origin.start_line == 10
        assert cell.sas_origin.end_line == 25
        assert cell.sas_origin.construct_type == "PROC_SQL"

    def test_cell_without_origin(self) -> None:
        cell = _code_cell("pass")
        assert cell.sas_origin is None

    def test_cell_model_fields(self) -> None:
        model = _make_model(warnings=["warning A"], cells=[_code_cell("x = 1")])
        assert model.job_name == "job_test"
        assert model.confidence == 0.85
        assert len(model.warnings) == 1
        assert len(model.cells) == 1


# ---------------------------------------------------------------------------
# AC-2: DatabricksPyRenderer
# ---------------------------------------------------------------------------


class TestDatabricksPyRenderer:
    def test_renders_py_extension(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model()
        out = tmp_path / "nb"
        renderer.render(model, out)
        assert (tmp_path / "nb.py").exists()

    def test_starts_with_notebook_source(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        renderer.render(_make_model(), tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert content.startswith("# Databricks notebook source")

    def test_command_separator_present(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        renderer.render(_make_model(), tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "# COMMAND ----------" in content

    def test_markdown_cell_uses_magic(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model(cells=[_md_cell("## Seção\nTexto aqui")])
        renderer.render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "# MAGIC %md" in content
        assert "# MAGIC ## Seção" in content

    def test_code_cell_written_directly(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model(cells=[_code_cell("result = spark.sql('SELECT 1')")])
        renderer.render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "result = spark.sql('SELECT 1')" in content

    def test_header_cell_present(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model()
        renderer.render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "job_test" in content
        assert "job_test.sas" in content
        assert "0.85" in content

    def test_header_includes_warnings(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model(warnings=["PROC FORMAT não convertido"])
        renderer.render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "PROC FORMAT não convertido" in content

    def test_cells_ordered_by_order_field(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model(cells=[
            _code_cell("# segundo", order=2),
            _code_cell("# primeiro", order=1),
        ])
        renderer.render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert content.index("# primeiro") < content.index("# segundo")

    def test_imports_cell_f_detected(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model(cells=[_code_cell("df.select(F.col('x'))")])
        renderer.render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "import pyspark.sql.functions as F" in content

    def test_imports_cell_window_detected(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model(cells=[_code_cell("w = Window.partitionBy('id')")])
        renderer.render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "from pyspark.sql.window import Window" in content

    def test_no_imports_when_none_needed(self, tmp_path: Path) -> None:
        renderer = DatabricksPyRenderer()
        model = _make_model(cells=[_code_cell("x = 1 + 1")])
        renderer.render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "Sem imports adicionais necessários" in content


# ---------------------------------------------------------------------------
# AC-3: JupyterIpynbRenderer
# ---------------------------------------------------------------------------


class TestJupyterIpynbRenderer:
    def test_renders_ipynb_extension(self, tmp_path: Path) -> None:
        renderer = JupyterIpynbRenderer()
        renderer.render(_make_model(), tmp_path / "nb")
        assert (tmp_path / "nb.ipynb").exists()

    def test_valid_json(self, tmp_path: Path) -> None:
        renderer = JupyterIpynbRenderer()
        renderer.render(_make_model(), tmp_path / "nb")
        data = json.loads((tmp_path / "nb.ipynb").read_text())
        assert data["nbformat"] == 4

    def test_databricks_metadata_present(self, tmp_path: Path) -> None:
        renderer = JupyterIpynbRenderer()
        renderer.render(_make_model(), tmp_path / "nb")
        data = json.loads((tmp_path / "nb.ipynb").read_text())
        meta = data["metadata"]
        assert "application/vnd.databricks.v1+notebook" in meta
        dbx = meta["application/vnd.databricks.v1+notebook"]
        assert dbx["defaultLanguage"] == "python"
        assert dbx["notebookName"] == "job_test"
        assert dbx["cluster_source"] == "NEW"

    def test_code_cell_structure(self, tmp_path: Path) -> None:
        renderer = JupyterIpynbRenderer()
        model = _make_model(cells=[_code_cell("x = 1")])
        renderer.render(model, tmp_path / "nb")
        data = json.loads((tmp_path / "nb.ipynb").read_text())
        code_cells = [c for c in data["cells"] if c["cell_type"] == "code"]
        assert any("x = 1" in "".join(c["source"]) for c in code_cells)

    def test_markdown_cell_structure(self, tmp_path: Path) -> None:
        renderer = JupyterIpynbRenderer()
        model = _make_model(cells=[_md_cell("## Título")])
        renderer.render(model, tmp_path / "nb")
        data = json.loads((tmp_path / "nb.ipynb").read_text())
        md_cells = [c for c in data["cells"] if c["cell_type"] == "markdown"]
        assert any("## Título" in "".join(c["source"]) for c in md_cells)

    def test_header_in_ipynb(self, tmp_path: Path) -> None:
        renderer = JupyterIpynbRenderer()
        model = _make_model()
        renderer.render(model, tmp_path / "nb")
        data = json.loads((tmp_path / "nb.ipynb").read_text())
        all_source = " ".join("".join(c["source"]) for c in data["cells"])
        assert "job_test" in all_source
        assert "0.85" in all_source


# ---------------------------------------------------------------------------
# AC-4: Equivalência funcional entre formatos
# ---------------------------------------------------------------------------


class TestFormatEquivalence:
    def test_both_formats_contain_same_code(self, tmp_path: Path) -> None:
        code = "df = spark.read.table('main.raw.t')"
        model = _make_model(cells=[_code_cell(code)])

        DatabricksPyRenderer().render(model, tmp_path / "nb_py")
        JupyterIpynbRenderer().render(model, tmp_path / "nb_ipynb")

        py_content = (tmp_path / "nb_py.py").read_text()
        ipynb_data = json.loads((tmp_path / "nb_ipynb.ipynb").read_text())
        ipynb_all = " ".join("".join(c["source"]) for c in ipynb_data["cells"])

        assert code in py_content
        assert code in ipynb_all

    def test_both_formats_contain_warnings(self, tmp_path: Path) -> None:
        model = _make_model(warnings=["aviso de teste"])

        DatabricksPyRenderer().render(model, tmp_path / "py")
        JupyterIpynbRenderer().render(model, tmp_path / "ipynb")

        assert "aviso de teste" in (tmp_path / "py.py").read_text()
        ipynb_all = " ".join(
            "".join(c["source"])
            for c in json.loads((tmp_path / "ipynb.ipynb").read_text())["cells"]
        )
        assert "aviso de teste" in ipynb_all


# ---------------------------------------------------------------------------
# AC-5: Célula de header
# ---------------------------------------------------------------------------


class TestHeaderCell:
    def test_header_no_warnings_shows_none(self, tmp_path: Path) -> None:
        model = _make_model(warnings=[])
        DatabricksPyRenderer().render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "_nenhum_" in content

    def test_header_migration_date_present(self, tmp_path: Path) -> None:
        model = _make_model()
        DatabricksPyRenderer().render(model, tmp_path / "nb")
        content = (tmp_path / "nb.py").read_text()
        assert "2026-03-31" in content


# ---------------------------------------------------------------------------
# AC-6: NotebookGenerator seleciona renderer correto
# ---------------------------------------------------------------------------


class TestNotebookGenerator:
    def test_py_format_generates_py_file(self, tmp_path: Path) -> None:
        gen = NotebookGenerator(notebook_format="py")
        out = gen.generate(_make_model(), tmp_path / "nb")
        assert out.suffix == ".py"
        assert out.exists()

    def test_ipynb_format_generates_ipynb_file(self, tmp_path: Path) -> None:
        gen = NotebookGenerator(notebook_format="ipynb")
        out = gen.generate(_make_model(), tmp_path / "nb")
        assert out.suffix == ".ipynb"
        assert out.exists()

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(ValueError, match="notebook_format inválido"):
            NotebookGenerator(notebook_format="html")

    def test_default_format_is_py(self, tmp_path: Path) -> None:
        gen = NotebookGenerator()
        out = gen.generate(_make_model(), tmp_path / "nb")
        assert out.suffix == ".py"
