"""Testes para document/architecture.py — ArchitectureDocumentor."""

from __future__ import annotations

from pathlib import Path

from sas2dbx.document.architecture import ArchitectureDocumentor
from sas2dbx.models.dependency_graph import DependencyGraph, JobNode
from sas2dbx.models.migration_result import JobStatus, MigrationResult

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _graph_with_deps() -> DependencyGraph:
    g = DependencyGraph()
    for name in ["job_a", "job_b", "job_c"]:
        g.jobs[name] = JobNode(
            job_name=name,
            path=Path(f"{name}.sas"),
            inputs=["SASDATA.T1"] if name == "job_a" else [f"WORK.{name.upper()}_IN"],
            outputs=[f"WORK.{name.upper()}_OUT"],
            macros_called=["MACRO_UTILS"] if name == "job_b" else [],
            macros_defined=["MACRO_UTILS"] if name == "job_a" else [],
        )
    g.explicit_edges = [("job_b", "job_a"), ("job_c", "job_b")]
    return g


def _results() -> list[MigrationResult]:
    return [
        MigrationResult(job_id="job_a", status=JobStatus.DONE, confidence=0.92),
        MigrationResult(job_id="job_b", status=JobStatus.DONE, confidence=0.80),
        MigrationResult(job_id="job_c", status=JobStatus.FAILED, error="LLM timeout"),
    ]


# ---------------------------------------------------------------------------
# Geração de conteúdo
# ---------------------------------------------------------------------------


class TestArchitectureDocumentor:
    def test_returns_string(self) -> None:
        doc = ArchitectureDocumentor()
        result = doc.generate_architecture_md(DependencyGraph(), [], {})
        assert isinstance(result, str)

    def test_has_main_header(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(DependencyGraph(), [], {})
        assert "# Arquitetura do Projeto SAS Migrado" in md

    def test_inventory_section_present(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "## Inventário de Jobs" in md

    def test_inventory_lists_all_jobs(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "job_a" in md
        assert "job_b" in md
        assert "job_c" in md

    def test_inventory_shows_status(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "done" in md
        assert "failed" in md

    def test_inventory_shows_confidence(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "92%" in md

    def test_dependency_graph_section_present(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "## Grafo de Dependências" in md

    def test_dependency_graph_mermaid_block(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "```mermaid" in md
        assert "graph LR" in md

    def test_dependency_graph_has_edges(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "-->" in md

    def test_no_deps_graph_lists_isolated_nodes(self) -> None:
        g = DependencyGraph()
        g.jobs["solo"] = JobNode(job_name="solo", path=Path("solo.sas"))
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(g, [], {})
        assert "solo" in md

    def test_dataset_map_section(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "## Mapa de Datasets" in md

    def test_dataset_map_lists_datasets(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "SASDATA.T1" in md

    def test_macro_map_section(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "## Mapa de Macros" in md

    def test_macro_map_shows_macro(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "MACRO_UTILS" in md

    def test_macro_map_shows_defined_in(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        # MACRO_UTILS definida em job_a
        assert "job_a" in md

    def test_coverage_section_present(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "## Cobertura de Migração" in md

    def test_coverage_totals(self) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        assert "3" in md   # total de jobs

    def test_no_macros_skips_section(self) -> None:
        g = DependencyGraph()
        g.jobs["j"] = JobNode(job_name="j", path=Path("j.sas"))
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(g, [], {})
        assert "## Mapa de Macros" not in md

    def test_no_datasets_skips_section(self) -> None:
        g = DependencyGraph()
        g.jobs["j"] = JobNode(job_name="j", path=Path("j.sas"))
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(g, [], {})
        assert "## Mapa de Datasets" not in md


# ---------------------------------------------------------------------------
# Write to disk
# ---------------------------------------------------------------------------


class TestArchitectureWrite:
    def test_write_creates_file(self, tmp_path: Path) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(DependencyGraph(), [], {})
        out = doc.write(md, tmp_path)
        assert out.exists()
        assert out.name == "ARCHITECTURE.md"

    def test_write_content_correct(self, tmp_path: Path) -> None:
        doc = ArchitectureDocumentor()
        md = doc.generate_architecture_md(_graph_with_deps(), _results(), {})
        out = doc.write(md, tmp_path)
        assert out.read_text() == md

    def test_write_creates_output_dir(self, tmp_path: Path) -> None:
        doc = ArchitectureDocumentor()
        md = "# test"
        out = doc.write(md, tmp_path / "nested" / "dir")
        assert out.exists()
