"""Testes para document/visual.py — ArchitectureExplorer."""

from __future__ import annotations

import json
from pathlib import Path

from sas2dbx.document.visual import ArchitectureExplorer, _compute_layers, _extract_objective
from sas2dbx.models.dependency_graph import DependencyGraph, JobNode
from sas2dbx.models.migration_result import JobStatus, MigrationResult

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _graph() -> DependencyGraph:
    g = DependencyGraph()
    for name in ["job_a", "job_b", "job_c"]:
        g.jobs[name] = JobNode(
            job_name=name,
            path=Path(f"{name}.sas"),
            inputs=["SASDATA.IN"] if name == "job_a" else [],
            outputs=[f"WORK.{name.upper()}"],
        )
    g.explicit_edges = [("job_b", "job_a"), ("job_c", "job_b")]
    return g


def _results() -> list[MigrationResult]:
    return [
        MigrationResult(job_id="job_a", status=JobStatus.DONE, confidence=0.92,
                        warnings=["UC_PREFIX em saveAsTable"]),
        MigrationResult(job_id="job_b", status=JobStatus.DONE, confidence=0.80),
        MigrationResult(job_id="job_c", status=JobStatus.FAILED, error="LLM timeout"),
    ]


_JOB_DOCS = {
    "job_a": "# job_a\n\n## Objetivo\nCarrega clientes ativos da base raw.\n\n## Datasets\n...",
    "job_b": "# job_b\n\n## Objetivo\nAgrega vendas por departamento.\n",
}


# ---------------------------------------------------------------------------
# generate_data_json
# ---------------------------------------------------------------------------


class TestGenerateDataJson:
    def test_returns_dict(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(DependencyGraph(), [], {})
        assert isinstance(data, dict)

    def test_has_required_keys(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        assert "project" in data
        assert "summary" in data
        assert "jobs" in data
        assert "edges" in data
        assert "execution_order" in data

    def test_summary_counts(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        s = data["summary"]
        assert s["total_jobs"] == 3
        assert s["done"] == 2
        assert s["failed"] == 1

    def test_summary_avg_confidence(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        # (0.92 + 0.80) / 2 = 0.86
        assert abs(data["summary"]["avg_confidence"] - 0.86) < 0.001

    def test_all_jobs_in_data(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        assert "job_a" in data["jobs"]
        assert "job_b" in data["jobs"]
        assert "job_c" in data["jobs"]

    def test_job_has_required_fields(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        job = data["jobs"]["job_a"]
        for field in ("name", "status", "tier", "confidence", "inputs", "outputs",
                      "objective", "warnings", "prereqs", "dependents"):
            assert field in job, f"campo '{field}' ausente"

    def test_job_status_values(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        assert data["jobs"]["job_a"]["status"] == "done"
        assert data["jobs"]["job_c"]["status"] == "failed"

    def test_job_error_field(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        assert data["jobs"]["job_c"]["error"] == "LLM timeout"

    def test_job_inputs_from_graph(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        assert "SASDATA.IN" in data["jobs"]["job_a"]["inputs"]

    def test_edges_reflect_graph(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        edge_pairs = [tuple(e) for e in data["edges"]]
        assert ("job_b", "job_a") in edge_pairs

    def test_prereqs_and_dependents(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        assert "job_a" in data["jobs"]["job_b"]["prereqs"]
        assert "job_b" in data["jobs"]["job_a"]["dependents"]

    def test_objective_extracted_from_doc(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(_graph(), _results(), _JOB_DOCS)
        assert "clientes" in data["jobs"]["job_a"]["objective"].lower()

    def test_custom_job_tiers(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(
            _graph(), _results(), {}, job_tiers={"job_a": "llm"}
        )
        assert data["jobs"]["job_a"]["tier"] == "llm"

    def test_project_name(self) -> None:
        exp = ArchitectureExplorer(project_name="Meu Projeto")
        data = exp.generate_data_json(DependencyGraph(), [], {})
        assert data["project"] == "Meu Projeto"

    def test_empty_graph(self) -> None:
        exp = ArchitectureExplorer()
        data = exp.generate_data_json(DependencyGraph(), [], {})
        assert data["summary"]["total_jobs"] == 0
        assert data["jobs"] == {}


# ---------------------------------------------------------------------------
# generate_html
# ---------------------------------------------------------------------------


class TestGenerateHtml:
    def test_returns_string(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert isinstance(html, str)

    def test_is_valid_html_structure(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "<!DOCTYPE html>" in html
        assert "<html" in html
        assert "</html>" in html

    def test_embeds_json_data(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "job_a" in html
        assert "job_b" in html

    def test_has_svg_graph(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "<svg" in html
        assert "</svg>" in html

    def test_svg_has_all_job_nodes(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "node-job_a" in html
        assert "node-job_b" in html
        assert "node-job_c" in html

    def test_svg_has_edges(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert 'class="edge"' in html

    def test_has_detail_panel(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "detail-panel" in html

    def test_has_js_show_detail(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "showDetail" in html
        assert "closeDetail" in html

    def test_no_external_dependencies(self) -> None:
        """R15: sem CDN, sem src externo."""
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "cdn.jsdelivr" not in html
        assert "unpkg.com" not in html
        assert "cdnjs" not in html
        assert 'src="http' not in html

    def test_has_metrics_cards(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "Total Jobs" in html
        assert "Migrados" in html
        assert "Confiança Média" in html

    def test_has_legend(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        assert "Tier 1" in html
        assert "Tier 2" in html
        assert "Tier 3" in html

    def test_json_is_parseable(self) -> None:
        """JSON embutido deve ser válido."""
        exp = ArchitectureExplorer()
        html = exp.generate_html(_graph(), _results(), _JOB_DOCS)
        # Extrai conteúdo entre "const DATA = " e ";\n\nfunction"
        start = html.index("const DATA = ") + len("const DATA = ")
        end = html.index(";\n\nfunction showDetail")
        json_str = html[start:end]
        data = json.loads(json_str)
        assert data["summary"]["total_jobs"] == 3

    def test_empty_graph_still_renders(self) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(DependencyGraph(), [], {})
        assert "<!DOCTYPE html>" in html


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestArchitectureExplorerWrite:
    def test_write_creates_file(self, tmp_path: Path) -> None:
        exp = ArchitectureExplorer()
        html = exp.generate_html(DependencyGraph(), [], {})
        out = exp.write(html, tmp_path)
        assert out.exists()
        assert out.name == "architecture_explorer.html"

    def test_write_custom_filename(self, tmp_path: Path) -> None:
        exp = ArchitectureExplorer()
        out = exp.write("<html></html>", tmp_path, filename="custom.html")
        assert out.name == "custom.html"

    def test_write_creates_output_dir(self, tmp_path: Path) -> None:
        exp = ArchitectureExplorer()
        out = exp.write("<html></html>", tmp_path / "nested")
        assert out.exists()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_compute_layers_linear(self) -> None:
        order = ["a", "b", "c"]
        edges = [["b", "a"], ["c", "b"]]
        layers = _compute_layers(order, edges)
        assert layers["a"] == 0
        assert layers["b"] == 1
        assert layers["c"] == 2

    def test_compute_layers_no_edges(self) -> None:
        order = ["a", "b", "c"]
        layers = _compute_layers(order, [])
        assert all(v == 0 for v in layers.values())

    def test_extract_objective_present(self) -> None:
        doc = "# job\n\n## Objetivo\nCarrega dados de clientes.\n\n## Datasets\n..."
        result = _extract_objective(doc)
        assert "Carrega dados de clientes" in result

    def test_extract_objective_missing(self) -> None:
        doc = "# job\n\n## Datasets\n..."
        result = _extract_objective(doc)
        assert result == ""

    def test_extract_objective_empty_doc(self) -> None:
        assert _extract_objective("") == ""
