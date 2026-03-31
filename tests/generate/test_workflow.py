"""Testes para generate/workflow.py — WorkflowGenerator."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from sas2dbx.generate.workflow import WorkflowConfig, WorkflowGenerator
from sas2dbx.models.dependency_graph import DependencyGraph, JobNode

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _graph_linear() -> DependencyGraph:
    """job_a → job_b → job_c (cadeia linear)."""
    g = DependencyGraph()
    for name in ["job_a", "job_b", "job_c"]:
        g.jobs[name] = JobNode(job_name=name, path=Path(f"{name}.sas"))
    g.explicit_edges = [("job_b", "job_a"), ("job_c", "job_b")]
    return g


def _graph_no_deps() -> DependencyGraph:
    """Três jobs sem dependências entre si."""
    g = DependencyGraph()
    for name in ["job_x", "job_y", "job_z"]:
        g.jobs[name] = JobNode(job_name=name, path=Path(f"{name}.sas"))
    return g


def _graph_fan_in() -> DependencyGraph:
    """job_c depende de job_a e job_b (fan-in)."""
    g = DependencyGraph()
    for name in ["job_a", "job_b", "job_c"]:
        g.jobs[name] = JobNode(job_name=name, path=Path(f"{name}.sas"))
    g.explicit_edges = [("job_c", "job_a"), ("job_c", "job_b")]
    return g


# ---------------------------------------------------------------------------
# Formato YAML (default)
# ---------------------------------------------------------------------------


class TestWorkflowYAML:
    def test_generates_yaml_file(self, tmp_path: Path) -> None:
        gen = WorkflowGenerator()
        out = gen.generate(_graph_linear(), tmp_path / "wf")
        assert out.suffix == ".yaml"
        assert out.exists()

    def test_yaml_is_valid(self, tmp_path: Path) -> None:
        gen = WorkflowGenerator()
        out = gen.generate(_graph_linear(), tmp_path / "wf")
        data = yaml.safe_load(out.read_text())
        assert isinstance(data, dict)

    def test_pipeline_name_default(self, tmp_path: Path) -> None:
        gen = WorkflowGenerator()
        out = gen.generate(_graph_no_deps(), tmp_path / "wf")
        data = yaml.safe_load(out.read_text())
        assert data["name"] == "sas_migration_pipeline"

    def test_pipeline_name_custom(self, tmp_path: Path) -> None:
        cfg = WorkflowConfig(pipeline_name="meu_pipeline")
        gen = WorkflowGenerator(cfg)
        out = gen.generate(_graph_no_deps(), tmp_path / "wf")
        data = yaml.safe_load(out.read_text())
        assert data["name"] == "meu_pipeline"

    def test_all_jobs_present_as_tasks(self, tmp_path: Path) -> None:
        gen = WorkflowGenerator()
        out = gen.generate(_graph_linear(), tmp_path / "wf")
        data = yaml.safe_load(out.read_text())
        task_keys = {t["task_key"] for t in data["tasks"]}
        assert task_keys == {"job_a", "job_b", "job_c"}

    def test_notebook_path_uses_base(self, tmp_path: Path) -> None:
        cfg = WorkflowConfig(notebook_base_path="/Shared/notebooks")
        gen = WorkflowGenerator(cfg)
        out = gen.generate(_graph_no_deps(), tmp_path / "wf")
        data = yaml.safe_load(out.read_text())
        for task in data["tasks"]:
            assert task["notebook_task"]["notebook_path"].startswith("/Shared/notebooks/")

    def test_linear_depends_on_correct(self, tmp_path: Path) -> None:
        gen = WorkflowGenerator()
        out = gen.generate(_graph_linear(), tmp_path / "wf")
        data = yaml.safe_load(out.read_text())
        by_key = {t["task_key"]: t for t in data["tasks"]}
        assert by_key["job_a"]["depends_on"] == []
        assert {"task_key": "job_a"} in by_key["job_b"]["depends_on"]
        assert {"task_key": "job_b"} in by_key["job_c"]["depends_on"]

    def test_no_deps_all_empty_depends_on(self, tmp_path: Path) -> None:
        gen = WorkflowGenerator()
        out = gen.generate(_graph_no_deps(), tmp_path / "wf")
        data = yaml.safe_load(out.read_text())
        for task in data["tasks"]:
            assert task["depends_on"] == []

    def test_fan_in_both_prereqs_present(self, tmp_path: Path) -> None:
        gen = WorkflowGenerator()
        out = gen.generate(_graph_fan_in(), tmp_path / "wf")
        data = yaml.safe_load(out.read_text())
        by_key = {t["task_key"]: t for t in data["tasks"]}
        deps = {d["task_key"] for d in by_key["job_c"]["depends_on"]}
        assert deps == {"job_a", "job_b"}


# ---------------------------------------------------------------------------
# Formato JSON
# ---------------------------------------------------------------------------


class TestWorkflowJSON:
    def test_generates_json_file(self, tmp_path: Path) -> None:
        cfg = WorkflowConfig(output_format="json")
        gen = WorkflowGenerator(cfg)
        out = gen.generate(_graph_linear(), tmp_path / "wf")
        assert out.suffix == ".json"
        assert out.exists()

    def test_json_is_valid(self, tmp_path: Path) -> None:
        cfg = WorkflowConfig(output_format="json")
        gen = WorkflowGenerator(cfg)
        out = gen.generate(_graph_linear(), tmp_path / "wf")
        data = json.loads(out.read_text())
        assert "tasks" in data

    def test_json_and_yaml_equivalent(self, tmp_path: Path) -> None:
        graph = _graph_fan_in()
        yaml_out = WorkflowGenerator(WorkflowConfig(output_format="yaml")).generate(
            graph, tmp_path / "wf_yaml"
        )
        json_out = WorkflowGenerator(WorkflowConfig(output_format="json")).generate(
            graph, tmp_path / "wf_json"
        )
        yaml_data = yaml.safe_load(yaml_out.read_text())
        json_data = json.loads(json_out.read_text())
        assert yaml_data["name"] == json_data["name"]
        assert len(yaml_data["tasks"]) == len(json_data["tasks"])
