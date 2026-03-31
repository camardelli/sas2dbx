"""Testes para generate/report.py — ReportGenerator."""

from __future__ import annotations

import json
from pathlib import Path

from sas2dbx.generate.report import ReportConfig, ReportGenerator
from sas2dbx.models.migration_result import (
    JobStatus,
    MigrationResult,
    ValidationError,
    ValidationResult,
    ValidationWarning,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _done(
    job_id: str, confidence: float = 0.9, warnings: list[str] | None = None
) -> MigrationResult:
    return MigrationResult(
        job_id=job_id,
        status=JobStatus.DONE,
        confidence=confidence,
        output_path=f"/out/{job_id}.py",
        warnings=warnings or [],
        completed_at="2026-03-31T10:00:00+00:00",
    )


def _failed(job_id: str) -> MigrationResult:
    return MigrationResult(
        job_id=job_id,
        status=JobStatus.FAILED,
        error="LLM timeout",
    )


def _with_validation(job_id: str, is_valid: bool = True) -> MigrationResult:
    vr = ValidationResult(
        is_valid=is_valid,
        syntax_ok=True,
        errors=(
            []
            if is_valid
            else [ValidationError(code="MISSING_F_IMPORT", message="F não importado")]
        ),
        warnings=[ValidationWarning(code="UC_PREFIX", message="sem prefixo catalog")],
    )
    r = _done(job_id)
    r.validation_result = vr
    return r


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


class TestReportJSON:
    def test_generates_json_file(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("job_a")], tmp_path)
        json_paths = [p for p in paths if p.suffix == ".json"]
        assert len(json_paths) == 1
        assert json_paths[0].exists()

    def test_json_valid_structure(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("job_a"), _failed("job_b")], tmp_path)
        data = json.loads(next(p for p in paths if p.suffix == ".json").read_text())
        assert "summary" in data
        assert "jobs" in data

    def test_summary_counts(self, tmp_path: Path) -> None:
        results = [_done("a"), _done("b"), _failed("c")]
        gen = ReportGenerator()
        paths = gen.generate(results, tmp_path)
        data = json.loads(next(p for p in paths if p.suffix == ".json").read_text())
        s = data["summary"]
        assert s["total_jobs"] == 3
        assert s["fully_migrated"] == 2
        assert s["failed"] == 1

    def test_avg_confidence_only_done_jobs(self, tmp_path: Path) -> None:
        results = [_done("a", 0.8), _done("b", 0.6), _failed("c")]
        gen = ReportGenerator()
        paths = gen.generate(results, tmp_path)
        data = json.loads(next(p for p in paths if p.suffix == ".json").read_text())
        # avg de 0.8 e 0.6 = 0.7; job_c (failed) não entra
        assert abs(data["summary"]["avg_confidence"] - 0.7) < 0.001

    def test_job_entry_fields(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("job_x", warnings=["aviso 1"])], tmp_path)
        data = json.loads(next(p for p in paths if p.suffix == ".json").read_text())
        job = data["jobs"][0]
        assert job["job_id"] == "job_x"
        assert job["status"] == "done"
        assert job["warnings"] == ["aviso 1"]
        assert job["output_path"] == "/out/job_x.py"

    def test_failed_job_has_error_field(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_failed("job_err")], tmp_path)
        data = json.loads(next(p for p in paths if p.suffix == ".json").read_text())
        job = data["jobs"][0]
        assert job["error"] == "LLM timeout"

    def test_validation_result_in_json(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_with_validation("job_v", is_valid=False)], tmp_path)
        data = json.loads(next(p for p in paths if p.suffix == ".json").read_text())
        val = data["jobs"][0]["validation"]
        assert val["is_valid"] is False
        assert any(e["code"] == "MISSING_F_IMPORT" for e in val["errors"])
        assert any(w["code"] == "UC_PREFIX" for w in val["warnings"])

    def test_no_validation_field_when_none(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("job_no_val")], tmp_path)
        data = json.loads(next(p for p in paths if p.suffix == ".json").read_text())
        assert "validation" not in data["jobs"][0]

    def test_custom_output_stem(self, tmp_path: Path) -> None:
        cfg = ReportConfig(output_stem="relatorio_final")
        gen = ReportGenerator(cfg)
        paths = gen.generate([_done("a")], tmp_path)
        json_path = next(p for p in paths if p.suffix == ".json")
        assert json_path.stem == "relatorio_final"

    def test_json_only_when_markdown_disabled(self, tmp_path: Path) -> None:
        cfg = ReportConfig(include_markdown=False)
        gen = ReportGenerator(cfg)
        paths = gen.generate([_done("a")], tmp_path)
        assert all(p.suffix == ".json" for p in paths)
        assert len(paths) == 1


# ---------------------------------------------------------------------------
# Markdown output
# ---------------------------------------------------------------------------


class TestReportMarkdown:
    def test_generates_markdown_file(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("job_a")], tmp_path)
        md_paths = [p for p in paths if p.suffix == ".md"]
        assert len(md_paths) == 1
        assert md_paths[0].exists()

    def test_markdown_contains_job_id(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("job_abc")], tmp_path)
        md = next(p for p in paths if p.suffix == ".md").read_text()
        assert "job_abc" in md

    def test_markdown_contains_summary_table(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("a"), _failed("b")], tmp_path)
        md = next(p for p in paths if p.suffix == ".md").read_text()
        assert "Total de jobs" in md
        assert "Migrados com sucesso" in md

    def test_markdown_shows_warnings(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("j", warnings=["PROC FORMAT não convertido"])], tmp_path)
        md = next(p for p in paths if p.suffix == ".md").read_text()
        assert "PROC FORMAT não convertido" in md

    def test_markdown_shows_error_for_failed(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_failed("job_fail")], tmp_path)
        md = next(p for p in paths if p.suffix == ".md").read_text()
        assert "LLM timeout" in md

    def test_markdown_only_when_json_disabled(self, tmp_path: Path) -> None:
        cfg = ReportConfig(include_json=False)
        gen = ReportGenerator(cfg)
        paths = gen.generate([_done("a")], tmp_path)
        assert all(p.suffix == ".md" for p in paths)
        assert len(paths) == 1

    def test_both_files_generated_by_default(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([_done("a")], tmp_path)
        suffixes = {p.suffix for p in paths}
        assert ".json" in suffixes
        assert ".md" in suffixes

    def test_empty_results_list(self, tmp_path: Path) -> None:
        gen = ReportGenerator()
        paths = gen.generate([], tmp_path)
        data = json.loads(next(p for p in paths if p.suffix == ".json").read_text())
        assert data["summary"]["total_jobs"] == 0
        assert data["summary"]["avg_confidence"] == 0.0
