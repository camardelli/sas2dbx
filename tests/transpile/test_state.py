"""Testes para transpile/state.py — MigrationStateManager."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sas2dbx.models.migration_result import JobStatus, MigrationResult
from sas2dbx.transpile.state import MigrationStateManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _manager(tmp_path: Path) -> MigrationStateManager:
    return MigrationStateManager(tmp_path / "output")


def _done_result(job_id: str, confidence: float = 0.9) -> MigrationResult:
    return MigrationResult(
        job_id=job_id,
        status=JobStatus.DONE,
        confidence=confidence,
        output_path=f"/out/{job_id}.py",
    )


# ---------------------------------------------------------------------------
# init_fresh
# ---------------------------------------------------------------------------

class TestInitFresh:
    def test_creates_state_file(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a", "job_b"])
        assert mgr.state_path.exists()

    def test_all_jobs_start_as_pending(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a", "job_b"])
        assert mgr.get_job_status("job_a") == JobStatus.PENDING
        assert mgr.get_job_status("job_b") == JobStatus.PENDING

    def test_overwrites_existing_state(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a"])
        mgr.mark_done("job_a", _done_result("job_a"))
        # segundo init_fresh deve resetar
        mgr.init_fresh(["job_a"])
        assert mgr.get_job_status("job_a") == JobStatus.PENDING

    def test_state_file_is_valid_json(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a"])
        with open(mgr.state_path, encoding="utf-8") as f:
            data = json.load(f)
        assert data["version"] == "1.0"
        assert "started_at" in data
        assert "jobs" in data


# ---------------------------------------------------------------------------
# mark_started / mark_done / mark_failed
# ---------------------------------------------------------------------------

class TestMarkTransitions:
    def test_mark_started(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a"])
        mgr.mark_started("job_a")
        assert mgr.get_job_status("job_a") == JobStatus.IN_PROGRESS

    def test_mark_done(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a"])
        mgr.mark_done("job_a", _done_result("job_a"))
        assert mgr.get_job_status("job_a") == JobStatus.DONE

    def test_mark_failed(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a"])
        mgr.mark_failed("job_a", "LLM timeout")
        assert mgr.get_job_status("job_a") == JobStatus.FAILED

    def test_mark_done_persists_confidence(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a"])
        mgr.mark_done("job_a", _done_result("job_a", confidence=0.88))
        with open(mgr.state_path, encoding="utf-8") as f:
            data = json.load(f)
        assert data["jobs"]["job_a"]["confidence"] == pytest.approx(0.88)

    def test_mark_failed_persists_error(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a"])
        mgr.mark_failed("job_a", "conexão recusada")
        with open(mgr.state_path, encoding="utf-8") as f:
            data = json.load(f)
        assert data["jobs"]["job_a"]["error"] == "conexão recusada"

    def test_each_mark_saves_immediately(self, tmp_path: Path) -> None:
        """Verificar que mark_* grava em disco imediatamente."""
        mgr = _manager(tmp_path)
        mgr.init_fresh(["job_a"])
        mgr.mark_started("job_a")
        # Ler state file diretamente do disco
        with open(mgr.state_path, encoding="utf-8") as f:
            data = json.load(f)
        assert data["jobs"]["job_a"]["status"] == "in_progress"


# ---------------------------------------------------------------------------
# get_pending_jobs
# ---------------------------------------------------------------------------

class TestGetPendingJobs:
    def test_all_pending_initially(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["a", "b", "c"])
        assert mgr.get_pending_jobs(["a", "b", "c"]) == ["a", "b", "c"]

    def test_done_jobs_excluded(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["a", "b", "c"])
        mgr.mark_done("a", _done_result("a"))
        mgr.mark_done("b", _done_result("b"))
        pending = mgr.get_pending_jobs(["a", "b", "c"])
        assert pending == ["c"]

    def test_failed_jobs_included_in_pending(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["a", "b"])
        mgr.mark_failed("a", "erro")
        pending = mgr.get_pending_jobs(["a", "b"])
        assert "a" in pending

    def test_preserves_execution_order(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["a", "b", "c", "d"])
        mgr.mark_done("b", _done_result("b"))
        pending = mgr.get_pending_jobs(["a", "b", "c", "d"])
        assert pending == ["a", "c", "d"]

    def test_all_done_returns_empty(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["a", "b"])
        mgr.mark_done("a", _done_result("a"))
        mgr.mark_done("b", _done_result("b"))
        assert mgr.get_pending_jobs(["a", "b"]) == []


# ---------------------------------------------------------------------------
# load (resume)
# ---------------------------------------------------------------------------

class TestLoad:
    def test_load_existing_state(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["a", "b"])
        mgr.mark_done("a", _done_result("a"))

        # Novo manager carrega o mesmo state
        mgr2 = MigrationStateManager(tmp_path / "output")
        ok = mgr2.load()
        assert ok
        assert mgr2.get_job_status("a") == JobStatus.DONE
        assert mgr2.get_job_status("b") == JobStatus.PENDING

    def test_load_missing_file_returns_false(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        ok = mgr.load()
        assert not ok

    def test_load_corrupted_json_returns_false(self, tmp_path: Path) -> None:
        output = tmp_path / "output"
        output.mkdir()
        (output / ".sas2dbx_state.json").write_text("{ invalid json", encoding="utf-8")
        mgr = MigrationStateManager(output)
        ok = mgr.load()
        assert not ok
        # Estado deve ser vazio após falha
        assert mgr.get_all_statuses() == {}

    def test_resume_skips_done_jobs(self, tmp_path: Path) -> None:
        mgr = _manager(tmp_path)
        mgr.init_fresh(["a", "b", "c"])
        mgr.mark_done("a", _done_result("a"))
        mgr.mark_done("c", _done_result("c"))

        mgr2 = MigrationStateManager(tmp_path / "output")
        mgr2.load()
        pending = mgr2.get_pending_jobs(["a", "b", "c"])
        assert pending == ["b"]


# ---------------------------------------------------------------------------
# Engine integration — TranspilationEngine com state
# ---------------------------------------------------------------------------

class TestTranspilationEngine:
    def _make_sas_file(self, tmp_path: Path, name: str) -> object:
        from sas2dbx.models.sas_ast import SASFile
        p = tmp_path / f"{name}.sas"
        p.write_text(f"DATA {name}_out; SET inp; RUN;", encoding="utf-8")
        return SASFile(path=p, size_bytes=p.stat().st_size)

    def test_engine_run_creates_state_file(self, tmp_path: Path) -> None:
        from sas2dbx.transpile.engine import TranspilationEngine
        output = tmp_path / "out"
        files = [self._make_sas_file(tmp_path, "job_a"), self._make_sas_file(tmp_path, "job_b")]
        engine = TranspilationEngine(output_dir=output)
        engine.run(files, ["job_a", "job_b"])
        assert (output / ".sas2dbx_state.json").exists()

    def test_engine_all_jobs_done_after_run(self, tmp_path: Path) -> None:
        from sas2dbx.transpile.engine import TranspilationEngine
        output = tmp_path / "out"
        files = [self._make_sas_file(tmp_path, "job_a"), self._make_sas_file(tmp_path, "job_b")]
        engine = TranspilationEngine(output_dir=output)
        results = engine.run(files, ["job_a", "job_b"])
        assert all(r.status == JobStatus.DONE for r in results)

    def test_engine_resume_skips_done_jobs(self, tmp_path: Path) -> None:
        from sas2dbx.transpile.engine import TranspilationEngine
        output = tmp_path / "out"
        files = [self._make_sas_file(tmp_path, "job_a"), self._make_sas_file(tmp_path, "job_b")]

        # Primeira execução — marca job_a como done no state
        engine1 = TranspilationEngine(output_dir=output)
        engine1.run(files, ["job_a", "job_b"])

        # Patch do engine para detectar quais jobs foram reprocessados
        processed: list[str] = []
        engine2 = TranspilationEngine(output_dir=output, resume=True)
        original = engine2._transpile_job

        def _tracking(job_id, sas_file):
            processed.append(job_id)
            return original(job_id, sas_file)

        engine2._transpile_job = _tracking
        engine2.run(files, ["job_a", "job_b"])
        # Com resume, ambos já estão DONE — nenhum deve ser reprocessado
        assert processed == []

    def test_engine_failure_continues_remaining_jobs(self, tmp_path: Path) -> None:
        from sas2dbx.transpile.engine import TranspilationEngine
        output = tmp_path / "out"
        files = [
            self._make_sas_file(tmp_path, "job_a"),
            self._make_sas_file(tmp_path, "job_b"),
            self._make_sas_file(tmp_path, "job_c"),
        ]
        engine = TranspilationEngine(output_dir=output)

        # Força falha no job_b
        original = engine._transpile_job
        def _fail_b(job_id, sas_file):
            if job_id == "job_b":
                return MigrationResult(job_id=job_id, status=JobStatus.FAILED, error="forced")
            return original(job_id, sas_file)

        engine._transpile_job = _fail_b
        results = engine.run(files, ["job_a", "job_b", "job_c"])

        statuses = {r.job_id: r.status for r in results}
        assert statuses["job_a"] == JobStatus.DONE
        assert statuses["job_b"] == JobStatus.FAILED
        assert statuses["job_c"] == JobStatus.DONE
