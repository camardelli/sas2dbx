"""Testes para HealthMonitor e PipelineHealth."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sas2dbx.evolve.health import HealthMonitor, PipelineHealth, HealthAction


def _make_monitor(tmp_path: Path) -> HealthMonitor:
    return HealthMonitor(tmp_path / "health.json", checkpoint_interval=10)


class TestPipelineHealth:
    def test_health_score_perfect(self):
        h = PipelineHealth(
            timestamp="2026-01-01T00:00:00Z",
            jobs_processed=100,
            jobs_total=1000,
            first_attempt_success_rate=1.0,
            healing_success_rate=1.0,
            evolution_success_rate=1.0,
            code_fixes_applied=0,
            code_fixes_reverted=0,
        )
        assert h.health_score == 100.0

    def test_health_score_zero(self):
        h = PipelineHealth(
            timestamp="2026-01-01T00:00:00Z",
            jobs_processed=10,
            jobs_total=1000,
            first_attempt_success_rate=0.0,
            healing_success_rate=0.0,
            evolution_success_rate=0.0,
            code_fixes_applied=5,
            code_fixes_reverted=5,
        )
        assert h.health_score == 0.0

    def test_health_score_mid(self):
        h = PipelineHealth(
            timestamp="2026-01-01T00:00:00Z",
            jobs_processed=50,
            jobs_total=100,
            first_attempt_success_rate=0.80,
            healing_success_rate=0.60,
            evolution_success_rate=0.50,
            code_fixes_applied=2,
            code_fixes_reverted=0,
        )
        score = h.health_score
        assert 60 < score < 90

    def test_progress_pct(self):
        h = PipelineHealth(
            timestamp="",
            jobs_processed=250,
            jobs_total=1000,
        )
        assert h.progress_pct == 25.0

    def test_to_dict_has_health_score(self):
        h = PipelineHealth(
            timestamp="2026-01-01T00:00:00Z",
            jobs_processed=10,
            jobs_total=100,
            first_attempt_success_rate=0.9,
        )
        d = h.to_dict()
        assert "health_score" in d
        assert "progress_pct" in d


class TestHealthMonitor:
    def test_continue_on_healthy_pipeline(self, tmp_path):
        monitor = _make_monitor(tmp_path)
        for _ in range(90):
            monitor.record_job(success=True)

        action = monitor.checkpoint(90, 1000)
        assert action.action == "CONTINUE"

    def test_warning_on_low_success_rate(self, tmp_path):
        monitor = _make_monitor(tmp_path)
        # 65% de sucesso — abaixo do WARNING_SUCCESS_RATE=70%
        for i in range(100):
            monitor.record_job(success=(i % 3 != 0))  # ~67%

        action = monitor.checkpoint(100, 1000)
        assert action.action in ("WARNING", "PAUSE_PIPELINE")

    def test_pause_on_critical_success_rate(self, tmp_path):
        monitor = _make_monitor(tmp_path)
        # 40% — abaixo do CRITICAL_SUCCESS_RATE=50%
        for i in range(100):
            monitor.record_job(success=(i % 10 < 4))

        action = monitor.checkpoint(100, 1000)
        assert action.should_pause

    def test_pause_on_declining_health_score(self, tmp_path):
        monitor = _make_monitor(tmp_path)

        # Simula 3 checkpoints com health score em queda
        # Checkpoint 1: 90% sucesso
        for _ in range(50):
            monitor.record_job(success=True)
        monitor.checkpoint(50, 1000)

        # Checkpoint 2: 70% sucesso
        for i in range(50):
            monitor.record_job(success=(i % 10 < 7))
        monitor.checkpoint(100, 1000)

        # Checkpoint 3: 45% sucesso — queda > 10pts dispara pausa
        for i in range(50):
            monitor.record_job(success=(i % 10 < 4))
        action = monitor.checkpoint(150, 1000)
        # Deve pausar (critical) ou warning dependendo da queda
        assert action.action in ("PAUSE_PIPELINE", "WARNING")

    def test_pause_on_high_revert_rate(self, tmp_path):
        monitor = _make_monitor(tmp_path)
        # 3 fixes aplicados, 2 revertidos = 67% > 20%
        monitor.record_fix_applied()
        monitor.record_fix_applied()
        monitor.record_fix_applied()
        monitor.record_fix_reverted()
        monitor.record_fix_reverted()
        for _ in range(50):
            monitor.record_job(success=True)

        action = monitor.checkpoint(50, 1000)
        assert action.should_pause

    def test_persist_and_reload(self, tmp_path):
        monitor = _make_monitor(tmp_path)
        for _ in range(30):
            monitor.record_job(success=True)
        monitor.checkpoint(30, 1000)

        # Recarrega
        monitor2 = _make_monitor(tmp_path)
        assert len(monitor2.all_snapshots()) == 1
        assert monitor2.all_snapshots()[0]["jobs_processed"] == 30

    def test_latest_returns_last_snapshot(self, tmp_path):
        monitor = _make_monitor(tmp_path)
        for _ in range(20):
            monitor.record_job(success=True)
        monitor.checkpoint(20, 100)
        monitor.checkpoint(20, 100)  # segundo checkpoint

        latest = monitor.latest()
        assert latest is not None
        assert latest.jobs_processed == 20

    def test_record_ks_entry_increments_counter(self, tmp_path):
        monitor = _make_monitor(tmp_path)
        monitor.record_ks_entry()
        monitor.record_ks_entry()
        for _ in range(10):
            monitor.record_job(success=True)
        monitor.checkpoint(10, 100)
        latest = monitor.latest()
        assert latest.knowledge_entries_added == 2
