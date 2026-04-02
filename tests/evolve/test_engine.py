"""Testes para EvolutionEngine — orquestrador central do Sprint 10."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sas2dbx.evolve.agent import EvolutionProposal, FileModification, EvolutionTest
from sas2dbx.evolve.applier import ApplyResult
from sas2dbx.evolve.engine import EvolutionEngine, EvolutionResult
from sas2dbx.evolve.gate import GateResult
from sas2dbx.evolve.health import HealthMonitor
from sas2dbx.evolve.unresolved import UnresolvedError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def project(tmp_path: Path) -> Path:
    (tmp_path / "sas2dbx_work" / "catalog").mkdir(parents=True)
    (tmp_path / "knowledge" / "mappings" / "generated").mkdir(parents=True)
    return tmp_path


@pytest.fixture()
def health(project: Path) -> HealthMonitor:
    return HealthMonitor(project / "sas2dbx_work" / "catalog" / "health_snapshots.json")


@pytest.fixture()
def engine(project: Path, health: HealthMonitor) -> EvolutionEngine:
    mock_llm = MagicMock()
    return EvolutionEngine(
        llm_client=mock_llm,
        project_root=project,
        health_monitor=health,
        test_timeout=30,
    )


def _make_error() -> UnresolvedError:
    return UnresolvedError(
        job_id="job_engine_test",
        migration_id="mig-001",
        notebook_path="/tmp/nb.py",
        sas_original="PROC SQL; SELECT PRXMATCH FROM t; QUIT;",
        pyspark_generated="spark.sql('SELECT PRXMATCH FROM t')",
        databricks_error="Undefined function: PRXMATCH",
        error_category="missing_function",
    )


def _valid_proposal(risk_level: str = "low") -> EvolutionProposal:
    return EvolutionProposal(
        fix_type="knowledge_store",
        risk_level=risk_level,
        description="Mapeia PRXMATCH → regexp_extract",
        files_to_modify=[
            FileModification(
                path="knowledge/mappings/generated/functions_map.yaml",
                action="append",
                content="PRXMATCH:\n  pyspark: regexp_extract\n",
                reason="função ausente no Spark",
            )
        ],
        test=EvolutionTest(
            path="tests/evolve/test_prxmatch.py",
            content="def test_prxmatch(): assert True\n",
        ),
    )


# ---------------------------------------------------------------------------
# TestEvolutionEngineNoProposal
# ---------------------------------------------------------------------------

class TestEvolutionEngineNoProposal:
    """Caminhos onde o LLM não gera proposta válida."""

    def test_no_proposal_returns_no_proposal_decision(self, engine: EvolutionEngine):
        error = _make_error()
        with patch.object(engine._analyzer, "analyze_sync", return_value=None):
            result = engine.process(error)

        assert result.proposal_generated is False
        assert result.gate_decision == "NO_PROPOSAL"
        assert result.applied is False

    def test_invalid_proposal_returns_no_proposal(self, engine: EvolutionEngine):
        error = _make_error()
        invalid = EvolutionProposal(
            fix_type="unknown_type",
            risk_level="low",
            description="bad",
            files_to_modify=[],
        )
        with patch.object(engine._analyzer, "analyze_sync", return_value=invalid):
            result = engine.process(error)

        assert result.gate_decision == "NO_PROPOSAL"

    def test_no_proposal_records_evolve_failure_in_health(self, engine: EvolutionEngine, health: HealthMonitor):
        error = _make_error()
        with patch.object(engine._analyzer, "analyze_sync", return_value=None):
            engine.process(error)

        # evolve_attempted=True, evolve_success=False deve ser registrado
        assert health._counters["evolve_fail"] == 1
        assert health._counters["evolve_ok"] == 0


# ---------------------------------------------------------------------------
# TestEvolutionEngineGateApprove
# ---------------------------------------------------------------------------

class TestEvolutionEngineGateApprove:
    """Caminho feliz: proposta gerada → gate APPROVE → fix aplicado."""

    def test_approved_fix_returns_applied_true(self, engine: EvolutionEngine, project: Path):
        error = _make_error()
        proposal = _valid_proposal("low")

        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("APPROVE", "ok")), \
             patch.object(engine._applier, "apply", return_value=ApplyResult(
                 proposal_description=proposal.description,
                 files_modified=["knowledge/mappings/generated/functions_map.yaml"],
                 status="APPLIED",
                 reason="ok",
             )):
            result = engine.process(error)

        assert result.applied is True
        assert result.gate_decision == "APPROVE"
        assert result.fix_type == "knowledge_store"
        assert result.proposal_generated is True

    def test_approved_fix_increments_ks_entry_counter(self, engine: EvolutionEngine, health: HealthMonitor):
        error = _make_error()
        proposal = _valid_proposal("low")

        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("APPROVE", "ok")), \
             patch.object(engine._applier, "apply", return_value=ApplyResult(
                 proposal_description="d", files_modified=[], status="APPLIED", reason="ok"
             )):
            engine.process(error)

        assert health._counters["ks_entries"] == 1

    def test_approved_error_pattern_increments_pattern_counter(self, engine: EvolutionEngine, health: HealthMonitor):
        error = _make_error()
        proposal = EvolutionProposal(
            fix_type="error_pattern",
            risk_level="low",
            description="Novo padrão",
            files_to_modify=[
                FileModification(
                    path="sas2dbx/validate/heal/patterns.py",
                    action="append",
                    content="# pattern",
                    reason="teste",
                )
            ],
            test=EvolutionTest(path="tests/evolve/test_p.py", content="def test_p(): assert True"),
        )

        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("APPROVE", "ok")), \
             patch.object(engine._applier, "apply", return_value=ApplyResult(
                 proposal_description="d", files_modified=[], status="APPLIED", reason="ok"
             )):
            engine.process(error)

        assert health._counters["patterns"] == 1

    def test_approve_notify_also_applies_fix(self, engine: EvolutionEngine):
        error = _make_error()
        proposal = _valid_proposal("medium")

        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("APPROVE_NOTIFY", "notified")), \
             patch.object(engine._applier, "apply", return_value=ApplyResult(
                 proposal_description="d", files_modified=[], status="APPLIED", reason="ok"
             )):
            result = engine.process(error)

        assert result.applied is True
        assert result.gate_decision == "APPROVE_NOTIFY"


# ---------------------------------------------------------------------------
# TestEvolutionEngineGateReject
# ---------------------------------------------------------------------------

class TestEvolutionEngineGateReject:
    """Caminho de rejeição: gate REJECT → applied=False."""

    def test_rejected_fix_not_applied(self, engine: EvolutionEngine):
        error = _make_error()
        proposal = _valid_proposal("low")

        apply_spy = MagicMock()
        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("REJECT", "sandbox falhou")), \
             patch.object(engine._applier, "apply", apply_spy):
            result = engine.process(error)

        apply_spy.assert_not_called()
        assert result.applied is False
        assert result.gate_decision == "REJECT"

    def test_applier_failed_returns_applied_false(self, engine: EvolutionEngine):
        """FixApplier retornando FAILED não é considerado aplicado."""
        error = _make_error()
        proposal = _valid_proposal("low")

        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("APPROVE", "ok")), \
             patch.object(engine._applier, "apply", return_value=ApplyResult(
                 proposal_description="d", files_modified=[], status="FAILED", reason="rollback"
             )):
            result = engine.process(error)

        assert result.applied is False


# ---------------------------------------------------------------------------
# TestEvolutionEngineQuarantine
# ---------------------------------------------------------------------------

class TestEvolutionEngineQuarantine:
    """Caminho de quarentena: gate QUARANTINE → fix armazenado para humano."""

    def test_quarantined_fix_not_applied(self, engine: EvolutionEngine):
        error = _make_error()
        proposal = _valid_proposal("high")

        apply_spy = MagicMock()
        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("QUARANTINE", "high risk")), \
             patch.object(engine._applier, "apply", apply_spy):
            result = engine.process(error)

        apply_spy.assert_not_called()
        assert result.applied is False
        assert result.gate_decision == "QUARANTINE"

    def test_quarantine_entry_id_populated(self, engine: EvolutionEngine):
        error = _make_error()
        proposal = _valid_proposal("high")

        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("QUARANTINE", "high risk")):
            result = engine.process(error)

        assert result.quarantine_entry_id is not None
        # Entrada deve estar visível no QuarantineStore
        pending = engine.quarantine.list_pending()
        assert any(e["entry_id"] == result.quarantine_entry_id for e in pending)

    def test_quarantine_count_increases_after_process(self, engine: EvolutionEngine):
        error = _make_error()
        proposal = _valid_proposal("high")

        initial_count = engine.quarantine.count_pending()
        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("QUARANTINE", "high risk")):
            engine.process(error)

        assert engine.quarantine.count_pending() == initial_count + 1


# ---------------------------------------------------------------------------
# TestEvolutionEngineUnresolvedPersistence
# ---------------------------------------------------------------------------

class TestEvolutionEngineUnresolvedPersistence:
    """O UnresolvedError deve ser salvo em disco independente do resultado."""

    def test_unresolved_saved_to_disk_on_no_proposal(self, engine: EvolutionEngine, project: Path):
        error = _make_error()
        with patch.object(engine._analyzer, "analyze_sync", return_value=None):
            engine.process(error)

        unresolved_dir = project / "sas2dbx_work" / "catalog" / "unresolved"
        saved_files = list(unresolved_dir.glob("*.json"))
        assert len(saved_files) >= 1

    def test_unresolved_saved_to_disk_on_approve(self, engine: EvolutionEngine, project: Path):
        error = _make_error()
        proposal = _valid_proposal("low")

        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("APPROVE", "ok")), \
             patch.object(engine._applier, "apply", return_value=ApplyResult(
                 proposal_description="d", files_modified=[], status="APPLIED", reason="ok"
             )):
            engine.process(error)

        unresolved_dir = project / "sas2dbx_work" / "catalog" / "unresolved"
        assert list(unresolved_dir.glob("*.json"))


# ---------------------------------------------------------------------------
# TestEvolutionEngineNotifyJobResult
# ---------------------------------------------------------------------------

class TestEvolutionEngineNotifyJobResult:
    """notify_job_result delega ao FixApplier e registra rollback no health."""

    def test_notify_success_returns_true(self, engine: EvolutionEngine):
        assert engine.notify_job_result(True) is True

    def test_notify_triggers_rollback_on_worsened_failure_rate(self, engine: EvolutionEngine, health: HealthMonitor):
        # Simula um fix APPLIED antes
        error = _make_error()
        proposal = _valid_proposal("low")

        with patch.object(engine._analyzer, "analyze_sync", return_value=proposal), \
             patch.object(engine._gate, "evaluate", return_value=GateResult("APPROVE", "ok")), \
             patch.object(engine._applier, "apply", return_value=ApplyResult(
                 proposal_description="d", files_modified=[], status="APPLIED", reason="ok"
             )):
            engine.process(error)

        # Força taxa de falha alta após o fix
        engine._applier._pre_fix_failure_rate = 0.0
        for _ in range(9):
            engine._applier.record_job_result(False)
        should_continue = engine.notify_job_result(False)  # fecha a janela com rollback

        assert should_continue is False
        assert health._counters["fixes_reverted"] == 1


# ---------------------------------------------------------------------------
# TestEvolutionEngineProperties
# ---------------------------------------------------------------------------

class TestEvolutionEngineProperties:
    def test_quarantine_property_returns_store(self, engine: EvolutionEngine):
        from sas2dbx.evolve.quarantine import QuarantineStore
        assert isinstance(engine.quarantine, QuarantineStore)

    def test_health_property_returns_monitor(self, engine: EvolutionEngine, health: HealthMonitor):
        assert engine.health is health
