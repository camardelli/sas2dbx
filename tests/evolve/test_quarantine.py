"""Testes para QuarantineStore."""

from __future__ import annotations

from pathlib import Path

import pytest

from sas2dbx.evolve.agent import EvolutionProposal, FileModification, EvolutionTest
from sas2dbx.evolve.quarantine import QuarantineStore


def _make_high_risk_proposal() -> EvolutionProposal:
    return EvolutionProposal(
        fix_type="engine_rule",
        risk_level="high",
        description="Alterar engine.py para novo construto",
        files_to_modify=[
            FileModification(
                path="sas2dbx/transpile/engine.py",
                action="modify",
                content="# novo código",
                reason="teste quarentena",
                old_string="# ponto antigo",
            )
        ],
        test=EvolutionTest(
            path="tests/evolve/test_fix_engine.py",
            content="def test_new_rule(): assert True",
        ),
    )


class TestQuarantineStore:
    def setup_method(self, method):
        pass

    def test_submit_creates_pending_entry(self, tmp_path):
        store = QuarantineStore(tmp_path / "quarantine.json")
        entry_id = store.submit("job_001", _make_high_risk_proposal())

        assert entry_id
        pending = store.list_pending()
        assert len(pending) == 1
        assert pending[0]["entry_id"] == entry_id
        assert pending[0]["job_id"] == "job_001"
        assert pending[0]["status"] == "PENDING"

    def test_approve_returns_proposal(self, tmp_path):
        store = QuarantineStore(tmp_path / "quarantine.json")
        entry_id = store.submit("job_001", _make_high_risk_proposal())

        proposal = store.approve(entry_id, note="Revisado e aprovado")
        assert proposal is not None
        assert proposal.fix_type == "engine_rule"
        assert proposal.risk_level == "high"

        # Não deve mais aparecer como pending
        assert store.count_pending() == 0

    def test_reject_removes_from_pending(self, tmp_path):
        store = QuarantineStore(tmp_path / "quarantine.json")
        entry_id = store.submit("job_002", _make_high_risk_proposal())
        result = store.reject(entry_id, note="Fix muito invasivo")

        assert result
        assert store.count_pending() == 0

    def test_approve_nonexistent_returns_none(self, tmp_path):
        store = QuarantineStore(tmp_path / "quarantine.json")
        assert store.approve("nonexistent-id") is None

    def test_persist_and_reload(self, tmp_path):
        path = tmp_path / "quarantine.json"
        store = QuarantineStore(path)
        store.submit("job_persist", _make_high_risk_proposal())

        # Recarrega
        store2 = QuarantineStore(path)
        assert store2.count_pending() == 1
        assert store2.list_pending()[0]["job_id"] == "job_persist"

    def test_multiple_entries(self, tmp_path):
        store = QuarantineStore(tmp_path / "quarantine.json")
        id1 = store.submit("job_a", _make_high_risk_proposal())
        id2 = store.submit("job_b", _make_high_risk_proposal())

        assert store.count_pending() == 2
        store.reject(id1, "rejeitado")
        assert store.count_pending() == 1
        assert store.list_pending()[0]["entry_id"] == id2

    def test_proposal_summary_contains_key_info(self, tmp_path):
        store = QuarantineStore(tmp_path / "quarantine.json")
        store.submit("job_summary", _make_high_risk_proposal())

        pending = store.list_pending()
        summary = pending[0]["proposal_summary"]
        assert summary["fix_type"] == "engine_rule"
        assert summary["risk_level"] == "high"
        assert "sas2dbx/transpile/engine.py" in summary["files"]
