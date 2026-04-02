"""Testes para QualityGate — validação em sandbox."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sas2dbx.evolve.agent import EvolutionProposal, FileModification, EvolutionTest
from sas2dbx.evolve.gate import QualityGate, GateResult


def _make_proposal(
    fix_type: str = "knowledge_store",
    risk_level: str = "low",
    files: list[FileModification] | None = None,
    test: EvolutionTest | None = None,
) -> EvolutionProposal:
    if files is None:
        files = [
            FileModification(
                path="knowledge/mappings/generated/functions_map.yaml",
                action="append",
                content="TEST_FN:\n  pyspark: regexp_extract\n  confidence: 0.9",
                reason="Test fix",
            )
        ]
    if test is None:
        test = EvolutionTest(
            path="tests/evolve/test_fix_gate.py",
            content="def test_fix(): assert True",
        )
    return EvolutionProposal(
        fix_type=fix_type,
        risk_level=risk_level,
        description="Test proposal",
        files_to_modify=files,
        test=test,
    )


class TestQualityGateDecisions:
    def setup_method(self):
        self.gate = QualityGate(project_root=Path("/tmp"), test_timeout=30)

    def test_reject_without_test(self):
        proposal = _make_proposal(test=None)
        result = self.gate.evaluate(proposal)
        assert result.decision == "REJECT"
        assert result.decision == "REJECT"
        assert "sem teste" in result.reason.lower() or "test" in result.reason.lower()

    def test_reject_invalid_proposal(self):
        proposal = EvolutionProposal(
            fix_type="unknown_type",
            risk_level="low",
            description="bad",
            files_to_modify=[],
        )
        result = self.gate.evaluate(proposal)
        assert result.decision == "REJECT"

    def test_quarantine_high_risk(self):
        """Fix de alto risco vai para quarentena sem rodar sandbox."""
        proposal = _make_proposal(
            fix_type="engine_rule",
            risk_level="high",
            files=[
                FileModification(
                    path="sas2dbx/transpile/engine.py",
                    action="modify",
                    content="# new code",
                    reason="engine fix",
                )
            ],
        )
        result = self.gate.evaluate(proposal)
        assert result.decision == "QUARANTINE"
        assert not result.approved

    def test_reject_out_of_scope_file(self):
        """Arquivo fora do escopo para o risk_level deve ser rejeitado."""
        proposal = _make_proposal(
            fix_type="knowledge_store",
            risk_level="low",
            files=[
                FileModification(
                    path="sas2dbx/transpile/engine.py",  # fora do escopo para low
                    action="modify",
                    content="# hack",
                    reason="scope violation",
                )
            ],
        )
        result = self.gate.evaluate(proposal)
        assert result.decision == "REJECT"
        assert "fora do escopo" in result.reason.lower()

    def test_approve_when_tests_pass(self):
        """Simula gate aprovando quando pytest retorna 0."""
        proposal = _make_proposal(risk_level="low")

        with patch.object(
            self.gate, "_run_tests", return_value={"passed": True, "output": "1 passed"}
        ):
            with patch.object(
                self.gate,
                "_run_in_sandbox",
                return_value={"passed": True, "output": "2 passed"},
            ):
                result = self.gate.evaluate(proposal)
        assert result.decision == "APPROVE"
        assert result.approved

    def test_approve_notify_for_medium_risk(self):
        """Risk level medium → APPROVE_NOTIFY."""
        proposal = _make_proposal(
            fix_type="prompt_refinement",
            risk_level="medium",
            files=[
                FileModification(
                    path="sas2dbx/transpile/llm/prompts.py",
                    action="append",
                    content="# nova regra",
                    reason="refinamento",
                )
            ],
        )

        with patch.object(
            self.gate, "_run_tests", return_value={"passed": True, "output": "1 passed"}
        ):
            with patch.object(
                self.gate,
                "_run_in_sandbox",
                return_value={"passed": True, "output": "2 passed"},
            ):
                result = self.gate.evaluate(proposal)
        assert result.decision == "APPROVE_NOTIFY"
        assert result.approved

    def test_reject_when_existing_tests_fail(self):
        """Se os testes existentes falham antes do fix, rejeita."""
        proposal = _make_proposal()

        with patch.object(
            self.gate,
            "_run_tests",
            return_value={"passed": False, "output": "3 failed"},
        ):
            result = self.gate.evaluate(proposal)
        assert result.decision == "REJECT"
        assert "instável" in result.reason.lower()

    def test_reject_when_sandbox_tests_fail(self):
        """Se sandbox falha, rejeita mesmo que testes existentes passem."""
        proposal = _make_proposal()

        with patch.object(
            self.gate, "_run_tests", return_value={"passed": True, "output": "ok"}
        ):
            with patch.object(
                self.gate,
                "_run_in_sandbox",
                return_value={"passed": False, "output": "1 failed"},
            ):
                result = self.gate.evaluate(proposal)
        assert result.decision == "REJECT"

    def test_gate_result_approved_property(self):
        r_approve = GateResult("APPROVE", "ok")
        r_notify = GateResult("APPROVE_NOTIFY", "ok")
        r_quarantine = GateResult("QUARANTINE", "high risk")
        r_reject = GateResult("REJECT", "fail")

        assert r_approve.approved
        assert r_notify.approved
        assert not r_quarantine.approved
        assert not r_reject.approved
