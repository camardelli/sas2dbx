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
        # Violação de escopo escala para QUARANTINE (não descarta — vai para revisão humana)
        assert result.decision == "QUARANTINE"
        assert "escopo" in result.reason.lower()

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


class TestQualityGateSandboxIntegration:
    """Testes de integração reais — sem mocks de _run_tests ou _run_in_sandbox.

    Roda pytest via subprocess em um projeto mínimo em tmpdir.
    Garante que o comportamento real do sandbox está correto sob condições reais.
    """

    @pytest.fixture(autouse=True)
    def setup_project(self, tmp_path):
        """Cria estrutura mínima de projeto para execução real de pytest."""
        # Baseline: tests/ com um teste sempre-verdadeiro
        tests_dir = tmp_path / "tests"
        tests_dir.mkdir()
        (tests_dir / "__init__.py").write_text("")
        (tests_dir / "test_baseline.py").write_text(
            "def test_baseline_always_passes(): assert True\n"
        )

        # Arquivos dentro do escopo low/medium
        km_dir = tmp_path / "knowledge" / "mappings" / "generated"
        km_dir.mkdir(parents=True)
        (km_dir / "functions_map.yaml").write_text(
            "# SAS functions mapping\nCAT:\n  pyspark: concat\n  confidence: 0.9\n"
        )

        heal_dir = tmp_path / "sas2dbx" / "validate" / "heal"
        heal_dir.mkdir(parents=True)
        (heal_dir / "patterns.py").write_text(
            "# Error patterns\nPATTERNS: list = []\n"
        )
        (heal_dir / "fixer.py").write_text(
            "# Fixer\n"
        )

        self.project_root = tmp_path
        self.gate = QualityGate(project_root=tmp_path, test_timeout=60)

    def _proposal_append(self, content: str = "NEW_FN:\n  pyspark: new_fn\n") -> EvolutionProposal:
        return EvolutionProposal(
            fix_type="knowledge_store",
            risk_level="low",
            description="Adiciona mapeamento de função",
            files_to_modify=[
                FileModification(
                    path="knowledge/mappings/generated/functions_map.yaml",
                    action="append",
                    content=content,
                    reason="nova função SAS",
                )
            ],
            test=EvolutionTest(
                path="tests/evolve/test_fix_integration.py",
                content="def test_fix_integration(): assert True\n",
            ),
        )

    def _proposal_modify(self, old_string: str, new_content: str) -> EvolutionProposal:
        return EvolutionProposal(
            fix_type="knowledge_store",
            risk_level="low",
            description="Modifica mapeamento existente",
            files_to_modify=[
                FileModification(
                    path="knowledge/mappings/generated/functions_map.yaml",
                    action="modify",
                    content=new_content,
                    reason="atualizar mapeamento",
                    old_string=old_string,
                )
            ],
            test=EvolutionTest(
                path="tests/evolve/test_fix_modify.py",
                content="def test_fix_modify(): assert True\n",
            ),
        )

    def test_sandbox_action_add_real(self):
        """action='add' cria arquivo novo — sandbox roda pytest real e passa."""
        result = self.gate._run_in_sandbox(
            EvolutionProposal(
                fix_type="knowledge_store",
                risk_level="low",
                description="Adiciona novo arquivo",
                files_to_modify=[
                    FileModification(
                        path="knowledge/mappings/generated/new_map.yaml",
                        action="add",
                        content="NEW:\n  pyspark: new\n",
                        reason="novo mapeamento",
                    )
                ],
                test=EvolutionTest(
                    path="tests/evolve/test_new_map.py",
                    content="def test_new_map(): assert True\n",
                ),
            )
        )
        assert result["passed"], f"sandbox falhou: {result['output']}"

    def test_sandbox_action_append_real(self):
        """action='append' concatena conteúdo ao arquivo — sandbox passa."""
        proposal = self._proposal_append("APPEND_FN:\n  pyspark: append_fn\n")
        result = self.gate._run_in_sandbox(proposal)
        assert result["passed"], f"sandbox falhou: {result['output']}"

    def test_sandbox_action_modify_old_string_found_real(self):
        """action='modify' com old_string presente — arquivo modificado, sandbox passa."""
        proposal = self._proposal_modify(
            old_string="CAT:\n  pyspark: concat\n  confidence: 0.9\n",
            new_content="CAT:\n  pyspark: concat_ws\n  confidence: 0.95\n",
        )
        result = self.gate._run_in_sandbox(proposal)
        assert result["passed"], f"sandbox falhou: {result['output']}"

    def test_sandbox_action_modify_old_string_not_found_returns_failure(self):
        """action='modify' com old_string ausente → passed=False (bug fix crítico).

        Sem este fix, o sandbox continuava silenciosamente, o teste passava com
        'assert True', e o gate retornava APPROVE para um fix nunca aplicado.
        """
        proposal = self._proposal_modify(
            old_string="ESTA_STRING_NAO_EXISTE_NO_ARQUIVO",
            new_content="SUBSTITUIÇÃO_NUNCA_APLICADA",
        )
        result = self.gate._run_in_sandbox(proposal)
        assert not result["passed"], "sandbox deveria falhar quando old_string não encontrado"
        assert "old_string" in result["output"].lower() or "não encontrado" in result["output"]

    def test_sandbox_failing_test_returns_reject_real(self):
        """Teste com assert False no conteúdo → sandbox retorna passed=False."""
        proposal = EvolutionProposal(
            fix_type="knowledge_store",
            risk_level="low",
            description="Fix com teste quebrado",
            files_to_modify=[
                FileModification(
                    path="knowledge/mappings/generated/functions_map.yaml",
                    action="append",
                    content="BROKEN:\n  pyspark: broken\n",
                    reason="teste quebrado propositalmente",
                )
            ],
            test=EvolutionTest(
                path="tests/evolve/test_broken.py",
                content="def test_broken(): assert False, 'propositalmente falho'\n",
            ),
        )
        result = self.gate._run_in_sandbox(proposal)
        assert not result["passed"]
        assert "propositalmente falho" in result["output"] or "failed" in result["output"].lower()

    def test_evaluate_end_to_end_approve_real(self):
        """evaluate() completo sem mocks — deve retornar APPROVE para fix válido."""
        proposal = self._proposal_append("E2E_FN:\n  pyspark: e2e_fn\n  confidence: 0.9\n")
        result = self.gate.evaluate(proposal)
        assert result.decision == "APPROVE", (
            f"Esperava APPROVE, obteve {result.decision}: {result.reason}\n{result.test_output}"
        )
        assert result.approved
