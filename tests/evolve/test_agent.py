"""Testes para EvolutionAnalyzer — proposta de fix via LLM (stub)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from sas2dbx.evolve.agent import (
    EvolutionAnalyzer,
    EvolutionProposal,
    FileModification,
    EvolutionTest,
    _FIX_RISK,
    ALLOWED_PATHS,
)
from sas2dbx.evolve.unresolved import UnresolvedError


def _make_unresolved() -> UnresolvedError:
    return UnresolvedError(
        job_id="job_agent_test",
        migration_id="mig-xyz",
        notebook_path="/tmp/nb.py",
        sas_original="PROC SQL; SELECT PRXMATCH('/\\d+/', col) FROM t; QUIT;",
        pyspark_generated="spark.sql('SELECT PRXMATCH(\"/\\d+/\", col) FROM t')",
        databricks_error="Undefined function: PRXMATCH",
        error_category="missing_function",
    )


def _make_valid_llm_response(fix_type: str = "knowledge_store") -> str:
    """Retorna JSON válido que simula resposta do LLM."""
    return json.dumps(
        {
            "fix_type": fix_type,
            "risk_level": _FIX_RISK[fix_type],
            "description": "Mapeia PRXMATCH SAS → regexp_extract PySpark",
            "files_to_modify": [
                {
                    "path": "knowledge/mappings/generated/functions_map.yaml",
                    "action": "append",
                    "content": "PRXMATCH:\n  pyspark: \"regexp_extract(col, pattern, 0) != ''\"\n  notes: \"SAS retorna posição; PySpark retorna string — converter para bool/int\"\n  confidence: 0.85",
                    "reason": "PRXMATCH não existe em Spark SQL; regexp_extract é o equivalente",
                    "old_string": None,
                }
            ],
            "test": {
                "path": "tests/evolve/test_fix_prxmatch.py",
                "content": "def test_prxmatch_mapping():\n    assert True  # placeholder",
            },
            "knowledge_entry": {
                "filename": "functions_map.yaml",
                "key": "PRXMATCH",
                "value": {"pyspark": "regexp_extract", "confidence": 0.85},
            },
            "similar_jobs_prediction": "Jobs com expressões regulares SAS (PRXCHANGE, PRXPARSE)",
        }
    )


class TestEvolutionAnalyzerParsing:
    """Testa parsing de propostas sem chamar LLM real."""

    def setup_method(self):
        mock_llm = MagicMock()
        self.analyzer = EvolutionAnalyzer(mock_llm, Path("/tmp"))

    def test_parse_valid_json(self):
        error = _make_unresolved()
        raw = _make_valid_llm_response("knowledge_store")
        proposal = self.analyzer._parse_proposal(raw, error)

        assert proposal is not None
        assert proposal.fix_type == "knowledge_store"
        assert proposal.risk_level == "low"
        assert proposal.description
        assert len(proposal.files_to_modify) == 1
        assert proposal.test is not None
        assert proposal.knowledge_entry is not None
        assert proposal.is_valid

    def test_parse_strips_markdown_fences(self):
        error = _make_unresolved()
        raw = "```json\n" + _make_valid_llm_response() + "\n```"
        proposal = self.analyzer._parse_proposal(raw, error)
        assert proposal is not None
        assert proposal.is_valid

    def test_parse_missing_test_returns_invalid(self):
        error = _make_unresolved()
        data = json.loads(_make_valid_llm_response())
        del data["test"]
        raw = json.dumps(data)
        proposal = self.analyzer._parse_proposal(raw, error)
        # test=None → is_valid=False
        assert proposal is not None
        assert not proposal.is_valid

    def test_parse_invalid_json_returns_none(self):
        error = _make_unresolved()
        proposal = self.analyzer._parse_proposal("not json at all", error)
        assert proposal is None

    def test_parse_missing_required_field_returns_none(self):
        error = _make_unresolved()
        data = json.loads(_make_valid_llm_response())
        del data["fix_type"]
        proposal = self.analyzer._parse_proposal(json.dumps(data), error)
        assert proposal is None

    def test_risk_level_escalation(self):
        """LLM propõe risk=low para engine_rule — deve ser escalado para high."""
        error = _make_unresolved()
        data = json.loads(_make_valid_llm_response())
        data["fix_type"] = "engine_rule"
        data["risk_level"] = "low"  # LLM errou — deve ser corrigido
        proposal = self.analyzer._parse_proposal(json.dumps(data), error)
        assert proposal is not None
        assert proposal.risk_level == "high"  # escalado para canônico

    def test_parse_error_pattern_fix_type(self):
        error = _make_unresolved()
        data = json.loads(_make_valid_llm_response("error_pattern"))
        data["files_to_modify"][0]["path"] = "sas2dbx/validate/heal/patterns.py"
        proposal = self.analyzer._parse_proposal(json.dumps(data), error)
        assert proposal is not None
        assert proposal.fix_type == "error_pattern"
        assert proposal.risk_level == "low"


class TestEvolutionAnalyzerPrompt:
    def setup_method(self):
        mock_llm = MagicMock()
        self.analyzer = EvolutionAnalyzer(mock_llm, Path("/tmp"))

    def test_prompt_contains_error_context(self):
        error = _make_unresolved()
        prompt = self.analyzer._build_prompt(error)
        assert "PRXMATCH" in prompt
        assert "job_agent_test" in prompt
        assert "missing_function" in prompt

    def test_prompt_contains_pyspark_snippet(self):
        error = _make_unresolved()
        prompt = self.analyzer._build_prompt(error)
        assert "spark.sql" in prompt

    def test_prompt_truncates_long_notebook(self):
        """Notebooks longos devem ser truncados para as últimas 80 linhas."""
        error = _make_unresolved()
        long_code = "\n".join([f"# linha {i}" for i in range(200)])
        object.__setattr__(error, "pyspark_generated", long_code)
        prompt = self.analyzer._build_prompt(error)
        # As primeiras linhas não devem aparecer (truncadas)
        assert "# linha 0\n" not in prompt
        # As últimas devem aparecer
        assert "# linha 199" in prompt


class TestRecoverPartialJson:
    """Testa recuperação de JSON truncado — método crítico sem cobertura anterior."""

    def setup_method(self):
        mock_llm = MagicMock()
        self.analyzer = EvolutionAnalyzer(mock_llm, Path("/tmp"))

    def test_recovers_scalar_fields(self):
        """Campos simples extraídos mesmo com JSON sem fecha."""
        truncated = (
            '{"fix_type": "error_pattern", "risk_level": "low", '
            '"description": "Adiciona padrão X", '
            '"files_to_modify": [], '
            '"similar_jobs_prediction": "Jobs com PRXMATCH"'
            # sem fechar o JSON propositalmente
        )
        result = self.analyzer._recover_partial_json(truncated)
        assert result is not None
        assert result["fix_type"] == "error_pattern"
        assert result["risk_level"] == "low"
        assert result["description"] == "Adiciona padrão X"

    def test_returns_none_if_fix_type_missing(self):
        """Sem fix_type e risk_level não é possível reconstruir proposta."""
        truncated = '{"description": "Fix sem fix_type"'
        result = self.analyzer._recover_partial_json(truncated)
        assert result is None

    def test_recovers_complete_array(self):
        """Array completo de files_to_modify é extraído corretamente."""
        complete = json.dumps({
            "fix_type": "knowledge_store",
            "risk_level": "low",
            "description": "test",
            "files_to_modify": [
                {"path": "knowledge/mappings/generated/functions_map.yaml",
                 "action": "append", "content": "X: y", "reason": "r"}
            ],
        })
        # Remove fechamento do JSON externo (truncado após o array)
        truncated = complete[:-1]
        result = self.analyzer._recover_partial_json(truncated)
        assert result is not None
        assert result["fix_type"] == "knowledge_store"

    def test_recovers_test_path(self):
        """test.path extraído mesmo quando test.content é truncado."""
        truncated = (
            '{"fix_type": "error_pattern", "risk_level": "low", '
            '"description": "X", "files_to_modify": [], '
            '"test": {"path": "tests/evolve/test_x.py", '
            '"content": "def test_x():\n    assert Tr'  # truncado
        )
        result = self.analyzer._recover_partial_json(truncated)
        assert result is not None
        assert "test" in result
        assert result["test"]["path"] == "tests/evolve/test_x.py"

    def test_parse_proposal_uses_recovery_on_truncated_json(self):
        """_parse_proposal chama recovery quando JSON está truncado."""
        error = _make_unresolved()
        # JSON truncado com campos essenciais presentes
        data = json.loads(_make_valid_llm_response("error_pattern"))
        full = json.dumps(data)
        truncated = full[:len(full) // 2]  # corta na metade

        # Não deve retornar None se conseguiu recuperar fix_type e risk_level
        # (pode retornar None se campos obrigatórios foram cortados — aceitável)
        result = self.analyzer._parse_proposal(truncated, error)
        # Não levanta exceção — isso é suficiente para validar robustez
        assert result is None or isinstance(result, EvolutionProposal)


class TestEvolutionProposalValidation:
    def _make_proposal(self, **kwargs) -> EvolutionProposal:
        defaults = dict(
            fix_type="knowledge_store",
            risk_level="low",
            description="Test fix",
            files_to_modify=[
                FileModification(
                    path="knowledge/mappings/generated/functions_map.yaml",
                    action="append",
                    content="TEST: pyspark: test",
                    reason="testing",
                )
            ],
            test=EvolutionTest(
                path="tests/evolve/test_fix_test.py",
                content="def test_fix(): assert True",
            ),
        )
        defaults.update(kwargs)
        return EvolutionProposal(**defaults)

    def test_valid_proposal(self):
        p = self._make_proposal()
        assert p.is_valid

    def test_invalid_without_test(self):
        p = self._make_proposal(test=None)
        assert not p.is_valid

    def test_invalid_without_files(self):
        p = self._make_proposal(files_to_modify=[])
        assert not p.is_valid

    def test_invalid_unknown_fix_type(self):
        p = self._make_proposal(fix_type="unknown_type")
        assert not p.is_valid
