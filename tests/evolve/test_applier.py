"""Testes para FixApplier — apply, rollback, hot-reload e monitoramento pós-fix."""

from __future__ import annotations

from pathlib import Path

import pytest

from sas2dbx.evolve.agent import EvolutionProposal, FileModification, EvolutionTest
from sas2dbx.evolve.applier import FixApplier, ApplyResult


def _make_proposal(
    path: str = "knowledge/mappings/generated/functions_map.yaml",
    action: str = "append",
    content: str = "NEW_FN:\n  pyspark: new\n",
    old_string: str | None = None,
) -> EvolutionProposal:
    return EvolutionProposal(
        fix_type="knowledge_store",
        risk_level="low",
        description="Fix de teste",
        files_to_modify=[
            FileModification(path=path, action=action, content=content, reason="teste", old_string=old_string)
        ],
        test=EvolutionTest(
            path="tests/evolve/test_fix_applier_gen.py",
            content="def test_gen(): assert True\n",
        ),
    )


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    """Projeto mínimo com arquivo de mapeamento pré-existente."""
    km = tmp_path / "knowledge" / "mappings" / "generated"
    km.mkdir(parents=True)
    (km / "functions_map.yaml").write_text(
        "CAT:\n  pyspark: concat\n  confidence: 0.9\n", encoding="utf-8"
    )
    (tmp_path / "tests" / "evolve").mkdir(parents=True)
    return tmp_path


@pytest.fixture()
def applier(project: Path) -> FixApplier:
    return FixApplier(project_root=project)


class TestFixApplierActionAdd:
    def test_add_creates_new_file(self, project: Path, applier: FixApplier):
        proposal = _make_proposal(
            path="knowledge/mappings/generated/new_map.yaml",
            action="add",
            content="X:\n  pyspark: y\n",
        )
        result = applier.apply(proposal)
        assert result.status == "APPLIED"
        assert (project / "knowledge/mappings/generated/new_map.yaml").exists()

    def test_add_overwrites_existing_file(self, project: Path, applier: FixApplier):
        target = project / "knowledge/mappings/generated/functions_map.yaml"
        proposal = _make_proposal(
            path="knowledge/mappings/generated/functions_map.yaml",
            action="add",
            content="REPLACED:\n  pyspark: replaced\n",
        )
        result = applier.apply(proposal)
        assert result.status == "APPLIED"
        assert "REPLACED" in target.read_text(encoding="utf-8")


class TestFixApplierActionAppend:
    def test_append_adds_to_existing(self, project: Path, applier: FixApplier):
        target = project / "knowledge/mappings/generated/functions_map.yaml"
        proposal = _make_proposal(action="append", content="APPENDED:\n  pyspark: app\n")
        result = applier.apply(proposal)
        assert result.status == "APPLIED"
        content = target.read_text(encoding="utf-8")
        assert "CAT:" in content
        assert "APPENDED:" in content

    def test_append_to_nonexistent_creates_file(self, project: Path, applier: FixApplier):
        proposal = _make_proposal(
            path="knowledge/mappings/generated/nonexistent.yaml",
            action="append",
            content="NEW:\n  pyspark: new\n",
        )
        result = applier.apply(proposal)
        assert result.status == "APPLIED"
        assert (project / "knowledge/mappings/generated/nonexistent.yaml").exists()


class TestFixApplierActionModify:
    def test_modify_replaces_old_string(self, project: Path, applier: FixApplier):
        target = project / "knowledge/mappings/generated/functions_map.yaml"
        proposal = _make_proposal(
            action="modify",
            content="CAT:\n  pyspark: concat_ws\n  confidence: 0.95\n",
            old_string="CAT:\n  pyspark: concat\n  confidence: 0.9\n",
        )
        result = applier.apply(proposal)
        assert result.status == "APPLIED"
        assert "concat_ws" in target.read_text(encoding="utf-8")
        assert "concat\n" not in target.read_text(encoding="utf-8")

    def test_modify_old_string_not_found_triggers_rollback(self, project: Path, applier: FixApplier):
        """Quando old_string não existe, apply deve retornar FAILED e fazer rollback."""
        original = (project / "knowledge/mappings/generated/functions_map.yaml").read_text()
        proposal = _make_proposal(
            action="modify",
            content="NOVO_CONTEUDO",
            old_string="ESSA_STRING_NAO_EXISTE",
        )
        result = applier.apply(proposal)
        assert result.status == "FAILED"
        assert "old_string" in result.reason.lower() or "rollback" in result.reason.lower() or "não encontrado" in result.reason.lower()
        # Arquivo original intacto após rollback
        assert (project / "knowledge/mappings/generated/functions_map.yaml").read_text() == original

    def test_modify_without_old_string_overwrites(self, project: Path, applier: FixApplier):
        """action=modify sem old_string → sobrescreve o arquivo inteiro."""
        target = project / "knowledge/mappings/generated/functions_map.yaml"
        proposal = _make_proposal(
            action="modify",
            content="OVERWRITTEN:\n  pyspark: overwritten\n",
            old_string=None,
        )
        result = applier.apply(proposal)
        assert result.status == "APPLIED"
        assert "OVERWRITTEN" in target.read_text(encoding="utf-8")


class TestFixApplierTest:
    def test_apply_writes_test_file(self, project: Path, applier: FixApplier):
        proposal = _make_proposal()
        result = applier.apply(proposal)
        assert result.status == "APPLIED"
        assert (project / "tests/evolve/test_fix_applier_gen.py").exists()

    def test_files_modified_includes_test(self, project: Path, applier: FixApplier):
        proposal = _make_proposal()
        result = applier.apply(proposal)
        assert any("test_fix_applier_gen" in f for f in result.files_modified)


class TestFixApplierRollback:
    def test_rollback_last_restores_file(self, project: Path, applier: FixApplier):
        target = project / "knowledge/mappings/generated/functions_map.yaml"
        original = target.read_text(encoding="utf-8")

        proposal = _make_proposal(action="append", content="TEMP_APPEND\n")
        applier.apply(proposal)
        assert "TEMP_APPEND" in target.read_text(encoding="utf-8")

        rolled_back = applier.rollback_last()
        assert rolled_back
        assert target.read_text(encoding="utf-8") == original

    def test_rollback_removes_added_file(self, project: Path, applier: FixApplier):
        new_file = project / "knowledge/mappings/generated/temp_add.yaml"
        proposal = _make_proposal(
            path="knowledge/mappings/generated/temp_add.yaml",
            action="add",
            content="TEMP: true\n",
        )
        applier.apply(proposal)
        assert new_file.exists()

        applier.rollback_last()
        assert not new_file.exists()


class TestFixApplierMonitoring:
    def test_monitoring_no_rollback_when_improvement(self, project: Path, applier: FixApplier):
        proposal = _make_proposal(action="append", content="MON_APPEND\n")
        applier.apply(proposal, pre_fix_failure_rate=0.5)

        # 10 jobs bem-sucedidos → melhora em relação aos 50% anteriores
        for _ in range(10):
            assert applier.record_job_result(True)

    def test_monitoring_rollback_when_failure_rate_worsens(self, project: Path, applier: FixApplier):
        target = project / "knowledge/mappings/generated/functions_map.yaml"
        original = target.read_text(encoding="utf-8")

        proposal = _make_proposal(action="append", content="BAD_APPEND\n")
        applier.apply(proposal, pre_fix_failure_rate=0.0)

        # 9 falhas em 10 jobs → 90% falha vs 0% antes → rollback
        for i in range(9):
            applier.record_job_result(False)
        result = applier.record_job_result(False)  # 10º job fecha a janela
        assert result is False  # rollback acionado

        # Arquivo deve ter voltado ao estado original
        assert target.read_text(encoding="utf-8") == original

    def test_monitoring_window_not_complete_no_rollback(self, project: Path, applier: FixApplier):
        proposal = _make_proposal(action="append", content="WIN_APPEND\n")
        applier.apply(proposal, pre_fix_failure_rate=0.0)

        # Apenas 5 de 10 jobs — janela incompleta, sem rollback ainda
        for _ in range(5):
            result = applier.record_job_result(False)
        assert result is True  # janela não fechou


class TestFixApplierHistory:
    def test_history_persisted_after_apply(self, project: Path, applier: FixApplier):
        proposal = _make_proposal(action="append", content="HIST\n")
        applier.apply(proposal)

        hist_path = project / "sas2dbx_work" / "catalog" / "evolution_history.json"
        assert hist_path.exists()
        import json
        entries = json.loads(hist_path.read_text(encoding="utf-8"))
        assert len(entries) == 1
        assert entries[0]["status"] == "APPLIED"
        assert entries[0]["fix_type"] == "knowledge_store"

    def test_failed_apply_not_persisted_to_history(self, project: Path, applier: FixApplier):
        proposal = _make_proposal(
            action="modify",
            content="X",
            old_string="INEXISTENTE",
        )
        result = applier.apply(proposal)
        assert result.status == "FAILED"

        hist_path = project / "sas2dbx_work" / "catalog" / "evolution_history.json"
        # Histórico não deve existir ou deve estar vazio
        if hist_path.exists():
            import json
            entries = json.loads(hist_path.read_text(encoding="utf-8"))
            assert all(e["status"] != "APPLIED" for e in entries)
