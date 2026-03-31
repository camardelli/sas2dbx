"""Testes para LLMHarvester — C1, C2, C3, C4 e integração com KnowledgeHarvester."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.populate.harvester import HarvestMode, KnowledgeHarvester
from sas2dbx.knowledge.populate.llm_harvester import (
    HarvestReport,
    LLMHarvester,
    _LLM_TARGET_FILES,
)
from sas2dbx.transpile.llm.client import LLMConfig, LLMResponse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_llm_config() -> LLMConfig:
    return LLMConfig(api_key="sk-test-fake")


def _valid_functions_yaml() -> str:
    """YAML válido para functions_map.yaml."""
    return yaml.dump({
        "INTCK": {
            "pyspark": "months_between({arg2}, {arg1})",
            "notes": "Ordem dos argumentos invertida.",
            "confidence": 0.9,
            "variants": {"MONTH": "months_between({arg2}, {arg1})"},
        },
        "CATX": {
            "pyspark": "concat_ws({delimiter}, {args})",
            "notes": "CATX remove leading/trailing spaces.",
            "confidence": 0.95,
        },
    })


def _valid_sql_dialect_yaml() -> str:
    """YAML válido para sql_dialect_map.yaml (usa 'spark' em vez de 'pyspark')."""
    return yaml.dump({
        "CALCULATED": {
            "sas": "SELECT CALCULATED total * 0.1 AS tax",
            "spark": "WITH t AS (SELECT ...) SELECT t.total * 0.1 AS tax FROM t",
            "notes": "CALCULATED não existe em Spark SQL.",
            "confidence": 0.9,
        },
    })


def _stub_response(content: str, tokens: int = 50) -> LLMResponse:
    return LLMResponse(content=content, provider_used="stub", tokens_used=tokens, latency_ms=1.0)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def harvester(tmp_path: Path) -> LLMHarvester:
    return LLMHarvester(base_path=tmp_path, llm_config=_make_llm_config())


@pytest.fixture
def knowledge_harvester(tmp_path: Path) -> KnowledgeHarvester:
    return KnowledgeHarvester(base_path=tmp_path)


# ---------------------------------------------------------------------------
# C4 — options_map.yaml excluído
# ---------------------------------------------------------------------------

class TestC4OptionsMapExcluded:
    def test_options_map_not_in_target_files(self) -> None:
        """options_map.yaml não deve estar na lista de arquivos LLM target."""
        assert "options_map.yaml" not in _LLM_TARGET_FILES

    def test_target_files_count(self) -> None:
        """Deve ter exatamente 5 arquivos target."""
        assert len(_LLM_TARGET_FILES) == 5

    def test_expected_files_present(self) -> None:
        expected = {
            "functions_map.yaml",
            "formats_map.yaml",
            "informats_map.yaml",
            "proc_map.yaml",
            "sql_dialect_map.yaml",
        }
        assert set(_LLM_TARGET_FILES) == expected


# ---------------------------------------------------------------------------
# C2 — YAML parse fallback
# ---------------------------------------------------------------------------

class TestC2YamlParseFallback:
    def test_parse_direct_valid_yaml(self, harvester: LLMHarvester) -> None:
        """Parse direto de YAML válido deve funcionar."""
        content = _valid_functions_yaml()
        result = harvester._parse_yaml(content)
        assert "INTCK" in result

    def test_parse_yaml_block_extraction(self, harvester: LLMHarvester) -> None:
        """Deve extrair YAML de bloco ```yaml ... ```."""
        content = (
            "Aqui está o mapeamento:\n"
            "```yaml\n"
            "INTCK:\n"
            "  pyspark: months_between(end, start)\n"
            "  notes: ordem invertida\n"
            "  confidence: 0.9\n"
            "```\n"
            "Pronto."
        )
        result = harvester._parse_yaml(content)
        assert "INTCK" in result

    def test_parse_yaml_block_without_language_tag(self, harvester: LLMHarvester) -> None:
        """Deve extrair YAML de bloco ``` sem tag de linguagem."""
        content = "```\nINTCK:\n  pyspark: x\n  notes: y\n  confidence: 0.9\n```"
        result = harvester._parse_yaml(content)
        assert "INTCK" in result

    def test_parse_invalid_yaml_returns_empty(self, harvester: LLMHarvester) -> None:
        """YAML inválido deve retornar dict vazio."""
        content = "isso não é yaml válido: {{{broken"
        result = harvester._parse_yaml(content)
        assert result == {}

    def test_parse_non_dict_returns_empty(self, harvester: LLMHarvester) -> None:
        """YAML que não é dict (ex: lista) deve retornar {}."""
        content = "- item1\n- item2\n"
        result = harvester._parse_yaml(content)
        assert result == {}


# ---------------------------------------------------------------------------
# C1 — Schema validation
# ---------------------------------------------------------------------------

class TestC1SchemaValidation:
    def test_valid_entry_passes(self, harvester: LLMHarvester) -> None:
        raw = {
            "INTCK": {"pyspark": "months_between(x,y)", "notes": "ok", "confidence": 0.9}
        }
        valid, skipped = harvester._validate_entries(raw, "functions_map.yaml")
        assert "INTCK" in valid
        assert skipped == 0

    def test_missing_pyspark_field_discarded(self, harvester: LLMHarvester) -> None:
        raw = {"INTCK": {"notes": "sem pyspark", "confidence": 0.9}}
        valid, skipped = harvester._validate_entries(raw, "functions_map.yaml")
        assert valid == {}
        assert skipped == 1

    def test_missing_notes_field_discarded(self, harvester: LLMHarvester) -> None:
        raw = {"INTCK": {"pyspark": "x", "confidence": 0.9}}
        valid, skipped = harvester._validate_entries(raw, "functions_map.yaml")
        assert valid == {}
        assert skipped == 1

    def test_confidence_out_of_range_discarded(self, harvester: LLMHarvester) -> None:
        raw = {"INTCK": {"pyspark": "x", "notes": "y", "confidence": 1.5}}
        valid, skipped = harvester._validate_entries(raw, "functions_map.yaml")
        assert valid == {}
        assert skipped == 1

    def test_confidence_zero_is_valid(self, harvester: LLMHarvester) -> None:
        raw = {"INTCK": {"pyspark": "x", "notes": "y", "confidence": 0.0}}
        valid, skipped = harvester._validate_entries(raw, "functions_map.yaml")
        assert "INTCK" in valid
        assert skipped == 0

    def test_non_dict_value_discarded(self, harvester: LLMHarvester) -> None:
        raw = {"INTCK": "string_instead_of_dict"}
        valid, skipped = harvester._validate_entries(raw, "functions_map.yaml")
        assert valid == {}
        assert skipped == 1

    def test_sql_dialect_uses_spark_field(self, harvester: LLMHarvester) -> None:
        """sql_dialect_map.yaml requer 'spark' em vez de 'pyspark'."""
        raw = {
            "CALCULATED": {
                "sas": "SELECT CALCULATED x",
                "spark": "WITH t AS (...) SELECT ...",
                "notes": "sem equivalente direto",
                "confidence": 0.9,
            }
        }
        valid, skipped = harvester._validate_entries(raw, "sql_dialect_map.yaml")
        assert "CALCULATED" in valid
        assert skipped == 0

    def test_sql_dialect_pyspark_field_not_required(self, harvester: LLMHarvester) -> None:
        """sql_dialect_map não exige 'pyspark' — usa 'spark'."""
        raw = {
            "CALCULATED": {
                "spark": "CTE equivalent",
                "notes": "note",
                "confidence": 0.85,
            }
        }
        valid, skipped = harvester._validate_entries(raw, "sql_dialect_map.yaml")
        assert "CALCULATED" in valid
        assert skipped == 0

    def test_optional_variants_preserved(self, harvester: LLMHarvester) -> None:
        raw = {
            "INTCK": {
                "pyspark": "months_between(x,y)",
                "notes": "ok",
                "confidence": 0.9,
                "variants": {"MONTH": "months_between"},
            }
        }
        valid, _ = harvester._validate_entries(raw, "functions_map.yaml")
        assert valid["INTCK"]["variants"] == {"MONTH": "months_between"}


# ---------------------------------------------------------------------------
# _write_generated — comportamento de merge
# ---------------------------------------------------------------------------

class TestWriteGenerated:
    def test_creates_generated_dir_and_file(
        self, harvester: LLMHarvester, tmp_path: Path
    ) -> None:
        harvester._write_generated("functions_map.yaml", {"INTCK": {"pyspark": "x", "notes": "y", "confidence": 0.9}})
        assert (tmp_path / "mappings" / "generated" / "functions_map.yaml").exists()

    def test_merges_with_existing_content(
        self, harvester: LLMHarvester, tmp_path: Path
    ) -> None:
        """Novas entradas sobrescrevem por chave; entradas não-presentes são preservadas."""
        gen_dir = tmp_path / "mappings" / "generated"
        gen_dir.mkdir(parents=True)
        (gen_dir / "functions_map.yaml").write_text(
            yaml.dump({"OLD_FUNC": {"pyspark": "old", "notes": "preserved", "confidence": 0.5}}),
            encoding="utf-8",
        )
        harvester._write_generated(
            "functions_map.yaml",
            {"INTCK": {"pyspark": "new", "notes": "new note", "confidence": 0.9}},
        )
        content = yaml.safe_load(
            (gen_dir / "functions_map.yaml").read_text(encoding="utf-8")
        )
        assert "OLD_FUNC" in content  # preservado
        assert "INTCK" in content      # novo

    def test_new_entry_overwrites_same_key(
        self, harvester: LLMHarvester, tmp_path: Path
    ) -> None:
        gen_dir = tmp_path / "mappings" / "generated"
        gen_dir.mkdir(parents=True)
        (gen_dir / "functions_map.yaml").write_text(
            yaml.dump({"INTCK": {"pyspark": "old_value", "notes": "old", "confidence": 0.5}}),
            encoding="utf-8",
        )
        harvester._write_generated(
            "functions_map.yaml",
            {"INTCK": {"pyspark": "new_value", "notes": "new", "confidence": 0.9}},
        )
        content = yaml.safe_load(
            (gen_dir / "functions_map.yaml").read_text(encoding="utf-8")
        )
        assert content["INTCK"]["pyspark"] == "new_value"


# ---------------------------------------------------------------------------
# HarvestReport dataclass
# ---------------------------------------------------------------------------

class TestHarvestReport:
    def test_default_values(self) -> None:
        report = HarvestReport()
        assert report.sources_processed == []
        assert report.entries_per_file == {}
        assert report.tokens_used == 0
        assert report.errors == []
        assert report.skipped_entries == 0

    def test_accumulation(self) -> None:
        report = HarvestReport()
        report.sources_processed.append("functions_map.yaml")
        report.entries_per_file["functions_map.yaml"] = 60
        report.tokens_used += 1500
        assert len(report.sources_processed) == 1
        assert report.entries_per_file["functions_map.yaml"] == 60
        assert report.tokens_used == 1500


# ---------------------------------------------------------------------------
# C3 — harvest_all_sync (integração com stub LLM)
# ---------------------------------------------------------------------------

class TestC3HarvestAllSync:
    def test_harvest_all_sync_returns_report(
        self, harvester: LLMHarvester, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """harvest_all_sync() deve retornar HarvestReport sem levantar exceção."""
        from sas2dbx.transpile.llm import client as llm_module

        async def _stub(self_inner, prompt: str) -> LLMResponse:
            return _stub_response(_valid_functions_yaml())

        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub)

        report = harvester.harvest_all_sync()
        assert isinstance(report, HarvestReport)

    def test_harvest_all_sync_processes_all_files(
        self, harvester: LLMHarvester, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Todos os 5 arquivos target devem ser processados."""
        from sas2dbx.transpile.llm import client as llm_module

        async def _stub(self_inner, prompt: str) -> LLMResponse:
            return _stub_response(_valid_functions_yaml())

        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub)

        report = harvester.harvest_all_sync()
        assert len(report.sources_processed) == 5

    def test_harvest_all_sync_accumulates_tokens(
        self, harvester: LLMHarvester, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from sas2dbx.transpile.llm import client as llm_module

        async def _stub(self_inner, prompt: str) -> LLMResponse:
            return _stub_response(_valid_functions_yaml(), tokens=100)

        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub)

        report = harvester.harvest_all_sync()
        assert report.tokens_used == 500  # 5 arquivos × 100 tokens

    def test_harvest_all_creates_generated_files(
        self, harvester: LLMHarvester, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from sas2dbx.transpile.llm import client as llm_module

        async def _stub(self_inner, prompt: str) -> LLMResponse:
            return _stub_response(_valid_functions_yaml())

        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub)

        harvester.harvest_all_sync()

        gen_dir = tmp_path / "mappings" / "generated"
        assert gen_dir.is_dir()
        for filename in _LLM_TARGET_FILES:
            assert (gen_dir / filename).exists(), f"Arquivo ausente: {filename}"

    def test_harvest_llm_error_captured_in_report(
        self, harvester: LLMHarvester, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Erro de LLM em um arquivo não deve travar os demais."""
        from sas2dbx.transpile.llm import client as llm_module
        call_count = {"n": 0}

        async def _stub_with_one_error(self_inner, prompt: str) -> LLMResponse:
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise RuntimeError("LLM timeout simulado")
            return _stub_response(_valid_functions_yaml())

        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub_with_one_error)

        report = harvester.harvest_all_sync()
        assert len(report.errors) == 1
        assert len(report.sources_processed) == 4  # 5 - 1 com erro

    def test_sql_dialect_map_uses_correct_schema(
        self, harvester: LLMHarvester, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """sql_dialect_map.yaml deve usar 'spark' em vez de 'pyspark'."""
        from sas2dbx.transpile.llm import client as llm_module

        async def _stub(self_inner, prompt: str) -> LLMResponse:
            return _stub_response(_valid_sql_dialect_yaml())

        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub)

        harvester.harvest_all_sync()

        gen_file = tmp_path / "mappings" / "generated" / "sql_dialect_map.yaml"
        content = yaml.safe_load(gen_file.read_text(encoding="utf-8"))
        assert "CALCULATED" in content


# ---------------------------------------------------------------------------
# KnowledgeHarvester.harvest_llm() — integração com dispatch
# ---------------------------------------------------------------------------

class TestKnowledgeHarvesterLLMDispatch:
    def test_harvest_llm_raises_without_config(
        self, knowledge_harvester: KnowledgeHarvester
    ) -> None:
        """harvest_llm sem llm_config deve levantar ValueError."""
        with pytest.raises(ValueError, match="llm_config"):
            knowledge_harvester.harvest_llm(llm_config=None)

    def test_harvest_dispatch_llm_mode(
        self,
        knowledge_harvester: KnowledgeHarvester,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """harvest() com mode=LLM deve chamar harvest_llm()."""
        called = {"ok": False}

        def _mock_harvest_llm(self_inner, llm_config=None):
            called["ok"] = True

        monkeypatch.setattr(KnowledgeHarvester, "harvest_llm", _mock_harvest_llm)

        llm_config = _make_llm_config()
        knowledge_harvester.harvest("sas", mode=HarvestMode.LLM, llm_config=llm_config)
        assert called["ok"]

    def test_harvest_llm_mode_calls_llm_harvester(
        self,
        knowledge_harvester: KnowledgeHarvester,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Mode=LLM deve criar arquivos em mappings/generated/."""
        from sas2dbx.transpile.llm import client as llm_module

        async def _stub(self_inner, prompt: str) -> LLMResponse:
            return _stub_response(_valid_functions_yaml())

        monkeypatch.setattr(llm_module.LLMClient, "complete", _stub)

        knowledge_harvester.harvest_llm(llm_config=_make_llm_config())

        gen_dir = tmp_path / "mappings" / "generated"
        assert gen_dir.is_dir()
        assert any(gen_dir.iterdir())
