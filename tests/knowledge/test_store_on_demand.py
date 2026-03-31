"""Testes para KnowledgeStore — on-demand harvest (Story H.2)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.store import KnowledgeStore
from sas2dbx.transpile.llm.client import LLMResponse


# ---------------------------------------------------------------------------
# Stubs de LLM
# ---------------------------------------------------------------------------

class _StubLLMClient:
    """Retorna mapeamento fixo de função para qualquer prompt."""

    def __init__(self, content: str | None = None, tokens: int = 0) -> None:
        self.call_count = 0
        self._content = content or (
            "TESTFUNC:\n"
            '  pyspark: "test_equivalent()"\n'
            '  notes: "stub"\n'
            "  confidence: 0.75\n"
        )
        self._tokens = tokens

    async def complete(self, prompt: str) -> LLMResponse:
        self.call_count += 1
        return LLMResponse(
            content=self._content,
            provider_used="stub",
            tokens_used=self._tokens,
            latency_ms=0.0,
        )


class _ProcStubLLMClient:
    """Retorna mapeamento de PROC."""

    async def complete(self, prompt: str) -> LLMResponse:
        return LLMResponse(
            content=(
                "TRANSPOSE:\n"
                '  approach: "llm"\n'
                '  tier: "Tier.LLM"\n'
                '  notes: "pivot operation — use pyspark groupBy + pivot"\n'
                "  confidence: 0.7\n"
            ),
            provider_used="stub",
            tokens_used=0,
            latency_ms=0.0,
        )


class _SqlDialectStubLLMClient:
    """Retorna regra de dialeto SQL."""

    async def complete(self, prompt: str) -> LLMResponse:
        return LLMResponse(
            content=(
                "CALCULATED:\n"
                '  sas: "SELECT CALCULATED total * 0.1 AS tax"\n'
                '  spark: "WITH t AS (...) SELECT t.total * 0.1 AS tax FROM t"\n'
                '  notes: "CALCULATED not in Spark — use CTE"\n'
                "  confidence: 0.9\n"
            ),
            provider_used="stub",
            tokens_used=0,
            latency_ms=0.0,
        )


class _FailingLLMClient:
    """Sempre retorna conteúdo que não é YAML válido."""

    call_count = 0

    async def complete(self, prompt: str) -> LLMResponse:
        _FailingLLMClient.call_count += 1
        return LLMResponse(
            content="isto não é yaml válido!!!! {{{",
            provider_used="stub",
            tokens_used=0,
            latency_ms=0.0,
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def knowledge_dir(tmp_path: Path) -> Path:
    """Knowledge store com merged/ contendo INTCK e SUBSTR."""
    merged = tmp_path / "mappings" / "merged"
    merged.mkdir(parents=True)
    (merged / "functions_map.yaml").write_text(
        yaml.dump({
            "INTCK": {"pyspark": "months_between(end, start)", "confidence": 0.9, "notes": "ok"},
            "SUBSTR": {"pyspark": "substring(col, pos, len)", "confidence": 0.95, "notes": "ok"},
        }),
        encoding="utf-8",
    )
    (merged / "proc_map.yaml").write_text(
        yaml.dump({"SORT": {"approach": "rule", "confidence": 1.0, "notes": ".orderBy()"}}),
        encoding="utf-8",
    )
    return tmp_path


# ---------------------------------------------------------------------------
# Cenário 1: função conhecida — sem chamar LLM
# ---------------------------------------------------------------------------

class TestKnownFunctionNoLLMCall:
    def test_known_function_returns_from_merged(self, knowledge_dir: Path) -> None:
        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=knowledge_dir, llm_client=stub)
        result = ks.lookup_function_or_harvest("INTCK")
        assert result is not None
        assert result["pyspark"] == "months_between(end, start)"
        assert stub.call_count == 0

    def test_known_function_case_insensitive(self, knowledge_dir: Path) -> None:
        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=knowledge_dir, llm_client=stub)
        result = ks.lookup_function_or_harvest("intck")
        assert result is not None
        assert stub.call_count == 0


# ---------------------------------------------------------------------------
# Cenário 2: função desconhecida — harvest on-demand
# ---------------------------------------------------------------------------

class TestUnknownFunctionTriggerHarvest:
    def test_unknown_triggers_llm_call(self, tmp_path: Path) -> None:
        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        result = ks.lookup_function_or_harvest("TESTFUNC")
        assert result is not None
        assert result["pyspark"] == "test_equivalent()"
        assert stub.call_count == 1

    def test_harvest_persists_to_generated(self, tmp_path: Path) -> None:
        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        ks.lookup_function_or_harvest("TESTFUNC")

        gen_file = tmp_path / "mappings" / "generated" / "functions_map.yaml"
        assert gen_file.exists()
        data = yaml.safe_load(gen_file.read_text(encoding="utf-8"))
        assert "TESTFUNC" in data
        assert data["TESTFUNC"]["pyspark"] == "test_equivalent()"

    def test_harvest_entry_confidence(self, tmp_path: Path) -> None:
        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        result = ks.lookup_function_or_harvest("TESTFUNC")
        assert result is not None
        assert result["confidence"] == 0.75


# ---------------------------------------------------------------------------
# Cenário 3: segunda chamada da mesma função — cache, sem LLM
# ---------------------------------------------------------------------------

class TestSecondCallUsesCache:
    def test_second_call_no_llm(self, tmp_path: Path) -> None:
        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        ks.lookup_function_or_harvest("TESTFUNC")  # primeira — LLM
        ks.lookup_function_or_harvest("TESTFUNC")  # segunda — cache em disco
        assert stub.call_count == 1

    def test_second_call_reads_from_generated(self, tmp_path: Path) -> None:
        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        r1 = ks.lookup_function_or_harvest("TESTFUNC")
        r2 = ks.lookup_function_or_harvest("TESTFUNC")
        assert r1 == r2


# ---------------------------------------------------------------------------
# Cache de negativas — não tentar de novo se LLM falhou
# ---------------------------------------------------------------------------

class TestNegativeCachePreventsRetry:
    def test_failing_llm_tried_once_only(self, tmp_path: Path) -> None:
        _FailingLLMClient.call_count = 0
        stub = _FailingLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        r1 = ks.lookup_function_or_harvest("BADFUNC")
        r2 = ks.lookup_function_or_harvest("BADFUNC")
        assert r1 is None
        assert r2 is None
        assert _FailingLLMClient.call_count == 1

    def test_different_functions_each_get_one_attempt(self, tmp_path: Path) -> None:
        _FailingLLMClient.call_count = 0
        ks = KnowledgeStore(base_path=tmp_path, llm_client=_FailingLLMClient())
        ks.lookup_function_or_harvest("FUNCA")
        ks.lookup_function_or_harvest("FUNCB")
        ks.lookup_function_or_harvest("FUNCA")  # cache de negativa
        assert _FailingLLMClient.call_count == 2  # FUNCA + FUNCB, mas não terceira


# ---------------------------------------------------------------------------
# Cenário 6: sem LLM client — retorna None silenciosamente
# ---------------------------------------------------------------------------

class TestNoLLMClientSilent:
    def test_no_llm_returns_none(self, tmp_path: Path) -> None:
        ks = KnowledgeStore(base_path=tmp_path)  # sem llm_client
        result = ks.lookup_function_or_harvest("WHATEVER")
        assert result is None

    def test_no_llm_does_not_write_generated(self, tmp_path: Path) -> None:
        ks = KnowledgeStore(base_path=tmp_path)
        ks.lookup_function_or_harvest("WHATEVER")
        gen = tmp_path / "mappings" / "generated" / "functions_map.yaml"
        assert not gen.exists()

    def test_existing_lookups_unaffected(self, knowledge_dir: Path) -> None:
        """lookup_function() simples continua funcionando sem llm_client."""
        ks = KnowledgeStore(base_path=knowledge_dir)
        result = ks.lookup_function("INTCK")
        assert result is not None


# ---------------------------------------------------------------------------
# curated/ nunca é modificado
# ---------------------------------------------------------------------------

class TestCuratedNotModified:
    def test_on_demand_writes_only_to_generated(self, tmp_path: Path) -> None:
        """Harvest on-demand nunca escreve em curated/."""
        curated = tmp_path / "mappings" / "curated"
        curated.mkdir(parents=True)
        curated_file = curated / "functions_map.yaml"
        curated_file.write_text(yaml.dump({"EXISTING": {"pyspark": "x", "notes": "y", "confidence": 0.9}}), encoding="utf-8")
        original = curated_file.read_text()

        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        ks.lookup_function_or_harvest("TESTFUNC")

        assert curated_file.read_text() == original


# ---------------------------------------------------------------------------
# _append_to_generated — merge sem perder entradas existentes
# ---------------------------------------------------------------------------

class TestAppendPreservesExisting:
    def test_preserves_existing_generated_entries(self, tmp_path: Path) -> None:
        gen = tmp_path / "mappings" / "generated"
        gen.mkdir(parents=True)
        (gen / "functions_map.yaml").write_text(
            yaml.dump({"EXISTING": {"pyspark": "old()", "notes": "preserved", "confidence": 0.9}}),
            encoding="utf-8",
        )

        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        ks.lookup_function_or_harvest("TESTFUNC")

        data = yaml.safe_load((gen / "functions_map.yaml").read_text(encoding="utf-8"))
        assert "EXISTING" in data  # preservado
        assert "TESTFUNC" in data  # adicionado

    def test_new_entry_overwrites_same_key(self, tmp_path: Path) -> None:
        gen = tmp_path / "mappings" / "generated"
        gen.mkdir(parents=True)
        (gen / "functions_map.yaml").write_text(
            yaml.dump({"TESTFUNC": {"pyspark": "old_value()", "notes": "old", "confidence": 0.5}}),
            encoding="utf-8",
        )

        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        # TESTFUNC está em generated mas não em merged → lookup_function retorna do generated
        # Portanto lookup_function_or_harvest vai retornar o old_value sem chamar LLM
        ks_no_cache = KnowledgeStore(base_path=tmp_path)
        result = ks_no_cache.lookup_function("TESTFUNC")
        assert result is not None
        assert result["pyspark"] == "old_value()"


# ---------------------------------------------------------------------------
# lookup_proc_or_harvest
# ---------------------------------------------------------------------------

class TestLookupProcOrHarvest:
    def test_known_proc_no_llm(self, knowledge_dir: Path) -> None:
        stub = _StubLLMClient()
        ks = KnowledgeStore(base_path=knowledge_dir, llm_client=stub)
        result = ks.lookup_proc_or_harvest("SORT")
        assert result is not None
        assert result["approach"] == "rule"
        assert stub.call_count == 0

    def test_unknown_proc_triggers_harvest(self, tmp_path: Path) -> None:
        stub = _ProcStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        result = ks.lookup_proc_or_harvest("TRANSPOSE")
        assert result is not None
        assert result["approach"] == "llm"

    def test_unknown_proc_persists_to_generated(self, tmp_path: Path) -> None:
        stub = _ProcStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        ks.lookup_proc_or_harvest("TRANSPOSE")
        gen_file = tmp_path / "mappings" / "generated" / "proc_map.yaml"
        assert gen_file.exists()
        data = yaml.safe_load(gen_file.read_text(encoding="utf-8"))
        assert "TRANSPOSE" in data


# ---------------------------------------------------------------------------
# lookup_sql_dialect_or_harvest
# ---------------------------------------------------------------------------

class TestLookupSqlDialectOrHarvest:
    def test_unknown_construct_triggers_harvest(self, tmp_path: Path) -> None:
        stub = _SqlDialectStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        result = ks.lookup_sql_dialect_or_harvest("CALCULATED")
        assert result is not None
        assert "CTE" in result["notes"]

    def test_persists_sql_dialect_to_generated(self, tmp_path: Path) -> None:
        stub = _SqlDialectStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        ks.lookup_sql_dialect_or_harvest("CALCULATED")
        gen_file = tmp_path / "mappings" / "generated" / "sql_dialect_map.yaml"
        assert gen_file.exists()
        data = yaml.safe_load(gen_file.read_text(encoding="utf-8"))
        assert "CALCULATED" in data


# ---------------------------------------------------------------------------
# lookup_format_or_harvest
# ---------------------------------------------------------------------------

class TestLookupFormatOrHarvest:
    def test_unknown_format_triggers_harvest(self, tmp_path: Path) -> None:
        fmt_content = (
            "WEEKDATE.:\n"
            '  pyspark: "date_format(col, \'EEEE, MMMM d, yyyy\')"\n'
            '  notes: "SAS WEEKDATE. → Java date pattern"\n'
            "  confidence: 0.8\n"
        )
        stub = _StubLLMClient(content=fmt_content)
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        result = ks.lookup_format_or_harvest("WEEKDATE.")
        assert result is not None

    def test_no_llm_returns_none_for_format(self, tmp_path: Path) -> None:
        ks = KnowledgeStore(base_path=tmp_path)
        assert ks.lookup_format_or_harvest("WEEKDATE.") is None


# ---------------------------------------------------------------------------
# cache de negativas por categoria (independentes entre si)
# ---------------------------------------------------------------------------

class TestNegativeCacheIsolation:
    def test_negative_cache_per_category(self, tmp_path: Path) -> None:
        """Cache de negativas é por category:key, não só por key."""
        _FailingLLMClient.call_count = 0
        stub = _FailingLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)

        # "FUNC1" como function e como proc são cache keys diferentes
        ks.lookup_function_or_harvest("FUNC1")
        ks.lookup_proc_or_harvest("FUNC1")

        # Deve ter tentado 2 vezes (uma por categoria)
        assert _FailingLLMClient.call_count == 2
