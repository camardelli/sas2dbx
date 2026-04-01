"""Testes para KnowledgeStore — A.1 async-native lookups."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
import yaml

from sas2dbx.knowledge.store import KnowledgeStore
from sas2dbx.transpile.llm.client import LLMResponse


# ---------------------------------------------------------------------------
# Stubs async
# ---------------------------------------------------------------------------


class _AsyncStubLLMClient:
    """Retorna mapeamento fixo para qualquer prompt."""

    def __init__(self) -> None:
        self.call_count = 0

    async def complete(self, prompt: str) -> LLMResponse:
        self.call_count += 1
        return LLMResponse(
            content=(
                "ASYNCFUNC:\n"
                '  pyspark: "async_equivalent()"\n'
                '  notes: "async stub"\n'
                "  confidence: 0.8\n"
            ),
            provider_used="stub",
            tokens_used=0,
            latency_ms=0.0,
        )


class _AsyncFailingLLMClient:
    """Sempre retorna YAML inválido."""

    def __init__(self) -> None:
        self.call_count = 0

    async def complete(self, prompt: str) -> LLMResponse:
        self.call_count += 1
        return LLMResponse(
            content="isto não é yaml {{{",
            provider_used="stub",
            tokens_used=0,
            latency_ms=0.0,
        )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ks_with_merged(tmp_path: Path) -> KnowledgeStore:
    merged = tmp_path / "mappings" / "merged"
    merged.mkdir(parents=True)
    (merged / "functions_map.yaml").write_text(
        yaml.dump({
            "INTCK": {"pyspark": "months_between(end, start)", "confidence": 0.9, "notes": "ok"},
        }),
        encoding="utf-8",
    )
    (merged / "proc_map.yaml").write_text(
        yaml.dump({"SORT": {"approach": "rule", "confidence": 1.0, "notes": ".orderBy()"}}),
        encoding="utf-8",
    )
    stub = _AsyncStubLLMClient()
    return KnowledgeStore(base_path=tmp_path, llm_client=stub)


# ---------------------------------------------------------------------------
# alookup_function_or_harvest
# ---------------------------------------------------------------------------


class TestAsyncLookupFunction:
    def test_known_function_returns_without_llm_call(self, ks_with_merged: Path) -> None:
        ks = ks_with_merged
        stub = ks._llm_client
        result = asyncio.run(ks.alookup_function_or_harvest("INTCK"))
        assert result is not None
        assert result["pyspark"] == "months_between(end, start)"
        assert stub.call_count == 0

    def test_case_insensitive(self, ks_with_merged: KnowledgeStore) -> None:
        result = asyncio.run(ks_with_merged.alookup_function_or_harvest("intck"))
        assert result is not None

    def test_unknown_function_triggers_async_llm(self, tmp_path: Path) -> None:
        stub = _AsyncStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        result = asyncio.run(ks.alookup_function_or_harvest("ASYNCFUNC"))
        assert result is not None
        assert result["pyspark"] == "async_equivalent()"
        assert stub.call_count == 1

    def test_unknown_function_persists_to_generated(self, tmp_path: Path) -> None:
        stub = _AsyncStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        asyncio.run(ks.alookup_function_or_harvest("ASYNCFUNC"))
        gen = tmp_path / "mappings" / "generated" / "functions_map.yaml"
        assert gen.exists()
        data = yaml.safe_load(gen.read_text(encoding="utf-8"))
        assert "ASYNCFUNC" in data

    def test_no_llm_returns_none(self, tmp_path: Path) -> None:
        ks = KnowledgeStore(base_path=tmp_path)
        result = asyncio.run(ks.alookup_function_or_harvest("WHATEVER"))
        assert result is None

    def test_second_call_uses_cache_no_llm(self, tmp_path: Path) -> None:
        stub = _AsyncStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        asyncio.run(ks.alookup_function_or_harvest("ASYNCFUNC"))
        asyncio.run(ks.alookup_function_or_harvest("ASYNCFUNC"))
        assert stub.call_count == 1

    def test_failing_llm_returns_none(self, tmp_path: Path) -> None:
        stub = _AsyncFailingLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        result = asyncio.run(ks.alookup_function_or_harvest("BADFUNC"))
        assert result is None

    def test_failing_llm_tried_once_only(self, tmp_path: Path) -> None:
        stub = _AsyncFailingLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        asyncio.run(ks.alookup_function_or_harvest("BADFUNC"))
        asyncio.run(ks.alookup_function_or_harvest("BADFUNC"))
        assert stub.call_count == 1


# ---------------------------------------------------------------------------
# alookup_proc_or_harvest
# ---------------------------------------------------------------------------


class TestAsyncLookupProc:
    def test_known_proc_no_llm_call(self, ks_with_merged: KnowledgeStore) -> None:
        stub = ks_with_merged._llm_client
        result = asyncio.run(ks_with_merged.alookup_proc_or_harvest("SORT"))
        assert result is not None
        assert result["approach"] == "rule"
        assert stub.call_count == 0

    def test_unknown_proc_triggers_llm(self, tmp_path: Path) -> None:
        stub = _AsyncStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        asyncio.run(ks.alookup_proc_or_harvest("ASYNCFUNC"))
        assert stub.call_count == 1


# ---------------------------------------------------------------------------
# alookup_informat_or_harvest — L3
# ---------------------------------------------------------------------------


class TestAsyncLookupInformat:
    def test_no_llm_returns_none(self, tmp_path: Path) -> None:
        ks = KnowledgeStore(base_path=tmp_path)
        result = asyncio.run(ks.alookup_informat_or_harvest("DATE9."))
        assert result is None

    def test_unknown_informat_triggers_llm(self, tmp_path: Path) -> None:
        stub = _AsyncStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        asyncio.run(ks.alookup_informat_or_harvest("ASYNCFUNC"))
        assert stub.call_count == 1

    def test_known_informat_from_merged_no_llm(self, tmp_path: Path) -> None:
        merged = tmp_path / "mappings" / "merged"
        merged.mkdir(parents=True)
        (merged / "informats_map.yaml").write_text(
            yaml.dump({"DATE9.": {"pyspark": "to_date(col, 'ddMMMyyyy')", "confidence": 0.9}}),
            encoding="utf-8",
        )
        stub = _AsyncStubLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)
        result = asyncio.run(ks.alookup_informat_or_harvest("DATE9."))
        assert result is not None
        assert stub.call_count == 0


# ---------------------------------------------------------------------------
# Isolação do negative cache por categoria (async)
# ---------------------------------------------------------------------------


class TestAsyncNegativeCacheIsolation:
    def test_negative_cache_per_category(self, tmp_path: Path) -> None:
        """Cache de negativas async é independente por category:key."""
        stub = _AsyncFailingLLMClient()
        ks = KnowledgeStore(base_path=tmp_path, llm_client=stub)

        asyncio.run(ks.alookup_function_or_harvest("FUNC1"))
        asyncio.run(ks.alookup_proc_or_harvest("FUNC1"))

        assert stub.call_count == 2
