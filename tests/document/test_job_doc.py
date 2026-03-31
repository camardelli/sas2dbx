"""Testes para document/job_doc.py e document/prompts.py."""

from __future__ import annotations

import asyncio
from pathlib import Path

from sas2dbx.analyze.parser import BlockDeps
from sas2dbx.document.job_doc import JobDocResult, JobDocumentor, _merge_block_deps
from sas2dbx.document.prompts import build_job_doc_prompt
from sas2dbx.models.dependency_graph import DependencyGraph, JobNode
from sas2dbx.models.sas_ast import ClassificationResult, Tier
from sas2dbx.transpile.llm.client import LLMResponse

# ---------------------------------------------------------------------------
# Stub LLM Client (R14 — zero chamadas reais ao Claude)
# ---------------------------------------------------------------------------

_STUB_RESPONSE = """## Objetivo
Carrega clientes ativos da base raw e enriquece com dados de vendas.

## Diagrama de fluxo
```mermaid
graph LR
  SASDATA.CLIENTES_RAW -->|filter| CLIENTES_FILTRADOS
  CLIENTES_FILTRADOS & SASDATA.VENDAS -->|left join| WORK.CLIENTES_ENRIQUECIDOS
```

## Datasets
| Dataset | Tipo | Descrição |
|---------|------|-----------|
| SASDATA.CLIENTES_RAW | Input | Base bruta de clientes |
| WORK.CLIENTES_ENRIQUECIDOS | Output | Clientes enriquecidos |

## Transformações
1. DATA step: filtra clientes com status=A
2. PROC SORT: ordena por id_cliente
3. PROC SQL: LEFT JOIN com vendas

## Dependências
- **Prerequisitos:** nenhum
- **Dependentes:** job_002

## Construtos SAS detectados
| Construto | Tier | Confidence |
|-----------|------|------------|
| DATA_STEP | Tier Rule | 95% |

## Riscos e observações
Nenhum construto Tier 3 detectado."""


class _StubLLMClient:
    """LLMClient stub para testes — sem chamadas reais ao Claude."""

    def __init__(self, response: str = _STUB_RESPONSE) -> None:
        self._response = response
        self.call_count = 0
        self.last_prompt: str = ""

    async def complete(self, prompt: str) -> LLMResponse:
        self.call_count += 1
        self.last_prompt = prompt
        return LLMResponse(
            content=self._response,
            provider_used="stub",
            tokens_used=150,
            latency_ms=1.0,
        )

    def complete_sync(self, prompt: str) -> LLMResponse:
        return asyncio.run(self.complete(prompt))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_graph(job_name: str = "job_001") -> DependencyGraph:
    g = DependencyGraph()
    g.jobs[job_name] = JobNode(job_name=job_name, path=Path(f"{job_name}.sas"))
    g.jobs["job_002"] = JobNode(job_name="job_002", path=Path("job_002.sas"))
    g.explicit_edges = [("job_002", job_name)]
    return g


def _make_block_deps() -> list[BlockDeps]:
    return [
        BlockDeps(
            inputs=["SASDATA.CLIENTES_RAW"],
            outputs=["CLIENTES_FILTRADOS"],
            libnames_declared=["SASDATA"],
        ),
        BlockDeps(
            inputs=["CLIENTES_FILTRADOS", "SASDATA.VENDAS"],
            outputs=["WORK.CLIENTES_ENRIQUECIDOS"],
        ),
    ]


def _make_classifications() -> list[ClassificationResult]:
    return [
        ClassificationResult(construct_type="DATA_STEP", tier=Tier.RULE, confidence=0.95),
        ClassificationResult(construct_type="PROC_SQL", tier=Tier.RULE, confidence=0.90),
    ]


def _run_doc(doc: JobDocumentor, job: str = "job_001") -> JobDocResult:
    """Helper: executa generate_doc com fixtures padrão."""
    return asyncio.run(
        doc.generate_doc(
            job, SAS_CODE, _make_block_deps(), _make_classifications(), _make_graph()
        )
    )


SAS_CODE = """\
DATA clientes_filtrados;
    SET sasdata.clientes_raw;
    WHERE status = 'A';
RUN;
PROC SQL;
    CREATE TABLE work.clientes_enriquecidos AS
    SELECT c.*, v.total_vendas FROM clientes_filtrados c
    LEFT JOIN sasdata.vendas v ON c.id = v.id;
QUIT;
"""


# ---------------------------------------------------------------------------
# Tests: JobDocumentor
# ---------------------------------------------------------------------------


class TestJobDocumentor:
    def test_generate_doc_returns_job_doc_result(self) -> None:
        stub = _StubLLMClient()
        doc = JobDocumentor(llm_client=stub)
        result = asyncio.run(
            doc.generate_doc(
                job_name="job_001",
                sas_code=SAS_CODE,
                block_deps=_make_block_deps(),
                classification_results=_make_classifications(),
                graph=_make_graph(),
            )
        )
        assert isinstance(result, JobDocResult)
        assert result.job_name == "job_001"
        assert result.from_llm is True

    def test_generate_doc_content_has_header(self) -> None:
        result = _run_doc(JobDocumentor(llm_client=_StubLLMClient()))
        assert result.content.startswith("# job_001")

    def test_generate_doc_content_has_llm_response(self) -> None:
        result = _run_doc(JobDocumentor(llm_client=_StubLLMClient()))
        assert "## Objetivo" in result.content
        assert "## Transformações" in result.content

    def test_generate_doc_tokens_recorded(self) -> None:
        result = _run_doc(JobDocumentor(llm_client=_StubLLMClient()))
        assert result.tokens_used == 150

    def test_generate_doc_calls_llm_once(self) -> None:
        stub = _StubLLMClient()
        _run_doc(JobDocumentor(llm_client=stub))
        assert stub.call_count == 1

    def test_generate_doc_sync_works(self) -> None:
        stub = _StubLLMClient()
        doc = JobDocumentor(llm_client=stub)
        result = doc.generate_doc_sync(
            "job_001", SAS_CODE, _make_block_deps(), _make_classifications(), _make_graph()
        )
        assert isinstance(result, JobDocResult)
        assert result.job_name == "job_001"

    def test_generate_doc_without_knowledge_store(self) -> None:
        """R12 — KS opcional: sem KS não deve quebrar."""
        doc = JobDocumentor(llm_client=_StubLLMClient(), knowledge_store=None)
        result = _run_doc(doc)
        assert result.from_llm is True

    def test_prompt_contains_job_name(self) -> None:
        stub = _StubLLMClient()
        doc = JobDocumentor(llm_client=stub)
        asyncio.run(
            doc.generate_doc("job_001_carga_clientes", SAS_CODE, [], [], DependencyGraph())
        )
        assert "job_001_carga_clientes" in stub.last_prompt

    def test_prompt_contains_sas_code(self) -> None:
        stub = _StubLLMClient()
        doc = JobDocumentor(llm_client=stub)
        asyncio.run(doc.generate_doc("job_001", SAS_CODE, [], [], DependencyGraph()))
        assert "clientes_filtrados" in stub.last_prompt

    def test_write_doc_creates_file(self, tmp_path: Path) -> None:
        result = JobDocResult(job_name="job_001", content="# job_001\n\n## Objetivo\nTeste.")
        doc = JobDocumentor(llm_client=_StubLLMClient())
        out = doc.write_doc(result, tmp_path / "jobs")
        assert out.exists()
        assert out.name == "job_001_README.md"

    def test_write_doc_content_correct(self, tmp_path: Path) -> None:
        content = "# job_001\n\n## Objetivo\nConteúdo de teste."
        result = JobDocResult(job_name="job_001", content=content)
        doc = JobDocumentor(llm_client=_StubLLMClient())
        out = doc.write_doc(result, tmp_path / "jobs")
        assert out.read_text() == content

    def test_graph_prereqs_in_prompt(self) -> None:
        """Prerequisitos do grafo devem aparecer no prompt."""
        stub = _StubLLMClient()
        doc = JobDocumentor(llm_client=stub)
        graph = DependencyGraph()
        graph.jobs["job_000"] = JobNode(job_name="job_000", path=Path("job_000.sas"))
        graph.jobs["job_001"] = JobNode(job_name="job_001", path=Path("job_001.sas"))
        graph.explicit_edges = [("job_001", "job_000")]
        asyncio.run(doc.generate_doc("job_001", SAS_CODE, [], [], graph))
        assert "job_000" in stub.last_prompt

    def test_graph_dependents_in_prompt(self) -> None:
        """Dependentes do grafo devem aparecer no prompt."""
        stub = _StubLLMClient()
        _run_doc(JobDocumentor(llm_client=stub))
        assert "job_002" in stub.last_prompt


# ---------------------------------------------------------------------------
# Tests: _merge_block_deps
# ---------------------------------------------------------------------------


class TestMergeBlockDeps:
    def test_merges_inputs_dedup(self) -> None:
        bd1 = BlockDeps(inputs=["A.T1", "A.T2"])
        bd2 = BlockDeps(inputs=["A.T2", "A.T3"])
        merged = _merge_block_deps([bd1, bd2])
        assert merged.inputs == ["A.T1", "A.T2", "A.T3"]

    def test_merges_outputs(self) -> None:
        bd1 = BlockDeps(outputs=["OUT1"])
        bd2 = BlockDeps(outputs=["OUT2"])
        merged = _merge_block_deps([bd1, bd2])
        assert set(merged.outputs) == {"OUT1", "OUT2"}

    def test_merges_macros(self) -> None:
        bd1 = BlockDeps(macros_called=["MACRO_A"])
        bd2 = BlockDeps(macros_called=["MACRO_B"], macros_defined=["MACRO_B"])
        merged = _merge_block_deps([bd1, bd2])
        assert "MACRO_A" in merged.macros_called
        assert "MACRO_B" in merged.macros_called
        assert "MACRO_B" in merged.macros_defined

    def test_empty_list_returns_empty(self) -> None:
        merged = _merge_block_deps([])
        assert merged.inputs == []
        assert merged.outputs == []


# ---------------------------------------------------------------------------
# Tests: build_job_doc_prompt
# ---------------------------------------------------------------------------


class TestBuildJobDocPrompt:
    def test_prompt_contains_job_name(self) -> None:
        p = build_job_doc_prompt("meu_job", "RUN;", [], [], [], [], [], [], [], [])
        assert "meu_job" in p

    def test_prompt_contains_sas_code(self) -> None:
        p = build_job_doc_prompt("j", "PROC SQL; QUIT;", [], [], [], [], [], [], [], [])
        assert "PROC SQL" in p

    def test_prompt_lists_inputs(self) -> None:
        p = build_job_doc_prompt("j", "", ["DS.IN1", "DS.IN2"], [], [], [], [], [], [], [])
        assert "DS.IN1" in p
        assert "DS.IN2" in p

    def test_prompt_lists_outputs(self) -> None:
        p = build_job_doc_prompt("j", "", [], ["WORK.OUT"], [], [], [], [], [], [])
        assert "WORK.OUT" in p

    def test_prompt_lists_prereqs(self) -> None:
        p = build_job_doc_prompt("j", "", [], [], [], [], [], [], ["job_000"], [])
        assert "job_000" in p

    def test_prompt_lists_dependents(self) -> None:
        p = build_job_doc_prompt("j", "", [], [], [], [], [], [], [], ["job_002"])
        assert "job_002" in p

    def test_prompt_includes_knowledge_context(self) -> None:
        ks_ctx = "PROC SORT: use orderBy()"
        p = build_job_doc_prompt("j", "", [], [], [], [], [], [], [], [], ks_ctx)
        assert ks_ctx in p

    def test_prompt_no_knowledge_context_when_empty(self) -> None:
        p = build_job_doc_prompt("j", "", [], [], [], [], [], [], [], [], "")
        assert "Knowledge Store" not in p

    def test_prompt_formats_constructs(self) -> None:
        crs = [ClassificationResult(construct_type="PROC_SQL", tier=Tier.RULE, confidence=0.9)]
        p = build_job_doc_prompt("j", "", [], [], [], [], [], crs, [], [])
        assert "PROC_SQL" in p
        assert "Rule" in p

    def test_prompt_no_header_tag(self) -> None:
        """O prompt não deve incluir `# job_name` — adicionado externamente pelo documentor."""
        p = build_job_doc_prompt("meu_job", "", [], [], [], [], [], [], [], [])
        assert "# meu_job" not in p
