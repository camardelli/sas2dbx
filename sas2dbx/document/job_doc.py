"""JobDocumentor — geração de README.md por job SAS via LLM.

Consome metadata já extraídas pelo pipeline (parser, classifier, dependency graph)
e chama o LLM para produzir documentação técnica estruturada.

Decisões arquiteturais:
  R11: generate_doc() é async; generate_doc_sync() para uso em CLI não-async.
  R12: KnowledgeStore é injeção opcional — sem KS o prompt vai sem contexto extra.
  R14: Testes injetam _StubLLMClient — zero chamadas reais ao Claude.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.analyze.parser import BlockDeps
from sas2dbx.document.prompts import build_job_doc_prompt
from sas2dbx.models.dependency_graph import DependencyGraph
from sas2dbx.models.sas_ast import ClassificationResult
from sas2dbx.transpile.llm.client import LLMClient, LLMResponse

logger = logging.getLogger(__name__)

# Tipo mínimo de KnowledgeStore para evitar import circular — duck typing
_KSLike = object  # qualquer objeto com lookup_function/get_reference


@dataclass
class JobDocResult:
    """Resultado da documentação de um job.

    Attributes:
        job_name: Nome do job documentado.
        content: Conteúdo markdown completo do README.
        tokens_used: Tokens consumidos na chamada LLM (0 se sem LLM).
        from_llm: True se o conteúdo veio de uma chamada real ao LLM.
    """

    job_name: str
    content: str
    tokens_used: int = 0
    from_llm: bool = False


class JobDocumentor:
    """Gera README.md por job SAS usando LLM + metadata do pipeline.

    Args:
        llm_client: Cliente LLM para chamadas ao Claude.
        knowledge_store: Knowledge Store opcional para enriquecer o prompt.
            Se None, o prompt é enviado sem contexto técnico adicional.
    """

    def __init__(
        self,
        llm_client: LLMClient,
        knowledge_store: object | None = None,
    ) -> None:
        self._llm = llm_client
        self._ks = knowledge_store

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def generate_doc(
        self,
        job_name: str,
        sas_code: str,
        block_deps: list[BlockDeps],
        classification_results: list[ClassificationResult],
        graph: DependencyGraph,
    ) -> JobDocResult:
        """Gera o README.md completo para um job SAS.

        Args:
            job_name: Nome do job (filename sem extensão).
            sas_code: Código SAS completo do job.
            block_deps: Lista de BlockDeps extraídas pelo parser.
            classification_results: Classificações por bloco.
            graph: Grafo de dependências completo do projeto.

        Returns:
            JobDocResult com o conteúdo markdown e metadata da chamada.
        """
        merged = _merge_block_deps(block_deps)
        prereqs, dependents = _resolve_graph_context(job_name, graph)
        knowledge_context = self._build_knowledge_context(merged, classification_results)

        prompt = build_job_doc_prompt(
            job_name=job_name,
            sas_code=sas_code,
            inputs=merged.inputs,
            outputs=merged.outputs,
            libnames=merged.libnames_declared,
            macros_called=merged.macros_called,
            macros_defined=merged.macros_defined,
            constructs=classification_results,
            prereqs=prereqs,
            dependents=dependents,
            knowledge_context=knowledge_context,
        )

        logger.info("JobDocumentor: gerando doc para '%s'", job_name)
        response: LLMResponse = await self._llm.complete(prompt)

        content = f"# {job_name}\n\n{response.content.strip()}\n"
        return JobDocResult(
            job_name=job_name,
            content=content,
            tokens_used=response.tokens_used,
            from_llm=True,
        )

    def generate_doc_sync(
        self,
        job_name: str,
        sas_code: str,
        block_deps: list[BlockDeps],
        classification_results: list[ClassificationResult],
        graph: DependencyGraph,
    ) -> JobDocResult:
        """Versão síncrona de generate_doc() — conveniência para CLI não-async."""
        return asyncio.run(
            self.generate_doc(
                job_name=job_name,
                sas_code=sas_code,
                block_deps=block_deps,
                classification_results=classification_results,
                graph=graph,
            )
        )

    def write_doc(self, result: JobDocResult, output_dir: Path) -> Path:
        """Grava o README.md do job em disco.

        Args:
            result: JobDocResult com o conteúdo gerado.
            output_dir: Diretório de destino.

        Returns:
            Caminho do arquivo gravado.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        out = output_dir / f"{result.job_name}_README.md"
        out.write_text(result.content, encoding="utf-8")
        logger.info("JobDocumentor: README gravado em %s", out)
        return out

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_knowledge_context(
        self,
        merged: BlockDeps,
        classification_results: list[ClassificationResult],
    ) -> str:
        """Monta contexto do Knowledge Store para injeção no prompt.

        Retorna string vazia se KS não disponível (graceful degradation — R12).
        """
        if self._ks is None:
            return ""

        sections: list[str] = []

        # Lookup de funções/construtos detectados
        for cr in classification_results:
            lookup = getattr(self._ks, "lookup_proc", None)
            if callable(lookup):
                entry = lookup(cr.construct_type)
                if entry and entry.get("notes"):
                    sections.append(f"- {cr.construct_type}: {entry['notes']}")

        return "\n".join(sections)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _merge_block_deps(block_deps: list[BlockDeps]) -> BlockDeps:
    """Agrega BlockDeps de múltiplos blocos num único BlockDeps do job."""
    merged = BlockDeps()
    for bd in block_deps:
        for ds in bd.inputs:
            if ds not in merged.inputs:
                merged.inputs.append(ds)
        for ds in bd.outputs:
            if ds not in merged.outputs:
                merged.outputs.append(ds)
        for lb in bd.libnames_declared:
            if lb not in merged.libnames_declared:
                merged.libnames_declared.append(lb)
        for mc in bd.macros_called:
            if mc not in merged.macros_called:
                merged.macros_called.append(mc)
        for md in bd.macros_defined:
            if md not in merged.macros_defined:
                merged.macros_defined.append(md)
    return merged


def _resolve_graph_context(
    job_name: str, graph: DependencyGraph
) -> tuple[list[str], list[str]]:
    """Extrai prerequisitos e dependentes do grafo para o job dado."""
    all_edges = graph.get_all_edges()
    prereqs = sorted(prereq for dep, prereq in all_edges if dep == job_name)
    dependents = sorted(dep for dep, prereq in all_edges if prereq == job_name)
    return prereqs, dependents
