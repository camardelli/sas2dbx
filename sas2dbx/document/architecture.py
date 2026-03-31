"""ArchitectureDocumentor — geração de ARCHITECTURE.md consolidado.

Não usa LLM. Consome DependencyGraph + list[MigrationResult] + job_docs
para gerar uma visão completa do projeto com grafo mermaid renderizável.
"""

from __future__ import annotations

import logging
from pathlib import Path

from sas2dbx.models.dependency_graph import DependencyGraph
from sas2dbx.models.migration_result import JobStatus, MigrationResult

logger = logging.getLogger(__name__)


class ArchitectureDocumentor:
    """Gera ARCHITECTURE.md consolidado do projeto SAS migrado.

    Não faz chamadas LLM. Todo conteúdo é derivado dos modelos do pipeline.
    """

    def generate_architecture_md(
        self,
        graph: DependencyGraph,
        migration_results: list[MigrationResult],
        job_docs: dict[str, str],
    ) -> str:
        """Retorna ARCHITECTURE.md completo como string.

        Args:
            graph: Grafo de dependências resolvido.
            migration_results: Resultados de migração por job.
            job_docs: Mapa job_name → conteúdo do README.md (pode ser vazio).

        Returns:
            Conteúdo markdown completo do ARCHITECTURE.md.
        """
        results_by_id = {r.job_id: r for r in migration_results}
        execution_order = graph.get_execution_order()

        sections: list[str] = [
            "# Arquitetura do Projeto SAS Migrado",
            "",
            _build_inventory(execution_order, results_by_id),
            _build_dependency_graph(graph, results_by_id),
            _build_dataset_map(graph),
            _build_macro_map(graph),
            _build_coverage_summary(execution_order, results_by_id),
        ]
        return "\n".join(s for s in sections if s)

    def write(self, content: str, output_dir: Path) -> Path:
        """Grava ARCHITECTURE.md em disco.

        Args:
            content: Conteúdo gerado por generate_architecture_md().
            output_dir: Diretório de destino.

        Returns:
            Caminho do arquivo gravado.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        out = output_dir / "ARCHITECTURE.md"
        out.write_text(content, encoding="utf-8")
        logger.info("ArchitectureDocumentor: ARCHITECTURE.md gravado em %s", out)
        return out


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------


def _build_inventory(
    execution_order: list[str],
    results_by_id: dict[str, MigrationResult],
) -> str:
    rows = [
        "## Inventário de Jobs", "",
        "| Job | Status | Confiança |",
        "|-----|--------|-----------|",
    ]
    for job_name in execution_order:
        result = results_by_id.get(job_name)
        if result:
            status = result.status.value
            confidence = f"{result.confidence:.0%}"
        else:
            status = "não migrado"
            confidence = "—"
        rows.append(f"| `{job_name}` | {status} | {confidence} |")
    return "\n".join(rows)


def _build_dependency_graph(
    graph: DependencyGraph,
    results_by_id: dict[str, MigrationResult],
) -> str:
    lines = ["## Grafo de Dependências", "", "```mermaid", "graph LR"]

    all_edges = graph.get_all_edges()
    if not all_edges:
        # Jobs sem dependências — lista isolada
        for job_name in graph.jobs:
            lines.append(f"  {_mermaid_id(job_name)}[{job_name}]")
    else:
        seen_nodes: set[str] = set()
        for dependent, prerequisite in sorted(all_edges):
            dep_id = _mermaid_id(dependent)
            pre_id = _mermaid_id(prerequisite)
            lines.append(f"  {pre_id}[{prerequisite}] --> {dep_id}[{dependent}]")
            seen_nodes.add(dependent)
            seen_nodes.add(prerequisite)
        # Jobs sem arestas (isolados)
        for job_name in graph.jobs:
            if job_name not in seen_nodes:
                lines.append(f"  {_mermaid_id(job_name)}[{job_name}]")

    lines.append("```")
    return "\n".join(lines)


def _build_dataset_map(graph: DependencyGraph) -> str:
    # Agrega produtores e consumidores por dataset
    producers: dict[str, list[str]] = {}
    consumers: dict[str, list[str]] = {}

    for job_name, node in graph.jobs.items():
        for ds in node.outputs:
            producers.setdefault(ds, []).append(job_name)
        for ds in node.inputs:
            consumers.setdefault(ds, []).append(job_name)

    all_datasets = sorted(set(producers) | set(consumers))
    if not all_datasets:
        return ""

    rows = [
        "## Mapa de Datasets",
        "",
        "| Dataset | Produzido por | Consumido por |",
        "|---------|--------------|---------------|",
    ]
    for ds in all_datasets:
        prod = ", ".join(f"`{j}`" for j in sorted(producers.get(ds, []))) or "—"
        cons = ", ".join(f"`{j}`" for j in sorted(consumers.get(ds, []))) or "—"
        rows.append(f"| `{ds}` | {prod} | {cons} |")
    return "\n".join(rows)


def _build_macro_map(graph: DependencyGraph) -> str:
    # Agrega onde macros são definidas e onde são chamadas
    defined_in: dict[str, list[str]] = {}
    called_in: dict[str, list[str]] = {}

    for job_name, node in graph.jobs.items():
        for m in node.macros_defined:
            defined_in.setdefault(m, []).append(job_name)
        for m in node.macros_called:
            called_in.setdefault(m, []).append(job_name)

    all_macros = sorted(set(defined_in) | set(called_in))
    if not all_macros:
        return ""

    rows = [
        "## Mapa de Macros",
        "",
        "| Macro | Definida em | Chamada em |",
        "|-------|------------|------------|",
    ]
    for macro in all_macros:
        defn = ", ".join(f"`{j}`" for j in sorted(defined_in.get(macro, []))) or "—"
        calls = ", ".join(f"`{j}`" for j in sorted(called_in.get(macro, []))) or "—"
        rows.append(f"| `{macro}` | {defn} | {calls} |")
    return "\n".join(rows)


def _build_coverage_summary(
    execution_order: list[str],
    results_by_id: dict[str, MigrationResult],
) -> str:
    total = len(execution_order)
    if total == 0:
        return ""

    def _status(j: str) -> JobStatus:
        return results_by_id.get(j, MigrationResult(job_id=j)).status

    done = sum(1 for j in execution_order if _status(j) == JobStatus.DONE)
    failed = sum(1 for j in execution_order if _status(j) == JobStatus.FAILED)
    not_migrated = total - done - failed

    done_results = [
        results_by_id[j]
        for j in execution_order
        if j in results_by_id and results_by_id[j].status == JobStatus.DONE
    ]
    avg_conf = (
        sum(r.confidence for r in done_results) / len(done_results)
        if done_results else 0.0
    )

    auto_pct = done / total if total else 0.0

    return "\n".join([
        "## Cobertura de Migração",
        "",
        "| Métrica | Valor |",
        "|---------|-------|",
        f"| Total de jobs | {total} |",
        f"| Migrados automaticamente | {done} ({auto_pct:.0%}) |",
        f"| Falharam | {failed} |",
        f"| Não migrados | {not_migrated} |",
        f"| Confiança média (jobs DONE) | {avg_conf:.0%} |",
    ])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mermaid_id(name: str) -> str:
    """Converte nome de job para ID válido em mermaid (sem hífens problemáticos)."""
    return name.replace("-", "_").replace(".", "_")
