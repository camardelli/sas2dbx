"""Modelos do grafo de dependências entre jobs SAS."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class JobNode:
    """Representa um job SAS e seus datasets de entrada/saída.

    Attributes:
        job_name: Nome do job (filename sem extensão).
        path: Caminho absoluto do arquivo .sas.
        inputs: Datasets lidos pelo job (normalizados para UPPER.UPPER).
        outputs: Datasets criados pelo job (normalizados para UPPER.UPPER).
        libnames_declared: Bibliotecas declaradas localmente no job.
        macros_called: Nomes de macros invocadas.
        macros_defined: Nomes de macros definidas neste job.
    """

    job_name: str
    path: Path
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    libnames_declared: list[str] = field(default_factory=list)
    macros_called: list[str] = field(default_factory=list)
    macros_defined: list[str] = field(default_factory=list)


@dataclass
class DependencyGraph:
    """Grafo de dependências entre jobs SAS.

    Attributes:
        jobs: Mapa job_name → JobNode.
        explicit_edges: Dependências explícitas via libnames.yaml (depends_on_jobs).
            Formato: (job_dependente, job_prerequisito).
        implicit_edges: Dependências detectadas por overlap de datasets.
            Formato: (job_dependente, job_prerequisito, dataset_name).
        global_libnames: LIBNAMEs extraídos do autoexec.sas que sobrepõem locais.
            Formato: libname → catalog_schema.
        warnings: Avisos de dependências implícitas que requerem validação humana.
    """

    jobs: dict[str, JobNode] = field(default_factory=dict)
    explicit_edges: list[tuple[str, str]] = field(default_factory=list)
    implicit_edges: list[tuple[str, str, str]] = field(default_factory=list)
    global_libnames: dict[str, str] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def get_implicit_dependencies(self) -> list[tuple[str, str, str]]:
        """Retorna lista de dependências implícitas detectadas.

        Returns:
            Lista de (job_dependente, job_prerequisito, dataset_name).
        """
        return list(self.implicit_edges)

    def get_all_edges(self) -> list[tuple[str, str]]:
        """Retorna todas as arestas (explicit + implicit) sem duplicatas."""
        all_edges: set[tuple[str, str]] = set(self.explicit_edges)
        all_edges.update((dep, prereq) for dep, prereq, _ in self.implicit_edges)
        return sorted(all_edges)

    def get_execution_order(self) -> list[str]:
        """Retorna jobs em ordem topológica de execução (Kahn's algorithm).

        Jobs sem dependências vêm primeiro. Se há ciclo, retorna o que foi
        possível ordenar e inclui os jobs restantes no final (com warning).

        Returns:
            Lista de job_names em ordem de execução sugerida.
        """
        all_job_names = list(self.jobs.keys())
        all_edges = self.get_all_edges()

        # Constrói grafo de prerequisitos: job -> set de jobs que devem vir antes
        predecessors: dict[str, set[str]] = {j: set() for j in all_job_names}
        for dependent, prerequisite in all_edges:
            if dependent in predecessors and prerequisite in all_job_names:
                predecessors[dependent].add(prerequisite)

        # Kahn's algorithm
        in_degree = {j: len(preds) for j, preds in predecessors.items()}
        queue = sorted(j for j, deg in in_degree.items() if deg == 0)
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for job, preds in predecessors.items():
                if node in preds:
                    preds.discard(node)
                    in_degree[job] -= 1
                    if in_degree[job] == 0:
                        queue.append(job)
                        queue.sort()

        # Jobs não alcançados (ciclo ou desconexos)
        remaining = [j for j in all_job_names if j not in result]
        result.extend(sorted(remaining))
        return result
