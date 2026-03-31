"""Gerador de Databricks Workflow definition (YAML/JSON).

Transforma o DependencyGraph numa definição de workflow compatível com
Databricks Workflows API, onde cada job SAS vira uma task com notebook_task
e as dependências são representadas pelo campo depends_on.

Formato de saída:
  name: sas_migration_pipeline
  tasks:
    - task_key: job_001
      notebook_task:
        notebook_path: /Repos/migrated/job_001
      depends_on: []
    - task_key: job_002
      notebook_task:
        notebook_path: /Repos/migrated/job_002
      depends_on:
        - task_key: job_001
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

import yaml

from sas2dbx.models.dependency_graph import DependencyGraph

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class WorkflowConfig:
    """Configuração do gerador de workflow.

    Attributes:
        pipeline_name: Nome do pipeline no Databricks Workflows.
        notebook_base_path: Prefixo do path dos notebooks (ex: /Repos/migrated).
        output_format: "yaml" ou "json".
    """

    pipeline_name: str = "sas_migration_pipeline"
    notebook_base_path: str = "/Repos/migrated"
    output_format: str = "yaml"


# ---------------------------------------------------------------------------
# WorkflowGenerator
# ---------------------------------------------------------------------------


class WorkflowGenerator:
    """Gera definição de Databricks Workflow a partir do DependencyGraph.

    Args:
        config: Configurações do workflow gerado.
    """

    def __init__(self, config: WorkflowConfig | None = None) -> None:
        self._config = config or WorkflowConfig()

    def generate(self, graph: DependencyGraph, output_path: Path) -> Path:
        """Serializa o DependencyGraph como workflow Databricks.

        Args:
            graph: Grafo de dependências já resolvido.
            output_path: Caminho destino sem extensão (extensão adicionada conforme formato).

        Returns:
            Caminho efetivo do arquivo gravado.
        """
        definition = self._build_definition(graph)

        fmt = self._config.output_format.lower()
        if fmt not in ("yaml", "json"):
            logger.warning(
                "WorkflowGenerator: output_format desconhecido '%s' — usando yaml",
                self._config.output_format,
            )
        if fmt == "json":
            out = output_path.with_suffix(".json")
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                json.dumps(definition, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        else:
            out = output_path.with_suffix(".yaml")
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                yaml.dump(
                    definition,
                    default_flow_style=False,
                    allow_unicode=True,
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

        logger.info(
            "WorkflowGenerator: workflow gravado em %s (%d tasks)",
            out,
            len(definition["tasks"]),
        )
        return out

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_definition(self, graph: DependencyGraph) -> dict:
        """Constrói o dict de definição do workflow."""
        execution_order = graph.get_execution_order()
        all_edges = graph.get_all_edges()

        # Pré-computa predecessores por job
        predecessors: dict[str, list[str]] = {j: [] for j in execution_order}
        for dependent, prerequisite in all_edges:
            if dependent in predecessors:
                if prerequisite not in predecessors[dependent]:
                    predecessors[dependent].append(prerequisite)

        tasks = []
        for job_name in execution_order:
            deps = sorted(predecessors.get(job_name, []))
            task: dict = {
                "task_key": job_name,
                "notebook_task": {
                    "notebook_path": f"{self._config.notebook_base_path}/{job_name}",
                },
            }
            if deps:
                task["depends_on"] = [{"task_key": d} for d in deps]
            else:
                task["depends_on"] = []
            tasks.append(task)

        return {
            "name": self._config.pipeline_name,
            "tasks": tasks,
        }
