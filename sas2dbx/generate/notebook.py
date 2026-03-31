"""Generator de notebooks Databricks — CellModel intermediário + renderers.

Fluxo:
  1. `CellModel` representa o notebook de forma agnóstica de formato.
  2. `DatabricksPyRenderer` serializa para `.py` (Databricks source format).
  3. `JupyterIpynbRenderer` serializa para `.ipynb` (JSON com metadata Databricks).
  4. `NotebookGenerator` seleciona o renderer correto pelo `notebook_format`.

Decisão arquitetural R10: CellModel intermediário evita duplicação de lógica
de ordenação, rastreabilidade e header entre os dois formatos de saída.
"""

from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

from sas2dbx.models.migration_result import SASOrigin

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CellModel — representação intermediária agnóstica de formato
# ---------------------------------------------------------------------------


class CellType(StrEnum):
    """Tipo de célula no notebook."""

    CODE = "code"
    MARKDOWN = "markdown"


@dataclass
class Cell:
    """Célula individual de um notebook.

    Attributes:
        cell_type: CODE ou MARKDOWN.
        source: Conteúdo da célula.
        sas_origin: Rastreabilidade ao bloco SAS de origem (None para células geradas).
        order: Posição na sequência (0-indexed).
    """

    cell_type: CellType
    source: str
    sas_origin: SASOrigin | None = None
    order: int = 0


@dataclass
class CellModel:
    """Representação intermediária de um notebook gerado.

    Attributes:
        job_name: Nome do job SAS (filename sem extensão).
        sas_source_file: Nome do arquivo SAS de origem.
        migration_date: Data da migração (ISO 8601).
        confidence: Score médio de confiança (0.0–1.0).
        warnings: Avisos gerados durante a transpilação.
        cells: Células na ordem de execução.
    """

    job_name: str
    sas_source_file: str
    migration_date: str
    confidence: float
    warnings: list[str]
    cells: list[Cell] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Renderer interface
# ---------------------------------------------------------------------------


class NotebookRenderer(ABC):
    """Interface base para renderers de notebook."""

    @abstractmethod
    def render(self, model: CellModel, output_path: Path) -> None:
        """Serializa o CellModel para disco no formato do renderer.

        Args:
            model: CellModel a serializar.
            output_path: Caminho de destino (sem extensão — o renderer adiciona a correta).
        """


# ---------------------------------------------------------------------------
# DatabricksPyRenderer — formato .py source
# ---------------------------------------------------------------------------

_PY_MAGIC_SEPARATOR = "# COMMAND ----------\n"
_PY_MAGIC_MD_PREFIX = "# MAGIC "


class DatabricksPyRenderer(NotebookRenderer):
    """Renderiza CellModel para .py no Databricks source format.

    Formato:
      - Arquivo começa com `# Databricks notebook source`
      - Células separadas por `# COMMAND ----------`
      - Células markdown usam `# MAGIC %md` + linhas prefixadas com `# MAGIC `
      - Células de código são escritas diretamente
    """

    def render(self, model: CellModel, output_path: Path) -> None:
        output_path = output_path.with_suffix(".py")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        lines: list[str] = ["# Databricks notebook source\n"]

        header_cell = _build_header_cell(model)
        imports_cell = _build_imports_cell(model)

        all_cells = [header_cell, imports_cell] + sorted(model.cells, key=lambda c: c.order)

        for cell in all_cells:
            lines.append(_PY_MAGIC_SEPARATOR)
            if cell.cell_type == CellType.MARKDOWN:
                lines.append("# MAGIC %md\n")
                for md_line in cell.source.splitlines():
                    lines.append(f"{_PY_MAGIC_MD_PREFIX}{md_line}\n")
            else:
                lines.append(cell.source.rstrip("\n") + "\n")

        output_path.write_text("".join(lines), encoding="utf-8")
        logger.info("DatabricksPyRenderer: notebook gravado em %s", output_path)


# ---------------------------------------------------------------------------
# JupyterIpynbRenderer — formato .ipynb
# ---------------------------------------------------------------------------


class JupyterIpynbRenderer(NotebookRenderer):
    """Renderiza CellModel para .ipynb (JSON) com metadata Databricks.

    O metadata do notebook inclui a chave `application/vnd.databricks.v1+notebook`
    com `defaultLanguage: python` e `notebookMetadata` para compatibilidade com
    Databricks Repos e import direto via UI.
    """

    def render(self, model: CellModel, output_path: Path) -> None:
        output_path = output_path.with_suffix(".ipynb")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        header_cell = _build_header_cell(model)
        imports_cell = _build_imports_cell(model)
        all_cells = [header_cell, imports_cell] + sorted(model.cells, key=lambda c: c.order)

        ipynb_cells = []
        for cell in all_cells:
            if cell.cell_type == CellType.MARKDOWN:
                ipynb_cells.append({
                    "cell_type": "markdown",
                    "metadata": {},
                    "source": _to_ipynb_source(cell.source),
                })
            else:
                ipynb_cells.append({
                    "cell_type": "code",
                    "execution_count": None,
                    "metadata": {},
                    "outputs": [],
                    "source": _to_ipynb_source(cell.source),
                })

        notebook = {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {"name": "python", "version": "3.11.0"},
                "application/vnd.databricks.v1+notebook": {
                    "defaultLanguage": "python",
                    "notebookMetadata": {
                        "pythonIndentUnit": 4,
                    },
                    "notebookName": model.job_name,
                    "cluster_source": "NEW",
                },
            },
            "cells": ipynb_cells,
        }

        output_path.write_text(
            json.dumps(notebook, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("JupyterIpynbRenderer: notebook gravado em %s", output_path)


# ---------------------------------------------------------------------------
# NotebookGenerator — entry point pública
# ---------------------------------------------------------------------------


class NotebookGenerator:
    """Seleciona o renderer pelo formato configurado e gera o notebook.

    Args:
        notebook_format: "py" para Databricks source format, "ipynb" para Jupyter.
    """

    _RENDERERS: dict[str, type[NotebookRenderer]] = {
        "py": DatabricksPyRenderer,
        "ipynb": JupyterIpynbRenderer,
    }

    def __init__(self, notebook_format: str = "py") -> None:
        renderer_cls = self._RENDERERS.get(notebook_format)
        if renderer_cls is None:
            raise ValueError(
                f"notebook_format inválido: '{notebook_format}'. "
                f"Valores aceitos: {list(self._RENDERERS)}"
            )
        self._renderer: NotebookRenderer = renderer_cls()
        self._format = notebook_format

    def generate(self, model: CellModel, output_path: Path) -> Path:
        """Gera o notebook a partir do CellModel.

        Args:
            model: CellModel a serializar.
            output_path: Caminho destino (extensão adicionada pelo renderer).

        Returns:
            Caminho efetivo do arquivo gerado (com extensão correta).
        """
        self._renderer.render(model, output_path)
        return output_path.with_suffix(f".{self._format}")


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------


def _build_header_cell(model: CellModel) -> Cell:
    """Gera a célula de header com metadata do job."""
    warnings_text = "\n".join(f"- {w}" for w in model.warnings) if model.warnings else "_nenhum_"
    source = (
        f"# {model.job_name}\n"
        f"**Migrado de:** {model.sas_source_file}  \n"
        f"**Data migração:** {model.migration_date}  \n"
        f"**Confiança:** {model.confidence:.2f}  \n"
        f"**Warnings:**  \n{warnings_text}"
    )
    return Cell(cell_type=CellType.MARKDOWN, source=source, order=-2)


def _build_imports_cell(model: CellModel) -> Cell:
    """Gera célula de imports detectando F e Window no código das células."""
    all_code = "\n".join(
        c.source for c in model.cells if c.cell_type == CellType.CODE
    )
    imports: list[str] = []
    if re.search(r"\bF\s*\.\s*\w+\s*\(", all_code):
        imports.append("import pyspark.sql.functions as F")
    if re.search(r"\bWindow\s*\.", all_code):
        imports.append("from pyspark.sql.window import Window")

    source = "\n".join(imports) if imports else "# Sem imports adicionais necessários"
    return Cell(cell_type=CellType.CODE, source=source, order=-1)


def _to_ipynb_source(text: str) -> list[str]:
    """Converte texto em lista de linhas com `\\n` terminador (formato ipynb)."""
    lines = text.splitlines()
    return [line + "\n" for line in lines[:-1]] + ([lines[-1]] if lines else [])


