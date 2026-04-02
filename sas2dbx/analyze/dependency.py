"""Analisador de dependências entre jobs SAS.

Constrói um DependencyGraph a partir de uma lista de SASFiles, com suporte a:
  - autoexec.sas: LIBNAMEs globais que sobrepõem declarações locais
  - libnames.yaml: dependências manuais via campo depends_on_jobs
  - Dependências implícitas: jobs que compartilham datasets de saída/entrada
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import yaml

from sas2dbx.analyze.parser import BlockDeps, extract_block_deps
from sas2dbx.ingest.reader import read_sas_file, split_blocks
from sas2dbx.models.dependency_graph import DependencyGraph, JobNode
from sas2dbx.models.sas_ast import SASFile

logger = logging.getLogger(__name__)

# Libname declaration pattern (used in autoexec parsing)
_RE_LIBNAME_PATH = re.compile(
    r"^\s*LIBNAME\s+(\w+)\s+(?:'([^']*)'|\"([^\"]*)\"|(\S+))",
    re.IGNORECASE | re.MULTILINE,
)

# WORK library — session-scoped in SAS, não cria dependências persistentes entre jobs
_WORK_LIBRARIES = frozenset({"WORK"})


class DependencyAnalyzer:
    """Analisa dependências entre jobs SAS e constrói o grafo.

    Args:
        autoexec_path: Caminho opcional para autoexec.sas com LIBNAMEs globais.
        libnames_yaml: Caminho opcional para libnames.yaml com depends_on_jobs.
    """

    def __init__(
        self,
        autoexec_path: str | Path | None = None,
        libnames_yaml: str | Path | None = None,
    ) -> None:
        self._autoexec_path = Path(autoexec_path) if autoexec_path else None
        self._libnames_yaml = Path(libnames_yaml) if libnames_yaml else None
        self._global_libnames: dict[str, str] = {}
        self._manual_deps: dict[str, list[str]] = {}  # libname → [job_names]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(self, sas_files: list[SASFile]) -> DependencyGraph:
        """Analisa lista de SASFiles e retorna o grafo de dependências.

        Args:
            sas_files: Lista de SASFile (do scanner).

        Returns:
            DependencyGraph com jobs, edges e warnings.
        """
        # Reseta estado para garantir idempotência entre chamadas sucessivas
        self._global_libnames = {}
        self._manual_deps = {}

        # 1 — Carrega contexto global
        if self._autoexec_path:
            self._global_libnames = self._parse_autoexec(self._autoexec_path)
            logger.info(
                "Dependency: %d LIBNAME(s) extraídos do autoexec: %s",
                len(self._global_libnames),
                list(self._global_libnames.keys()),
            )

        if self._libnames_yaml:
            self._manual_deps = self._parse_libnames_yaml(self._libnames_yaml)
            logger.info(
                "Dependency: %d entrada(s) com depends_on_jobs em libnames.yaml",
                sum(len(v) for v in self._manual_deps.values()),
            )

        # 2 — Analisa cada job
        graph = DependencyGraph(global_libnames=dict(self._global_libnames))
        for sas_file in sas_files:
            node = self._analyze_job(sas_file)
            graph.jobs[node.job_name] = node

        # 3 — Detecta dependências explícitas (libnames.yaml)
        graph.explicit_edges = self._build_explicit_edges(graph.jobs)

        # 4 — Detecta dependências implícitas (overlap de datasets)
        graph.implicit_edges = self._build_implicit_edges(graph.jobs)

        # 5 — Gera warnings para implícitas
        for dep, prereq, ds in graph.implicit_edges:
            msg = (
                f"Dependência implícita detectada: '{dep}' lê '{ds}' "
                f"que é criado por '{prereq}' — requer validação humana"
            )
            graph.warnings.append(msg)
            logger.warning("Dependency: %s", msg)

        logger.info(
            "Dependency: grafo construído — %d job(s), %d aresta(s) explícita(s), "
            "%d implícita(s)",
            len(graph.jobs),
            len(graph.explicit_edges),
            len(graph.implicit_edges),
        )
        return graph

    # ------------------------------------------------------------------
    # Internal — autoexec + libnames.yaml
    # ------------------------------------------------------------------

    def _parse_autoexec(self, path: Path) -> dict[str, str]:
        """Extrai mapeamento LIBNAME→path/catalog do autoexec.sas.

        Returns:
            Dict libname_upper → path_string.
        """
        if not path.exists():
            logger.warning("Dependency: autoexec não encontrado: %s", path)
            return {}

        code, _ = read_sas_file(path)
        libnames: dict[str, str] = {}

        for m in _RE_LIBNAME_PATH.finditer(code):
            name = m.group(1).upper()
            # Captura path: grupo 2 (aspas simples), 3 (aspas duplas) ou 4 (sem aspas)
            lib_path = m.group(2) or m.group(3) or m.group(4) or ""
            libnames[name] = lib_path.strip()

        return libnames

    def _parse_libnames_yaml(self, path: Path) -> dict[str, list[str]]:
        """Extrai mapeamento libname → [job_names] do libnames.yaml.

        Lê o campo `depends_on_jobs` de cada entrada.

        Returns:
            Dict libname_upper → lista de job_names que são prerequisitos.
        """
        if not path.exists():
            logger.warning("Dependency: libnames.yaml não encontrado: %s", path)
            return {}

        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        result: dict[str, list[str]] = {}
        for libname, config in data.items():
            if not isinstance(config, dict):
                continue
            deps = config.get("depends_on_jobs") or []
            if deps:
                result[libname.upper()] = [str(j) for j in deps]

        return result

    # ------------------------------------------------------------------
    # Internal — job analysis
    # ------------------------------------------------------------------

    def _analyze_job(self, sas_file: SASFile) -> JobNode:
        """Lê e analisa um arquivo SAS, retornando JobNode."""
        job_name = sas_file.path.stem
        node = JobNode(job_name=job_name, path=sas_file.path)

        try:
            code, _ = read_sas_file(sas_file.path)
            blocks = split_blocks(code, source_file=sas_file.path)
        except Exception as exc:
            logger.error("Dependency: erro ao ler %s — %s", sas_file.path, exc)
            return node

        inputs_set: set[str] = set()
        outputs_set: set[str] = set()
        libnames_set: set[str] = set()
        macros_called_set: set[str] = set()
        macros_defined_set: set[str] = set()

        for block in blocks:
            deps: BlockDeps = extract_block_deps(block)
            inputs_set.update(deps.inputs)
            outputs_set.update(deps.outputs)
            libnames_set.update(deps.libnames_declared)
            macros_called_set.update(deps.macros_called)
            macros_defined_set.update(deps.macros_defined)

        # Verifica sobreposição de LIBNAMEs do autoexec
        for lib in list(libnames_set):
            if lib in self._global_libnames:
                logger.warning(
                    "Dependency: LIBNAME '%s' em '%s' sobreposto pelo autoexec.sas",
                    lib,
                    job_name,
                )

        node.inputs = sorted(inputs_set)
        node.outputs = sorted(outputs_set)
        node.libnames_declared = sorted(libnames_set)
        node.macros_called = sorted(
            m for m in macros_called_set if m not in macros_defined_set
        )
        node.macros_defined = sorted(macros_defined_set)

        logger.debug(
            "Dependency: job '%s' — %d inputs, %d outputs, %d macros",
            job_name,
            len(node.inputs),
            len(node.outputs),
            len(node.macros_called),
        )
        return node

    # ------------------------------------------------------------------
    # Internal — edge building
    # ------------------------------------------------------------------

    def _build_explicit_edges(
        self, jobs: dict[str, JobNode]
    ) -> list[tuple[str, str]]:
        """Constrói arestas explícitas via libnames.yaml (depends_on_jobs).

        Para cada job que declara um LIBNAME com depends_on_jobs, adiciona
        aresta (job_atual → prerequisito) para cada prerequisito listado.
        """
        edges: list[tuple[str, str]] = []
        job_names = set(jobs.keys())

        for job_name, node in jobs.items():
            for lib in node.libnames_declared:
                prereq_jobs = self._manual_deps.get(lib, [])
                for prereq in prereq_jobs:
                    if prereq in job_names and prereq != job_name:
                        edge = (job_name, prereq)
                        if edge not in edges:
                            edges.append(edge)
                            logger.debug(
                                "Dependency: aresta explícita %s → %s (via LIBNAME %s)",
                                job_name, prereq, lib,
                            )

        return edges

    def _build_implicit_edges(
        self, jobs: dict[str, JobNode]
    ) -> list[tuple[str, str, str]]:
        """Detecta dependências implícitas por overlap de datasets.

        Se job A cria dataset X e job B lê dataset X, então B depende de A.
        Datasets da biblioteca WORK são excluídos (session-scoped).

        Returns:
            Lista de (job_dependente, job_prerequisito, dataset_name).
        """
        # Mapa dataset → lista de jobs que o criam.
        # Apenas datasets qualificados (lib.tabela) criam arestas persistentes.
        # Nomes sem qualificador de biblioteca em SAS são WORK-scoped (sessão)
        # e não criam dependências reais entre jobs.
        producers: dict[str, list[str]] = {}
        for job_name, node in jobs.items():
            for ds in node.outputs:
                if "." not in ds:
                    continue  # sem qualificador → WORK implícito, ignora
                lib = ds.split(".")[0]
                if lib in _WORK_LIBRARIES:
                    continue
                producers.setdefault(ds, []).append(job_name)

        implicit: list[tuple[str, str, str]] = []
        seen: set[tuple[str, str, str]] = set()

        for job_name, node in jobs.items():
            for ds in node.inputs:
                if "." not in ds:
                    continue  # sem qualificador → WORK implícito, ignora
                lib = ds.split(".")[0]
                if lib in _WORK_LIBRARIES:
                    continue
                if ds in producers:
                    for producer in producers[ds]:
                        if producer != job_name:
                            triple = (job_name, producer, ds)
                            if triple not in seen:
                                seen.add(triple)
                                implicit.append(triple)

        return implicit
