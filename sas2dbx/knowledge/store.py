"""KnowledgeStore — API de acesso ao knowledge store (lookup + retrieval).

Lê sempre de mappings/merged/ (curated > generated).
Fallback para mappings/generated/ se merged/ não existir ainda.

On-demand harvest (H.2):
  Quando llm_client é fornecido, lookups *_or_harvest() tentam gerar
  a entrada via LLM se não encontrada, gravam em generated/ e retornam.
  Sem llm_client, comportamento idêntico aos lookups simples.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class KnowledgeStore:
    """
    RAG file-based knowledge store for SAS→PySpark mappings and references.

    Priority for all lookups: mappings/merged/ → mappings/generated/
    (merged/ already contains curated > generated, but generated/ is the
    ultimate fallback when build-mappings hasn't run yet).

    Args:
        base_path: Raiz do knowledge store.
        llm_client: LLMClient opcional. Quando fornecido, habilita on-demand
            harvest: se uma chave não for encontrada nos YAMLs, chama o LLM
            para gerar o mapeamento, grava em generated/ e retorna.
            Sem llm_client, os métodos *_or_harvest() se comportam
            exatamente como os lookups simples (retornam None se não acham).
    """

    def __init__(
        self,
        base_path: str | Path = "./knowledge",
        llm_client: object | None = None,
    ) -> None:
        self.base_path = Path(base_path)
        self._mappings_merged = self.base_path / "mappings" / "merged"
        self._mappings_generated = self.base_path / "mappings" / "generated"
        self._sas_reference = self.base_path / "sas_reference"
        self._pyspark_reference = self.base_path / "pyspark_reference"
        self._custom = self.base_path / "custom"
        self._cache: dict[str, dict] = {}  # lazy cache: path_str → parsed YAML
        self._llm_client = llm_client
        self._harvest_attempted: set[str] = set()  # cache de negativas por sessão

    # -------------------------------------------------------------------------
    # Lookup — deterministic, no LLM
    # -------------------------------------------------------------------------

    def lookup_function(self, func_name: str) -> dict[str, Any] | None:
        """
        Lookup a SAS function mapping.
        Reads from merged/ (curated > generated) with fallback to generated/.
        Returns None if not found.
        """
        return self._lookup_in_mapping("functions_map.yaml", func_name.upper())

    def lookup_proc(self, proc_name: str) -> dict[str, Any] | None:
        """
        Lookup a PROC mapping (approach: rule/llm/manual).
        Returns None if not found.
        """
        return self._lookup_in_mapping("proc_map.yaml", proc_name.upper())

    def lookup_sql_dialect(self, construct: str) -> dict[str, Any] | None:
        """
        Lookup a SQL dialect difference (SAS SQL → Spark SQL).
        Returns None if not found.
        """
        return self._lookup_in_mapping("sql_dialect_map.yaml", construct.upper())

    def lookup_format(self, format_name: str) -> dict[str, Any] | None:
        """Lookup a SAS format → Python/Spark format mapping."""
        return self._lookup_in_mapping("formats_map.yaml", format_name.upper())

    # -------------------------------------------------------------------------
    # Lookup com fallback on-demand (H.2)
    # -------------------------------------------------------------------------

    def lookup_function_or_harvest(self, func_name: str) -> dict[str, Any] | None:
        """Lookup de função SAS com harvest on-demand se não encontrado.

        Fluxo: lookup simples → se None e llm_client configurado → harvest LLM
        → grava em generated/ → invalida cache → retorna.

        Args:
            func_name: Nome da função SAS (case-insensitive).

        Returns:
            Dict do mapeamento ou None se não encontrado e harvest falhou/indisponível.
        """
        result = self.lookup_function(func_name)
        if result is not None:
            return result
        return self._on_demand_harvest("function", func_name.upper(), "functions_map.yaml")

    def lookup_proc_or_harvest(self, proc_name: str) -> dict[str, Any] | None:
        """Lookup de PROC com harvest on-demand se não encontrado."""
        result = self.lookup_proc(proc_name)
        if result is not None:
            return result
        return self._on_demand_harvest("proc", proc_name.upper(), "proc_map.yaml")

    def lookup_sql_dialect_or_harvest(self, construct: str) -> dict[str, Any] | None:
        """Lookup de regra SQL dialect com harvest on-demand se não encontrado."""
        result = self.lookup_sql_dialect(construct)
        if result is not None:
            return result
        return self._on_demand_harvest("sql_dialect", construct.upper(), "sql_dialect_map.yaml")

    def lookup_format_or_harvest(self, format_name: str) -> dict[str, Any] | None:
        """Lookup de formato SAS com harvest on-demand se não encontrado."""
        result = self.lookup_format(format_name)
        if result is not None:
            return result
        return self._on_demand_harvest("format", format_name.upper(), "formats_map.yaml")

    # -------------------------------------------------------------------------
    # Retrieval — markdown reference docs
    # -------------------------------------------------------------------------

    def get_reference(self, source: str, category: str, name: str) -> str | None:
        """
        Retrieve a markdown reference doc for context injection into LLM prompts.

        Args:
            source: 'sas' | 'pyspark'
            category: e.g. 'procs', 'functions', 'dataframe_api', 'spark_sql'
            name: e.g. 'proc_means', 'months_between', 'groupby_agg'

        Returns:
            Markdown content as string, or None if file doesn't exist.
        """
        if source == "sas":
            ref_path = self._sas_reference / category / f"{name}.md"
        elif source == "pyspark":
            ref_path = self._pyspark_reference / category / f"{name}.md"
        else:
            logger.warning("Unknown reference source: %r. Use 'sas' or 'pyspark'.", source)
            return None

        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8")

        logger.debug("Reference not found: %s", ref_path)
        return None

    # -------------------------------------------------------------------------
    # Custom environment
    # -------------------------------------------------------------------------

    def get_custom(self, name: str) -> dict[str, Any]:
        """
        Load a custom environment file (libnames, macros, conventions).

        Args:
            name: filename without extension (e.g. 'libnames', 'macros')

        Returns:
            Parsed YAML as dict, or empty dict if file doesn't exist.
        """
        custom_file = self._custom / f"{name}.yaml"
        if custom_file.exists():
            with open(custom_file, encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    def _load_mapping(self, directory: Path, filename: str) -> dict[str, Any]:
        """Carrega YAML com cache em memória. Lê do disco apenas na primeira chamada."""
        cache_key = str(directory / filename)
        if cache_key not in self._cache:
            mapping_file = directory / filename
            if mapping_file.exists():
                with open(mapping_file, encoding="utf-8") as f:
                    self._cache[cache_key] = yaml.safe_load(f) or {}
            else:
                self._cache[cache_key] = {}
        return self._cache[cache_key]

    def _lookup_in_mapping(self, filename: str, key: str) -> dict[str, Any] | None:
        """
        Lookup a key in a mapping YAML file (com cache em memória).

        Search order:
          1. mappings/merged/  — ground truth (curated > generated)
          2. mappings/generated/ — fallback if build-mappings hasn't run yet

        Returns the entry dict, or None if not found in either.
        """
        for directory in (self._mappings_merged, self._mappings_generated):
            data = self._load_mapping(directory, filename)
            if key in data:
                return data[key]
        return None

    def invalidate_cache(self) -> None:
        """Limpa o cache em memória. Usar após build-mappings ou edição manual dos YAMLs."""
        self._cache.clear()

    # -------------------------------------------------------------------------
    # Internal — on-demand harvest (H.2)
    # -------------------------------------------------------------------------

    def _on_demand_harvest(
        self,
        category: str,
        key: str,
        filename: str,
    ) -> dict[str, Any] | None:
        """Tenta harvest on-demand de uma entrada individual via LLM.

        Guards:
          - LLM client não configurado → retorna None silenciosamente
          - Já tentou este key nesta sessão → retorna None (cache de negativas)
          - LLM falhou ou retornou YAML inválido → loga warning, retorna None

        Não levanta exceção — falha silenciosa é intencional para não
        interromper o pipeline de transpilação.
        """
        if self._llm_client is None:
            return None

        cache_key = f"{category}:{key}"
        if cache_key in self._harvest_attempted:
            return None

        self._harvest_attempted.add(cache_key)

        logger.info(
            "KnowledgeStore: on-demand harvest para %s '%s'...", category, key
        )

        try:
            from sas2dbx.knowledge.populate.llm_harvester import LLMSingleHarvester

            harvester = LLMSingleHarvester(self._llm_client)
            entry = harvester.harvest_single_sync(category=category, key=key)

            if entry is None:
                logger.warning(
                    "KnowledgeStore: on-demand harvest retornou None para %s '%s'",
                    category, key,
                )
                return None

            self._append_to_generated(filename, key, entry)
            self.invalidate_cache()

            logger.info(
                "KnowledgeStore: on-demand harvest OK — %s '%s' (confidence=%.2f)",
                category, key, entry.get("confidence", 0),
            )
            return entry

        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "KnowledgeStore: on-demand harvest falhou para %s '%s': %s",
                category, key, exc,
            )
            return None

    def _append_to_generated(self, filename: str, key: str, entry: dict) -> None:
        """Adiciona ou atualiza uma entrada em generated/ sem sobrescrever outras.

        Thread-safe: usa read-modify-write com replace atômico via os.replace().

        Args:
            filename: Nome do arquivo YAML (ex: 'functions_map.yaml').
            key: Chave da entrada (ex: 'PRXMATCH').
            entry: Dict com o mapeamento validado.
        """
        path = self._mappings_generated / filename
        path.parent.mkdir(parents=True, exist_ok=True)

        existing: dict = {}
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    loaded = yaml.safe_load(f)
                    if isinstance(loaded, dict):
                        existing = loaded
            except yaml.YAMLError:
                logger.warning(
                    "KnowledgeStore: falha ao ler %s — sobrescrevendo", path
                )

        existing[key] = entry

        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            yaml.dump(
                existing,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=True,
            )
        os.replace(tmp, path)
