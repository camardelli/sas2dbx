"""KnowledgeStore — API de acesso ao knowledge store (lookup + retrieval).

Lê sempre de mappings/merged/ (curated > generated).
Fallback para mappings/generated/ se merged/ não existir ainda.
"""

from __future__ import annotations

import logging
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
    """

    def __init__(self, base_path: str | Path = "./knowledge") -> None:
        self.base_path = Path(base_path)
        self._mappings_merged = self.base_path / "mappings" / "merged"
        self._mappings_generated = self.base_path / "mappings" / "generated"
        self._sas_reference = self.base_path / "sas_reference"
        self._pyspark_reference = self.base_path / "pyspark_reference"
        self._custom = self.base_path / "custom"

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

    def _lookup_in_mapping(self, filename: str, key: str) -> dict[str, Any] | None:
        """
        Lookup a key in a mapping YAML file.

        Search order:
          1. mappings/merged/  — ground truth (curated > generated)
          2. mappings/generated/ — fallback if build-mappings hasn't run yet

        Returns the entry dict, or None if not found in either.
        """
        for directory in (self._mappings_merged, self._mappings_generated):
            mapping_file = directory / filename
            if not mapping_file.exists():
                continue
            with open(mapping_file, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            if key in data:
                return data[key]
        return None
