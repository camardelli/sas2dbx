"""Validação de integridade e cobertura do Knowledge Store."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from sas2dbx.models.sas_ast import ValidationReport

logger = logging.getLogger(__name__)

_MAPPING_FILES = [
    "functions_map.yaml",
    "formats_map.yaml",
    "informats_map.yaml",
    "options_map.yaml",
    "proc_map.yaml",
    "sql_dialect_map.yaml",
]

_REQUIRED_FIELDS_BY_FILE: dict[str, list[str]] = {
    "functions_map.yaml": ["pyspark"],
    "proc_map.yaml": ["approach"],
    "sql_dialect_map.yaml": ["sas", "spark", "notes"],
    "formats_map.yaml": ["pyspark"],
    "informats_map.yaml": ["pyspark"],
    "options_map.yaml": ["pyspark"],
}


def validate_knowledge_store(base_path: str | Path = "./knowledge") -> ValidationReport:
    """Valida integridade e cobertura do Knowledge Store.

    Checks:
    1. Estrutura: merged/ existe e contém os 6 arquivos de mapping.
    2. Funções: toda função em functions_map tem campo 'pyspark' preenchido.
    3. SQL dialect: toda regra tem campos sas, spark, notes preenchidos.
    4. PROCs: todo PROC no proc_map tem abordagem definida.
    5. Custom: se libnames.yaml existe, valida campos catalog e schema.
    6. Referências cruzadas: PROCs mapeados como 'rule' têm .md em sas_reference/procs/.
    7. Cobertura: calcula % de entradas com confidence >= 0.7.

    Returns:
        ValidationReport com status, warnings, coverage_stats.
    """
    base = Path(base_path)
    merged_dir = base / "mappings" / "merged"

    warnings: list[str] = []
    missing_refs: list[str] = []
    total_entries: dict[str, int] = {}
    is_valid = True

    # Check 1 — estrutura de diretórios
    if not merged_dir.exists():
        warnings.append("mappings/merged/ não existe — execute 'sas2dbx knowledge build-mappings'")
        is_valid = False
        return ValidationReport(
            is_valid=False,
            total_entries={},
            warnings=warnings,
            coverage=0.0,
            missing_references=[],
        )

    missing_files = [f for f in _MAPPING_FILES if not (merged_dir / f).exists()]
    if missing_files:
        for mf in missing_files:
            warnings.append(f"Arquivo de mapping ausente em merged/: {mf}")
        is_valid = False

    # Carregar todos os mappings
    all_mappings: dict[str, dict[str, Any]] = {}
    for filename in _MAPPING_FILES:
        path = merged_dir / filename
        if path.exists():
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            all_mappings[filename] = data
            total_entries[filename] = len(data)
        else:
            all_mappings[filename] = {}
            total_entries[filename] = 0

    # Check 2-4 — campos obrigatórios por arquivo
    for filename, required_fields in _REQUIRED_FIELDS_BY_FILE.items():
        data = all_mappings.get(filename, {})
        for key, entry in data.items():
            if not isinstance(entry, dict):
                continue
            for field in required_fields:
                if not entry.get(field):
                    warnings.append(
                        f"{filename}: entrada '{key}' sem campo obrigatório '{field}'"
                    )

    # Check 5 — custom/libnames.yaml se existir
    libnames_path = base / "custom" / "libnames.yaml"
    if libnames_path.exists():
        with open(libnames_path, encoding="utf-8") as f:
            libnames = yaml.safe_load(f) or {}
        for lib_name, lib_cfg in libnames.items():
            if isinstance(lib_cfg, dict):
                if not lib_cfg.get("catalog"):
                    warnings.append(f"libnames.yaml: '{lib_name}' sem campo 'catalog'")
                if not lib_cfg.get("schema"):
                    warnings.append(f"libnames.yaml: '{lib_name}' sem campo 'schema'")

    # Check 6 — referências cruzadas (PROCs rule → .md)
    procs_dir = base / "sas_reference" / "procs"
    proc_data = all_mappings.get("proc_map.yaml", {})
    for proc_name, proc_cfg in proc_data.items():
        if not isinstance(proc_cfg, dict):
            continue
        if proc_cfg.get("approach") == "rule":
            md_path = procs_dir / f"proc_{proc_name.lower()}.md"
            if not md_path.exists():
                missing_refs.append(f"sas_reference/procs/proc_{proc_name.lower()}.md")

    # Check 7 — cobertura (confidence >= 0.7)
    total_with_confidence = 0
    above_threshold = 0
    for _filename, data in all_mappings.items():
        for entry in data.values():
            if not isinstance(entry, dict):
                continue
            if "confidence" in entry:
                total_with_confidence += 1
                if entry["confidence"] >= 0.7:
                    above_threshold += 1

    coverage = (above_threshold / total_with_confidence) if total_with_confidence > 0 else 0.0

    # Referências ausentes são warnings, não erros críticos
    if missing_refs:
        for ref in missing_refs:
            warnings.append(f"Referência .md ausente (não-crítico): {ref}")

    logger.info(
        "validate: %d entradas, cobertura=%.0f%%, %d warnings",
        sum(total_entries.values()),
        coverage * 100,
        len(warnings),
    )

    return ValidationReport(
        is_valid=is_valid,
        total_entries=total_entries,
        warnings=warnings,
        coverage=coverage,
        missing_references=missing_refs,
    )
