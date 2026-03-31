"""Normalizer — pipeline de build-mappings: generated/ + curated/ → merged/.

Regra fundamental:
  - generated/ é sobrescrito livremente pelo harvest automatizado
  - curated/   NUNCA é sobrescrito — curadoria manual tem precedência absoluta
  - merged/    = generated/ atualizado com todas as chaves de curated/ (curated vence conflitos)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Todos os arquivos de mapeamento gerenciados pelo pipeline
MAPPING_FILES = [
    "functions_map.yaml",
    "formats_map.yaml",
    "informats_map.yaml",
    "options_map.yaml",
    "proc_map.yaml",
    "sql_dialect_map.yaml",
]


def build_mappings(base_path: str | Path = "./knowledge") -> dict[str, int]:
    """
    Executa o merge de mappings: generated/ + curated/ → merged/.

    Curated SEMPRE vence em conflito de chave.
    Nunca modifica curated/.

    Returns:
        Dict com contagem de entradas por arquivo: {filename: total_entries}
    """
    base = Path(base_path)
    generated_dir = base / "mappings" / "generated"
    curated_dir = base / "mappings" / "curated"
    merged_dir = base / "mappings" / "merged"

    # Garante que os diretórios existem
    for d in (generated_dir, curated_dir, merged_dir):
        d.mkdir(parents=True, exist_ok=True)

    results: dict[str, int] = {}

    for filename in MAPPING_FILES:
        generated_data = _load_yaml(generated_dir / filename)
        curated_data = _load_yaml(curated_dir / filename)

        # Merge: começa com generated, sobrescreve com curated
        merged_data: dict[str, Any] = {**generated_data, **curated_data}

        _write_yaml(merged_dir / filename, merged_data)

        total = len(merged_data)
        overrides = len(curated_data)
        results[filename] = total

        logger.info(
            "build-mappings: %-30s %3d entradas (%d override(s) de curated/)",
            filename,
            total,
            overrides,
        )

    _write_manifest(base, results, curated_dir, generated_dir)

    return results


# -------------------------------------------------------------------------
# Manifest
# -------------------------------------------------------------------------

def _write_manifest(
    base: Path,
    results: dict[str, int],
    curated_dir: Path,
    generated_dir: Path,
) -> None:
    """Escreve knowledge/manifest.yaml com metadados do build."""
    mappings_meta: dict[str, Any] = {}

    for filename in MAPPING_FILES:
        curated_data = _load_yaml(curated_dir / filename)
        generated_data = _load_yaml(generated_dir / filename)
        total = results.get(filename, 0)

        # Calcular confidence médio das entradas merged
        merged_data: dict[str, Any] = {**generated_data, **curated_data}
        confidences = [
            v["confidence"]
            for v in merged_data.values()
            if isinstance(v, dict) and "confidence" in v
        ]
        avg_confidence = (sum(confidences) / len(confidences)) if confidences else 0.0

        file_key = filename.replace(".yaml", "")
        mappings_meta[file_key] = {
            "total_entries": total,
            "curated_entries": len(curated_data),
            "generated_entries": len(generated_data),
            "avg_confidence": round(avg_confidence, 3),
        }

    manifest: dict[str, Any] = {
        "version": "1.0.0",
        "last_build": datetime.now(timezone.utc).isoformat(),
        "mappings": mappings_meta,
    }

    _write_yaml(base / "manifest.yaml", manifest)
    logger.info("manifest.yaml atualizado em %s", base / "manifest.yaml")


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict[str, Any]:
    """Carrega YAML, retorna dict vazio se arquivo não existe."""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _write_yaml(path: Path, data: dict[str, Any]) -> None:
    """Escreve data em YAML, criando diretórios pai se necessário."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(
            data,
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=True,
        )
