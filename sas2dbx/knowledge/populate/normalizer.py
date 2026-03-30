"""Normalizer — pipeline de build-mappings: generated/ + curated/ → merged/.

Regra fundamental:
  - generated/ é sobrescrito livremente pelo harvest automatizado
  - curated/   NUNCA é sobrescrito — curadoria manual tem precedência absoluta
  - merged/    = generated/ atualizado com todas as chaves de curated/ (curated vence conflitos)
"""

from __future__ import annotations

import logging
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

    return results


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
