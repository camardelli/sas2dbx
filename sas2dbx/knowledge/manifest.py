"""Knowledge Store manifest — cobertura, versões e estatísticas.

Lê e atualiza knowledge/manifest.yaml que rastreia:
  - Entradas por arquivo de mapeamento
  - Tokens consumidos pelo LLM harvest
  - Data do último harvest
  - Score médio de confidence
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_MANIFEST_FILENAME = "manifest.yaml"

_DEFAULT_MANIFEST: dict[str, Any] = {
    "version": "1.0.0",
    "last_updated": None,
    "entries_per_file": {},
    "tokens_used_total": 0,
    "avg_confidence": 0.0,
    "on_demand_harvests": 0,
}


def read_manifest(base_path: str | Path) -> dict[str, Any]:
    """Lê o manifest.yaml do Knowledge Store.

    Args:
        base_path: Raiz do knowledge store.

    Returns:
        Dict com os dados do manifest. Retorna defaults se o arquivo não existir.
    """
    path = Path(base_path) / _MANIFEST_FILENAME
    if not path.exists():
        return dict(_DEFAULT_MANIFEST)

    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # Preenche chaves faltantes com defaults
        for key, default in _DEFAULT_MANIFEST.items():
            data.setdefault(key, default)
        return data
    except yaml.YAMLError as exc:
        logger.warning("manifest.py: falha ao ler manifest — usando defaults. %s", exc)
        return dict(_DEFAULT_MANIFEST)


def write_manifest(base_path: str | Path, data: dict[str, Any]) -> None:
    """Grava o manifest.yaml.

    Args:
        base_path: Raiz do knowledge store.
        data: Dados a gravar.
    """
    path = Path(base_path) / _MANIFEST_FILENAME
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=True)
    import os
    os.replace(tmp, path)
    logger.debug("manifest.py: manifest atualizado em %s", path)


def update_from_merged(base_path: str | Path) -> dict[str, Any]:
    """Atualiza manifest.yaml com estatísticas calculadas de mappings/merged/.

    Lê todos os arquivos YAML de merged/, conta entradas e calcula confidence médio.
    Grava e retorna o manifest atualizado.

    Args:
        base_path: Raiz do knowledge store.

    Returns:
        Dict com o manifest atualizado.
    """
    merged_dir = Path(base_path) / "mappings" / "merged"
    manifest = read_manifest(base_path)

    entries_per_file: dict[str, int] = {}
    confidence_scores: list[float] = []

    if merged_dir.exists():
        for yaml_file in sorted(merged_dir.glob("*.yaml")):
            try:
                with open(yaml_file, encoding="utf-8") as f:
                    data = yaml.safe_load(f) or {}
                entries_per_file[yaml_file.name] = len(data)
                for entry in data.values():
                    if isinstance(entry, dict) and "confidence" in entry:
                        confidence_scores.append(float(entry["confidence"]))
            except yaml.YAMLError as exc:
                logger.warning("manifest.py: falha ao ler %s: %s", yaml_file.name, exc)

    manifest["entries_per_file"] = entries_per_file
    manifest["avg_confidence"] = (
        sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
    )
    manifest["last_updated"] = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    write_manifest(base_path, manifest)
    return manifest


def increment_on_demand_counter(base_path: str | Path) -> None:
    """Incrementa o contador de on-demand harvests no manifest.

    Args:
        base_path: Raiz do knowledge store.
    """
    manifest = read_manifest(base_path)
    manifest["on_demand_harvests"] = manifest.get("on_demand_harvests", 0) + 1
    write_manifest(base_path, manifest)
