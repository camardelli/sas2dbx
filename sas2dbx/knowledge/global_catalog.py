"""GlobalCatalog — catálogo persistente cross-migração de padrões e erros.

Armazena em {work_dir}/catalog/global_patterns.json e acumula:
  - Padrões de conversão SAS → PySpark descobertos durante migrações
  - Erros catalogados com sintoma, causa raiz e fix aplicado
  - Taxa de reuso por onda (métrica de saúde do catálogo)

Thread-safe via filelock simples (rename atômico).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_CATALOG_FILENAME = "global_patterns.json"

_EMPTY_CATALOG: dict = {
    "version": "1.0",
    "created_at": "",
    "updated_at": "",
    "conversion_patterns": [],
    "error_catalog": [],
    "ghost_sources": [],
    "reuse_rate_by_wave": [],
    "stats": {
        "total_migrations": 0,
        "total_healings": 0,
        "auto_fixed": 0,
    },
}


class GlobalCatalog:
    """Catálogo global cross-migração.

    Args:
        work_dir: Diretório raiz de trabalho (ex: /data/sas2dbx_work).
            O catálogo fica em {work_dir}/catalog/global_patterns.json.
    """

    def __init__(self, work_dir: Path) -> None:
        self._catalog_dir = work_dir / "catalog"
        self._catalog_dir.mkdir(parents=True, exist_ok=True)
        self._path = self._catalog_dir / _CATALOG_FILENAME

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_migration(self, migration_id: str) -> None:
        """Incrementa contador de migrações processadas."""
        catalog = self._load()
        catalog["stats"]["total_migrations"] += 1
        catalog["updated_at"] = _now()
        self._save(catalog)

    def record_conversion_pattern(
        self,
        sas_construct: str,
        spark_equivalent: str,
        confidence: float = 1.0,
        notes: str = "",
    ) -> None:
        """Registra ou atualiza padrão de conversão SAS → PySpark.

        Se o padrão já existir (mesmo sas_construct), incrementa o contador
        de usos e atualiza a confiança média ponderada.
        """
        catalog = self._load()
        patterns = catalog["conversion_patterns"]

        existing = next((p for p in patterns if p["sas"] == sas_construct), None)
        if existing:
            # Média ponderada da confiança
            n = existing["uses"]
            existing["confidence"] = (existing["confidence"] * n + confidence) / (n + 1)
            existing["uses"] += 1
            existing["last_seen"] = _now()
        else:
            patterns.append({
                "sas": sas_construct,
                "spark": spark_equivalent,
                "confidence": confidence,
                "notes": notes,
                "uses": 1,
                "first_seen": _now(),
                "last_seen": _now(),
            })

        catalog["updated_at"] = _now()
        self._save(catalog)

    def record_error(
        self,
        pattern_key: str,
        symptom: str,
        category: str,
        auto_fixed: bool,
        notebook: str = "",
    ) -> None:
        """Registra erro encontrado e se foi corrigido automaticamente.

        Acumula ocorrências por pattern_key para identificar erros recorrentes.
        """
        catalog = self._load()
        errors = catalog["error_catalog"]

        existing = next((e for e in errors if e["pattern_key"] == pattern_key), None)
        if existing:
            existing["occurrences"] += 1
            existing["auto_fixed"] += 1 if auto_fixed else 0
            existing["last_seen"] = _now()
        else:
            errors.append({
                "pattern_key": pattern_key,
                "symptom": symptom,
                "category": category,
                "occurrences": 1,
                "auto_fixed": 1 if auto_fixed else 0,
                "first_seen": _now(),
                "last_seen": _now(),
                "example_notebook": notebook,
            })

        catalog["stats"]["total_healings"] += 1
        if auto_fixed:
            catalog["stats"]["auto_fixed"] += 1
        catalog["updated_at"] = _now()
        self._save(catalog)

    def record_ghost_source(
        self,
        table_name: str,
        migration_id: str,
        notebook: str = "",
    ) -> None:
        """Registra tabela fantasma (referenciada no SAS mas inexistente no catálogo Databricks).

        Permite identificar padrões de fontes que consistentemente não existem
        e priorizar remapeamento ou descarte.
        """
        catalog = self._load()
        ghosts = catalog["ghost_sources"]

        existing = next((g for g in ghosts if g["table_name"] == table_name), None)
        if existing:
            existing["occurrences"] += 1
            existing["last_seen"] = _now()
            if migration_id not in existing.get("migrations", []):
                existing.setdefault("migrations", []).append(migration_id)
        else:
            ghosts.append({
                "table_name": table_name,
                "occurrences": 1,
                "example_notebook": notebook,
                "migrations": [migration_id],
                "first_seen": _now(),
                "last_seen": _now(),
            })

        catalog["updated_at"] = _now()
        self._save(catalog)

    def record_wave_reuse_rate(self, rate: float) -> None:
        """Registra taxa de reuso de padrões de uma onda de migração.

        Métrica de saúde: > 60% a partir da onda 3 indica catálogo funcionando.
        """
        catalog = self._load()
        catalog["reuse_rate_by_wave"].append(round(rate, 3))
        catalog["updated_at"] = _now()
        self._save(catalog)

    def get_known_ghost_tables(self) -> set[str]:
        """Retorna conjunto de tabelas fantasma já catalogadas."""
        catalog = self._load()
        return {g["table_name"] for g in catalog.get("ghost_sources", [])}

    def get_known_error_patterns(self) -> list[str]:
        """Retorna lista de pattern_keys de erros já catalogados."""
        catalog = self._load()
        return [e["pattern_key"] for e in catalog.get("error_catalog", [])]

    def get_stats(self) -> dict:
        """Retorna estatísticas do catálogo."""
        catalog = self._load()
        return {
            **catalog["stats"],
            "conversion_patterns": len(catalog["conversion_patterns"]),
            "error_patterns": len(catalog["error_catalog"]),
            "ghost_sources": len(catalog["ghost_sources"]),
            "reuse_rate_by_wave": catalog["reuse_rate_by_wave"],
            "current_fix_rate": (
                round(catalog["stats"]["auto_fixed"] / catalog["stats"]["total_healings"], 2)
                if catalog["stats"]["total_healings"] > 0
                else 0.0
            ),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        """Carrega o catálogo do disco. Retorna estrutura vazia se não existir."""
        if not self._path.exists():
            catalog = dict(_EMPTY_CATALOG)
            catalog["created_at"] = _now()
            catalog["updated_at"] = _now()
            return catalog
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("GlobalCatalog: erro ao ler catálogo — reiniciando: %s", exc)
            catalog = dict(_EMPTY_CATALOG)
            catalog["created_at"] = _now()
            catalog["updated_at"] = _now()
            return catalog

    def _save(self, catalog: dict) -> None:
        """Salva o catálogo de forma atômica (write + rename)."""
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=self._catalog_dir, prefix=".tmp_catalog_", suffix=".json"
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(catalog, f, ensure_ascii=False, indent=2)
            Path(tmp_path).replace(self._path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")
