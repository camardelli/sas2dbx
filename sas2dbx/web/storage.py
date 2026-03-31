"""MigrationStorage — gerencia diretórios de trabalho por migração.

Estrutura de diretório por migração:
  {work_dir}/migrations/{uuid}/
    ├── input/        .sas files extraídos do zip
    ├── output/       notebooks .py gerados (+ .sas2dbx_state.json)
    ├── docs/         READMEs + ARCHITECTURE.md
    ├── explorer.html Architecture Explorer HTML
    ├── meta.json     metadata da migração (status global, config, timestamps)
    └── upload.zip    arquivo original enviado pelo usuário
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

_STATE_FILENAME = ".sas2dbx_state.json"


class MigrationNotFoundError(KeyError):
    """Migração não encontrada no storage."""


class MigrationStorage:
    """Gerencia diretórios de trabalho por migração.

    Args:
        work_dir: Diretório raiz onde as migrações são armazenadas.
    """

    def __init__(self, work_dir: Path) -> None:
        self._work_dir = work_dir
        self._work_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def create_migration(self, migration_id: str, config: dict) -> Path:
        """Cria a estrutura de diretório para uma nova migração.

        Args:
            migration_id: UUID da migração.
            config: Parâmetros recebidos no POST (encoding, catalog, schema, etc).

        Returns:
            Path do diretório raiz da migração.
        """
        migration_dir = self._migration_dir(migration_id)
        for subdir in ("input", "output", "docs"):
            (migration_dir / subdir).mkdir(parents=True, exist_ok=True)

        meta = {
            "migration_id": migration_id,
            "status": "pending",
            "created_at": _now_iso(),
            "config": config,
            "error": None,
        }
        self._save_json(migration_dir / "meta.json", meta)
        logger.info("MigrationStorage: criada migração %s", migration_id)
        return migration_dir

    def get_migration_dir(self, migration_id: str) -> Path:
        """Retorna o diretório raiz da migração.

        Raises:
            MigrationNotFoundError: Se a migração não existir.
        """
        path = self._migration_dir(migration_id)
        if not path.exists():
            raise MigrationNotFoundError(migration_id)
        return path

    # ------------------------------------------------------------------
    # Meta
    # ------------------------------------------------------------------

    def get_meta(self, migration_id: str) -> dict:
        """Lê meta.json da migração.

        Raises:
            MigrationNotFoundError: Se a migração não existir.
        """
        path = self._migration_dir(migration_id) / "meta.json"
        if not path.exists():
            raise MigrationNotFoundError(migration_id)
        return json.loads(path.read_text(encoding="utf-8"))

    def save_meta(self, migration_id: str, meta: dict) -> None:
        """Grava meta.json de forma atômica."""
        path = self._migration_dir(migration_id) / "meta.json"
        self._save_json(path, meta)

    def update_status(
        self,
        migration_id: str,
        status: str,
        error: str | None = None,
    ) -> None:
        """Atualiza o campo status (e opcionalmente error) do meta.json."""
        meta = self.get_meta(migration_id)
        meta["status"] = status
        if error is not None:
            meta["error"] = error
        self.save_meta(migration_id, meta)

    # ------------------------------------------------------------------
    # Status consolidado (meta + state)
    # ------------------------------------------------------------------

    def get_status(self, migration_id: str) -> dict:
        """Consolida meta.json + state.json em um dict de status.

        Returns:
            Dict com migration_id, status, progress (contagens por status),
            jobs (lista de JobProgress), created_at e error.
        """
        meta = self.get_meta(migration_id)
        state_path = (
            self._migration_dir(migration_id) / "output" / _STATE_FILENAME
        )

        jobs: list[dict] = []
        progress = {
            "total": 0,
            "done": 0,
            "failed": 0,
            "pending": 0,
            "in_progress": 0,
        }

        if state_path.exists():
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
                for job_id, job_data in state.get("jobs", {}).items():
                    job_status = job_data.get("status", "pending")
                    confidence = job_data.get("confidence")
                    jobs.append({
                        "job_id": job_id,
                        "status": job_status,
                        "confidence": confidence,
                    })
                    progress["total"] += 1
                    key = job_status if job_status in progress else "pending"
                    progress[key] += 1
            except (json.JSONDecodeError, KeyError):
                logger.warning(
                    "MigrationStorage: state.json corrompido para %s", migration_id
                )

        return {
            "migration_id": migration_id,
            "status": meta["status"],
            "created_at": meta["created_at"],
            "progress": progress,
            "jobs": jobs,
            "error": meta.get("error"),
        }

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def list_migrations(self) -> list[dict]:
        """Lista todas as migrações com status resumido, ordenadas por data (mais recente primeiro).
        """
        migrations_root = self._work_dir / "migrations"
        if not migrations_root.exists():
            return []

        results: list[dict] = []
        for d in migrations_root.iterdir():
            meta_path = d / "meta.json"
            if not meta_path.exists():
                continue
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                results.append({
                    "migration_id": meta["migration_id"],
                    "status": meta["status"],
                    "created_at": meta["created_at"],
                })
            except (json.JSONDecodeError, KeyError):
                pass

        results.sort(key=lambda m: m["created_at"], reverse=True)
        return results

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _migration_dir(self, migration_id: str) -> Path:
        return self._work_dir / "migrations" / migration_id

    @staticmethod
    def _save_json(path: Path, data: dict) -> None:
        """Grava JSON de forma atômica (write-to-temp + os.replace)."""
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds")
