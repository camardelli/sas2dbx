"""MigrationStateManager — checkpointing para o engine de transpilação.

Persiste o estado de cada job em `{output_dir}/.sas2dbx_state.json` em tempo
real, permitindo retomar (--resume) uma migração interrompida a partir do
último job bem-sucedido.

Formato do arquivo:
  {
    "version": "1.0",
    "started_at": "<ISO 8601>",
    "jobs": {
      "job_001": {"status": "done", "completed_at": "...", "confidence": 0.92},
      "job_002": {"status": "failed", "error": "LLM timeout"},
      "job_003": {"status": "pending"}
    }
  }
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import UTC, datetime
from pathlib import Path

from sas2dbx.models.migration_result import JobStatus, MigrationResult

logger = logging.getLogger(__name__)

_STATE_FILENAME = ".sas2dbx_state.json"
_STATE_VERSION = "1.0"


class MigrationStateManager:
    """Gerencia o estado persistente de uma migração SAS2DBX.

    Cada chamada a `mark_*` grava imediatamente o state file em disco para
    garantir durabilidade mesmo em caso de crash do processo.

    Args:
        output_dir: Diretório de saída onde `.sas2dbx_state.json` será gravado.
    """

    def __init__(self, output_dir: Path) -> None:
        self._path = output_dir / _STATE_FILENAME
        self._state: dict = {}
        self._lock = threading.Lock()  # PP2-06: thread-safety para transpilação paralela

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def init_fresh(self, all_jobs: list[str]) -> None:
        """Inicializa estado do zero, sobrescrevendo qualquer state anterior.

        Args:
            all_jobs: Lista de job_ids a migrar.
        """
        self._state = {
            "version": _STATE_VERSION,
            "started_at": _now_iso(),
            "jobs": {job: {"status": JobStatus.PENDING.value} for job in all_jobs},
        }
        self._save()
        logger.info("StateManager: estado inicializado com %d job(s)", len(all_jobs))

    def load(self) -> bool:
        """Carrega state file do disco para resume.

        Returns:
            True se o arquivo existia e foi carregado com sucesso.
            False se não existia ou estava corrompido (state resetado para vazio).
        """
        if not self._path.exists():
            logger.warning("StateManager: state file não encontrado em %s", self._path)
            self._state = {}
            return False
        try:
            with open(self._path, encoding="utf-8") as f:
                self._state = json.load(f)
            logger.info(
                "StateManager: state carregado — %d job(s)",
                len(self._state.get("jobs", {})),
            )
            return True
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(
                "StateManager: state corrompido (%s) — iniciando do zero", exc
            )
            self._state = {}
            return False

    # ------------------------------------------------------------------
    # Status mutations
    # ------------------------------------------------------------------

    def mark_started(self, job_id: str) -> None:
        """Registra que um job começou a ser processado."""
        self._set_job(job_id, {"status": JobStatus.IN_PROGRESS.value})

    def mark_done(self, job_id: str, result: MigrationResult) -> None:
        """Registra conclusão bem-sucedida de um job."""
        entry: dict = {
            "status": JobStatus.DONE.value,
            "completed_at": _now_iso(),
            "confidence": result.confidence,
        }
        if result.output_path:
            entry["output_path"] = result.output_path
        if result.warnings:
            entry["warnings_count"] = len(result.warnings)
        self._set_job(job_id, entry)

    def mark_failed(self, job_id: str, error: str) -> None:
        """Registra falha de um job."""
        self._set_job(job_id, {
            "status": JobStatus.FAILED.value,
            "completed_at": _now_iso(),
            "error": error,
        })

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_job_status(self, job_id: str) -> JobStatus:
        """Retorna o status atual de um job."""
        raw = self._state.get("jobs", {}).get(job_id, {}).get("status", "pending")
        try:
            return JobStatus(raw)
        except ValueError:
            return JobStatus.PENDING

    def get_pending_jobs(self, all_jobs: list[str]) -> list[str]:
        """Retorna jobs que ainda precisam ser processados.

        Jobs com status DONE são pulados. FAILED e PENDING são re-tentados.

        Args:
            all_jobs: Lista completa de job_ids na ordem de execução.

        Returns:
            Subconjunto de all_jobs excluindo os que estão DONE.
        """
        done = {
            job_id
            for job_id, info in self._state.get("jobs", {}).items()
            if info.get("status") == JobStatus.DONE.value
        }
        pending = [j for j in all_jobs if j not in done]
        skipped = len(all_jobs) - len(pending)
        if skipped:
            logger.info(
                "StateManager: %d job(s) pulados (DONE), %d pendentes", skipped, len(pending)
            )
        return pending

    def get_all_statuses(self) -> dict[str, JobStatus]:
        """Retorna mapa job_id → JobStatus para todos os jobs no state."""
        result = {}
        for job_id, info in self._state.get("jobs", {}).items():
            raw = info.get("status", "pending")
            try:
                result[job_id] = JobStatus(raw)
            except ValueError:
                result[job_id] = JobStatus.PENDING
        return result

    @property
    def state_path(self) -> Path:
        """Caminho do arquivo de estado."""
        return self._path

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _set_job(self, job_id: str, entry: dict) -> None:
        with self._lock:
            if "jobs" not in self._state:
                self._state["jobs"] = {}
            self._state["jobs"][job_id] = entry
            self._save_locked()

    def _save_locked(self) -> None:
        """Grava state em disco — deve ser chamado com self._lock adquirido."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._state, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self._path)
        logger.debug("StateManager: state gravado em %s", self._path)

    def _save(self) -> None:
        """Grava state em disco (adquire lock internamente)."""
        with self._lock:
            self._save_locked()


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="seconds")
