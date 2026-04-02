"""Quarantine — fila de fixes de alto risco para aprovação humana.

Fixes Tier C (engine_rule, context_rule) nunca são auto-aplicados.
São armazenados aqui até que um operador revise e aprove via API.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sas2dbx.evolve.agent import EvolutionProposal

logger = logging.getLogger(__name__)


@dataclass
class QuarantineEntry:
    """Um fix em quarentena aguardando aprovação humana.

    Attributes:
        entry_id: UUID gerado no momento do registro.
        job_id: Job que originou o UnresolvedError.
        proposal_summary: Resumo da proposta (fix_type, description, files).
        proposal_json: Proposta completa serializada.
        status: "PENDING" | "APPROVED" | "REJECTED".
        reviewer_note: Nota do revisor (preenchida ao aprovar/rejeitar).
        created_at: ISO 8601 UTC.
        reviewed_at: ISO 8601 UTC (None se ainda PENDING).
    """

    entry_id: str
    job_id: str
    proposal_summary: dict
    proposal_json: str
    status: str
    reviewer_note: str
    created_at: str
    reviewed_at: str | None


class QuarantineStore:
    """Armazena e gerencia fixes em quarentena.

    Args:
        store_path: Arquivo JSON onde as entradas são persistidas.
    """

    def __init__(self, store_path: Path) -> None:
        self._path = store_path
        self._entries: list[QuarantineEntry] = self._load()

    def submit(self, job_id: str, proposal: EvolutionProposal) -> str:
        """Adiciona proposta à quarentena.

        Args:
            job_id: Job que originou o erro.
            proposal: EvolutionProposal de alto risco.

        Returns:
            entry_id gerado.
        """
        import uuid

        entry_id = str(uuid.uuid4())[:12]
        entry = QuarantineEntry(
            entry_id=entry_id,
            job_id=job_id,
            proposal_summary={
                "fix_type": proposal.fix_type,
                "risk_level": proposal.risk_level,
                "description": proposal.description,
                "files": [fm.path for fm in proposal.files_to_modify],
            },
            proposal_json=json.dumps(
                {
                    "fix_type": proposal.fix_type,
                    "risk_level": proposal.risk_level,
                    "description": proposal.description,
                    "files_to_modify": [
                        {
                            "path": fm.path,
                            "action": fm.action,
                            "content": fm.content,
                            "reason": fm.reason,
                            "old_string": fm.old_string,
                        }
                        for fm in proposal.files_to_modify
                    ],
                    "test": {
                        "path": proposal.test.path,
                        "content": proposal.test.content,
                    }
                    if proposal.test
                    else None,
                    "similar_jobs_prediction": proposal.similar_jobs_prediction,
                },
                ensure_ascii=False,
                indent=2,
            ),
            status="PENDING",
            reviewer_note="",
            created_at=datetime.now(timezone.utc).isoformat(),
            reviewed_at=None,
        )
        self._entries.append(entry)
        self._save()
        logger.info("QuarantineStore: fix em quarentena — entry_id=%s job=%s", entry_id, job_id)
        return entry_id

    def approve(self, entry_id: str, note: str = "") -> EvolutionProposal | None:
        """Aprova um fix em quarentena.

        Args:
            entry_id: ID da entrada a aprovar.
            note: Nota do revisor.

        Returns:
            EvolutionProposal reconstruída se encontrada, None caso contrário.
        """
        entry = self._find(entry_id)
        if not entry:
            return None

        entry.status = "APPROVED"
        entry.reviewer_note = note
        entry.reviewed_at = datetime.now(timezone.utc).isoformat()
        self._save()
        logger.info("QuarantineStore: entry_id=%s APROVADO", entry_id)

        return self._reconstruct_proposal(entry)

    def reject(self, entry_id: str, note: str = "") -> bool:
        """Rejeita um fix em quarentena.

        Args:
            entry_id: ID da entrada a rejeitar.
            note: Motivo da rejeição.

        Returns:
            True se encontrado e rejeitado.
        """
        entry = self._find(entry_id)
        if not entry:
            return False

        entry.status = "REJECTED"
        entry.reviewer_note = note
        entry.reviewed_at = datetime.now(timezone.utc).isoformat()
        self._save()
        logger.info("QuarantineStore: entry_id=%s REJEITADO: %s", entry_id, note)
        return True

    def list_pending(self) -> list[dict]:
        """Retorna lista de entradas PENDING como dicts (para API)."""
        return [
            {
                "entry_id": e.entry_id,
                "job_id": e.job_id,
                "proposal_summary": e.proposal_summary,
                "created_at": e.created_at,
                "status": e.status,
            }
            for e in self._entries
            if e.status == "PENDING"
        ]

    def count_pending(self) -> int:
        """Retorna número de fixes aguardando aprovação."""
        return sum(1 for e in self._entries if e.status == "PENDING")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _find(self, entry_id: str) -> QuarantineEntry | None:
        for e in self._entries:
            if e.entry_id == entry_id:
                return e
        return None

    def _reconstruct_proposal(self, entry: QuarantineEntry) -> EvolutionProposal | None:
        """Reconstrói EvolutionProposal a partir do JSON salvo."""
        from sas2dbx.evolve.agent import (
            EvolutionProposal,
            FileModification,
            EvolutionTest,
        )

        try:
            data = json.loads(entry.proposal_json)
            files = [
                FileModification(**fm) for fm in data.get("files_to_modify", [])
            ]
            test_data = data.get("test")
            test = EvolutionTest(**test_data) if test_data else None
            return EvolutionProposal(
                fix_type=data["fix_type"],
                risk_level=data["risk_level"],
                description=data["description"],
                files_to_modify=files,
                test=test,
                similar_jobs_prediction=data.get("similar_jobs_prediction", ""),
            )
        except Exception as exc:
            logger.error("QuarantineStore: erro ao reconstruir proposta: %s", exc)
            return None

    def _save(self) -> None:
        """Persiste entradas em disco (escrita atômica)."""
        import tempfile
        import os

        data = [asdict(e) for e in self._entries]
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd, tmp_path = tempfile.mkstemp(
                dir=self._path.parent, suffix=".tmp"
            )
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            Path(tmp_path).replace(self._path)
        except OSError as exc:
            logger.error("QuarantineStore: erro ao salvar: %s", exc)

    def _load(self) -> list[QuarantineEntry]:
        """Carrega entradas existentes do disco."""
        try:
            if self._path.exists():
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                return [QuarantineEntry(**e) for e in raw]
        except Exception:
            pass
        return []
