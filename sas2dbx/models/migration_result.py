"""Modelos de resultado de migração — JobStatus, MigrationResult."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class JobStatus(StrEnum):
    """Status de um job no pipeline de migração."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


@dataclass
class MigrationResult:
    """Resultado da migração de um único job SAS.

    Attributes:
        job_id: Nome do job (filename sem extensão).
        status: Status final da migração.
        confidence: Score médio de confiança dos blocos (0.0–1.0).
        output_path: Caminho do notebook gerado (None se falhou).
        warnings: Lista de avisos gerados durante a transpilação.
        error: Mensagem de erro em caso de falha.
        completed_at: ISO 8601 timestamp de conclusão.
    """

    job_id: str
    status: JobStatus = JobStatus.PENDING
    confidence: float = 0.0
    output_path: str | None = None
    warnings: list[str] = field(default_factory=list)
    error: str | None = None
    completed_at: str | None = None
