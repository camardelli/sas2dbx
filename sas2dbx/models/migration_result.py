"""Modelos de resultado de migração — JobStatus, MigrationResult, ValidationResult."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


@dataclass
class SASOrigin:
    """Rastreabilidade de uma célula ao bloco SAS de origem.

    Attributes:
        job_name: Nome do job SAS (filename sem extensão).
        start_line: Linha inicial no arquivo SAS (1-indexed).
        end_line: Linha final no arquivo SAS (1-indexed).
        construct_type: Tipo de construto (ex: "PROC_SQL", "DATA_STEP").
    """

    job_name: str
    start_line: int
    end_line: int
    construct_type: str


class JobStatus(StrEnum):
    """Status de um job no pipeline de migração."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


@dataclass
class ValidationWarning:
    """Aviso semântico no código PySpark gerado (não bloqueia geração)."""

    code: str          # identificador curto (ex: "UC_PREFIX")
    message: str       # descrição legível
    line: int | None = None  # linha aproximada no código gerado


@dataclass
class ValidationError:
    """Erro semântico no código PySpark gerado (bloqueia uso sem correção)."""

    code: str
    message: str
    line: int | None = None


@dataclass
class ValidationResult:
    """Resultado da validação semântica de um bloco PySpark.

    Attributes:
        is_valid: True se não há erros (warnings são permitidos).
        syntax_ok: True se ast.parse() passou.
        warnings: Avisos que não bloqueiam geração.
        errors: Erros que requerem correção.
    """

    is_valid: bool
    syntax_ok: bool
    warnings: list[ValidationWarning] = field(default_factory=list)
    errors: list[ValidationError] = field(default_factory=list)


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
    validation_result: ValidationResult | None = None
