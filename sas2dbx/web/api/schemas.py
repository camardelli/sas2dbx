"""Pydantic schemas para a API REST do sas2dbx web."""

from __future__ import annotations

from pydantic import BaseModel, Field


class MigrationResponse(BaseModel):
    migration_id: str
    status: str
    created_at: str


class JobProgress(BaseModel):
    job_id: str
    status: str
    confidence: float | None = None


class ProgressSummary(BaseModel):
    total: int
    done: int
    failed: int
    pending: int
    in_progress: int


class MigrationStatusResponse(BaseModel):
    migration_id: str
    status: str
    created_at: str
    progress: ProgressSummary
    jobs: list[JobProgress]
    error: str | None = None


class MigrationSummary(BaseModel):
    migration_id: str
    status: str
    created_at: str


class JobResult(BaseModel):
    job_id: str
    status: str
    confidence: float | None = None
    error: str | None = None
    has_notebook: bool = False
    has_doc: bool = False


class MigrationResultsResponse(BaseModel):
    migration_id: str
    status: str
    created_at: str
    summary: ProgressSummary
    jobs: list[JobResult]
    explorer_url: str
    download_url: str


# ---------------------------------------------------------------------------
# Sprint 8 — Validation schemas
# ---------------------------------------------------------------------------


class DatabricksConfigRequest(BaseModel):
    model_config = {"populate_by_name": True}

    host: str
    token: str
    catalog: str = "main"
    db_schema: str = Field("migrated", alias="schema")
    node_type_id: str = "i3.xlarge"
    spark_version: str = "13.3.x-scala2.12"
    warehouse_id: str | None = None


class DatabricksConfigStatus(BaseModel):
    model_config = {"populate_by_name": True}

    host: str
    catalog: str
    db_schema: str = Field("migrated", alias="schema")
    node_type_id: str
    spark_version: str
    warehouse_id: str | None
    is_complete: bool


class ValidationRequest(BaseModel):
    tables: list[str] = []
    deploy_only: bool = False
    collect_only: bool = False


class TableValidationResult(BaseModel):
    table_name: str
    row_count: int
    column_count: int
    sample_rows: list[dict] = []
    error: str | None = None
    status: str


class ValidationSummary(BaseModel):
    total_notebooks: int = 0
    notebooks_ok: int = 0
    notebooks_failed: int = 0
    notebooks_skipped: int = 0
    total_rows_collected: int = 0
    overall_status: str
    # Legacy fields (single-notebook)
    total_tables: int = 0
    tables_ok: int = 0
    tables_error: int = 0


class ValidationResponse(BaseModel):
    migration_id: str
    validation_status: str  # pending | running | done | failed
    generated_at: str | None = None
    pipeline: dict | None = None
    summary: ValidationSummary | None = None
    notebook_results: list[dict] = []
    tables: list[TableValidationResult] = []
    error: str | None = None  # mensagem de erro quando validation_status == "failed"


# ---------------------------------------------------------------------------
# Sprint 9 — Self-Healing schemas
# ---------------------------------------------------------------------------


class ExecutionResultPayload(BaseModel):
    """Resultado de execução a ser curado."""

    run_id: int
    status: str
    duration_ms: int
    error: str | None = None


class HealRequest(BaseModel):
    """Payload para iniciar cura de um notebook."""

    notebook_name: str
    execution_result: ExecutionResultPayload
    max_iterations: int = 2


class HealResponse(BaseModel):
    """Resposta imediata ao disparo de healing."""

    healing_id: str
    migration_id: str
    status: str  # running | done | failed


class HealStatusResponse(BaseModel):
    """Status detalhado de um processo de healing."""

    healing_id: str
    migration_id: str
    status: str
    healed: bool = False
    iterations: int = 0
    strategy: str = "none"
    description: str = ""
    error: str | None = None
    evolution: dict | None = None  # Sprint 10: resultado do EvolutionEngine quando healing falha
