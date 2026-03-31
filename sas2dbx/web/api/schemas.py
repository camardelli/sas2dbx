"""Pydantic schemas para a API REST do sas2dbx web."""

from __future__ import annotations

from pydantic import BaseModel


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


class MigrationResultsResponse(BaseModel):
    migration_id: str
    summary: dict
    jobs: list[dict]
    architecture_explorer_url: str
    download_url: str
