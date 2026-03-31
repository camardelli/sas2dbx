"""Rotas REST da API sas2dbx web.

Endpoints:
  POST /api/migrations                  — upload .zip, cria migração
  GET  /api/migrations/                 — lista migrações (R23)
  GET  /api/migrations/{migration_id}   — status + progresso por job

Endpoints de resultado (Story 7.3):
  GET  /api/migrations/{migration_id}/results
  GET  /api/migrations/{migration_id}/explorer
  GET  /api/migrations/{migration_id}/download

Decisões arquiteturais aplicadas:
  R18: migration_id validado como UUID via anotação de tipo FastAPI.
  R19: upload limitado por MAX_UPLOAD_MB (configurado no app factory).
  R23: GET /api/migrations/ para histórico da Tela 1.
"""

from __future__ import annotations

import io
import logging
import zipfile
from uuid import UUID, uuid4

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse

from sas2dbx.web.api.schemas import (
    JobProgress,
    JobResult,
    MigrationResponse,
    MigrationResultsResponse,
    MigrationStatusResponse,
    MigrationSummary,
    ProgressSummary,
)
from sas2dbx.web.storage import MigrationNotFoundError, MigrationStorage

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def _storage(request: Request) -> MigrationStorage:
    return request.app.state.storage


def _max_upload_bytes(request: Request) -> int:
    return request.app.state.max_upload_bytes


# ---------------------------------------------------------------------------
# POST /migrations
# ---------------------------------------------------------------------------


@router.post("/migrations", response_model=MigrationResponse, status_code=201)
async def create_migration(
    request: Request,
    file: UploadFile = File(..., description="Arquivo .zip com os jobs .sas"),
    autoexec_filename: str = Form(default="autoexec.sas"),
    encoding: str = Form(default="auto"),
    catalog: str = Form(default="main"),
    db_schema: str = Form(default="migrated"),
) -> MigrationResponse:
    """Recebe o .zip, cria a migração e retorna o migration_id.

    O processamento real ocorre no background worker (Story 7.2).
    Status inicial: "pending".
    """
    storage = _storage(request)
    max_bytes = _max_upload_bytes(request)

    # Valida content-type
    if file.content_type not in ("application/zip", "application/x-zip-compressed"):
        raise HTTPException(
            status_code=415,
            detail="Apenas arquivos .zip são aceitos.",
        )

    # Lê o arquivo verificando o tamanho (R19)
    content = await file.read()
    if len(content) > max_bytes:
        max_mb = max_bytes // (1024 * 1024)
        raise HTTPException(
            status_code=413,
            detail=f"Arquivo excede o limite de {max_mb} MB.",
        )

    migration_id = str(uuid4())
    config = {
        "autoexec_filename": autoexec_filename,
        "encoding": encoding,
        "catalog": catalog,
        "schema": db_schema,
        "original_filename": file.filename,
    }

    migration_dir = storage.create_migration(migration_id, config)

    # Salva o zip original
    zip_path = migration_dir / "upload.zip"
    zip_path.write_bytes(content)

    logger.info(
        "POST /migrations: criada %s (%d bytes, arquivo=%s)",
        migration_id,
        len(content),
        file.filename,
    )

    # Dispara pipeline em background (Story 7.2)
    request.app.state.worker.start(migration_id)

    meta = storage.get_meta(migration_id)
    return MigrationResponse(
        migration_id=migration_id,
        status=meta["status"],
        created_at=meta["created_at"],
    )


# ---------------------------------------------------------------------------
# GET /migrations/
# ---------------------------------------------------------------------------


@router.get("/migrations/", response_model=list[MigrationSummary])
async def list_migrations(request: Request) -> list[MigrationSummary]:
    """Lista todas as migrações com status resumido (mais recente primeiro).

    Usado pela Tela 1 para exibir histórico de migrações anteriores (R23).
    """
    storage = _storage(request)
    return [MigrationSummary(**m) for m in storage.list_migrations()]


# ---------------------------------------------------------------------------
# GET /migrations/{migration_id}
# ---------------------------------------------------------------------------


@router.get("/migrations/{migration_id}", response_model=MigrationStatusResponse)
async def get_migration_status(
    migration_id: UUID,  # R18: FastAPI valida UUID4 automaticamente
    request: Request,
) -> MigrationStatusResponse:
    """Retorna status atual da migração + progresso por job.

    Usado pelo frontend para polling a cada 3s durante o processamento.
    """
    storage = _storage(request)
    try:
        data = storage.get_status(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None

    progress = ProgressSummary(**data["progress"])
    jobs = [JobProgress(**j) for j in data["jobs"]]

    return MigrationStatusResponse(
        migration_id=data["migration_id"],
        status=data["status"],
        created_at=data["created_at"],
        progress=progress,
        jobs=jobs,
        error=data.get("error"),
    )


# ---------------------------------------------------------------------------
# Story 7.3 — Endpoints de resultado
# ---------------------------------------------------------------------------


@router.get("/migrations/{migration_id}/results", response_model=MigrationResultsResponse)
async def get_migration_results(
    migration_id: UUID,
    request: Request,
) -> MigrationResultsResponse:
    """Retorna resultados completos (disponível quando status=done).

    Consolida state.json com presença de notebooks e docs gerados.
    """
    storage = _storage(request)
    try:
        data = storage.get_status(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None
    if data["status"] != "done":
        raise HTTPException(status_code=409, detail="Migração ainda não concluída.")

    migration_dir = storage.get_migration_dir(str(migration_id))
    output_dir = migration_dir / "output"
    docs_jobs_dir = migration_dir / "docs" / "jobs"
    mid_str = str(migration_id)

    jobs: list[JobResult] = []
    for job in data["jobs"]:
        job_id = job["job_id"]
        notebook = output_dir / f"{job_id}.py"
        doc = docs_jobs_dir / f"{job_id}.md"
        jobs.append(JobResult(
            job_id=job_id,
            status=job["status"],
            confidence=job.get("confidence"),
            error=job.get("error"),
            has_notebook=notebook.exists(),
            has_doc=doc.exists(),
        ))

    return MigrationResultsResponse(
        migration_id=mid_str,
        status=data["status"],
        created_at=data["created_at"],
        summary=ProgressSummary(**data["progress"]),
        jobs=jobs,
        explorer_url=f"/api/migrations/{mid_str}/explorer",
        download_url=f"/api/migrations/{mid_str}/download",
    )


@router.get("/migrations/{migration_id}/explorer", response_class=HTMLResponse)
async def get_explorer(
    migration_id: UUID,
    request: Request,
) -> HTMLResponse:
    """Serve o HTML do Architecture Explorer."""
    storage = _storage(request)
    try:
        migration_dir = storage.get_migration_dir(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None
    explorer_path = migration_dir / "explorer.html"
    if not explorer_path.exists():
        raise HTTPException(status_code=404, detail="Explorer ainda não gerado.")
    return HTMLResponse(content=explorer_path.read_text(encoding="utf-8"))


@router.get("/migrations/{migration_id}/download")
async def download_results(
    migration_id: UUID,
    request: Request,
) -> StreamingResponse:
    """Retorna .zip com notebooks (.py) + docs (.md) + explorer.html."""
    storage = _storage(request)
    try:
        meta = storage.get_meta(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None
    if meta["status"] != "done":
        raise HTTPException(status_code=409, detail="Migração ainda não concluída.")

    migration_dir = storage.get_migration_dir(str(migration_id))
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Notebooks gerados
        output_dir = migration_dir / "output"
        for nb in sorted(output_dir.glob("*.py")):
            zf.write(nb, arcname=f"output/{nb.name}")

        # Docs (ARCHITECTURE.md + jobs/*.md)
        docs_dir = migration_dir / "docs"
        for doc in sorted(docs_dir.rglob("*.md")):
            zf.write(doc, arcname=f"docs/{doc.relative_to(docs_dir)}")

        # Explorer HTML
        explorer_path = migration_dir / "explorer.html"
        if explorer_path.exists():
            zf.write(explorer_path, arcname="explorer.html")

    buf.seek(0)
    filename = f"migration_{str(migration_id)[:8]}.zip"
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
