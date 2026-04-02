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
    DatabricksConfigRequest,
    DatabricksConfigStatus,
    HealRequest,
    HealResponse,
    HealStatusResponse,
    JobProgress,
    JobResult,
    MigrationResponse,
    MigrationResultsResponse,
    MigrationStatusResponse,
    MigrationSummary,
    ProgressSummary,
    TableValidationResult,
    ValidationRequest,
    ValidationResponse,
    ValidationSummary,
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


def _dbx_config(request: Request):
    """Retorna DatabricksConfig do app.state, ou None se não configurado."""
    return getattr(request.app.state, "databricks_config", None)


# ---------------------------------------------------------------------------
# POST /migrations
# ---------------------------------------------------------------------------


@router.post("/migrations", response_model=MigrationResponse, status_code=201)
async def create_migration(
    request: Request,
    file: UploadFile = File(..., description="Arquivo .zip com os jobs .sas"),
    autoexec_filename: str = Form(default="autoexec.sas"),
    encoding: str = Form(default="auto"),
    catalog: str = Form(default=""),
    db_schema: str = Form(default=""),
) -> MigrationResponse:
    """Recebe o .zip, cria a migração e retorna o migration_id.

    O processamento real ocorre no background worker (Story 7.2).
    Status inicial: "pending".
    """
    storage = _storage(request)
    max_bytes = _max_upload_bytes(request)

    # Usa catalog/schema do DatabricksConfig se não fornecidos explicitamente
    dbx = _dbx_config(request)
    if not catalog:
        catalog = dbx.catalog if dbx else "main"
    if not db_schema:
        db_schema = dbx.schema if dbx else "migrated"

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
# DELETE /migrations/{migration_id}
# ---------------------------------------------------------------------------


@router.delete("/migrations/{migration_id}", status_code=204)
async def delete_migration(
    migration_id: UUID,
    request: Request,
) -> None:
    """Remove todos os artefatos de uma migração.

    Permite ao usuário descartar uma migração com ZIP incorreto e fazer
    um novo upload. Retorna 204 No Content em caso de sucesso.
    """
    storage = _storage(request)
    try:
        storage.delete_migration(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None
    logger.info("DELETE /migrations/%s: removida", migration_id)


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


# ---------------------------------------------------------------------------
# Sprint 8 — Validation endpoints
# ---------------------------------------------------------------------------


@router.post("/config/databricks", response_model=DatabricksConfigStatus, status_code=200)
async def set_databricks_config(
    request: Request,
    body: DatabricksConfigRequest,
) -> DatabricksConfigStatus:
    """Configura credenciais Databricks para o pipeline de validação.

    As credenciais são armazenadas em app.state.databricks_config (em memória,
    não persistidas em disco para segurança).
    """
    try:
        from sas2dbx.validate.config import DatabricksConfig
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="databricks-sdk não instalado — pip install sas2dbx[databricks]",
        ) from None

    cfg = DatabricksConfig(
        host=body.host.rstrip("/"),
        token=body.token,
        catalog=body.catalog,
        schema=body.db_schema,
        node_type_id=body.node_type_id,
        spark_version=body.spark_version,
        warehouse_id=body.warehouse_id or None,
    )
    request.app.state.databricks_config = cfg
    logger.info("POST /config/databricks: configuração atualizada para %s", cfg.host)
    return DatabricksConfigStatus(**_config_to_status_dict(cfg))


@router.get("/config/databricks/status", response_model=DatabricksConfigStatus)
async def get_databricks_config_status(request: Request) -> DatabricksConfigStatus:
    """Retorna status da configuração Databricks atual (sem o token)."""
    cfg = _dbx_config(request)
    if cfg is None:
        raise HTTPException(
            status_code=404,
            detail="Configuração Databricks não definida. Use POST /config/databricks.",
        )
    return DatabricksConfigStatus(**_config_to_status_dict(cfg))


@router.post("/migrations/{migration_id}/validate", response_model=ValidationResponse, status_code=202)
async def start_validation(
    migration_id: UUID,
    request: Request,
    body: ValidationRequest,
) -> ValidationResponse:
    """Dispara o pipeline de validação (deploy → execute → collect) em background.

    Requer que POST /config/databricks tenha sido chamado antes.
    """
    storage = _storage(request)
    cfg = _dbx_config(request)

    if cfg is None:
        raise HTTPException(
            status_code=409,
            detail="Configure as credenciais Databricks primeiro via POST /config/databricks.",
        )

    try:
        meta = storage.get_meta(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None

    if meta["status"] != "done":
        raise HTTPException(status_code=409, detail="Migração ainda não concluída.")

    # Limpa resultado anterior (retry) para que o polling não leia estado antigo
    meta["validation"] = {"status": "running"}
    storage.save_meta(str(migration_id), meta)

    # Dispara validação em background
    worker = request.app.state.worker
    worker.start_validation(str(migration_id), cfg, body.tables, body.deploy_only, body.collect_only)

    return ValidationResponse(
        migration_id=str(migration_id),
        validation_status="running",
    )


@router.get("/migrations/{migration_id}/validation", response_model=ValidationResponse)
async def get_validation_status(
    migration_id: UUID,
    request: Request,
) -> ValidationResponse:
    """Retorna status atual da validação (polling)."""
    storage = _storage(request)
    try:
        meta = storage.get_meta(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None

    validation = meta.get("validation")
    if validation is None:
        return ValidationResponse(
            migration_id=str(migration_id),
            validation_status="pending",
        )

    return _build_validation_response(str(migration_id), validation)


@router.get("/migrations/{migration_id}/validation/report")
async def get_validation_report(
    migration_id: UUID,
    request: Request,
) -> dict:
    """Retorna o relatório de validação completo (disponível após status=done)."""
    storage = _storage(request)
    try:
        meta = storage.get_meta(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None

    validation = meta.get("validation")
    if validation is None or validation.get("status") != "done":
        raise HTTPException(status_code=409, detail="Validação ainda não concluída.")

    return validation.get("report", {})


# ---------------------------------------------------------------------------
# Sprint 9 — Self-Healing endpoints
# ---------------------------------------------------------------------------


@router.post("/migrations/{migration_id}/heal", response_model=HealResponse, status_code=202)
async def start_healing(
    migration_id: UUID,
    request: Request,
    body: HealRequest,
) -> HealResponse:
    """Dispara o pipeline de self-healing para um notebook com execução falha.

    Requer que POST /config/databricks tenha sido chamado antes.
    Requer que body.execution_result.status == "FAILED".
    """
    from uuid import uuid4

    storage = _storage(request)
    cfg = _dbx_config(request)

    if cfg is None:
        raise HTTPException(
            status_code=409,
            detail="Configure as credenciais Databricks primeiro via POST /config/databricks.",
        )

    if body.execution_result.status != "FAILED":
        raise HTTPException(
            status_code=422,
            detail="execution_result.status deve ser 'FAILED' para iniciar healing.",
        )

    try:
        storage.get_meta(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None

    healing_id = str(uuid4())
    worker = request.app.state.worker
    worker.start_healing(str(migration_id), healing_id, cfg, body)

    logger.info(
        "POST /migrations/%s/heal: healing_id=%s notebook=%s",
        migration_id, healing_id, body.notebook_name,
    )
    return HealResponse(
        healing_id=healing_id,
        migration_id=str(migration_id),
        status="running",
    )


@router.get("/migrations/{migration_id}/heal/{healing_id}", response_model=HealStatusResponse)
async def get_healing_status(
    migration_id: UUID,
    healing_id: str,
    request: Request,
) -> HealStatusResponse:
    """Retorna status atual de um processo de healing (polling)."""
    storage = _storage(request)
    try:
        meta = storage.get_meta(str(migration_id))
    except MigrationNotFoundError:
        raise HTTPException(status_code=404, detail="Migração não encontrada.") from None

    healings = meta.get("healings", {})
    healing = healings.get(healing_id)
    if healing is None:
        raise HTTPException(status_code=404, detail="Healing não encontrado.")

    return HealStatusResponse(
        healing_id=healing_id,
        migration_id=str(migration_id),
        status=healing.get("status", "running"),
        healed=healing.get("healed", False),
        iterations=healing.get("iterations", 0),
        strategy=healing.get("strategy", "none"),
        description=healing.get("description", ""),
        error=healing.get("error"),
    )


# ---------------------------------------------------------------------------
# Evolution API — Sprint 10
# ---------------------------------------------------------------------------


@router.get("/evolution/health")
async def get_evolution_health(request: Request):
    """Retorna snapshots de saúde do pipeline de evolução."""
    storage: MigrationStorage = request.app.state.storage
    health_path = storage.work_dir / "catalog" / "health_snapshots.json"
    if not health_path.exists():
        return {"snapshots": [], "latest": None}
    import json
    snapshots = json.loads(health_path.read_text(encoding="utf-8"))
    latest = snapshots[-1] if snapshots else None
    return {"snapshots": snapshots, "latest": latest}


@router.get("/evolution/quarantine")
async def list_quarantine(request: Request):
    """Lista fixes em quarentena aguardando aprovação humana."""
    from sas2dbx.evolve.quarantine import QuarantineStore

    storage: MigrationStorage = request.app.state.storage
    store = QuarantineStore(storage.work_dir / "catalog" / "quarantine.json")
    return {"pending": store.list_pending(), "count": store.count_pending()}


@router.post("/evolution/quarantine/{entry_id}/approve")
async def approve_quarantine(entry_id: str, request: Request):
    """Aprova um fix em quarentena e o aplica no código-fonte."""
    from sas2dbx.evolve.quarantine import QuarantineStore
    from sas2dbx.evolve.gate import QualityGate
    from sas2dbx.evolve.applier import FixApplier
    from pathlib import Path

    storage: MigrationStorage = request.app.state.storage
    store = QuarantineStore(storage.work_dir / "catalog" / "quarantine.json")
    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    note = body.get("note", "") if isinstance(body, dict) else ""

    proposal = store.approve(entry_id, note=note)
    if not proposal:
        raise HTTPException(status_code=404, detail=f"Entrada {entry_id} não encontrada.")

    # Aplica o fix aprovado
    project_root = Path(__file__).resolve().parents[3]
    applier = FixApplier(
        project_root,
        history_path=storage.work_dir / "catalog" / "evolution_history.json",
    )
    result = applier.apply(proposal)
    return {
        "entry_id": entry_id,
        "status": result.status,
        "files_modified": result.files_modified,
        "hot_reloaded": result.hot_reloaded,
    }


@router.post("/evolution/quarantine/{entry_id}/reject")
async def reject_quarantine(entry_id: str, request: Request):
    """Rejeita um fix em quarentena."""
    from sas2dbx.evolve.quarantine import QuarantineStore

    storage: MigrationStorage = request.app.state.storage
    store = QuarantineStore(storage.work_dir / "catalog" / "quarantine.json")
    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    note = body.get("note", "") if isinstance(body, dict) else ""

    ok = store.reject(entry_id, note=note)
    if not ok:
        raise HTTPException(status_code=404, detail=f"Entrada {entry_id} não encontrada.")
    return {"entry_id": entry_id, "status": "REJECTED"}


@router.get("/evolution/history")
async def get_evolution_history(request: Request):
    """Retorna histórico de fixes aplicados pelo Evolution Engine."""
    storage: MigrationStorage = request.app.state.storage
    history_path = storage.work_dir / "catalog" / "evolution_history.json"
    if not history_path.exists():
        return {"fixes": []}
    import json
    fixes = json.loads(history_path.read_text(encoding="utf-8"))
    return {"fixes": fixes, "total": len(fixes)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config_to_status_dict(cfg) -> dict:
    """Converte DatabricksConfig para dict compatível com DatabricksConfigStatus."""
    raw = cfg.to_dict()
    # Renomeia 'schema' → 'db_schema' para evitar colisão com BaseModel
    raw["db_schema"] = raw.pop("schema", "migrated")
    return raw


def _build_validation_response(migration_id: str, validation: dict) -> ValidationResponse:
    """Constrói ValidationResponse a partir do dict persistido no meta."""
    report = validation.get("report") or {}
    summary_data = report.get("summary")
    tables_data = report.get("tables", [])
    notebook_results = report.get("notebooks") or validation.get("notebook_results", [])

    summary = None
    if summary_data:
        summary = ValidationSummary(**summary_data)

    tables = [TableValidationResult(**t) for t in tables_data]

    return ValidationResponse(
        migration_id=migration_id,
        validation_status=validation.get("status", "pending"),
        generated_at=report.get("generated_at"),
        pipeline=report.get("pipeline"),
        summary=summary,
        notebook_results=notebook_results,
        tables=tables,
        error=validation.get("error"),
    )
