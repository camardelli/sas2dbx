"""FastAPI app factory para o sas2dbx web.

Uso:
  # Via CLI (recomendado para piloto):
  sas2dbx serve --port 8000 --work-dir ./sas2dbx_work

  # Via uvicorn direto:
  uvicorn "sas2dbx.web.app:create_app" --factory --port 8000

Decisões arquiteturais:
  R22: ANTHROPIC_API_KEY lida aqui e injetada no worker via construtor.
  R19: MAX_UPLOAD_MB lido aqui e disponibilizado via app.state.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

# Garante que logs da aplicação apareçam no stdout (uvicorn não configura root logger)
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
)

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger(__name__)

_DEFAULT_WORK_DIR = "./sas2dbx_work"
_DEFAULT_MAX_UPLOAD_MB = 100


def create_app(
    work_dir: str | None = None,
    max_upload_mb: int | None = None,
) -> FastAPI:
    """Cria e configura a aplicação FastAPI.

    Args:
        work_dir: Diretório raiz onde as migrações são armazenadas.
        max_upload_mb: Limite de upload em MB. Se None, lê MAX_UPLOAD_MB
            do ambiente (padrão 100).

    Returns:
        Instância FastAPI configurada e pronta para ser servida pelo uvicorn.
    """
    from sas2dbx.transpile.llm.client import LLMConfig
    from sas2dbx.web.api.routes import router
    from sas2dbx.web.storage import MigrationStorage
    from sas2dbx.web.worker import MigrationWorker

    resolved_work_dir = (
        work_dir if work_dir is not None else os.environ.get("WORK_DIR", _DEFAULT_WORK_DIR)
    )
    resolved_max_mb = (
        max_upload_mb if max_upload_mb is not None
        else int(os.environ.get("MAX_UPLOAD_MB", str(_DEFAULT_MAX_UPLOAD_MB)))
    )

    app = FastAPI(
        title="SAS2DBX",
        description="SAS Query to Databricks Migrator — piloto SKY",
        version="0.1.0",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
    )

    # Injeta dependências compartilhadas em app.state (R22, R19)
    storage = MigrationStorage(Path(resolved_work_dir))
    llm_config = LLMConfig(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    app.state.storage = storage
    app.state.worker = MigrationWorker(storage=storage, llm_config=llm_config)
    app.state.max_upload_bytes = resolved_max_mb * 1024 * 1024
    app.state.work_dir = Path(resolved_work_dir)

    # Sprint 8: DatabricksConfig opcional (None se variáveis ausentes)
    dbx_config = None
    try:
        from sas2dbx.validate.config import DatabricksConfig
        if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
            dbx_config = DatabricksConfig.from_env()
            logger.info("App: DatabricksConfig carregada do ambiente (%s)", dbx_config.host)
    except ImportError:
        logger.debug("App: databricks-sdk não instalado — validação Databricks indisponível")
    except Exception as exc:  # noqa: BLE001
        logger.warning("App: falha ao carregar DatabricksConfig do ambiente: %s", exc)
    app.state.databricks_config = dbx_config

    # API routes
    app.include_router(router, prefix="/api")

    # Serve static files (React build — Story 7.4)
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
        logger.info("App: servindo frontend de %s", static_dir)
    else:
        logger.info("App: static/ não encontrado — frontend não disponível ainda")

    logger.info(
        "App: iniciado — work_dir=%s, max_upload=%sMB",
        resolved_work_dir,
        resolved_max_mb,
    )
    return app
