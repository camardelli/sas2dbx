"""Deploy de notebooks gerados para o workspace Databricks.

Requer databricks-sdk:
    pip install sas2dbx[databricks]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.validate.config import DatabricksConfig

logger = logging.getLogger(__name__)


@dataclass
class DeployResult:
    """Resultado do deploy de um notebook no Databricks.

    Attributes:
        workspace_path: Caminho do notebook no workspace Databricks.
        job_id: ID do job criado no Databricks Workflows.
        run_id: ID da última execução (preenchido pelo executor, None após deploy).
    """

    workspace_path: str
    job_id: int
    run_id: int | None = None


class DatabricksDeployer:
    """Sobe notebooks .py para o workspace e cria Databricks Workflows.

    Args:
        config: Credenciais e parâmetros de cluster para o workspace.
    """

    _WORKSPACE_ROOT = "/sas2dbx_migrations"

    def __init__(self, config: DatabricksConfig) -> None:
        self._config = config
        self._client = self._build_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def deploy(self, notebook_path: Path, job_name: str) -> DeployResult:
        """Faz upload do notebook e cria/atualiza o job no Databricks.

        Args:
            notebook_path: Caminho local do notebook .py gerado.
            job_name: Nome único para identificar o job no workspace.

        Returns:
            DeployResult com workspace_path e job_id preenchidos.

        Raises:
            ImportError: Se databricks-sdk não estiver instalado.
            RuntimeError: Se o upload ou criação do job falhar.
        """
        workspace_path = f"{self._WORKSPACE_ROOT}/{job_name}"
        self._upload_notebook(notebook_path, workspace_path)
        logger.info("Deployer: notebook enviado para %s", workspace_path)

        job_id = self._create_or_update_job(workspace_path, job_name)
        logger.info("Deployer: job criado/atualizado — id=%d, nome=%s", job_id, job_name)

        return DeployResult(workspace_path=workspace_path, job_id=job_id)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_client(self):
        """Constrói WorkspaceClient do databricks-sdk."""
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError as exc:
            raise ImportError(
                "databricks-sdk não está instalado. "
                "Execute: pip install sas2dbx[databricks]"
            ) from exc
        return WorkspaceClient(host=self._config.host, token=self._config.token)

    def _upload_notebook(self, local_path: Path, workspace_path: str) -> None:
        """Faz upload do arquivo .py como notebook Python no workspace."""
        import base64

        from databricks.sdk.service.workspace import ImportFormat, Language

        # Garante que o diretório pai existe antes do upload
        parent = workspace_path.rsplit("/", 1)[0]
        self._client.workspace.mkdirs(path=parent)

        source = local_path.read_bytes()
        encoded = base64.b64encode(source).decode("ascii")

        self._client.workspace.import_(
            path=workspace_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=encoded,
            overwrite=True,
        )

    def _create_or_update_job(self, workspace_path: str, job_name: str) -> int:
        """Cria ou sobrescreve o job Databricks Workflows para o notebook.

        Args:
            workspace_path: Caminho do notebook no workspace.
            job_name: Nome do job.

        Returns:
            job_id (int).
        """
        existing = None
        for job in self._client.jobs.list(name=job_name):
            existing = job
            break

        settings = self._build_job_settings(workspace_path, job_name)

        if existing is not None:
            self._client.jobs.reset(job_id=existing.job_id, new_settings=settings)
            return existing.job_id

        response = self._client.jobs.create(
            name=settings.name,
            tasks=settings.tasks,
        )
        return response.job_id

    def _build_job_settings(self, workspace_path: str, job_name: str):
        """Monta JobSettings tipado para o SDK Databricks.

        Sem especificação de cluster: workspaces serverless usam serverless
        automaticamente; workspaces clássicos requerem existing_cluster_id
        ou new_cluster configurados externamente.
        """
        from databricks.sdk.service.jobs import JobSettings, NotebookTask, Source, Task

        task = Task(
            task_key="run_notebook",
            notebook_task=NotebookTask(
                notebook_path=workspace_path,
                source=Source.WORKSPACE,
            ),
        )

        # Cluster clássico: usa existing_cluster_id se fornecido na config
        if self._config.cluster_id:
            task.existing_cluster_id = self._config.cluster_id

        return JobSettings(name=job_name, tasks=[task])
