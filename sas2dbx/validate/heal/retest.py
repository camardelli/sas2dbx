"""Retest — re-deploy seletivo e re-execução após correção.

Usa WorkflowExecutor e DatabricksDeployer do Sprint 8.
Não acessa config.cluster_id (não existe em DatabricksConfig).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.deployer import DatabricksDeployer, DeployResult
from sas2dbx.validate.executor import ExecutionResult, WorkflowExecutor

logger = logging.getLogger(__name__)


@dataclass
class RetestResult:
    """Resultado de uma re-execução após patch.

    Attributes:
        deploy_result: DeployResult do novo deploy.
        execution_result: ExecutionResult da re-execução.
        improved: True se status passou de FAILED para SUCCESS.
    """

    deploy_result: DeployResult
    execution_result: ExecutionResult
    improved: bool


class RetestEngine:
    """Re-executa notebooks corrigidos no Databricks.

    Usa a API pública de DatabricksDeployer.deploy() (não acessa
    métodos internos). A configuração de cluster vem de DatabricksConfig
    (node_type_id + spark_version) — não há campo cluster_id.

    Args:
        config: DatabricksConfig com credenciais e parâmetros de cluster.
        timeout_s: Timeout em segundos para execução do workflow.
    """

    def __init__(self, config: DatabricksConfig, timeout_s: int = 1800) -> None:
        self._config = config
        self._deployer = DatabricksDeployer(config)
        self._executor = WorkflowExecutor(config, timeout_s=timeout_s)

    def retest(self, notebook_path: Path, job_name: str) -> RetestResult:
        """Faz re-deploy e re-execução do notebook corrigido.

        Fluxo:
          1. Chama deployer.deploy(notebook_path, job_name) — API pública
          2. Chama executor.execute(deploy_result.job_id)
          3. Determina improved = execution_result.status == "SUCCESS"

        Args:
            notebook_path: Path local do notebook .py já corrigido.
            job_name: Nome do job (mesmo nome do deploy original para sobrescrever).

        Returns:
            RetestResult com deploy, execução e flag improved.

        Raises:
            RuntimeError: Propaga exceções de deploy ou execução sem wrapping.
        """
        logger.info("RetestEngine: re-deploy de %s", job_name)
        deploy_result = self._deployer.deploy(notebook_path, job_name)
        logger.info(
            "RetestEngine: re-deploy concluído — job_id=%d, executando...",
            deploy_result.job_id,
        )

        execution_result = self._executor.execute(deploy_result.job_id)
        deploy_result.run_id = execution_result.run_id

        improved = execution_result.status == "SUCCESS"
        logger.info(
            "RetestEngine: retest %s — status=%s",
            "OK" if improved else "FALHOU",
            execution_result.status,
        )
        if not improved and execution_result.error:
            logger.info(
                "RetestEngine: erro após retest — %s",
                str(execution_result.error)[:300],
            )

        return RetestResult(
            deploy_result=deploy_result,
            execution_result=execution_result,
            improved=improved,
        )
