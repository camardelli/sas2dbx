"""Execução síncrona e assíncrona de Databricks Workflows.

Requer databricks-sdk:
    pip install sas2dbx[databricks]
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

from sas2dbx.validate.config import DatabricksConfig

logger = logging.getLogger(__name__)

_POLL_INTERVAL_S = 5
_DEFAULT_TIMEOUT_S = 1800  # 30 min


@dataclass
class ExecutionResult:
    """Resultado de uma execução de workflow.

    Attributes:
        run_id: ID do run no Databricks.
        status: "SUCCESS", "FAILED" ou "TIMEOUT".
        duration_ms: Duração real em milissegundos.
        error: Mensagem de erro (None se sucesso).
    """

    run_id: int
    status: str
    duration_ms: int
    error: str | None = None


class WorkflowExecutor:
    """Dispara e monitora execuções de Databricks Workflows.

    Args:
        config: Credenciais e parâmetros de workspace.
        timeout_s: Timeout em segundos para aguardar a conclusão do run.
        poll_interval_s: Intervalo de polling em segundos.
    """

    def __init__(
        self,
        config: DatabricksConfig,
        timeout_s: int = _DEFAULT_TIMEOUT_S,
        poll_interval_s: int = _POLL_INTERVAL_S,
    ) -> None:
        self._config = config
        self._timeout_s = timeout_s
        self._poll_interval_s = poll_interval_s
        self._client = self._build_client()

    # ------------------------------------------------------------------
    # Public API — sync
    # ------------------------------------------------------------------

    def execute(
        self,
        job_id: int,
        on_progress: Callable[..., None] | None = None,
    ) -> ExecutionResult:
        """Dispara o job e aguarda sincronicamente até conclusão ou timeout.

        Args:
            job_id: ID do job Databricks a executar.
            on_progress: Callback opcional chamado a cada poll com (run_id, state).

        Returns:
            ExecutionResult com status final.
        """
        run_id = self._trigger_run(job_id)
        logger.info("Executor: run iniciado — run_id=%d, job_id=%d", run_id, job_id)

        start = time.monotonic()
        while True:
            elapsed_s = time.monotonic() - start
            if elapsed_s > self._timeout_s:
                logger.warning("Executor: timeout após %.0fs — run_id=%d", elapsed_s, run_id)
                return ExecutionResult(
                    run_id=run_id,
                    status="TIMEOUT",
                    duration_ms=int(elapsed_s * 1000),
                    error=f"Timeout após {self._timeout_s}s",
                )

            state, error = self._poll_run(run_id)
            if on_progress is not None:
                try:
                    on_progress(run_id, state)
                except Exception:  # noqa: BLE001
                    pass

            if state in ("SUCCESS", "FAILED", "CANCELED", "SKIPPED"):
                duration_ms = int((time.monotonic() - start) * 1000)
                status = "SUCCESS" if state == "SUCCESS" else "FAILED"
                return ExecutionResult(
                    run_id=run_id,
                    status=status,
                    duration_ms=duration_ms,
                    error=error,
                )

            time.sleep(self._poll_interval_s)

    # ------------------------------------------------------------------
    # Public API — async (D3: usa asyncio.sleep para não bloquear event loop)
    # ------------------------------------------------------------------

    async def execute_async(
        self,
        job_id: int,
        on_progress: Callable[..., None] | None = None,
    ) -> ExecutionResult:
        """Versão assíncrona de execute() — usa asyncio.sleep entre polls.

        Adequada para uso em contexto FastAPI / servidor web.
        """
        import asyncio

        run_id = self._trigger_run(job_id)
        logger.info("Executor[async]: run iniciado — run_id=%d, job_id=%d", run_id, job_id)

        start = time.monotonic()
        while True:
            elapsed_s = time.monotonic() - start
            if elapsed_s > self._timeout_s:
                logger.warning(
                    "Executor[async]: timeout após %.0fs — run_id=%d", elapsed_s, run_id
                )
                return ExecutionResult(
                    run_id=run_id,
                    status="TIMEOUT",
                    duration_ms=int(elapsed_s * 1000),
                    error=f"Timeout após {self._timeout_s}s",
                )

            state, error = self._poll_run(run_id)
            if on_progress is not None:
                try:
                    on_progress(run_id, state)
                except Exception:  # noqa: BLE001
                    pass

            if state in ("SUCCESS", "FAILED", "CANCELED", "SKIPPED"):
                duration_ms = int((time.monotonic() - start) * 1000)
                status = "SUCCESS" if state == "SUCCESS" else "FAILED"
                return ExecutionResult(
                    run_id=run_id,
                    status=status,
                    duration_ms=duration_ms,
                    error=error,
                )

            await asyncio.sleep(self._poll_interval_s)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_client(self):
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError as exc:
            raise ImportError(
                "databricks-sdk não está instalado. "
                "Execute: pip install sas2dbx[databricks]"
            ) from exc
        return WorkspaceClient(host=self._config.host, token=self._config.token)

    def _trigger_run(self, job_id: int) -> int:
        """Dispara uma execução do job e retorna o run_id."""
        response = self._client.jobs.run_now(job_id=job_id)
        return response.run_id

    def _poll_run(self, run_id: int) -> tuple[str, str | None]:
        """Consulta o estado atual do run.

        Returns:
            (state, error_message) onde state é o life_cycle_state do run.
        """
        run = self._client.jobs.get_run(run_id=run_id)
        state = run.state
        life_cycle = state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN"
        result = state.result_state.value if state.result_state else None

        # TERMINATED: job concluiu normalmente (SUCCESS ou FAILED)
        if life_cycle == "TERMINATED":
            final = result or "UNKNOWN"
            if final != "SUCCESS":
                error = self._fetch_task_error(run) or state.state_message or None
            else:
                error = None
            return final, error

        # INTERNAL_ERROR: estado terminal de falha de infra/tarefa Databricks
        if life_cycle == "INTERNAL_ERROR":
            error = self._fetch_task_error(run) or state.state_message or "Internal error no Databricks"
            return "FAILED", error

        return life_cycle, None

    def _fetch_task_error(self, run) -> str | None:
        """Tenta buscar a mensagem de erro detalhada da task que falhou."""
        try:
            for task in run.tasks or []:
                if task.run_id and task.state and task.state.result_state:
                    result = task.state.result_state.value if task.state.result_state else None
                    if result and result != "SUCCESS":
                        output = self._client.jobs.get_run_output(run_id=task.run_id)
                        if output.error:
                            # Retorna apenas a primeira linha (sem o traceback completo)
                            return output.error.split("\n")[0]
        except Exception:  # noqa: BLE001
            pass
        return None
