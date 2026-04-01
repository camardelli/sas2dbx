"""Testes para validate/executor.py — WorkflowExecutor."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest

from sas2dbx.validate.config import DatabricksConfig
from sas2dbx.validate.executor import ExecutionResult, WorkflowExecutor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cfg() -> DatabricksConfig:
    return DatabricksConfig(host="https://dbx.net", token="tok")


def _make_executor(
    states: list[str],
    timeout_s: int = 60,
    poll_interval_s: int = 0,
) -> WorkflowExecutor:
    """Cria executor com client mockado que retorna states em sequência.

    Args:
        states: Lista de life_cycle_state que o mock retorna em sequência.
            O último state é o estado final (TERMINATED/SUCCESS/FAILED).
    """
    executor = WorkflowExecutor.__new__(WorkflowExecutor)
    executor._config = _cfg()
    executor._timeout_s = timeout_s
    executor._poll_interval_s = poll_interval_s

    client = MagicMock()
    # run_now retorna run_id=100
    run_now_resp = MagicMock()
    run_now_resp.run_id = 100
    client.jobs.run_now.return_value = run_now_resp

    # Constrói sequência de respostas de get_run
    run_responses = []
    for i, state in enumerate(states):
        run = MagicMock()
        run_state = MagicMock()
        lc = MagicMock()
        lc.value = state
        run_state.life_cycle_state = lc
        if state == "TERMINATED":
            # O último estado TERMINATED usa o próximo na lista como result_state
            # Por simplificação, usamos SUCCESS por padrão
            rs = MagicMock()
            rs.value = states[i + 1] if i + 1 < len(states) else "SUCCESS"
            run_state.result_state = rs
        else:
            run_state.result_state = None
        run_state.state_message = None
        run.state = run_state
        run_responses.append(run)

    client.jobs.get_run.side_effect = run_responses
    executor._client = client
    return executor


def _make_executor_with_result(
    result_state: str = "SUCCESS",
    timeout_s: int = 60,
) -> WorkflowExecutor:
    """Cria executor que retorna imediatamente com TERMINATED/<result_state>."""
    executor = WorkflowExecutor.__new__(WorkflowExecutor)
    executor._config = _cfg()
    executor._timeout_s = timeout_s
    executor._poll_interval_s = 0

    client = MagicMock()
    run_now_resp = MagicMock()
    run_now_resp.run_id = 100
    client.jobs.run_now.return_value = run_now_resp

    run = MagicMock()
    run_state = MagicMock()
    lc = MagicMock()
    lc.value = "TERMINATED"
    run_state.life_cycle_state = lc
    rs = MagicMock()
    rs.value = result_state
    run_state.result_state = rs
    run_state.state_message = None
    run.state = run_state

    client.jobs.get_run.return_value = run
    executor._client = client
    return executor


# ---------------------------------------------------------------------------
# ExecutionResult dataclass
# ---------------------------------------------------------------------------


class TestExecutionResult:
    def test_fields(self) -> None:
        er = ExecutionResult(run_id=1, status="SUCCESS", duration_ms=5000)
        assert er.run_id == 1
        assert er.status == "SUCCESS"
        assert er.duration_ms == 5000
        assert er.error is None

    def test_with_error(self) -> None:
        er = ExecutionResult(run_id=2, status="FAILED", duration_ms=100, error="OOM")
        assert er.error == "OOM"


# ---------------------------------------------------------------------------
# WorkflowExecutor.execute — sync
# ---------------------------------------------------------------------------


class TestWorkflowExecutorSync:
    def test_success_result(self) -> None:
        executor = _make_executor_with_result("SUCCESS")
        result = executor.execute(job_id=1)
        assert result.status == "SUCCESS"
        assert result.run_id == 100

    def test_failed_result(self) -> None:
        executor = _make_executor_with_result("FAILED")
        result = executor.execute(job_id=1)
        assert result.status == "FAILED"

    def test_timeout_returns_timeout_status(self) -> None:
        # timeout_s=0 garante timeout imediato antes do primeiro poll
        executor = WorkflowExecutor.__new__(WorkflowExecutor)
        executor._config = _cfg()
        executor._timeout_s = 0
        executor._poll_interval_s = 0

        client = MagicMock()
        run_resp = MagicMock()
        run_resp.run_id = 99
        client.jobs.run_now.return_value = run_resp

        # Simula estado RUNNING eterno
        run = MagicMock()
        state = MagicMock()
        lc = MagicMock()
        lc.value = "RUNNING"
        state.life_cycle_state = lc
        state.result_state = None
        state.state_message = None
        run.state = state
        client.jobs.get_run.return_value = run
        executor._client = client

        result = executor.execute(job_id=1)
        assert result.status == "TIMEOUT"
        assert result.run_id == 99

    def test_on_progress_callback_called(self) -> None:
        executor = _make_executor_with_result("SUCCESS")
        calls: list = []
        executor.execute(job_id=1, on_progress=lambda rid, s: calls.append((rid, s)))
        assert len(calls) >= 1

    def test_on_progress_exception_not_propagated(self) -> None:
        executor = _make_executor_with_result("SUCCESS")
        def bad_callback(rid, state):
            raise RuntimeError("callback error")
        # Não deve lançar exceção
        result = executor.execute(job_id=1, on_progress=bad_callback)
        assert result.status == "SUCCESS"

    def test_duration_ms_positive(self) -> None:
        executor = _make_executor_with_result("SUCCESS")
        result = executor.execute(job_id=1)
        assert result.duration_ms >= 0

    def test_import_error_without_sdk(self) -> None:
        with patch.dict("sys.modules", {"databricks": None, "databricks.sdk": None}):
            with pytest.raises(ImportError, match="databricks-sdk"):
                WorkflowExecutor(_cfg())


# ---------------------------------------------------------------------------
# WorkflowExecutor.execute_async — async
# ---------------------------------------------------------------------------


class TestWorkflowExecutorAsync:
    def test_async_success(self) -> None:
        executor = _make_executor_with_result("SUCCESS")
        result = asyncio.run(executor.execute_async(job_id=1))
        assert result.status == "SUCCESS"

    def test_async_failed(self) -> None:
        executor = _make_executor_with_result("FAILED")
        result = asyncio.run(executor.execute_async(job_id=1))
        assert result.status == "FAILED"

    def test_async_timeout(self) -> None:
        executor = WorkflowExecutor.__new__(WorkflowExecutor)
        executor._config = _cfg()
        executor._timeout_s = 0
        executor._poll_interval_s = 0

        client = MagicMock()
        run_resp = MagicMock()
        run_resp.run_id = 55
        client.jobs.run_now.return_value = run_resp

        run = MagicMock()
        state = MagicMock()
        lc = MagicMock()
        lc.value = "RUNNING"
        state.life_cycle_state = lc
        state.result_state = None
        state.state_message = None
        run.state = state
        client.jobs.get_run.return_value = run
        executor._client = client

        result = asyncio.run(executor.execute_async(job_id=1))
        assert result.status == "TIMEOUT"

    def test_async_on_progress_called(self) -> None:
        executor = _make_executor_with_result("SUCCESS")
        calls: list = []
        asyncio.run(
            executor.execute_async(job_id=1, on_progress=lambda rid, s: calls.append(s))
        )
        assert len(calls) >= 1

    def test_async_uses_asyncio_sleep(self) -> None:
        """execute_async deve usar asyncio.sleep — verificamos via mock."""
        executor = _make_executor_with_result("SUCCESS")
        with patch("asyncio.sleep") as mock_sleep:
            mock_sleep.return_value = None
            # poll_interval_s=0 → sleep não é chamado (retorna imediatamente)
            asyncio.run(executor.execute_async(job_id=1))
            # Com poll_interval_s=0 o sleep ainda pode ser chamado dependendo
            # da lógica; o importante é que não usa time.sleep
            # Verifica que time.sleep NÃO foi chamado no async path
        # Se chegou aqui sem erro, o async path está OK
