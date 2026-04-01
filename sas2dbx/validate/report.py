"""Geração de relatório de validação consolidado."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sas2dbx.validate.collector import TableValidation
    from sas2dbx.validate.deployer import DeployResult
    from sas2dbx.validate.executor import ExecutionResult


def generate_validation_report(
    deploy_result: "DeployResult",
    execution_result: "ExecutionResult",
    table_validations: "list[TableValidation]",
) -> dict:
    """Consolida resultados de deploy, execução e coleta em um relatório único.

    Args:
        deploy_result: Resultado do deploy (workspace_path, job_id, run_id).
        execution_result: Resultado da execução do workflow.
        table_validations: Lista de validações de tabelas Delta.

    Returns:
        Dict estruturado com o relatório completo.
    """
    total_tables = len(table_validations)
    tables_ok = sum(1 for tv in table_validations if tv.error is None)
    tables_error = total_tables - tables_ok

    total_rows = sum(tv.row_count for tv in table_validations if tv.error is None)

    return {
        "generated_at": datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
        "pipeline": {
            "deploy": {
                "workspace_path": deploy_result.workspace_path,
                "job_id": deploy_result.job_id,
                "run_id": deploy_result.run_id,
            },
            "execution": {
                "run_id": execution_result.run_id,
                "status": execution_result.status,
                "duration_ms": execution_result.duration_ms,
                "error": execution_result.error,
            },
        },
        "summary": {
            "total_tables": total_tables,
            "tables_ok": tables_ok,
            "tables_error": tables_error,
            "total_rows_collected": total_rows,
            "overall_status": _overall_status(execution_result.status, tables_error, total_tables),
        },
        "tables": [
            {
                "table_name": tv.table_name,
                "row_count": tv.row_count,
                "column_count": tv.column_count,
                "sample_rows": tv.sample_rows,
                "error": tv.error,
                "status": "ok" if tv.error is None else "error",
            }
            for tv in table_validations
        ],
    }


def _overall_status(execution_status: str, tables_error: int, total_tables: int) -> str:
    """Determina o status geral do pipeline de validação.

    Returns:
        "success" — execução OK e todas as tabelas coletadas sem erro.
        "partial" — execução OK mas algumas tabelas com erro de coleta.
        "failed"  — execução falhou (FAILED ou TIMEOUT).
    """
    if execution_status != "SUCCESS":
        return "failed"
    if tables_error == 0:
        return "success"
    if tables_error < total_tables:
        return "partial"
    return "failed"
