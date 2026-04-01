"""Coleta resultados de tabelas Delta no workspace Databricks via SQL Warehouse.

Requer databricks-sdk:
    pip install sas2dbx[databricks]
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from sas2dbx.validate.config import DatabricksConfig

logger = logging.getLogger(__name__)

_MAX_SAMPLE_ROWS = 5


@dataclass
class TableValidation:
    """Resultado da validação de uma tabela Delta gerada pelo pipeline.

    Attributes:
        table_name: Nome qualificado da tabela (catalog.schema.table).
        row_count: Número de linhas na tabela.
        column_count: Número de colunas no schema.
        sample_rows: Até _MAX_SAMPLE_ROWS linhas de amostra (list[dict]).
        error: Mensagem de erro se a coleta falhou (None se sucesso).
    """

    table_name: str
    row_count: int
    column_count: int
    sample_rows: list[dict] = field(default_factory=list)
    error: str | None = None


class DatabricksCollector:
    """Conecta a um SQL Warehouse e coleta estatísticas de tabelas Delta.

    Args:
        config: Credenciais e parâmetros de workspace.
            Se config.warehouse_id estiver preenchido, usa esse warehouse.
            Caso contrário, busca o primeiro warehouse em estado RUNNING.
    """

    def __init__(self, config: DatabricksConfig) -> None:
        self._config = config
        self._client = self._build_client()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, table_names: list[str]) -> list[TableValidation]:
        """Coleta row_count, column_count e amostra de cada tabela.

        Args:
            table_names: Lista de nomes de tabelas (podem ser qualificados
                com catalog.schema.table ou apenas table — nesse caso
                usa config.catalog e config.schema).

        Returns:
            Lista de TableValidation, uma por tabela.
        """
        warehouse_id = self._get_or_create_warehouse()
        results: list[TableValidation] = []

        for table in table_names:
            fqn = self._qualify(table)
            validation = self._collect_table(warehouse_id, fqn)
            results.append(validation)

        return results

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

    def _get_or_create_warehouse(self) -> str:
        """Retorna o warehouse_id a usar para as queries SQL.

        Precedência:
          1. config.warehouse_id (se preenchido, retorna diretamente)
          2. Primeiro warehouse em estado RUNNING
          3. Cria novo starter warehouse (fallback)

        Returns:
            warehouse_id (str).
        """
        # D2 — usa config.warehouse_id se fornecido
        if self._config.warehouse_id:
            logger.debug("Collector: usando warehouse configurado %s", self._config.warehouse_id)
            return self._config.warehouse_id

        # D2 — filtra por estado RUNNING
        for wh in self._client.warehouses.list():
            state = wh.state.value if wh.state else ""
            if state == "RUNNING":
                logger.debug("Collector: warehouse RUNNING encontrado — id=%s", wh.id)
                return wh.id

        # Fallback: cria starter warehouse
        logger.info("Collector: nenhum warehouse RUNNING encontrado — criando starter warehouse")
        response = self._client.warehouses.create(
            name="sas2dbx-validation",
            cluster_size="2X-Small",
            auto_stop_mins=10,
            warehouse_type="PRO",
        )
        return response.id

    def _qualify(self, table_name: str) -> str:
        """Garante que a tabela tenha catalog.schema.table."""
        parts = table_name.split(".")
        if len(parts) == 3:
            return table_name
        if len(parts) == 2:
            return f"{self._config.catalog}.{table_name}"
        return f"{self._config.catalog}.{self._config.schema}.{table_name}"

    def _collect_table(self, warehouse_id: str, fqn: str) -> TableValidation:
        """Executa queries SQL para coletar métricas de uma tabela."""
        try:
            row_count = self._query_scalar(
                warehouse_id, f"SELECT COUNT(*) FROM {fqn}"
            )
            cols = self._query_columns(warehouse_id, fqn)
            sample = self._query_sample(warehouse_id, fqn, _MAX_SAMPLE_ROWS)
            return TableValidation(
                table_name=fqn,
                row_count=int(row_count),
                column_count=len(cols),
                sample_rows=sample,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Collector: falha ao coletar %s: %s", fqn, exc)
            return TableValidation(
                table_name=fqn,
                row_count=0,
                column_count=0,
                error=str(exc),
            )

    def _query_scalar(self, warehouse_id: str, sql: str) -> int:
        """Executa SQL e retorna o valor da primeira célula como int."""
        result = self._client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout="30s",
        )
        rows = result.result.data_array or []
        if rows:
            return int(rows[0][0])
        return 0

    def _query_columns(self, warehouse_id: str, fqn: str) -> list[str]:
        """Retorna lista de nomes de colunas da tabela."""
        result = self._client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"DESCRIBE TABLE {fqn}",
            wait_timeout="30s",
        )
        schema = result.manifest.schema if result.manifest else None
        if schema and schema.columns:
            rows = result.result.data_array or []
            # DESCRIBE retorna (col_name, data_type, comment) — filtra partições/metadados
            return [
                row[0]
                for row in rows
                if row and row[0] and not row[0].startswith("#")
            ]
        return []

    def _query_sample(self, warehouse_id: str, fqn: str, limit: int) -> list[dict]:
        """Coleta até `limit` linhas de amostra como lista de dicts."""
        if not re.match(r"^[\w.]+$", fqn):
            raise ValueError(f"Nome de tabela inválido: {fqn!r}")
        result = self._client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=f"SELECT * FROM {fqn} LIMIT {int(limit)}",
            wait_timeout="30s",
        )
        schema = result.manifest.schema if result.manifest else None
        col_names: list[str] = []
        if schema and schema.columns:
            col_names = [c.name for c in schema.columns]

        rows = result.result.data_array or []
        sample: list[dict] = []
        for row in rows:
            if col_names:
                sample.append(dict(zip(col_names, row, strict=False)))
            else:
                sample.append({"_row": list(row)})
        return sample
