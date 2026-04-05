"""DatabricksChecker — verifica existência de tabelas via UC REST API.

Usa WorkspaceClient.tables.get() (Unity Catalog REST API) — sem warehouse,
sem statement_execution. Instantâneo e sem custo de cluster.

Offline mode: se não houver credenciais, retorna GateDecision=SKIP.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sas2dbx.inventory import InventoryEntry

logger = logging.getLogger(__name__)


@dataclass
class DatabricksConfig:
    """Configuração mínima para conexão com Databricks."""

    host: str
    token: str


@dataclass
class CheckResult:
    """Resultado do check de todas as tabelas SOURCE."""

    total: int
    found: int
    missing: list[InventoryEntry] = field(default_factory=list)
    schemas: dict[str, list[dict]] = field(default_factory=dict)
    """FQN → list[{name, type}] para tabelas existentes."""
    mode: str = "online"
    """'online' | 'skip' (sem credenciais)."""


class DatabricksChecker:
    """Verifica tabelas SOURCE no Databricks via UC REST API."""

    def __init__(self, config: DatabricksConfig) -> None:
        self._config = config

    def check_table(self, fqn: str) -> tuple[bool, list[dict] | None]:
        """Verifica se tabela existe e retorna schema.

        Usa client.tables.get() — Unity Catalog REST API, sem warehouse.

        Args:
            fqn: Nome completo (catalog.schema.table).

        Returns:
            (exists, columns) — columns é list[{"name": str, "type": str}] ou None.
        """
        try:
            from databricks.sdk import WorkspaceClient  # type: ignore[import]
            client = WorkspaceClient(host=self._config.host, token=self._config.token)
            table_info = client.tables.get(full_name=fqn)
            columns = [
                {
                    "name": col.name,
                    "type": str(getattr(col, "type_text", None) or "STRING"),
                }
                for col in (getattr(table_info, "columns", None) or [])
                if getattr(col, "name", None)
            ]
            return True, columns or None
        except Exception as exc:  # noqa: BLE001
            logger.debug("DatabricksChecker: tabela '%s' não encontrada — %s", fqn, exc)
            return False, None

    def check_all(self, entries: list[InventoryEntry]) -> CheckResult:
        """Verifica todas as tabelas SOURCE.

        Ignora entries com role != SOURCE e entries sem table_fqn (UNRESOLVED).

        Returns:
            CheckResult com contagens, tabelas ausentes e schemas capturados.
        """
        sources = [e for e in entries if e.role == "SOURCE" and e.table_fqn]
        result = CheckResult(total=len(sources))

        for entry in sources:
            assert entry.table_fqn is not None  # satisfaz type checker
            exists, columns = self.check_table(entry.table_fqn)
            entry.exists_in_databricks = exists
            entry.columns = columns

            if exists:
                result.found += 1
                if columns:
                    result.schemas[entry.table_fqn] = columns
                    logger.debug(
                        "DatabricksChecker: %s — encontrada (%d colunas)",
                        entry.table_fqn, len(columns),
                    )
            else:
                result.missing.append(entry)
                logger.info(
                    "DatabricksChecker: %s — AUSENTE (referenciada em: %s)",
                    entry.table_fqn, ", ".join(entry.referenced_in),
                )

        logger.info(
            "DatabricksChecker: %d/%d tabela(s) SOURCE encontradas; %d ausente(s)",
            result.found, result.total, len(result.missing),
        )
        return result


def check_offline() -> CheckResult:
    """Retorna resultado SKIP quando sem credenciais Databricks.

    O InventoryGate trata SKIP como 'sem verificação — prosseguir com aviso'.
    Útil para desenvolvimento local e CI sem acesso ao Databricks.
    """
    logger.warning(
        "DatabricksChecker: modo offline — tabelas não verificadas; "
        "schemas.yaml não será gerado pelo inventory"
    )
    return CheckResult(total=0, found=0, mode="skip")
