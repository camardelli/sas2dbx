"""Configuração de conexão Databricks para o pipeline de validação."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class DatabricksConfig:
    """Credenciais e parâmetros de cluster/warehouse para validação em Databricks.

    Attributes:
        host: URL do workspace Databricks (ex: https://adb-123.azuredatabricks.net).
        token: Personal Access Token ou Service Principal token.
        catalog: Unity Catalog de destino (padrão: "main").
        schema: Schema de destino (padrão: "migrated").
        node_type_id: Tipo de nó do cluster (padrão: "i3.xlarge").
        spark_version: Versão Databricks Runtime (padrão: "13.3.x-scala2.12").
        warehouse_id: ID do SQL Warehouse existente para coleta. Se None,
            o collector busca o primeiro warehouse RUNNING ou cria um.
    """

    host: str
    token: str
    catalog: str = "main"
    schema: str = "migrated"
    node_type_id: str = "i3.xlarge"
    spark_version: str = "13.3.x-scala2.12"
    warehouse_id: str | None = None

    @classmethod
    def from_env(cls) -> "DatabricksConfig":
        """Constrói a config a partir de variáveis de ambiente.

        Variáveis lidas:
            DATABRICKS_HOST (obrigatório)
            DATABRICKS_TOKEN (obrigatório)
            DATABRICKS_CATALOG (padrão: main)
            DATABRICKS_SCHEMA (padrão: migrated)
            DATABRICKS_NODE_TYPE_ID (padrão: i3.xlarge)
            DATABRICKS_SPARK_VERSION (padrão: 13.3.x-scala2.12)
            DATABRICKS_WAREHOUSE_ID (opcional)

        Returns:
            DatabricksConfig com os valores do ambiente.

        Raises:
            ValueError: Se DATABRICKS_HOST ou DATABRICKS_TOKEN estiverem ausentes.
        """
        host = os.environ.get("DATABRICKS_HOST", "")
        token = os.environ.get("DATABRICKS_TOKEN", "")
        if not host or not token:
            raise ValueError(
                "DATABRICKS_HOST e DATABRICKS_TOKEN são obrigatórios. "
                "Defina as variáveis de ambiente ou passe DatabricksConfig diretamente."
            )
        return cls(
            host=host.rstrip("/"),
            token=token,
            catalog=os.environ.get("DATABRICKS_CATALOG", "main"),
            schema=os.environ.get("DATABRICKS_SCHEMA", "migrated"),
            node_type_id=os.environ.get("DATABRICKS_NODE_TYPE_ID", "i3.xlarge"),
            spark_version=os.environ.get("DATABRICKS_SPARK_VERSION", "13.3.x-scala2.12"),
            warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID") or None,
        )

    def is_complete(self) -> bool:
        """Retorna True se host e token estão preenchidos."""
        return bool(self.host and self.token)

    def to_dict(self) -> dict:
        """Serializa para dict (sem o token por segurança)."""
        return {
            "host": self.host,
            "catalog": self.catalog,
            "schema": self.schema,
            "node_type_id": self.node_type_id,
            "spark_version": self.spark_version,
            "warehouse_id": self.warehouse_id,
            "is_complete": self.is_complete(),
        }
