"""Módulo de validação — deploya notebooks em Databricks e verifica resultados.

Uso:
    from sas2dbx.validate.config import DatabricksConfig
    from sas2dbx.validate.deployer import DatabricksDeployer
    from sas2dbx.validate.executor import WorkflowExecutor
    from sas2dbx.validate.collector import DatabricksCollector
    from sas2dbx.validate.report import generate_validation_report

Requer dependência opcional:
    pip install sas2dbx[databricks]
"""
