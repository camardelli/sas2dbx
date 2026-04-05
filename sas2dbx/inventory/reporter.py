"""InventoryReporter — gera inventory_report.json e schemas.yaml.

O schemas.yaml gerado pelo reporter usa o formato list[dict] (Story 5.1)
e é salvo ANTES da transpilação, garantindo que o LLM receba schemas reais
desde a primeira chamada.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from sas2dbx.inventory import GateResult, InventoryEntry, InventoryReport
from sas2dbx.inventory.checker import CheckResult

logger = logging.getLogger(__name__)


class InventoryReporter:
    """Gera relatórios e persiste schemas do inventory."""

    def build_report(
        self,
        entries: list[InventoryEntry],
        check_result: CheckResult,
        gate: GateResult,
    ) -> InventoryReport:
        """Constrói o InventoryReport final."""
        return InventoryReport(
            gate=gate,
            entries=entries,
            schemas=check_result.schemas,
        )

    def save_report(self, report: InventoryReport, output_dir: Path) -> Path:
        """Salva inventory_report.json em output_dir.

        Args:
            report: Relatório completo.
            output_dir: Diretório de saída dos notebooks.

        Returns:
            Path do arquivo gerado.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        report_path = output_dir / "inventory_report.json"
        report_path.write_text(
            json.dumps(report.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("InventoryReporter: relatório salvo em %s", report_path)
        return report_path

    def save_schemas(self, report: InventoryReport, output_dir: Path) -> Path | None:
        """Salva schemas.yaml com os schemas capturados do Databricks.

        Usa formato list[dict] com {name, type} (Story 5.1).
        Só salva se houver schemas capturados (tabelas SOURCE existentes).

        IMPORTANTE: este arquivo é escrito ANTES da transpilação, para que o
        LLM receba schemas reais desde a primeira chamada (resolve timing do job_107).

        Args:
            report: InventoryReport com schemas capturados.
            output_dir: Diretório de saída dos notebooks.

        Returns:
            Path do arquivo gerado, ou None se sem schemas.
        """
        if not report.schemas:
            logger.debug("InventoryReporter: sem schemas capturados — schemas.yaml não gerado")
            return None

        try:
            import yaml  # type: ignore[import]
        except ImportError:
            logger.warning("InventoryReporter: PyYAML não disponível — schemas.yaml não salvo")
            return None

        output_dir.mkdir(parents=True, exist_ok=True)
        schemas_path = output_dir / "schemas.yaml"

        # Carrega existente para merge incremental (entries manuais têm precedência)
        existing: dict = {}
        if schemas_path.exists():
            try:
                existing = yaml.safe_load(schemas_path.read_text(encoding="utf-8")) or {}
            except Exception as exc:  # noqa: BLE001
                logger.warning("InventoryReporter: falha ao ler schemas.yaml existente — %s", exc)

        # Merge: novas tabelas adicionadas; existentes preservadas
        for fqn, cols in report.schemas.items():
            if fqn not in existing:
                existing[fqn] = {"columns": cols}
                logger.debug("InventoryReporter: schema adicionado para %s (%d colunas)", fqn, len(cols))

        schemas_path.write_text(
            yaml.dump(existing, allow_unicode=True, default_flow_style=False),
            encoding="utf-8",
        )
        logger.info(
            "InventoryReporter: schemas de %d tabela(s) salvo(s) em %s (antes da transpilação)",
            len(report.schemas), schemas_path,
        )
        return schemas_path

    def print_summary(self, report: InventoryReport) -> None:
        """Imprime sumário do inventory no log (INFO level)."""
        d = report.to_dict()
        s = d["summary"]
        logger.info(
            "Inventory: %d tabela(s) — SOURCE=%d (found=%d, missing=%d) | "
            "TARGET=%d | INTERMEDIATE=%d | UNRESOLVED=%d | Gate=%s",
            s["total_tables"], s["source_tables"], s["source_found"], s["source_missing"],
            s["target_tables"], s["intermediate_tables"], s["unresolved_tables"],
            d["gate_result"],
        )
        for m in d["missing_tables"]:
            logger.warning("  MISSING SOURCE: %s (refs: %s)", m["table"], ", ".join(m["referenced_in"]))
        for u in d["unresolved_tables"]:
            logger.warning("  UNRESOLVED: %s (libname não mapeado)", u["table"])
