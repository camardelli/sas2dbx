"""Source Inventory Module — verifica tabelas ANTES da transpilação.

Fluxo:
  1. TableExtractor.extract(blocks) → list[InventoryEntry]
  2. map_libnames(entries, libname_map) → resolve FQN
  3. classify_roles(entries) → SOURCE/TARGET/INTERMEDIATE/UNRESOLVED
  4. DatabricksChecker.check_all(entries) → exists + columns
  5. InventoryGate.evaluate(result) → GateResult (PASS/WARN/BLOCK/SKIP)
  6. InventoryReporter.save_schemas() → schemas.yaml tipado antes da transpilação
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class InventoryEntry:
    """Referência a uma tabela encontrada no código SAS."""

    table_short: str
    """Nome curto (ex: 'vendas_raw')."""

    libname: str
    """Biblioteca SAS (ex: 'TELCO'). Vazia se referência sem LIBNAME."""

    role: Literal["SOURCE", "TARGET", "INTERMEDIATE", "UNRESOLVED"]
    """Papel da tabela no código SAS."""

    sas_context: str
    """Contexto SAS onde foi encontrada (ex: 'SET', 'FROM', 'DATA', 'CREATE TABLE')."""

    referenced_in: list[str] = field(default_factory=list)
    """Nomes dos jobs/arquivos que referenciam esta tabela."""

    table_fqn: str | None = None
    """Nome completo resolvido (ex: 'telcostar.operacional.vendas_raw'). None se UNRESOLVED."""

    exists_in_databricks: bool | None = None
    """None = não verificado. True/False após checker."""

    columns: list[dict] | None = None
    """Schema [{name, type}] se existe no Databricks."""


@dataclass
class GateResult:
    """Resultado da avaliação do InventoryGate."""

    decision: Literal["PASS", "WARN", "BLOCK", "SKIP"]
    message: str
    missing_count: int = 0


@dataclass
class InventoryReport:
    """Relatório completo do inventory."""

    gate: GateResult
    entries: list[InventoryEntry] = field(default_factory=list)
    schemas: dict[str, list[dict]] = field(default_factory=dict)
    """Schemas capturados para tabelas SOURCE existentes no Databricks."""

    @property
    def source_entries(self) -> list[InventoryEntry]:
        return [e for e in self.entries if e.role == "SOURCE"]

    @property
    def missing_sources(self) -> list[InventoryEntry]:
        return [e for e in self.entries if e.role == "SOURCE" and not e.exists_in_databricks]

    def to_dict(self) -> dict:
        missing = self.missing_sources
        sources = self.source_entries
        return {
            "gate_result": self.gate.decision,
            "gate_message": self.gate.message,
            "summary": {
                "total_tables": len(self.entries),
                "source_tables": len(sources),
                "target_tables": sum(1 for e in self.entries if e.role == "TARGET"),
                "intermediate_tables": sum(1 for e in self.entries if e.role == "INTERMEDIATE"),
                "unresolved_tables": sum(1 for e in self.entries if e.role == "UNRESOLVED"),
                "source_found": sum(1 for e in sources if e.exists_in_databricks),
                "source_missing": len(missing),
            },
            "missing_tables": [
                {
                    "table": e.table_fqn or f"{e.libname}.{e.table_short}",
                    "libname": e.libname,
                    "context": e.sas_context,
                    "referenced_in": e.referenced_in,
                    "action_required": "Criar tabela no Databricks com schema compatível",
                }
                for e in missing
            ],
            "unresolved_tables": [
                {
                    "table": f"{e.libname}.{e.table_short}" if e.libname else e.table_short,
                    "libname": e.libname,
                    "context": e.sas_context,
                    "referenced_in": e.referenced_in,
                    "action_required": "Mapear LIBNAME em knowledge/custom/libnames.yaml",
                }
                for e in self.entries
                if e.role == "UNRESOLVED"
            ],
            "schemas_captured": {
                fqn: [{"name": c["name"], "type": c["type"]} for c in cols]
                for fqn, cols in self.schemas.items()
            },
        }
