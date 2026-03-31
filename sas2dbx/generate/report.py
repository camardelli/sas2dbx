"""Gerador de relatório de migração — JSON estruturado + markdown summary.

Consolida a lista de MigrationResults em:
  1. `migration_report.json` — estrutura completa auditável
  2. `migration_report.md` — sumário legível para revisão humana

Formato JSON:
  {
    "summary": {
      "total_jobs": 3,
      "fully_migrated": 2,
      "partial": 0,
      "failed": 1,
      "avg_confidence": 0.89,
      "generated_at": "2026-03-31T..."
    },
    "jobs": [
      {
        "job_id": "job_001",
        "status": "done",
        "confidence": 0.92,
        "output_path": "...",
        "warnings": [...],
        "validation": {"is_valid": true, "errors": [], "warnings": []}
      }
    ]
  }
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sas2dbx.models.migration_result import JobStatus, MigrationResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@dataclass
class ReportConfig:
    """Configuração do gerador de relatório.

    Attributes:
        include_json: Gerar arquivo JSON.
        include_markdown: Gerar arquivo markdown.
        output_stem: Stem do nome do arquivo (sem extensão).
    """

    include_json: bool = True
    include_markdown: bool = True
    output_stem: str = "migration_report"


# ---------------------------------------------------------------------------
# ReportGenerator
# ---------------------------------------------------------------------------


class ReportGenerator:
    """Gera relatório de migração a partir da lista de MigrationResults.

    Args:
        config: Configurações do relatório.
    """

    def __init__(self, config: ReportConfig | None = None) -> None:
        self._config = config or ReportConfig()

    def generate(self, results: list[MigrationResult], output_dir: Path) -> list[Path]:
        """Gera os artefatos de relatório.

        Args:
            results: Lista de MigrationResult, um por job.
            output_dir: Diretório de saída.

        Returns:
            Lista de caminhos dos arquivos gerados.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        report_data = self._build_report_data(results)
        generated: list[Path] = []

        if self._config.include_json:
            json_path = output_dir / f"{self._config.output_stem}.json"
            json_path.write_text(
                json.dumps(report_data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            generated.append(json_path)
            logger.info("ReportGenerator: JSON gravado em %s", json_path)

        if self._config.include_markdown:
            md_path = output_dir / f"{self._config.output_stem}.md"
            md_path.write_text(
                self._build_markdown(report_data),
                encoding="utf-8",
            )
            generated.append(md_path)
            logger.info("ReportGenerator: Markdown gravado em %s", md_path)

        return generated

    # ------------------------------------------------------------------
    # Internal — data builders
    # ------------------------------------------------------------------

    def _build_report_data(self, results: list[MigrationResult]) -> dict:
        """Constrói o dict completo do relatório."""
        total = len(results)
        fully_migrated = sum(1 for r in results if r.status == JobStatus.DONE)
        failed = sum(1 for r in results if r.status == JobStatus.FAILED)
        # "partial" = DONE mas com warnings de validação ou baixa confiança
        partial = sum(
            1
            for r in results
            if r.status == JobStatus.DONE
            and (r.warnings or (r.validation_result and r.validation_result.warnings))
        )

        done_confidences = [r.confidence for r in results if r.status == JobStatus.DONE]
        avg_confidence = (
            sum(done_confidences) / len(done_confidences) if done_confidences else 0.0
        )

        jobs = []
        for r in results:
            entry: dict = {
                "job_id": r.job_id,
                "status": r.status.value,
                "confidence": round(r.confidence, 4),
            }
            if r.output_path:
                entry["output_path"] = r.output_path
            if r.warnings:
                entry["warnings"] = r.warnings
            if r.error:
                entry["error"] = r.error
            if r.completed_at:
                entry["completed_at"] = r.completed_at
            if r.validation_result is not None:
                vr = r.validation_result
                entry["validation"] = {
                    "is_valid": vr.is_valid,
                    "syntax_ok": vr.syntax_ok,
                    "errors": [{"code": e.code, "message": e.message} for e in vr.errors],
                    "warnings": [{"code": w.code, "message": w.message} for w in vr.warnings],
                }
            jobs.append(entry)

        return {
            "summary": {
                "total_jobs": total,
                "fully_migrated": fully_migrated,
                "partial": partial,
                "failed": failed,
                "avg_confidence": round(avg_confidence, 4),
                "generated_at": _now_iso(),
            },
            "jobs": jobs,
        }

    def _build_markdown(self, report: dict) -> str:
        """Renderiza o relatório como markdown."""
        s = report["summary"]
        lines: list[str] = [
            "# Relatório de Migração SAS2DBX",
            "",
            "## Sumário",
            "",
            "| Métrica | Valor |",
            "|---|---|",
            f"| Total de jobs | {s['total_jobs']} |",
            f"| Migrados com sucesso | {s['fully_migrated']} |",
            f"| Parciais | {s['partial']} |",
            f"| Falharam | {s['failed']} |",
            f"| Confiança média | {s['avg_confidence']:.2%} |",
            f"| Gerado em | {s['generated_at']} |",
            "",
            "## Jobs",
            "",
        ]

        for job in report["jobs"]:
            status_icon = {
                "done": "✅",
                "failed": "❌",
                "pending": "⏳",
                "in_progress": "🔄",
            }.get(job["status"], "❓")

            lines.append(f"### {status_icon} `{job['job_id']}` — {job['status']}")
            lines.append("")
            lines.append(f"- **Confiança:** {job['confidence']:.2%}")

            if job.get("output_path"):
                lines.append(f"- **Saída:** `{job['output_path']}`")

            if job.get("warnings"):
                lines.append(f"- **Warnings ({len(job['warnings'])}):**")
                for w in job["warnings"]:
                    lines.append(f"  - {w}")

            if job.get("error"):
                lines.append(f"- **Erro:** {job['error']}")

            if val := job.get("validation"):
                valid_str = "válido" if val["is_valid"] else "inválido"
                lines.append(f"- **Validação PySpark:** {valid_str}")
                if val["errors"]:
                    lines.append(f"  - Erros: {', '.join(e['code'] for e in val['errors'])}")
                if val["warnings"]:
                    lines.append(f"  - Avisos: {', '.join(w['code'] for w in val['warnings'])}")

            lines.append("")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="seconds")
