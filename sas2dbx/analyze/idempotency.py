"""IdempotencyAnalyzer — detecta operações não-idempotentes em notebooks gerados.

Seção 6.5 do roteiro humano: cada bloco migrado deve ser idempotente —
rodar duas vezes deve produzir o mesmo resultado.

SAS é naturalmente não-idempotente em várias operações:
  - PROC APPEND adiciona linhas a cada execução
  - OUTPUT sem condicional duplica registros
  - MODIFY altera in-place sem controle de versão

Este módulo analisa o notebook gerado e classifica cada operação de escrita
como idempotente ou não-idempotente, emitindo recomendações.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class IdempotencyIssue:
    """Operação não-idempotente identificada."""

    issue_type: str     # "append_without_dedup", "missing_overwrite", etc.
    description: str
    recommendation: str
    line: int | None = None
    snippet: str = ""


@dataclass
class IdempotencyReport:
    """Resultado da análise de idempotência de um notebook."""

    notebook: str
    issues: list[IdempotencyIssue] = field(default_factory=list)
    write_operations: int = 0
    idempotent_writes: int = 0

    @property
    def is_idempotent(self) -> bool:
        return len(self.issues) == 0

    @property
    def idempotency_rate(self) -> float:
        if self.write_operations == 0:
            return 1.0
        return self.idempotent_writes / self.write_operations

    def summary(self) -> str:
        if self.is_idempotent:
            return f"Idempotente ({self.write_operations} escrita(s) segura(s))"
        return (
            f"{len(self.issues)} problema(s) de idempotência — "
            f"{self.idempotent_writes}/{self.write_operations} escrita(s) segura(s)"
        )


# ---------------------------------------------------------------------------
# Padrões
# ---------------------------------------------------------------------------

_RE_SAVE_AS_TABLE = re.compile(
    r'\.write(?:\.format\([^)]+\))?\.mode\(\s*["\'](\w+)["\']\s*\)'
    r'(?:\.option\([^)]+\))*\.saveAsTable\(\s*["\']([^"\']+)["\']\s*\)',
    re.IGNORECASE,
)

_RE_WRITE_MODE = re.compile(
    r'\.write\.mode\(\s*["\'](\w+)["\']\s*\)',
    re.IGNORECASE,
)

_RE_MERGE_INTO = re.compile(r'\bMERGE\s+INTO\b', re.IGNORECASE)

_RE_INSERT_INTO = re.compile(r'\bINSERT\s+(?:INTO|OVERWRITE)\b', re.IGNORECASE)

_RE_DROP_DUP = re.compile(r'\.dropDuplicates\(|\.distinct\(', re.IGNORECASE)

_RE_OVERWRITE_SCHEMA = re.compile(r'overwriteSchema', re.IGNORECASE)

_RE_CREATE_OR_REPLACE = re.compile(
    r'CREATE\s+OR\s+REPLACE\s+TABLE',
    re.IGNORECASE,
)


class IdempotencyAnalyzer:
    """Analisa notebooks PySpark em busca de operações não-idempotentes."""

    def analyze_notebook(self, notebook_path: Path) -> IdempotencyReport:
        """Analisa um notebook e retorna relatório de idempotência."""
        report = IdempotencyReport(notebook=notebook_path.stem)
        try:
            content = notebook_path.read_text(encoding="utf-8")
        except OSError:
            return report

        lines = content.splitlines()

        self._check_append_writes(content, lines, report)
        self._check_missing_overwrite(content, lines, report)
        self._check_insert_without_merge(content, lines, report)
        self._count_safe_writes(content, report)

        return report

    def analyze_directory(self, output_dir: Path) -> list[IdempotencyReport]:
        """Analisa todos os notebooks .py em output_dir."""
        return [self.analyze_notebook(nb) for nb in sorted(output_dir.glob("*.py"))]

    # ------------------------------------------------------------------
    # Checkers
    # ------------------------------------------------------------------

    def _check_append_writes(
        self, content: str, lines: list[str], report: IdempotencyReport
    ) -> None:
        """Detecta .write.mode('append') sem deduplicação."""
        for i, line in enumerate(lines, 1):
            if re.search(r'\.mode\(["\']append["\']\)', line, re.IGNORECASE):
                report.write_operations += 1
                # Verifica se há dropDuplicates/distinct nas 10 linhas anteriores
                context_before = "\n".join(lines[max(0, i - 10): i])
                if not _RE_DROP_DUP.search(context_before):
                    report.issues.append(IdempotencyIssue(
                        issue_type="append_without_dedup",
                        description="write.mode('append') sem deduplicação — duplica dados a cada execução",
                        recommendation=(
                            "Substituir por MERGE INTO (Delta Lake) para operações incrementais: "
                            "spark.sql('MERGE INTO target USING source ON key = key "
                            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *')"
                        ),
                        line=i,
                        snippet=line.strip()[:120],
                    ))
                else:
                    report.idempotent_writes += 1

    def _check_missing_overwrite(
        self, content: str, lines: list[str], report: IdempotencyReport
    ) -> None:
        """Detecta saveAsTable sem mode='overwrite' (modo default é error)."""
        for i, line in enumerate(lines, 1):
            if re.search(r'\.saveAsTable\(', line, re.IGNORECASE):
                report.write_operations += 1
                # Verifica se há overwrite nas 3 linhas anteriores
                ctx = "\n".join(lines[max(0, i - 3): i + 1])
                if re.search(r'\.mode\(["\']overwrite["\']\)', ctx, re.IGNORECASE):
                    report.idempotent_writes += 1
                elif _RE_MERGE_INTO.search(ctx):
                    report.idempotent_writes += 1
                else:
                    report.issues.append(IdempotencyIssue(
                        issue_type="missing_overwrite_mode",
                        description="saveAsTable() sem mode='overwrite' — segundo run falha com AnalysisException",
                        recommendation="Adicionar .write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable(...)",
                        line=i,
                        snippet=line.strip()[:120],
                    ))

    def _check_insert_without_merge(
        self, content: str, lines: list[str], report: IdempotencyReport
    ) -> None:
        """Detecta INSERT INTO sem MERGE — pode duplicar em re-execuções."""
        if _RE_INSERT_INTO.search(content) and not _RE_MERGE_INTO.search(content):
            # Encontra primeira ocorrência para reportar linha
            for i, line in enumerate(lines, 1):
                if _RE_INSERT_INTO.search(line):
                    report.write_operations += 1
                    report.issues.append(IdempotencyIssue(
                        issue_type="insert_without_merge",
                        description="INSERT INTO sem MERGE — pode duplicar registros em re-execuções",
                        recommendation=(
                            "Usar MERGE INTO com cláusula WHEN MATCHED THEN UPDATE "
                            "para garantir upsert idempotente"
                        ),
                        line=i,
                        snippet=line.strip()[:120],
                    ))
                    break

    def _count_safe_writes(self, content: str, report: IdempotencyReport) -> None:
        """Conta CREATE OR REPLACE TABLE e MERGE INTO como escritas seguras."""
        safe = len(_RE_CREATE_OR_REPLACE.findall(content)) + len(_RE_MERGE_INTO.findall(content))
        report.write_operations += safe
        report.idempotent_writes += safe
