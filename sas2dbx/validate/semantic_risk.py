"""SemanticRiskAnalyzer — identifica riscos semânticos em notebooks gerados.

Seção 7b + 8 do roteiro humano: após transpilação, antes do deploy,
emite avisos estruturados sobre padrões que podem causar divergência
silenciosa entre a saída SAS e a saída Databricks.

Riscos detectados:
  SR-001  JOIN sem verificação de cardinalidade (explosão de linhas)
  SR-002  Coluna de data/timestamp armazenada como STRING
  SR-003  Filtro de exclusão sem tratamento de NULL
  SR-004  WINDOW function sem ORDER BY determinístico
  SR-005  Coluna numérica com cast implícito (truncamento silencioso)
  SR-006  .write.mode("append") sem deduplicação (duplicação a cada run)
  SR-007  ORDER BY ausente antes de operações row-number/rank
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SemanticRisk:
    """Risco semântico identificado em um notebook."""

    code: str          # identificador: SR-001, SR-002, etc.
    description: str   # o que pode dar errado
    mitigation: str    # como mitigar
    line: int | None = None
    snippet: str = ""


@dataclass
class SemanticRiskReport:
    """Resultado da análise de riscos de um notebook."""

    notebook: str
    risks: list[SemanticRisk] = field(default_factory=list)

    @property
    def has_risks(self) -> bool:
        return len(self.risks) > 0

    def summary(self) -> str:
        if not self.risks:
            return "Nenhum risco semântico detectado"
        codes = ", ".join(r.code for r in self.risks)
        return f"{len(self.risks)} risco(s): {codes}"


# ---------------------------------------------------------------------------
# Padrões de detecção
# ---------------------------------------------------------------------------

# SR-001: JOIN sem count/distinct check próximo
_RE_JOIN = re.compile(r'\bjoin\s*\(', re.IGNORECASE)

# SR-002: coluna de data como StringType ou inferida como string
_RE_DATE_AS_STRING = re.compile(
    r'\.withColumn\(["\'](\w*(?:dt|date|data|mes|ano|timestamp)\w*)["\'],\s*F\.lit\(["\']',
    re.IGNORECASE,
)
_RE_STRING_DATE_COL = re.compile(
    r'StringType\(\).*?(?:dt_|date|data|mes|ano)',
    re.IGNORECASE | re.DOTALL,
)

# SR-003: filter com != ou > mas sem isNotNull
_RE_FILTER_WITHOUT_NULL = re.compile(
    r'\.filter\([^)]*(?:!=|>|<)[^)]*\)',
    re.IGNORECASE,
)
_RE_HAS_IS_NOT_NULL = re.compile(r'isNotNull\(\)', re.IGNORECASE)

# SR-004: Window sem orderBy
_RE_WINDOW_NO_ORDER = re.compile(
    r'Window\.partitionBy\([^)]+\)(?![\s\S]{0,100}\.orderBy)',
    re.IGNORECASE,
)

# SR-005: cast sem tratamento de erro (try_cast não usado)
_RE_UNSAFE_CAST = re.compile(
    r'\.cast\(["\'](?:int|long|double|float|decimal)["\']',
    re.IGNORECASE,
)

# SR-006: append sem dedup
_RE_APPEND_MODE = re.compile(
    r'\.write\.mode\(["\']append["\']\)',
    re.IGNORECASE,
)
_RE_DROP_DUPLICATES = re.compile(r'\.dropDuplicates\(|\.distinct\(', re.IGNORECASE)

# SR-007: row_number/rank sem orderBy
_RE_ROW_NUMBER = re.compile(
    r'F\.row_number\(\)|F\.rank\(\)|F\.dense_rank\(\)',
    re.IGNORECASE,
)
_RE_ORDER_BY_WINDOW = re.compile(r'\.orderBy\(', re.IGNORECASE)


class SemanticRiskAnalyzer:
    """Analisa notebooks PySpark gerados em busca de riscos semânticos."""

    def analyze_notebook(self, notebook_path: Path) -> SemanticRiskReport:
        """Analisa um notebook e retorna riscos detectados."""
        report = SemanticRiskReport(notebook=notebook_path.stem)
        try:
            content = notebook_path.read_text(encoding="utf-8")
        except OSError:
            return report

        lines = content.splitlines()

        self._check_join_cardinality(content, lines, report)
        self._check_date_as_string(content, lines, report)
        self._check_filter_null(content, lines, report)
        self._check_window_order(content, lines, report)
        self._check_unsafe_cast(content, lines, report)
        self._check_append_dedup(content, lines, report)
        self._check_rownum_order(content, lines, report)

        return report

    def analyze_directory(self, output_dir: Path) -> list[SemanticRiskReport]:
        """Analisa todos os notebooks .py em output_dir."""
        reports = []
        for nb in sorted(output_dir.glob("*.py")):
            report = self.analyze_notebook(nb)
            if report.has_risks:
                reports.append(report)
        return reports

    # ------------------------------------------------------------------
    # Checkers
    # ------------------------------------------------------------------

    def _check_join_cardinality(
        self, content: str, lines: list[str], report: SemanticRiskReport
    ) -> None:
        for i, line in enumerate(lines, 1):
            if _RE_JOIN.search(line):
                # Risco: join sem assert de cardinalidade nas 20 linhas seguintes
                context = "\n".join(lines[i : i + 20])
                if not re.search(r'assert|count\(\)|dropDuplicates', context, re.IGNORECASE):
                    report.risks.append(SemanticRisk(
                        code="SR-001",
                        description="JOIN sem verificação de cardinalidade — risco de explosão de linhas",
                        mitigation="Adicionar assert df.count() == expected ou verificar unicidade das chaves antes do join",
                        line=i,
                        snippet=line.strip()[:120],
                    ))
                    break  # um aviso por notebook é suficiente

    def _check_date_as_string(
        self, content: str, lines: list[str], report: SemanticRiskReport
    ) -> None:
        for i, line in enumerate(lines, 1):
            if _RE_DATE_AS_STRING.search(line):
                report.risks.append(SemanticRisk(
                    code="SR-002",
                    description="Coluna de data/timestamp atribuída como literal string — comparações e ordenações podem divergir",
                    mitigation="Usar F.to_date() ou F.to_timestamp() em vez de F.lit() para colunas de data",
                    line=i,
                    snippet=line.strip()[:120],
                ))
                break

    def _check_filter_null(
        self, content: str, lines: list[str], report: SemanticRiskReport
    ) -> None:
        for i, line in enumerate(lines, 1):
            if _RE_FILTER_WITHOUT_NULL.search(line) and not _RE_HAS_IS_NOT_NULL.search(line):
                report.risks.append(SemanticRisk(
                    code="SR-003",
                    description="Filtro de exclusão sem tratamento de NULL — NULLs são silenciosamente excluídos no Spark, comportamento pode diferir do SAS",
                    mitigation="Adicionar .filter(F.col('x').isNotNull()) antes do filtro ou usar isNull() explicitamente",
                    line=i,
                    snippet=line.strip()[:120],
                ))
                break

    def _check_window_order(
        self, content: str, lines: list[str], report: SemanticRiskReport
    ) -> None:
        if _RE_WINDOW_NO_ORDER.search(content):
            report.risks.append(SemanticRisk(
                code="SR-004",
                description="Window.partitionBy() sem .orderBy() — resultado não determinístico; pode diferir do RETAIN/BY group do SAS",
                mitigation="Adicionar .orderBy() à especificação da Window para garantir ordenação determinística",
            ))

    def _check_unsafe_cast(
        self, content: str, lines: list[str], report: SemanticRiskReport
    ) -> None:
        for i, line in enumerate(lines, 1):
            if _RE_UNSAFE_CAST.search(line):
                report.risks.append(SemanticRisk(
                    code="SR-005",
                    description="cast() numérico sem tratamento de erro — valores não-numéricos viram NULL silenciosamente",
                    mitigation="Usar F.try_cast() (Spark 3.4+) ou adicionar filtro de validação antes do cast",
                    line=i,
                    snippet=line.strip()[:120],
                ))
                break

    def _check_append_dedup(
        self, content: str, lines: list[str], report: SemanticRiskReport
    ) -> None:
        if _RE_APPEND_MODE.search(content) and not _RE_DROP_DUPLICATES.search(content):
            report.risks.append(SemanticRisk(
                code="SR-006",
                description=".write.mode('append') sem deduplicação — cada execução adiciona linhas, causando duplicação silenciosa",
                mitigation="Substituir por MERGE INTO (Delta) ou adicionar .dropDuplicates() antes do write",
            ))

    def _check_rownum_order(
        self, content: str, lines: list[str], report: SemanticRiskReport
    ) -> None:
        if _RE_ROW_NUMBER.search(content) and not _RE_ORDER_BY_WINDOW.search(content):
            report.risks.append(SemanticRisk(
                code="SR-007",
                description="row_number()/rank() sem .orderBy() na Window — numeração não determinística",
                mitigation="Especificar .orderBy() na Window spec antes de aplicar row_number()/rank()",
            ))
