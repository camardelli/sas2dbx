"""UnresolvedError — captura e empacota contexto completo para o Evolution Engine.

Quando o SelfHealingPipeline esgota suas iterações sem resolver um erro,
este módulo empacota TUDO que o EvolutionAnalyzer precisa para propor um
fix no código-fonte da aplicação.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class HealingAttempt:
    """Uma tentativa de fix pelo SelfHealingPipeline.

    Attributes:
        iteration: Número da tentativa (1-based).
        strategy: Estratégia tentada ("deterministic", "llm", "none", etc.).
        fix_applied: Descrição do fix aplicado (None se strategy=="none").
        retest_status: Status do retest ("SUCCESS", "FAILED", "TIMEOUT", None).
        error_after: Erro após a tentativa (None se retest bem-sucedido).
    """

    iteration: int
    strategy: str
    fix_applied: str | None
    retest_status: str | None
    error_after: str | None


@dataclass
class UnresolvedError:
    """Pacote completo de contexto para o Evolution Engine.

    Contém TUDO que o EvolutionAnalyzer precisa para analisar e propor um
    fix no código-fonte da aplicação — NÃO no notebook individual.

    Attributes:
        job_id: Identificador do job que falhou.
        migration_id: ID da migração (UUID).
        notebook_path: Caminho absoluto do notebook que falhou.
        sas_original: Código SAS original do bloco que gerou o notebook.
        pyspark_generated: Conteúdo do notebook PySpark que falhou.
        databricks_error: Mensagem de erro do Databricks (última tentativa).
        error_category: Categoria do patterns.py ("missing_table", "UNKNOWN", etc.).
        healing_attempts: Tentativas de fix que falharam.
        construct_type: Tipo do construto SAS ("PROC_SQL", "DATA_STEP", etc.).
        knowledge_context: Contexto do KS usado no prompt de transpilação.
        similar_jobs_affected: Outros jobs com o mesmo padrão de erro (heurística).
        timestamp: ISO 8601 UTC.
    """

    job_id: str
    migration_id: str
    notebook_path: str
    sas_original: str
    pyspark_generated: str
    databricks_error: str
    error_category: str
    healing_attempts: list[HealingAttempt] = field(default_factory=list)
    construct_type: str = "UNKNOWN"
    knowledge_context: dict = field(default_factory=dict)
    similar_jobs_affected: list[str] = field(default_factory=list)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_json(self) -> str:
        """Serializa para JSON compacto."""
        data = asdict(self)
        return json.dumps(data, ensure_ascii=False, indent=2)

    def save(self, output_dir: Path) -> Path:
        """Persiste em disco para auditoria e retomada.

        Args:
            output_dir: Diretório onde salvar (criado se não existir).

        Returns:
            Path do arquivo salvo.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"unresolved_{self.job_id}_{self.timestamp[:10]}.json"
        path = output_dir / filename
        path.write_text(self.to_json(), encoding="utf-8")
        logger.info("UnresolvedError salvo em %s", path)
        return path

    @classmethod
    def from_healing_report(
        cls,
        job_id: str,
        migration_id: str,
        notebook_path: Path,
        healing_report: object,  # HealingReport — evita import circular
        sas_original: str = "",
        construct_type: str = "UNKNOWN",
        knowledge_context: dict | None = None,
        similar_jobs: list[str] | None = None,
    ) -> "UnresolvedError":
        """Constrói UnresolvedError a partir de um HealingReport falho.

        Args:
            job_id: ID do job.
            migration_id: ID da migração.
            notebook_path: Caminho do notebook.
            healing_report: HealingReport com healed=False.
            sas_original: Código SAS original.
            construct_type: Tipo do construto.
            knowledge_context: Contexto do KS.
            similar_jobs: Jobs com padrão similar.

        Returns:
            UnresolvedError pronto para o EvolutionAnalyzer.
        """
        # Lê conteúdo atual do notebook (pode ter sido modificado pelo fixer)
        try:
            pyspark_generated = Path(notebook_path).read_text(encoding="utf-8")
        except OSError:
            pyspark_generated = "<notebook não encontrado>"

        # Extrai último erro
        last_error = ""
        report = healing_report
        if hasattr(report, "original_result") and report.original_result:
            last_error = report.original_result.error or ""
        if hasattr(report, "suggestion") and report.suggestion:
            if (
                report.suggestion.retest_result is not None
                and hasattr(report.suggestion.retest_result, "execution_result")
                and report.suggestion.retest_result.execution_result
            ):
                exec_result = report.suggestion.retest_result.execution_result
                last_error = exec_result.error or last_error

        # Extrai categoria — tenta report.diagnostic, depois report.suggestion.diagnostic
        category = "UNKNOWN"
        diag = None
        if hasattr(report, "diagnostic") and report.diagnostic:
            diag = report.diagnostic
        elif (
            hasattr(report, "suggestion")
            and report.suggestion
            and hasattr(report.suggestion, "diagnostic")
            and report.suggestion.diagnostic
        ):
            diag = report.suggestion.diagnostic
        if diag and hasattr(diag, "category"):
            category = diag.category

        # Constrói tentativas
        attempts: list[HealingAttempt] = []
        if hasattr(report, "iterations"):
            for i in range(report.iterations):
                attempts.append(
                    HealingAttempt(
                        iteration=i + 1,
                        strategy=getattr(report.suggestion, "strategy", "unknown")
                        if i == report.iterations - 1
                        else "unknown",
                        fix_applied=getattr(report.suggestion, "description", None)
                        if i == report.iterations - 1
                        else None,
                        retest_status="FAILED",
                        error_after=last_error if i == report.iterations - 1 else None,
                    )
                )

        return cls(
            job_id=job_id,
            migration_id=migration_id,
            notebook_path=str(notebook_path),
            sas_original=sas_original,
            pyspark_generated=pyspark_generated,
            databricks_error=last_error,
            error_category=category,
            healing_attempts=attempts,
            construct_type=construct_type,
            knowledge_context=knowledge_context or {},
            similar_jobs_affected=similar_jobs or [],
        )
