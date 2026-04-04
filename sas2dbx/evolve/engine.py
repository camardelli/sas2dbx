"""EvolutionEngine — orquestra o ciclo completo de auto-evolução.

Chamado pelo worker quando o SelfHealingPipeline esgota suas tentativas
sem curar o notebook. O engine:

  1. Empacota o contexto em UnresolvedError
  2. Chama EvolutionAnalyzer para proposta via LLM
  3. Avalia a proposta no QualityGate
  4. Aplica ou coloca em quarentena conforme o risk_level
  5. Registra tudo no HealthMonitor
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from sas2dbx.evolve.unresolved import UnresolvedError
from sas2dbx.evolve.agent import EvolutionAnalyzer, EvolutionProposal
from sas2dbx.evolve.gate import QualityGate, GateResult
from sas2dbx.evolve.applier import FixApplier, ApplyResult
from sas2dbx.evolve.quarantine import QuarantineStore
from sas2dbx.evolve.health import HealthMonitor

logger = logging.getLogger(__name__)


@dataclass
class EvolutionResult:
    """Resultado de uma rodada de auto-evolução.

    Attributes:
        unresolved_id: job_id do UnresolvedError processado.
        proposal_generated: True se EvolutionAnalyzer gerou proposta válida.
        gate_decision: "APPROVE" | "APPROVE_NOTIFY" | "QUARANTINE" | "REJECT" | "NO_PROPOSAL".
        applied: True se fix foi efetivamente aplicado no código-fonte.
        quarantine_entry_id: ID da entrada em quarentena (se QUARANTINE).
        fix_type: Tipo do fix proposto (para métricas).
        risk_level: Risk level do fix.
        description: Descrição do fix.
        elapsed_seconds: Tempo total da análise + gate + apply.
    """

    unresolved_id: str
    proposal_generated: bool
    gate_decision: str
    applied: bool
    quarantine_entry_id: str | None = None
    fix_type: str = ""
    risk_level: str = ""
    description: str = ""
    elapsed_seconds: float = 0.0


class EvolutionEngine:
    """Orquestra análise → gate → apply para UnresolvedErrors.

    Args:
        llm_client: LLMClient para o EvolutionAnalyzer.
        project_root: Raiz do projeto.
        health_monitor: HealthMonitor compartilhado com o worker.
        unresolved_dir: Diretório para salvar UnresolvedErrors em disco.
        test_timeout: Timeout em segundos para pytest no QualityGate.
    """

    def __init__(
        self,
        llm_client: object,
        project_root: Path,
        health_monitor: HealthMonitor,
        unresolved_dir: Path | None = None,
        test_timeout: int = 120,
        catalog_dir: Path | None = None,
        kb: object | None = None,
    ) -> None:
        # catalog_dir: onde quarantine.json e evolution_history.json são persistidos.
        # Separa dados persistentes do project_root para suportar ambientes Docker
        # onde project_root aponta para o código-fonte e o volume montado é outro path.
        _catalog = catalog_dir or (project_root / "sas2dbx_work" / "catalog")
        self._analyzer = EvolutionAnalyzer(llm_client, project_root, kb=kb)
        self._gate = QualityGate(project_root, test_timeout)
        self._applier = FixApplier(
            project_root,
            history_path=_catalog / "evolution_history.json",
        )
        self._quarantine = QuarantineStore(
            _catalog / "quarantine.json"
        )
        self._health = health_monitor
        self._unresolved_dir = unresolved_dir or (_catalog / "unresolved")

    def process(self, error: UnresolvedError) -> EvolutionResult:
        """Processa um UnresolvedError — análise → gate → apply.

        Args:
            error: UnresolvedError com contexto completo.

        Returns:
            EvolutionResult com o desfecho.
        """
        t0 = time.monotonic()

        # Persiste para auditoria
        error.save(self._unresolved_dir)

        logger.info(
            "EvolutionEngine: processando job=%s category=%s",
            error.job_id,
            error.error_category,
        )

        # 1. Análise via LLM
        proposal = self._analyzer.analyze_sync(error)
        if not proposal or not proposal.is_valid:
            elapsed = time.monotonic() - t0
            logger.warning(
                "EvolutionEngine: EvolutionAnalyzer não gerou proposta válida para job=%s",
                error.job_id,
            )
            self._health.record_job(
                success=False,
                evolve_attempted=True,
                evolve_success=False,
                evolve_time_seconds=elapsed,
            )
            return EvolutionResult(
                unresolved_id=error.job_id,
                proposal_generated=False,
                gate_decision="NO_PROPOSAL",
                applied=False,
                elapsed_seconds=elapsed,
            )

        logger.info(
            "EvolutionEngine: proposta gerada — fix_type=%s risk=%s",
            proposal.fix_type,
            proposal.risk_level,
        )

        # 2. Quality Gate
        gate_result = self._gate.evaluate(proposal)
        logger.info(
            "EvolutionEngine: QualityGate decision=%s reason=%s",
            gate_result.decision,
            gate_result.reason,
        )

        elapsed = time.monotonic() - t0
        quarantine_id: str | None = None
        applied = False

        # 3. Apply ou Quarantine
        if gate_result.decision == "QUARANTINE":
            quarantine_id = self._quarantine.submit(error.job_id, proposal)
            logger.info(
                "EvolutionEngine: fix em quarentena — entry_id=%s", quarantine_id
            )

        elif gate_result.approved:
            apply_result = self._applier.apply(proposal)
            applied = apply_result.status == "APPLIED"

            if applied:
                # Atualiza métricas de evolução
                self._health.record_fix_applied()
                if proposal.fix_type == "error_pattern":
                    self._health.record_pattern_added()
                elif proposal.fix_type == "knowledge_store":
                    self._health.record_ks_entry()

                logger.info(
                    "EvolutionEngine: fix aplicado com hot-reload — %s",
                    apply_result.files_modified,
                )
            else:
                logger.warning(
                    "EvolutionEngine: FixApplier retornou status=%s", apply_result.status
                )

        # 4. Registra no HealthMonitor
        self._health.record_job(
            success=applied,
            evolve_attempted=True,
            evolve_success=applied,
            evolve_time_seconds=elapsed,
        )

        return EvolutionResult(
            unresolved_id=error.job_id,
            proposal_generated=True,
            gate_decision=gate_result.decision,
            applied=applied,
            quarantine_entry_id=quarantine_id,
            fix_type=proposal.fix_type,
            risk_level=proposal.risk_level,
            description=proposal.description,
            elapsed_seconds=elapsed,
        )

    def notify_job_result(self, success: bool) -> bool:
        """Informa resultado de um job pós-fix para monitoramento de rollback.

        Args:
            success: True se o job passou.

        Returns:
            False se rollback automático foi acionado.
        """
        should_continue = self._applier.record_job_result(success)
        if not should_continue:
            self._health.record_fix_reverted()
        return should_continue

    @property
    def quarantine(self) -> QuarantineStore:
        """Acesso ao QuarantineStore (para API de aprovação humana)."""
        return self._quarantine

    @property
    def health(self) -> HealthMonitor:
        """Acesso ao HealthMonitor (para API de dashboard)."""
        return self._health
