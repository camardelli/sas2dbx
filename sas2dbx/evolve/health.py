"""HealthMonitor — métricas de saúde do pipeline e alertas.

Checkpoints automáticos a cada N jobs. Se o health score cai mais de 10
pontos entre checkpoints, ou se a taxa de sucesso fica abaixo de 50%, o
pipeline é pausado para investigação.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PipelineHealth:
    """Snapshot de saúde do pipeline em um ponto no tempo.

    Attributes:
        timestamp: ISO 8601 UTC.
        jobs_processed: Jobs processados até este checkpoint.
        jobs_total: Total de jobs no batch.
        first_attempt_success_rate: % que passou na 1ª tentativa (0.0-1.0).
        healing_success_rate: % resolvido pelo self-healing (dos que falharam na 1ª).
        evolution_success_rate: % resolvido pelo evolution agent (dos residuais).
        knowledge_entries_added: Novas entradas no KS nesta sessão.
        patterns_added: Novos error patterns adicionados.
        code_fixes_applied: Fixes no código-fonte aplicados.
        code_fixes_reverted: Fixes revertidos (auto-rollback).
        avg_time_per_job_seconds: Tempo médio por job em segundos.
        avg_healing_time_seconds: Tempo médio do self-healing.
        avg_evolution_time_seconds: Tempo médio do evolution agent.
        human_review_pending: Jobs na fila de revisão humana.
        quarantine_pending: Fixes em quarentena aguardando aprovação.
    """

    timestamp: str
    jobs_processed: int
    jobs_total: int

    first_attempt_success_rate: float = 0.0
    healing_success_rate: float = 0.0
    evolution_success_rate: float = 0.0

    knowledge_entries_added: int = 0
    patterns_added: int = 0
    code_fixes_applied: int = 0
    code_fixes_reverted: int = 0

    avg_time_per_job_seconds: float = 0.0
    avg_healing_time_seconds: float = 0.0
    avg_evolution_time_seconds: float = 0.0

    human_review_pending: int = 0
    quarantine_pending: int = 0

    @property
    def health_score(self) -> float:
        """Score 0-100 de saúde do pipeline.

        Ponderação:
          40% first_attempt_success_rate
          25% healing_success_rate
          20% evolution_success_rate
          15% (1 - revert_rate)
        """
        revert_rate = self.code_fixes_reverted / max(self.code_fixes_applied, 1)
        revert_score = max(0.0, 1.0 - revert_rate)
        return (
            self.first_attempt_success_rate * 40.0
            + self.healing_success_rate * 25.0
            + self.evolution_success_rate * 20.0
            + revert_score * 15.0
        )

    @property
    def progress_pct(self) -> float:
        """Percentual de progresso do batch (0-100)."""
        if self.jobs_total <= 0:
            return 0.0
        return min(100.0, self.jobs_processed / self.jobs_total * 100)

    def to_dict(self) -> dict:
        """Serializa para dict (para API e persistência)."""
        d = asdict(self)
        d["health_score"] = round(self.health_score, 1)
        d["progress_pct"] = round(self.progress_pct, 1)
        return d


@dataclass
class HealthAction:
    """Ação recomendada pelo HealthMonitor.

    Attributes:
        action: "CONTINUE" | "WARNING" | "PAUSE_PIPELINE".
        reason: Motivo legível.
    """

    action: str
    reason: str

    @property
    def should_pause(self) -> bool:
        return self.action == "PAUSE_PIPELINE"

    @property
    def is_warning(self) -> bool:
        return self.action == "WARNING"


class HealthMonitor:
    """Monitora saúde do pipeline e dispara alertas e pausas automáticas.

    Args:
        storage_path: Arquivo JSON para persistir snapshots de saúde.
        checkpoint_interval: Jobs entre checkpoints automáticos.
    """

    CHECKPOINT_INTERVAL = 50
    PAUSE_THRESHOLD_DROP = 10.0    # pontos de queda no health score
    PAUSE_REVERT_RATE = 0.20       # 20% de reverts dispara pausa
    WARNING_SUCCESS_RATE = 0.70
    CRITICAL_SUCCESS_RATE = 0.50

    def __init__(
        self,
        storage_path: Path,
        checkpoint_interval: int = CHECKPOINT_INTERVAL,
    ) -> None:
        self._path = storage_path
        self._interval = checkpoint_interval
        self._snapshots: list[PipelineHealth] = self._load()
        # Contadores acumulados desde o início do batch
        self._counters: dict[str, int | float] = {
            "first_ok": 0,
            "first_fail": 0,
            "heal_ok": 0,
            "heal_fail": 0,
            "evolve_ok": 0,
            "evolve_fail": 0,
            "ks_entries": 0,
            "patterns": 0,
            "fixes_applied": 0,
            "fixes_reverted": 0,
            "time_total": 0.0,
            "time_healing": 0.0,
            "time_evolution": 0.0,
        }

    # ------------------------------------------------------------------
    # Recording events
    # ------------------------------------------------------------------

    def record_job(
        self,
        success: bool,
        heal_attempted: bool = False,
        heal_success: bool = False,
        evolve_attempted: bool = False,
        evolve_success: bool = False,
        job_time_seconds: float = 0.0,
        heal_time_seconds: float = 0.0,
        evolve_time_seconds: float = 0.0,
    ) -> None:
        """Registra resultado de um job processado."""
        if success and not heal_attempted:
            self._counters["first_ok"] += 1
        else:
            self._counters["first_fail"] += 1

        if heal_attempted:
            if heal_success:
                self._counters["heal_ok"] += 1
            else:
                self._counters["heal_fail"] += 1

        if evolve_attempted:
            if evolve_success:
                self._counters["evolve_ok"] += 1
            else:
                self._counters["evolve_fail"] += 1

        self._counters["time_total"] = float(self._counters["time_total"]) + job_time_seconds
        self._counters["time_healing"] = float(self._counters["time_healing"]) + heal_time_seconds
        self._counters["time_evolution"] = float(self._counters["time_evolution"]) + evolve_time_seconds

    def record_ks_entry(self) -> None:
        """Registra adição de entrada no Knowledge Store."""
        self._counters["ks_entries"] += 1

    def record_pattern_added(self) -> None:
        """Registra adição de error pattern."""
        self._counters["patterns"] += 1

    def record_fix_applied(self) -> None:
        """Registra fix no código-fonte aplicado."""
        self._counters["fixes_applied"] += 1

    def record_fix_reverted(self) -> None:
        """Registra rollback de fix."""
        self._counters["fixes_reverted"] += 1

    # ------------------------------------------------------------------
    # Checkpoint
    # ------------------------------------------------------------------

    def checkpoint(
        self,
        jobs_processed: int,
        jobs_total: int,
        quarantine_pending: int = 0,
        human_review_pending: int = 0,
    ) -> HealthAction:
        """Avalia saúde e retorna ação recomendada.

        Args:
            jobs_processed: Jobs processados até agora.
            jobs_total: Total de jobs no batch.
            quarantine_pending: Fixes em quarentena.
            human_review_pending: Jobs pendentes de revisão humana.

        Returns:
            HealthAction com decision e reason.
        """
        health = self._build_health(
            jobs_processed, jobs_total, quarantine_pending, human_review_pending
        )
        self._snapshots.append(health)
        # Mantém no máximo 200 snapshots em memória (evita crescimento ilimitado)
        if len(self._snapshots) > 200:
            self._snapshots = self._snapshots[-200:]
        self._save()

        # Tendência de queda em 3 checkpoints consecutivos
        if len(self._snapshots) >= 3:
            last3 = [s.health_score for s in self._snapshots[-3:]]
            if last3[0] > last3[1] > last3[2]:
                drop = last3[0] - last3[2]
                if drop > self.PAUSE_THRESHOLD_DROP:
                    return HealthAction(
                        "PAUSE_PIPELINE",
                        f"Health score em queda contínua: {last3[0]:.0f} → {last3[2]:.0f} "
                        f"(queda de {drop:.0f} pts em 3 checkpoints)",
                    )

        # Taxa de sucesso crítica
        if health.first_attempt_success_rate < self.CRITICAL_SUCCESS_RATE:
            return HealthAction(
                "PAUSE_PIPELINE",
                f"Taxa de sucesso na 1ª tentativa crítica: "
                f"{health.first_attempt_success_rate:.0%} (mínimo: "
                f"{self.CRITICAL_SUCCESS_RATE:.0%})",
            )

        # Taxa de revert alta
        if health.code_fixes_applied > 0:
            revert_rate = health.code_fixes_reverted / health.code_fixes_applied
            if revert_rate > self.PAUSE_REVERT_RATE:
                return HealthAction(
                    "PAUSE_PIPELINE",
                    f"Taxa de rollback de fixes alta: {revert_rate:.0%} "
                    f"(limite: {self.PAUSE_REVERT_RATE:.0%})",
                )

        # Aviso de taxa baixa
        if health.first_attempt_success_rate < self.WARNING_SUCCESS_RATE:
            return HealthAction(
                "WARNING",
                f"Taxa de sucesso na 1ª tentativa abaixo do ideal: "
                f"{health.first_attempt_success_rate:.0%} "
                f"(alvo: {self.WARNING_SUCCESS_RATE:.0%})",
            )

        return HealthAction(
            "CONTINUE",
            f"Health score: {health.health_score:.0f}/100 | "
            f"1ª tentativa: {health.first_attempt_success_rate:.0%} | "
            f"Progresso: {health.progress_pct:.0f}%",
        )

    def latest(self) -> PipelineHealth | None:
        """Retorna o snapshot mais recente."""
        return self._snapshots[-1] if self._snapshots else None

    def all_snapshots(self) -> list[dict]:
        """Retorna todos os snapshots como lista de dicts (para API/dashboard)."""
        return [s.to_dict() for s in self._snapshots]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_health(
        self,
        jobs_processed: int,
        jobs_total: int,
        quarantine_pending: int,
        human_review_pending: int,
    ) -> PipelineHealth:
        """Constrói PipelineHealth a partir dos contadores acumulados."""
        c = self._counters
        total_first = int(c["first_ok"]) + int(c["first_fail"])
        total_heal = int(c["heal_ok"]) + int(c["heal_fail"])
        total_evolve = int(c["evolve_ok"]) + int(c["evolve_fail"])

        first_rate = int(c["first_ok"]) / max(total_first, 1)
        heal_rate = int(c["heal_ok"]) / max(total_heal, 1) if total_heal > 0 else 0.0
        evolve_rate = int(c["evolve_ok"]) / max(total_evolve, 1) if total_evolve > 0 else 0.0

        avg_time = float(c["time_total"]) / max(jobs_processed, 1)
        avg_heal = float(c["time_healing"]) / max(total_heal, 1) if total_heal > 0 else 0.0
        avg_evolve = float(c["time_evolution"]) / max(total_evolve, 1) if total_evolve > 0 else 0.0

        return PipelineHealth(
            timestamp=datetime.now(timezone.utc).isoformat(),
            jobs_processed=jobs_processed,
            jobs_total=jobs_total,
            first_attempt_success_rate=round(first_rate, 4),
            healing_success_rate=round(heal_rate, 4),
            evolution_success_rate=round(evolve_rate, 4),
            knowledge_entries_added=int(c["ks_entries"]),
            patterns_added=int(c["patterns"]),
            code_fixes_applied=int(c["fixes_applied"]),
            code_fixes_reverted=int(c["fixes_reverted"]),
            avg_time_per_job_seconds=round(avg_time, 2),
            avg_healing_time_seconds=round(avg_heal, 2),
            avg_evolution_time_seconds=round(avg_evolve, 2),
            human_review_pending=human_review_pending,
            quarantine_pending=quarantine_pending,
        )

    def _save(self) -> None:
        """Persiste snapshots em disco."""
        import tempfile, os

        data = [s.to_dict() for s in self._snapshots]
        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd, tmp = tempfile.mkstemp(dir=self._path.parent, suffix=".tmp")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            Path(tmp).replace(self._path)
        except OSError as exc:
            logger.warning("HealthMonitor: não foi possível salvar snapshots: %s", exc)

    def _load(self) -> list[PipelineHealth]:
        """Carrega snapshots existentes."""
        try:
            if self._path.exists():
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                return [
                    PipelineHealth(
                        timestamp=d["timestamp"],
                        jobs_processed=d["jobs_processed"],
                        jobs_total=d["jobs_total"],
                        first_attempt_success_rate=d.get("first_attempt_success_rate", 0.0),
                        healing_success_rate=d.get("healing_success_rate", 0.0),
                        evolution_success_rate=d.get("evolution_success_rate", 0.0),
                        knowledge_entries_added=d.get("knowledge_entries_added", 0),
                        patterns_added=d.get("patterns_added", 0),
                        code_fixes_applied=d.get("code_fixes_applied", 0),
                        code_fixes_reverted=d.get("code_fixes_reverted", 0),
                        avg_time_per_job_seconds=d.get("avg_time_per_job_seconds", 0.0),
                        avg_healing_time_seconds=d.get("avg_healing_time_seconds", 0.0),
                        avg_evolution_time_seconds=d.get("avg_evolution_time_seconds", 0.0),
                        human_review_pending=d.get("human_review_pending", 0),
                        quarantine_pending=d.get("quarantine_pending", 0),
                    )
                    for d in raw
                ]
        except Exception:
            pass
        return []
