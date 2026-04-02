"""FixApplier — aplica fixes aprovados com hot-reload e auto-rollback.

Fluxo:
  1. Snapshot dos arquivos a modificar (para rollback)
  2. Aplica fix (escreve arquivos em disco)
  3. Hot-reload dos módulos afetados (importlib.reload — sem restart Docker)
  4. Inicia monitoramento: próximos N jobs
  5. Se taxa de falha AUMENTA → rollback automático

Importante: Docker rebuild NÃO acontece aqui. Hot-reload é suficiente para
fixes Tier A/B (patterns, prompts, knowledge store). Tier C (engine rules)
vai para quarentena e nunca é auto-aplicado.
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import logging
import shutil
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

from sas2dbx.evolve.agent import EvolutionProposal, FileModification

logger = logging.getLogger(__name__)

# Módulos que suportam hot-reload após modificação de arquivo
_RELOADABLE_MODULES: dict[str, str] = {
    "sas2dbx/validate/heal/patterns.py": "sas2dbx.validate.heal.patterns",
    "sas2dbx/transpile/llm/prompts.py": "sas2dbx.transpile.llm.prompts",
    "sas2dbx/document/prompts.py": "sas2dbx.document.prompts",
}


@dataclass
class ApplySnapshot:
    """Snapshot do estado dos arquivos antes de aplicar o fix (para rollback).

    Attributes:
        file_path: Caminho relativo do arquivo modificado.
        original_content: Conteúdo antes do fix (None se arquivo era inexistente).
    """

    file_path: str
    original_content: str | None  # None = arquivo não existia antes


@dataclass
class ApplyResult:
    """Resultado da aplicação de um fix.

    Attributes:
        proposal_description: Descrição do fix aplicado.
        files_modified: Caminhos dos arquivos modificados.
        status: "APPLIED" | "REVERTED" | "FAILED".
        reason: Motivo do status (útil em caso de REVERTED/FAILED).
        hot_reloaded: Módulos que foram hot-reloaded.
        timestamp: ISO 8601 UTC.
    """

    proposal_description: str
    files_modified: list[str]
    status: str
    reason: str
    hot_reloaded: list[str] = field(default_factory=list)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class FixApplier:
    """Aplica fixes aprovados com hot-reload e monitoramento de impacto.

    Args:
        project_root: Raiz do projeto (onde os arquivos vivem).
        monitoring_window: Número de jobs para monitorar após cada fix.
        history_path: Caminho para persistir histórico de fixes aplicados.
    """

    MONITORING_WINDOW = 10

    def __init__(
        self,
        project_root: Path,
        monitoring_window: int = MONITORING_WINDOW,
        history_path: Path | None = None,
    ) -> None:
        self._root = project_root
        self._monitoring_window = monitoring_window
        self._history_path = history_path or project_root / "sas2dbx_work" / "catalog" / "evolution_history.json"
        self._history: list[dict] = self._load_history()
        # Métricas para monitoramento pós-fix
        self._monitoring_results: list[bool] = []  # True=sucesso, False=falha
        self._pre_fix_failure_rate: float = 0.0
        self._active_snapshots: list[ApplySnapshot] = []

    def apply(
        self,
        proposal: EvolutionProposal,
        pre_fix_failure_rate: float = 0.0,
    ) -> ApplyResult:
        """Aplica fix no código-fonte com hot-reload.

        Args:
            proposal: EvolutionProposal aprovado pelo QualityGate.
            pre_fix_failure_rate: Taxa de falha antes do fix (0.0-1.0).

        Returns:
            ApplyResult com status APPLIED ou FAILED.
        """
        self._pre_fix_failure_rate = pre_fix_failure_rate
        self._monitoring_results = []
        self._active_snapshots = []

        modified: list[str] = []
        hot_reloaded: list[str] = []

        try:
            # 1. Snapshot para rollback
            for fm in proposal.files_to_modify:
                target = self._root / fm.path
                original = target.read_text(encoding="utf-8") if target.exists() else None
                self._active_snapshots.append(ApplySnapshot(fm.path, original))

            # 2. Aplica modificações
            for fm in proposal.files_to_modify:
                applied = self._apply_modification(fm)
                if applied:
                    modified.append(fm.path)

            # 3. Hot-reload dos módulos afetados
            for path in modified:
                module_name = _RELOADABLE_MODULES.get(path)
                if module_name:
                    reloaded = self._hot_reload(module_name)
                    if reloaded:
                        hot_reloaded.append(module_name)

            # 4. Persiste o teste novo (não precisa reload — apenas arquivo)
            if proposal.test:
                test_target = self._root / proposal.test.path
                test_target.parent.mkdir(parents=True, exist_ok=True)
                test_target.write_text(proposal.test.content, encoding="utf-8")
                modified.append(proposal.test.path)

            result = ApplyResult(
                proposal_description=proposal.description,
                files_modified=modified,
                status="APPLIED",
                reason=f"Fix aplicado com sucesso: {proposal.fix_type}",
                hot_reloaded=hot_reloaded,
            )

            self._record_history(proposal, result)
            logger.info(
                "FixApplier: fix aplicado — %d arquivo(s) modificado(s), %d módulo(s) reloaded",
                len(modified),
                len(hot_reloaded),
            )
            return result

        except Exception as exc:
            logger.error("FixApplier: erro ao aplicar fix: %s", exc)
            # Rollback automático em caso de erro de I/O
            self._rollback()
            return ApplyResult(
                proposal_description=proposal.description,
                files_modified=[],
                status="FAILED",
                reason=f"Erro ao aplicar fix: {exc}",
            )

    def record_job_result(self, success: bool) -> bool:
        """Registra resultado de um job pós-fix para monitoramento.

        Args:
            success: True se o job passou, False se falhou.

        Returns:
            True se deve continuar; False se rollback foi acionado.
        """
        self._monitoring_results.append(success)

        if len(self._monitoring_results) < self._monitoring_window:
            return True  # janela de monitoramento não completa ainda

        if len(self._monitoring_results) == self._monitoring_window:
            current_failure_rate = 1 - (
                sum(self._monitoring_results) / len(self._monitoring_results)
            )
            if current_failure_rate > self._pre_fix_failure_rate + 0.1:
                logger.warning(
                    "FixApplier: taxa de falha PIOROU após fix "
                    "(antes=%.0f%% depois=%.0f%%) — rollback automático",
                    self._pre_fix_failure_rate * 100,
                    current_failure_rate * 100,
                )
                self._rollback()
                return False

            logger.info(
                "FixApplier: fix confirmado após %d jobs "
                "(taxa de falha: antes=%.0f%% depois=%.0f%%)",
                self._monitoring_window,
                self._pre_fix_failure_rate * 100,
                current_failure_rate * 100,
            )
        return True

    def rollback_last(self) -> bool:
        """Rollback manual do último fix aplicado.

        Returns:
            True se rollback bem-sucedido.
        """
        return self._rollback()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _apply_modification(self, fm: FileModification) -> bool:
        """Aplica uma FileModification. Retorna True se modificou."""
        target = self._root / fm.path
        target.parent.mkdir(parents=True, exist_ok=True)

        if fm.action == "add":
            target.write_text(fm.content, encoding="utf-8")
            return True

        elif fm.action == "append":
            existing = target.read_text(encoding="utf-8") if target.exists() else ""
            target.write_text(existing + "\n" + fm.content, encoding="utf-8")
            return True

        elif fm.action == "modify":
            if fm.old_string and target.exists():
                existing = target.read_text(encoding="utf-8")
                if fm.old_string in existing:
                    target.write_text(
                        existing.replace(fm.old_string, fm.content, 1),
                        encoding="utf-8",
                    )
                    return True
                else:
                    raise ValueError(
                        f"old_string não encontrado em '{fm.path}' — "
                        "fix abortado com rollback automático"
                    )
            else:
                target.write_text(fm.content, encoding="utf-8")
                return True

        logger.warning("FixApplier: action desconhecida '%s' — pulando", fm.action)
        return False

    def _hot_reload(self, module_name: str) -> bool:
        """Recarrega um módulo Python em memória. Retorna True se bem-sucedido."""
        import sys

        try:
            if module_name in sys.modules:
                importlib.reload(sys.modules[module_name])
                logger.debug("FixApplier: hot-reload OK para %s", module_name)
                return True
            else:
                # Módulo não estava importado — importa agora
                importlib.import_module(module_name)
                return True
        except Exception as exc:
            logger.warning("FixApplier: hot-reload falhou para %s: %s", module_name, exc)
            return False

    def _rollback(self) -> bool:
        """Restaura arquivos ao estado anterior ao fix."""
        if not self._active_snapshots:
            return False

        success = True
        for snap in self._active_snapshots:
            target = self._root / snap.file_path
            try:
                if snap.original_content is None:
                    # Arquivo não existia → remove
                    if target.exists():
                        target.unlink()
                else:
                    target.write_text(snap.original_content, encoding="utf-8")

                # Hot-reload após rollback
                module_name = _RELOADABLE_MODULES.get(snap.file_path)
                if module_name:
                    self._hot_reload(module_name)
            except Exception as exc:
                logger.error("FixApplier: erro no rollback de %s: %s", snap.file_path, exc)
                success = False

        self._active_snapshots = []
        logger.info("FixApplier: rollback %s", "OK" if success else "com erros")
        return success

    def _record_history(self, proposal: EvolutionProposal, result: ApplyResult) -> None:
        """Persiste histórico de fix aplicado."""
        self._history.append(
            {
                "timestamp": result.timestamp,
                "fix_type": proposal.fix_type,
                "risk_level": proposal.risk_level,
                "description": proposal.description,
                "files": result.files_modified,
                "status": result.status,
                "hot_reloaded": result.hot_reloaded,
            }
        )
        try:
            self._history_path.parent.mkdir(parents=True, exist_ok=True)
            self._history_path.write_text(
                json.dumps(self._history, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.warning("FixApplier: não foi possível salvar histórico: %s", exc)

    def _load_history(self) -> list[dict]:
        """Carrega histórico existente."""
        try:
            if self._history_path.exists():
                return json.loads(self._history_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        return []
