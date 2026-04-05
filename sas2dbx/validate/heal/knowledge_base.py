"""HealingKnowledgeBase — base de conhecimento evolutivo do self-healing.

Registra tentativas de fix e seus resultados por padrão de erro, permitindo:
  1. Detectar loops (mesmo erro, mesmo fix, múltiplas falhas)
  2. Injetar histórico de falhas no prompt do EvolutionEngine
  3. Pular fixes que já provaram não funcionar para este padrão

O KB é persistido em catalog_dir/healing_kb.json para sobreviver entre
sessões de validação e healing.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Número mínimo de falhas consecutivas com o mesmo fix para considerar "stuck"
_STUCK_THRESHOLD = 2
# Número de falhas de um fix para considerar que deve ser pulado
_SKIP_THRESHOLD = 2


class HealingKnowledgeBase:
    """Base de conhecimento evolutivo do ciclo de self-healing.

    Mantém histórico de tentativas de fix por padrão de erro e permite:
    - Detectar quando o sistema está em loop (is_stuck)
    - Decidir se um fix específico deve ser pulado (should_skip_fix)
    - Fornecer contexto rico ao EvolutionEngine via LLM (get_context_for_llm)

    Args:
        kb_path: Caminho para o arquivo JSON de persistência
                 (normalmente catalog_dir/healing_kb.json).
    """

    def __init__(self, kb_path: Path) -> None:
        self._path = kb_path
        self._data: dict[str, Any] = self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_attempt(
        self,
        error_category: str,
        error_key: str,
        fix_name: str,
        job_id: str,
        result: str,  # "success" | "failed" | "skipped"
        reason: str = "",
        healing_id: str = "",
    ) -> None:
        """Registra uma tentativa de fix para um padrão de erro.

        Args:
            error_category: Categoria do erro (ex: "unresolved_column_suggestion").
            error_key: Chave canônica do erro (ex: "fl_ativo").
            fix_name: Nome do fix aplicado (ex: "_fix_placeholder_add_column").
            job_id: Nome do notebook/job.
            result: "success", "failed" ou "skipped".
            reason: Descrição do resultado (mensagem de erro ou motivo do skip).
            healing_id: UUID do processo de healing (para correlação).
        """
        key = self._full_key(error_category, error_key)
        entry = self._data.setdefault(key, {
            "error_category": error_category,
            "error_key": error_key,
            "attempts": [],
        })

        entry["attempts"].append({
            "fix_name": fix_name,
            "job_id": job_id,
            "result": result,
            "reason": reason[:500] if reason else "",
            "healing_id": healing_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        self._save()
        logger.debug(
            "HealingKB: registrado %s/%s fix=%s result=%s",
            error_category, error_key, fix_name, result,
        )

    def is_stuck(self, error_category: str, error_key: str) -> bool:
        """True se o sistema está em loop para este padrão de erro.

        Define "stuck" como: as últimas N tentativas para este padrão
        resultaram todas em "failed", independente do fix usado.

        Args:
            error_category: Categoria do erro.
            error_key: Chave canônica do erro.

        Returns:
            True se stuck (loop detectado).
        """
        key = self._full_key(error_category, error_key)
        entry = self._data.get(key)
        if not entry:
            return False

        attempts = entry.get("attempts", [])
        if len(attempts) < _STUCK_THRESHOLD:
            return False

        # Verifica as últimas N tentativas
        recent = attempts[-_STUCK_THRESHOLD:]
        return all(a["result"] == "failed" for a in recent)

    def should_skip_fix(
        self, error_category: str, error_key: str, fix_name: str
    ) -> bool:
        """True se este fix específico já falhou vezes suficientes para este padrão.

        Args:
            error_category: Categoria do erro.
            error_key: Chave canônica do erro.
            fix_name: Nome do fix candidato.

        Returns:
            True se o fix deve ser pulado.
        """
        key = self._full_key(error_category, error_key)
        entry = self._data.get(key)
        if not entry:
            return False

        failures = sum(
            1
            for a in entry.get("attempts", [])
            if a["fix_name"] == fix_name and a["result"] == "failed"
        )
        return failures >= _SKIP_THRESHOLD

    def get_context_for_llm(self, error_category: str, error_key: str) -> str:
        """Retorna contexto formatado do histórico para injeção no prompt do LLM.

        O contexto inclui todas as tentativas de fix para este padrão de erro,
        ordenadas cronologicamente, para que o EvolutionEngine possa evitar
        sugerir fixes que já provaram não funcionar.

        Args:
            error_category: Categoria do erro.
            error_key: Chave canônica do erro.

        Returns:
            String formatada com o histórico de tentativas, ou string vazia
            se não há histórico.
        """
        key = self._full_key(error_category, error_key)
        entry = self._data.get(key)
        if not entry or not entry.get("attempts"):
            return ""

        lines = [
            f"HISTÓRICO DE TENTATIVAS ANTERIORES (padrão: {error_category}/{error_key}):",
        ]
        for i, att in enumerate(entry["attempts"], 1):
            status_emoji = "✓" if att["result"] == "success" else "✗"
            lines.append(
                f"  {i}. [{status_emoji}] fix={att['fix_name']}"
                f" | resultado={att['result']}"
                f" | job={att['job_id']}"
                + (f" | motivo: {att['reason']}" if att.get("reason") else "")
            )

        stuck = self.is_stuck(error_category, error_key)
        if stuck:
            lines.append(
                f"\n⚠ ATENÇÃO: Sistema em loop para este padrão — "
                f"{_STUCK_THRESHOLD} tentativas consecutivas falharam. "
                "Considere uma abordagem completamente diferente."
            )

        return "\n".join(lines)

    def compute_error_key(self, diagnostic: object) -> str:
        """Extrai chave canônica de um ErrorDiagnostic.

        A chave é usada para agrupar tentativas do mesmo padrão de erro.
        Estratégia: usa a entidade mais específica disponível no diagnóstico.

        Args:
            diagnostic: ErrorDiagnostic (duck typing — acessa .entities e .category).

        Returns:
            String canônica (ex: "fl_ativo", "telcostar.operacional.clientes").
        """
        if diagnostic is None:
            return "unknown"

        entities: dict = getattr(diagnostic, "entities", {}) or {}

        # Prioridade: bad_column → suggestion → table_name → column_name → pattern_key
        # bad_column: coluna específica não resolvida (unresolved_column_suggestion) —
        # deve ter precedência para que erros em colunas distintas recebam chaves
        # diferentes no KB e não sejam confundidos com um loop (false positive).
        for field in ("bad_column", "suggestion", "table_name", "column_name"):
            val = entities.get(field, "")
            if val:
                # Normaliza: minúsculas, remove espaços extras
                return re.sub(r"\s+", "_", val.strip().lower())

        # Fallback: pattern_key do diagnóstico
        pattern_key = getattr(diagnostic, "pattern_key", "") or ""
        if pattern_key:
            return pattern_key.lower()

        return "unknown"

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _full_key(self, error_category: str, error_key: str) -> str:
        return f"{error_category}:{error_key}"

    def _load(self) -> dict[str, Any]:
        """Carrega KB do disco; retorna dict vazio se arquivo não existe."""
        if not self._path.exists():
            return {}
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("HealingKB: erro ao carregar %s: %s — iniciando vazio", self._path, exc)
            return {}

    def _save(self) -> None:
        """Persiste KB no disco."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(
                json.dumps(self._data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.error("HealingKB: falha ao salvar %s: %s", self._path, exc)
