"""InventoryGate — decide se a transpilação pode prosseguir.

Modos:
  STRICT  (default): bloqueia se qualquer SOURCE está ausente
  LENIENT:           permite se < 20% das SOURCEs estão ausentes
  FORCE:             sempre permite (operador aceita riscos)
  SKIP:              inventory não rodou (sem credenciais) — prossegue com aviso
"""

from __future__ import annotations

import logging
from typing import Literal

from sas2dbx.inventory import GateResult
from sas2dbx.inventory.checker import CheckResult

logger = logging.getLogger(__name__)

GateMode = Literal["STRICT", "LENIENT", "FORCE", "SKIP"]

# Threshold para modo LENIENT (20% de tabelas ausentes permitidas)
_LENIENT_THRESHOLD = 0.20


class InventoryGate:
    """Avalia o resultado do checker e emite GateResult."""

    def evaluate(
        self,
        check_result: CheckResult,
        mode: GateMode = "STRICT",
    ) -> GateResult:
        """Avalia se a transpilação pode prosseguir.

        Args:
            check_result: Resultado de DatabricksChecker.check_all().
            mode: Modo de avaliação (STRICT / LENIENT / FORCE / SKIP).

        Returns:
            GateResult com decision (PASS / WARN / BLOCK / SKIP) e mensagem.
        """
        # Inventory não rodou (sem credenciais)
        if check_result.mode == "skip" or mode == "SKIP":
            logger.warning("InventoryGate: modo SKIP — tabelas não verificadas, transpilação prossegue")
            return GateResult(
                decision="SKIP",
                message="Inventory não executado (sem credenciais Databricks) — transpilação prossegue sem verificação de schemas",
                missing_count=0,
            )

        missing = check_result.missing
        total = check_result.total

        if not missing:
            logger.info("InventoryGate: PASS — todas as %d tabela(s) SOURCE encontradas", total)
            return GateResult(
                decision="PASS",
                message=f"Todas as {total} tabela(s) SOURCE existem no Databricks",
                missing_count=0,
            )

        missing_count = len(missing)

        if mode == "FORCE":
            logger.warning(
                "InventoryGate: WARN (FORCE) — %d tabela(s) ausente(s), transpilação forçada",
                missing_count,
            )
            missing_names = ", ".join(e.table_fqn or e.table_short for e in missing)
            return GateResult(
                decision="WARN",
                message=f"{missing_count} tabela(s) ausente(s) — forçando transpilação (mode=FORCE): {missing_names}",
                missing_count=missing_count,
            )

        if mode == "LENIENT":
            pct = missing_count / max(total, 1)
            if pct < _LENIENT_THRESHOLD:
                logger.warning(
                    "InventoryGate: WARN (LENIENT) — %d/%d tabela(s) ausente(s) (%.0f%% < threshold %.0f%%)",
                    missing_count, total, pct * 100, _LENIENT_THRESHOLD * 100,
                )
                missing_names = ", ".join(e.table_fqn or e.table_short for e in missing)
                return GateResult(
                    decision="WARN",
                    message=(
                        f"{missing_count}/{total} tabela(s) ausente(s) ({pct:.0%}) — "
                        f"abaixo do threshold {_LENIENT_THRESHOLD:.0%}, prosseguindo: {missing_names}"
                    ),
                    missing_count=missing_count,
                )
            else:
                logger.error(
                    "InventoryGate: BLOCK (LENIENT) — %d/%d tabela(s) ausente(s) (%.0f%% >= threshold %.0f%%)",
                    missing_count, total, pct * 100, _LENIENT_THRESHOLD * 100,
                )
                missing_names = "\n  - ".join(e.table_fqn or e.table_short for e in missing)
                return GateResult(
                    decision="BLOCK",
                    message=(
                        f"{missing_count}/{total} tabela(s) ausente(s) ({pct:.0%}) — "
                        f"acima do threshold {_LENIENT_THRESHOLD:.0%}. Tabelas:\n  - {missing_names}"
                    ),
                    missing_count=missing_count,
                )

        # STRICT (default)
        logger.error(
            "InventoryGate: BLOCK (STRICT) — %d tabela(s) SOURCE ausente(s)",
            missing_count,
        )
        missing_names = "\n  - ".join(
            f"{e.table_fqn or (e.libname + '.' + e.table_short)} (refs: {', '.join(e.referenced_in)})"
            for e in missing
        )
        return GateResult(
            decision="BLOCK",
            message=(
                f"{missing_count} tabela(s) SOURCE ausente(s) no Databricks.\n"
                f"Crie as tabelas ou use --inventory-mode=FORCE para prosseguir:\n"
                f"  - {missing_names}"
            ),
            missing_count=missing_count,
        )
