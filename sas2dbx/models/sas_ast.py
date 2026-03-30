"""Modelos de dados do AST SAS e classificação de constructs."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Tier(str, Enum):
    """Tier de transpilação de um construct SAS."""

    RULE = "rule"    # Tier 1 — determinístico, sem LLM
    LLM = "llm"      # Tier 2 — assistido por LLM
    MANUAL = "manual"  # Tier 3 — flag manual, sem tentativa de transpilação automática


@dataclass(frozen=True)
class ClassificationResult:
    """Resultado da classificação de um bloco SAS.

    Attributes:
        construct_type: Identificador do tipo de construct (ex: "PROC_SQL", "UNKNOWN").
        tier: Tier de transpilação associado.
        confidence: Confiança na classificação (1.0=Tier1, 0.8=Tier2, 0.0=Tier3).
    """

    construct_type: str
    tier: Tier
    confidence: float
