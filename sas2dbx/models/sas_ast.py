"""Modelos de dados do AST SAS e classificação de constructs."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path


class Tier(StrEnum):
    """Tier de transpilação de um construct SAS."""

    RULE = "rule"    # Tier 1 — determinístico, sem LLM
    LLM = "llm"      # Tier 2 — assistido por LLM
    MANUAL = "manual"  # Tier 3 — flag manual, sem tentativa de transpilação automática


@dataclass
class ValidationReport:
    """Relatório de validação do Knowledge Store.

    Attributes:
        is_valid: True se não há erros críticos (warnings não bloqueiam).
        total_entries: Contagem de entradas por arquivo de mapping.
        warnings: Avisos não-bloqueantes (ex: PROC sem .md correspondente).
        coverage: Fração de entradas com confidence >= 0.7.
        missing_references: PROCs/funções sem doc .md correspondente.
    """

    is_valid: bool
    total_entries: dict[str, int]
    warnings: list[str]
    coverage: float
    missing_references: list[str]


@dataclass
class SASFile:
    """Arquivo .sas descoberto pelo scanner.

    Attributes:
        path: Caminho absoluto do arquivo.
        size_bytes: Tamanho em bytes.
        encoding: Encoding detectado na leitura.
    """

    path: Path
    size_bytes: int
    encoding: str = "utf-8"


@dataclass
class SASBlock:
    """Bloco lógico de código SAS (DATA step, PROC, LIBNAME, %MACRO, etc.).

    Attributes:
        raw_code: Código SAS original do bloco.
        start_line: Linha inicial no arquivo fonte (1-indexed).
        end_line: Linha final no arquivo fonte (1-indexed).
        source_file: Arquivo de origem (None se criado em memória).
        classification: Resultado da classificação (None se ainda não classificado).
    """

    raw_code: str
    start_line: int
    end_line: int
    source_file: Path | None = None
    classification: ClassificationResult | None = field(default=None, compare=False)


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
