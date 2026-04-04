"""Diagnóstico de erros de execução do pipeline Databricks."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from sas2dbx.validate.heal.patterns import match_pattern

logger = logging.getLogger(__name__)


@dataclass
class ErrorDiagnostic:
    """Resultado do diagnóstico de um erro de execução.

    Attributes:
        error_raw: Mensagem de erro original do Databricks.
        category: Categoria semântica (ex: "missing_table"). None se desconhecida.
        entities: Entidades extraídas da mensagem (nomes de tabelas, colunas, etc).
        deterministic_fix: Chave da ação de correção determinística, ou None.
        severity: "CRITICAL" | "HIGH" | "MEDIUM" | "UNKNOWN".
        llm_analysis: Análise textual do LLM (None se LLM não disponível).
        pattern_key: Chave do padrão que fez match (None se nenhum).
    """

    error_raw: str
    category: str | None = None
    entities: dict[str, str] = field(default_factory=dict)
    deterministic_fix: str | None = None
    severity: str = "UNKNOWN"
    llm_analysis: str | None = None
    pattern_key: str | None = None


class DiagnosticsEngine:
    """Analisa erros de execução e produz diagnósticos estruturados.

    Combina matching determinístico (patterns.py) com análise LLM opcional
    para erros não catalogados ou que precisam de contexto adicional.

    Args:
        llm_client: LLMClient opcional. Se None, apenas matching determinístico.
    """

    def __init__(self, llm_client: object | None = None) -> None:
        self._llm = llm_client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def diagnose(
        self,
        error_raw: str,
        job_name: str = "",
    ) -> ErrorDiagnostic:
        """Diagnostica uma mensagem de erro do Databricks.

        Fluxo:
          1. Normaliza a mensagem via _interpolate()
          2. Chama match_pattern() do catálogo
          3. Extrai entidades (_extract_entities)
          4. Se nenhum padrão e LLM disponível: chama _llm_diagnose()
          5. Retorna ErrorDiagnostic preenchido

        Args:
            error_raw: Stacktrace ou mensagem de erro do Databricks.
            job_name: Nome do job (para contexto na mensagem genérica).

        Returns:
            ErrorDiagnostic preenchido.
        """
        normalized = self._normalize(error_raw)

        pattern_key, entry = match_pattern(normalized)

        if entry is not None:
            entities = self._extract_entities(normalized, pattern_key)
            return ErrorDiagnostic(
                error_raw=error_raw,
                category=entry["category"],
                entities=entities,
                deterministic_fix=entry.get("deterministic_fix"),
                severity=entry.get("severity", "UNKNOWN"),
                pattern_key=pattern_key,
            )

        # Sem match determinístico — tentar LLM
        llm_analysis = None
        if self._llm is not None:
            llm_analysis = self._llm_diagnose(normalized)

        return ErrorDiagnostic(
            error_raw=error_raw,
            category=None,
            entities={},
            deterministic_fix=None,
            severity="UNKNOWN",
            llm_analysis=llm_analysis,
            pattern_key=None,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _normalize(self, raw_error: str) -> str:
        """Normaliza a mensagem de erro para matching.

        Transformações:
          - Strip de whitespace excessivo
          - Remove stack trace Java (linhas que começam com '\tat ')
          - Remove prefixos de timestamp (ex: "2024-01-01 12:00:00 ERROR ")

        Args:
            raw_error: Mensagem bruta do Databricks.

        Returns:
            Mensagem normalizada para matching.
        """
        lines = raw_error.splitlines()
        # Remove linhas de stack trace Java
        filtered = [ln for ln in lines if not ln.strip().startswith("\tat ")]
        joined = " ".join(filtered)
        # Remove timestamps
        joined = re.sub(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[^\s]*\s*", "", joined)
        # Normaliza espaços
        joined = re.sub(r"\s+", " ", joined).strip()
        return joined

    def _extract_entities(self, error_message: str, pattern_key: str | None) -> dict[str, str]:
        """Extrai entidades nomeadas da mensagem de erro.

        Args:
            error_message: Mensagem de erro normalizada.
            pattern_key: Chave do padrão que casou (pode ser None).

        Returns:
            Dict com entidades extraídas. Vazio se nenhuma encontrada.
        """
        entities: dict[str, str] = {}

        # Tabela: padrão Databricks "[TABLE_OR_VIEW_NOT_FOUND] The table or view `cat`.`sch`.`tbl`"
        dbx_table_match = re.search(
            r"table or view\s+`([^`]+)`\.`([^`]+)`\.`([^`]+)`",
            error_message,
            re.IGNORECASE,
        )
        if dbx_table_match:
            entities["table_name"] = f"{dbx_table_match.group(1)}.{dbx_table_match.group(2)}.{dbx_table_match.group(3)}"
        else:
            # Padrão genérico "Table or view not found: <name>"
            table_match = re.search(
                r"Table or view not found[:\s]+[`'\"]?([\w.]+)[`'\"]?",
                error_message,
                re.IGNORECASE,
            )
            if table_match:
                entities["table_name"] = table_match.group(1)

        # Coluna: padrão "cannot resolve `<col>` given input columns"
        col_match = re.search(
            r"cannot resolve\s+[`'\"]?([\w.]+)[`'\"]?\s+given",
            error_message,
            re.IGNORECASE,
        )
        if col_match:
            entities["column_name"] = col_match.group(1)

        # Coluna duplicada: "Output column <col> already exists"
        # Sobrescreve column_name intencionalmente — este extrator é mais específico
        # que o "cannot resolve" acima para erros de IllegalArgumentException
        dup_col_match = re.search(
            r"Output column\s+([\w]+)\s+already exists",
            error_message,
            re.IGNORECASE,
        )
        if dup_col_match:
            entities["column_name"] = dup_col_match.group(1)

        # Função: padrão "Undefined function: <name>"
        func_match = re.search(
            r"Undefined function[:\s]+[`'\"]?([\w]+)[`'\"]?",
            error_message,
            re.IGNORECASE,
        )
        if func_match:
            entities["function_name"] = func_match.group(1)

        # Módulo: padrão "No module named '<name>'"
        mod_match = re.search(
            r"No module named '([\w.]+)'",
            error_message,
        )
        if mod_match:
            entities["module_name"] = mod_match.group(1)

        # Catálogo errado: padrão "Catalog 'name' was not found"
        cat_match = re.search(
            r"Catalog '([\w]+)' was not found",
            error_message,
            re.IGNORECASE,
        )
        if cat_match:
            entities["wrong_catalog"] = cat_match.group(1)

        # Coluna não resolvida com sugestão.
        # Databricks emite o formato: with name `alias`.`coluna` cannot be resolved
        # O regex anterior capturava `alias` (alias) em vez de `coluna`.
        # Fix: (?:`alias`\.)? opcional antes do nome da coluna — captura sempre o último segmento.
        unresolved_col_match = re.search(
            r"with name\s+(?:`[\w]+`\.)?`([\w]+)`\s+cannot be resolved",
            error_message,
            re.IGNORECASE,
        )
        if unresolved_col_match:
            entities["unresolved_column"] = unresolved_col_match.group(1)

        # Sugestões no formato: [`alias`.`coluna`, `alias2`.`coluna2`, ...]
        # O regex anterior falhava porque o [^`\]]+ parava no primeiro backtick (antes de coluna).
        suggestion_match = re.search(
            r"Did you mean one of the following\?\s*\[(?:`[\w]+`\.)?`([\w]+)`",
            error_message,
            re.IGNORECASE,
        )
        if suggestion_match:
            entities["suggested_column"] = suggestion_match.group(1)

        # Infere catalog/schema do nome da tabela
        if "table_name" in entities:
            parts = entities["table_name"].split(".")
            if len(parts) >= 2:
                entities["catalog"] = parts[0]
                entities["schema"] = parts[1] if len(parts) >= 3 else "unknown"

        return entities

    def _llm_diagnose(self, error_message: str) -> str | None:
        """Envia a mensagem de erro ao LLM para análise livre.

        Não propaga LLMProviderError — apenas loga warning e retorna None.

        Args:
            error_message: Mensagem normalizada.

        Returns:
            Texto da análise LLM ou None em caso de falha.
        """
        try:
            from sas2dbx.transpile.llm.client import LLMProviderError

            prompt = (
                "You are a Databricks expert. Analyze this execution error and explain "
                "the root cause in 2-3 sentences, then suggest the most likely fix. "
                f"Error: {error_message[:2000]}"
            )
            response = self._llm.complete_sync(prompt)
            return response.content
        except Exception as exc:  # noqa: BLE001
            logger.warning("DiagnosticsEngine: LLM diagnose falhou: %s", exc)
            return None
