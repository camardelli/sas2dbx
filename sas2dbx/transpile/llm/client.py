"""LLM Client — abstração de provider para o transpiler SAS2DBX.

Suporta dois providers:
  - AnthropicProvider: SDK anthropic direto (default / fallback)
  - KhonGatewayProvider: gateway HTTP Khon.ai (Bearer token)

O LLMClient tenta o provider configurado primeiro; em caso de erro 5xx
do gateway faz fallback automático para Anthropic direto.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config & Response
# ---------------------------------------------------------------------------


@dataclass
class LLMConfig:
    """Configuração do LLM client.

    Attributes:
        provider: "anthropic" | "khon"
        model: ID do modelo (ex: "claude-sonnet-4-6")
        api_key: Chave da API Anthropic
        khon_url: URL base do gateway Khon.ai
        khon_token: Bearer token do gateway Khon.ai
        max_tokens: Limite de tokens na resposta (default 4096)
        temperature: Temperatura (default 0.0 — determinístico para code gen)
        retry_attempts: Tentativas máximas em falha transitória (default 3)
        retry_base_delay: Delay base em segundos para exponential backoff (default 1.0)
    """

    provider: str = "anthropic"
    model: str = "claude-sonnet-4-6"
    api_key: str | None = None
    khon_url: str | None = None
    khon_token: str | None = None
    max_tokens: int = 4096
    temperature: float = 0.0
    retry_attempts: int = 3
    retry_base_delay: float = 1.0
    timeout: float = 120.0  # timeout total por chamada em segundos


@dataclass
class LLMResponse:
    """Resposta de uma chamada ao LLM.

    Attributes:
        content: Texto gerado pelo modelo.
        provider_used: Nome do provider que respondeu ("anthropic" | "khon").
        tokens_used: Total de tokens consumidos (input + output).
        latency_ms: Latência da chamada em milissegundos.
    """

    content: str
    provider_used: str
    tokens_used: int
    latency_ms: float


# ---------------------------------------------------------------------------
# Provider interface e erros
# ---------------------------------------------------------------------------


class LLMProvider(ABC):
    """Interface base para providers LLM."""

    @abstractmethod
    async def complete(
        self,
        prompt: str,
        max_tokens: int,
        temperature: float,
        timeout: float = 120.0,
    ) -> LLMResponse:
        """Envia prompt e retorna resposta.

        Raises:
            LLMRateLimitError: em rate limit (recuperável via retry).
            LLMGatewayError: em erro 5xx do gateway (aciona fallback).
            LLMProviderError: em erros não-recuperáveis (4xx, credenciais).
        """

    @abstractmethod
    def is_available(self) -> bool:
        """Retorna True se o provider está configurado e potencialmente disponível."""


class LLMProviderError(Exception):
    """Erro não-recuperável do provider (ex: 4xx, credenciais inválidas)."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class LLMRateLimitError(LLMProviderError):
    """Rate limit atingido — recuperável via retry com backoff."""


class LLMGatewayError(LLMProviderError):
    """Erro 5xx do gateway — aciona fallback para provider secundário."""


# ---------------------------------------------------------------------------
# LLMClient
# ---------------------------------------------------------------------------


class LLMClient:
    """Cliente LLM com seleção automática de provider, retry e fallback.

    Fluxo:
      1. Tenta o provider primário (configurado em `config.provider`).
      2. Em `LLMRateLimitError`: retry com exponential backoff (até retry_attempts).
      3. Em `LLMGatewayError` (5xx): fallback automático para AnthropicProvider
         (somente se `api_key` estiver configurada e o provider primário for "khon").

    Args:
        config: Configuração do client.
    """

    def __init__(self, config: LLMConfig) -> None:
        self._config = config
        self._primary: LLMProvider = self._build_primary()
        self._fallback: LLMProvider | None = self._build_fallback()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def complete_sync(self, prompt: str) -> LLMResponse:
        """Versão síncrona de `complete()` — conveniência para código não-async."""
        return asyncio.run(self.complete(prompt))

    async def complete(self, prompt: str) -> LLMResponse:
        """Envia prompt ao provider configurado com retry e fallback.

        Args:
            prompt: Texto completo do prompt.

        Returns:
            LLMResponse com content, provider_used, tokens_used e latency_ms.

        Raises:
            LLMProviderError: quando todos os providers e tentativas esgotam.
        """
        try:
            return await self._complete_with_retry(self._primary, prompt)
        except LLMGatewayError as exc:
            if self._fallback and self._fallback.is_available():
                logger.warning(
                    "LLMClient: gateway error (%s) — fallback para AnthropicProvider", exc
                )
                return await self._complete_with_retry(self._fallback, prompt)
            raise

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _complete_with_retry(
        self, provider: LLMProvider, prompt: str
    ) -> LLMResponse:
        """Executa chamada ao provider com exponential backoff em rate limit e gateway errors."""
        last_exc: Exception | None = None
        for attempt in range(1, self._config.retry_attempts + 1):
            try:
                return await provider.complete(
                    prompt=prompt,
                    max_tokens=self._config.max_tokens,
                    temperature=self._config.temperature,
                    timeout=self._config.timeout,
                )
            except LLMRateLimitError as exc:
                last_exc = exc
                delay = self._config.retry_base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "LLMClient: rate limit (attempt %d/%d) — aguardando %.1fs",
                    attempt,
                    self._config.retry_attempts,
                    delay,
                )
                await asyncio.sleep(delay)
            except LLMGatewayError as exc:
                last_exc = exc
                if attempt == self._config.retry_attempts:
                    raise  # esgotou tentativas → propaga para acionar fallback
                delay = self._config.retry_base_delay * (2 ** (attempt - 1))
                logger.warning(
                    "LLMClient: gateway/conexão error (attempt %d/%d) — aguardando %.1fs: %s",
                    attempt,
                    self._config.retry_attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
        raise LLMProviderError(
            f"Todas as {self._config.retry_attempts} tentativas falharam"
        ) from last_exc

    def _build_primary(self) -> LLMProvider:
        from sas2dbx.transpile.llm.providers.anthropic import AnthropicProvider
        from sas2dbx.transpile.llm.providers.khon import KhonGatewayProvider

        if self._config.provider == "khon":
            return KhonGatewayProvider(
                url=self._config.khon_url or "",
                token=self._config.khon_token or "",
                model=self._config.model,
            )
        if self._config.provider != "anthropic":
            logger.warning(
                "LLMClient: provider desconhecido '%s' — usando AnthropicProvider como padrão",
                self._config.provider,
            )
        return AnthropicProvider(
            api_key=self._config.api_key or "",
            model=self._config.model,
        )

    def _build_fallback(self) -> LLMProvider | None:
        """Fallback é AnthropicProvider quando o primário é Khon e há api_key."""
        if self._config.provider == "khon" and self._config.api_key:
            from sas2dbx.transpile.llm.providers.anthropic import AnthropicProvider

            return AnthropicProvider(
                api_key=self._config.api_key,
                model=self._config.model,
            )
        return None
