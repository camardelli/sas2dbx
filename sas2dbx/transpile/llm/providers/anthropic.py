"""Provider Anthropic — wrapper do SDK anthropic oficial."""

from __future__ import annotations

import logging
import time

from sas2dbx.transpile.llm.client import (
    LLMGatewayError,
    LLMProvider,
    LLMProviderError,
    LLMRateLimitError,
    LLMResponse,
)

logger = logging.getLogger(__name__)


class AnthropicProvider(LLMProvider):
    """Provider que usa o SDK `anthropic` diretamente.

    Args:
        api_key: Chave da API Anthropic.
        model: ID do modelo (ex: "claude-sonnet-4-6").
    """

    def __init__(self, api_key: str, model: str) -> None:
        self._api_key = api_key
        self._model = model

    def is_available(self) -> bool:
        return bool(self._api_key)

    async def complete(
        self,
        prompt: str,
        max_tokens: int,
        temperature: float,
        timeout: float = 120.0,
    ) -> LLMResponse:
        """Chama a API Anthropic e retorna LLMResponse.

        Raises:
            LLMRateLimitError: em HTTP 429.
            LLMGatewayError: em HTTP 5xx.
            LLMProviderError: em outros erros da API.
        """
        try:
            import anthropic
        except ImportError as exc:
            raise LLMProviderError(
                "Pacote 'anthropic' não instalado. Execute: pip install anthropic"
            ) from exc

        t0 = time.monotonic()
        try:
            # async with garante que o httpx client fecha antes do event loop encerrar
            # timeout configurável: connect=10s + read=timeout (para respostas longas com max_tokens alto)
            import httpx
            http_timeout = httpx.Timeout(connect=10.0, read=timeout, write=30.0, pool=10.0)
            async with anthropic.AsyncAnthropic(api_key=self._api_key, timeout=http_timeout) as client:
                message = await client.messages.create(
                    model=self._model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    messages=[{"role": "user", "content": prompt}],
                )
        except anthropic.RateLimitError as exc:
            raise LLMRateLimitError(str(exc), status_code=429) from exc
        except anthropic.APIStatusError as exc:
            if exc.status_code >= 500:
                raise LLMGatewayError(str(exc), status_code=exc.status_code) from exc
            raise LLMProviderError(str(exc), status_code=exc.status_code) from exc
        except anthropic.APIConnectionError as exc:
            raise LLMGatewayError(f"Erro de conexão: {exc}") from exc

        latency_ms = (time.monotonic() - t0) * 1000
        content = message.content[0].text if message.content else ""
        tokens_used = message.usage.input_tokens + message.usage.output_tokens

        logger.debug(
            "AnthropicProvider: model=%s tokens=%d latency=%.0fms",
            self._model,
            tokens_used,
            latency_ms,
        )

        return LLMResponse(
            content=content,
            provider_used="anthropic",
            tokens_used=tokens_used,
            latency_ms=latency_ms,
        )
