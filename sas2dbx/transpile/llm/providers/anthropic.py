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
            # timeout configurável: connect=30s + read=timeout (para respostas longas com max_tokens alto)
            # max_retries=0: desabilita retry interno do SDK — LLMClient._complete_with_retry()
            # é o único retry controller (evita cascata de up to 2×3 = 6 tentativas não declaradas)
            import httpx
            http_timeout = httpx.Timeout(connect=30.0, read=timeout, write=30.0, pool=10.0)
            async with anthropic.AsyncAnthropic(
                api_key=self._api_key,
                timeout=http_timeout,
                max_retries=0,
            ) as client:
                if max_tokens >= 4096:
                    # Streaming para requests longos: evita que proxies/Cloudflare cortem
                    # conexões idle enquanto o modelo ainda está gerando a resposta.
                    # stream.get_final_message() aguarda o fim do stream e retorna o objeto
                    # messages.Message completo — compatível com o path normal abaixo.
                    async with client.messages.stream(
                        model=self._model,
                        max_tokens=max_tokens,
                        temperature=temperature,
                        messages=[{"role": "user", "content": prompt}],
                    ) as stream:
                        message = await stream.get_final_message()
                else:
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
