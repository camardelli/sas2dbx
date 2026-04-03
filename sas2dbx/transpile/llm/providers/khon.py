"""Provider Khon.ai Gateway — wrapper HTTP com Bearer token auth."""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request

from sas2dbx.transpile.llm.client import (
    LLMGatewayError,
    LLMProvider,
    LLMProviderError,
    LLMRateLimitError,
    LLMResponse,
)

logger = logging.getLogger(__name__)


class KhonGatewayProvider(LLMProvider):
    """Provider que chama o gateway HTTP Khon.ai.

    O gateway expõe uma API compatível com a Anthropic Messages API.
    Autenticação via Bearer token no header Authorization.

    Args:
        url: URL base do gateway (ex: "https://gateway.khon.ai").
        token: Bearer token de autenticação.
        model: ID do modelo a usar.
    """

    def __init__(self, url: str, token: str, model: str) -> None:
        self._url = url.rstrip("/")
        self._token = token
        self._model = model

    def is_available(self) -> bool:
        return bool(self._url and self._token)

    async def complete(
        self,
        prompt: str,
        max_tokens: int,
        temperature: float,
        timeout: float = 120.0,
    ) -> LLMResponse:
        """Chama o gateway Khon e retorna LLMResponse.

        Raises:
            LLMRateLimitError: em HTTP 429.
            LLMGatewayError: em HTTP 5xx ou erro de conexão.
            LLMProviderError: em outros erros HTTP.
        """
        payload = json.dumps({
            "model": self._model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}],
        }).encode("utf-8")

        req = urllib.request.Request(
            url=f"{self._url}/v1/messages",
            data=payload,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            method="POST",
        )

        t0 = time.monotonic()
        try:
            # NOTE: urllib.request.urlopen é síncrono/bloqueante dentro de async def.
            # Para o MVP sequencial isso é aceitável; em uso concorrente bloqueia o
            # event loop. Substituir por aiohttp/httpx quando necessário.
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            status = exc.code
            if status == 429:
                raise LLMRateLimitError(f"Rate limit (HTTP 429): {exc}", status_code=429) from exc
            if status >= 500:
                raise LLMGatewayError(
                    f"Gateway error (HTTP {status}): {exc}", status_code=status
                ) from exc
            raise LLMProviderError(f"HTTP {status}: {exc}", status_code=status) from exc
        except urllib.error.URLError as exc:
            raise LLMGatewayError(f"Erro de conexão com gateway: {exc}") from exc

        latency_ms = (time.monotonic() - t0) * 1000

        content_blocks = body.get("content", [])
        content = content_blocks[0].get("text", "") if content_blocks else ""
        usage = body.get("usage", {})
        tokens_used = usage.get("input_tokens", 0) + usage.get("output_tokens", 0)

        logger.debug(
            "KhonGatewayProvider: model=%s tokens=%d latency=%.0fms",
            self._model,
            tokens_used,
            latency_ms,
        )

        return LLMResponse(
            content=content,
            provider_used="khon",
            tokens_used=tokens_used,
            latency_ms=latency_ms,
        )
