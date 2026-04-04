"""Testes para transpile/llm/client.py — LLMClient, providers e retry."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sas2dbx.transpile.llm.client import (
    LLMClient,
    LLMConfig,
    LLMGatewayError,
    LLMProvider,
    LLMProviderError,
    LLMRateLimitError,
    LLMResponse,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(provider: str = "anthropic") -> LLMResponse:
    return LLMResponse(
        content="# PySpark code",
        provider_used=provider,
        tokens_used=100,
        latency_ms=50.0,
    )


class _FakeProvider(LLMProvider):
    """Provider fake para testes — configurável via side_effect."""

    def __init__(self, responses: list) -> None:
        self._responses = list(responses)
        self._calls = 0

    def is_available(self) -> bool:
        return True

    async def complete(self, prompt: str, max_tokens: int, temperature: float, timeout: float = 120.0, system: str | None = None) -> LLMResponse:
        self._calls += 1
        result = self._responses[min(self._calls - 1, len(self._responses) - 1)]
        if isinstance(result, Exception):
            raise result
        return result


def _run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# LLMConfig defaults
# ---------------------------------------------------------------------------

class TestLLMConfigDefaults:
    def test_default_provider(self) -> None:
        cfg = LLMConfig()
        assert cfg.provider == "anthropic"

    def test_default_temperature_zero(self) -> None:
        cfg = LLMConfig()
        assert cfg.temperature == 0.0

    def test_default_retry_attempts(self) -> None:
        cfg = LLMConfig()
        assert cfg.retry_attempts == 3

    def test_default_max_tokens(self) -> None:
        cfg = LLMConfig()
        assert cfg.max_tokens == 4096


# ---------------------------------------------------------------------------
# AnthropicProvider — via mock do SDK
# ---------------------------------------------------------------------------

class TestAnthropicProvider:
    def test_anthropic_provider_complete(self) -> None:
        from sas2dbx.transpile.llm.providers.anthropic import AnthropicProvider

        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="# result")]
        mock_message.usage.input_tokens = 50
        mock_message.usage.output_tokens = 30

        with patch("anthropic.AsyncAnthropic") as MockClient:
            MockClient.return_value.__aenter__ = AsyncMock(return_value=MockClient.return_value)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value.messages.create = AsyncMock(return_value=mock_message)
            provider = AnthropicProvider(api_key="test-key", model="claude-sonnet-4-6")
            resp = _run(provider.complete("prompt", max_tokens=100, temperature=0.0))

        assert resp.content == "# result"
        assert resp.provider_used == "anthropic"
        assert resp.tokens_used == 80

    def test_anthropic_provider_rate_limit_raises(self) -> None:
        import anthropic as sdk

        from sas2dbx.transpile.llm.providers.anthropic import AnthropicProvider

        with patch("anthropic.AsyncAnthropic") as MockClient:
            MockClient.return_value.__aenter__ = AsyncMock(return_value=MockClient.return_value)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value.messages.create = AsyncMock(
                side_effect=sdk.RateLimitError(
                    message="rate limit",
                    response=MagicMock(status_code=429),
                    body={},
                )
            )
            provider = AnthropicProvider(api_key="key", model="m")
            with pytest.raises(LLMRateLimitError):
                _run(provider.complete("p", 100, 0.0))

    def test_anthropic_provider_5xx_raises_gateway_error(self) -> None:
        import anthropic as sdk

        from sas2dbx.transpile.llm.providers.anthropic import AnthropicProvider

        with patch("anthropic.AsyncAnthropic") as MockClient:
            MockClient.return_value.__aenter__ = AsyncMock(return_value=MockClient.return_value)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)
            err = sdk.APIStatusError(
                message="server error",
                response=MagicMock(status_code=500),
                body={},
            )
            err.status_code = 500
            MockClient.return_value.messages.create = AsyncMock(side_effect=err)
            provider = AnthropicProvider(api_key="key", model="m")
            with pytest.raises(LLMGatewayError):
                _run(provider.complete("p", 100, 0.0))

    def test_anthropic_provider_not_available_without_key(self) -> None:
        from sas2dbx.transpile.llm.providers.anthropic import AnthropicProvider

        provider = AnthropicProvider(api_key="", model="m")
        assert not provider.is_available()

    def test_anthropic_provider_available_with_key(self) -> None:
        from sas2dbx.transpile.llm.providers.anthropic import AnthropicProvider

        provider = AnthropicProvider(api_key="sk-test", model="m")
        assert provider.is_available()


# ---------------------------------------------------------------------------
# KhonGatewayProvider — via mock urllib
# ---------------------------------------------------------------------------

class TestKhonGatewayProvider:
    def test_khon_provider_complete(self) -> None:
        import json
        from unittest.mock import MagicMock, patch

        from sas2dbx.transpile.llm.providers.khon import KhonGatewayProvider

        body = json.dumps({
            "content": [{"text": "# pyspark"}],
            "usage": {"input_tokens": 40, "output_tokens": 20},
        }).encode()

        mock_resp = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.read.return_value = body

        with patch("urllib.request.urlopen", return_value=mock_resp):
            provider = KhonGatewayProvider(
                url="https://gateway.khon.ai", token="tok", model="m"
            )
            resp = _run(provider.complete("prompt", 100, 0.0))

        assert resp.content == "# pyspark"
        assert resp.provider_used == "khon"
        assert resp.tokens_used == 60

    def test_khon_provider_429_raises_rate_limit(self) -> None:
        import urllib.error

        from sas2dbx.transpile.llm.providers.khon import KhonGatewayProvider

        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(None, 429, "Too Many", {}, None),
        ):
            provider = KhonGatewayProvider(url="http://x", token="t", model="m")
            with pytest.raises(LLMRateLimitError):
                _run(provider.complete("p", 100, 0.0))

    def test_khon_provider_503_raises_gateway_error(self) -> None:
        import urllib.error

        from sas2dbx.transpile.llm.providers.khon import KhonGatewayProvider

        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(None, 503, "Unavailable", {}, None),
        ):
            provider = KhonGatewayProvider(url="http://x", token="t", model="m")
            with pytest.raises(LLMGatewayError):
                _run(provider.complete("p", 100, 0.0))

    def test_khon_not_available_without_config(self) -> None:
        from sas2dbx.transpile.llm.providers.khon import KhonGatewayProvider

        provider = KhonGatewayProvider(url="", token="", model="m")
        assert not provider.is_available()


# ---------------------------------------------------------------------------
# LLMClient — seleção de provider e fallback
# ---------------------------------------------------------------------------

class TestLLMClientProviderSelection:
    def test_anthropic_provider_used_when_configured(self) -> None:
        cfg = LLMConfig(provider="anthropic", api_key="key")
        client = LLMClient(cfg)
        assert client._primary.__class__.__name__ == "AnthropicProvider"

    def test_khon_provider_used_when_configured(self) -> None:
        cfg = LLMConfig(provider="khon", khon_url="http://x", khon_token="t")
        client = LLMClient(cfg)
        assert client._primary.__class__.__name__ == "KhonGatewayProvider"

    def test_fallback_set_when_khon_with_api_key(self) -> None:
        cfg = LLMConfig(
            provider="khon", khon_url="http://x", khon_token="t", api_key="key"
        )
        client = LLMClient(cfg)
        assert client._fallback is not None
        assert client._fallback.__class__.__name__ == "AnthropicProvider"

    def test_no_fallback_when_anthropic_primary(self) -> None:
        cfg = LLMConfig(provider="anthropic", api_key="key")
        client = LLMClient(cfg)
        assert client._fallback is None

    def test_no_fallback_when_khon_without_api_key(self) -> None:
        cfg = LLMConfig(provider="khon", khon_url="http://x", khon_token="t")
        client = LLMClient(cfg)
        assert client._fallback is None


# ---------------------------------------------------------------------------
# LLMClient — provider_used indica quem respondeu
# ---------------------------------------------------------------------------

class TestLLMClientResponse:
    def _client_with_providers(
        self, primary: LLMProvider, fallback: LLMProvider | None = None
    ) -> LLMClient:
        cfg = LLMConfig(provider="anthropic", api_key="key")
        client = LLMClient(cfg)
        client._primary = primary
        client._fallback = fallback
        return client

    def test_response_provider_used_reflects_anthropic(self) -> None:
        provider = _FakeProvider([_make_response("anthropic")])
        client = self._client_with_providers(provider)
        resp = _run(client.complete("prompt"))
        assert resp.provider_used == "anthropic"

    def test_response_provider_used_reflects_khon(self) -> None:
        provider = _FakeProvider([_make_response("khon")])
        client = self._client_with_providers(provider)
        resp = _run(client.complete("prompt"))
        assert resp.provider_used == "khon"


# ---------------------------------------------------------------------------
# LLMClient — fallback em LLMGatewayError
# ---------------------------------------------------------------------------

class TestLLMClientFallback:
    def _client_with_providers(
        self, primary: LLMProvider, fallback: LLMProvider | None = None
    ) -> LLMClient:
        cfg = LLMConfig(provider="khon", khon_url="http://x", khon_token="t", api_key="key")
        client = LLMClient(cfg)
        client._primary = primary
        client._fallback = fallback
        return client

    def test_fallback_triggered_on_gateway_error(self) -> None:
        primary = _FakeProvider([LLMGatewayError("503", status_code=503)])
        fallback = _FakeProvider([_make_response("anthropic")])
        client = self._client_with_providers(primary, fallback)

        resp = _run(client.complete("prompt"))
        assert resp.provider_used == "anthropic"

    def test_no_fallback_raises_gateway_error(self) -> None:
        primary = _FakeProvider([LLMGatewayError("503")])
        client = self._client_with_providers(primary, fallback=None)

        with pytest.raises(LLMGatewayError):
            _run(client.complete("prompt"))

    def test_fallback_unavailable_raises_gateway_error(self) -> None:
        primary = _FakeProvider([LLMGatewayError("503")])
        fallback = MagicMock(spec=LLMProvider)
        fallback.is_available.return_value = False
        client = self._client_with_providers(primary, fallback)

        with pytest.raises(LLMGatewayError):
            _run(client.complete("prompt"))


# ---------------------------------------------------------------------------
# LLMClient — retry com backoff
# ---------------------------------------------------------------------------

class TestLLMClientRetry:
    def _client_with_primary(self, primary: LLMProvider) -> LLMClient:
        cfg = LLMConfig(provider="anthropic", api_key="key", retry_attempts=3,
                        retry_base_delay=0.0)
        client = LLMClient(cfg)
        client._primary = primary
        return client

    def test_retry_on_rate_limit_then_success(self) -> None:
        provider = _FakeProvider([
            LLMRateLimitError("429"),
            LLMRateLimitError("429"),
            _make_response("anthropic"),
        ])
        client = self._client_with_primary(provider)
        resp = _run(client.complete("prompt"))
        assert resp.provider_used == "anthropic"
        assert provider._calls == 3

    def test_retry_exhausted_raises(self) -> None:
        provider = _FakeProvider([LLMRateLimitError("429")] * 4)
        client = self._client_with_primary(provider)
        with pytest.raises(LLMProviderError):
            _run(client.complete("prompt"))
        assert provider._calls == 3  # retry_attempts=3

    def test_no_retry_on_provider_error(self) -> None:
        provider = _FakeProvider([LLMProviderError("auth fail", status_code=401)])
        client = self._client_with_primary(provider)
        with pytest.raises(LLMProviderError):
            _run(client.complete("prompt"))
        assert provider._calls == 1  # sem retry


# ---------------------------------------------------------------------------
# LLMClient — complete_sync
# ---------------------------------------------------------------------------

class TestLLMClientSync:
    def test_complete_sync_returns_response(self) -> None:
        cfg = LLMConfig(provider="anthropic", api_key="key")
        client = LLMClient(cfg)
        client._primary = _FakeProvider([_make_response("anthropic")])
        resp = client.complete_sync("prompt")
        assert isinstance(resp, LLMResponse)
        assert resp.content == "# PySpark code"
