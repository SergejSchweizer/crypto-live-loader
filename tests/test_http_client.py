"""Tests for HTTP client configuration behavior."""

from __future__ import annotations

from email.message import Message
from io import BytesIO
from urllib.error import HTTPError, URLError

import pytest

from ingestion import http_client
from ingestion.config import Config


class _FakeResponse:
    """Context manager that mimics a urllib HTTP response."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def test_default_http_request_config_loads_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify default HTTP settings are cached for a process."""

    calls = 0

    def fake_load_config() -> Config:
        nonlocal calls
        calls += 1
        return {
            "http": {
                "timeout_s": 8,
                "max_retries": 2,
                "retry_backoff_s": 1,
            }
        }

    http_client.default_http_request_config.cache_clear()
    monkeypatch.setattr(http_client, "load_config", fake_load_config)

    try:
        first = http_client.default_http_request_config()
        second = http_client.default_http_request_config()

        assert first == second
        assert first.timeout_s == 8
        assert first.max_retries == 2
        assert first.retry_backoff_s == 1
        assert calls == 1
    finally:
        http_client.default_http_request_config.cache_clear()


def test_get_json_encodes_params_and_decodes_response(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify JSON GET requests include query params and decode payloads."""

    seen: dict[str, object] = {}

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        seen["url"] = url
        seen["timeout"] = timeout
        return _FakeResponse(b'{"ok": true}')

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        http_client,
        "default_http_request_config",
        lambda: http_client.HttpRequestConfig(timeout_s=8, max_retries=0, retry_backoff_s=1),
    )

    result = http_client.get_json("https://example.test/api", params={"symbol": "BTC USD"})

    assert result == {"ok": True}
    assert seen == {"url": "https://example.test/api?symbol=BTC+USD", "timeout": 8}


def test_get_json_retries_transient_http_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify retryable HTTP errors use backoff before succeeding."""

    attempts = 0
    sleeps: list[tuple[int, float]] = []

    def fake_urlopen(url: str, timeout: float) -> _FakeResponse:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise HTTPError(url, 429, "rate limited", Message(), BytesIO())
        return _FakeResponse(b'{"ok": true}')

    monkeypatch.setattr(http_client, "urlopen", fake_urlopen)
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: sleeps.append((attempt, backoff_s)))

    result = http_client.get_json(
        "https://example.test/api",
        timeout_s=3,
        max_retries=1,
        retry_backoff_s=0.25,
    )

    assert result == {"ok": True}
    assert attempts == 2
    assert sleeps == [(0, 0.25)]


def test_get_json_raises_for_connection_error_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify repeated connection failures raise a contextual client error."""

    monkeypatch.setattr(http_client, "urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(URLError("down")))
    monkeypatch.setattr(http_client, "_retry_sleep", lambda attempt, backoff_s: None)

    with pytest.raises(http_client.HttpClientError, match="Connection error"):
        http_client.get_json("https://example.test/api", max_retries=1, retry_backoff_s=0)


def test_get_json_raises_for_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify invalid JSON payloads raise a contextual client error."""

    monkeypatch.setattr(http_client, "urlopen", lambda *_args, **_kwargs: _FakeResponse(b"not-json"))

    with pytest.raises(http_client.HttpClientError, match="Invalid JSON"):
        http_client.get_json("https://example.test/api", max_retries=0)
