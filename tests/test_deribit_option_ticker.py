"""Tests for Deribit per-instrument option ticker source adapter."""

from __future__ import annotations

import pytest

from sources import deribit_option_ticker


def test_fetch_option_ticker_uses_instrument_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify per-instrument ticker requests pass the expected Deribit params."""

    def fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        assert url == deribit_option_ticker.DERIBIT_OPTION_TICKER_URL
        assert params == {"instrument_name": "BTC-30JUN26-120000-C"}
        return {"result": {"instrument_name": "BTC-30JUN26-120000-C", "bid_iv": 55.1}}

    monkeypatch.setattr(deribit_option_ticker, "get_json", fake_get_json)

    row = deribit_option_ticker.fetch_option_ticker("btc-30jun26-120000-c")

    assert row["bid_iv"] == 55.1


def test_fetch_option_ticker_rejects_invalid_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify malformed ticker payloads fail loudly."""

    monkeypatch.setattr(deribit_option_ticker, "get_json", lambda *_, **__: {"result": []})

    with pytest.raises(ValueError, match="ticker payload"):
        deribit_option_ticker.fetch_option_ticker("BTC-30JUN26-120000-C")
