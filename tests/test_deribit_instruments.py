"""Tests for Deribit instrument metadata source adapter."""

from __future__ import annotations

import pytest

from sources import deribit_instruments


def test_fetch_instruments_uses_currency_kind_and_expired(monkeypatch: pytest.MonkeyPatch) -> None:
    """fetch_instruments should pass expected Deribit query params."""

    def fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        assert url == deribit_instruments.DERIBIT_INSTRUMENTS_URL
        assert params == {"currency": "BTC", "kind": "option", "expired": "false"}
        return {"result": [{"instrument_name": "BTC-30JUN26-120000-C"}]}

    monkeypatch.setattr(deribit_instruments, "get_json", fake_get_json)
    rows = deribit_instruments.fetch_instruments(currency="btc", kind="option", expired=False)
    assert rows[0]["instrument_name"] == "BTC-30JUN26-120000-C"
