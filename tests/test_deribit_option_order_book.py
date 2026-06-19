"""Tests for Deribit option order-book source adapter."""

from __future__ import annotations

import pytest

from sources import deribit_option_order_book
from sources.deribit_option_order_book import fetch_option_order_book


def test_fetch_option_order_book_calls_deribit_get_order_book(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify option order-book requests use normalized instruments and requested depth."""

    calls: list[tuple[str, dict[str, object] | None]] = []

    def fake_get_json(url: str, params: dict[str, object] | None = None) -> dict[str, object]:
        calls.append((url, params))
        return {"result": {"instrument_name": "BTC-30JUN26-120000-C", "timestamp": 1_779_606_712_000}}

    monkeypatch.setattr(deribit_option_order_book, "get_json", fake_get_json)

    row = fetch_option_order_book("btc-30jun26-120000-c", depth=20)

    assert row["instrument_name"] == "BTC-30JUN26-120000-C"
    assert calls == [
        (
            "https://www.deribit.com/api/v2/public/get_order_book",
            {"instrument_name": "BTC-30JUN26-120000-C", "depth": 20},
        )
    ]


def test_fetch_option_order_book_rejects_invalid_request() -> None:
    """Verify malformed instruments and invalid depth fail before network calls."""

    with pytest.raises(ValueError, match="Expected Deribit option instrument"):
        fetch_option_order_book("BTC-PERPETUAL", depth=20)

    with pytest.raises(ValueError, match="depth must be positive"):
        fetch_option_order_book("BTC-30JUN26-120000-C", depth=0)
