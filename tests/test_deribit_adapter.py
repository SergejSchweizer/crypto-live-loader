"""Conformance tests for source adapter behavior."""

from __future__ import annotations

import pytest

from domain.models import RawSnapshot
from sources.deribit.adapter import DeribitAdapter


def test_deribit_adapter_normalizes_symbol() -> None:
    """Deribit adapter should normalize common perpetual aliases."""

    adapter = DeribitAdapter()
    assert adapter.normalize_symbol("btcusdt") == "BTC-PERPETUAL"


def test_deribit_adapter_fetch_snapshot_returns_canonical_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deribit adapter should map exchange payload into RawSnapshot."""

    def fake_fetch_order_book_snapshot(symbol: str, depth: int) -> dict[str, object]:
        assert symbol == "BTC"
        assert depth == 10
        return {
            "exchange": "deribit",
            "symbol": "BTC-PERPETUAL",
            "timestamp_ms": 1_700_000_000_000,
            "bids": [(100.0, 1.0)],
            "asks": [(100.1, 2.0)],
            "mark_price": 100.05,
            "index_price": 100.0,
            "open_interest": 1000.0,
            "funding_8h": 0.0001,
            "current_funding": 0.00001,
        }

    from sources.deribit import adapter as deribit_adapter

    monkeypatch.setattr(deribit_adapter, "fetch_order_book_snapshot", fake_fetch_order_book_snapshot)
    result = DeribitAdapter().fetch_snapshot(symbol="BTC", depth=10)

    assert isinstance(result, RawSnapshot)
    assert result.exchange == "deribit"
    assert result.symbol == "BTC-PERPETUAL"
    assert result.bids[0].price == 100.0
    assert result.asks[0].amount == 2.0
