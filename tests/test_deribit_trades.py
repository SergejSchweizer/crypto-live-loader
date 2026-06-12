"""Tests for Deribit recent trade source mapping and filtering."""

from __future__ import annotations

import pytest

from sources import deribit_trades


def test_trade_currency_mapping_for_btc_eth_sol() -> None:
    """BTC/ETH map directly while SOL maps to Deribit USDC trade tape."""

    btc = deribit_trades.resolve_trades_currency_request("BTC", "option")
    eth = deribit_trades.resolve_trades_currency_request("eth", "future")
    sol = deribit_trades.resolve_trades_currency_request("sol", "option")

    assert btc.source_currency == "BTC"
    assert btc.instrument_prefix == "BTC-"
    assert eth.source_currency == "ETH"
    assert eth.kind == "future"
    assert sol.source_currency == "USDC"
    assert sol.instrument_prefix == "SOL_USDC-"


def test_sol_trade_fetch_filters_sol_usdc_instruments(monkeypatch: pytest.MonkeyPatch) -> None:
    """SOL should keep only SOL_USDC instruments from the USDC endpoint response."""

    request = deribit_trades.resolve_trades_currency_request("SOL", "future")

    def fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        assert url == deribit_trades.DERIBIT_LAST_TRADES_BY_CURRENCY_URL
        assert params == {
            "currency": "USDC",
            "kind": "future",
            "count": 100,
            "sorting": "asc",
            "start_timestamp": 1_779_606_710_000,
        }
        return {
            "result": {
                "trades": [
                    {"instrument_name": "SOL_USDC-PERPETUAL", "trade_id": "SOL-1"},
                    {"instrument_name": "BTC_USDC-PERPETUAL", "trade_id": "BTC-1"},
                ]
            }
        }

    monkeypatch.setattr(deribit_trades, "get_json", fake_get_json)
    rows = deribit_trades.fetch_last_trades_by_currency(
        request,
        count=100,
        start_timestamp=1_779_606_710_000,
    )

    assert [row["trade_id"] for row in rows] == ["SOL-1"]


def test_unsupported_trade_kind_fails() -> None:
    """Unsupported Deribit trade kinds should fail before issuing HTTP requests."""

    with pytest.raises(ValueError, match="Unsupported trade kind"):
        deribit_trades.resolve_trades_currency_request("BTC", "spot")
