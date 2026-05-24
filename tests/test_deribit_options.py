"""Tests for Deribit options source mapping and filtering."""

from __future__ import annotations

import pytest

from sources import deribit_options


def test_btc_eth_currency_mapping() -> None:
    """BTC and ETH should map directly to same Deribit currency."""

    btc = deribit_options.resolve_options_currency_request("BTC")
    eth = deribit_options.resolve_options_currency_request("eth")

    assert btc.source_currency == "BTC"
    assert btc.requested_currency == "BTC"
    assert eth.source_currency == "ETH"
    assert eth.requested_currency == "ETH"


def test_sol_maps_to_usdc_and_filters_sol_usdc_instruments(monkeypatch: pytest.MonkeyPatch) -> None:
    """SOL should map to USDC and keep only SOL_USDC options."""

    request = deribit_options.resolve_options_currency_request("SOL")

    def fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        assert params == {"currency": "USDC", "kind": "option"}
        return {
            "result": [
                {"instrument_name": "SOL_USDC-30JUN26-250-C"},
                {"instrument_name": "BTC_USDC-30JUN26-90000-C"},
            ]
        }

    monkeypatch.setattr(deribit_options, "get_json", fake_get_json)
    rows = deribit_options.fetch_option_book_summary_rows(request)

    assert [row["instrument_name"] for row in rows] == ["SOL_USDC-30JUN26-250-C"]
