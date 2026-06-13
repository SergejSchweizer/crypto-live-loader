"""Tests for Deribit futures summary source adapter."""

from __future__ import annotations

import pytest

from sources import deribit_futures


def test_fetch_futures_summary_maps_sol_to_usdc(monkeypatch: pytest.MonkeyPatch) -> None:
    """SOL futures summary should use USDC and keep only SOL_USDC instruments."""

    def fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        assert url == deribit_futures.DERIBIT_FUTURES_SUMMARY_URL
        assert params == {"currency": "USDC", "kind": "future"}
        return {
            "result": [
                {"instrument_name": "SOL_USDC-PERPETUAL"},
                {"instrument_name": "BTC_USDC-PERPETUAL"},
            ]
        }

    monkeypatch.setattr(deribit_futures, "get_json", fake_get_json)

    rows, source_currency = deribit_futures.fetch_futures_book_summary_rows("SOL")

    assert source_currency == "USDC"
    assert [row["instrument_name"] for row in rows] == ["SOL_USDC-PERPETUAL"]
