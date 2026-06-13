"""Tests for Deribit volatility-index source adapter."""

from __future__ import annotations

import pytest

from sources import deribit_volatility_index


def test_fetch_volatility_index_maps_sol_to_usdc(monkeypatch: pytest.MonkeyPatch) -> None:
    """SOL volatility-index probes should use USDC without fabricating SOL rows."""

    def fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        assert url == deribit_volatility_index.DERIBIT_VOLATILITY_INDEX_URL
        assert params == {
            "currency": "USDC",
            "start_timestamp": 1,
            "end_timestamp": 2,
            "resolution": "60",
        }
        return {"result": {"data": [[1, 2, 3, 4, 5]]}}

    monkeypatch.setattr(deribit_volatility_index, "get_json", fake_get_json)
    candles, source_currency = deribit_volatility_index.fetch_volatility_index_candles(
        "SOL",
        start_timestamp=1,
        end_timestamp=2,
        resolution=60,
    )

    assert source_currency == "USDC"
    assert candles == [[1, 2, 3, 4, 5]]
