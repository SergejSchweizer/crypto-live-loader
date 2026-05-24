"""Tests for Deribit index price source adapter."""

from __future__ import annotations

import pytest

from sources import deribit_index_price


def test_fetch_index_price_reads_numeric_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """fetch_index_price should extract a numeric index price from payload."""

    def fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        assert url == deribit_index_price.DERIBIT_INDEX_PRICE_URL
        assert params == {"index_name": "btc_usd"}
        return {"result": {"index_price": 68123.45}}

    monkeypatch.setattr(deribit_index_price, "get_json", fake_get_json)
    price = deribit_index_price.fetch_index_price("btc_usd")
    assert price == 68123.45
