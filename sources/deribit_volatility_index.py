"""Deribit volatility index candle source adapter."""

from __future__ import annotations

from typing import cast

from ingestion.http_client import get_json
from sources.deribit.public_api import deribit_public_result_mapping

DERIBIT_VOLATILITY_INDEX_URL = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
SUPPORTED_VOLATILITY_INDEX_CURRENCIES = ("BTC", "ETH", "SOL")


def volatility_index_source_currency(currency: str) -> str:
    """Map logical volatility-index currency requests to Deribit endpoint currencies."""

    normalized = currency.strip().upper()
    if normalized in {"BTC", "ETH"}:
        return normalized
    if normalized == "SOL":
        return "USDC"
    supported = ", ".join(SUPPORTED_VOLATILITY_INDEX_CURRENCIES)
    raise ValueError(f"Unsupported volatility index currency: {normalized}. Supported: {supported}.")


def fetch_volatility_index_candles(
    currency: str,
    *,
    start_timestamp: int,
    end_timestamp: int,
    resolution: int,
) -> tuple[list[list[object]], str]:
    """Fetch Deribit volatility-index candles for one logical currency."""

    source_currency = volatility_index_source_currency(currency)
    result = deribit_public_result_mapping(
        DERIBIT_VOLATILITY_INDEX_URL,
        params={
            "currency": source_currency,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "resolution": str(resolution),
        },
        context="volatility index",
        json_getter=get_json,
    )
    data = result.get("data")
    if not isinstance(data, list):
        raise ValueError("Unexpected Deribit volatility index candle data")
    return [cast(list[object], candle) for candle in data if isinstance(candle, list)], source_currency
