"""Deribit index price source adapter."""

from __future__ import annotations

from ingestion.http_client import get_json

DERIBIT_INDEX_PRICE_URL = "https://www.deribit.com/api/v2/public/get_index_price"
DERIBIT_INDEX_PRICE_SOURCE = "public/get_index_price"


def fetch_index_price(index_name: str) -> float:
    """Fetch one Deribit index price by index name."""

    payload = get_json(DERIBIT_INDEX_PRICE_URL, params={"index_name": index_name})
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit index price response format")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit index price payload")
    price = result.get("index_price")
    if not isinstance(price, int | float):
        raise ValueError(f"Deribit index payload missing numeric index_price for {index_name}")
    return float(price)
