"""Deribit index price source adapter."""

from __future__ import annotations

from ingestion.http_client import get_json
from sources.deribit.public_api import deribit_public_result_mapping

DERIBIT_INDEX_PRICE_URL = "https://www.deribit.com/api/v2/public/get_index_price"
DERIBIT_INDEX_PRICE_SOURCE = "public/get_index_price"


def fetch_index_price(index_name: str) -> float:
    """Fetch one Deribit index price by index name."""

    result = deribit_public_result_mapping(
        DERIBIT_INDEX_PRICE_URL,
        params={"index_name": index_name},
        context="index price",
        json_getter=get_json,
    )
    price = result.get("index_price")
    if not isinstance(price, int | float):
        raise ValueError(f"Deribit index payload missing numeric index_price for {index_name}")
    return float(price)
