"""Deribit option order-book source adapter."""

from __future__ import annotations

from typing import cast

from ingestion.http_client import get_json

DERIBIT_OPTION_ORDER_BOOK_URL = "https://www.deribit.com/api/v2/public/get_order_book"
DERIBIT_OPTION_ORDER_BOOK_SOURCE = "public/get_order_book"


def fetch_option_order_book(instrument_name: str, depth: int) -> dict[str, object]:
    """Fetch one Deribit option order-book snapshot by instrument name."""

    normalized_instrument = instrument_name.strip().upper()
    if not _looks_like_option_instrument(normalized_instrument):
        raise ValueError(f"Expected Deribit option instrument name, got '{instrument_name}'")
    if depth <= 0:
        raise ValueError("depth must be positive")

    payload = get_json(
        DERIBIT_OPTION_ORDER_BOOK_URL,
        params={"instrument_name": normalized_instrument, "depth": depth},
    )
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit option order-book response format")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit option order-book payload")
    return cast(dict[str, object], result)


def _looks_like_option_instrument(instrument_name: str) -> bool:
    parts = instrument_name.split("-")
    if len(parts) < 4:
        return False
    return parts[-1] in {"C", "P"} and parts[-2].replace(".", "", 1).isdigit()
