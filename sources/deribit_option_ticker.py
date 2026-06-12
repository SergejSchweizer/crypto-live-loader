"""Deribit per-instrument option ticker source adapter."""

from __future__ import annotations

from typing import cast

from ingestion.http_client import get_json

DERIBIT_OPTION_TICKER_URL = "https://www.deribit.com/api/v2/public/ticker"
DERIBIT_OPTION_TICKER_SOURCE = "public/ticker"


def fetch_option_ticker(instrument_name: str) -> dict[str, object]:
    """Fetch one Deribit ticker payload for an option instrument.

    Args:
        instrument_name (str): Deribit option instrument name.

    Returns:
        dict[str, object]: Raw ticker payload from the Deribit ``result`` field.

    Raises:
        ValueError: If the instrument name or response payload is invalid.
    """

    normalized_instrument = instrument_name.strip().upper()
    if not normalized_instrument:
        raise ValueError("instrument_name must not be empty")

    payload = get_json(DERIBIT_OPTION_TICKER_URL, params={"instrument_name": normalized_instrument})
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit option ticker response format")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit option ticker payload")
    return cast(dict[str, object], result)
