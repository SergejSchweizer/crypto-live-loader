"""Deribit instrument metadata source adapter."""

from __future__ import annotations

from typing import cast

from ingestion.http_client import get_json

DERIBIT_INSTRUMENTS_URL = "https://www.deribit.com/api/v2/public/get_instruments"
DERIBIT_INSTRUMENT_URL = "https://www.deribit.com/api/v2/public/get_instrument"
DERIBIT_INSTRUMENTS_SOURCE = "public/get_instruments"
DERIBIT_INSTRUMENT_SOURCE = "public/get_instrument"


def fetch_instruments(currency: str, kind: str = "option", expired: bool = False) -> list[dict[str, object]]:
    """Fetch Deribit instruments for one currency and kind."""

    payload = get_json(
        DERIBIT_INSTRUMENTS_URL,
        params={"currency": currency.upper(), "kind": kind, "expired": str(expired).lower()},
    )
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit instruments response format")
    result = payload.get("result")
    if not isinstance(result, list):
        raise ValueError("Unexpected Deribit instruments payload")
    return [cast(dict[str, object], row) for row in result if isinstance(row, dict)]


def fetch_instrument(instrument_name: str) -> dict[str, object]:
    """Fetch one Deribit instrument metadata payload."""

    payload = get_json(DERIBIT_INSTRUMENT_URL, params={"instrument_name": instrument_name})
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit instrument response format")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit instrument payload")
    return cast(dict[str, object], result)
