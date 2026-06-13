"""Deribit instrument metadata source adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from ingestion.http_client import get_json

DERIBIT_INSTRUMENTS_URL = "https://www.deribit.com/api/v2/public/get_instruments"
DERIBIT_INSTRUMENT_URL = "https://www.deribit.com/api/v2/public/get_instrument"
DERIBIT_INSTRUMENTS_SOURCE = "public/get_instruments"
DERIBIT_INSTRUMENT_SOURCE = "public/get_instrument"
SUPPORTED_INSTRUMENT_CURRENCIES = ("BTC", "ETH", "SOL")


@dataclass(frozen=True, slots=True)
class InstrumentsCurrencyRequest:
    """Resolved Deribit instrument metadata request for one logical currency."""

    requested_currency: str
    source_currency: str
    instrument_prefix: str | None


def resolve_instruments_currency_request(currency: str) -> InstrumentsCurrencyRequest:
    """Map logical BTC/ETH/SOL metadata requests into Deribit endpoint parameters."""

    normalized = currency.strip().upper()
    if normalized == "BTC":
        return InstrumentsCurrencyRequest(requested_currency="BTC", source_currency="BTC", instrument_prefix="BTC-")
    if normalized == "ETH":
        return InstrumentsCurrencyRequest(requested_currency="ETH", source_currency="ETH", instrument_prefix="ETH-")
    if normalized == "SOL":
        return InstrumentsCurrencyRequest(
            requested_currency="SOL",
            source_currency="USDC",
            instrument_prefix="SOL_USDC-",
        )

    supported = ", ".join(SUPPORTED_INSTRUMENT_CURRENCIES)
    raise ValueError(f"Unsupported instrument currency: {normalized}. Supported: {supported}.")


def fetch_instruments(currency: str, kind: str = "option", expired: bool = False) -> list[dict[str, object]]:
    """Fetch Deribit instruments for one currency and kind."""

    request = resolve_instruments_currency_request(currency)
    payload = get_json(
        DERIBIT_INSTRUMENTS_URL,
        params={"currency": request.source_currency, "kind": kind, "expired": str(expired).lower()},
    )
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit instruments response format")
    result = payload.get("result")
    if not isinstance(result, list):
        raise ValueError("Unexpected Deribit instruments payload")
    rows = [cast(dict[str, object], row) for row in result if isinstance(row, dict)]
    if request.instrument_prefix is None:
        return rows
    return [row for row in rows if str(row.get("instrument_name", "")).startswith(request.instrument_prefix)]


def fetch_instrument(instrument_name: str) -> dict[str, object]:
    """Fetch one Deribit instrument metadata payload."""

    payload = get_json(DERIBIT_INSTRUMENT_URL, params={"instrument_name": instrument_name})
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit instrument response format")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit instrument payload")
    return cast(dict[str, object], result)
