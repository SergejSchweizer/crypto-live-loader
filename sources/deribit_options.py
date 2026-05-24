"""Deribit options chain summary source adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from ingestion.http_client import get_json

DERIBIT_OPTIONS_SUMMARY_URL = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
DERIBIT_OPTIONS_SOURCE = "public/get_book_summary_by_currency"

SUPPORTED_OPTION_CURRENCIES = ("BTC", "ETH", "SOL")


@dataclass(frozen=True)
class OptionsCurrencyRequest:
    """Resolved fetch parameters for one requested options currency."""

    requested_currency: str
    source_currency: str
    instrument_prefix: str | None


def resolve_options_currency_request(currency: str) -> OptionsCurrencyRequest:
    """Map a logical currency into Deribit endpoint parameters."""

    normalized = currency.strip().upper()
    if normalized == "BTC":
        return OptionsCurrencyRequest(requested_currency="BTC", source_currency="BTC", instrument_prefix="BTC-")
    if normalized == "ETH":
        return OptionsCurrencyRequest(requested_currency="ETH", source_currency="ETH", instrument_prefix="ETH-")
    if normalized == "SOL":
        return OptionsCurrencyRequest(
            requested_currency="SOL",
            source_currency="USDC",
            instrument_prefix="SOL_USDC-",
        )
    supported = ", ".join(SUPPORTED_OPTION_CURRENCIES)
    raise ValueError(f"Unsupported option currency: {normalized}. Supported: {supported}.")


def fetch_option_book_summary_rows(request: OptionsCurrencyRequest) -> list[dict[str, object]]:
    """Fetch Deribit option summary rows for one currency request."""

    payload = get_json(
        DERIBIT_OPTIONS_SUMMARY_URL,
        params={"currency": request.source_currency, "kind": "option"},
    )
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit option summary response format")
    result = payload.get("result")
    if not isinstance(result, list):
        raise ValueError("Unexpected Deribit option summary payload")

    rows = [cast(dict[str, object], row) for row in result if isinstance(row, dict)]
    if request.instrument_prefix is None:
        return rows
    return [row for row in rows if str(row.get("instrument_name", "")).startswith(request.instrument_prefix)]
