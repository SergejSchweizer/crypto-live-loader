"""Deribit futures summary source adapter."""

from __future__ import annotations

from ingestion.http_client import get_json
from sources.deribit.public_api import deribit_public_result_rows
from sources.deribit_instruments import resolve_instruments_currency_request

DERIBIT_FUTURES_SUMMARY_URL = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
DERIBIT_FUTURES_SUMMARY_SOURCE = "public/get_book_summary_by_currency"


def fetch_futures_book_summary_rows(currency: str) -> tuple[list[dict[str, object]], str]:
    """Fetch Deribit futures summary rows for one logical currency."""

    request = resolve_instruments_currency_request(currency)
    rows = deribit_public_result_rows(
        DERIBIT_FUTURES_SUMMARY_URL,
        params={"currency": request.source_currency, "kind": "future"},
        context="futures summary",
        json_getter=get_json,
    )

    if request.instrument_prefix is not None:
        rows = [row for row in rows if str(row.get("instrument_name", "")).startswith(request.instrument_prefix)]
    return rows, request.source_currency
