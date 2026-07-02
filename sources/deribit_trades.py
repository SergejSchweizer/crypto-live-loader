"""Deribit recent trade tape source adapter."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from ingestion.http_client import get_json
from sources.deribit.public_api import deribit_result_from_payload

DERIBIT_LAST_TRADES_BY_CURRENCY_URL = "https://www.deribit.com/api/v2/public/get_last_trades_by_currency"
DERIBIT_LAST_TRADES_BY_INSTRUMENT_URL = "https://www.deribit.com/api/v2/public/get_last_trades_by_instrument"
SUPPORTED_TRADE_CURRENCIES = ("BTC", "ETH", "SOL")
SUPPORTED_TRADE_KINDS = ("option", "future")


@dataclass(frozen=True, slots=True)
class TradesCurrencyRequest:
    """Resolved Deribit trade-tape request for one logical currency and instrument kind."""

    requested_currency: str
    source_currency: str
    instrument_prefix: str | None
    kind: str


def resolve_trades_currency_request(currency: str, kind: str) -> TradesCurrencyRequest:
    """Map one logical trade-tape currency into Deribit endpoint parameters.

    Args:
        currency (str): Logical requested currency, such as BTC, ETH, or SOL.
        kind (str): Deribit instrument kind. Supported values are option and future.

    Returns:
        TradesCurrencyRequest: Normalized endpoint currency, instrument prefix, and kind.

    Raises:
        ValueError: If the currency or kind is unsupported.
    """

    normalized_currency = currency.strip().upper()
    normalized_kind = kind.strip().lower()
    if normalized_kind not in SUPPORTED_TRADE_KINDS:
        supported_kinds = ", ".join(SUPPORTED_TRADE_KINDS)
        raise ValueError(f"Unsupported trade kind: {normalized_kind}. Supported: {supported_kinds}.")

    if normalized_currency == "BTC":
        return TradesCurrencyRequest(
            requested_currency="BTC",
            source_currency="BTC",
            instrument_prefix="BTC-",
            kind=normalized_kind,
        )
    if normalized_currency == "ETH":
        return TradesCurrencyRequest(
            requested_currency="ETH",
            source_currency="ETH",
            instrument_prefix="ETH-",
            kind=normalized_kind,
        )
    if normalized_currency == "SOL":
        return TradesCurrencyRequest(
            requested_currency="SOL",
            source_currency="USDC",
            instrument_prefix="SOL_USDC-",
            kind=normalized_kind,
        )

    supported_currencies = ", ".join(SUPPORTED_TRADE_CURRENCIES)
    raise ValueError(f"Unsupported trade currency: {normalized_currency}. Supported: {supported_currencies}.")


def fetch_last_trades_by_currency(
    request: TradesCurrencyRequest,
    *,
    count: int,
    start_timestamp: int | None,
    sorting: str = "asc",
) -> list[dict[str, object]]:
    """Fetch recent Deribit public trades for one currency/kind request.

    Args:
        request (TradesCurrencyRequest): Resolved Deribit currency and kind parameters.
        count (int): Maximum number of trades to request.
        start_timestamp (int | None): Inclusive Unix millisecond lower bound, when provided.
        sorting (str): Deribit trade sorting direction, normally asc for overlap windows.

    Returns:
        list[dict[str, object]]: Raw Deribit trade rows, filtered to the requested SOL prefix when needed.
    """

    params: dict[str, object] = {
        "currency": request.source_currency,
        "kind": request.kind,
        "count": count,
        "sorting": sorting,
    }
    if start_timestamp is not None:
        params["start_timestamp"] = start_timestamp

    payload = get_json(DERIBIT_LAST_TRADES_BY_CURRENCY_URL, params=params)
    rows = _trade_rows_from_payload(payload)
    if request.instrument_prefix is None:
        return rows
    return [row for row in rows if str(row.get("instrument_name", "")).startswith(request.instrument_prefix)]


def fetch_last_trades_by_instrument(
    instrument_name: str,
    *,
    count: int,
    start_timestamp: int | None,
    sorting: str = "asc",
) -> list[dict[str, object]]:
    """Fetch recent Deribit public trades for one explicit instrument.

    Args:
        instrument_name (str): Deribit instrument name.
        count (int): Maximum number of trades to request.
        start_timestamp (int | None): Inclusive Unix millisecond lower bound, when provided.
        sorting (str): Deribit trade sorting direction.

    Returns:
        list[dict[str, object]]: Raw Deribit trade rows.
    """

    params: dict[str, object] = {
        "instrument_name": instrument_name.strip().upper(),
        "count": count,
        "sorting": sorting,
    }
    if start_timestamp is not None:
        params["start_timestamp"] = start_timestamp

    return _trade_rows_from_payload(get_json(DERIBIT_LAST_TRADES_BY_INSTRUMENT_URL, params=params))


def _trade_rows_from_payload(payload: object) -> list[dict[str, object]]:
    result = deribit_result_from_payload(payload, context="trade")
    if isinstance(result, list):
        return [cast(dict[str, object], row) for row in result if isinstance(row, dict)]
    if isinstance(result, Mapping):
        result_payload = cast(Mapping[str, object], result)
        trades = result_payload.get("trades")
        if isinstance(trades, list):
            return [cast(dict[str, object], row) for row in trades if isinstance(row, dict)]
    raise ValueError("Unexpected Deribit trade payload")
