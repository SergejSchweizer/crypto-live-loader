"""Per-instrument option ticker normalization for Bronze ingestion."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

OPTION_INSTRUMENT_TICKER_DATASET_TYPE = "option_instrument_ticker_snapshot_1m"
OPTION_INSTRUMENT_TICKER_SCHEMA_VERSION = "v1"
OPTION_INSTRUMENT_TICKER_SOURCE = "rest_ticker"


@dataclass(frozen=True, slots=True)
class OptionInstrumentTickerSnapshotRow:
    """One normalized per-instrument option ticker snapshot record."""

    exchange: str
    dataset_type: str
    source: str
    currency: str
    instrument_name: str
    instrument_type: str
    snapshot_time: datetime
    exchange_creation_time: datetime | None
    ingested_at: datetime
    run_id: str
    bid_price: float | None
    ask_price: float | None
    best_bid_price: float | None
    best_ask_price: float | None
    bid_iv: float | None
    ask_iv: float | None
    mark_iv: float | None
    mark_price: float | None
    last_price: float | None
    underlying_price: float | None
    underlying_index: str | None
    interest_rate: float | None
    open_interest: float | None
    volume: float | None
    volume_usd: float | None
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    rho: float | None
    raw_payload_hash: str
    schema_version: str


def utc_run_id() -> str:
    """Create a UTC run identifier for option instrument ticker Bronze writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def snapshot_time_floor_minute(now: datetime | None = None) -> datetime:
    """Return a UTC snapshot time floored to minute."""

    base = now or datetime.now(UTC)
    utc_base = base.astimezone(UTC)
    return utc_base.replace(second=0, microsecond=0)


def normalize_option_instrument_ticker_rows(
    rows: dict[str, dict[str, object]],
    *,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str = OPTION_INSTRUMENT_TICKER_SOURCE,
    schema_version: str = OPTION_INSTRUMENT_TICKER_SCHEMA_VERSION,
) -> tuple[list[OptionInstrumentTickerSnapshotRow], list[str]]:
    """Normalize Deribit per-instrument ticker payloads into typed Bronze rows.

    Args:
        rows (dict[str, dict[str, object]]): Raw ticker payloads keyed by requested instrument.
        run_id (str): Idempotent run identifier for this collection pass.
        snapshot_time (datetime): UTC minute timestamp assigned to the fetch batch.
        ingested_at (datetime): UTC ingestion timestamp.
        source (str): Source identifier written to Bronze rows.
        schema_version (str): Schema version written to Bronze rows.

    Returns:
        tuple[list[OptionInstrumentTickerSnapshotRow], list[str]]: Normalized rows and rejection messages.
    """

    normalized: list[OptionInstrumentTickerSnapshotRow] = []
    errors: list[str] = []
    for requested_instrument, row in rows.items():
        normalized_row = _normalize_option_instrument_ticker_row(
            requested_instrument=requested_instrument,
            row=row,
            run_id=run_id,
            snapshot_time=snapshot_time,
            ingested_at=ingested_at,
            source=source,
            schema_version=schema_version,
        )
        if normalized_row is None:
            instrument_name = str(row.get("instrument_name", requested_instrument))
            errors.append(f"Rejected option ticker row instrument_name={instrument_name}")
            continue
        normalized.append(normalized_row)
    return normalized, errors


def _normalize_option_instrument_ticker_row(
    *,
    requested_instrument: str,
    row: dict[str, object],
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str,
    schema_version: str,
) -> OptionInstrumentTickerSnapshotRow | None:
    instrument_name = _to_optional_str(row.get("instrument_name")) or requested_instrument.strip().upper()
    if not _looks_like_option_instrument(instrument_name):
        return None

    greeks = row.get("greeks")
    greeks_payload = cast(dict[str, object], greeks) if isinstance(greeks, dict) else {}
    stats = row.get("stats")
    stats_payload = cast(dict[str, object], stats) if isinstance(stats, dict) else {}
    return OptionInstrumentTickerSnapshotRow(
        exchange="deribit",
        dataset_type=OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        source=source,
        currency=_option_currency(instrument_name),
        instrument_name=instrument_name,
        instrument_type="option",
        snapshot_time=snapshot_time,
        exchange_creation_time=_timestamp_ms_to_datetime(row.get("creation_timestamp")),
        ingested_at=ingested_at,
        run_id=run_id,
        bid_price=_to_optional_float(row.get("bid_price")),
        ask_price=_to_optional_float(row.get("ask_price")),
        best_bid_price=_to_optional_float(row.get("best_bid_price")),
        best_ask_price=_to_optional_float(row.get("best_ask_price")),
        bid_iv=_to_optional_float(row.get("bid_iv")),
        ask_iv=_to_optional_float(row.get("ask_iv")),
        mark_iv=_to_optional_float(row.get("mark_iv")),
        mark_price=_to_optional_float(row.get("mark_price")),
        last_price=_to_optional_float(row.get("last_price")),
        underlying_price=_to_optional_float(row.get("underlying_price")),
        underlying_index=_to_optional_str(row.get("underlying_index")),
        interest_rate=_to_optional_float(row.get("interest_rate")),
        open_interest=_to_optional_float(row.get("open_interest")),
        volume=_to_optional_float(stats_payload.get("volume")),
        volume_usd=_to_optional_float(stats_payload.get("volume_usd")),
        delta=_to_optional_float(greeks_payload.get("delta")),
        gamma=_to_optional_float(greeks_payload.get("gamma")),
        theta=_to_optional_float(greeks_payload.get("theta")),
        vega=_to_optional_float(greeks_payload.get("vega")),
        rho=_to_optional_float(greeks_payload.get("rho")),
        raw_payload_hash=_raw_payload_hash(row),
        schema_version=schema_version,
    )


def _looks_like_option_instrument(instrument_name: str) -> bool:
    parts = instrument_name.split("-")
    if len(parts) < 4:
        return False
    return parts[-1] in {"C", "P"} and parts[-2].replace(".", "", 1).isdigit()


def _option_currency(instrument_name: str) -> str:
    base = instrument_name.split("-", 1)[0]
    return base.removesuffix("_USDC")


def _timestamp_ms_to_datetime(value: object) -> datetime | None:
    if value is None or not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(str(value))


def _to_optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _raw_payload_hash(row: dict[str, object]) -> str:
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
