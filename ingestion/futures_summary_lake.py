"""Parquet lake writer for Bronze futures summary snapshots."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

from ingestion.futures_summary import FuturesSummarySnapshotRow
from ingestion.lake_writer import upsert_partitioned_records

FuturesSummaryPartitionKey = tuple[str, str, str, str, str, str, str, str, str]
FuturesSummaryNaturalKey = tuple[str, str, str, datetime]


def futures_summary_partition_path(lake_root: str, key: FuturesSummaryPartitionKey) -> Path:
    """Return the Bronze destination directory for one futures summary partition."""

    dataset_type, exchange, instrument_type, currency, source, year, month, date, hour = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"currency={currency}"
        / f"source={source}"
        / f"year={year}"
        / f"month={month}"
        / f"date={date}"
        / f"hour={hour}"
    )


def futures_summary_snapshot_record(row: FuturesSummarySnapshotRow) -> dict[str, object]:
    """Convert one typed futures summary row to a parquet record."""

    return {
        "schema_version": row.schema_version,
        "dataset_type": row.dataset_type,
        "exchange": row.exchange,
        "source": row.source,
        "currency": row.currency,
        "requested_currency": row.requested_currency,
        "source_currency": row.source_currency,
        "instrument_name": row.instrument_name,
        "instrument_type": row.instrument_type,
        "snapshot_time": row.snapshot_time,
        "exchange_creation_time": row.exchange_creation_time,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "bid_price": row.bid_price,
        "ask_price": row.ask_price,
        "mid_price": row.mid_price,
        "mark_price": row.mark_price,
        "last": row.last,
        "open_interest": row.open_interest,
        "volume": row.volume,
        "volume_usd": row.volume_usd,
        "high": row.high,
        "low": row.low,
        "price_change": row.price_change,
        "underlying_price": row.underlying_price,
        "estimated_delivery_price": row.estimated_delivery_price,
        "interest_rate": row.interest_rate,
        "raw_payload_hash": row.raw_payload_hash,
    }


def _natural_key(record: dict[str, object]) -> FuturesSummaryNaturalKey:
    snapshot_time = record["snapshot_time"]
    if not isinstance(snapshot_time, datetime):
        raise ValueError("snapshot_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["instrument_name"]),
        str(record["source"]),
        snapshot_time,
    )


def _sort_key(record: dict[str, object]) -> str:
    snapshot_time = record["snapshot_time"]
    if not isinstance(snapshot_time, datetime):
        raise ValueError("snapshot_time must be datetime")
    return f"{snapshot_time.isoformat()}|{record['instrument_name']}"


def save_futures_summary_snapshot_parquet_lake(
    rows: list[FuturesSummarySnapshotRow],
    lake_root: str,
) -> list[str]:
    """Upsert futures summary rows into hourly Bronze parquet files."""

    def partition_key(row: FuturesSummarySnapshotRow) -> FuturesSummaryPartitionKey:
        return (
            row.dataset_type,
            row.exchange,
            row.instrument_type,
            row.currency,
            row.source,
            row.snapshot_time.strftime("%Y"),
            row.snapshot_time.strftime("%m"),
            row.snapshot_time.strftime("%d"),
            row.snapshot_time.strftime("%H"),
        )

    return upsert_partitioned_records(
        rows=rows,
        lake_root=lake_root,
        partition_key=partition_key,
        partition_path=lambda root, key: futures_summary_partition_path(
            lake_root=root,
            key=cast(FuturesSummaryPartitionKey, key),
        ),
        record_builder=futures_summary_snapshot_record,
        natural_key=_natural_key,
        sort_key=_sort_key,
        staging_name=lambda records: f".staging-{records[0]['run_id']}.parquet",
    )
