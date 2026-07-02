"""Parquet lake writer for Bronze recent trade tape snapshots."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

from ingestion.lake_writer import upsert_partitioned_records
from ingestion.recent_trades import RecentTradeSnapshotRow

RecentTradePartitionKey = tuple[str, str, str, str, str, str, str, str, str]
RecentTradeNaturalKey = tuple[str, str, str]


def recent_trade_partition_path(lake_root: str, key: RecentTradePartitionKey) -> Path:
    """Return the Bronze destination directory for one recent-trade partition."""

    (
        dataset_type,
        exchange,
        instrument_type,
        currency,
        source,
        year_partition,
        month_partition,
        date_partition,
        hour_partition,
    ) = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"currency={currency}"
        / f"source={source}"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
        / f"hour={hour_partition}"
    )


def recent_trade_snapshot_record(row: RecentTradeSnapshotRow) -> dict[str, object]:
    """Convert one typed recent-trade row to a parquet record."""

    return {
        "schema_version": row.schema_version,
        "dataset_type": row.dataset_type,
        "exchange": row.exchange,
        "source": row.source,
        "requested_currency": row.requested_currency,
        "source_currency": row.source_currency,
        "currency": row.currency,
        "instrument_name": row.instrument_name,
        "instrument_type": row.instrument_type,
        "kind": row.kind,
        "trade_id": row.trade_id,
        "trade_sequence": row.trade_sequence,
        "exchange_timestamp": row.exchange_timestamp,
        "snapshot_time": row.snapshot_time,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "price": row.price,
        "amount": row.amount,
        "direction": row.direction,
        "tick_direction": row.tick_direction,
        "mark_price": row.mark_price,
        "index_price": row.index_price,
        "iv": row.iv,
        "liquidation": row.liquidation,
        "block_trade_id": row.block_trade_id,
        "signed_amount": row.signed_amount,
        "notional": row.notional,
        "raw_payload_hash": row.raw_payload_hash,
    }


def _natural_key(record: dict[str, object]) -> RecentTradeNaturalKey:
    return (
        str(record["exchange"]),
        str(record["instrument_name"]),
        str(record["trade_id"]),
    )


def _sort_key(record: dict[str, object]) -> str:
    exchange_timestamp = record["exchange_timestamp"]
    if not isinstance(exchange_timestamp, datetime):
        raise ValueError("exchange_timestamp must be datetime")
    return f"{exchange_timestamp.isoformat()}|{record['instrument_name']}|{record['trade_id']}"


def save_recent_trade_snapshot_parquet_lake(
    rows: list[RecentTradeSnapshotRow],
    lake_root: str,
) -> list[str]:
    """Upsert recent trade rows into hourly Bronze parquet files."""

    def partition_key(row: RecentTradeSnapshotRow) -> RecentTradePartitionKey:
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
        partition_path=lambda root, key: recent_trade_partition_path(
            lake_root=root,
            key=cast(RecentTradePartitionKey, key),
        ),
        record_builder=recent_trade_snapshot_record,
        natural_key=_natural_key,
        sort_key=_sort_key,
        staging_name=lambda records: f".staging-{records[0]['run_id']}.parquet",
    )
