"""Parquet lake writer for Bronze volatility-index snapshots."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path

from ingestion.parquet_repository import ParquetUpsertRepository
from ingestion.volatility_index import VolatilityIndexSnapshotRow

VolatilityIndexPartitionKey = tuple[str, str, str, str, str, str, str, str]
VolatilityIndexNaturalKey = tuple[str, str, datetime, int]


def volatility_index_partition_path(lake_root: str, key: VolatilityIndexPartitionKey) -> Path:
    """Return the Bronze destination directory for one volatility-index partition."""

    dataset_type, exchange, currency, source, year, month, date, hour = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"currency={currency}"
        / f"source={source}"
        / f"year={year}"
        / f"month={month}"
        / f"date={date}"
        / f"hour={hour}"
    )


def volatility_index_snapshot_record(row: VolatilityIndexSnapshotRow) -> dict[str, object]:
    """Convert one typed volatility-index row to a parquet record."""

    return {
        "schema_version": row.schema_version,
        "dataset_type": row.dataset_type,
        "exchange": row.exchange,
        "source": row.source,
        "currency": row.currency,
        "source_currency": row.source_currency,
        "timestamp": row.timestamp,
        "open": row.open,
        "high": row.high,
        "low": row.low,
        "close": row.close,
        "resolution": row.resolution,
        "snapshot_time": row.snapshot_time,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "raw_payload_hash": row.raw_payload_hash,
    }


def _natural_key(record: dict[str, object]) -> VolatilityIndexNaturalKey:
    timestamp = record["timestamp"]
    if not isinstance(timestamp, datetime):
        raise ValueError("timestamp must be datetime")
    resolution = record["resolution"]
    if not isinstance(resolution, int):
        raise ValueError("resolution must be int")
    return (
        str(record["exchange"]),
        str(record["currency"]),
        timestamp,
        resolution,
    )


def save_volatility_index_snapshot_parquet_lake(
    rows: list[VolatilityIndexSnapshotRow],
    lake_root: str,
) -> list[str]:
    """Upsert volatility-index rows into hourly Bronze parquet files."""

    repository = ParquetUpsertRepository()
    grouped: defaultdict[VolatilityIndexPartitionKey, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        key: VolatilityIndexPartitionKey = (
            row.dataset_type,
            row.exchange,
            row.currency,
            row.source,
            row.snapshot_time.strftime("%Y"),
            row.snapshot_time.strftime("%m"),
            row.snapshot_time.strftime("%d"),
            row.snapshot_time.strftime("%H"),
        )
        grouped[key].append(volatility_index_snapshot_record(row))

    written_files: list[str] = []
    for key, records in grouped.items():
        file_path = volatility_index_partition_path(lake_root=lake_root, key=key) / "data.parquet"
        written_files.append(
            repository.upsert(
                file_path=file_path,
                records=records,
                natural_key=lambda item: _natural_key(item),
                sort_key=lambda item: str(item["timestamp"]),
                staging_name=f".staging-{records[0]['run_id']}.parquet",
            )
        )
    return sorted(written_files)
