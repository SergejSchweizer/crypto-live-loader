"""Parquet lake writer for bronze index price snapshots."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import cast

from ingestion.file_lock import locked_output_path
from ingestion.index_price import IndexPriceSnapshotRow

IndexPricePartitionKey = tuple[str, str, str, str, str]
IndexPriceNaturalKey = tuple[str, str, datetime, str]


def index_price_partition_path(lake_root: str, key: IndexPricePartitionKey) -> Path:
    """Return destination directory for one index price partition."""

    dataset_type, exchange, index_name, month_partition, date_partition = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"index_name={index_name}"
        / f"month={month_partition}"
        / f"date={date_partition}"
    )


def index_price_snapshot_record(row: IndexPriceSnapshotRow) -> dict[str, object]:
    """Convert one typed index price snapshot row to a parquet record."""

    return {
        "schema_version": row.schema_version,
        "dataset_type": row.dataset_type,
        "exchange": row.exchange,
        "source": row.source,
        "index_name": row.index_name,
        "snapshot_time": row.snapshot_time,
        "event_time": row.event_time,
        "price": row.price,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "raw_payload_hash": row.raw_payload_hash,
    }


def _natural_key(record: dict[str, object]) -> IndexPriceNaturalKey:
    event_time = record["event_time"]
    if not isinstance(event_time, datetime):
        raise ValueError("event_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["index_name"]),
        event_time,
        str(record["source"]),
    )


def save_index_price_snapshot_parquet_lake(
    rows_by_index: dict[str, list[IndexPriceSnapshotRow]],
    lake_root: str,
) -> list[str]:
    """Persist index price rows into idempotent parquet partitions."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    grouped: defaultdict[IndexPricePartitionKey, list[dict[str, object]]] = defaultdict(list)
    for rows in rows_by_index.values():
        for row in rows:
            key: IndexPricePartitionKey = (
                row.dataset_type,
                row.exchange,
                row.index_name,
                row.snapshot_time.strftime("%Y-%m"),
                row.snapshot_time.strftime("%Y-%m-%d"),
            )
            grouped[key].append(index_price_snapshot_record(row))

    written_files: list[str] = []
    for key, records in grouped.items():
        part_dir = index_price_partition_path(lake_root=lake_root, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"
        with locked_output_path(file_path):
            existing_rows: list[dict[str, object]] = []
            if file_path.exists():
                existing_rows = pq.ParquetFile(file_path).read().to_pylist()  # type: ignore[no-untyped-call]
            merged: dict[IndexPriceNaturalKey, dict[str, object]] = {
                _natural_key(existing_record): existing_record for existing_record in existing_rows
            }
            for record in records:
                merged[_natural_key(record)] = record
            output_rows = sorted(merged.values(), key=lambda item: cast(datetime, item["event_time"]))
            table = pa.Table.from_pylist(output_rows)
            staging = part_dir / ".staging-data.parquet"
            pq.write_table(table, staging)  # type: ignore[no-untyped-call]
            staging.replace(file_path)
        written_files.append(str(file_path.resolve()))
    return sorted(written_files)
