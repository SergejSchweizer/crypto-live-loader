"""Parquet lake writer for bronze instrument metadata snapshots."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from ingestion.file_lock import locked_output_path
from ingestion.instrument_metadata import InstrumentMetadataSnapshotRow

InstrumentMetadataPartitionKey = tuple[str, str, str]
InstrumentMetadataNaturalKey = tuple[str, str, str]


def instrument_metadata_partition_path(lake_root: str, key: InstrumentMetadataPartitionKey) -> Path:
    """Return destination directory for one daily instrument metadata partition."""

    dataset_type, exchange, date_partition = key
    return Path(lake_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}" / f"date={date_partition}"


def instrument_metadata_snapshot_record(row: InstrumentMetadataSnapshotRow) -> dict[str, object]:
    """Convert one typed instrument metadata row to a parquet record."""

    return {
        "schema_version": row.schema_version,
        "dataset_type": row.dataset_type,
        "exchange": row.exchange,
        "source": row.source,
        "snapshot_date": row.snapshot_date,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "instrument_name": row.instrument_name,
        "kind": row.kind,
        "base_currency": row.base_currency,
        "quote_currency": row.quote_currency,
        "counter_currency": row.counter_currency,
        "settlement_currency": row.settlement_currency,
        "instrument_type": row.instrument_type,
        "tick_size": row.tick_size,
        "contract_size": row.contract_size,
        "min_trade_amount": row.min_trade_amount,
        "is_active": row.is_active,
        "creation_timestamp": row.creation_timestamp,
        "expiration_timestamp": row.expiration_timestamp,
        "option_type": row.option_type,
        "strike": row.strike,
        "raw_payload_hash": row.raw_payload_hash,
    }


def _natural_key(record: dict[str, object]) -> InstrumentMetadataNaturalKey:
    return (
        str(record["exchange"]),
        str(record["instrument_name"]),
        str(record["snapshot_date"]),
    )


def save_instrument_metadata_snapshot_parquet_lake(
    rows_by_date: dict[str, list[InstrumentMetadataSnapshotRow]],
    lake_root: str,
) -> list[str]:
    """Persist daily instrument metadata rows as idempotent bronze parquet files."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    grouped: defaultdict[InstrumentMetadataPartitionKey, list[dict[str, object]]] = defaultdict(list)
    for rows in rows_by_date.values():
        for row in rows:
            key: InstrumentMetadataPartitionKey = (
                row.dataset_type,
                row.exchange,
                row.snapshot_date.isoformat(),
            )
            grouped[key].append(instrument_metadata_snapshot_record(row))

    written_files: list[str] = []
    for key, records in grouped.items():
        part_dir = instrument_metadata_partition_path(lake_root=lake_root, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"
        with locked_output_path(file_path):
            existing_rows: list[dict[str, object]] = []
            if file_path.exists():
                existing_rows = pq.ParquetFile(file_path).read().to_pylist()  # type: ignore[no-untyped-call]
            merged: dict[InstrumentMetadataNaturalKey, dict[str, object]] = {
                _natural_key(record): record for record in existing_rows
            }
            for record in records:
                merged[_natural_key(record)] = record
            output_rows = sorted(merged.values(), key=lambda item: str(item["instrument_name"]))
            table = pa.Table.from_pylist(output_rows)
            staging = part_dir / ".staging-data.parquet"
            pq.write_table(table, staging)  # type: ignore[no-untyped-call]
            staging.replace(file_path)
        written_files.append(str(file_path.resolve()))
    return sorted(written_files)
