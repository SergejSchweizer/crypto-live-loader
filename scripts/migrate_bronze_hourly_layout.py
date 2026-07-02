"""Migrate Bronze parquet data from daily to hourly partitions."""

from __future__ import annotations

import argparse
import shutil
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TypeAlias

from ingestion.file_lock import locked_output_path
from ingestion.parquet_repository import ParquetRecord, ParquetUpsertRepository, SortableValue

NaturalKeyBuilder: TypeAlias = Callable[[ParquetRecord], tuple[object, ...]]
SortKeyBuilder: TypeAlias = Callable[[ParquetRecord], SortableValue]

BRONZE_DATASET_TIMESTAMP_COLUMNS: dict[str, str | None] = {
    "perp_l2_snapshot_1m": "event_time",
    "options_ticker_snapshot_1m": "snapshot_time",
    "options_l2_snapshot_1m": "snapshot_time",
    "instrument_metadata_snapshot_daily": None,
    "index_price_snapshot_1m": "event_time",
}


def main() -> None:
    """Parse arguments and migrate Bronze files to hourly partition directories."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-lake-root", default="lake/bronze", help="Bronze lake root to migrate")
    args = parser.parse_args()
    summary = migrate_bronze_hourly_layout(Path(args.bronze_lake_root))
    print(
        "bronze hourly migration complete "
        f"source_files={summary.source_files} target_files={summary.target_files} rows={summary.rows}"
    )


@dataclass
class MigrationSummary:
    """Mutable migration counters for the Bronze hourly layout move."""

    source_files: int = 0
    target_files: int = 0
    rows: int = 0


def migrate_bronze_hourly_layout(bronze_lake_root: Path) -> MigrationSummary:
    """Move daily Bronze ``data.parquet`` files into hour-partitioned directories.

    Args:
        bronze_lake_root (Path): Root directory containing Bronze dataset partitions.

    Returns:
        MigrationSummary: Counters describing migrated source files, target files, and rows.

    Raises:
        RuntimeError: If ``pyarrow`` is unavailable.
    """

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for Bronze migration. Install project dependencies.") from exc

    repository = ParquetUpsertRepository()
    summary = MigrationSummary()
    for source_file in sorted(bronze_lake_root.glob("dataset_type=*/**/data.parquet")):
        if _is_hourly_data_file(source_file):
            continue
        dataset_type = _partition_value(source_file, "dataset_type")
        if dataset_type not in BRONZE_DATASET_TIMESTAMP_COLUMNS:
            continue

        parquet_file = pq.ParquetFile(source_file)  # type: ignore[no-untyped-call]  # pyarrow parquet readers are untyped.
        if parquet_file.metadata.num_rows == 0:
            source_file.unlink()
            _remove_empty_parents(source_file.parent, stop_at=bronze_lake_root)
            summary.source_files += 1
            continue

        staged_files, staged_rows = _stage_hourly_tables(dataset_type=dataset_type, source_file=source_file)
        for hour_partition, staging_file in staged_files.items():
            target_file = source_file.parent / f"hour={hour_partition}" / "data.parquet"
            _commit_staged_hour_file(
                repository=repository,
                dataset_type=dataset_type,
                target_file=target_file,
                staging_file=staging_file,
            )
            summary.target_files += 1
            summary.rows += staged_rows[hour_partition]

        source_file.unlink()
        shutil.rmtree(_migration_staging_dir(source_file), ignore_errors=True)
        _remove_empty_parents(source_file.parent, stop_at=bronze_lake_root)
        summary.source_files += 1

    return summary


def _stage_hourly_tables(dataset_type: str, source_file: Path) -> tuple[dict[str, Path], dict[str, int]]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    staging_dir = _migration_staging_dir(source_file)
    shutil.rmtree(staging_dir, ignore_errors=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    writers: dict[str, Any] = {}
    staged_files: dict[str, Path] = {}
    staged_rows: dict[str, int] = {}
    try:
        parquet_file = pq.ParquetFile(source_file)  # type: ignore[no-untyped-call]  # pyarrow parquet readers are untyped.
        for batch in parquet_file.iter_batches(batch_size=50_000):  # type: ignore[no-untyped-call]  # pyarrow batch iteration is untyped.
            table = pa.Table.from_batches([batch])
            for hour_partition, hour_rows in _group_table_rows_by_hour(
                dataset_type=dataset_type,
                rows=table.to_pylist(),
            ).items():
                staging_file = staging_dir / f"hour={hour_partition}.parquet"
                hour_table = pa.Table.from_pylist(hour_rows)
                if hour_partition not in writers:
                    writers[hour_partition] = pq.ParquetWriter(  # type: ignore[no-untyped-call]  # pyarrow parquet writers are untyped.
                        staging_file,
                        hour_table.schema,
                    )
                    staged_files[hour_partition] = staging_file
                    staged_rows[hour_partition] = 0
                writers[hour_partition].write_table(hour_table)
                staged_rows[hour_partition] += hour_table.num_rows
    finally:
        for writer in writers.values():
            writer.close()
    return staged_files, staged_rows


def _group_table_rows_by_hour(dataset_type: str, rows: list[ParquetRecord]) -> dict[str, list[ParquetRecord]]:
    timestamp_column = BRONZE_DATASET_TIMESTAMP_COLUMNS[dataset_type]
    if timestamp_column is None:
        return {"00": rows}

    grouped_rows: defaultdict[str, list[ParquetRecord]] = defaultdict(list)
    for row in rows:
        timestamp = row[timestamp_column]
        if not isinstance(timestamp, datetime):
            raise ValueError(f"Expected datetime in {timestamp_column}, got {timestamp!r}")
        grouped_rows[timestamp.strftime("%H")].append(row)
    return dict(sorted(grouped_rows.items()))


def _commit_staged_hour_file(
    *,
    repository: ParquetUpsertRepository,
    dataset_type: str,
    target_file: Path,
    staging_file: Path,
) -> None:
    import pyarrow.parquet as pq

    if target_file.exists():
        repository.upsert(
            file_path=target_file,
            records=pq.ParquetFile(staging_file).read().to_pylist(),  # type: ignore[no-untyped-call]  # pyarrow parquet readers are untyped.
            natural_key=_natural_key_builder(dataset_type),
            sort_key=_sort_key_builder(dataset_type),
            staging_name=".staging-hourly-migration.parquet",
        )
        return

    target_file.parent.mkdir(parents=True, exist_ok=True)
    with locked_output_path(target_file):
        staging_file.replace(target_file)


def _migration_staging_dir(source_file: Path) -> Path:
    return source_file.parent / ".hourly-migration"


def _natural_key_builder(dataset_type: str) -> NaturalKeyBuilder:
    if dataset_type == "perp_l2_snapshot_1m":
        return lambda row: (
            row["exchange"],
            row["instrument_type"],
            row["symbol"],
            row["depth"],
            row["source"],
            row["event_time"],
        )
    if dataset_type == "options_ticker_snapshot_1m":
        return lambda row: (
            row["exchange"],
            row["currency"],
            row["instrument_name"],
            row["source"],
            row["snapshot_time"],
        )
    if dataset_type == "options_l2_snapshot_1m":
        return lambda row: (
            row["exchange"],
            row["instrument_name"],
            row["source"],
            row["depth"],
            row["exchange_timestamp"],
        )
    if dataset_type == "instrument_metadata_snapshot_daily":
        return lambda row: (row["exchange"], row["instrument_name"], row["snapshot_date"])
    if dataset_type == "index_price_snapshot_1m":
        return lambda row: (row["exchange"], row["index_name"], row["event_time"], row["source"])
    raise ValueError(f"Unsupported Bronze dataset type: {dataset_type}")


def _sort_key_builder(dataset_type: str) -> SortKeyBuilder:
    if dataset_type == "perp_l2_snapshot_1m":
        return lambda row: _datetime_sort_key(row, "event_time")
    if dataset_type == "options_ticker_snapshot_1m":
        return lambda row: (
            f"{_datetime_sort_key(row, 'snapshot_time').isoformat()}|{row['currency']}|{row['instrument_name']}"
        )
    if dataset_type == "options_l2_snapshot_1m":
        return lambda row: f"{_datetime_sort_key(row, 'exchange_timestamp').isoformat()}|{row['instrument_name']}"
    if dataset_type == "instrument_metadata_snapshot_daily":
        return lambda row: str(row["instrument_name"])
    if dataset_type == "index_price_snapshot_1m":
        return lambda row: _datetime_sort_key(row, "event_time")
    raise ValueError(f"Unsupported Bronze dataset type: {dataset_type}")


def _datetime_sort_key(row: ParquetRecord, column: str) -> datetime:
    value = row[column]
    if not isinstance(value, datetime):
        raise ValueError(f"Expected datetime in {column}, got {value!r}")
    return value


def _is_hourly_data_file(path: Path) -> bool:
    return path.parent.name.startswith("hour=")


def _partition_value(path: Path, name: str) -> str:
    prefix = f"{name}="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    raise ValueError(f"Missing {name} partition in {path}")


def _remove_empty_parents(start: Path, stop_at: Path) -> None:
    current = start
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


if __name__ == "__main__":
    main()
