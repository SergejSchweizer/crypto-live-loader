"""Tests for shared Bronze lake writer helpers."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from ingestion.lake_writer import bronze_partition_path, upsert_partitioned_records
from ingestion.parquet_repository import ParquetRecord


def test_bronze_partition_path_preserves_partition_order() -> None:
    """Verify ordered partition mappings define deterministic Hive-style paths."""

    result = bronze_partition_path(
        "lake/bronze",
        {
            "dataset_type": "example_snapshot_1m",
            "exchange": "deribit",
            "symbol": "BTC",
            "hour": "07",
        },
    )

    assert str(result).endswith("dataset_type=example_snapshot_1m/exchange=deribit/symbol=BTC/hour=07")


def test_upsert_partitioned_records_groups_sorts_and_dedupes(tmp_path: Path) -> None:
    """Verify shared partitioned writes preserve repository upsert semantics."""

    first_rows: list[ParquetRecord] = [
        {"partition": "BTC", "id": "2", "value": "old", "run_id": "run-1"},
        {"partition": "BTC", "id": "1", "value": "first", "run_id": "run-1"},
    ]
    second_rows: list[ParquetRecord] = [{"partition": "BTC", "id": "2", "value": "replacement", "run_id": "run-2"}]

    first_files = upsert_partitioned_records(
        rows=first_rows,
        lake_root=str(tmp_path),
        partition_key=lambda row: (row["partition"],),
        partition_path=lambda root, key: bronze_partition_path(root, {"partition": key[0]}),
        record_builder=lambda row: row,
        natural_key=lambda row: (row["id"],),
        sort_key=lambda row: str(row["id"]),
        staging_name=lambda records: f".staging-{records[0]['run_id']}.parquet",
    )
    second_files = upsert_partitioned_records(
        rows=second_rows,
        lake_root=str(tmp_path),
        partition_key=lambda row: (row["partition"],),
        partition_path=lambda root, key: bronze_partition_path(root, {"partition": key[0]}),
        record_builder=lambda row: row,
        natural_key=lambda row: (row["id"],),
        sort_key=lambda row: str(row["id"]),
        staging_name=lambda records: f".staging-{records[0]['run_id']}.parquet",
    )

    rows = pq.ParquetFile(second_files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert first_files == second_files
    assert rows == [
        {"partition": "BTC", "id": "1", "value": "first", "run_id": "run-1"},
        {"partition": "BTC", "id": "2", "value": "replacement", "run_id": "run-2"},
    ]
