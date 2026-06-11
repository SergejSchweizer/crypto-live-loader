"""Tests for index price bronze parquet lake functions."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.index_price import IndexPriceSnapshotRow
from ingestion.index_price_lake import index_price_partition_path, save_index_price_snapshot_parquet_lake


def _sample_row(index_name: str = "btc_usd") -> IndexPriceSnapshotRow:
    ts = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return IndexPriceSnapshotRow(
        schema_version="v1",
        dataset_type="index_price_snapshot_1m",
        exchange="deribit",
        source="rest_get_index_price",
        index_name=index_name,
        snapshot_time=ts,
        event_time=ts,
        price=68000.0,
        ingested_at=ts,
        run_id="20260524T071500000000Z",
        raw_payload_hash="abc",
    )


def test_index_price_partition_path() -> None:
    result = index_price_partition_path(
        "lake/bronze",
        ("index_price_snapshot_1m", "deribit", "btc_usd", "2026", "05", "24"),
    )
    assert str(result).endswith(
        "dataset_type=index_price_snapshot_1m/exchange=deribit/index_name=btc_usd/year=2026/month=05/date=24"
    )


def test_save_index_price_snapshot_parquet_lake_writes_partitioned_file(tmp_path: Path) -> None:
    files = save_index_price_snapshot_parquet_lake(
        rows_by_index={"btc_usd": [_sample_row()]},
        lake_root=str(tmp_path),
    )
    assert len(files) == 1
    rows = pq.ParquetFile(files[0]).read().to_pylist()  # type: ignore[no-untyped-call]
    assert rows[0]["index_name"] == "btc_usd"
