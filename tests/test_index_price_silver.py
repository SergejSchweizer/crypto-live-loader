"""Tests for index-price Silver transformations."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.index_price import IndexPriceSnapshotRow
from ingestion.index_price_lake import save_index_price_snapshot_parquet_lake
from ingestion.index_price_silver import transform_index_price_bronze_to_silver


def _index_price_row(index_name: str, price: float, minute: int) -> IndexPriceSnapshotRow:
    ts = datetime(2026, 6, 8, 5, minute, tzinfo=UTC)
    return IndexPriceSnapshotRow(
        schema_version="v1",
        dataset_type="index_price_snapshot_1m",
        exchange="deribit",
        source="rest_get_index_price",
        index_name=index_name,
        snapshot_time=ts,
        event_time=ts,
        price=price,
        ingested_at=ts,
        run_id=f"run-{minute}",
        raw_payload_hash=f"hash-{minute}",
    )


def test_transform_index_price_bronze_to_silver_writes_and_skips_unchanged(
    tmp_path: Path,
) -> None:
    """Verify index-price Silver transform writes monthly partitions and state."""

    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    save_index_price_snapshot_parquet_lake(
        rows_by_index={
            "btc_usd": [
                _index_price_row("btc_usd", 100_000.0, 0),
                _index_price_row("btc_usd", 100_100.0, 1),
            ]
        },
        lake_root=str(bronze_root),
    )

    first_files = transform_index_price_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
    )
    second_files = transform_index_price_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
    )

    assert len(first_files) == 1
    assert second_files == []
    silver = pl.read_parquet(first_files[0])
    assert silver["dataset_type"].to_list() == ["index_price_snapshot_features_1m"] * 2
    assert silver["price_delta"].to_list() == [None, 100.0]
    assert (silver_root / "_silver_index_price_transform_state.json").exists()


def test_transform_index_price_bronze_to_silver_returns_empty_without_inputs(
    tmp_path: Path,
) -> None:
    """Verify index-price Silver transform is a no-op without Bronze input."""

    assert (
        transform_index_price_bronze_to_silver(
            bronze_lake_root=str(tmp_path / "bronze"),
            silver_lake_root=str(tmp_path / "silver"),
        )
        == []
    )
