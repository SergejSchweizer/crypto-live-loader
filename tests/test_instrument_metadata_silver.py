"""Tests for instrument-metadata Silver transformations."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from ingestion.instrument_metadata import InstrumentMetadataSnapshotRow
from ingestion.instrument_metadata_lake import save_instrument_metadata_snapshot_parquet_lake
from ingestion.instrument_metadata_silver import transform_instrument_metadata_bronze_to_silver


def _instrument_row(kind: str = "option") -> InstrumentMetadataSnapshotRow:
    return InstrumentMetadataSnapshotRow(
        schema_version="v1",
        dataset_type="instrument_metadata_snapshot_daily",
        exchange="deribit",
        source="rest_get_instruments",
        snapshot_date=date(2026, 6, 8),
        ingested_at=datetime(2026, 6, 8, 5, 0, tzinfo=UTC),
        run_id="run-1",
        instrument_name="BTC-30JUN26-120000-C",
        kind=kind,
        base_currency="BTC",
        quote_currency="USD",
        counter_currency=None,
        settlement_currency="BTC",
        instrument_type="reversed",
        tick_size=0.1,
        contract_size=1.0,
        min_trade_amount=0.1,
        is_active=True,
        creation_timestamp=datetime(2026, 5, 1, tzinfo=UTC),
        expiration_timestamp=datetime(2026, 6, 30, tzinfo=UTC),
        option_type="call",
        strike=120_000.0,
        raw_payload_hash="hash-1",
    )


def test_transform_instrument_metadata_bronze_to_silver_writes_and_skips_unchanged(
    tmp_path: Path,
) -> None:
    """Verify instrument-metadata Silver transform writes monthly partitions and state."""

    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    save_instrument_metadata_snapshot_parquet_lake(
        rows_by_date={"2026-06-08": [_instrument_row()]},
        lake_root=str(bronze_root),
    )

    first_files = transform_instrument_metadata_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
    )
    second_files = transform_instrument_metadata_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
    )

    assert len(first_files) == 1
    assert second_files == []
    silver = pl.read_parquet(first_files[0])
    assert silver["dataset_type"].to_list() == ["instrument_metadata_snapshot_features_daily"]
    assert silver["is_option"].to_list() == [True]
    assert silver["days_to_expiration"].to_list() == [22]
    assert (silver_root / "_silver_instrument_metadata_transform_state.json").exists()


def test_transform_instrument_metadata_bronze_to_silver_returns_empty_without_inputs(
    tmp_path: Path,
) -> None:
    """Verify instrument-metadata Silver transform is a no-op without Bronze input."""

    assert (
        transform_instrument_metadata_bronze_to_silver(
            bronze_lake_root=str(tmp_path / "bronze"),
            silver_lake_root=str(tmp_path / "silver"),
        )
        == []
    )
