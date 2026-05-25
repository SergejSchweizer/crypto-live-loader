"""Tests for bronze-to-silver option chain transformations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.options import OptionTickerSnapshotRow
from ingestion.options_lake import save_options_ticker_snapshot_parquet_lake
from ingestion.options_silver import (
    option_chain_features_from_bronze,
    option_silver_partition_path,
    option_silver_transform_state_path,
    transform_option_bronze_to_silver,
)


def _sample_option_row(
    *,
    instrument_name: str = "BTC-30JUN26-120000-C",
    snapshot_second: int = 1,
) -> OptionTickerSnapshotRow:
    return OptionTickerSnapshotRow(
        exchange="deribit",
        dataset_type="options_ticker_snapshot_1m",
        source="rest_get_book_summary_by_currency",
        currency="BTC",
        requested_currency="BTC",
        source_currency="BTC",
        instrument_name=instrument_name,
        base_currency="BTC",
        quote_currency="BTC",
        instrument_type="option",
        snapshot_time=datetime(2026, 5, 24, 7, 15, snapshot_second, tzinfo=UTC),
        exchange_creation_time=datetime(2026, 5, 24, 7, 15, snapshot_second, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, snapshot_second, tzinfo=UTC),
        run_id="run-id",
        bid_price=0.01,
        ask_price=0.02,
        mid_price=0.015,
        mark_price=0.016,
        mark_iv=45.0,
        underlying_price=75_000.0,
        underlying_index="BTC-30JUN26",
        interest_rate=0.0,
        open_interest=10.0,
        volume=2.0,
        volume_usd=1500.0,
        high=0.03,
        low=0.01,
        last=0.02,
        price_change=10.0,
        raw_payload_hash="hash",
        schema_version="v1",
    )


def test_option_silver_partition_path_uses_monthly_layout() -> None:
    result = option_silver_partition_path(
        "lake/silver",
        ("deribit", "option", "BTC", "2026-05"),
    )
    assert str(result).endswith(
        "dataset_type=option_chain_features_1m/exchange=deribit/instrument_type=option/currency=BTC/month=2026-05"
    )


def test_option_chain_features_from_bronze_parses_contract_fields(tmp_path: Path) -> None:
    bronze_files = save_options_ticker_snapshot_parquet_lake(
        rows_by_currency={"BTC": [_sample_option_row()]},
        lake_root=str(tmp_path / "bronze"),
    )
    bronze = pl.read_parquet(bronze_files)
    silver = option_chain_features_from_bronze(bronze)
    row = silver.row(0, named=True)

    assert row["dataset_type"] == "option_chain_features_1m"
    assert row["option_type"] == "call"
    assert row["strike"] == 120000.0
    assert row["expiry_date"] is not None
    assert row["tau_years"] is not None
    assert row["quality_flags"] == []


def test_option_chain_features_from_bronze_flags_invalid_instrument_name(tmp_path: Path) -> None:
    bronze_files = save_options_ticker_snapshot_parquet_lake(
        rows_by_currency={"BTC": [_sample_option_row(instrument_name="BAD-SYMBOL")]},
        lake_root=str(tmp_path / "bronze"),
    )
    bronze = pl.read_parquet(bronze_files)
    silver = option_chain_features_from_bronze(bronze)
    flags = silver.row(0, named=True)["quality_flags"]
    assert "invalid_instrument_name" in flags


def test_transform_option_bronze_to_silver_writes_monthly_artifacts(tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    save_options_ticker_snapshot_parquet_lake(
        rows_by_currency={"BTC": [_sample_option_row()]},
        lake_root=str(bronze_root),
    )

    first_files = transform_option_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
    )
    second_files = transform_option_bronze_to_silver(
        bronze_lake_root=str(bronze_root),
        silver_lake_root=str(silver_root),
    )

    assert second_files == []
    assert any(file_path.endswith("2026-05.parquet") for file_path in first_files)
    assert any(file_path.endswith("2026-05.json") for file_path in first_files)
    assert any(file_path.endswith("2026-05.png") for file_path in first_files)

    json_file = next(file_path for file_path in first_files if file_path.endswith(".json"))
    metadata = json.loads(Path(json_file).read_text(encoding="utf-8"))
    assert metadata["dataset_type"] == "option_chain_features_1m"
    assert option_silver_transform_state_path(str(silver_root)).exists()
