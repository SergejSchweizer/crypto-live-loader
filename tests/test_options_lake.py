"""Tests for options bronze parquet lake functions."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.options import OptionTickerSnapshotRow
from ingestion.options_lake import option_snapshot_partition_path, save_options_ticker_snapshot_parquet_lake


def _sample_row(currency: str = "BTC", instrument_name: str = "BTC-30JUN26-120000-C") -> OptionTickerSnapshotRow:
    return OptionTickerSnapshotRow(
        exchange="deribit",
        dataset_type="options_ticker_snapshot_1m",
        source="rest_get_book_summary_by_currency",
        currency=currency,
        requested_currency=currency,
        source_currency="BTC" if currency == "BTC" else "USDC",
        instrument_name=instrument_name,
        base_currency=currency,
        quote_currency=currency,
        instrument_type="option",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        exchange_creation_time=datetime(2026, 5, 24, 7, 14, 59, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
        run_id="20260524T071500000000Z",
        bid_price=None,
        ask_price=0.11,
        mid_price=0.12,
        mark_price=0.13,
        mark_iv=None,
        underlying_price=76800.0,
        underlying_index="BTC-30JUN26",
        interest_rate=None,
        open_interest=10.0,
        volume=1.0,
        volume_usd=80.0,
        high=None,
        low=None,
        last=None,
        price_change=None,
        raw_payload_hash="abc",
        schema_version="v1",
    )


def test_bronze_partition_path() -> None:
    """Option bronze path should include dataset identity and currency/date partitions."""

    result = option_snapshot_partition_path(
        "lake/bronze",
        ("options_ticker_snapshot_1m", "deribit", "option", "BTC", "2026", "05", "24"),
    )
    assert str(result).endswith(
        "dataset_type=options_ticker_snapshot_1m/exchange=deribit/instrument_type=option/currency=BTC/"
        "source=rest_get_book_summary_by_currency/year=2026/month=05/date=24"
    )


def test_save_options_ticker_snapshot_parquet_lake_writes_partitioned_files(tmp_path: Path) -> None:
    """Option bronze writer should persist run-partitioned parquet files."""

    files = save_options_ticker_snapshot_parquet_lake(
        rows_by_currency={"BTC": [_sample_row()]},
        lake_root=str(tmp_path),
    )

    assert len(files) == 1
    assert "dataset_type=options_ticker_snapshot_1m" in files[0]
    assert "part-20260524T071500000000Z.parquet" in files[0]

    rows = pq.ParquetFile(files[0]).read().to_pylist()  # type: ignore[no-untyped-call]
    assert rows[0]["instrument_name"] == "BTC-30JUN26-120000-C"
