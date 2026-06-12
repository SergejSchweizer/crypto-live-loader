"""Tests for options bronze parquet lake functions."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.options import OptionTickerSnapshotRow
from ingestion.options_lake import option_snapshot_partition_path, save_options_ticker_snapshot_parquet_lake


def _sample_row(
    currency: str = "BTC",
    instrument_name: str = "BTC-30JUN26-120000-C",
    minute: int = 15,
) -> OptionTickerSnapshotRow:
    snapshot_time = datetime(2026, 5, 24, 7, minute, tzinfo=UTC)
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
        snapshot_time=snapshot_time,
        exchange_creation_time=datetime(2026, 5, 24, 7, max(0, minute - 1), 59, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, minute, 1, tzinfo=UTC),
        run_id=f"20260524T07{minute:02d}00000000Z",
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


def test_save_options_ticker_snapshot_parquet_lake_writes_daily_file(tmp_path: Path) -> None:
    """Option bronze writer should persist one upserted parquet file per day."""

    files = save_options_ticker_snapshot_parquet_lake(
        rows_by_currency={"BTC": [_sample_row(), _sample_row(instrument_name="BTC-30JUN26-130000-C", minute=16)]},
        lake_root=str(tmp_path),
    )

    assert len(files) == 1
    assert "dataset_type=options_ticker_snapshot_1m" in files[0]
    assert files[0].endswith("data.parquet")

    rows = pq.ParquetFile(files[0]).read().to_pylist()  # type: ignore[no-untyped-call]
    assert rows[0]["instrument_name"] == "BTC-30JUN26-120000-C"
    assert rows[1]["instrument_name"] == "BTC-30JUN26-130000-C"


def test_save_options_ticker_snapshot_parquet_lake_upserts_daily_file(tmp_path: Path) -> None:
    """Option bronze writer should merge repeat writes by natural key."""

    first = _sample_row()
    replacement = replace(_sample_row(), raw_payload_hash="replacement")

    first_files = save_options_ticker_snapshot_parquet_lake(
        rows_by_currency={"BTC": [first]},
        lake_root=str(tmp_path),
    )
    second_files = save_options_ticker_snapshot_parquet_lake(
        rows_by_currency={"BTC": [replacement]},
        lake_root=str(tmp_path),
    )

    rows = pq.ParquetFile(second_files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert first_files == second_files
    assert len(rows) == 1
    assert rows[0]["raw_payload_hash"] == "replacement"
