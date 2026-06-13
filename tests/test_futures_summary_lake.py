"""Tests for futures summary Bronze parquet lake functions."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.futures_summary import FuturesSummarySnapshotRow
from ingestion.futures_summary_lake import futures_summary_partition_path, save_futures_summary_snapshot_parquet_lake


def _sample_row(raw_payload_hash: str = "abc") -> FuturesSummarySnapshotRow:
    timestamp = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return FuturesSummarySnapshotRow(
        schema_version="v1",
        dataset_type="futures_summary_snapshot_1m",
        exchange="deribit",
        source="rest_get_book_summary_by_currency",
        currency="BTC",
        requested_currency="BTC",
        source_currency="BTC",
        instrument_name="BTC-PERPETUAL",
        instrument_type="perp",
        snapshot_time=timestamp,
        exchange_creation_time=None,
        ingested_at=timestamp,
        run_id="20260524T071500000000Z",
        bid_price=68000.0,
        ask_price=68010.0,
        mid_price=68005.0,
        mark_price=68004.0,
        last=68003.0,
        open_interest=10.0,
        volume=20.0,
        volume_usd=1000.0,
        high=68100.0,
        low=67900.0,
        price_change=1.0,
        underlying_price=68000.0,
        estimated_delivery_price=67995.0,
        interest_rate=0.03,
        raw_payload_hash=raw_payload_hash,
    )


def test_futures_summary_partition_path() -> None:
    """Verify futures summary paths include instrument type, currency, source, and hour."""

    result = futures_summary_partition_path(
        "lake/bronze",
        ("futures_summary_snapshot_1m", "deribit", "perp", "BTC", "rest", "2026", "05", "24", "07"),
    )

    assert str(result).endswith(
        "dataset_type=futures_summary_snapshot_1m/exchange=deribit/instrument_type=perp/"
        "currency=BTC/source=rest/year=2026/month=05/date=24/hour=07"
    )


def test_save_futures_summary_snapshot_parquet_lake_upserts(tmp_path: Path) -> None:
    """Verify futures summary rows are upserted by instrument and snapshot time."""

    first_files = save_futures_summary_snapshot_parquet_lake(rows=[_sample_row()], lake_root=str(tmp_path))
    second_files = save_futures_summary_snapshot_parquet_lake(
        rows=[replace(_sample_row(), raw_payload_hash="replacement")],
        lake_root=str(tmp_path),
    )
    rows = pq.ParquetFile(second_files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert first_files == second_files
    assert len(rows) == 1
    assert rows[0]["raw_payload_hash"] == "replacement"
