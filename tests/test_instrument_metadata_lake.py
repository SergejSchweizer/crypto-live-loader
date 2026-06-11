"""Tests for instrument metadata bronze parquet lake functions."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.instrument_metadata import InstrumentMetadataSnapshotRow
from ingestion.instrument_metadata_lake import (
    instrument_metadata_partition_path,
    save_instrument_metadata_snapshot_parquet_lake,
)


def _sample_row(instrument_name: str = "BTC-30JUN26-120000-C") -> InstrumentMetadataSnapshotRow:
    return InstrumentMetadataSnapshotRow(
        schema_version="v1",
        dataset_type="instrument_metadata_snapshot_daily",
        exchange="deribit",
        source="rest_get_instruments",
        snapshot_date=date(2026, 5, 24),
        ingested_at=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        run_id="20260524T071500000000Z",
        instrument_name=instrument_name,
        kind="option",
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
        strike=120000.0,
        raw_payload_hash="abc",
    )


def test_instrument_metadata_partition_path() -> None:
    result = instrument_metadata_partition_path(
        "lake/bronze",
        ("instrument_metadata_snapshot_daily", "deribit", "2026", "05", "24"),
    )
    assert str(result).endswith(
        "dataset_type=instrument_metadata_snapshot_daily/exchange=deribit/year=2026/month=05/date=24"
    )


def test_save_instrument_metadata_snapshot_parquet_lake_writes_partitioned_file(tmp_path: Path) -> None:
    files = save_instrument_metadata_snapshot_parquet_lake(
        rows_by_date={"2026-05-24": [_sample_row()]},
        lake_root=str(tmp_path),
    )
    assert len(files) == 1
    rows = pq.ParquetFile(files[0]).read().to_pylist()  # type: ignore[no-untyped-call]
    assert rows[0]["instrument_name"] == "BTC-30JUN26-120000-C"
