"""Tests for volatility-index Bronze parquet lake functions."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.volatility_index import VolatilityIndexSnapshotRow
from ingestion.volatility_index_lake import save_volatility_index_snapshot_parquet_lake, volatility_index_partition_path


def _sample_row(raw_payload_hash: str = "abc") -> VolatilityIndexSnapshotRow:
    timestamp = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return VolatilityIndexSnapshotRow(
        schema_version="v1",
        dataset_type="volatility_index_snapshot_1m",
        exchange="deribit",
        source="rest_get_volatility_index_data",
        currency="BTC",
        source_currency="BTC",
        timestamp=timestamp,
        open=50.0,
        high=51.0,
        low=49.0,
        close=50.5,
        resolution=60,
        snapshot_time=timestamp,
        ingested_at=timestamp,
        run_id="20260524T071500000000Z",
        raw_payload_hash=raw_payload_hash,
    )


def test_volatility_index_partition_path() -> None:
    """Verify volatility-index paths include currency, source, and hour."""

    result = volatility_index_partition_path(
        "lake/bronze",
        ("volatility_index_snapshot_1m", "deribit", "BTC", "rest", "2026", "05", "24", "07"),
    )

    assert str(result).endswith(
        "dataset_type=volatility_index_snapshot_1m/exchange=deribit/currency=BTC/"
        "source=rest/year=2026/month=05/date=24/hour=07"
    )


def test_save_volatility_index_snapshot_parquet_lake_upserts(tmp_path: Path) -> None:
    """Verify volatility-index rows are upserted by currency, timestamp, and resolution."""

    first_files = save_volatility_index_snapshot_parquet_lake(rows=[_sample_row()], lake_root=str(tmp_path))
    second_files = save_volatility_index_snapshot_parquet_lake(
        rows=[replace(_sample_row(), raw_payload_hash="replacement")],
        lake_root=str(tmp_path),
    )
    rows = pq.ParquetFile(second_files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert first_files == second_files
    assert len(rows) == 1
    assert rows[0]["raw_payload_hash"] == "replacement"
