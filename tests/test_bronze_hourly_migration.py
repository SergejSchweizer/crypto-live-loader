"""Tests for Bronze hourly partition migration."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.migrate_bronze_hourly_layout import migrate_bronze_hourly_layout


def test_migrate_bronze_hourly_layout_splits_daily_l2_file(tmp_path: Path) -> None:
    """Verify daily Bronze files are split into hourly partition files."""

    source_file = (
        tmp_path
        / "dataset_type=perps_l2_snapshot_1m"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC-PERPETUAL"
        / "depth=50"
        / "source=rest_order_book"
        / "year=2026"
        / "month=06"
        / "date=12"
        / "data.parquet"
    )
    source_file.parent.mkdir(parents=True)
    rows = [
        _l2_row(datetime(2026, 6, 12, 3, 15, tzinfo=UTC)),
        _l2_row(datetime(2026, 6, 12, 4, 1, tzinfo=UTC)),
    ]
    pq.write_table(pa.Table.from_pylist(rows), source_file)  # type: ignore[no-untyped-call]

    summary = migrate_bronze_hourly_layout(tmp_path)

    hour_03 = source_file.parent / "hour=03" / "data.parquet"
    hour_04 = source_file.parent / "hour=04" / "data.parquet"
    assert summary.source_files == 1
    assert summary.target_files == 2
    assert summary.rows == 2
    assert not source_file.exists()
    assert hour_03.exists()
    assert hour_04.exists()
    assert pq.ParquetFile(hour_03).read().num_rows == 1  # type: ignore[no-untyped-call]
    assert pq.ParquetFile(hour_04).read().num_rows == 1  # type: ignore[no-untyped-call]


def _l2_row(event_time: datetime) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "dataset_type": "perps_l2_snapshot_1m",
        "exchange": "deribit",
        "symbol": "BTC-PERPETUAL",
        "instrument_type": "perp",
        "event_time": event_time,
        "ingested_at": event_time,
        "run_id": event_time.strftime("%Y%m%dT%H%M%S000000Z"),
        "source": "rest_order_book",
        "depth": 50,
        "fetch_duration_s": 0.1,
        "bids": [],
        "asks": [],
        "mark_price": 100.0,
        "index_price": 99.0,
        "open_interest": 1.0,
        "funding_8h": 0.0,
        "current_funding": 0.0,
    }
