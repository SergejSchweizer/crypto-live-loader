"""Tests for recent trade tape Bronze parquet lake functions."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.recent_trades import RecentTradeSnapshotRow
from ingestion.recent_trades_lake import recent_trade_partition_path, save_recent_trade_snapshot_parquet_lake


def _sample_row(raw_payload_hash: str = "abc") -> RecentTradeSnapshotRow:
    timestamp = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return RecentTradeSnapshotRow(
        schema_version="v1",
        dataset_type="recent_trade_snapshot_1m",
        exchange="deribit",
        source="rest_get_last_trades_by_currency",
        requested_currency="BTC",
        source_currency="BTC",
        currency="BTC",
        instrument_name="BTC-PERPETUAL",
        instrument_type="perp",
        kind="future",
        trade_id="BTC-1",
        trade_sequence=1,
        exchange_timestamp=timestamp,
        snapshot_time=timestamp,
        ingested_at=timestamp,
        run_id="20260524T071500000000Z",
        price=68_000.0,
        amount=2.0,
        direction="buy",
        tick_direction=0,
        mark_price=68_001.0,
        index_price=67_999.0,
        iv=None,
        liquidation=None,
        block_trade_id=None,
        signed_amount=2.0,
        notional=136_000.0,
        raw_payload_hash=raw_payload_hash,
    )


def test_recent_trade_partition_path() -> None:
    """Verify recent trade paths include type, currency, source, and hour partitions."""

    result = recent_trade_partition_path(
        "lake/bronze",
        (
            "recent_trade_snapshot_1m",
            "deribit",
            "perp",
            "BTC",
            "rest_get_last_trades_by_currency",
            "2026",
            "05",
            "24",
            "07",
        ),
    )

    assert str(result).endswith(
        "dataset_type=recent_trade_snapshot_1m/exchange=deribit/instrument_type=perp/currency=BTC/"
        "source=rest_get_last_trades_by_currency/year=2026/month=05/date=24/hour=07"
    )


def test_save_recent_trade_snapshot_parquet_lake_upserts_trade_id(tmp_path: Path) -> None:
    """Verify repeated overlap-window trades are deduped by natural key."""

    first_files = save_recent_trade_snapshot_parquet_lake(rows=[_sample_row()], lake_root=str(tmp_path))
    second_files = save_recent_trade_snapshot_parquet_lake(
        rows=[replace(_sample_row(), raw_payload_hash="replacement")],
        lake_root=str(tmp_path),
    )

    rows = pq.ParquetFile(second_files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert first_files == second_files
    assert len(rows) == 1
    assert rows[0]["trade_id"] == "BTC-1"
    assert rows[0]["raw_payload_hash"] == "replacement"
