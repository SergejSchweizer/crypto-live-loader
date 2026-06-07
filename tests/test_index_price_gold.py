"""Tests for index-price Gold artifact behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.index_price_gold import _gold_index_price_from_silver, _write_gold_index_price


def test_write_gold_index_price_writes_plot_when_enabled(tmp_path: Path) -> None:
    """Verify Gold index-price artifacts include a PNG profile when plotting is enabled."""

    silver = pl.DataFrame(
        [
            {
                "exchange": "deribit",
                "index_name": "btc_usd",
                "ts_event": datetime(2026, 6, 7, 10, 0, tzinfo=UTC),
                "price": 100_000.0,
                "log_return_1m": 0.0,
            },
            {
                "exchange": "deribit",
                "index_name": "btc_usd",
                "ts_event": datetime(2026, 6, 7, 10, 0, 30, tzinfo=UTC),
                "price": 100_100.0,
                "log_return_1m": 0.001,
            },
        ]
    )
    gold = _gold_index_price_from_silver(silver)

    files = _write_gold_index_price(gold=gold, lake_root=str(tmp_path), plot=True)

    assert any(file_path.endswith("data.parquet") for file_path in files)
    png_file = next(file_path for file_path in files if file_path.endswith("data.png"))
    assert Path(png_file).stat().st_size > 0
