"""Tests for instrument-metadata Gold artifact behavior."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from ingestion.instrument_metadata_gold import _gold_instrument_metadata_from_silver, _write_gold_instrument_metadata


def test_write_gold_instrument_metadata_writes_plot_when_enabled(tmp_path: Path) -> None:
    """Verify Gold instrument-metadata artifacts include a PNG profile when plotting is enabled."""

    silver = pl.DataFrame(
        [
            {
                "exchange": "deribit",
                "snapshot_date": date(2026, 6, 7),
                "kind": "option",
                "base_currency": "BTC",
                "is_active": True,
                "is_option": True,
                "strike": 100_000.0,
            },
            {
                "exchange": "deribit",
                "snapshot_date": date(2026, 6, 7),
                "kind": "option",
                "base_currency": "BTC",
                "is_active": False,
                "is_option": True,
                "strike": 110_000.0,
            },
        ]
    )
    gold = _gold_instrument_metadata_from_silver(silver)

    files = _write_gold_instrument_metadata(gold=gold, lake_root=str(tmp_path), plot=True)

    assert any(file_path.endswith("data.parquet") for file_path in files)
    png_file = next(file_path for file_path in files if file_path.endswith("data.png"))
    assert Path(png_file).stat().st_size > 0
