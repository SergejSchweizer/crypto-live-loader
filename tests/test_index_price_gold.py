"""Tests for index-price Gold artifact behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.index_price_gold import (
    _gold_index_price_from_silver,
    _write_gold_index_price,
    transform_index_price_silver_to_gold,
)


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


def test_transform_index_price_silver_to_gold_writes_and_skips_unchanged(tmp_path: Path) -> None:
    """Verify Gold index-price transform writes state and skips unchanged inputs."""

    silver_root = tmp_path / "silver"
    silver_dir = (
        silver_root
        / "dataset_type=index_price_snapshot_features_1m"
        / "exchange=deribit"
        / "index_name=btc_usd"
        / "month=2026-06"
    )
    silver_dir.mkdir(parents=True)
    silver_file = silver_dir / "2026-06.parquet"
    pl.DataFrame(
        [
            {
                "exchange": "deribit",
                "index_name": "btc_usd",
                "ts_event": datetime(2026, 6, 7, 10, 0, tzinfo=UTC),
                "price": 100_000.0,
                "log_return_1m": 0.0,
            }
        ]
    ).write_parquet(silver_file)

    first_files = transform_index_price_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(tmp_path / "gold"),
        plot=False,
    )
    second_files = transform_index_price_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(tmp_path / "gold"),
        plot=False,
    )

    assert any(file_path.endswith("data.parquet") for file_path in first_files)
    assert second_files == []


def test_transform_index_price_silver_to_gold_rejects_unknown_fill_policy(
    tmp_path: Path,
) -> None:
    """Verify Gold index-price transform rejects unsupported fill policies."""

    try:
        transform_index_price_silver_to_gold(
            silver_lake_root=str(tmp_path / "silver"),
            gold_lake_root=str(tmp_path / "gold"),
            fill_policy="mystery",
        )
    except ValueError as exc:
        assert "Unsupported fill policy" in str(exc)
    else:
        raise AssertionError("expected unsupported fill policy to raise")
