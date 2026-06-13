"""Tests for volatility-index candle normalization."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.volatility_index import normalize_volatility_index_candles


def test_volatility_index_normalizes_ohlc_candle() -> None:
    """Verify Deribit volatility-index candles become typed Bronze rows."""

    rows, errors = normalize_volatility_index_candles(
        [[1_779_606_720_000, 52.1, 53.2, 51.9, 52.8]],
        currency="BTC",
        source_currency="BTC",
        resolution=60,
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert errors == []
    assert rows[0].dataset_type == "volatility_index_snapshot_1m"
    assert rows[0].timestamp == datetime(2026, 5, 24, 7, 12, tzinfo=UTC)
    assert rows[0].open == 52.1
    assert rows[0].close == 52.8
