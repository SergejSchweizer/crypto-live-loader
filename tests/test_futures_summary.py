"""Tests for futures summary normalization."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.futures_summary import normalize_futures_summary_rows


def test_futures_summary_normalizes_prices_and_delivery_fields() -> None:
    """Verify futures summary rows preserve basis-relevant market fields."""

    rows, errors = normalize_futures_summary_rows(
        [
            {
                "instrument_name": "BTC-27JUN26",
                "creation_timestamp": 1_779_606_711_643,
                "bid_price": 68000,
                "ask_price": 68010,
                "mark_price": 68005,
                "last": 68002,
                "open_interest": 10,
                "volume": 20,
                "volume_usd": 1_360_000,
                "underlying_price": 67990,
                "estimated_delivery_price": 67995,
                "interest_rate": 0.03,
            }
        ],
        requested_currency="BTC",
        source_currency="BTC",
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert errors == []
    assert rows[0].dataset_type == "futures_summary_snapshot_1m"
    assert rows[0].mid_price == 68005.0
    assert rows[0].estimated_delivery_price == 67995.0
    assert rows[0].instrument_type == "future"
