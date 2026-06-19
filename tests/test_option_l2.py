"""Tests for option order-book Bronze normalization."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.option_l2 import normalize_option_l2_snapshot_rows


def test_option_l2_normalizes_book_iv_stats_and_greeks() -> None:
    """Verify Deribit option order-book depth and IV fields are preserved."""

    rows, errors = normalize_option_l2_snapshot_rows(
        {
            "SOL_USDC-30JUN26-250-C": {
                "instrument_name": "SOL_USDC-30JUN26-250-C",
                "timestamp": 1_779_606_712_000,
                "state": "open",
                "bids": [[0.11, 4.2], [0.1, 1.5]],
                "asks": [[0.12, 3.7]],
                "best_bid_price": 0.11,
                "best_ask_price": 0.12,
                "best_bid_amount": 4.2,
                "best_ask_amount": 3.7,
                "bid_iv": 54.1,
                "ask_iv": 55.2,
                "mark_iv": 54.8,
                "underlying_price": 250.1,
                "underlying_index": "SOL_USDC-30JUN26",
                "index_price": 250.2,
                "interest_rate": 0.03,
                "open_interest": 100,
                "stats": {"volume": 10, "volume_usd": 1000, "high": 0.14, "low": 0.08, "price_change": 1.5},
                "greeks": {"delta": 0.42, "gamma": 0.01, "theta": -0.2, "vega": 1.3, "rho": 0.5},
            }
        },
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
        depth=20,
        fetch_durations_s={"SOL_USDC-30JUN26-250-C": 0.123},
    )

    assert errors == []
    assert rows[0].dataset_type == "option_l2_snapshot_1m"
    assert rows[0].currency == "SOL"
    assert rows[0].depth == 20
    assert rows[0].fetch_duration_s == 0.123
    assert rows[0].bid_levels == 2
    assert rows[0].ask_levels == 1
    assert rows[0].bids == [{"price": 0.11, "amount": 4.2}, {"price": 0.1, "amount": 1.5}]
    assert rows[0].asks == [{"price": 0.12, "amount": 3.7}]
    assert rows[0].bid_iv == 54.1
    assert rows[0].ask_iv == 55.2
    assert rows[0].delta == 0.42
    assert rows[0].volume_usd == 1000.0
    assert rows[0].exchange_timestamp == datetime(2026, 5, 24, 7, 11, 52, tzinfo=UTC)


def test_option_l2_rejects_non_option_or_missing_timestamp() -> None:
    """Verify malformed option L2 payloads are rejected structurally."""

    rows, errors = normalize_option_l2_snapshot_rows(
        {
            "BTC-PERPETUAL": {"instrument_name": "BTC-PERPETUAL", "timestamp": 1_779_606_712_000},
            "BTC-30JUN26-120000-C": {"instrument_name": "BTC-30JUN26-120000-C"},
        },
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
        depth=20,
        fetch_durations_s={},
    )

    assert rows == []
    assert len(errors) == 2
