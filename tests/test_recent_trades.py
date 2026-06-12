"""Tests for recent trade tape normalization."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.recent_trades import normalize_recent_trade_rows, overlap_start_timestamp_ms


def test_recent_trade_normalizes_option_iv_and_signed_flow() -> None:
    """Verify option trade tape fields and signed taker flow are preserved."""

    snapshot_time = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    rows, errors = normalize_recent_trade_rows(
        [
            {
                "trade_id": "BTC-123",
                "trade_seq": 123,
                "instrument_name": "BTC-30JUN26-120000-C",
                "timestamp": 1_779_606_712_000,
                "price": 0.12,
                "amount": 3.5,
                "direction": "sell",
                "tick_direction": 2,
                "mark_price": 0.121,
                "index_price": 68_000.0,
                "iv": 54.2,
                "block_trade_id": "block-1",
            }
        ],
        requested_currency="BTC",
        source_currency="BTC",
        kind="option",
        run_id="run",
        snapshot_time=snapshot_time,
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert errors == []
    assert rows[0].dataset_type == "recent_trade_snapshot_1m"
    assert rows[0].instrument_type == "option"
    assert rows[0].exchange_timestamp == datetime(2026, 5, 24, 7, 11, 52, tzinfo=UTC)
    assert rows[0].iv == 54.2
    assert rows[0].signed_amount == -3.5
    assert rows[0].notional == 0.42


def test_recent_trade_identifies_perpetual_future_kind() -> None:
    """Verify future-kind perpetual instruments get a perp instrument type."""

    rows, errors = normalize_recent_trade_rows(
        [
            {
                "trade_id": "SOL-1",
                "instrument_name": "SOL_USDC-PERPETUAL",
                "timestamp": 1_779_606_712_000,
                "price": 150.0,
                "amount": 2,
                "direction": "buy",
            }
        ],
        requested_currency="SOL",
        source_currency="USDC",
        kind="future",
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert errors == []
    assert rows[0].currency == "SOL"
    assert rows[0].instrument_type == "perp"
    assert rows[0].signed_amount == 2.0


def test_recent_trade_rejects_missing_key_fields() -> None:
    """Rows missing timestamp, instrument, or trade id should be rejected."""

    rows, errors = normalize_recent_trade_rows(
        [{"instrument_name": "BTC-PERPETUAL", "trade_id": "1"}],
        requested_currency="BTC",
        source_currency="BTC",
        kind="future",
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert rows == []
    assert len(errors) == 1


def test_overlap_start_timestamp_ms_uses_snapshot_time() -> None:
    """Verify overlap windows are computed from the run snapshot minute."""

    result = overlap_start_timestamp_ms(datetime(2026, 5, 24, 7, 15, tzinfo=UTC), 90)

    assert result == 1_779_606_810_000
