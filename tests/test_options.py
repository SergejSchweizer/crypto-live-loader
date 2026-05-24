"""Tests for option ticker normalization rules."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.options import normalize_option_ticker_rows


def test_option_row_normalization() -> None:
    """Deribit rows should normalize into typed bronze option records."""

    rows, errors = normalize_option_ticker_rows(
        [
            {
                "instrument_name": "BTC-30JUN26-120000-C",
                "base_currency": "BTC",
                "quote_currency": "BTC",
                "creation_timestamp": 1_779_606_711_643,
                "mark_price": 0.0023,
                "underlying_price": 76839.1,
            }
        ],
        requested_currency="BTC",
        source_currency="BTC",
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert errors == []
    assert rows[0].currency == "BTC"
    assert rows[0].instrument_name == "BTC-30JUN26-120000-C"
    assert rows[0].exchange_creation_time is not None


def test_missing_optional_fields_allowed() -> None:
    """Illiquid rows with null optional fields should still be persisted."""

    rows, errors = normalize_option_ticker_rows(
        [{"instrument_name": "ETH-30JUN26-5000-P"}],
        requested_currency="ETH",
        source_currency="ETH",
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert errors == []
    assert rows[0].bid_price is None
    assert rows[0].mark_iv is None


def test_invalid_missing_instrument_name_rejected() -> None:
    """Rows without valid instrument_name should be rejected structurally."""

    rows, errors = normalize_option_ticker_rows(
        [{"bid_price": 0.1}],
        requested_currency="BTC",
        source_currency="BTC",
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert rows == []
    assert len(errors) == 1
