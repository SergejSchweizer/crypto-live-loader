"""Tests for options Silver-to-Gold surface transformations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.options_gold import (
    option_gold_transform_state_path,
    option_surface_m1_from_silver,
    transform_option_silver_to_gold,
)
from ingestion.options_silver import save_silver_option_chain_features


def _sample_silver_options_frame() -> pl.DataFrame:
    ts = datetime(2026, 5, 24, 7, 15, 10, tzinfo=UTC)
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "option_chain_features_1m",
            "ts_snapshot": ts,
            "exchange": "deribit",
            "currency": "BTC",
            "instrument_type": "option",
            "source": "rest_get_book_summary_by_currency",
            "run_id": "r",
            "month": "2026-05",
            "instrument_name": "BTC-30JUN26-120000-C",
            "expiry_date": datetime(2026, 6, 30, tzinfo=UTC).date(),
            "expiry_timestamp": datetime(2026, 6, 30, tzinfo=UTC),
            "strike": 120000.0,
            "option_type": "call",
            "days_to_expiry": 37.0,
            "tau_years": 37.0 / 365.0,
            "underlying_price": 75000.0,
            "moneyness": 0.625,
            "log_moneyness": -0.47,
            "bid_price": 0.01,
            "ask_price": 0.02,
            "mid_price": 0.015,
            "mark_price": 0.016,
            "mark_iv": 42.0,
            "interest_rate": 0.0,
            "open_interest": 10.0,
            "volume": 2.0,
            "volume_usd": 1500.0,
            "spread": 0.01,
            "spread_bps": 1.3,
            "is_atm_candidate": False,
            "is_valid_for_surface": True,
            "quality_flags": [],
        },
        {
            "schema_version": "v1",
            "dataset_type": "option_chain_features_1m",
            "ts_snapshot": ts,
            "exchange": "deribit",
            "currency": "BTC",
            "instrument_type": "option",
            "source": "rest_get_book_summary_by_currency",
            "run_id": "r",
            "month": "2026-05",
            "instrument_name": "BTC-30JUN26-70000-P",
            "expiry_date": datetime(2026, 6, 30, tzinfo=UTC).date(),
            "expiry_timestamp": datetime(2026, 6, 30, tzinfo=UTC),
            "strike": 70000.0,
            "option_type": "put",
            "days_to_expiry": 37.0,
            "tau_years": 37.0 / 365.0,
            "underlying_price": 75000.0,
            "moneyness": 1.07,
            "log_moneyness": 0.07,
            "bid_price": 0.03,
            "ask_price": 0.04,
            "mid_price": 0.035,
            "mark_price": 0.036,
            "mark_iv": 48.0,
            "interest_rate": 0.0,
            "open_interest": 12.0,
            "volume": 3.0,
            "volume_usd": 2000.0,
            "spread": 0.01,
            "spread_bps": 1.3,
            "is_atm_candidate": True,
            "is_valid_for_surface": True,
            "quality_flags": [],
        },
    ]
    return pl.DataFrame(rows)


def test_option_surface_m1_from_silver_computes_surface_row() -> None:
    gold = option_surface_m1_from_silver(_sample_silver_options_frame())
    row = gold.row(0, named=True)
    assert row["dataset_type"] == "option_surface_m1"
    assert row["currency"] == "BTC"
    assert row["contract_count"] == 2
    assert row["valid_surface_contract_count"] == 2
    assert row["surface_coverage_ratio"] == 1.0
    assert row["atm_iv"] is not None


def test_transform_option_silver_to_gold_writes_artifacts_and_state(tmp_path: Path) -> None:
    silver_root = tmp_path / "silver"
    gold_root = tmp_path / "gold"
    save_silver_option_chain_features(
        silver=_sample_silver_options_frame(),
        lake_root=str(silver_root),
        plot=False,
        manifest=False,
    )
    first_files = transform_option_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
    )
    second_files = transform_option_silver_to_gold(
        silver_lake_root=str(silver_root),
        gold_lake_root=str(gold_root),
    )
    assert second_files == []
    assert any(file_path.endswith("2026-05.parquet") for file_path in first_files)
    assert any(file_path.endswith("2026-05.json") for file_path in first_files)
    assert option_gold_transform_state_path(str(gold_root)).exists()
    json_file = next(path for path in first_files if path.endswith("2026-05.json"))
    payload = json.loads(Path(json_file).read_text(encoding="utf-8"))
    assert payload["dataset_type"] == "option_surface_m1"
