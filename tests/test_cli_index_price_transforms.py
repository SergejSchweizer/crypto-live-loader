"""Tests for index-price silver/gold CLI transform command behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from api import cli
from api.constants import INDEX_PRICE_GOLD_BUILDER_COMMAND, INDEX_PRICE_SILVER_BUILDER_COMMAND
from ingestion.config import Config


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config: Config = {
        "http": {"timeout_s": 8, "max_retries": 2, "retry_backoff_s": 1},
        "runtime": {"log_dir": str(tmp_path), "log_rotation_days": 7, "log_backup_count": 0},
        "ingestion": {
            "index_price": {
                "symbols": ["btc_usd"],
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_get_index_price",
                "schema_version": "v1",
                "json_output": True,
            },
            "silver_lake_root": "lake/silver",
            "gold_lake_root": "lake/gold",
            "json_output": True,
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def test_cli_index_price_silver_builder_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "transform_index_price_bronze_to_silver", lambda **_: ["/tmp/index_silver.parquet"])
    monkeypatch.setattr(
        "sys.argv",
        ["main.py", INDEX_PRICE_SILVER_BUILDER_COMMAND, "--bronze-lake-root", "b", "--silver-lake-root", "s"],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["command"] == INDEX_PRICE_SILVER_BUILDER_COMMAND
    assert output["artifact_files"] == ["/tmp/index_silver.parquet"]


def test_cli_index_price_gold_builder_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "transform_index_price_silver_to_gold", lambda **_: ["/tmp/index_gold.parquet"])
    monkeypatch.setattr(
        "sys.argv",
        ["main.py", INDEX_PRICE_GOLD_BUILDER_COMMAND, "--silver-lake-root", "s", "--gold-lake-root", "g"],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["command"] == INDEX_PRICE_GOLD_BUILDER_COMMAND
    assert output["artifact_files"] == ["/tmp/index_gold.parquet"]
