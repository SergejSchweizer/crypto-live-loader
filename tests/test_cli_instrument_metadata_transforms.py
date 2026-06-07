"""Tests for instrument-metadata silver/gold CLI transform command behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from api import cli
from api.constants import INSTRUMENT_METADATA_GOLD_BUILDER_COMMAND, INSTRUMENT_METADATA_SILVER_BUILDER_COMMAND
from ingestion.config import Config


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config: Config = {
        "http": {"timeout_s": 8, "max_retries": 2, "retry_backoff_s": 1},
        "runtime": {"log_dir": str(tmp_path), "log_rotation_days": 7, "log_backup_count": 0},
        "ingestion": {
            "instrument_metadata": {
                "currencies": ["BTC"],
                "kind": "option",
                "include_inactive": False,
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_get_instruments",
                "schema_version": "v1",
                "json_output": True,
            },
            "silver_lake_root": "lake/silver",
            "gold_lake_root": "lake/gold",
            "json_output": True,
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def test_cli_instrument_metadata_silver_builder_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "transform_instrument_metadata_bronze_to_silver",
        lambda **_: ["/tmp/instrument_metadata_silver.parquet"],
    )
    monkeypatch.setattr(
        "sys.argv",
        ["main.py", INSTRUMENT_METADATA_SILVER_BUILDER_COMMAND, "--bronze-lake-root", "b", "--silver-lake-root", "s"],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["command"] == INSTRUMENT_METADATA_SILVER_BUILDER_COMMAND
    assert output["artifact_files"] == ["/tmp/instrument_metadata_silver.parquet"]


def test_cli_instrument_metadata_gold_builder_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "transform_instrument_metadata_silver_to_gold",
        lambda **_: ["/tmp/instrument_metadata_gold.parquet"],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            INSTRUMENT_METADATA_GOLD_BUILDER_COMMAND,
            "--silver-lake-root",
            "s",
            "--gold-lake-root",
            "g",
            "--debug",
        ],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["command"] == INSTRUMENT_METADATA_GOLD_BUILDER_COMMAND
    assert output["artifact_files"] == ["/tmp/instrument_metadata_gold.parquet"]
