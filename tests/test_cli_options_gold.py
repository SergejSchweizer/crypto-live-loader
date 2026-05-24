"""Tests for options gold CLI command behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from api import cli
from api.constants import OPTIONS_GOLD_BUILDER_COMMAND
from ingestion.config import Config


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config: Config = {
        "http": {"timeout_s": 8, "max_retries": 2, "retry_backoff_s": 1},
        "runtime": {"log_dir": str(tmp_path), "log_rotation_days": 7, "log_backup_count": 0},
        "ingestion": {
            "exchange": "deribit",
            "symbols": ["BTC", "ETH"],
            "levels": 50,
            "snapshot_count": 5,
            "poll_interval_s": 10,
            "max_runtime_s": 50,
            "save_parquet_lake": False,
            "lake_root": "lake/bronze",
            "silver_lake_root": "lake/silver",
            "gold_lake_root": "lake/gold",
            "json_output": True,
            "options": {
                "enabled": True,
                "currencies": ["BTC", "ETH", "SOL"],
                "fetch_concurrency": 3,
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_get_book_summary_by_currency",
                "schema_version": "v1",
                "json_output": True,
            },
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def test_options_gold_builder_outputs_artifact_files(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[dict[str, object]] = []

    def fake_transform_option_silver_to_gold(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["/tmp/lake/gold/options/2026-05.parquet"]

    monkeypatch.setattr(cli, "transform_option_silver_to_gold", fake_transform_option_silver_to_gold)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTIONS_GOLD_BUILDER_COMMAND,
            "--silver-lake-root",
            "custom/silver",
            "--gold-lake-root",
            "custom/gold",
            "--no-plot",
            "--manifest",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["command"] == OPTIONS_GOLD_BUILDER_COMMAND
    assert output["artifact_files"] == ["/tmp/lake/gold/options/2026-05.parquet"]
    assert calls == [
        {
            "silver_lake_root": "custom/silver",
            "gold_lake_root": "custom/gold",
            "plot": False,
            "manifest": True,
        }
    ]
