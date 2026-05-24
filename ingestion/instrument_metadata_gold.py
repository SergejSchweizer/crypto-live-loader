"""Polars transformations from silver instrument metadata to gold summaries."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.artifact_state import file_fingerprints, load_json_state, write_json_state
from ingestion.incremental import inputs_unchanged

INSTRUMENT_METADATA_GOLD_DATASET_TYPE = "instrument_metadata_daily_summary"
INSTRUMENT_METADATA_GOLD_SCHEMA_VERSION = "v1"
INSTRUMENT_METADATA_GOLD_STATE_FILE_NAME = "_gold_instrument_metadata_transform_state.json"
INSTRUMENT_METADATA_GOLD_NATURAL_KEY = ["exchange", "snapshot_date", "kind", "base_currency"]


def transform_instrument_metadata_silver_to_gold(
    silver_lake_root: str,
    gold_lake_root: str,
    plot: bool = True,
    manifest: bool = True,
    fill_missing_minutes: bool = False,
    fill_policy: str = "neighbor",
) -> list[str]:
    """Transform silver instrument metadata features into daily gold summaries."""

    if fill_policy not in {"neighbor", "hybrid", "kalman"}:
        raise ValueError(f"Unsupported fill policy '{fill_policy}'")

    silver_files = sorted(
        Path(silver_lake_root).glob("dataset_type=instrument_metadata_snapshot_features_daily/**/*.parquet")
    )
    if not silver_files:
        return []

    state_path = Path(gold_lake_root) / INSTRUMENT_METADATA_GOLD_STATE_FILE_NAME
    current_fingerprints = file_fingerprints(silver_files)
    state = load_json_state(state_path)
    previous_fingerprints = state.get("silver_inputs", {})
    transform_settings_unchanged = (
        state.get("plot") == plot
        and state.get("manifest") == manifest
        and state.get("fill_missing_minutes") == fill_missing_minutes
        and state.get("fill_policy") == fill_policy
    )
    if transform_settings_unchanged and inputs_unchanged(previous_fingerprints, current_fingerprints):
        return []

    silver = pl.read_parquet([str(path) for path in silver_files])
    gold = _gold_instrument_metadata_from_silver(silver)
    written_files = _write_gold_instrument_metadata(gold=gold, lake_root=gold_lake_root)
    write_json_state(
        state_path,
        {
            "schema_version": INSTRUMENT_METADATA_GOLD_SCHEMA_VERSION,
            "silver_lake_root": str(Path(silver_lake_root).resolve()),
            "gold_lake_root": str(Path(gold_lake_root).resolve()),
            "silver_inputs": current_fingerprints,
            "plot": plot,
            "manifest": manifest,
            "fill_missing_minutes": fill_missing_minutes,
            "fill_policy": fill_policy,
            "last_written_files": written_files,
        },
    )
    return written_files


def _gold_instrument_metadata_from_silver(silver: pl.DataFrame) -> pl.DataFrame:
    grouped = silver.group_by(["exchange", "snapshot_date", "kind", "base_currency"], maintain_order=True).agg(
        pl.len().alias("instrument_count"),
        pl.col("is_active").fill_null(False).cast(pl.Int64).sum().alias("active_instrument_count"),
        pl.col("is_option").fill_null(False).cast(pl.Int64).sum().alias("option_instrument_count"),
        pl.col("strike").mean().alias("mean_strike"),
    )
    return grouped.with_columns(
        pl.lit(INSTRUMENT_METADATA_GOLD_SCHEMA_VERSION).alias("schema_version"),
        pl.lit(INSTRUMENT_METADATA_GOLD_DATASET_TYPE).alias("dataset_type"),
    ).select(
        "schema_version",
        "dataset_type",
        "exchange",
        "snapshot_date",
        "kind",
        "base_currency",
        "instrument_count",
        "active_instrument_count",
        "option_instrument_count",
        "mean_strike",
    )


def _write_gold_instrument_metadata(gold: pl.DataFrame, lake_root: str) -> list[str]:
    written_files: list[str] = []
    if gold.is_empty():
        return written_files
    for partition in gold.partition_by(["exchange"]):
        exchange = str(partition.row(0, named=True)["exchange"])
        out_dir = Path(lake_root) / f"dataset_type={INSTRUMENT_METADATA_GOLD_DATASET_TYPE}" / f"exchange={exchange}"
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / "data.parquet"
        output = partition
        if file_path.exists():
            output = pl.concat([pl.read_parquet(file_path), partition], how="vertical")
        output = output.unique(subset=INSTRUMENT_METADATA_GOLD_NATURAL_KEY, keep="last").sort(
            ["snapshot_date", "kind", "base_currency"]
        )
        staging = out_dir / f".staging-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}.parquet"
        output.write_parquet(staging)
        staging.replace(file_path)
        written_files.append(str(file_path.resolve()))
    return sorted(written_files)
