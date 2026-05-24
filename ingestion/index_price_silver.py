"""Polars transformations from bronze index prices to silver features."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.artifact_state import file_fingerprints, load_json_state, write_json_state
from ingestion.incremental import changed_input_files, inputs_unchanged

INDEX_PRICE_SILVER_DATASET_TYPE = "index_price_snapshot_features_1m"
INDEX_PRICE_SILVER_SCHEMA_VERSION = "v1"
INDEX_PRICE_SILVER_STATE_FILE_NAME = "_silver_index_price_transform_state.json"
INDEX_PRICE_SILVER_NATURAL_KEY = ["exchange", "index_name", "ts_event", "source"]


def transform_index_price_bronze_to_silver(
    bronze_lake_root: str,
    silver_lake_root: str,
) -> list[str]:
    """Transform bronze index price snapshots into monthly silver feature partitions."""

    bronze_files = sorted(Path(bronze_lake_root).glob("dataset_type=index_price_snapshot_1m/**/*.parquet"))
    if not bronze_files:
        return []

    state_path = Path(silver_lake_root) / INDEX_PRICE_SILVER_STATE_FILE_NAME
    current_fingerprints = file_fingerprints(bronze_files)
    state = load_json_state(state_path)
    previous_fingerprints = state.get("bronze_inputs", {})
    if inputs_unchanged(previous_fingerprints, current_fingerprints):
        return []

    changed_files = changed_input_files(
        files=bronze_files,
        previous_fingerprints=previous_fingerprints,
        current_fingerprints=current_fingerprints,
    )
    bronze = pl.read_parquet([str(path) for path in changed_files])
    silver = _silver_index_price_features_from_bronze(bronze)
    written_files = _save_silver_index_price_features(silver=silver, lake_root=silver_lake_root)
    write_json_state(
        state_path,
        {
            "schema_version": INDEX_PRICE_SILVER_SCHEMA_VERSION,
            "bronze_lake_root": str(Path(bronze_lake_root).resolve()),
            "silver_lake_root": str(Path(silver_lake_root).resolve()),
            "bronze_inputs": current_fingerprints,
            "last_changed_inputs": [str(path.resolve()) for path in changed_files],
            "last_written_files": written_files,
        },
    )
    return written_files


def _silver_index_price_features_from_bronze(bronze: pl.DataFrame) -> pl.DataFrame:
    prepared = (
        bronze.sort(["exchange", "index_name", "event_time"])
        .with_columns(
            pl.lit(INDEX_PRICE_SILVER_SCHEMA_VERSION).alias("schema_version"),
            pl.lit(INDEX_PRICE_SILVER_DATASET_TYPE).alias("dataset_type"),
            pl.col("event_time").alias("ts_event"),
            pl.col("ingested_at").alias("ts_received"),
            pl.col("event_time").dt.strftime("%Y-%m").alias("month"),
        )
        .with_columns(
            pl.col("price").shift(1).over(["exchange", "index_name"]).alias("price_prev"),
        )
        .with_columns(
            (pl.col("price") - pl.col("price_prev")).alias("price_delta"),
            pl.when((pl.col("price_prev") > 0) & pl.col("price").is_not_null())
            .then((pl.col("price") / pl.col("price_prev")).log())
            .otherwise(None)
            .alias("log_return_1m"),
        )
    )
    return prepared.select(
        "schema_version",
        "dataset_type",
        "exchange",
        "source",
        "index_name",
        "month",
        "ts_event",
        "ts_received",
        "price",
        "price_prev",
        "price_delta",
        "log_return_1m",
        "run_id",
    )


def _save_silver_index_price_features(silver: pl.DataFrame, lake_root: str) -> list[str]:
    written_files: list[str] = []
    if silver.is_empty():
        return written_files
    for partition in silver.partition_by(["exchange", "index_name", "month"]):
        first = partition.row(0, named=True)
        exchange = str(first["exchange"])
        index_name = str(first["index_name"])
        month = str(first["month"])
        out_dir = (
            Path(lake_root)
            / f"dataset_type={INDEX_PRICE_SILVER_DATASET_TYPE}"
            / f"exchange={exchange}"
            / f"index_name={index_name}"
            / f"month={month}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"{month}.parquet"
        output = partition
        if file_path.exists():
            output = pl.concat([pl.read_parquet(file_path), partition], how="vertical")
        output = output.unique(subset=INDEX_PRICE_SILVER_NATURAL_KEY, keep="last").sort("ts_event")
        staging = out_dir / f".staging-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}.parquet"
        output.write_parquet(staging)
        staging.replace(file_path)
        written_files.append(str(file_path.resolve()))
    return sorted(written_files)
