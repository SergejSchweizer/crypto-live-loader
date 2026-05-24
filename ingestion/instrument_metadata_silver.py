"""Polars transformations from bronze instrument metadata to silver features."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.artifact_state import file_fingerprints, load_json_state, write_json_state
from ingestion.incremental import inputs_unchanged

INSTRUMENT_METADATA_SILVER_DATASET_TYPE = "instrument_metadata_snapshot_features_daily"
INSTRUMENT_METADATA_SILVER_SCHEMA_VERSION = "v1"
INSTRUMENT_METADATA_SILVER_STATE_FILE_NAME = "_silver_instrument_metadata_transform_state.json"
INSTRUMENT_METADATA_SILVER_NATURAL_KEY = ["exchange", "instrument_name", "snapshot_date"]


def transform_instrument_metadata_bronze_to_silver(
    bronze_lake_root: str,
    silver_lake_root: str,
) -> list[str]:
    """Transform bronze instrument metadata snapshots into daily silver features."""

    bronze_files = sorted(Path(bronze_lake_root).glob("dataset_type=instrument_metadata_snapshot_daily/**/*.parquet"))
    if not bronze_files:
        return []

    state_path = Path(silver_lake_root) / INSTRUMENT_METADATA_SILVER_STATE_FILE_NAME
    current_fingerprints = file_fingerprints(bronze_files)
    state = load_json_state(state_path)
    previous_fingerprints = state.get("bronze_inputs", {})
    if inputs_unchanged(previous_fingerprints, current_fingerprints):
        return []

    bronze = pl.read_parquet([str(path) for path in bronze_files])
    silver = _silver_instrument_metadata_from_bronze(bronze)
    written_files = _write_silver_instrument_metadata(silver=silver, lake_root=silver_lake_root)
    write_json_state(
        state_path,
        {
            "schema_version": INSTRUMENT_METADATA_SILVER_SCHEMA_VERSION,
            "bronze_lake_root": str(Path(bronze_lake_root).resolve()),
            "silver_lake_root": str(Path(silver_lake_root).resolve()),
            "bronze_inputs": current_fingerprints,
            "last_written_files": written_files,
        },
    )
    return written_files


def _silver_instrument_metadata_from_bronze(bronze: pl.DataFrame) -> pl.DataFrame:
    prepared = bronze.with_columns(
        pl.lit(INSTRUMENT_METADATA_SILVER_SCHEMA_VERSION).alias("schema_version"),
        pl.lit(INSTRUMENT_METADATA_SILVER_DATASET_TYPE).alias("dataset_type"),
        pl.col("snapshot_date").cast(pl.Date).dt.strftime("%Y-%m").alias("month"),
        pl.when(pl.col("kind") == "option").then(True).otherwise(False).alias("is_option"),
        pl.when(pl.col("expiration_timestamp").is_not_null() & pl.col("snapshot_date").is_not_null())
        .then(
            (
                pl.col("expiration_timestamp").cast(pl.Date).cast(pl.Datetime("us"))
                - pl.col("snapshot_date").cast(pl.Date).cast(pl.Datetime("us"))
            ).dt.total_days()
        )
        .otherwise(None)
        .alias("days_to_expiration"),
    )
    return prepared.select(
        "schema_version",
        "dataset_type",
        "exchange",
        "source",
        "snapshot_date",
        "month",
        "instrument_name",
        "kind",
        "base_currency",
        "quote_currency",
        "settlement_currency",
        "instrument_type",
        "tick_size",
        "contract_size",
        "min_trade_amount",
        "is_active",
        "option_type",
        "strike",
        "is_option",
        "days_to_expiration",
        "run_id",
        "ingested_at",
    )


def _write_silver_instrument_metadata(silver: pl.DataFrame, lake_root: str) -> list[str]:
    written_files: list[str] = []
    if silver.is_empty():
        return written_files
    for partition in silver.partition_by(["exchange", "month"]):
        first = partition.row(0, named=True)
        exchange = str(first["exchange"])
        month = str(first["month"])
        out_dir = (
            Path(lake_root)
            / f"dataset_type={INSTRUMENT_METADATA_SILVER_DATASET_TYPE}"
            / f"exchange={exchange}"
            / f"month={month}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"{month}.parquet"
        output = partition
        if file_path.exists():
            output = pl.concat([pl.read_parquet(file_path), partition], how="vertical")
        output = output.unique(subset=INSTRUMENT_METADATA_SILVER_NATURAL_KEY, keep="last").sort(
            ["snapshot_date", "instrument_name"]
        )
        staging = out_dir / f".staging-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}.parquet"
        output.write_parquet(staging)
        staging.replace(file_path)
        written_files.append(str(file_path.resolve()))
    return sorted(written_files)
