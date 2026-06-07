"""Polars transformations from silver index prices to gold minute aggregates."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ingestion.artifact_state import file_fingerprints, load_json_state, write_json_state
from ingestion.incremental import inputs_unchanged
from ingestion.polars_parquet_store import is_committed_parquet_path, upsert_partition_parquet

INDEX_PRICE_GOLD_DATASET_TYPE = "index_price_m1_features"
INDEX_PRICE_GOLD_SCHEMA_VERSION = "v1"
INDEX_PRICE_GOLD_STATE_FILE_NAME = "_gold_index_price_transform_state.json"
INDEX_PRICE_GOLD_NATURAL_KEY = ["exchange", "index_name", "ts_minute"]


def transform_index_price_silver_to_gold(
    silver_lake_root: str,
    gold_lake_root: str,
    plot: bool = True,
    manifest: bool = True,
    fill_missing_minutes: bool = False,
    fill_policy: str = "neighbor",
) -> list[str]:
    """Transform silver index price features into gold minute aggregates."""

    if fill_policy not in {"neighbor", "hybrid", "kalman"}:
        raise ValueError(f"Unsupported fill policy '{fill_policy}'")

    silver_files = sorted(
        path
        for path in Path(silver_lake_root).glob("dataset_type=index_price_snapshot_features_1m/**/*.parquet")
        if is_committed_parquet_path(path)
    )
    if not silver_files:
        return []

    state_path = Path(gold_lake_root) / INDEX_PRICE_GOLD_STATE_FILE_NAME
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
    gold = _gold_index_price_from_silver(silver)
    written_files = _write_gold_index_price(gold=gold, lake_root=gold_lake_root, plot=plot)
    write_json_state(
        state_path,
        {
            "schema_version": INDEX_PRICE_GOLD_SCHEMA_VERSION,
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


def _gold_index_price_from_silver(silver: pl.DataFrame) -> pl.DataFrame:
    prepared = silver.with_columns(
        pl.col("ts_event").dt.truncate("1m").alias("ts_minute"),
    )
    grouped = prepared.group_by(["exchange", "index_name", "ts_minute"], maintain_order=True).agg(
        pl.col("price").first().alias("price_open"),
        pl.col("price").max().alias("price_high"),
        pl.col("price").min().alias("price_low"),
        pl.col("price").last().alias("price_close"),
        pl.col("price").mean().alias("price_mean"),
        pl.col("log_return_1m").mean().alias("log_return_1m_mean"),
        pl.len().alias("snapshot_count"),
    )
    return grouped.with_columns(
        pl.lit(INDEX_PRICE_GOLD_SCHEMA_VERSION).alias("schema_version"),
        pl.lit(INDEX_PRICE_GOLD_DATASET_TYPE).alias("dataset_type"),
    ).select(
        "schema_version",
        "dataset_type",
        "exchange",
        "index_name",
        "ts_minute",
        "snapshot_count",
        "price_open",
        "price_high",
        "price_low",
        "price_close",
        "price_mean",
        "log_return_1m_mean",
    )


def _write_gold_index_price(gold: pl.DataFrame, lake_root: str, plot: bool = True) -> list[str]:
    written_files: list[str] = []
    if gold.is_empty():
        return written_files
    for partition in gold.partition_by(["exchange", "index_name"]):
        first = partition.row(0, named=True)
        exchange = str(first["exchange"])
        index_name = str(first["index_name"])
        out_dir = (
            Path(lake_root)
            / f"dataset_type={INDEX_PRICE_GOLD_DATASET_TYPE}"
            / f"exchange={exchange}"
            / f"index_name={index_name}"
            / "timeframe=1m"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / "data.parquet"
        upsert_partition_parquet(
            file_path=file_path,
            partition=partition,
            natural_key=INDEX_PRICE_GOLD_NATURAL_KEY,
            sort_by="ts_minute",
        )
        written_files.append(str(file_path.resolve()))
        if plot:
            plot_path = out_dir / "data.png"
            _write_gold_index_price_plot(partition.sort("ts_minute"), plot_path)
            written_files.append(str(plot_path.resolve()))
    return sorted(written_files)


def _write_gold_index_price_plot(gold: pl.DataFrame, path: Path) -> None:
    """Write a compact PNG profile for one Gold index-price partition."""

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    columns = ["price_close", "price_mean", "log_return_1m_mean", "snapshot_count"]
    present = [column for column in columns if column in gold.columns]
    if not present:
        path.touch()
        return

    fig, axes = plt.subplots(len(present), 1, figsize=(12, max(4, len(present) * 1.8)), squeeze=False)
    fig.patch.set_facecolor("#111217")
    ts_values = gold["ts_minute"].to_list()
    for index, column in enumerate(present):
        axis = axes[index][0]
        axis.set_facecolor("#161922")
        axis.plot(ts_values, gold[column].to_list(), color="#88c0d0", linewidth=0.9)
        axis.set_title(column, color="#eceff4", fontsize=9, loc="left")
        axis.tick_params(colors="#d8dee9", labelsize=7)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120, facecolor=fig.get_facecolor())
    plt.close(fig)
