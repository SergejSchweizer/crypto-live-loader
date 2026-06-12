"""Polars transformations from bronze option snapshots to silver option-chain features."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import polars as pl
import pyarrow.parquet as pq

from ingestion.artifact_io import column_dtype_metadata, timestamp_bounds_iso, write_json_artifact
from ingestion.artifact_state import file_fingerprints, load_json_state, write_json_state
from ingestion.file_lock import locked_output_path
from ingestion.incremental import changed_input_files, inputs_unchanged
from ingestion.polars_parquet_store import upsert_partition_parquet

SILVER_OPTION_DATASET_TYPE = "option_chain_features_1m"
SILVER_OPTION_SCHEMA_VERSION = "v1"
SILVER_OPTION_NATURAL_KEY = ["exchange", "currency", "instrument_name", "ts_snapshot"]
SILVER_OPTION_STATE_FILE_NAME = "_silver_options_transform_state.json"
OPTION_BRONZE_READ_BATCH_SIZE = 240

OptionSilverPartitionKey = tuple[str, str, str, str]


def option_silver_partition_path(lake_root: str, key: OptionSilverPartitionKey) -> Path:
    """Return the monthly silver destination directory for option chain features."""

    exchange, instrument_type, currency, month_partition = key
    return (
        Path(lake_root)
        / f"dataset_type={SILVER_OPTION_DATASET_TYPE}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"currency={currency}"
        / f"month={month_partition}"
    )


def option_silver_transform_state_path(silver_lake_root: str) -> Path:
    """Return the options Silver incremental transform state path."""

    return Path(silver_lake_root) / SILVER_OPTION_STATE_FILE_NAME


def options_bronze_parquet_files(bronze_lake_root: str) -> list[Path]:
    """Return option bronze parquet inputs in deterministic order."""

    root = Path(bronze_lake_root)
    return sorted(root.glob("dataset_type=options_ticker_snapshot_1m/**/*.parquet"))


def transform_option_bronze_to_silver(
    bronze_lake_root: str,
    silver_lake_root: str,
    plot: bool = True,
    manifest: bool = True,
) -> list[str]:
    """Transform bronze option snapshots into monthly silver option chain partitions."""

    bronze_files = options_bronze_parquet_files(bronze_lake_root)
    if not bronze_files:
        return []

    state_path = option_silver_transform_state_path(silver_lake_root)
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

    all_groups = _option_bronze_partition_groups(bronze_files)
    changed_groups = _option_bronze_partition_groups(changed_files)
    written_files: set[str] = set()
    for key in changed_groups:
        written_files.update(
            _replace_silver_option_partition_from_bronze_files(
                bronze_files=all_groups[key],
                lake_root=silver_lake_root,
                plot=False,
                manifest=False,
            )
        )
    written_files = _refresh_silver_option_artifacts(written_files, plot=plot, manifest=manifest)
    write_json_state(
        state_path,
        {
            "schema_version": SILVER_OPTION_SCHEMA_VERSION,
            "bronze_lake_root": str(Path(bronze_lake_root).resolve()),
            "silver_lake_root": str(Path(silver_lake_root).resolve()),
            "bronze_inputs": current_fingerprints,
            "last_changed_inputs": [str(path.resolve()) for path in changed_files],
            "last_written_files": sorted(written_files),
        },
    )
    return sorted(written_files)


def _option_bronze_partition_groups(files: list[Path]) -> dict[tuple[str, str], list[Path]]:
    """Group option bronze inputs by the monthly Silver partition they rebuild."""

    grouped: dict[tuple[str, str], list[Path]] = {}
    for path in files:
        key = (
            _path_partition_value(path, "currency") or "",
            _path_month_partition(path) or "",
        )
        grouped.setdefault(key, []).append(path)
    return {key: sorted(group) for key, group in sorted(grouped.items())}


def _option_bronze_file_groups(files: list[Path]) -> list[list[Path]]:
    """Group changed option inputs into bounded batches for monthly output upserts."""

    grouped: dict[tuple[str, str, str], list[Path]] = {}
    for path in files:
        key = (
            _path_partition_value(path, "currency") or "",
            _path_month_partition(path) or "",
            _path_day_partition(path) or "",
        )
        grouped.setdefault(key, []).append(path)

    batches: list[list[Path]] = []
    for _, group in sorted(grouped.items()):
        sorted_group = sorted(group)
        # Option snapshots are high-cardinality market data; bounded reads prevent an
        # initial backfill from materializing a full currency-month of bronze parquet.
        for start_index in range(0, len(sorted_group), OPTION_BRONZE_READ_BATCH_SIZE):
            batches.append(sorted_group[start_index : start_index + OPTION_BRONZE_READ_BATCH_SIZE])
    return batches


def _path_partition_value(path: Path, name: str, prefix_length: int | None = None) -> str | None:
    prefix = f"{name}="
    for part in path.parts:
        if part.startswith(prefix):
            value = part[len(prefix) :]
            return value[:prefix_length] if prefix_length is not None else value
    return None


def _path_month_partition(path: Path) -> str | None:
    """Return the logical ``YYYY-MM`` month for supported bronze layouts."""

    year_partition = _path_partition_value(path, "year")
    month_partition = _path_partition_value(path, "month")
    if year_partition and month_partition and len(month_partition) == 2:
        return f"{year_partition}-{month_partition}"
    if month_partition and len(month_partition) == 7:
        return month_partition
    return _path_partition_value(path, "date", prefix_length=7)


def _path_day_partition(path: Path) -> str | None:
    """Return a stable daily grouping key for supported bronze layouts."""

    year_partition = _path_partition_value(path, "year")
    month_partition = _path_partition_value(path, "month")
    date_partition = _path_partition_value(path, "date")
    if year_partition and month_partition and date_partition and len(month_partition) == 2 and len(date_partition) == 2:
        return f"{year_partition}-{month_partition}-{date_partition}"
    return date_partition


def option_bronze_parquet_files(bronze_lake_root: str) -> list[Path]:
    """Backward-compatible alias for options bronze parquet discovery."""

    return options_bronze_parquet_files(bronze_lake_root)


def option_chain_features_from_bronze(bronze: pl.DataFrame) -> pl.DataFrame:
    """Build one silver option-chain row per bronze option snapshot row."""

    return cast(  # type: ignore[redundant-cast]  # ty sees LazyFrame.collect as DataFrame | InProcessQuery.
        pl.DataFrame,
        _option_chain_features_from_bronze_lazy(bronze.lazy()).collect(),
    )


def _option_chain_features_from_bronze_lazy(bronze: pl.LazyFrame) -> pl.LazyFrame:
    """Build lazy silver option-chain features without materializing bronze inputs first."""

    expiry_date_expr = (
        pl.col("instrument_name")
        .str.extract(r"^[^-]+-([0-9]{1,2}[A-Z]{3}[0-9]{2})-", group_index=1)
        .str.to_date(format="%d%b%y", strict=False)
    )
    strike_expr = pl.col("instrument_name").str.extract(
        r"^[^-]+-[0-9]{1,2}[A-Z]{3}[0-9]{2}-([0-9]+(?:\.[0-9]+)?)-[CP]$",
        group_index=1,
    )
    option_type_raw = pl.col("instrument_name").str.extract(r"-([CP])$", group_index=1)
    is_parsed = expiry_date_expr.is_not_null() & strike_expr.is_not_null() & option_type_raw.is_not_null()
    spread_expr = (
        pl.when(pl.col("bid_price").is_not_null() & pl.col("ask_price").is_not_null())
        .then(pl.col("ask_price") - pl.col("bid_price"))
        .otherwise(None)
    )

    enriched = (
        bronze.with_columns(
            pl.lit(SILVER_OPTION_SCHEMA_VERSION).alias("schema_version"),
            pl.lit(SILVER_OPTION_DATASET_TYPE).alias("dataset_type"),
            pl.col("snapshot_time").cast(pl.Datetime("us")).alias("ts_snapshot"),
            pl.col("snapshot_time").dt.strftime("%Y-%m").alias("month"),
            expiry_date_expr.alias("expiry_date"),
            strike_expr.cast(pl.Float64, strict=False).alias("strike"),
            pl.when(option_type_raw == "C")
            .then(pl.lit("call"))
            .when(option_type_raw == "P")
            .then(pl.lit("put"))
            .otherwise(None)
            .alias("option_type"),
            is_parsed.alias("_is_parsed"),
            spread_expr.alias("spread"),
        )
        .with_columns(
            pl.col("expiry_date").cast(pl.Datetime("us")).alias("expiry_timestamp"),
            (
                (pl.col("expiry_date").cast(pl.Datetime("us")) - pl.col("ts_snapshot")).dt.total_seconds() / 86_400.0
            ).alias("days_to_expiry"),
        )
        .with_columns(
            (pl.col("days_to_expiry") / 365.0).alias("tau_years"),
            pl.when((pl.col("underlying_price") > 0) & (pl.col("strike") > 0))
            .then(pl.col("underlying_price") / pl.col("strike"))
            .otherwise(None)
            .alias("moneyness"),
            pl.when((pl.col("underlying_price") > 0) & (pl.col("strike") > 0))
            .then((pl.col("underlying_price") / pl.col("strike")).log())
            .otherwise(None)
            .alias("log_moneyness"),
            pl.when(
                (pl.col("bid_price").is_not_null())
                & (pl.col("ask_price").is_not_null())
                & (pl.col("underlying_price") > 0)
            )
            .then((pl.col("spread") / pl.col("underlying_price")) * 10_000)
            .otherwise(None)
            .alias("spread_bps"),
        )
        .with_columns(
            _quality_flags_expr().alias("quality_flags"),
        )
        .with_columns(
            pl.col("quality_flags").list.len().eq(0).alias("is_valid_for_surface"),
            pl.when((pl.col("moneyness").is_not_null()) & (pl.col("moneyness") > 0.95) & (pl.col("moneyness") < 1.05))
            .then(True)
            .otherwise(False)
            .alias("is_atm_candidate"),
        )
    )

    return enriched.select(
        "schema_version",
        "dataset_type",
        "ts_snapshot",
        "exchange",
        "currency",
        "instrument_type",
        "source",
        "run_id",
        "month",
        "instrument_name",
        "expiry_date",
        "expiry_timestamp",
        "strike",
        "option_type",
        "days_to_expiry",
        "tau_years",
        "underlying_price",
        "moneyness",
        "log_moneyness",
        "bid_price",
        "ask_price",
        "mid_price",
        "mark_price",
        "mark_iv",
        "interest_rate",
        "open_interest",
        "volume",
        "volume_usd",
        "spread",
        "spread_bps",
        "is_atm_candidate",
        "is_valid_for_surface",
        "quality_flags",
    )


def _replace_silver_option_partition_from_bronze_files(
    bronze_files: list[Path],
    lake_root: str,
    plot: bool,
    manifest: bool,
) -> list[str]:
    """Replace one monthly Silver options partition from a complete bronze file set."""

    if not bronze_files:
        return []

    first_path = bronze_files[0]
    key = (
        _path_partition_value(first_path, "exchange") or "",
        _path_partition_value(first_path, "instrument_type") or "",
        _path_partition_value(first_path, "currency") or "",
        _path_month_partition(first_path) or "",
    )
    part_dir = option_silver_partition_path(lake_root=lake_root, key=key)
    month_partition = key[3]
    file_path = part_dir / f"{month_partition}.parquet"
    metadata_path = part_dir / f"{month_partition}.json"
    plot_path = part_dir / f"{month_partition}.png"
    staging_path = part_dir / f".staging-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}.parquet"

    part_dir.mkdir(parents=True, exist_ok=True)
    with locked_output_path(file_path):
        writer: Any | None = None
        parquet_writer_factory: Any = pq.ParquetWriter
        try:
            for batch in _option_bronze_file_groups(bronze_files):
                bronze = pl.read_parquet([str(path) for path in batch])
                # Full backfills can exceed host memory when materialized as one
                # currency-month; batch-level sorting keeps row groups deterministic
                # while preserving one monthly artifact per Silver partition.
                silver = (
                    option_chain_features_from_bronze(bronze)
                    .unique(subset=SILVER_OPTION_NATURAL_KEY, keep="last")
                    .sort("ts_snapshot")
                )
                if silver.is_empty():
                    continue
                table = silver.to_arrow()
                if writer is None:
                    writer = parquet_writer_factory(staging_path, table.schema, compression="zstd")
                assert writer is not None
                writer.write_table(table)
            if writer is not None:
                writer.close()
                writer = None
                staging_path.replace(file_path)
        finally:
            if writer is not None:
                writer.close()
            try:
                staging_path.unlink()
            except FileNotFoundError:
                pass

    written_files = [str(file_path.resolve())]
    if manifest or plot:
        silver = pl.read_parquet(file_path)
        if manifest:
            write_json_artifact(metadata_path, silver_option_artifact_metadata(silver))
            written_files.append(str(metadata_path.resolve()))
        if plot:
            write_silver_option_profile_png(silver, plot_path)
            written_files.append(str(plot_path.resolve()))
    return sorted(written_files)


def save_silver_option_chain_features(
    silver: pl.DataFrame,
    lake_root: str,
    plot: bool = True,
    manifest: bool = True,
) -> list[str]:
    """Persist silver option-chain rows and monthly artifact files idempotently."""

    written_files: list[str] = []
    if silver.is_empty():
        return written_files

    for partition in silver.partition_by(["exchange", "instrument_type", "currency", "month"]):
        first = partition.row(0, named=True)
        key = (
            str(first["exchange"]),
            str(first["instrument_type"]),
            str(first["currency"]),
            str(first["month"]),
        )
        part_dir = option_silver_partition_path(lake_root=lake_root, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        month_partition = key[3]
        file_path = part_dir / f"{month_partition}.parquet"
        metadata_path = part_dir / f"{month_partition}.json"
        plot_path = part_dir / f"{month_partition}.png"
        output = upsert_partition_parquet(
            file_path=file_path,
            partition=partition,
            natural_key=SILVER_OPTION_NATURAL_KEY,
            sort_by="ts_snapshot",
        )
        written_files.append(str(file_path.resolve()))
        if manifest:
            write_json_artifact(metadata_path, silver_option_artifact_metadata(output))
            written_files.append(str(metadata_path.resolve()))
        if plot:
            write_silver_option_profile_png(output, plot_path)
            written_files.append(str(plot_path.resolve()))
    return sorted(written_files)


def _refresh_silver_option_artifacts(parquet_files: set[str], plot: bool, manifest: bool) -> set[str]:
    """Refresh optional artifacts once for each touched Silver options partition."""

    written_files = set(parquet_files)
    if not plot and not manifest:
        return written_files

    for parquet_file in sorted(parquet_files):
        parquet_path = Path(parquet_file)
        silver = pl.read_parquet(parquet_path)
        if manifest:
            metadata_path = parquet_path.with_suffix(".json")
            write_json_artifact(metadata_path, silver_option_artifact_metadata(silver))
            written_files.add(str(metadata_path.resolve()))
        if plot:
            plot_path = parquet_path.with_suffix(".png")
            write_silver_option_profile_png(silver, plot_path)
            written_files.add(str(plot_path.resolve()))
    return written_files


def silver_option_artifact_metadata(silver: pl.DataFrame) -> dict[str, Any]:
    """Build JSON metadata for one monthly silver options artifact."""

    ts_min, ts_max = timestamp_bounds_iso(silver, "ts_snapshot")
    return {
        "dataset_type": SILVER_OPTION_DATASET_TYPE,
        "schema_version": SILVER_OPTION_SCHEMA_VERSION,
        "build_timestamp_utc": datetime.now(UTC).isoformat(),
        "row_count": silver.height,
        "column_count": len(silver.columns),
        "timestamp_min": ts_min,
        "timestamp_max": ts_max,
        "currencies": (
            sorted(str(item) for item in silver["currency"].unique().to_list()) if "currency" in silver.columns else []
        ),
        "columns": column_dtype_metadata(silver),
    }


def write_silver_option_profile_png(silver: pl.DataFrame, path: Path) -> None:
    """Write a compact PNG profile for monthly Silver options artifacts."""

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    numeric_cols = [
        "mark_iv",
        "underlying_price",
        "spread_bps",
        "open_interest",
        "volume",
    ]
    present_cols = [column for column in numeric_cols if column in silver.columns]
    if not present_cols:
        path.touch()
        return

    fig, axes = plt.subplots(len(present_cols), 1, figsize=(12, max(4, len(present_cols) * 1.8)), squeeze=False)
    fig.patch.set_facecolor("#111217")
    ts_values = silver["ts_snapshot"].to_list()
    for index, column in enumerate(present_cols):
        ax = axes[index][0]
        values = silver[column].to_list()
        clean = [value for value in values if isinstance(value, int | float) and math.isfinite(float(value))]
        ax.set_facecolor("#161922")
        ax.plot(ts_values, values, color="#88c0d0", linewidth=0.8)
        ax.set_title(f"{column} (n={len(clean)})", color="#eceff4", fontsize=9, loc="left")
        ax.tick_params(colors="#d8dee9", labelsize=7)
        for spine in ax.spines.values():
            spine.set_color("#3b4252")
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120, facecolor=fig.get_facecolor())
    plt.close(fig)


def _quality_flags_expr() -> pl.Expr:
    return pl.concat_list(
        _flag_expr(pl.col("bid_price").is_null(), "missing_bid"),
        _flag_expr(pl.col("ask_price").is_null(), "missing_ask"),
        _flag_expr(pl.col("mid_price").is_null(), "missing_mid"),
        _flag_expr(pl.col("mark_iv").is_null(), "missing_mark_iv"),
        _flag_expr(pl.col("underlying_price").is_null(), "missing_underlying_price"),
        _flag_expr(pl.col("spread").is_not_null() & (pl.col("spread") < 0), "negative_spread"),
        _flag_expr(
            pl.col("underlying_price").is_not_null() & (pl.col("underlying_price") <= 0),
            "zero_or_negative_underlying",
        ),
        _flag_expr(pl.col("tau_years").is_null() | (pl.col("tau_years") <= 0), "expired_or_invalid_tau"),
        _flag_expr(pl.col("open_interest").fill_null(0.0) <= 0, "illiquid_zero_oi"),
        _flag_expr(pl.col("spread_bps").is_not_null() & (pl.col("spread_bps") > 100.0), "wide_spread"),
        _flag_expr(pl.col("_is_parsed").not_(), "invalid_instrument_name"),
    ).list.drop_nulls()


def _flag_expr(condition: pl.Expr, value: str) -> pl.Expr:
    return pl.when(condition).then(pl.lit(value)).otherwise(None)
