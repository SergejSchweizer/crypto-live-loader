"""Polars transformations from options Silver features to Gold option surface artifacts."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from ingestion.artifact_io import timestamp_bounds_iso, write_json_artifact
from ingestion.artifact_state import file_fingerprints, load_json_state, write_json_state
from ingestion.incremental import inputs_unchanged
from ingestion.polars_parquet_store import upsert_partition_parquet

OPTION_SURFACE_DATASET_TYPE = "option_surface_m1"
OPTION_SURFACE_SCHEMA_VERSION = "v1"
OPTION_SURFACE_STATE_FILE_NAME = "_gold_options_transform_state.json"
OPTION_SURFACE_NATURAL_KEY = ["ts_minute", "currency", "expiry_date"]
OPTION_GOLD_FILL_POLICY_NEIGHBOR = "neighbor"
OPTION_GOLD_FILL_POLICY_HYBRID = "hybrid"
OPTION_GOLD_FILL_POLICY_KALMAN = "kalman"
OPTION_GOLD_FILL_POLICIES = (
    OPTION_GOLD_FILL_POLICY_NEIGHBOR,
    OPTION_GOLD_FILL_POLICY_HYBRID,
    OPTION_GOLD_FILL_POLICY_KALMAN,
)


def option_gold_transform_state_path(gold_lake_root: str) -> Path:
    """Return the options Gold incremental transform state path."""

    return Path(gold_lake_root) / OPTION_SURFACE_STATE_FILE_NAME


def option_silver_parquet_files(silver_lake_root: str) -> list[Path]:
    """Return options Silver parquet inputs in deterministic order."""

    return sorted(Path(silver_lake_root).glob("dataset_type=option_chain_features_1m/**/*.parquet"))


def transform_option_silver_to_gold(
    silver_lake_root: str,
    gold_lake_root: str,
    plot: bool = True,
    manifest: bool = True,
    fill_missing_minutes: bool = False,
    fill_policy: str = OPTION_GOLD_FILL_POLICY_NEIGHBOR,
) -> list[str]:
    """Transform options Silver rows into Gold option surface artifacts."""

    if fill_policy not in OPTION_GOLD_FILL_POLICIES:
        raise ValueError(f"Unsupported fill policy '{fill_policy}'")

    silver_files = option_silver_parquet_files(silver_lake_root)
    if not silver_files:
        return []

    state_path = option_gold_transform_state_path(gold_lake_root)
    current_fingerprints = file_fingerprints(silver_files)
    state = load_json_state(state_path)
    previous_fingerprints = state.get("silver_inputs", {})
    transform_settings_unchanged = (
        state.get("fill_missing_minutes") == fill_missing_minutes and state.get("fill_policy") == fill_policy
    )
    if transform_settings_unchanged and inputs_unchanged(previous_fingerprints, current_fingerprints):
        return []

    silver = pl.read_parquet([str(path) for path in silver_files])
    gold = option_surface_m1_from_silver(silver)
    written_files = write_option_surface_m1_artifacts(
        gold=gold,
        gold_lake_root=gold_lake_root,
        plot=plot,
        manifest=manifest,
    )
    write_json_state(
        state_path,
        {
            "schema_version": OPTION_SURFACE_SCHEMA_VERSION,
            "silver_lake_root": str(Path(silver_lake_root).resolve()),
            "gold_lake_root": str(Path(gold_lake_root).resolve()),
            "silver_inputs": current_fingerprints,
            "fill_missing_minutes": fill_missing_minutes,
            "fill_policy": fill_policy,
            "last_written_files": written_files,
        },
    )
    return written_files


def option_surface_m1_from_silver(silver: pl.DataFrame) -> pl.DataFrame:
    """Aggregate options Silver rows into minute-level option surface features."""

    if silver.is_empty():
        return pl.DataFrame()

    base = silver.with_columns(
        pl.col("ts_snapshot").dt.truncate("1m").alias("ts_minute"),
    )
    groups = base.partition_by(["ts_minute", "currency", "expiry_date"], maintain_order=False)
    rows: list[dict[str, object]] = []
    for frame in groups:
        row = _surface_row_from_group(frame)
        if row is not None:
            rows.append(row)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["currency", "ts_minute", "expiry_date"])


def _surface_row_from_group(frame: pl.DataFrame) -> dict[str, object] | None:
    ts_minute = frame["ts_minute"][0]
    currency = frame["currency"][0]
    expiry_date = frame["expiry_date"][0]
    if not isinstance(ts_minute, datetime) or not isinstance(currency, str):
        return None

    candidates = [item for item in frame.to_dicts() if isinstance(item.get("log_moneyness"), int | float)]
    if not candidates:
        return None
    atm = min(candidates, key=lambda item: abs(float(item["log_moneyness"])))
    calls = [item for item in candidates if item.get("option_type") == "call"]
    puts = [item for item in candidates if item.get("option_type") == "put"]
    near_call = min(calls, key=lambda item: abs(float(item["log_moneyness"]))) if calls else None
    near_put = min(puts, key=lambda item: abs(float(item["log_moneyness"]))) if puts else None
    iv_call = _as_float(near_call.get("mark_iv")) if near_call else None
    iv_put = _as_float(near_put.get("mark_iv")) if near_put else None
    atm_iv = _as_float(atm.get("mark_iv"))
    if atm_iv is None:
        atm_iv = _mean_nullable([iv_call, iv_put])

    call_m = _as_float(near_call.get("log_moneyness")) if near_call else None
    put_m = _as_float(near_put.get("log_moneyness")) if near_put else None
    skew_slope = None
    if iv_call is not None and iv_put is not None and call_m is not None and put_m is not None:
        denom = abs(call_m) + abs(put_m)
        if denom > 0:
            skew_slope = (iv_call - iv_put) / denom

    smile_curvature = None
    if iv_call is not None and iv_put is not None and atm_iv is not None:
        smile_curvature = ((iv_call + iv_put) / 2.0) - atm_iv

    term_days = _as_float(frame["days_to_expiry"].mean())
    contract_count = frame.height
    valid_count = int(frame["is_valid_for_surface"].cast(pl.Int64).sum())
    coverage = float(valid_count / contract_count) if contract_count > 0 else 0.0
    open_interest_sum = _as_float(frame["open_interest"].fill_null(0.0).sum()) or 0.0
    volume_sum = _as_float(frame["volume"].fill_null(0.0).sum()) or 0.0

    return {
        "schema_version": OPTION_SURFACE_SCHEMA_VERSION,
        "dataset_type": OPTION_SURFACE_DATASET_TYPE,
        "ts_minute": ts_minute,
        "month": ts_minute.strftime("%Y-%m"),
        "exchange": "deribit",
        "instrument_type": "option",
        "currency": currency,
        "expiry_date": expiry_date,
        "term_days": term_days,
        "term_bucket": _term_bucket(term_days),
        "atm_iv": atm_iv,
        "atm_strike": _as_float(atm.get("strike")),
        "atm_moneyness": _as_float(atm.get("moneyness")),
        "iv_near_atm_call": iv_call,
        "iv_near_atm_put": iv_put,
        "open_interest_sum": open_interest_sum,
        "volume_sum": volume_sum,
        "contract_count": contract_count,
        "valid_surface_contract_count": valid_count,
        "surface_coverage_ratio": coverage,
        "skew_slope": skew_slope,
        "smile_curvature": smile_curvature,
        "rr25": None,
        "bf25": None,
    }


def write_option_surface_m1_artifacts(
    gold: pl.DataFrame,
    gold_lake_root: str,
    plot: bool = True,
    manifest: bool = True,
) -> list[str]:
    """Persist Gold option surface rows into monthly artifact files."""

    written_files: list[str] = []
    if gold.is_empty():
        return written_files

    for part in gold.partition_by(["currency", "month"]):
        first = part.row(0, named=True)
        currency = str(first["currency"])
        month = str(first["month"])
        out_dir = (
            Path(gold_lake_root)
            / f"dataset_type={OPTION_SURFACE_DATASET_TYPE}"
            / "exchange=deribit"
            / "instrument_type=option"
            / f"currency={currency}"
            / f"month={month}"
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = out_dir / f"{month}.parquet"
        json_path = out_dir / f"{month}.json"
        png_path = out_dir / f"{month}.png"
        output = upsert_partition_parquet(
            file_path=parquet_path,
            partition=part,
            natural_key=OPTION_SURFACE_NATURAL_KEY,
            sort_by="ts_minute",
        )
        written_files.append(str(parquet_path.resolve()))
        if manifest:
            write_json_artifact(json_path, option_surface_metadata(output))
            written_files.append(str(json_path.resolve()))
        if plot:
            write_option_surface_plot(output, png_path)
            written_files.append(str(png_path.resolve()))

    return sorted(written_files)


def option_surface_metadata(gold: pl.DataFrame) -> dict[str, object]:
    """Build JSON metadata for one Gold option surface artifact."""

    ts_min, ts_max = timestamp_bounds_iso(gold, "ts_minute")
    return {
        "dataset_type": OPTION_SURFACE_DATASET_TYPE,
        "schema_version": OPTION_SURFACE_SCHEMA_VERSION,
        "build_timestamp_utc": datetime.now(UTC).isoformat(),
        "row_count": gold.height,
        "column_count": len(gold.columns),
        "timestamp_min": ts_min,
        "timestamp_max": ts_max,
        "currencies": sorted(str(item) for item in gold["currency"].unique().to_list()),
    }


def write_option_surface_plot(gold: pl.DataFrame, path: Path) -> None:
    """Write a compact profile PNG for Gold option surface artifacts."""

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    cols = ["atm_iv", "skew_slope", "smile_curvature", "surface_coverage_ratio"]
    present = [column for column in cols if column in gold.columns]
    if not present:
        path.touch()
        return
    fig, axes = plt.subplots(len(present), 1, figsize=(12, max(4, len(present) * 1.8)), squeeze=False)
    fig.patch.set_facecolor("#111217")
    ts = gold["ts_minute"].to_list()
    for idx, column in enumerate(present):
        ax = axes[idx][0]
        vals = gold[column].to_list()
        clean = [value for value in vals if isinstance(value, int | float) and math.isfinite(float(value))]
        ax.set_facecolor("#161922")
        ax.plot(ts, vals, color="#88c0d0", linewidth=0.9)
        ax.set_title(f"{column} (n={len(clean)})", color="#eceff4", fontsize=9, loc="left")
        ax.tick_params(colors="#d8dee9", labelsize=7)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=120, facecolor=fig.get_facecolor())
    plt.close(fig)


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _mean_nullable(values: list[float | None]) -> float | None:
    clean = [value for value in values if value is not None]
    if not clean:
        return None
    return float(sum(clean) / len(clean))


def _term_bucket(term_days: float | None) -> str:
    if term_days is None:
        return "unknown"
    if term_days <= 7:
        return "ultra_short"
    if term_days <= 30:
        return "short"
    if term_days <= 90:
        return "medium"
    return "long"
