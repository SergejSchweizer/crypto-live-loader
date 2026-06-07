"""Shared atomic parquet persistence helpers for Polars-based artifacts."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TypeAlias

import polars as pl

from ingestion.file_lock import locked_output_path

SortColumns: TypeAlias = str | list[str]


def is_committed_parquet_path(path: Path) -> bool:
    """Return whether a parquet path is a committed artifact rather than writer scratch state."""

    return path.suffix == ".parquet" and not any(part.startswith(".") for part in path.parts)


def upsert_partition_parquet(
    *,
    file_path: Path,
    partition: pl.DataFrame,
    natural_key: list[str],
    sort_by: SortColumns,
    legacy_file_path: Path | None = None,
) -> pl.DataFrame:
    """Write one parquet partition atomically with dedupe and deterministic sorting."""

    file_path.parent.mkdir(parents=True, exist_ok=True)
    staging_path = file_path.parent / f".staging-{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}.parquet"
    with locked_output_path(file_path):
        output = partition
        existing_file_path = file_path if file_path.exists() else legacy_file_path
        if existing_file_path is not None and existing_file_path.exists():
            output = pl.concat([pl.read_parquet(existing_file_path), partition], how="vertical")
        output = output.unique(subset=natural_key, keep="last").sort(sort_by)
        output.write_parquet(staging_path)
        staging_path.replace(file_path)
    return output
