"""Shared helpers for artifact metadata and JSON writes."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl


def write_json_artifact(path: Path, payload: dict[str, Any]) -> None:
    """Write one JSON artifact with deterministic formatting."""

    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def timestamp_bounds_iso(frame: pl.DataFrame, column: str) -> tuple[str | None, str | None]:
    """Return ISO timestamp min/max for a datetime-like column."""

    if column not in frame.columns or frame.height == 0:
        return None, None
    ts_min = frame[column].min()
    ts_max = frame[column].max()
    return _to_iso(ts_min), _to_iso(ts_max)


def column_dtype_metadata(frame: pl.DataFrame) -> list[dict[str, str]]:
    """Return compact column-name and dtype metadata."""

    return [{"name": column, "dtype": str(frame[column].dtype)} for column in frame.columns]


def _to_iso(value: object) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None
