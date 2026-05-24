"""Shared parquet repository helpers for partitioned lake persistence."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TypeAlias

from ingestion.file_lock import locked_output_path

ParquetRecord: TypeAlias = dict[str, object]
NaturalKeyBuilder: TypeAlias = Callable[[ParquetRecord], tuple[object, ...]]
SortableValue: TypeAlias = datetime | str | int | float
SortKeyBuilder: TypeAlias = Callable[[ParquetRecord], SortableValue]


class ParquetUpsertRepository:
    """Repository for idempotent parquet upserts on one target file."""

    def upsert(
        self,
        *,
        file_path: Path,
        records: list[ParquetRecord],
        natural_key: NaturalKeyBuilder,
        sort_key: SortKeyBuilder,
        staging_name: str = ".staging-data.parquet",
    ) -> str:
        """Merge existing and new records by natural key and write atomically."""

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

        file_path.parent.mkdir(parents=True, exist_ok=True)
        with locked_output_path(file_path):
            existing_rows: list[ParquetRecord] = []
            if file_path.exists():
                existing_rows = pq.ParquetFile(file_path).read().to_pylist()  # type: ignore[no-untyped-call]
            merged: dict[tuple[object, ...], ParquetRecord] = {natural_key(row): row for row in existing_rows}
            for row in records:
                merged[natural_key(row)] = row
            output_rows = sorted(merged.values(), key=sort_key)
            table = pa.Table.from_pylist(output_rows)
            staging = file_path.parent / staging_name
            pq.write_table(table, staging)  # type: ignore[no-untyped-call]
            staging.replace(file_path)
        return str(file_path.resolve())
