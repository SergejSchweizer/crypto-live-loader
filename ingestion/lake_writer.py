"""Shared helpers for partitioned Bronze parquet lake writes."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TypeAlias, TypeVar

from ingestion.parquet_repository import NaturalKeyBuilder, ParquetRecord, ParquetUpsertRepository, SortKeyBuilder

PartitionKey: TypeAlias = tuple[object, ...]
PartitionPathBuilder: TypeAlias = Callable[[str, PartitionKey], Path]

RowT = TypeVar("RowT")


def bronze_partition_path(lake_root: str, partition_parts: dict[str, object]) -> Path:
    """Build a Hive-style Bronze partition path from ordered partition values.

    Args:
        lake_root (str): Root Bronze lake directory.
        partition_parts (dict[str, object]): Ordered partition names and values. Insertion order
            defines the path layout and therefore remains dataset-owned.

    Returns:
        Path: Directory path for one Bronze partition.
    """

    path = Path(lake_root)
    for name, value in partition_parts.items():
        path /= f"{name}={value}"
    return path


def upsert_partitioned_records(
    *,
    rows: Iterable[RowT],
    lake_root: str,
    partition_key: Callable[[RowT], PartitionKey],
    partition_path: PartitionPathBuilder,
    record_builder: Callable[[RowT], ParquetRecord],
    natural_key: NaturalKeyBuilder,
    sort_key: SortKeyBuilder,
    staging_name: Callable[[list[ParquetRecord]], str],
) -> list[str]:
    """Group rows by partition and upsert each group into ``data.parquet``.

    Args:
        rows (Iterable[RowT]): Typed dataset rows to persist.
        lake_root (str): Root Bronze lake directory.
        partition_key (Callable[[RowT], PartitionKey]): Builds the dataset-owned partition key.
        partition_path (PartitionPathBuilder): Maps the partition key to its Bronze directory.
        record_builder (Callable[[RowT], ParquetRecord]): Serializes one typed row to a parquet row.
        natural_key (NaturalKeyBuilder): Idempotent row key for repository upserts.
        sort_key (SortKeyBuilder): Deterministic ordering key for repository output.
        staging_name (Callable[[list[ParquetRecord]], str]): Builds the atomic staging filename for
            one partition group.

    Returns:
        list[str]: Sorted absolute paths written by the repository.
    """

    repository = ParquetUpsertRepository()
    grouped: defaultdict[PartitionKey, list[ParquetRecord]] = defaultdict(list)
    for row in rows:
        grouped[partition_key(row)].append(record_builder(row))

    written_files: list[str] = []
    for key, records in grouped.items():
        file_path = partition_path(lake_root, key) / "data.parquet"
        written_files.append(
            repository.upsert(
                file_path=file_path,
                records=records,
                natural_key=natural_key,
                sort_key=sort_key,
                staging_name=staging_name(records),
            )
        )
    return sorted(written_files)
