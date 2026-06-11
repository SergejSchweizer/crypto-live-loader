"""Parquet lake writer for bronze option ticker snapshots."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from ingestion.options import OptionTickerSnapshotRow

OptionPartitionKey = tuple[str, str, str, str, str, str, str]


def option_snapshot_partition_path(lake_root: str, key: OptionPartitionKey) -> Path:
    """Return the bronze destination directory for one option snapshot partition."""

    dataset_type, exchange, instrument_type, currency, year_partition, month_partition, date_partition = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"currency={currency}"
        / "source=rest_get_book_summary_by_currency"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
    )


def options_ticker_snapshot_record(row: OptionTickerSnapshotRow) -> dict[str, object]:
    """Convert one typed option snapshot row to a parquet record."""

    return {
        "exchange": row.exchange,
        "dataset_type": row.dataset_type,
        "source": row.source,
        "currency": row.currency,
        "requested_currency": row.requested_currency,
        "source_currency": row.source_currency,
        "instrument_name": row.instrument_name,
        "base_currency": row.base_currency,
        "quote_currency": row.quote_currency,
        "instrument_type": row.instrument_type,
        "snapshot_time": row.snapshot_time,
        "exchange_creation_time": row.exchange_creation_time,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "bid_price": row.bid_price,
        "ask_price": row.ask_price,
        "mid_price": row.mid_price,
        "mark_price": row.mark_price,
        "mark_iv": row.mark_iv,
        "underlying_price": row.underlying_price,
        "underlying_index": row.underlying_index,
        "interest_rate": row.interest_rate,
        "open_interest": row.open_interest,
        "volume": row.volume,
        "volume_usd": row.volume_usd,
        "high": row.high,
        "low": row.low,
        "last": row.last,
        "price_change": row.price_change,
        "raw_payload_hash": row.raw_payload_hash,
        "schema_version": row.schema_version,
    }


def save_options_ticker_snapshot_parquet_lake(
    rows_by_currency: dict[str, list[OptionTickerSnapshotRow]],
    lake_root: str,
) -> list[str]:
    """Append option snapshot rows into daily bronze parquet partitions."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    grouped: defaultdict[OptionPartitionKey, list[dict[str, object]]] = defaultdict(list)
    for rows in rows_by_currency.values():
        for row in rows:
            key: OptionPartitionKey = (
                row.dataset_type,
                row.exchange,
                row.instrument_type,
                row.currency,
                row.snapshot_time.strftime("%Y"),
                row.snapshot_time.strftime("%m"),
                row.snapshot_time.strftime("%d"),
            )
            grouped[key].append(options_ticker_snapshot_record(row))

    written_files: list[str] = []
    for key, records in grouped.items():
        part_dir = option_snapshot_partition_path(lake_root=lake_root, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        run_id = records[0]["run_id"]
        file_path = part_dir / f"part-{run_id}.parquet"
        table = pa.Table.from_pylist(records)
        pq.write_table(table, file_path)  # type: ignore[no-untyped-call]
        written_files.append(str(file_path.resolve()))

    return sorted(written_files)


def option_ticker_snapshot_record(row: OptionTickerSnapshotRow) -> dict[str, object]:
    """Backward-compatible alias for options ticker record serialization."""

    return options_ticker_snapshot_record(row)


def save_option_ticker_snapshot_parquet_lake(
    rows_by_currency: dict[str, list[OptionTickerSnapshotRow]],
    lake_root: str,
) -> list[str]:
    """Backward-compatible alias for options ticker parquet persistence."""

    return save_options_ticker_snapshot_parquet_lake(rows_by_currency=rows_by_currency, lake_root=lake_root)
