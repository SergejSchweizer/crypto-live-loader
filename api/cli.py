"""Command-line interface for Deribit L2 order book ingestion."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from time import perf_counter
from typing import TypeAlias, cast

from api.constants import (
    BRONZE_BUILDER_COMMAND,
    INDEX_PRICE_BRONZE_BUILDER_COMMAND,
    INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
    LEGACY_BRONZE_BUILDER_COMMAND,
    OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
    OPTIONS_BRONZE_BUILDER_COMMAND,
    VALIDATE_SYMBOLS_COMMAND,
)
from api.runtime import configure_logging
from domain.models import RawSnapshot
from ingestion.config import (
    Config,
    config_bool,
    config_float,
    config_int,
    config_section,
    config_str,
    config_str_list,
    load_config,
)
from ingestion.index_price import (
    INDEX_PRICE_DATASET_TYPE,
    INDEX_PRICE_SCHEMA_VERSION,
    INDEX_PRICE_SOURCE,
    IndexPriceSnapshotRow,
    normalize_index_price_snapshot_row,
)
from ingestion.index_price import (
    snapshot_time_floor_minute as index_snapshot_time_floor_minute,
)
from ingestion.index_price import (
    utc_run_id as index_utc_run_id,
)
from ingestion.index_price_lake import save_index_price_snapshot_parquet_lake
from ingestion.instrument_metadata import (
    INSTRUMENT_METADATA_DATASET_TYPE,
    INSTRUMENT_METADATA_SCHEMA_VERSION,
    INSTRUMENT_METADATA_SOURCE,
    InstrumentMetadataSnapshotRow,
    normalize_instrument_metadata_rows,
    snapshot_date_utc,
)
from ingestion.instrument_metadata import (
    utc_run_id as instrument_utc_run_id,
)
from ingestion.instrument_metadata_lake import save_instrument_metadata_snapshot_parquet_lake
from ingestion.l2 import L2Snapshot, fetch_l2_snapshots_for_symbols
from ingestion.lake import save_l2_snapshot_parquet_lake
from ingestion.option_instrument_ticker import (
    OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
    OPTION_INSTRUMENT_TICKER_SCHEMA_VERSION,
    OPTION_INSTRUMENT_TICKER_SOURCE,
    OptionInstrumentTickerSnapshotRow,
    normalize_option_instrument_ticker_rows,
)
from ingestion.option_instrument_ticker import (
    snapshot_time_floor_minute as option_instrument_ticker_snapshot_time_floor_minute,
)
from ingestion.option_instrument_ticker import (
    utc_run_id as option_instrument_ticker_utc_run_id,
)
from ingestion.option_instrument_ticker_lake import save_option_instrument_ticker_snapshot_parquet_lake
from ingestion.option_ticker_universe import select_option_ticker_prediction_universe
from ingestion.options import (
    OPTION_TICKER_DATASET_TYPE,
    OPTION_TICKER_SCHEMA_VERSION,
    OPTION_TICKER_SOURCE,
    OptionTickerSnapshotRow,
    normalize_options_ticker_rows,
    snapshot_time_floor_minute,
    utc_run_id,
)
from ingestion.options_lake import save_options_ticker_snapshot_parquet_lake
from sources.deribit_index_price import fetch_index_price
from sources.deribit_instruments import fetch_instruments
from sources.deribit_option_ticker import fetch_option_ticker
from sources.deribit_options import (
    fetch_option_book_summary_rows,
    resolve_options_currency_request,
)
from sources.registry import source_adapter_for_exchange

__all__ = ["build_parser", "main"]

SnapshotsBySymbol = dict[str, list[L2Snapshot]]
OptionRowsByCurrency = dict[str, list[OptionTickerSnapshotRow]]
OptionInstrumentTickerRowsByInstrument = dict[str, OptionInstrumentTickerSnapshotRow]
InstrumentMetadataRowsByDate = dict[str, list[InstrumentMetadataSnapshotRow]]
IndexPriceRowsBySymbol = dict[str, list[IndexPriceSnapshotRow]]
CommandHandler: TypeAlias = Callable[[argparse.Namespace, logging.Logger, Config], None]


def _boolean_optional_flag(
    parser: argparse.ArgumentParser,
    name: str,
    default: bool,
    help_text: str,
) -> None:
    parser.add_argument(
        f"--{name}",
        action=argparse.BooleanOptionalAction,
        default=default,
        help=help_text,
    )


def _debug_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable DEBUG-level logging for this command",
    )


def _enable_debug_logging(args: argparse.Namespace, logger: logging.Logger) -> None:
    if not bool(getattr(args, "debug", False)):
        return
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers:
        handler.setLevel(logging.DEBUG)


def build_parser(config: Config | None = None) -> argparse.ArgumentParser:
    """Create the CLI parser."""

    config = config or load_config()
    ingestion_config = config_section(config, "ingestion")
    parser = argparse.ArgumentParser(description="crypto-live-loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    l2_parser = subparsers.add_parser(
        BRONZE_BUILDER_COMMAND,
        aliases=[LEGACY_BRONZE_BUILDER_COMMAND],
        help="Fetch Deribit L2 snapshots and persist raw rows to the bronze parquet lake",
    )
    l2_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default=config_str(ingestion_config, "exchange", "deribit"),
    )
    l2_parser.add_argument(
        "--symbols",
        nargs="+",
        default=config_str_list(ingestion_config, "symbols", ["BTC", "ETH"]),
        type=str,
        help="Symbols to fetch, separated by spaces or commas",
    )
    l2_parser.add_argument(
        "--levels",
        type=int,
        default=config_int(ingestion_config, "levels", 50),
        help="Number of book levels per side to request",
    )
    l2_parser.add_argument(
        "--snapshot-count",
        type=int,
        default=config_int(ingestion_config, "snapshot_count", 5),
        help="Polling ticks per symbol to collect per run",
    )
    l2_parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=config_float(ingestion_config, "poll_interval_s", 10.0),
        help="Sleep interval between polling ticks",
    )
    l2_parser.add_argument(
        "--lake-root",
        default=config_str(ingestion_config, "lake_root", "lake/bronze"),
        help="Root directory for parquet lake files",
    )
    l2_parser.add_argument(
        "--max-runtime-s",
        type=float,
        default=config_float(ingestion_config, "max_runtime_s", 50.0),
        help="Maximum collection runtime in seconds; 0 disables the budget",
    )
    l2_parser.add_argument(
        "--save-parquet-lake",
        action=argparse.BooleanOptionalAction,
        default=config_bool(ingestion_config, "save_parquet_lake", False),
        help="Save raw L2 snapshots to bronze parquet lake partitions",
    )
    l2_parser.add_argument(
        "--json-output",
        action=argparse.BooleanOptionalAction,
        default=config_bool(ingestion_config, "json_output", True),
        help="Print JSON output",
    )
    _debug_flag(l2_parser)

    options_config = config_section(ingestion_config, "options")
    options_parser = subparsers.add_parser(
        OPTIONS_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit option ticker snapshots and persist raw rows to the bronze parquet lake",
    )
    options_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default="deribit",
    )
    options_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(options_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Option symbols/currencies to fetch, separated by spaces or commas",
    )
    options_parser.add_argument(
        "--lake-root",
        default=config_str(options_config, "lake_root", "lake/bronze"),
        help="Root directory for parquet lake files",
    )
    options_parser.add_argument(
        "--save-parquet-lake",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save raw option ticker snapshots to bronze parquet lake partitions",
    )
    options_parser.add_argument(
        "--schema-version",
        default=config_str(options_config, "schema_version", OPTION_TICKER_SCHEMA_VERSION),
        help="Schema version tag to annotate each normalized row",
    )
    options_parser.add_argument(
        "--source",
        default=config_str(options_config, "source", OPTION_TICKER_SOURCE),
        help="Source identifier written to bronze rows",
    )
    options_parser.add_argument(
        "--json-output",
        action=argparse.BooleanOptionalAction,
        default=config_bool(options_config, "json_output", config_bool(ingestion_config, "json_output", True)),
        help="Print JSON output",
    )
    _debug_flag(options_parser)

    option_instrument_ticker_config = config_section(ingestion_config, "option_instrument_ticker")
    option_instrument_ticker_parser = subparsers.add_parser(
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit per-option ticker IV and Greeks into the bronze parquet lake",
    )
    option_instrument_ticker_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default="deribit",
    )
    option_instrument_ticker_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(option_instrument_ticker_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Option symbols/currencies used when discovering active instruments",
    )
    option_instrument_ticker_parser.add_argument(
        "--instruments",
        nargs="+",
        default=config_str_list(option_instrument_ticker_config, "instruments", []),
        type=str,
        help="Explicit Deribit option instruments to fetch; skips currency discovery when provided",
    )
    option_instrument_ticker_parser.add_argument(
        "--lake-root",
        default=config_str(option_instrument_ticker_config, "lake_root", "lake/bronze"),
        help="Root directory for parquet lake files",
    )
    option_instrument_ticker_parser.add_argument(
        "--max-instruments-per-run",
        type=int,
        default=config_int(option_instrument_ticker_config, "max_instruments_per_run", 20),
        help="Maximum selected option instruments to fetch sequentially per currency",
    )
    _boolean_optional_flag(
        option_instrument_ticker_parser,
        "save-parquet-lake",
        config_bool(option_instrument_ticker_config, "save_parquet_lake", True),
        "Save per-option ticker snapshots to bronze parquet lake partitions",
    )
    option_instrument_ticker_parser.add_argument(
        "--schema-version",
        default=config_str(option_instrument_ticker_config, "schema_version", OPTION_INSTRUMENT_TICKER_SCHEMA_VERSION),
        help="Schema version tag to annotate each normalized row",
    )
    option_instrument_ticker_parser.add_argument(
        "--source",
        default=config_str(option_instrument_ticker_config, "source", OPTION_INSTRUMENT_TICKER_SOURCE),
        help="Source identifier written to bronze rows",
    )
    _boolean_optional_flag(
        option_instrument_ticker_parser,
        "json-output",
        config_bool(
            option_instrument_ticker_config,
            "json_output",
            config_bool(ingestion_config, "json_output", True),
        ),
        "Print JSON output",
    )
    _debug_flag(option_instrument_ticker_parser)

    instrument_metadata_config = config_section(ingestion_config, "instrument_metadata")
    instrument_metadata_parser = subparsers.add_parser(
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit instrument metadata snapshots and persist raw rows to the bronze parquet lake",
    )
    instrument_metadata_parser.add_argument(
        "--symbols",
        "--currencies",
        dest="currencies",
        nargs="+",
        default=config_str_list(instrument_metadata_config, "currencies", ["BTC", "ETH", "SOL"]),
        type=str,
        help="Base symbols/currencies to fetch instrument metadata for",
    )
    instrument_metadata_parser.add_argument(
        "--kind",
        default=config_str(instrument_metadata_config, "kind", "option"),
        help="Deribit instrument kind filter, for example option or future",
    )
    _boolean_optional_flag(
        instrument_metadata_parser,
        "include-inactive",
        config_bool(instrument_metadata_config, "include_inactive", False),
        "Include expired/inactive instruments from Deribit",
    )
    instrument_metadata_parser.add_argument(
        "--lake-root",
        default=config_str(instrument_metadata_config, "lake_root", "lake/bronze"),
        help="Root directory for parquet lake files",
    )
    _boolean_optional_flag(
        instrument_metadata_parser,
        "save-parquet-lake",
        config_bool(instrument_metadata_config, "save_parquet_lake", True),
        "Save raw instrument metadata rows to bronze parquet lake partitions",
    )
    instrument_metadata_parser.add_argument(
        "--schema-version",
        default=config_str(instrument_metadata_config, "schema_version", INSTRUMENT_METADATA_SCHEMA_VERSION),
        help="Schema version tag to annotate each normalized row",
    )
    instrument_metadata_parser.add_argument(
        "--source",
        default=config_str(instrument_metadata_config, "source", INSTRUMENT_METADATA_SOURCE),
        help="Source identifier written to bronze rows",
    )
    _boolean_optional_flag(
        instrument_metadata_parser,
        "json-output",
        config_bool(instrument_metadata_config, "json_output", config_bool(ingestion_config, "json_output", True)),
        "Print JSON output",
    )
    _debug_flag(instrument_metadata_parser)

    index_price_config = config_section(ingestion_config, "index_price")
    index_price_parser = subparsers.add_parser(
        INDEX_PRICE_BRONZE_BUILDER_COMMAND,
        help="Fetch Deribit index prices and persist minutely raw rows to the bronze parquet lake",
    )
    index_price_parser.add_argument(
        "--symbols",
        nargs="+",
        default=config_str_list(index_price_config, "symbols", ["btc_usd", "eth_usd", "sol_usdc"]),
        type=str,
        help="Deribit index names to fetch, separated by spaces or commas",
    )
    index_price_parser.add_argument(
        "--lake-root",
        default=config_str(index_price_config, "lake_root", "lake/bronze"),
        help="Root directory for parquet lake files",
    )
    _boolean_optional_flag(
        index_price_parser,
        "save-parquet-lake",
        config_bool(index_price_config, "save_parquet_lake", True),
        "Save raw index price rows to bronze parquet lake partitions",
    )
    index_price_parser.add_argument(
        "--source",
        default=config_str(index_price_config, "source", INDEX_PRICE_SOURCE),
        help="Source identifier written to bronze rows",
    )
    index_price_parser.add_argument(
        "--schema-version",
        default=config_str(index_price_config, "schema_version", INDEX_PRICE_SCHEMA_VERSION),
        help="Schema version tag to annotate each normalized row",
    )
    _boolean_optional_flag(
        index_price_parser,
        "json-output",
        config_bool(index_price_config, "json_output", config_bool(ingestion_config, "json_output", True)),
        "Print JSON output",
    )
    _debug_flag(index_price_parser)

    validate_parser = subparsers.add_parser(
        VALIDATE_SYMBOLS_COMMAND,
        help="Resolve symbols and check whether Deribit returns a usable L2 book",
    )
    validate_parser.add_argument(
        "--exchange",
        choices=["deribit"],
        default=config_str(ingestion_config, "exchange", "deribit"),
    )
    validate_parser.add_argument(
        "--symbols",
        nargs="+",
        default=config_str_list(ingestion_config, "symbols", ["BTC", "ETH"]),
        type=str,
        help="Symbols to validate, separated by spaces or commas",
    )
    validate_parser.add_argument(
        "--levels",
        type=int,
        default=1,
        help="Number of book levels per side to request for validation",
    )
    validate_parser.add_argument(
        "--json-output",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print JSON output",
    )
    _debug_flag(validate_parser)

    return parser


def _serialize_l2_snapshot(item: L2Snapshot) -> dict[str, object]:
    """Convert an L2 snapshot into a JSON-safe output dictionary."""

    data = asdict(item)
    timestamp = data["timestamp"]
    if not hasattr(timestamp, "isoformat"):
        raise ValueError("timestamp must be datetime-like")
    data["timestamp"] = timestamp.isoformat()
    return data


def _normalize_cli_symbols(values: list[str]) -> list[str]:
    """Normalize CLI symbol values from space- or comma-delimited inputs."""

    symbols: list[str] = []
    for value in values:
        symbols.extend(item.strip().upper() for item in value.replace(",", " ").split() if item.strip())
    if not symbols:
        raise ValueError("at least one symbol is required")
    return symbols


def _normalize_cli_currencies(values: list[str]) -> list[str]:
    """Normalize CLI currency values from space- or comma-delimited inputs."""

    currencies: list[str] = []
    for value in values:
        currencies.extend(item.strip().upper() for item in value.replace(",", " ").split() if item.strip())
    if not currencies:
        raise ValueError("at least one currency is required")
    return currencies


def _normalize_cli_index_symbols(values: list[str]) -> list[str]:
    """Normalize CLI index-symbol values from space- or comma-delimited inputs."""

    symbols: list[str] = []
    for value in values:
        symbols.extend(item.strip().lower() for item in value.replace(",", " ").split() if item.strip())
    if not symbols:
        raise ValueError("at least one index symbol is required")
    return symbols


def _normalize_cli_instruments(values: list[str]) -> list[str]:
    """Normalize CLI instrument values from space- or comma-delimited inputs."""

    instruments: list[str] = []
    for value in values:
        instruments.extend(item.strip().upper() for item in value.replace(",", " ").split() if item.strip())
    return sorted(dict.fromkeys(instruments))


def _emit_json_output(enabled: bool, payload: Mapping[str, object]) -> None:
    """Print one JSON payload when output is enabled."""

    if enabled:
        print(json.dumps(payload, indent=2))


def _log_value(value: object) -> str:
    if value is None:
        return "none"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return f"{value:.3f}"
    if isinstance(value, Mapping):
        return ",".join(f"{key}:{_log_value(item)}" for key, item in sorted(value.items()))
    if isinstance(value, list | tuple | set):
        return ",".join(str(item) for item in value)
    return str(value)


def _format_log_fields(fields: Mapping[str, object]) -> str:
    return " ".join(f"{key}={_log_value(value)}" for key, value in sorted(fields.items()))


def _log_job_event(
    logger: logging.Logger,
    level: int,
    command: str,
    event: str,
    **fields: object,
) -> None:
    logger.log(level, "job_event command=%s event=%s %s", command, event, _format_log_fields(fields))


def _fetch_options_rows_for_currencies(
    currencies: list[str],
) -> tuple[dict[str, list[dict[str, object]]], dict[str, str], dict[str, str]]:
    """Fetch option summary rows sequentially for all requested currencies."""

    rows_by_currency: dict[str, list[dict[str, object]]] = {}
    source_currency_by_requested: dict[str, str] = {}
    errors: dict[str, str] = {}

    for currency in currencies:
        request = resolve_options_currency_request(currency)
        source_currency_by_requested[request.requested_currency] = request.source_currency
        try:
            rows_by_currency[request.requested_currency] = fetch_option_book_summary_rows(request)
        except Exception as exc:  # noqa: BLE001
            errors[request.requested_currency] = str(exc)
    return rows_by_currency, source_currency_by_requested, errors


def _fetch_option_ticker_rows_for_instruments(
    instruments: list[str],
) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
    """Fetch per-instrument option ticker rows sequentially."""

    rows_by_instrument: dict[str, dict[str, object]] = {}
    errors: dict[str, str] = {}

    for instrument_name in instruments:
        try:
            rows_by_instrument[instrument_name] = fetch_option_ticker(instrument_name)
        except Exception as exc:  # noqa: BLE001
            errors[instrument_name] = str(exc)
    return rows_by_instrument, errors


def _log_bronze_builder_summary(
    logger: logging.Logger,
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: dict[str, list[L2Snapshot]],
    requested_snapshots: int,
    parquet_files: list[str],
    elapsed_s: float,
    parquet_error: str | None = None,
) -> None:
    """Write a compact run-level bronze-builder summary."""

    collected_total = sum(len(snapshots_by_symbol.get(symbol.upper(), [])) for symbol in symbols)
    requested_total = requested_snapshots * len(symbols)
    status = "partial" if collected_total < requested_total else "complete"
    if parquet_error is not None:
        status = "parquet_error"
    _log_job_event(
        logger,
        logging.INFO,
        BRONZE_BUILDER_COMMAND,
        "run_summary",
        elapsed_s=elapsed_s,
        errors=1 if parquet_error is not None else 0,
        exchange=exchange,
        parquet_error=parquet_error,
        parquet_files=len(parquet_files),
        snapshots_collected=collected_total,
        snapshots_requested=requested_total,
        status=status,
        symbols=[symbol.upper() for symbol in symbols],
    )


def _build_snapshot_output(
    exchange: str,
    symbols: list[str],
    snapshots_by_symbol: SnapshotsBySymbol,
    requested_snapshots: int,
    logger: logging.Logger,
) -> dict[str, object]:
    """Build JSON output for raw bronze snapshots and log per-symbol collection status."""

    output: dict[str, object] = {exchange: {}}
    exchange_output = cast(dict[str, object], output[exchange])

    for symbol in symbols:
        symbol_key = symbol.upper()
        snapshots = snapshots_by_symbol.get(symbol_key, [])
        _log_partial_snapshot_warning(
            logger=logger,
            symbol=symbol_key,
            collected_snapshots=len(snapshots),
            requested_snapshots=requested_snapshots,
        )
        exchange_output[symbol_key] = [_serialize_l2_snapshot(item) for item in snapshots]
        _log_job_event(
            logger,
            logging.INFO,
            BRONZE_BUILDER_COMMAND,
            "snapshot_stats",
            exchange=exchange,
            snapshots_collected=len(snapshots),
            snapshots_requested=requested_snapshots,
            symbol=symbol_key,
        )

    return output


def _log_partial_snapshot_warning(
    logger: logging.Logger,
    symbol: str,
    collected_snapshots: int,
    requested_snapshots: int,
) -> None:
    """Log a warning when the run collected fewer snapshots than requested."""

    if collected_snapshots >= requested_snapshots:
        return
    logger.warning(
        "bronze-builder collected partial snapshots symbol=%s collected=%s requested=%s",
        symbol,
        collected_snapshots,
        requested_snapshots,
    )


def _persist_bronze_snapshots(
    snapshots_by_symbol: SnapshotsBySymbol,
    lake_root: str,
    depth: int,
    enabled: bool,
    output: dict[str, object],
    logger: logging.Logger,
) -> tuple[list[str], str | None]:
    """Persist raw L2 snapshots when requested and annotate the CLI output."""

    if not enabled:
        return [], None

    try:
        parquet_files = save_l2_snapshot_parquet_lake(
            snapshots_by_symbol=snapshots_by_symbol,
            lake_root=lake_root,
            depth=depth,
        )
        output["_parquet_files"] = parquet_files
        return parquet_files, None
    except Exception as exc:  # noqa: BLE001
        parquet_error = str(exc)
        output["_parquet_error"] = parquet_error
        logger.exception("bronze-builder raw snapshot parquet write failed")
        return [], parquet_error


def _estimated_poll_runtime_s(snapshot_count: int, poll_interval_s: float) -> float:
    """Estimate runtime spent sleeping between polling ticks."""

    return max(0, snapshot_count - 1) * poll_interval_s


def _warn_for_long_poll_schedule(
    logger: logging.Logger,
    snapshot_count: int,
    poll_interval_s: float,
    max_runtime_s: float,
) -> None:
    """Warn when bronze-builder polling settings are likely to collide with minute cron runs."""

    estimated_s = _estimated_poll_runtime_s(snapshot_count=snapshot_count, poll_interval_s=poll_interval_s)
    if max_runtime_s > 0 and estimated_s >= max_runtime_s:
        logger.warning(
            "bronze-builder polling sleep budget may exceed max runtime estimated_sleep_s=%.3f max_runtime_s=%.3f",
            estimated_s,
            max_runtime_s,
        )
    if estimated_s >= 60:
        logger.warning(
            "bronze-builder polling sleep budget is at least one minute estimated_sleep_s=%.3f; cron runs may overlap",
            estimated_s,
        )


def _run_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    """Run L2 snapshot collection and optional raw bronze persistence."""

    started_at = perf_counter()
    exchange = cast(str, args.exchange)
    symbols = _normalize_cli_symbols(cast(list[str], args.symbols))
    requested_snapshots = int(args.snapshot_count)
    max_runtime_s = float(args.max_runtime_s)
    _warn_for_long_poll_schedule(
        logger=logger,
        snapshot_count=requested_snapshots,
        poll_interval_s=float(args.poll_interval_s),
        max_runtime_s=max_runtime_s,
    )
    snapshots_by_symbol = fetch_l2_snapshots_for_symbols(
        exchange=exchange,
        symbols=symbols,
        depth=int(args.levels),
        snapshot_count=requested_snapshots,
        poll_interval_s=float(args.poll_interval_s),
        max_runtime_s=max_runtime_s if max_runtime_s > 0 else None,
    )

    output = _build_snapshot_output(
        exchange=exchange,
        symbols=symbols,
        snapshots_by_symbol=snapshots_by_symbol,
        requested_snapshots=requested_snapshots,
        logger=logger,
    )
    parquet_files, parquet_error = _persist_bronze_snapshots(
        snapshots_by_symbol=snapshots_by_symbol,
        lake_root=cast(str, args.lake_root),
        depth=int(args.levels),
        enabled=bool(args.save_parquet_lake),
        output=output,
        logger=logger,
    )

    _emit_json_output(bool(args.json_output), output)
    _log_bronze_builder_summary(
        logger=logger,
        exchange=exchange,
        symbols=symbols,
        snapshots_by_symbol=snapshots_by_symbol,
        requested_snapshots=requested_snapshots,
        parquet_files=parquet_files,
        elapsed_s=perf_counter() - started_at,
        parquet_error=parquet_error,
    )


def _run_options_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    """Run Deribit options chain snapshot collection and optional bronze persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    run_id = utc_run_id()
    snapshot_time = snapshot_time_floor_minute()
    ingested_at = datetime.now(UTC)

    raw_rows_by_currency, source_currency_by_requested, fetch_errors = _fetch_options_rows_for_currencies(
        currencies=currencies
    )

    rows_by_currency: OptionRowsByCurrency = {}
    normalization_errors: list[str] = []
    for currency in currencies:
        source_currency = source_currency_by_requested.get(currency, "")
        raw_rows = raw_rows_by_currency.get(currency, [])
        rows, row_errors = normalize_options_ticker_rows(
            raw_rows,
            requested_currency=currency,
            source_currency=source_currency,
            run_id=run_id,
            snapshot_time=snapshot_time,
            ingested_at=ingested_at,
            source=cast(str, args.source),
            schema_version=cast(str, args.schema_version),
        )
        rows_by_currency[currency] = rows
        normalization_errors.extend(row_errors)

    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_options_ticker_snapshot_parquet_lake(
            rows_by_currency=rows_by_currency,
            lake_root=cast(str, args.lake_root),
        )

    currency_results: dict[str, dict[str, object]] = {}
    for currency in currencies:
        if currency in fetch_errors:
            currency_results[currency] = {
                "rows": 0,
                "status": "error",
                "error": fetch_errors[currency],
            }
            continue
        currency_results[currency] = {
            "rows": len(rows_by_currency.get(currency, [])),
            "status": "ok",
            "source_currency": source_currency_by_requested.get(currency, ""),
        }

    errors = list(fetch_errors.values()) + normalization_errors
    output = {
        "command": OPTIONS_BRONZE_BUILDER_COMMAND,
        "exchange": cast(str, args.exchange),
        "dataset_type": OPTION_TICKER_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_currencies": currencies,
        "rows_written": sum(len(rows) for rows in rows_by_currency.values()),
        "currency_results": currency_results,
        "output_files": output_files,
        "errors": errors,
    }
    _emit_json_output(bool(args.json_output), output)
    _log_job_event(
        logger,
        logging.INFO,
        OPTIONS_BRONZE_BUILDER_COMMAND,
        "run_summary",
        currencies=currencies,
        elapsed_s=perf_counter() - started_at,
        errors=len(errors),
        files=len(output_files),
        rows_written=output["rows_written"],
        run_id=run_id,
        snapshot_time=output["snapshot_time"],
        status="complete" if not errors else "partial",
    )


def _select_option_ticker_prediction_universe_by_currency(
    currencies: list[str],
    explicit_instruments: list[str],
    max_instruments_per_currency: int,
) -> tuple[dict[str, list[str]], list[str]]:
    """Return explicit or summary-selected option instruments grouped by requested currency."""

    normalized_explicit = _normalize_cli_instruments(explicit_instruments)
    if normalized_explicit:
        return {"explicit": normalized_explicit}, []

    instruments_by_currency: dict[str, list[str]] = {}
    errors: list[str] = []
    for currency in currencies:
        try:
            request = resolve_options_currency_request(currency)
            rows = fetch_option_book_summary_rows(request)
            instruments_by_currency[currency] = select_option_ticker_prediction_universe(
                rows,
                max_instruments=max_instruments_per_currency,
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{currency}: {exc}")
            instruments_by_currency[currency] = []
    return instruments_by_currency, errors


def _limit_option_ticker_instruments_by_currency(
    instruments_by_currency: dict[str, list[str]],
    *,
    explicit_instruments: list[str],
    max_instruments_per_run: int,
) -> list[str]:
    """Limit selected prediction-universe instruments per requested option currency."""

    if explicit_instruments:
        return instruments_by_currency.get("explicit", [])

    selected: list[str] = []
    requested_count = max(1, max_instruments_per_run)
    for currency_instruments in instruments_by_currency.values():
        selected.extend(currency_instruments[:requested_count])
    return selected


def _run_option_instrument_ticker_bronze_builder(
    args: argparse.Namespace,
    logger: logging.Logger,
) -> None:
    """Run Deribit per-instrument option ticker collection and optional Bronze persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    explicit_instruments = _normalize_cli_instruments(cast(list[str], args.instruments))
    max_instruments_per_currency = max(1, int(args.max_instruments_per_run))
    instruments_by_currency, discovery_errors = _select_option_ticker_prediction_universe_by_currency(
        currencies=currencies,
        explicit_instruments=explicit_instruments,
        max_instruments_per_currency=max_instruments_per_currency,
    )
    run_id = option_instrument_ticker_utc_run_id()
    snapshot_time = option_instrument_ticker_snapshot_time_floor_minute()
    ingested_at = datetime.now(UTC)
    instruments = _limit_option_ticker_instruments_by_currency(
        instruments_by_currency,
        explicit_instruments=explicit_instruments,
        max_instruments_per_run=max_instruments_per_currency,
    )

    raw_rows_by_instrument, fetch_errors = _fetch_option_ticker_rows_for_instruments(instruments=instruments)
    rows, normalization_errors = normalize_option_instrument_ticker_rows(
        raw_rows_by_instrument,
        run_id=run_id,
        snapshot_time=snapshot_time,
        ingested_at=ingested_at,
        source=cast(str, args.source),
        schema_version=cast(str, args.schema_version),
    )

    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_option_instrument_ticker_snapshot_parquet_lake(
            rows=rows,
            lake_root=cast(str, args.lake_root),
        )

    errors = discovery_errors + list(fetch_errors.values()) + normalization_errors
    instruments_discovered = sum(len(currency_instruments) for currency_instruments in instruments_by_currency.values())
    currency_results = {
        currency: {
            "instruments_discovered": len(currency_instruments),
            "instruments_requested": len(
                [instrument for instrument in instruments if instrument in currency_instruments]
            ),
        }
        for currency, currency_instruments in instruments_by_currency.items()
    }
    output = {
        "command": OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "exchange": cast(str, args.exchange),
        "dataset_type": OPTION_INSTRUMENT_TICKER_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_currencies": currencies,
        "instruments_discovered": instruments_discovered,
        "instruments_requested": len(instruments),
        "currency_results": currency_results,
        "rows_written": len(rows),
        "output_files": output_files,
        "errors": errors,
    }
    _emit_json_output(bool(args.json_output), output)
    _log_job_event(
        logger,
        logging.INFO,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "run_summary",
        currencies=currencies,
        elapsed_s=perf_counter() - started_at,
        errors=len(errors),
        files=len(output_files),
        instruments_discovered=instruments_discovered,
        instruments_requested=len(instruments),
        rows_written=len(rows),
        run_id=run_id,
        snapshot_time=output["snapshot_time"],
        status="complete" if not errors else "partial",
    )


def _run_instrument_metadata_bronze_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Deribit instrument metadata snapshot collection and optional persistence."""

    started_at = perf_counter()
    currencies = _normalize_cli_currencies(cast(list[str], args.currencies))
    kind = cast(str, args.kind).strip().lower()
    include_inactive = bool(args.include_inactive)
    run_id = instrument_utc_run_id()
    ingested_at = datetime.now(UTC)
    snapshot_date = snapshot_date_utc()

    raw_rows: list[dict[str, object]] = []
    fetch_errors: list[str] = []
    for currency in currencies:
        try:
            raw_rows.extend(fetch_instruments(currency=currency, kind=kind, expired=include_inactive))
        except Exception as exc:  # noqa: BLE001
            fetch_errors.append(f"{currency}: {exc}")

    rows, normalize_errors = normalize_instrument_metadata_rows(
        raw_rows,
        run_id=run_id,
        snapshot_date=snapshot_date,
        ingested_at=ingested_at,
        source=cast(str, args.source),
        schema_version=cast(str, args.schema_version),
    )
    rows_by_date: InstrumentMetadataRowsByDate = {snapshot_date.isoformat(): rows}
    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_instrument_metadata_snapshot_parquet_lake(
            rows_by_date=rows_by_date,
            lake_root=cast(str, args.lake_root),
        )

    output = {
        "command": INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        "exchange": "deribit",
        "dataset_type": INSTRUMENT_METADATA_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_date": snapshot_date.isoformat(),
        "requested_currencies": currencies,
        "kind": kind,
        "include_inactive": include_inactive,
        "rows_written": len(rows),
        "output_files": output_files,
        "errors": fetch_errors + normalize_errors,
    }
    _emit_json_output(bool(args.json_output), output)
    error_count = len(fetch_errors) + len(normalize_errors)
    _log_job_event(
        logger,
        logging.INFO,
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
        "run_summary",
        currencies=currencies,
        elapsed_s=perf_counter() - started_at,
        errors=error_count,
        files=len(output_files),
        kind=kind,
        rows_written=len(rows),
        run_id=run_id,
        snapshot_date=snapshot_date.isoformat(),
        status="complete" if error_count == 0 else "partial",
    )


def _run_index_price_bronze_builder(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run Deribit index-price snapshot collection and optional persistence."""

    started_at = perf_counter()
    symbols = _normalize_cli_index_symbols(cast(list[str], args.symbols))
    run_id = index_utc_run_id()
    ingested_at = datetime.now(UTC)
    snapshot_time = index_snapshot_time_floor_minute()

    rows_by_symbol: IndexPriceRowsBySymbol = {}
    errors: list[str] = []
    for symbol in symbols:
        try:
            price = fetch_index_price(symbol)
            row = normalize_index_price_snapshot_row(
                index_name=symbol,
                price=price,
                run_id=run_id,
                snapshot_time=snapshot_time,
                ingested_at=ingested_at,
                source=cast(str, args.source),
                schema_version=cast(str, args.schema_version),
            )
            rows_by_symbol[symbol] = [row]
        except Exception as exc:  # noqa: BLE001
            rows_by_symbol[symbol] = []
            errors.append(f"{symbol}: {exc}")

    output_files: list[str] = []
    if bool(args.save_parquet_lake):
        output_files = save_index_price_snapshot_parquet_lake(
            rows_by_index=rows_by_symbol,
            lake_root=cast(str, args.lake_root),
        )

    output = {
        "command": INDEX_PRICE_BRONZE_BUILDER_COMMAND,
        "exchange": "deribit",
        "dataset_type": INDEX_PRICE_DATASET_TYPE,
        "run_id": run_id,
        "snapshot_time": snapshot_time.isoformat().replace("+00:00", "Z"),
        "requested_symbols": symbols,
        "rows_written": sum(len(rows) for rows in rows_by_symbol.values()),
        "output_files": output_files,
        "errors": errors,
    }
    _emit_json_output(bool(args.json_output), output)
    _log_job_event(
        logger,
        logging.INFO,
        INDEX_PRICE_BRONZE_BUILDER_COMMAND,
        "run_summary",
        elapsed_s=perf_counter() - started_at,
        errors=len(errors),
        files=len(output_files),
        rows_written=output["rows_written"],
        run_id=run_id,
        snapshot_time=output["snapshot_time"],
        status="complete" if not errors else "partial",
        symbols=symbols,
    )


def _valid_top_of_book(snapshot: RawSnapshot) -> bool:
    """Return whether snapshot top-of-book values are present and ordered."""

    if not snapshot.bids or not snapshot.asks:
        return False
    top_bid = snapshot.bids[0]
    top_ask = snapshot.asks[0]
    return top_bid.price > 0 and top_ask.price > 0 and top_bid.price < top_ask.price


def _validate_symbol(exchange: str, symbol: str, depth: int) -> dict[str, object]:
    """Validate one symbol by fetching a shallow order book from one adapter."""

    adapter = source_adapter_for_exchange(exchange)
    normalized_symbol = adapter.normalize_symbol(symbol)
    try:
        snapshot = adapter.fetch_snapshot(symbol=symbol, depth=depth)
        valid_book = _valid_top_of_book(snapshot)
        return {
            "symbol": symbol,
            "normalized_symbol": snapshot.symbol,
            "valid_book": valid_book,
            "bid_levels": len(snapshot.bids),
            "ask_levels": len(snapshot.asks),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "symbol": symbol,
            "normalized_symbol": normalized_symbol,
            "valid_book": False,
            "bid_levels": 0,
            "ask_levels": 0,
            "error": str(exc),
        }


def _run_validate_symbols(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run symbol alias and order book validation."""

    symbols = _normalize_cli_symbols(cast(list[str], args.symbols))
    depth = int(args.levels)
    if depth <= 0:
        raise ValueError("levels must be positive")

    exchange = cast(str, args.exchange)
    results = [_validate_symbol(exchange=exchange, symbol=symbol, depth=depth) for symbol in symbols]
    output = {
        "exchange": exchange,
        "symbols": results,
        "all_valid": all(bool(item["valid_book"]) for item in results),
    }
    _emit_json_output(bool(args.json_output), output)
    _log_job_event(
        logger,
        logging.INFO,
        VALIDATE_SYMBOLS_COMMAND,
        "run_summary",
        all_valid=output["all_valid"],
        exchange=args.exchange,
        status="complete",
        symbols=symbols,
    )


def _handle_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _run_bronze_builder(args=args, logger=logger, config=config)


def _handle_options_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _run_options_bronze_builder(args=args, logger=logger, config=config)


def _handle_option_instrument_ticker_bronze_builder(
    args: argparse.Namespace,
    logger: logging.Logger,
    config: Config,
) -> None:
    _ = config
    _run_option_instrument_ticker_bronze_builder(args=args, logger=logger)


def _handle_instrument_metadata_bronze_builder(
    args: argparse.Namespace, logger: logging.Logger, config: Config
) -> None:
    _ = config
    _run_instrument_metadata_bronze_builder(args=args, logger=logger)


def _handle_index_price_bronze_builder(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _ = config
    _run_index_price_bronze_builder(args=args, logger=logger)


def _handle_validate_symbols(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    _ = config
    _run_validate_symbols(args=args, logger=logger)


def command_handlers() -> dict[str, CommandHandler]:
    """Return command handler registry used by CLI dispatch."""

    return {
        BRONZE_BUILDER_COMMAND: _handle_bronze_builder,
        OPTIONS_BRONZE_BUILDER_COMMAND: _handle_options_bronze_builder,
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND: _handle_option_instrument_ticker_bronze_builder,
        INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND: _handle_instrument_metadata_bronze_builder,
        INDEX_PRICE_BRONZE_BUILDER_COMMAND: _handle_index_price_bronze_builder,
        VALIDATE_SYMBOLS_COMMAND: _handle_validate_symbols,
    }


def dispatch_command(args: argparse.Namespace, logger: logging.Logger, config: Config) -> None:
    """Dispatch one parsed command to its registered handler."""

    _enable_debug_logging(args=args, logger=logger)
    handlers = command_handlers()
    command = cast(str, args.command)
    _log_job_event(
        logger,
        logging.DEBUG,
        command,
        "dispatch",
        args={key: value for key, value in sorted(vars(args).items()) if key != "command"},
    )
    handler = handlers.get(command)
    if handler is None:
        raise ValueError(f"Unsupported command '{command}'")
    handler(args, logger, config)


def main() -> None:
    """CLI entrypoint."""

    config = load_config()
    parser = build_parser(config)
    args = parser.parse_args()
    logger = configure_logging(module_name=str(args.command), config=config)
    dispatch_command(args=args, logger=logger, config=config)


if __name__ == "__main__":
    main()
