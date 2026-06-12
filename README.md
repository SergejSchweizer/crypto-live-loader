# crypto-live-loader

Deterministic Deribit Bronze market-data ingestion for quantitative research and production analytics.

## Table of Contents
- [1. Executive Summary](#1-executive-summary)
- [2. System Scope and Guarantees](#2-system-scope-and-guarantees)
- [3. Data Lineage](#3-data-lineage)
- [4. Architecture](#4-architecture)
- [5. Environment and Installation](#5-environment-and-installation)
- [6. Configuration Model](#6-configuration-model)
- [7. CLI Runbook](#7-cli-runbook)
- [8. Dataset Specifications](#8-dataset-specifications)
- [9. Idempotency](#9-idempotency)
- [10. Data Quality and Validation](#10-data-quality-and-validation)
- [11. Observability and Logging](#11-observability-and-logging)
- [12. Scheduling and Overnight Operations](#12-scheduling-and-overnight-operations)
- [13. Quality Gates](#13-quality-gates)
- [14. Risk Notes and Limitations](#14-risk-notes-and-limitations)
- [15. Roadmap](#15-roadmap)

## 1. Executive Summary

`crypto-live-loader` provides a reproducible Bronze-only market-data ingestion stack centered on Deribit public REST endpoints.

Primary use cases:
- Build and maintain continuous raw market-history snapshots.
- Persist raw L2, options ticker, instrument metadata, and index-price records.
- Keep raw lake writes deterministic, idempotent, and restart-safe.

## 2. System Scope and Guarantees

In scope:
- Deribit L2 perpetual order-book ingestion (`l2_snapshot`).
- Deribit options ticker-chain ingestion (`options_ticker_snapshot_1m`).
- Deribit instrument metadata ingestion (`instrument_metadata_snapshot_daily`).
- Deribit index-price ingestion (`index_price_snapshot_1m`).

Operational guarantees:
- Deterministic partitioning and typed normalization.
- Idempotent upsert semantics for Bronze parquet files.
- Bounded runtime and concurrency settings for scheduled collectors.
- Explicit debug logging for every CLI command.

Out of scope:
- Exchange private endpoints.
- Tick-stream websocket collectors.
- Derived feature, aggregation, or artifact layers.
- Remote metadata catalogs and distributed locking.

## 3. Data Lineage

```text
Deribit REST endpoints
  -> Bronze raw snapshots (partitioned parquet)
```

Endpoint lineage:
- `public/get_order_book` -> `l2_snapshot`
- `public/get_book_summary_by_currency` -> `options_ticker_snapshot_1m`
- `public/get_instruments` -> `instrument_metadata_snapshot_daily`
- `public/get_index_price` -> `index_price_snapshot_1m`

## 4. Architecture

```text
api/
  cli.py         command parsing, orchestration, output contracts
  runtime.py     logging and runtime settings
  constants.py   command names

ingestion/
  * bronze normalizers/writers
  * parquet repository and file locking
  * runtime configuration

domain/
  source contracts and typed models

sources/
  Deribit fetchers and adapter wiring
```

## 5. Environment and Installation

Prerequisites:
- Python `>=3.11`

Setup:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

Runtime-only:

```bash
pip install -e .
```

## 6. Configuration Model

Configuration source of truth:
- Runtime file: `config.yaml`
- Built-in defaults: `ingestion/config.py`

Key controls:
- Transport: `http.timeout_s`, `http.max_retries`, `http.retry_backoff_s`
- Runtime: `runtime.fetch_concurrency`, log rotation settings
- Ingestion: symbols, depth, cadence, runtime budget
- Lake root: `ingestion.lake_root`
- Per-domain settings: `ingestion.options.*`, `ingestion.instrument_metadata.*`, `ingestion.index_price.*`

Important behavior:
- `max_runtime_s = 0` disables L2 runtime budget.
- Options, instrument metadata, and index-price Bronze builders default to saving parquet output.

## 7. CLI Runbook

### Bronze

```bash
python main.py l2-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
python main.py options-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind option
python main.py index-price-bronze-builder --debug --symbols btc_usd eth_usd sol_usdc
```

### Utility

```bash
python main.py validate-symbols --debug --symbols BTC ETH SOL
```

## 8. Dataset Specifications

### 8.1 Summary Matrix

| Dataset | Layer | Related endpoint | Tracked range | Data semantics |
|---|---|---|---|---|
| `l2_snapshot` | Bronze | Deribit `public/get_order_book` | Tick snapshots (`event_time`) | Raw order-book snapshots |
| `options_ticker_snapshot_1m` | Bronze | Deribit `public/get_book_summary_by_currency` | Minute snapshots (`snapshot_time`) | Raw option ticker rows |
| `instrument_metadata_snapshot_daily` | Bronze | Deribit `public/get_instruments` | Daily snapshots (`snapshot_date`) | Raw instrument metadata rows |
| `index_price_snapshot_1m` | Bronze | Deribit `public/get_index_price` | Minute snapshots (`snapshot_time`/`event_time`) | Raw index-price rows |

### 8.2 Storage Layout

Bronze root:

```text
lake/bronze/
  dataset_type=l2_snapshot/exchange=<exchange>/instrument_type=perp/symbol=<symbol>/depth=<depth>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=options_ticker_snapshot_1m/exchange=<exchange>/instrument_type=option/currency=<currency>/source=<source>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=instrument_metadata_snapshot_daily/exchange=<exchange>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
  dataset_type=index_price_snapshot_1m/exchange=<exchange>/index_name=<index_name>/year=YYYY/month=MM/date=DD/hour=HH/data.parquet
```

## 9. Idempotency

Idempotency behavior:
- Upsert-based datasets merge by natural keys and deterministic sort order.
- L2 rows are keyed by exchange, instrument type, symbol, depth, source, and event time.
- Options rows are keyed by exchange, currency, instrument name, source, and snapshot time.
- Instrument metadata rows are keyed by exchange, instrument name, and snapshot date.
- Index-price rows are keyed by exchange, index name, event time, and source.

## 10. Data Quality and Validation

Built-in validation examples:
- L2 `validate-symbols` checks symbol normalization and top-of-book sanity.
- Bronze normalizers reject malformed option instrument names and unsupported option currency mappings.
- External calls are bounded by configured timeout, retry, and concurrency settings.

## 11. Observability and Logging

- Every CLI command accepts `--debug` to opt into DEBUG-level logging for that run.
- Log files are module-scoped in the configured log directory.
- Log directory derives from `logfile` parent; falls back to runtime `log_dir`.
- Runtime logs use one formatter: `timestamp level module_scope logger message`.
- Job lifecycle messages use a stable key-value envelope:
  - `job_event command=<command> event=dispatch ...`
  - `job_event command=<command> event=debug_config ...`
  - `job_event command=<command> event=run_summary ...`
- Rotation controls:
  - `runtime.log_rotation_days`
  - `runtime.log_backup_count`

## 12. Scheduling and Overnight Operations

Example production cron:

```cron
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py l2-bronze-builder --debug --symbols BTC ETH SOL
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py options-bronze-builder --debug --symbols BTC ETH SOL
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py index-price-bronze-builder --debug --symbols btc_usd eth_usd sol_usdc
15 3 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind option
```

## 13. Quality Gates

```bash
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/pyright --level error
.venv/bin/pytest -q
```

Convenience target:

```bash
make check
```

## 14. Risk Notes and Limitations

- Deribit-only source support.
- REST polling only; websocket microburst capture is out of scope.
- Local parquet storage only; no distributed metadata service.
- Concurrent writers targeting the same parquet partition rely on local file locks.

## 15. Roadmap

- Add websocket collectors for higher-frequency market state.
- Add schema migration regression tooling.
- Add replay tools for historical incident/backtest audits.
- Add multi-exchange adapters behind current source contracts.
