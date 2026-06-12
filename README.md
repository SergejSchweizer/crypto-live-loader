# crypto-live-loader

Deterministic Deribit market-data ingestion and feature pipeline for quantitative research and production analytics.

## Table of Contents
- [1. Executive Summary](#1-executive-summary)
- [2. System Scope and Guarantees](#2-system-scope-and-guarantees)
- [3. Data Lineage](#3-data-lineage)
- [4. Architecture](#4-architecture)
- [5. Environment and Installation](#5-environment-and-installation)
- [6. Configuration Model](#6-configuration-model)
- [7. CLI Runbook](#7-cli-runbook)
- [8. Dataset Specifications](#8-dataset-specifications)
- [9. Incremental Processing and Idempotency](#9-incremental-processing-and-idempotency)
- [10. Data Quality and Validation](#10-data-quality-and-validation)
- [11. Observability and Logging](#11-observability-and-logging)
- [12. Scheduling and Overnight Operations](#12-scheduling-and-overnight-operations)
- [13. Quality Gates](#13-quality-gates)
- [14. Risk Notes and Limitations](#14-risk-notes-and-limitations)
- [15. Roadmap](#15-roadmap)

## 1. Executive Summary

`crypto-live-loader` provides a reproducible Bronze -> Silver -> Gold market-data stack centered on Deribit public REST endpoints.

Primary use cases for a quant desk:
- Build and maintain continuous raw market-history snapshots.
- Generate model-ready feature layers from raw order-book and options surfaces.
- Produce deterministic aggregated datasets with lineage and state-based incremental recomputation.

## 2. System Scope and Guarantees

In scope:
- Deribit L2 perpetual order-book ingestion (`l2_snapshot`).
- Deribit options ticker-chain ingestion (`options_ticker_snapshot_1m`).
- Deribit instrument metadata ingestion (`instrument_metadata_snapshot_daily`).
- Deribit index-price ingestion (`index_price_snapshot_1m`).
- Polars-based Silver feature transforms.
- Gold aggregation layers and artifact emission.

Operational guarantees:
- Deterministic partitioning and typed normalization.
- Idempotent upsert semantics for datasets using shared parquet upsert repository.
- Incremental rebuild skipping when source fingerprints are unchanged.
- Explicit state files for transform provenance.

Out of scope:
- Exchange private endpoints.
- Tick-stream websocket collectors.
- Remote metadata catalogs and distributed locking.

## 3. Data Lineage

```text
Deribit REST endpoints
  -> Bronze raw snapshots (partitioned parquet)
  -> Silver feature datasets (monthly parquet + optional artifacts)
  -> Gold aggregates (parquet + optional artifacts)
```

Endpoint lineage:
- `public/get_order_book` -> `l2_snapshot` -> `l2_snapshot_features` -> `l2_m1_features`
- `public/get_book_summary_by_currency` -> `options_ticker_snapshot_1m` -> `option_chain_features_1m` -> `option_surface_m1`
- `public/get_instruments` -> `instrument_metadata_snapshot_daily` -> `instrument_metadata_snapshot_features_daily` -> `instrument_metadata_daily_summary`
- `public/get_index_price` -> `index_price_snapshot_1m` -> `index_price_snapshot_features_1m` -> `index_price_m1_features`

## 4. Architecture

```text
api/
  cli.py         command parsing, orchestration, output contracts
  runtime.py     logging and runtime settings
  constants.py   command names

ingestion/
  * bronze normalizers/writers
  * silver transforms
  * gold transforms
  * state, parquet, plotting, and artifact utilities

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
- Lake roots: Bronze/Silver/Gold paths
- Per-domain settings: `ingestion.options.*`, `ingestion.instrument_metadata.*`, `ingestion.index_price.*`

Important behavior:
- `max_runtime_s = 0` disables L2 runtime budget.
- `options-bronze-builder` currently defaults to saving parquet output.

## 7. CLI Runbook

### Bronze

```bash
python main.py l2-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
python main.py options-bronze-builder --debug --symbols BTC ETH SOL --save-parquet-lake
python main.py instrument-metadata-bronze-builder --debug --symbols BTC ETH SOL --kind option
python main.py index-price-bronze-builder --debug --symbols btc_usd eth_usd sol_usdc
```

### Silver

```bash
python main.py l2-silver-builder --debug
python main.py options-silver-builder --debug
python main.py instrument-metadata-silver-builder --debug
python main.py index-price-silver-builder --debug
```

### Gold

```bash
python main.py l2-gold-builder --debug --plot --fill-missing-minutes --fill-policy kalman
python main.py options-gold-builder --debug --plot --fill-missing-minutes --fill-policy kalman
python main.py instrument-metadata-gold-builder --debug --plot --fill-missing-minutes --fill-policy kalman
python main.py index-price-gold-builder --debug --plot --fill-missing-minutes --fill-policy kalman
```

### Utility

```bash
python main.py validate-symbols --debug --symbols BTC ETH SOL
```

## 8. Dataset Specifications

### 8.1 Summary Matrix

| Dataset | Layer | Related endpoint | Tracked range | Silver transformation |
|---|---|---|---|---|
| `l2_snapshot` | Bronze | Deribit `public/get_order_book` | Tick snapshots (`event_time`) | Input to `l2_snapshot_features` |
| `options_ticker_snapshot_1m` | Bronze | Deribit `public/get_book_summary_by_currency` | Minute snapshots (`snapshot_time`) | Input to `option_chain_features_1m` |
| `instrument_metadata_snapshot_daily` | Bronze | Deribit `public/get_instruments` | Daily snapshots (`snapshot_date`) | Input to `instrument_metadata_snapshot_features_daily` |
| `index_price_snapshot_1m` | Bronze | Deribit `public/get_index_price` | Minute snapshots (`snapshot_time`/`event_time`) | Input to `index_price_snapshot_features_1m` |
| `l2_snapshot_features` | Silver | Derived from `l2_snapshot` | Snapshot cadence, monthly partitions | Spread/microprice/depth-window feature engineering |
| `option_chain_features_1m` | Silver | Derived from `options_ticker_snapshot_1m` | Minute chain state, monthly partitions | Contract parsing + tenor/moneyness/spread quality features |
| `instrument_metadata_snapshot_features_daily` | Silver | Derived from `instrument_metadata_snapshot_daily` | Daily, monthly partitions | Adds `month`, `is_option`, `days_to_expiration` |
| `index_price_snapshot_features_1m` | Silver | Derived from `index_price_snapshot_1m` | Minute, monthly partitions | Adds return features (`price_prev`, `price_delta`, `log_return_1m`) |
| `l2_m1_features` | Gold | Derived from Silver L2 features | Dense 1-minute timeline per partition | Consumes Silver output |
| `option_surface_m1` | Gold | Derived from Silver option features | Minute surface summaries | Consumes Silver output |
| `instrument_metadata_daily_summary` | Gold | Derived from Silver metadata features | Daily summaries | Consumes Silver output |
| `index_price_m1_features` | Gold | Derived from Silver index features | Minute summaries | Consumes Silver output |

### 8.2 Storage Layouts

Bronze root:

```text
lake/bronze/
  dataset_type=l2_snapshot/exchange=<exchange>/instrument_type=perp/symbol=<symbol>/depth=<depth>/source=<source>/year=YYYY/month=MM/date=DD/data.parquet
  dataset_type=options_ticker_snapshot_1m/exchange=<exchange>/instrument_type=option/currency=<currency>/source=<source>/year=YYYY/month=MM/date=DD/data.parquet
  dataset_type=instrument_metadata_snapshot_daily/exchange=<exchange>/year=YYYY/month=MM/date=DD/data.parquet
  dataset_type=index_price_snapshot_1m/exchange=<exchange>/index_name=<index_name>/year=YYYY/month=MM/date=DD/data.parquet
```

Silver root:

```text
lake/silver/
  dataset_type=l2_snapshot_features/...
  dataset_type=option_chain_features_1m/...
  dataset_type=instrument_metadata_snapshot_features_daily/...
  dataset_type=index_price_snapshot_features_1m/...
```

Gold root:

```text
lake/gold/
  dataset_type=l2_m1_features/...
  dataset_type=option_surface_m1/...
  dataset_type=instrument_metadata_daily_summary/...
  dataset_type=index_price_m1_features/...
```

Artifact behavior:
- Silver L2 and options writers can emit monthly parquet plus optional `.json` manifests and `.png` profiles.
- Gold L2 and options writers can emit parquet plus optional `.json` manifests and `.png` profiles.
- Gold instrument-metadata and index-price writers emit `data.parquet` and, when `--plot` is enabled, `data.png`.
- Gold readers ignore hidden writer scratch parquet paths such as `.staging-*.parquet`.

## 9. Incremental Processing and Idempotency

State files are used to avoid redundant recomputation:
- Silver: fingerprint Bronze inputs and skip unchanged sets.
- Gold: fingerprint Silver inputs and skip unchanged sets.

Idempotency behavior:
- Upsert-based datasets merge by natural keys and deterministic sort order.
- Options Bronze merges rows by snapshot natural key into one parquet file per daily partition.

## 10. Data Quality and Validation

Built-in validation examples:
- L2 `validate-symbols` checks symbol normalization and top-of-book sanity.
- Silver L2 emits `is_valid` and `validation_flags`.
- Silver options emits `quality_flags` and `is_valid_for_surface`.

Gold L2 quality controls:
- Dense minute timeline with explicit missing-minute representation.
- Coverage fields: `snapshot_count`, `coverage_ratio`, `is_complete_minute`.
- Optional fill policies: `neighbor`, `hybrid`, `kalman`.

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
# disabled 2026-06-12 bronze-only: 15 15 * * * cd /home/vcs/git/crypto-live-loader && nice -n 10 .venv/bin/python main.py l2-silver-builder --debug --no-plot --no-manifest
# disabled 2026-06-12 bronze-only: 15 16 * * * cd /home/vcs/git/crypto-live-loader && nice -n 10 .venv/bin/python main.py options-silver-builder --debug --no-plot --no-manifest
# disabled 2026-06-12 bronze-only: 15 19 * * * cd /home/vcs/git/crypto-live-loader && nice -n 10 .venv/bin/python main.py instrument-metadata-silver-builder --debug
# disabled 2026-06-12 bronze-only: 15 20 * * * cd /home/vcs/git/crypto-live-loader && nice -n 10 .venv/bin/python main.py index-price-silver-builder --debug
# disabled 2026-06-12 bronze-only: 15 17 * * * cd /home/vcs/git/crypto-live-loader && nice -n 10 .venv/bin/python main.py l2-gold-builder --debug --plot --no-manifest --fill-missing-minutes --fill-policy kalman
# disabled 2026-06-12 bronze-only: 15 18 * * * cd /home/vcs/git/crypto-live-loader && nice -n 10 .venv/bin/python main.py options-gold-builder --debug --plot --no-manifest --fill-missing-minutes --fill-policy kalman
# disabled 2026-06-12 bronze-only: 15 21 * * * cd /home/vcs/git/crypto-live-loader && nice -n 10 .venv/bin/python main.py instrument-metadata-gold-builder --debug --plot --no-manifest --fill-missing-minutes --fill-policy kalman
# disabled 2026-06-12 bronze-only: 15 22 * * * cd /home/vcs/git/crypto-live-loader && nice -n 10 .venv/bin/python main.py index-price-gold-builder --debug --plot --no-manifest --fill-missing-minutes --fill-policy kalman
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
- REST polling (no websocket microburst capture).
- Local parquet storage and local state (no distributed metadata service).
- Concurrent writers targeting same artifact can race if launched unsafely.

## 15. Roadmap

- Add websocket collectors for higher-frequency market state.
- Add schema migration regression tooling.
- Add replay tools for historical incident/backtest audits.
- Add multi-exchange adapters behind current source contracts.
