# crypto-live-loader

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
  - [Package Dependencies](#package-dependencies)
- [Configuration](#configuration)
- [Current Jobs](#current-jobs)
- [Usage](#usage)
  - [Bronze Builder](#bronze-builder)
  - [Options Bronze Builder](#options-bronze-builder)
  - [Options Silver Builder](#options-silver-builder)
  - [Gold Builder](#gold-builder)
  - [Options Gold Builder](#options-gold-builder)
- [Modules](#modules)
- [Bronze Datasets](#bronze-datasets)
  - [Bronze Dataset: `l2_snapshot`](#bronze-dataset-l2_snapshot)
  - [Bronze Dataset: `option_ticker_snapshot_1m`](#bronze-dataset-option_ticker_snapshot_1m)
- [Dataset Catalog](#dataset-catalog)
  - [Dataset: `l2_snapshot`](#dataset-l2_snapshot)
  - [Dataset: `option_ticker_snapshot_1m`](#dataset-option_ticker_snapshot_1m)
  - [Dataset: `instrument_metadata_snapshot_daily`](#dataset-instrument_metadata_snapshot_daily)
  - [Dataset: `index_price_snapshot_1m`](#dataset-index_price_snapshot_1m)
  - [Dataset: `l2_snapshot_features`](#dataset-l2_snapshot_features)
  - [Dataset: `option_chain_features_1m`](#dataset-option_chain_features_1m)
  - [Dataset: `instrument_metadata_snapshot_features_daily`](#dataset-instrument_metadata_snapshot_features_daily)
  - [Dataset: `index_price_snapshot_features_1m`](#dataset-index_price_snapshot_features_1m)
  - [Dataset: `l2_m1_features`](#dataset-l2_m1_features)
  - [Dataset: `option_surface_m1`](#dataset-option_surface_m1)
  - [Dataset: `instrument_metadata_daily_summary`](#dataset-instrument_metadata_daily_summary)
  - [Dataset: `index_price_m1_features`](#dataset-index_price_m1_features)
- [Testing](#testing)
- [Known Limitations](#known-limitations)
- [Future Improvements](#future-improvements)

## Project Overview

`crypto-live-loader` is a focused Deribit Level 2 order book ingestion tool. It collects bounded public order book snapshots, normalizes them into typed `L2Snapshot` records, and persists raw snapshots to a local bronze Parquet lake.
It also includes Polars-based Silver and Gold transforms for fixed-width L2 snapshot features and M1 aggregate artifacts.

Current scope is intentionally narrow:

- Fetch Deribit perpetual L2 order book snapshots through the public REST API.
- Poll multiple symbols concurrently with bounded runtime controls.
- Optionally persist raw snapshots to idempotent daily bronze Parquet partitions.
- Expose ingestion and transform CLI commands: `l2-bronze-builder`, `options-bronze-builder`, `l2-silver-builder`,
  `instrument-metadata-bronze-builder`, `index-price-bronze-builder`, `options-silver-builder`, `l2-gold-builder`,
  and `options-gold-builder`.
- Validate symbol aliases before scheduled jobs with `validate-symbols`.

Former OHLCV, standalone open-interest, standalone funding, research-report, and database-ingestion surfaces have been removed.

## Architecture

```text
CLI
  -> Runtime config
  -> Async multi-symbol L2 polling
  -> Deribit public/get_order_book adapter
  -> L2Snapshot normalization
  -> Optional raw bronze Parquet lake writer
  -> JSON run output and structured logs

CLI (validate-symbols)
  -> Symbol alias normalization
  -> Shallow Deribit order book fetch
  -> Valid book report

Lake transform
  -> l2-silver-builder CLI command
  -> Polars Bronze-to-Silver snapshot feature transform
  -> Monthly silver Parquet, JSON metadata, and PNG profile artifacts
  -> l2-gold-builder CLI command
  -> Polars Silver-to-Gold M1 aggregation
  -> Optional missing-minute numeric fill from adjacent Gold minute averages
  -> Versioned full-timeframe Gold Parquet, JSON metadata, and PNG profile artifacts
  -> Incremental state files that skip unchanged Bronze/Silver inputs
```

Current top-level code layout:

```text
api/
  constants.py  # shared command names and runtime artifact names
  cli.py        # CLI parser, run orchestration, output shaping
  runtime.py    # logging and concurrency config
ingestion/
  artifact_state.py
  config.py
  http_client.py
  l2.py
  lake.py
  silver.py
  gold.py
  exchanges/deribit_l2.py
tests/
config.yaml
main.py
pyproject.toml
README.md
AGENTS.md
```

## Installation

### Prerequisites

- Python 3.11+

### Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

For runtime-only installs, use `pip install -e .`. The `dev` extra installs the pinned quality-gate tools used by `make check`.

### Package Dependencies

Runtime dependencies are declared in `pyproject.toml`:

| Package | Used For |
|---|---|
| `matplotlib` | Silver and Gold profile PNG artifact generation. |
| `pyarrow` | Bronze, Silver, and Gold Parquet file reads/writes. |
| `polars` | Bronze-to-Silver L2 features and Silver-to-Gold M1 transformations. |

Development dependencies are grouped under the `dev` extra and cover `pytest`, `ruff`, and `mypy`.

## Configuration

Runtime defaults live in tracked `config.yaml`. CLI options override these defaults for a single run.
HTTP defaults are resolved once per process so each request does not reread `config.yaml`.

Supported config keys:

| Key | Purpose |
|---|---|
| `logfile` | Base logfile path used to derive the canonical log directory. Defaults to `.logs/crypto-live-loader.log`. Runtime logs are written per module in that directory. |
| `http.timeout_s` | HTTP request timeout in seconds. Defaults to `8`. |
| `http.max_retries` | Retry count for transient request failures. Defaults to `2`. |
| `http.retry_backoff_s` | Base retry backoff in seconds. Defaults to `1`. |
| `runtime.log_dir` | Fallback log directory used only when `logfile` is unset. Defaults to repo-local `.logs/`. |
| `runtime.log_rotation_days` | Log rotation interval in days. Defaults to `7` for weekly rotation. |
| `runtime.log_backup_count` | Number of rotated logs to delete after. `0` keeps older logs indefinitely. |
| `runtime.fetch_concurrency` | Maximum concurrent symbol fetches per polling tick. |
| `ingestion.exchange` | Exchange name. Currently only `deribit`. |
| `ingestion.symbols` | Symbol list. |
| `ingestion.levels` | Requested order book depth per side. |
| `ingestion.snapshot_count` | Number of polling ticks per symbol. Defaults to `5`. |
| `ingestion.poll_interval_s` | Sleep interval between polling ticks. Defaults to `10` seconds. |
| `ingestion.max_runtime_s` | Optional runtime budget. Defaults to `50` seconds. `0` disables the budget. |
| `ingestion.save_parquet_lake` | Save raw L2 snapshots to the bronze Parquet lake when true. |
| `ingestion.lake_root` | Parquet lake root directory. |
| `ingestion.silver_lake_root` | Silver artifact lake root directory. Defaults to `lake/silver`. |
| `ingestion.gold_lake_root` | Gold artifact root directory. Defaults to `lake/gold`. |
| `ingestion.json_output` | Print CLI JSON output when true. |
| `ingestion.options.currencies` | Option currencies for options Bronze runs. Defaults to `["BTC", "ETH", "SOL"]`. |
| `ingestion.options.fetch_concurrency` | Max concurrent options-currency fetches per run. Defaults to `3`. |
| `ingestion.options.save_parquet_lake` | Save raw options snapshots to bronze Parquet when true. |
| `ingestion.options.lake_root` | Bronze root for options snapshots. Defaults to `lake/bronze`. |
| `ingestion.options.source` | Source marker written to options bronze rows. Defaults to `rest_get_book_summary_by_currency`. |
| `ingestion.options.schema_version` | Options bronze schema version label. Defaults to `v1`. |
| `ingestion.options.json_output` | Print options builder JSON output when true. |

## Current Jobs

The production cron setup runs Bronze builders every minute, metadata daily, and downstream Silver/Gold on daily staggered windows from the repository root:

```cron
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py l2-bronze-builder --symbols BTC ETH SOL
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py options-bronze-builder --symbols BTC ETH SOL
* * * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py index-price-bronze-builder --symbols btc_usd eth_usd sol_usdc
15 3 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py instrument-metadata-bronze-builder --symbols BTC ETH SOL --kind option
15 15 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py l2-silver-builder --plot --manifest
15 16 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py options-silver-builder --plot --manifest
15 19 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py instrument-metadata-silver-builder
15 20 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py index-price-silver-builder
15 17 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py l2-gold-builder --plot --manifest --fill-missing-minutes --fill-policy kalman
15 18 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py options-gold-builder --plot --manifest --fill-missing-minutes --fill-policy kalman
15 21 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py instrument-metadata-gold-builder
15 22 * * * cd /home/vcs/git/crypto-live-loader && .venv/bin/python main.py index-price-gold-builder
```

| Job | Purpose | Reads | Writes | Log File |
|---|---|---|---|---|
| `l2-bronze-builder` | Poll Deribit REST L2 snapshots for BTC, ETH, and SOL. | Deribit public order book API. | Daily Bronze Parquet partitions under `lake/bronze/`. | Module-specific `.log` file under configured log directory |
| `options-bronze-builder` | Fetch Deribit options chain ticker snapshots for BTC/ETH/SOL and write raw Bronze rows. | Deribit `public/get_book_summary_by_currency` (SOL via `USDC` filtered to `SOL_USDC-`). | Daily Bronze Parquet partitions under `lake/bronze/dataset_type=option_ticker_snapshot_1m/`. | Module-specific `.log` file under configured log directory |
| `instrument-metadata-bronze-builder` | Fetch Deribit instrument metadata snapshots for configured currencies/kind. | Deribit `public/get_instruments`. | Daily Bronze Parquet under `lake/bronze/dataset_type=instrument_metadata_snapshot_daily/`. | Module-specific `.log` file under configured log directory |
| `index-price-bronze-builder` | Fetch Deribit index prices for configured index symbols. | Deribit `public/get_index_price`. | Daily/monthly Bronze Parquet under `lake/bronze/dataset_type=index_price_snapshot_1m/`. | Module-specific `.log` file under configured log directory |
| `l2-silver-builder` | Transform changed Bronze snapshots into fixed-width Silver snapshot feature rows. | `lake/bronze/` | Monthly Silver Parquet, JSON metadata, PNG profile artifacts, and `_silver_transform_state.json` under `lake/silver/`. | Module-specific `.log` file under configured log directory |
| `options-silver-builder` | Transform changed Bronze options snapshots into Silver option-chain feature rows. | `lake/bronze/dataset_type=option_ticker_snapshot_1m/` | Monthly Silver Parquet, JSON metadata, PNG profile artifacts, and `_silver_options_transform_state.json` under `lake/silver/`. | Module-specific `.log` file under configured log directory |
| `instrument-metadata-silver-builder` | Transform daily Bronze instrument metadata into Silver feature snapshots. | `lake/bronze/dataset_type=instrument_metadata_snapshot_daily/` | Monthly Silver Parquet and incremental state under `lake/silver/`. | Module-specific `.log` file under configured log directory |
| `index-price-silver-builder` | Transform Bronze index price snapshots into Silver minute features. | `lake/bronze/dataset_type=index_price_snapshot_1m/` | Monthly Silver Parquet and incremental state under `lake/silver/`. | Module-specific `.log` file under configured log directory |
| `l2-gold-builder` | Aggregate changed Silver symbol partitions into dense M1 Gold datasets. | `lake/silver/` | Versioned timeframe Parquet, JSON metadata, PNG profile artifacts, and `_gold_transform_state.json` under `lake/gold/`. | Module-specific `.log` file under configured log directory |
| `options-gold-builder` | Aggregate options Silver rows into minute-level Gold option surface features. | `lake/silver/dataset_type=option_chain_features_1m/` | Monthly Gold Parquet, JSON metadata, PNG profile artifacts, and `_gold_options_transform_state.json` under `lake/gold/`. | Module-specific `.log` file under configured log directory |
| `instrument-metadata-gold-builder` | Aggregate Silver instrument metadata into daily Gold summary rows. | `lake/silver/dataset_type=instrument_metadata_snapshot_features_daily/` | Gold Parquet and incremental state under `lake/gold/`. | Module-specific `.log` file under configured log directory |
| `index-price-gold-builder` | Aggregate Silver index price features into Gold minute summaries. | `lake/silver/dataset_type=index_price_snapshot_features_1m/` | Gold Parquet and incremental state under `lake/gold/`. | Module-specific `.log` file under configured log directory |

Jobs do not use process-level serialization, so overlapping scheduled launches are allowed. Logs rotate weekly, and rotated logs are kept indefinitely by default.

## Usage

### Bronze Builder

```text
python main.py l2-bronze-builder [options]
```

| Option | Meaning |
|---|---|
| `--exchange {deribit}` | Exchange adapter to use. Only `deribit` is currently supported. |
| `--symbols SYMBOLS [SYMBOLS ...]` | Symbols to fetch. Accepts space-separated or comma-separated values. |
| `--levels LEVELS` | Number of order book levels per side. |
| `--snapshot-count SNAPSHOT_COUNT` | Polling ticks per symbol. Defaults to `5`. |
| `--poll-interval-s POLL_INTERVAL_S` | Sleep interval between polling ticks. Defaults to `10` seconds. |
| `--lake-root LAKE_ROOT` | Root directory for optional Parquet output. |
| `--max-runtime-s MAX_RUNTIME_S` | Runtime budget in seconds. `0` disables the budget. |
| `--save-parquet-lake`, `--no-save-parquet-lake` | Enable or disable Parquet persistence. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Symbols are normalized to Deribit perpetual instruments. For example, `BTC`, `BTCUSDT`, `BTCUSD`, and `BTC-PERPETUAL` resolve to `BTC-PERPETUAL`.
`SOL` resolves to Deribit's active `SOL_USDC-PERPETUAL` market.

Fetch BTC, ETH, and SOL snapshots at the default cadence of five polling ticks per run and print JSON:

```bash
python main.py l2-bronze-builder --symbols BTC ETH SOL
```

Comma-separated symbols are also accepted:

```bash
python main.py l2-bronze-builder --symbols BTC,ETH,SOL
```

Save raw snapshots to the bronze Parquet lake:

```bash
python main.py l2-bronze-builder \
  --symbols BTC ETH SOL \
  --levels 50 \
  --save-parquet-lake
```

With the defaults, each run collects five raw snapshots per symbol with a 50-second runtime budget. Bronze persistence appends those snapshots as distinct parquet rows in the symbol's daily partition; it does not aggregate them.

Runtime logs are written to module-specific files in the configured log directory (default: `.logs/`). The log directory is derived from `logfile` (its parent directory) and falls back to `runtime.log_dir` when `logfile` is unset. Logs rotate weekly and `runtime.log_backup_count: 0` keeps older rotated logs.

### Silver Builder

```text
python main.py l2-silver-builder [options]
```

| Option | Meaning |
|---|---|
| `--bronze-lake-root BRONZE_LAKE_ROOT` | Root directory for bronze Parquet input files. Defaults to `lake/bronze`. |
| `--silver-lake-root SILVER_LAKE_ROOT` | Root directory for Silver output artifact files. Defaults to `lake/silver`. |
| `--depth DEPTH` | Expected book depth used for fixed-width Silver arrays. Defaults to `50`. |
| `--plot`, `--no-plot` | Enable or suppress Silver PNG profile generation. Defaults to enabled. |
| `--manifest`, `--no-manifest` | Enable or suppress Silver JSON metadata manifest generation. Defaults to enabled. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Transform Bronze L2 snapshots into monthly Silver snapshot feature artifacts:

```bash
python main.py l2-silver-builder
```

The Silver job logs to its module-specific `.log` file in the configured log directory.
It records Bronze parquet content fingerprints in `lake/silver/_silver_transform_state.json`; subsequent runs read and merge only Bronze partition files whose content changed since the prior successful run.

Each Silver month partition writes three artifacts. The parquet file is named by month, and the metadata/plot files use the same month marker:

```text
YYYY-MM.parquet
YYYY-MM.json
YYYY-MM.png
```

### Options Bronze Builder

```text
python main.py options-bronze-builder [options]
```

| Option | Meaning |
|---|---|
| `--exchange {deribit}` | Exchange adapter to use. Currently `deribit` only. |
| `--symbols SYMBOLS [SYMBOLS ...]` | Option symbols/currencies to fetch. Supports space/comma-separated values. Legacy alias: `--currencies`. |
| `--lake-root LAKE_ROOT` | Root directory for optional options Bronze Parquet output. |
| `--save-parquet-lake`, `--no-save-parquet-lake` | Enable or disable options bronze Parquet persistence. |
| `--schema-version SCHEMA_VERSION` | Schema version marker written to rows. |
| `--source SOURCE` | Source marker written to rows. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

`SOL` is fetched via Deribit `currency=USDC` and filtered to `instrument_name` values prefixed with `SOL_USDC-`.

Example:

```bash
python main.py options-bronze-builder --symbols BTC ETH SOL --save-parquet-lake
```

### Options Silver Builder

```text
python main.py options-silver-builder [options]
```

| Option | Meaning |
|---|---|
| `--bronze-lake-root BRONZE_LAKE_ROOT` | Root directory for options bronze Parquet input files. |
| `--silver-lake-root SILVER_LAKE_ROOT` | Root directory for options Silver output files. |
| `--plot`, `--no-plot` | Enable or suppress options Silver PNG profile generation. |
| `--manifest`, `--no-manifest` | Enable or suppress options Silver JSON metadata generation. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Example:

```bash
python main.py options-silver-builder
```

### Gold Builder

```text
python main.py l2-gold-builder [options]
```

| Option | Meaning |
|---|---|
| `--silver-lake-root SILVER_LAKE_ROOT` | Root directory for Silver Parquet input files. Defaults to `lake/silver`. |
| `--gold-lake-root GOLD_LAKE_ROOT` | Root directory for Gold artifact output files. Defaults to `lake/gold`. |
| `--expected-snapshots-per-minute EXPECTED` | Expected Silver snapshots per minute for quality coverage. Defaults to `6`. |
| `--completeness-threshold THRESHOLD` | Minimum coverage ratio for a complete minute. Defaults to `0.8`. |
| `--fill-missing-minutes`, `--no-fill-missing-minutes` | Enable or disable Gold missing-minute feature filling. Defaults to disabled. |
| `--fill-policy {neighbor,hybrid,kalman}` | Fill strategy used when `--fill-missing-minutes` is enabled. Defaults to `neighbor`. |
| `--plot`, `--no-plot` | Enable or suppress Gold PNG profile generation. Defaults to enabled. |
| `--manifest`, `--no-manifest` | Enable or suppress Gold JSON metadata manifest generation. Defaults to enabled. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Transform Silver L2 snapshot features into M1 Gold artifacts:

```bash
python main.py l2-gold-builder
```

Gold is M1-only. Each dataset is materialized on the full one-minute scale from its lowest observed minute through its latest observed minute. Observed minutes use first/max/min/last/mean/std semantics inside the minute. Missing minutes are written as explicit rows with `snapshot_count = 0`, `coverage_ratio = 0.0`, `is_complete_minute = false`, `quality_flags = ["missing_minute"]`, and numeric feature values set to `NaN`; values are not forward-filled. With the default quality policy, `expected_snapshots_per_minute = 6`, `coverage_ratio = snapshot_count / 6`, and `is_complete_minute = coverage_ratio >= 0.8`.
When `--fill-missing-minutes` is enabled with `--fill-policy neighbor`, missing Gold rows keep `snapshot_count = 0`, `coverage_ratio = 0.0`, and `is_complete_minute = false`, but numeric feature values are replaced by the average of the immediately preceding and following non-missing Gold minutes. Filled rows add `filled_neighbor_average` to `quality_flags`.
When `--fill-policy hybrid` is selected, short internal gap runs are linearly interpolated (`filled_linear_interpolation`), short boundary runs are filled from the nearest observed minute (`filled_forward_boundary` or `filled_backward_boundary`), and long gaps remain missing with `missing_long_gap`.
When `--fill-policy kalman` is selected, Gold first applies the hybrid rules and then uses one-dimensional Kalman smoothing to fill long internal missing runs; successful long-gap fills are flagged as `filled_kalman_long_gap`.
The Gold builder records Silver parquet content fingerprints by symbol in `lake/gold/_gold_transform_state.json`. Unchanged symbols are skipped; changed symbols are rebuilt from all Silver files for that symbol so each emitted Gold artifact remains a full timeframe dataset.

For each exchange, instrument type, base asset, source symbol, depth, and timeframe, Gold writes a full versioned dataset at the `timeframe=1m` leaf. The three artifacts share the same reproducibility basename:

```text
lake/gold/dataset_type=l2_m1_features/feature_set_version=gold_l2_m1_v1/exchange=deribit/instrument_type=perp/base_asset=BTC/symbol=BTC-PERPETUAL/depth=50/timeframe=1m/BTC_L2_<jsonhash>_<gitcommithash>.parquet
lake/gold/dataset_type=l2_m1_features/feature_set_version=gold_l2_m1_v1/exchange=deribit/instrument_type=perp/base_asset=BTC/symbol=BTC-PERPETUAL/depth=50/timeframe=1m/BTC_L2_<jsonhash>_<gitcommithash>.json
lake/gold/dataset_type=l2_m1_features/feature_set_version=gold_l2_m1_v1/exchange=deribit/instrument_type=perp/base_asset=BTC/symbol=BTC-PERPETUAL/depth=50/timeframe=1m/BTC_L2_<jsonhash>_<gitcommithash>.png
```

The JSON metadata contains dataset-level and feature-level metadata, including dataset type, feature-set version, timeframe, hash string, UTC build timestamp, row/column stats, timestamp bounds, missing-minute counts, source Silver dataset summaries, source fingerprint hash, Gold content hash, and per-feature dtype/null/NaN/numeric distribution stats. It intentionally does not store filesystem paths.

The PNG profile uses the same basename. It plots all numeric Gold features as feature rows: line plots on the left panel and dark distribution histograms on the right panel. Line plots use at most 3,000 evenly spaced rows representing the full time scale; Parquet and JSON artifacts keep the complete dataset. Missing minutes are visible as broken line segments caused by `NaN` values and as red shaded spans. The plot header carries key manifest metadata, while each feature subplot shows only a compact left-side metadata window with feature name, time range, and row statistics.

### Gold Profile Example

The BTC-PERPETUAL `1m` profile example (Deribit perp, depth `50`) is the expected shape for a fully generated Gold PNG:

![Gold 1m BTC profile example](docs/assets/gold_profile_btc_1m_example.png)

- Header block:
  - dataset identity (`exchange`, `symbol`, `instrument_type`, `depth`)
  - reproducibility fields (`hash`, git commit prefix, timeframe window)
  - row counts and missing-minute count
- Left column:
  - one time-series panel per Gold numeric feature (for example `mid_open`, `mid_close`, `spread_bps_mean`, depth-window pressure/imbalance, and market carry fields)
  - the line should remain continuous where rows were successfully filled
  - red shaded spans indicate **unfilled** missing minutes only
- Right column:
  - per-feature histograms for value distribution and range sanity checks
- Typical visual cues in this example:
  - price-family panels (`mid_*`, `microprice_*`) move together with short-term spikes
  - spread and imbalance panels show bursty microstructure behavior with sparse extremes
  - depth-volume means are right-skewed with occasional tall bursts

Use this profile view as a fast QA pass:

- If red spans persist in dense periods, inspect fill policy and run logs.
- If one feature remains fragmented while peers are smooth, inspect source-value finiteness for that feature.
- If histogram mass shifts abruptly across runs, compare source fingerprint/hash and recent market regime.

The Gold job logs to its module-specific `.log` file in the configured log directory and allows overlapping transform launches.

### Options Gold Builder

```text
python main.py options-gold-builder [options]
```

| Option | Meaning |
|---|---|
| `--silver-lake-root SILVER_LAKE_ROOT` | Root directory for options Silver Parquet input files. |
| `--gold-lake-root GOLD_LAKE_ROOT` | Root directory for options Gold artifact output files. |
| `--fill-missing-minutes`, `--no-fill-missing-minutes` | Enable or disable options Gold missing-minute filling mode. Defaults to disabled. |
| `--fill-policy {neighbor,hybrid,kalman}` | Fill policy recorded for options Gold runs when filling mode is enabled. Defaults to `neighbor`. |
| `--plot`, `--no-plot` | Enable or suppress options Gold PNG profile generation. |
| `--manifest`, `--no-manifest` | Enable or suppress options Gold JSON metadata generation. |
| `--json-output`, `--no-json-output` | Enable or suppress JSON output. Logs are still emitted. |

Example:

```bash
python main.py options-gold-builder

python main.py options-gold-builder --fill-missing-minutes --fill-policy kalman
```

Validate symbols before adding them to cron:

```bash
python main.py validate-symbols --symbols BTC ETH SOL
```

The Parquet layout is:

```text
lake/bronze/
  dataset_type=l2_snapshot/
    exchange=deribit/
      instrument_type=perp/
        symbol=BTC-PERPETUAL/
          depth=50/
            source=rest_order_book/
              month=YYYY-MM/
                date=YYYY-MM-DD/
                  data.parquet
```

The Silver artifact layout is monthly to avoid unnecessary small-file fragmentation:

```text
lake/silver/
  dataset_type=l2_snapshot_features/
    exchange=deribit/
      instrument_type=perp/
        symbol=BTC-PERPETUAL/
          month=YYYY-MM/
            YYYY-MM.parquet
            YYYY-MM.json
            YYYY-MM.png
```

The Gold artifact layout is partitioned to the full versioned timeframe dataset level:

```text
lake/gold/
  dataset_type=l2_m1_features/
    feature_set_version=gold_l2_m1_v1/
      exchange=deribit/
        instrument_type=perp/
          base_asset=BTC/
            symbol=BTC-PERPETUAL/
              depth=50/
                timeframe=1m/
                  BTC_L2_<jsonhash>_<gitcommithash>.parquet
                  BTC_L2_<jsonhash>_<gitcommithash>.json
                  BTC_L2_<jsonhash>_<gitcommithash>.png
          base_asset=ETH/
            symbol=ETH-PERPETUAL/
              depth=50/
                timeframe=1m/
                  ETH_L2_<jsonhash>_<gitcommithash>.parquet
                  ETH_L2_<jsonhash>_<gitcommithash>.json
                  ETH_L2_<jsonhash>_<gitcommithash>.png
```

## Modules

| Module | Responsibility |
|---|---|
| `api/constants.py` | Shared command names and runtime artifact paths. |
| `api/cli.py` | CLI parsing, builder orchestration, JSON output, parquet/artifact dispatch, and run logging. |
| `api/runtime.py` | Logging setup and concurrency config. |
| `ingestion/artifact_state.py` | Content fingerprints and atomic JSON state files for incremental transforms. |
| `ingestion/config.py` | Deterministic `config.yaml` loading and typed config accessors. |
| `ingestion/http_client.py` | Minimal JSON HTTP client with retries and cached per-process default settings. |
| `ingestion/exchanges/deribit_l2.py` | Deribit order book adapter and symbol normalization. |
| `ingestion/l2.py` | L2 dataclasses, async polling, and snapshot normalization. |
| `ingestion/lake.py` | Idempotent Parquet writer for raw L2 snapshots. |
| `sources/deribit_options.py` | Deribit options chain summary fetcher and currency mapping rules. |
| `ingestion/options.py` | Options bronze normalization and lightweight row validation. |
| `ingestion/options_lake.py` | Partitioned bronze Parquet writer for options snapshot rows. |
| `ingestion/options_silver.py` | Polars options Bronze-to-Silver transform and monthly artifact writer. |
| `ingestion/options_gold.py` | Polars options Silver-to-Gold option-surface transform and monthly artifact writer. |
| `ingestion/silver.py` | Polars Bronze-to-Silver transform and monthly Silver artifact writer. |
| `ingestion/gold.py` | Polars Silver-to-Gold dense M1 aggregation, versioned timeframe artifact writing, metadata, and PNG profiles. |

## Bronze Datasets

Bronze is fetch-plus-persist only. No feature engineering, interpolation, surface fitting, or Greek computation is done at this layer.

### Bronze Dataset: `l2_snapshot`

Source: Deribit `public/get_order_book` (REST polling)

Partition layout:

```text
lake/bronze/
  dataset_type=l2_snapshot/
    exchange=deribit/
      instrument_type=perp/
        symbol=<instrument>/
          depth=<requested_depth>/
            source=rest_order_book/
              month=YYYY-MM/
                date=YYYY-MM-DD/
                  data.parquet
```

| Column | Meaning | Why it is important |
|---|---|---|
| `schema_version` | Bronze row schema version (currently `v1`). | Keeps schema evolution explicit and reproducible across runs. |
| `dataset_type` | Dataset identifier (`l2_snapshot`). | Distinguishes this raw order-book feed from other Bronze datasets. |
| `exchange` | Venue identifier (`deribit`). | Preserves source lineage for multi-exchange compatibility later. |
| `instrument_type` | Instrument class (`perp`). | Keeps contract family explicit for downstream grouping and model scope. |
| `symbol` | Canonical Deribit instrument name (for example `BTC-PERPETUAL`). | Primary identity key for per-instrument time series and partitions. |
| `event_time` | Exchange event timestamp for this snapshot. | Core market-time axis for Silver/Gold aggregation and backtesting order. |
| `ingested_at` | UTC time this pipeline wrote the row. | Enables latency and ingestion-quality diagnostics. |
| `run_id` | Unique ingestion-run identifier. | Traceability for debugging run-level issues and replay. |
| `source` | Source channel marker (`rest_order_book`). | Distinguishes collection channel if additional collectors are added later. |
| `depth` | Requested book depth per side. | Defines expected array widths and feature comparability in Silver. |
| `bids` | Raw bid ladder as ordered `(price, amount)` levels. | Preserves full microstructure needed for depth/imbalance features. |
| `asks` | Raw ask ladder as ordered `(price, amount)` levels. | Same as bids; required for spread, microprice, and pressure metrics. |
| `mark_price` | Deribit mark price at snapshot time. | Stable reference price used by downstream quality and carry analysis. |
| `index_price` | Deribit index price at snapshot time. | Spot-style anchor for basis/funding context. |
| `open_interest` | Deribit open interest value in payload. | Positioning/liquidity context for market regime diagnostics. |
| `funding_8h` | 8-hour funding metric from Deribit payload. | Carry signal component for perp market behavior. |
| `current_funding` | Current funding rate from Deribit payload. | Short-horizon carry and pressure context in Silver/Gold features. |

### Bronze Dataset: `option_ticker_snapshot_1m`

Source: Deribit `public/get_book_summary_by_currency` with `kind=option`

Currency mapping:
- `BTC` -> `currency=BTC`
- `ETH` -> `currency=ETH`
- `SOL` -> `currency=USDC` and keep only `instrument_name` prefixed by `SOL_USDC-`

Partition layout:

```text
lake/bronze/
  dataset_type=option_ticker_snapshot_1m/
    exchange=deribit/
      instrument_type=option/
        currency=<BTC|ETH|SOL>/
          source=rest_get_book_summary_by_currency/
            date=YYYY-MM-DD/
              part-<run_id>.parquet
```

| Column | Meaning | Why it is important |
|---|---|---|
| `schema_version` | Bronze row schema version (currently `v1`). | Keeps options schema changes controlled and auditable. |
| `dataset_type` | Dataset identifier (`option_ticker_snapshot_1m`). | Prevents mixing chain snapshots with perp L2 snapshots. |
| `exchange` | Venue identifier (`deribit`). | Source lineage and multi-venue compatibility. |
| `source` | Source channel marker (`rest_get_book_summary_by_currency`). | Reproducibility of fetch path and endpoint provenance. |
| `currency` | Logical analysis currency (`BTC`, `ETH`, `SOL`). | Stable partition key for downstream option-surface research. |
| `requested_currency` | Currency requested by CLI/config. | Verifies user intent and supports run-level diagnostics. |
| `source_currency` | Currency sent to Deribit endpoint (`BTC`, `ETH`, or `USDC` for SOL). | Critical for SOL mapping traceability and auditability. |
| `instrument_name` | Deribit option instrument id (for example `BTC-30JUN26-120000-C`). | Contract identity used to parse expiry/strike/type in Silver. |
| `base_currency` | Base currency returned by Deribit row. | Extra contract context and integrity check against expected currency. |
| `quote_currency` | Quote currency returned by Deribit row. | Pricing denomination context and contract validation support. |
| `instrument_type` | Instrument class (`option`). | Keeps downstream transforms scoped to option contracts only. |
| `snapshot_time` | Pipeline-observed snapshot timestamp (minute-aligned). | Primary backtest/feature time axis for chain-state observations. |
| `exchange_creation_time` | Deribit row `creation_timestamp` converted to datetime. | Exchange-side freshness/lag checks against pipeline observation time. |
| `ingested_at` | UTC time row was persisted. | End-to-end ingestion timing and operational observability. |
| `run_id` | Unique ingestion-run identifier. | Run traceability and partial-failure diagnosis. |
| `bid_price` | Best bid price from summary row. | Liquidity and tradability signal; spread computation input. |
| `ask_price` | Best ask price from summary row. | Liquidity and tradability signal; spread computation input. |
| `mid_price` | Mid quote from summary row if provided. | Fair-value proxy for pricing and sanity checks. |
| `mark_price` | Deribit mark price for contract. | Main non-noisy option price anchor used in analytics. |
| `mark_iv` | Implied volatility of mark price (option-specific). | Core volatility-surface signal for Silver/Gold features. |
| `underlying_price` | Underlying reference price in row. | Required for moneyness/log-moneyness and surface alignment. |
| `underlying_index` | Deribit underlying index label. | Reference-series provenance and cross-checking. |
| `interest_rate` | Option-specific interest-rate field from Deribit. | Needed later for model-based IV/Greek workflows. |
| `open_interest` | Contract open interest from summary row. | Liquidity/participation proxy and surface quality context. |
| `volume` | Contract traded volume in native units. | Activity and liquidity context for filtering and weighting. |
| `volume_usd` | Contract traded volume in USD terms. | Cross-currency comparable activity metric. |
| `high` | Session high price from summary row. | Intraday range context and sanity checks. |
| `low` | Session low price from summary row. | Intraday range context and sanity checks. |
| `last` | Last traded price from summary row. | Observed traded price anchor when quotes are sparse. |
| `price_change` | Session price change metric from summary row. | Regime/momentum context for exploratory analysis. |
| `raw_payload_hash` | Deterministic hash of raw row payload. | Row-level reproducibility and tamper/change detection. |

## Dataset Catalog

This section documents each persisted dataset with explicit column contracts, column meaning, and why each field matters operationally.

### Dataset: `l2_snapshot`
Layer: Bronze

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version` | Dataset schema label (`v1`). | Controlled schema evolution and reproducibility. |
| `dataset_type` | Dataset identity (`l2_snapshot`). | Prevents cross-dataset mixing in downstream transforms. |
| `exchange` | Venue identifier (`deribit`). | Source lineage and multi-venue extensibility. |
| `symbol` | Canonical instrument (for example `BTC-PERPETUAL`). | Primary instrument key for partitioning and joins. |
| `instrument_type` | Instrument class (`perp`). | Keeps analytics scoped to comparable contract families. |
| `event_time` | Exchange snapshot timestamp. | True market-time axis for feature and bar aggregation. |
| `ingested_at` | Pipeline write timestamp in UTC. | Latency and freshness diagnostics. |
| `run_id` | Unique run identifier. | End-to-end traceability and replay auditing. |
| `source` | Collection channel (`rest_order_book`). | Distinguishes payload provenance. |
| `depth` | Requested levels per side. | Defines expected array widths and comparability. |
| `fetch_duration_s` | Per-call wall-clock duration. | Runtime performance and incident diagnostics. |
| `bids` | Ordered bid levels (`price`, `amount`). | Required for depth/imbalance/microprice features. |
| `asks` | Ordered ask levels (`price`, `amount`). | Required for spread and pressure signals. |
| `mark_price` | Deribit mark price. | Stable valuation anchor for quality checks. |
| `index_price` | Deribit index price. | Spot anchor for basis/carry context. |
| `open_interest` | Open interest at snapshot. | Positioning/liquidity regime signal. |
| `funding_8h` | 8-hour funding metric. | Carry and regime context. |
| `current_funding` | Current funding rate. | Short-horizon carry pressure signal. |

### Dataset: `option_ticker_snapshot_1m`
Layer: Bronze

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version` | Dataset schema label (`v1`). | Controlled evolution for options raw payloads. |
| `dataset_type` | Dataset identity (`option_ticker_snapshot_1m`). | Prevents mixing with perp L2 raw rows. |
| `exchange` | Venue identifier (`deribit`). | Lineage and multi-venue compatibility. |
| `source` | Endpoint channel (`rest_get_book_summary_by_currency`). | Reproducibility and endpoint provenance. |
| `currency` | Logical analysis currency (`BTC`,`ETH`,`SOL`). | Stable partition key for options analytics. |
| `requested_currency` | User/config requested currency. | Intent traceability. |
| `source_currency` | Endpoint currency (`BTC`,`ETH`,`USDC` for SOL). | Auditability for SOL mapping behavior. |
| `instrument_name` | Option instrument id. | Contract identity and parsing source. |
| `base_currency` | Base currency from payload. | Integrity checks and contract context. |
| `quote_currency` | Quote currency from payload. | Pricing denomination context. |
| `instrument_type` | Instrument class (`option`). | Downstream transform scope control. |
| `snapshot_time` | Pipeline minute snapshot time. | Canonical time axis for chain-state features. |
| `exchange_creation_time` | Payload creation timestamp. | Staleness checks vs observed time. |
| `ingested_at` | Persist timestamp UTC. | Pipeline timing observability. |
| `run_id` | Unique run id. | Debug/replay traceability. |
| `bid_price`,`ask_price`,`mid_price` | Top quote values. | Liquidity/spread feature inputs. |
| `mark_price`,`mark_iv` | Mark valuation and implied vol. | Core surface and volatility signals. |
| `underlying_price`,`underlying_index` | Underlying reference values. | Moneyness and surface alignment inputs. |
| `interest_rate` | Option rate field. | Future model/greeks compatibility. |
| `open_interest`,`volume`,`volume_usd` | Activity/liquidity metrics. | Filtering, weighting, and quality context. |
| `high`,`low`,`last`,`price_change` | Session context fields. | Sanity checks and regime context. |
| `raw_payload_hash` | Deterministic payload hash. | Row-level reproducibility and tamper detection. |

### Dataset: `instrument_metadata_snapshot_daily`
Layer: Bronze

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Schema control and dataset routing. |
| `exchange`,`source` | Venue and endpoint source. | Lineage and fetch-path auditability. |
| `snapshot_date`,`ingested_at`,`run_id` | Snapshot identity/time/run metadata. | Daily reproducibility and run tracing. |
| `instrument_name` | Instrument identifier. | Primary natural key within snapshot date. |
| `kind`,`instrument_type`,`option_type` | Deribit instrument classification. | Contract family filtering and modeling scope. |
| `base_currency`,`quote_currency`,`counter_currency`,`settlement_currency` | Currency descriptors. | Settlement/pricing context and validation. |
| `tick_size`,`contract_size`,`min_trade_amount` | Trading rule fields. | Execution constraints and quality checks. |
| `is_active` | Active vs expired state. | Daily tradable-universe construction. |
| `creation_timestamp`,`expiration_timestamp` | Lifecycle timestamps. | Tenor calculations and expiry handling. |
| `strike` | Strike value for options. | Surface bucketing and contract geometry. |
| `raw_payload_hash` | Deterministic payload hash. | Snapshot reproducibility and change detection. |

### Dataset: `index_price_snapshot_1m`
Layer: Bronze

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Safe schema evolution and routing. |
| `exchange`,`source` | Venue and endpoint source. | Source lineage and auditability. |
| `index_name` | Deribit index key (`btc_usd`, etc.). | Primary partition and join key. |
| `snapshot_time`,`event_time` | Minute snapshot timestamps. | Canonical time alignment for aggregation. |
| `price` | Index price value. | Core signal for downstream returns/features. |
| `ingested_at`,`run_id` | Persist/run metadata. | Pipeline diagnostics and replay traceability. |
| `raw_payload_hash` | Deterministic row hash. | Reproducibility and change auditing. |

### Dataset: `l2_snapshot_features`
Layer: Silver

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Stable downstream expectations. |
| `ts_event`,`ts_received`,`month` | Time and partition metadata. | Temporal aggregation and scalable storage. |
| `exchange`,`symbol`,`instrument_type`,`source`,`run_id`,`depth` | Source identity fields. | Deterministic grouping and reproducibility. |
| `best_bid_*`,`best_ask_*`,`mid_price`,`spread`,`spread_bps`,`microprice` | Top-of-book price features. | Core microstructure state and trading signals. |
| `bid_prices`,`ask_prices`,`bid_sizes`,`ask_sizes` | Fixed-width depth arrays. | Consistent model-ready tensor representation. |
| `bid_volume_N`,`ask_volume_N`,`imbalance_N` | Depth-window liquidity/imbalance metrics. | Order-book pressure and directional context. |
| `mark_price`,`index_price`,`open_interest`,`funding_rate`,`funding_8h` | Carry/market context fields. | Regime and valuation conditioning. |
| `is_valid`,`validation_flags` | Deterministic quality status. | Guardrails for downstream training/backtests. |

### Dataset: `option_chain_features_1m`
Layer: Silver

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Stable schema for surface transforms. |
| `ts_snapshot`,`month` | Snapshot and partition timestamps. | Time alignment and scalable partitioning. |
| `exchange`,`currency`,`instrument_type`,`source`,`run_id`,`instrument_name` | Source and contract identity. | Deterministic grouping and auditability. |
| `expiry_date`,`expiry_timestamp`,`strike`,`option_type` | Contract geometry fields. | Surface axes and tenor/strike bucketing. |
| `days_to_expiry`,`tau_years` | Time-to-expiry features. | Volatility term-structure modeling input. |
| `underlying_price`,`moneyness`,`log_moneyness` | Relative moneyness state. | Comparable cross-strike analytics. |
| `bid_price`,`ask_price`,`mid_price`,`mark_price`,`mark_iv` | Pricing and IV fields. | Core volatility-surface signal set. |
| `interest_rate`,`open_interest`,`volume`,`volume_usd` | Market context/liquidity. | Quality filtering and weighting signals. |
| `spread`,`spread_bps` | Quote quality/liquidity measures. | Tradability and noise filtering. |
| `is_atm_candidate`,`is_valid_for_surface`,`quality_flags` | Surface fitness diagnostics. | Robustness and controlled data usage. |

### Dataset: `instrument_metadata_snapshot_features_daily`
Layer: Silver

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Stable downstream contract. |
| `exchange`,`source`,`snapshot_date`,`month`,`run_id`,`ingested_at` | Snapshot identity and partition metadata. | Daily rebuild reproducibility. |
| `instrument_name`,`kind`,`base_currency`,`quote_currency`,`settlement_currency`,`instrument_type` | Contract descriptors. | Universe segmentation and filtering. |
| `tick_size`,`contract_size`,`min_trade_amount`,`is_active` | Trading rule state. | Execution constraints and validity checks. |
| `option_type`,`strike` | Option geometry descriptors. | Surface-compatible metadata features. |
| `is_option`,`days_to_expiration` | Derived metadata features. | Fast downstream daily analytics. |

### Dataset: `index_price_snapshot_features_1m`
Layer: Silver

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Schema stability for minute features. |
| `exchange`,`source`,`index_name`,`month`,`run_id` | Identity and partition metadata. | Deterministic joins and partitioning. |
| `ts_event`,`ts_received` | Event and receive timestamps. | Latency checks and temporal ordering. |
| `price`,`price_prev`,`price_delta`,`log_return_1m` | Core index return features. | Basis for risk, volatility, and momentum analytics. |

### Dataset: `l2_m1_features`
Layer: Gold

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type`,`feature_set_version` | Contract/version identifiers. | Reproducibility and controlled evolution. |
| `exchange`,`symbol`,`instrument_type`,`depth`,`ts_minute` | Minute identity fields. | Deterministic joins and time-series indexing. |
| `snapshot_count`,`coverage_ratio`,`first_snapshot_ts`,`last_snapshot_ts` | Coverage quality metadata. | Detect sparse/minute-quality issues. |
| `mid_*`,`spread_bps_*`,`microprice_*` | Minute price/liquidity aggregates. | Robust feature layer for modeling/backtests. |
| `imbalance_*`,`bid_volume_*`,`ask_volume_*`,`book_pressure_*` | Depth pressure aggregates. | Market microstructure signal compression. |
| `mark_price_last`,`index_price_last`,`open_interest_last`,`funding_rate_last` | Carry/market context snapshots. | Regime-aware downstream modeling. |
| `is_complete_minute`,`quality_flags` | Minute quality and fill annotations. | Safe filtering and explainable data quality. |

### Dataset: `option_surface_m1`
Layer: Gold

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Stable consumer contract. |
| `exchange`,`instrument_type`,`currency`,`ts_minute`,`month`,`expiry_date` | Surface key dimensions. | Deterministic indexing by tenor/currency/time. |
| `term_days`,`term_bucket` | Expiry bucket descriptors. | Term-structure analytics. |
| `atm_iv`,`atm_strike`,`atm_moneyness` | ATM surface anchor fields. | Quick volatility regime reference. |
| `iv_near_atm_call`,`iv_near_atm_put` | Near-ATM side-specific IVs. | Skew and directional volatility diagnostics. |
| `open_interest_sum`,`volume_sum`,`contract_count`,`valid_surface_contract_count`,`surface_coverage_ratio` | Surface quality/liquidity metrics. | Fitness checks for trading/research usage. |
| `skew_slope`,`smile_curvature`,`rr25`,`bf25` | Derived shape metrics. | Compact smile/skew monitoring features. |

### Dataset: `instrument_metadata_daily_summary`
Layer: Gold

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Stable daily summary contract. |
| `exchange`,`snapshot_date`,`kind`,`base_currency` | Summary grouping keys. | Daily universe trend tracking by contract family. |
| `instrument_count` | Total contracts in group. | Universe size monitoring. |
| `active_instrument_count` | Active contracts in group. | Live tradable-set health. |
| `option_instrument_count` | Option contracts in group. | Product-mix monitoring. |
| `mean_strike` | Mean strike in group. | Daily contract-distribution drift signal. |

### Dataset: `index_price_m1_features`
Layer: Gold

| Column | Meaning | Why it matters |
|---|---|---|
| `schema_version`,`dataset_type` | Contract and identity fields. | Stable minute-summary contract. |
| `exchange`,`index_name`,`ts_minute` | Minute identity keys. | Deterministic index time-series. |
| `snapshot_count` | Number of silver rows aggregated in minute. | Coverage quality and sparsity checks. |
| `price_open`,`price_high`,`price_low`,`price_close`,`price_mean` | OHLC-style minute aggregates. | Core intraminute state for analytics. |
| `log_return_1m_mean` | Average minute log-return signal. | Volatility/momentum feature for downstream models. |


## Testing

Run the full verification suite:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy .
```

Or use:

```bash
make check
```

Current coverage highlights:

- CLI command parsing/defaults and builder dispatch behavior.
- Bronze parquet partition layout and idempotent merge semantics.
- Silver feature calculations, incremental invalidation, and artifact toggles.
- Gold M1 aggregation with missing-minute densification and configurable neighbor/hybrid/kalman fill policies.
- Gold edge-case guards where fill is skipped when adjacent observed minutes are unavailable.
- Gold incremental invalidation when completeness or fill policy settings change.
- Schema contracts for Bronze, Silver, and Gold outputs.

## Known Limitations

- Only Deribit perpetual L2 order book snapshots are supported.
- The loader uses REST polling, not a streaming websocket feed.
- Parquet persistence is local-file based and does not include a database sink.
- Failed per-symbol fetches inside a polling tick are logged, isolated, and skipped for that tick.
- Incremental transform state is local to one lake root and is not a distributed metadata catalog.
- Overlapping local writer processes can race when they write the same artifact path at the same time.

## Future Improvements

- Add a websocket collector for higher-frequency L2 sampling.
- Add explicit schema-version migration tests for Parquet outputs.
- Add replay utilities for validating stored raw snapshots.
- Add exchange adapters behind the existing L2 interfaces.
