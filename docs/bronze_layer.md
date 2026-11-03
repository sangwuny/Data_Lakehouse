# Bronze Layer

This document describes the current Bronze layer for the FRED/ALFRED lakehouse. Bronze preserves source-like external data, collection lineage, request metadata, canonical FRED series metadata, and enough timing fields for downstream point-in-time reconstruction.

## Purpose

The Bronze layer has six jobs:

1. Preserve raw FRED/ALFRED API responses and raw observation values.
2. Store ALFRED revision-aware series as observation versions with `realtime_start`, `realtime_end`, and `vintage_date`.
3. Store non-revision current-only series in a separate current raw table.
4. Backfill current-only series whose FRED full history is restricted by using Macrotrends or Yahoo Finance bootstrap sources.
5. Carry canonical FRED metadata such as units, frequency, and seasonal adjustment into all current/bootstrap rows.
6. Track every collection run, request parameter hash, payload hash, retry result, and source provider.

## Active Notebooks

```text
notebooks/databricks/
  configs/
    fred_seed_series.json
    build_fred_series_metadata.py
    fred_series_metadata.json              generated output
  macrotrends/
    BAMLH0A0HYM2_chart_20260702T065042.csv
  bronze/
    01a_bronze_fred_bootstrap_versions.py
    01b_bronze_fred_incremental_versions.py
    01c_bronze_fred_current_observations.py
    01d_bronze_macrotrends_observations.py
    01e_bronze_alfred_reproducibility_audit.py
    01f_bronze_yahoo_finance_observations.py
```

The active flow is `01a`, `01b`, `01c`, `01d`, `01e`, and `01f`. Older physical daily snapshot experiments are no longer the operating design.

## Seed And Metadata Routing

The seed catalog is `notebooks/databricks/configs/fred_seed_series.json`. The generated metadata config is documented separately in `docs/fred_series_metadata_config.md`.

`build_fred_series_metadata.py` reads the seed catalog, calls the FRED `/fred/series` endpoint, and writes `fred_series_metadata.json`. The generated file is used by non-FRED bootstrap loaders so Macrotrends and Yahoo rows carry the same canonical FRED metadata that FRED itself would provide.

Important metadata fields:

- `title`
- `frequency`, `frequency_short`
- `units`, `units_short`
- `seasonal_adjustment`, `seasonal_adjustment_short`
- `observation_start`, `observation_end`, `last_updated`
- seed fields such as `domain`, `priority`, `expected_frequency`, and `alfred_available`

Routing:

```text
alfred_available = true   -> bronze/01a and bronze/01b
alfred_available = false  -> bronze/01c
external bootstrap gaps   -> configs/build_fred_series_metadata.py, then bronze/01d Macrotrends or bronze/01f Yahoo Finance, then bronze/01c refresh
```

`01d` and `01f` require `fred_series_metadata.json` by default. Their `series_units_json` widget is now an override mechanism, not the primary source of units.

## Delta Tables

Default storage location:

```text
Catalog: fred_lakehouse
Schema : bronze
Format : Delta table
```

| Table | Role |
|---|---|
| `fred_raw_response_payloads` | Raw FRED/ALFRED JSON payloads |
| `fred_ingestion_runs` | Request-level ingestion history |
| `fred_series_metadata_versions` | ALFRED/FRED series metadata versions from versioned loaders |
| `fred_observation_versions` | ALFRED revision-aware observation versions |
| `fred_vintage_dates_seen` | ALFRED vintage date checks |
| `fred_incremental_watermarks` | Incremental watermarks for `01b` |
| `fred_current_observations_raw` | FRED current-only rows plus Macrotrends/Yahoo bootstrap rows |
| `fred_run_summary` | Bronze run summaries |

`fred_current_observations_raw` keeps `source = fred` for downstream compatibility, while the actual provider is preserved in `data_provider`, such as `fred_api`, `macrotrends`, or `yahoo_finance`.

Current-only raw rows include common observation metadata used by Silver and Gold:

```text
domain, priority, expected_frequency
period_start_inferred, period_end_inferred, period_inference_basis
data_provider, title, frequency, frequency_short
units, units_short, seasonal_adjustment, seasonal_adjustment_short
source_metadata_json
```

## Notebook Roles

### `01a_bronze_fred_bootstrap_versions.py`

Initial full ALFRED revision-history load for ALFRED-capable series.

Important defaults:

```text
series_ids = ALL
include_vintages = true
realtime_start = 1776-07-04
realtime_end = 9999-12-31
output_type = 1
sleep_seconds = 0
retry_sleep_seconds = 0.01,0.05,0.1
```

### `01b_bronze_fred_incremental_versions.py`

Incremental ALFRED refresh. It uses vintage dates and watermarks to avoid reloading unchanged revision windows.

### `01c_bronze_fred_current_observations.py`

FRED current-only refresh. It stores rows by `source`, `series_id`, and `observation_date`; if a value or metadata field changes, the row is updated.

`01c` calls both FRED observations and FRED series metadata. The `observation_start` widget is blank by default; when left blank, the notebook uses `1776-07-04` so current-only loads request the full available FRED history.

### `01d_bronze_macrotrends_observations.py`

Bootstrap loader for manually downloaded Macrotrends CSV files. It currently supports `BAMLH0A0HYM2` backfill into `fred_current_observations_raw`. These rows are current reconstructed history, not ALFRED vintage records.

Macrotrends does not provide canonical FRED units, so `01d` reads `fred_series_metadata.json` and writes FRED-compatible metadata into the Bronze row.

### `01e_bronze_alfred_reproducibility_audit.py`

Interactive reproducibility audit. The user selects series and as-of dates, and the notebook compares the internal reconstruction against live ALFRED vintage-date responses. It returns match/mismatch results without persisting a separate audit table.

### `01f_bronze_yahoo_finance_observations.py`

Bootstrap loader for Yahoo Finance chart history. It currently supports `SP500` via `^GSPC` and writes daily current reconstructed market history into `fred_current_observations_raw`.

Yahoo does not provide FRED units, so `01f` reads `fred_series_metadata.json` and uses Yahoo metadata only as supplemental source lineage.

## Reproducibility Scope

The Bronze reproducibility audit answers two questions:

1. Does the internal as-of reconstruction match ALFRED for a selected vintage date?
2. Do `realtime_start`, `realtime_end`, and `available_at` correctly describe which values were visible at that as-of date?

Direct comparison to original releasing institutions such as BEA, BLS, or the Federal Reserve is a separate cross-source validation problem. It needs explicit mappings from FRED series IDs to official source datasets, units, frequencies, and adjustment rules.

## Recommended Execution

Initial build:

```text
1. configs/build_fred_series_metadata.py
2. bronze/01a_bronze_fred_bootstrap_versions.py
3. bronze/01d_bronze_macrotrends_observations.py      when needed
4. bronze/01f_bronze_yahoo_finance_observations.py   when needed
5. bronze/01c_bronze_fred_current_observations.py
6. bronze/01e_bronze_alfred_reproducibility_audit.py
7. silver/02a_silver_fred_bootstrap_versions.py
8. silver/02c_silver_fred_current_observations.py
9. gold/03_gold_fred_asof_features.py
```

Refresh workflow:

```text
configs/build_fred_series_metadata.py when seed metadata changes
-> bronze/01b_bronze_fred_incremental_versions.py
-> bronze/01c_bronze_fred_current_observations.py
-> bronze/01e_bronze_alfred_reproducibility_audit.py
-> silver/02b_silver_fred_incremental_versions.py
-> silver/02c_silver_fred_current_observations.py
-> gold/03_gold_fred_asof_features.py
```

`01d` and `01f` are bootstrap helpers, so they are not normally part of the daily workflow after the initial backfill.
