# FRED Series Metadata Config

This document describes `notebooks/databricks/configs/build_fred_series_metadata.py` and the generated `fred_series_metadata.json` file.

## Purpose

Macrotrends and Yahoo Finance are used only as bootstrap sources for series whose long history is not available from the normal FRED current-observation path. Those providers do not carry canonical FRED units, frequency, or seasonal adjustment metadata. The metadata builder closes that gap by fetching canonical metadata from the FRED `/fred/series` endpoint for every series in `fred_seed_series.json`.

The generated metadata config lets external bootstrap rows enter Bronze with the same source semantics that FRED rows carry:

```text
units, units_short
frequency, frequency_short
seasonal_adjustment, seasonal_adjustment_short
title, observation_start, observation_end, last_updated
seed domain, priority, expected_frequency, alfred_available
```

## Notebook

```text
notebooks/databricks/configs/build_fred_series_metadata.py
```

Inputs:

```text
notebooks/databricks/configs/fred_seed_series.json
FRED API /fred/series
```

Output:

```text
notebooks/databricks/configs/fred_series_metadata.json
```

The generated JSON is intentionally separate from `fred_seed_series.json`. The seed catalog remains the manually maintained routing list, while `fred_series_metadata.json` is generated canonical source metadata.

## Key Parameters

| Parameter | Default | Description |
|---|---:|---|
| `secret_scope` | `fred-lakehouse` | Databricks secret scope containing the FRED API key |
| `secret_key` | `fred_api_key` | Secret key name |
| `seed_catalog_path` | `fred_seed_series.json` | Seed catalog path |
| `output_path` | `fred_series_metadata.json` | Generated metadata JSON path |
| `series_ids` | `ALL` | Optional comma-separated subset |
| `sleep_seconds` | `0` | Delay between FRED requests |
| `retry_sleep_seconds` | `0.05,0.1,0.5` | Retry backoff schedule |

Local execution can use environment variables with matching names such as `FRED_API_KEY`, `SEED_CATALOG_PATH`, and `OUTPUT_PATH`.

## Consumers

The generated config is consumed by:

```text
bronze/01d_bronze_macrotrends_observations.py
bronze/01f_bronze_yahoo_finance_observations.py
```

Both loaders require `fred_series_metadata.json` by default. Their `series_units_json` widget remains only as an explicit override mechanism.

`01c_bronze_fred_current_observations.py` does not need the generated JSON because it calls FRED observations and FRED series metadata directly during the load.

## Recommended Flow

```text
1. Update fred_seed_series.json when the tracked series list changes.
2. Run configs/build_fred_series_metadata.py.
3. Commit or deploy the generated fred_series_metadata.json with the notebook/config bundle.
4. Run Macrotrends or Yahoo bootstrap loaders when external backfill is needed.
5. Run 01c for current FRED refreshes after bootstrap history has been loaded.
```

## Validation Queries

Check that the generated JSON covers every seed series before running bootstrap loaders:

```sql
-- In Databricks, read the JSON with Spark if it has been uploaded to DBFS/workspace storage.
SELECT
  series_id,
  units,
  units_short,
  frequency,
  frequency_short,
  seasonal_adjustment,
  seasonal_adjustment_short
FROM metadata_json_view
ORDER BY series_id;
```

Check Bronze rows after bootstrap:

```sql
SELECT
  series_id,
  data_provider,
  count(*) AS row_count,
  min(observation_date) AS min_observation_date,
  max(observation_date) AS max_observation_date,
  first(units, true) AS units,
  first(units_short, true) AS units_short,
  first(frequency, true) AS frequency,
  first(frequency_short, true) AS frequency_short
FROM fred_lakehouse.bronze.fred_current_observations_raw
WHERE data_provider IN ('macrotrends', 'yahoo_finance')
GROUP BY series_id, data_provider
ORDER BY series_id, data_provider;
```
