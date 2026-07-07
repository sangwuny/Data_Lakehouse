# Silver Layer

This document describes the current Silver layer for the FRED/ALFRED lakehouse. Silver converts Bronze data into typed, quality-tagged, analytics-ready tables while preserving the time semantics needed by Gold.

## Purpose

Silver has five jobs:

1. Convert raw string dates and values into typed date and numeric columns.
2. Identify missing values, parse errors, invalid real-time windows, and outliers.
3. Preserve ALFRED revision ordering and point-in-time usability.
4. Keep FRED current-only data separate from ALFRED revision-aware data.
5. Provide stable inputs for the integrated Gold as-of feature mart.

Silver does not create purpose-specific modeling features. It provides clean enterprise tables that Gold can transform for dashboards and relationship scoring.

## Inputs And Outputs

```text
ALFRED revision-aware input : fred_lakehouse.bronze.fred_observation_versions
ALFRED Silver output       : fred_lakehouse.silver.fred_observation_versions_cleaned

Current-only input         : fred_lakehouse.bronze.fred_current_observations_raw
Current Silver output      : fred_lakehouse.silver.fred_current_observations_cleaned
```

`fred_observation_versions_cleaned` is the point-in-time-safe revision-aware table. `fred_current_observations_cleaned` is the cleaned current reconstructed history table for FRED current-only series, including rows initially bootstrapped from Macrotrends or Yahoo Finance.

## Active Notebooks

```text
notebooks/databricks/silver/
  02a_silver_fred_bootstrap_versions.py
  02b_silver_fred_incremental_versions.py
  02c_silver_fred_current_observations.py
```

- `02a` cleans the full ALFRED revision-aware Bronze table during initial build.
- `02b` cleans only changed ALFRED series after incremental Bronze runs.
- `02c` cleans FRED current-only raw rows, including external bootstrap rows stored in the same Bronze current table.

## Main Tables And Views

```text
fred_lakehouse.silver
- fred_observation_versions_cleaned
- fred_version_series_catalog
- fred_version_quality_report
- fred_version_lineage_events
- fred_version_run_summary
- fred_observations_asof_ready              (view)
- fred_observations_current                 (view)
- fred_current_observations_cleaned
- fred_current_series_catalog
- fred_current_quality_report
- fred_current_run_summary
- fred_current_observations_analytics_ready (view)
```

## Revision-Aware Silver

`02a` and `02b` produce the fields needed for point-in-time reconstruction:

- `value_numeric`
- `observation_date`, `period_start`, `period_end`
- `available_at`, `realtime_start`, `realtime_end`, `vintage_date`
- `revision_number`, `revision_count`, `is_current_version`
- `is_point_in_time_usable`
- `quality_status`, `quality_issues`
- `is_outlier`, `outlier_score`
- Bronze and Silver lineage columns

Gold uses the real-time window and available-at fields from this table to choose only values visible at the requested `as_of_date`.

## Current-Only Silver

`02c` cleans `fred_current_observations_raw` into `fred_current_observations_cleaned`.

Important semantics:

- These rows do not have ALFRED vintage history.
- They represent current reconstructed history.
- Gold can still include them in as-of analysis by using `observation_date <= as_of_date`.
- `is_point_in_time_safe = false` is preserved so the difference from ALFRED revision-aware rows is explicit.
- Provider lineage is preserved for rows sourced from FRED, Macrotrends, or Yahoo Finance.

## Gold Integration

The current Gold layer is a single integrated notebook:

```text
silver.fred_observation_versions_cleaned
silver.fred_current_observations_cleaned
-> gold/03_gold_fred_asof_features.py
```

Gold integrates both Silver inputs into:

- `fred_asof_observations`
- `fred_period_features_long`
- `fred_transformed_features_long`
- `fred_series_feature_snapshot`
- `fred_relationship_candidate_scores`

## Recommended Execution

Initial build:

```text
bronze/01a_bronze_fred_bootstrap_versions.py
-> bronze/01d_bronze_macrotrends_observations.py      when needed
-> bronze/01f_bronze_yahoo_finance_observations.py   when needed
-> bronze/01c_bronze_fred_current_observations.py
-> silver/02a_silver_fred_bootstrap_versions.py
-> silver/02c_silver_fred_current_observations.py
-> gold/03_gold_fred_asof_features.py
```

Refresh workflow:

```text
bronze/01b_bronze_fred_incremental_versions.py
-> bronze/01c_bronze_fred_current_observations.py
-> silver/02b_silver_fred_incremental_versions.py
-> silver/02c_silver_fred_current_observations.py
-> gold/03_gold_fred_asof_features.py
```

## Summary

Silver sits between Bronze preservation and Gold analysis. It keeps ALFRED revision-aware rows and FRED current-only rows separate, preserves their different time semantics, and gives Gold stable clean inputs for integrated as-of analysis.
