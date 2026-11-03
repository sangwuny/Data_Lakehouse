# Gold Layer

This document describes the current Gold feature mart for the FRED/ALFRED lakehouse. Gold integrates clean Silver inputs into as-of observations, period features, transformed feature rows, and latest feature snapshots.

Lagged correlation analysis is documented separately in `docs/gold_lagged_correlation_analysis.md`.

## Purpose

The Gold feature mart has five jobs:

1. Reconstruct what was visible at a selected `as_of_date`.
2. Integrate ALFRED revision-aware rows and FRED current-only rows into one serving layer.
3. Aggregate observations to a target frequency such as daily, weekly, monthly, quarterly, annual, or native.
4. Generate common transformed feature rows for comparison, dashboarding, and modeling.
5. Publish latest series snapshots and quality/run summaries.

Gold is not a raw preservation layer. Bronze preserves source data and Silver performs cleaning. Gold focuses on analysis-ready as-of features.

## Active Notebooks

```text
notebooks/databricks/gold/03_gold_fred_asof_features.py
notebooks/databricks/gold/03b_gold_lagged_correlation_analysis.py
```

`03b_gold_lagged_correlation_analysis.py` computes lagged Pearson correlation scores for EDA and dashboard ranking. It should not be interpreted as causal screening or causal variable selection.

Input Silver tables for `03_gold_fred_asof_features.py`:

```text
fred_lakehouse.silver.fred_observation_versions_cleaned
fred_lakehouse.silver.fred_current_observations_cleaned
```

Macrotrends and Yahoo Finance bootstrap rows enter Gold through `silver.fred_current_observations_cleaned`; Gold does not need a separate provider-specific branch for them.

## Key Parameters

| Parameter | Default | Description |
|---|---:|---|
| `catalog` | `fred_lakehouse` | Unity Catalog name |
| `silver_schema` | `silver` | Input Silver schema |
| `gold_schema` | `gold` | Output Gold schema |
| `as_of_date` | blank | Blank means UTC today |
| `analysis_start_date` | blank | Blank means earliest available period |
| `analysis_end_date` | blank | Blank means `as_of_date` |
| `series_ids` | `ALL` | Series list to process |
| `target_frequency` | `monthly` | `daily`, `weekly`, `monthly`, `quarterly`, `annual`, or `native` |
| `aggregation_method` | `last` | `last` or `mean` |
| `include_quality_warnings` | `true` | Include Silver warning rows |
| `optimize_tables` | `true` | Attempt Delta `OPTIMIZE ZORDER` |

The `analysis_start_date` and `analysis_end_date` are part of the feature identity and are carried into period/transformed outputs. They also affect full-sample transforms such as `z_score_full_sample` and `index_base100`.

## Output Tables And Views

| Object | Type | Role |
|---|---|---|
| `fred_asof_observations` | Delta table | Observation rows visible at `as_of_date` |
| `fred_period_features_long` | Delta table | Period-level values after target-frequency aggregation |
| `fred_transformed_features_long` | Delta table | Long-format transformed values |
| `fred_series_feature_snapshot` | Delta table | Latest period feature snapshot per series and as-of run |
| `fred_latest_feature_snapshot` | View | Latest as-of snapshot by series/window/frequency/aggregation |
| `fred_gold_quality_report` | Delta table | Gold quality rule results |
| `fred_gold_run_summary` | Delta table | Run-level row counts and lineage ranges |

The primary table for dashboards and downstream modeling is `fred_transformed_features_long`.

03b additionally writes lagged correlation outputs:

| Object | Type | Role |
|---|---|---|
| `fred_lagged_correlation_scores` | Delta table | Lagged Pearson correlation scores |
| `fred_top_lagged_correlations` | View | High-scoring lagged correlations for EDA/ranking |
| `fred_lagged_correlation_run_summary` | Delta table | 03b run summary |

## Processing Flow

```text
silver.fred_observation_versions_cleaned
silver.fred_current_observations_cleaned
  -> gold/03_gold_fred_asof_features.py
  -> gold.fred_asof_observations
  -> gold.fred_period_features_long
  -> gold.fred_transformed_features_long
  -> gold.fred_series_feature_snapshot

Optional EDA/ranking:
gold.fred_transformed_features_long
  -> gold/03b_gold_lagged_correlation_analysis.py
  -> gold.fred_lagged_correlation_scores
  -> gold.fred_top_lagged_correlations
```

## As-Of Semantics

Revision-aware ALFRED rows are selected only when they were visible at the requested `as_of_date`:

```text
is_point_in_time_usable
value_numeric IS NOT NULL
observation_date <= as_of_date
realtime_start <= as_of_date
realtime_end >= as_of_date
coalesce(available_at, realtime_start) <= as_of_date
```

If multiple versions exist for the same `series_id` and `observation_date`, Gold selects the latest visible row by `realtime_start`, `available_at`, and `observation_version_id`.

Current-only rows do not have ALFRED revision windows. Gold includes them when `observation_date <= as_of_date` and marks them with:

```text
history_type = current_only
availability_basis = observation_date
is_revision_aware = false
```

## Period Features

Gold supports these target frequencies:

```text
daily, weekly, monthly, quarterly, annual, native
```

For non-native frequencies, period boundaries are recomputed from `observation_date`. For `native`, Gold uses the Silver `period_start` and `period_end` fields when available.

The `aggregation_method` controls the selected value:

- `last`: use the latest observation in the period.
- `mean`: use the period average.

The table `fred_period_features_long` also carries source metadata such as `source_frequency`, `source_frequency_short`, `units`, `units_short`, and `seasonal_adjustment`.

## Transformed Features

`fred_transformed_features_long` emits one row per `series_id`, `period_start`, and `transform_type`.

| Transform | Meaning | Unit |
|---|---|---|
| `raw` | Selected period value | Source unit |
| `change_1` | Current minus 1-period lag | Source unit or percentage point |
| `change_12` | Current minus 12-period lag | Source unit or percentage point |
| `pct_change_1` | 1-period percentage change | Percent |
| `pct_change_12` | 12-period percentage change | Percent |
| `log_diff_1` | 1-period log difference times 100 | Log percent |
| `log_diff_12` | 12-period log difference times 100 | Log percent |
| `z_score_full_sample` | Full-window z-score | Standard deviation |
| `index_base100` | Full-window base-100 index | Index base 100 |

Unit-aware interpretation matters:

- Level, money, count, and price-index series often need `log_diff_*` or `pct_change_*` for stationarity-sensitive work.
- Rates and spreads such as `FEDFUNDS`, `DGS10`, and `T10Y2Y` usually use `raw` or `change_*`; percent changes of rates are often hard to interpret.
- `z_score_full_sample` and `index_base100` rescale values but do not make a non-stationary series stationary.

## Feature Snapshot

`fred_series_feature_snapshot` stores the latest period per series for the selected as-of/window/frequency/aggregation combination. It is shaped for Databricks Feature Engineering in Unity Catalog.

Important columns:

```text
feature_timestamp
latest_period_start, latest_period_end
value_numeric
lag_1_value_numeric, lag_3_value_numeric, lag_6_value_numeric, lag_12_value_numeric
diff_1_value_numeric, pct_change_1
rolling_mean_3, rolling_mean_6, rolling_mean_12, rolling_stddev_12
source_observation_count, quality_warning_count, outlier_count
last_observation_date, last_available_at, last_realtime_start, last_realtime_end
```

## Merge Keys

| Table | Merge key |
|---|---|
| `fred_asof_observations` | `as_of_date`, `series_id`, `observation_date` |
| `fred_period_features_long` | `as_of_date`, `target_frequency`, `aggregation_method`, `series_id`, `period_start` |
| `fred_transformed_features_long` | `as_of_date`, `target_frequency`, `aggregation_method`, `series_id`, `period_start`, `transform_type` |
| `fred_series_feature_snapshot` | `as_of_date`, `target_frequency`, `aggregation_method`, `series_id` |
| `fred_lagged_correlation_scores` | `as_of_date`, `analysis_start_date`, `analysis_end_date`, `target_frequency`, `aggregation_method`, `transform_type`, `target_series_id`, `comparison_series_id`, `lag_periods` |

## Quality And Run Summary

`fred_gold_quality_report` records these checks:

```text
asof_not_before_observation
visible_inside_realtime_window
period_feature_value_not_null
```

`fred_gold_run_summary` records run-level row counts and lineage windows.

## Recommended Settings

Common exploratory settings:

```text
target_frequency = monthly
aggregation_method = last
include_quality_warnings = true
```

For macro/market comparisons, explicitly set a common `analysis_start_date` when series start dates are very different. This makes transformed feature comparisons and lagged correlations easier to interpret.

## Causality Scope

The Gold layer currently does not include causal-inference notebooks. See `docs/causality_scope.md` for the project-level boundary.

The former 04 notebooks were removed because methods such as Granger-style tests and PCMCI require assumptions that cannot be justified from the collected FRED/ALFRED/current-only dataset alone, especially the absence of unobserved confounding. The project keeps correlation analysis for exploration, but it does not use correlation rankings as evidence for causality.
