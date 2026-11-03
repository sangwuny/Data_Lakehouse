# Gold Lagged Correlation Analysis

This document describes `03b_gold_lagged_correlation_analysis.py`.

This module is a lagged Pearson correlation analysis step. It is for EDA, dashboards, and relationship ranking. It must not be interpreted as causal screening, and its scores should not be used to include or exclude variables from causal inference.

## Notebook

```text
notebooks/databricks/gold/03b_gold_lagged_correlation_analysis.py
```

Input table:

```text
fred_lakehouse.gold.fred_transformed_features_long
```

Outputs:

| Object | Type | Role |
|---|---|---|
| `fred_lagged_correlation_scores` | Delta table | Lagged Pearson correlation scores |
| `fred_top_lagged_correlations` | View | High-scoring lagged correlations for EDA/ranking |
| `fred_lagged_correlation_run_summary` | Delta table | Run-level counts and settings |

## Key Parameters

| Parameter | Default | Description |
|---|---:|---|
| `target_series_id` | `GDPC1` | Target series |
| `comparison_series_ids` | `ALL` | Comparison series list or `ALL` |
| `target_frequency` | `monthly` | Frequency used in Gold transformed features |
| `aggregation_method` | `last` | Period aggregation used in Gold transformed features |
| `correlation_transform_type` | `raw` | Transform used for both target and comparison series |
| `max_lag_periods` | `12` | Maximum comparison lag tested |
| `min_pair_count` | `AUTO` | Minimum aligned target/comparison pairs, with frequency defaults |
| `reliable_pair_count` | `AUTO` | Pair count at which the sample reliability weight reaches 1 |
| `min_correlation_score` | `0.3` | Threshold for the top correlation view |
| `optimize_tables` | `true` | Attempt Delta `OPTIMIZE ZORDER` |

The current implementation uses one `correlation_transform_type` for all selected series. For mixed-unit economic data, choose this deliberately rather than treating the transform as a cosmetic option.

## Method

For each comparison series and lag, the notebook aligns target values at time `t` with comparison values at time `t - lag`.

Example for monthly data and `lag_periods = 3`:

```text
target period     = 2020-06-01
comparison period = 2020-03-01
```

It computes Pearson correlation on aligned non-null pairs:

```text
pearson_corr = corr(target_value_numeric, comparison_value_numeric)
r_squared    = pearson_corr ^ 2
```

It also computes sample coverage and reliability:

```text
coverage_rate            = pair_count / target_period_count
sample_reliability_score = min(1, pair_count / reliable_pair_count)
coverage_weighted_score  = r_squared * coverage_rate
correlation_score        = r_squared * sample_reliability_score
correlation_score_method = r_squared_sample_reliability
```

`correlation_score` rewards strong linear association but penalizes comparison series with too few aligned observations. `coverage_weighted_score` is also stored as a stricter reference score when overall target-period coverage matters.

## Important Columns

| Column | Meaning |
|---|---|
| `target_series_id` | Target series |
| `comparison_series_id` | Comparison series in the lagged relationship ranking |
| `transform_type` | Transform used for the score |
| `lag_periods` | Comparison lag relative to target |
| `pair_count` | Number of aligned non-null target/comparison pairs |
| `target_period_count` | Number of target periods in the analysis window |
| `coverage_rate` | `pair_count / target_period_count` |
| `reliable_pair_count` | Pair count used to cap the reliability weight |
| `sample_reliability_score` | `min(1, pair_count / reliable_pair_count)` |
| `pearson_corr` | Pearson correlation coefficient |
| `abs_pearson_corr` | Absolute Pearson correlation coefficient |
| `r_squared` | Squared correlation |
| `coverage_weighted_score` | `r_squared * coverage_rate` |
| `correlation_score` | Main ranking score |
| `correlation_rank` | Rank within the target/run partition |

## Interpretation

A high `correlation_score` means:

```text
At this transform and lag, the comparison series has a strong linear relationship with the target,
and the relationship is supported by enough aligned observations to be worth inspecting.
```

It does not mean:

```text
the comparison series causes the target
the comparison series should be used as a causal variable
a low-score comparison series has no causal relationship
```

Correlation and causality are different concepts. Correlation can be absent even when a causal relationship exists, and correlation can be strong because of common causes, trends, regime shifts, shared measurement conventions, or other confounding structures.

## Recommended Use

Use this notebook for:

- EDA
- dashboard relationship ranking
- lagged co-movement inspection
- finding interesting historical co-movement patterns

Do not use it as a causal-inference gate.

For market targets such as `NASDAQCOM` or `SP500`, avoid raw-level analysis unless that is intentional. Growth/return transforms such as `log_diff_1` or `pct_change_1` are often more appropriate. For rate and spread series, `change_1` or `raw` is usually easier to interpret than percent change.

## Limitations

- Pearson correlation captures linear association only.
- It does not condition on other variables.
- It does not test forecasting improvement.
- It can rank spurious relationships highly if non-stationary levels are used.
- It cannot establish or rule out causality.
