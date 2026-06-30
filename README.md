# FRED Lakehouse

Databricks Lakehouse project for FRED/ALFRED economic time-series data.

The active implementation uses Databricks notebooks, Delta Lake, and Unity Catalog. Files are organized by Medallion layer under `notebooks/databricks`.

## Current Local Path

```text
C:\Users\mike1\OneDrive\Desktop\Data_Lakehouse
```

Use relative repository paths in Databricks and documentation whenever possible. The local path above is only the current desktop workspace location.

## Active Layout

```text
notebooks/databricks/
  configs/
    fred_seed_series.json
  bronze/
    01a_bronze_fred_bootstrap_versions.py
    01b_bronze_fred_incremental_versions.py
    01c_bronze_fred_current_observations.py
    01e_bronze_alfred_reproducibility_audit.py
  silver/
    02a_silver_fred_bootstrap_versions.py
    02b_silver_fred_incremental_versions.py
  gold/
    03a_gold_fred_bootstrap_causal_features.py
    03b_gold_fred_incremental_causal_features.py
docs/
  bronze_layer.md
  silver_layer.md
  gold_layer.md
```

## Layer Summary

### Bronze

The main Bronze pipeline is vintage-date based, not dense daily physical snapshots.

- `notebooks/databricks/bronze/01a_bronze_fred_bootstrap_versions.py` performs the initial ALFRED-capable revision-history load.
- `notebooks/databricks/bronze/01b_bronze_fred_incremental_versions.py` performs incremental ALFRED vintage-date checks and loads only changed revision windows.
- `notebooks/databricks/bronze/01c_bronze_fred_current_observations.py` handles FRED-only current observations, such as series without ALFRED revision history.
- `notebooks/databricks/bronze/01e_bronze_alfred_reproducibility_audit.py` directly compares internal point-in-time reconstruction against live ALFRED `vintage_dates` API responses for selected as-of dates.

The seed catalog lives at `notebooks/databricks/configs/fred_seed_series.json`.

```text
alfred_available = true   -> bronze/01a + bronze/01b
alfred_available = false  -> bronze/01c
```

### Silver

Silver cleans and conforms ALFRED-capable Bronze observation versions into `fred_observation_versions_cleaned`. It preserves point-in-time fields, quality status, revision ordering, and lineage for Gold.

### Gold

Gold builds serving marts for as-of observations, period features, transformed long-format features, feature snapshots, and causal candidate scores.

The current Gold structure includes `fred_transformed_features_long`, which stores comparable transformed values such as `pct_change_12`, `log_diff_12`, `z_score_full_sample`, and `index_base100`.

## Typical Execution

```text
Initial build:
1. bronze/01a_bronze_fred_bootstrap_versions.py
2. bronze/01c_bronze_fred_current_observations.py for FRED-only current series
3. bronze/01e_bronze_alfred_reproducibility_audit.py for ALFRED point-in-time spot checks
4. silver/02a_silver_fred_bootstrap_versions.py
5. gold/03a_gold_fred_bootstrap_causal_features.py

Incremental workflow:
1. bronze/01b_bronze_fred_incremental_versions.py
2. bronze/01e_bronze_alfred_reproducibility_audit.py for scheduled sample as-of dates
3. silver/02b_silver_fred_incremental_versions.py
4. gold/03b_gold_fred_incremental_causal_features.py
```

## Documentation

- `docs/bronze_layer.md`: Bronze ingestion design, seed routing, Delta tables, API/retry policy, and ALFRED reproducibility audit.
- `docs/silver_layer.md`: Silver cleaning, quality tagging, revision features, and lineage.
- `docs/gold_layer.md`: Gold serving marts, transformed features, dashboard usage, and causal candidate scoring.