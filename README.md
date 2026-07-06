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
  macrotrends/
    BAMLH0A0HYM2_chart_20260702T065042.csv
  bronze/
    01a_bronze_fred_bootstrap_versions.py
    01b_bronze_fred_incremental_versions.py
    01c_bronze_fred_current_observations.py
    01d_bronze_macrotrends_observations.py
    01e_bronze_alfred_reproducibility_audit.py
    01f_bronze_yahoo_finance_observations.py
  silver/
    02a_silver_fred_bootstrap_versions.py
    02b_silver_fred_incremental_versions.py
    02c_silver_fred_current_observations.py
  gold/
    03_gold_fred_asof_features.py
docs/
  bronze_layer.md
  silver_layer.md
  gold_layer.md
```

## Layer Summary

### Bronze

Bronze preserves raw-ish external observations and collection lineage.

- `notebooks/databricks/bronze/01a_bronze_fred_bootstrap_versions.py` performs the initial ALFRED-capable revision-history load.
- `notebooks/databricks/bronze/01b_bronze_fred_incremental_versions.py` performs incremental ALFRED vintage-date checks and loads changed revision windows.
- `notebooks/databricks/bronze/01c_bronze_fred_current_observations.py` handles FRED current-only observations.
- `notebooks/databricks/bronze/01d_bronze_macrotrends_observations.py` loads manually downloaded Macrotrends bootstrap history into the current-observation raw table, currently for `BAMLH0A0HYM2`.
- `notebooks/databricks/bronze/01e_bronze_alfred_reproducibility_audit.py` directly compares internal point-in-time reconstruction against live ALFRED `vintage_dates` API responses for selected as-of dates.
- `notebooks/databricks/bronze/01f_bronze_yahoo_finance_observations.py` loads Yahoo Finance bootstrap history into the current-observation raw table, currently for `SP500` via `^GSPC`.

The seed catalog lives at `notebooks/databricks/configs/fred_seed_series.json`.

```text
alfred_available = true   -> bronze/01a + bronze/01b
alfred_available = false  -> bronze/01c current refresh
external bootstrap gaps   -> bronze/01d Macrotrends or bronze/01f Yahoo Finance, then bronze/01c for later FRED refreshes
```

### Silver

Silver cleans and conforms Bronze tables into two analytic inputs:

- `fred_observation_versions_cleaned` for ALFRED revision-aware point-in-time rows.
- `fred_current_observations_cleaned` for FRED current-only rows, including bootstrap rows sourced from Macrotrends or Yahoo Finance when they were loaded into the Bronze current table.

### Gold

Gold is implemented as a single as-of-date mart in `notebooks/databricks/gold/03_gold_fred_asof_features.py`.

It integrates ALFRED revision-aware rows and FRED current-only rows, applies an optional analysis window, creates common transformation rows, and scores relationship candidates. The primary dashboard table is `fred_transformed_features_long`; the relationship score table is `fred_relationship_candidate_scores`.

Gold layer implementation is complete up to the integrated as-of feature mart and relationship-candidate scoring workflow.

## Typical Execution

```text
Initial build:
1. bronze/01a_bronze_fred_bootstrap_versions.py
2. bronze/01d_bronze_macrotrends_observations.py when Macrotrends bootstrap history is needed
3. bronze/01f_bronze_yahoo_finance_observations.py when Yahoo Finance bootstrap history is needed
4. bronze/01c_bronze_fred_current_observations.py for FRED current-only rows and later refreshes
5. bronze/01e_bronze_alfred_reproducibility_audit.py for ALFRED point-in-time spot checks
6. silver/02a_silver_fred_bootstrap_versions.py
7. silver/02c_silver_fred_current_observations.py
8. gold/03_gold_fred_asof_features.py

Refresh workflow:
1. bronze/01b_bronze_fred_incremental_versions.py
2. bronze/01c_bronze_fred_current_observations.py
3. bronze/01e_bronze_alfred_reproducibility_audit.py for scheduled sample as-of dates
4. silver/02b_silver_fred_incremental_versions.py
5. silver/02c_silver_fred_current_observations.py
6. gold/03_gold_fred_asof_features.py with the desired as_of_date and analysis window
```

## Documentation

- `docs/bronze_layer.md`: Bronze ingestion design, seed routing, external bootstrap sources, Delta tables, API/retry policy, and ALFRED reproducibility audit.
- `docs/silver_layer.md`: Silver cleaning, quality tagging, revision features, current-only cleaning, and lineage.
- `docs/gold_layer.md`: Integrated as-of Gold mart, transformed features, dashboard usage, and relationship candidate scoring.
