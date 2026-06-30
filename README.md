# FRED Lakehouse

Databricks Lakehouse project for FRED/ALFRED economic time-series data.

The active implementation uses Databricks notebooks, Delta Lake, and Unity Catalog. Files are organized by Medallion layer under `notebooks/databricks`.

## Active Layout

```text
notebooks/databricks/
  configs/
    fred_seed_series.json
  bronze/
    01a_bronze_fred_bootstrap_versions.py
    01b_bronze_fred_incremental_versions.py
    01c_bronze_fred_current_observations.py
  silver/
    02a_silver_fred_bootstrap_versions.py
    02b_silver_fred_incremental_versions.py
  gold/
    03a_gold_fred_bootstrap_causal_features.py
    03b_gold_fred_incremental_causal_features.py
docs/
  bronze_layer.md
```

## Bronze Direction

The main Bronze pipeline is vintage-date based, not dense daily physical snapshots.

- `notebooks/databricks/bronze/01a_bronze_fred_bootstrap_versions.py` performs the initial ALFRED-capable revision-history load.
- `notebooks/databricks/bronze/01b_bronze_fred_incremental_versions.py` performs incremental ALFRED vintage-date checks and loads only changed revision windows.
- `notebooks/databricks/bronze/01c_bronze_fred_current_observations.py` handles FRED-only current observations, such as series without ALFRED revision history.

The seed catalog lives at `notebooks/databricks/configs/fred_seed_series.json`.

```text
alfred_available = true   -> bronze/01a + bronze/01b
alfred_available = false  -> bronze/01c
```

## Typical Execution

```text
1. Run bronze/01a once for ALFRED-capable Bronze history.
2. Run silver/02a once to build Silver from Bronze versions.
3. Schedule bronze/01b for incremental Bronze revision checks.
4. Schedule silver/02b after bronze/01b for incremental Silver cleaning.
5. Run bronze/01c separately for FRED-only current series.
6. Run gold/03a once for initial causal features.
7. Schedule gold/03b after silver/02b for incremental causal features.
```

See `docs/bronze_layer.md` for the detailed Bronze operating pattern.