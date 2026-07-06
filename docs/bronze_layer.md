# Bronze Layer

This document describes the current Bronze layer for the FRED/ALFRED lakehouse. Bronze preserves source-like external data, collection lineage, request metadata, and enough timing fields for downstream point-in-time reconstruction.

## Purpose

The Bronze layer has five jobs:

1. Preserve raw FRED/ALFRED API responses and raw observation values.
2. Store ALFRED revision-aware series as observation versions with `realtime_start`, `realtime_end`, and `vintage_date`.
3. Store non-revision current-only series in a separate current raw table.
4. Backfill current-only series whose FRED full history is restricted by using Macrotrends or Yahoo Finance bootstrap sources.
5. Track every collection run, request parameter hash, payload hash, retry result, and source provider.

## Active Notebooks

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
```

The active flow is `01a`, `01b`, `01c`, `01d`, `01e`, and `01f`. Older physical daily snapshot experiments are no longer the operating design.

## Seed Routing

The seed catalog is `notebooks/databricks/configs/fred_seed_series.json`. It currently contains 27 series: 21 ALFRED-capable series and 6 FRED current-only series.

```text
alfred_available = true   -> bronze/01a and bronze/01b
alfred_available = false  -> bronze/01c
external bootstrap gaps   -> bronze/01d Macrotrends or bronze/01f Yahoo Finance, then bronze/01c refresh
```

| Rank | Series ID | Domain | Frequency | Role | Bronze route |
|---:|---|---|---|---|---|
| 1 | `GDPC1` | national_accounts | quarterly | Real Gross Domestic Product | ALFRED version |
| 2 | `PCE` | income_consumption | monthly | Personal Consumption Expenditures | ALFRED version |
| 3 | `W875RX1` | income_consumption | monthly | Real Personal Income excluding Current Transfer Receipts | FRED current |
| 4 | `PAYEMS` | labor | monthly | All Employees: Total Nonfarm Payrolls | ALFRED version |
| 5 | `UNRATE` | labor | monthly | Unemployment Rate | ALFRED version |
| 6 | `ICSA` | labor | weekly | Initial Claims | ALFRED version |
| 7 | `AWHMAN` | labor | monthly | Average Weekly Hours: Manufacturing | ALFRED version |
| 8 | `INDPRO` | production | monthly | Industrial Production Index | ALFRED version |
| 9 | `CMRMTSPL` | production | monthly | Real Manufacturing and Trade Industries Sales | FRED current |
| 10 | `CPIAUCSL` | inflation | monthly | Consumer Price Index: All Items | ALFRED version |
| 11 | `PCEPILFE` | inflation | monthly | Core PCE Price Index | ALFRED version |
| 12 | `FEDFUNDS` | rates | monthly | Effective Federal Funds Rate | ALFRED version |
| 13 | `GS10` | rates | monthly | 10-Year Treasury Constant Maturity Rate | ALFRED version |
| 14 | `T10YFFM` | rates | monthly | spread | FRED current |
| 15 | `M2SL` | money_credit | monthly | M2 Money Stock | ALFRED version |
| 16 | `BAMLH0A0HYM2` | credit | daily | ICE BofA US High Yield Index Option-Adjusted Spread | Macrotrends bootstrap + FRED current |
| 17 | `SP500` | financial_markets | daily | / | Yahoo Finance bootstrap + FRED current |
| 18 | `PERMIT` | housing | monthly | Building Permits: Total Units | ALFRED version |
| 19 | `NEWORDER` | investment | monthly | New Orders: Nondefense Capital Goods ex. Aircraft | ALFRED version |
| 20 | `UMCSENT` | sentiment | monthly | University of Michigan Consumer Sentiment | ALFRED version |
| 21 | `WALCL` | liquidity | weekly | / | ALFRED version |
| 22 | `BOGMBASE` | liquidity | monthly | / | ALFRED version |
| 23 | `DGS10` | rates | daily | 10  / | ALFRED version |
| 24 | `T10Y2Y` | rates | daily | / | ALFRED version |
| 25 | `T10Y3M` | rates | daily | 10-3  / | ALFRED version |
| 26 | `NASDAQCOM` | financial_markets | daily | / | FRED current |
| 27 | `BOGZ1FL663067003Q` | leverage | quarterly | / margin loan proxy | ALFRED version |

Current-only series:

```text
W875RX1, CMRMTSPL, T10YFFM, BAMLH0A0HYM2, SP500, NASDAQCOM
```

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
| `fred_series_metadata_versions` | Series metadata versions |
| `fred_observation_versions` | ALFRED revision-aware observation versions |
| `fred_vintage_dates_seen` | ALFRED vintage date checks |
| `fred_incremental_watermarks` | Incremental watermarks for `01b` |
| `fred_current_observations_raw` | FRED current-only rows plus Macrotrends/Yahoo bootstrap rows |
| `fred_run_summary` | Bronze run summaries |

`fred_current_observations_raw` keeps `source = fred` for downstream compatibility, while the actual provider is preserved in `data_provider`, such as `fred_api`, `macrotrends`, or `yahoo_finance`.

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

FRED current-only refresh. It stores rows by `source`, `series_id`, and `observation_date`; if a value changes, the row is updated. This notebook also refreshes series that were initially backfilled by Macrotrends or Yahoo Finance.

### `01d_bronze_macrotrends_observations.py`

Bootstrap loader for manually downloaded Macrotrends CSV files. It currently supports `BAMLH0A0HYM2` backfill into `fred_current_observations_raw`. These rows are current reconstructed history, not ALFRED vintage records.

### `01e_bronze_alfred_reproducibility_audit.py`

Interactive reproducibility audit. The user selects series and as-of dates, and the notebook compares the internal reconstruction against live ALFRED vintage-date responses. It returns match/mismatch results without persisting a separate audit table.

### `01f_bronze_yahoo_finance_observations.py`

Bootstrap loader for Yahoo Finance chart history. It currently supports `SP500` via `^GSPC` and writes daily current reconstructed market history into `fred_current_observations_raw`.

## Reproducibility Scope

The Bronze reproducibility audit answers two questions:

1. Does the internal as-of reconstruction match ALFRED for a selected vintage date?
2. Do `realtime_start`, `realtime_end`, and `available_at` correctly describe which values were visible at that as-of date?

Direct comparison to original releasing institutions such as BEA, BLS, or the Federal Reserve is a separate cross-source validation problem. It needs explicit mappings from FRED series IDs to official source datasets, units, frequencies, and adjustment rules.

## Recommended Execution

Initial build:

```text
1. bronze/01a_bronze_fred_bootstrap_versions.py
2. bronze/01d_bronze_macrotrends_observations.py      when needed
3. bronze/01f_bronze_yahoo_finance_observations.py   when needed
4. bronze/01c_bronze_fred_current_observations.py
5. bronze/01e_bronze_alfred_reproducibility_audit.py
6. silver/02a_silver_fred_bootstrap_versions.py
7. silver/02c_silver_fred_current_observations.py
8. gold/03_gold_fred_asof_features.py
```

Refresh workflow:

```text
bronze/01b_bronze_fred_incremental_versions.py
-> bronze/01c_bronze_fred_current_observations.py
-> bronze/01e_bronze_alfred_reproducibility_audit.py
-> silver/02b_silver_fred_incremental_versions.py
-> silver/02c_silver_fred_current_observations.py
-> gold/03_gold_fred_asof_features.py
```

`01d` and `01f` are bootstrap helpers, so they are not normally part of the daily workflow.
