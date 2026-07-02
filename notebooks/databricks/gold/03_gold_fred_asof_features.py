# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold FRED/ALFRED As-Of Feature Mart
# MAGIC
# MAGIC Single as-of-date Gold layer build for integrated ALFRED revision-aware and FRED current-only indicators.
# MAGIC
# MAGIC This notebook uses Databricks-native building blocks as much as possible:
# MAGIC
# MAGIC - Delta tables for durable serving marts.
# MAGIC - Delta `MERGE` for idempotent re-runs.
# MAGIC - Spark SQL window functions for period alignment, lag features, and candidate screening.
# MAGIC - Delta table properties for Change Data Feed and write optimization.
# MAGIC - A Feature Store-compatible snapshot table keyed by `series_id` and `as_of_date`.
# MAGIC
# MAGIC Recommended operation:
# MAGIC
# MAGIC ```text
# MAGIC Lakeflow Job:
# MAGIC   01a/01b Bronze ALFRED + 01c Bronze FRED current
# MAGIC   -> 02a/02b Silver ALFRED + 02c Silver FRED current
# MAGIC   -> 03 Gold as-of feature mart
# MAGIC ```

# COMMAND ----------

from datetime import datetime, timezone

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")
dbutils.widgets.text("as_of_date", "", "As-of date, blank = UTC today")
dbutils.widgets.text("series_ids", "ALL", "Series IDs: GDPC1,UNRATE or ALL")
dbutils.widgets.text("candidate_series_ids", "ALL", "Candidate series IDs: ALL or comma list")
dbutils.widgets.text("target_series_id", "GDPC1", "Target series ID for candidate screening")
dbutils.widgets.dropdown("target_frequency", "monthly", ["daily", "monthly", "quarterly", "annual", "native"], "Target frequency")
dbutils.widgets.dropdown("aggregation_method", "last", ["last", "mean"], "Period aggregation")
dbutils.widgets.dropdown("relationship_transform_type", "raw", ["raw", "change_1", "change_12", "pct_change_1", "pct_change_12", "log_diff_1", "log_diff_12", "z_score_full_sample", "index_base100"], "Transform used for relationship scoring")
dbutils.widgets.text("max_lag_periods", "12", "Max candidate lag periods")
dbutils.widgets.text("min_pair_count", "24", "Minimum aligned target/candidate pairs")
dbutils.widgets.text("min_candidate_score", "0.3", "Minimum candidate score for top relationship view")
dbutils.widgets.dropdown("include_quality_warnings", "true", ["true", "false"], "Include Silver warning rows")
dbutils.widgets.dropdown("optimize_tables", "true", ["true", "false"], "Run OPTIMIZE when available")

CATALOG = dbutils.widgets.get("catalog").strip()
SILVER_SCHEMA = dbutils.widgets.get("silver_schema").strip()
GOLD_SCHEMA = dbutils.widgets.get("gold_schema").strip()
REQUESTED_AS_OF_DATE = dbutils.widgets.get("as_of_date").strip() or None
LOAD_TYPE = "as_of"
SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
CANDIDATE_SERIES_IDS_PARAM = dbutils.widgets.get("candidate_series_ids").strip()
TARGET_SERIES_ID = dbutils.widgets.get("target_series_id").strip().upper()
TARGET_FREQUENCY = dbutils.widgets.get("target_frequency").strip().lower()
AGGREGATION_METHOD = dbutils.widgets.get("aggregation_method").strip().lower()
RELATIONSHIP_TRANSFORM_TYPE = dbutils.widgets.get("relationship_transform_type").strip().lower()
MAX_LAG_PERIODS = int(dbutils.widgets.get("max_lag_periods"))
MIN_PAIR_COUNT = int(dbutils.widgets.get("min_pair_count"))
MIN_CANDIDATE_SCORE = float(dbutils.widgets.get("min_candidate_score"))
INCLUDE_QUALITY_WARNINGS = dbutils.widgets.get("include_quality_warnings").strip().lower() == "true"
OPTIMIZE_TABLES = dbutils.widgets.get("optimize_tables").strip().lower() == "true"

GOLD_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
GOLD_PROCESSED_AT_UTC = datetime.now(timezone.utc).isoformat()
AS_OF_DATE = REQUESTED_AS_OF_DATE or datetime.now(timezone.utc).date().isoformat()

TRANSFORM_NAME = "build_fred_gold_asof_feature_mart"
TRANSFORM_VERSION = "1.0.0-asof-integrated"

SUPPORTED_TRANSFORM_TYPES = {
    "raw",
    "change_1",
    "change_12",
    "pct_change_1",
    "pct_change_12",
    "log_diff_1",
    "log_diff_12",
    "z_score_full_sample",
    "index_base100",
}

# COMMAND ----------

def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def comma_sql_literals(values: list[str]) -> str:
    return ", ".join(sql_literal(value) for value in values)


def normalize_id_list(value: str) -> list[str]:
    if value.strip().upper() == "ALL":
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def silver_table(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(SILVER_SCHEMA)}.{quote_ident(table)}"


def gold_table(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(GOLD_SCHEMA)}.{quote_ident(table)}"


def table_exists(qualified_name: str) -> bool:
    try:
        return spark.catalog.tableExists(qualified_name)
    except Exception:
        try:
            spark.table(qualified_name).limit(1).collect()
            return True
        except Exception:
            return False


def create_empty_delta_table_from_view(table: str, view_name: str) -> None:
    if table_exists(table.replace("`", "")):
        return
    spark.sql(f"CREATE TABLE {table} USING DELTA AS SELECT * FROM {quote_ident(view_name)} WHERE 1 = 0")


def spark_sql_type(data_type) -> str:
    return data_type.simpleString().upper()


def ensure_table_has_source_columns(table: str, view_name: str) -> None:
    existing_columns = {field.name for field in spark.table(table.replace("`", "")).schema.fields}
    for field in spark.table(view_name).schema.fields:
        if field.name in existing_columns:
            continue
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({quote_ident(field.name)} {spark_sql_type(field.dataType)})")


def delete_where(table: str, predicate: str) -> None:
    if not table_exists(table.replace("`", "")):
        return
    spark.sql(f"DELETE FROM {table} WHERE {predicate}")


def merge_view(table: str, view_name: str, key_fields: list[str]) -> None:
    create_empty_delta_table_from_view(table, view_name)
    ensure_table_has_source_columns(table, view_name)
    source_columns = [field.name for field in spark.table(view_name).schema.fields]
    on_clause = " AND ".join(f"target.{quote_ident(field)} <=> source.{quote_ident(field)}" for field in key_fields)
    update_clause = ", ".join(f"target.{quote_ident(column)} = source.{quote_ident(column)}" for column in source_columns)
    insert_columns = ", ".join(quote_ident(column) for column in source_columns)
    insert_values = ", ".join(f"source.{quote_ident(column)}" for column in source_columns)
    spark.sql(
        f"""
        MERGE INTO {table} AS target
        USING {quote_ident(view_name)} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )


def append_view(table: str, view_name: str) -> None:
    (
        spark.table(view_name)
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(table)
    )


def set_delta_properties(table: str) -> None:
    try:
        spark.sql(
            f"""
            ALTER TABLE {table}
            SET TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true',
                'lakehouse.layer' = 'gold',
                'lakehouse.source' = 'fred'
            )
            """
        )
    except Exception as exc:
        print(f"Delta table properties were not applied to {table.replace('`', '')}: {exc}")


def optimize_table(table: str, zorder_columns: list[str]) -> None:
    if not OPTIMIZE_TABLES:
        return
    try:
        cols = ", ".join(quote_ident(column) for column in zorder_columns)
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")
    except Exception as exc:
        print(f"OPTIMIZE skipped for {table.replace('`', '')}: {exc}")


spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(GOLD_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(GOLD_SCHEMA)}")
# Serverless does not support the global Delta auto-merge Spark conf.
# Schema evolution is handled through writer options where append writes need it.

if not table_exists(f"{CATALOG}.{SILVER_SCHEMA}.fred_observation_versions_cleaned"):
    raise ValueError("Silver ALFRED table was not found. Run 02a/02b Silver versioned cleaning first.")
if not table_exists(f"{CATALOG}.{SILVER_SCHEMA}.fred_current_observations_cleaned"):
    raise ValueError("Silver FRED current table was not found. Run 01c Bronze and 02c Silver current first.")

if TARGET_FREQUENCY not in {"daily", "monthly", "quarterly", "annual", "native"}:
    raise ValueError(f"Unsupported target_frequency: {TARGET_FREQUENCY}")

if AGGREGATION_METHOD not in {"last", "mean"}:
    raise ValueError(f"Unsupported aggregation_method: {AGGREGATION_METHOD}")

if RELATIONSHIP_TRANSFORM_TYPE not in SUPPORTED_TRANSFORM_TYPES:
    raise ValueError(f"Unsupported relationship_transform_type: {RELATIONSHIP_TRANSFORM_TYPE}")

selected_series = normalize_id_list(SERIES_IDS_PARAM)
candidate_series = normalize_id_list(CANDIDATE_SERIES_IDS_PARAM)
if selected_series:
    selected_series = sorted(set(selected_series + [TARGET_SERIES_ID] + candidate_series))

selected_series_base_predicate = "1 = 1" if not selected_series else f"upper(series_id) IN ({comma_sql_literals(selected_series)})"
quality_predicate = "quality_status <> 'error'" if INCLUDE_QUALITY_WARNINGS else "quality_status = 'valid'"

def collect_series_ids(predicate: str) -> list[str]:
    return [
        row["series_id"]
        for row in spark.sql(
            f"""
            SELECT DISTINCT series_id
            FROM (
                SELECT DISTINCT upper(series_id) AS series_id
                FROM {silver_table("fred_observation_versions_cleaned")}
                WHERE is_point_in_time_usable
                  AND value_numeric IS NOT NULL
                  AND {predicate}
                UNION
                SELECT DISTINCT upper(series_id) AS series_id
                FROM {silver_table("fred_current_observations_cleaned")}
                WHERE value_numeric IS NOT NULL
                  AND {predicate}
            ) series_union
            ORDER BY series_id
            """
        ).collect()
    ]

all_selected_series = collect_series_ids(selected_series_base_predicate)
if TARGET_SERIES_ID not in all_selected_series:
    target_rows = collect_series_ids(f"upper(series_id) = {sql_literal(TARGET_SERIES_ID)}")
    all_selected_series.extend(target_rows)
all_selected_series = sorted(set(all_selected_series))
if TARGET_SERIES_ID not in all_selected_series:
    raise ValueError(
        f"Target series {TARGET_SERIES_ID} was not found in Silver cleaned observations. "
        "Use a series_id present in ALFRED or FRED current Silver tables, for example GDPC1 or SP500."
    )

processing_series = all_selected_series

if not processing_series:
    message = "No selected Silver rows found for Gold as-of feature mart."
    print(message)
    dbutils.notebook.exit(message)

series_predicate = f"upper(series_id) IN ({comma_sql_literals(processing_series)})"
processing_series_delete_predicate = f"upper(series_id) IN ({comma_sql_literals(processing_series)})"
scoring_candidate_series = [] if not candidate_series else candidate_series
candidate_scoring_predicate = "1 = 1" if not candidate_series else f"upper(candidate.series_id) IN ({comma_sql_literals(candidate_series)})"
candidate_delete_predicate = "1 = 1" if not candidate_series else f"upper(candidate_series_id) IN ({comma_sql_literals(candidate_series)})"
print(f"Gold target schema: {CATALOG}.{GOLD_SCHEMA}")
print(f"Gold run_id: {GOLD_RUN_ID}")
print(f"Load type: {LOAD_TYPE}")
print(f"As-of date: {AS_OF_DATE}")
print(f"Target frequency: {TARGET_FREQUENCY}")
print(f"Aggregation method: {AGGREGATION_METHOD}")
print(f"Relationship transform type: {RELATIONSHIP_TRANSFORM_TYPE}")
print(f"Target series: {TARGET_SERIES_ID}")
print(f"Max lag periods: {MAX_LAG_PERIODS}")
print(f"Minimum candidate score: {MIN_CANDIDATE_SCORE}")
print(f"Include Silver warnings: {INCLUDE_QUALITY_WARNINGS}")
print(f"Processing series count: {len(processing_series)}")
print("Processing series:", ", ".join(processing_series[:20]) + (" ..." if len(processing_series) > 20 else ""))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_asof_candidates AS
    SELECT
        {sql_literal(GOLD_RUN_ID)} AS gold_run_id,
        {sql_literal(GOLD_PROCESSED_AT_UTC)} AS gold_processed_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        DATE {sql_literal(AS_OF_DATE)} AS as_of_date,
        'revision_aware' AS history_type,
        'realtime_window' AS availability_basis,
        true AS is_revision_aware,
        source,
        series_id,
        domain,
        priority,
        silver_run_id,
        silver_processed_at_utc,
        first_seen_bronze_run_id,
        first_collected_at_utc,
        last_seen_bronze_run_id,
        last_collected_at_utc,
        to_date(observation_date) AS observation_date,
        to_date(period_start) AS period_start,
        to_date(period_end) AS period_end,
        period_inference_basis,
        value_numeric,
        value_raw,
        to_date(realtime_start) AS realtime_start,
        to_date(realtime_end) AS realtime_end,
        to_date(vintage_date) AS vintage_date,
        to_date(coalesce(available_at, realtime_start)) AS available_at,
        frequency,
        frequency_short,
        units,
        units_short,
        seasonal_adjustment,
        observation_version_id,
        revision_number,
        revision_count,
        is_observation_date_revised,
        is_outlier,
        outlier_score,
        quality_status,
        quality_issues,
        ROW_NUMBER() OVER (
            PARTITION BY series_id, to_date(observation_date)
            ORDER BY to_date(realtime_start) DESC, to_date(coalesce(available_at, realtime_start)) DESC, observation_version_id DESC
        ) AS asof_version_rank
    FROM {silver_table("fred_observation_versions_cleaned")}
    WHERE is_point_in_time_usable
      AND {quality_predicate}
      AND value_numeric IS NOT NULL
      AND to_date(observation_date) <= DATE {sql_literal(AS_OF_DATE)}
      AND to_date(realtime_start) <= DATE {sql_literal(AS_OF_DATE)}
      AND to_date(realtime_end) >= DATE {sql_literal(AS_OF_DATE)}
      AND to_date(coalesce(available_at, realtime_start)) <= DATE {sql_literal(AS_OF_DATE)}
      AND {series_predicate}
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW gold_alfred_asof_observations_stage AS
    SELECT * EXCEPT (asof_version_rank)
    FROM gold_asof_candidates
    WHERE asof_version_rank = 1
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_fred_current_asof_observations_stage AS
    SELECT
        {sql_literal(GOLD_RUN_ID)} AS gold_run_id,
        {sql_literal(GOLD_PROCESSED_AT_UTC)} AS gold_processed_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        DATE {sql_literal(AS_OF_DATE)} AS as_of_date,
        'current_only' AS history_type,
        'observation_date' AS availability_basis,
        false AS is_revision_aware,
        source,
        upper(series_id) AS series_id,
        domain,
        priority,
        silver_run_id,
        silver_processed_at_utc,
        bronze_run_id AS first_seen_bronze_run_id,
        collected_at_utc AS first_collected_at_utc,
        bronze_run_id AS last_seen_bronze_run_id,
        collected_at_utc AS last_collected_at_utc,
        to_date(observation_date) AS observation_date,
        to_date(period_start) AS period_start,
        to_date(period_end) AS period_end,
        period_inference_basis,
        value_numeric,
        value_raw,
        to_date(realtime_start) AS realtime_start,
        to_date(realtime_end) AS realtime_end,
        CAST(NULL AS DATE) AS vintage_date,
        to_date(observation_date) AS available_at,
        expected_frequency AS frequency,
        CAST(NULL AS STRING) AS frequency_short,
        CAST(NULL AS STRING) AS units,
        CAST(NULL AS STRING) AS units_short,
        CAST(NULL AS STRING) AS seasonal_adjustment,
        current_observation_id AS observation_version_id,
        CAST(NULL AS INT) AS revision_number,
        CAST(NULL AS INT) AS revision_count,
        false AS is_observation_date_revised,
        is_outlier,
        outlier_score,
        quality_status,
        quality_issues
    FROM {silver_table("fred_current_observations_cleaned")}
    WHERE {quality_predicate}
      AND value_numeric IS NOT NULL
      AND to_date(observation_date) <= DATE {sql_literal(AS_OF_DATE)}
      AND {series_predicate}
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW gold_asof_observations_stage AS
    SELECT * FROM gold_alfred_asof_observations_stage
    UNION ALL
    SELECT * FROM gold_fred_current_asof_observations_stage
    """
)

delete_where(
    gold_table("fred_asof_observations"),
    f"as_of_date = DATE {sql_literal(AS_OF_DATE)} AND {processing_series_delete_predicate}",
)
merge_view(
    gold_table("fred_asof_observations"),
    "gold_asof_observations_stage",
    ["as_of_date", "series_id", "observation_date"],
)
set_delta_properties(gold_table("fred_asof_observations"))

# COMMAND ----------

period_start_expr = {
    "daily": "observation_date",
    "monthly": "to_date(date_trunc('MONTH', observation_date))",
    "quarterly": "to_date(date_trunc('QUARTER', observation_date))",
    "annual": "to_date(date_trunc('YEAR', observation_date))",
    "native": "coalesce(period_start, observation_date)",
}[TARGET_FREQUENCY]

period_end_expr = {
    "daily": "observation_date",
    "monthly": "last_day(to_date(date_trunc('MONTH', observation_date)))",
    "quarterly": "date_sub(add_months(to_date(date_trunc('QUARTER', observation_date)), 3), 1)",
    "annual": "make_date(year(observation_date), 12, 31)",
    "native": "coalesce(period_end, observation_date)",
}[TARGET_FREQUENCY]

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_period_candidates AS
    SELECT
        *,
        {sql_literal(TARGET_FREQUENCY)} AS target_frequency,
        {sql_literal(AGGREGATION_METHOD)} AS aggregation_method,
        {period_start_expr} AS target_period_start,
        {period_end_expr} AS target_period_end,
        ROW_NUMBER() OVER (
            PARTITION BY as_of_date, series_id, {period_start_expr}
            ORDER BY observation_date DESC, realtime_start DESC, observation_version_id DESC
        ) AS period_last_rank
    FROM gold_asof_observations_stage
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_period_features_stage AS
    WITH period_aggregates AS (
        SELECT
            gold_run_id,
            gold_processed_at_utc,
            transform_name,
            transform_version,
            as_of_date,
            target_frequency,
            aggregation_method,
            source,
            first(history_type, true) AS history_type,
            first(availability_basis, true) AS availability_basis,
            max(CAST(is_revision_aware AS INT)) = 1 AS is_revision_aware,
            series_id,
            max(domain) AS domain,
            max(priority) AS priority,
            target_period_start AS period_start,
            target_period_end AS period_end,
            avg(value_numeric) AS mean_value_numeric,
            min(value_numeric) AS min_value_numeric,
            max(value_numeric) AS max_value_numeric,
            count(*) AS source_observation_count,
            sum(CASE WHEN quality_status = 'warning' THEN 1 ELSE 0 END) AS quality_warning_count,
            sum(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_count,
            sum(CASE WHEN is_observation_date_revised THEN 1 ELSE 0 END) AS revised_observation_count,
            max(revision_count) AS max_revision_count,
            max(CASE WHEN period_last_rank = 1 THEN value_numeric END) AS last_value_numeric,
            max(CASE WHEN period_last_rank = 1 THEN observation_date END) AS last_observation_date,
            max(CASE WHEN period_last_rank = 1 THEN available_at END) AS last_available_at,
            max(CASE WHEN period_last_rank = 1 THEN realtime_start END) AS last_realtime_start,
            max(CASE WHEN period_last_rank = 1 THEN realtime_end END) AS last_realtime_end,
            max(CASE WHEN period_last_rank = 1 THEN observation_version_id END) AS last_observation_version_id,
            max(frequency) AS source_frequency,
            max(frequency_short) AS source_frequency_short,
            max(units) AS units,
            max(units_short) AS units_short,
            max(seasonal_adjustment) AS seasonal_adjustment,
            min(silver_processed_at_utc) AS source_min_silver_processed_at_utc,
            max(silver_processed_at_utc) AS source_max_silver_processed_at_utc,
            min(first_collected_at_utc) AS source_min_first_collected_at_utc,
            max(last_collected_at_utc) AS source_max_last_collected_at_utc
        FROM gold_period_candidates
        GROUP BY
            gold_run_id,
            gold_processed_at_utc,
            transform_name,
            transform_version,
            as_of_date,
            target_frequency,
            aggregation_method,
            source,
            series_id,
            target_period_start,
            target_period_end
    )
    SELECT
        *,
        CASE
            WHEN aggregation_method = 'mean' THEN mean_value_numeric
            ELSE last_value_numeric
        END AS value_numeric,
        sha2(concat_ws('|', as_of_date, target_frequency, aggregation_method, series_id, period_start), 256) AS gold_period_feature_id
    FROM period_aggregates
    WHERE period_start IS NOT NULL
    """
)

delete_where(
    gold_table("fred_period_features_long"),
    f"as_of_date = DATE {sql_literal(AS_OF_DATE)} AND target_frequency = {sql_literal(TARGET_FREQUENCY)} AND aggregation_method = {sql_literal(AGGREGATION_METHOD)} AND {processing_series_delete_predicate}",
)
merge_view(
    gold_table("fred_period_features_long"),
    "gold_period_features_stage",
    ["as_of_date", "target_frequency", "aggregation_method", "series_id", "period_start"],
)
set_delta_properties(gold_table("fred_period_features_long"))

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW gold_transformed_feature_base AS
    SELECT
        *,
        LAG(value_numeric, 1) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS transform_lag_1_value_numeric,
        LAG(value_numeric, 12) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS transform_lag_12_value_numeric,
        CASE WHEN value_numeric > 0 THEN log(value_numeric) END AS transform_log_value_numeric,
        LAG(CASE WHEN value_numeric > 0 THEN log(value_numeric) END, 1) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS transform_lag_1_log_value_numeric,
        LAG(CASE WHEN value_numeric > 0 THEN log(value_numeric) END, 12) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS transform_lag_12_log_value_numeric,
        AVG(value_numeric) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
        ) AS transform_series_mean_value_numeric,
        STDDEV_SAMP(value_numeric) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
        ) AS transform_series_stddev_value_numeric,
        FIRST_VALUE(value_numeric) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS transform_base_value_numeric,
        FIRST_VALUE(period_start) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS transform_base_period_start
    FROM gold_period_features_stage
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW gold_transformed_features_stage AS
    SELECT
        *,
        sha2(concat_ws('|', as_of_date, target_frequency, aggregation_method, series_id, period_start, transform_type), 256) AS gold_transformed_feature_id
    FROM (
        SELECT
            gold_run_id,
            gold_processed_at_utc,
            transform_name,
            transform_version,
            as_of_date,
            target_frequency,
            aggregation_method,
            source,
            history_type,
            availability_basis,
            is_revision_aware,
            series_id,
            domain,
            priority,
            source_frequency,
            source_frequency_short,
            units,
            units_short,
            seasonal_adjustment,
            period_start,
            period_end,
            transform_type,
            transformed_value,
            transformed_unit,
            value_numeric AS base_value_numeric,
            comparison_lag_periods,
            lookback_periods,
            calculation_method,
            quality_warning_count,
            outlier_count,
            revised_observation_count,
            max_revision_count,
            source_observation_count,
            last_observation_date,
            last_available_at,
            last_realtime_start,
            last_realtime_end,
            last_observation_version_id,
            gold_period_feature_id AS source_period_feature_id
        FROM gold_transformed_feature_base
        LATERAL VIEW STACK(
            9,
            'raw', value_numeric, coalesce(units_short, units, 'source_unit'), CAST(NULL AS INT), CAST(0 AS INT), 'selected_period_value',
            'change_1', value_numeric - transform_lag_1_value_numeric, CASE WHEN units = 'Percent' OR units_short = 'Percent' THEN 'percentage_point' ELSE coalesce(units_short, units, 'source_unit') END, CAST(1 AS INT), CAST(1 AS INT), 'current_minus_lag_1',
            'change_12', value_numeric - transform_lag_12_value_numeric, CASE WHEN units = 'Percent' OR units_short = 'Percent' THEN 'percentage_point' ELSE coalesce(units_short, units, 'source_unit') END, CAST(12 AS INT), CAST(12 AS INT), 'current_minus_lag_12',
            'pct_change_1', CASE WHEN transform_lag_1_value_numeric IS NULL OR transform_lag_1_value_numeric = 0 THEN NULL ELSE 100.0 * (value_numeric / transform_lag_1_value_numeric - 1.0) END, 'percent', CAST(1 AS INT), CAST(1 AS INT), 'current_div_lag_1_minus_1_times_100',
            'pct_change_12', CASE WHEN transform_lag_12_value_numeric IS NULL OR transform_lag_12_value_numeric = 0 THEN NULL ELSE 100.0 * (value_numeric / transform_lag_12_value_numeric - 1.0) END, 'percent', CAST(12 AS INT), CAST(12 AS INT), 'current_div_lag_12_minus_1_times_100',
            'log_diff_1', 100.0 * (transform_log_value_numeric - transform_lag_1_log_value_numeric), 'log_percent', CAST(1 AS INT), CAST(1 AS INT), 'natural_log_current_minus_lag_1_times_100',
            'log_diff_12', 100.0 * (transform_log_value_numeric - transform_lag_12_log_value_numeric), 'log_percent', CAST(12 AS INT), CAST(12 AS INT), 'natural_log_current_minus_lag_12_times_100',
            'z_score_full_sample', CASE WHEN transform_series_stddev_value_numeric IS NULL OR transform_series_stddev_value_numeric = 0 THEN NULL ELSE (value_numeric - transform_series_mean_value_numeric) / transform_series_stddev_value_numeric END, 'standard_deviation', CAST(NULL AS INT), CAST(NULL AS INT), 'value_minus_asof_series_mean_div_asof_series_stddev',
            'index_base100', CASE WHEN transform_base_value_numeric IS NULL OR transform_base_value_numeric = 0 THEN NULL ELSE 100.0 * value_numeric / transform_base_value_numeric END, 'index_base100', CAST(NULL AS INT), CAST(NULL AS INT), concat('base_period_start=', CAST(transform_base_period_start AS STRING))
        ) stacked AS transform_type, transformed_value, transformed_unit, comparison_lag_periods, lookback_periods, calculation_method
    ) transformed_rows
    WHERE transformed_value IS NOT NULL
    """
)

delete_where(
    gold_table("fred_transformed_features_long"),
    f"as_of_date = DATE {sql_literal(AS_OF_DATE)} AND target_frequency = {sql_literal(TARGET_FREQUENCY)} AND aggregation_method = {sql_literal(AGGREGATION_METHOD)} AND {processing_series_delete_predicate}",
)
merge_view(
    gold_table("fred_transformed_features_long"),
    "gold_transformed_features_stage",
    ["as_of_date", "target_frequency", "aggregation_method", "series_id", "period_start", "transform_type"],
)
set_delta_properties(gold_table("fred_transformed_features_long"))

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW gold_feature_table_stage AS
    SELECT
        *,
        period_start AS feature_timestamp,
        LAG(value_numeric, 1) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS lag_1_value_numeric,
        LAG(value_numeric, 3) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS lag_3_value_numeric,
        LAG(value_numeric, 6) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS lag_6_value_numeric,
        LAG(value_numeric, 12) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS lag_12_value_numeric,
        value_numeric - LAG(value_numeric, 1) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
        ) AS diff_1_value_numeric,
        CASE
            WHEN LAG(value_numeric, 1) OVER (
                PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
                ORDER BY period_start
            ) IS NULL
              OR LAG(value_numeric, 1) OVER (
                PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
                ORDER BY period_start
            ) = 0
            THEN NULL
            ELSE value_numeric / LAG(value_numeric, 1) OVER (
                PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
                ORDER BY period_start
            ) - 1.0
        END AS pct_change_1,
        avg(value_numeric) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_mean_3,
        avg(value_numeric) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS rolling_mean_6,
        avg(value_numeric) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS rolling_mean_12,
        stddev_samp(value_numeric) OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS rolling_stddev_12,
        ROW_NUMBER() OVER (
            PARTITION BY as_of_date, target_frequency, aggregation_method, series_id
            ORDER BY period_start DESC
        ) AS latest_period_rank
    FROM gold_period_features_stage
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW gold_series_feature_snapshot_stage AS
    SELECT
        gold_run_id,
        gold_processed_at_utc,
        transform_name,
        transform_version,
        as_of_date,
        target_frequency,
        aggregation_method,
        source,
        history_type,
        availability_basis,
        is_revision_aware,
        series_id,
        domain,
        priority,
        source_frequency,
        source_frequency_short,
        units,
        units_short,
        seasonal_adjustment,
        feature_timestamp,
        period_start AS latest_period_start,
        period_end AS latest_period_end,
        value_numeric,
        lag_1_value_numeric,
        lag_3_value_numeric,
        lag_6_value_numeric,
        lag_12_value_numeric,
        diff_1_value_numeric,
        pct_change_1,
        rolling_mean_3,
        rolling_mean_6,
        rolling_mean_12,
        rolling_stddev_12,
        source_observation_count,
        quality_warning_count,
        outlier_count,
        revised_observation_count,
        max_revision_count,
        last_observation_date,
        last_available_at,
        last_realtime_start,
        last_realtime_end,
        last_observation_version_id,
        source_min_silver_processed_at_utc,
        source_max_silver_processed_at_utc,
        source_min_first_collected_at_utc,
        source_max_last_collected_at_utc,
        sha2(concat_ws('|', as_of_date, target_frequency, aggregation_method, series_id), 256) AS gold_feature_snapshot_id
    FROM gold_feature_table_stage
    WHERE latest_period_rank = 1
    """
)

delete_where(
    gold_table("fred_series_feature_snapshot"),
    f"as_of_date = DATE {sql_literal(AS_OF_DATE)} AND target_frequency = {sql_literal(TARGET_FREQUENCY)} AND aggregation_method = {sql_literal(AGGREGATION_METHOD)} AND {processing_series_delete_predicate}",
)
merge_view(
    gold_table("fred_series_feature_snapshot"),
    "gold_series_feature_snapshot_stage",
    ["as_of_date", "target_frequency", "aggregation_method", "series_id"],
)
set_delta_properties(gold_table("fred_series_feature_snapshot"))

# COMMAND ----------

lag_period_shift_expr = {
    "daily": "date_sub(target.period_start, lag.lag_periods)",
    "monthly": "add_months(target.period_start, -lag.lag_periods)",
    "quarterly": "add_months(target.period_start, -3 * lag.lag_periods)",
    "annual": "add_months(target.period_start, -12 * lag.lag_periods)",
    "native": "add_months(target.period_start, -lag.lag_periods)",
}[TARGET_FREQUENCY]

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_lag_values AS
    SELECT explode(sequence(0, {MAX_LAG_PERIODS})) AS lag_periods
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_target_periods AS
    SELECT
        as_of_date,
        target_frequency,
        aggregation_method,
        transform_type,
        period_start,
        transformed_value AS target_value_numeric
    FROM gold_transformed_features_stage
    WHERE upper(series_id) = {sql_literal(TARGET_SERIES_ID)}
      AND transform_type = {sql_literal(RELATIONSHIP_TRANSFORM_TYPE)}
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_candidate_pairs AS
    SELECT
        target.as_of_date,
        target.target_frequency,
        target.aggregation_method,
        target.transform_type,
        {sql_literal(TARGET_SERIES_ID)} AS target_series_id,
        candidate.series_id AS candidate_series_id,
        candidate.history_type AS candidate_history_type,
        candidate.availability_basis AS candidate_availability_basis,
        candidate.is_revision_aware AS candidate_is_revision_aware,
        candidate.domain AS candidate_domain,
        candidate.priority AS candidate_priority,
        lag.lag_periods,
        target.period_start AS target_period_start,
        candidate.period_start AS candidate_period_start,
        target.target_value_numeric,
        candidate.transformed_value AS candidate_value_numeric,
        candidate.quality_warning_count AS candidate_quality_warning_count,
        candidate.outlier_count AS candidate_outlier_count,
        candidate.revised_observation_count AS candidate_revised_observation_count
    FROM gold_target_periods target
    CROSS JOIN gold_lag_values lag
    JOIN gold_transformed_features_stage candidate
      ON candidate.as_of_date = target.as_of_date
     AND candidate.target_frequency = target.target_frequency
     AND candidate.aggregation_method = target.aggregation_method
     AND candidate.transform_type = target.transform_type
     AND candidate.period_start = {lag_period_shift_expr}
    WHERE upper(candidate.series_id) <> {sql_literal(TARGET_SERIES_ID)}
      AND {candidate_scoring_predicate}
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_relationship_candidate_scores_stage AS
    WITH target_period_counts AS (
        SELECT
            as_of_date,
            target_frequency,
            aggregation_method,
            transform_type,
            count(*) AS target_period_count
        FROM gold_target_periods
        GROUP BY as_of_date, target_frequency, aggregation_method, transform_type
    ),
    scored AS (
        SELECT
            pairs.as_of_date,
            pairs.target_frequency,
            pairs.aggregation_method,
            pairs.transform_type,
            pairs.target_series_id,
            pairs.candidate_series_id,
            max(pairs.candidate_history_type) AS candidate_history_type,
            max(pairs.candidate_availability_basis) AS candidate_availability_basis,
            max(CAST(pairs.candidate_is_revision_aware AS INT)) = 1 AS candidate_is_revision_aware,
            max(pairs.candidate_domain) AS candidate_domain,
            max(pairs.candidate_priority) AS candidate_priority,
            pairs.lag_periods,
            count(*) AS pair_count,
            corr(pairs.target_value_numeric, pairs.candidate_value_numeric) AS pearson_corr,
            avg(abs(pairs.candidate_quality_warning_count)) AS avg_candidate_quality_warning_count,
            avg(abs(pairs.candidate_outlier_count)) AS avg_candidate_outlier_count,
            sum(CASE WHEN pairs.candidate_revised_observation_count > 0 THEN 1 ELSE 0 END) AS revised_period_count
        FROM gold_candidate_pairs pairs
        WHERE pairs.target_value_numeric IS NOT NULL
          AND pairs.candidate_value_numeric IS NOT NULL
        GROUP BY
            pairs.as_of_date,
            pairs.target_frequency,
            pairs.aggregation_method,
            pairs.transform_type,
            pairs.target_series_id,
            pairs.candidate_series_id,
            pairs.lag_periods
    )
    SELECT
        {sql_literal(GOLD_RUN_ID)} AS gold_run_id,
        {sql_literal(GOLD_PROCESSED_AT_UTC)} AS gold_processed_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        scored.as_of_date,
        scored.target_frequency,
        scored.aggregation_method,
        scored.transform_type,
        scored.target_series_id,
        scored.candidate_series_id,
        scored.candidate_history_type,
        scored.candidate_availability_basis,
        scored.candidate_is_revision_aware,
        scored.candidate_domain,
        scored.candidate_priority,
        scored.lag_periods,
        scored.pair_count,
        counts.target_period_count,
        scored.pair_count / counts.target_period_count AS coverage_rate,
        scored.pearson_corr,
        abs(scored.pearson_corr) AS abs_pearson_corr,
        power(scored.pearson_corr, 2) AS r_squared,
        power(scored.pearson_corr, 2) * (scored.pair_count / counts.target_period_count) AS candidate_score,
        scored.avg_candidate_quality_warning_count,
        scored.avg_candidate_outlier_count,
        scored.revised_period_count,
        DENSE_RANK() OVER (
            PARTITION BY scored.as_of_date, scored.target_frequency, scored.aggregation_method, scored.transform_type, scored.target_series_id
            ORDER BY power(scored.pearson_corr, 2) * (scored.pair_count / counts.target_period_count) DESC NULLS LAST
        ) AS candidate_rank,
        sha2(concat_ws('|', scored.as_of_date, scored.target_frequency, scored.aggregation_method, scored.transform_type, scored.target_series_id, scored.candidate_series_id, scored.lag_periods), 256) AS gold_relationship_candidate_score_id
    FROM scored
    JOIN target_period_counts counts USING (as_of_date, target_frequency, aggregation_method, transform_type)
    WHERE scored.pair_count >= {MIN_PAIR_COUNT}
    """
)

create_empty_delta_table_from_view(gold_table("fred_relationship_candidate_scores"), "gold_relationship_candidate_scores_stage")
ensure_table_has_source_columns(gold_table("fred_relationship_candidate_scores"), "gold_relationship_candidate_scores_stage")
delete_where(
    gold_table("fred_relationship_candidate_scores"),
    f"as_of_date = DATE {sql_literal(AS_OF_DATE)} AND target_frequency = {sql_literal(TARGET_FREQUENCY)} AND aggregation_method = {sql_literal(AGGREGATION_METHOD)} AND transform_type = {sql_literal(RELATIONSHIP_TRANSFORM_TYPE)} AND target_series_id = {sql_literal(TARGET_SERIES_ID)} AND {candidate_delete_predicate}",
)
merge_view(
    gold_table("fred_relationship_candidate_scores"),
    "gold_relationship_candidate_scores_stage",
    ["as_of_date", "target_frequency", "aggregation_method", "transform_type", "target_series_id", "candidate_series_id", "lag_periods"],
)
set_delta_properties(gold_table("fred_relationship_candidate_scores"))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {gold_table("fred_latest_feature_snapshot")} AS
    SELECT * EXCEPT (latest_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY target_frequency, aggregation_method, series_id
                ORDER BY as_of_date DESC, gold_processed_at_utc DESC
            ) AS latest_rank
        FROM {gold_table("fred_series_feature_snapshot")}
    ) ranked
    WHERE latest_rank = 1
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {gold_table("fred_top_relationship_candidates")} AS
    SELECT * EXCEPT (latest_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY target_frequency, aggregation_method, transform_type, target_series_id, candidate_series_id, lag_periods
                ORDER BY as_of_date DESC, gold_processed_at_utc DESC
            ) AS latest_rank
        FROM {gold_table("fred_relationship_candidate_scores")}
        WHERE candidate_score >= {MIN_CANDIDATE_SCORE}
    ) ranked
    WHERE latest_rank = 1
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_quality_report_stage AS
    SELECT
        {sql_literal(GOLD_RUN_ID)} AS gold_run_id,
        {sql_literal(GOLD_PROCESSED_AT_UTC)} AS evaluated_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        DATE {sql_literal(AS_OF_DATE)} AS as_of_date,
        {sql_literal(TARGET_FREQUENCY)} AS target_frequency,
        {sql_literal(AGGREGATION_METHOD)} AS aggregation_method,
        'asof_not_before_observation' AS rule_name,
        'error' AS severity,
        CAST((SELECT count(*) FROM gold_asof_observations_stage) AS BIGINT) AS total_count,
        CAST((SELECT count(*) FROM gold_asof_observations_stage WHERE as_of_date < observation_date) AS BIGINT) AS failed_count
    UNION ALL
    SELECT
        {sql_literal(GOLD_RUN_ID)},
        {sql_literal(GOLD_PROCESSED_AT_UTC)},
        {sql_literal(TRANSFORM_NAME)},
        {sql_literal(TRANSFORM_VERSION)},
        DATE {sql_literal(AS_OF_DATE)},
        {sql_literal(TARGET_FREQUENCY)},
        {sql_literal(AGGREGATION_METHOD)},
        'visible_inside_realtime_window',
        'error',
        CAST((SELECT count(*) FROM gold_asof_observations_stage) AS BIGINT),
        CAST((
            SELECT count(*)
            FROM gold_asof_observations_stage
            WHERE as_of_date < realtime_start OR as_of_date > realtime_end
        ) AS BIGINT)
    UNION ALL
    SELECT
        {sql_literal(GOLD_RUN_ID)},
        {sql_literal(GOLD_PROCESSED_AT_UTC)},
        {sql_literal(TRANSFORM_NAME)},
        {sql_literal(TRANSFORM_VERSION)},
        DATE {sql_literal(AS_OF_DATE)},
        {sql_literal(TARGET_FREQUENCY)},
        {sql_literal(AGGREGATION_METHOD)},
        'period_feature_value_not_null',
        'warning',
        CAST((SELECT count(*) FROM gold_period_features_stage) AS BIGINT),
        CAST((SELECT count(*) FROM gold_period_features_stage WHERE value_numeric IS NULL) AS BIGINT)
    UNION ALL
    SELECT
        {sql_literal(GOLD_RUN_ID)},
        {sql_literal(GOLD_PROCESSED_AT_UTC)},
        {sql_literal(TRANSFORM_NAME)},
        {sql_literal(TRANSFORM_VERSION)},
        DATE {sql_literal(AS_OF_DATE)},
        {sql_literal(TARGET_FREQUENCY)},
        {sql_literal(AGGREGATION_METHOD)},
        'candidate_scores_meet_min_pair_count',
        'warning',
        CAST((SELECT count(*) FROM gold_relationship_candidate_scores_stage) AS BIGINT),
        CAST((SELECT count(*) FROM gold_relationship_candidate_scores_stage WHERE pair_count < {MIN_PAIR_COUNT}) AS BIGINT)
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW gold_quality_report_final AS
    SELECT
        *,
        CASE WHEN failed_count = 0 THEN 'pass' ELSE 'fail' END AS rule_status,
        CASE WHEN total_count = 0 THEN NULL ELSE failed_count / total_count END AS failure_rate
    FROM gold_quality_report_stage
    """
)

append_view(gold_table("fred_gold_quality_report"), "gold_quality_report_final")
set_delta_properties(gold_table("fred_gold_quality_report"))
# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_run_summary_stage AS
    SELECT
        {sql_literal(GOLD_RUN_ID)} AS gold_run_id,
        {sql_literal(GOLD_PROCESSED_AT_UTC)} AS processed_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        DATE {sql_literal(AS_OF_DATE)} AS as_of_date,
        {sql_literal(TARGET_FREQUENCY)} AS target_frequency,
        {sql_literal(AGGREGATION_METHOD)} AS aggregation_method,
        {sql_literal(RELATIONSHIP_TRANSFORM_TYPE)} AS relationship_transform_type,
        {sql_literal(TARGET_SERIES_ID)} AS target_series_id,
        {sql_literal(LOAD_TYPE)} AS load_type,
        CAST({len(processing_series)} AS BIGINT) AS processing_series_count,
        CAST((SELECT count(*) FROM gold_asof_observations_stage) AS BIGINT) AS asof_observation_count,
        CAST((SELECT count(*) FROM gold_period_features_stage) AS BIGINT) AS period_feature_count,
        CAST((SELECT count(*) FROM gold_transformed_features_stage) AS BIGINT) AS transformed_feature_count,
        CAST((SELECT count(*) FROM gold_series_feature_snapshot_stage) AS BIGINT) AS feature_snapshot_count,
        CAST((SELECT count(*) FROM gold_relationship_candidate_scores_stage) AS BIGINT) AS relationship_candidate_score_count,
        CAST((SELECT count(DISTINCT series_id) FROM gold_asof_observations_stage) AS BIGINT) AS series_count,
        CAST((SELECT count(*) FROM gold_asof_observations_stage WHERE quality_status = 'warning') AS BIGINT) AS silver_warning_rows_used,
        CAST((SELECT count(*) FROM gold_quality_report_final WHERE rule_status = 'fail') AS BIGINT) AS failed_quality_rule_count,
        (SELECT min(silver_processed_at_utc) FROM gold_asof_observations_stage) AS source_min_silver_processed_at_utc,
        (SELECT max(silver_processed_at_utc) FROM gold_asof_observations_stage) AS source_max_silver_processed_at_utc,
        (SELECT min(first_collected_at_utc) FROM gold_asof_observations_stage) AS source_min_first_collected_at_utc,
        (SELECT max(last_collected_at_utc) FROM gold_asof_observations_stage) AS source_max_last_collected_at_utc,
        {sql_literal(str(INCLUDE_QUALITY_WARNINGS).lower())} AS include_quality_warnings,
        {MAX_LAG_PERIODS} AS max_lag_periods,
        {MIN_PAIR_COUNT} AS min_pair_count,
        {MIN_CANDIDATE_SCORE} AS min_candidate_score
    """
)

append_view(gold_table("fred_gold_run_summary"), "gold_run_summary_stage")
set_delta_properties(gold_table("fred_gold_run_summary"))

# COMMAND ----------

for table, zorder_columns in [
    (gold_table("fred_asof_observations"), ["as_of_date", "series_id"]),
    (gold_table("fred_period_features_long"), ["as_of_date", "series_id"]),
    (gold_table("fred_transformed_features_long"), ["as_of_date", "series_id", "transform_type"]),
    (gold_table("fred_series_feature_snapshot"), ["as_of_date", "series_id"]),
    (gold_table("fred_relationship_candidate_scores"), ["as_of_date", "target_series_id"]),
]:
    optimize_table(table, zorder_columns)

# COMMAND ----------

print(
    f"""
Feature Store registration note:
  The table {CATALOG}.{GOLD_SCHEMA}.fred_series_feature_snapshot is shaped for Databricks
  Feature Engineering in Unity Catalog. If the workspace supports UC primary-key
  constraints, register series_id + as_of_date as the time-series key:

  ALTER TABLE {CATALOG}.{GOLD_SCHEMA}.fred_series_feature_snapshot
  ADD CONSTRAINT pk_fred_series_feature_snapshot
  PRIMARY KEY (series_id, as_of_date TIMESERIES);

  This enables point-in-time feature lookups for training sets without future leakage.
"""
)

display(spark.table(gold_table("fred_gold_run_summary")).where(f"gold_run_id = '{GOLD_RUN_ID}'"))

# COMMAND ----------

display(
    spark.table(gold_table("fred_top_relationship_candidates"))
    .where(f"as_of_date = DATE '{AS_OF_DATE}' AND transform_type = '{RELATIONSHIP_TRANSFORM_TYPE}' AND target_series_id = '{TARGET_SERIES_ID}'")
    .orderBy("candidate_score", ascending=False)
)

