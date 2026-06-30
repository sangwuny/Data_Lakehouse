# Databricks notebook source
# MAGIC %md
# MAGIC # 02a Silver FRED Versioned Bootstrap Cleaning
# MAGIC
# MAGIC Performs the initial full Silver cleaning for revision-aware Bronze FRED/ALFRED observation versions.
# MAGIC
# MAGIC This notebook is designed for the versioned Bronze schema built by
# MAGIC `01a_bronze_fred_bootstrap_versions.py` and prepares the Silver tables for daily incremental cleaning.
# MAGIC It uses Spark SQL/DataFrame transformations, Delta MERGE, `try_cast`, window functions,
# MAGIC and quality flags instead of collecting rows to the driver.

# COMMAND ----------

from datetime import datetime, timezone
from typing import Any

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("series_ids", "ALL", "Series IDs: GDPC1,UNRATE or ALL")
dbutils.widgets.text("outlier_threshold", "6.0", "Robust z-score outlier threshold")
dbutils.widgets.dropdown("include_missing_in_silver", "true", ["true", "false"], "Keep missing rows")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SILVER_SCHEMA = dbutils.widgets.get("silver_schema").strip()
SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
OUTLIER_THRESHOLD = float(dbutils.widgets.get("outlier_threshold"))
INCLUDE_MISSING_IN_SILVER = dbutils.widgets.get("include_missing_in_silver").lower() == "true"

SOURCE = "fred"
TRANSFORM_NAME = "clean_fred_versioned_observations"
TRANSFORM_VERSION = "0.3.0"
LOAD_TYPE = "bootstrap"
SILVER_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
SILVER_PROCESSED_AT_UTC = datetime.now(timezone.utc).isoformat()

# COMMAND ----------

def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def bronze_table(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}.{quote_ident(table)}"


def silver_table(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(SILVER_SCHEMA)}.{quote_ident(table)}"


spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(SILVER_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(SILVER_SCHEMA)}")

print(f"Silver target schema: {CATALOG}.{SILVER_SCHEMA}")
print(f"Silver run_id: {SILVER_RUN_ID}")
print(f"Transform version: {TRANSFORM_VERSION}")
print(f"Load type: {LOAD_TYPE}")

# COMMAND ----------

if SERIES_IDS_PARAM.upper() == "ALL":
    selected_series = [
        row["series_id"]
        for row in spark.sql(
            f"""
            SELECT DISTINCT series_id
            FROM {bronze_table("fred_observation_versions")}
            ORDER BY series_id
            """
        ).collect()
    ]
else:
    selected_series = [item.strip().upper() for item in SERIES_IDS_PARAM.split(",") if item.strip()]

if not selected_series:
    raise ValueError("No series selected for Silver processing.")

series_filter = ""
if SERIES_IDS_PARAM.upper() != "ALL":
    series_values = ", ".join(sql_literal(series_id) for series_id in selected_series)
    series_filter = f"WHERE series_id IN ({series_values})"

print(f"Selected series count: {len(selected_series)}")
print("Selected series:", ", ".join(selected_series[:20]) + (" ..." if len(selected_series) > 20 else ""))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW selected_bronze_versions AS
    SELECT *
    FROM {bronze_table("fred_observation_versions")}
    {series_filter}
    """
)

selected_count = spark.sql("SELECT COUNT(*) AS row_count FROM selected_bronze_versions").collect()[0]["row_count"]
if selected_count == 0:
    raise ValueError("Selected series have no rows in bronze.fred_observation_versions.")

print(f"Selected Bronze observation version rows: {selected_count}")

# COMMAND ----------

missing_filter = "" if INCLUDE_MISSING_IN_SILVER else "WHERE NOT is_missing"

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW silver_base_parse AS
    SELECT
        observation_version_id,
        value_hash,
        source,
        series_id,
        domain,
        priority,
        to_date(observation_date) AS observation_date,
        to_date(period_start_inferred) AS period_start,
        to_date(period_end_inferred) AS period_end,
        period_inference_basis,
        value_raw,
        trim(value_raw) AS value_text,
        try_cast(trim(value_raw) AS DOUBLE) AS value_numeric,
        to_date(realtime_start) AS realtime_start,
        to_date(realtime_end) AS realtime_end,
        to_date(vintage_date) AS vintage_date,
        to_date(available_at) AS available_at,
        frequency,
        frequency_short,
        units,
        units_short,
        seasonal_adjustment,
        request_params_hash,
        first_seen_bronze_run_id,
        first_collected_at_utc,
        last_seen_bronze_run_id,
        last_collected_at_utc,
        seen_count,
        CASE
            WHEN value_raw IS NULL THEN true
            WHEN trim(value_raw) = '' THEN true
            WHEN trim(value_raw) = '.' THEN true
            WHEN try_cast(trim(value_raw) AS DOUBLE) IS NULL THEN true
            ELSE false
        END AS is_missing,
        CASE
            WHEN value_raw IS NULL THEN 'null'
            WHEN trim(value_raw) = '' THEN 'empty_string'
            WHEN trim(value_raw) = '.' THEN 'fred_dot'
            WHEN try_cast(trim(value_raw) AS DOUBLE) IS NULL THEN 'numeric_parse_error'
            ELSE NULL
        END AS missing_reason,
        CASE
            WHEN value_raw IS NOT NULL
             AND trim(value_raw) NOT IN ('', '.')
             AND try_cast(trim(value_raw) AS DOUBLE) IS NULL
            THEN true ELSE false
        END AS is_numeric_parse_error,
        CASE
            WHEN to_date(observation_date) IS NULL THEN true
            WHEN to_date(realtime_start) IS NULL THEN true
            WHEN to_date(realtime_end) IS NULL THEN true
            WHEN to_date(realtime_start) > to_date(realtime_end) THEN true
            ELSE false
        END AS is_realtime_range_error,
        CASE WHEN realtime_end = '9999-12-31' THEN true ELSE false END AS is_current_version
    FROM selected_bronze_versions
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_base AS
    SELECT *
    FROM silver_base_parse
    {missing_filter}
    """
)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW silver_revision_features AS
    SELECT
        base.*,
        COUNT(*) OVER (PARTITION BY observation_version_id) AS observation_version_id_count,
        COUNT(*) OVER (PARTITION BY series_id, observation_date) AS revision_count,
        ROW_NUMBER() OVER (
            PARTITION BY series_id, observation_date
            ORDER BY realtime_start, realtime_end, value_hash, observation_version_id
        ) AS revision_number,
        LAG(value_numeric) OVER (
            PARTITION BY series_id, observation_date
            ORDER BY realtime_start, realtime_end, value_hash, observation_version_id
        ) AS previous_revision_value_numeric
    FROM silver_base base
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW silver_revision_delta AS
    SELECT
        *,
        value_numeric - previous_revision_value_numeric AS revision_delta_value,
        CASE WHEN observation_version_id_count > 1 THEN true ELSE false END AS is_duplicate_observation_version,
        CASE WHEN revision_count > 1 THEN true ELSE false END AS is_observation_date_revised,
        CASE
            WHEN observation_date IS NOT NULL
             AND realtime_start IS NOT NULL
             AND realtime_end IS NOT NULL
             AND realtime_start <= realtime_end
             AND NOT is_missing
            THEN true ELSE false
        END AS is_point_in_time_usable
    FROM silver_revision_features
    """
)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_level_median AS
    SELECT
        series_id,
        percentile_approx(value_numeric, 0.5) AS level_median
    FROM silver_revision_delta
    WHERE value_numeric IS NOT NULL
    GROUP BY series_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW level_abs_deviation AS
    SELECT
        obs.observation_version_id,
        abs(obs.value_numeric - med.level_median) AS level_abs_deviation
    FROM silver_revision_delta obs
    JOIN series_level_median med USING (series_id)
    WHERE obs.value_numeric IS NOT NULL
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_level_mad AS
    SELECT
        obs.series_id,
        percentile_approx(dev.level_abs_deviation, 0.5) AS level_mad
    FROM silver_revision_delta obs
    JOIN level_abs_deviation dev USING (observation_version_id)
    GROUP BY obs.series_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW current_observation_diffs AS
    SELECT
        observation_version_id,
        value_numeric - LAG(value_numeric) OVER (
            PARTITION BY series_id
            ORDER BY observation_date, observation_version_id
        ) AS observation_diff_value
    FROM silver_revision_delta
    WHERE is_current_version AND value_numeric IS NOT NULL
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_diff_median AS
    SELECT
        obs.series_id,
        percentile_approx(diff.observation_diff_value, 0.5) AS diff_median
    FROM silver_revision_delta obs
    JOIN current_observation_diffs diff USING (observation_version_id)
    WHERE diff.observation_diff_value IS NOT NULL
    GROUP BY obs.series_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW diff_abs_deviation AS
    SELECT
        obs.series_id,
        diff.observation_version_id,
        abs(diff.observation_diff_value - med.diff_median) AS diff_abs_deviation
    FROM current_observation_diffs diff
    JOIN silver_revision_delta obs USING (observation_version_id)
    JOIN series_diff_median med ON obs.series_id = med.series_id
    WHERE diff.observation_diff_value IS NOT NULL
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_diff_mad AS
    SELECT
        series_id,
        percentile_approx(diff_abs_deviation, 0.5) AS diff_mad
    FROM diff_abs_deviation
    GROUP BY series_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_revision_median AS
    SELECT
        series_id,
        percentile_approx(revision_delta_value, 0.5) AS revision_delta_median
    FROM silver_revision_delta
    WHERE revision_delta_value IS NOT NULL
    GROUP BY series_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW revision_abs_deviation AS
    SELECT
        obs.observation_version_id,
        abs(obs.revision_delta_value - med.revision_delta_median) AS revision_delta_abs_deviation
    FROM silver_revision_delta obs
    JOIN series_revision_median med USING (series_id)
    WHERE obs.revision_delta_value IS NOT NULL
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_revision_mad AS
    SELECT
        obs.series_id,
        percentile_approx(dev.revision_delta_abs_deviation, 0.5) AS revision_delta_mad
    FROM silver_revision_delta obs
    JOIN revision_abs_deviation dev USING (observation_version_id)
    GROUP BY obs.series_id
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_observations_final_raw AS
    SELECT
        {sql_literal(SILVER_RUN_ID)} AS silver_run_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS silver_processed_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        obs.observation_version_id,
        obs.value_hash,
        obs.source,
        obs.series_id,
        obs.domain,
        obs.priority,
        obs.observation_date,
        obs.period_start,
        obs.period_end,
        obs.period_inference_basis,
        obs.value_raw,
        obs.value_numeric,
        obs.is_missing,
        obs.missing_reason,
        obs.is_numeric_parse_error,
        obs.realtime_start,
        obs.realtime_end,
        obs.vintage_date,
        obs.available_at,
        obs.is_realtime_range_error,
        obs.is_current_version,
        obs.is_point_in_time_usable,
        obs.frequency,
        obs.frequency_short,
        obs.units,
        obs.units_short,
        obs.seasonal_adjustment,
        obs.revision_number,
        obs.revision_count,
        obs.is_observation_date_revised,
        obs.previous_revision_value_numeric,
        obs.revision_delta_value,
        diff.observation_diff_value,
        med.level_median,
        mad.level_mad,
        diff_med.diff_median,
        diff_mad.diff_mad,
        rev_med.revision_delta_median,
        rev_mad.revision_delta_mad,
        CASE
            WHEN obs.value_numeric IS NULL THEN NULL
            WHEN mad.level_mad IS NULL OR mad.level_mad = 0 THEN 0.0
            ELSE 0.6745 * (obs.value_numeric - med.level_median) / mad.level_mad
        END AS outlier_level_score,
        CASE
            WHEN diff.observation_diff_value IS NULL THEN NULL
            WHEN diff_mad.diff_mad IS NULL OR diff_mad.diff_mad = 0 THEN 0.0
            ELSE 0.6745 * (diff.observation_diff_value - diff_med.diff_median) / diff_mad.diff_mad
        END AS outlier_diff_score,
        CASE
            WHEN obs.revision_delta_value IS NULL THEN NULL
            WHEN rev_mad.revision_delta_mad IS NULL OR rev_mad.revision_delta_mad = 0 THEN 0.0
            ELSE 0.6745 * (obs.revision_delta_value - rev_med.revision_delta_median) / rev_mad.revision_delta_mad
        END AS outlier_revision_score,
        obs.is_duplicate_observation_version,
        obs.observation_version_id_count,
        obs.request_params_hash,
        obs.first_seen_bronze_run_id,
        obs.first_collected_at_utc,
        obs.last_seen_bronze_run_id,
        obs.last_collected_at_utc,
        obs.seen_count
    FROM silver_revision_delta obs
    LEFT JOIN series_level_median med USING (series_id)
    LEFT JOIN series_level_mad mad USING (series_id)
    LEFT JOIN current_observation_diffs diff USING (observation_version_id)
    LEFT JOIN series_diff_median diff_med USING (series_id)
    LEFT JOIN series_diff_mad diff_mad USING (series_id)
    LEFT JOIN series_revision_median rev_med USING (series_id)
    LEFT JOIN series_revision_mad rev_mad USING (series_id)
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_observations_final AS
    SELECT
        *,
        greatest(
            coalesce(abs(outlier_diff_score), 0.0),
            coalesce(abs(outlier_revision_score), 0.0)
        ) AS outlier_score,
        {OUTLIER_THRESHOLD} AS outlier_threshold,
        'robust_zscore_mad_current_diff_revision_delta' AS outlier_method,
        CASE
            WHEN greatest(
                coalesce(abs(outlier_diff_score), 0.0),
                coalesce(abs(outlier_revision_score), 0.0)
            ) > {OUTLIER_THRESHOLD}
            THEN true ELSE false
        END AS is_outlier,
        CASE
            WHEN is_realtime_range_error OR is_numeric_parse_error OR is_duplicate_observation_version THEN 'error'
            WHEN is_missing OR greatest(
                coalesce(abs(outlier_diff_score), 0.0),
                coalesce(abs(outlier_revision_score), 0.0)
            ) > {OUTLIER_THRESHOLD} THEN 'warning'
            ELSE 'valid'
        END AS quality_status,
        concat_ws(',', array(
            CASE WHEN is_missing THEN concat('missing:', coalesce(missing_reason, 'unknown')) END,
            CASE WHEN is_numeric_parse_error THEN 'numeric_parse_error' END,
            CASE WHEN is_realtime_range_error THEN 'realtime_range_error' END,
            CASE WHEN is_duplicate_observation_version THEN 'duplicate_observation_version_id' END,
            CASE WHEN greatest(
                coalesce(abs(outlier_diff_score), 0.0),
                coalesce(abs(outlier_revision_score), 0.0)
            ) > {OUTLIER_THRESHOLD} THEN 'outlier' END
        )) AS quality_issues
    FROM silver_observations_final_raw
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW silver_observations_stage AS
    SELECT * EXCEPT (silver_key_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY observation_version_id
                ORDER BY last_collected_at_utc DESC, last_seen_bronze_run_id DESC
            ) AS silver_key_rank
        FROM silver_observations_final
        WHERE observation_version_id IS NOT NULL
    ) ranked
    WHERE silver_key_rank = 1
    """
)

# COMMAND ----------

def ensure_delta_table_from_view(table: str, view_name: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {silver_table(table)}
        USING DELTA
        AS SELECT * FROM {quote_ident(view_name)} WHERE 1 = 0
        """
    )


def merge_view(table: str, view_name: str, key_fields: list[str]) -> None:
    ensure_delta_table_from_view(table, view_name)
    columns = spark.table(view_name).columns
    on_clause = " AND ".join([f"target.{quote_ident(field)} <=> source.{quote_ident(field)}" for field in key_fields])
    update_clause = ", ".join([f"target.{quote_ident(column)} = source.{quote_ident(column)}" for column in columns])
    insert_columns = ", ".join(quote_ident(column) for column in columns)
    insert_values = ", ".join(f"source.{quote_ident(column)}" for column in columns)
    spark.sql(
        f"""
        MERGE INTO {silver_table(table)} AS target
        USING {quote_ident(view_name)} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )


def append_view(table: str, view_name: str) -> None:
    ensure_delta_table_from_view(table, view_name)
    spark.table(view_name).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(silver_table(table))

# COMMAND ----------

merge_view("fred_observation_versions_cleaned", "silver_observations_stage", ["observation_version_id"])

spark.sql(
    f"""
    ALTER TABLE {silver_table("fred_observation_versions_cleaned")}
    SET TBLPROPERTIES (
        'quality.layer' = 'silver',
        'source.table' = '{CATALOG}.{BRONZE_SCHEMA}.fred_observation_versions',
        'transform.name' = '{TRANSFORM_NAME}',
        'transform.version' = '{TRANSFORM_VERSION}'
    )
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_quality_report_stage AS
    SELECT
        {sql_literal(SILVER_RUN_ID)} AS silver_run_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS processed_at_utc,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        series_id,
        first(frequency, true) AS frequency,
        first(frequency_short, true) AS frequency_short,
        first(units, true) AS units,
        first(seasonal_adjustment, true) AS seasonal_adjustment,
        COUNT(*) AS row_count,
        COUNT(DISTINCT observation_date) AS observation_date_count,
        MIN(observation_date) AS date_min,
        MAX(observation_date) AS date_max,
        MIN(value_numeric) AS numeric_min,
        MAX(value_numeric) AS numeric_max,
        SUM(CASE WHEN is_current_version THEN 1 ELSE 0 END) AS current_version_count,
        SUM(CASE WHEN is_missing THEN 1 ELSE 0 END) AS missing_count,
        AVG(CASE WHEN is_missing THEN 1.0 ELSE 0.0 END) AS missing_rate,
        SUM(CASE WHEN is_numeric_parse_error THEN 1 ELSE 0 END) AS numeric_parse_error_count,
        SUM(CASE WHEN is_realtime_range_error THEN 1 ELSE 0 END) AS realtime_range_error_count,
        SUM(CASE WHEN is_duplicate_observation_version THEN 1 ELSE 0 END) AS duplicate_observation_version_count,
        COUNT(DISTINCT CASE WHEN is_observation_date_revised THEN observation_date END) AS revised_observation_date_count,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_count,
        AVG(CASE WHEN is_outlier THEN 1.0 ELSE 0.0 END) AS outlier_rate,
        SUM(CASE WHEN quality_status = 'error' THEN 1 ELSE 0 END) AS quality_error_count,
        SUM(CASE WHEN quality_status = 'warning' THEN 1 ELSE 0 END) AS quality_warning_count,
        MIN(first_seen_bronze_run_id) AS first_seen_bronze_run_id,
        MAX(last_seen_bronze_run_id) AS last_seen_bronze_run_id,
        COUNT(DISTINCT last_seen_bronze_run_id) AS contributing_bronze_run_count
    FROM silver_observations_stage
    GROUP BY series_id
    """
)

append_view("fred_version_quality_report", "silver_quality_report_stage")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_series_catalog_stage AS
    SELECT
        series_id,
        first(source, true) AS source,
        first(domain, true) AS domain,
        first(priority, true) AS priority,
        first(frequency, true) AS frequency,
        first(frequency_short, true) AS frequency_short,
        first(units, true) AS units,
        first(units_short, true) AS units_short,
        first(seasonal_adjustment, true) AS seasonal_adjustment,
        COUNT(*) AS version_row_count,
        COUNT(DISTINCT observation_date) AS observation_date_count,
        MIN(observation_date) AS observation_start,
        MAX(observation_date) AS observation_end,
        SUM(CASE WHEN is_current_version THEN 1 ELSE 0 END) AS current_version_count,
        COUNT(DISTINCT CASE WHEN is_observation_date_revised THEN observation_date END) AS revised_observation_date_count,
        SUM(CASE WHEN quality_status = 'error' THEN 1 ELSE 0 END) AS quality_error_count,
        SUM(CASE WHEN quality_status = 'warning' THEN 1 ELSE 0 END) AS quality_warning_count,
        MIN(first_seen_bronze_run_id) AS first_seen_bronze_run_id,
        MAX(last_seen_bronze_run_id) AS last_seen_bronze_run_id,
        {sql_literal(SILVER_RUN_ID)} AS last_silver_run_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS last_silver_processed_at_utc
    FROM silver_observations_stage
    GROUP BY series_id
    """
)

merge_view("fred_version_series_catalog", "silver_series_catalog_stage", ["series_id"])

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_lineage_stage AS
    SELECT
        sha2(concat_ws('|', {sql_literal(SILVER_RUN_ID)}, series_id, 'fred_observation_versions_cleaned'), 256) AS lineage_id,
        {sql_literal(SILVER_RUN_ID)} AS silver_run_id,
        series_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS processed_at_utc,
        'bronze' AS source_layer,
        '{CATALOG}.{BRONZE_SCHEMA}.fred_observation_versions' AS source_observation_versions_table,
        '{CATALOG}.{BRONZE_SCHEMA}.fred_series_metadata_versions' AS source_metadata_versions_table,
        'silver' AS target_layer,
        '{CATALOG}.{SILVER_SCHEMA}.fred_observation_versions_cleaned' AS target_observations_table,
        '{CATALOG}.{SILVER_SCHEMA}.fred_version_quality_report' AS target_quality_report_table,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        to_json(named_struct(
            'parse_numeric', 'try_cast value_raw to double',
            'missing_values', 'null, empty string, FRED dot, numeric parse error',
            'point_in_time_rule', 'realtime_start <= as_of_date <= realtime_end',
            'outlier_method', 'robust z-score using median absolute deviation',
            'merge_key', 'observation_version_id'
        )) AS rules_json,
        COUNT(*) AS row_count_output,
        COUNT(DISTINCT first_seen_bronze_run_id) AS first_seen_bronze_run_count,
        COUNT(DISTINCT last_seen_bronze_run_id) AS last_seen_bronze_run_count
    FROM silver_observations_stage
    GROUP BY series_id
    """
)

append_view("fred_version_lineage_events", "silver_lineage_stage")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_run_summary_stage AS
    SELECT
        {sql_literal(SILVER_RUN_ID)} AS silver_run_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS processed_at_utc,
        {sql_literal(LOAD_TYPE)} AS load_type,
        CAST(NULL AS STRING) AS requested_bronze_run_id,
        CAST(NULL AS STRING) AS previous_silver_watermark_at_utc,
        CAST(COUNT(*) AS BIGINT) AS changed_bronze_row_count,
        CAST(COUNT(DISTINCT series_id) AS BIGINT) AS affected_series_count,
        MIN(last_collected_at_utc) AS source_min_last_collected_at_utc,
        MAX(last_collected_at_utc) AS source_max_last_collected_at_utc,
        {sql_literal(SOURCE)} AS source,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        COUNT(DISTINCT series_id) AS series_count,
        COUNT(*) AS row_count_output,
        SUM(CASE WHEN is_current_version THEN 1 ELSE 0 END) AS current_version_count,
        SUM(CASE WHEN is_missing THEN 1 ELSE 0 END) AS missing_count,
        SUM(CASE WHEN is_numeric_parse_error THEN 1 ELSE 0 END) AS numeric_parse_error_count,
        SUM(CASE WHEN is_realtime_range_error THEN 1 ELSE 0 END) AS realtime_range_error_count,
        SUM(CASE WHEN is_duplicate_observation_version THEN 1 ELSE 0 END) AS duplicate_observation_version_count,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_count,
        SUM(CASE WHEN quality_status = 'error' THEN 1 ELSE 0 END) AS quality_error_count,
        SUM(CASE WHEN quality_status = 'warning' THEN 1 ELSE 0 END) AS quality_warning_count,
        COUNT(DISTINCT first_seen_bronze_run_id) AS first_seen_bronze_run_count,
        COUNT(DISTINCT last_seen_bronze_run_id) AS last_seen_bronze_run_count,
        MIN(first_seen_bronze_run_id) AS min_first_seen_bronze_run_id,
        MAX(last_seen_bronze_run_id) AS max_last_seen_bronze_run_id,
        {sql_literal(str(INCLUDE_MISSING_IN_SILVER).lower())} AS include_missing_in_silver,
        {OUTLIER_THRESHOLD} AS outlier_threshold
    FROM silver_observations_stage
    """
)

append_view("fred_version_run_summary", "silver_run_summary_stage")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {silver_table("fred_observations_asof_ready")} AS
    SELECT
        series_id,
        observation_date,
        period_start,
        period_end,
        value_numeric,
        value_raw,
        realtime_start,
        realtime_end,
        vintage_date,
        available_at,
        frequency,
        frequency_short,
        units,
        units_short,
        seasonal_adjustment,
        observation_version_id,
        is_current_version,
        is_observation_date_revised,
        revision_number,
        revision_count,
        is_outlier,
        outlier_score,
        quality_status,
        quality_issues
    FROM {silver_table("fred_observation_versions_cleaned")}
    WHERE is_point_in_time_usable
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {silver_table("fred_observations_current")} AS
    SELECT *
    FROM {silver_table("fred_observations_asof_ready")}
    WHERE is_current_version
    """
)

# COMMAND ----------

summary_df = spark.table(silver_table("fred_version_run_summary")).where(f"silver_run_id = '{SILVER_RUN_ID}'")
display(summary_df)

# COMMAND ----------

display(
    spark.table(silver_table("fred_version_quality_report"))
    .where(f"silver_run_id = '{SILVER_RUN_ID}'")
    .orderBy("series_id")
)