# Databricks notebook source
# MAGIC %md
# MAGIC # 02c Silver FRED Current Observation Cleaning
# MAGIC
# MAGIC Cleans FRED-only current observations loaded by `01c_bronze_fred_current_observations.py`.
# MAGIC
# MAGIC This notebook intentionally keeps FRED current-only observations separate from
# MAGIC ALFRED revision-aware observation versions. Rows are not point-in-time safe;
# MAGIC they represent the latest current value mirrored from FRED for each
# MAGIC `source + series_id + observation_date` key.

# COMMAND ----------

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import Row

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("seed_catalog_path", "../configs/fred_seed_series.json", "Seed catalog path")
dbutils.widgets.text("series_ids", "ALL", "FRED-only series IDs: SP500 or ALL")
dbutils.widgets.text("outlier_threshold", "6.0", "Robust z-score outlier threshold")
dbutils.widgets.dropdown("include_missing_in_silver", "true", ["true", "false"], "Keep missing rows")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SILVER_SCHEMA = dbutils.widgets.get("silver_schema").strip()
SEED_CATALOG_PATH = dbutils.widgets.get("seed_catalog_path").strip()
SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
OUTLIER_THRESHOLD = float(dbutils.widgets.get("outlier_threshold"))
INCLUDE_MISSING_IN_SILVER = dbutils.widgets.get("include_missing_in_silver").lower() == "true"

SOURCE = "fred"
TRANSFORM_NAME = "clean_fred_current_observations"
TRANSFORM_VERSION = "0.1.0"
LOAD_TYPE = "current"
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


def table_exists(qualified_name: str) -> bool:
    try:
        return spark.catalog.tableExists(qualified_name)
    except Exception:
        try:
            spark.table(qualified_name).limit(1).collect()
            return True
        except Exception:
            return False


spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(SILVER_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(SILVER_SCHEMA)}")

print(f"Silver FRED current target schema: {CATALOG}.{SILVER_SCHEMA}")
print(f"Silver FRED current run_id: {SILVER_RUN_ID}")
print(f"Transform version: {TRANSFORM_VERSION}")
print(f"Load type: {LOAD_TYPE}")

if not table_exists(f"{CATALOG}.{BRONZE_SCHEMA}.fred_current_observations_raw"):
    raise ValueError("Bronze table was not found: fred_current_observations_raw. Run bronze/01c first.")

# COMMAND ----------

def dbfs_to_local_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path.removeprefix("dbfs:/").lstrip("/")
    return path


def read_text_file(path: str) -> str:
    return Path(dbfs_to_local_path(path)).read_text(encoding="utf-8")


def catalog_path_candidates(path: str) -> list[str]:
    candidates = [path]
    if not Path(path).is_absolute() and not path.startswith("dbfs:/"):
        cwd = Path.cwd()
        candidates.extend([str(cwd / path), str(cwd.parent / path), str(cwd.parent.parent / path)])
    return list(dict.fromkeys(candidates))


def load_seed_catalog(path: str) -> list[dict[str, Any]]:
    errors = []
    for candidate in catalog_path_candidates(path):
        try:
            payload = json.loads(read_text_file(candidate))
            if not isinstance(payload, list):
                raise ValueError("seed catalog JSON must be a list")
            print(f"Loaded seed catalog: {candidate}")
            return payload
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")
    raise FileNotFoundError(
        "Seed catalog file was not found. Upload notebooks/databricks/configs/fred_seed_series.json to Databricks "
        "and set the seed_catalog_path widget. Tried:\n" + "\n".join(errors)
    )


seed_rows = []
for item in load_seed_catalog(SEED_CATALOG_PATH):
    seed_rows.append(
        Row(
            series_id=str(item["series_id"]).strip().upper(),
            domain=item.get("domain"),
            priority=item.get("priority"),
            expected_frequency=item.get("expected_frequency"),
            description=item.get("description"),
            role=item.get("role"),
            alfred_available=bool(item.get("alfred_available", True)),
        )
    )

spark.createDataFrame(seed_rows).createOrReplaceTempView("fred_seed_catalog_stage")

# COMMAND ----------

if SERIES_IDS_PARAM.upper() == "ALL":
    selected_series = [
        row["series_id"]
        for row in spark.sql(
            f"""
            SELECT DISTINCT upper(series_id) AS series_id
            FROM {bronze_table("fred_current_observations_raw")}
            ORDER BY series_id
            """
        ).collect()
    ]
else:
    selected_series = [item.strip().upper() for item in SERIES_IDS_PARAM.split(",") if item.strip()]

if not selected_series:
    raise ValueError("No FRED current series selected for Silver processing.")

series_values = ", ".join(sql_literal(series_id) for series_id in selected_series)
series_predicate = f"upper(bronze.series_id) IN ({series_values})"

print(f"Selected FRED current series count: {len(selected_series)}")
print("Selected FRED current series:", ", ".join(selected_series[:20]) + (" ..." if len(selected_series) > 20 else ""))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW selected_bronze_current AS
    SELECT
        bronze.*,
        seed.domain,
        seed.priority,
        seed.expected_frequency,
        seed.description,
        seed.role,
        seed.alfred_available
    FROM {bronze_table("fred_current_observations_raw")} bronze
    LEFT JOIN fred_seed_catalog_stage seed
      ON upper(bronze.series_id) = seed.series_id
    WHERE {series_predicate}
    """
)

selected_count = spark.sql("SELECT COUNT(*) AS row_count FROM selected_bronze_current").collect()[0]["row_count"]
if selected_count == 0:
    raise ValueError("Selected series have no rows in bronze.fred_current_observations_raw.")

print(f"Selected Bronze FRED current rows: {selected_count}")

# COMMAND ----------

missing_filter = "" if INCLUDE_MISSING_IN_SILVER else "WHERE NOT is_missing"

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW silver_current_base_parse AS
    SELECT
        sha2(concat_ws('|', source, upper(series_id), observation_date), 256) AS current_observation_id,
        sha2(concat_ws('|', source, upper(series_id), observation_date, coalesce(value_raw, '')), 256) AS value_hash,
        source,
        upper(series_id) AS series_id,
        domain,
        priority,
        lower(expected_frequency) AS expected_frequency,
        description,
        role,
        'current_only' AS history_type,
        false AS is_point_in_time_safe,
        true AS is_current_value,
        to_date(observation_date) AS observation_date,
        CASE
            WHEN lower(expected_frequency) = 'monthly' THEN to_date(date_trunc('MONTH', to_date(observation_date)))
            WHEN lower(expected_frequency) = 'quarterly' THEN to_date(date_trunc('QUARTER', to_date(observation_date)))
            WHEN lower(expected_frequency) = 'annual' THEN to_date(date_trunc('YEAR', to_date(observation_date)))
            WHEN lower(expected_frequency) = 'weekly' THEN date_sub(to_date(observation_date), 6)
            ELSE to_date(observation_date)
        END AS period_start,
        CASE
            WHEN lower(expected_frequency) = 'monthly' THEN last_day(to_date(observation_date))
            WHEN lower(expected_frequency) = 'quarterly' THEN date_sub(add_months(to_date(date_trunc('QUARTER', to_date(observation_date))), 3), 1)
            WHEN lower(expected_frequency) = 'annual' THEN make_date(year(to_date(observation_date)), 12, 31)
            ELSE to_date(observation_date)
        END AS period_end,
        CASE
            WHEN expected_frequency IS NULL THEN 'observation_date_only'
            ELSE concat('seed_expected_frequency:', lower(expected_frequency))
        END AS period_inference_basis,
        value_raw,
        trim(value_raw) AS value_text,
        try_cast(trim(value_raw) AS DOUBLE) AS value_numeric,
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
        to_date(realtime_start) AS realtime_start,
        to_date(realtime_end) AS realtime_end,
        to_date(collection_date) AS collection_date,
        to_date(collection_date) AS available_at,
        endpoint,
        request_params_json,
        request_params_hash,
        redacted_url,
        bronze_run_id,
        collected_at_utc,
        load_type,
        attempts,
        CASE
            WHEN to_date(observation_date) IS NULL THEN true
            WHEN to_date(realtime_start) IS NULL THEN true
            WHEN to_date(realtime_end) IS NULL THEN true
            WHEN to_date(realtime_start) > to_date(realtime_end) THEN true
            ELSE false
        END AS is_realtime_range_error
    FROM selected_bronze_current
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_current_base AS
    SELECT *
    FROM silver_current_base_parse
    {missing_filter}
    """
)

# COMMAND ----------

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW current_observation_diffs AS
    SELECT
        current_observation_id,
        value_numeric - LAG(value_numeric) OVER (
            PARTITION BY series_id
            ORDER BY observation_date, current_observation_id
        ) AS observation_diff_value
    FROM silver_current_base
    WHERE value_numeric IS NOT NULL
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_level_median AS
    SELECT
        series_id,
        percentile_approx(value_numeric, 0.5) AS level_median
    FROM silver_current_base
    WHERE value_numeric IS NOT NULL
    GROUP BY series_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW level_abs_deviation AS
    SELECT
        obs.current_observation_id,
        abs(obs.value_numeric - med.level_median) AS level_abs_deviation
    FROM silver_current_base obs
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
    FROM silver_current_base obs
    JOIN level_abs_deviation dev USING (current_observation_id)
    GROUP BY obs.series_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW series_diff_median AS
    SELECT
        obs.series_id,
        percentile_approx(diff.observation_diff_value, 0.5) AS diff_median
    FROM silver_current_base obs
    JOIN current_observation_diffs diff USING (current_observation_id)
    WHERE diff.observation_diff_value IS NOT NULL
    GROUP BY obs.series_id
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW diff_abs_deviation AS
    SELECT
        obs.series_id,
        diff.current_observation_id,
        abs(diff.observation_diff_value - med.diff_median) AS diff_abs_deviation
    FROM current_observation_diffs diff
    JOIN silver_current_base obs USING (current_observation_id)
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

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_current_observations_final AS
    SELECT
        {sql_literal(SILVER_RUN_ID)} AS silver_run_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS silver_processed_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        obs.current_observation_id,
        obs.value_hash,
        obs.source,
        obs.series_id,
        obs.domain,
        obs.priority,
        obs.expected_frequency,
        obs.description,
        obs.role,
        obs.history_type,
        obs.is_point_in_time_safe,
        obs.is_current_value,
        obs.observation_date,
        obs.period_start,
        obs.period_end,
        obs.period_inference_basis,
        obs.value_raw,
        obs.value_text,
        obs.value_numeric,
        obs.is_missing,
        obs.missing_reason,
        obs.is_numeric_parse_error,
        obs.realtime_start,
        obs.realtime_end,
        obs.collection_date,
        obs.available_at,
        obs.endpoint,
        obs.request_params_json,
        obs.request_params_hash,
        obs.redacted_url,
        obs.bronze_run_id,
        obs.collected_at_utc,
        obs.load_type,
        obs.attempts,
        obs.is_realtime_range_error,
        diff.observation_diff_value,
        med.level_median,
        mad.level_mad,
        diff_med.diff_median,
        diff_mad.diff_mad,
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
        CAST(NULL AS DOUBLE) AS outlier_revision_score,
        abs(coalesce(
            CASE
                WHEN diff.observation_diff_value IS NULL THEN NULL
                WHEN diff_mad.diff_mad IS NULL OR diff_mad.diff_mad = 0 THEN 0.0
                ELSE 0.6745 * (diff.observation_diff_value - diff_med.diff_median) / diff_mad.diff_mad
            END,
            0.0
        )) AS outlier_score,
        {OUTLIER_THRESHOLD} AS outlier_threshold,
        'robust_zscore_mad_current_diff' AS outlier_method,
        CASE
            WHEN abs(coalesce(
                CASE
                    WHEN diff.observation_diff_value IS NULL THEN NULL
                    WHEN diff_mad.diff_mad IS NULL OR diff_mad.diff_mad = 0 THEN 0.0
                    ELSE 0.6745 * (diff.observation_diff_value - diff_med.diff_median) / diff_mad.diff_mad
                END,
                0.0
            )) > {OUTLIER_THRESHOLD}
            THEN true ELSE false
        END AS is_outlier,
        CASE
            WHEN obs.is_realtime_range_error OR obs.is_numeric_parse_error THEN 'error'
            WHEN obs.is_missing OR abs(coalesce(
                CASE
                    WHEN diff.observation_diff_value IS NULL THEN NULL
                    WHEN diff_mad.diff_mad IS NULL OR diff_mad.diff_mad = 0 THEN 0.0
                    ELSE 0.6745 * (diff.observation_diff_value - diff_med.diff_median) / diff_mad.diff_mad
                END,
                0.0
            )) > {OUTLIER_THRESHOLD} THEN 'warning'
            ELSE 'valid'
        END AS quality_status,
        concat_ws(',', array(
            CASE WHEN obs.is_missing THEN concat('missing:', coalesce(obs.missing_reason, 'unknown')) END,
            CASE WHEN obs.is_numeric_parse_error THEN 'numeric_parse_error' END,
            CASE WHEN obs.is_realtime_range_error THEN 'realtime_range_error' END,
            CASE WHEN abs(coalesce(
                CASE
                    WHEN diff.observation_diff_value IS NULL THEN NULL
                    WHEN diff_mad.diff_mad IS NULL OR diff_mad.diff_mad = 0 THEN 0.0
                    ELSE 0.6745 * (diff.observation_diff_value - diff_med.diff_median) / diff_mad.diff_mad
                END,
                0.0
            )) > {OUTLIER_THRESHOLD} THEN 'outlier' END
        )) AS quality_issues
    FROM silver_current_base obs
    LEFT JOIN current_observation_diffs diff USING (current_observation_id)
    LEFT JOIN series_level_median med USING (series_id)
    LEFT JOIN series_level_mad mad USING (series_id)
    LEFT JOIN series_diff_median diff_med USING (series_id)
    LEFT JOIN series_diff_mad diff_mad USING (series_id)
    """
)

spark.sql(
    """
    CREATE OR REPLACE TEMP VIEW silver_current_observations_stage AS
    SELECT * EXCEPT (silver_key_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY source, series_id, observation_date
                ORDER BY collected_at_utc DESC, bronze_run_id DESC, current_observation_id DESC
            ) AS silver_key_rank
        FROM silver_current_observations_final
        WHERE current_observation_id IS NOT NULL
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


def set_delta_properties(table: str, source_table: str) -> None:
    spark.sql(
        f"""
        ALTER TABLE {silver_table(table)}
        SET TBLPROPERTIES (
            'quality.layer' = 'silver',
            'source.table' = '{source_table}',
            'transform.name' = '{TRANSFORM_NAME}',
            'transform.version' = '{TRANSFORM_VERSION}',
            'point_in_time_safe' = 'false'
        )
        """
    )

# COMMAND ----------

merge_view(
    "fred_current_observations_cleaned",
    "silver_current_observations_stage",
    ["source", "series_id", "observation_date"],
)
set_delta_properties(
    "fred_current_observations_cleaned",
    f"{CATALOG}.{BRONZE_SCHEMA}.fred_current_observations_raw",
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_current_quality_report_stage AS
    SELECT
        {sql_literal(SILVER_RUN_ID)} AS silver_run_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS processed_at_utc,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        series_id,
        first(expected_frequency, true) AS expected_frequency,
        first(domain, true) AS domain,
        first(priority, true) AS priority,
        COUNT(*) AS row_count,
        COUNT(DISTINCT observation_date) AS observation_date_count,
        MIN(observation_date) AS date_min,
        MAX(observation_date) AS date_max,
        MIN(value_numeric) AS numeric_min,
        MAX(value_numeric) AS numeric_max,
        SUM(CASE WHEN is_missing THEN 1 ELSE 0 END) AS missing_count,
        AVG(CASE WHEN is_missing THEN 1.0 ELSE 0.0 END) AS missing_rate,
        SUM(CASE WHEN is_numeric_parse_error THEN 1 ELSE 0 END) AS numeric_parse_error_count,
        SUM(CASE WHEN is_realtime_range_error THEN 1 ELSE 0 END) AS realtime_range_error_count,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_count,
        AVG(CASE WHEN is_outlier THEN 1.0 ELSE 0.0 END) AS outlier_rate,
        SUM(CASE WHEN quality_status = 'error' THEN 1 ELSE 0 END) AS quality_error_count,
        SUM(CASE WHEN quality_status = 'warning' THEN 1 ELSE 0 END) AS quality_warning_count,
        MIN(collection_date) AS collection_date_min,
        MAX(collection_date) AS collection_date_max,
        MIN(bronze_run_id) AS min_bronze_run_id,
        MAX(bronze_run_id) AS max_bronze_run_id
    FROM silver_current_observations_stage
    GROUP BY series_id
    """
)

append_view("fred_current_quality_report", "silver_current_quality_report_stage")
set_delta_properties(
    "fred_current_quality_report",
    f"{CATALOG}.{BRONZE_SCHEMA}.fred_current_observations_raw",
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_current_series_catalog_stage AS
    SELECT
        series_id,
        first(source, true) AS source,
        first(domain, true) AS domain,
        first(priority, true) AS priority,
        first(expected_frequency, true) AS expected_frequency,
        first(description, true) AS description,
        first(role, true) AS role,
        'current_only' AS history_type,
        false AS is_point_in_time_safe,
        COUNT(*) AS observation_row_count,
        COUNT(DISTINCT observation_date) AS observation_date_count,
        MIN(observation_date) AS observation_start,
        MAX(observation_date) AS observation_end,
        SUM(CASE WHEN quality_status = 'error' THEN 1 ELSE 0 END) AS quality_error_count,
        SUM(CASE WHEN quality_status = 'warning' THEN 1 ELSE 0 END) AS quality_warning_count,
        MIN(collection_date) AS collection_date_min,
        MAX(collection_date) AS collection_date_max,
        {sql_literal(SILVER_RUN_ID)} AS last_silver_run_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS last_silver_processed_at_utc
    FROM silver_current_observations_stage
    GROUP BY series_id
    """
)

merge_view("fred_current_series_catalog", "silver_current_series_catalog_stage", ["series_id"])
set_delta_properties(
    "fred_current_series_catalog",
    f"{CATALOG}.{BRONZE_SCHEMA}.fred_current_observations_raw",
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW silver_current_run_summary_stage AS
    SELECT
        {sql_literal(SILVER_RUN_ID)} AS silver_run_id,
        {sql_literal(SILVER_PROCESSED_AT_UTC)} AS processed_at_utc,
        {sql_literal(LOAD_TYPE)} AS load_type,
        CAST(COUNT(*) AS BIGINT) AS changed_bronze_row_count,
        CAST(COUNT(DISTINCT series_id) AS BIGINT) AS affected_series_count,
        MIN(collected_at_utc) AS source_min_collected_at_utc,
        MAX(collected_at_utc) AS source_max_collected_at_utc,
        {sql_literal(SOURCE)} AS source,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        COUNT(DISTINCT series_id) AS series_count,
        COUNT(*) AS row_count_output,
        SUM(CASE WHEN is_missing THEN 1 ELSE 0 END) AS missing_count,
        SUM(CASE WHEN is_numeric_parse_error THEN 1 ELSE 0 END) AS numeric_parse_error_count,
        SUM(CASE WHEN is_realtime_range_error THEN 1 ELSE 0 END) AS realtime_range_error_count,
        SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) AS outlier_count,
        SUM(CASE WHEN quality_status = 'error' THEN 1 ELSE 0 END) AS quality_error_count,
        SUM(CASE WHEN quality_status = 'warning' THEN 1 ELSE 0 END) AS quality_warning_count,
        {sql_literal(str(INCLUDE_MISSING_IN_SILVER).lower())} AS include_missing_in_silver,
        {OUTLIER_THRESHOLD} AS outlier_threshold
    FROM silver_current_observations_stage
    """
)

append_view("fred_current_run_summary", "silver_current_run_summary_stage")
set_delta_properties(
    "fred_current_run_summary",
    f"{CATALOG}.{BRONZE_SCHEMA}.fred_current_observations_raw",
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {silver_table("fred_current_observations_analytics_ready")} AS
    SELECT
        source,
        series_id,
        domain,
        priority,
        expected_frequency,
        description,
        role,
        history_type,
        is_point_in_time_safe,
        observation_date,
        period_start,
        period_end,
        period_inference_basis,
        value_numeric,
        value_raw,
        value_text,
        collection_date,
        available_at,
        realtime_start,
        realtime_end,
        current_observation_id,
        is_missing,
        is_outlier,
        outlier_score,
        quality_status,
        quality_issues
    FROM {silver_table("fred_current_observations_cleaned")}
    WHERE quality_status <> 'error'
    """
)

# COMMAND ----------

summary_df = spark.table(silver_table("fred_current_run_summary")).where(f"silver_run_id = '{SILVER_RUN_ID}'")
display(summary_df)

# COMMAND ----------

display(
    spark.table(silver_table("fred_current_quality_report"))
    .where(f"silver_run_id = '{SILVER_RUN_ID}'")
    .orderBy("series_id")
)
