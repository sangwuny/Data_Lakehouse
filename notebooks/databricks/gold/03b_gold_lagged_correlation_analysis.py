# Databricks notebook source
# MAGIC %md
# MAGIC # 03b Gold Lagged Correlation Analysis
# MAGIC
# MAGIC Target-specific lagged Pearson correlation analysis built on top of
# MAGIC `03_gold_fred_asof_features.py` outputs.
# MAGIC
# MAGIC This notebook reads `fred_transformed_features_long` and writes:
# MAGIC
# MAGIC - `fred_lagged_correlation_scores`
# MAGIC - `fred_top_lagged_correlations`
# MAGIC - `fred_lagged_correlation_run_summary`
# MAGIC
# MAGIC It is an EDA/ranking stage for lagged co-movement, not a causal discovery
# MAGIC or causal test.

# COMMAND ----------

from datetime import datetime, timezone

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")
dbutils.widgets.text("as_of_date", "", "As-of date, blank = UTC today")
dbutils.widgets.text("analysis_start_date", "", "Analysis start date, blank = earliest available")
dbutils.widgets.text("analysis_end_date", "", "Analysis end date, blank = as-of date")
dbutils.widgets.text("target_series_id", "GDPC1", "Target series ID for lagged correlation analysis")
dbutils.widgets.text("comparison_series_ids", "ALL", "Comparison series IDs: ALL or comma list")
dbutils.widgets.dropdown("target_frequency", "monthly", ["daily", "weekly", "monthly", "quarterly", "annual", "native"], "Target frequency")
dbutils.widgets.dropdown("aggregation_method", "last", ["last", "mean"], "Period aggregation")
dbutils.widgets.dropdown("correlation_transform_type", "raw", ["raw", "change_1", "change_12", "pct_change_1", "pct_change_12", "log_diff_1", "log_diff_12", "z_score_full_sample", "index_base100"], "Transform used for correlation scoring")
dbutils.widgets.text("max_lag_periods", "12", "Max comparison lag periods")
dbutils.widgets.text("min_pair_count", "AUTO", "Minimum aligned target/comparison pairs, AUTO = frequency default")
dbutils.widgets.text("reliable_pair_count", "AUTO", "Pair count for full score reliability, AUTO = frequency default")
dbutils.widgets.text("min_correlation_score", "0.3", "Minimum correlation score for top correlation view")
dbutils.widgets.dropdown("optimize_tables", "true", ["true", "false"], "Run OPTIMIZE when available")

CATALOG = dbutils.widgets.get("catalog").strip()
GOLD_SCHEMA = dbutils.widgets.get("gold_schema").strip()
REQUESTED_AS_OF_DATE = dbutils.widgets.get("as_of_date").strip() or None
REQUESTED_ANALYSIS_START_DATE = dbutils.widgets.get("analysis_start_date").strip() or None
REQUESTED_ANALYSIS_END_DATE = dbutils.widgets.get("analysis_end_date").strip() or None
TARGET_SERIES_ID = dbutils.widgets.get("target_series_id").strip().upper()
COMPARISON_SERIES_IDS_PARAM = dbutils.widgets.get("comparison_series_ids").strip()
TARGET_FREQUENCY = dbutils.widgets.get("target_frequency").strip().lower()
AGGREGATION_METHOD = dbutils.widgets.get("aggregation_method").strip().lower()
CORRELATION_TRANSFORM_TYPE = dbutils.widgets.get("correlation_transform_type").strip().lower()
MAX_LAG_PERIODS = int(dbutils.widgets.get("max_lag_periods"))
MIN_PAIR_COUNT_PARAM = dbutils.widgets.get("min_pair_count").strip()
RELIABLE_PAIR_COUNT_PARAM = dbutils.widgets.get("reliable_pair_count").strip()
MIN_CORRELATION_SCORE = float(dbutils.widgets.get("min_correlation_score"))
OPTIMIZE_TABLES = dbutils.widgets.get("optimize_tables").strip().lower() == "true"

LOAD_TYPE = "lagged_correlation_analysis"
GOLD_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
GOLD_PROCESSED_AT_UTC = datetime.now(timezone.utc).isoformat()
AS_OF_DATE = REQUESTED_AS_OF_DATE or datetime.now(timezone.utc).date().isoformat()
ANALYSIS_START_DATE = REQUESTED_ANALYSIS_START_DATE
ANALYSIS_END_DATE = REQUESTED_ANALYSIS_END_DATE or AS_OF_DATE

TRANSFORM_NAME = "build_fred_gold_lagged_correlation_analysis"
TRANSFORM_VERSION = "0.2.0-lagged-correlation-analysis"
CORRELATION_SCORE_METHOD = "r_squared_sample_reliability"

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

DEFAULT_MIN_PAIR_COUNT_BY_FREQUENCY = {
    "daily": 252,
    "weekly": 104,
    "monthly": 36,
    "quarterly": 24,
    "annual": 15,
    "native": 36,
}

DEFAULT_RELIABLE_PAIR_COUNT_BY_FREQUENCY = {
    "daily": 756,
    "weekly": 260,
    "monthly": 60,
    "quarterly": 40,
    "annual": 25,
    "native": 60,
}

# COMMAND ----------

def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_date_expr(value: str | None) -> str:
    if value is None:
        return "CAST(NULL AS DATE)"
    return f"DATE {sql_literal(value)}"


def comma_sql_literals(values: list[str]) -> str:
    return ", ".join(sql_literal(value) for value in values)


def normalize_id_list(value: str) -> list[str]:
    if value.strip().upper() == "ALL":
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


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
    if table_exists(table.replace("`", "")):
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
    create_empty_delta_table_from_view(table, view_name)
    ensure_table_has_source_columns(table, view_name)
    spark.table(view_name).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table)


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
                'lakehouse.source' = 'fred',
                'lakehouse.module' = 'lagged_correlation_analysis'
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


def validate_iso_date(name: str, value: str | None) -> None:
    if value is None:
        return
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"{name} must be formatted as YYYY-MM-DD: {value}") from exc


def parse_auto_int(value: str, default_value: int, name: str) -> int:
    normalized = value.strip().upper()
    if normalized in {"", "AUTO"}:
        return default_value
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be positive or AUTO: {value}")
    return parsed

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(GOLD_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(GOLD_SCHEMA)}")

validate_iso_date("as_of_date", AS_OF_DATE)
validate_iso_date("analysis_start_date", ANALYSIS_START_DATE)
validate_iso_date("analysis_end_date", ANALYSIS_END_DATE)

if ANALYSIS_START_DATE is not None and ANALYSIS_START_DATE > ANALYSIS_END_DATE:
    raise ValueError("analysis_start_date must be less than or equal to analysis_end_date")
if ANALYSIS_END_DATE > AS_OF_DATE:
    raise ValueError("analysis_end_date must be less than or equal to as_of_date")
if TARGET_FREQUENCY not in DEFAULT_MIN_PAIR_COUNT_BY_FREQUENCY:
    raise ValueError(f"Unsupported target_frequency: {TARGET_FREQUENCY}")
if AGGREGATION_METHOD not in {"last", "mean"}:
    raise ValueError(f"Unsupported aggregation_method: {AGGREGATION_METHOD}")
if CORRELATION_TRANSFORM_TYPE not in SUPPORTED_TRANSFORM_TYPES:
    raise ValueError(f"Unsupported correlation_transform_type: {CORRELATION_TRANSFORM_TYPE}")
if not table_exists(f"{CATALOG}.{GOLD_SCHEMA}.fred_transformed_features_long"):
    raise ValueError("Gold transformed feature table was not found. Run 03_gold_fred_asof_features.py first.")

MIN_PAIR_COUNT = parse_auto_int(
    MIN_PAIR_COUNT_PARAM,
    DEFAULT_MIN_PAIR_COUNT_BY_FREQUENCY[TARGET_FREQUENCY],
    "min_pair_count",
)
RELIABLE_PAIR_COUNT = parse_auto_int(
    RELIABLE_PAIR_COUNT_PARAM,
    DEFAULT_RELIABLE_PAIR_COUNT_BY_FREQUENCY[TARGET_FREQUENCY],
    "reliable_pair_count",
)
if RELIABLE_PAIR_COUNT < MIN_PAIR_COUNT:
    raise ValueError("reliable_pair_count must be greater than or equal to min_pair_count")

comparison_series = normalize_id_list(COMPARISON_SERIES_IDS_PARAM)
series_source_filter = (
    "1 = 1"
    if not comparison_series
    else f"(upper(series_id) = {sql_literal(TARGET_SERIES_ID)} OR upper(series_id) IN ({comma_sql_literals(comparison_series)}))"
)
comparison_scoring_predicate = "1 = 1" if not comparison_series else f"upper(comparison.series_id) IN ({comma_sql_literals(comparison_series)})"
comparison_delete_predicate = "1 = 1" if not comparison_series else f"upper(comparison_series_id) IN ({comma_sql_literals(comparison_series)})"

print(f"Gold target schema: {CATALOG}.{GOLD_SCHEMA}")
print(f"Gold run_id: {GOLD_RUN_ID}")
print(f"Load type: {LOAD_TYPE}")
print(f"As-of date: {AS_OF_DATE}")
print(f"Analysis range: {ANALYSIS_START_DATE or 'earliest'} to {ANALYSIS_END_DATE}")
print(f"Target frequency: {TARGET_FREQUENCY}")
print(f"Aggregation method: {AGGREGATION_METHOD}")
print(f"Correlation transform type: {CORRELATION_TRANSFORM_TYPE}")
print(f"Target series: {TARGET_SERIES_ID}")
print(f"Comparison series: {'ALL' if not comparison_series else ', '.join(comparison_series)}")
print(f"Max lag periods: {MAX_LAG_PERIODS}")
print(f"Minimum pair count: {MIN_PAIR_COUNT}")
print(f"Reliable pair count: {RELIABLE_PAIR_COUNT}")
print(f"Minimum correlation score: {MIN_CORRELATION_SCORE}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_lagged_correlation_source_features AS
    SELECT *
    FROM {gold_table("fred_transformed_features_long")}
    WHERE as_of_date = DATE {sql_literal(AS_OF_DATE)}
      AND analysis_start_date <=> {sql_date_expr(ANALYSIS_START_DATE)}
      AND analysis_end_date <=> {sql_date_expr(ANALYSIS_END_DATE)}
      AND target_frequency = {sql_literal(TARGET_FREQUENCY)}
      AND aggregation_method = {sql_literal(AGGREGATION_METHOD)}
      AND transform_type = {sql_literal(CORRELATION_TRANSFORM_TYPE)}
      AND transformed_value IS NOT NULL
      AND {series_source_filter}
    """
)

source_feature_count = spark.sql("SELECT count(*) AS row_count FROM gold_lagged_correlation_source_features").collect()[0]["row_count"]
target_period_count = spark.sql(
    f"""
    SELECT count(*) AS row_count
    FROM gold_lagged_correlation_source_features
    WHERE upper(series_id) = {sql_literal(TARGET_SERIES_ID)}
    """
).collect()[0]["row_count"]

if target_period_count == 0:
    message = (
        f"No transformed target rows found for {TARGET_SERIES_ID}. "
        "Run 03 Gold for this as_of_date, analysis window, frequency, aggregation, and transform first."
    )
    print(message)
    dbutils.notebook.exit(message)

print(f"Lagged correlation source feature rows: {source_feature_count}")
print(f"Target transformed periods: {target_period_count}")

# COMMAND ----------

lag_period_shift_expr = {
    "daily": "date_sub(target.period_start, lag.lag_periods)",
    "weekly": "date_sub(target.period_start, 7 * lag.lag_periods)",
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
        analysis_start_date,
        analysis_end_date,
        target_frequency,
        aggregation_method,
        transform_type,
        period_start,
        transformed_value AS target_value_numeric
    FROM gold_lagged_correlation_source_features
    WHERE upper(series_id) = {sql_literal(TARGET_SERIES_ID)}
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_lagged_correlation_pairs AS
    SELECT
        target.as_of_date,
        target.analysis_start_date,
        target.analysis_end_date,
        target.target_frequency,
        target.aggregation_method,
        target.transform_type,
        {sql_literal(TARGET_SERIES_ID)} AS target_series_id,
        comparison.series_id AS comparison_series_id,
        comparison.history_type AS comparison_history_type,
        comparison.availability_basis AS comparison_availability_basis,
        comparison.is_revision_aware AS comparison_is_revision_aware,
        comparison.domain AS comparison_domain,
        comparison.priority AS comparison_priority,
        lag.lag_periods,
        target.period_start AS target_period_start,
        comparison.period_start AS comparison_period_start,
        target.target_value_numeric,
        comparison.transformed_value AS comparison_value_numeric,
        comparison.quality_warning_count AS comparison_quality_warning_count,
        comparison.outlier_count AS comparison_outlier_count,
        comparison.revised_observation_count AS comparison_revised_observation_count
    FROM gold_target_periods target
    CROSS JOIN gold_lag_values lag
    JOIN gold_lagged_correlation_source_features comparison
      ON comparison.as_of_date = target.as_of_date
     AND comparison.analysis_start_date <=> target.analysis_start_date
     AND comparison.analysis_end_date <=> target.analysis_end_date
     AND comparison.target_frequency = target.target_frequency
     AND comparison.aggregation_method = target.aggregation_method
     AND comparison.transform_type = target.transform_type
     AND comparison.period_start = {lag_period_shift_expr}
    WHERE upper(comparison.series_id) <> {sql_literal(TARGET_SERIES_ID)}
      AND {comparison_scoring_predicate}
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_lagged_correlation_scores_stage AS
    WITH target_period_counts AS (
        SELECT
            as_of_date,
            analysis_start_date,
            analysis_end_date,
            target_frequency,
            aggregation_method,
            transform_type,
            count(*) AS target_period_count
        FROM gold_target_periods
        GROUP BY as_of_date, analysis_start_date, analysis_end_date, target_frequency, aggregation_method, transform_type
    ),
    scored AS (
        SELECT
            pairs.as_of_date,
            pairs.analysis_start_date,
            pairs.analysis_end_date,
            pairs.target_frequency,
            pairs.aggregation_method,
            pairs.transform_type,
            pairs.target_series_id,
            pairs.comparison_series_id,
            max(pairs.comparison_history_type) AS comparison_history_type,
            max(pairs.comparison_availability_basis) AS comparison_availability_basis,
            max(CAST(pairs.comparison_is_revision_aware AS INT)) = 1 AS comparison_is_revision_aware,
            max(pairs.comparison_domain) AS comparison_domain,
            max(pairs.comparison_priority) AS comparison_priority,
            pairs.lag_periods,
            count(*) AS pair_count,
            corr(pairs.target_value_numeric, pairs.comparison_value_numeric) AS pearson_corr,
            avg(abs(pairs.comparison_quality_warning_count)) AS avg_comparison_quality_warning_count,
            avg(abs(pairs.comparison_outlier_count)) AS avg_comparison_outlier_count,
            sum(CASE WHEN pairs.comparison_revised_observation_count > 0 THEN 1 ELSE 0 END) AS revised_period_count
        FROM gold_lagged_correlation_pairs pairs
        WHERE pairs.target_value_numeric IS NOT NULL
          AND pairs.comparison_value_numeric IS NOT NULL
        GROUP BY
            pairs.as_of_date,
            pairs.analysis_start_date,
            pairs.analysis_end_date,
            pairs.target_frequency,
            pairs.aggregation_method,
            pairs.transform_type,
            pairs.target_series_id,
            pairs.comparison_series_id,
            pairs.lag_periods
    )
    SELECT
        {sql_literal(GOLD_RUN_ID)} AS gold_run_id,
        {sql_literal(GOLD_PROCESSED_AT_UTC)} AS gold_processed_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        scored.as_of_date,
        scored.analysis_start_date,
        scored.analysis_end_date,
        scored.target_frequency,
        scored.aggregation_method,
        scored.transform_type,
        scored.target_series_id,
        scored.comparison_series_id,
        scored.comparison_history_type,
        scored.comparison_availability_basis,
        scored.comparison_is_revision_aware,
        scored.comparison_domain,
        scored.comparison_priority,
        scored.lag_periods,
        scored.pair_count,
        counts.target_period_count,
        CAST(scored.pair_count AS DOUBLE) / CAST(counts.target_period_count AS DOUBLE) AS coverage_rate,
        CAST({RELIABLE_PAIR_COUNT} AS BIGINT) AS reliable_pair_count,
        least(CAST(1.0 AS DOUBLE), CAST(scored.pair_count AS DOUBLE) / CAST({RELIABLE_PAIR_COUNT} AS DOUBLE)) AS sample_reliability_score,
        scored.pearson_corr,
        abs(scored.pearson_corr) AS abs_pearson_corr,
        power(scored.pearson_corr, 2) AS r_squared,
        power(scored.pearson_corr, 2) * (CAST(scored.pair_count AS DOUBLE) / CAST(counts.target_period_count AS DOUBLE)) AS coverage_weighted_score,
        power(scored.pearson_corr, 2) * least(CAST(1.0 AS DOUBLE), CAST(scored.pair_count AS DOUBLE) / CAST({RELIABLE_PAIR_COUNT} AS DOUBLE)) AS correlation_score,
        {sql_literal(CORRELATION_SCORE_METHOD)} AS correlation_score_method,
        scored.avg_comparison_quality_warning_count,
        scored.avg_comparison_outlier_count,
        scored.revised_period_count,
        DENSE_RANK() OVER (
            PARTITION BY scored.as_of_date, scored.analysis_start_date, scored.analysis_end_date, scored.target_frequency, scored.aggregation_method, scored.transform_type, scored.target_series_id
            ORDER BY power(scored.pearson_corr, 2) * least(CAST(1.0 AS DOUBLE), CAST(scored.pair_count AS DOUBLE) / CAST({RELIABLE_PAIR_COUNT} AS DOUBLE)) DESC NULLS LAST
        ) AS correlation_rank,
        sha2(concat_ws('|', scored.as_of_date, scored.analysis_start_date, scored.analysis_end_date, scored.target_frequency, scored.aggregation_method, scored.transform_type, scored.target_series_id, scored.comparison_series_id, scored.lag_periods), 256) AS gold_lagged_correlation_score_id
    FROM scored
    JOIN target_period_counts counts
      ON counts.as_of_date = scored.as_of_date
     AND counts.analysis_start_date <=> scored.analysis_start_date
     AND counts.analysis_end_date <=> scored.analysis_end_date
     AND counts.target_frequency = scored.target_frequency
     AND counts.aggregation_method = scored.aggregation_method
     AND counts.transform_type = scored.transform_type
    WHERE scored.pair_count >= {MIN_PAIR_COUNT}
    """
)

# COMMAND ----------

create_empty_delta_table_from_view(gold_table("fred_lagged_correlation_scores"), "gold_lagged_correlation_scores_stage")
ensure_table_has_source_columns(gold_table("fred_lagged_correlation_scores"), "gold_lagged_correlation_scores_stage")
delete_where(
    gold_table("fred_lagged_correlation_scores"),
    f"as_of_date = DATE {sql_literal(AS_OF_DATE)} "
    f"AND analysis_start_date <=> {sql_date_expr(ANALYSIS_START_DATE)} "
    f"AND analysis_end_date <=> {sql_date_expr(ANALYSIS_END_DATE)} "
    f"AND target_frequency = {sql_literal(TARGET_FREQUENCY)} "
    f"AND aggregation_method = {sql_literal(AGGREGATION_METHOD)} "
    f"AND transform_type = {sql_literal(CORRELATION_TRANSFORM_TYPE)} "
    f"AND target_series_id = {sql_literal(TARGET_SERIES_ID)} "
    f"AND {comparison_delete_predicate}",
)
merge_view(
    gold_table("fred_lagged_correlation_scores"),
    "gold_lagged_correlation_scores_stage",
    ["as_of_date", "analysis_start_date", "analysis_end_date", "target_frequency", "aggregation_method", "transform_type", "target_series_id", "comparison_series_id", "lag_periods"],
)
set_delta_properties(gold_table("fred_lagged_correlation_scores"))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {gold_table("fred_top_lagged_correlations")} AS
    SELECT * EXCEPT (latest_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY analysis_start_date, analysis_end_date, target_frequency, aggregation_method, transform_type, target_series_id, comparison_series_id, lag_periods
                ORDER BY as_of_date DESC, gold_processed_at_utc DESC
            ) AS latest_rank
        FROM {gold_table("fred_lagged_correlation_scores")}
        WHERE correlation_score >= {MIN_CORRELATION_SCORE}
    ) ranked
    WHERE latest_rank = 1
    """
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW gold_lagged_correlation_run_summary_stage AS
    SELECT
        {sql_literal(GOLD_RUN_ID)} AS gold_run_id,
        {sql_literal(GOLD_PROCESSED_AT_UTC)} AS processed_at_utc,
        {sql_literal(TRANSFORM_NAME)} AS transform_name,
        {sql_literal(TRANSFORM_VERSION)} AS transform_version,
        DATE {sql_literal(AS_OF_DATE)} AS as_of_date,
        {sql_date_expr(ANALYSIS_START_DATE)} AS analysis_start_date,
        {sql_date_expr(ANALYSIS_END_DATE)} AS analysis_end_date,
        {sql_literal(TARGET_FREQUENCY)} AS target_frequency,
        {sql_literal(AGGREGATION_METHOD)} AS aggregation_method,
        {sql_literal(CORRELATION_TRANSFORM_TYPE)} AS correlation_transform_type,
        {sql_literal(TARGET_SERIES_ID)} AS target_series_id,
        {sql_literal(LOAD_TYPE)} AS load_type,
        {MAX_LAG_PERIODS} AS max_lag_periods,
        {MIN_PAIR_COUNT} AS min_pair_count,
        {RELIABLE_PAIR_COUNT} AS reliable_pair_count,
        {MIN_CORRELATION_SCORE} AS min_correlation_score,
        {sql_literal(CORRELATION_SCORE_METHOD)} AS correlation_score_method,
        {sql_literal('ALL' if not comparison_series else ','.join(comparison_series))} AS comparison_series_filter,
        CAST((SELECT count(*) FROM gold_lagged_correlation_source_features) AS BIGINT) AS source_feature_count,
        CAST((SELECT count(*) FROM gold_target_periods) AS BIGINT) AS target_period_count,
        CAST((SELECT count(*) FROM gold_lagged_correlation_pairs) AS BIGINT) AS lagged_correlation_pair_count,
        CAST((SELECT count(*) FROM gold_lagged_correlation_scores_stage) AS BIGINT) AS lagged_correlation_score_count,
        CAST((SELECT count(DISTINCT comparison_series_id) FROM gold_lagged_correlation_scores_stage) AS BIGINT) AS comparison_series_count,
        CAST((SELECT count(*) FROM gold_lagged_correlation_scores_stage WHERE correlation_score >= {MIN_CORRELATION_SCORE}) AS BIGINT) AS top_correlation_score_count
    """
)

append_view(gold_table("fred_lagged_correlation_run_summary"), "gold_lagged_correlation_run_summary_stage")
set_delta_properties(gold_table("fred_lagged_correlation_run_summary"))

# COMMAND ----------

for table, zorder_columns in [
    (gold_table("fred_lagged_correlation_scores"), ["as_of_date", "target_series_id", "comparison_series_id"]),
    (gold_table("fred_lagged_correlation_run_summary"), ["as_of_date", "target_series_id"]),
]:
    optimize_table(table, zorder_columns)

# COMMAND ----------

print(
    f"""
Lagged correlation analysis ready:
  Source table: {CATALOG}.{GOLD_SCHEMA}.fred_transformed_features_long
  Score table: {CATALOG}.{GOLD_SCHEMA}.fred_lagged_correlation_scores
  Top correlation view: {CATALOG}.{GOLD_SCHEMA}.fred_top_lagged_correlations
  Run summary: {CATALOG}.{GOLD_SCHEMA}.fred_lagged_correlation_run_summary

Correlation score:
  correlation_score = r_squared * min(1, pair_count / reliable_pair_count)
"""
)

display(spark.table(gold_table("fred_lagged_correlation_run_summary")).where(f"gold_run_id = '{GOLD_RUN_ID}'"))

# COMMAND ----------

display(
    spark.table(gold_table("fred_top_lagged_correlations"))
    .where(
        f"as_of_date = DATE '{AS_OF_DATE}' "
        f"AND analysis_start_date <=> {sql_date_expr(ANALYSIS_START_DATE)} "
        f"AND analysis_end_date <=> {sql_date_expr(ANALYSIS_END_DATE)} "
        f"AND transform_type = '{CORRELATION_TRANSFORM_TYPE}' "
        f"AND target_series_id = '{TARGET_SERIES_ID}'"
    )
    .orderBy("correlation_score", ascending=False)
)