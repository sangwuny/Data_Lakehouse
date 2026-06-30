# Databricks notebook source
# MAGIC %md
# MAGIC # 01e Bronze ALFRED Reproducibility Audit
# MAGIC
# MAGIC External reconciliation notebook for revision-aware point-in-time data.
# MAGIC
# MAGIC This notebook compares:
# MAGIC
# MAGIC 1. Values reconstructed from `fred_observation_versions` for a chosen `as_of_date`.
# MAGIC 2. Values returned directly by the FRED/ALFRED API with `vintage_dates = as_of_date`.
# MAGIC
# MAGIC The result is displayed and returned for the requested series/date selection. No audit table is written.

# COMMAND ----------

import json
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("secret_scope", "fred-lakehouse", "Secret scope")
dbutils.widgets.text("secret_key", "fred_api_key", "Secret key")
dbutils.widgets.text("seed_catalog_path", "../configs/fred_seed_series.json", "Seed catalog path")
dbutils.widgets.text("series_ids", "GDPC1", "ALFRED series IDs: GDPC1,UNRATE or ALL")
dbutils.widgets.text("as_of_dates", "", "As-of dates, comma-separated; blank = UTC today")
dbutils.widgets.text("observation_start", "2010-01-01", "Observation start")
dbutils.widgets.text("observation_end", "", "Observation end, blank = each as_of_date")
dbutils.widgets.text("max_observations_per_series", "0", "Latest N observations per series/as_of_date, 0 = all")
dbutils.widgets.text("sleep_seconds", "0.1", "Sleep seconds between API calls")
dbutils.widgets.dropdown("fail_on_mismatch", "false", ["true", "false"], "Fail notebook on mismatch")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SECRET_KEY = dbutils.widgets.get("secret_key").strip()
SEED_CATALOG_PATH = dbutils.widgets.get("seed_catalog_path").strip()
SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
AS_OF_DATES_PARAM = dbutils.widgets.get("as_of_dates").strip()
OBSERVATION_START = dbutils.widgets.get("observation_start").strip() or "1776-07-04"
OBSERVATION_END = dbutils.widgets.get("observation_end").strip() or None
MAX_OBSERVATIONS_PER_SERIES = int(dbutils.widgets.get("max_observations_per_series"))
SLEEP_SECONDS = float(dbutils.widgets.get("sleep_seconds"))
FAIL_ON_MISMATCH = dbutils.widgets.get("fail_on_mismatch").strip().lower() == "true"

SOURCE = "fred"
AUDIT_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
EVALUATED_AT_UTC = datetime.now(timezone.utc).isoformat()

# COMMAND ----------

def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def table_name(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}.{quote_ident(table)}"


def unquoted_table_name(table: str) -> str:
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table}"


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def normalize_id_list(value: str) -> list[str]:
    if value.strip().upper() == "ALL":
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def parse_iso_dates(value: str) -> list[str]:
    if not value:
        return [datetime.now(timezone.utc).date().isoformat()]
    dates = [item.strip() for item in value.split(",") if item.strip()]
    for item in dates:
        date.fromisoformat(item)
    return dates


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
    raise FileNotFoundError("Seed catalog file was not found. Tried:\n" + "\n".join(errors))


def normalize_seed_catalog(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    specs = []
    seen = set()
    for item in items:
        series_id = str(item["series_id"]).strip().upper()
        if series_id in seen:
            continue
        seen.add(series_id)
        specs.append(
            {
                "series_id": series_id,
                "domain": item.get("domain"),
                "priority": item.get("priority"),
                "expected_frequency": item.get("expected_frequency"),
                "alfred_available": bool(item.get("alfred_available", True)),
                "description": item.get("description"),
            }
        )
    return specs


def table_exists(table: str) -> bool:
    try:
        return spark.catalog.tableExists(unquoted_table_name(table))
    except Exception:
        try:
            spark.table(table_name(table)).limit(1).collect()
            return True
        except Exception:
            return False


spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(BRONZE_SCHEMA)}")

if not table_exists("fred_observation_versions"):
    raise ValueError("Bronze table was not found: fred_observation_versions. Run bronze/01a or bronze/01b first.")

seed_catalog = normalize_seed_catalog(load_seed_catalog(SEED_CATALOG_PATH))
requested_series = normalize_id_list(SERIES_IDS_PARAM)
seed_by_id = {item["series_id"]: item for item in seed_catalog}
if requested_series:
    missing_requested = [series_id for series_id in requested_series if series_id not in seed_by_id]
    if missing_requested:
        raise ValueError(f"Series IDs were not found in seed catalog: {missing_requested}")
    selected_specs = [seed_by_id[series_id] for series_id in requested_series]
else:
    selected_specs = seed_catalog

fred_current_selected = [item["series_id"] for item in selected_specs if not item.get("alfred_available", True)]
if fred_current_selected:
    raise ValueError(
        "This audit requires ALFRED revision history. These FRED-current series are not eligible: "
        + ", ".join(fred_current_selected)
    )

selected_series = sorted(item["series_id"] for item in selected_specs)
as_of_dates = parse_iso_dates(AS_OF_DATES_PARAM)

print(f"ALFRED reproducibility audit schema: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Audit run_id: {AUDIT_RUN_ID}")
print("Selected series:", ", ".join(selected_series))
print("As-of dates:", ", ".join(as_of_dates))
print(f"Observation start: {OBSERVATION_START or 'series default'}")
print(f"Observation end: {OBSERVATION_END or 'each as_of_date'}")
print(f"Max observations per series/as_of_date: {MAX_OBSERVATIONS_PER_SERIES}")

# COMMAND ----------

FRED_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
print("FRED API key loaded:", bool(FRED_API_KEY))


class FredApiError(RuntimeError):
    pass


def build_url(endpoint: str, params: dict[str, Any]) -> str:
    return "https://api.stlouisfed.org" + endpoint + "?" + parse.urlencode(params)


def fred_get(endpoint: str, params: dict[str, Any], *, max_retries: int = 3, backoff_seconds: float = 1.0) -> dict[str, Any]:
    clean_params = {key: value for key, value in params.items() if value not in (None, "")}
    request_params = {**clean_params, "api_key": FRED_API_KEY, "file_type": "json"}
    redacted_params = {**clean_params, "api_key": "REDACTED", "file_type": "json"}
    url = build_url(endpoint, request_params)
    redacted_url = build_url(endpoint, redacted_params)

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            req = request.Request(url, headers={"User-Agent": "fred-lakehouse-alfred-reproducibility-audit/0.1"})
            with request.urlopen(req, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if "error_code" in payload or "error_message" in payload:
                raise FredApiError(str(payload))
            return {
                "endpoint": endpoint,
                "params": clean_params,
                "redacted_url": redacted_url,
                "payload": payload,
                "attempts": attempt,
            }
        except error.HTTPError as exc:
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            last_error = FredApiError(f"HTTP Error {exc.code}: {exc.reason}; body={body}")
            if attempt < max_retries:
                time.sleep(backoff_seconds * attempt)
        except (error.URLError, TimeoutError, json.JSONDecodeError, FredApiError) as exc:
            last_error = exc
            if attempt < max_retries:
                time.sleep(backoff_seconds * attempt)
    raise FredApiError(f"FRED request failed after {max_retries} attempts: {last_error}") from last_error


def fetch_alfred_vintage_observations(series_id: str, as_of_date: str) -> list[dict[str, Any]]:
    endpoint = "/fred/series/observations"
    limit = 100000
    offset = 0
    rows = []
    while True:
        response = fred_get(
            endpoint,
            {
                "series_id": series_id,
                "limit": limit,
                "offset": offset,
                "sort_order": "asc",
                "observation_start": OBSERVATION_START,
                "observation_end": OBSERVATION_END or as_of_date,
                "vintage_dates": as_of_date,
            },
        )
        observations = response["payload"].get("observations", [])
        for item in observations:
            rows.append(
                {
                    "series_id": series_id,
                    "as_of_date": as_of_date,
                    "observation_date": item.get("date"),
                    "external_value_raw": item.get("value"),
                    "external_realtime_start": item.get("realtime_start"),
                    "external_realtime_end": item.get("realtime_end"),
                    "external_endpoint": endpoint,
                    "external_request_params_json": json.dumps(response["params"], ensure_ascii=False, sort_keys=True),
                    "external_redacted_url": response["redacted_url"],
                    "external_attempts": str(response.get("attempts")),
                }
            )
        total_count = int(response["payload"].get("count", len(observations)))
        offset += len(observations)
        if offset >= total_count or not observations:
            break
    return rows


def latest_n_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return rows
    grouped = {}
    for row in rows:
        grouped.setdefault((row["series_id"], row["as_of_date"]), []).append(row)
    limited = []
    for _, group_rows in grouped.items():
        limited.extend(sorted(group_rows, key=lambda item: item["observation_date"] or "")[-limit:])
    return limited

# COMMAND ----------

external_rows = []
for series_id in selected_series:
    for as_of_date in as_of_dates:
        rows = fetch_alfred_vintage_observations(series_id, as_of_date)
        external_rows.extend(rows)
        print(f"{series_id}: as_of={as_of_date} external_rows={len(rows)}")
        if SLEEP_SECONDS > 0:
            time.sleep(SLEEP_SECONDS)

external_rows = latest_n_rows(external_rows, MAX_OBSERVATIONS_PER_SERIES)

EXTERNAL_SCHEMA = StructType(
    [
        StructField("series_id", StringType(), False),
        StructField("as_of_date", StringType(), False),
        StructField("observation_date", StringType(), True),
        StructField("external_value_raw", StringType(), True),
        StructField("external_realtime_start", StringType(), True),
        StructField("external_realtime_end", StringType(), True),
        StructField("external_endpoint", StringType(), True),
        StructField("external_request_params_json", StringType(), True),
        StructField("external_redacted_url", StringType(), True),
        StructField("external_attempts", StringType(), True),
    ]
)

REQUEST_SCHEMA = StructType(
    [
        StructField("series_id", StringType(), False),
        StructField("as_of_date", StringType(), False),
        StructField("observation_start", StringType(), True),
        StructField("observation_end", StringType(), True),
    ]
)

request_rows = [
    {
        "series_id": series_id,
        "as_of_date": as_of_date,
        "observation_start": OBSERVATION_START,
        "observation_end": OBSERVATION_END or as_of_date,
    }
    for series_id in selected_series
    for as_of_date in as_of_dates
]

spark.createDataFrame([Row(**row) for row in external_rows], schema=EXTERNAL_SCHEMA).createOrReplaceTempView("audit_external_alfred")
spark.createDataFrame([Row(**row) for row in request_rows], schema=REQUEST_SCHEMA).createOrReplaceTempView("audit_requests")

if MAX_OBSERVATIONS_PER_SERIES > 0:
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW audit_requested_observation_keys AS
        SELECT DISTINCT series_id, as_of_date, observation_date
        FROM audit_external_alfred
        """
    )
    observation_key_join = """
    JOIN audit_requested_observation_keys keys
      ON candidates.series_id = keys.series_id
     AND candidates.as_of_date = keys.as_of_date
     AND candidates.observation_date = keys.observation_date
    """
else:
    observation_key_join = ""

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW audit_internal_candidates AS
    SELECT
        requests.as_of_date,
        observations.series_id,
        observations.observation_date,
        observations.value_raw AS internal_value_raw,
        observations.realtime_start AS internal_realtime_start,
        observations.realtime_end AS internal_realtime_end,
        observations.vintage_date AS internal_vintage_date,
        observations.available_at AS internal_available_at,
        observations.observation_version_id AS internal_observation_version_id,
        observations.first_seen_bronze_run_id AS internal_first_seen_bronze_run_id,
        observations.last_seen_bronze_run_id AS internal_last_seen_bronze_run_id,
        ROW_NUMBER() OVER (
            PARTITION BY requests.as_of_date, observations.series_id, observations.observation_date
            ORDER BY
                try_cast(observations.realtime_start AS DATE) DESC,
                try_cast(coalesce(observations.available_at, observations.realtime_start) AS DATE) DESC,
                observations.observation_version_id DESC
        ) AS internal_rank
    FROM {table_name("fred_observation_versions")} observations
    JOIN audit_requests requests
      ON observations.series_id = requests.series_id
    WHERE try_cast(observations.observation_date AS DATE) >= try_cast(requests.observation_start AS DATE)
      AND try_cast(observations.observation_date AS DATE) <= try_cast(requests.observation_end AS DATE)
      AND try_cast(observations.realtime_start AS DATE) <= try_cast(requests.as_of_date AS DATE)
      AND try_cast(observations.realtime_end AS DATE) >= try_cast(requests.as_of_date AS DATE)
      AND try_cast(coalesce(observations.available_at, observations.realtime_start) AS DATE) <= try_cast(requests.as_of_date AS DATE)
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW audit_internal_reconstructed AS
    SELECT
        candidates.as_of_date,
        candidates.series_id,
        candidates.observation_date,
        candidates.internal_value_raw,
        candidates.internal_realtime_start,
        candidates.internal_realtime_end,
        candidates.internal_vintage_date,
        candidates.internal_available_at,
        candidates.internal_observation_version_id,
        candidates.internal_first_seen_bronze_run_id,
        candidates.internal_last_seen_bronze_run_id
    FROM audit_internal_candidates candidates
    {observation_key_join}
    WHERE candidates.internal_rank = 1
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMP VIEW audit_reconciliation_stage AS
    SELECT
        {sql_literal(AUDIT_RUN_ID)} AS audit_run_id,
        {sql_literal(EVALUATED_AT_UTC)} AS evaluated_at_utc,
        coalesce(internal.series_id, external.series_id) AS series_id,
        coalesce(internal.as_of_date, external.as_of_date) AS as_of_date,
        coalesce(internal.observation_date, external.observation_date) AS observation_date,
        internal.internal_value_raw,
        external.external_value_raw,
        try_cast(internal.internal_value_raw AS DOUBLE) AS internal_value_numeric,
        try_cast(external.external_value_raw AS DOUBLE) AS external_value_numeric,
        CASE
            WHEN internal.series_id IS NULL THEN NULL
            WHEN external.series_id IS NULL THEN NULL
            WHEN try_cast(internal.internal_value_raw AS DOUBLE) IS NULL THEN NULL
            WHEN try_cast(external.external_value_raw AS DOUBLE) IS NULL THEN NULL
            ELSE abs(try_cast(internal.internal_value_raw AS DOUBLE) - try_cast(external.external_value_raw AS DOUBLE))
        END AS numeric_abs_diff,
        internal.internal_realtime_start,
        internal.internal_realtime_end,
        internal.internal_vintage_date,
        internal.internal_available_at,
        internal.internal_observation_version_id,
        internal.internal_first_seen_bronze_run_id,
        internal.internal_last_seen_bronze_run_id,
        external.external_realtime_start,
        external.external_realtime_end,
        external.external_endpoint,
        external.external_request_params_json,
        external.external_redacted_url,
        external.external_attempts,
        CASE
            WHEN internal.series_id IS NULL THEN 'missing_internal'
            WHEN external.series_id IS NULL THEN 'missing_external'
            WHEN trim(CAST(internal.internal_value_raw AS STRING)) <=> trim(CAST(external.external_value_raw AS STRING)) THEN 'match'
            ELSE 'value_mismatch'
        END AS match_status
    FROM audit_internal_reconstructed internal
    FULL OUTER JOIN audit_external_alfred external
      ON internal.series_id = external.series_id
     AND internal.as_of_date = external.as_of_date
     AND internal.observation_date = external.observation_date
    """
)

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT
            series_id,
            as_of_date,
            count(*) AS compared_rows,
            sum(CASE WHEN match_status = 'match' THEN 1 ELSE 0 END) AS matched_rows,
            sum(CASE WHEN match_status = 'value_mismatch' THEN 1 ELSE 0 END) AS value_mismatch_rows,
            sum(CASE WHEN match_status = 'missing_internal' THEN 1 ELSE 0 END) AS missing_internal_rows,
            sum(CASE WHEN match_status = 'missing_external' THEN 1 ELSE 0 END) AS missing_external_rows,
            max(numeric_abs_diff) AS max_numeric_abs_diff
        FROM audit_reconciliation_stage
        GROUP BY series_id, as_of_date
        ORDER BY series_id, as_of_date
        """
    )
)

display(
    spark.sql(
        """
        SELECT *
        FROM audit_reconciliation_stage
        WHERE match_status <> 'match'
        ORDER BY series_id, as_of_date, observation_date
        """
    )
)

# COMMAND ----------

result_row = spark.sql(
    """
    SELECT
        count(*) AS compared_rows,
        sum(CASE WHEN match_status = 'match' THEN 1 ELSE 0 END) AS matched_rows,
        sum(CASE WHEN match_status <> 'match' THEN 1 ELSE 0 END) AS mismatch_count,
        sum(CASE WHEN match_status = 'value_mismatch' THEN 1 ELSE 0 END) AS value_mismatch_rows,
        sum(CASE WHEN match_status = 'missing_internal' THEN 1 ELSE 0 END) AS missing_internal_rows,
        sum(CASE WHEN match_status = 'missing_external' THEN 1 ELSE 0 END) AS missing_external_rows,
        max(numeric_abs_diff) AS max_numeric_abs_diff
    FROM audit_reconciliation_stage
    """
).collect()[0]

mismatch_count = int(result_row["mismatch_count"] or 0)
values_match = mismatch_count == 0

summary = {
    "audit_run_id": AUDIT_RUN_ID,
    "evaluated_at_utc": EVALUATED_AT_UTC,
    "selected_series": ",".join(selected_series),
    "as_of_dates": ",".join(as_of_dates),
    "compared_rows": int(result_row["compared_rows"] or 0),
    "matched_rows": int(result_row["matched_rows"] or 0),
    "mismatch_count": mismatch_count,
    "value_mismatch_rows": int(result_row["value_mismatch_rows"] or 0),
    "missing_internal_rows": int(result_row["missing_internal_rows"] or 0),
    "missing_external_rows": int(result_row["missing_external_rows"] or 0),
    "max_numeric_abs_diff": None if result_row["max_numeric_abs_diff"] is None else str(result_row["max_numeric_abs_diff"]),
    "values_match": str(values_match).lower(),
}

display(spark.createDataFrame([summary]))

result_json = json.dumps(summary, ensure_ascii=False, sort_keys=True)

print(f"ALFRED reproducibility values_match: {values_match}")
print(f"ALFRED reproducibility mismatches: {mismatch_count}")
print(result_json)

if FAIL_ON_MISMATCH and mismatch_count > 0:
    raise ValueError(f"ALFRED reproducibility audit failed with {mismatch_count} mismatched row(s): {result_json}")

dbutils.notebook.exit(result_json)