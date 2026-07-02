# Databricks notebook source
# MAGIC %md
# MAGIC # 01c Bronze FRED Current Observations
# MAGIC
# MAGIC Loads FRED-only current observations for series that do not provide ALFRED
# MAGIC revision history. This notebook intentionally writes to a separate Bronze
# MAGIC table so current-only data is not mixed into the ALFRED vintage-date
# MAGIC observation version table.

# COMMAND ----------

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("secret_scope", "fred-lakehouse", "Secret scope")
dbutils.widgets.text("secret_key", "fred_api_key", "Secret key")
dbutils.widgets.text("seed_catalog_path", "../configs/fred_seed_series.json", "Seed catalog path")
dbutils.widgets.text("series_ids", "ALL", "FRED-only series IDs: SP500 or ALL")
dbutils.widgets.text("observation_start", "2010-01-01", "Observation start")
dbutils.widgets.text("observation_end", "", "Observation end")
dbutils.widgets.text("sleep_seconds", "0", "Sleep seconds")
dbutils.widgets.text("retry_sleep_seconds", "0.05,0.1,0.5", "Retry sleep seconds")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SECRET_KEY = dbutils.widgets.get("secret_key").strip()
SEED_CATALOG_PATH = dbutils.widgets.get("seed_catalog_path").strip()
SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
OBSERVATION_START = dbutils.widgets.get("observation_start").strip() or "2010-01-01"
OBSERVATION_END = dbutils.widgets.get("observation_end").strip() or None
SLEEP_SECONDS = float(dbutils.widgets.get("sleep_seconds"))
RETRY_SLEEP_SECONDS_PARAM = dbutils.widgets.get("retry_sleep_seconds").strip()

SOURCE = "fred"
LOAD_TYPE = "fred_current_observations"
BRONZE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
COLLECTION_DATE = datetime.strptime(BRONZE_RUN_ID, "%Y%m%dT%H%M%SZ").date().isoformat()

# COMMAND ----------

def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def table_name(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}.{quote_ident(table)}"


spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(BRONZE_SCHEMA)}")

print(f"Bronze FRED current target schema: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Bronze FRED current run_id: {BRONZE_RUN_ID}")
print(f"Collection date: {COLLECTION_DATE}")
print(f"Observation range: {OBSERVATION_START} to {OBSERVATION_END or 'latest'}")

# COMMAND ----------

FRED_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
print("FRED API key loaded:", bool(FRED_API_KEY))

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


seed_catalog = normalize_seed_catalog(load_seed_catalog(SEED_CATALOG_PATH))
fred_only_catalog = [item for item in seed_catalog if not item.get("alfred_available", True)]
if SERIES_IDS_PARAM.upper() == "ALL":
    selected_specs = fred_only_catalog
else:
    requested = [item.strip().upper() for item in SERIES_IDS_PARAM.split(",") if item.strip()]
    catalog_by_id = {item["series_id"]: item for item in seed_catalog}
    missing = [series_id for series_id in requested if series_id not in catalog_by_id]
    if missing:
        raise ValueError(f"Series IDs were not found in seed catalog: {missing}")
    selected_specs = [catalog_by_id[series_id] for series_id in requested]
    alfred_requested = [item["series_id"] for item in selected_specs if item.get("alfred_available", True)]
    if alfred_requested:
        raise ValueError(
            "These series provide ALFRED revision history and belong in 01a/01b, not the FRED-current loader: "
            + ", ".join(alfred_requested)
        )

if not selected_specs:
    raise ValueError("No FRED-only current series selected.")

print(f"Seed catalog series count: {len(seed_catalog)}")
print(f"FRED-only series count: {len(fred_only_catalog)}")
print(f"Selected FRED current series count: {len(selected_specs)}")
print("Selected FRED current series:", ", ".join(item["series_id"] for item in selected_specs[:20]) + (" ..." if len(selected_specs) > 20 else ""))

# COMMAND ----------

class FredApiError(RuntimeError):
    pass


def iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def parse_sleep_seconds_list(value: str) -> list[float]:
    if not value:
        return []
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def retry_sleep_schedule(base_sleep_seconds: float) -> list[float]:
    schedule = [base_sleep_seconds]
    for seconds in RETRY_SLEEP_SECONDS:
        if seconds > base_sleep_seconds and seconds not in schedule:
            schedule.append(seconds)
    return schedule


def sleep_if_needed(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


RETRY_SLEEP_SECONDS = parse_sleep_seconds_list(RETRY_SLEEP_SECONDS_PARAM)
print("Retry sleep seconds:", ", ".join(str(item) for item in RETRY_SLEEP_SECONDS) or "none")


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
            req = request.Request(url, headers={"User-Agent": "causal-lakehouse-fred-current-bronze/0.2"})
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


def series_current_observations(series_id: str) -> list[dict[str, Any]]:
    responses = []
    limit = 100000
    offset = 0
    while True:
        response = fred_get(
            "/fred/series/observations",
            {
                "series_id": series_id,
                "limit": limit,
                "offset": offset,
                "sort_order": "asc",
                "observation_start": OBSERVATION_START,
                "observation_end": OBSERVATION_END,
            },
        )
        responses.append(response)
        observations = response["payload"].get("observations", [])
        total_count = int(response["payload"].get("count", len(observations)))
        offset += len(observations)
        if offset >= total_count or not observations:
            break
    return responses

# COMMAND ----------

def string_schema(fields: list[str], integer_fields: set[str] | None = None) -> StructType:
    integer_fields = integer_fields or set()
    return StructType([StructField(name, IntegerType() if name in integer_fields else StringType(), True) for name in fields])


CURRENT_OBSERVATION_SCHEMA = string_schema(
    [
        "realtime_start",
        "realtime_end",
        "observation_date",
        "value_raw",
        "source",
        "series_id",
        "endpoint",
        "request_params_json",
        "request_params_hash",
        "redacted_url",
        "bronze_run_id",
        "collection_date",
        "collected_at_utc",
        "load_type",
        "attempts",
    ],
    {"attempts"},
)

CURRENT_DEDUPE_KEY_FIELDS = ["source", "series_id", "observation_date"]


def spark_sql_type(field: StructField) -> str:
    if isinstance(field.dataType, IntegerType):
        return "INT"
    return "STRING"


def ensure_delta_table(table: str, schema: StructType) -> None:
    columns = ",\n  ".join(f"{quote_ident(field.name)} {spark_sql_type(field)}" for field in schema.fields)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name(table)} (\n  {columns}\n) USING DELTA")
    spark.sql(
        f"""
        ALTER TABLE {table_name(table)} SET TBLPROPERTIES (
          'quality' = 'bronze',
          'lakehouse.layer' = 'bronze',
          'pipeline.family' = 'fred_current',
          'point_in_time_safe' = 'false'
        )
        """
    )


def dedupe_rows(rows: list[dict[str, Any]], key_fields: list[str]) -> list[dict[str, Any]]:
    deduped = {}
    for row in rows:
        deduped[tuple(row.get(field) for field in key_fields)] = row
    return list(deduped.values())


def merge_upsert_rows(table: str, rows: list[dict[str, Any]], schema: StructType, key_fields: list[str]) -> None:
    if not rows:
        return
    rows = dedupe_rows(rows, key_fields)
    ordered_rows = [Row(**{field.name: row.get(field.name) for field in schema.fields}) for row in rows]
    view_name = f"staging_{table}_{stable_hash([BRONZE_RUN_ID, table, len(rows)])}"
    spark.createDataFrame(ordered_rows, schema=schema).createOrReplaceTempView(view_name)

    update_fields = [field.name for field in schema.fields if field.name not in key_fields]
    source_value_fields = ["value_raw", "realtime_start", "realtime_end"]
    on_clause = " AND ".join(f"target.{quote_ident(field)} <=> source.{quote_ident(field)}" for field in key_fields)
    update_predicate = " OR ".join(
        f"NOT (target.{quote_ident(field)} <=> source.{quote_ident(field)})" for field in source_value_fields
    )
    update_clause = ", ".join(f"target.{quote_ident(field)} = source.{quote_ident(field)}" for field in update_fields)
    insert_cols = ", ".join(quote_ident(field.name) for field in schema.fields)
    insert_vals = ", ".join(f"source.{quote_ident(field.name)}" for field in schema.fields)
    spark.sql(
        f"""
        MERGE INTO {table_name(table)} AS target
        USING {quote_ident(view_name)} AS source
        ON {on_clause}
        WHEN MATCHED AND ({update_predicate}) THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )

ensure_delta_table("fred_current_observations_raw", CURRENT_OBSERVATION_SCHEMA)

# COMMAND ----------

def response_to_current_rows(
    spec: dict[str, Any],
    response: dict[str, Any],
    collected_at: str,
) -> list[dict[str, Any]]:
    params_json = json.dumps(response["params"], ensure_ascii=False, sort_keys=True)
    params_hash = stable_hash(response["params"])
    rows = []
    for item in response["payload"].get("observations", []):
        rows.append(
            {
                "realtime_start": item.get("realtime_start"),
                "realtime_end": item.get("realtime_end"),
                "observation_date": item.get("date"),
                "value_raw": item.get("value"),
                "source": SOURCE,
                "series_id": spec["series_id"],
                "endpoint": response["endpoint"],
                "request_params_json": params_json,
                "request_params_hash": params_hash,
                "redacted_url": response["redacted_url"],
                "bronze_run_id": BRONZE_RUN_ID,
                "collection_date": COLLECTION_DATE,
                "collected_at_utc": collected_at,
                "load_type": LOAD_TYPE,
                "attempts": response.get("attempts"),
            }
        )
    return rows

# COMMAND ----------

run_started_at = iso_utc()
results = []
active_sleep_seconds = SLEEP_SECONDS

for spec in selected_specs:
    series_id = spec["series_id"]
    started_at = iso_utc()
    attempt_schedule = retry_sleep_schedule(active_sleep_seconds)
    last_exc = None

    for attempt_index, attempt_sleep_seconds in enumerate(attempt_schedule, start=1):
        try:
            if attempt_index > 1:
                sleep_if_needed(attempt_sleep_seconds)
            responses = series_current_observations(series_id)
            collected_at = iso_utc()
            rows = []
            for response in responses:
                rows.extend(response_to_current_rows(spec, response, collected_at))
            merge_upsert_rows("fred_current_observations_raw", rows, CURRENT_OBSERVATION_SCHEMA, CURRENT_DEDUPE_KEY_FIELDS)
            active_sleep_seconds = attempt_sleep_seconds
            results.append(
                {
                    "series_id": series_id,
                    "status": "success",
                    "row_count": len(rows),
                    "retry_count": attempt_index - 1,
                    "sleep_seconds_used": attempt_sleep_seconds,
                    "started_at_utc": started_at,
                    "finished_at_utc": iso_utc(),
                }
            )
            print(f"{series_id}: success rows={len(rows)} sleep_seconds={attempt_sleep_seconds} retries={attempt_index - 1}")
            break
        except Exception as exc:
            last_exc = exc
            if attempt_index < len(attempt_schedule):
                next_sleep_seconds = attempt_schedule[attempt_index]
                print(
                    f"{series_id}: failed attempt={attempt_index} sleep_seconds={attempt_sleep_seconds} "
                    f"error={exc}; retrying with sleep_seconds={next_sleep_seconds}"
                )
            else:
                active_sleep_seconds = attempt_sleep_seconds
                results.append(
                    {
                        "series_id": series_id,
                        "status": "failed",
                        "row_count": 0,
                        "retry_count": attempt_index - 1,
                        "sleep_seconds_used": attempt_sleep_seconds,
                        "error_message": str(last_exc),
                        "started_at_utc": started_at,
                        "finished_at_utc": iso_utc(),
                    }
                )
                print(
                    f"{series_id}: failed after retries={attempt_index - 1} "
                    f"sleep_seconds={attempt_sleep_seconds} error={last_exc}"
                )

    sleep_if_needed(active_sleep_seconds)

try:
    dbutils.jobs.taskValues.set(key="bronze_fred_current_run_id", value=BRONZE_RUN_ID)
    dbutils.jobs.taskValues.set(key="collection_date", value=COLLECTION_DATE)
except Exception as exc:
    print(f"Task values were not set: {exc}")

# COMMAND ----------

summary = {
    "bronze_run_id": BRONZE_RUN_ID,
    "collection_date": COLLECTION_DATE,
    "source": SOURCE,
    "load_type": LOAD_TYPE,
    "series_count": len(selected_specs),
    "succeeded": sum(1 for result in results if result["status"] == "success"),
    "failed": sum(1 for result in results if result["status"] == "failed"),
    "row_count": sum(result["row_count"] for result in results),
    "started_at_utc": run_started_at,
    "finished_at_utc": iso_utc(),
}

display(spark.createDataFrame([summary]))

# COMMAND ----------

selected_series_sql = ", ".join("'" + spec["series_id"].replace("'", "''") + "'" for spec in selected_specs)

display(
    spark.sql(
        f"""
        SELECT series_id, COUNT(*) AS rows
        FROM {table_name("fred_current_observations_raw")}
        WHERE series_id IN ({selected_series_sql})
        GROUP BY series_id
        ORDER BY series_id
        """
    )
)

# COMMAND ----------

display(spark.createDataFrame(results))