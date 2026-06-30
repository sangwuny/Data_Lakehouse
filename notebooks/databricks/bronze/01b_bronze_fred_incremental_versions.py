# Databricks notebook source
# MAGIC %md
# MAGIC # 01b Bronze FRED Daily Incremental Version Load
# MAGIC
# MAGIC Daily incremental load for revision-aware Bronze storage.
# MAGIC This notebook reads only a recent real-time window, supports FRED real-time or vintage-style output,
# MAGIC and merges new or revised observation versions into canonical Delta tables.

# COMMAND ----------

import calendar
import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any
from urllib import error, parse, request

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("secret_scope", "fred-lakehouse", "Secret scope")
dbutils.widgets.text("secret_key", "fred_api_key", "Secret key")
dbutils.widgets.text("seed_catalog_path", "../configs/fred_seed_series.json", "Seed catalog path")
dbutils.widgets.text("series_ids", "GDP", "Series IDs: GDP,UNRATE or ALL")
dbutils.widgets.text("sleep_seconds", "0.5", "Sleep seconds")
dbutils.widgets.text("observation_start", "", "Observation start")
dbutils.widgets.text("observation_end", "", "Observation end")
dbutils.widgets.text("realtime_start", "", "Realtime start override")
dbutils.widgets.text("realtime_end", "", "Realtime end override")
dbutils.widgets.text("lookback_days", "30", "Fallback lookback days")
dbutils.widgets.text("overlap_days", "3", "Overlap days from watermark")
dbutils.widgets.text("output_type", "1", "FRED output_type, incremental default 1")

CATALOG = dbutils.widgets.get("catalog").strip()
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SECRET_KEY = dbutils.widgets.get("secret_key").strip()
SEED_CATALOG_PATH = dbutils.widgets.get("seed_catalog_path").strip()
SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
SLEEP_SECONDS = float(dbutils.widgets.get("sleep_seconds"))
OBSERVATION_START = dbutils.widgets.get("observation_start").strip() or None
OBSERVATION_END = dbutils.widgets.get("observation_end").strip() or None
REQUESTED_REALTIME_START = dbutils.widgets.get("realtime_start").strip() or None
REQUESTED_REALTIME_END = dbutils.widgets.get("realtime_end").strip() or None
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
OVERLAP_DAYS = int(dbutils.widgets.get("overlap_days"))
OUTPUT_TYPE = int(dbutils.widgets.get("output_type"))

SOURCE = "fred"
LOAD_TYPE = "incremental"
BRONZE_SCHEMA = "bronze"
BRONZE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
COLLECTION_DATE = datetime.strptime(BRONZE_RUN_ID, "%Y%m%dT%H%M%SZ").date().isoformat()
FRED_REALTIME_TODAY = datetime.now(ZoneInfo("America/Chicago")).date().isoformat()

# COMMAND ----------

def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def table_name(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}.{quote_ident(table)}"


spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(BRONZE_SCHEMA)}")

print(f"Bronze target schema: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Bronze incremental run_id: {BRONZE_RUN_ID}")
print(f"Collection date: {COLLECTION_DATE}")
print(f"FRED realtime today: {FRED_REALTIME_TODAY}")
print(f"FRED output_type: {OUTPUT_TYPE}")
if OUTPUT_TYPE != 1:
    print("Warning: output_type 2/3 returns vintage-date cross-tab data and may not include realtime_end.")

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
alfred_catalog = [item for item in seed_catalog if item.get("alfred_available", True)]
fred_only_catalog = [item for item in seed_catalog if not item.get("alfred_available", True)]
if SERIES_IDS_PARAM.upper() == "ALL":
    selected_specs = alfred_catalog
else:
    requested = [item.strip().upper() for item in SERIES_IDS_PARAM.split(",") if item.strip()]
    catalog_by_id = {item["series_id"]: item for item in seed_catalog}
    selected_specs = []
    fred_only_requested = []
    for series_id in requested:
        spec = catalog_by_id.get(
            series_id,
            {
                "series_id": series_id,
                "domain": None,
                "priority": None,
                "expected_frequency": None,
                "alfred_available": True,
                "description": None,
            },
        )
        if not spec.get("alfred_available", True):
            fred_only_requested.append(series_id)
        else:
            selected_specs.append(spec)
    if fred_only_requested:
        raise ValueError(
            "These series do not provide ALFRED revision history and are excluded from the vintage-date Bronze pipeline: "
            + ", ".join(fred_only_requested)
        )

if not selected_specs:
    raise ValueError("No ALFRED-capable series selected for the vintage-date Bronze pipeline.")

print(f"Seed catalog series count: {len(seed_catalog)}")
print(f"ALFRED-capable series count: {len(alfred_catalog)}")
print(f"FRED-only excluded series count: {len(fred_only_catalog)}")
print(f"Selected ALFRED vintage series count: {len(selected_specs)}")
print("Selected ALFRED vintage series:", ", ".join(item["series_id"] for item in selected_specs[:20]) + (" ..." if len(selected_specs) > 20 else ""))

# COMMAND ----------

class FredApiError(RuntimeError):
    pass


def iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def infer_period_bounds(observation_date: str | None, frequency_short: str | None) -> dict[str, str | None]:
    observed = parse_iso_date(observation_date)
    frequency = (frequency_short or "").upper()
    if observed is None:
        return {
            "period_start_inferred": None,
            "period_end_inferred": None,
            "period_inference_basis": "unavailable",
        }
    if frequency == "D":
        start = end = observed
        basis = "daily_observation_date"
    elif frequency == "M":
        start = date(observed.year, observed.month, 1)
        end = date(observed.year, observed.month, calendar.monthrange(observed.year, observed.month)[1])
        basis = "monthly_calendar_period"
    elif frequency == "Q":
        start_month = observed.month
        end_month = min(start_month + 2, 12)
        start = date(observed.year, start_month, 1)
        end = date(observed.year, end_month, calendar.monthrange(observed.year, end_month)[1])
        basis = "quarterly_calendar_period"
    elif frequency == "A":
        start = date(observed.year, 1, 1)
        end = date(observed.year, 12, 31)
        basis = "annual_calendar_period"
    else:
        start = None
        end = None
        basis = "not_inferred_for_frequency"
    return {
        "period_start_inferred": start.isoformat() if start else None,
        "period_end_inferred": end.isoformat() if end else None,
        "period_inference_basis": basis,
    }


def extract_vintage_date_from_key(key: str) -> str | None:
    parts = key.replace("_", "-").split("-")
    if len(parts) < 3:
        return None
    candidate = "-".join(parts[-3:])
    return candidate if parse_iso_date(candidate) else None


def normalized_observation_items(item: dict[str, Any]) -> list[dict[str, str | None]]:
    if item.get("date") is not None and item.get("value") is not None:
        realtime_start = item.get("realtime_start")
        return [
            {
                "observation_date": item.get("date"),
                "value_raw": item.get("value"),
                "realtime_start": realtime_start,
                "realtime_end": item.get("realtime_end"),
                "vintage_date": realtime_start,
                "available_at": realtime_start,
            }
        ]

    observation_date = item.get("date") or item.get("observation_date")
    normalized = []
    for key, value in item.items():
        if key in {"date", "observation_date", "realtime_start", "realtime_end"}:
            continue
        if value in (None, ""):
            continue
        vintage_date = extract_vintage_date_from_key(key)
        if vintage_date is None:
            continue
        normalized.append(
            {
                "observation_date": observation_date,
                "value_raw": str(value),
                "realtime_start": vintage_date,
                "realtime_end": None,
                "vintage_date": vintage_date,
                "available_at": vintage_date,
            }
        )
    return normalized

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
            req = request.Request(url, headers={"User-Agent": "causal-lakehouse-versioned-bronze/0.1"})
            with request.urlopen(req, timeout=30) as response:
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


def series_metadata(series_id: str) -> dict[str, Any]:
    return fred_get("/fred/series", {"series_id": series_id})


def series_vintage_dates(series_id: str, realtime_start: str, realtime_end: str) -> tuple[dict[str, Any], list[str]]:
    response = fred_get(
        "/fred/series/vintagedates",
        {
            "series_id": series_id,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
        },
    )
    vintage_dates = [str(item) for item in response["payload"].get("vintage_dates", [])]
    return response, vintage_dates


def is_no_vintage_dates_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return "no vintage" in message or "vintage dates" in message and "not found" in message


def is_fred_only_no_alfred_error(exc: Exception) -> bool:
    return "does not exist in alfred" in str(exc).lower()


def series_observations(series_id: str, realtime_start: str, realtime_end: str) -> list[dict[str, Any]]:
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
                "realtime_start": realtime_start,
                "realtime_end": realtime_end,
                "output_type": OUTPUT_TYPE,
            },
        )
        responses.append(response)
        observations = response["payload"].get("observations", [])
        total_count = int(response["payload"].get("count", len(observations)))
        offset += len(observations)
        if offset >= total_count or len(observations) == 0:
            break
    return responses

# COMMAND ----------

def string_schema(fields: list[str], integer_fields: set[str] | None = None) -> StructType:
    integer_fields = integer_fields or set()
    return StructType([StructField(name, IntegerType() if name in integer_fields else StringType(), True) for name in fields])


RAW_PAYLOAD_SCHEMA = string_schema(
    [
        "payload_hash", "source", "series_id", "endpoint", "request_params_hash",
        "redacted_url", "response_json", "first_seen_bronze_run_id",
        "first_collected_at_utc", "last_seen_bronze_run_id", "last_collected_at_utc",
        "seen_count",
    ],
    {"seen_count"},
)

INGESTION_RUN_SCHEMA = string_schema(
    [
        "ingestion_event_id", "bronze_run_id", "collection_date", "source", "load_type",
        "series_id", "endpoint", "output_type", "realtime_start", "realtime_end",
        "observation_start", "observation_end", "request_params_json",
        "request_params_hash", "redacted_url", "payload_hash", "response_count",
        "attempts", "status", "error_message", "started_at_utc", "finished_at_utc",
    ],
    {"output_type", "response_count", "attempts"},
)

METADATA_VERSION_SCHEMA = string_schema(
    [
        "metadata_hash", "source", "series_id", "domain", "priority", "expected_frequency",
        "title", "frequency", "frequency_short", "units", "units_short",
        "seasonal_adjustment", "seasonal_adjustment_short", "observation_start",
        "observation_end", "realtime_start", "realtime_end", "last_updated",
        "popularity", "notes", "first_seen_bronze_run_id", "first_collected_at_utc",
        "last_seen_bronze_run_id", "last_collected_at_utc", "seen_count",
    ],
    {"seen_count"},
)

OBSERVATION_VERSION_SCHEMA = string_schema(
    [
        "observation_version_id", "value_hash", "source", "series_id", "domain",
        "priority", "observation_date", "period_start_inferred", "period_end_inferred",
        "period_inference_basis", "value_raw", "realtime_start", "realtime_end",
        "vintage_date", "available_at", "frequency", "frequency_short", "units",
        "units_short", "seasonal_adjustment", "request_params_hash",
        "first_seen_bronze_run_id", "first_collected_at_utc", "last_seen_bronze_run_id",
        "last_collected_at_utc", "seen_count",
    ],
    {"seen_count"},
)

WATERMARK_SCHEMA = string_schema([
    "source", "series_id", "last_successful_realtime_end", "last_successful_bronze_run_id", "updated_at_utc"
])

RUN_SUMMARY_SCHEMA = string_schema(
    [
        "bronze_run_id", "collection_date", "source", "load_type", "series_count",
        "succeeded", "failed", "observations_returned", "started_at_utc",
        "finished_at_utc",
    ],
    {"series_count", "succeeded", "failed", "observations_returned"},
)


def spark_sql_type(field: StructField) -> str:
    if isinstance(field.dataType, IntegerType):
        return "INT"
    return "STRING"


def ensure_delta_table(table: str, schema: StructType) -> None:
    columns = ",\n  ".join(f"{quote_ident(field.name)} {spark_sql_type(field)}" for field in schema.fields)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name(table)} (\n  {columns}\n) USING DELTA")


def append_rows(table: str, rows: list[dict[str, Any]], schema: StructType) -> None:
    if not rows:
        return
    ordered_rows = [Row(**{field.name: row.get(field.name) for field in schema.fields}) for row in rows]
    spark.createDataFrame(ordered_rows, schema=schema).write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name(table))


def dedupe_rows(rows: list[dict[str, Any]], key_fields: list[str]) -> list[dict[str, Any]]:
    deduped = {}
    for row in rows:
        deduped[tuple(row.get(field) for field in key_fields)] = row
    return list(deduped.values())


def merge_rows(
    table: str,
    rows: list[dict[str, Any]],
    schema: StructType,
    key_fields: list[str],
    update_assignments: dict[str, str],
) -> None:
    if not rows:
        return
    rows = dedupe_rows(rows, key_fields)
    ordered_rows = [Row(**{field.name: row.get(field.name) for field in schema.fields}) for row in rows]
    view_name = f"staging_{table}_{stable_hash([BRONZE_RUN_ID, table, len(rows)])}"
    spark.createDataFrame(ordered_rows, schema=schema).createOrReplaceTempView(view_name)

    on_clause = " AND ".join(f"target.{quote_ident(field)} <=> source.{quote_ident(field)}" for field in key_fields)
    update_clause = ", ".join(f"target.{quote_ident(field)} = {expr}" for field, expr in update_assignments.items())
    insert_cols = ", ".join(quote_ident(field.name) for field in schema.fields)
    insert_vals = ", ".join(f"source.{quote_ident(field.name)}" for field in schema.fields)
    spark.sql(
        f"""
        MERGE INTO {table_name(table)} AS target
        USING {quote_ident(view_name)} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


for table, schema in [
    ("fred_raw_response_payloads", RAW_PAYLOAD_SCHEMA),
    ("fred_ingestion_runs", INGESTION_RUN_SCHEMA),
    ("fred_series_metadata_versions", METADATA_VERSION_SCHEMA),
    ("fred_observation_versions", OBSERVATION_VERSION_SCHEMA),
    ("fred_incremental_watermarks", WATERMARK_SCHEMA),
    ("fred_run_summary", RUN_SUMMARY_SCHEMA),
]:
    ensure_delta_table(table, schema)

# COMMAND ----------

def resolve_window(series_id: str) -> tuple[str, str]:
    realtime_end = REQUESTED_REALTIME_END or FRED_REALTIME_TODAY
    if REQUESTED_REALTIME_START:
        return REQUESTED_REALTIME_START, realtime_end

    rows = spark.sql(
        f"""
        SELECT last_successful_realtime_end
        FROM {table_name("fred_incremental_watermarks")}
        WHERE source = '{SOURCE}' AND series_id = '{series_id}'
        LIMIT 1
        """
    ).collect()
    if rows and rows[0]["last_successful_realtime_end"]:
        previous = date.fromisoformat(rows[0]["last_successful_realtime_end"])
        return (previous - timedelta(days=OVERLAP_DAYS)).isoformat(), realtime_end

    return (date.fromisoformat(COLLECTION_DATE) - timedelta(days=LOOKBACK_DAYS)).isoformat(), realtime_end


def payload_hash(response: dict[str, Any]) -> str:
    return stable_hash(response["payload"])


def response_count(response: dict[str, Any]) -> int:
    payload = response["payload"]
    if "observations" in payload:
        return len(payload.get("observations") or [])
    if "seriess" in payload:
        return len(payload.get("seriess") or [])
    if "vintage_dates" in payload:
        return len(payload.get("vintage_dates") or [])
    return 0


def raw_payload_row(series_id: str, response: dict[str, Any], collected_at: str) -> dict[str, Any]:
    return {
        "payload_hash": payload_hash(response),
        "source": SOURCE,
        "series_id": series_id,
        "endpoint": response["endpoint"],
        "request_params_hash": stable_hash(response["params"]),
        "redacted_url": response["redacted_url"],
        "response_json": json.dumps(response["payload"], ensure_ascii=False, sort_keys=True),
        "first_seen_bronze_run_id": BRONZE_RUN_ID,
        "first_collected_at_utc": collected_at,
        "last_seen_bronze_run_id": BRONZE_RUN_ID,
        "last_collected_at_utc": collected_at,
        "seen_count": 1,
    }


def ingestion_event_row(
    series_id: str,
    response: dict[str, Any] | None,
    *,
    status: str,
    started_at: str,
    finished_at: str,
    realtime_start: str | None = None,
    realtime_end: str | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    params = response["params"] if response else {"series_id": series_id, "realtime_start": realtime_start, "realtime_end": realtime_end, "output_type": OUTPUT_TYPE}
    request_hash = stable_hash(params)
    endpoint = response["endpoint"] if response else "/fred/series/observations"
    return {
        "ingestion_event_id": stable_hash([BRONZE_RUN_ID, series_id, endpoint, request_hash, status]),
        "bronze_run_id": BRONZE_RUN_ID,
        "collection_date": COLLECTION_DATE,
        "source": SOURCE,
        "load_type": LOAD_TYPE,
        "series_id": series_id,
        "endpoint": endpoint,
        "output_type": int(params.get("output_type", OUTPUT_TYPE)) if params.get("output_type", OUTPUT_TYPE) is not None else None,
        "realtime_start": params.get("realtime_start"),
        "realtime_end": params.get("realtime_end"),
        "observation_start": params.get("observation_start"),
        "observation_end": params.get("observation_end"),
        "request_params_json": json.dumps(params, ensure_ascii=False, sort_keys=True),
        "request_params_hash": request_hash,
        "redacted_url": response["redacted_url"] if response else None,
        "payload_hash": payload_hash(response) if response else None,
        "response_count": response_count(response) if response else 0,
        "attempts": response["attempts"] if response else None,
        "status": status,
        "error_message": error_message,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
    }


def metadata_version_row(spec: dict[str, Any], metadata: dict[str, Any], collected_at: str) -> dict[str, Any]:
    series = (metadata["payload"].get("seriess") or [{}])[0]
    metadata_hash = stable_hash({"series_id": spec["series_id"], "metadata": series})
    return {
        "metadata_hash": metadata_hash,
        "source": SOURCE,
        "series_id": spec["series_id"],
        "domain": spec.get("domain"),
        "priority": spec.get("priority"),
        "expected_frequency": spec.get("expected_frequency"),
        "title": series.get("title"),
        "frequency": series.get("frequency"),
        "frequency_short": series.get("frequency_short"),
        "units": series.get("units"),
        "units_short": series.get("units_short"),
        "seasonal_adjustment": series.get("seasonal_adjustment"),
        "seasonal_adjustment_short": series.get("seasonal_adjustment_short"),
        "observation_start": series.get("observation_start"),
        "observation_end": series.get("observation_end"),
        "realtime_start": series.get("realtime_start"),
        "realtime_end": series.get("realtime_end"),
        "last_updated": series.get("last_updated"),
        "popularity": str(series.get("popularity")) if series.get("popularity") is not None else None,
        "notes": series.get("notes"),
        "first_seen_bronze_run_id": BRONZE_RUN_ID,
        "first_collected_at_utc": collected_at,
        "last_seen_bronze_run_id": BRONZE_RUN_ID,
        "last_collected_at_utc": collected_at,
        "seen_count": 1,
    }


def existing_observation_version_index(series_id: str) -> dict[tuple[str | None, str | None], list[dict[str, Any]]]:
    rows = spark.sql(
        f"""
        SELECT *
        FROM {table_name("fred_observation_versions")}
        WHERE source = '{SOURCE}'
          AND series_id = '{series_id}'
        """
    ).collect()
    index = {}
    for row in rows:
        item = row.asDict(recursive=True)
        key = (item.get("observation_date"), item.get("value_raw"))
        index.setdefault(key, []).append(item)
    return index


def interval_end(value: str | None) -> str:
    return value or "9999-12-31"


def find_covering_observation_version(
    existing_versions: dict[tuple[str | None, str | None], list[dict[str, Any]]],
    *,
    observation_date: str | None,
    value_raw: str | None,
    realtime_start: str | None,
    realtime_end: str | None,
) -> dict[str, Any] | None:
    if realtime_start is None:
        return None

    candidate_end = interval_end(realtime_end)
    for existing in existing_versions.get((observation_date, value_raw), []):
        existing_start = existing.get("realtime_start")
        if existing_start is None:
            continue
        existing_end = interval_end(existing.get("realtime_end"))
        if existing_start <= realtime_start and existing_end >= candidate_end:
            return existing
    return None


def row_for_existing_observation_version(existing: dict[str, Any], collected_at: str) -> dict[str, Any]:
    row = {field.name: existing.get(field.name) for field in OBSERVATION_VERSION_SCHEMA.fields}
    row["last_seen_bronze_run_id"] = BRONZE_RUN_ID
    row["last_collected_at_utc"] = collected_at
    return row


def observation_version_rows(
    spec: dict[str, Any],
    metadata: dict[str, Any],
    observation_responses: list[dict[str, Any]],
    collected_at: str,
) -> list[dict[str, Any]]:
    series = (metadata["payload"].get("seriess") or [{}])[0]
    existing_versions = existing_observation_version_index(spec["series_id"])
    rows = []
    for response in observation_responses:
        request_hash = stable_hash(response["params"])
        for item in response["payload"].get("observations", []):
            for normalized_item in normalized_observation_items(item):
                observation_date = normalized_item["observation_date"]
                value_raw = normalized_item["value_raw"]
                realtime_start = normalized_item["realtime_start"]
                realtime_end = normalized_item["realtime_end"]
                vintage_date = normalized_item["vintage_date"]
                available_at = normalized_item["available_at"]
                existing = find_covering_observation_version(
                    existing_versions,
                    observation_date=observation_date,
                    value_raw=value_raw,
                    realtime_start=realtime_start,
                    realtime_end=realtime_end,
                )
                if existing:
                    rows.append(row_for_existing_observation_version(existing, collected_at))
                    continue

                period = infer_period_bounds(observation_date, series.get("frequency_short"))
                version_key = {
                    "source": SOURCE,
                    "series_id": spec["series_id"],
                    "observation_date": observation_date,
                    "value_raw": value_raw,
                    "realtime_start": realtime_start,
                    "realtime_end": realtime_end,
                }
                rows.append(
                    {
                        "observation_version_id": stable_hash(version_key),
                        "value_hash": stable_hash(version_key),
                        "source": SOURCE,
                        "series_id": spec["series_id"],
                        "domain": spec.get("domain"),
                        "priority": spec.get("priority"),
                        "observation_date": observation_date,
                        **period,
                        "value_raw": value_raw,
                        "realtime_start": realtime_start,
                        "realtime_end": realtime_end,
                        "vintage_date": vintage_date,
                        "available_at": available_at,
                        "frequency": series.get("frequency"),
                        "frequency_short": series.get("frequency_short"),
                        "units": series.get("units"),
                        "units_short": series.get("units_short"),
                        "seasonal_adjustment": series.get("seasonal_adjustment"),
                        "request_params_hash": request_hash,
                        "first_seen_bronze_run_id": BRONZE_RUN_ID,
                        "first_collected_at_utc": collected_at,
                        "last_seen_bronze_run_id": BRONZE_RUN_ID,
                        "last_collected_at_utc": collected_at,
                        "seen_count": 1,
                    }
                )
    return rows


SEEN_UPDATE = {
    "last_seen_bronze_run_id": "source.`last_seen_bronze_run_id`",
    "last_collected_at_utc": "source.`last_collected_at_utc`",
    "seen_count": "COALESCE(target.`seen_count`, 0) + 1",
}

WATERMARK_UPDATE = {
    "last_successful_realtime_end": "source.`last_successful_realtime_end`",
    "last_successful_bronze_run_id": "source.`last_successful_bronze_run_id`",
    "updated_at_utc": "source.`updated_at_utc`",
}

# COMMAND ----------

run_started_at = iso_utc()
summary = {
    "bronze_run_id": BRONZE_RUN_ID,
    "collection_date": COLLECTION_DATE,
    "source": SOURCE,
    "load_type": LOAD_TYPE,
    "series_count": len(selected_specs),
    "succeeded": 0,
    "failed": 0,
    "observations_returned": 0,
}

for spec in selected_specs:
    series_id = spec["series_id"]
    started_at = iso_utc()
    realtime_start, realtime_end = resolve_window(series_id)
    try:
        metadata = series_metadata(series_id)
        vintage_response = None
        vintage_dates = []
        skip_reason = "no_new_vintage_dates_in_realtime_window"
        try:
            vintage_response, vintage_dates = series_vintage_dates(series_id, realtime_start, realtime_end)
        except FredApiError as vintage_exc:
            if is_fred_only_no_alfred_error(vintage_exc):
                skip_reason = "series_not_available_in_alfred_revision_history"
            elif is_no_vintage_dates_error(vintage_exc):
                skip_reason = "no_new_vintage_dates_in_realtime_window"
            else:
                raise

        if not vintage_dates:
            finished_at = iso_utc()
            collected_at = finished_at
            source_responses = [metadata] + ([vintage_response] if vintage_response else [])
            raw_rows = [raw_payload_row(series_id, response, collected_at) for response in source_responses]
            ingestion_rows = [
                ingestion_event_row(series_id, response, status="success", started_at=started_at, finished_at=finished_at)
                for response in source_responses
            ]
            ingestion_rows.append(
                ingestion_event_row(
                    series_id,
                    None,
                    status="skipped",
                    started_at=started_at,
                    finished_at=finished_at,
                    realtime_start=realtime_start,
                    realtime_end=realtime_end,
                    error_message=skip_reason,
                )
            )
            metadata_rows = [metadata_version_row(spec, metadata, collected_at)]
            watermark_rows = [
                {
                    "source": SOURCE,
                    "series_id": series_id,
                    "last_successful_realtime_end": realtime_end,
                    "last_successful_bronze_run_id": BRONZE_RUN_ID,
                    "updated_at_utc": collected_at,
                }
            ]

            merge_rows("fred_raw_response_payloads", raw_rows, RAW_PAYLOAD_SCHEMA, ["payload_hash"], SEEN_UPDATE)
            append_rows("fred_ingestion_runs", ingestion_rows, INGESTION_RUN_SCHEMA)
            merge_rows("fred_series_metadata_versions", metadata_rows, METADATA_VERSION_SCHEMA, ["metadata_hash"], SEEN_UPDATE)
            merge_rows("fred_incremental_watermarks", watermark_rows, WATERMARK_SCHEMA, ["source", "series_id"], WATERMARK_UPDATE)

            summary["succeeded"] += 1
            print(f"{series_id}: skipped window={realtime_start}..{realtime_end} reason={skip_reason}")
            time.sleep(SLEEP_SECONDS)
            continue

        observation_responses = series_observations(series_id, realtime_start, realtime_end)
        finished_at = iso_utc()
        collected_at = finished_at
        responses = [metadata, vintage_response, *observation_responses]

        raw_rows = [raw_payload_row(series_id, response, collected_at) for response in responses]
        ingestion_rows = [
            ingestion_event_row(series_id, response, status="success", started_at=started_at, finished_at=finished_at)
            for response in responses
        ]
        metadata_rows = [metadata_version_row(spec, metadata, collected_at)]
        obs_rows = observation_version_rows(spec, metadata, observation_responses, collected_at)
        watermark_rows = [
            {
                "source": SOURCE,
                "series_id": series_id,
                "last_successful_realtime_end": realtime_end,
                "last_successful_bronze_run_id": BRONZE_RUN_ID,
                "updated_at_utc": collected_at,
            }
        ]

        merge_rows("fred_raw_response_payloads", raw_rows, RAW_PAYLOAD_SCHEMA, ["payload_hash"], SEEN_UPDATE)
        append_rows("fred_ingestion_runs", ingestion_rows, INGESTION_RUN_SCHEMA)
        merge_rows("fred_series_metadata_versions", metadata_rows, METADATA_VERSION_SCHEMA, ["metadata_hash"], SEEN_UPDATE)
        merge_rows("fred_observation_versions", obs_rows, OBSERVATION_VERSION_SCHEMA, ["observation_version_id"], SEEN_UPDATE)
        merge_rows("fred_incremental_watermarks", watermark_rows, WATERMARK_SCHEMA, ["source", "series_id"], WATERMARK_UPDATE)

        summary["succeeded"] += 1
        summary["observations_returned"] += len(obs_rows)
        print(f"{series_id}: success window={realtime_start}..{realtime_end} vintage_dates={len(vintage_dates)} candidate_versions={len(obs_rows)}")
    except Exception as exc:
        finished_at = iso_utc()
        append_rows(
            "fred_ingestion_runs",
            [
                ingestion_event_row(
                    series_id,
                    None,
                    status="failed",
                    started_at=started_at,
                    finished_at=finished_at,
                    realtime_start=realtime_start,
                    realtime_end=realtime_end,
                    error_message=str(exc),
                )
            ],
            INGESTION_RUN_SCHEMA,
        )
        summary["failed"] += 1
        print(f"{series_id}: failed window={realtime_start}..{realtime_end} error={exc}")

    time.sleep(SLEEP_SECONDS)

summary["started_at_utc"] = run_started_at
summary["finished_at_utc"] = iso_utc()
append_rows("fred_run_summary", [summary], RUN_SUMMARY_SCHEMA)

try:
    dbutils.jobs.taskValues.set(key="bronze_run_id", value=BRONZE_RUN_ID)
    dbutils.jobs.taskValues.set(key="collection_date", value=COLLECTION_DATE)
except Exception as exc:
    print(f"Task values were not set: {exc}")

display(spark.createDataFrame([summary], schema=RUN_SUMMARY_SCHEMA))

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT series_id, last_successful_realtime_end, last_successful_bronze_run_id, updated_at_utc
        FROM {table_name("fred_incremental_watermarks")}
        ORDER BY series_id
        """
    )
)

