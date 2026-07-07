# Databricks notebook source
# MAGIC %md
# MAGIC # 01f Bronze Yahoo Finance Observation Load
# MAGIC
# MAGIC Loads market index history from Yahoo Finance chart data into a separate Bronze raw table.
# MAGIC
# MAGIC This notebook is intended for market series whose FRED API history is restricted,
# MAGIC especially `SP500`, where Yahoo Finance provides daily full-history observations
# MAGIC through the `^GSPC` symbol. Rows are current reconstructed market history, not
# MAGIC ALFRED point-in-time vintage records.

# COMMAND ----------

import hashlib
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib import error, parse, request

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("series_ids", "SP500", "Series IDs: SP500 or ALL")
dbutils.widgets.text("symbol_map_json", '{"SP500":"^GSPC"}', "Series ID to Yahoo symbol JSON")
dbutils.widgets.text("observation_start", "1900-01-01", "Observation start")
dbutils.widgets.text("observation_end", "", "Observation end, blank = tomorrow UTC")
dbutils.widgets.dropdown("interval", "1d", ["1d", "1wk", "1mo"], "Yahoo interval")
dbutils.widgets.dropdown("price_field", "close", ["close", "adj_close"], "Primary value field")
dbutils.widgets.text("retry_sleep_seconds", "1,2,5", "Retry sleep seconds")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
SYMBOL_MAP_JSON = dbutils.widgets.get("symbol_map_json").strip()
OBSERVATION_START = dbutils.widgets.get("observation_start").strip() or "1900-01-01"
OBSERVATION_END = dbutils.widgets.get("observation_end").strip() or None
INTERVAL = dbutils.widgets.get("interval").strip().lower()
PRICE_FIELD = dbutils.widgets.get("price_field").strip().lower()
RETRY_SLEEP_SECONDS_PARAM = dbutils.widgets.get("retry_sleep_seconds").strip()

SOURCE = "fred"
DATA_PROVIDER = "yahoo_finance"
LOAD_TYPE = "yahoo_finance_full_history_observations"
BRONZE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
COLLECTION_DATE = datetime.strptime(BRONZE_RUN_ID, "%Y%m%dT%H%M%SZ").date().isoformat()
COLLECTED_AT_UTC = datetime.now(timezone.utc).isoformat()
YAHOO_CHART_ENDPOINT = "https://query1.finance.yahoo.com/v8/finance/chart"

# COMMAND ----------

def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def table_name(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}.{quote_ident(table)}"


spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(BRONZE_SCHEMA)}")

print(f"Bronze Yahoo Finance target schema: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Bronze Yahoo Finance run_id: {BRONZE_RUN_ID}")
print(f"Collection date: {COLLECTION_DATE}")
print(f"Observation range: {OBSERVATION_START} to {OBSERVATION_END or 'tomorrow UTC'}")
print(f"Interval: {INTERVAL}; price field: {PRICE_FIELD}")

# COMMAND ----------

class YahooFinanceApiError(RuntimeError):
    pass


def sql_date_to_epoch_seconds(value: str) -> int:
    return int(datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def default_period2_date() -> str:
    return (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()


def normalize_id_list(value: str) -> list[str]:
    if value.strip().upper() == "ALL":
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def parse_retry_sleep_seconds(value: str) -> list[float]:
    if not value:
        return []
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def full_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_url(symbol: str, params: dict[str, Any]) -> str:
    encoded_symbol = parse.quote(symbol, safe="")
    return f"{YAHOO_CHART_ENDPOINT}/{encoded_symbol}?" + parse.urlencode(params)


def yahoo_get_chart(symbol: str, params: dict[str, Any]) -> dict[str, Any]:
    clean_params = {key: value for key, value in params.items() if value not in (None, "")}
    url = build_url(symbol, clean_params)
    retry_sleep_seconds = [0.0] + parse_retry_sleep_seconds(RETRY_SLEEP_SECONDS_PARAM)
    last_error: Exception | None = None

    for attempt, sleep_seconds in enumerate(retry_sleep_seconds, start=1):
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        try:
            req = request.Request(
                url,
                headers={"User-Agent": "causal-lakehouse-yahoo-finance-bronze/0.1"},
            )
            with request.urlopen(req, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
            chart = payload.get("chart", {})
            error_payload = chart.get("error")
            if error_payload:
                raise YahooFinanceApiError(str(error_payload))
            result = chart.get("result") or []
            if not result:
                raise YahooFinanceApiError("Yahoo chart response contained no result rows.")
            return {
                "symbol": symbol,
                "endpoint": f"{YAHOO_CHART_ENDPOINT}/{symbol}",
                "params": clean_params,
                "url": url,
                "payload": payload,
                "attempts": attempt,
            }
        except error.HTTPError as exc:
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            last_error = YahooFinanceApiError(f"HTTP Error {exc.code}: {exc.reason}; body={body}")
        except (error.URLError, TimeoutError, json.JSONDecodeError, YahooFinanceApiError) as exc:
            last_error = exc

    raise YahooFinanceApiError(f"Yahoo Finance request failed after {len(retry_sleep_seconds)} attempts: {last_error}") from last_error


try:
    symbol_map = {str(key).upper(): str(value) for key, value in json.loads(SYMBOL_MAP_JSON).items()}
except Exception as exc:
    raise ValueError("symbol_map_json must be a JSON object such as {\"SP500\":\"^GSPC\"}") from exc

requested_series = normalize_id_list(SERIES_IDS_PARAM)
selected_series = sorted(symbol_map) if not requested_series else requested_series
missing_series = [series_id for series_id in selected_series if series_id not in symbol_map]
if missing_series:
    raise ValueError(f"Series IDs are missing from symbol_map_json: {missing_series}")
if INTERVAL not in {"1d", "1wk", "1mo"}:
    raise ValueError(f"Unsupported interval: {INTERVAL}")
if PRICE_FIELD not in {"close", "adj_close"}:
    raise ValueError(f"Unsupported price_field: {PRICE_FIELD}")

period1 = sql_date_to_epoch_seconds(OBSERVATION_START)
period2_date = OBSERVATION_END or default_period2_date()
period2 = sql_date_to_epoch_seconds(period2_date)
if period2 <= period1:
    raise ValueError("observation_end must be later than observation_start")

print(f"Selected Yahoo Finance series count: {len(selected_series)}")
print("Selected Yahoo Finance series:", ", ".join(f"{series_id}={symbol_map[series_id]}" for series_id in selected_series))

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

def as_raw(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def observation_date_from_timestamp(timestamp: int, gmtoffset_seconds: int) -> str:
    return datetime.fromtimestamp(timestamp + gmtoffset_seconds, tz=timezone.utc).date().isoformat()


def response_to_observation_rows(series_id: str, response: dict[str, Any]) -> list[dict[str, Any]]:
    payload = response["payload"]
    result = payload["chart"]["result"][0]
    timestamps = result.get("timestamp") or []
    meta = result.get("meta") or {}
    quote_rows = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    adjclose_rows = ((result.get("indicators") or {}).get("adjclose") or [{}])[0]

    open_values = quote_rows.get("open") or []
    high_values = quote_rows.get("high") or []
    low_values = quote_rows.get("low") or []
    close_values = quote_rows.get("close") or []
    volume_values = quote_rows.get("volume") or []
    adj_close_values = adjclose_rows.get("adjclose") or []

    gmtoffset_seconds = int(meta.get("gmtoffset", 0) or 0)
    response_metadata = {
        "currency": meta.get("currency"),
        "symbol": meta.get("symbol"),
        "exchangeName": meta.get("exchangeName"),
        "instrumentType": meta.get("instrumentType"),
        "timezone": meta.get("timezone"),
        "exchangeTimezoneName": meta.get("exchangeTimezoneName"),
        "gmtoffset": meta.get("gmtoffset"),
        "dataGranularity": meta.get("dataGranularity"),
        "range": meta.get("range"),
        "validRanges": meta.get("validRanges"),
    }
    response_hash = full_hash(payload)
    request_metadata = {
        **response["params"],
        "data_provider": DATA_PROVIDER,
        "symbol": response["symbol"],
        "price_field": PRICE_FIELD,
        "response_metadata": response_metadata,
        "response_hash": response_hash,
    }
    params_json = json.dumps(request_metadata, ensure_ascii=False, sort_keys=True)
    params_hash = stable_hash(request_metadata)

    rows = []
    for index, timestamp in enumerate(timestamps):
        close_raw = as_raw(close_values[index] if index < len(close_values) else None)
        adj_close_raw = as_raw(adj_close_values[index] if index < len(adj_close_values) else None)
        value_raw = adj_close_raw if PRICE_FIELD == "adj_close" else close_raw
        if value_raw is None:
            continue
        observation_date = observation_date_from_timestamp(int(timestamp), gmtoffset_seconds)
        rows.append(
            {
                "source": SOURCE,
                "series_id": series_id,
                "symbol": response["symbol"],
                "observation_date": observation_date,
                "value_raw": value_raw,
                "price_field": PRICE_FIELD,
                "open_raw": as_raw(open_values[index] if index < len(open_values) else None),
                "high_raw": as_raw(high_values[index] if index < len(high_values) else None),
                "low_raw": as_raw(low_values[index] if index < len(low_values) else None),
                "close_raw": close_raw,
                "adj_close_raw": adj_close_raw,
                "volume_raw": as_raw(volume_values[index] if index < len(volume_values) else None),
                "currency": meta.get("currency"),
                "exchange_name": meta.get("exchangeName"),
                "exchange_timezone_name": meta.get("exchangeTimezoneName"),
                "gmtoffset_seconds": str(gmtoffset_seconds),
                "realtime_start": observation_date,
                "realtime_end": "9999-12-31",
                "endpoint": response["endpoint"],
                "request_params_json": params_json,
                "request_params_hash": params_hash,
                "redacted_url": response["url"],
                "response_metadata_json": json.dumps(response_metadata, ensure_ascii=False, sort_keys=True),
                "response_hash": response_hash,
                "bronze_run_id": BRONZE_RUN_ID,
                "collection_date": COLLECTION_DATE,
                "collected_at_utc": COLLECTED_AT_UTC,
                "load_type": LOAD_TYPE,
                "attempts": response["attempts"],
            }
        )
    return rows

# COMMAND ----------

run_started_at = datetime.now(timezone.utc).isoformat()
results = []
all_rows = []

for series_id in selected_series:
    started_at = datetime.now(timezone.utc).isoformat()
    symbol = symbol_map[series_id]
    try:
        response = yahoo_get_chart(
            symbol,
            {
                "period1": period1,
                "period2": period2,
                "interval": INTERVAL,
                "events": "history",
                "includeAdjustedClose": "true",
            },
        )
        rows = response_to_observation_rows(series_id, response)
        all_rows.extend(rows)
        results.append(
            {
                "series_id": series_id,
                "symbol": symbol,
                "status": "success",
                "row_count": len(rows),
                "attempts": response["attempts"],
                "started_at_utc": started_at,
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
        print(f"{series_id} ({symbol}): success rows={len(rows)} attempts={response['attempts']}")
    except Exception as exc:
        results.append(
            {
                "series_id": series_id,
                "symbol": symbol,
                "status": "failed",
                "row_count": 0,
                "error_message": str(exc),
                "started_at_utc": started_at,
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
        print(f"{series_id} ({symbol}): failed error={exc}")

failed = [result for result in results if result["status"] == "failed"]
if failed:
    display(spark.createDataFrame(results))
    raise RuntimeError("At least one Yahoo Finance request failed.")

merge_upsert_rows("fred_current_observations_raw", all_rows, CURRENT_OBSERVATION_SCHEMA, CURRENT_DEDUPE_KEY_FIELDS)

try:
    dbutils.jobs.taskValues.set(key="bronze_yahoo_finance_run_id", value=BRONZE_RUN_ID)
    dbutils.jobs.taskValues.set(key="collection_date", value=COLLECTION_DATE)
except Exception as exc:
    print(f"Task values were not set: {exc}")

# COMMAND ----------

summary = {
    "bronze_run_id": BRONZE_RUN_ID,
    "collection_date": COLLECTION_DATE,
    "source": SOURCE,
    "load_type": LOAD_TYPE,
    "series_count": len(selected_series),
    "succeeded": sum(1 for result in results if result["status"] == "success"),
    "failed": sum(1 for result in results if result["status"] == "failed"),
    "row_count": len(all_rows),
    "started_at_utc": run_started_at,
    "finished_at_utc": datetime.now(timezone.utc).isoformat(),
}

display(spark.createDataFrame([summary]))

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT series_id, min(observation_date) AS min_observation_date,
               max(observation_date) AS max_observation_date,
               count(*) AS rows
        FROM {table_name("fred_current_observations_raw")}
        WHERE bronze_run_id = '{BRONZE_RUN_ID}'
        GROUP BY series_id
        ORDER BY series_id
        """
    )
)

# COMMAND ----------

display(spark.createDataFrame(results))