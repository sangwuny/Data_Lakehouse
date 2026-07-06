# Databricks notebook source
# MAGIC %md
# MAGIC # 01d Bronze Macrotrends Observation Load
# MAGIC
# MAGIC Loads manually downloaded Macrotrends CSV files into a separate Bronze raw table.
# MAGIC
# MAGIC This notebook is intended for market series whose FRED API history is restricted,
# MAGIC currently `BAMLH0A0HYM2`. Use `01f_bronze_yahoo_finance_observations.py` for `SP500`. Rows are current reconstructed history from
# MAGIC Macrotrends files, not ALFRED point-in-time vintage records.

# COMMAND ----------

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

dbutils.widgets.text("catalog", "fred_lakehouse", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text("source_data_path", "../macrotrends", "Macrotrends CSV file or directory")
dbutils.widgets.text("file_glob", "*_chart_*.csv", "File glob when source_data_path is a directory")
dbutils.widgets.text("series_ids", "BAMLH0A0HYM2", "Series IDs: BAMLH0A0HYM2 or ALL")
dbutils.widgets.text("date_formats", "%m/%d/%Y,%Y-%m-%d", "Accepted CSV date formats")

CATALOG = dbutils.widgets.get("catalog").strip()
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema").strip()
SOURCE_DATA_PATH = dbutils.widgets.get("source_data_path").strip()
FILE_GLOB = dbutils.widgets.get("file_glob").strip() or "*.csv"
SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
DATE_FORMATS = [item.strip() for item in dbutils.widgets.get("date_formats").split(",") if item.strip()]

SOURCE = "fred"
DATA_PROVIDER = "macrotrends"
LOAD_TYPE = "macrotrends_full_history_observations"
BRONZE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
COLLECTION_DATE = datetime.strptime(BRONZE_RUN_ID, "%Y%m%dT%H%M%SZ").date().isoformat()
COLLECTED_AT_UTC = datetime.now(timezone.utc).isoformat()

# COMMAND ----------

def quote_ident(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def table_name(table: str) -> str:
    return f"{quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}.{quote_ident(table)}"


spark.sql(f"CREATE CATALOG IF NOT EXISTS {quote_ident(CATALOG)}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(CATALOG)}.{quote_ident(BRONZE_SCHEMA)}")
spark.sql(f"USE CATALOG {quote_ident(CATALOG)}")
spark.sql(f"USE SCHEMA {quote_ident(BRONZE_SCHEMA)}")

print(f"Bronze Macrotrends target schema: {CATALOG}.{BRONZE_SCHEMA}")
print(f"Bronze Macrotrends run_id: {BRONZE_RUN_ID}")
print(f"Collection date: {COLLECTION_DATE}")
print(f"Source data path: {SOURCE_DATA_PATH}")
print(f"File glob: {FILE_GLOB}")

# COMMAND ----------

def dbfs_to_local_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path.removeprefix("dbfs:/").lstrip("/")
    return path


def path_candidates(path: str) -> list[Path]:
    local_path = Path(dbfs_to_local_path(path))
    candidates = [local_path]
    if not local_path.is_absolute() and not path.startswith("dbfs:/"):
        cwd = Path.cwd()
        candidates.extend([cwd / local_path, cwd.parent / local_path, cwd.parent.parent / local_path])
    return list(dict.fromkeys(candidates))


def resolve_source_files(path: str, file_glob: str) -> list[Path]:
    errors = []
    for candidate in path_candidates(path):
        try:
            if candidate.is_file():
                return [candidate]
            if candidate.is_dir():
                files = sorted(item for item in candidate.glob(file_glob) if item.is_file())
                if files:
                    return files
                errors.append(f"{candidate}: no files matched {file_glob}")
            else:
                errors.append(f"{candidate}: path does not exist")
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")
    raise FileNotFoundError("Macrotrends source files were not found. Tried:\n" + "\n".join(errors))


def normalize_id_list(value: str) -> list[str]:
    if value.strip().upper() == "ALL":
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def infer_series_id(path: Path) -> str:
    stem = path.stem
    if "_chart_" in stem:
        return stem.split("_chart_", 1)[0].upper()
    return stem.split("_", 1)[0].upper()


def parse_observation_date(value: str) -> str:
    cleaned = value.strip().strip('"')
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Unsupported Macrotrends date value: {value}")


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def iso_utc_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


selected_series = set(normalize_id_list(SERIES_IDS_PARAM))
source_files = resolve_source_files(SOURCE_DATA_PATH, FILE_GLOB)
selected_files = [path for path in source_files if not selected_series or infer_series_id(path) in selected_series]

if not selected_files:
    raise ValueError("No Macrotrends files selected. Check source_data_path, file_glob, and series_ids.")

print(f"Found Macrotrends files: {len(source_files)}")
print(f"Selected Macrotrends files: {len(selected_files)}")
for path in selected_files:
    print(f"- {infer_series_id(path)}: {path}")

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

def rows_from_macrotrends_csv(path: Path) -> list[dict[str, Any]]:
    series_id = infer_series_id(path)
    file_hash = file_sha256(path)
    file_stat = path.stat()
    file_metadata = {
        "source_file_name": path.name,
        "source_file_size_bytes": str(file_stat.st_size),
        "source_file_modified_at_utc": iso_utc_from_timestamp(file_stat.st_mtime),
        "source_file_hash": file_hash,
    }
    request_params = {
        "data_provider": DATA_PROVIDER,
        "source_data_path": SOURCE_DATA_PATH,
        "file_glob": FILE_GLOB,
        "date_formats": DATE_FORMATS,
        "source_file_path": str(path),
        "source_file_name": path.name,
        "source_file_modified_at_utc": file_metadata["source_file_modified_at_utc"],
        "source_file_size_bytes": file_metadata["source_file_size_bytes"],
        "source_file_hash": file_metadata["source_file_hash"],
    }

    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if "Date" not in (reader.fieldnames or []) or "Value" not in (reader.fieldnames or []):
            raise ValueError(f"Macrotrends CSV must contain Date and Value columns: {path}")
        for row_number, item in enumerate(reader, start=2):
            date_raw = (item.get("Date") or "").strip()
            value_raw = (item.get("Value") or "").strip()
            if not date_raw:
                continue
            observation_date = parse_observation_date(date_raw)
            row_hash = stable_hash(
                {
                    "source": SOURCE,
                    "series_id": series_id,
                    "observation_date": observation_date,
                    "value_raw": value_raw,
                    "source_file_hash": file_hash,
                }
            )
            row_params = {
                **request_params,
                "source_row_number": row_number,
                "row_hash": row_hash,
            }
            rows.append(
                {
                    "source": SOURCE,
                    "series_id": series_id,
                    "observation_date": observation_date,
                    "value_raw": value_raw,
                    "realtime_start": observation_date,
                    "realtime_end": "9999-12-31",
                    "endpoint": "macrotrends_csv",
                    "request_params_json": json.dumps(row_params, sort_keys=True, ensure_ascii=False),
                    "request_params_hash": stable_hash(row_params),
                    "redacted_url": "",
                    "source_file_path": str(path),
                    "source_file_name": file_metadata["source_file_name"],
                    "source_file_modified_at_utc": file_metadata["source_file_modified_at_utc"],
                    "source_file_size_bytes": file_metadata["source_file_size_bytes"],
                    "source_file_hash": file_metadata["source_file_hash"],
                    "source_row_number": row_number,
                    "row_hash": row_hash,
                    "bronze_run_id": BRONZE_RUN_ID,
                    "collection_date": COLLECTION_DATE,
                    "collected_at_utc": COLLECTED_AT_UTC,
                    "load_type": LOAD_TYPE,
                    "attempts": 1,
                }
            )
    return rows


results = []
all_rows = []
for path in selected_files:
    started_at = datetime.now(timezone.utc).isoformat()
    try:
        rows = rows_from_macrotrends_csv(path)
        all_rows.extend(rows)
        results.append(
            {
                "series_id": infer_series_id(path),
                "source_file_name": path.name,
                "status": "success",
                "row_count": len(rows),
                "started_at_utc": started_at,
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
        print(f"{infer_series_id(path)}: parsed rows={len(rows)} file={path.name}")
    except Exception as exc:
        results.append(
            {
                "series_id": infer_series_id(path),
                "source_file_name": path.name,
                "status": "failed",
                "row_count": 0,
                "error_message": str(exc),
                "started_at_utc": started_at,
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
        print(f"{infer_series_id(path)}: failed file={path.name} error={exc}")

failed = [result for result in results if result["status"] == "failed"]
if failed:
    display(spark.createDataFrame(results))
    raise RuntimeError("At least one Macrotrends file failed to parse.")

merge_upsert_rows(
    "fred_current_observations_raw",
    all_rows,
    CURRENT_OBSERVATION_SCHEMA,
    CURRENT_DEDUPE_KEY_FIELDS,
)

try:
    dbutils.jobs.taskValues.set(key="bronze_macrotrends_run_id", value=BRONZE_RUN_ID)
    dbutils.jobs.taskValues.set(key="collection_date", value=COLLECTION_DATE)
except Exception as exc:
    print(f"Task values were not set: {exc}")

# COMMAND ----------

summary = {
    "bronze_run_id": BRONZE_RUN_ID,
    "collection_date": COLLECTION_DATE,
    "source": SOURCE,
    "load_type": LOAD_TYPE,
    "file_count": len(selected_files),
    "series_count": len({row["series_id"] for row in all_rows}),
    "row_count": len(all_rows),
    "collected_at_utc": COLLECTED_AT_UTC,
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