# Databricks notebook source
# MAGIC %md
# MAGIC # Build FRED Series Metadata Config
# MAGIC
# MAGIC Reads `fred_seed_series.json`, fetches canonical FRED series metadata, and writes
# MAGIC `fred_series_metadata.json` for non-FRED bootstrap loaders such as Macrotrends
# MAGIC and Yahoo Finance.

# COMMAND ----------

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request

# COMMAND ----------

try:
    dbutils.widgets.text("secret_scope", "fred-lakehouse", "Secret scope")
    dbutils.widgets.text("secret_key", "fred_api_key", "Secret key")
    dbutils.widgets.text("seed_catalog_path", "fred_seed_series.json", "Seed catalog path")
    dbutils.widgets.text("output_path", "fred_series_metadata.json", "Output metadata JSON path")
    dbutils.widgets.text("series_ids", "ALL", "Series IDs: ALL or comma list")
    dbutils.widgets.text("sleep_seconds", "0", "Sleep seconds between FRED requests")
    dbutils.widgets.text("retry_sleep_seconds", "0.05,0.1,0.5", "Retry sleep seconds")

    SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
    SECRET_KEY = dbutils.widgets.get("secret_key").strip()
    SEED_CATALOG_PATH = dbutils.widgets.get("seed_catalog_path").strip()
    OUTPUT_PATH = dbutils.widgets.get("output_path").strip()
    SERIES_IDS_PARAM = dbutils.widgets.get("series_ids").strip()
    SLEEP_SECONDS = float(dbutils.widgets.get("sleep_seconds"))
    RETRY_SLEEP_SECONDS_PARAM = dbutils.widgets.get("retry_sleep_seconds").strip()
    FRED_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
except NameError:
    SECRET_SCOPE = os.getenv("FRED_SECRET_SCOPE", "")
    SECRET_KEY = os.getenv("FRED_SECRET_KEY", "")
    SEED_CATALOG_PATH = os.getenv("SEED_CATALOG_PATH", "fred_seed_series.json")
    OUTPUT_PATH = os.getenv("OUTPUT_PATH", "fred_series_metadata.json")
    SERIES_IDS_PARAM = os.getenv("SERIES_IDS", "ALL")
    SLEEP_SECONDS = float(os.getenv("SLEEP_SECONDS", "0"))
    RETRY_SLEEP_SECONDS_PARAM = os.getenv("RETRY_SLEEP_SECONDS", "0.05,0.1,0.5")
    FRED_API_KEY = os.getenv("FRED_API_KEY", "")

FRED_SERIES_ENDPOINT = "https://api.stlouisfed.org/fred/series"
GENERATED_AT_UTC = datetime.now(timezone.utc).isoformat()

if not FRED_API_KEY:
    raise ValueError("FRED API key was not found. Use Databricks secrets or set FRED_API_KEY locally.")

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
        if "__file__" in globals():
            candidates.append(Path(__file__).resolve().parent / local_path)
    return list(dict.fromkeys(candidates))


def load_seed_catalog(path: str) -> tuple[list[dict[str, Any]], Path]:
    errors = []
    for candidate in path_candidates(path):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            if not isinstance(payload, list):
                raise ValueError("seed catalog JSON must be a list")
            print(f"Loaded seed catalog: {candidate}")
            return payload, candidate
        except Exception as exc:
            errors.append(f"{candidate}: {exc}")
    raise FileNotFoundError("Seed catalog file was not found. Tried:\n" + "\n".join(errors))


def normalize_id_list(value: str) -> list[str]:
    if value.strip().upper() == "ALL":
        return []
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def parse_retry_sleep_seconds(value: str) -> list[float]:
    if not value:
        return []
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def output_path_for_seed(output_path: str, resolved_seed_path: Path) -> Path:
    local_path = Path(dbfs_to_local_path(output_path))
    if local_path.is_absolute() or output_path.startswith("dbfs:/"):
        return local_path
    return resolved_seed_path.parent / local_path


def fred_get_series(series_id: str) -> dict[str, Any]:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
    }
    url = FRED_SERIES_ENDPOINT + "?" + parse.urlencode(params)
    retry_sleep_seconds = [0.0] + parse_retry_sleep_seconds(RETRY_SLEEP_SECONDS_PARAM)
    last_error: Exception | None = None

    for attempt, sleep_seconds in enumerate(retry_sleep_seconds, start=1):
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        try:
            with request.urlopen(url, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
            rows = payload.get("seriess") or []
            if not rows:
                raise ValueError(f"FRED returned no metadata for {series_id}")
            return rows[0]
        except error.HTTPError as exc:
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            last_error = RuntimeError(f"HTTP Error {exc.code}: {exc.reason}; body={body}")
        except (error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            last_error = exc

    raise RuntimeError(f"FRED metadata request failed for {series_id}: {last_error}") from last_error


def normalized_metadata(seed_item: dict[str, Any], fred_item: dict[str, Any]) -> dict[str, Any]:
    series_id = str(seed_item["series_id"]).upper()
    return {
        "series_id": series_id,
        "title": fred_item.get("title") or seed_item.get("description"),
        "frequency": fred_item.get("frequency"),
        "frequency_short": fred_item.get("frequency_short"),
        "units": fred_item.get("units"),
        "units_short": fred_item.get("units_short"),
        "seasonal_adjustment": fred_item.get("seasonal_adjustment"),
        "seasonal_adjustment_short": fred_item.get("seasonal_adjustment_short"),
        "observation_start": fred_item.get("observation_start"),
        "observation_end": fred_item.get("observation_end"),
        "last_updated": fred_item.get("last_updated"),
        "popularity": fred_item.get("popularity"),
        "notes": fred_item.get("notes"),
        "expected_frequency": seed_item.get("expected_frequency"),
        "domain": seed_item.get("domain"),
        "priority": seed_item.get("priority"),
        "alfred_available": bool(seed_item.get("alfred_available", True)),
        "seed_description": seed_item.get("description"),
        "metadata_provider": "fred_api",
        "metadata_endpoint": FRED_SERIES_ENDPOINT,
    }

# COMMAND ----------

seed_catalog, resolved_seed_path = load_seed_catalog(SEED_CATALOG_PATH)
seed_by_id = {str(item["series_id"]).upper(): item for item in seed_catalog}
requested_series = normalize_id_list(SERIES_IDS_PARAM)
selected_ids = sorted(seed_by_id) if not requested_series else requested_series
missing_ids = [series_id for series_id in selected_ids if series_id not in seed_by_id]
if missing_ids:
    raise ValueError(f"Series IDs were not found in seed catalog: {missing_ids}")

print(f"FRED series metadata target count: {len(selected_ids)}")

series_metadata = {}
failed = []
for index, series_id in enumerate(selected_ids, start=1):
    try:
        fred_item = fred_get_series(series_id)
        series_metadata[series_id] = normalized_metadata(seed_by_id[series_id], fred_item)
        print(f"[{index}/{len(selected_ids)}] {series_id}: metadata loaded")
        if SLEEP_SECONDS > 0:
            time.sleep(SLEEP_SECONDS)
    except Exception as exc:
        failed.append({"series_id": series_id, "error": str(exc)})
        print(f"[{index}/{len(selected_ids)}] {series_id}: failed error={exc}")

if failed:
    raise RuntimeError("At least one FRED metadata request failed: " + json.dumps(failed, ensure_ascii=False))

payload = {
    "generated_at_utc": GENERATED_AT_UTC,
    "metadata_provider": "fred_api",
    "metadata_endpoint": FRED_SERIES_ENDPOINT,
    "seed_catalog_path": str(resolved_seed_path),
    "series_count": len(series_metadata),
    "series": series_metadata,
}

output_path = output_path_for_seed(OUTPUT_PATH, resolved_seed_path)
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

print(f"Wrote FRED series metadata JSON: {output_path}")

try:
    display(series_metadata)
except Exception:
    pass