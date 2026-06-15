from __future__ import annotations

import hashlib
import json
from typing import Any


TRANSFORM_VERSION = "0.1.0"


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def make_lineage_id(
    *,
    silver_run_id: str,
    series_id: str,
    source_observations_path: str,
    target_observations_path: str,
) -> str:
    return stable_hash(
        {
            "silver_run_id": silver_run_id,
            "series_id": series_id,
            "source_observations_path": source_observations_path,
            "target_observations_path": target_observations_path,
        }
    )


def build_lineage_event(
    *,
    lineage_id: str,
    silver_run_id: str,
    bronze_run_id: str | None,
    collection_date: str | None,
    series_id: str,
    processed_at_utc: str,
    source_observations_path: str,
    source_metadata_path: str | None,
    source_vintage_dates_path: str | None,
    target_observations_path: str,
    target_quality_report_path: str,
    target_lineage_path: str,
    row_count_input: int,
    row_count_output: int,
    quality_summary: dict[str, Any],
) -> dict[str, Any]:
    return {
        "lineage_id": lineage_id,
        "silver_run_id": silver_run_id,
        "bronze_run_id": bronze_run_id,
        "collection_date": collection_date,
        "series_id": series_id,
        "processed_at_utc": processed_at_utc,
        "source_layer": "bronze",
        "source_observations_path": source_observations_path,
        "source_metadata_path": source_metadata_path,
        "source_vintage_dates_path": source_vintage_dates_path,
        "target_layer": "silver",
        "target_observations_path": target_observations_path,
        "target_quality_report_path": target_quality_report_path,
        "target_lineage_path": target_lineage_path,
        "transform_name": "clean_fred_observations",
        "transform_version": TRANSFORM_VERSION,
        "rules": [
            "preserve_bronze_value_raw",
            "parse_value_raw_to_float",
            "flag_fred_dot_empty_null_and_parse_missing",
            "classify_exact_duplicates_without_dropping_rows",
            "detect_repeated_observation_dates_without_treating_vintages_as_deletions",
            "flag_robust_zscore_mad_diff_outliers",
            "preserve_realtime_period_and_source_paths",
        ],
        "row_count_input": row_count_input,
        "row_count_output": row_count_output,
        "quality_summary": quality_summary,
    }
