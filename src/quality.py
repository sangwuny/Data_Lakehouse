from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from datetime import date
from statistics import median
from typing import Any


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


def parse_numeric(value_raw: Any) -> dict[str, Any]:
    if value_raw is None:
        return {
            "value_numeric": None,
            "is_missing": True,
            "missing_reason": "null",
            "is_numeric_parse_error": False,
        }

    text = str(value_raw).strip()
    if text == ".":
        return {
            "value_numeric": None,
            "is_missing": True,
            "missing_reason": "fred_dot",
            "is_numeric_parse_error": False,
        }
    if text == "":
        return {
            "value_numeric": None,
            "is_missing": True,
            "missing_reason": "empty_string",
            "is_numeric_parse_error": False,
        }

    try:
        value = float(text)
    except ValueError:
        return {
            "value_numeric": None,
            "is_missing": True,
            "missing_reason": "numeric_parse_error",
            "is_numeric_parse_error": True,
        }

    if not math.isfinite(value):
        return {
            "value_numeric": None,
            "is_missing": True,
            "missing_reason": "non_finite_numeric",
            "is_numeric_parse_error": True,
        }

    return {
        "value_numeric": value,
        "is_missing": False,
        "missing_reason": None,
        "is_numeric_parse_error": False,
    }


def robust_z_scores(values: list[float | None]) -> list[float | None]:
    numeric_values = [value for value in values if value is not None]
    if len(numeric_values) < 3:
        return [None for _ in values]

    center = median(numeric_values)
    absolute_deviations = [abs(value - center) for value in numeric_values]
    mad = median(absolute_deviations)
    if mad == 0:
        return [0.0 if value is not None else None for value in values]

    return [0.6745 * (value - center) / mad if value is not None else None for value in values]


def compute_diff_values(rows: list[dict[str, Any]]) -> list[float | None]:
    indexed = list(enumerate(rows))
    indexed.sort(
        key=lambda item: (
            parse_iso_date(item[1].get("observation_date")) or date.min,
            item[1].get("realtime_start") or "",
            item[1].get("realtime_end") or "",
        )
    )

    diffs: list[float | None] = [None for _ in rows]
    previous_value: float | None = None
    for original_index, row in indexed:
        value = row.get("value_numeric")
        if value is None or previous_value is None:
            diffs[original_index] = None
        else:
            diffs[original_index] = value - previous_value
        if value is not None:
            previous_value = value
    return diffs


def classify_duplicates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    exact_groups: dict[tuple[Any, ...], list[int]] = defaultdict(list)
    date_groups: dict[tuple[Any, ...], list[int]] = defaultdict(list)

    for index, row in enumerate(rows):
        exact_key = (
            row.get("series_id"),
            row.get("observation_date"),
            row.get("realtime_start"),
            row.get("realtime_end"),
            row.get("value_raw"),
            row.get("bronze_run_id"),
        )
        date_key = (row.get("series_id"), row.get("observation_date"))
        exact_groups[exact_key].append(index)
        date_groups[date_key].append(index)

    flags = [
        {
            "is_duplicate": False,
            "duplicate_type": None,
            "duplicate_group_id": None,
            "duplicate_group_size": 1,
            "duplicate_keep_candidate": True,
            "is_observation_date_repeated": False,
            "observation_date_group_count": 1,
        }
        for _ in rows
    ]

    for key, indexes in exact_groups.items():
        if len(indexes) <= 1:
            continue
        group_id = stable_hash(key)
        for position, index in enumerate(indexes):
            flags[index].update(
                {
                    "is_duplicate": position > 0,
                    "duplicate_type": "exact",
                    "duplicate_group_id": group_id,
                    "duplicate_group_size": len(indexes),
                    "duplicate_keep_candidate": position == 0,
                }
            )

    for key, indexes in date_groups.items():
        if len(indexes) <= 1:
            continue
        unique_realtime = {
            (
                rows[index].get("realtime_start"),
                rows[index].get("realtime_end"),
                rows[index].get("value_raw"),
            )
            for index in indexes
        }
        repeated_type = "same_observation_date"
        if len(unique_realtime) > 1:
            repeated_type = "same_observation_date_different_realtime_or_value"
        for index in indexes:
            flags[index]["is_observation_date_repeated"] = True
            flags[index]["observation_date_group_count"] = len(indexes)
            if flags[index]["duplicate_type"] is None:
                flags[index]["duplicate_type"] = repeated_type

    return flags


def add_quality_flags(rows: list[dict[str, Any]], *, outlier_threshold: float = 6.0) -> list[dict[str, Any]]:
    enriched = [dict(row) for row in rows]

    for row in enriched:
        row.update(parse_numeric(row.get("value_raw")))

    duplicate_flags = classify_duplicates(enriched)
    level_scores = robust_z_scores([row.get("value_numeric") for row in enriched])
    diff_values = compute_diff_values(enriched)
    diff_scores = robust_z_scores(diff_values)

    for row, duplicate_flag, level_score, diff_value, diff_score in zip(
        enriched,
        duplicate_flags,
        level_scores,
        diff_values,
        diff_scores,
        strict=True,
    ):
        row.update(duplicate_flag)
        diff_abs = abs(diff_score) if diff_score is not None else None
        outlier_score = diff_abs
        row.update(
            {
                "diff_value": diff_value,
                "outlier_level_score": level_score,
                "outlier_diff_score": diff_score,
                "outlier_score": outlier_score,
                "outlier_method": "robust_zscore_mad_diff",
                "outlier_threshold": outlier_threshold,
                "is_outlier": outlier_score is not None and outlier_score > outlier_threshold,
            }
        )

    return enriched


def summarize_quality(rows: list[dict[str, Any]]) -> dict[str, Any]:
    row_count = len(rows)
    observation_dates = [row.get("observation_date") for row in rows if row.get("observation_date")]
    numeric_values = [row.get("value_numeric") for row in rows if row.get("value_numeric") is not None]

    missing_count = sum(1 for row in rows if row.get("is_missing"))
    duplicate_count = sum(1 for row in rows if row.get("is_duplicate"))
    repeated_date_count = sum(1 for row in rows if row.get("is_observation_date_repeated"))
    outlier_count = sum(1 for row in rows if row.get("is_outlier"))
    parse_error_count = sum(1 for row in rows if row.get("is_numeric_parse_error"))

    return {
        "row_count": row_count,
        "date_min": min(observation_dates) if observation_dates else None,
        "date_max": max(observation_dates) if observation_dates else None,
        "numeric_min": min(numeric_values) if numeric_values else None,
        "numeric_max": max(numeric_values) if numeric_values else None,
        "missing_count": missing_count,
        "missing_rate": missing_count / row_count if row_count else None,
        "numeric_parse_error_count": parse_error_count,
        "duplicate_count": duplicate_count,
        "repeated_observation_date_count": repeated_date_count,
        "outlier_count": outlier_count,
        "outlier_rate": outlier_count / row_count if row_count else None,
    }
