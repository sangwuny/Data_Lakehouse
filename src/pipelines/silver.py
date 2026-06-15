from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.config import PROJECT_ROOT
from src.lineage import TRANSFORM_VERSION, build_lineage_event, make_lineage_id
from src.quality import add_quality_flags, summarize_quality


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp(moment: datetime | None = None) -> str:
    return (moment or utc_now()).strftime("%Y%m%dT%H%M%SZ")


def iso_utc(moment: datetime | None = None) -> str:
    return (moment or utc_now()).isoformat()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def project_path(path: Path) -> str:
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


@dataclass(frozen=True)
class SilverPaths:
    bronze_tables_dir: Path
    silver_root: Path
    tables_dir: Path
    catalog_dir: Path
    logs_dir: Path


@dataclass(frozen=True)
class ProcessResult:
    series_id: str
    status: str
    row_count_input: int
    row_count_output: int
    missing_count: int
    duplicate_count: int
    outlier_count: int
    bronze_run_id: str | None
    collection_date: str | None
    warning: str | None = None


@dataclass(frozen=True)
class BronzeRunPartition:
    series_id: str
    collection_date: str
    run_id: str
    path: Path


class SilverFredPipeline:
    def __init__(
        self,
        *,
        bronze_tables_dir: Path | None = None,
        silver_root: Path | None = None,
        silver_run_id: str | None = None,
        outlier_threshold: float = 6.0,
    ) -> None:
        self.silver_run_id = silver_run_id or utc_stamp()
        root = silver_root or PROJECT_ROOT / "data" / "silver" / "fred"
        self.paths = SilverPaths(
            bronze_tables_dir=bronze_tables_dir or PROJECT_ROOT / "data" / "bronze" / "fred" / "tables",
            silver_root=root,
            tables_dir=root / "tables",
            catalog_dir=root / "catalog",
            logs_dir=root / "logs",
        )
        self.outlier_threshold = outlier_threshold

    def discover_series(self) -> list[str]:
        if not self.paths.bronze_tables_dir.exists():
            return []
        return sorted(
            path.name.split("=", 1)[1]
            for path in self.paths.bronze_tables_dir.glob("series_id=*")
            if path.is_dir()
        )

    def discover_bronze_runs(self, series_id: str) -> list[BronzeRunPartition]:
        series_root = self.paths.bronze_tables_dir / f"series_id={series_id}"
        runs: list[BronzeRunPartition] = []
        for collection_dir in series_root.glob("collection_date=*"):
            if not collection_dir.is_dir():
                continue
            collection_date = collection_dir.name.split("=", 1)[1]
            for run_dir in collection_dir.glob("run_id=*"):
                if not run_dir.is_dir():
                    continue
                run_id = run_dir.name.split("=", 1)[1]
                runs.append(BronzeRunPartition(series_id, collection_date, run_id, run_dir))
        return sorted(runs, key=lambda run: (run.collection_date, run.run_id))

    def select_bronze_run(
        self,
        series_id: str,
        *,
        requested_bronze_run_id: str | None,
        requested_collection_date: str | None,
    ) -> BronzeRunPartition | None:
        runs = self.discover_bronze_runs(series_id)
        if requested_collection_date:
            runs = [run for run in runs if run.collection_date == requested_collection_date]
        if requested_bronze_run_id:
            runs = [run for run in runs if run.run_id == requested_bronze_run_id]
        if not runs:
            return None
        return runs[-1]

    def select_series(
        self,
        *,
        requested_series: list[str] | None = None,
        limit: int | None = None,
    ) -> list[str]:
        available = self.discover_series()
        if requested_series:
            requested = {series_id.upper() for series_id in requested_series}
            selected = [series_id for series_id in available if series_id in requested]
        else:
            selected = available
        if limit is not None:
            selected = selected[:limit]
        return selected

    def process(
        self,
        *,
        series_ids: list[str],
        requested_bronze_run_id: str | None = None,
        requested_collection_date: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        if dry_run:
            return {
                "silver_run_id": self.silver_run_id,
                "dry_run": True,
                "series_count": len(series_ids),
                "series_ids": series_ids,
                "bronze_run_id": requested_bronze_run_id or "latest_per_series",
                "collection_date": requested_collection_date,
            }

        results: list[ProcessResult] = []
        catalog_rows: list[dict[str, Any]] = []
        lineage_events: list[dict[str, Any]] = []

        for series_id in series_ids:
            result, catalog_row, lineage_event = self.process_one(
                series_id,
                requested_bronze_run_id=requested_bronze_run_id,
                requested_collection_date=requested_collection_date,
            )
            results.append(result)
            if catalog_row:
                catalog_rows.append(catalog_row)
            if lineage_event:
                lineage_events.append(lineage_event)

        catalog_path = self.paths.catalog_dir / "series_catalog.jsonl"
        write_jsonl(catalog_path, catalog_rows)
        append_jsonl(self.paths.logs_dir / "lineage_events.jsonl", lineage_events)

        summary = {
            "silver_run_id": self.silver_run_id,
            "dry_run": False,
            "series_count": len(series_ids),
            "succeeded": sum(1 for result in results if result.status == "success"),
            "failed": sum(1 for result in results if result.status == "failed"),
            "row_count_input": sum(result.row_count_input for result in results),
            "row_count_output": sum(result.row_count_output for result in results),
            "missing_count": sum(result.missing_count for result in results),
            "duplicate_count": sum(result.duplicate_count for result in results),
            "outlier_count": sum(result.outlier_count for result in results),
            "warnings": [
                {"series_id": result.series_id, "warning": result.warning}
                for result in results
                if result.warning
            ],
            "catalog_path": project_path(catalog_path),
        }
        write_json(self.paths.logs_dir / f"silver_run_summary_{self.silver_run_id}.json", summary)
        return summary

    def process_one(
        self,
        series_id: str,
        *,
        requested_bronze_run_id: str | None,
        requested_collection_date: str | None,
    ) -> tuple[ProcessResult, dict[str, Any] | None, dict[str, Any] | None]:
        selected_run = self.select_bronze_run(
            series_id,
            requested_bronze_run_id=requested_bronze_run_id,
            requested_collection_date=requested_collection_date,
        )
        if selected_run is None:
            return (
                ProcessResult(series_id, "failed", 0, 0, 0, 0, 0, requested_bronze_run_id, requested_collection_date, "matching bronze run not found"),
                None,
                None,
            )

        source_dir = selected_run.path
        source_observations_path = source_dir / "observations.jsonl"
        source_metadata_path = source_dir / "metadata.jsonl"
        source_vintage_path = source_dir / "vintage_dates.jsonl"

        observations = read_jsonl(source_observations_path)
        if not observations:
            return (
                ProcessResult(series_id, "failed", 0, 0, 0, 0, 0, selected_run.run_id, selected_run.collection_date, "missing bronze observations"),
                None,
                None,
            )

        bronze_run_id = selected_run.run_id
        collection_date = selected_run.collection_date
        selected_observations = [row for row in observations if row.get("run_id") == bronze_run_id]
        if not selected_observations:
            selected_observations = observations
        if not selected_observations:
            return (
                ProcessResult(series_id, "failed", len(observations), 0, 0, 0, 0, bronze_run_id, collection_date, "requested run_id not found"),
                None,
                None,
            )

        metadata_rows = [row for row in read_jsonl(source_metadata_path) if row.get("run_id") == bronze_run_id]
        if not metadata_rows:
            metadata_rows = read_jsonl(source_metadata_path)
        vintage_rows = [row for row in read_jsonl(source_vintage_path) if row.get("run_id") == bronze_run_id]
        if not vintage_rows:
            vintage_rows = read_jsonl(source_vintage_path)
        metadata_row = metadata_rows[-1] if metadata_rows else {}

        processed_at = iso_utc()
        target_dir = (
            self.paths.tables_dir
            / f"series_id={series_id}"
            / f"collection_date={collection_date}"
            / f"run_id={bronze_run_id}"
        )
        target_observations_path = target_dir / "observations.jsonl"
        target_quality_path = target_dir / "quality_report.json"
        target_lineage_path = target_dir / "lineage.json"

        lineage_id = make_lineage_id(
            silver_run_id=self.silver_run_id,
            series_id=series_id,
            source_observations_path=project_path(source_observations_path),
            target_observations_path=project_path(target_observations_path),
        )
        silver_rows = self.build_silver_rows(
            selected_observations,
            silver_run_id=self.silver_run_id,
            lineage_id=lineage_id,
            processed_at_utc=processed_at,
        )
        quality_summary = summarize_quality(silver_rows)

        quality_report = {
            "series_id": series_id,
            "silver_run_id": self.silver_run_id,
            "bronze_run_id": bronze_run_id,
            "collection_date": collection_date,
            "processed_at_utc": processed_at,
            "transform_version": TRANSFORM_VERSION,
            "frequency": metadata_row.get("frequency"),
            "frequency_short": metadata_row.get("frequency_short"),
            "units": metadata_row.get("units"),
            "seasonal_adjustment": metadata_row.get("seasonal_adjustment"),
            "source_observations_path": project_path(source_observations_path),
            "source_metadata_path": project_path(source_metadata_path) if source_metadata_path.exists() else None,
            "source_vintage_dates_path": project_path(source_vintage_path) if source_vintage_path.exists() else None,
            "target_observations_path": project_path(target_observations_path),
            "vintage_date_count": len(vintage_rows),
            **quality_summary,
        }

        lineage_event = build_lineage_event(
            lineage_id=lineage_id,
            silver_run_id=self.silver_run_id,
            bronze_run_id=bronze_run_id,
            collection_date=collection_date,
            series_id=series_id,
            processed_at_utc=processed_at,
            source_observations_path=project_path(source_observations_path),
            source_metadata_path=project_path(source_metadata_path) if source_metadata_path.exists() else None,
            source_vintage_dates_path=project_path(source_vintage_path) if source_vintage_path.exists() else None,
            target_observations_path=project_path(target_observations_path),
            target_quality_report_path=project_path(target_quality_path),
            target_lineage_path=project_path(target_lineage_path),
            row_count_input=len(selected_observations),
            row_count_output=len(silver_rows),
            quality_summary=quality_summary,
        )

        write_jsonl(target_observations_path, silver_rows)
        write_json(target_quality_path, quality_report)
        write_json(target_lineage_path, lineage_event)

        catalog_row = self.build_catalog_row(
            series_id=series_id,
            metadata_row=metadata_row,
            quality_report=quality_report,
            target_observations_path=target_observations_path,
            target_quality_path=target_quality_path,
            target_lineage_path=target_lineage_path,
        )
        warning = None if vintage_rows else "no vintage_dates rows for selected bronze run"
        result = ProcessResult(
            series_id=series_id,
            status="success",
            row_count_input=len(selected_observations),
            row_count_output=len(silver_rows),
            missing_count=quality_summary["missing_count"],
            duplicate_count=quality_summary["duplicate_count"],
            outlier_count=quality_summary["outlier_count"],
            bronze_run_id=bronze_run_id,
            collection_date=collection_date,
            warning=warning,
        )
        return result, catalog_row, lineage_event

    def build_silver_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        silver_run_id: str,
        lineage_id: str,
        processed_at_utc: str,
    ) -> list[dict[str, Any]]:
        sorted_rows = sorted(
            rows,
            key=lambda row: (
                row.get("observation_date") or "",
                row.get("realtime_start") or "",
                row.get("realtime_end") or "",
            ),
        )
        normalized_rows: list[dict[str, Any]] = []
        for row in sorted_rows:
            bronze_run_id = row.get("run_id")
            normalized = dict(row)
            normalized.pop("run_id", None)
            normalized["bronze_run_id"] = bronze_run_id
            normalized_rows.append(normalized)

        quality_rows = add_quality_flags(normalized_rows, outlier_threshold=self.outlier_threshold)
        silver_rows: list[dict[str, Any]] = []
        for row in quality_rows:
            silver_row = {
                "silver_run_id": silver_run_id,
                "bronze_run_id": row.get("bronze_run_id"),
                "collection_date": row.get("collection_date"),
                "lineage_id": lineage_id,
                "source": row.get("source"),
                "series_id": row.get("series_id"),
                "domain": row.get("domain"),
                "priority": row.get("priority"),
                "observation_date": row.get("observation_date"),
                "period_start_inferred": row.get("period_start_inferred"),
                "period_end_inferred": row.get("period_end_inferred"),
                "period_inference_basis": row.get("period_inference_basis"),
                "realtime_start": row.get("realtime_start"),
                "realtime_end": row.get("realtime_end"),
                "frequency": row.get("frequency"),
                "frequency_short": row.get("frequency_short"),
                "units": row.get("units"),
                "units_short": row.get("units_short"),
                "seasonal_adjustment": row.get("seasonal_adjustment"),
                "value_raw": row.get("value_raw"),
                "value_numeric": row.get("value_numeric"),
                "is_missing": row.get("is_missing"),
                "missing_reason": row.get("missing_reason"),
                "is_numeric_parse_error": row.get("is_numeric_parse_error"),
                "is_duplicate": row.get("is_duplicate"),
                "duplicate_type": row.get("duplicate_type"),
                "duplicate_group_id": row.get("duplicate_group_id"),
                "duplicate_group_size": row.get("duplicate_group_size"),
                "duplicate_keep_candidate": row.get("duplicate_keep_candidate"),
                "is_observation_date_repeated": row.get("is_observation_date_repeated"),
                "observation_date_group_count": row.get("observation_date_group_count"),
                "diff_value": row.get("diff_value"),
                "is_outlier": row.get("is_outlier"),
                "outlier_method": row.get("outlier_method"),
                "outlier_threshold": row.get("outlier_threshold"),
                "outlier_score": row.get("outlier_score"),
                "outlier_level_score": row.get("outlier_level_score"),
                "outlier_diff_score": row.get("outlier_diff_score"),
                "collected_at_utc": row.get("collected_at_utc"),
                "silver_processed_at_utc": processed_at_utc,
                "transform_version": TRANSFORM_VERSION,
                "observations_raw_path": row.get("observations_raw_path"),
                "metadata_raw_path": row.get("metadata_raw_path"),
                "manifest_path": row.get("manifest_path"),
                "request_params_hash": row.get("request_params_hash"),
            }
            silver_rows.append(silver_row)
        return silver_rows

    def build_catalog_row(
        self,
        *,
        series_id: str,
        metadata_row: dict[str, Any],
        quality_report: dict[str, Any],
        target_observations_path: Path,
        target_quality_path: Path,
        target_lineage_path: Path,
    ) -> dict[str, Any]:
        return {
            "silver_run_id": self.silver_run_id,
            "bronze_run_id": quality_report.get("bronze_run_id"),
            "collection_date": quality_report.get("collection_date"),
            "series_id": series_id,
            "domain": metadata_row.get("domain"),
            "priority": metadata_row.get("priority"),
            "title": metadata_row.get("title"),
            "frequency": metadata_row.get("frequency"),
            "frequency_short": metadata_row.get("frequency_short"),
            "units": metadata_row.get("units"),
            "seasonal_adjustment": metadata_row.get("seasonal_adjustment"),
            "observation_start": metadata_row.get("observation_start"),
            "observation_end": metadata_row.get("observation_end"),
            "last_updated": metadata_row.get("last_updated"),
            "row_count": quality_report.get("row_count"),
            "date_min": quality_report.get("date_min"),
            "date_max": quality_report.get("date_max"),
            "missing_rate": quality_report.get("missing_rate"),
            "duplicate_count": quality_report.get("duplicate_count"),
            "outlier_count": quality_report.get("outlier_count"),
            "silver_observations_path": project_path(target_observations_path),
            "quality_report_path": project_path(target_quality_path),
            "lineage_path": project_path(target_lineage_path),
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clean FRED Bronze data into the Silver layer.")
    parser.add_argument("--series", nargs="*", default=None, help="Optional explicit FRED series IDs.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of discovered series.")
    parser.add_argument("--run-id", default=None, help="Optional Bronze run_id. Defaults to latest per series.")
    parser.add_argument("--collection-date", default=None, help="Optional Bronze collection date, YYYY-MM-DD.")
    parser.add_argument("--dry-run", action="store_true", help="Show selected series without writing Silver outputs.")
    parser.add_argument("--outlier-threshold", type=float, default=6.0, help="Robust z-score threshold.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pipeline = SilverFredPipeline(outlier_threshold=args.outlier_threshold)
    selected = pipeline.select_series(requested_series=args.series, limit=args.limit)
    summary = pipeline.process(
        series_ids=selected,
        requested_bronze_run_id=args.run_id,
        requested_collection_date=args.collection_date,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
