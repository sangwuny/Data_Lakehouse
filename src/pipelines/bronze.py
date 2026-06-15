from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from src.config import Settings, get_settings
from src.fred_client import FredApiError, FredClient, FredResponse


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp(moment: datetime | None = None) -> str:
    return (moment or utc_now()).strftime("%Y%m%dT%H%M%SZ")


def collection_date_from_run_id(run_id: str) -> str:
    try:
        return datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").date().isoformat()
    except ValueError:
        return utc_now().date().isoformat()


def iso_utc(moment: datetime | None = None) -> str:
    return (moment or utc_now()).isoformat()


def stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=True).encode("utf-8")
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


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def append_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


@dataclass(frozen=True)
class SeriesSpec:
    series_id: str
    domain: str | None = None
    priority: str | None = None
    expected_frequency: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class BronzePaths:
    raw_dir: Path
    tables_dir: Path
    logs_dir: Path


class BronzeFredCollector:
    def __init__(self, settings: Settings, *, run_id: str | None = None) -> None:
        self.settings = settings
        self.run_id = run_id or utc_stamp()
        self.collection_date = collection_date_from_run_id(self.run_id)
        self.paths = BronzePaths(
            raw_dir=settings.fred_bronze_root / "raw",
            tables_dir=settings.fred_bronze_root / "tables",
            logs_dir=settings.fred_bronze_root / "logs",
        )

    def project_path(self, path: Path) -> str:
        try:
            return path.relative_to(self.settings.project_root).as_posix()
        except ValueError:
            return path.as_posix()

    def series_table_path(self, series_id: str, filename: str) -> Path:
        return (
            self.paths.tables_dir
            / f"series_id={series_id}"
            / f"collection_date={self.collection_date}"
            / f"run_id={self.run_id}"
            / filename
        )

    def load_catalog(self, catalog_path: Path) -> list[SeriesSpec]:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        specs: list[SeriesSpec] = []
        seen: set[str] = set()
        for item in payload:
            series_id = item["series_id"].strip().upper()
            if series_id in seen:
                continue
            seen.add(series_id)
            specs.append(
                SeriesSpec(
                    series_id=series_id,
                    domain=item.get("domain"),
                    priority=item.get("priority"),
                    expected_frequency=item.get("expected_frequency"),
                    description=item.get("description"),
                )
            )
        return specs

    def collect(
        self,
        specs: list[SeriesSpec],
        *,
        dry_run: bool = False,
        include_vintages: bool = False,
        sleep_seconds: float = 0.25,
        observation_start: str | None = None,
        observation_end: str | None = None,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
    ) -> dict[str, Any]:
        if dry_run:
            return {
                "run_id": self.run_id,
                "collection_date": self.collection_date,
                "dry_run": True,
                "series_count": len(specs),
                "series_ids": [spec.series_id for spec in specs],
            }

        client = FredClient(self.settings.fred_api_key)
        summary = {
            "run_id": self.run_id,
            "collection_date": self.collection_date,
            "dry_run": False,
            "series_count": len(specs),
            "succeeded": 0,
            "failed": 0,
            "observations": 0,
        }

        for spec in specs:
            result = self.collect_one(
                client,
                spec,
                include_vintages=include_vintages,
                observation_start=observation_start,
                observation_end=observation_end,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
            )
            if result["status"] == "success":
                summary["succeeded"] += 1
                summary["observations"] += result.get("row_count", 0)
            else:
                summary["failed"] += 1
            time.sleep(sleep_seconds)

        write_json(self.paths.logs_dir / f"run_summary_{self.run_id}.json", summary)
        return summary

    def collect_one(
        self,
        client: FredClient,
        spec: SeriesSpec,
        *,
        include_vintages: bool,
        observation_start: str | None,
        observation_end: str | None,
        realtime_start: str | None,
        realtime_end: str | None,
    ) -> dict[str, Any]:
        started_at = iso_utc()
        series_raw_dir = (
            self.paths.raw_dir
            / "source=fred"
            / f"series_id={spec.series_id}"
            / f"collection_date={self.collection_date}"
            / f"run_id={self.run_id}"
        )
        try:
            metadata = client.series_metadata(spec.series_id)
            observations = client.series_observations(
                spec.series_id,
                observation_start=observation_start,
                observation_end=observation_end,
                realtime_start=realtime_start,
                realtime_end=realtime_end,
            )
            vintages = None
            vintage_error_message = None
            if include_vintages:
                try:
                    vintages = client.series_vintage_dates(spec.series_id)
                except FredApiError as exc:
                    vintage_error_message = str(exc)

            metadata_path = series_raw_dir / "metadata.json"
            observations_path = series_raw_dir / "observations.json"
            vintages_path = series_raw_dir / "vintages.json"
            manifest_path = series_raw_dir / "request_manifest.json"

            write_json(metadata_path, metadata.payload)
            write_json(observations_path, observations.payload)
            if vintages is not None:
                write_json(vintages_path, vintages.payload)

            manifest = self.build_manifest(
                spec,
                metadata,
                observations,
                vintages,
                vintage_error_message=vintage_error_message,
            )
            write_json(manifest_path, manifest)

            metadata_row = self.normalize_metadata(
                spec,
                metadata,
                metadata_path=metadata_path,
                manifest_path=manifest_path,
            )
            append_jsonl(
                self.series_table_path(spec.series_id, "metadata.jsonl"),
                [metadata_row],
            )

            observation_rows = self.normalize_observations(
                spec,
                metadata,
                observations,
                observations_path=observations_path,
                metadata_path=metadata_path,
                manifest_path=manifest_path,
            )
            row_count = append_jsonl(
                self.series_table_path(spec.series_id, "observations.jsonl"),
                observation_rows,
            )
            vintage_count = 0
            if vintages is not None:
                vintage_rows = self.normalize_vintages(
                    spec,
                    vintages,
                    vintages_path=vintages_path,
                    manifest_path=manifest_path,
                )
                vintage_count = append_jsonl(
                    self.series_table_path(spec.series_id, "vintage_dates.jsonl"),
                    vintage_rows,
                )

            log_row = {
                "run_id": self.run_id,
                "collection_date": self.collection_date,
                "source": "fred",
                "series_id": spec.series_id,
                "status": "success",
                "started_at_utc": started_at,
                "finished_at_utc": iso_utc(),
                "row_count": row_count,
                "metadata_attempts": metadata.attempts,
                "observations_attempts": observations.attempts,
                "vintages_attempts": vintages.attempts if vintages else None,
                "vintage_date_count": vintage_count,
                "vintage_error_message": vintage_error_message,
                "raw_dir": self.project_path(series_raw_dir),
                "error_message": None,
            }
            append_jsonl(self.paths.logs_dir / "collection_log.jsonl", [log_row])
            return log_row
        except FredApiError as exc:
            log_row = {
                "run_id": self.run_id,
                "collection_date": self.collection_date,
                "source": "fred",
                "series_id": spec.series_id,
                "status": "failed",
                "started_at_utc": started_at,
                "finished_at_utc": iso_utc(),
                "row_count": 0,
                "raw_dir": self.project_path(series_raw_dir),
                "error_message": str(exc),
            }
            append_jsonl(self.paths.logs_dir / "collection_log.jsonl", [log_row])
            return log_row

    def build_manifest(
        self,
        spec: SeriesSpec,
        metadata: FredResponse,
        observations: FredResponse,
        vintages: FredResponse | None,
        vintage_error_message: str | None = None,
    ) -> dict[str, Any]:
        requests = [metadata, observations]
        if vintages is not None:
            requests.append(vintages)

        return {
            "run_id": self.run_id,
            "collection_date": self.collection_date,
            "source": "fred",
            "series_id": spec.series_id,
            "domain": spec.domain,
            "priority": spec.priority,
            "expected_frequency": spec.expected_frequency,
            "collected_at_utc": iso_utc(),
            "request_params_hash": stable_hash([r.params for r in requests]),
            "requests": [
                {
                    "endpoint": response.endpoint,
                    "params": response.params,
                    "redacted_url": response.redacted_url,
                    "attempts": response.attempts,
                }
                for response in requests
            ],
            "optional_vintage_error_message": vintage_error_message,
        }

    def normalize_metadata(
        self,
        spec: SeriesSpec,
        metadata: FredResponse,
        *,
        metadata_path: Path,
        manifest_path: Path,
    ) -> dict[str, Any]:
        series = (metadata.payload.get("seriess") or [{}])[0]
        return {
            "run_id": self.run_id,
            "collection_date": self.collection_date,
            "source": "fred",
            "series_id": spec.series_id,
            "domain": spec.domain,
            "priority": spec.priority,
            "expected_frequency": spec.expected_frequency,
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
            "popularity": series.get("popularity"),
            "notes": series.get("notes"),
            "collected_at_utc": iso_utc(),
            "metadata_raw_path": self.project_path(metadata_path),
            "manifest_path": self.project_path(manifest_path),
            "request_params_hash": stable_hash(metadata.params),
        }

    def normalize_observations(
        self,
        spec: SeriesSpec,
        metadata: FredResponse,
        observations: FredResponse,
        *,
        observations_path: Path,
        metadata_path: Path,
        manifest_path: Path,
    ) -> Iterable[dict[str, Any]]:
        series = (metadata.payload.get("seriess") or [{}])[0]
        for item in observations.payload.get("observations", []):
            period = infer_period_bounds(item.get("date"), series.get("frequency_short"))
            yield {
                "run_id": self.run_id,
                "collection_date": self.collection_date,
                "source": "fred",
                "series_id": spec.series_id,
                "domain": spec.domain,
                "priority": spec.priority,
                "observation_date": item.get("date"),
                **period,
                "value_raw": item.get("value"),
                "realtime_start": item.get("realtime_start"),
                "realtime_end": item.get("realtime_end"),
                "frequency": series.get("frequency"),
                "frequency_short": series.get("frequency_short"),
                "units": series.get("units"),
                "units_short": series.get("units_short"),
                "seasonal_adjustment": series.get("seasonal_adjustment"),
                "collected_at_utc": iso_utc(),
                "observations_raw_path": self.project_path(observations_path),
                "metadata_raw_path": self.project_path(metadata_path),
                "manifest_path": self.project_path(manifest_path),
                "request_params_hash": stable_hash(observations.params),
            }

    def normalize_vintages(
        self,
        spec: SeriesSpec,
        vintages: FredResponse,
        *,
        vintages_path: Path,
        manifest_path: Path,
    ) -> Iterable[dict[str, Any]]:
        for vintage_date in vintages.payload.get("vintage_dates", []):
            yield {
                "run_id": self.run_id,
                "collection_date": self.collection_date,
                "source": "fred",
                "series_id": spec.series_id,
                "domain": spec.domain,
                "priority": spec.priority,
                "vintage_date": vintage_date,
                "collected_at_utc": iso_utc(),
                "vintages_raw_path": self.project_path(vintages_path),
                "manifest_path": self.project_path(manifest_path),
                "request_params_hash": stable_hash(vintages.params),
            }


def select_specs(
    specs: list[SeriesSpec],
    *,
    series_ids: list[str] | None,
    priorities: set[str] | None,
    limit: int | None,
) -> list[SeriesSpec]:
    selected = specs
    if series_ids:
        wanted = {series_id.upper() for series_id in series_ids}
        selected = [spec for spec in selected if spec.series_id in wanted]
    if priorities:
        selected = [spec for spec in selected if (spec.priority or "").lower() in priorities]
    if limit is not None:
        selected = selected[:limit]
    return selected


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect FRED data into the Bronze layer.")
    parser.add_argument("--catalog", type=Path, default=None, help="Path to FRED seed catalog JSON.")
    parser.add_argument("--series", nargs="*", default=None, help="Optional explicit FRED series IDs.")
    parser.add_argument("--priority", nargs="*", default=None, help="Filter by priority, e.g. core.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of catalog series.")
    parser.add_argument("--sleep-seconds", type=float, default=0.25, help="Delay between series requests.")
    parser.add_argument("--include-vintages", action="store_true", help="Collect FRED vintage date metadata.")
    parser.add_argument("--dry-run", action="store_true", help="Show selected series without calling FRED.")
    parser.add_argument("--observation-start", default=None, help="YYYY-MM-DD observation start.")
    parser.add_argument("--observation-end", default=None, help="YYYY-MM-DD observation end.")
    parser.add_argument("--realtime-start", default=None, help="YYYY-MM-DD FRED realtime start.")
    parser.add_argument("--realtime-end", default=None, help="YYYY-MM-DD FRED realtime end.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    settings = get_settings()
    collector = BronzeFredCollector(settings)
    catalog_path = args.catalog or settings.default_catalog_path
    specs = collector.load_catalog(catalog_path)
    selected = select_specs(
        specs,
        series_ids=args.series,
        priorities={p.lower() for p in args.priority} if args.priority else None,
        limit=args.limit,
    )
    summary = collector.collect(
        selected,
        dry_run=args.dry_run,
        include_vintages=args.include_vintages,
        sleep_seconds=args.sleep_seconds,
        observation_start=args.observation_start,
        observation_end=args.observation_end,
        realtime_start=args.realtime_start,
        realtime_end=args.realtime_end,
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
