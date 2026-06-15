from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any
from urllib import error, parse, request


class FredApiError(RuntimeError):
    """Raised when FRED returns an error or an invalid response."""


@dataclass(frozen=True)
class FredResponse:
    endpoint: str
    params: dict[str, Any]
    redacted_url: str
    payload: dict[str, Any]
    attempts: int


class FredClient:
    base_url = "https://api.stlouisfed.org"

    def __init__(
        self,
        api_key: str | None,
        *,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:
        if not api_key:
            raise FredApiError("FRED_API_KEY is missing. Add it to .env first.")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    def series_metadata(self, series_id: str) -> FredResponse:
        return self.get("/fred/series", {"series_id": series_id})

    def series_vintage_dates(self, series_id: str) -> FredResponse:
        return self.get("/fred/series/vintagedates", {"series_id": series_id})

    def series_observations(
        self,
        series_id: str,
        *,
        observation_start: str | None = None,
        observation_end: str | None = None,
        realtime_start: str | None = None,
        realtime_end: str | None = None,
        limit: int = 100000,
    ) -> FredResponse:
        params: dict[str, Any] = {
            "series_id": series_id,
            "limit": limit,
            "sort_order": "asc",
        }
        optional = {
            "observation_start": observation_start,
            "observation_end": observation_end,
            "realtime_start": realtime_start,
            "realtime_end": realtime_end,
        }
        params.update({key: value for key, value in optional.items() if value})
        return self.get("/fred/series/observations", params)

    def get(self, endpoint: str, params: dict[str, Any]) -> FredResponse:
        clean_params = {k: v for k, v in params.items() if v is not None}
        request_params = {
            **clean_params,
            "api_key": self.api_key,
            "file_type": "json",
        }
        url = self._build_url(endpoint, request_params)
        redacted_url = self._build_url(endpoint, {**clean_params, "api_key": "REDACTED", "file_type": "json"})

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                req = request.Request(
                    url,
                    headers={"User-Agent": "causal-lakehouse-bronze/0.1"},
                )
                with request.urlopen(req, timeout=self.timeout_seconds) as response:
                    body = response.read().decode("utf-8")
                payload = json.loads(body)
                if "error_code" in payload or "error_message" in payload:
                    raise FredApiError(str(payload))
                return FredResponse(
                    endpoint=endpoint,
                    params=clean_params,
                    redacted_url=redacted_url,
                    payload=payload,
                    attempts=attempt,
                )
            except (error.URLError, TimeoutError, json.JSONDecodeError, FredApiError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(self.backoff_seconds * attempt)

        raise FredApiError(f"FRED request failed after {self.max_retries} attempts: {last_error}") from last_error

    def _build_url(self, endpoint: str, params: dict[str, Any]) -> str:
        query = parse.urlencode(params)
        return f"{self.base_url}{endpoint}?{query}"
