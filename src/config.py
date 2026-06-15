from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_dotenv(path: Path | None = None) -> None:
    """Load simple KEY=VALUE pairs without requiring python-dotenv."""
    env_path = path or PROJECT_ROOT / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class Settings:
    project_root: Path
    fred_api_key: str | None
    bronze_root: Path
    fred_bronze_root: Path
    default_catalog_path: Path


def get_settings() -> Settings:
    load_dotenv()
    bronze_root = PROJECT_ROOT / "data" / "bronze"
    return Settings(
        project_root=PROJECT_ROOT,
        fred_api_key=os.getenv("FRED_API_KEY"),
        bronze_root=bronze_root,
        fred_bronze_root=bronze_root / "fred",
        default_catalog_path=PROJECT_ROOT / "configs" / "fred_seed_series.json",
    )
