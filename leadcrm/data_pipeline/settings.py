"""
Central configuration for the free-first data pipeline.

Values default to sane local settings but can be overridden via environment vars
or Django settings. Keeping config in one place makes it easy to tune throttles
without editing scraper code.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass
class PipelineSettings:
    storage_root: Path
    user_agent: str
    registry_throttle_rps: float
    assessor_refresh_days: int
    source_matrix_path: Path

    def load_sources(self) -> Dict[str, Any]:
        """Load registry/municipality metadata from JSON."""
        if not self.source_matrix_path.exists():
            raise FileNotFoundError(
                f"Source matrix not found at {self.source_matrix_path}. "
                "Copy config/sources.sample.json to sources.json and customize."
            )
        with self.source_matrix_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _float_env(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except ValueError:
        return default


def _int_env(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except ValueError:
        return default


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_STORAGE = BASE_DIR / "data" / "raw"
DEFAULT_SOURCE_MATRIX = BASE_DIR / "data_pipeline" / "config" / "sources.json"

pipeline_settings = PipelineSettings(
    storage_root=Path(_env("DATA_PIPELINE_STORAGE_ROOT", str(DEFAULT_STORAGE))),
    user_agent=_env("SCRAPER_USER_AGENT", "LeadCRM-FreePipeline/0.1"),
    registry_throttle_rps=_float_env("REGISTRY_THROTTLE_RPS", 0.5),
    assessor_refresh_days=_int_env("ASSESSOR_REFRESH_DAYS", 365),
    source_matrix_path=Path(_env("SOURCE_MATRIX_PATH", str(DEFAULT_SOURCE_MATRIX))),
)

# Ensure the storage path exists locally (no-op if already created).
pipeline_settings.storage_root.mkdir(parents=True, exist_ok=True)
