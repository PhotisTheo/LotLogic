"""Shared logic for assessor scrapers."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

from ...settings import PipelineSettings


@dataclass
class AssessorRecord:
    """Normalized tax assessment row."""

    municipality_code: str
    parcel_id: str
    tax_year: int
    assessed_total: float
    assessed_land: Optional[float] = None
    assessed_building: Optional[float] = None
    tax_amount: Optional[float] = None
    source_url: Optional[str] = None
    raw_payload: Dict[str, Any] = field(default_factory=dict)


class BaseAssessorSource:
    """Handles HTTP session + throttling applied per municipality/platform."""

    def __init__(self, config: Dict[str, Any], settings: PipelineSettings):
        self.config = config
        self.settings = settings
        self.logger = logging.getLogger(f"assessor.{config.get('municipality', 'unknown')}")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})
        self._last_request_ts = 0.0

    def _throttle(self):
        rps = self.config.get("throttle_rps", 1.0)
        wait = max(0, (1 / rps) - (time.time() - self._last_request_ts))
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.time()

    def fetch(self, parcel_id: Optional[str], address: Optional[str]) -> List[AssessorRecord]:
        raise NotImplementedError
