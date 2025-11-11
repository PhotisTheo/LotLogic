"""Base class for corporate entity scrapers."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests

from ...settings import PipelineSettings


@dataclass
class CorporateRecord:
    """Normalized corporate entity information."""

    entity_name: str
    entity_id: str  # State filing ID
    entity_type: str  # LLC, Corp, LLP, etc.
    status: str  # Active, Dissolved, etc.

    # Owner/Manager information
    principal_name: Optional[str] = None
    principal_title: Optional[str] = None  # e.g., "Managing Member", "President"
    registered_agent: Optional[str] = None

    # Contact information
    business_phone: Optional[str] = None
    business_email: Optional[str] = None
    business_address: Optional[str] = None

    # Filing dates
    formation_date: Optional[str] = None
    last_annual_report: Optional[str] = None

    # Source tracking
    source_url: Optional[str] = None
    raw_data: Dict[str, Any] = field(default_factory=dict)


class BaseCorporateSource:
    """Base class for corporate filing scrapers."""

    def __init__(self, config: Dict[str, Any], settings: PipelineSettings):
        self.config = config
        self.settings = settings
        self.logger = logging.getLogger(f"corporate.{config.get('id', 'unknown')}")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})
        self._last_request_ts = 0.0

    def _throttle(self):
        """Sleep to respect rate limits."""
        rps = self.config.get("throttle_rps", 0.5)
        wait = max(0, (1 / rps) - (time.time() - self._last_request_ts))
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.time()

    def search(self, entity_name: str) -> List[CorporateRecord]:
        """Search for corporate entities by name."""
        raise NotImplementedError

    def get_entity_details(self, entity_id: str) -> Optional[CorporateRecord]:
        """Get detailed information for a specific entity."""
        raise NotImplementedError
