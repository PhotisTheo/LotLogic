"""Common helpers for registry scrapers."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

import requests

from ...settings import PipelineSettings


@dataclass
class RegistryRecord:
    """Normalized mortgage/foreclosure row."""

    registry_id: str
    loc_id: Optional[str]
    address: Optional[str]
    owner: Optional[str]
    instrument_type: str
    document_date: str
    lender: Optional[str]
    amount: Optional[float]
    raw_document_path: Optional[str]
    document_text: Optional[str] = None
    raw_metadata: Dict[str, Any] = field(default_factory=dict)


class BaseRegistrySource:
    """Handles throttling + session housekeeping for registry scrapers."""

    def __init__(self, config: Dict[str, Any], settings: PipelineSettings):
        self.config = config
        self.settings = settings
        self.logger = logging.getLogger(f"registry.{config.get('id', 'unknown')}")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": settings.user_agent})
        self._last_request_ts = 0.0

    def _throttle(self):
        """Sleep to respect per-registry rate limits."""
        rps = self.config.get("throttle_rps", self.settings.registry_throttle_rps)
        wait = max(0, (1 / rps) - (time.time() - self._last_request_ts))
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.time()

    # --- Methods for subclasses to override ---
    def search(self, address: Optional[str], owner: Optional[str], loc_id: Optional[str]) -> List[RegistryRecord]:
        raise NotImplementedError

    def _download_document(self, url: str, suffix: str = ".pdf", metadata: Optional[Dict[str, Any]] = None) -> str:
        """Download document to storage, return path."""
        self._throttle()
        if not url.lower().startswith("http"):
            base = self.config.get("base_url", "")
            url = base.rstrip("/") + "/" + url.lstrip("/")
        self.logger.info(f"Downloading document from: {url}")
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        # Detect file type from content-type header if suffix not specified
        if suffix == ".pdf":
            content_type = resp.headers.get("content-type", "").lower()
            if "tiff" in content_type or "tif" in content_type:
                suffix = ".tiff"
            elif "image" in content_type:
                suffix = ".tiff"  # Assume TIFF for generic images from registries

        storage_dir = self.settings.storage_root / "registries" / self.config["id"]
        storage_dir.mkdir(parents=True, exist_ok=True)

        # Create filename with timestamp and metadata
        timestamp = int(time.time() * 1000)
        doc_id = metadata.get("document_number", "unknown") if metadata else "unknown"
        filename = f"doc_{doc_id}_{timestamp}{suffix}"
        path = storage_dir / filename

        # Write the document
        path.write_bytes(resp.content)
        self.logger.info(f"Saved document to: {path}")
        return str(path)
