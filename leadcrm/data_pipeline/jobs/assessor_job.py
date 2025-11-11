"""
Assessor ingestion job orchestrator.

Loads municipality configuration, spins up the appropriate assessor adapter,
and persists normalized tax assessment rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..sources.assessors.vision import VisionAssessorSource
from ..storage import database
from ..settings import pipeline_settings


ASSESSOR_ADAPTERS = {
    "vision": VisionAssessorSource,
    # Future: "patriot": PatriotAssessorSource, etc.
}


@dataclass
class AssessorJob:
    config: Dict[str, Any]

    def _build_adapter(self):
        adapter_key = self.config.get("platform")
        adapter_cls = ASSESSOR_ADAPTERS.get(adapter_key)
        if not adapter_cls:
            raise ValueError(f"No assessor adapter registered for '{adapter_key}'")
        return adapter_cls(self.config, pipeline_settings)

    def run(
        self,
        parcel_id: Optional[str] = None,
        address: Optional[str] = None,
        dry_run: bool = False,
    ) -> None:
        adapter = self._build_adapter()
        records = adapter.fetch(parcel_id=parcel_id, address=address)

        if not records:
            adapter.logger.info("No assessor data returned.")
            return

        adapter.logger.info("Fetched %s assessor row(s)", len(records))
        if dry_run:
            return

        for record in records:
            database.save_assessor_record(record)
