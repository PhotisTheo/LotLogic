"""Vision Government Solutions assessor adapter (skeleton)."""

from __future__ import annotations

from typing import List, Optional

from .base import BaseAssessorSource, AssessorRecord


class VisionAssessorSource(BaseAssessorSource):
    """
    Placeholder adapter for Vision portals.
    Implementation will scrape property cards or download CSV exports where available.
    """

    def fetch(self, parcel_id: Optional[str], address: Optional[str]) -> List[AssessorRecord]:
        self.logger.info(
            "Vision fetch stub called for muni=%s parcel_id=%s address=%s",
            self.config.get("muni_code"),
            parcel_id,
            address,
        )
        # TODO: Implement actual HTTP requests + parsing.
        return []
