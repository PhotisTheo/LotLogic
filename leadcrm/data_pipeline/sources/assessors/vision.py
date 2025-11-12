"""Vision Government Solutions assessor adapter."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import requests

from .base import BaseAssessorSource, AssessorRecord


class VisionAssessorSource(BaseAssessorSource):
    """
    Adapter that consumes Vision property cards or open-data exports.

    Boston publishes its assessment roll via CKAN, which we access using the
    `datastore_search` API (resource_id provided in config). Other Vision towns
    can point to CSV downloads or HTML portals.
    """

    def fetch(self, parcel_id: Optional[str], address: Optional[str]) -> List[AssessorRecord]:
        resource_id = self.config.get("resource_id")
        download_url = self.config.get("download_url")

        if resource_id:
            return self._fetch_from_ckan(resource_id, parcel_id=parcel_id)
        if download_url:
            return self._fetch_from_csv(download_url, parcel_id=parcel_id)

        self.logger.warning("No resource configured for Vision assessor source %s", self.config.get("muni_code"))
        return []

    # ------------------------------------------------------------------ #
    # CKAN API ingestion (Boston, etc.)
    # ------------------------------------------------------------------ #
    def _fetch_from_ckan(self, resource_id: str, parcel_id: Optional[str]) -> List[AssessorRecord]:
        params = {"resource_id": resource_id, "limit": 1000}
        if parcel_id:
            params["filters"] = {"PID": parcel_id}

        records: List[AssessorRecord] = []
        offset = 0

        while True:
            params["offset"] = offset
            response = self.session.get(
                "https://data.boston.gov/api/3/action/datastore_search",
                params=params,
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            result = payload.get("result", {})
            raw_records = result.get("records", [])

            for raw in raw_records:
                record = self._normalize_ckan_record(raw)
                if record:
                    records.append(record)

            if parcel_id or len(raw_records) < params["limit"]:
                break

            offset += params["limit"]

        return records

    def _normalize_ckan_record(self, raw: Dict[str, str]) -> Optional[AssessorRecord]:
        try:
            assessed_total = float(raw.get("AV_TOTAL", "0").replace(",", ""))
        except ValueError:
            assessed_total = 0.0

        if assessed_total <= 0:
            return None

        assessed_land = _safe_float(raw.get("AV_LAND"))
        assessed_building = _safe_float(raw.get("AV_BLDG"))
        tax_amount = _safe_float(raw.get("GROSS_TAX"))
        parcel_id = raw.get("PID") or raw.get("GIS_ID")

        return AssessorRecord(
            municipality_code=self.config.get("muni_code", ""),
            parcel_id=parcel_id or "",
            tax_year=int(self.config.get("tax_year", 0) or 0),
            assessed_total=assessed_total,
            assessed_land=assessed_land,
            assessed_building=assessed_building,
            tax_amount=tax_amount,
            source_url=self.config.get("source_url"),
            raw_payload=raw,
        )

    # ------------------------------------------------------------------ #
    # CSV ingestion placeholder
    # ------------------------------------------------------------------ #
    def _fetch_from_csv(self, url: str, parcel_id: Optional[str]) -> List[AssessorRecord]:
        self.logger.warning("CSV ingestion not implemented yet for Vision sources (%s).", url)
        return []


def _safe_float(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value.replace(",", ""))
    except ValueError:
        return None
