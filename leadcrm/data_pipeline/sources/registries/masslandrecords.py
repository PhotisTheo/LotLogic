"""Adapter for the ACS MassLandRecords portal."""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseRegistrySource, RegistryRecord


def _clean(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    stripped = " ".join(text.split())
    return stripped or None


class MassLandRecordsSource(BaseRegistrySource):
    """
    Handles the legacy ASP.NET search form that powers most MassLandRecords districts.

    The adapter relies on configuration to describe which input names correspond to
    owner vs. address searches so we can reuse it across counties even if field IDs
    differ slightly. The defaults reflect the most common deployment, but each registry
    entry inside config/sources.json can override them.
    """

    DEFAULT_FORM_PATH = "DocumentSearch.aspx"
    DEFAULT_RESULTS_TABLE_ID = "ctl00_cphMain_gvResults"
    DEFAULT_DOCUMENT_LINK_SELECTOR = "a"

    COLUMN_ALIASES = {
        "document_number": ["document number", "doc #", "doc number", "document"],
        "instrument_type": ["document type", "doc type", "type"],
        "recording_date": ["recording date", "rec date", "date"],
        "book": ["book"],
        "page": ["page"],
        "party1": ["party 1", "party1", "grantor", "grantee"],
        "party2": ["party 2", "party2", "grantee"],
    }

    def __init__(self, config, settings):
        super().__init__(config, settings)
        self.logger.setLevel(logging.INFO)

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def search(
        self,
        address: Optional[str],
        owner: Optional[str],
        loc_id: Optional[str],
    ) -> List[RegistryRecord]:
        if not owner and not address:
            raise ValueError("MassLandRecords search requires at least an owner or address.")

        mode = self._select_mode(owner, address)
        html = self._submit_search(mode, owner, address)
        records = self._parse_results(html, address, owner, loc_id)

        if not records:
            self.logger.info("No registry results for owner=%s address=%s", owner, address)
        else:
            self.logger.info("Parsed %s record(s) from MassLandRecords.", len(records))
        return records

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _select_mode(self, owner: Optional[str], address: Optional[str]) -> Dict:
        search_modes = self.config.get("search_modes") or {}
        if not search_modes:
            raise ValueError(
                "MassLandRecords configuration missing 'search_modes'. "
                "Update config/sources.json with owner/address form metadata."
            )
        if owner and "owner" in search_modes:
            return search_modes["owner"]
        if address and "address" in search_modes:
            return search_modes["address"]

        # Fallback to whichever mode is available.
        if owner and search_modes:
            return next(iter(search_modes.values()))
        if address and search_modes:
            return next(iter(search_modes.values()))

        raise ValueError("No usable search mode configured for MassLandRecords.")

    def _submit_search(self, mode: Dict, owner: Optional[str], address: Optional[str]) -> str:
        form_path = mode.get("form_path") or self.config.get("form_path") or self.DEFAULT_FORM_PATH
        form_url = self._build_url(form_path)

        self.logger.info(f"Fetching form from: {form_url}")
        self._throttle()
        response = self.session.get(form_url, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        payload = self._extract_hidden_fields(soup)

        if not payload:
            # Log more details for debugging
            hidden_inputs = soup.select("input[type=hidden]")
            self.logger.warning(f"Found {len(hidden_inputs)} hidden fields total")
            for inp in hidden_inputs[:5]:  # Log first 5 for debugging
                self.logger.warning(f"  - {inp.get('name')}: {inp.get('value', '')[:50]}")
            self.logger.warning("No ASP.NET fields found. This may be a different form type or an error page.")
            # Allow continuing with empty payload for non-ASP.NET forms (like ALIS)
            payload = {}

        payload.update(mode.get("static_fields", {}))

        submit_field = mode.get("submit_field")
        submit_value = mode.get("submit_value", "Search")
        if submit_field:
            payload[submit_field] = submit_value
        else:
            # Fall back to raising the button via EVENTTARGET.
            payload["__EVENTTARGET"] = mode.get("event_target", "")
            payload["__EVENTARGUMENT"] = ""

        for source_key, field_name in (mode.get("fields") or {}).items():
            value = self._resolve_field_value(source_key, owner, address)
            if value:
                payload[field_name] = value

        for field_name, field_value in (mode.get("instrument_filters") or {}).items():
            payload[field_name] = field_value

        method = (mode.get("method") or "POST").upper()
        self._throttle()

        if method == "GET":
            resp = self.session.get(
                form_url,
                params=payload,
                timeout=60,
            )
        else:
            resp = self.session.post(
                form_url,
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=60,
            )
        resp.raise_for_status()
        return resp.text

    def _extract_hidden_fields(self, soup: BeautifulSoup) -> Dict[str, str]:
        data: Dict[str, str] = {}
        # Extract all hidden fields, prioritizing ASP.NET fields
        for hidden in soup.select("input[type=hidden]"):
            name = hidden.get("name")
            value = hidden.get("value", "")
            if name:
                # Include all hidden fields for maximum compatibility
                data[name] = value
        return data

    def _resolve_field_value(self, source_key: str, owner: Optional[str], address: Optional[str]) -> Optional[str]:
        if source_key == "owner":
            return owner
        if source_key in {"owner_last", "owner_first", "owner_middle"}:
            return self._split_owner(owner).get(source_key.split("_")[1])
        if source_key == "address":
            return address
        if source_key == "street_number":
            return self._split_address(address).get("number")
        if source_key == "street_name":
            return self._split_address(address).get("street")
        if source_key == "street_suffix":
            return self._split_address(address).get("suffix")
        return None

    @staticmethod
    def _split_address(address: Optional[str]) -> Dict[str, str]:
        if not address:
            return {}
        match = re.match(r"^\s*(\d+)\s+(.+)$", address)
        if not match:
            return {"street": address.strip()}
        number, remainder = match.groups()
        parts = remainder.split()
        if len(parts) == 1:
            street = parts[0]
            suffix = ""
        else:
            street = " ".join(parts[:-1])
            suffix = parts[-1]
        return {"number": number, "street": street, "suffix": suffix}

    @staticmethod
    def _split_owner(owner: Optional[str]) -> Dict[str, str]:
        if not owner:
            return {}
        parts = owner.replace(",", " ").split()
        if not parts:
            return {}
        if len(parts) == 1:
            return {"last": parts[0]}
        if len(parts) == 2:
            return {"first": parts[0], "last": parts[1]}
        return {"first": parts[0], "middle": " ".join(parts[1:-1]), "last": parts[-1]}

    def _parse_results(
        self,
        html: str,
        address: Optional[str],
        owner: Optional[str],
        loc_id: Optional[str],
    ) -> List[RegistryRecord]:
        soup = BeautifulSoup(html, "html.parser")
        table = self._locate_results_table(soup)
        if not table:
            self.logger.warning("MassLandRecords results table not found.")
            return []

        header_map = self._build_header_map(table)
        if not header_map:
            self.logger.warning("MassLandRecords table header could not be parsed.")
            return []

        records: List[RegistryRecord] = []
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if not cells:
                continue
            record = self._build_record_from_row(
                cells=cells,
                header_map=header_map,
                row=row,
                address=address,
                owner=owner,
                loc_id=loc_id,
            )
            if record:
                records.append(record)
        return records

    def _locate_results_table(self, soup: BeautifulSoup):
        table_id = self.config.get("results_table_id", self.DEFAULT_RESULTS_TABLE_ID)
        table = soup.find("table", id=table_id)
        if table:
            return table
        table_selector = self.config.get("results_table_selector")
        if table_selector:
            return soup.select_one(table_selector)
        return soup.find("table")

    def _build_header_map(self, table) -> Dict[str, int]:
        header_map: Dict[str, int] = {}
        header_cells = table.find_all("th")
        if not header_cells:
            # Some deployments use first row <td> as header
            header_row = table.find("tr")
            if header_row:
                header_cells = header_row.find_all(["td", "th"])

        for idx, cell in enumerate(header_cells):
            key = self._normalize_header(cell.get_text())
            if key:
                header_map[key] = idx
        return header_map

    def _normalize_header(self, text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        return " ".join(text.lower().split())

    def _header_index(self, header_map: Dict[str, int], key: str) -> Optional[int]:
        if key in header_map:
            return header_map[key]
        for alias in self.COLUMN_ALIASES.get(key, []):
            idx = header_map.get(alias)
            if idx is not None:
                return idx
        return None

    def _build_record_from_row(
        self,
        cells,
        header_map: Dict[str, int],
        row,
        address: Optional[str],
        owner: Optional[str],
        loc_id: Optional[str],
    ) -> Optional[RegistryRecord]:
        def cell_text(key: str) -> Optional[str]:
            idx = self._header_index(header_map, key)
            if idx is None or idx >= len(cells):
                return None
            return _clean(cells[idx].get_text())

        metadata: Dict[str, Optional[str]] = {
            "document_number": cell_text("document_number"),
            "book": cell_text("book"),
            "page": cell_text("page"),
            "party1": cell_text("party1"),
            "party2": cell_text("party2"),
        }

        doc_link = self._extract_document_link(row)
        if doc_link:
            metadata["document_url"] = doc_link

        instrument_type = cell_text("instrument_type")
        recording_date = cell_text("recording_date")
        lender = metadata.get("party1")

        instrument_filters = set(filter(None, self.config.get("instrument_types", [])))
        if instrument_filters and instrument_type not in instrument_filters:
            return None

        # Download document if URL available
        doc_path = None
        doc_url = metadata.get("document_url")
        if doc_url:
            try:
                doc_path = self._download_document(doc_url, metadata=metadata)
            except Exception as e:
                self.logger.warning(f"Failed to download document from {doc_url}: {e}")

        return RegistryRecord(
            registry_id=self.config.get("id", "unknown"),
            loc_id=loc_id,
            address=address,
            owner=owner,
            instrument_type=instrument_type or "Unknown",
            document_date=recording_date or "",
            lender=lender,
            amount=None,
            raw_document_path=doc_path,
            raw_metadata={k: v for k, v in metadata.items() if v},
        )

    def _extract_document_link(self, row) -> Optional[str]:
        selector = self.config.get("document_link_selector") or self.DEFAULT_DOCUMENT_LINK_SELECTOR
        link = row.select_one(selector) if selector != self.DEFAULT_DOCUMENT_LINK_SELECTOR else row.find("a")
        if not link:
            return None
        href = link.get("href")
        if not href:
            return None
        return self._build_url(href)

    def _build_url(self, path: str) -> str:
        base_url = self.config.get("base_url") or ""
        return urljoin(base_url if base_url.endswith("/") else base_url + "/", path)
