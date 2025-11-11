"""Massachusetts Secretary of the Commonwealth corporate filing scraper."""

from __future__ import annotations

import re
from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .base import BaseCorporateSource, CorporateRecord


class MASecretarySource(BaseCorporateSource):
    """
    Scrapes corporate entity data from Massachusetts Secretary of the Commonwealth.

    URL: https://corp.sec.state.ma.us/CorpWeb/CorpSearch/CorpSearch.aspx

    This scraper handles:
    - LLC ownership lookups
    - Registered agent information
    - Business addresses and contact info
    - Formation dates and filing status
    """

    BASE_URL = "https://corp.sec.state.ma.us/CorpWeb"
    SEARCH_PATH = "/CorpSearch/CorpSearch.aspx"
    DETAIL_PATH = "/CorpSearch/CorpSummary.aspx"

    def search(self, entity_name: str) -> List[CorporateRecord]:
        """
        Search for corporations/LLCs by name.

        Args:
            entity_name: Business name to search (e.g., "ABC Properties LLC")

        Returns:
            List of CorporateRecord objects matching the search
        """
        self.logger.info(f"Searching for entity: {entity_name}")

        # Clean up entity name for search
        search_term = self._clean_entity_name(entity_name)

        try:
            # Step 1: Get the search form
            search_url = urljoin(self.BASE_URL, self.SEARCH_PATH)
            self._throttle()
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            viewstate = self._extract_viewstate(soup)

            if not viewstate:
                self.logger.warning("Could not extract VIEWSTATE from search form")
                return []

            # Step 2: Submit search
            self._throttle()
            search_data = {
                **viewstate,
                "ctl00$MainContent$txtEntityName": search_term,
                "ctl00$MainContent$btnSearch": "Search",
                "ctl00$MainContent$ddlEntityType": "ALL",  # Search all entity types
            }

            response = self.session.post(
                search_url,
                data=search_data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30
            )
            response.raise_for_status()

            # Step 3: Parse results
            results = self._parse_search_results(response.text)

            # Step 4: Fetch details for each result
            detailed_records = []
            for record in results[:5]:  # Limit to top 5 results
                if record.entity_id:
                    detailed = self.get_entity_details(record.entity_id)
                    if detailed:
                        detailed_records.append(detailed)
                    else:
                        detailed_records.append(record)
                else:
                    detailed_records.append(record)

            self.logger.info(f"Found {len(detailed_records)} entities for '{entity_name}'")
            return detailed_records

        except Exception as e:
            self.logger.error(f"Error searching for entity '{entity_name}': {e}")
            return []

    def get_entity_details(self, entity_id: str) -> Optional[CorporateRecord]:
        """
        Get detailed information for a specific entity.

        Args:
            entity_id: State entity ID number

        Returns:
            CorporateRecord with full details or None if not found
        """
        self.logger.info(f"Fetching details for entity ID: {entity_id}")

        try:
            detail_url = urljoin(self.BASE_URL, self.DETAIL_PATH)
            self._throttle()

            params = {"FEIN": entity_id}
            response = self.session.get(detail_url, params=params, timeout=30)
            response.raise_for_status()

            return self._parse_entity_details(response.text, entity_id)

        except Exception as e:
            self.logger.error(f"Error fetching details for entity {entity_id}: {e}")
            return None

    def _clean_entity_name(self, name: str) -> str:
        """Clean entity name for search."""
        # Remove common suffixes for better matching
        name = re.sub(r'\s+(LLC|L\.L\.C\.|Inc\.|Corporation|Corp\.)$', '', name, flags=re.IGNORECASE)
        return name.strip()

    def _extract_viewstate(self, soup: BeautifulSoup) -> dict:
        """Extract ASP.NET viewstate and other hidden fields."""
        data = {}
        for hidden in soup.select("input[type=hidden]"):
            name = hidden.get("name")
            value = hidden.get("value", "")
            if name:
                data[name] = value
        return data

    def _parse_search_results(self, html: str) -> List[CorporateRecord]:
        """Parse the search results page."""
        soup = BeautifulSoup(html, "html.parser")
        records = []

        # Find the results table (usually has ID like GridView1)
        results_table = soup.find("table", {"class": "Grid"}) or soup.find("table", id=re.compile(".*GridView.*"))

        if not results_table:
            self.logger.warning("No results table found")
            return []

        # Parse each row
        rows = results_table.find_all("tr")[1:]  # Skip header
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            # Extract entity ID from link
            link = row.find("a")
            entity_id = None
            if link and link.get("href"):
                match = re.search(r'FEIN=([^&]+)', link.get("href"))
                if match:
                    entity_id = match.group(1)

            # Parse row data
            entity_name = cells[0].get_text(strip=True) if len(cells) > 0 else ""
            entity_type = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            status = cells[2].get_text(strip=True) if len(cells) > 2 else ""

            if entity_name:
                record = CorporateRecord(
                    entity_name=entity_name,
                    entity_id=entity_id or "",
                    entity_type=entity_type,
                    status=status,
                    source_url=f"{self.BASE_URL}{self.DETAIL_PATH}?FEIN={entity_id}" if entity_id else None
                )
                records.append(record)

        return records

    def _parse_entity_details(self, html: str, entity_id: str) -> Optional[CorporateRecord]:
        """Parse the entity detail page."""
        soup = BeautifulSoup(html, "html.parser")

        # Extract all label-value pairs
        data = {}
        for label in soup.find_all("span", {"class": "LabelText"}):
            label_text = label.get_text(strip=True).replace(":", "")
            value_span = label.find_next_sibling("span")
            if value_span:
                data[label_text] = value_span.get_text(strip=True)

        # Alternative: look for table rows
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).replace(":", "")
                value = cells[1].get_text(strip=True)
                data[key] = value

        # Extract key fields
        entity_name = data.get("Entity Name") or data.get("Business Name") or ""
        entity_type = data.get("Entity Type") or data.get("Type") or ""
        status = data.get("Status") or data.get("Entity Status") or ""

        # Extract principal/manager info
        principal_title = None
        principal_name = None

        for title in ["President", "Manager", "Principal", "Managing Member"]:
            if data.get(title):
                principal_name = data[title]
                principal_title = title
                break

        # Registered agent
        registered_agent = data.get("Registered Agent") or data.get("Agent Name") or None

        # Business address
        business_address = (
            data.get("Business Address") or
            data.get("Principal Address") or
            data.get("Principal Office Address") or
            None
        )

        # Dates
        formation_date = (
            data.get("Date of Organization") or
            data.get("Formation Date") or
            data.get("Date of Incorporation") or
            None
        )

        last_annual_report = data.get("Last Annual Report") or data.get("Last Report Date") or None

        # Phone (might be in agent or business info)
        business_phone = data.get("Phone") or data.get("Telephone") or None

        if not entity_name:
            self.logger.warning(f"Could not extract entity name for ID {entity_id}")
            return None

        record = CorporateRecord(
            entity_name=entity_name,
            entity_id=entity_id,
            entity_type=entity_type,
            status=status,
            principal_name=principal_name,
            principal_title=principal_title,
            registered_agent=registered_agent,
            business_address=business_address,
            business_phone=business_phone,
            formation_date=formation_date,
            last_annual_report=last_annual_report,
            source_url=f"{self.BASE_URL}{self.DETAIL_PATH}?FEIN={entity_id}",
            raw_data=data
        )

        return record
