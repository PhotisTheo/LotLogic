"""
Service module for fetching liens and legal actions from public sources.

Sources:
- CourtListener API (Federal cases, bankruptcies)
- MA Trial Court (State court cases)
- Municipal tax lien lists (town websites)
"""

import logging
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from django.conf import settings

logger = logging.getLogger(__name__)


# ============================================================================
# CourtListener API Integration (Federal Cases & Bankruptcies)
# ============================================================================

COURTLISTENER_API_BASE = "https://www.courtlistener.com/api/rest/v4"
COURTLISTENER_API_KEY = getattr(settings, "COURTLISTENER_API_KEY", None)


class CourtListenerError(Exception):
    """Exception raised for CourtListener API errors"""
    pass


def search_courtlistener_by_name(
    name: str,
    state: str = "MA",
    limit: int = 20
) -> List[Dict]:
    """
    Search CourtListener for cases involving a person/entity name.

    Uses the v4 dockets API to search for cases by party name.
    Focuses on MA federal courts and bankruptcy courts.

    Args:
        name: Full name to search (e.g., "John Smith")
        state: State abbreviation (default: MA)
        limit: Maximum results to return

    Returns:
        List of case dictionaries with normalized fields

    Example result:
        {
            'source': 'CourtListener',
            'case_number': '1:20-bk-10001',
            'court': 'US Bankruptcy Court (MA)',
            'action_type': 'bankruptcy_ch7',
            'status': 'closed',
            'filing_date': '2020-01-15',
            'plaintiff': 'In re: John Smith',
            'defendant': '',
            'description': 'Chapter 7 Bankruptcy',
            'source_url': 'https://www.courtlistener.com/docket/...',
        }
    """
    if not COURTLISTENER_API_KEY:
        logger.warning("CourtListener API key not configured")
        return []

    try:
        # Search for dockets by party name using v4 API
        # Focus on MA federal courts (district and bankruptcy)
        url = f"{COURTLISTENER_API_BASE}/dockets/"

        headers = {
            "Authorization": f"Token {COURTLISTENER_API_KEY}",
        }

        results = []

        # Search MA bankruptcy courts (most relevant for lien/foreclosure searches)
        ma_bankruptcy_courts = ["mab"]  # MA Bankruptcy Court

        for court_id in ma_bankruptcy_courts:
            params = {
                "court": court_id,
                "parties__name__icontains": name,  # Search party names
                "page_size": limit,
            }

            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            for docket in data.get("results", [])[:limit]:
                normalized = _normalize_courtlistener_docket(docket)
                if normalized:
                    results.append(normalized)

        # Also search MA federal district court
        ma_district_court = "mad"  # MA District Court
        params = {
            "court": ma_district_court,
            "parties__name__icontains": name,
            "page_size": limit,
        }

        response = requests.get(url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        for docket in data.get("results", [])[:limit]:
            normalized = _normalize_courtlistener_docket(docket)
            if normalized:
                results.append(normalized)

        return results[:limit]  # Limit total results

    except requests.exceptions.RequestException as e:
        logger.error(f"CourtListener API error: {e}")
        # Don't raise - just return empty list to avoid blocking other sources
        return []


def _normalize_courtlistener_docket(docket: Dict) -> Optional[Dict]:
    """
    Convert CourtListener v4 docket API result to our standardized format.

    Args:
        docket: Docket object from CourtListener v4 API

    Returns:
        Normalized case dictionary or None if parsing fails
    """
    try:
        # Get basic docket info
        docket_number = docket.get("docket_number", "")
        case_name = docket.get("case_name", "")
        court = docket.get("court", "")
        date_filed = docket.get("date_filed", "")

        # Determine action type from court and case name
        action_type = "federal_civil"
        court_type = "federal_district_ma"

        # Map court ID to our court types
        if court == "mab":  # MA Bankruptcy Court
            court_type = "federal_bankruptcy_ma"
            # Determine bankruptcy chapter from case name
            case_name_lower = case_name.lower()
            if "chapter 7" in case_name_lower or "ch 7" in case_name_lower or "ch. 7" in case_name_lower:
                action_type = "bankruptcy_ch7"
            elif "chapter 11" in case_name_lower or "ch 11" in case_name_lower or "ch. 11" in case_name_lower:
                action_type = "bankruptcy_ch11"
            elif "chapter 13" in case_name_lower or "ch 13" in case_name_lower or "ch. 13" in case_name_lower:
                action_type = "bankruptcy_ch13"
            else:
                action_type = "bankruptcy_other"
        elif court == "mad":  # MA District Court
            court_type = "federal_district_ma"
            if "criminal" in case_name.lower():
                action_type = "federal_criminal"
            else:
                action_type = "federal_civil"

        # Parse parties
        plaintiff = ""
        defendant = ""
        if " v. " in case_name or " v " in case_name:
            parts = case_name.replace(" v. ", " v ").split(" v ", 1)
            if len(parts) == 2:
                plaintiff = parts[0].strip()
                defendant = parts[1].strip()
        elif "In re:" in case_name or "In Re:" in case_name:
            # Bankruptcy case format
            plaintiff = case_name
        else:
            plaintiff = case_name

        # Get docket ID for URL
        docket_id = docket.get("id", "")
        absolute_url = docket.get("absolute_url", "")
        if not absolute_url and docket_id:
            absolute_url = f"/docket/{docket_id}/"

        # Map status
        status = _map_courtlistener_status(docket.get("date_terminated", ""))

        # Build description from available fields
        cause = docket.get("cause", "")
        nature_of_suit = docket.get("nature_of_suit", "")
        summary = docket.get("summary", "")

        description_parts = [case_name]
        if cause:
            description_parts.append(f"Cause: {cause}")
        if nature_of_suit:
            description_parts.append(f"Nature: {nature_of_suit}")
        if summary:
            description_parts.append(summary)

        description = " | ".join(description_parts)

        return {
            "source": "CourtListener",
            "case_number": docket_number,
            "court": court_type,
            "action_type": action_type,
            "status": status,
            "filing_date": date_filed,
            "plaintiff": plaintiff,
            "defendant": defendant,
            "description": description,
            "source_url": f"https://www.courtlistener.com{absolute_url}",
            "pacer_case_id": docket.get("pacer_case_id", ""),
        }
    except Exception as e:
        logger.warning(f"Failed to normalize CourtListener docket: {e}")
        return None


def _map_courtlistener_status(date_terminated: Optional[str]) -> str:
    """
    Map CourtListener case status to our status values.

    Args:
        date_terminated: Date the case was terminated (empty string or None if active)

    Returns:
        Status string: "closed", "active", or "pending"
    """
    # If there's a termination date, the case is closed
    if date_terminated:
        return "closed"
    else:
        # Case is still active/pending
        return "pending"


# ============================================================================
# MA Trial Court Integration
# ============================================================================

def search_ma_trial_court(
    name: str,
    court_type: Optional[str] = None
) -> List[Dict]:
    """
    Search MA Trial Court public records for cases.

    NOTE: This is a placeholder for manual lookup guidance.
    MA Trial Court eAccess requires authentication and has TOS restrictions.

    Args:
        name: Party name to search
        court_type: Specific court (housing, district, superior, land)

    Returns:
        List of case dictionaries (currently returns guidance for manual lookup)
    """
    # MA Trial Court does not provide a public API
    # Users must search manually via https://www.masscourts.org/eservices/home.page.9

    return [
        {
            "source": "MA Trial Court (Manual Lookup Required)",
            "case_number": "MANUAL_LOOKUP",
            "court": "ma_district",
            "action_type": "state_civil",
            "status": "pending",
            "filing_date": None,
            "plaintiff": "",
            "defendant": name,
            "description": f"Please search MA Trial Court eAccess for: {name}",
            "source_url": "https://www.masscourts.org/eservices/home.page.9",
            "notes": (
                "MA Trial Court requires manual search:\n"
                "1. Visit https://www.masscourts.org/eservices/home.page.9\n"
                "2. Search by party name\n"
                "3. Review Housing, District, Superior, and Land Court records\n"
                "4. Manually enter any findings into this system"
            ),
        }
    ]


# ============================================================================
# Municipal Tax Lien Integration
# ============================================================================

def search_municipal_tax_liens(
    town_name: str,
    address: str,
    owner_name: str
) -> List[Dict]:
    """
    Search for municipal tax liens on town websites.

    Many MA towns publish tax title/delinquent lists as PDFs or CSVs.
    This function provides guidance for manual lookup.

    Args:
        town_name: Name of the town (e.g., "Salem")
        address: Property address
        owner_name: Property owner name

    Returns:
        List of lien dictionaries (currently returns guidance)
    """
    # Most towns publish tax lien lists but in different formats
    # Common patterns:
    # - PDF: "Tax Title List" or "Delinquent Tax List"
    # - CSV: Downloadable spreadsheet
    # - Online search: Collector's website

    town_lower = town_name.lower().replace(" ", "")

    # Common URL patterns
    possible_urls = [
        f"https://www.{town_lower}ma.gov/treasurer",
        f"https://www.{town_lower}ma.gov/collector",
        f"https://www.{town_lower}.ma.us/treasurer",
        f"https://www.{town_lower}.ma.us/collector",
    ]

    return [
        {
            "source": f"{town_name} Tax Collector (Manual Lookup)",
            "lien_type": "tax_municipal",
            "lien_holder": f"Town of {town_name}",
            "status": "active",
            "amount": None,
            "recording_date": None,
            "notes": (
                f"Check these sources for {town_name} tax liens:\n\n"
                f"1. Town website collector/treasurer pages:\n" +
                "\n".join(f"   - {url}" for url in possible_urls) + "\n\n"
                f"2. Search for 'Tax Title List' or 'Delinquent Tax List' PDFs\n"
                f"3. Look for property at: {address}\n"
                f"4. Owner name: {owner_name}\n\n"
                f"If not available online, file a Public Records Request under M.G.L. c. 66, § 10"
            ),
            "source_url": possible_urls[0],
        }
    ]


# ============================================================================
# Registry of Deeds Integration (MassLandRecords)
# ============================================================================

def search_registry_liens(
    county: str,
    book: Optional[str] = None,
    page: Optional[str] = None,
    owner_name: Optional[str] = None
) -> List[Dict]:
    """
    Search Registry of Deeds for recorded liens.

    MassLandRecords.com provides access but requires subscription.
    This provides guidance for manual lookup.

    Args:
        county: County name (e.g., "Essex")
        book: Optional book number
        page: Optional page number
        owner_name: Optional owner name to search

    Returns:
        List of lien dictionaries (guidance for manual lookup)
    """
    base_url = "https://masslandrecords.com"

    guidance = {
        "source": f"{county} County Registry of Deeds",
        "lien_type": "mortgage",
        "status": "active",
        "notes": (
            f"Search {county} County Registry of Deeds:\n\n"
            f"1. Visit: {base_url}\n"
            f"2. Select {county} County\n"
            f"3. Search by:\n"
        ),
        "source_url": base_url,
    }

    if owner_name:
        guidance["notes"] += f"   - Owner name: {owner_name}\n"
    if book and page:
        guidance["notes"] += f"   - Document: Book {book}, Page {page}\n"

    guidance["notes"] += (
        "\n4. Look for:\n"
        "   - Mortgages\n"
        "   - Federal/State tax liens\n"
        "   - Mechanics liens\n"
        "   - Judgment liens\n"
        "   - UCC filings\n"
        "\n5. Record findings in this system"
    )

    return [guidance]


# ============================================================================
# UCC Filing Search (Secretary of Commonwealth)
# ============================================================================

def search_ucc_filings(
    entity_name: str
) -> List[Dict]:
    """
    Search for UCC filings (personal property liens).

    MA Secretary of Commonwealth maintains UCC database.
    Requires manual search via their portal.

    Args:
        entity_name: Business or individual name

    Returns:
        List of UCC filing dictionaries (guidance)
    """
    return [
        {
            "source": "MA Secretary of Commonwealth - UCC Search",
            "lien_type": "ucc",
            "lien_holder": "Unknown",
            "status": "active",
            "notes": (
                f"Search UCC filings for: {entity_name}\n\n"
                "1. Visit: https://www.sec.state.ma.us/cor/corucc/uccidx.htm\n"
                "2. Click 'UCC Search'\n"
                "3. Search by debtor name\n"
                "4. Review active UCC-1 financing statements\n"
                "5. Note filing numbers and secured parties\n"
                "6. Record findings here with:\n"
                "   - Secured party (lien holder)\n"
                "   - Filing date\n"
                "   - Filing number\n"
                "   - Collateral description"
            ),
            "source_url": "https://www.sec.state.ma.us/cor/corucc/uccidx.htm",
        }
    ]


# ============================================================================
# Unified Search Function
# ============================================================================

def search_all_sources(
    owner_name: str,
    address: Optional[str] = None,
    town_name: Optional[str] = None,
    county: Optional[str] = None
) -> Tuple[List[Dict], List[Dict]]:
    """
    Search all available sources for liens and legal actions.

    Args:
        owner_name: Property owner name
        address: Property address (for tax lien search)
        town_name: Town name (for tax lien search)
        county: County name (for registry search)

    Returns:
        Tuple of (legal_actions, liens) - lists of dictionaries
    """
    legal_actions = []
    liens = []

    # Search CourtListener for federal cases
    try:
        courtlistener_results = search_courtlistener_by_name(owner_name)
        legal_actions.extend(courtlistener_results)
    except Exception as e:
        logger.error(f"CourtListener search failed: {e}")

    # Add MA Trial Court guidance
    legal_actions.extend(search_ma_trial_court(owner_name))

    # Search for municipal tax liens
    if town_name and address:
        liens.extend(search_municipal_tax_liens(town_name, address, owner_name))

    # Add Registry of Deeds guidance
    if county:
        liens.extend(search_registry_liens(county, owner_name=owner_name))

    # Add UCC filing guidance
    liens.extend(search_ucc_filings(owner_name))

    return legal_actions, liens
