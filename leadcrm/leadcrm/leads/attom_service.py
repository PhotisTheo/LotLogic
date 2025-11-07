
import requests
from typing import Optional
from decimal import Decimal, InvalidOperation
from django.conf import settings
from .models import AttomData, SavedParcelList
from .services import get_massgis_parcel_detail, _normalize_loc_id


def _safe_decimal(value, default=None):
    """Safely convert a value to Decimal, returning default if conversion fails."""
    if value is None:
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def _safe_int(value, default=None):
    """Safely convert a value to int, returning default if conversion fails."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _extract_foreclosure_details(property_data: dict) -> dict:
    """Extract detailed foreclosure information from ATTOM API response."""
    foreclosure_data = property_data.get("foreclosure", {})

    details = {
        "foreclosure_recording_date": foreclosure_data.get("recordingDate"),
        "foreclosure_auction_date": foreclosure_data.get("auctionDate"),
        "foreclosure_estimated_value": _safe_decimal(foreclosure_data.get("estimatedValue")),
        "foreclosure_judgment_amount": _safe_decimal(foreclosure_data.get("judgmentAmount")),
        "foreclosure_default_amount": _safe_decimal(foreclosure_data.get("defaultAmount")),
        "foreclosure_stage": foreclosure_data.get("stage") or foreclosure_data.get("foreclosureStatus"),
        "foreclosure_document_type": foreclosure_data.get("documentType"),
    }

    return details


def _extract_mortgage_details(property_data: dict) -> dict:
    """Extract detailed mortgage information from ATTOM API response."""
    # Try to get mortgage from assessment section first (expandedprofile endpoint)
    assessment = property_data.get("assessment", {})
    mortgage_raw = assessment.get("mortgage", {})

    # If not in assessment, try direct mortgage field (other endpoints)
    if not mortgage_raw:
        mortgage_raw = property_data.get("mortgage", {})

    # Handle both dict and list formats
    if isinstance(mortgage_raw, list):
        mortgage_data = mortgage_raw[0] if mortgage_raw else {}
    elif isinstance(mortgage_raw, dict):
        # For expandedprofile, mortgage data is in FirstConcurrent
        if "FirstConcurrent" in mortgage_raw:
            mortgage_data = mortgage_raw.get("FirstConcurrent", {})
        else:
            mortgage_data = mortgage_raw
    else:
        mortgage_data = {}

    # Extract lender name from nested structure
    lender_info = mortgage_data.get("lender", {})
    lender_name = (
        lender_info.get("lastname") or
        lender_info.get("companyname") or
        mortgage_data.get("lenderName") or
        mortgage_data.get("lenderNameBeneficiary") or
        mortgage_data.get("lenderLastName")  # expandedprofile format
    )

    # Extract term and convert to years if needed
    # ATTOM may return term in months (e.g., 360) or years (e.g., 30)
    term_raw = mortgage_data.get("termYears") or mortgage_data.get("term")
    term_years = None
    if term_raw is not None:
        term_int = _safe_int(term_raw)
        if term_int:
            # If term is > 100, it's likely in months, convert to years
            if term_int > 100:
                term_years = term_int // 12  # Convert months to years
            else:
                term_years = term_int

    details = {
        "mortgage_loan_amount": _safe_decimal(mortgage_data.get("amount")),
        "mortgage_loan_type": mortgage_data.get("loantypecode") or mortgage_data.get("loanTypeCode") or mortgage_data.get("loanType") or mortgage_data.get("mortgageType"),
        "mortgage_lender_name": lender_name,
        "mortgage_interest_rate": _safe_decimal(mortgage_data.get("interestRate") or mortgage_data.get("interestrate")),
        "mortgage_term_years": term_years,
        "mortgage_recording_date": mortgage_data.get("date") or mortgage_data.get("recordingDate"),
        "mortgage_due_date": mortgage_data.get("duedate") or mortgage_data.get("dueDate") or mortgage_data.get("maturityDate"),
        "mortgage_loan_number": mortgage_data.get("loanNumber") or mortgage_data.get("trustDeedDocumentNumber"),
    }

    return details


def _extract_tax_details(property_data: dict) -> dict:
    """Extract detailed tax information from ATTOM API response."""
    # Tax info can be in different places depending on the endpoint
    assessment_data = property_data.get("assessment", {})
    tax_data = assessment_data.get("tax", {}) or property_data.get("tax", {})

    # Get assessed value from nested structure
    assessed_info = assessment_data.get("assessed", {})
    assessed_value = (
        _safe_decimal(assessed_info.get("assdTtlValue")) or  # expandedprofile format
        _safe_decimal(assessed_info.get("assdttlvalue")) or
        _safe_decimal(assessment_data.get("assessedValue")) or
        _safe_decimal(tax_data.get("assessedValue"))
    )

    # Get tax year and amount
    tax_year = (
        _safe_int(tax_data.get("taxYear")) or  # expandedprofile format
        _safe_int(tax_data.get("taxyear")) or
        _safe_int(assessment_data.get("year"))
    )

    tax_amount = (
        _safe_decimal(tax_data.get("taxAmt")) or  # expandedprofile format
        _safe_decimal(tax_data.get("taxamt")) or
        _safe_decimal(tax_data.get("annualTaxAmount"))
    )

    details = {
        "tax_assessment_year": tax_year,
        "tax_assessed_value": assessed_value,
        "tax_amount_annual": tax_amount,
        "tax_delinquent_year": _safe_int(tax_data.get("delinquentYear") or tax_data.get("delinquent_year")),
    }

    return details


def _extract_propensity_score(property_data: dict) -> dict:
    """Extract propensity to default score from ATTOM API response."""
    # Propensity score might be in different locations
    propensity_data = property_data.get("propensity", {}) or property_data.get("propensityToDefault", {})

    score = propensity_data.get("score") or propensity_data.get("propensityScore")
    decile = propensity_data.get("decile") or propensity_data.get("propensityDecile")

    details = {
        "propensity_to_default_score": _safe_int(score),
        "propensity_to_default_decile": _safe_int(decile),
    }

    return details


def _fetch_from_attom_endpoint(endpoint: str, address1: str, address2: str, extra_params: Optional[dict] = None) -> dict:
    """
    Helper to fetch data from a specific ATTOM endpoint.

    Args:
        endpoint: API endpoint path (e.g., '/property/detail', '/assessment/detail')
        address1: Street address
        address2: City, state, zip

    Returns:
        Raw API response dict or empty dict on error
    """
    api_key = settings.ATTOM_API_KEY
    if not api_key:
        print(f"ATTOM_API_KEY not found in settings.")
        return {}

    url = f"https://api.gateway.attomdata.com/propertyapi/v1.0.0{endpoint}"

    headers = {
        "apikey": api_key,
        "accept": "application/json",
    }
    params = {
        "address1": address1,
        "address2": address2,
    }
    if extra_params:
        for key, value in extra_params.items():
            if value:
                params[key] = value

    try:
        print(f"[ATTOM DEBUG] Making request to: {url}")
        print(f"[ATTOM DEBUG] Params: {params}")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        print(f"[ATTOM DEBUG] Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()

        # Check if ATTOM returned "SuccessWithoutResult" (valid request but no data)
        status = data.get("status", {})
        if status.get("msg") == "SuccessWithoutResult":
            print(f"[ATTOM DEBUG] ATTOM returned SuccessWithoutResult - property not in their database")
            print(f"[ATTOM DEBUG] Requested: {params.get('address1')}, {params.get('address2')}")
            return {}

        return data
    except requests.exceptions.HTTPError as e:
        # 400 errors typically mean the address isn't in ATTOM's database
        if e.response.status_code == 400:
            try:
                error_data = e.response.json()
                if error_data.get("status", {}).get("msg") == "SuccessWithoutResult":
                    print(f"[ATTOM DEBUG] Property not found in ATTOM database (SuccessWithoutResult)")
                    print(f"[ATTOM DEBUG] Requested: {params.get('address1')}, {params.get('address2')}")
                else:
                    print(f"[ATTOM DEBUG] 400 Error - Response body: {e.response.text[:500]}")
            except:
                print(f"[ATTOM DEBUG] 400 Error - Response body: {e.response.text[:500]}")
            print(f"Address not found in ATTOM database for {endpoint}")
        else:
            print(f"HTTP error fetching from {endpoint}: {e}")
        return {}
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from {endpoint}: {e}")
        return {}
    except Exception as e:
        print(f"Unexpected error fetching from {endpoint}: {type(e).__name__}: {e}")
        return {}


def fetch_attom_data_for_address(address1: str, address2: str, owner_name: Optional[str] = None) -> dict:
    """
    Fetches comprehensive ATTOM data for a given address using the expandedprofile endpoint.
    This endpoint includes mortgage, assessment, tax, and property details in a single API call.

    Args:
        address1: Street address (e.g., "123 Main St")
        address2: City, state, zip (e.g., "Haverhill, MA 01830")

    Returns:
        Dictionary containing all ATTOM data fields
    """
    api_key = settings.ATTOM_API_KEY
    if not api_key:
        print("ATTOM_API_KEY not found in settings.")
        return {}

    # Clean up addresses - remove extra whitespace that causes 400 errors
    address1 = " ".join(address1.split()) if address1 else ""
    address2 = " ".join(address2.split()) if address2 else ""

    owner_param = None
    if owner_name:
        owner_param = " ".join(str(owner_name).split())

    print(f"[ATTOM DEBUG] Fetching comprehensive ATTOM data")
    print(f"[ATTOM DEBUG] Address1: '{address1}'")
    print(f"[ATTOM DEBUG] Address2: '{address2}'")
    print(f"[ATTOM DEBUG] Owner: '{owner_param}'" if owner_param else "[ATTOM DEBUG] Owner: None")

    # Initialize result structure
    result = {
        "pre_foreclosure": False,
        "mortgage_default": False,
        "tax_default": False,
        "raw_response": {},
    }

    # Fetch all data from /property/expandedprofile (includes mortgage, assessment, tax data)
    print("  → Fetching property data from expandedprofile endpoint...")

    # Try with owner name first if provided
    profile_response = {}
    if owner_param:
        print(f"  → Trying with owner name: {owner_param}")
        profile_response = _fetch_from_attom_endpoint(
            "/property/expandedprofile",
            address1,
            address2,
            extra_params={"ownername": owner_param},
        )

    # If no results with owner name or owner not provided, try without it
    if not profile_response or not profile_response.get("property"):
        if owner_param:
            print("  → Owner name lookup failed, retrying without owner name parameter...")
        profile_response = _fetch_from_attom_endpoint(
            "/property/expandedprofile",
            address1,
            address2,
            extra_params=None,
        )

    # If still no results, try normalizing the address (remove unit numbers, try common abbreviations)
    if not profile_response or not profile_response.get("property"):
        print("  → Initial lookup failed, trying address variations...")

        # Try removing unit/apartment numbers from address
        import re
        address1_base = re.sub(r'\s*#.*$', '', address1)  # Remove # and everything after
        address1_base = re.sub(r'\s*(Unit|Apt|Suite|Ste)\s+.*$', '', address1_base, flags=re.IGNORECASE)

        if address1_base != address1:
            print(f"  → Trying without unit number: {address1_base}")
            profile_response = _fetch_from_attom_endpoint(
                "/property/expandedprofile",
                address1_base,
                address2,
                extra_params=None,
            )

    if profile_response and profile_response.get("property"):
        property_data = profile_response["property"][0]

        # Extract mortgage details
        mortgage_details = _extract_mortgage_details(property_data)
        result.update(mortgage_details)

        # Extract tax and assessment details
        tax_details = _extract_tax_details(property_data)
        result.update(tax_details)

        # Store raw response
        result["raw_response"]["expandedprofile"] = profile_response

        # Check for tax delinquency
        if tax_details.get("tax_delinquent_year"):
            result["tax_default"] = True

        # Check for foreclosure flags in property data
        foreclosure_summary = property_data.get("foreclosure", {}).get("summary")
        if foreclosure_summary:
            result["pre_foreclosure"] = True
            if "tax" in str(foreclosure_summary).lower():
                result["tax_default"] = True
            if "mortgage" in str(foreclosure_summary).lower():
                result["mortgage_default"] = True

            # Extract foreclosure details if present
            foreclosure_details = _extract_foreclosure_details(property_data)
            result.update(foreclosure_details)

        # Try to extract propensity score if available
        propensity_details = _extract_propensity_score(property_data)
        result.update(propensity_details)

        print(f"  ✓ Successfully fetched property data from ATTOM")
    else:
        print(f"  ✗ No data returned from ATTOM endpoint")
        print(f"  ℹ️  This property may not be in ATTOM's database. This is normal for some properties.")
        print(f"  ℹ️  ATTOM coverage varies by location and property type.")

    return result


def _find_unit_for_loc(parcel, loc_id: str, unit_key: Optional[str] = None) -> Optional[dict]:
    """
    Attempt to locate a specific unit within a parcel by loc_id or row/source key.
    """
    normalized_target = _normalize_loc_id(loc_id)
    candidate: Optional[dict] = None

    for unit in parcel.units_detail or []:
        unit_loc = unit.get("loc_id")
        if unit_loc and _normalize_loc_id(unit_loc) == normalized_target:
            candidate = unit
            break

    if candidate or not unit_key:
        return candidate

    target_key = str(unit_key).strip().lower()
    for unit in parcel.units_detail or []:
        for key_name in ("row_key", "source_id", "id", "unit_number"):
            value = unit.get(key_name)
            if value and str(value).strip().lower() == target_key:
                return unit
    return candidate


def get_or_fetch_attom_data(
    town_id: int,
    loc_id: str,
    max_age_days: int = None,
    *,
    parcel=None,
    unit: Optional[dict] = None,
    unit_key: Optional[str] = None,
) -> dict:
    """
    Get ATTOM data from cache if fresh, otherwise fetch from API.
    This implements cross-user caching to minimize API calls.

    Args:
        town_id: MassGIS town ID
        loc_id: Parcel location ID
        max_age_days: Maximum age of cached data in days (default: from settings.ATTOM_CACHE_MAX_AGE_DAYS)

    Returns:
        parcel: Optional pre-fetched parcel detail to avoid reloading MassGIS data
        unit: Optional unit dictionary (from parcel.units_detail)
        unit_key: Optional key to help locate unit within parcel if `unit` not provided

    Returns:
        Dictionary containing ATTOM data fields or empty dict if unavailable
    """
    from django.utils import timezone
    from datetime import timedelta

    # Use setting if max_age_days not provided
    if max_age_days is None:
        max_age_days = getattr(settings, "ATTOM_CACHE_MAX_AGE_DAYS", 60)

    cache_key = build_attom_cache_key(loc_id, unit)
    # Check if we have recent cached data for this parcel (across ALL users)
    cache_cutoff = timezone.now() - timedelta(days=max_age_days)
    cached_record = AttomData.objects.filter(
        town_id=town_id,
        loc_id=cache_key,
        last_updated__gte=cache_cutoff
    ).order_by('-last_updated').first()

    if cached_record:
        print(f"Using cached ATTOM data for parcel {cache_key} (last updated: {cached_record.last_updated})")
        # Convert the model instance to a dict
        return _attom_model_to_dict(cached_record)

    # No fresh cache, need to fetch from API
    print(f"No fresh cache found for parcel {cache_key}, fetching from ATTOM API...")

    if parcel is None:
        try:
            parcel = get_massgis_parcel_detail(town_id, loc_id)
        except Exception as e:
            print(f"Error fetching parcel details for loc_id {loc_id}: {e}")
            return {}

    target_unit = unit or _find_unit_for_loc(parcel, loc_id, unit_key=unit_key)

    address1 = parcel.site_address
    if target_unit:
        unit_site_address = target_unit.get("site_address")
        unit_number = target_unit.get("unit_number")
        mailing_street = target_unit.get("mailing_street")
        if unit_site_address:
            address1 = unit_site_address
        elif mailing_street:
            address1 = mailing_street
        elif unit_number and address1:
            address1 = f"{address1} #{unit_number}"

    if not address1 and parcel.units_detail:
        for unit in parcel.units_detail:
            if unit.get("site_address"):
                address1 = unit["site_address"]
                break

    if not address1:
        print(f"Parcel {loc_id} is missing street address information.")
        return {}

    mailing_city = target_unit.get("mailing_city") if target_unit else None
    mailing_state = target_unit.get("mailing_state") if target_unit else None
    mailing_zip = target_unit.get("mailing_zip") if target_unit else None

    if target_unit and target_unit.get("mailing_address"):
        mailing_parts = [part.strip() for part in target_unit.get("mailing_address", "").split(",") if part.strip()]
        if not mailing_city and len(mailing_parts) >= 2:
            for part in mailing_parts[1:]:
                if part and not any(ch.isdigit() for ch in part):
                    mailing_city = part
                    break
            if not mailing_city and len(mailing_parts) > 1:
                mailing_city = mailing_parts[1]

        for part in reversed(mailing_parts):
            if not mailing_zip and any(ch.isdigit() for ch in part):
                digits = "".join(ch for ch in part if ch.isdigit())
                if len(digits) >= 5:
                    mailing_zip = digits[:5]
            if not mailing_state and len(part) == 2 and part.isalpha():
                mailing_state = part.upper()

    city_part = mailing_city or (target_unit.get("site_city") if target_unit else None) or parcel.site_city

    state_code = (mailing_state or getattr(settings, "ATTOM_DEFAULT_STATE", "MA")).upper()
    postal_zip = mailing_zip or parcel.site_zip

    if city_part and postal_zip:
        address2 = f"{city_part}, {state_code} {postal_zip}"
    elif city_part:
        address2 = f"{city_part}, {state_code}"
    else:
        address2 = state_code

    if not city_part:
        print(f"Parcel {loc_id} is missing city information for ATTOM lookup; using state fallback.")

    owner_name = None
    if target_unit:
        owner_name = target_unit.get("owner")
    if not owner_name:
        owner_name = getattr(parcel, "owner_name", None)

    print(f"[ATTOM DEBUG] Constructed addresses before calling API:")
    print(f"[ATTOM DEBUG] address1={repr(address1)}")
    print(f"[ATTOM DEBUG] address2={repr(address2)}")
    print(f"[ATTOM DEBUG] owner_name={repr(owner_name)}")

    # Fetch from API
    attom_data = fetch_attom_data_for_address(address1, address2, owner_name=owner_name)

    if attom_data:
        ensure_attom_cache_record(
            town_id=town_id,
            loc_id=cache_key,
            payload=attom_data,
            saved_list=None,
        )
        base_key = build_attom_cache_key(loc_id, None)
        if base_key != cache_key:
            ensure_attom_cache_record(
                town_id=town_id,
                loc_id=base_key,
                payload=attom_data,
                saved_list=None,
            )

    return attom_data


def _attom_model_to_dict(attom_record: "AttomData") -> dict:
    """Convert an AttomData model instance to a dictionary."""
    return {
        "pre_foreclosure": attom_record.pre_foreclosure,
        "mortgage_default": attom_record.mortgage_default,
        "tax_default": attom_record.tax_default,
        "foreclosure_recording_date": attom_record.foreclosure_recording_date,
        "foreclosure_auction_date": attom_record.foreclosure_auction_date,
        "foreclosure_estimated_value": attom_record.foreclosure_estimated_value,
        "foreclosure_judgment_amount": attom_record.foreclosure_judgment_amount,
        "foreclosure_default_amount": attom_record.foreclosure_default_amount,
        "foreclosure_stage": attom_record.foreclosure_stage,
        "foreclosure_document_type": attom_record.foreclosure_document_type,
        "mortgage_loan_amount": attom_record.mortgage_loan_amount,
        "mortgage_loan_type": attom_record.mortgage_loan_type,
        "mortgage_lender_name": attom_record.mortgage_lender_name,
        "mortgage_interest_rate": attom_record.mortgage_interest_rate,
        "mortgage_term_years": attom_record.mortgage_term_years,
        "mortgage_recording_date": attom_record.mortgage_recording_date,
        "mortgage_due_date": attom_record.mortgage_due_date,
        "mortgage_loan_number": attom_record.mortgage_loan_number,
        "tax_assessment_year": attom_record.tax_assessment_year,
        "tax_assessed_value": attom_record.tax_assessed_value,
        "tax_amount_annual": attom_record.tax_amount_annual,
        "tax_delinquent_year": attom_record.tax_delinquent_year,
        "propensity_to_default_score": attom_record.propensity_to_default_score,
        "propensity_to_default_decile": attom_record.propensity_to_default_decile,
        "raw_response": attom_record.raw_response,
    }


def build_attom_cache_key(loc_id: str, unit: Optional[dict] = None) -> str:
    """
    Build a cache key for ATTOM data that is unique per unit when applicable.
    """
    base = _normalize_loc_id(loc_id)
    if unit:
        unit_identifier = (
            unit.get("loc_id")
            or unit.get("source_id")
            or unit.get("row_key")
            or unit.get("id")
        )
        if unit_identifier:
            unit_key = _normalize_loc_id(unit_identifier)
            if not unit_key:
                unit_key = str(unit_identifier).strip()
            if unit_key:
                return f"{base}::UNIT::{unit_key}"
    return base


def ensure_attom_cache_record(*, town_id: int, loc_id: str, payload: dict, saved_list: Optional[SavedParcelList] = None) -> tuple["AttomData", bool]:
    """
    Persist ATTOM data for a parcel, optionally scoped to a saved list, and return the record.

    Args:
        town_id: Parcel town identifier
        loc_id: Parcel LOC_ID
        payload: Dictionary of ATTOM fields to store
        saved_list: Optional saved list to associate cached data with

    Returns:
        Tuple of (AttomData instance, created flag)
    """
    if payload is None:
        payload = {}

    defaults = dict(payload)
    defaults.setdefault("raw_response", payload.get("raw_response", {}) or {})

    record, created = AttomData.objects.update_or_create(
        town_id=town_id,
        loc_id=loc_id,
        saved_list=saved_list,
        defaults=defaults,
    )
    return record, created


def update_attom_data_for_parcel(saved_list: SavedParcelList, town_id: int, loc_id: str):
    """
    Fetches comprehensive foreclosure, mortgage, tax, and default data from the ATTOM API
    for a given parcel and updates the corresponding AttomData model with detailed information.
    Uses cross-user caching to minimize API calls - only fetches new data if cache is stale.
    Cache age is controlled by settings.ATTOM_CACHE_MAX_AGE_DAYS (default: 60 days).
    """
    api_key = settings.ATTOM_API_KEY
    if not api_key:
        print("ATTOM_API_KEY not found in settings.")
        return

    # Try to get data from cache first (uses settings.ATTOM_CACHE_MAX_AGE_DAYS)
    attom_data = get_or_fetch_attom_data(town_id, loc_id)

    # Check if we got valid data from the endpoint
    raw_responses = attom_data.get("raw_response", {})
    has_data = bool(
        raw_responses.get("expandedprofile", {}).get("property")
    )

    if not attom_data or not has_data:
        print(f"No property data available for parcel {loc_id}")
        # Still create a record with empty data to avoid repeated API calls
        empty_payload = {
            "pre_foreclosure": False,
            "mortgage_default": False,
            "tax_default": False,
            "raw_response": raw_responses,
        }
        ensure_attom_cache_record(
            town_id=town_id,
            loc_id=loc_id,
            payload=empty_payload,
            saved_list=saved_list,
        )
        ensure_attom_cache_record(
            town_id=town_id,
            loc_id=loc_id,
            payload=empty_payload,
            saved_list=None,
        )
        return

    # Save to database (this will update last_updated timestamp)
    try:
        _, created_saved = ensure_attom_cache_record(
            town_id=town_id,
            loc_id=loc_id,
            payload=attom_data,
            saved_list=saved_list,
        )
        ensure_attom_cache_record(
            town_id=town_id,
            loc_id=loc_id,
            payload=attom_data,
            saved_list=None,
        )
        action = "Created" if created_saved else "Updated"
        print(f"{action} ATTOM data for parcel {loc_id}")
    except Exception as e:
        print(f"Error saving ATTOM data to database for parcel {loc_id}: {type(e).__name__}: {e}")
