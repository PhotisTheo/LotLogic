"""
Persistence helpers for writing scraper outputs into Django models.

Maps scraped data to AttomData model and performs upserts based on loc_id/town_id.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Dict, Optional

logger = logging.getLogger("pipeline.storage")

# Import Django models
try:
    import django
    import os
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leadcrm.settings')
    django.setup()
    from leads.models import AttomData, CorporateEntity
    from django.utils import timezone
    from datetime import timedelta
    DJANGO_AVAILABLE = True
except Exception as e:
    logger.warning(f"Django not available for database operations: {e}")
    DJANGO_AVAILABLE = False


def check_cache_freshness(loc_id: str, town_id: Optional[int] = None, max_age_days: int = 90) -> tuple[bool, Optional[int]]:
    """
    Check if cached data exists and is fresh for a given parcel.

    Args:
        loc_id: Parcel location ID
        town_id: Optional town ID
        max_age_days: Maximum cache age in days (default 90)

    Returns:
        Tuple of (is_fresh: bool, attom_data_id: Optional[int])
    """
    if not DJANGO_AVAILABLE:
        return False, None

    try:
        # Try to find existing record
        query = {'loc_id': loc_id}
        if town_id:
            query['town_id'] = town_id

        attom_data = AttomData.objects.filter(**query).first()

        if not attom_data:
            logger.debug(f"No cached data found for loc_id={loc_id}")
            return False, None

        # Check if data is fresh
        if not attom_data.last_updated:
            logger.debug(f"Cached data exists but no last_updated timestamp for loc_id={loc_id}")
            return False, attom_data.id

        age = timezone.now() - attom_data.last_updated
        is_fresh = age < timedelta(days=max_age_days)

        if is_fresh:
            days_old = age.days
            logger.info(f"Cache HIT: Data for loc_id={loc_id} is {days_old} days old (fresh)")
        else:
            logger.info(f"Cache MISS: Data for loc_id={loc_id} is {age.days} days old (stale)")

        return is_fresh, attom_data.id

    except Exception as e:
        logger.error(f"Error checking cache freshness: {e}")
        return False, None


def save_registry_record(record) -> Optional[int]:
    """
    Save a RegistryRecord to the AttomData model.

    Args:
        record: RegistryRecord dataclass instance

    Returns:
        AttomData ID if saved, None otherwise
    """
    if not DJANGO_AVAILABLE:
        logger.warning("Django not available. Cannot save record.")
        return None

    if not record.loc_id:
        logger.warning("Record missing loc_id, cannot save: %s", record)
        return None

    try:
        # Extract town_id from loc_id if possible (format: TOWNID_LOCID)
        town_id = None
        if record.loc_id and '_' in record.loc_id:
            try:
                town_id = int(record.loc_id.split('_')[0])
            except (ValueError, IndexError):
                pass

        # Find or create AttomData record
        attom_data, created = AttomData.objects.get_or_create(
            loc_id=record.loc_id,
            town_id=town_id,
            defaults={
                'raw_response': {}
            }
        )

        # Update fields based on instrument type
        if record.instrument_type == "MORTGAGE":
            # Update mortgage fields
            if record.amount:
                attom_data.mortgage_loan_amount = Decimal(str(record.amount))
            if record.lender:
                attom_data.mortgage_lender_name = record.lender
            if record.document_date:
                attom_data.mortgage_recording_date = record.document_date

            # Add parsed data from metadata
            if record.raw_metadata.get('parsed_interest_rate'):
                try:
                    attom_data.mortgage_interest_rate = Decimal(str(record.raw_metadata['parsed_interest_rate']))
                except (ValueError, TypeError):
                    pass
            if record.raw_metadata.get('parsed_term_years'):
                attom_data.mortgage_term_years = record.raw_metadata['parsed_term_years']

        elif record.instrument_type == "LIS PENDENS":
            # Mark as pre-foreclosure
            attom_data.pre_foreclosure = True
            attom_data.foreclosure_stage = "Lis Pendens"
            attom_data.foreclosure_document_type = record.instrument_type
            if record.document_date:
                attom_data.foreclosure_recording_date = record.document_date

        # Store raw metadata in JSON field
        if not attom_data.raw_response:
            attom_data.raw_response = {}

        # Add source provenance
        if 'scrape_sources' not in attom_data.raw_response:
            attom_data.raw_response['scrape_sources'] = []

        attom_data.raw_response['scrape_sources'].append({
            'source': 'registry',
            'registry_id': record.registry_id,
            'instrument_type': record.instrument_type,
            'document_date': record.document_date,
            'document_path': record.raw_document_path,
            'metadata': record.raw_metadata,
        })

        attom_data.save()

        action = "Created" if created else "Updated"
        logger.info(f"{action} AttomData record {attom_data.id} for loc_id={record.loc_id}")
        return attom_data.id

    except Exception as e:
        logger.error(f"Failed to save registry record: {e}", exc_info=True)
        return None


def save_assessor_record(record) -> Optional[int]:
    """
    Save an AssessorRecord to the AttomData model.

    Args:
        record: AssessorRecord dataclass instance

    Returns:
        AttomData ID if saved, None otherwise
    """
    if not DJANGO_AVAILABLE:
        logger.warning("Django not available. Cannot save record.")
        return None

    if not record.parcel_id:
        logger.warning("Record missing parcel_id, cannot save: %s", record)
        return None

    try:
        # Extract town_id from municipality_code
        town_id = None
        try:
            town_id = int(record.municipality_code)
        except (ValueError, TypeError):
            pass

        # Find or create AttomData record
        attom_data, created = AttomData.objects.get_or_create(
            loc_id=record.parcel_id,
            town_id=town_id,
            defaults={
                'raw_response': {}
            }
        )

        # Update tax assessment fields
        if record.tax_year:
            attom_data.tax_assessment_year = record.tax_year
        if record.assessed_total:
            attom_data.tax_assessed_value = Decimal(str(record.assessed_total))
        if record.tax_amount:
            attom_data.tax_amount_annual = Decimal(str(record.tax_amount))

        # Store raw metadata in JSON field
        if not attom_data.raw_response:
            attom_data.raw_response = {}

        # Add source provenance
        if 'scrape_sources' not in attom_data.raw_response:
            attom_data.raw_response['scrape_sources'] = []

        attom_data.raw_response['scrape_sources'].append({
            'source': 'assessor',
            'municipality': record.municipality_code,
            'tax_year': record.tax_year,
            'assessed_land': str(record.assessed_land) if record.assessed_land else None,
            'assessed_building': str(record.assessed_building) if record.assessed_building else None,
            'assessed_total': str(record.assessed_total) if record.assessed_total else None,
            'source_url': record.source_url,
            'raw_payload': record.raw_payload,
        })

        attom_data.save()

        action = "Created" if created else "Updated"
        logger.info(f"{action} AttomData record {attom_data.id} for parcel_id={record.parcel_id}")
        return attom_data.id

    except Exception as e:
        logger.error(f"Failed to save assessor record: {e}", exc_info=True)
        return None


def check_corporate_cache_freshness(entity_name: str, max_age_days: int = 180) -> tuple[bool, Optional[int]]:
    """
    Check if cached corporate entity data exists and is fresh.

    Args:
        entity_name: Legal name of the entity
        max_age_days: Maximum cache age in days (default 180)

    Returns:
        Tuple of (is_fresh: bool, corporate_entity_id: Optional[int])
    """
    if not DJANGO_AVAILABLE:
        return False, None

    try:
        # Try to find existing record by entity name (case-insensitive)
        corporate_entity = CorporateEntity.objects.filter(
            entity_name__iexact=entity_name.strip()
        ).first()

        if not corporate_entity:
            logger.debug(f"No cached data found for entity_name={entity_name}")
            return False, None

        # Check if data is fresh
        if not corporate_entity.last_updated:
            logger.debug(f"Cached data exists but no last_updated timestamp for entity_name={entity_name}")
            return False, corporate_entity.id

        age = timezone.now() - corporate_entity.last_updated
        is_fresh = age < timedelta(days=max_age_days)

        if is_fresh:
            days_old = age.days
            logger.info(f"Corporate cache HIT: Data for entity_name={entity_name} is {days_old} days old (fresh)")
        else:
            logger.info(f"Corporate cache MISS: Data for entity_name={entity_name} is {age.days} days old (stale)")

        return is_fresh, corporate_entity.id

    except Exception as e:
        logger.error(f"Error checking corporate cache freshness: {e}")
        return False, None


def save_corporate_record(record) -> Optional[int]:
    """
    Save a CorporateRecord to the CorporateEntity model.

    Args:
        record: CorporateRecord dataclass instance

    Returns:
        CorporateEntity ID if saved, None otherwise
    """
    if not DJANGO_AVAILABLE:
        logger.warning("Django not available. Cannot save corporate record.")
        return None

    if not record.entity_id or not record.entity_name:
        logger.warning("Corporate record missing entity_id or entity_name, cannot save: %s", record)
        return None

    try:
        # Find or create CorporateEntity record by entity_id
        corporate_entity, created = CorporateEntity.objects.update_or_create(
            entity_id=record.entity_id,
            defaults={
                'entity_name': record.entity_name,
                'entity_type': record.entity_type or 'llc',
                'status': record.status or 'active',
                'principal_name': record.principal_name,
                'principal_title': record.principal_title,
                'registered_agent': record.registered_agent,
                'business_phone': record.business_phone,
                'business_email': record.business_email,
                'business_address': record.business_address,
                'formation_date': record.formation_date,
                'last_annual_report': record.last_annual_report,
                'source_url': record.source_url,
                'raw_data': record.raw_data or {},
            }
        )

        action = "Created" if created else "Updated"
        logger.info(
            f"{action} CorporateEntity record {corporate_entity.id} for "
            f"entity_id={record.entity_id} ({record.entity_name})"
        )

        if record.principal_name:
            logger.info(f"  Principal: {record.principal_name}")
        if record.business_phone:
            logger.info(f"  Phone: {record.business_phone}")

        return corporate_entity.id

    except Exception as e:
        logger.error(f"Failed to save corporate record: {e}", exc_info=True)
        return None
