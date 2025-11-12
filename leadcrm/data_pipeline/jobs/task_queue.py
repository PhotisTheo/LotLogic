"""
Celery task definitions for running pipeline jobs asynchronously.

Background tasks for scraping property data from registries and assessors.
"""

from __future__ import annotations

from typing import Dict, Any, Optional
import logging

from celery import shared_task

from .registry_job import RegistryJob
from .assessor_job import AssessorJob
from ..settings import pipeline_settings

logger = logging.getLogger(__name__)


@shared_task(bind=True, name='data_pipeline.scrape_registry')
def run_registry_task(
    self,
    config: Dict[str, Any],
    address: Optional[str] = None,
    owner: Optional[str] = None,
    loc_id: Optional[str] = None,
    dry_run: bool = False,
    force_refresh: bool = False,
    max_cache_age_days: int = 90,
) -> Dict[str, Any]:
    """
    Entry point for async registry scrapes.

    Args:
        config: Registry configuration dict
        address: Property address to search
        owner: Owner name to search
        loc_id: Location ID (town_id + parcel_id)
        dry_run: If True, don't save to database
        force_refresh: If True, scrape even if cache is fresh
        max_cache_age_days: Maximum cache age before refresh

    Returns:
        Dict with status and any error messages
    """
    try:
        logger.info(f"Starting registry scrape task {self.request.id} for loc_id={loc_id}")

        job = RegistryJob(config)
        job.run(
            address=address,
            owner=owner,
            loc_id=loc_id,
            dry_run=dry_run,
            force_refresh=force_refresh,
            max_cache_age_days=max_cache_age_days,
        )

        logger.info(f"Registry scrape task {self.request.id} completed successfully")
        return {"status": "success", "loc_id": loc_id}

    except Exception as e:
        logger.error(f"Registry scrape task {self.request.id} failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e), "loc_id": loc_id}


@shared_task(bind=True, name='data_pipeline.scrape_assessor')
def run_assessor_task(
    self,
    config: Dict[str, Any],
    parcel_id: Optional[str] = None,
    address: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Entry point for async assessor scrapes.

    Args:
        config: Assessor configuration dict
        parcel_id: Parcel ID to search
        address: Property address to search
        dry_run: If True, don't save to database

    Returns:
        Dict with status and any error messages
    """
    try:
        logger.info(f"Starting assessor scrape task {self.request.id} for parcel_id={parcel_id}")

        job = AssessorJob(config)
        job.run(
            parcel_id=parcel_id,
            address=address,
            dry_run=dry_run,
        )

        logger.info(f"Assessor scrape task {self.request.id} completed successfully")
        return {"status": "success", "parcel_id": parcel_id}

    except Exception as e:
        logger.error(f"Assessor scrape task {self.request.id} failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e), "parcel_id": parcel_id}


def build_default_payload() -> Dict[str, Any]:
    """Helper for schedulers to know standard defaults."""
    return {
        "dry_run": False,
        "storage_root": str(pipeline_settings.storage_root),
    }
