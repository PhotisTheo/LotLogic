"""
Celery tasks for background processing.
"""
from celery import shared_task
from django.core.management import call_command
from django.utils import timezone
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)


@shared_task(name='leads.refresh_all_parcels')
def refresh_all_parcels():
    """
    Refresh all Massachusetts parcel data from MassGIS.
    Runs weekly to keep data up-to-date.
    """
    logger.info("Starting weekly parcel refresh...")

    try:
        # Run the precompute command
        call_command('precompute_all_parcels', batch_size=1000)
        logger.info("Parcel refresh completed successfully")
        return "Success"
    except Exception as exc:
        logger.error(f"Parcel refresh failed: {exc}", exc_info=True)
        raise


@shared_task(name='leads.refresh_town_parcels')
def refresh_town_parcels(town_id: int):
    """
    Refresh parcels for a specific town.
    Useful for targeted updates.
    """
    logger.info(f"Refreshing parcels for town {town_id}...")

    try:
        call_command('precompute_all_parcels', town_id=[town_id], batch_size=1000)
        logger.info(f"Town {town_id} refresh completed")
        return f"Success: town {town_id}"
    except Exception as exc:
        logger.error(f"Town {town_id} refresh failed: {exc}", exc_info=True)
        raise


@shared_task(name='leads.refresh_scraped_documents')
def refresh_scraped_documents():
    """
    Refresh scraped documents (mortgages, liens, foreclosures) for parcels with stale data.
    Runs weekly to keep property documents up-to-date.

    Follows same pattern as refresh_all_parcels but for ATTOM replacement pipeline.
    """
    from .models import AttomData

    logger.info("Starting weekly scraped document refresh...")

    try:
        # Find parcels with data older than 90 days
        stale_threshold = timezone.now() - timedelta(days=90)
        stale_parcels = AttomData.objects.filter(
            last_updated__lt=stale_threshold
        ).values_list('town_id', 'loc_id')[:1000]  # Limit to 1000 parcels per week

        logger.info(f"Found {len(stale_parcels)} parcels with stale data (>90 days old)")

        # Queue scraping tasks for each parcel
        from data_pipeline.jobs.task_queue import run_registry_task
        from data_pipeline.town_registry_map import get_registry_for_town

        refreshed_count = 0
        for town_id, loc_id in stale_parcels:
            registry_id = get_registry_for_town(town_id)
            if registry_id:
                # Queue async task
                run_registry_task.delay(
                    config={'registry_id': registry_id},
                    loc_id=f"{town_id}-{loc_id}",
                    force_refresh=True
                )
                refreshed_count += 1

        logger.info(f"Queued {refreshed_count} document refresh tasks")
        return f"Success: queued {refreshed_count} refreshes"

    except Exception as exc:
        logger.error(f"Document refresh failed: {exc}", exc_info=True)
        raise
