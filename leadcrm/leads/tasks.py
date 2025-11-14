"""
Celery tasks for background processing.
"""
from celery import shared_task
from django.core.management import call_command
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
