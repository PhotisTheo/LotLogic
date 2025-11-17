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
def refresh_scraped_documents(batch_size: int = 25000):
    """
    Refresh scraped documents (mortgages, liens, foreclosures) for ALL parcels.
    Runs weekly to keep property documents up-to-date statewide.

    Processes parcels in batches, prioritizing:
    1. Parcels with no ATTOM data yet
    2. Parcels with stale data (>90 days old)
    3. All other parcels

    Args:
        batch_size: Number of parcels to process per weekly run (default: 5000)
    """
    from .models import MassGISParcel, AttomData

    logger.info(f"Starting weekly scraped document refresh (batch_size={batch_size})...")

    try:
        # Get all parcel IDs from MassGIS
        all_parcels = set(
            MassGISParcel.objects.values_list('town_id', 'loc_id')
        )
        logger.info(f"Total parcels in database: {len(all_parcels)}")

        # Get parcels that already have ATTOM data
        scraped_parcels = set(
            AttomData.objects.values_list('town_id', 'loc_id')
        )
        logger.info(f"Parcels with existing data: {len(scraped_parcels)}")

        # Priority 1: Parcels with no data yet
        unscraped_parcels = list(all_parcels - scraped_parcels)
        logger.info(f"Parcels without data: {len(unscraped_parcels)}")

        # Priority 2: Parcels with stale data (>90 days)
        stale_threshold = timezone.now() - timedelta(days=90)
        stale_parcels = list(
            AttomData.objects.filter(
                last_updated__lt=stale_threshold
            ).values_list('town_id', 'loc_id')
        )
        logger.info(f"Parcels with stale data (>90 days): {len(stale_parcels)}")

        # Combine priorities: unscraped first, then stale
        parcels_to_scrape = unscraped_parcels[:batch_size]

        if len(parcels_to_scrape) < batch_size:
            remaining = batch_size - len(parcels_to_scrape)
            parcels_to_scrape.extend(stale_parcels[:remaining])

        logger.info(f"Scraping {len(parcels_to_scrape)} parcels this week")

        # Queue scraping tasks
        from data_pipeline.jobs.task_queue import run_registry_task
        from data_pipeline.town_registry_map import get_registry_for_town

        queued_count = 0
        skipped_count = 0

        for town_id, loc_id in parcels_to_scrape:
            registry_id = get_registry_for_town(town_id)
            if registry_id:
                # Queue async task
                run_registry_task.delay(
                    config={'registry_id': registry_id},
                    loc_id=f"{town_id}-{loc_id}",
                    force_refresh=True
                )
                queued_count += 1
            else:
                skipped_count += 1

        logger.info(f"Queued {queued_count} scraping tasks")
        if skipped_count > 0:
            logger.warning(f"Skipped {skipped_count} parcels (no registry mapping)")

        # Calculate completion progress
        total_to_scrape = len(all_parcels)
        total_scraped_after = len(scraped_parcels) + queued_count
        progress_pct = (total_scraped_after / total_to_scrape) * 100 if total_to_scrape > 0 else 0

        logger.info(f"Progress: {total_scraped_after}/{total_to_scrape} parcels ({progress_pct:.1f}%)")

        # Estimate weeks to completion
        if queued_count > 0:
            weeks_remaining = (total_to_scrape - total_scraped_after) / queued_count
            logger.info(f"Estimated weeks to full coverage: {int(weeks_remaining)}")

        return f"Success: queued {queued_count} scrapes, {progress_pct:.1f}% complete"

    except Exception as exc:
        logger.error(f"Document refresh failed: {exc}", exc_info=True)
        raise


@shared_task(name='leads.scrape_saved_list_parcels')
def scrape_saved_list_parcels(saved_list_id: int):
    """
    Scrape documents for all parcels in a saved list.
    Triggered automatically when a user saves a list.

    Args:
        saved_list_id: ID of the SavedParcelList to scrape
    """
    from .models import SavedParcelList
    from data_pipeline.jobs.task_queue import run_registry_task
    from data_pipeline.town_registry_map import get_registry_for_town

    logger.info(f"Starting scrape for saved list {saved_list_id}...")

    try:
        saved_list = SavedParcelList.objects.get(id=saved_list_id)
        loc_ids = saved_list.loc_ids  # List of {town_id, loc_id} dicts

        logger.info(f"Scraping {len(loc_ids)} parcels from list '{saved_list.name}'")

        queued_count = 0
        skipped_count = 0

        for parcel_info in loc_ids:
            town_id = parcel_info.get('town_id')
            loc_id = parcel_info.get('loc_id')

            if not town_id or not loc_id:
                skipped_count += 1
                continue

            registry_id = get_registry_for_town(town_id)
            if registry_id:
                # Queue async scraping task
                run_registry_task.delay(
                    config={'registry_id': registry_id},
                    loc_id=f"{town_id}-{loc_id}",
                    force_refresh=False,  # Use cache if available (within 90 days)
                    max_cache_age_days=90,
                )
                queued_count += 1
            else:
                logger.warning(f"No registry mapping for town {town_id}, skipping {loc_id}")
                skipped_count += 1

        logger.info(f"Saved list '{saved_list.name}': queued {queued_count} scraping tasks, skipped {skipped_count}")
        return f"Success: queued {queued_count} scrapes for list '{saved_list.name}'"

    except SavedParcelList.DoesNotExist:
        logger.error(f"SavedParcelList {saved_list_id} not found")
        raise
    except Exception as exc:
        logger.error(f"Failed to scrape saved list {saved_list_id}: {exc}", exc_info=True)
        raise
