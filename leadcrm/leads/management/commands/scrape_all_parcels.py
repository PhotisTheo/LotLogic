"""
Management command to scrape registry documents for all Massachusetts parcels.
Queues Celery tasks to scrape mortgages, liens, and foreclosures from Registry of Deeds.
"""
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from datetime import timedelta

from ...models import MassGISParcel, AttomData


class Command(BaseCommand):
    help = "Scrape registry documents for all parcels (or subset by town/filter)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--town-id",
            action="append",
            type=int,
            dest="town_ids",
            help="Limit to specific town ID(s). Can be repeated.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=5000,
            help="Number of parcels to queue per run (default: 5000).",
        )
        parser.add_argument(
            "--unscraped-only",
            action="store_true",
            help="Only scrape parcels with no data yet.",
        )
        parser.add_argument(
            "--stale-days",
            type=int,
            default=90,
            help="Refresh parcels with data older than N days (default: 90).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be queued without actually queueing tasks.",
        )

    def handle(self, *args, **options):
        town_ids = options.get("town_ids")
        batch_size = options["batch_size"]
        unscraped_only = options["unscraped_only"]
        stale_days = options["stale_days"]
        dry_run = options["dry_run"]

        self.stdout.write(self.style.NOTICE("=" * 60))
        self.stdout.write(self.style.NOTICE("Registry Document Scraping"))
        self.stdout.write(self.style.NOTICE("=" * 60))

        # Build parcel queryset
        if town_ids:
            parcels_qs = MassGISParcel.objects.filter(town_id__in=town_ids)
            self.stdout.write(f"Filtering to {len(town_ids)} town(s): {town_ids}")
        else:
            parcels_qs = MassGISParcel.objects.all()
            self.stdout.write("Processing ALL towns statewide")

        total_parcels = parcels_qs.count()
        self.stdout.write(f"Total parcels in scope: {total_parcels:,}")

        # Get existing ATTOM data
        scraped_parcels = set(
            AttomData.objects.values_list('town_id', 'loc_id')
        )
        self.stdout.write(f"Parcels with existing data: {len(scraped_parcels):,}")

        # Build target list
        all_parcels = list(parcels_qs.values_list('town_id', 'loc_id'))

        if unscraped_only:
            # Only scrape parcels with no data
            parcels_to_scrape = [
                (tid, lid) for tid, lid in all_parcels
                if (tid, lid) not in scraped_parcels
            ]
            self.stdout.write(f"Unscraped parcels: {len(parcels_to_scrape):,}")
        else:
            # Priority 1: Unscraped parcels
            unscraped = [
                (tid, lid) for tid, lid in all_parcels
                if (tid, lid) not in scraped_parcels
            ]

            # Priority 2: Stale parcels
            stale_threshold = timezone.now() - timedelta(days=stale_days)
            stale = list(
                AttomData.objects.filter(
                    last_updated__lt=stale_threshold
                ).values_list('town_id', 'loc_id')
            )

            self.stdout.write(f"Unscraped parcels: {len(unscraped):,}")
            self.stdout.write(f"Stale parcels (>{stale_days} days): {len(stale):,}")

            # Combine: unscraped first, then stale
            parcels_to_scrape = unscraped[:batch_size]
            if len(parcels_to_scrape) < batch_size:
                remaining = batch_size - len(parcels_to_scrape)
                parcels_to_scrape.extend(stale[:remaining])

        # Limit to batch size
        parcels_to_scrape = parcels_to_scrape[:batch_size]

        self.stdout.write(self.style.WARNING(f"\nQueueing {len(parcels_to_scrape):,} parcels for scraping..."))

        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN - No tasks will be queued"))
            # Show sample
            sample = parcels_to_scrape[:5]
            self.stdout.write("\nSample parcels (first 5):")
            for tid, lid in sample:
                self.stdout.write(f"  - Town {tid}, LOC_ID {lid}")
            return

        # Queue scraping tasks
        from data_pipeline.jobs.task_queue import run_registry_task
        from data_pipeline.town_registry_map import get_registry_for_town

        queued = 0
        skipped = 0

        for town_id, loc_id in parcels_to_scrape:
            registry_id = get_registry_for_town(town_id)
            if registry_id:
                # Queue async Celery task
                run_registry_task.delay(
                    config={'registry_id': registry_id},
                    loc_id=f"{town_id}-{loc_id}",
                    force_refresh=True
                )
                queued += 1

                # Progress indicator every 100 parcels
                if queued % 100 == 0:
                    self.stdout.write(f"  Queued: {queued:,}...", ending="\r")
            else:
                skipped += 1

        self.stdout.write("\n")
        self.stdout.write(self.style.SUCCESS(f"✓ Queued {queued:,} scraping tasks"))

        if skipped > 0:
            self.stdout.write(self.style.WARNING(f"⚠ Skipped {skipped:,} parcels (no registry mapping)"))

        # Show progress
        total_to_scrape = total_parcels
        total_scraped_after = len(scraped_parcels) + queued
        progress_pct = (total_scraped_after / total_to_scrape) * 100 if total_to_scrape > 0 else 0

        self.stdout.write(f"\nProgress: {total_scraped_after:,}/{total_to_scrape:,} parcels ({progress_pct:.1f}%)")

        # Estimate completion
        if queued > 0 and total_scraped_after < total_to_scrape:
            weeks_remaining = (total_to_scrape - total_scraped_after) / queued
            self.stdout.write(f"Estimated weeks to full coverage: {int(weeks_remaining)}")

        self.stdout.write(self.style.SUCCESS("\n✓ Done"))
