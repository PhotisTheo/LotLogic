"""
Django management command to enrich parcel data using the free scraping pipeline
instead of the paid ATTOM API.

Usage:
    python manage.py enrich_parcel_scraped --town-id 35 --loc-id 0101234000
    python manage.py enrich_parcel_scraped --town-id 35 --loc-id 0101234000 --force
"""

import logging
from django.core.management.base import BaseCommand
from leads.models import Town
from data_pipeline.jobs.registry_job import RegistryJob
from data_pipeline.town_registry_map import get_registry_for_town

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Enrich parcel data using free scraping pipeline (ATTOM replacement)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--town-id',
            type=int,
            required=True,
            help='Town ID (e.g., 35 for Boston)'
        )
        parser.add_argument(
            '--loc-id',
            type=str,
            required=True,
            help='Location ID / Parcel ID (e.g., 0101234000)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force refresh even if data exists and is recent'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Test scraping without saving to database'
        )

    def handle(self, *args, **options):
        town_id = options['town_id']
        loc_id = options['loc_id']
        force_refresh = options['force']
        dry_run = options['dry_run']

        # Get town info
        try:
            town = Town.objects.get(town_id=town_id)
        except Town.DoesNotExist:
            self.stdout.write(self.style.ERROR(f'Town ID {town_id} not found'))
            return

        self.stdout.write(f'\n{"="*60}')
        self.stdout.write(self.style.SUCCESS(f'Enriching Parcel Data via Free Scraping Pipeline'))
        self.stdout.write(f'{"="*60}')
        self.stdout.write(f'Town: {town.name} (ID: {town_id})')
        self.stdout.write(f'Parcel: {loc_id}')
        self.stdout.write(f'Force Refresh: {force_refresh}')
        self.stdout.write(f'Dry Run: {dry_run}')
        self.stdout.write(f'{"="*60}\n')

        # Determine which registry to use
        registry_id = get_registry_for_town(town_id)
        if not registry_id:
            self.stdout.write(
                self.style.ERROR(f'No registry mapping found for {town.name} (ID: {town_id})')
            )
            self.stdout.write(
                self.style.WARNING('This town may not be supported yet in the scraping pipeline.')
            )
            return

        self.stdout.write(f'📍 Registry: {registry_id}')

        # Initialize registry job
        try:
            job = RegistryJob(
                registry_id=registry_id,
                dry_run=dry_run
            )
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Failed to initialize registry job: {e}'))
            return

        # Run the scraper
        self.stdout.write(f'\n🔍 Searching registry for parcel {loc_id}...\n')

        try:
            results = job.run(
                loc_id=loc_id,
                force_refresh=force_refresh,
                max_cache_age_days=90 if not force_refresh else 0
            )

            if not results:
                self.stdout.write(self.style.WARNING('No results found from registry'))
                return

            # Display results
            self.stdout.write(f'\n{"="*60}')
            self.stdout.write(self.style.SUCCESS(f'✓ Found {len(results)} records'))
            self.stdout.write(f'{"="*60}\n')

            for idx, result in enumerate(results, 1):
                self.stdout.write(f'\nRecord #{idx}:')
                self.stdout.write(f'  Document Type: {result.get("instrument_type", "N/A")}')
                self.stdout.write(f'  Recording Date: {result.get("document_date", "N/A")}')
                self.stdout.write(f'  Book/Page: {result.get("book", "N/A")} / {result.get("page", "N/A")}')

                # Show parsed mortgage data if available
                if result.get('parsed_data'):
                    parsed = result['parsed_data']
                    self.stdout.write(f'\n  📋 Parsed Mortgage Data:')
                    if parsed.get('loan_amount'):
                        self.stdout.write(f'     Loan Amount: ${parsed["loan_amount"]:,.2f}')
                    if parsed.get('lender_name'):
                        self.stdout.write(f'     Lender: {parsed["lender_name"]}')
                    if parsed.get('interest_rate'):
                        self.stdout.write(f'     Interest Rate: {parsed["interest_rate"]}%')
                    if parsed.get('loan_term_months'):
                        years = parsed["loan_term_months"] // 12
                        self.stdout.write(f'     Term: {years} years ({parsed["loan_term_months"]} months)')

                # Show document path if downloaded
                if result.get('document_path'):
                    self.stdout.write(f'  📄 Document: {result["document_path"]}')

            if dry_run:
                self.stdout.write(f'\n{"="*60}')
                self.stdout.write(self.style.WARNING('DRY RUN - No data saved to database'))
                self.stdout.write(f'{"="*60}\n')
            else:
                self.stdout.write(f'\n{"="*60}')
                self.stdout.write(self.style.SUCCESS('✓ Data saved to AttomData model'))
                self.stdout.write(f'{"="*60}\n')

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'\n❌ Error during scraping: {e}'))
            logger.exception('Registry scraping failed')
            import traceback
            traceback.print_exc()
            return

        self.stdout.write(self.style.SUCCESS('\n✓ Enrichment complete!\n'))
