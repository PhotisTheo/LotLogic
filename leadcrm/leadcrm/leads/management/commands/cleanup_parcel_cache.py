"""
Django management command to clean up expired MassGIS parcel cache entries.

Usage:
    python manage.py cleanup_parcel_cache
    python manage.py cleanup_parcel_cache --dry-run
"""
from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from leads.models import MassGISParcelCache


class Command(BaseCommand):
    help = 'Clean up MassGIS parcel cache entries older than 90 days'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting',
        )
        parser.add_argument(
            '--days',
            type=int,
            default=90,
            help='Age in days after which cache entries are considered expired (default: 90)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        days = options['days']

        expiry_date = timezone.now() - timedelta(days=days)

        self.stdout.write(
            self.style.NOTICE(f'Finding cache entries not accessed since {expiry_date.date()}...')
        )

        expired_entries = MassGISParcelCache.objects.filter(last_accessed__lt=expiry_date)
        count = expired_entries.count()

        if count == 0:
            self.stdout.write(self.style.SUCCESS('No expired cache entries found.'))
            return

        if dry_run:
            self.stdout.write(
                self.style.WARNING(f'DRY RUN: Would delete {count} expired cache entries')
            )

            # Show sample of what would be deleted
            sample_size = min(10, count)
            self.stdout.write(f'\nSample of entries that would be deleted (showing {sample_size}):')
            for entry in expired_entries[:sample_size]:
                days_old = (timezone.now() - entry.last_accessed).days
                self.stdout.write(
                    f'  - Town {entry.town_id}, LOC_ID {entry.loc_id} '
                    f'(last accessed {days_old} days ago)'
                )

            if count > sample_size:
                self.stdout.write(f'  ... and {count - sample_size} more')
        else:
            self.stdout.write(
                self.style.WARNING(f'Deleting {count} expired cache entries...')
            )

            deleted_count, _ = expired_entries.delete()

            self.stdout.write(
                self.style.SUCCESS(f'Successfully deleted {deleted_count} expired cache entries.')
            )

        # Show cache statistics
        self.stdout.write('\n' + self.style.NOTICE('Cache Statistics:'))
        total_entries = MassGISParcelCache.objects.count()
        self.stdout.write(f'  Total cache entries: {total_entries}')

        if total_entries > 0:
            # Group by town to show distribution
            from django.db.models import Count
            town_stats = MassGISParcelCache.objects.values('town_id').annotate(
                count=Count('town_id')
            ).order_by('-count')[:10]

            self.stdout.write('\n  Top 10 towns by cache entries:')
            for stat in town_stats:
                self.stdout.write(f'    Town {stat["town_id"]}: {stat["count"]} entries')
