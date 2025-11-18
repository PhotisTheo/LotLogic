"""
Scheduled tasks runner for Railway cron service.
Runs market values computation on a schedule.
"""
from __future__ import annotations

import time
from datetime import datetime, time as dt_time
from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone


class Command(BaseCommand):
    help = "Run scheduled tasks in a loop (for Railway cron service)"

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--run-hour",
            type=int,
            default=2,
            help="Hour of day to run market values compute (0-23, default: 2 = 2 AM UTC)",
        )
        parser.add_argument(
            "--lookback-days",
            type=int,
            default=365,
            help="Number of days of sale history for comps (default: 365)",
        )
        parser.add_argument(
            "--target-comps",
            type=int,
            default=5,
            help="Target number of comps per parcel (default: 5)",
        )

    def handle(self, *args, **options) -> None:
        run_hour = options["run_hour"]
        lookback_days = options["lookback_days"]
        target_comps = options["target_comps"]

        last_run_date = None

        self.stdout.write(
            self.style.SUCCESS(
                f"Scheduled tasks service started. Will run market values compute daily at {run_hour:02d}:00 UTC"
            )
        )

        while True:
            now = timezone.now()
            current_date = now.date()
            current_hour = now.hour

            # Check if it's time to run (at the specified hour, once per day)
            if current_hour == run_hour and last_run_date != current_date:
                self.stdout.write(
                    self.style.NOTICE(
                        f"[{now.isoformat()}] Starting market values computation..."
                    )
                )

                try:
                    call_command(
                        "compute_market_values",
                        lookback_days=lookback_days,
                        target_comps=target_comps,
                        batch_size=500,
                    )

                    last_run_date = current_date
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"[{timezone.now().isoformat()}] Market values computation completed successfully"
                        )
                    )
                except Exception as e:
                    self.stderr.write(
                        self.style.ERROR(
                            f"[{timezone.now().isoformat()}] Market values computation failed: {e}"
                        )
                    )
                    # Don't update last_run_date so it will retry next hour

            # Sleep for 30 minutes before checking again
            time.sleep(1800)
