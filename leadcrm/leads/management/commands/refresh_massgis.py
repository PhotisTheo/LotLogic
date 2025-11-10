from django.core.management.base import BaseCommand, CommandError

from leads import services


class Command(BaseCommand):
    help = "Refresh cached MassGIS parcel datasets."

    def add_arguments(self, parser):
        parser.add_argument(
            "--town",
            action="append",
            type=int,
            dest="towns",
            help="MassGIS town ID to refresh (can be provided multiple times).",
        )
        parser.add_argument(
            "--all",
            action="store_true",
            dest="all_towns",
            help="Refresh every town listed in the MassGIS catalog (can take a while).",
        )
        parser.add_argument(
            "--stale-days",
            type=int,
            default=30,
            help="Refresh datasets older than this many days (ignored when --force is used). Default: 30.",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force a re-download even if the cached dataset appears up to date.",
        )
        parser.add_argument(
            "--skip-remote-check",
            action="store_true",
            help="Skip the remote Last-Modified HEAD request (use only staleness / force criteria).",
        )

    def handle(self, *args, **options):
        towns = options.get("towns") or []
        all_towns = options.get("all_towns")
        stale_days = options["stale_days"]
        force = options["force"]
        use_remote = not options["skip_remote_check"]

        catalog = services.get_massgis_catalog()

        if not towns and not all_towns:
            raise CommandError("Provide at least one --town or use --all to refresh the entire catalog.")

        if all_towns:
            town_ids = sorted(catalog.keys())
        else:
            town_ids = []
            for raw_id in towns:
                if raw_id not in catalog:
                    raise CommandError(f"Town id {raw_id} is not present in the MassGIS catalog.")
                town_ids.append(raw_id)

        refreshed_count = 0
        skipped_count = 0

        for town_id in town_ids:
            town = catalog[town_id]
            refreshed, reason = services.refresh_massgis_dataset(
                town,
                force=force,
                stale_after_days=None if force else stale_days,
                use_remote_headers=use_remote,
            )
            if refreshed:
                refreshed_count += 1
                self.stdout.write(self.style.SUCCESS(f"{town.name}: refreshed ({reason})"))
            else:
                skipped_count += 1
                self.stdout.write(f"{town.name}: up-to-date ({reason})")

        summary = f"Refreshed {refreshed_count} dataset(s); {skipped_count} already up to date."
        if refreshed_count:
            self.stdout.write(self.style.SUCCESS(summary))
        else:
            self.stdout.write(summary)
