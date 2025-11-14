"""
Management command to precompute and store all MassGIS parcel data in database.
Enables instant statewide parcel search without file I/O overhead.
"""
from __future__ import annotations

from datetime import datetime, date
from typing import Dict, List, Optional, Sequence
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

from ...models import MassGISParcel
from ...services import (
    MassGISDataError,
    _ensure_massgis_dataset,
    _load_assess_records,
    _normalize_loc_id,
    _should_replace_assess_record,
    get_massgis_catalog,
    _classify_use_code,
)


class Command(BaseCommand):
    help = "Precompute all MassGIS parcel data and store in database for instant search."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--town-id",
            action="append",
            type=int,
            dest="town_ids",
            help="Limit processing to specific town id(s). Can be repeated.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Rows per bulk upsert batch (default: 1000).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="For debugging – limit number of parcels per town.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Process data but skip database writes.",
        )
        parser.add_argument(
            "--north-shore",
            action="store_true",
            help="Process North Shore towns only (Salem, Beverly, Marblehead, etc.)",
        )

    def handle(self, *args, **options) -> None:
        town_ids = options.get("town_ids")
        batch_size = options["batch_size"]
        limit = options.get("limit")
        dry_run = options["dry_run"]
        north_shore = options["north_shore"]

        catalog = get_massgis_catalog()
        if not catalog:
            raise CommandError("MassGIS catalog is empty. Run parcel sync first.")

        # North Shore town IDs (approximate - you can adjust)
        NORTH_SHORE_TOWNS = {
            237,  # Salem
            46,   # Beverly
            164,  # Marblehead
            214,  # Peabody
            86,   # Danvers
            106,  # Gloucester
            242,  # Swampscott
            209,  # Nahant
            213,  # Lynn
            239,  # Saugus
            251,  # Revere
        }

        if north_shore:
            town_iterable = [catalog[tid] for tid in sorted(NORTH_SHORE_TOWNS) if tid in catalog]
            self.stdout.write(self.style.NOTICE(f"Processing {len(town_iterable)} North Shore towns"))
        elif town_ids:
            missing = [tid for tid in town_ids if tid not in catalog]
            if missing:
                raise CommandError(f"Unknown town ids requested: {missing}")
            town_iterable = [catalog[tid] for tid in town_ids]
        else:
            town_iterable = [catalog[key] for key in sorted(catalog.keys())]
            self.stdout.write(self.style.NOTICE(f"Processing ALL {len(town_iterable)} towns"))

        overall_saved = 0
        for town in town_iterable:
            self.stdout.write(self.style.NOTICE(f"[{town.town_id}] {town.name}…"))

            try:
                dataset_dir = _ensure_massgis_dataset(town)
            except MassGISDataError as exc:
                self.stderr.write(self.style.ERROR(f"  ✗ Failed to download dataset: {exc}"))
                continue

            records = _load_assess_records(str(dataset_dir))
            if not records:
                self.stderr.write(self.style.WARNING("  ⚠ No assessment records found."))
                continue

            deduped_records = self._dedupe_records(records)
            if limit:
                deduped_records = deduped_records[:limit]

            parcel_objects = self._build_parcel_objects(
                town_id=town.town_id,
                records=deduped_records,
            )

            if not parcel_objects:
                self.stderr.write(self.style.WARNING("  ⚠ No valid parcels to save."))
                continue

            saved = self._persist_parcels(
                parcels=parcel_objects,
                batch_size=batch_size,
                dry_run=dry_run,
            )
            overall_saved += saved

            self.stdout.write(
                self.style.SUCCESS(f"  ✓ {saved} parcels saved")
            )

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run complete; no data was persisted."))
        else:
            self.stdout.write(self.style.SUCCESS(f"Done. Persisted {overall_saved} parcels."))

    def _dedupe_records(self, records: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
        """Remove duplicate parcels, keeping the best version of each."""
        best: Dict[str, Dict[str, object]] = {}
        for record in records:
            loc_raw = record.get("LOC_ID") or record.get("PAR_ID") or record.get("PROP_ID")
            normalized = _normalize_loc_id(loc_raw)
            if not normalized:
                continue
            existing = best.get(normalized)
            if existing is None or _should_replace_assess_record(record, existing):
                best[normalized] = record
        return list(best.values())

    def _build_parcel_objects(
        self,
        town_id: int,
        records: List[Dict[str, object]],
    ) -> List[MassGISParcel]:
        """Convert raw assessment records to MassGISParcel objects."""
        parcels = []

        for record in records:
            loc_raw = record.get("LOC_ID") or record.get("PAR_ID") or record.get("PROP_ID")
            loc_id = _normalize_loc_id(loc_raw)
            if not loc_id:
                continue

            # Extract all fields
            parcel = MassGISParcel(
                town_id=town_id,
                loc_id=loc_id,

                # Address
                site_address=self._safe_str(record.get("SITE_ADDR") or record.get("MAIL_ADDRESS")),
                site_city=self._safe_str(record.get("SITE_CITY") or record.get("MAIL_CITY")),
                site_zip=self._safe_str(record.get("SITE_ZIP") or record.get("MAIL_ZIP")),

                # Owner
                owner_name=self._safe_str(record.get("OWN_NAME") or record.get("OWNER")),
                owner_address=self._safe_str(record.get("MAIL_ADDR") or record.get("MAIL_ADDRESS")),
                owner_city=self._safe_str(record.get("MAIL_CITY")),
                owner_state=self._safe_str(record.get("MAIL_STATE")),
                owner_zip=self._safe_str(record.get("MAIL_ZIP")),
                absentee=self._is_absentee(record),

                # Property classification
                use_code=self._safe_str(record.get("USE_CODE")),
                property_type=self._safe_str(record.get("PROP_TYPE") or record.get("USE_DESC")),
                property_category=_classify_use_code(record.get("USE_CODE")),
                style=self._safe_str(record.get("STYLE")),
                zoning=self._safe_str(record.get("ZONING")),

                # Financial
                total_value=self._safe_int(record.get("TOTAL_VAL") or record.get("FY25TOTAL")),
                land_value=self._safe_int(record.get("LAND_VAL") or record.get("FY25LAND")),
                building_value=self._safe_int(record.get("BLDG_VAL") or record.get("FY25BLDG")),

                # Physical
                lot_size=self._safe_float(record.get("LOT_SIZE")),
                lot_units=self._safe_str(record.get("LOT_UNITS")),
                living_area=self._safe_int(record.get("LS_AREA") or record.get("LIVING_AREA")),
                units=self._safe_int(record.get("UNITS") or record.get("NUM_UNITS")),
                bedrooms=self._safe_int(record.get("BEDROOMS") or record.get("NUM_ROOMS")),
                bathrooms=self._safe_float(record.get("BATHROOMS") or record.get("NUM_BATHS")),
                year_built=self._safe_int(record.get("YR_BUILT") or record.get("YEAR_BUILT")),

                # Sale info
                last_sale_date=self._safe_date(record.get("SALE_DATE")),
                last_sale_price=self._safe_int(record.get("SALE_PRICE")),

                # Computed fields
                equity_percent=self._calc_equity(record),
                years_owned=self._calc_years_owned(record),

                # Metadata
                fiscal_year=self._safe_str(record.get("FY") or record.get("FISCAL_YEAR")),
                data_source="massgis",
            )

            parcels.append(parcel)

        return parcels

    def _persist_parcels(
        self,
        parcels: List[MassGISParcel],
        batch_size: int,
        dry_run: bool,
    ) -> int:
        """Bulk upsert parcels to database."""
        if dry_run:
            return len(parcels)

        saved_count = 0
        for i in range(0, len(parcels), batch_size):
            batch = parcels[i:i + batch_size]

            # Use bulk_create with update_conflicts for upsert behavior
            with transaction.atomic():
                MassGISParcel.objects.bulk_create(
                    batch,
                    update_conflicts=True,
                    unique_fields=["town_id", "loc_id"],
                    update_fields=[
                        "site_address", "site_city", "site_zip",
                        "owner_name", "owner_address", "owner_city", "owner_state", "owner_zip",
                        "absentee", "use_code", "property_type", "property_category",
                        "style", "zoning", "total_value", "land_value", "building_value",
                        "lot_size", "lot_units", "living_area", "units", "bedrooms",
                        "bathrooms", "year_built", "last_sale_date", "last_sale_price",
                        "equity_percent", "years_owned", "fiscal_year", "data_source",
                        "last_updated",
                    ],
                )
            saved_count += len(batch)

        return saved_count

    # Helper methods for safe data extraction

    def _safe_str(self, value) -> Optional[str]:
        if value is None or value == "":
            return None
        return str(value).strip()[:500]  # Respect max_length

    def _safe_int(self, value) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            if isinstance(value, (int, float)):
                return int(value)
            return int(float(str(value).replace(",", "")))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            if isinstance(value, (int, float)):
                return float(value)
            return float(str(value).replace(",", ""))
        except (ValueError, TypeError):
            return None

    def _safe_date(self, value) -> Optional[date]:
        if value is None or value == "":
            return None
        try:
            if isinstance(value, date):
                return value
            if isinstance(value, str):
                # Try common date formats
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"]:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
        except (ValueError, TypeError):
            pass
        return None

    def _is_absentee(self, record: Dict[str, object]) -> bool:
        """Determine if owner is absentee (different mailing address)."""
        site_city = (record.get("SITE_CITY") or "").strip().upper()
        mail_city = (record.get("MAIL_CITY") or "").strip().upper()

        if not site_city or not mail_city:
            return False

        return site_city != mail_city

    def _calc_equity(self, record: Dict[str, object]) -> Optional[float]:
        """Calculate equity percentage if sale data available."""
        total_val = self._safe_int(record.get("TOTAL_VAL") or record.get("FY25TOTAL"))
        sale_price = self._safe_int(record.get("SALE_PRICE"))

        if not total_val or not sale_price or sale_price == 0:
            return None

        equity = ((total_val - sale_price) / sale_price) * 100
        return round(equity, 2)

    def _calc_years_owned(self, record: Dict[str, object]) -> Optional[float]:
        """Calculate years owned since last sale."""
        sale_date = self._safe_date(record.get("SALE_DATE"))

        if not sale_date:
            return None

        today = date.today()
        delta = today - sale_date
        return round(delta.days / 365.25, 2)
