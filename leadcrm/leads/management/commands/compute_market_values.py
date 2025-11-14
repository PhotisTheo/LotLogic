from __future__ import annotations

import math
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from statistics import mean
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from ...models import ParcelMarketValue
from ...valuation_engine import ParcelValuationEngine
from ...services import (
    MassGISDataError,
    _ensure_massgis_dataset,
    _load_assess_records,
    _normalize_loc_id,
    _should_replace_assess_record,
    get_massgis_catalog,
)


MODEL_VERSION = "hybrid-v1.0"


class Command(BaseCommand):
    help = "Precompute hybrid market values for MassGIS parcels."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--town-id",
            action="append",
            type=int,
            dest="town_ids",
            help="Limit processing to specific town id(s).",
        )
        parser.add_argument(
            "--lookback-days",
            type=int,
            default=365,
            help="Number of days of sale history to consider for comps (default: 365).",
        )
        parser.add_argument(
            "--target-comps",
            type=int,
            default=5,
            help="Ideal number of comparable sales to blend per parcel (default: 5).",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=500,
            help="Rows per bulk upsert batch (default: 500).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            help="For debugging – limit number of parcels per town.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Compute valuations but skip database writes.",
        )

    def handle(self, *args, **options) -> None:
        town_ids = options.get("town_ids")
        lookback_days = options["lookback_days"]
        target_comps = options["target_comps"]
        batch_size = options["batch_size"]
        limit = options.get("limit")
        dry_run = options["dry_run"]

        catalog = get_massgis_catalog()
        if not catalog:
            raise CommandError("MassGIS catalog is empty. Run parcel sync first.")

        if town_ids:
            missing = [tid for tid in town_ids if tid not in catalog]
            if missing:
                raise CommandError(f"Unknown town ids requested: {missing}")
            town_iterable = [catalog[tid] for tid in town_ids]
        else:
            town_iterable = [catalog[key] for key in sorted(catalog.keys())]

        engine = ParcelValuationEngine(
            lookback_days=lookback_days,
            target_comp_count=target_comps,
        )

        overall_saved = 0
        for town in town_iterable:
            self.stdout.write(self.style.NOTICE(f"[{town.town_id}] Preparing dataset for {town.name}…"))
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

            clean_records = engine.build_clean_records(deduped_records)
            if not clean_records:
                self.stderr.write(self.style.WARNING("  ⚠ No usable parcel records after cleaning."))
                continue

            valuations, model, stats = engine.compute(clean_records)
            if not valuations:
                self.stderr.write(self.style.WARNING("  ⚠ Engine returned no valuations."))
                continue

            saved = self._persist_results(
                town_id=town.town_id,
                valuations=valuations,
                batch_size=batch_size,
                dry_run=dry_run,
            )
            overall_saved += saved

            r2_display = f"{(model.r2 if model else 0.0):.2f}" if model else "n/a"
            avg_value = self._safe_mean([val.market_value for val in valuations])
            median_value = self._safe_median([val.market_value for val in valuations])
            self.stdout.write(
                self.style.SUCCESS(
                    "  ✓ {saved} valuations | model r² {r2} | avg ${avg_val} | median ${median}".format(
                        saved=saved,
                        r2=r2_display,
                        avg_val=f"{avg_value:,.0f}" if avg_value else "n/a",
                        median=f"{median_value:,.0f}" if median_value else "n/a",
                    )
                )
            )

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run complete; no data was persisted."))
        else:
            self.stdout.write(self.style.SUCCESS(f"Done. Persisted {overall_saved} market values."))

    def _dedupe_records(self, records: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
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

    def _persist_results(
        self,
        *,
        town_id: int,
        valuations: Sequence,
        batch_size: int,
        dry_run: bool,
    ) -> int:
        if dry_run:
            return sum(1 for val in valuations if val.market_value is not None)

        valued_at = timezone.now()
        total_saved = 0
        buffer: List[ParcelMarketValue] = []
        update_fields = [
            "market_value",
            "market_value_per_sqft",
            "comparable_value",
            "comparable_count",
            "comparable_avg_psf",
            "hedonic_value",
            "hedonic_r2",
            "valuation_confidence",
            "methodology",
            "model_version",
            "valued_at",
            "payload",
            "updated_at",
        ]

        for valuation in valuations:
            if valuation.market_value is None:
                continue

            payload = {
                "comps": [comp.as_payload() for comp in valuation.comps],
                "inputs": valuation.inputs,
                "hedonic": {
                    "value": valuation.hedonic_value,
                    "r2": valuation.hedonic_r2,
                },
            }

            record = ParcelMarketValue(
                town_id=town_id,
                loc_id=valuation.loc_id,
                market_value=_decimal(valuation.market_value),
                market_value_per_sqft=_decimal(valuation.market_value_per_sqft),
                comparable_value=_decimal(valuation.comparable_value),
                comparable_count=valuation.comparable_count,
                comparable_avg_psf=_decimal(valuation.comparable_avg_psf),
                hedonic_value=_decimal(valuation.hedonic_value),
                hedonic_r2=valuation.hedonic_r2,
                valuation_confidence=valuation.confidence,
                methodology=ParcelMarketValue.METHODOLOGY_HYBRID_V1,
                model_version=MODEL_VERSION,
                valued_at=valued_at,
                payload=payload,
            )
            record.created_at = valued_at
            record.updated_at = valued_at
            buffer.append(record)

            if len(buffer) >= batch_size:
                total_saved += self._bulk_upsert(buffer, update_fields)
                buffer = []

        if buffer:
            total_saved += self._bulk_upsert(buffer, update_fields)

        return total_saved

    def _bulk_upsert(self, objects: Sequence[ParcelMarketValue], update_fields: Sequence[str]) -> int:
        ParcelMarketValue.objects.bulk_create(
            objects,
            batch_size=len(objects),
            update_conflicts=True,
            unique_fields=["town_id", "loc_id"],
            update_fields=list(update_fields),
        )
        return len(objects)

    def _safe_mean(self, values: Sequence[Optional[float]]) -> Optional[float]:
        filtered = [value for value in values if value]
        return mean(filtered) if filtered else None

    def _safe_median(self, values: Sequence[Optional[float]]) -> Optional[float]:
        filtered = sorted(value for value in values if value)
        if not filtered:
            return None
        mid = len(filtered) // 2
        if len(filtered) % 2:
            return filtered[mid]
        return (filtered[mid - 1] + filtered[mid]) / 2


def _decimal(value: Optional[float]) -> Optional[Decimal]:
    if value is None or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    quantized = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return quantized
