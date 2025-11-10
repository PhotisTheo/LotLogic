import json
from typing import List, Optional

from django.core.management.base import BaseCommand
from django.db.utils import NotSupportedError

from accounts.models import get_workspace_owner
from leads.models import Lead, SavedParcelList, SkipTraceRecord


def _normalize_loc_id(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _collect_phone_payload(lead: Lead) -> List[dict]:
    phones: List[dict] = []
    for index in range(1, 4):
        number = getattr(lead, f"phone_{index}", None)
        if not number:
            continue
        phones.append(
            {
                "number": number,
                "type": None,
                "score": None,
                "dnc": getattr(lead, f"dnc_{index}", None),
            }
        )
    return phones


def _first_saved_list_town_id(loc_id: Optional[str], *, created_by) -> Optional[int]:
    normalized = _normalize_loc_id(loc_id)
    if not normalized:
        return None

    base_qs = SavedParcelList.objects.all()
    if created_by is not None:
        base_qs = base_qs.filter(created_by=created_by)
    try:
        match = (
            base_qs.filter(loc_ids__contains=[loc_id])
            .values_list("town_id", flat=True)
            .first()
        )
        if match is not None:
            return match
        if normalized != loc_id:
            match = (
                base_qs.filter(loc_ids__contains=[normalized])
                .values_list("town_id", flat=True)
                .first()
            )
            if match is not None:
                return match
    except NotSupportedError:
        pass

    for saved_list in base_qs.iterator():
        loc_ids = saved_list.loc_ids or []
        for entry in loc_ids:
            if _normalize_loc_id(entry) == normalized:
                return saved_list.town_id
    return None


class Command(BaseCommand):
    help = "Backfill SkipTraceRecord entries using existing lead contact details."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would change without writing to the database.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        created = 0
        skipped = 0

        leads = Lead.objects.all().order_by("pk")
        for lead in leads:
            owner = get_workspace_owner(lead.created_by)
            if owner is None:
                skipped += 1
                continue

            loc_id_candidates: List[str] = []
            if lead.loc_id:
                loc_id_candidates.append(lead.loc_id)
            loc_id_candidates.append(f"LEAD-{lead.pk}")

            normalized_candidates = []
            for candidate in loc_id_candidates:
                normalized = _normalize_loc_id(candidate)
                if normalized and normalized not in normalized_candidates:
                    normalized_candidates.append(normalized)

            if not normalized_candidates:
                skipped += 1
                continue

            phones = _collect_phone_payload(lead)
            email = lead.email or ""

            if not phones and not email:
                skipped += 1
                continue

            existing_record = SkipTraceRecord.objects.filter(
                created_by=owner,
                loc_id__in=normalized_candidates,
            ).order_by("-updated_at").first()
            if existing_record:
                skipped += 1
                continue

            town_id: Optional[int] = None
            if lead.loc_id:
                town_id = _first_saved_list_town_id(lead.loc_id, created_by=owner)

            payload = {
                "owner_name": lead.owner_name or lead.mailing_owner or "",
                "email": email,
                "phones": phones,
                "raw_payload": {
                    "source": "lead_backfill",
                    "lead_id": lead.pk,
                },
            }

            if dry_run:
                created += 1
                continue

            primary_loc_id = normalized_candidates[0]
            SkipTraceRecord.objects.update_or_create(
                created_by=owner,
                town_id=town_id,
                loc_id=primary_loc_id,
                defaults=payload,
            )

            for alias in normalized_candidates[1:]:
                SkipTraceRecord.objects.update_or_create(
                    created_by=owner,
                    town_id=None,
                    loc_id=alias,
                    defaults=payload,
                )

            created += 1

        summary = {
            "created": created,
            "skipped": skipped,
            "dry_run": dry_run,
        }
        self.stdout.write(json.dumps(summary, indent=2))
