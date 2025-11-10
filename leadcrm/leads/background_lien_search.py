"""
Background worker for searching liens and legal actions.

This module provides a background thread pool for searching CourtListener
and other sources without blocking the main request thread.

Rate limits:
- CourtListener: 5,000 queries/hour (authenticated)
- This is ~83 queries/minute, plenty for real-time searches
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from datetime import date, datetime, timedelta
from django.utils import timezone
from django.db import OperationalError, transaction

from .models import LienRecord, LegalAction, LienSearchAttempt
from .lien_legal_service import search_all_sources

logger = logging.getLogger(__name__)

# Global thread pool for background searches (lazy init)
_search_executor: Optional[ThreadPoolExecutor] = None
_executor_lock = threading.Lock()

# Track which parcels are currently being searched to avoid duplicates
_active_searches = set()
_active_searches_lock = threading.Lock()


def _values_differ(current_value, new_value) -> bool:
    """Helper that compares model values, normalising dates to ISO strings."""
    if isinstance(current_value, (datetime, date)):
        current_value = current_value.isoformat()
    if isinstance(new_value, (datetime, date)):
        new_value = new_value.isoformat()
    return current_value != new_value


def ensure_legal_action_record(user, town_id: int, loc_id: str, action_data: dict) -> tuple[bool, bool]:
    """
    Ensure the given legal action data is stored for the parcel.

    Returns:
        (created, updated) tuple indicating whether a new record was created or an existing
        shared record was updated.
    """
    case_number = action_data.get("case_number")
    if not case_number or case_number == "MANUAL_LOOKUP":
        return False, False

    source = (action_data.get("source") or "").strip()
    shared_source = source.lower() == "courtlistener"

    lookup_kwargs = {
        "town_id": town_id,
        "loc_id": loc_id,
        "case_number": case_number,
    }
    if source:
        lookup_kwargs["source"] = source

    queryset = LegalAction.objects.filter(**lookup_kwargs)
    if not shared_source:
        queryset = queryset.filter(created_by=user)

    existing = queryset.first()

    field_values = {
        "action_type": action_data.get("action_type", "other"),
        "status": action_data.get("status", "pending"),
        "court": action_data.get("court", "other"),
        "plaintiff": action_data.get("plaintiff", ""),
        "defendant": action_data.get("defendant", ""),
        "filing_date": action_data.get("filing_date"),
        "description": action_data.get("description", ""),
        "source_url": action_data.get("source_url", ""),
        "pacer_case_id": action_data.get("pacer_case_id", ""),
    }

    notes_value = action_data.get("notes")
    if not notes_value and shared_source:
        notes_value = "Auto-imported from CourtListener"

    if existing:
        changed_fields = []
        for field, new_value in field_values.items():
            if _values_differ(getattr(existing, field), new_value):
                setattr(existing, field, new_value)
                changed_fields.append(field)

        if notes_value and not existing.notes:
            existing.notes = notes_value
            changed_fields.append("notes")

        if changed_fields:
            changed_fields.append("updated_at")
            existing.save(update_fields=changed_fields)
            return False, True
        return False, False

    create_kwargs = {
        **lookup_kwargs,
        **field_values,
        "created_by": user,
        "notes": notes_value or "",
    }

    if "source" not in create_kwargs:
        create_kwargs["source"] = source

    LegalAction.objects.create(**create_kwargs)
    return True, False


def search_parcel_background(user, town_id: int, loc_id: str, parcel_data: dict, *, force: bool = False):
    """
    Queue a background search for liens and legal actions on a parcel.

    Args:
        user: Django user object (for saving results)
        town_id: Town ID
        loc_id: Parcel LOC_ID
        parcel_data: Dictionary with owner_name, address, town_name, county

    Returns:
        True if search was queued, False if already in progress
    """
    # Create unique key for this parcel
    search_key = f"{user.id}:{town_id}:{loc_id}"

    # Check if already searching this parcel
    with _active_searches_lock:
        if search_key in _active_searches:
            logger.debug(f"Search already in progress for {town_id}/{loc_id}")
            return False
        _active_searches.add(search_key)

    # Submit to thread pool
    try:
        executor = _get_search_executor()
        future = executor.submit(
            _perform_parcel_search,
            user,
            town_id,
            loc_id,
            parcel_data,
            search_key
        )
    except RuntimeError as e:
        # Recreate executor if it was shut down (e.g., during Django reload)
        logger.warning("Background search executor unavailable; recreating thread pool")
        try:
            executor = _restart_search_executor()
            future = executor.submit(
                _perform_parcel_search,
                user,
                town_id,
                loc_id,
                parcel_data,
                search_key
            )
        except (RuntimeError, Exception) as e:
            # If still failing, interpreter is shutting down - gracefully skip
            logger.warning(f"Cannot schedule search for {town_id}/{loc_id}: {type(e).__name__}: {e}")
            with _active_searches_lock:
                _active_searches.discard(search_key)
            return False

    # Add callback to remove from active searches when done
    try:
        future.add_done_callback(lambda f: _search_complete(search_key, f))
    except RuntimeError:
        # If callback registration fails, just clean up and continue
        logger.warning(f"Cannot register callback for {town_id}/{loc_id}: future already done")
        with _active_searches_lock:
            _active_searches.discard(search_key)

    logger.info(f"Queued background search for {town_id}/{loc_id} (owner: {parcel_data.get('owner_name', 'Unknown')}), force={force}")
    return True


def _perform_parcel_search(user, town_id: int, loc_id: str, parcel_data: dict, search_key: str):
    """
    Perform the actual search (runs in background thread).

    Args:
        user: Django user object
        town_id: Town ID
        loc_id: Parcel LOC_ID
        parcel_data: Dictionary with owner_name, address, town_name, county
        search_key: Unique key for tracking
    """
    try:
        owner_name = parcel_data.get('owner_name', '')
        address = parcel_data.get('address', '')
        town_name = parcel_data.get('town_name', '')
        county = parcel_data.get('county', '')

        if not owner_name:
            logger.debug(f"Skipping search for {town_id}/{loc_id} - no owner name")
            return

        logger.info(f"Starting background search for {town_id}/{loc_id} - {owner_name}")

        # Search all sources
        legal_actions_found, liens_found = search_all_sources(
            owner_name=owner_name,
            address=address,
            town_name=town_name,
            county=county
        )

        # Save legal actions (only real data, skip manual lookup placeholders)
        saved_actions = 0
        updated_actions = 0
        for action_data in legal_actions_found:
            created, updated = ensure_legal_action_record(user, town_id, loc_id, action_data)
            if created:
                saved_actions += 1
            elif updated:
                updated_actions += 1

        # Save liens (only real data with recording dates, amounts, or book numbers)
        saved_liens = 0
        for lien_data in liens_found:
            # Skip generic guidance records
            source = lien_data.get("source", "")
            if "Manual Lookup" in source or "Check these sources" in lien_data.get("notes", ""):
                continue

            # Only save if it has real identifying information
            if not (lien_data.get("recording_date") or lien_data.get("amount") or lien_data.get("book_number")):
                continue

            # Create real lien record
            if not LienRecord.objects.filter(
                created_by=user,
                town_id=town_id,
                loc_id=loc_id,
                source=source
            ).exists():
                LienRecord.objects.create(
                    created_by=user,
                    town_id=town_id,
                    loc_id=loc_id,
                    lien_type=lien_data.get("lien_type", "other"),
                    status=lien_data.get("status", "active"),
                    lien_holder=lien_data.get("lien_holder", ""),
                    amount=lien_data.get("amount"),
                    recording_date=lien_data.get("recording_date"),
                    source=source,
                    source_url=lien_data.get("source_url", ""),
                    notes=lien_data.get("notes", "")
                )
                saved_liens += 1

        if saved_actions > 0 or saved_liens > 0:
            logger.info(f"✓ Saved {saved_actions} legal action(s) and {saved_liens} lien(s) for {town_id}/{loc_id}")
        elif updated_actions > 0:
            logger.info(f"↻ Updated {updated_actions} existing legal action(s) for {town_id}/{loc_id}")
        else:
            logger.debug(f"No liens/legal actions found for {town_id}/{loc_id}")

        # Record that we searched this parcel (update or create)
        # This prevents re-searching the same parcel within the cache period (90 days)
        _record_search_attempt_with_retry(
            user=user,
            town_id=town_id,
            loc_id=loc_id,
            found_liens=saved_liens > 0,
            found_legal_actions=(saved_actions + updated_actions) > 0,
        )

    except Exception as e:
        logger.error(f"Background search failed for {town_id}/{loc_id}: {e}", exc_info=True)


def _search_complete(search_key: str, future):
    """
    Callback when a search completes.

    Args:
        search_key: Unique key for the search
        future: Future object from ThreadPoolExecutor
    """
    # Remove from active searches
    with _active_searches_lock:
        _active_searches.discard(search_key)

    # Check for exceptions
    try:
        future.result()
    except Exception as e:
        logger.error(f"Background search exception for {search_key}: {e}", exc_info=True)


def _record_search_attempt_with_retry(*, user, town_id: int, loc_id: str, found_liens: bool, found_legal_actions: bool, retries: int = 3, delay: float = 0.25) -> None:
    for attempt in range(retries):
        try:
            with transaction.atomic():
                LienSearchAttempt.objects.update_or_create(
                    created_by=user,
                    town_id=town_id,
                    loc_id=loc_id,
                    defaults={
                        'found_liens': found_liens,
                        'found_legal_actions': found_legal_actions,
                    }
                )
            logger.debug(f"Recorded search attempt for {town_id}/{loc_id}")
            return
        except OperationalError as exc:
            is_locked = 'database is locked' in str(exc).lower()
            if not is_locked or attempt == retries - 1:
                logger.warning(
                    "Failed to record lien search attempt for %s/%s: %s",
                    town_id,
                    loc_id,
                    exc,
                )
                return
            time.sleep(delay * (attempt + 1))


def _get_search_executor() -> ThreadPoolExecutor:
    """
    Return an active ThreadPoolExecutor, recreating as needed.

    Raises:
        RuntimeError: If the interpreter is shutting down and cannot create executor
    """
    global _search_executor
    with _executor_lock:
        if _search_executor is None or getattr(_search_executor, "_shutdown", False):
            try:
                _search_executor = ThreadPoolExecutor(
                    max_workers=10, thread_name_prefix="lien_search"
                )
            except RuntimeError:
                # Interpreter is shutting down, can't create executor
                logger.warning("Cannot create ThreadPoolExecutor: interpreter shutdown")
                raise
    return _search_executor


def _restart_search_executor() -> ThreadPoolExecutor:
    """
    Force creation of a new executor, shutting down the old one.

    Raises:
        RuntimeError: If the interpreter is shutting down and cannot create executor
    """
    global _search_executor
    with _executor_lock:
        old_executor = _search_executor
        try:
            _search_executor = ThreadPoolExecutor(
                max_workers=10, thread_name_prefix="lien_search"
            )
        except RuntimeError:
            # Interpreter is shutting down, can't create executor
            logger.warning("Cannot restart ThreadPoolExecutor: interpreter shutdown")
            raise
    if old_executor and not getattr(old_executor, "_shutdown", False):
        try:
            old_executor.shutdown(wait=False)
        except Exception as e:
            logger.debug(f"Error shutting down old executor: {e}")
    return _search_executor


def should_search_parcel(user, town_id: int, loc_id: str) -> bool:
    """
    Check if a parcel needs to be searched.

    This checks if we've searched this parcel recently (within 90 days).
    It looks at LienSearchAttempt records first, which track all search attempts
    regardless of whether liens/actions were found.

    Args:
        user: Django user object
        town_id: Town ID
        loc_id: Parcel LOC_ID

    Returns:
        True if parcel should be searched, False otherwise
    """
    cutoff = timezone.now() - timedelta(days=90)

    # If any recent search attempt exists for this parcel, skip re-querying.
    if LienSearchAttempt.objects.filter(
        town_id=town_id,
        loc_id=loc_id,
        searched_at__gte=cutoff,
    ).exists():
        logger.debug(f"Skipping {town_id}/{loc_id} - shared search attempt within cache window")
        return False

    # If we already have fresh CourtListener data cached for this parcel, do not re-search.
    if LegalAction.objects.filter(
        town_id=town_id,
        loc_id=loc_id,
        source__iexact="CourtListener",
        updated_at__gte=cutoff,
    ).exists():
        logger.debug(f"Skipping {town_id}/{loc_id} - CourtListener cache still fresh")
        return False

    # First check if we have a recent search attempt (most efficient check)
    # This prevents re-searching parcels that have no liens/actions
    try:
        search_attempt = LienSearchAttempt.objects.get(
            created_by=user,
            town_id=town_id,
            loc_id=loc_id
        )

        # Check if search attempt is within cache period (90 days)
        if search_attempt.searched_at >= cutoff:
            # Recent search found, don't search again
            logger.debug(f"Skipping {town_id}/{loc_id} - searched {search_attempt.searched_at}")
            return False
        else:
            # Old search attempt, can search again
            logger.debug(f"Re-searching {town_id}/{loc_id} - last search was {search_attempt.searched_at}")
            return True

    except LienSearchAttempt.DoesNotExist:
        # No search attempt record exists, should search
        logger.debug(f"No search attempt for {town_id}/{loc_id} - will search")
        return True
