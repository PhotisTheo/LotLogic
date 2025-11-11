"""
Corporate entity lookup job orchestrator.

Takes a corporate entity name (usually from GIS parcel owner data),
scrapes MA Secretary of Commonwealth, and extracts owner/manager info.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..sources.corporate.ma_secretary import MASecretarySource
from ..storage import database
from ..settings import pipeline_settings


CORPORATE_ADAPTERS = {
    "ma_secretary": MASecretarySource,
}


@dataclass
class CorporateJob:
    """
    Job to look up corporate entity information from state filings.
    """
    config: Dict[str, Any]

    def _build_adapter(self):
        adapter_key = self.config.get("adapter", "ma_secretary")
        adapter_cls = CORPORATE_ADAPTERS.get(adapter_key)
        if not adapter_cls:
            raise ValueError(f"No corporate adapter registered for key '{adapter_key}'")
        return adapter_cls(self.config, pipeline_settings)

    def run(
        self,
        entity_name: str,
        dry_run: bool = False,
        force_refresh: bool = False,
        max_cache_age_days: int = 180,
    ) -> Optional[Dict[str, Any]]:
        """
        Look up corporate entity information.

        Args:
            entity_name: Legal name of the entity (e.g., "ABC REALTY LLC")
            dry_run: If True, fetch data but skip database writes
            force_refresh: If True, force scrape even if cache is fresh
            max_cache_age_days: Maximum cache age in days (default: 180)

        Returns:
            Dict with entity info if found, None otherwise
        """
        adapter = self._build_adapter()

        # Check cache freshness before scraping
        if not force_refresh:
            is_fresh, entity_id = database.check_corporate_cache_freshness(
                entity_name=entity_name,
                max_age_days=max_cache_age_days
            )
            if is_fresh:
                adapter.logger.info(
                    f"Skipping scrape for entity_name='{entity_name}' - cache is fresh (< {max_cache_age_days} days old). "
                    f"Use --force-refresh to override."
                )
                # Return cached data
                from leads.models import CorporateEntity
                cached = CorporateEntity.objects.get(id=entity_id)
                return {
                    'entity_name': cached.entity_name,
                    'entity_id': cached.entity_id,
                    'principal_name': cached.principal_name,
                    'principal_title': cached.principal_title,
                    'business_phone': cached.business_phone,
                    'business_address': cached.business_address,
                    'status': cached.status,
                }

        # Scrape entity data
        results = adapter.search(entity_name=entity_name)

        if not results:
            adapter.logger.info(f"No corporate records found for '{entity_name}'.")
            return None

        if len(results) > 1:
            adapter.logger.warning(
                f"Found {len(results)} entities matching '{entity_name}'. "
                f"Using the first active match."
            )
            # Prefer active entities
            active_results = [r for r in results if r.status and r.status.lower() == 'active']
            record = active_results[0] if active_results else results[0]
        else:
            record = results[0]

        adapter.logger.info(f"Found entity: {record.entity_name} ({record.entity_id})")
        if record.principal_name:
            adapter.logger.info(f"  Principal: {record.principal_name}")
        if record.business_phone:
            adapter.logger.info(f"  Phone: {record.business_phone}")

        if dry_run:
            adapter.logger.info("Dry run complete. Would have saved corporate record.")
            return {
                'entity_name': record.entity_name,
                'entity_id': record.entity_id,
                'principal_name': record.principal_name,
                'principal_title': record.principal_title,
                'business_phone': record.business_phone,
                'business_address': record.business_address,
                'status': record.status,
            }

        # Save to database
        entity_id = database.save_corporate_record(record)

        if entity_id:
            return {
                'entity_name': record.entity_name,
                'entity_id': record.entity_id,
                'principal_name': record.principal_name,
                'principal_title': record.principal_title,
                'business_phone': record.business_phone,
                'business_address': record.business_address,
                'status': record.status,
                'database_id': entity_id,
            }

        return None
