"""
Registry ingestion job orchestrator.

Takes a registry config entry, delegates scraping to the adapter, then sends
the parsed results to storage/normalization layers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ..sources.registries.masslandrecords import MassLandRecordsSource
from ..parsers import mortgage_parser
from ..storage import database
from ..settings import pipeline_settings


REGISTRY_ADAPTERS = {
    "masslandrecords": MassLandRecordsSource,
}


@dataclass
class RegistryJob:
    config: Dict[str, Any]

    def _build_adapter(self):
        adapter_key = self.config.get("adapter")
        adapter_cls = REGISTRY_ADAPTERS.get(adapter_key)
        if not adapter_cls:
            raise ValueError(f"No adapter registered for key '{adapter_key}'")
        return adapter_cls(self.config, pipeline_settings)

    def run(
        self,
        address: Optional[str] = None,
        owner: Optional[str] = None,
        loc_id: Optional[str] = None,
        dry_run: bool = False,
        force_refresh: bool = False,
        max_cache_age_days: int = 90,
    ) -> None:
        adapter = self._build_adapter()

        # Check cache freshness before scraping (if loc_id is known)
        if loc_id and not force_refresh:
            is_fresh, attom_id = database.check_cache_freshness(
                loc_id=loc_id,
                max_age_days=max_cache_age_days
            )
            if is_fresh:
                adapter.logger.info(
                    f"Skipping scrape for loc_id={loc_id} - cache is fresh (< {max_cache_age_days} days old). "
                    f"Use --force-refresh to override."
                )
                return

        results = adapter.search(address=address, owner=owner, loc_id=loc_id)

        if not results:
            adapter.logger.info("No registry records found.")
            return

        adapter.logger.info("Fetched %s registry record(s)", len(results))

        # Parse documents and enrich records
        for record in results:
            if record.raw_document_path and record.instrument_type == "MORTGAGE":
                adapter.logger.info(f"Parsing mortgage document: {record.raw_document_path}")
                parsed_data = mortgage_parser.parse_mortgage_document(record.raw_document_path)

                # Enrich record with parsed data
                if parsed_data.get('amount') and not record.amount:
                    record.amount = float(parsed_data['amount'])
                if parsed_data.get('lender') and not record.lender:
                    record.lender = parsed_data['lender']

                # Add parsed data to metadata
                record.raw_metadata['parsed_interest_rate'] = str(parsed_data.get('interest_rate')) if parsed_data.get('interest_rate') else None
                record.raw_metadata['parsed_term_years'] = parsed_data.get('term_years')

        if dry_run:
            adapter.logger.info("Dry run complete. Would have saved %s records.", len(results))
            for i, record in enumerate(results, 1):
                adapter.logger.info(f"  Record {i}: {record.instrument_type} - {record.lender} - ${record.amount}")
            return

        for record in results:
            database.save_registry_record(record)
