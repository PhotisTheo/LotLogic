"""
Developer-friendly CLI for running scraper jobs locally.

Examples:
    python -m leadcrm.data_pipeline.cli registry-run --registry suffolk --address "123 Main St, Boston"
    python -m leadcrm.data_pipeline.cli assessor-run --municipality 2507000 --parcel-id 12345
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .jobs.registry_job import RegistryJob
from .jobs.assessor_job import AssessorJob
from .jobs.corporate_job import CorporateJob
from .settings import pipeline_settings


def _load_sources():
    try:
        return pipeline_settings.load_sources()
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)


def run_registry(args: argparse.Namespace) -> None:
    sources = _load_sources()
    registry_config = next(
        (r for r in sources.get("registries", []) if r["id"] == args.registry),
        None,
    )
    if not registry_config:
        raise SystemExit(f"Registry '{args.registry}' not found in sources.json")
    job = RegistryJob(registry_config)
    job.run(
        address=args.address,
        owner=args.owner,
        loc_id=args.loc_id,
        dry_run=args.dry_run,
        force_refresh=args.force_refresh,
        max_cache_age_days=args.max_cache_age,
    )


def run_assessor(args: argparse.Namespace) -> None:
    sources = _load_sources()
    muni_config = next(
        (m for m in sources.get("municipalities", []) if m["muni_code"] == args.municipality),
        None,
    )
    if not muni_config:
        raise SystemExit(f"Municipality '{args.municipality}' not found in sources.json")
    job = AssessorJob(muni_config)
    job.run(parcel_id=args.parcel_id, address=args.address, dry_run=args.dry_run)


def run_corporate(args: argparse.Namespace) -> None:
    """Run corporate entity lookup from MA Secretary of Commonwealth."""
    # Use a simple config for MA Secretary adapter
    corporate_config = {
        "id": "ma_secretary",
        "name": "Massachusetts Secretary of Commonwealth",
        "adapter": "ma_secretary",
    }
    job = CorporateJob(corporate_config)
    result = job.run(
        entity_name=args.entity_name,
        dry_run=args.dry_run,
        force_refresh=args.force_refresh,
        max_cache_age_days=args.max_cache_age,
    )

    if result:
        print("\n" + "=" * 60)
        print(f"Entity: {result['entity_name']}")
        print(f"Entity ID: {result['entity_id']}")
        print(f"Status: {result.get('status', 'N/A')}")
        if result.get('principal_name'):
            principal_str = result['principal_name']
            if result.get('principal_title'):
                principal_str += f" ({result['principal_title']})"
            print(f"Principal: {principal_str}")
        if result.get('business_phone'):
            print(f"Phone: {result['business_phone']}")
        if result.get('business_address'):
            print(f"Address: {result['business_address']}")
        print("=" * 60)
    else:
        print(f"\nNo results found for '{args.entity_name}'")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="LeadCRM data pipeline CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    reg_parser = subparsers.add_parser("registry-run", help="Run a registry scrape for a single parcel")
    reg_parser.add_argument("--registry", required=True, help="Registry ID (see sources.json)")
    reg_parser.add_argument("--address", help="Property address string")
    reg_parser.add_argument("--owner", help="Owner/party name to search")
    reg_parser.add_argument("--loc-id", dest="loc_id", help="Internal parcel LOC_ID")
    reg_parser.add_argument("--dry-run", action="store_true", help="Fetch data but skip database writes")
    reg_parser.add_argument("--force-refresh", action="store_true", help="Force scrape even if cache is fresh")
    reg_parser.add_argument("--max-cache-age", type=int, default=90, help="Max cache age in days (default: 90)")
    reg_parser.set_defaults(func=run_registry)

    ass_parser = subparsers.add_parser("assessor-run", help="Run assessor ingestion for a parcel/municipality")
    ass_parser.add_argument("--municipality", required=True, help="Municipality code (FIPS-like)")
    ass_parser.add_argument("--parcel-id", required=False, help="Parcel identifier / map-block-lot")
    ass_parser.add_argument("--address", help="Optional address override")
    ass_parser.add_argument("--dry-run", action="store_true", help="Skip DB writes")
    ass_parser.set_defaults(func=run_assessor)

    corp_parser = subparsers.add_parser("corporate-run", help="Look up corporate entity info (LLC owners, etc.)")
    corp_parser.add_argument("--entity-name", required=True, dest="entity_name", help="Legal entity name (e.g., 'ABC REALTY LLC')")
    corp_parser.add_argument("--dry-run", action="store_true", help="Fetch data but skip database writes")
    corp_parser.add_argument("--force-refresh", action="store_true", help="Force scrape even if cache is fresh")
    corp_parser.add_argument("--max-cache-age", type=int, default=180, help="Max cache age in days (default: 180)")
    corp_parser.set_defaults(func=run_corporate)

    subparsers.add_parser("show-config", help="Print the loaded sources matrix").set_defaults(func=show_config)
    return parser


def show_config(args: argparse.Namespace) -> None:
    """Pretty-print sources for sanity checks."""
    sources = _load_sources()
    print(json.dumps(sources, indent=2))


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
