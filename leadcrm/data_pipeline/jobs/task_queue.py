"""
Celery/RQ task definitions for running pipeline jobs asynchronously.

The actual queue backend is not configured yet; this module simply wires up
callables that the worker can import once Celery/RQ is configured.
"""

from __future__ import annotations

from typing import Dict, Any

from .registry_job import RegistryJob
from .assessor_job import AssessorJob
from ..settings import pipeline_settings


def run_registry_task(config: Dict[str, Any], payload: Dict[str, Any]) -> None:
    """Entry point for async registry scrapes."""
    job = RegistryJob(config)
    job.run(
        address=payload.get("address"),
        owner=payload.get("owner"),
        loc_id=payload.get("loc_id"),
        dry_run=payload.get("dry_run", False),
    )


def run_assessor_task(config: Dict[str, Any], payload: Dict[str, Any]) -> None:
    """Entry point for async assessor scrapes."""
    job = AssessorJob(config)
    job.run(
        parcel_id=payload.get("parcel_id"),
        address=payload.get("address"),
        dry_run=payload.get("dry_run", False),
    )


def build_default_payload() -> Dict[str, Any]:
    """Helper for schedulers to know standard defaults."""
    return {
        "dry_run": False,
        "storage_root": str(pipeline_settings.storage_root),
    }
