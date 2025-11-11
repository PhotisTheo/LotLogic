"""Parcel ID normalization utilities."""

from __future__ import annotations


def to_loc_id(muni_code: str, parcel_id: str) -> str:
    """Combine municipality + parcel identifiers into a canonical LOC_ID."""
    return f"{muni_code}-{parcel_id}"
