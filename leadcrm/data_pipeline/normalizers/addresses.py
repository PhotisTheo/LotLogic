"""Address normalization wrappers (libpostal/USPS placeholder)."""

from __future__ import annotations

from typing import Dict


def normalize(address: str) -> Dict[str, str]:
    """Return components such as street, city, state, ZIP. Stub for now."""
    return {"formatted": address}
