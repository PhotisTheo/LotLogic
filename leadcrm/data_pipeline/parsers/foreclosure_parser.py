"""Helpers for foreclosure-stage detection from registry documents."""

from __future__ import annotations

from typing import Dict, Optional


def parse_foreclosure_text(text: str) -> Dict[str, Optional[str]]:
    """Return foreclosure stage metadata from OCR text (stub)."""
    return {
        "foreclosure_stage": None,
        "auction_date": None,
        "recording_date": None,
    }
