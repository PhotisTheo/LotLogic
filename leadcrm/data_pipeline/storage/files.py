"""File storage helpers (local filesystem by default)."""

from __future__ import annotations

from pathlib import Path
from typing import Union

from ..settings import pipeline_settings


def save_binary(content: bytes, relative_path: str) -> str:
    """Persist bytes under storage_root / relative_path."""
    full_path = pipeline_settings.storage_root / relative_path
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_bytes(content)
    return str(full_path)


def open_binary(relative_path: str) -> bytes:
    """Convenience helper for reading stored bytes."""
    full_path = pipeline_settings.storage_root / relative_path
    return full_path.read_bytes()
