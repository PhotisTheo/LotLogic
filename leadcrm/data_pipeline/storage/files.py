"""File storage helpers (S3 with local fallback)."""

from __future__ import annotations

from pathlib import Path
from typing import Union
import logging

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage

from ..settings import pipeline_settings

logger = logging.getLogger(__name__)


def save_binary(content: bytes, relative_path: str) -> str:
    """
    Persist bytes to S3 or local filesystem.

    Returns:
        S3 key or local file path
    """
    # Use S3 if configured, otherwise fall back to local filesystem
    if hasattr(settings, 'USE_S3') and settings.USE_S3:
        try:
            # Save to S3 under scraped_documents/
            s3_key = f"scraped_documents/{relative_path}"
            file_obj = ContentFile(content)
            saved_path = default_storage.save(s3_key, file_obj)
            logger.info(f"Saved document to S3: {saved_path}")
            return saved_path
        except Exception as e:
            logger.error(f"Failed to save to S3, falling back to local: {e}")
            # Fall through to local storage on error

    # Local filesystem fallback
    full_path = pipeline_settings.storage_root / relative_path
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.write_bytes(content)
    logger.info(f"Saved document locally: {full_path}")
    return str(full_path)


def open_binary(relative_path: str) -> bytes:
    """
    Read bytes from S3 or local filesystem.

    Args:
        relative_path: S3 key or local path
    """
    # Try S3 first if configured
    if hasattr(settings, 'USE_S3') and settings.USE_S3:
        try:
            with default_storage.open(relative_path, 'rb') as f:
                return f.read()
        except Exception as e:
            logger.warning(f"Failed to read from S3, trying local: {e}")

    # Local filesystem fallback
    full_path = pipeline_settings.storage_root / relative_path
    return full_path.read_bytes()


def generate_download_url(file_path: str, expiry_seconds: int = 3600) -> str:
    """
    Generate a presigned URL for document download.

    Args:
        file_path: S3 key or local path
        expiry_seconds: URL expiration time (default 1 hour)

    Returns:
        Presigned S3 URL or local URL
    """
    # For S3, generate presigned URL
    if hasattr(settings, 'USE_S3') and settings.USE_S3:
        try:
            # Generate presigned URL that expires
            url = default_storage.url(file_path)
            return url
        except Exception as e:
            logger.error(f"Failed to generate S3 URL: {e}")
            return ""

    # For local files, return a Django URL (requires view)
    from django.urls import reverse
    return reverse('serve_scraped_document', kwargs={'file_path': file_path})
