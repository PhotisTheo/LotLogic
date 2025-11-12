"""Utilities for extracting text from registry documents (PDF/TIFF)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from pdfminer.high_level import extract_text as pdf_extract_text
from PIL import Image
import pytesseract

logger = logging.getLogger("pipeline.document_text")


def extract_text(file_path: str) -> Optional[str]:
    """
    Best-effort text extraction that handles PDF text layers first and falls
    back to OCR via Tesseract when dealing with TIFF/image-only documents.
    """
    path = Path(file_path)
    suffix = path.suffix.lower()

    try:
        if suffix == ".pdf":
            return _extract_pdf(path)
        if suffix in {".tif", ".tiff", ".png", ".jpg", ".jpeg"}:
            return _extract_image(path)
        # Unknown extension: try PDF parser first
        return _extract_pdf(path)
    except Exception as exc:
        logger.warning("Failed to extract text from %s: %s", file_path, exc)
        return None


def _extract_pdf(path: Path) -> Optional[str]:
    text = pdf_extract_text(str(path))
    return text.strip() or None


def _extract_image(path: Path) -> Optional[str]:
    with Image.open(path) as img:
        text = pytesseract.image_to_string(img)
    return text.strip() or None
