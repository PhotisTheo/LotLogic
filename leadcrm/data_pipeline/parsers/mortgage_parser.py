"""Utilities for extracting mortgage data from registry documents."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Optional, Union

from .document_text import extract_text


def parse_mortgage_document(file_path: Union[str, Path]) -> Dict[str, Optional[Union[str, Decimal, int]]]:
    """
    Extract mortgage data from a PDF or TIFF document.

    Returns a dict with:
        - lender: str
        - amount: Decimal
        - interest_rate: Decimal
        - term_years: int
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return _empty_result()

    text = extract_text(str(file_path))
    if not text:
        return _empty_result()

    result = parse_mortgage_text(text)
    result["raw_text"] = text
    return result


def parse_mortgage_text(text: str) -> Dict[str, Optional[Union[str, Decimal, int]]]:
    """
    Extract lender, amount, interest rate, and term from text.
    Uses regex patterns to identify common mortgage document formats.
    """
    result = _empty_result()

    # Extract loan amount
    result['amount'] = _extract_amount(text)

    # Extract interest rate
    result['interest_rate'] = _extract_interest_rate(text)

    # Extract term
    result['term_years'] = _extract_term(text)

    # Extract lender name
    result['lender'] = _extract_lender(text)

    return result


def _empty_result() -> Dict[str, Optional[Union[str, Decimal, int]]]:
    """Return empty mortgage data dict."""
    return {
        "lender": None,
        "amount": None,
        "interest_rate": None,
        "term_years": None,
        "raw_text": None,
    }


def _extract_amount(text: str) -> Optional[Decimal]:
    """Extract loan amount from text."""
    # Pattern 1: "Principal Amount: $450,000.00" or "Loan Amount: $450,000"
    patterns = [
        r'(?:principal|loan|mortgage)\s+amount[:\s]+\$?([\d,]+\.?\d*)',
        r'sum\s+of[:\s]+\$?([\d,]+\.?\d*)',
        r'indebtedness[:\s]+\$?([\d,]+\.?\d*)',
        # Pattern for written amounts: "Four Hundred Fifty Thousand"
        r'(?:principal|loan|mortgage)\s+amount[:\s]+([\w\s]+?)(?:dollars|and)',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            amount_str = match.group(1).strip()
            # Try to convert numeric format
            if re.match(r'[\d,\.]+', amount_str):
                try:
                    cleaned = amount_str.replace(',', '')
                    return Decimal(cleaned)
                except (InvalidOperation, ValueError):
                    continue
            # Try to convert written format (e.g., "Four Hundred Thousand")
            amount_num = _parse_written_number(amount_str)
            if amount_num:
                return Decimal(str(amount_num))

    return None


def _extract_interest_rate(text: str) -> Optional[Decimal]:
    """Extract interest rate from text."""
    # Pattern: "Interest Rate: 5.25%" or "at a rate of 5.25% per annum"
    patterns = [
        r'interest\s+rate[:\s]+([\d\.]+)%?',
        r'rate\s+of[:\s]+([\d\.]+)%?\s+per',
        r'bearing\s+interest\s+at[:\s]+([\d\.]+)%?',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                rate = Decimal(match.group(1))
                # Normalize: if rate is > 1, assume it's already percentage (5.25)
                # if rate is < 1, assume it's decimal form (0.0525)
                if rate < 1:
                    rate = rate * 100
                return rate
            except (InvalidOperation, ValueError):
                continue

    return None


def _extract_term(text: str) -> Optional[int]:
    """Extract loan term in years from text."""
    # Pattern: "Term: 30 years" or "360 months"
    patterns = [
        r'term[:\s]+(\d+)\s+years?',
        r'(\d+)\s+years?\s+term',
        r'term[:\s]+(\d+)\s+months?',
        r'(\d+)\s+months?\s+term',
    ]

    for i, pattern in enumerate(patterns):
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                value = int(match.group(1))
                # If pattern mentioned months (index 2 or 3), convert to years
                if i >= 2:
                    value = value // 12
                return value
            except ValueError:
                continue

    return None


def _extract_lender(text: str) -> Optional[str]:
    """Extract lender name from text."""
    # Common patterns for lender identification
    patterns = [
        r'lender[:\s]+([A-Z][A-Za-z\s,\.&]+(?:Bank|Mortgage|Credit Union|Financial|Lending|Corp|Company|Inc|LLC))',
        r'(?:to|from)[:\s]+([A-Z][A-Za-z\s,\.&]+(?:Bank|Mortgage|Credit Union|Financial|Lending|Corp|Company|Inc|LLC))',
        r'holder[:\s]+([A-Z][A-Za-z\s,\.&]+(?:Bank|Mortgage|Credit Union|Financial|Lending|Corp|Company|Inc|LLC))',
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            lender = match.group(1).strip()
            # Clean up extra whitespace
            lender = re.sub(r'\s+', ' ', lender)
            if len(lender) > 5:  # Minimum reasonable lender name length
                return lender

    return None


def _parse_written_number(text: str) -> Optional[int]:
    """
    Parse written numbers like 'Four Hundred Fifty Thousand' to 450000.
    Basic implementation for common amounts.
    """
    text = text.lower().strip()

    # Number word mappings
    ones = {
        'zero': 0, 'one': 1, 'two': 2, 'three': 3, 'four': 4,
        'five': 5, 'six': 6, 'seven': 7, 'eight': 8, 'nine': 9,
        'ten': 10, 'eleven': 11, 'twelve': 12, 'thirteen': 13,
        'fourteen': 14, 'fifteen': 15, 'sixteen': 16, 'seventeen': 17,
        'eighteen': 18, 'nineteen': 19
    }

    tens = {
        'twenty': 20, 'thirty': 30, 'forty': 40, 'fifty': 50,
        'sixty': 60, 'seventy': 70, 'eighty': 80, 'ninety': 90
    }

    scales = {
        'hundred': 100,
        'thousand': 1000,
        'million': 1000000,
    }

    words = text.replace('-', ' ').replace(',', '').split()
    current = 0
    result = 0

    for word in words:
        if word in ones:
            current += ones[word]
        elif word in tens:
            current += tens[word]
        elif word in scales:
            if word == 'hundred':
                current *= scales[word]
            else:
                current *= scales[word]
                result += current
                current = 0

    result += current
    return result if result > 0 else None
