"""
Free-first data ingestion pipeline for Massachusetts property data.

This package holds scrapers, parsers, and ETL utilities that replace
the Attom API by sourcing public records (registries + municipal assessors).
"""

from .settings import pipeline_settings  # noqa: F401
