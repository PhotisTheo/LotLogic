from __future__ import annotations

import base64
import csv
import io
import json
import logging
import math
import re
import shutil
import threading
import time
import zipfile
from collections import defaultdict
import os
from decimal import Decimal, InvalidOperation

import requests
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from functools import lru_cache
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union
from urllib import error, parse, request
from urllib.error import URLError
from urllib.parse import urljoin, urlparse

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional dependency
    pd = None

from django.conf import settings


logger = logging.getLogger(__name__)

GEOCODER_URL = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
FEMA_EXPORT_URL = "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/export"
FEMA_VIEWER_BASE_URL = "https://msc.fema.gov/portal/home"
ESRI_WORLD_IMAGERY_EXPORT = "https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/export"
USER_AGENT = "Mozilla/5.0 (LeadCRM/1.0; +https://example.com)"


def _env_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return value.strip().lower() in {"true", "1", "yes", "on"}


def _env_int(key: str, default: int) -> int:
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent.parent
GISDATA_ROOT = PROJECT_ROOT / "gisdata"
logger.info("GISDATA_ROOT set to: %s (exists: %s)", GISDATA_ROOT, GISDATA_ROOT.exists())
STATIC_DATA_ROOT = APP_DIR / "data"
STATIC_DATA_ROOT.mkdir(exist_ok=True)
MASSGIS_DOWNLOAD_DIR = GISDATA_ROOT / "downloads"
MASSGIS_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
MASSGIS_DATASET_INDEX = GISDATA_ROOT / "dataset_index.json"
MASSGIS_CATALOG_LISTING_URL = "https://download.massgis.digital.mass.gov/shapefiles/l3parcels/"
MASSGIS_CATALOG_CACHE = GISDATA_ROOT / "massgis_catalog.json"
MASSGIS_CATALOG_CACHE_MAX_AGE = timedelta(days=1)
MASSGIS_DATASET_TTL = timedelta(days=90)  # Quarterly refresh (90 days)
MASSGIS_LIVE_FETCH_ENABLED = _env_bool("MASSGIS_LIVE_FETCH_ENABLED", True)
MASSGIS_DIRECTORY_TIMEOUT_SECONDS = _env_int("MASSGIS_DIRECTORY_TIMEOUT", 10)

# S3 GIS Storage Configuration
USE_S3_FOR_GIS = os.getenv("USE_S3_FOR_GIS", "True").lower() in ("true", "1", "yes")
S3_GIS_BUCKET = os.getenv("AWS_STORAGE_BUCKET_NAME", "")
S3_GIS_PREFIX = "gisdata/"
BOSTON_ASSESSMENT_S3_BUCKET = os.getenv("BOSTON_ASSESSMENT_S3_BUCKET") or S3_GIS_BUCKET
BOSTON_ASSESSMENT_S3_KEY = os.getenv(
    "BOSTON_ASSESSMENT_S3_KEY",
    "gisdata/fy2025-property-assessment-data_12_30_2024.csv",
)
BOSTON_ASSESSMENT_S3_ENCODING = os.getenv("BOSTON_ASSESSMENT_S3_ENCODING", "utf-8-sig")
MASSGIS_EXCEL_FILENAME = "MassGIS_Parcel_Download_Links.xlsx"
MASSGIS_TOWNS_URL = "https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/townssurvey_shp.zip"
MASSGIS_TOWNS_DIR = GISDATA_ROOT / "townssurvey"
MASSGIS_TOWN_BOUNDARIES_CACHE_PATH = MASSGIS_TOWNS_DIR / "town_boundaries.geojson"
MASSGIS_EXCEL_PATH = GISDATA_ROOT / MASSGIS_EXCEL_FILENAME
BOSTON_TOWN_ID = 35
BOSTON_DATASET_SLUG = "BOSTON_TAXPAR"
BOSTON_DEFAULT_DATASET_IDENTIFIER = "boston::property-assessment-parcels"
BOSTON_DEFAULT_FISCAL_YEAR = "2023"
BOSTON_OPEN_DATA_DOMAINS = (
    "https://bostonopendata.boston.opendata.arcgis.com",
    "https://bostonopendata-boston.opendata.arcgis.com",
)
BOSTON_OPEN_DATA_SEARCH_PATH = "/datasets?q=*parcel&sort_by=relevance"
BOSTON_OPEN_DATA_DOWNLOAD_TEMPLATE = (
    "https://opendata.arcgis.com/api/v3/datasets/{slug}/downloads/data?format=shp&spatialRefId=26986"
)
BOSTON_OPEN_DATA_GEOJSON_TEMPLATE = (
    "https://opendata.arcgis.com/api/v3/datasets/{slug}/downloads/data?format=geojson&spatialRefId=4326"
)
BOSTON_OPENDATA_API_BASE = "https://opendata.arcgis.com/api/v3"
BOSTON_DOWNLOAD_FORMAT = "shp"
BOSTON_DOWNLOAD_SRID = 26986
BOSTON_DOWNLOAD_WHERE = "1=1"
BOSTON_CKAN_PACKAGE = "property-assessment-parcels"
BOSTON_CKAN_API_TEMPLATE = "https://data.boston.gov/api/3/action/package_show?id={package}"
BOSTON_CONFIG_ENV_VAR = "MASSGIS_BOSTON_SHAPEFILE_URL"
BOSTON_NEIGHBORHOODS_PATH = GISDATA_ROOT / "boston_neighborhoods.geojson"
STATIC_MASSGIS_CATALOG_PATH = STATIC_DATA_ROOT / "massgis_catalog.json"
BOSTON_NEIGHBORHOODS_STATIC_PATH = STATIC_DATA_ROOT / "boston_neighborhoods.geojson"
BOSTON_NEIGHBORHOODS_DATASET_SLUG = os.getenv(
    "BOSTON_NEIGHBORHOODS_DATASET", "boston::neighborhoods"
)
BOSTON_NEIGHBORHOODS_OVERRIDE_URL = os.getenv("BOSTON_NEIGHBORHOODS_URL")
BATCHDATA_SKIPTRACE_ENDPOINT = "https://api.batchdata.com/api/v1/property/skip-trace"
BATCHDATA_DNC_ENDPOINT = "https://api.batchdata.com/api/v1/phone/dnc"
BATCHDATA_TIMEOUT = 20

_TOWN_BOUNDARIES_CACHE_LOCK = threading.Lock()
_TOWN_BOUNDARIES_CACHE: Optional[Dict[str, Any]] = None

DEFAULT_MORTGAGE_TERM_YEARS = 30
DEFAULT_INITIAL_LTV = 0.80

MORTGAGE_RATE_BY_YEAR: Dict[int, float] = {
    1971: 7.542,
    1972: 7.383,
    1973: 8.045,
    1974: 9.187,
    1975: 9.047,
    1976: 8.866,
    1977: 8.845,
    1978: 9.642,
    1979: 11.204,
    1980: 13.740,
    1981: 16.642,
    1982: 16.044,
    1983: 13.235,
    1984: 13.878,
    1985: 12.430,
    1986: 10.187,
    1987: 10.213,
    1988: 10.342,
    1989: 10.319,
    1990: 10.129,
    1991: 9.247,
    1992: 8.390,
    1993: 7.315,
    1994: 8.381,
    1995: 7.935,
    1996: 7.806,
    1997: 7.599,
    1998: 6.943,
    1999: 7.440,
    2000: 8.053,
    2001: 6.968,
    2002: 6.537,
    2003: 5.827,
    2004: 5.839,
    2005: 5.867,
    2006: 6.413,
    2007: 6.337,
    2008: 6.027,
    2009: 5.037,
    2010: 4.690,
    2011: 4.448,
    2012: 3.657,
    2013: 3.976,
    2014: 4.169,
    2015: 3.851,
    2016: 3.654,
    2017: 3.990,
    2018: 4.545,
    2019: 3.936,
    2020: 3.112,
    2021: 2.958,
    2022: 5.344,
    2023: 6.807,
    2024: 6.721,
    2025: 6.697,
}

# Massachusetts Mainland (EPSG:26986) parameters for Lambert Conformal Conic projection
_MA_FALSE_EASTING = 200000.0
_MA_FALSE_NORTHING = 750000.0
_MA_STANDARD_PARALLEL_1 = math.radians(41.71666666666667)
_MA_STANDARD_PARALLEL_2 = math.radians(42.68333333333333)
_MA_LATITUDE_OF_ORIGIN = math.radians(41.0)
_MA_CENTRAL_MERIDIAN = math.radians(-71.5)
_MA_SEMI_MAJOR_AXIS = 6378137.0
_MA_EARTH_FLATTENING = 1 / 298.257222101
_MA_ECCENTRICITY = math.sqrt(2 * _MA_EARTH_FLATTENING - _MA_EARTH_FLATTENING**2)


def _lcc_m(phi: float) -> float:
    sin_phi = math.sin(phi)
    return math.cos(phi) / math.sqrt(1 - (_MA_ECCENTRICITY**2) * sin_phi**2)


def _lcc_t(phi: float) -> float:
    sin_phi = math.sin(phi)
    numerator = math.tan(math.pi / 4 - phi / 2)
    denominator = math.pow((1 - _MA_ECCENTRICITY * sin_phi) / (1 + _MA_ECCENTRICITY * sin_phi), _MA_ECCENTRICITY / 2)
    return numerator / denominator


_MA_M1 = _lcc_m(_MA_STANDARD_PARALLEL_1)
_MA_M2 = _lcc_m(_MA_STANDARD_PARALLEL_2)
_MA_T1 = _lcc_t(_MA_STANDARD_PARALLEL_1)
_MA_T2 = _lcc_t(_MA_STANDARD_PARALLEL_2)
_MA_T0 = _lcc_t(_MA_LATITUDE_OF_ORIGIN)
_MA_N = (math.log(_MA_M1) - math.log(_MA_M2)) / (math.log(_MA_T1) - math.log(_MA_T2))
_MA_F = _MA_M1 / (_MA_N * math.pow(_MA_T1, _MA_N))
_MA_RHO0 = _MA_SEMI_MAJOR_AXIS * _MA_F * math.pow(_MA_T0, _MA_N)


def _load_dataset_index() -> Dict[str, Dict[str, str]]:
    if MASSGIS_DATASET_INDEX.exists():
        try:
            with MASSGIS_DATASET_INDEX.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except json.JSONDecodeError:
            logger.warning("Corrupt MassGIS dataset index – rebuilding from scratch.")
    return {}


def _save_dataset_index(data: Dict[str, Dict[str, str]]) -> None:
    MASSGIS_DATASET_INDEX.parent.mkdir(parents=True, exist_ok=True)
    with MASSGIS_DATASET_INDEX.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)


def _update_dataset_index_entry(slug: str, **updates) -> None:
    data = _load_dataset_index()
    entry = data.get(slug, {})
    for key, value in updates.items():
        if isinstance(value, datetime):
            value = value.isoformat()
        if value is not None:
            entry[key] = value
    data[slug] = entry
    _save_dataset_index(data)


def _ensure_dataset_index_entry(slug: str, *, source_url: str) -> None:
    data = _load_dataset_index()
    if slug not in data:
        _update_dataset_index_entry(
            slug,
            source_url=source_url,
            downloaded_at=datetime.now(timezone.utc),
            last_checked=datetime.now(timezone.utc),
        )
    else:
        _update_dataset_index_entry(slug, source_url=source_url, last_checked=datetime.now(timezone.utc))


def _record_dataset_download(slug: str, source_url: str, last_modified: Optional[datetime] = None) -> None:
    now = datetime.now(timezone.utc)
    updates = {
        "source_url": source_url,
        "downloaded_at": now,
        "last_checked": now,
    }
    if last_modified is not None:
        updates["last_modified"] = last_modified
    _update_dataset_index_entry(slug, **updates)


def _delete_local_dataset(slug: str) -> None:
    base_dir = GISDATA_ROOT / slug
    if base_dir.exists():
        shutil.rmtree(base_dir, ignore_errors=True)
    else:
        existing = _find_existing_dataset_dir(slug)
        if existing is not None:
            shutil.rmtree(existing, ignore_errors=True)
    zip_path = MASSGIS_DOWNLOAD_DIR / f"{slug}.zip"
    if zip_path.exists():
        zip_path.unlink()
    data = _load_dataset_index()
    if slug in data:
        data.pop(slug)
        _save_dataset_index(data)


# S3 GIS Storage Helper Functions
def _get_s3_client():
    """Get boto3 S3 client if S3 is configured."""
    if not USE_S3_FOR_GIS or not S3_GIS_BUCKET:
        return None
    try:
        import boto3
        return boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_S3_REGION_NAME', 'us-east-1')
        )
    except Exception as exc:
        logger.warning("Failed to create S3 client for GIS storage: %s", exc)
        return None


def _check_s3_dataset_exists(slug: str) -> Optional[datetime]:
    """Check if dataset exists in S3 and return its last modified time."""
    s3 = _get_s3_client()
    if not s3:
        return None

    try:
        # Check if the zip file exists in S3
        key = f"{S3_GIS_PREFIX}{slug}.zip"
        response = s3.head_object(Bucket=S3_GIS_BUCKET, Key=key)
        return response['LastModified']
    except Exception:
        return None


def _download_from_s3(slug: str, local_zip_path: Path) -> bool:
    """Download GIS dataset from S3 to local filesystem."""
    s3 = _get_s3_client()
    if not s3:
        return False

    try:
        key = f"{S3_GIS_PREFIX}{slug}.zip"
        logger.info("Downloading %s from S3: s3://%s/%s", slug, S3_GIS_BUCKET, key)
        local_zip_path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(S3_GIS_BUCKET, key, str(local_zip_path))
        logger.info("Successfully downloaded %s from S3", slug)
        return True
    except Exception as exc:
        logger.warning("Failed to download %s from S3: %s", slug, exc)
        return False


def _upload_to_s3(slug: str, local_zip_path: Path) -> bool:
    """Upload GIS dataset from local filesystem to S3."""
    s3 = _get_s3_client()
    if not s3:
        return False

    try:
        key = f"{S3_GIS_PREFIX}{slug}.zip"
        logger.info("Uploading %s to S3: s3://%s/%s", slug, S3_GIS_BUCKET, key)
        s3.upload_file(str(local_zip_path), S3_GIS_BUCKET, key)
        logger.info("Successfully uploaded %s to S3", slug)
        return True
    except Exception as exc:
        logger.warning("Failed to upload %s to S3: %s", slug, exc)
        return False


def _download_boston_assessment_csv_from_s3() -> Optional[io.StringIO]:
    """Return a text stream for the Boston assessment CSV stored in S3."""
    bucket = BOSTON_ASSESSMENT_S3_BUCKET or S3_GIS_BUCKET
    key = BOSTON_ASSESSMENT_S3_KEY
    if not bucket or not key:
        return None

    s3 = _get_s3_client()
    if not s3:
        return None

    try:
        logger.info("Downloading Boston assessment CSV from S3: s3://%s/%s", bucket, key)
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response.get("Body")
        if body is None:
            logger.warning("Boston assessment CSV response missing body (bucket=%s, key=%s)", bucket, key)
            return None
        payload = body.read()
        if not isinstance(payload, (bytes, bytearray)):
            payload = bytes(payload)
        text = payload.decode(BOSTON_ASSESSMENT_S3_ENCODING or "utf-8-sig", errors="replace")
        return io.StringIO(text)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Unable to download Boston assessment CSV from S3 (bucket=%s, key=%s): %s",
            bucket,
            key,
            exc,
        )
        return None


def _is_s3_dataset_stale(last_modified: datetime) -> bool:
    """Check if S3 dataset is older than quarterly refresh period (90 days)."""
    now = datetime.now(timezone.utc)
    # Make last_modified timezone-aware if it isn't
    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=timezone.utc)
    age = now - last_modified
    return age > MASSGIS_DATASET_TTL


def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _fetch_remote_last_modified(url: str) -> Optional[datetime]:
    request_object = request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with request.urlopen(request_object) as response:
            header = response.headers.get("Last-Modified")
            if header:
                try:
                    dt = parsedate_to_datetime(header)
                    if dt and dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except (TypeError, ValueError):
                    logger.debug("Unable to parse Last-Modified header '%s' from %s", header, url)
    except Exception as exc:  # noqa: BLE001
        logger.debug("HEAD request failed for %s: %s", url, exc)
    return None


def _is_stale(timestamp: Optional[str], max_age: timedelta = MASSGIS_CATALOG_CACHE_MAX_AGE) -> bool:
    if not timestamp:
        return True
    try:
        dt = datetime.fromisoformat(timestamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return datetime.now(timezone.utc) - dt > max_age


class _MassGISCatalogLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag, attrs) -> None:  # type: ignore[override]
        if tag.lower() != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href")
        if href and href.lower().endswith(".zip"):
            self.links.append(href)


_catalog_refresh_lock = threading.Lock()
_catalog_refresh_pending = False


def _load_catalog_payload() -> Optional[dict]:
    if not MASSGIS_CATALOG_CACHE.exists():
        return None
    try:
        with MASSGIS_CATALOG_CACHE.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (json.JSONDecodeError, OSError):
        logger.warning("Unable to read cached MassGIS catalog index; fresh download required.")
        return None

def _catalog_payload_is_stale(payload: Optional[dict]) -> bool:
    if not payload:
        return True
    fetched_at = payload.get("fetched_at")
    if not fetched_at:
        return False
    return _is_stale(fetched_at, MASSGIS_CATALOG_CACHE_MAX_AGE)


def _parse_catalog_entries(entries: dict) -> Dict[int, MassGISTown]:
    catalog: Dict[int, MassGISTown] = {}
    for key, value in entries.items():
        if not isinstance(value, dict):
            continue

        try:
            town_id = int(key)
        except (TypeError, ValueError):
            continue

        name = value.get("name")
        shapefile_url = value.get("shapefile_url")
        dataset_slug = value.get("dataset_slug")
        if not (name and shapefile_url and dataset_slug):
            continue

        catalog[town_id] = MassGISTown(
            town_id=town_id,
            name=name,
            shapefile_url=shapefile_url,
            gdb_url=value.get("gdb_url"),
            fiscal_year=value.get("fiscal_year"),
            dataset_slug=dataset_slug,
        )

    return _apply_boston_catalog_override(catalog)


def _load_cached_catalog(*, enforce_ttl: bool = True) -> Optional[Dict[int, MassGISTown]]:
    payload = _load_catalog_payload()
    if payload is None:
        return None
    if enforce_ttl and _catalog_payload_is_stale(payload):
        return None

    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return None

    catalog = _parse_catalog_entries(entries)
    return catalog or None


def _load_static_catalog() -> Optional[Dict[int, MassGISTown]]:
    if not STATIC_MASSGIS_CATALOG_PATH.exists():
        return None
    try:
        with STATIC_MASSGIS_CATALOG_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to load bundled MassGIS catalog fallback: %s", exc)
        return None

    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return None
    catalog = _parse_catalog_entries(entries)
    return catalog or None


def _save_catalog_cache(catalog: Dict[int, MassGISTown]) -> None:
    payload = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "entries": {
            str(town_id): {
                "name": town.name,
                "shapefile_url": town.shapefile_url,
                "gdb_url": town.gdb_url,
                "fiscal_year": town.fiscal_year,
                "dataset_slug": town.dataset_slug,
            }
            for town_id, town in catalog.items()
        },
    }
    MASSGIS_CATALOG_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with MASSGIS_CATALOG_CACHE.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _download_massgis_directory_listing() -> str:
    if not MASSGIS_LIVE_FETCH_ENABLED:
        raise MassGISDataError("Live MassGIS directory fetch disabled via configuration.")
    logger.info("Fetching MassGIS parcel directory index from %s", MASSGIS_CATALOG_LISTING_URL)
    req = request.Request(MASSGIS_CATALOG_LISTING_URL, headers={"User-Agent": USER_AGENT})
    try:
        with request.urlopen(req, timeout=MASSGIS_DIRECTORY_TIMEOUT_SECONDS) as response:
            encoding = response.headers.get_content_charset("utf-8")
            raw = response.read()
    except Exception as exc:  # noqa: BLE001
        raise MassGISDataError("Unable to fetch MassGIS parcel directory listing.") from exc

    return raw.decode(encoding or "utf-8", errors="replace")


def _parse_fiscal_year_value(label: Optional[str]) -> int:
    if not label:
        return 0
    match = re.search(r"\d{2,4}", label)
    if not match:
        return 0
    digits = match.group()
    if len(digits) == 2:
        digits = f"20{digits}"
    try:
        return int(digits)
    except ValueError:
        return 0


def _slug_has_suffix_number(slug: str) -> bool:
    return bool(re.search(r"(?:\s|_)\d+$", slug))


def _choose_catalog_entry(options: List["MassGISTown"]) -> "MassGISTown":
    preferred = options[0]
    preferred_year = _parse_fiscal_year_value(preferred.fiscal_year)
    preferred_suffix = _slug_has_suffix_number(preferred.dataset_slug)

    for candidate in options[1:]:
        candidate_year = _parse_fiscal_year_value(candidate.fiscal_year)
        candidate_suffix = _slug_has_suffix_number(candidate.dataset_slug)

        if candidate_year > preferred_year:
            preferred = candidate
            preferred_year = candidate_year
            preferred_suffix = candidate_suffix
            continue

        if candidate_year == preferred_year:
            if preferred_suffix and not candidate_suffix:
                preferred = candidate
                preferred_suffix = candidate_suffix
                continue

            if candidate_suffix == preferred_suffix:
                if candidate.dataset_slug < preferred.dataset_slug:
                    preferred = candidate
                    preferred_suffix = candidate_suffix

    return preferred


def _merge_catalog_entries(
    base: Dict[int, "MassGISTown"], incoming: Dict[int, "MassGISTown"]
) -> Dict[int, "MassGISTown"]:
    if not incoming:
        return base

    merged = dict(base)
    for town_id, candidate in incoming.items():
        existing = merged.get(town_id)
        if existing is None:
            merged[town_id] = candidate
        else:
            merged[town_id] = _choose_catalog_entry([existing, candidate])
    return merged


def _extract_boston_dataset_slug_from_html(html: str) -> Optional[str]:
    if not html:
        return None
    matches = re.findall(r"/datasets/([^\"'>]+)/about", html, flags=re.IGNORECASE)
    for match in matches:
        decoded = parse.unquote(match).strip()
        if not decoded:
            continue
        slug = decoded.split("?", 1)[0]
        if "parcel" in slug.lower():
            return slug
    return None


def _get_configured_boston_shapefile_url() -> Optional[str]:
    settings_value = getattr(settings, "MASSGIS_BOSTON_SHAPEFILE_URL", None)
    env_value = os.getenv(BOSTON_CONFIG_ENV_VAR)
    candidate = settings_value or env_value
    if not candidate:
        return None
    candidate = candidate.strip()
    if not candidate:
        return None
    logger.info("Using configured Boston shapefile URL: %s", candidate)
    return candidate


def _fetch_boston_ckan_download_url() -> Optional[str]:
    api_url = BOSTON_CKAN_API_TEMPLATE.format(package=BOSTON_CKAN_PACKAGE)
    try:
        payload = _request_json(api_url, timeout=30)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Unable to fetch Boston CKAN metadata: %s", exc)
        return None

    result = payload.get("result") or {}
    resources = result.get("resources") or []
    for resource in resources:
        resource_url = resource.get("url")
        if not resource_url:
            continue
        fmt = (resource.get("format") or "").lower()
        name = (resource.get("name") or "").lower()
        if fmt in {"shp", "shapefile", "zip"}:
            logger.info("Using Boston CKAN resource '%s' (%s)", resource.get("name"), resource_url)
            return resource_url
        if any(keyword in name for keyword in ("parcel", "shapefile")):
            logger.info("Using Boston CKAN resource '%s' (%s)", resource.get("name"), resource_url)
            return resource_url
    return None


def _download_text(url: str, *, timeout: int = 15) -> str:
    req = request.Request(url, headers={"User-Agent": USER_AGENT})
    with request.urlopen(req, timeout=timeout) as response:
        encoding = response.headers.get_content_charset("utf-8") or "utf-8"
        raw = response.read()
    return raw.decode(encoding, errors="replace")


def _discover_boston_dataset_slug() -> Optional[str]:
    for domain in BOSTON_OPEN_DATA_DOMAINS:
        url = urljoin(domain, BOSTON_OPEN_DATA_SEARCH_PATH)
        try:
            html = _download_text(url, timeout=20)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Unable to query Boston open data portal at %s: %s", url, exc)
            continue
        slug = _extract_boston_dataset_slug_from_html(html)
        if slug:
            logger.info("Discovered Boston parcel dataset slug '%s' from %s", slug, domain)
            return slug
    return None


@lru_cache(maxsize=1)
def _resolve_boston_shapefile_url() -> Optional[str]:
    configured = _get_configured_boston_shapefile_url()
    if configured:
        return configured

    ckan_url = _fetch_boston_ckan_download_url()
    if ckan_url:
        return ckan_url

    slug = _discover_boston_dataset_slug() or BOSTON_DEFAULT_DATASET_IDENTIFIER
    if not slug:
        logger.warning("Unable to determine Boston parcel dataset identifier.")
        return None
    slug = slug.strip().strip("/")
    if not slug:
        logger.warning("Boston parcel dataset identifier was empty after normalization.")
        return None
    encoded_slug = parse.quote(slug, safe=":")  # keep dataset namespace intact
    download_url = BOSTON_OPEN_DATA_DOWNLOAD_TEMPLATE.format(slug=encoded_slug)
    logger.info("Using Boston parcel download url %s", download_url)
    return download_url


def _apply_boston_catalog_override(catalog: Dict[int, MassGISTown]) -> Dict[int, MassGISTown]:
    try:
        shapefile_url = _resolve_boston_shapefile_url()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to resolve Boston parcel download URL: %s", exc)
        return catalog

    if not shapefile_url:
        return catalog

    existing = catalog.get(BOSTON_TOWN_ID)
    name = existing.name if existing else "BOSTON"
    fiscal_year = existing.fiscal_year if existing else BOSTON_DEFAULT_FISCAL_YEAR
    gdb_url = existing.gdb_url if existing else None

    catalog[BOSTON_TOWN_ID] = MassGISTown(
        town_id=BOSTON_TOWN_ID,
        name=name,
        shapefile_url=shapefile_url,
        gdb_url=gdb_url,
        fiscal_year=fiscal_year,
        dataset_slug=BOSTON_DATASET_SLUG,
    )
    return catalog


def _normalize_neighborhood_slug(value: str) -> str:
    cleaned = re.sub(r"[^A-Z0-9]+", "-", value.upper()).strip("-")
    return cleaned or value.upper()


def _ensure_boston_neighborhoods_file() -> Optional[Path]:
    if BOSTON_NEIGHBORHOODS_PATH.exists():
        return BOSTON_NEIGHBORHOODS_PATH

    # First try to copy a packaged fallback file (kept in source control).
    if BOSTON_NEIGHBORHOODS_STATIC_PATH.exists():
        try:
            BOSTON_NEIGHBORHOODS_PATH.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(BOSTON_NEIGHBORHOODS_STATIC_PATH, BOSTON_NEIGHBORHOODS_PATH)
            logger.info(
                "Copied packaged Boston neighborhoods file from %s",
                BOSTON_NEIGHBORHOODS_STATIC_PATH,
            )
            return BOSTON_NEIGHBORHOODS_PATH
        except OSError as exc:
            logger.warning(
                "Unable to copy packaged Boston neighborhoods file: %s",
                exc,
            )
            # Fallback to reading directly from the static path.
            return BOSTON_NEIGHBORHOODS_STATIC_PATH

    # Otherwise attempt to download from known public endpoints.
    download_candidates: List[str] = []
    if BOSTON_NEIGHBORHOODS_OVERRIDE_URL:
        download_candidates.append(BOSTON_NEIGHBORHOODS_OVERRIDE_URL)
    if BOSTON_NEIGHBORHOODS_DATASET_SLUG:
        download_candidates.append(
            BOSTON_OPEN_DATA_GEOJSON_TEMPLATE.format(
                slug=BOSTON_NEIGHBORHOODS_DATASET_SLUG
            )
        )
    download_candidates.extend(
        [
            "https://opendata.arcgis.com/datasets/cf4bfb71a6f64620a505c4ccd32f4a24_0.geojson",
            "https://opendata.arcgis.com/datasets/35cb9b45716b43b4b47c0830f2b18d62_0.geojson",
        ]
    )

    seen_urls: set[str] = set()
    for url in download_candidates:
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        try:
            _download_file(url, BOSTON_NEIGHBORHOODS_PATH, timeout=60)
            logger.info("Downloaded Boston neighborhoods GeoJSON from %s", url)
            return BOSTON_NEIGHBORHOODS_PATH
        except MassGISDownloadError as exc:
            logger.warning("Unable to download Boston neighborhoods from %s: %s", url, exc)
            continue

    logger.warning("Boston neighborhoods GeoJSON could not be located or downloaded.")
    return None


@lru_cache(maxsize=1)
def _load_boston_neighborhood_geojson() -> Optional[Dict[str, Any]]:
    data_path = _ensure_boston_neighborhoods_file()
    if data_path is None or not Path(data_path).exists():
        logger.info(
            "Boston neighborhoods file not found (checked %s)",
            data_path or BOSTON_NEIGHBORHOODS_PATH,
        )
        return None

    try:
        with data_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to load Boston neighborhoods GeoJSON: %s", exc)
        return None

    features = data.get("features")
    if not isinstance(features, list):
        data["features"] = []
        return data

    for feature in features:
        properties = feature.setdefault("properties", {})
        name_value = properties.get("name") or properties.get("Name")
        if not name_value:
            continue
        properties["slug"] = _normalize_neighborhood_slug(str(name_value))
    return data


def _point_in_ring(lon: float, lat: float, ring: List[Tuple[float, float]]) -> bool:
    if len(ring) < 3:
        return False
    inside = False
    x = lon
    y = lat
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        intersects = ((yi > y) != (yj > y)) and (
            x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside
        j = i
    return inside


def _polygon_contains_point(rings: List[List[Tuple[float, float]]], lon: float, lat: float) -> bool:
    if not rings:
        return False
    if not _point_in_ring(lon, lat, rings[0]):
        return False
    for hole in rings[1:]:
        if _point_in_ring(lon, lat, hole):
            return False
    return True


def _neighborhood_contains_point(neighborhood: BostonNeighborhood, lon: float, lat: float) -> bool:
    minx, miny, maxx, maxy = neighborhood.bbox
    if lon < minx or lon > maxx or lat < miny or lat > maxy:
        return False
    for polygon in neighborhood.polygons:
        if _polygon_contains_point(polygon, lon, lat):
            return True
    return False


def _bbox_intersects(a: Tuple[float, float, float, float], b: Tuple[float, float, float, float]) -> bool:
    aw, as_, ae, an = a
    bw, bs, be, bn = b
    return not (ae < bw or aw > be or an < bs or as_ > bn)


@lru_cache(maxsize=1)
def _load_boston_neighborhood_index() -> Dict[str, BostonNeighborhood]:
    neighborhoods: Dict[str, BostonNeighborhood] = {}
    geojson = _load_boston_neighborhood_geojson()
    if not geojson:
        return neighborhoods

    features = geojson.get("features") or []
    for feature in features:
        geometry = feature.get("geometry") or {}
        coords = geometry.get("coordinates")
        gtype = geometry.get("type")
        if not coords or gtype not in {"Polygon", "MultiPolygon"}:
            continue

        properties = feature.get("properties") or {}
        name_value = properties.get("name") or properties.get("Name")
        if not name_value:
            continue
        slug = properties.get("slug") or _normalize_neighborhood_slug(str(name_value))
        properties["slug"] = slug

        polygons: List[List[List[Tuple[float, float]]]] = []
        minx = float("inf")
        miny = float("inf")
        maxx = float("-inf")
        maxy = float("-inf")

        def _process_polygon(polygon_coords: List[List[List[float]]]) -> None:
            nonlocal minx, miny, maxx, maxy
            polygon_rings: List[List[Tuple[float, float]]] = []
            for ring in polygon_coords:
                normalized_ring: List[Tuple[float, float]] = []
                for vertex in ring:
                    if len(vertex) < 2:
                        continue
                    lon = float(vertex[0])
                    lat = float(vertex[1])
                    normalized_ring.append((lon, lat))
                    if lon < minx:
                        minx = lon
                    if lon > maxx:
                        maxx = lon
                    if lat < miny:
                        miny = lat
                    if lat > maxy:
                        maxy = lat
                if normalized_ring:
                    polygon_rings.append(normalized_ring)
            if polygon_rings:
                polygons.append(polygon_rings)

        if gtype == "Polygon":
            _process_polygon(coords)
        else:  # MultiPolygon
            for poly in coords:
                _process_polygon(poly)

        if not polygons:
            continue

        bbox = (minx, miny, maxx, maxy)
        neighborhoods[slug] = BostonNeighborhood(
            name=str(name_value),
            slug=slug,
            polygons=polygons,
            bbox=bbox,
            acres=_to_number(properties.get("acres")),
            square_miles=_to_number(properties.get("sqmiles")),
            neighborhood_id=_clean_string(properties.get("neighborhood_id")),
        )

    return neighborhoods


def _get_boston_neighborhood(value: str) -> Optional[BostonNeighborhood]:
    if not value:
        return None
    slug = _normalize_neighborhood_slug(value)
    return _load_boston_neighborhood_index().get(slug)


def get_boston_neighborhoods() -> List[Dict[str, object]]:
    neighborhoods = _load_boston_neighborhood_index()
    response: List[Dict[str, object]] = []
    for neighborhood in sorted(neighborhoods.values(), key=lambda item: item.name):
        west, south, east, north = neighborhood.bbox
        response.append(
            {
                "name": neighborhood.name,
                "slug": neighborhood.slug,
                "neighborhood_id": neighborhood.neighborhood_id,
                "acres": neighborhood.acres,
                "square_miles": neighborhood.square_miles,
                "bbox": {
                    "west": west,
                    "south": south,
                    "east": east,
                    "north": north,
                },
            }
        )
    return response


def get_boston_neighborhoods_geojson() -> Dict[str, Any]:
    geojson = _load_boston_neighborhood_geojson()
    if geojson is None:
        return {"type": "FeatureCollection", "features": []}
    return geojson


def _request_json(
    url: str,
    *,
    method: str = "GET",
    data: Optional[bytes] = None,
    timeout: int = 30,
    headers: Optional[Dict[str, str]] = None,
) -> dict:
    req_headers = {"User-Agent": USER_AGENT}
    if headers:
        req_headers.update(headers)
    parsed = urlparse(url)
    if parsed.hostname and "opendata.arcgis.com" in parsed.hostname:
        referer = BOSTON_OPEN_DATA_DOMAINS[0]
        req_headers.setdefault("Referer", referer)
        req_headers.setdefault("Origin", referer)
        req_headers.setdefault("Accept", "application/json")
        req_headers.setdefault("X-Requested-With", "XMLHttpRequest")
    req = request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout) as response:
            encoding = response.headers.get_content_charset("utf-8") or "utf-8"
            raw = response.read()
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        logger.error(
            "HTTP error %s calling %s: %s %s",
            exc.code,
            url,
            exc.reason,
            detail.strip()[:500],
        )
        raise
    try:
        return json.loads(raw.decode(encoding, errors="replace"))
    except json.JSONDecodeError as exc:  # noqa: BLE001
        logger.debug("Unable to parse JSON response from %s: %s", url, exc)
        raise


def _extract_boston_dataset_identifier(source: Optional[str]) -> Optional[str]:
    if not source:
        return None
    source = source.strip()
    if not source:
        return None
    if "://" not in source:
        return source
    parsed = urlparse(source)
    match = re.search(r"/datasets/([^/]+)/", parsed.path, flags=re.IGNORECASE)
    if match:
        return parse.unquote(match.group(1))
    return None


def _start_boston_download_job(dataset_id: str) -> str:
    slug = parse.quote(dataset_id, safe=":@")
    url = f"{BOSTON_OPENDATA_API_BASE}/datasets/{slug}/downloads"
    referer = f"https://bostonopendata-boston.opendata.arcgis.com/datasets/{dataset_id}/about"
    origin = "https://bostonopendata-boston.opendata.arcgis.com"
    payload = json.dumps(
        {
            "format": BOSTON_DOWNLOAD_FORMAT,
            "spatialRefId": BOSTON_DOWNLOAD_SRID,
            "where": BOSTON_DOWNLOAD_WHERE,
        }
    ).encode("utf-8")
    response = _request_json(
        url,
        method="POST",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Referer": referer,
            "Origin": origin,
        },
    )
    data = response.get("data") or {}
    job_id = data.get("id") or data.get("attributes", {}).get("downloadId")
    if not job_id:
        raise MassGISDownloadError("Boston download job response missing identifier.")
    return str(job_id)


def _poll_boston_download_job(dataset_id: str, job_id: str) -> Tuple[str, Optional[str]]:
    slug = parse.quote(dataset_id, safe=":@")
    job = parse.quote(job_id, safe="")
    url = f"{BOSTON_OPENDATA_API_BASE}/datasets/{slug}/downloads/{job}"
    referer = f"https://bostonopendata-boston.opendata.arcgis.com/datasets/{dataset_id}/about"
    origin = "https://bostonopendata-boston.opendata.arcgis.com"
    response = _request_json(url, timeout=30, headers={"Referer": referer, "Origin": origin})
    data = response.get("data") or {}
    attributes = data.get("attributes") or {}
    status = (attributes.get("status") or "").lower()

    links = data.get("links") or {}
    candidates = [
        attributes.get("downloadUrl"),
        attributes.get("resultUrl"),
        attributes.get("contentUrl"),
        attributes.get("url"),
        attributes.get("href"),
        links.get("download"),
        links.get("content"),
    ]
    download_url = next((item for item in candidates if isinstance(item, str) and item.strip()), None)
    if download_url and download_url.startswith("/"):
        download_url = urljoin("https://opendata.arcgis.com", download_url)

    return status, download_url


def _download_boston_dataset(dataset_id: str, zip_path: Path) -> None:
    job_id = _start_boston_download_job(dataset_id)
    deadline = time.time() + 600
    last_status = None

    while time.time() < deadline:
        status, download_url = _poll_boston_download_job(dataset_id, job_id)
        if status != last_status:
            logger.info("Boston download job %s status: %s", job_id, status or "unknown")
            last_status = status

        if status == "completed":
            if not download_url:
                raise MassGISDownloadError("Boston download job completed without a download URL.")
            _download_file(download_url, zip_path)
            return

        if status in {"failed", "error"}:
            raise MassGISDownloadError("Boston download job failed.")

        time.sleep(5)

    raise MassGISDownloadError("Boston download job timed out.")


def _find_use_code_lut(dataset_dir: Path) -> Optional[Path]:
    candidates = sorted(dataset_dir.glob("*UC_LUT*.dbf"))
    return candidates[0] if candidates else None


def massgis_stateplane_to_wgs84(x: float, y: float) -> Optional[Tuple[float, float]]:
    """Convert MassGIS State Plane coordinates to WGS84 longitude/latitude.

    Handles both EPSG:26986 (meters) and EPSG:2249 (US Survey Feet).
    """

    # Some municipalities (e.g., Boston open data) already provide WGS84 values.
    if -180.0 <= x <= 180.0 and -90.0 <= y <= 90.0:
        return x, y

    try:
        # Check if coordinates are in feet (EPSG:2249) based on magnitude
        # State Plane feet coordinates are typically > 500,000
        # State Plane meters coordinates are typically < 500,000
        if x > 500000 or y > 2000000:
            # Convert from US Survey Feet to meters
            US_SURVEY_FOOT_TO_METERS = 0.3048006096012192
            x = x * US_SURVEY_FOOT_TO_METERS
            y = y * US_SURVEY_FOOT_TO_METERS

        x_prime = x - _MA_FALSE_EASTING
        y_prime = _MA_RHO0 - (y - _MA_FALSE_NORTHING)

        rho = math.copysign(math.hypot(x_prime, y_prime), _MA_N)
        if rho == 0:
            # At the projection origin; longitude equals central meridian.
            return math.degrees(_MA_CENTRAL_MERIDIAN), 90.0 if _MA_N > 0 else -90.0

        theta = math.atan2(x_prime, y_prime)
        t_val = math.pow(rho / (_MA_SEMI_MAJOR_AXIS * _MA_F), 1 / _MA_N)
        phi = math.pi / 2 - 2 * math.atan(t_val)

        for _ in range(5):
            esin = _MA_ECCENTRICITY * math.sin(phi)
            phi = math.pi / 2 - 2 * math.atan(
                t_val * math.pow((1 - esin) / (1 + esin), _MA_ECCENTRICITY / 2)
            )

        lam = _MA_CENTRAL_MERIDIAN + theta / _MA_N
        lon = math.degrees(lam)
        lat = math.degrees(phi)
        return lon, lat
    except Exception as exc:  # noqa: BLE001
        logger.debug("Unable to convert MassGIS coordinates (%s, %s): %s", x, y, exc)
        return None


def get_property_imagery_url(
    longitude: float,
    latitude: float,
    *,
    view_meters: float = 150,
    width: int = 800,
    height: int = 600,
) -> str:
    """Return an ArcGIS World Imagery export URL centered on the provided lon/lat."""

    meters_per_degree_lat = 111_320
    meters_per_degree_lon = meters_per_degree_lat * max(math.cos(math.radians(latitude)), 0.0001)

    delta_lat = (view_meters / meters_per_degree_lat) / 2
    delta_lon = (view_meters / meters_per_degree_lon) / 2

    bbox = f"{longitude - delta_lon},{latitude - delta_lat},{longitude + delta_lon},{latitude + delta_lat}"
    params = {
        "bbox": bbox,
        "bboxSR": 4326,
        "imageSR": 4326,
        "size": f"{width},{height}",
        "format": "jpgpng",
        "f": "image",
        "transparent": "false",
    }
    return f"{ESRI_WORLD_IMAGERY_EXPORT}?{parse.urlencode(params)}"


@lru_cache(maxsize=32)
def _load_use_code_lut(dataset_dir: str) -> Dict[str, str]:
    if shapefile is None:
        logger.debug("pyshp not available; cannot load use code lookup table.")
        return {}

    directory = Path(dataset_dir)
    lut_path = _find_use_code_lut(directory)
    if lut_path is None:
        logger.debug("No use code look-up table found in %s", directory)
        return {}

    reader = shapefile.Reader(shp=None, shx=None, dbf=str(lut_path))
    field_names = [field[0] for field in reader.fields[1:]]

    code_index = None
    desc_index = None

    for index, raw_name in enumerate(field_names):
        name = raw_name.strip().upper()
        if name in {"USE_CODE", "USECODE", "CODE"} and code_index is None:
            code_index = index
        if name in {"USE_DESC", "USEDESC", "DESCRIPTION", "DESC"} and desc_index is None:
            desc_index = index

    if code_index is None:
        logger.debug("Use code column not found in %s", lut_path)
        reader.close()
        return {}

    lookup: Dict[str, str] = {}
    try:
        for raw_record in reader.iterRecords():
            code_value = raw_record[code_index]
            code = _clean_string(code_value)
            if not code:
                continue

            desc_value = raw_record[desc_index] if desc_index is not None else None
            description = _clean_string(desc_value) if desc_index is not None else None

            normalized = code.upper()
            lookup[normalized] = description or code
    finally:
        reader.close()

    return lookup


def get_massgis_property_type_choices(town_id: int) -> List[Tuple[str, str]]:
    town = _get_massgis_town(town_id)
    dataset_dir = _ensure_massgis_dataset(town)
    lookup = _load_use_code_lut(str(dataset_dir))
    codes_in_dataset: set[str] = set()
    for record in _load_assess_records(str(dataset_dir)):
        code_value = _clean_string(record.get("USE_CODE"))
        if not code_value:
            continue
        normalized = code_value.upper()
        codes_in_dataset.add(normalized)

    if not codes_in_dataset:
        return []

    def _label_for_code(code: str) -> str:
        return lookup.get(code) or lookup.get(code.lstrip("0")) or code

    options = sorted(
        ((code, _label_for_code(code)) for code in codes_in_dataset),
        key=lambda item: (item[1] or item[0]),
    )

    formatted: List[Tuple[str, str]] = []
    for code, label in options:
        display = label or code
        if label and code:
            display = f"{label} ({code})"
        formatted.append((code, display))
    return formatted


def _format_catalog_name(raw: str) -> str:
    cleaned = raw.replace("_", " ").replace("-", " ").strip()
    if not cleaned:
        return raw.title()
    if " " not in cleaned:
        cleaned = re.sub(r"(?<!^)(?=[A-Z])", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.title()


def _parse_catalog_slug(slug: str) -> Optional[Tuple[int, str, Optional[str]]]:
    match = re.search(r"_(M\d{3})_", slug, flags=re.IGNORECASE)
    if not match:
        return None

    code = match.group(1)
    try:
        town_id = int(code[1:])
    except ValueError:
        return None

    suffix = slug[match.end():]
    suffix = suffix.replace("_", " ").strip()
    if not suffix:
        return None

    tokens = [token for token in re.split(r"\s+", suffix) if token]
    fiscal_year = None
    name_tokens: List[str] = []

    for token in tokens:
        upper = token.upper()
        if upper.startswith("FY") and upper[2:].isdigit():
            fiscal_year = upper
            continue
        name_tokens.append(token)

    if not name_tokens:
        name_tokens = tokens

    display_name = _format_catalog_name(" ".join(name_tokens))
    return town_id, display_name, fiscal_year


def _fetch_massgis_catalog_from_directory() -> Dict[int, MassGISTown]:
    html = _download_massgis_directory_listing()
    parser = _MassGISCatalogLinkParser()
    parser.feed(html)

    candidates: Dict[int, List[MassGISTown]] = {}
    for href in parser.links:
        filename = parse.unquote(urlparse(href).path.split("/")[-1])
        if not filename.lower().endswith(".zip"):
            continue

        slug = Path(filename).stem
        parsed = _parse_catalog_slug(slug)
        if not parsed:
            continue

        town_id, display_name, fiscal_year = parsed
        shapefile_url = urljoin(MASSGIS_CATALOG_LISTING_URL, filename)

        entry = MassGISTown(
            town_id=town_id,
            name=display_name,
            shapefile_url=shapefile_url,
            gdb_url=None,
            fiscal_year=fiscal_year,
            dataset_slug=slug.upper(),
        )

        candidates.setdefault(town_id, []).append(entry)

    entries: Dict[int, MassGISTown] = {}
    for town_id, options in candidates.items():
        entries[town_id] = _choose_catalog_entry(options)

    if not entries:
        raise MassGISDataError("MassGIS directory listing did not include any parcel datasets.")

    return entries


def _load_catalog_from_local_datasets() -> Dict[int, MassGISTown]:
    candidates: Dict[int, List[MassGISTown]] = {}
    index = _load_dataset_index()

    for slug, metadata in index.items():
        parsed = _parse_catalog_slug(slug)
        if not parsed:
            continue
        town_id, display_name, fiscal_year = parsed
        source_url = metadata.get("source_url") or urljoin(
            MASSGIS_CATALOG_LISTING_URL, f"{slug}.zip"
        )
        entry = MassGISTown(
            town_id=town_id,
            name=display_name,
            shapefile_url=source_url,
            gdb_url=metadata.get("gdb_url"),
            fiscal_year=fiscal_year,
            dataset_slug=slug,
        )
        candidates.setdefault(town_id, []).append(entry)

    if not candidates:
        if not GISDATA_ROOT.exists():
            return {}

        for directory in GISDATA_ROOT.iterdir():
            if not directory.is_dir() or directory.name.startswith("."):
                continue
            raw_slug = directory.name
            normalized_slug = raw_slug.replace(" ", "_").upper()
            parsed = _parse_catalog_slug(normalized_slug)
            if not parsed:
                continue
            town_id, display_name, fiscal_year = parsed
            source_url = urljoin(
                MASSGIS_CATALOG_LISTING_URL, f"{normalized_slug}.zip"
            )
            entry = MassGISTown(
                town_id=town_id,
                name=display_name,
                shapefile_url=source_url,
                gdb_url=None,
                fiscal_year=fiscal_year,
                dataset_slug=normalized_slug,
            )
            candidates.setdefault(town_id, []).append(entry)
            _ensure_dataset_index_entry(normalized_slug, source_url=source_url)

    if not candidates:
        return {}

    return {town_id: _choose_catalog_entry(options) for town_id, options in candidates.items()}


def _load_catalog_from_excel() -> Dict[int, MassGISTown]:
    if pd is None or not MASSGIS_EXCEL_PATH.exists():
        return {}

    try:
        dataframe = pd.read_excel(MASSGIS_EXCEL_PATH)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to load MassGIS Excel catalog at %s: %s", MASSGIS_EXCEL_PATH, exc)
        return {}

    entries: Dict[int, MassGISTown] = {}
    for _, row in dataframe.iterrows():
        shapefile_url = row.get("Shapefile Download URL")
        if not isinstance(shapefile_url, str) or not shapefile_url.strip():
            continue
        shapefile_url = shapefile_url.strip()

        try:
            town_id = int(row.get("Town ID"))
        except (TypeError, ValueError):
            continue

        name_value = row.get("Town Name")
        if isinstance(name_value, str) and name_value.strip():
            name = name_value.strip().title()
        else:
            name = f"Town {town_id}"

        fiscal_year_value = row.get("Assessed Fiscal Year")
        fiscal_year = None
        if isinstance(fiscal_year_value, str):
            fiscal_year = fiscal_year_value.strip() or None
        elif isinstance(fiscal_year_value, (int, float)):
            fiscal_year = str(int(fiscal_year_value))

        gdb_url_value = row.get("File GDB Download URL")
        gdb_url = gdb_url_value.strip() if isinstance(gdb_url_value, str) and gdb_url_value.strip() else None

        slug = Path(urlparse(shapefile_url).path).stem
        if not slug:
            continue

        entries[town_id] = MassGISTown(
            town_id=town_id,
            name=name,
            shapefile_url=shapefile_url,
            gdb_url=gdb_url,
            fiscal_year=fiscal_year,
            dataset_slug=slug.upper(),
        )

    return entries
PARCEL_SEARCH_MAX_RESULTS = 250

try:
    import shapefile  # type: ignore
except ImportError:  # noqa: F401
    shapefile = None  # type: ignore[assignment]


@dataclass
class FemaMapResult:
    address: str
    image_url: Optional[str]
    viewer_url: str
    latitude: Optional[float]
    longitude: Optional[float]
    image_data: Optional[str] = None


@dataclass
class ParcelShapeResult:
    found: bool
    svg_markup: Optional[str]
    attribute_rows: List[Tuple[str, str]]
    centroid: Optional[Tuple[float, float]]
    area: Optional[float]
    width: Optional[float]
    height: Optional[float]
    source_hint: Optional[str]
    message: Optional[str]


class SkipTraceError(Exception):
    """Raised when skip trace data cannot be retrieved."""


@dataclass
class SkipTracePhone:
    number: Optional[str]
    type: Optional[str]
    score: Optional[float]
    dnc: Optional[str]


@dataclass
class SkipTraceResult:
    owner_name: Optional[str]
    email: Optional[str]
    phones: List[SkipTracePhone]
    raw_payload: Optional[Dict[str, object]] = None


@dataclass(frozen=True)
class MassGISTown:
    town_id: int
    name: str
    shapefile_url: str
    gdb_url: Optional[str]
    fiscal_year: Optional[str]
    dataset_slug: str


@dataclass(frozen=True)
class BostonNeighborhood:
    name: str
    slug: str
    polygons: List[List[List[Tuple[float, float]]]]
    bbox: Tuple[float, float, float, float]
    acres: Optional[float] = None
    square_miles: Optional[float] = None
    neighborhood_id: Optional[str] = None


from .models import AttomData, ParcelMarketValue


@dataclass
class ParcelSearchResult:
    town: MassGISTown
    loc_id: str
    site_address: str
    site_city: Optional[str]
    site_zip: Optional[str]
    owner_name: Optional[str]
    owner_address: Optional[str]
    absentee: bool
    property_category: str
    use_code: Optional[str]
    property_type: Optional[str]
    style: Optional[str]
    total_value: Optional[float]
    lot_size: Optional[float]
    zoning: Optional[str]
    equity_percent: Optional[float]
    units: Optional[int]
    attributes: Dict[str, object]
    estimated_mortgage_balance: Optional[float] = None
    estimated_equity_value: Optional[float] = None
    estimated_roi_percent: Optional[float] = None
    estimated_mortgage_rate_percent: Optional[float] = None
    estimated_monthly_payment: Optional[float] = None
    attom_data: Optional[AttomData] = None
    units_detail: Optional[List[Dict[str, object]]] = None
    market_value: Optional[float] = None
    market_value_per_sqft: Optional[float] = None
    market_value_updated_at: Optional[datetime] = None
    market_value_confidence: Optional[float] = None
    market_value_methodology: Optional[str] = None
    market_value_methodology_label: Optional[str] = None
    market_value_payload: Optional[Dict[str, object]] = None
    market_value_comparable_count: Optional[int] = None
    market_value_comparable_avg_psf: Optional[float] = None
    market_value_comparable_value: Optional[float] = None


class MassGISDataError(Exception):
    """Raised when MassGIS resources cannot be accessed."""


class MassGISDownloadError(MassGISDataError):
    """Raised when MassGIS shapefiles fail to download."""


def fetch_fema_map_for_lead(
    lead,
    *,
    timeout: int = 10,
    fallback_lon_lat: Optional[Tuple[float, float]] = None,
    download_image: bool = False,
) -> Optional[FemaMapResult]:
    """
    Attempt to retrieve a FEMA flood map image for the provided lead.

    Falls back to returning only the FEMA viewer link when imagery cannot be fetched.
    """
    address = _compose_site_address(lead)
    if not address:
        return None

    viewer_url = f"{FEMA_VIEWER_BASE_URL}?address={parse.quote_plus(address)}"

    lon_lat: Optional[Tuple[float, float]] = None
    if fallback_lon_lat is not None:
        lon_lat = fallback_lon_lat
    else:
        try:
            lon_lat = _geocode_address(address, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Geocoding failed for address '%s': %s", address, exc)

    if lon_lat is None:
        return FemaMapResult(
            address=address,
            image_url=None,
            viewer_url=viewer_url,
            latitude=None,
            longitude=None,
        )

    longitude, latitude = lon_lat

    image_url = _request_fema_map_image(longitude, latitude, timeout=timeout)
    image_data = None

    if download_image and image_url:
        try:
            image_data = _download_image_as_data_url(image_url, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "FEMA map export failed for address '%s' (lon=%s, lat=%s): %s",
                address,
                longitude,
                latitude,
                exc,
            )
            image_url = None
            image_data = None

    return FemaMapResult(
        address=address,
        image_url=image_url,
        viewer_url=viewer_url,
        latitude=latitude,
        longitude=longitude,
        image_data=image_data,
    )


def _compose_site_address(lead) -> Optional[str]:
    components = [
        getattr(lead, "site_address", None),
        getattr(lead, "site_city", None),
        getattr(lead, "site_state", None),  # Optional: only present if model provides it
        getattr(lead, "site_zip", None),
    ]

    # Fallback to owner state if no site-specific state exists
    if getattr(lead, "site_state", None) in (None, "") and getattr(lead, "owner_state", None):
        components.insert(-1, lead.owner_state)

    cleaned = [str(value).strip() for value in components if value and str(value).strip()]
    return ", ".join(cleaned) if cleaned else None


EARTH_RADIUS_MILES = 3958.7613


def _extract_point_coordinates(record: Dict[str, object]) -> Optional[Tuple[float, float, str]]:
    candidates = [
        ("X_COORD", "Y_COORD", "stateplane"),
        ("POINT_X", "POINT_Y", "stateplane"),
        ("X", "Y", "stateplane"),
        ("LONGITUDE", "LATITUDE", "wgs84"),
        ("LON", "LAT", "wgs84"),
        ("POINT_LON", "POINT_LAT", "wgs84"),
    ]

    for x_key, y_key, system in candidates:
        x_value = _to_number(record.get(x_key))
        y_value = _to_number(record.get(y_key))
        if x_value is not None and y_value is not None:
            return float(x_value), float(y_value), system
    return None


def _ensure_wgs84(point: Tuple[float, float, str]) -> Optional[Tuple[float, float]]:
    x, y, system = point
    if system == "wgs84":
        return (x, y)
    if system == "stateplane":
        converted = massgis_stateplane_to_wgs84(x, y)
        if converted:
            return converted
    return None


def _haversine_miles(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    lon1_rad = math.radians(lon1)
    lat1_rad = math.radians(lat1)
    lon2_rad = math.radians(lon2)
    lat2_rad = math.radians(lat2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS_MILES * c


def _distance_miles_between(
    point_a: Tuple[float, float, str],
    point_b: Tuple[float, float, str],
) -> Optional[float]:
    ax, ay, asystem = point_a
    bx, by, bsystem = point_b

    if asystem == bsystem == "stateplane":
        return math.hypot(ax - bx, ay - by) / 5280.0

    a_wgs = _ensure_wgs84(point_a)
    b_wgs = _ensure_wgs84(point_b)
    if a_wgs and b_wgs:
        return _haversine_miles(a_wgs[0], a_wgs[1], b_wgs[0], b_wgs[1])

    return None


def _normalize_street_fragment(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    street = text.split(",", 1)[0]
    if not street:
        return None

    street = street.upper()
    replacements = {
        " STREET": " ST",
        " STREE": " ST",
        " ROAD": " RD",
        " AVENUE": " AVE",
        " COURT": " CT",
        " DRIVE": " DR",
        " LANE": " LN",
        " PLACE": " PL",
        " TERRACE": " TER",
        " BOULEVARD": " BLVD",
        " CIRCLE": " CIR",
        " HIGHWAY": " HWY",
        " SOUTH": " S",
        " NORTH": " N",
        " EAST": " E",
        " WEST": " W",
    }
    for key, value in replacements.items():
        if street.endswith(key):
            street = street[: -len(key)] + value
    normalized = _normalize_compare_value((street,))
    return normalized or None


def _find_reference_point_from_records(
    records: Iterable[Dict[str, object]],
    address: str,
) -> Optional[Tuple[float, float, str]]:
    target_normalized = _normalize_street_fragment(address)
    if not target_normalized:
        return None

    partial_match: Optional[Tuple[float, float, str]] = None

    for record in records:
        candidate_address = _extract_site_address(record)
        normalized_candidate = _normalize_street_fragment(candidate_address)
        if not normalized_candidate:
            continue

        if normalized_candidate == target_normalized:
            point = _extract_point_coordinates(record)
            if point:
                return point

        if target_normalized in normalized_candidate or normalized_candidate in target_normalized:
            if partial_match is None:
                point = _extract_point_coordinates(record)
                if point:
                    partial_match = point

    return partial_match


def _lookup_mortgage_rate(year: int) -> float:
    if not MORTGAGE_RATE_BY_YEAR:
        return 6.0
    years = sorted(MORTGAGE_RATE_BY_YEAR)
    if year <= years[0]:
        return MORTGAGE_RATE_BY_YEAR[years[0]]
    if year >= years[-1]:
        return MORTGAGE_RATE_BY_YEAR[years[-1]]
    return MORTGAGE_RATE_BY_YEAR.get(year) or MORTGAGE_RATE_BY_YEAR[years[0]]


def _estimate_remaining_balance(
    sale_price: float,
    sale_date: datetime,
    *,
    ltv: float = DEFAULT_INITIAL_LTV,
    term_years: int = DEFAULT_MORTGAGE_TERM_YEARS,
) -> Optional[Tuple[float, Optional[float], Optional[float]]]:
    if sale_price is None or sale_price <= 0 or sale_date is None:
        return None

    principal = sale_price * max(min(ltv, 1.0), 0.0)
    if principal <= 0:
        return None

    total_months = term_years * 12
    if total_months <= 0:
        return None

    today = datetime.now(timezone.utc).date()
    sale_date = sale_date.date() if isinstance(sale_date, datetime) else sale_date

    months_elapsed = (today.year - sale_date.year) * 12 + (today.month - sale_date.month)
    if today.day < sale_date.day:
        months_elapsed -= 1
    if months_elapsed < 0:
        months_elapsed = 0

    annual_rate_percent = _lookup_mortgage_rate(sale_date.year)
    annual_rate = annual_rate_percent / 100.0
    monthly_rate = annual_rate / 12.0

    if months_elapsed <= 0:
        return principal, None, annual_rate_percent
    if months_elapsed >= total_months:
        return 0.0, None, annual_rate_percent

    if monthly_rate <= 0:
        remaining = principal * (1 - months_elapsed / total_months)
        return max(remaining, 0.0), None, annual_rate_percent

    factor = (1 + monthly_rate) ** total_months
    payment = principal * monthly_rate * factor / (factor - 1)
    remaining_factor = (1 + monthly_rate) ** months_elapsed
    balance = principal * remaining_factor - payment * ((remaining_factor - 1) / monthly_rate)
    return max(balance, 0.0), payment, annual_rate_percent


def _geocode_address(address: str, *, timeout: int) -> Optional[Tuple[float, float]]:
    params = {
        "address": address,
        "benchmark": "Public_AR_Current",
        "format": "json",
    }
    payload = _http_get_json(GEOCODER_URL, params=params, timeout=timeout)

    matches = payload.get("result", {}).get("addressMatches") or []

    if not matches:
        logger.info("No geocoding matches found for '%s'", address)
        return None

    best_match = matches[0]
    coordinates = best_match.get("coordinates")
    if not coordinates:
        return None

    return coordinates.get("x"), coordinates.get("y")


def geocode_address(address: str, *, timeout: int = 10) -> Optional[Tuple[float, float]]:
    """Public helper to geocode a textual address into (longitude, latitude)."""
    try:
        return _geocode_address(address, timeout=timeout)
    except URLError as exc:
        logger.warning("Geocoding request for '%s' failed: %s", address, exc)
        return None
    except TimeoutError as exc:
        logger.warning("Geocoding request for '%s' timed out: %s", address, exc)
        return None


def _request_fema_map_image(longitude: float, latitude: float, *, timeout: int) -> Optional[str]:
    delta = 0.01  # Roughly ~1km window; adjust as needed for larger parcels
    bbox = f"{longitude - delta},{latitude - delta},{longitude + delta},{latitude + delta}"

    params = {
        "bbox": bbox,
        "bboxSR": 4326,
        "imageSR": 3857,
        "size": "1000,800",
        "format": "png32",
        "f": "image",
        "transparent": "true",
        "dpi": 96,
    }

    # Include all layers by default; FEMA honours default visibility
    return f"{FEMA_EXPORT_URL}?{parse.urlencode(params)}"


def _download_image_as_data_url(url: str, *, timeout: int = 10) -> Optional[str]:
    request_object = request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with request.urlopen(request_object, timeout=timeout) as response:
            content_type = response.headers.get("Content-Type", "image/png")
            raw = response.read()
            if not raw:
                return None
            encoded = base64.b64encode(raw).decode("ascii")
            return f"data:{content_type};base64,{encoded}"
    except Exception as exc:  # noqa: BLE001
        logger.debug("Unable to download FEMA image %s: %s", url, exc)
        return None


def get_massgis_town_choices(include_placeholder: bool = True) -> List[Tuple[str, str]]:
    try:
        catalog = _load_massgis_catalog()
    except MassGISDataError as exc:
        logger.warning("Unable to load MassGIS catalog: %s", exc)
        return []

    choices: List[Tuple[str, str]] = []
    if include_placeholder:
        choices.append(("", "Select a town"))

    # Add all towns including Boston
    for entry in sorted(catalog.values(), key=lambda item: item.name):
        label = entry.name
        if entry.fiscal_year:
            label = f"{label} (FY {entry.fiscal_year})"
        choices.append((str(entry.town_id), label))

    # Sort choices by label (excluding placeholder)
    if include_placeholder:
        placeholder = choices[0]
        sorted_choices = sorted(choices[1:], key=lambda x: x[1])
        choices = [placeholder] + sorted_choices
    else:
        choices = sorted(choices, key=lambda x: x[1])

    return choices


def search_massgis_parcels(
    town_id: int,
    *,
    property_category: str = "any",
    commercial_subtype: str = "any",
    address_contains: str = "",
    style_contains: str = "",
    property_type: str = "any",
    equity_min: Optional[float] = None,
    absentee: str = "any",
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    min_years_owned: Optional[int] = None,
    max_years_owned: Optional[int] = None,
    proximity_address: Optional[str] = None,
    proximity_radius_miles: Optional[float] = None,
    limit: Optional[int] = None,
    shape_filter: Optional[Dict[str, Any]] = None,
) -> Tuple[MassGISTown, List[ParcelSearchResult], int]:
    town = _get_massgis_town(town_id)
    dataset_dir = _ensure_massgis_dataset(town)

    records = _load_assess_records(str(dataset_dir))
    use_code_lookup = _load_use_code_lut(str(dataset_dir))
    property_filter = (property_category or "any").lower()
    address_query = (address_contains or "").strip().lower()
    style_query = (style_contains or "").strip().lower()
    absentee_filter = (absentee or "any").lower()
    property_type_filter = _clean_string(property_type) or "any"
    property_type_filter = property_type_filter.upper()
    today = datetime.now(timezone.utc).date()

    min_price_value = float(min_price) if min_price is not None else None
    max_price_value = float(max_price) if max_price is not None else None
    min_years_owned_value = float(min_years_owned) if min_years_owned is not None else None
    max_years_owned_value = float(max_years_owned) if max_years_owned is not None else None

    radius_limit_miles = None
    reference_point: Optional[Tuple[float, float, str]] = None
    radius_center_source = None
    polygon_filter: Optional[List[Tuple[float, float]]] = None

    if shape_filter:
        if shape_filter.get("type") == "circle":
            try:
                center_lat = float(shape_filter.get("center_lat"))
                center_lng = float(shape_filter.get("center_lng"))
                radius_limit_miles = float(shape_filter.get("radius_miles"))
                reference_point = (center_lng, center_lat, "wgs84")
                radius_center_source = shape_filter.get("source") or "boundary"
            except (TypeError, ValueError):
                radius_limit_miles = None
                reference_point = None
        elif shape_filter.get("type") == "polygon":
            coords = shape_filter.get("coordinates") or []
            polygon_points: List[Tuple[float, float]] = []
            for coord in coords:
                try:
                    polygon_points.append((float(coord[0]), float(coord[1])))
                except (TypeError, ValueError, IndexError):
                    continue
            if polygon_points:
                polygon_filter = polygon_points
    center_address = _clean_string(proximity_address)
    if reference_point is None and center_address and proximity_radius_miles is not None:
        try:
            radius_limit_miles = float(proximity_radius_miles)
        except (TypeError, ValueError):
            radius_limit_miles = None
        if radius_limit_miles is not None and radius_limit_miles >= 0:
            geocode_query = center_address
            if town and town.name and town.name.lower() not in geocode_query.lower():
                geocode_query = f"{center_address}, {town.name}, Massachusetts"
            coords = geocode_address(geocode_query)
            if coords:
                lon, lat = coords
                reference_point = (float(lon), float(lat), "wgs84")
                radius_center_source = "geocode"
            if reference_point is None:
                reference_point = _find_reference_point_from_records(records, center_address)
                if reference_point is not None:
                    radius_center_source = "parcel"
                    logger.info("Radius filter: using parcel-derived reference point for '%s'", center_address)
                else:
                    logger.warning("Radius filter: unable to derive parcel reference point for '%s'", center_address)
            if reference_point is None:
                radius_limit_miles = None
        else:
            radius_limit_miles = None

    results: List[ParcelSearchResult] = []
    total_matches = 0
    radius_excluded = 0

    for record in records:
        loc_id = _clean_string(record.get("LOC_ID"))
        if not loc_id:
            continue

        category = _classify_use_code(record.get("USE_CODE"))
        if property_filter in {"residential", "commercial", "industrial"} and category.lower() != property_filter:
            continue

        # Apply commercial subtype filter if category is commercial
        if category.lower() == "commercial" and commercial_subtype != "any":
            subtype = _classify_commercial_subtype(record.get("USE_CODE"))
            # Normalize the subtype for comparison
            subtype_normalized = subtype.lower().replace(" ", "_")
            if subtype_normalized != commercial_subtype:
                continue

        site_address = _extract_site_address(record)
        if address_query and address_query not in (site_address or "").lower():
            continue

        style_value = _clean_string(record.get("STYLE"))
        if style_query and style_query not in (style_value or "").lower():
            continue

        use_code_raw = _clean_string(record.get("USE_CODE"))
        use_code_key = (use_code_raw or "").upper()
        property_type_label = use_code_lookup.get(use_code_key) or use_code_lookup.get(use_code_key.lstrip("0"), use_code_raw)

        if property_type_filter != "ANY":
            if not use_code_key and not use_code_raw:
                continue
            candidate_key = use_code_key or (use_code_raw or "").upper()
            if candidate_key != property_type_filter and candidate_key.lstrip("0") != property_type_filter:
                continue

        absentee_flag = _is_absentee(record)
        if absentee_filter == "absentee" and not absentee_flag:
            continue
        if absentee_filter in {"owner", "owner-occupied"} and absentee_flag:
            continue

        (
            equity_percent,
            estimated_balance,
            equity_value,
            roi_percent,
            annual_rate,
            monthly_payment,
        ) = calculate_equity_metrics(record)
        if equity_min is not None:
            if equity_percent is None or equity_percent < equity_min:
                continue

        assessed_value = _to_number(record.get("TOTAL_VAL"))
        if min_price_value is not None:
            if assessed_value is None or assessed_value < min_price_value:
                continue
        if max_price_value is not None:
            if assessed_value is None or assessed_value > max_price_value:
                continue

        if min_years_owned_value is not None or max_years_owned_value is not None:
            sale_date = _parse_massgis_date(record.get("LS_DATE"))
            if not sale_date:
                continue
            owned_years = (today - sale_date.date()).days / 365.25
            if min_years_owned_value is not None and owned_years < min_years_owned_value:
                continue
            if max_years_owned_value is not None and owned_years > max_years_owned_value:
                continue

        if radius_limit_miles is not None and reference_point is not None:
            target_point = _extract_point_coordinates(record)
            if not target_point:
                continue
            distance_miles = _distance_miles_between(reference_point, target_point)
            if distance_miles is None:
                continue
            if distance_miles > radius_limit_miles:
                radius_excluded += 1
                continue
        if polygon_filter:
            target_point = _extract_point_coordinates(record)
            if not target_point:
                continue
            wgs_point = _ensure_wgs84(target_point)
            if not wgs_point:
                continue
            point_lng, point_lat = wgs_point
            if not _point_in_polygon(point_lat, point_lng, polygon_filter):
                continue

        total_matches += 1
        if limit is not None and len(results) >= limit:
            continue

        result = ParcelSearchResult(
            town=town,
            loc_id=loc_id,
            site_address=site_address or "",
            site_city=_clean_string(record.get("CITY")),
            site_zip=_clean_zip(record.get("ZIP")),
            owner_name=_clean_string(record.get("OWNER1") or record.get("OWN_NAME")),
            owner_address=_compose_owner_address(record),
            absentee=absentee_flag,
            property_category=category,
            use_code=use_code_raw,
            property_type=property_type_label,
            style=style_value,
            total_value=_to_number(record.get("TOTAL_VAL")),
            lot_size=_to_number(record.get("LOT_SIZE")),
            zoning=_clean_string(record.get("ZONING")),
            equity_percent=equity_percent,
            units=_to_int(record.get("UNITS")),
            attributes=record,
            estimated_mortgage_balance=estimated_balance,
            estimated_equity_value=equity_value,
            estimated_roi_percent=roi_percent,
            estimated_mortgage_rate_percent=annual_rate,
            estimated_monthly_payment=monthly_payment,
        )
        results.append(result)

    results.sort(key=lambda item: (item.site_address or "", item.loc_id))
    metadata = {
        "radius_requested": radius_limit_miles is not None,
        "radius_center_found": reference_point is not None,
        "radius_center_source": radius_center_source,
        "radius_excluded_count": radius_excluded,
    }
    return town, results, total_matches, metadata


def has_precomputed_parcels(town_id: int) -> bool:
    """Check if a town has precomputed parcel data available."""
    from .models import MassGISParcel
    return MassGISParcel.objects.filter(town_id=town_id).exists()


def search_precomputed_parcels(
    town_id: Optional[int] = None,
    *,
    property_category: str = "any",
    address_contains: str = "",
    owner_contains: str = "",
    min_price: Optional[float] = None,
    max_price: Optional[float] = None,
    absentee: str = "any",
    min_years_owned: Optional[int] = None,
    max_years_owned: Optional[int] = None,
    limit: Optional[int] = 1000,
) -> List[Dict]:
    """
    Search precomputed MassGISParcel database for instant results.
    Much faster than file-based search - uses SQL indexes.

    Returns list of parcel dicts with all attributes.
    """
    from .models import MassGISParcel
    from django.db.models import Q

    query = MassGISParcel.objects.all()

    # Town filter
    if town_id:
        query = query.filter(town_id=town_id)

    # Property category
    if property_category and property_category.lower() != "any":
        query = query.filter(property_category__iexact=property_category)

    # Address search (case-insensitive contains)
    if address_contains:
        query = query.filter(site_address__icontains=address_contains.strip())

    # Owner search
    if owner_contains:
        query = query.filter(owner_name__icontains=owner_contains.strip())

    # Price range
    if min_price is not None:
        query = query.filter(total_value__gte=int(min_price))
    if max_price is not None:
        query = query.filter(total_value__lte=int(max_price))

    # Absentee filter
    if absentee and absentee.lower() == "yes":
        query = query.filter(absentee=True)
    elif absentee and absentee.lower() == "no":
        query = query.filter(absentee=False)

    # Years owned
    if min_years_owned is not None:
        query = query.filter(years_owned__gte=float(min_years_owned))
    if max_years_owned is not None:
        query = query.filter(years_owned__lte=float(max_years_owned))

    # Apply limit
    if limit:
        query = query[:limit]

    # Execute query and convert to dicts
    parcels = query.values(
        'town_id', 'loc_id', 'site_address', 'site_city', 'site_zip',
        'owner_name', 'owner_address', 'absentee',
        'property_category', 'property_type', 'use_code', 'style',
        'total_value', 'land_value', 'building_value',
        'lot_size', 'living_area', 'units', 'year_built',
        'last_sale_date', 'last_sale_price',
        'equity_percent', 'years_owned',
        'centroid_lon', 'centroid_lat',
    )

    return list(parcels)


def _get_cached_parcel_data(town_id: int, loc_id: str) -> Optional[Dict]:
    """
    Retrieve parcel data from cache if available and not expired.
    Returns None if cache miss or expired.
    """
    from .models import MassGISParcelCache
    from django.utils import timezone

    try:
        cache_entry = MassGISParcelCache.objects.get(town_id=town_id, loc_id=loc_id)

        # Check if expired (90 days)
        if cache_entry.is_expired:
            cache_entry.delete()
            return None

        # Update last_accessed timestamp
        cache_entry.last_accessed = timezone.now()
        cache_entry.save(update_fields=['last_accessed'])

        return cache_entry.parcel_data
    except MassGISParcelCache.DoesNotExist:
        return None


def _cache_parcel_data(town_id: int, loc_id: str, parcel_data: Dict) -> None:
    """
    Store parcel data in cross-user cache.
    Updates existing entry if present, creates new one otherwise.
    """
    from .models import MassGISParcelCache
    from django.utils import timezone

    MassGISParcelCache.objects.update_or_create(
        town_id=town_id,
        loc_id=loc_id,
        defaults={
            'parcel_data': parcel_data,
            'last_accessed': timezone.now(),
        }
    )


def _get_parcel_market_value_entry(town_id: int, loc_id: str) -> Optional[ParcelMarketValue]:
    normalized = _normalize_loc_id(loc_id)
    if not normalized:
        return None
    try:
        return ParcelMarketValue.objects.get(town_id=town_id, loc_id=normalized)
    except ParcelMarketValue.DoesNotExist:  # pragma: no cover - normal miss path
        return None


def _build_market_value_context(
    town_id: int,
    loc_id: str,
    record: Optional[Dict[str, object]] = None,
) -> Dict[str, Optional[object]]:
    market_value = None
    market_value_per_sqft = None
    market_value_confidence = None
    market_value_payload = None
    market_value_methodology = None
    market_value_methodology_label = None
    market_value_updated_at = None
    market_value_comparable_count = None
    market_value_comparable_avg_psf = None
    market_value_comparable_value = None

    entry = _get_parcel_market_value_entry(town_id, loc_id)
    if entry:
        market_value = _decimal_to_float(entry.market_value)
        market_value_per_sqft = _decimal_to_float(entry.market_value_per_sqft)
        market_value_confidence = entry.valuation_confidence
        market_value_payload = entry.payload or None
        market_value_methodology = entry.methodology
        market_value_methodology_label = entry.get_methodology_display()
        market_value_updated_at = entry.valued_at
        market_value_comparable_count = entry.comparable_count
        market_value_comparable_avg_psf = _decimal_to_float(entry.comparable_avg_psf)
        market_value_comparable_value = _decimal_to_float(entry.comparable_value)
        if market_value is not None and record is not None:
            record["MARKET_VALUE"] = market_value

    return {
        "market_value": market_value,
        "market_value_per_sqft": market_value_per_sqft,
        "market_value_confidence": market_value_confidence,
        "market_value_payload": market_value_payload,
        "market_value_methodology": market_value_methodology,
        "market_value_methodology_label": market_value_methodology_label,
        "market_value_updated_at": market_value_updated_at,
        "market_value_comparable_count": market_value_comparable_count,
        "market_value_comparable_avg_psf": market_value_comparable_avg_psf,
        "market_value_comparable_value": market_value_comparable_value,
    }


def _parcel_data_to_dict(parcel: ParcelSearchResult, record: Dict) -> Dict:
    """Convert ParcelSearchResult to cacheable dictionary."""
    return {
        'town_id': parcel.town.town_id,
        'town_name': parcel.town.name,
        'fiscal_year': parcel.town.fiscal_year,
        'shapefile_url': parcel.town.shapefile_url,
        'gdb_url': parcel.town.gdb_url,
        'dataset_slug': parcel.town.dataset_slug,
        'loc_id': parcel.loc_id,
        'site_address': parcel.site_address,
        'site_city': parcel.site_city,
        'site_zip': parcel.site_zip,
        'owner_name': parcel.owner_name,
        'owner_address': parcel.owner_address,
        'absentee': parcel.absentee,
        'property_category': parcel.property_category,
        'use_code': parcel.use_code,
        'property_type': parcel.property_type,
        'style': parcel.style,
        'total_value': parcel.total_value,
        'lot_size': parcel.lot_size,
        'zoning': parcel.zoning,
        'equity_percent': parcel.equity_percent,
        'units': parcel.units,
        'estimated_mortgage_balance': parcel.estimated_mortgage_balance,
        'estimated_equity_value': parcel.estimated_equity_value,
        'estimated_roi_percent': parcel.estimated_roi_percent,
        'estimated_mortgage_rate_percent': parcel.estimated_mortgage_rate_percent,
        'estimated_monthly_payment': parcel.estimated_monthly_payment,
        'attributes': record,
        'units_detail': parcel.units_detail,
        'market_value': parcel.market_value,
        'market_value_per_sqft': parcel.market_value_per_sqft,
        'market_value_updated_at': parcel.market_value_updated_at.isoformat() if parcel.market_value_updated_at else None,
        'market_value_confidence': parcel.market_value_confidence,
        'market_value_methodology': parcel.market_value_methodology,
        'market_value_methodology_label': parcel.market_value_methodology_label,
        'market_value_payload': parcel.market_value_payload,
        'market_value_comparable_count': parcel.market_value_comparable_count,
        'market_value_comparable_avg_psf': parcel.market_value_comparable_avg_psf,
        'market_value_comparable_value': parcel.market_value_comparable_value,
    }


def _dict_to_parcel_data(data: Dict) -> ParcelSearchResult:
    """Convert cached dictionary back to ParcelSearchResult."""
    town_name = data.get('town_name')
    fiscal_year = data.get('fiscal_year')
    shapefile_url = data.get('shapefile_url')
    gdb_url = data.get('gdb_url')
    dataset_slug = data.get('dataset_slug')

    if not shapefile_url or not dataset_slug:
        # Cache entry predates extended metadata – hydrate from canonical catalog.
        town_meta = _get_massgis_town(data['town_id'])
        shapefile_url = shapefile_url or town_meta.shapefile_url
        gdb_url = town_meta.gdb_url if gdb_url is None else gdb_url
        dataset_slug = dataset_slug or town_meta.dataset_slug
        if not town_name:
            town_name = town_meta.name
        if fiscal_year is None:
            fiscal_year = town_meta.fiscal_year

    town = MassGISTown(
        town_id=data['town_id'],
        name=town_name,
        shapefile_url=shapefile_url,
        gdb_url=gdb_url,
        fiscal_year=fiscal_year,
        dataset_slug=dataset_slug,
    )

    return ParcelSearchResult(
        town=town,
        loc_id=data['loc_id'],
        site_address=data['site_address'],
        site_city=data.get('site_city'),
        site_zip=data.get('site_zip'),
        owner_name=data.get('owner_name'),
        owner_address=data.get('owner_address'),
        absentee=data.get('absentee', False),
        property_category=data.get('property_category', ''),
        use_code=data.get('use_code'),
        property_type=data.get('property_type'),
        style=data.get('style'),
        total_value=data.get('total_value'),
        lot_size=data.get('lot_size'),
        zoning=data.get('zoning'),
        equity_percent=data.get('equity_percent'),
        units=data.get('units'),
        attributes=data.get('attributes', {}),
        estimated_mortgage_balance=data.get('estimated_mortgage_balance'),
        estimated_equity_value=data.get('estimated_equity_value'),
        estimated_roi_percent=data.get('estimated_roi_percent'),
        estimated_mortgage_rate_percent=data.get('estimated_mortgage_rate_percent'),
        estimated_monthly_payment=data.get('estimated_monthly_payment'),
        units_detail=data.get('units_detail'),
        market_value=data.get('market_value'),
        market_value_per_sqft=data.get('market_value_per_sqft'),
        market_value_updated_at=_parse_iso_datetime(data.get('market_value_updated_at')),
        market_value_confidence=data.get('market_value_confidence'),
        market_value_methodology=data.get('market_value_methodology'),
        market_value_methodology_label=data.get('market_value_methodology_label'),
        market_value_payload=data.get('market_value_payload'),
        market_value_comparable_count=data.get('market_value_comparable_count'),
        market_value_comparable_avg_psf=data.get('market_value_comparable_avg_psf'),
        market_value_comparable_value=data.get('market_value_comparable_value'),
    )


def get_massgis_parcel_detail(town_id: int, loc_id: str) -> ParcelSearchResult:
    """
    Get detailed parcel information with cross-user caching (90 days).
    """
    target = _normalize_loc_id(loc_id)

    # Try to get from cache first
    cached_data = _get_cached_parcel_data(town_id, target)
    if cached_data:
        logger.debug(f"Cache HIT for parcel {town_id}/{loc_id}")
        return _dict_to_parcel_data(cached_data)

    logger.debug(f"Cache MISS for parcel {town_id}/{loc_id}, loading from source")

    # Cache miss - load from source data
    town = _get_massgis_town(town_id)
    dataset_dir = _ensure_massgis_dataset(town)
    use_code_lookup = _load_use_code_lut(str(dataset_dir))

    best_record = None
    unit_records: List[Dict[str, object]] = []
    for record in _load_assess_records(str(dataset_dir)):
        record_loc = record.get("LOC_ID")
        if _normalize_loc_id(record_loc) == target:
            unit_records.append(record)
            if best_record is None or _should_replace_assess_record(record, best_record):
                best_record = record

    if best_record:
        record = best_record
        category = _classify_use_code(record.get("USE_CODE"))
        (
            equity_percent,
            estimated_balance,
            equity_value,
            roi_percent,
            annual_rate,
            monthly_payment,
        ) = calculate_equity_metrics(record)
        style_value = _clean_string(record.get("STYLE"))
        use_code_raw = _clean_string(record.get("USE_CODE"))
        use_code_key = (use_code_raw or "").upper()
        property_type_label = use_code_lookup.get(use_code_key) or use_code_lookup.get(
            use_code_key.lstrip("0"), use_code_raw
        )
        units_detail = _summarize_unit_records(unit_records)

        market_value_context = _build_market_value_context(town_id, target, record)

        parcel_result = ParcelSearchResult(
            town=town,
            loc_id=_clean_string(record.get("LOC_ID")) or target,
            site_address=_clean_string(record.get("SITE_ADDR")) or "",
            site_city=_clean_string(record.get("SITE_CITY")) or _clean_string(record.get("CITY")),
            site_zip=_clean_zip(record.get("SITE_ZIP")) or _clean_zip(record.get("ZIP")),
            owner_name=_clean_string(record.get("OWNER1") or record.get("OWN_NAME")),
            owner_address=_compose_owner_address(record),
            absentee=_is_absentee(record),
            property_category=category,
            use_code=use_code_raw,
            property_type=property_type_label,
            style=style_value,
            total_value=_to_number(record.get("TOTAL_VAL")),
            lot_size=_to_number(record.get("LOT_SIZE")),
            zoning=_clean_string(record.get("ZONING")),
            equity_percent=equity_percent,
            units=_to_int(record.get("UNITS")),
            attributes=record,
            estimated_mortgage_balance=estimated_balance,
            estimated_equity_value=equity_value,
            estimated_roi_percent=roi_percent,
            estimated_mortgage_rate_percent=annual_rate,
            estimated_monthly_payment=monthly_payment,
            units_detail=units_detail,
            **market_value_context,
        )

        # Cache the parcel data for future requests
        try:
            cache_dict = _parcel_data_to_dict(parcel_result, record)
            _cache_parcel_data(town_id, target, cache_dict)
            logger.debug(f"Cached parcel {town_id}/{loc_id}")
        except Exception as e:
            # Don't fail the request if caching fails
            logger.warning(f"Failed to cache parcel {town_id}/{loc_id}: {e}")

        return parcel_result

    # If not found in assessment database, try shapefile as fallback
    logger.info(f"Parcel {loc_id} not found in assessment database, trying shapefile fallback")

    try:
        from pathlib import Path
        tax_par_path = _find_taxpar_shapefile(Path(dataset_dir))
        shape_match = _lookup_parcel_record(tax_par_path, loc_id)

        if shape_match:
            shape, shape_attrs = shape_match

            # Build a minimal parcel record from shapefile data
            # Note: Some fields won't be available since they come from the assessment database
            fallback_record = {
                "LOC_ID": shape_attrs.get("LOC_ID", loc_id),
                "SITE_ADDR": shape_attrs.get("SITE_ADDR") or shape_attrs.get("LOC_ADDR"),
                "SITE_CITY": shape_attrs.get("SITE_CITY") or shape_attrs.get("CITY") or town.name,
                "SITE_ZIP": shape_attrs.get("SITE_ZIP") or shape_attrs.get("ZIP"),
                "OWNER1": shape_attrs.get("OWNER1") or shape_attrs.get("OWNER_NAME"),
                "MAIL_ADDR": shape_attrs.get("MAIL_ADDR"),
                "MAIL_CITY": shape_attrs.get("MAIL_CITY"),
                "MAIL_ST": shape_attrs.get("MAIL_ST"),
                "MAIL_ZIP": shape_attrs.get("MAIL_ZIP"),
                "USE_CODE": shape_attrs.get("USE_CODE"),
                "STYLE": shape_attrs.get("STYLE"),
                "TOTAL_VAL": shape_attrs.get("TOTAL_VAL"),
                "LOT_SIZE": shape_attrs.get("LOT_SIZE"),
                "LOT_UNITS": shape_attrs.get("LOT_UNITS"),
                "ZONING": shape_attrs.get("ZONING") or shape_attrs.get("ZONE"),
                "UNITS": shape_attrs.get("UNITS"),
                "YEAR_BUILT": shape_attrs.get("YEAR_BUILT") or shape_attrs.get("YR_BUILT"),
                "LAND_VAL": shape_attrs.get("LAND_VAL"),
                "BLDG_VAL": shape_attrs.get("BLDG_VAL"),
                "LS_PRICE": shape_attrs.get("LS_PRICE"),
                "LS_DATE": shape_attrs.get("LS_DATE"),
                "LS_BOOK": shape_attrs.get("LS_BOOK"),
                "LS_PAGE": shape_attrs.get("LS_PAGE"),
            }

            # Calculate derived values
            category = _classify_use_code(fallback_record.get("USE_CODE"))
            equity_percent, estimated_balance, equity_value, roi_percent, annual_rate, monthly_payment = calculate_equity_metrics(fallback_record)

            use_code_raw = _clean_string(fallback_record.get("USE_CODE"))
            use_code_key = (use_code_raw or "").upper()
            property_type_label = use_code_lookup.get(use_code_key) or use_code_lookup.get(
                use_code_key.lstrip("0"), use_code_raw
            )

            market_value_context = _build_market_value_context(town_id, target, fallback_record)

            parcel_result = ParcelSearchResult(
                town=town,
                loc_id=_clean_string(fallback_record.get("LOC_ID")) or target,
                site_address=_clean_string(fallback_record.get("SITE_ADDR")) or "",
                site_city=_clean_string(fallback_record.get("SITE_CITY")),
                site_zip=_clean_zip(fallback_record.get("SITE_ZIP")),
                owner_name=_clean_string(fallback_record.get("OWNER1")),
                owner_address=_compose_owner_address(fallback_record),
                absentee=_is_absentee(fallback_record),
                property_category=category,
                use_code=use_code_raw,
                property_type=property_type_label,
                style=_clean_string(fallback_record.get("STYLE")),
                total_value=_to_number(fallback_record.get("TOTAL_VAL")),
                lot_size=_to_number(fallback_record.get("LOT_SIZE")),
                zoning=_clean_string(fallback_record.get("ZONING")),
                equity_percent=equity_percent,
                units=_to_int(fallback_record.get("UNITS")),
                attributes=fallback_record,
                estimated_mortgage_balance=estimated_balance,
                estimated_equity_value=equity_value,
                estimated_roi_percent=roi_percent,
                estimated_mortgage_rate_percent=annual_rate,
                estimated_monthly_payment=monthly_payment,
                units_detail=None,  # Not available from shapefile alone
                **market_value_context,
            )

            # Cache the fallback parcel data
            try:
                cache_dict = _parcel_data_to_dict(parcel_result, fallback_record)
                _cache_parcel_data(town_id, target, cache_dict)
                logger.debug(f"Cached fallback parcel {town_id}/{loc_id}")
            except Exception as e:
                logger.warning(f"Failed to cache fallback parcel {town_id}/{loc_id}: {e}")

            logger.info(f"Successfully loaded parcel {loc_id} from shapefile fallback")
            return parcel_result

    except Exception as e:
        logger.warning(f"Shapefile fallback failed for {loc_id}: {e}")

    raise MassGISDataError(f"Parcel {loc_id} was not found for {town.name}.")


def load_massgis_parcels_by_ids(town_id: int, loc_ids: Iterable[str], *, saved_list=None) -> List[ParcelSearchResult]:
    """
    Load multiple parcels by ID with cross-user caching (90 days).
    """
    from .models import AttomData, MassGISParcelCache
    from django.utils import timezone

    normalized_targets = {_normalize_loc_id(value): value for value in loc_ids if value}
    if not normalized_targets:
        return []

    matches: List[ParcelSearchResult] = []
    cache_misses = set()

    # Step 1: Try to load all parcels from cache
    for normalized_loc_id in normalized_targets.keys():
        cached_data = _get_cached_parcel_data(town_id, normalized_loc_id)
        if cached_data:
            try:
                parcel = _dict_to_parcel_data(cached_data)
                matches.append(parcel)
            except Exception as e:
                logger.warning(f"Failed to deserialize cached parcel {town_id}/{normalized_loc_id}: {e}")
                cache_misses.add(normalized_loc_id)
        else:
            cache_misses.add(normalized_loc_id)

    # Log cache performance
    cache_hits = len(matches)
    total_requested = len(normalized_targets)
    if cache_hits > 0:
        logger.info(f"Cache HIT: {cache_hits}/{total_requested} parcels loaded from cache")
    if cache_misses:
        logger.info(f"Cache MISS: {len(cache_misses)}/{total_requested} parcels need loading from source")

    # Step 2: Load cache misses from CSV/DBF
    if cache_misses:
        town = _get_massgis_town(town_id)
        dataset_dir = _ensure_massgis_dataset(town)
        use_code_lookup = _load_use_code_lut(str(dataset_dir))

        best_records: Dict[str, Dict[str, object]] = {}
        unit_records_map: Dict[str, List[Dict[str, object]]] = defaultdict(list)

        # Only load records for cache misses
        for record in _load_assess_records(str(dataset_dir)):
            record_loc = record.get("LOC_ID")
            key = _normalize_loc_id(record_loc)
            if key in cache_misses:
                unit_records_map[key].append(record)
                existing = best_records.get(key)
                if existing is None or _should_replace_assess_record(record, existing):
                    best_records[key] = record

        # Step 3: Convert loaded records to ParcelSearchResult and cache them
        for key, record in best_records.items():
            category = _classify_use_code(record.get("USE_CODE"))
            (
                equity_percent,
                estimated_balance,
                equity_value,
                roi_percent,
                annual_rate,
                monthly_payment,
            ) = calculate_equity_metrics(record)
            style_value = _clean_string(record.get("STYLE"))
            use_code_raw = _clean_string(record.get("USE_CODE"))
            use_code_key = (use_code_raw or "").upper()
            property_type_label = use_code_lookup.get(use_code_key) or use_code_lookup.get(
                use_code_key.lstrip("0"), use_code_raw
            )

            market_value_context = _build_market_value_context(town_id, key, record)

            parcel_result = ParcelSearchResult(
                town=town,
                loc_id=_clean_string(record.get("LOC_ID")) or normalized_targets[key],
                site_address=_clean_string(record.get("SITE_ADDR")) or "",
                site_city=_clean_string(record.get("SITE_CITY")) or _clean_string(record.get("CITY")),
                site_zip=_clean_zip(record.get("SITE_ZIP")) or _clean_zip(record.get("ZIP")),
                owner_name=_clean_string(record.get("OWNER1") or record.get("OWN_NAME")),
                owner_address=_compose_owner_address(record),
                absentee=_is_absentee(record),
                property_category=category,
                use_code=use_code_raw,
                property_type=property_type_label,
                style=style_value,
                total_value=_to_number(record.get("TOTAL_VAL")),
                lot_size=_to_number(record.get("LOT_SIZE")),
                zoning=_clean_string(record.get("ZONING")),
                equity_percent=equity_percent,
                units=_to_int(record.get("UNITS")),
                attributes=record,
                estimated_mortgage_balance=estimated_balance,
                estimated_equity_value=equity_value,
                estimated_roi_percent=roi_percent,
                estimated_mortgage_rate_percent=annual_rate,
                estimated_monthly_payment=monthly_payment,
                units_detail=_summarize_unit_records(unit_records_map.get(key)),
                **market_value_context,
            )

            matches.append(parcel_result)

            # Cache the newly loaded parcel
            try:
                cache_dict = _parcel_data_to_dict(parcel_result, record)
                _cache_parcel_data(town_id, key, cache_dict)
            except Exception as e:
                logger.warning(f"Failed to cache parcel {town_id}/{key}: {e}")

    if matches and saved_list:
        match_loc_ids = {match.loc_id for match in matches if match.loc_id}
        if match_loc_ids:
            attom_data_map = {
                attom_data.loc_id: attom_data
                for attom_data in AttomData.objects.filter(saved_list=saved_list, loc_id__in=match_loc_ids)
            }
            for match in matches:
                if match.loc_id in attom_data_map:
                    match.attom_data = attom_data_map[match.loc_id]

    matches.sort(key=lambda item: (item.site_address or "", item.loc_id))
    return matches


def get_massgis_parcel_shape(town: MassGISTown, loc_id: str) -> ParcelShapeResult:
    dataset_dir = _ensure_massgis_dataset(town)
    try:
        tax_par_path = _find_taxpar_shapefile(Path(dataset_dir))
    except MassGISDataError as exc:
        return ParcelShapeResult(
            found=False,
            svg_markup=None,
            attribute_rows=[],
            centroid=None,
            area=None,
            width=None,
            height=None,
            source_hint=None,
            message=str(exc),
        )

    match = _lookup_parcel_record(tax_par_path, loc_id)
    if match is None:
        return ParcelShapeResult(
            found=False,
            svg_markup=None,
            attribute_rows=[],
            centroid=None,
            area=None,
            width=None,
            height=None,
            source_hint=tax_par_path.name,
            message="Parcel geometry not available in MassGIS dataset.",
        )

    shape, attributes = match
    neighbors = _collect_surrounding_shapes(tax_par_path, shape)
    svg_markup = _shape_to_svg(shape, neighbors=neighbors)
    centroid = _shape_centroid(shape)
    area = _shape_area(shape)
    width, height = _shape_dimensions(shape)

    return ParcelShapeResult(
        found=True,
        svg_markup=svg_markup,
        attribute_rows=_format_attribute_rows(attributes),
        centroid=centroid,
        area=area,
        width=width,
        height=height,
        source_hint=tax_par_path.name,
        message=None,
    )





def _http_get_json(url: str, *, params: Optional[dict], timeout: int) -> dict:
    query_string = parse.urlencode(params or {}, doseq=True)
    full_url = f"{url}?{query_string}" if query_string else url
    http_request = request.Request(full_url, headers={"User-Agent": USER_AGENT})

    try:
        with request.urlopen(http_request, timeout=timeout) as response:
            data = response.read()
    except error.HTTPError as exc:
        logger.error("HTTP error %s fetching %s: %s", exc.code, full_url, exc.reason)
        raise
    except error.URLError as exc:
        logger.error("Network error fetching %s: %s", full_url, exc.reason)
        raise

    try:
        return json.loads(data.decode("utf-8"))
    except json.JSONDecodeError as exc:  # noqa: F841
        logger.error("Unable to decode JSON payload from %s", full_url)
        raise


def _build_massgis_catalog_from_sources(
    *,
    fallback: Optional[Dict[int, MassGISTown]] = None,
) -> Dict[int, MassGISTown]:
    directory_catalog: Dict[int, MassGISTown] = {}
    directory_error: Optional[Exception] = None
    try:
        directory_catalog = _fetch_massgis_catalog_from_directory()
    except MassGISDataError as exc:
        directory_error = exc
        logger.warning("MassGIS directory fetch failed: %s", exc)
    except Exception as exc:  # noqa: BLE001
        directory_error = exc
        logger.exception("Unexpected error while fetching MassGIS directory listing: %s", exc)

    excel_catalog = _load_catalog_from_excel()
    if excel_catalog and directory_error:
        logger.info("Loaded MassGIS catalog data from local Excel fallback.")

    local_catalog = _load_catalog_from_local_datasets()
    if local_catalog and directory_error and not excel_catalog:
        logger.info("Loaded MassGIS catalog data from locally cached datasets.")

    static_catalog = _load_static_catalog()
    if static_catalog and directory_error and not (excel_catalog or local_catalog):
        logger.info("Loaded MassGIS catalog data from bundled fallback.")

    combined = {}
    if directory_catalog:
        combined = directory_catalog
    if excel_catalog:
        combined = _merge_catalog_entries(combined, excel_catalog)
    if local_catalog:
        combined = _merge_catalog_entries(combined, local_catalog)
    if static_catalog:
        combined = _merge_catalog_entries(combined, static_catalog)

    combined = _apply_boston_catalog_override(combined)

    if combined:
        _save_catalog_cache(combined)
        return combined

    fallback_catalog = (
        fallback
        or _load_cached_catalog(enforce_ttl=False)
        or _load_static_catalog()
    )
    if fallback_catalog:
        logger.info("Using cached MassGIS catalog due to missing active sources.")
        return fallback_catalog

    raise MassGISDataError("Unable to load MassGIS catalog from any available source.")


def _schedule_catalog_refresh() -> None:
    global _catalog_refresh_pending
    with _catalog_refresh_lock:
        if _catalog_refresh_pending:
            return
        _catalog_refresh_pending = True

    def _worker() -> None:
        try:
            fallback = _load_cached_catalog(enforce_ttl=False)
            catalog = _build_massgis_catalog_from_sources(fallback=fallback)
            if catalog:
                _load_massgis_catalog.cache_clear()
                _load_massgis_catalog()
        except Exception as exc:  # noqa: BLE001
            logger.debug("MassGIS catalog refresh failed: %s", exc)
        finally:
            with _catalog_refresh_lock:
                _catalog_refresh_pending = False

    threading.Thread(target=_worker, name="MassGISCatalogRefresh", daemon=True).start()


@lru_cache(maxsize=1)
def _load_massgis_catalog() -> Dict[int, MassGISTown]:
    payload = _load_catalog_payload()
    cached_catalog: Optional[Dict[int, MassGISTown]] = None
    if payload:
        entries = payload.get("entries")
        if isinstance(entries, dict):
            parsed = _parse_catalog_entries(entries)
            if parsed:
                cached_catalog = parsed

    if cached_catalog:
        if _catalog_payload_is_stale(payload):
            _schedule_catalog_refresh()
        return cached_catalog

    catalog = _build_massgis_catalog_from_sources()
    return catalog


def _get_massgis_town(town_id: int) -> MassGISTown:
    try:
        normalized_id = int(town_id)
    except (TypeError, ValueError) as exc:
        raise MassGISDataError(f"No MassGIS entry found for town id {town_id}.") from exc

    catalog = _load_massgis_catalog()
    town = catalog.get(normalized_id)
    if town is not None:
        return town

    logger.info("MassGIS catalog missing town %s – forcing refresh.", normalized_id)
    try:
        refreshed_catalog = _build_massgis_catalog_from_sources(fallback=catalog)
    except MassGISDataError as exc:
        raise MassGISDataError(f"No MassGIS entry found for town id {town_id}.") from exc

    _load_massgis_catalog.cache_clear()
    catalog = refreshed_catalog
    town = catalog.get(normalized_id)
    if town is not None:
        return town

    raise MassGISDataError(f"No MassGIS entry found for town id {town_id}.")


def get_massgis_catalog() -> Dict[int, MassGISTown]:
    return _load_massgis_catalog()


def preload_massgis_dataset(town_id: int) -> None:
    town = _get_massgis_town(town_id)
    _ensure_massgis_dataset(town)


def refresh_massgis_dataset(
    town: MassGISTown,
    *,
    force: bool = False,
    stale_after_days: Optional[int] = 30,
    use_remote_headers: bool = True,
) -> Tuple[bool, str]:
    slug = town.dataset_slug.upper()
    dataset_dir = GISDATA_ROOT / slug
    index = _load_dataset_index()
    entry = index.get(slug)
    now = datetime.now(timezone.utc)

    last_downloaded = _parse_iso_datetime(entry.get("downloaded_at") if entry else None)
    last_modified_cached = _parse_iso_datetime(entry.get("last_modified") if entry else None)

    remote_last_modified = None
    if use_remote_headers:
        remote_last_modified = _fetch_remote_last_modified(town.shapefile_url)

    needs_refresh = force or not dataset_dir.exists()
    reason = "forced" if force else "missing dataset"

    if not needs_refresh and stale_after_days is not None and last_downloaded is not None:
        if now - last_downloaded >= timedelta(days=stale_after_days):
            needs_refresh = True
            reason = f"older than {stale_after_days} days"

    if not needs_refresh and remote_last_modified is not None and last_modified_cached is not None:
        # Refresh if remote data is newer than what we have cached.
        if remote_last_modified > last_modified_cached:
            needs_refresh = True
            reason = "remote dataset updated"

    if not needs_refresh and remote_last_modified is not None and last_modified_cached is None:
        # We have no record of remote version but the server gave us one – refresh once.
        needs_refresh = True
        reason = "recording remote metadata"

    if not needs_refresh:
        _update_dataset_index_entry(slug, last_checked=now)
        return False, "up-to-date"

    _delete_local_dataset(slug)
    path = _ensure_massgis_dataset(town, last_modified=remote_last_modified)
    _update_dataset_index_entry(slug, last_checked=now)
    return True, reason


def _resolve_dataset_directory(root: Path) -> Path:
    """Return the directory that actually contains the assessment DBF."""
    assess_files = list(root.glob("*Assess*.dbf"))
    if assess_files:
        return root

    for child in sorted(root.iterdir()):
        if child.is_dir():
            assess_files = list(child.glob("*Assess*.dbf"))
            if assess_files:
                return child

    return root


def _find_existing_dataset_dir(slug: str) -> Optional[Path]:
    if not GISDATA_ROOT.exists():
        return None
    target = re.sub(r"[\s_]+", "_", slug).lower()
    for candidate in GISDATA_ROOT.iterdir():
        if candidate.is_dir() and candidate.name.lower() == target:
            return candidate
        candidate_normalized = re.sub(r"[\s_]+", "_", candidate.name).lower()
        if candidate_normalized == target:
            return candidate
    return None


def _ensure_massgis_dataset(town: MassGISTown, last_modified: Optional[datetime] = None) -> Path:
    # Keep downloads extracted to a subdirectory matching the ZIP slug to avoid
    # collisions while still allowing case-insensitive access. MassGIS slugs are
    # already uppercase, but we normalise as a safeguard for future changes.
    slug = town.dataset_slug.upper()
    shapefile_url = town.shapefile_url
    boston_dataset_id: Optional[str] = None

    if town.town_id == BOSTON_TOWN_ID:
        if shapefile_url and shapefile_url.upper().endswith("BOSTON_TAXPAR.ZIP"):
            try:
                resolved = _resolve_boston_shapefile_url()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Unable to resolve Boston dataset URL during download: %s", exc)
                resolved = None
            if resolved:
                logger.info("Overriding Boston dataset download url with %s", resolved)
                shapefile_url = resolved

        if shapefile_url and "opendata.arcgis.com" in shapefile_url.lower():
            boston_dataset_id = _extract_boston_dataset_identifier(shapefile_url)
        else:
            boston_dataset_id = None

    base_dir = GISDATA_ROOT / slug
    logger.info("Looking for MassGIS dataset %s at: %s", slug, base_dir)
    if not base_dir.exists():
        existing = _find_existing_dataset_dir(slug)
        if existing is not None:
            logger.info("Found existing dataset dir: %s", existing)
            base_dir = existing
        else:
            logger.info("No existing dataset dir found for %s", slug)

    now = datetime.now(timezone.utc)
    index = _load_dataset_index()
    entry = index.get(slug)

    if base_dir.exists():
        logger.info("Dataset dir exists: %s (entry: %s)", base_dir, entry)
        downloaded_at = _parse_iso_datetime(entry.get("downloaded_at")) if entry else None
        is_stale = True
        if downloaded_at is not None:
            is_stale = now - downloaded_at >= MASSGIS_DATASET_TTL

        if is_stale:
            logger.info(
                "Cached MassGIS dataset %s is older than %s; removing prior to refresh.",
                slug,
                MASSGIS_DATASET_TTL,
            )
            _delete_local_dataset(slug)
            base_dir = GISDATA_ROOT / slug
            entry = None
        else:
            updates = {
                "source_url": shapefile_url,
                "last_checked": now,
            }
            if last_modified is not None:
                updates["last_modified"] = last_modified
            _update_dataset_index_entry(slug, **updates)
            return _resolve_dataset_directory(base_dir)

    zip_path = MASSGIS_DOWNLOAD_DIR / f"{slug}.zip"

    # Try to get from S3 cache first
    s3_last_modified = _check_s3_dataset_exists(slug)
    if s3_last_modified and not _is_s3_dataset_stale(s3_last_modified):
        logger.info("Found %s in S3 cache (age: %s days)", slug, (now - s3_last_modified).days)
        if not zip_path.exists():
            if _download_from_s3(slug, zip_path):
                logger.info("Successfully retrieved %s from S3 cache", slug)
            else:
                logger.warning("Failed to download from S3, will fetch from source")
    elif s3_last_modified:
        logger.info("S3 cache for %s is stale (age: %s days), will refresh", slug, (now - s3_last_modified).days)

    # Validate existing zip file or download new one
    if zip_path.exists():
        try:
            # Test if zip file is valid
            with zipfile.ZipFile(zip_path, "r") as archive:
                # Test the zip file integrity
                if archive.testzip() is not None:
                    logger.warning("Corrupted zip file detected: %s - deleting and re-downloading", zip_path)
                    zip_path.unlink()
        except zipfile.BadZipFile:
            logger.warning("Invalid zip file detected: %s - deleting and re-downloading", zip_path)
            zip_path.unlink()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Error validating zip file %s: %s - deleting and re-downloading", zip_path, exc)
            zip_path.unlink()

    if not zip_path.exists():
        try:
            if boston_dataset_id and shapefile_url and "opendata.arcgis.com" in shapefile_url:
                logger.info("Starting Boston dataset download via Open Data API (%s).", boston_dataset_id)
                _download_boston_dataset(boston_dataset_id, zip_path)
            else:
                _download_file(shapefile_url, zip_path)

            # Upload to S3 cache after successful download
            if zip_path.exists():
                _upload_to_s3(slug, zip_path)
        except Exception as exc:  # noqa: BLE001
            raise MassGISDownloadError(
                f"Unable to download MassGIS shapefile for {town.name}."
            ) from exc

    # Verify zip file exists and is valid before extraction
    if not zip_path.exists():
        raise MassGISDownloadError(
            f"Zip file does not exist after download: {zip_path}"
        )

    # Extract with error handling
    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            # Validate zip integrity before extraction
            if archive.testzip() is not None:
                raise zipfile.BadZipFile("Zip file integrity check failed")
            archive.extractall(base_dir)
    except zipfile.BadZipFile as exc:
        # Clean up corrupted files
        logger.error("Failed to extract %s - removing corrupted files", zip_path)
        if zip_path.exists():
            zip_path.unlink()
        if base_dir.exists():
            import shutil
            shutil.rmtree(base_dir)
        raise MassGISDownloadError(
            f"Corrupted zip file for {town.name}. Please retry."
        ) from exc

    dataset_dir = _resolve_dataset_directory(base_dir)
    _record_dataset_download(slug, shapefile_url, last_modified)
    return dataset_dir


def _download_file(url: str, path: Path, *, timeout: int = 30) -> None:
    logger.info("Downloading %s to %s", url, path)
    path.parent.mkdir(parents=True, exist_ok=True)

    parsed = urlparse(url)
    if parsed.scheme == "file":
        local_path = Path(request.url2pathname(parsed.path))
        if not local_path.exists():
            raise MassGISDownloadError(f"Local file not found at {local_path}")
        shutil.copyfile(local_path, path)
        logger.info("Copied local dataset from %s (%d bytes)", local_path, path.stat().st_size)
        return

    req = request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with request.urlopen(req, timeout=timeout) as response:
            if response.status != 200:
                raise error.HTTPError(url, response.status, f"HTTP {response.status}", response.headers, None)
            path.write_bytes(response.read())
            logger.info("Successfully downloaded %s (%d bytes)", url, path.stat().st_size)
    except error.HTTPError as exc:
        logger.error("HTTP error %s downloading %s: %s", exc.code, url, exc.reason)
        raise MassGISDownloadError(f"HTTP error downloading {url}: {exc.reason}") from exc
    except error.URLError as exc:
        logger.error("Network error downloading %s: %s", url, exc.reason)
        raise MassGISDownloadError(f"Network error downloading {url}: {exc.reason}") from exc
    except TimeoutError as exc:
        logger.error("Timeout downloading %s: %s", url, exc)
        raise MassGISDownloadError(f"Timeout downloading {url}") from exc
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error downloading %s: %s", url, exc)
        raise MassGISDownloadError(f"Unexpected error downloading {url}") from exc


def _find_taxpar_shapefile(dataset_dir: Path) -> Path:
    candidates = sorted(dataset_dir.glob("*TaxPar*.shp"))
    if not candidates:
        raise MassGISDataError(
            f"No tax parcel shapefile found in {dataset_dir}. "
            "Download may be incomplete."
        )
    return candidates[0]


def _find_assess_dbf(dataset_dir: Path) -> Path:
    # For Boston, the TaxPar DBF contains the assessment data
    if dataset_dir.name.upper() == "BOSTON_TAXPAR":
        taxpar_candidates = sorted(dataset_dir.glob("*TaxPar*.dbf"))
        if taxpar_candidates:
            return taxpar_candidates[0]

    candidates = sorted(dataset_dir.glob("*Assess*.dbf"))
    if not candidates:
        raise MassGISDataError(
            f"No assessment DBF file found in {dataset_dir}. "
            "Ensure the MassGIS download was extracted correctly."
        )
    return candidates[0]


def _load_assess_records_impl(directory: Path) -> List[Dict[str, object]]:
    if directory.name.upper() == "BOSTON_TAXPAR":
        records = _load_boston_assess_records(directory)
        return records or []

    if shapefile is None:
        raise MassGISDataError(
            "The 'pyshp' package is required to load MassGIS assessment tables."
        )

    assess_dbf = _find_assess_dbf(directory)
    reader = shapefile.Reader(shp=None, shx=None, dbf=str(assess_dbf))
    field_names = [field[0] for field in reader.fields[1:]]

    records: List[Dict[str, object]] = []
    try:
        for raw_record in reader.iterRecords():
            record = {field_names[index]: raw_record[index] for index in range(len(field_names))}
            records.append(record)
    finally:
        reader.close()

    return records


@lru_cache(maxsize=32)
def _load_assess_records_cached(dataset_dir: str) -> List[Dict[str, object]]:
    return _load_assess_records_impl(Path(dataset_dir))


def _load_assess_records(dataset_dir: str) -> List[Dict[str, object]]:
    directory = Path(dataset_dir)
    if directory.name.upper() == "BOSTON_TAXPAR":
        return _load_assess_records_impl(directory)
    return _load_assess_records_cached(str(directory))


def _load_boston_assess_records(dataset_dir: Path) -> Optional[List[Dict[str, object]]]:
    stream = _download_boston_assessment_csv_from_s3()
    if stream is not None:
        try:
            records = _parse_boston_assessment_records(csv.DictReader(stream))
            logger.info(
                "Loaded %s Boston assessment records from S3 key %s",
                len(records),
                BOSTON_ASSESSMENT_S3_KEY,
            )
            return records
        except Exception as exc:  # noqa: BLE001
            logger.warning("Unable to parse Boston assessment CSV downloaded from S3: %s", exc)

    csv_candidates = [
        dataset_dir / "BOSTONAssess_FY25.csv",
        dataset_dir / "BOSTONAssess.csv",
        GISDATA_ROOT / "downloads" / "fy2025-property-assessment-data_12_30_2024.csv",
    ]
    csv_path = next((path for path in csv_candidates if path.exists()), None)
    if csv_path is None:
        logger.info("Boston assessment CSV not found in %s", dataset_dir)
        return None

    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            records = _parse_boston_assessment_records(csv.DictReader(handle))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to load Boston assessment CSV at %s: %s", csv_path, exc)
        return None

    logger.info("Loaded %s Boston assessment records from %s", len(records), csv_path.name)
    return records


def _parse_boston_assessment_records(reader: csv.DictReader) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    for row in reader:
        if not row:
            continue
        pid = _clean_string(
            row.get("MAP_PAR_ID")
            or row.get("PID")
            or row.get("GIS_ID")
        )
        if not pid:
            continue
        loc_id = _clean_string(
            row.get("LOC_ID")
            or row.get("GIS_ID")
            or row.get("CM_ID")
            or pid
        )
        site_addr = _compose_boston_site_address(row) or _clean_string(row.get("ADDR")) or None
        mail_street = _clean_string(row.get("MAIL_STREET_ADDRESS"))

        record = {
            "MAP_PAR_ID": pid,
            "PID": pid,
            "GIS_ID": _clean_string(row.get("GIS_ID")) or pid,
            "LOC_ID": loc_id,
            "UNIT_NUM": _clean_string(row.get("UNIT_NUM")),
            "SITE_ADDR": site_addr,
            "LOC_ADDR": site_addr or _clean_string(row.get("ST_NAME")),
            "SITE_CITY": _clean_string(row.get("CITY")) or "BOSTON",
            "SITE_ZIP": _clean_zip(row.get("ZIP_CODE")),
            "CITY": _clean_string(row.get("CITY")) or "BOSTON",
            "ZIP": _clean_zip(row.get("ZIP_CODE")),
            "OWNER1": _clean_string(row.get("OWNER")) or _clean_string(row.get("MAIL_ADDRESSEE")),
            "OWNER": _clean_string(row.get("OWNER")),
            "OWNER_NAME": _clean_string(row.get("OWNER")),
            "MAIL_ADDRESSEE": _clean_string(row.get("MAIL_ADDRESSEE")),
            "MAIL_ADDRESS": mail_street,
            "MAIL_CITY": _clean_string(row.get("MAIL_CITY")),
            "MAIL_STATE": _clean_string(row.get("MAIL_STATE")),
            "MAIL_ZIP": _clean_zip(row.get("MAIL_ZIP_CODE")),
            "OWN_ADDR": mail_street,
            "OWN_CITY": _clean_string(row.get("MAIL_CITY")),
            "OWN_STATE": _clean_string(row.get("MAIL_STATE")),
            "OWN_ZIP": _clean_zip(row.get("MAIL_ZIP_CODE")),
            "USE_CODE": _clean_string(row.get("LUC") or row.get("LU")),
            "USE_DESC": _clean_string(row.get("LU_DESC")) or _clean_string(row.get("LU")),
            "TOTAL_VAL": _parse_float_value(row.get("TOTAL_VALUE")),
            "TOTAL_VALUE": _parse_float_value(row.get("TOTAL_VALUE")),
            "LAND_VAL": _parse_float_value(row.get("LAND_VALUE")),
            "LAND_VALUE": _parse_float_value(row.get("LAND_VALUE")),
            "BLDG_VAL": _parse_float_value(row.get("BLDG_VALUE")),
            "BLDG_VALUE": _parse_float_value(row.get("BLDG_VALUE")),
            "LOT_SIZE": _parse_float_value(row.get("LAND_SF")),
            "LAND_SF": _parse_float_value(row.get("LAND_SF")),
            "LOT_UNITS": "sqft",
            "UNITS": _clean_string(row.get("RES_UNITS")) or _clean_string(row.get("NUM_BLDGS")),
            "YEAR_BUILT": _clean_string(row.get("YR_BUILT")),
            "YR_REMODEL": _clean_string(row.get("YR_REMODEL")),
            "STYLE": _clean_string(row.get("BLDG_TYPE")),
            "LUC": _clean_string(row.get("LUC")),
            "LU": _clean_string(row.get("LU")),
        }
        records.append(record)

    return records


def _assess_record_priority(record: Mapping[str, object]) -> Tuple[float, int]:
    total_val = _parse_float_value(record.get("TOTAL_VAL")) or 0.0
    use_code = (_clean_string(record.get("LUC")) or _clean_string(record.get("USE_CODE")) or "").upper()
    is_condo_main = use_code in {"995", "CM", "CONDMAIN", "CONDO MAIN"}
    return (total_val, 0 if not is_condo_main else -1)


def _should_replace_assess_record(candidate: Mapping[str, object], existing: Mapping[str, object]) -> bool:
    return _assess_record_priority(candidate) > _assess_record_priority(existing)


def _summarize_unit_records(records: Iterable[Mapping[str, object]]) -> List[Dict[str, object]]:
    summary: List[Dict[str, object]] = []
    seen_ids: set[str] = set()
    for index, record in enumerate(records or []):
        unit_number = _clean_string(record.get("UNIT_NUM"))
        owner = _clean_string(record.get("OWNER")) or _clean_string(record.get("OWNER1")) or _clean_string(record.get("MAIL_ADDRESSEE"))
        mailing_address = _compose_owner_address(record)
        total_value = _parse_float_value(record.get("TOTAL_VAL") or record.get("TOTAL_VALUE"))
        land_value = _parse_float_value(record.get("LAND_VAL") or record.get("LAND_VALUE"))
        building_value = _parse_float_value(record.get("BLDG_VAL") or record.get("BLDG_VALUE"))
        lot_size = _parse_float_value(record.get("LAND_SF") or record.get("LOT_SIZE"))
        building_area = _parse_float_value(record.get("BLD_AREA") or record.get("LIVING_AREA") or record.get("LIV_AREA"))
        use_code = (_clean_string(record.get("LUC")) or _clean_string(record.get("USE_CODE")) or "").upper()
        is_master = (
            use_code in {"995", "CM", "CONDMAIN", "CONDO MAIN"}
            or (not unit_number and (total_value or 0) == 0)
        )

        (
            equity_percent,
            estimated_balance,
            estimated_equity_value,
            roi_percent,
            _annual_rate,
            monthly_payment,
        ) = calculate_equity_metrics(record)

        def _safe_float(value: Optional[object]) -> Optional[float]:
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        equity_percent = _safe_float(equity_percent)
        estimated_balance = _safe_float(estimated_balance)
        estimated_equity_value = _safe_float(estimated_equity_value)
        roi_percent = _safe_float(roi_percent)
        monthly_payment = _safe_float(monthly_payment)

        sale_date = _clean_string(record.get("LS_DATE") or record.get("SALE_DATE"))
        sale_price = _parse_float_value(record.get("LS_PRICE") or record.get("SALE_PRICE"))
        book = _clean_string(record.get("BOOK") or record.get("DEED_BOOK"))
        page = _clean_string(record.get("PAGE") or record.get("DEED_PAGE"))

        def _format_currency_display(value: Optional[float]) -> Optional[str]:
            if value is None:
                return None
            return f"${value:,.0f}"

        valuation_items = [
            ("Total Value", _format_currency_display(total_value)),
            ("Land Value", _format_currency_display(land_value)),
            ("Building Value", _format_currency_display(building_value)),
            ("Building Area (sqft)", f"{building_area:,.0f}" if building_area else None),
            ("Est. Mortgage Balance", _format_currency_display(estimated_balance)),
            ("Est. Equity", _format_currency_display(estimated_equity_value)),
            (
                "Equity %",
                f"{equity_percent:.1f}%"
                if equity_percent is not None
                else None,
            ),
            (
                "Est. ROI %",
                f"{roi_percent:.1f}%"
                if roi_percent is not None
                else None,
            ),
            (
                "Est. Monthly Payment",
                _format_currency_display(monthly_payment),
            ),
        ]

        book_page = None
        if book and page:
            book_page = f"{book} / {page}"
        elif book:
            book_page = book
        elif page:
            book_page = page

        sale_history_items = [
            ("Sale Date", sale_date),
            ("Sale Price", _format_currency_display(sale_price)),
            ("Book / Page", book_page),
        ]
        base_record_key = (
            _clean_string(record.get("PAR_ID"))
            or _clean_string(record.get("PROP_ID"))
            or _clean_string(record.get("LOC_ID"))
            or unit_number
            or owner
        )
        if not base_record_key:
            base_record_key = f"unit-{index + 1}"

        unique_key = base_record_key
        suffix = 2
        while unique_key in seen_ids:
            unique_key = f"{base_record_key}#{suffix}"
            suffix += 1
        seen_ids.add(unique_key)

        summary.append(
            {
                "id": unique_key,
                "row_key": unique_key,
                "source_id": base_record_key,
                "unit_number": unit_number,
                "owner": owner,
                "mailing_address": mailing_address,
                "mailing_street": _clean_string(record.get("OWN_ADDR")),
                "mailing_city": _clean_string(record.get("OWN_CITY")),
                "mailing_state": _clean_string(record.get("OWN_STATE")),
                "mailing_zip": _clean_zip(record.get("OWN_ZIP")),
                "site_address": _clean_string(record.get("SITE_ADDR")),
                "loc_id": _clean_string(record.get("LOC_ID")),
                "total_value": total_value,
                "land_value": land_value,
                "building_value": building_value,
                "value_display": f"${total_value:,.0f}" if total_value else None,
                "land_value_display": f"${land_value:,.0f}" if land_value else None,
                "building_value_display": f"${building_value:,.0f}" if building_value else None,
                "is_master_record": is_master,
                "valuation": {label: value for label, value in valuation_items if value},
                "valuation_items": valuation_items,
                "sale_history": {label: value for label, value in sale_history_items if value},
                "sale_history_items": sale_history_items,
                "lot_size": lot_size,
                "building_area": building_area,
                "estimated_mortgage_balance": estimated_balance,
                "estimated_equity_value": estimated_equity_value,
                "estimated_equity_percent": equity_percent,
                "estimated_roi_percent": roi_percent,
                "estimated_monthly_payment": monthly_payment,
                "sale_date": sale_date,
                "sale_price": sale_price,
                "sale_book": book,
                "sale_page": page,
                "owner_overrides": {
                    "Unit": unit_number,
                    "Owner": owner,
                    "Mailing Address": mailing_address,
                    "Site Address": _clean_string(record.get("SITE_ADDR")),
                    "Total Value": _format_currency_display(total_value),
                    "Land Value": _format_currency_display(land_value),
                    "Building Value": _format_currency_display(building_value),
                },
            }
        )

    summary.sort(
        key=lambda item: (
            1 if item.get("is_master_record") else 0,
            item.get("unit_number") or "",
            item.get("owner") or "",
        )
    )
    return summary


def _find_usecode_lut_dbf(dataset_dir: Path) -> Optional[Path]:
    """Find the USE_CODE lookup table DBF file (e.g., M007UC_LUT_CY25_FY26.dbf)"""
    candidates = sorted(dataset_dir.glob("*UC_LUT*.dbf"))
    if not candidates:
        return None
    return candidates[0]


@lru_cache(maxsize=32)
def _load_usecode_lookup(dataset_dir: str) -> Dict[str, str]:
    """
    Load USE_CODE lookup table and return a dict mapping USE_CODE -> USE_DESC.
    Returns empty dict if no lookup table file is found.
    """
    if shapefile is None:
        return {}

    directory = Path(dataset_dir)
    usecode_dbf = _find_usecode_lut_dbf(directory)
    if not usecode_dbf:
        logger.info(f"No USE_CODE lookup table found in {dataset_dir}")
        return {}

    reader = shapefile.Reader(shp=None, shx=None, dbf=str(usecode_dbf))
    field_names = [field[0] for field in reader.fields[1:]]

    usecode_map: Dict[str, str] = {}
    try:
        for raw_record in reader.iterRecords():
            record = {field_names[index]: raw_record[index] for index in range(len(field_names))}
            use_code = str(record.get('USE_CODE', '')).strip()
            use_desc = str(record.get('USE_DESC', '')).strip()
            if use_code and use_desc:
                usecode_map[use_code] = use_desc
    finally:
        reader.close()

    logger.info(f"Loaded {len(usecode_map)} USE_CODE descriptions from {usecode_dbf.name}")
    return usecode_map


def _compose_boston_site_address(source: Mapping[str, object]) -> Optional[str]:
    parts = [
        _clean_string(source.get("ST_NUM")),
        _clean_string(source.get("ST_NUM2")),
        _clean_string(source.get("ST_NAME")),
        _clean_string(source.get("UNIT_NUM")),
    ]
    filtered = [part for part in parts if part]
    return " ".join(filtered) if filtered else None


def _parse_float_value(value: Optional[object]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    normalized = re.sub(r"[^\d\.\-]", "", text)
    if not normalized:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def _classify_use_code(use_code: Optional[object]) -> str:
    """
    Classify USE_CODE into major property categories.
    Returns: Residential, Commercial, Industrial, Exempt, Agricultural, Forest, Mixed, or Other
    """
    code = _clean_string(use_code)
    if not code:
        return "Unknown"

    leading = code[0]
    if leading == "1":
        return "Residential"
    if leading in {"2", "3"}:
        return "Commercial"
    if leading == "4":
        return "Industrial"
    if leading in {"5", "6"}:
        return "Commercial"
    if leading == "0":
        return "Exempt"
    if leading == "7":
        return "Agricultural"
    if leading == "8":
        return "Forest"
    if leading == "9":
        return "Mixed"
    return "Other"


def _classify_commercial_subtype(use_code: Optional[object]) -> str:
    """
    Classify commercial USE_CODE into subcategories.
    Returns: Retail, Office, Service, Mixed Use, or Other Commercial
    """
    code = _clean_string(use_code)
    if not code:
        return "Other Commercial"

    leading = code[0]
    if leading == "2":
        return "Retail"
    elif leading == "3":
        return "Office"
    elif leading == "5":
        return "Mixed Use"
    elif leading == "6":
        return "Service"
    else:
        return "Other Commercial"


def _get_use_description(use_code: str, usecode_lookup: Dict[str, str]) -> str:
    """
    Get use description from the USE_CODE lookup table.
    Falls back to classified category if lookup fails.

    Args:
        use_code: The USE_CODE value (e.g., "101", "102")
        usecode_lookup: Dict mapping USE_CODE -> USE_DESC

    Returns:
        USE_DESC if found in lookup, otherwise classified category
    """
    # Try to find in lookup table
    use_code_str = str(use_code).strip()
    if use_code_str in usecode_lookup:
        return usecode_lookup[use_code_str]

    # Fallback: use the classified category based on USE_CODE
    category = _classify_use_code(use_code)
    return category


def _compose_owner_address(record: Dict[str, object]) -> Optional[str]:
    parts = [
        _clean_string(record.get("OWN_ADDR")),
        _clean_string(record.get("OWN_CITY")),
        _clean_string(record.get("OWN_STATE")),
        _clean_zip(record.get("OWN_ZIP")),
    ]
    filtered = [part for part in parts if part]
    return ", ".join(filtered) if filtered else None


def _extract_site_address(record: Dict[str, object]) -> Optional[str]:
    """
    Return the best available street address for a parcel record.
    Some towns only populate LOC_ADDR (or other derived fields) instead of SITE_ADDR,
    so we need to check multiple sources before giving up.
    """
    candidates = [
        _clean_string(record.get("SITE_ADDR")),
        _clean_string(record.get("LOC_ADDR")),
        _clean_string(record.get("LOCATION")),
        _clean_string(record.get("FULL_STR")),
    ]
    for candidate in candidates:
        if candidate:
            return candidate
    return None


def _clean_string(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_zip(value: Optional[object]) -> Optional[str]:
    text = _clean_string(value)
    if not text:
        return None

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 9:
        return f"{digits[:5]}-{digits[5:9]}"
    if len(digits) >= 5:
        return digits[:5]
    return digits or None


_DIRECTION_EQUIVALENTS: Dict[str, str] = {
    "N": "NORTH",
    "S": "SOUTH",
    "E": "EAST",
    "W": "WEST",
    "NE": "NORTHEAST",
    "NW": "NORTHWEST",
    "SE": "SOUTHEAST",
    "SW": "SOUTHWEST",
}

_STREET_SUFFIX_EQUIVALENTS: Dict[str, str] = {
    "ALY": "ALLEY",
    "ALLEY": "ALLEY",
    "AVE": "AVENUE",
    "AV": "AVENUE",
    "AVENUE": "AVENUE",
    "BLVD": "BOULEVARD",
    "BOULEVARD": "BOULEVARD",
    "BRG": "BRIDGE",
    "BRIDGE": "BRIDGE",
    "CIR": "CIRCLE",
    "CIRCLE": "CIRCLE",
    "CT": "COURT",
    "COURT": "COURT",
    "DR": "DRIVE",
    "DRIVE": "DRIVE",
    "EXPY": "EXPRESSWAY",
    "EXPRESSWAY": "EXPRESSWAY",
    "FWY": "FREEWAY",
    "FREEWAY": "FREEWAY",
    "HWY": "HIGHWAY",
    "HIGHWAY": "HIGHWAY",
    "LN": "LANE",
    "LANE": "LANE",
    "PK": "PARK",
    "PARK": "PARK",
    "PKW": "PARKWAY",
    "PKWY": "PARKWAY",
    "PARKWAY": "PARKWAY",
    "PL": "PLACE",
    "PLACE": "PLACE",
    "PLZ": "PLAZA",
    "PLAZA": "PLAZA",
    "RD": "ROAD",
    "ROAD": "ROAD",
    "ROW": "ROW",
    "SQ": "SQUARE",
    "SQUARE": "SQUARE",
    "ST": "STREET",
    "STREET": "STREET",
    "TER": "TERRACE",
    "TERR": "TERRACE",
    "TERRACE": "TERRACE",
    "TPKE": "TURNPIKE",
    "TURNPIKE": "TURNPIKE",
    "TRL": "TRAIL",
    "TR": "TRAIL",
    "TRAIL": "TRAIL",
    "WAY": "WAY",
}

_APT_INDICATOR_TOKENS = {
    "APT",
    "APARTMENT",
    "UNIT",
    "STE",
    "SUITE",
    "FL",
    "FLOOR",
    "ROOM",
    "RM",
    "#",
}

_NORMALIZATION_MAP: Dict[str, str] = {
    **_DIRECTION_EQUIVALENTS,
    **_STREET_SUFFIX_EQUIVALENTS,
    "STREET": "ST",
    "ST": "ST",
    "ROAD": "RD",
    "RD": "RD",
    "AVENUE": "AVE",
    "AVE": "AVE",
    "AV": "AVE",
    "DRIVE": "DR",
    "DR": "DR",
    "LANE": "LN",
    "LN": "LN",
    "COURT": "CT",
    "CT": "CT",
    "PARKWAY": "PKWY",
    "PKWY": "PKWY",
    "PLACE": "PL",
    "PL": "PL",
    "TERRACE": "TER",
    "TER": "TER",
    "CIRCLE": "CIR",
    "CIR": "CIR",
    "APARTMENT": "APT",
    "APT": "APT",
    "UNIT": "UNIT",
    "SUITE": "SUITE",
    "FLOOR": "FL",
    "ROOM": "RM",
    "PO": "PO",
    "BOX": "BOX",
}


def _normalize_tokens(value: Optional[str]) -> List[str]:
    if not value:
        return []
    text = re.sub(r"[^\w\s]", " ", str(value).upper())
    text = re.sub(r"\s+", " ", text)
    tokens = re.split(r"[^A-Z0-9]+", text)
    normalized: List[str] = []
    for token in tokens:
        if not token:
            continue
        normalized.append(_NORMALIZATION_MAP.get(token, token))
    return normalized


def _normalize_city_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    tokens = _normalize_tokens(value)
    return "".join(tokens) or None


def _normalize_zip_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) >= 5:
        return digits[:5]
    return digits or None


def _normalize_street_address(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    text = str(value).upper()
    text = re.sub(r"P[\.\s]*O[\.\s]*\s*BOX", "PO BOX", text)
    tokens: List[str] = []
    for raw in re.findall(r"[A-Z0-9]+", text):
        mapped = _NORMALIZATION_MAP.get(raw, raw)
        if mapped in _APT_INDICATOR_TOKENS:
            break
        tokens.append(mapped)
    if not tokens:
        return None

    merged: List[str] = []
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token == "PO" and i + 1 < len(tokens) and tokens[i + 1] == "BOX":
            merged.append("POBOX")
            i += 2
            continue
        merged.append(token)
        i += 1

    return "".join(merged) or None


def _normalize_compare_value(parts: Iterable[Optional[str]]) -> str:
    normalized_tokens: List[str] = []
    for part in parts:
        normalized_tokens.extend(_normalize_tokens(part))
    return "".join(token for token in normalized_tokens if token)


def _is_absentee(record: Dict[str, object]) -> bool:
    site_address = _normalize_street_address(
        _extract_site_address(record) or _clean_string(record.get("SITE_ADDR"))
    )
    owner_address = _normalize_street_address(_clean_string(record.get("OWN_ADDR")))

    site_city = _normalize_city_value(
        _clean_string(record.get("SITE_CITY")) or _clean_string(record.get("CITY"))
    )
    owner_city = _normalize_city_value(_clean_string(record.get("OWN_CITY")))

    site_zip = _normalize_zip_value(
        _clean_zip(record.get("SITE_ZIP")) or _clean_zip(record.get("ZIP"))
    )
    owner_zip = _normalize_zip_value(_clean_zip(record.get("OWN_ZIP")))

    owner_known = any([owner_address, owner_city, owner_zip])
    if not owner_known:
        return False

    street_match = bool(
        site_address and owner_address and site_address == owner_address
    )
    city_match = bool(site_city and owner_city and site_city == owner_city)
    zip_match = bool(site_zip and owner_zip and site_zip == owner_zip)

    if street_match:
        if (not site_city or not owner_city or city_match) and (
            not site_zip or not owner_zip or zip_match
        ):
            return False

    if zip_match and city_match and owner_address and not site_address:
        return False

    return True


def _batchdata_headers() -> Dict[str, str]:
    api_key = getattr(settings, "BATCHDATA_API_KEY", "")
    if not api_key:
        raise SkipTraceError("Skip trace API key is not configured.")
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _batchdata_dnc_lookup(numbers: List[str]) -> Dict[str, str]:
    if not numbers:
        return {}
    headers = _batchdata_headers()
    results: Dict[str, str] = {}
    try:
        response = requests.post(
            BATCHDATA_DNC_ENDPOINT,
            headers=headers,
            json={"requests": numbers},
            timeout=BATCHDATA_TIMEOUT,
        )
        if response.status_code != 200:
            logger.warning("DNC lookup failed: %s %s", response.status_code, response.text)
            return {}
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("DNC lookup error: %s", exc)
        return {}

    for entry in payload.get("results", {}).get("phoneNumbers", []):
        number = entry.get("number")
        if number:
            results[number] = entry.get("dnc", "Unknown")
    return results


@lru_cache(maxsize=256)
def skiptrace_property(
    street: str,
    city: str,
    state: str,
    zip_code: str,
    *,
    max_phones: int = 3,
) -> SkipTraceResult:
    street = (street or "").strip()
    city = (city or "").strip()
    state = (state or "").strip()
    zip_code = (zip_code or "").strip()

    if not street or not city or not state:
        raise SkipTraceError("Street, city, and state are required for skip trace.")

    headers = _batchdata_headers()
    payload = {
        "requests": [
            {
                "propertyAddress": {
                    "street": street,
                    "city": city,
                    "state": state,
                    "zip": zip_code.split(".")[0],
                }
            }
        ]
    }

    try:
        response = requests.post(
            BATCHDATA_SKIPTRACE_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=BATCHDATA_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise SkipTraceError(f"Skip trace request failed: {exc}") from exc

    if response.status_code != 200:
        raise SkipTraceError(
            f"Skip trace API error {response.status_code}: {response.text}"
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise SkipTraceError("Skip trace response was not JSON.") from exc

    persons = data.get("results", {}).get("persons", [])
    if not persons:
        raise SkipTraceError("Skip trace returned no matches.")

    person = persons[0]
    owner_name = None
    name_meta = person.get("name") or {}
    if isinstance(name_meta, dict):
        owner_name = " ".join(filter(None, [name_meta.get("first"), name_meta.get("last")])).strip() or None
    else:
        owner_name = str(name_meta).strip() or None

    emails = person.get("emails") or []
    email_value = None
    if emails:
        if isinstance(emails[0], dict):
            email_value = emails[0].get("email")
        else:
            email_value = str(emails[0])

    raw_phones = person.get("phoneNumbers") or []
    sorted_phones = sorted(
        raw_phones,
        key=lambda entry: (entry.get("type") == "mobile", entry.get("score") or 0),
        reverse=True,
    )

    unique_numbers: Dict[str, Dict[str, object]] = {}
    for entry in sorted_phones:
        number = (entry.get("number") or "").strip()
        if not number or number in unique_numbers:
            continue
        unique_numbers[number] = entry
        if len(unique_numbers) >= max_phones:
            break

    dnc_numbers = list(unique_numbers.keys())
    dnc_lookup = _batchdata_dnc_lookup(dnc_numbers) if dnc_numbers else {}

    phones: List[SkipTracePhone] = []
    for number, meta in unique_numbers.items():
        phones.append(
            SkipTracePhone(
                number=number,
                type=meta.get("type"),
                score=meta.get("score"),
                dnc=dnc_lookup.get(number),
            )
        )

    return SkipTraceResult(owner_name=owner_name, email=email_value, phones=phones, raw_payload=data)


def _normalize_loc_id(value: Optional[object]) -> str:
    text = str(value or "").strip()
    return text.replace(" ", "").replace("-", "").upper()


def _to_number(value: Optional[object]) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    if abs(number - int(number)) < 0.001:
        return float(int(number))
    return number


def _to_decimal_number(value: Optional[object]) -> Optional[Decimal]:
    number = _to_number(value)
    if number is None:
        return None
    try:
        return Decimal(str(number))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _decimal_to_float(value: Optional[Decimal]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Optional[object]) -> Optional[int]:
    number = _to_number(value)
    if number is None:
        return None
    return int(round(number))


def calculate_equity_metrics(
    record: Dict[str, object]
) -> Tuple[
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
    Optional[float],
]:
    total_value = _to_number(record.get("MARKET_VALUE"))
    if total_value is None or total_value <= 0:
        total_value = _to_number(record.get("TOTAL_VAL"))
    sale_price = _to_number(record.get("LS_PRICE"))
    sale_date = _parse_massgis_date(record.get("LS_DATE"))

    if total_value is None or total_value <= 0:
        return None, None, None, None, None, None

    estimated_balance = None
    if sale_price is not None and sale_price > 0 and sale_date is not None:
        result = _estimate_remaining_balance(sale_price, sale_date)
        if isinstance(result, tuple):
            estimated_balance, monthly_payment, annual_rate = result
        else:
            estimated_balance = result
            monthly_payment = None
            annual_rate = None
    else:
        monthly_payment = None
        annual_rate = None

    if estimated_balance is None and sale_price is not None and sale_price >= 0:
        estimated_balance = sale_price

    if estimated_balance is None:
        return None, None, None, None, None, None

    equity_value = total_value - estimated_balance
    if equity_value < 0:
        equity_value = 0.0
    if estimated_balance > total_value:
        estimated_balance = total_value
    percent = (equity_value / total_value) * 100 if total_value else None
    initial_investment = None
    roi_percent = None
    if sale_price is not None and sale_price > 0:
        initial_investment = sale_price * max(1 - DEFAULT_INITIAL_LTV, 0.0)
        if initial_investment and initial_investment > 0:
            roi_percent = ((equity_value - initial_investment) / initial_investment) * 100

    return (
        percent,
        max(estimated_balance, 0.0),
        equity_value,
        roi_percent,
        annual_rate,
        monthly_payment,
    )


def _parse_massgis_date(value: Optional[object]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None

    candidates = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%Y%m%d",
        "%m/%d/%y",
        "%Y",
    ]
    for fmt in candidates:
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y":
                parsed = parsed.replace(month=1, day=1)
            return parsed
        except ValueError:
            continue
    return None





def fetch_parcel_shape_for_lead(lead) -> ParcelShapeResult:
    city = getattr(lead, "site_city", None)
    loc_id = getattr(lead, "loc_id", None)

    if not loc_id:
        return ParcelShapeResult(
            found=False,
            svg_markup=None,
            attribute_rows=[],
            centroid=None,
            area=None,
            width=None,
            height=None,
            source_hint=None,
            message="No parcel identifier on record for this lead.",
        )

    if shapefile is None:
        return ParcelShapeResult(
            found=False,
            svg_markup=None,
            attribute_rows=[],
            centroid=None,
            area=None,
            width=None,
            height=None,
            source_hint=None,
            message="Install the 'pyshp' package to enable parcel outlines from local GIS data.",
        )

    if city is None:
        return ParcelShapeResult(
            found=False,
            svg_markup=None,
            attribute_rows=[],
            centroid=None,
            area=None,
            width=None,
            height=None,
            source_hint=None,
            message="City missing on lead; unable to locate matching GIS dataset.",
        )

    shapefile_path = _find_parcel_shapefile(city)
    if shapefile_path is None:
        return ParcelShapeResult(
            found=False,
            svg_markup=None,
            attribute_rows=[],
            centroid=None,
            area=None,
            width=None,
            height=None,
            source_hint=city,
            message=f"No parcel shapefile found for city '{city}'.",
        )

    match = _lookup_parcel_record(shapefile_path, loc_id)
    if match is None:
        return ParcelShapeResult(
            found=False,
            svg_markup=None,
            attribute_rows=[],
            centroid=None,
            area=None,
            width=None,
            height=None,
            source_hint=shapefile_path.name,
            message=(
                "Matching parcel not found in local GIS dataset. "
                "Verify the lead's LOC_ID matches the GIS data."
            ),
        )

    shape, attributes = match
    neighbors = _collect_surrounding_shapes(shapefile_path, shape)
    svg_markup = _shape_to_svg(shape, neighbors=neighbors)
    centroid = _shape_centroid(shape)
    area = _shape_area(shape)
    width, height = _shape_dimensions(shape)
    attribute_rows = _format_attribute_rows(attributes)

    return ParcelShapeResult(
        found=True,
        svg_markup=svg_markup,
        attribute_rows=attribute_rows,
        centroid=centroid,
        area=area,
        width=width,
        height=height,
        source_hint=shapefile_path.name,
        message=None,
    )


def _find_parcel_shapefile(city_name: str) -> Optional[Path]:
    if not GISDATA_ROOT.exists():
        return None

    normalized_city = city_name.replace(" ", "").lower()
    candidates: List[Path] = []

    for directory in GISDATA_ROOT.iterdir():
        if not directory.is_dir():
            continue
        dir_name = directory.name.replace("_", "").replace(" ", "").lower()
        if normalized_city in dir_name:
            shapefiles = sorted(directory.glob("*TaxPar*.shp"))
            if shapefiles:
                candidates.append(shapefiles[0])

    return candidates[0] if candidates else None


def _lookup_parcel_record(shapefile_path: Path, loc_id: str):
    index = _load_parcel_index(str(shapefile_path))
    for candidate in _normalise_variants(loc_id):
        records = index.get(candidate)
        if records:
            return records[0]
    return None


@lru_cache(maxsize=16)
def _load_parcel_index(shapefile_path: str) -> Dict[str, List[Tuple["shapefile.Shape", Dict[str, object]]]]:
    reader = shapefile.Reader(shapefile_path)
    field_names = [field[0].strip() for field in reader.fields[1:]]
    index: Dict[str, List[Tuple["shapefile.Shape", Dict[str, object]]]] = {}

    try:
        for shape_record in reader.iterShapeRecords():
            record_dict = {
                field_names[i]: shape_record.record[i] for i in range(len(field_names))
            }
            keys = _collect_record_keys(record_dict)
            for key in keys:
                if not key:
                    continue
                index.setdefault(key, []).append((shape_record.shape, record_dict))
    finally:
        reader.close()

    return index


def _collect_record_keys(record: Dict[str, object]) -> Iterable[str]:
    preferred_fields = [
        "LOC_ID",
        "LOCID",
        "PAR_ID",
        "PARCEL_ID",
        "PARCELID",
        "PID",
        "MAP_PAR_ID",
        "MAP_PAR",
        "MAPLOT",
        "MAP_LOT",
        "GIS_ID",
        "GISID",
    ]

    keys: List[str] = []
    for field_name, value in record.items():
        if field_name.upper() in preferred_fields:
            keys.extend(_normalise_variants(value))

    # fallback: also use LOC_ID-style values even if field names differ
    if not keys:
        for value in record.values():
            keys.extend(_normalise_variants(value))
            if keys:
                break

    return keys


@lru_cache(maxsize=16)
def _load_all_records(shapefile_path: str) -> List[Tuple["shapefile.Shape", Dict[str, object]]]:
    reader = shapefile.Reader(shapefile_path)
    field_names = [field[0].strip() for field in reader.fields[1:]]
    records: List[Tuple["shapefile.Shape", Dict[str, object]]] = []

    try:
        for shape_record in reader.iterShapeRecords():
            record_dict = {
                field_names[i]: shape_record.record[i] for i in range(len(field_names))
            }
            records.append((shape_record.shape, record_dict))
    finally:
        reader.close()

    return records


def _collect_surrounding_shapes(
    shapefile_path: Path,
    target_shape: "shapefile.Shape",
    *,
    max_neighbors: int = 8,
    buffer_ratio: float = 0.15,
) -> List["shapefile.Shape"]:
    try:
        all_records = _load_all_records(str(shapefile_path))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to load surrounding parcels from %s: %s", shapefile_path, exc)
        return []

    min_x, min_y, max_x, max_y = target_shape.bbox
    span_x = max_x - min_x
    span_y = max_y - min_y
    pad_x = max(span_x, 1.0) * buffer_ratio
    pad_y = max(span_y, 1.0) * buffer_ratio

    extent = (min_x - pad_x, min_y - pad_y, max_x + pad_x, max_y + pad_y)

    neighbors: List["shapefile.Shape"] = []
    for shape, _ in all_records:
        if _bbox_equal(shape.bbox, target_shape.bbox):
            continue
        if _bbox_intersects(shape.bbox, extent):
            neighbors.append(shape)
            if len(neighbors) >= max_neighbors:
                break

    return neighbors


def _bbox_equal(
    bbox_a: Tuple[float, float, float, float],
    bbox_b: Tuple[float, float, float, float],
    *,
    tol: float = 1e-6,
) -> bool:
    return all(abs(a - b) <= tol for a, b in zip(bbox_a, bbox_b))


def _bbox_intersects(
    bbox_a: Tuple[float, float, float, float],
    bbox_b: Tuple[float, float, float, float],
) -> bool:
    a_min_x, a_min_y, a_max_x, a_max_y = bbox_a
    b_min_x, b_min_y, b_max_x, b_max_y = bbox_b
    return not (
        a_max_x < b_min_x
        or a_min_x > b_max_x
        or a_max_y < b_min_y
        or a_min_y > b_max_y
    )


def _normalise_variants(value: object) -> List[str]:
    if value is None:
        return []

    text = str(value).strip()
    if not text:
        return []

    variants = {
        text,
        text.upper(),
        text.lower(),
        text.replace("-", ""),
        text.replace("-", "").upper(),
        text.replace("-", "").lower(),
        text.replace(" ", ""),
        text.replace(" ", "").upper(),
        text.replace(" ", "").lower(),
    }

    stripped = text.lstrip("0")
    if stripped and stripped != text:
        variants.add(stripped)
        variants.add(stripped.upper())
        variants.add(stripped.lower())

    return [variant for variant in variants if variant]


def _shape_to_svg(
    shape: "shapefile.Shape",
    *,
    neighbors: Optional[List["shapefile.Shape"]] = None,
    width: int = 360,
    height: int = 360,
    padding: int = 12,
) -> str:
    neighbors = neighbors or []

    min_x, min_y, max_x, max_y = shape.bbox

    # Store original parcel dimensions
    parcel_span_x = max_x - min_x
    parcel_span_y = max_y - min_y

    if neighbors:
        # Calculate bounding box including neighbors
        neighbor_min_x = min(min_x, *(neighbor.bbox[0] for neighbor in neighbors))
        neighbor_min_y = min(min_y, *(neighbor.bbox[1] for neighbor in neighbors))
        neighbor_max_x = max(max_x, *(neighbor.bbox[2] for neighbor in neighbors))
        neighbor_max_y = max(max_y, *(neighbor.bbox[3] for neighbor in neighbors))

        # Limit expansion to 3x the parcel size in each direction to prevent extreme zoom out
        max_expansion_x = parcel_span_x * 3
        max_expansion_y = parcel_span_y * 3

        min_x = max(neighbor_min_x, min_x - max_expansion_x)
        min_y = max(neighbor_min_y, min_y - max_expansion_y)
        max_x = min(neighbor_max_x, max_x + max_expansion_x)
        max_y = min(neighbor_max_y, max_y + max_expansion_y)

    span_x = max_x - min_x
    span_y = max_y - min_y

    if span_x == 0 or span_y == 0:
        return ""

    scale = min((width - 2 * padding) / span_x, (height - 2 * padding) / span_y)

    def transform(point: Tuple[float, float]) -> Tuple[float, float]:
        x, y = point
        tx = padding + (x - min_x) * scale
        ty = height - (padding + (y - min_y) * scale)
        return tx, ty

    def build_path(target_shape: "shapefile.Shape") -> str:
        parts = list(target_shape.parts) + [len(target_shape.points)]
        segments: List[str] = []

        for start, end in zip(parts[:-1], parts[1:]):
            ring = target_shape.points[start:end]
            if not ring:
                continue

            x0, y0 = transform(ring[0])
            segment = [f"M{x0:.2f},{y0:.2f}"]

            for point in ring[1:]:
                x, y = transform(point)
                segment.append(f"L{x:.2f},{y:.2f}")

            segment.append("Z")
            segments.append(" ".join(segment))

        return " ".join(segments)

    main_path = build_path(shape)
    neighbor_paths = [build_path(neighbor) for neighbor in neighbors if neighbor.points]

    svg = (
        f'<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        f'xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Parcel outline">'
        f'<rect width="{width}" height="{height}" fill="#f8fafc" stroke="#e2e8f0" />'
    )

    for path in neighbor_paths:
        svg += (
            f'<path d="{path}" fill="#cbd5f5" fill-opacity="0.25" '
            f'stroke="#94a3b8" stroke-dasharray="4 4" stroke-width="1.4"/>'
        )

    svg += (
        f'<path d="{main_path}" fill="#3b82f6" fill-opacity="0.35" '
        f'stroke="#1d4ed8" stroke-width="2.4"/>'
        "</svg>"
    )
    return svg


def _shape_area(shape: "shapefile.Shape") -> Optional[float]:
    area = 0.0
    for ring in _iter_rings(shape):
        area += _signed_ring_area(ring)
    return abs(area) if area else None


def _shape_centroid(shape: "shapefile.Shape") -> Optional[Tuple[float, float]]:
    total_area = 0.0
    centroid_x = 0.0
    centroid_y = 0.0

    for ring in _iter_rings(shape):
        area = _signed_ring_area(ring)
        if area == 0:
            continue

        cx, cy = _ring_centroid(ring, area)
        total_area += area
        centroid_x += cx * area
        centroid_y += cy * area

    if total_area == 0:
        return None

    return centroid_x / total_area, centroid_y / total_area


def _shape_dimensions(shape: "shapefile.Shape") -> Tuple[Optional[float], Optional[float]]:
    min_x, min_y, max_x, max_y = shape.bbox
    width = max_x - min_x
    height = max_y - min_y
    if width <= 0 or height <= 0:
        return None, None
    return width, height


def _iter_rings(shape: "shapefile.Shape") -> Iterable[List[Tuple[float, float]]]:
    parts = list(shape.parts) + [len(shape.points)]
    for start, end in zip(parts[:-1], parts[1:]):
        ring = shape.points[start:end]
        if len(ring) >= 3:
            yield ring


def _signed_ring_area(ring: List[Tuple[float, float]]) -> float:
    area = 0.0
    for (x1, y1), (x2, y2) in zip(ring, ring[1:] + [ring[0]]):
        area += x1 * y2 - x2 * y1
    return area / 2.0


def _ring_centroid(ring: List[Tuple[float, float]], signed_area: float) -> Tuple[float, float]:
    factor = 1 / (6 * signed_area)
    sum_x = 0.0
    sum_y = 0.0
    for (x1, y1), (x2, y2) in zip(ring, ring[1:] + [ring[0]]):
        cross = x1 * y2 - x2 * y1
        sum_x += (x1 + x2) * cross
        sum_y += (y1 + y2) * cross
    return sum_x * factor, sum_y * factor


def _format_attribute_rows(attributes: Dict[str, object]) -> List[Tuple[str, str]]:
    preferred_order = [
        "LOC_ID",
        "MAP_PAR_ID",
        "PAR_ID",
        "PARCEL_ID",
        "GIS_ID",
        "MAP",
        "LOT",
        "BLOCK",
        "OWN_NAME",
        "OWNER1",
        "LOC_ADDR",
        "SITE_ADDR",
        "AREA",
        "LOT_SIZE",
        "ACRES",
        "LANDVAL",
        "BLDGVAL",
        "TOTALVAL",
    ]

    rows: List[Tuple[str, str]] = []
    seen = set()

    def include(field_name: str):
        value = attributes.get(field_name)
        if value in (None, "", " "):
            return
        label = field_name.replace("_", " ").title()
        rows.append((label, str(value).strip()))
        seen.add(field_name)

    for field in preferred_order:
        if field in attributes:
            include(field)

    if len(rows) < 8:
        for field_name, value in attributes.items():
            if field_name in seen:
                continue
            include(field_name)
            if len(rows) >= 12:
                break

    return rows[:12]


def _load_town_boundary_cache() -> Optional[Dict[str, Any]]:
    if not MASSGIS_TOWN_BOUNDARIES_CACHE_PATH.exists():
        return None

    try:
        with MASSGIS_TOWN_BOUNDARIES_CACHE_PATH.open("r", encoding="utf-8") as fp:
            data = json.load(fp)
            if isinstance(data, dict):
                return data
    except (OSError, json.JSONDecodeError) as exc:  # noqa: PERF203 - best-effort cache read
        logger.warning(
            "Failed to read cached town boundaries at %s: %s",
            MASSGIS_TOWN_BOUNDARIES_CACHE_PATH,
            exc,
        )
        try:
            MASSGIS_TOWN_BOUNDARIES_CACHE_PATH.unlink()
        except OSError:
            pass
    return None


def _write_town_boundary_cache(payload: Dict[str, Any]) -> None:
    try:
        MASSGIS_TOWN_BOUNDARIES_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with MASSGIS_TOWN_BOUNDARIES_CACHE_PATH.open("w", encoding="utf-8") as fp:
            json.dump(payload, fp)
    except OSError as exc:
        logger.warning(
            "Unable to write town boundaries cache to %s: %s",
            MASSGIS_TOWN_BOUNDARIES_CACHE_PATH,
            exc,
        )


def get_massgis_town_boundaries_geojson() -> Dict[str, Any]:
    """
    Download and convert MassGIS town boundaries to GeoJSON format.
    Returns a GeoJSON FeatureCollection with all town boundaries.
    """
    import shapefile

    global _TOWN_BOUNDARIES_CACHE
    with _TOWN_BOUNDARIES_CACHE_LOCK:
        if _TOWN_BOUNDARIES_CACHE is not None:
            return _TOWN_BOUNDARIES_CACHE
        cache_hit = _load_town_boundary_cache()
        if cache_hit is not None:
            _TOWN_BOUNDARIES_CACHE = cache_hit
            return cache_hit

        # Ensure town boundaries are downloaded
        zip_path = MASSGIS_DOWNLOAD_DIR / "townssurvey_shp.zip"
        MASSGIS_TOWNS_DIR.mkdir(parents=True, exist_ok=True)

        if not zip_path.exists():
            logger.info("Downloading MassGIS town boundaries from %s", MASSGIS_TOWNS_URL)
            _download_file(MASSGIS_TOWNS_URL, zip_path, timeout=60)

        # Extract if not already extracted
        shp_path = MASSGIS_TOWNS_DIR / "TOWNSSURVEY_POLYM.shp"
        if not shp_path.exists():
            logger.info("Extracting town boundaries to %s", MASSGIS_TOWNS_DIR)
            try:
                with zipfile.ZipFile(zip_path, "r") as archive:
                    if archive.testzip() is not None:
                        raise zipfile.BadZipFile("Zip file integrity check failed")
                    archive.extractall(MASSGIS_TOWNS_DIR)
            except zipfile.BadZipFile as exc:
                logger.error("Failed to extract town boundaries - removing corrupted file")
                if zip_path.exists():
                    zip_path.unlink()
                raise MassGISDownloadError("Corrupted town boundaries zip file") from exc

        # Read shapefile and convert to GeoJSON
        try:
            sf = shapefile.Reader(str(shp_path))
            features = []

            for shape_record in sf.shapeRecords():
                shape = shape_record.shape
                record = shape_record.record

                # Convert coordinates from State Plane to WGS84
                wgs84_coords = []
                for part_idx in range(len(shape.parts)):
                    start_idx = shape.parts[part_idx]
                    end_idx = shape.parts[part_idx + 1] if part_idx + 1 < len(shape.parts) else len(shape.points)

                    part_coords = []
                    for point in shape.points[start_idx:end_idx]:
                        lng, lat = massgis_stateplane_to_wgs84(point[0], point[1])
                        part_coords.append([lng, lat])

                    wgs84_coords.append(part_coords)

                # Get town attributes
                field_names = [field[0] for field in sf.fields[1:]]
                attributes = dict(zip(field_names, record))

                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "MultiPolygon" if len(wgs84_coords) > 1 else "Polygon",
                        "coordinates": [wgs84_coords] if len(wgs84_coords) > 1 else wgs84_coords
                    },
                    "properties": {
                        "TOWN": attributes.get("TOWN", ""),
                        "TOWN_ID": attributes.get("TOWN_ID", ""),
                        "POP2010": attributes.get("POP2010", 0),
                        "FOURCOLOR": attributes.get("FOURCOLOR", 0),
                    }
                }
                features.append(feature)

            payload = {
                "type": "FeatureCollection",
                "features": features
            }
            _write_town_boundary_cache(payload)
            _TOWN_BOUNDARIES_CACHE = payload
            return payload

        except Exception as exc:
            logger.error("Failed to read town boundaries shapefile: %s", exc)
            raise MassGISDataError(f"Failed to read town boundaries: {exc}") from exc


def get_towns_in_bbox(north: float, south: float, east: float, west: float) -> List[int]:
    """
    Find town IDs that intersect with the given bounding box (WGS84 coordinates).
    Returns list of town_ids.
    """
    import shapefile

    shp_path = MASSGIS_TOWNS_DIR / "TOWNSSURVEY_POLYM.shp"
    if not shp_path.exists():
        # Trigger download/extraction
        get_massgis_town_boundaries_geojson()

    try:
        sf = shapefile.Reader(str(shp_path))
        town_ids = []

        for shape_record in sf.shapeRecords():
            shape = shape_record.shape
            record = shape_record.record

            # Get town bounds in State Plane coordinates
            bbox_sp = shape.bbox  # [minX, minY, maxX, maxY] in State Plane

            # Convert bbox corners to WGS84
            sw_lng, sw_lat = massgis_stateplane_to_wgs84(bbox_sp[0], bbox_sp[1])
            ne_lng, ne_lat = massgis_stateplane_to_wgs84(bbox_sp[2], bbox_sp[3])

            # Check if town bbox intersects with query bbox
            # Two bboxes intersect if they overlap in both X and Y
            if not (ne_lng < west or sw_lng > east or ne_lat < south or sw_lat > north):
                # Get town_id from record
                field_names = [field[0] for field in sf.fields[1:]]
                attributes = dict(zip(field_names, record))
                town_id = attributes.get("TOWN_ID")
                if town_id:
                    town_ids.append(int(town_id))

        logger.info(f"Found {len(town_ids)} towns in bbox: {town_ids}")
        return town_ids

    except Exception as exc:
        logger.error("Failed to find towns in bbox: %s", exc)
        return []


def get_parcels_in_bbox(north: float, south: float, east: float, west: float,
                        limit: Optional[int] = None, shape_filter: Optional[Dict[str, Any]] = None,
                        **filters) -> List[Dict[str, Any]]:
    """
    Get parcels within a bounding box, optionally filtered.
    Returns list of parcel dictionaries with geometry and attributes.

    Available filters:
    - property_category: Filter by category (Residential, Commercial, etc.)
    - property_type: Filter by USE_DESC (e.g., "Single Family Residential")
    - min_price: Minimum assessed value
    - max_price: Maximum assessed value
    - equity_min: Minimum equity percentage
    - absentee: "absentee" for absentee owners only, "owner"/"owner-occupied" for owner-occupied
    - min_years_owned: Minimum years the property has been owned
    - max_years_owned: Maximum years the property has been owned
    - proximity_address: Center address for radius filter
    - proximity_radius_miles: Radius (in miles) around the center address
    - town_id: Only include parcels from this town id
    - town_name: Filter by specific town name

    If limit is None, returns all matching parcels (no limit).
    """
    import shapefile

    viewport_bbox = (west, south, east, north)
    polygon_filter: Optional[List[Tuple[float, float]]] = None
    radius_limit_miles = None
    reference_point: Optional[Tuple[float, float, str]] = None
    radius_center_source = None

    if shape_filter:
        if shape_filter.get("type") == "circle":
            try:
                center_lat = float(shape_filter.get("center_lat"))
                center_lng = float(shape_filter.get("center_lng"))
                radius_limit_miles = float(shape_filter.get("radius_miles"))
                reference_point = (center_lng, center_lat, "wgs84")
                radius_center_source = shape_filter.get("source") or "boundary"
            except (TypeError, ValueError):
                radius_limit_miles = None
                reference_point = None
        elif shape_filter.get("type") == "polygon":
            coords_raw = shape_filter.get("coordinates") or []
            polygon_points: List[Tuple[float, float]] = []
            for coord in coords_raw:
                try:
                    polygon_points.append((float(coord[0]), float(coord[1])))
                except (TypeError, ValueError, IndexError):
                    continue
            if polygon_points:
                polygon_filter = polygon_points
    polygon_bounds = _polygon_bounds(polygon_filter) if polygon_filter else None
    center_address = _clean_string(filters.pop('proximity_address', None))
    geocode_town_name = None
    if center_address:
        town_id_hint = filters.get('town_id')
        if town_id_hint is not None:
            try:
                hint_town = _get_massgis_town(int(town_id_hint))
                geocode_town_name = hint_town.name
            except Exception:
                geocode_town_name = None
        elif filters.get('town_name'):
            geocode_town_name = str(filters['town_name']).split(" (", 1)[0].strip()
    proximity_radius_value = filters.pop('proximity_radius_miles', None)

    if polygon_bounds:
        west = max(west, polygon_bounds["west"])
        east = min(east, polygon_bounds["east"])
        south = max(south, polygon_bounds["south"])
        north = min(north, polygon_bounds["north"])
        viewport_bbox = (west, south, east, north)

    if reference_point is None and center_address and proximity_radius_value not in (None, ''):
        try:
            radius_limit_miles = float(proximity_radius_value)
        except (TypeError, ValueError):
            radius_limit_miles = None
        if radius_limit_miles is not None and radius_limit_miles < 0:
            radius_limit_miles = None

        if radius_limit_miles is not None:
            geocode_query = center_address
            if geocode_town_name and geocode_town_name.lower() not in geocode_query.lower():
                geocode_query = f"{center_address}, {geocode_town_name}, Massachusetts"
            logger.info("Radius filter: geocoding '%s'", geocode_query)
            coords = geocode_address(geocode_query)
            if coords:
                reference_point = (float(coords[0]), float(coords[1]), "wgs84")
                logger.info("Radius filter: geocode hit at lon=%s lat=%s", coords[0], coords[1])
            else:
                logger.warning("Radius filter: geocode miss for '%s'", geocode_query)

    if radius_limit_miles is not None and reference_point is not None:
        wgs_point = _ensure_wgs84(reference_point)
        if wgs_point:
            ref_lon, ref_lat = wgs_point
            lat_delta = radius_limit_miles / 69.0
            lon_scale = max(math.cos(math.radians(ref_lat)) * 69.0, 1e-6)
            lon_delta = radius_limit_miles / lon_scale
            west = max(west, ref_lon - lon_delta)
            east = min(east, ref_lon + lon_delta)
            south = max(south, ref_lat - lat_delta)
            north = min(north, ref_lat + lat_delta)
            viewport_bbox = (west, south, east, north)
            logger.info(
                "Radius filter: adjusted viewport to W%.6f E%.6f S%.6f N%.6f for %.2f mi around (%s,%s)",
                west,
                east,
                south,
                north,
                radius_limit_miles,
                ref_lat,
                ref_lon,
            )

    neighborhood_filter = _clean_string(filters.pop('neighborhood', None))
    boston_neighborhood = (
        _get_boston_neighborhood(neighborhood_filter) if neighborhood_filter else None
    )

    if boston_neighborhood:
        bn_west, bn_south, bn_east, bn_north = boston_neighborhood.bbox
        clipped_west = max(west, bn_west)
        clipped_south = max(south, bn_south)
        clipped_east = min(east, bn_east)
        clipped_north = min(north, bn_north)
        if clipped_west >= clipped_east or clipped_south >= clipped_north:
            logger.info(
                "Viewport does not overlap clipped Boston neighborhood '%s'; returning no parcels.",
                boston_neighborhood.name,
            )
            return []
        west, south, east, north = clipped_west, clipped_south, clipped_east, clipped_north
        viewport_bbox = (west, south, east, north)

    # Find towns that intersect the bbox
    town_ids = get_towns_in_bbox(north, south, east, west)
    if not town_ids:
        return []

    target_town_id = filters.pop('town_id', None)
    if target_town_id is None and boston_neighborhood:
        target_town_id = BOSTON_TOWN_ID
    if target_town_id is not None:
        try:
            target_town_id_int = int(target_town_id)
        except (TypeError, ValueError):
            logger.warning("Invalid town_id filter value: %s", target_town_id)
            target_town_id_int = None
        if target_town_id_int is not None:
            if target_town_id_int in town_ids:
                town_ids = [target_town_id_int]
            else:
                logger.info(
                    "Requested town_id %s not present in bbox results %s; using requested town anyway.",
                    target_town_id_int,
                    town_ids,
                )
                town_ids = [target_town_id_int]

    # Filter by town name if specified
    # Exception: Don't filter by town name if we have a Boston neighborhood filter
    # because the neighborhood will handle the filtering
    filter_town_name = filters.pop('town_name', None)
    if filter_town_name and not boston_neighborhood:
        filter_town_name = filter_town_name.strip().upper()
        if " (" in filter_town_name:
            filter_town_name = filter_town_name.split(" (", 1)[0].strip()
        logger.info(f"Filtering by town name: '{filter_town_name}'")
        filtered_town_ids = []
        for tid in town_ids:
            try:
                town = _get_massgis_town(tid)
                town_name_upper = town.name.upper()

                # Exact match
                if town_name_upper == filter_town_name:
                    filtered_town_ids.append(tid)
                    logger.info(f"Matched town (exact): {town.name} (ID: {tid})")
                # Partial match - check if filter starts with town name (e.g., "Manchester-by-the-Sea" matches "Manchester")
                elif filter_town_name.startswith(town_name_upper):
                    filtered_town_ids.append(tid)
                    logger.info(f"Matched town (prefix): {town.name} (ID: {tid}) for filter '{filter_town_name}'")
                # Also check if town name is contained in filter with word boundaries
                elif town_name_upper in filter_town_name.replace('-', ' ').split():
                    filtered_town_ids.append(tid)
                    logger.info(f"Matched town (word): {town.name} (ID: {tid}) for filter '{filter_town_name}'")
            except Exception as e:
                logger.warning(f"Error getting town {tid}: {e}")
                continue
        town_ids = filtered_town_ids if filtered_town_ids else []
        logger.info(f"Filtered to {len(town_ids)} towns: {town_ids}")
    elif filter_town_name and boston_neighborhood:
        # When we have a Boston neighborhood, ignore town_name filter and use neighborhood
        logger.info(f"Ignoring town_name filter '{filter_town_name}' because Boston neighborhood '{boston_neighborhood.name}' is specified")

    if not town_ids:
        logger.warning("No towns found in bbox (or after filtering)")
        return []

    # Special handling for Boston: If Boston is in the town_ids but no neighborhood is specified,
    # return empty results to force users to select a neighborhood (Boston is too large otherwise)
    if BOSTON_TOWN_ID in town_ids and not boston_neighborhood:
        logger.info(
            "Boston detected without neighborhood filter - returning no parcels. "
            "Users must select a specific Boston neighborhood to view parcels."
        )
        return []

    if boston_neighborhood:
        if BOSTON_TOWN_ID not in town_ids:
            town_ids.append(BOSTON_TOWN_ID)
        town_ids = [tid for tid in town_ids if tid == BOSTON_TOWN_ID]
        if not town_ids:
            logger.info(
                "Neighborhood filter '%s' ignored because Boston is not in viewport results.",
                neighborhood_filter,
            )
            boston_neighborhood = None

    if boston_neighborhood and not _bbox_intersects(viewport_bbox, boston_neighborhood.bbox):
        logger.info(
            "Viewport does not intersect Boston neighborhood '%s'; returning no parcels.",
            boston_neighborhood.name,
        )
        return []

    parcels = []
    radius_removed = 0

    # Query parcels from each town
    for town_id in town_ids:
        if limit is not None and len(parcels) >= limit:
            break

        try:
            town = _get_massgis_town(town_id)
            dataset_dir = _ensure_massgis_dataset(town)
            tax_par_path = _find_taxpar_shapefile(Path(dataset_dir))

            sf = shapefile.Reader(str(tax_par_path))
            field_names = [field[0] for field in sf.fields[1:]]

            # Load assessment records with address data
            assess_records = _load_assess_records(str(dataset_dir))

            if radius_limit_miles is not None and reference_point is None and center_address:
                derived_point = _find_reference_point_from_records(assess_records, center_address)
                if derived_point:
                    reference_point = derived_point

            # Load USE_CODE lookup table for descriptions
            usecode_lookup = _load_usecode_lookup(str(dataset_dir))

            # Build a lookup dict by LOC_ID
            assess_index: Dict[str, Dict[str, object]] = {}
            unit_records_map: Dict[str, List[Dict[str, object]]] = defaultdict(list)
            for record in assess_records:
                for key_name in ("LOC_ID", "MAP_PAR_ID", "PID", "GIS_ID"):
                    key_value = _clean_string(record.get(key_name))
                    if not key_value:
                        continue
                    unit_records_map[key_value].append(record)
                    existing = assess_index.get(key_value)
                    if existing is None or _should_replace_assess_record(record, existing):
                        assess_index[key_value] = record

            enforce_neighborhood = boston_neighborhood is not None and town_id == BOSTON_TOWN_ID

            for shape_record in sf.shapeRecords():
                if limit is not None and len(parcels) >= limit:
                    break

                shape = shape_record.shape
                record = shape_record.record
                attributes = dict(zip(field_names, record))

                # Join with assessment data
                assess_data = None
                unit_records: Optional[List[Dict[str, object]]] = None
                lookup_keys = [
                    _clean_string(attributes.get('LOC_ID')),
                    _clean_string(attributes.get('MAP_PAR_ID')),
                ]
                for key in lookup_keys:
                    if key and key in assess_index:
                        assess_data = assess_index[key]
                        unit_records = unit_records_map.get(key)
                        break

                if assess_data:
                    attributes.update(assess_data)
                if unit_records is None:
                    for key in lookup_keys:
                        if key and unit_records_map.get(key):
                            unit_records = unit_records_map[key]
                            break

                # Get parcel centroid
                if not shape.points:
                    continue

                geometry = _shape_to_geojson_geometry(shape)
                if not geometry:
                    continue
                leaflet_geometry = _geojson_geometry_to_leaflet_latlngs(geometry)
                if not leaflet_geometry:
                    continue

                centroid_point = _geometry_centroid(geometry)
                if not centroid_point:
                    first_point = None
                    def _extract_first_latlng(structure):
                        if not isinstance(structure, list) or not structure:
                            return None
                        first = structure[0]
                        if isinstance(first, list) and first and isinstance(first[0], (int, float)):
                            return first
                        return _extract_first_latlng(first)
                    first_point = _extract_first_latlng(leaflet_geometry)
                    if first_point:
                        centroid_point = {"lat": first_point[0], "lng": first_point[1]}
                if not centroid_point:
                    continue
                lat = centroid_point["lat"]
                lng = centroid_point["lng"]

                # Check if centroid is in bbox
                if not (south <= lat <= north and west <= lng <= east):
                    continue

                if radius_limit_miles is not None and reference_point is not None:
                    target_point = (lng, lat, "wgs84")
                    distance_miles = _distance_miles_between(reference_point, target_point)
                    if distance_miles is None or distance_miles > radius_limit_miles:
                        continue

                if enforce_neighborhood and not _neighborhood_contains_point(boston_neighborhood, lng, lat):
                    continue

                # Skip parcels only if we truly have no reasonable address fallback
                site_addr = _resolve_parcel_address(attributes, town)
                if not site_addr:
                    continue
                attributes["SITE_ADDR"] = site_addr
                loc_addr = _clean_string(attributes.get('LOC_ADDR')) or ""

                if not attributes.get("SITE_CITY"):
                    attributes["SITE_CITY"] = town.name.title()

                # Apply filters
                # Address contains filter
                if filters.get('address_contains'):
                    address_search = filters['address_contains'].upper()
                    address = (site_addr or loc_addr).upper()
                    if address_search not in address:
                        continue

                if filters.get('property_category'):
                    use_code = attributes.get('USE_CODE', '')
                    category = _classify_use_code(use_code)
                    # Case-insensitive comparison
                    if category.lower() != filters['property_category'].lower():
                        continue

                if filters.get('property_type'):
                    # Filter by use description (not USE_CODE) to handle multiple codes with same description
                    use_code = attributes.get('USE_CODE', '')
                    use_desc = _get_use_description(use_code, usecode_lookup)
                    if use_desc != filters['property_type']:
                        continue

                if filters.get('min_price'):
                    total_value = _safe_float(attributes.get('TOTAL_VAL', 0))
                    if total_value < filters['min_price']:
                        continue

                if filters.get('max_price'):
                    total_value = _safe_float(attributes.get('TOTAL_VAL', 0))
                    if total_value > filters['max_price']:
                        continue

                # Equity filter
                if filters.get('equity_min'):
                    equity_percent, _, _, _, _, _ = calculate_equity_metrics(attributes)
                    if equity_percent is None or equity_percent < filters['equity_min']:
                        continue

                # Absentee filter
                absentee_filter = filters.get('absentee', '').lower()
                if absentee_filter:
                    is_absentee = _is_absentee(attributes)
                    if absentee_filter == "absentee" and not is_absentee:
                        continue
                    if absentee_filter in {"owner", "owner-occupied"} and is_absentee:
                        continue

                # Years owned filter
                if filters.get('min_years_owned') or filters.get('max_years_owned'):
                    from datetime import date
                    sale_date = _parse_massgis_date(attributes.get("LS_DATE"))
                    if not sale_date:
                        continue
                    owned_years = (date.today() - sale_date.date()).days / 365.25
                    min_years_owned_filter = filters.get('min_years_owned')
                    max_years_owned_filter = filters.get('max_years_owned')
                    if min_years_owned_filter and owned_years < min_years_owned_filter:
                        continue
                    if max_years_owned_filter and owned_years > max_years_owned_filter:
                        continue

                if radius_limit_miles is not None:
                    if reference_point is None:
                        logger.info("Skipping radius filter because reference point is still missing")
                    else:
                        target_point = (lng, lat, "wgs84")
                        distance_miles = _distance_miles_between(reference_point, target_point)
                        if distance_miles is None or distance_miles > radius_limit_miles:
                            radius_removed += 1
                            continue

                if polygon_filter and not _point_in_polygon(lat, lng, polygon_filter):
                    continue

                # Classify the USE_CODE to a readable category for color coding
                use_code = attributes.get('USE_CODE', '')
                property_category = _classify_use_code(use_code)

                # Get use description from town-specific USE_DESC column
                # The column name varies by town (e.g., M007UC_LUT_CY24_FY24_USE_DESC for town 007)

                # Get use description from USE_CODE lookup table
                use_desc = _get_use_description(use_code, usecode_lookup)

                # Calculate absentee status
                is_absentee = _is_absentee(attributes)

                # Calculate equity metrics
                equity_percent, _, _, _, _, _ = calculate_equity_metrics(attributes)

                total_value = _safe_float(attributes.get('TOTAL_VAL', 0))
                land_value = _safe_float(attributes.get('LAND_VAL', 0))
                building_value = _safe_float(attributes.get('BLDG_VAL', 0))
                lot_size = _safe_float(attributes.get('LOT_SIZE', 0))
                last_sale_price = _safe_float(attributes.get('LS_PRICE', 0))

                # Build parcel data
                parcel = {
                    'loc_id': attributes.get('LOC_ID', ''),
                    'town_id': town_id,
                    'town_name': town.name,
                    'address': _format_address(attributes),
                    'owner': attributes.get('OWNER1') or attributes.get('OWNER_NAME', 'Unknown'),
                    'owner_address': _compose_owner_address(attributes),
                    'total_value': total_value,
                    'land_value': land_value,
                    'building_value': building_value,
                    'property_type': use_desc,
                    'property_category': property_category,  # Add classified category for color coding
                    'use_code': use_code,
                    'use_description': use_desc,  # Add use description (e.g., "Single Family", "Two Family")
                    'style': _clean_string(attributes.get('STYLE')),
                    'year_built': attributes.get('YEAR_BUILT'),
                    'units': attributes.get('UNITS'),
                    'lot_size': lot_size,
                    'lot_units': _clean_string(attributes.get('LOT_UNITS')),
                    'zoning': _clean_string(attributes.get('ZONING')),
                    'zone': _clean_string(attributes.get('ZONE')),
                    'absentee': is_absentee,
                    'equity_percent': equity_percent,
                    'last_sale_price': last_sale_price,
                    'last_sale_date': _clean_string(attributes.get('LS_DATE')),
                    'site_city': _clean_string(attributes.get('SITE_CITY')) or _clean_string(attributes.get('CITY')),
                    'site_zip': _clean_string(attributes.get('SITE_ZIP')) or _clean_string(attributes.get('ZIP')),
                    'city': _clean_string(attributes.get('SITE_CITY')) or _clean_string(attributes.get('CITY')) or town.name.title(),
                    'zip': _clean_string(attributes.get('SITE_ZIP')) or _clean_string(attributes.get('ZIP')),
                    'value_display': f"${total_value:,.0f}" if total_value else None,
                    'centroid': centroid_point,
                    'geometry': leaflet_geometry,
                    'units_detail': _summarize_unit_records(unit_records) if unit_records else None,
                }

                parcels.append(parcel)

        except Exception as exc:
            logger.warning(f"Error loading parcels from town {town_id}: {exc}")
            continue

    if radius_limit_miles is not None and reference_point is not None:
        logger.info(
            "Radius filter summary: kept %s parcels, removed %s outside %.2f miles",
            len(parcels),
            radius_removed,
            radius_limit_miles,
        )

    logger.info(f"Returning {len(parcels)} parcels in bbox")
    return parcels


def _safe_float(value) -> float:
    """Safely convert value to float."""
    try:
        return float(value) if value else 0.0
    except (ValueError, TypeError):
        return 0.0


def _format_address(attributes: Dict[str, Any]) -> str:
    """Format parcel address from attributes."""
    parts = []

    resolved = _resolve_parcel_address(attributes)
    if resolved:
        parts.append(resolved)

    site_city = _clean_string(attributes.get('SITE_CITY'))
    if site_city:
        parts.append(site_city)

    site_zip = _clean_string(attributes.get('SITE_ZIP'))
    if site_zip:
        parts.append(site_zip)

    if parts:
        return ', '.join(parts)

    # If no address, use LOC_ID as identifier
    loc_id = _clean_string(attributes.get('LOC_ID'))
    if loc_id:
        return f'Parcel {loc_id}'

    return 'Unknown parcel'


def _resolve_parcel_address(attributes: Dict[str, Any], town: Optional[MassGISTown] = None) -> Optional[str]:
    """
    Build the most reasonable site address we can from the available record fields.
    Many L3 parcel datasets leave SITE_ADDR/LOC_ADDR blank but include FULL_STR or
    LOCATION fields that contain the civic address. We only skip parcels after all
    fallbacks are exhausted.
    """
    site_addr = _clean_string(attributes.get('SITE_ADDR'))
    loc_addr = _clean_string(attributes.get('LOC_ADDR'))
    if site_addr:
        return site_addr
    if loc_addr:
        return loc_addr

    fallback_candidates = [
        _clean_string(attributes.get('FULL_STR')),
        _clean_string(attributes.get('LOCATION')),
    ]
    for candidate in fallback_candidates:
        if candidate:
            return candidate

    map_par = _clean_string(attributes.get('MAP_PAR_ID'))
    loc_id = _clean_string(attributes.get('LOC_ID'))
    if map_par or loc_id:
        label = map_par or loc_id
        return f"Parcel {label}"

    if town is not None and town.town_id == BOSTON_TOWN_ID:
        # Boston datasets sometimes have enough context in other identifier fields.
        fallback_source = (
            _clean_string(attributes.get("MAP_PAR_ID"))
            or _clean_string(attributes.get("LOC_ID"))
        )
        if fallback_source:
            return f"Parcel {fallback_source}"

    return None


def _shape_to_geojson_geometry(shape) -> Optional[Dict[str, Any]]:
    """
    Convert a PyShp shape to GeoJSON geometry in WGS84 coordinates, preserving multipart rings.
    """
    if not shape or not hasattr(shape, "__geo_interface__"):
        return None

    geometry = shape.__geo_interface__
    return _transform_geometry_to_wgs84(geometry)


def _transform_geometry_to_wgs84(geometry: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")
    if not geom_type or not coords:
        return None

    if geom_type == "Polygon":
        rings = [_convert_ring_to_wgs84(ring) for ring in coords]
        rings = [ring for ring in rings if ring]
        if not rings:
            return None
        return {"type": "Polygon", "coordinates": rings}

    if geom_type == "MultiPolygon":
        polygons = []
        for polygon in coords:
            converted = [_convert_ring_to_wgs84(ring) for ring in polygon]
            converted = [ring for ring in converted if ring]
            if converted:
                polygons.append(converted)
        if not polygons:
            return None
        return {"type": "MultiPolygon", "coordinates": polygons}

    # Unexpected geometry type - treat as polygon if possible
    if isinstance(coords, list):
        return _transform_geometry_to_wgs84({"type": "Polygon", "coordinates": coords})
    return None


def _convert_ring_to_wgs84(ring: Iterable[Iterable[float]]) -> Optional[List[List[float]]]:
    converted: List[List[float]] = []
    for point in ring or []:
        if point is None or len(point) < 2:
            continue
        lng, lat = massgis_stateplane_to_wgs84(point[0], point[1])
        converted.append([lng, lat])
    if len(converted) < 3:
        return None
    if converted[0] != converted[-1]:
        converted.append(converted[0])
    return converted


def _geojson_geometry_to_leaflet_latlngs(geometry: Dict[str, Any]) -> List:
    """
    Convert a GeoJSON geometry (WGS84) into the nested lat/lng arrays expected by Leaflet.
    """
    if not geometry:
        return []

    geom_type = geometry.get("type")
    coords = geometry.get("coordinates") or []

    def convert_ring(ring: Iterable[Iterable[float]]) -> List[List[float]]:
        latlng_ring: List[List[float]] = []
        for point in ring or []:
            if point is None or len(point) < 2:
                continue
            lng, lat = point
            latlng_ring.append([lat, lng])
        if latlng_ring and latlng_ring[0] == latlng_ring[-1]:
            latlng_ring.pop()
        return latlng_ring

    if geom_type == "Polygon":
        rings = []
        for ring in coords:
            converted = convert_ring(ring)
            if converted:
                rings.append(converted)
        return rings

    if geom_type == "MultiPolygon":
        polygons = []
        for polygon in coords:
            converted_polygon = []
            for ring in polygon:
                converted = convert_ring(ring)
                if converted:
                    converted_polygon.append(converted)
            if converted_polygon:
                polygons.append(converted_polygon)
        return polygons

    return []


def _geometry_centroid(geometry: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """
    Compute centroid for GeoJSON geometry using the first outer ring.
    """
    if not geometry:
        return None

    coords = geometry.get("coordinates")
    if not coords:
        return None

    ring: Optional[List[List[float]]] = None
    if geometry.get("type") == "Polygon":
        ring = coords[0] if coords else None
    elif geometry.get("type") == "MultiPolygon":
        first_polygon = coords[0] if coords else None
        if first_polygon:
            ring = first_polygon[0] if first_polygon else None

    if not ring or len(ring) < 4:
        return None

    area = 0.0
    cx = 0.0
    cy = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i]
        x2, y2 = ring[i + 1]
        cross = x1 * y2 - x2 * y1
        area += cross
        cx += (x1 + x2) * cross
        cy += (y1 + y2) * cross

    area *= 0.5
    if abs(area) < 1e-9:
        return None

    cx /= (6 * area)
    cy /= (6 * area)
    return {"lat": cy, "lng": cx}


def _point_in_polygon(lat: float, lng: float, polygon: Sequence[Tuple[float, float]]) -> bool:
    """
    Ray casting algorithm for point-in-polygon using (lat, lng) ordering.
    """
    inside = False
    if not polygon:
        return False
    n = len(polygon)
    y = lat
    x = lng
    for i in range(n):
        y1, x1 = polygon[i]
        y2, x2 = polygon[(i + 1) % n]
        if (y1 > y) != (y2 > y):
            slope = (x2 - x1) / (y2 - y1 + 1e-9)
            x_intersect = slope * (y - y1) + x1
            if x < x_intersect:
                inside = not inside
    return inside


def _polygon_bounds(polygon: Sequence[Tuple[float, float]]) -> Optional[Dict[str, float]]:
    if not polygon:
        return None
    lats = [pt[0] for pt in polygon]
    lngs = [pt[1] for pt in polygon]
    if not lats or not lngs:
        return None
    return {
        "south": min(lats),
        "north": max(lats),
        "west": min(lngs),
        "east": max(lngs),
    }
