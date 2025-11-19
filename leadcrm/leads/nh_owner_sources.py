from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

OWNER_CACHE_TTL_SECONDS = getattr(settings, "NH_OWNER_CACHE_TTL_SECONDS", 86400)
OWNER_SOURCE_TIMEOUT = getattr(settings, "NH_OWNER_SOURCE_TIMEOUT", 12)


def _chunk(values: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


@dataclass(frozen=True)
class ArcGISTownSource:
    """Configuration for a NH town hosted on an ArcGIS FeatureServer."""

    endpoint: str
    parcel_id_field: str
    owner_fields: Sequence[str]
    mailing_address_field: str
    mailing_city_field: str
    mailing_state_field: str
    mailing_zip_field: str
    site_address_field: Optional[str] = None
    where_template: str = "UPPER({field}) = UPPER({value})"
    value_type: str = "string"  # 'string' or 'int'
    max_batch_size: int = 100
    description: Optional[str] = None

    def query_url(self) -> str:
        base = self.endpoint.rstrip("/")
        if base.lower().endswith("/query"):
            return base
        return f"{base}/query"

    def _format_value(self, value: str) -> str:
        if self.value_type == "int":
            try:
                return str(int(value))
            except (TypeError, ValueError):
                return "0"
        escaped = str(value or "").replace("'", "''")
        return f"'{escaped}'"

    def build_where(self, values: Sequence[str]) -> str:
        if not values:
            return "1=0"
        if len(values) == 1:
            return self.where_template.format(
                field=self.parcel_id_field,
                value=self._format_value(values[0]),
            )
        formatted = ", ".join(self._format_value(v) for v in values)
        return f"{self.parcel_id_field} IN ({formatted})"

    def out_fields(self) -> str:
        fields = {
            self.parcel_id_field,
            self.mailing_address_field,
            self.mailing_city_field,
            self.mailing_state_field,
            self.mailing_zip_field,
        }
        fields.update(self.owner_fields)
        if self.site_address_field:
            fields.add(self.site_address_field)
        return ",".join(sorted(f for f in fields if f))


ARC_GIS_CAI_BASE = "https://services5.arcgis.com/1f7xJHOXxX0LQVLK/arcgis/rest/services"


def _cai_service(town_slug: str) -> str:
    return f"{ARC_GIS_CAI_BASE}/{town_slug}/FeatureServer/0"


NH_OWNER_SOURCES: Dict[str, ArcGISTownSource] = {
    # Seacoast / border towns
    "Portsmouth": ArcGISTownSource(
        endpoint="https://gis.portsmouthnh.com/arcgis/rest/services/Public/Parcels/FeatureServer/0",
        parcel_id_field="PID",
        owner_fields=["OWNER1", "OWNER2"],
        mailing_address_field="MAILADDR",
        mailing_city_field="MAILCITY",
        mailing_state_field="MAILSTATE",
        mailing_zip_field="MAILZIP",
        site_address_field="SITEADDR",
        description="City of Portsmouth parcel layer (owner + mailing fields).",
    ),
    "Seabrook": ArcGISTownSource(
        endpoint=_cai_service("Seabrook_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Hampton": ArcGISTownSource(
        endpoint=_cai_service("Hampton_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Hampton Falls": ArcGISTownSource(
        endpoint=_cai_service("HamptonFalls_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Rye": ArcGISTownSource(
        endpoint=_cai_service("Rye_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    # Rockingham / Hillsborough border towns
    "Salem": ArcGISTownSource(
        endpoint=_cai_service("Salem_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Windham": ArcGISTownSource(
        endpoint=_cai_service("Windham_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Derry": ArcGISTownSource(
        endpoint=_cai_service("Derry_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Londonderry": ArcGISTownSource(
        endpoint=_cai_service("Londonderry_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Hudson": ArcGISTownSource(
        endpoint=_cai_service("Hudson_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Pelham": ArcGISTownSource(
        endpoint=_cai_service("Pelham_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Merrimack": ArcGISTownSource(
        endpoint=_cai_service("Merrimack_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Hollis": ArcGISTownSource(
        endpoint=_cai_service("Hollis_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Brookline": ArcGISTownSource(
        endpoint=_cai_service("Brookline_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
    "Nashua": ArcGISTownSource(
        endpoint=_cai_service("Nashua_TaxParcels"),
        parcel_id_field="PID",
        owner_fields=["OWN1", "OWN2"],
        mailing_address_field="M_ADDR",
        mailing_city_field="M_CITY",
        mailing_state_field="M_STATE",
        mailing_zip_field="M_ZIP",
        site_address_field="PROP_LOC",
    ),
}


def _normalize_key(town_name: str, parcel_id: str) -> tuple[str, str]:
    return (str(town_name or "").strip().lower(), str(parcel_id or "").strip())


def _get_cached_owner(town_name: str, parcel_id: str) -> Optional[Dict[str, object]]:
    key = _normalize_key(town_name, parcel_id)
    entry = _OWNER_CACHE.get(key)
    if not entry:
        return None
    timestamp, data = entry
    if OWNER_CACHE_TTL_SECONDS and (time.time() - timestamp) > OWNER_CACHE_TTL_SECONDS:
        _OWNER_CACHE.pop(key, None)
        return None
    return data


def _store_owner_cache(town_name: str, parcel_id: str, data: Dict[str, object]) -> None:
    _OWNER_CACHE[_normalize_key(town_name, parcel_id)] = (time.time(), data)


def _safe_get(props: Dict[str, object], field: Optional[str]) -> Optional[str]:
    if not field:
        return None
    return props.get(field.lower())


def _format_owner_record(
    town: str,
    source: ArcGISTownSource,
    attributes: Dict[str, object],
) -> Optional[Dict[str, object]]:
    props = {str(k).lower(): v for k, v in attributes.items()}
    pid = _safe_get(props, source.parcel_id_field)
    if not pid:
        return None

    owner_parts = [
        str(val).strip()
        for field in source.owner_fields
        if (val := _safe_get(props, field))
    ]
    owner_name = " & ".join([part for part in owner_parts if part])

    mailing_street = (_safe_get(props, source.mailing_address_field) or "").strip()
    mailing_city = (_safe_get(props, source.mailing_city_field) or "").strip()
    mailing_state = (_safe_get(props, source.mailing_state_field) or "").strip()
    mailing_zip = (_safe_get(props, source.mailing_zip_field) or "").strip()
    site_address = (_safe_get(props, source.site_address_field) or "").strip()

    mailing_full = ", ".join(
        [component for component in [mailing_street, f"{mailing_city} {mailing_state}".strip(), mailing_zip] if component]
    ).replace(" ,", ",").strip(", ")

    absentee = None
    if mailing_city:
        absentee = mailing_city.strip().lower() != str(town or "").strip().lower()
    elif mailing_state:
        absentee = mailing_state.strip().upper() not in {"NH", "NEW HAMPSHIRE"}

    data: Dict[str, object] = {
        "parcel_id": str(pid).strip(),
        "owner_name": owner_name or None,
        "owner_names": [part for part in owner_parts if part],
        "mailing_street": mailing_street or None,
        "mailing_city": mailing_city or None,
        "mailing_state": mailing_state or None,
        "mailing_zip": mailing_zip or None,
        "mailing_full": mailing_full or None,
        "site_address": site_address or None,
        "is_absentee": absentee,
        "source": source.description or "ArcGIS FeatureServer",
        "raw": attributes,
    }
    return data


_OWNER_CACHE: Dict[tuple[str, str], tuple[float, Dict[str, object]]] = {}


def bulk_fetch_nh_owner_info(town_name: str, parcel_ids: Sequence[str]) -> Dict[str, Dict[str, object]]:
    """Fetch owner records for a batch of parcel IDs."""
    if not parcel_ids:
        return {}

    source = NH_OWNER_SOURCES.get(town_name)
    if not source:
        return {}

    results: Dict[str, Dict[str, object]] = {}
    pending: List[str] = []

    for pid in parcel_ids:
        if not pid:
            continue
        cached = _get_cached_owner(town_name, pid)
        if cached:
            results[str(pid).strip()] = cached
        else:
            pending.append(str(pid).strip())

    if not pending:
        return results

    for chunk in _chunk(pending, source.max_batch_size):
        where = source.build_where(chunk)
        params = {
            "where": where,
            "outFields": source.out_fields(),
            "returnGeometry": "false",
            "f": "json",
        }
        try:
            response = requests.get(source.query_url(), params=params, timeout=OWNER_SOURCE_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            logger.warning("NH owner lookup failed for %s: %s", town_name, exc)
            continue
        features = payload.get("features") or []
        for feature in features:
            attributes = feature.get("attributes") or feature.get("properties") or {}
            normalized = _format_owner_record(town_name, source, attributes)
            if not normalized:
                continue
            pid = normalized.get("parcel_id")
            if not pid:
                continue
            results[str(pid).strip()] = normalized
            _store_owner_cache(town_name, pid, normalized)

    return results


def fetch_nh_owner_info(town_name: str, parcel_id: str) -> Optional[Dict[str, object]]:
    """Fetch owner info for a single parcel, checking the cache first."""
    if not parcel_id:
        return None
    cached = _get_cached_owner(town_name, parcel_id)
    if cached:
        return cached

    records = bulk_fetch_nh_owner_info(town_name, [parcel_id])
    return records.get(str(parcel_id).strip())


def get_configured_owner_towns() -> List[str]:
    """Return the list of NH towns with configured owner sources."""
    return sorted(NH_OWNER_SOURCES.keys())


__all__ = [
    "fetch_nh_owner_info",
    "bulk_fetch_nh_owner_info",
    "get_configured_owner_towns",
]
