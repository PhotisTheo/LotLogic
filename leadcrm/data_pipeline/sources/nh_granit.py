"""
NH GRANIT GIS data source adapter for parcel downloads.

Uses the ArcGIS REST API to query NH statewide parcel mosaic.
"""

import logging
import requests
from typing import Dict, List, Optional
import time

logger = logging.getLogger(__name__)


class NHGRANITSource:
    """
    Adapter for downloading NH parcel data from GRANIT ArcGIS REST API.

    The NH GRANIT system provides statewide parcel data through an ArcGIS MapServer.
    Unlike MassGIS which has per-town downloads, NH uses a single statewide mosaic
    that can be queried by municipality name.
    """

    BASE_URL = "https://nhgeodata.unh.edu/nhgeodata/rest/services/CAD/ParcelMosaic/MapServer"
    PARCELS_LAYER_ID = 0
    MAX_RECORDS_PER_REQUEST = 1000  # ArcGIS server limit

    def __init__(self, throttle_delay: float = 0.5):
        """
        Initialize the NH GRANIT source adapter.

        Args:
            throttle_delay: Seconds to wait between API requests (default 0.5)
        """
        self.throttle_delay = throttle_delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'LeadCRM-DataPipeline/1.0'
        })

    def get_parcels_for_municipality(
        self,
        municipality_name: str,
        out_fields: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Download all parcels for a specific NH municipality.

        Args:
            municipality_name: Name of NH town (e.g., "Portsmouth", "Nashua")
            out_fields: List of field names to return (None = all fields)

        Returns:
            List of parcel feature dictionaries with attributes and geometry

        Example:
            >>> source = NHGRANITSource()
            >>> parcels = source.get_parcels_for_municipality("Portsmouth")
            >>> len(parcels)
            3542
        """
        logger.info(f"Downloading parcels for {municipality_name}, NH")

        # First, get count and object IDs
        object_ids = self._get_all_object_ids(municipality_name)
        if not object_ids:
            logger.warning(f"No parcels found for {municipality_name}")
            return []

        logger.info(f"Found {len(object_ids)} parcels for {municipality_name}")

        # Download parcels in batches
        all_parcels = []
        for i in range(0, len(object_ids), self.MAX_RECORDS_PER_REQUEST):
            batch_ids = object_ids[i:i + self.MAX_RECORDS_PER_REQUEST]
            batch_parcels = self._download_parcels_by_ids(batch_ids, out_fields)
            all_parcels.extend(batch_parcels)

            logger.info(f"Downloaded {len(all_parcels)}/{len(object_ids)} parcels")
            time.sleep(self.throttle_delay)

        return all_parcels

    def _get_all_object_ids(self, municipality_name: str) -> List[int]:
        """
        Get all OBJECTID values for parcels in a municipality.

        This avoids the max record limit by only returning IDs, not full features.
        """
        query_url = f"{self.BASE_URL}/{self.PARCELS_LAYER_ID}/query"

        params = {
            "where": f"PBPLACE='{municipality_name}'",
            "returnIdsOnly": "true",
            "f": "json"
        }

        try:
            response = self.session.get(query_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "objectIds" in data:
                return data["objectIds"]
            elif "error" in data:
                logger.error(f"API error: {data['error']}")
                return []
            else:
                logger.warning(f"Unexpected response format: {data}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get object IDs: {e}")
            return []

    def _download_parcels_by_ids(
        self,
        object_ids: List[int],
        out_fields: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Download parcel features for a list of object IDs.
        """
        query_url = f"{self.BASE_URL}/{self.PARCELS_LAYER_ID}/query"

        # Build object ID list
        ids_str = ",".join(str(oid) for oid in object_ids)

        # Determine fields to return
        fields_str = "*" if out_fields is None else ",".join(out_fields)

        params = {
            "objectIds": ids_str,
            "outFields": fields_str,
            "returnGeometry": "true",
            "f": "geojson"
        }

        try:
            response = self.session.get(query_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if "features" in data:
                return data["features"]
            elif "error" in data:
                logger.error(f"API error downloading parcels: {data['error']}")
                return []
            else:
                logger.warning(f"No features in response")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download parcels: {e}")
            return []

    def get_layer_metadata(self) -> Optional[Dict]:
        """
        Get metadata about the parcels layer including field definitions.

        Returns:
            Dictionary with layer metadata or None if request fails
        """
        metadata_url = f"{self.BASE_URL}/{self.PARCELS_LAYER_ID}"

        try:
            response = self.session.get(metadata_url, params={"f": "json"}, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get layer metadata: {e}")
            return None

    def get_available_municipalities(self) -> List[str]:
        """
        Get list of all municipalities with parcel data in GRANIT.

        Returns:
            Sorted list of municipality names
        """
        query_url = f"{self.BASE_URL}/{self.PARCELS_LAYER_ID}/query"

        params = {
            "where": "1=1",
            "returnDistinctValues": "true",
            "outFields": "PBPLACE",
            "f": "json"
        }

        try:
            response = self.session.get(query_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "features" in data:
                municipalities = [f["attributes"]["PBPLACE"] for f in data["features"]]
                return sorted(set(municipalities))
            else:
                logger.warning("Could not retrieve municipality list")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get municipalities: {e}")
            return []


def download_nh_parcels(municipality_name: str) -> List[Dict]:
    """
    Convenience function to download all parcels for a NH municipality.

    Args:
        municipality_name: Name of NH town (e.g., "Portsmouth")

    Returns:
        List of parcel GeoJSON features
    """
    source = NHGRANITSource()
    return source.get_parcels_for_municipality(municipality_name)
