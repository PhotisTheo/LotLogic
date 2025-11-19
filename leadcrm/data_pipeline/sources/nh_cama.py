"""
NH CAMA (Computer Assisted Mass Appraisal) data loader.

Loads county-level NH CAMA shapefiles containing assessment data (tax values, etc.)
and merges with NH GRANIT parcel geometry data.
"""

import logging
import struct
from pathlib import Path
from typing import Dict, List, Optional
from functools import lru_cache

logger = logging.getLogger(__name__)


class NHCAMALoader:
    """
    Loader for NH CAMA assessment data from county shapefiles.

    CAMA files contain:
    - Tax assessed values (land, building, features, total)
    - Previous year values
    - Detailed addresses
    - Land use descriptions
    - Map/Block/Lot identifiers
    """

    def __init__(self, gisdata_path: str = "/app/gisdata/NH"):
        """
        Initialize CAMA loader.

        Args:
            gisdata_path: Path to NH CAMA data directory
        """
        self.gisdata_path = Path(gisdata_path)
        self._county_cache: Dict[str, Dict[str, Dict]] = {}  # county -> {PID -> data}

    def get_cama_data_for_parcel(self, county: str, pid: str) -> Optional[Dict]:
        """
        Get CAMA assessment data for a specific parcel.

        Args:
            county: County name (e.g., "Rockingham")
            pid: Parcel ID (e.g., "201.001.000")

        Returns:
            Dictionary of CAMA fields or None if not found
        """
        # Load county data if not cached
        if county not in self._county_cache:
            self._load_county(county)

        county_data = self._county_cache.get(county, {})
        return county_data.get(pid)

    def _load_county(self, county: str):
        """Load all CAMA data for a county into memory cache."""
        try:
            dbf_path = self.gisdata_path / f"{county}_ParcelsCAMA" / f"{county}_ParcelsCAMA.dbf"

            if not dbf_path.exists():
                logger.warning(f"CAMA file not found: {dbf_path}")
                self._county_cache[county] = {}
                return

            logger.info(f"Loading CAMA data for {county} County...")

            parcels = {}
            with open(dbf_path, 'rb') as f:
                # Read header
                f.seek(4)
                num_records = struct.unpack('<I', f.read(4))[0]
                header_length = struct.unpack('<H', f.read(2))[0]
                record_length = struct.unpack('<H', f.read(2))[0]

                # Read field definitions
                f.seek(32)
                fields = []
                while True:
                    field_info = f.read(32)
                    if field_info[0] == 0x0D:  # End of field descriptor
                        break

                    name = field_info[:11].split(b'\x00')[0].decode('ascii', errors='ignore')
                    field_type = chr(field_info[11])
                    length = field_info[16]

                    fields.append((name, field_type, length))

                # Read records
                for record_num in range(num_records):
                    f.seek(header_length + (record_num * record_length))
                    record_data = f.read(record_length)

                    if record_data[0] == 0x2A:  # Deleted record
                        continue

                    # Parse record
                    values = {}
                    pos = 1  # Skip deletion flag
                    for field_name, field_type, field_length in fields:
                        value = record_data[pos:pos+field_length].strip()
                        try:
                            value = value.decode('ascii', errors='ignore').strip()
                        except:
                            value = ''

                        # Convert numeric fields
                        if field_type in ('N', 'F') and value:
                            try:
                                value = float(value)
                            except ValueError:
                                pass

                        values[field_name] = value
                        pos += field_length

                    # Index by PID
                    pid = values.get('PID') or values.get('DisplayId') or values.get('RawId')
                    if pid and pid.strip():
                        parcels[pid.strip()] = values

            self._county_cache[county] = parcels
            logger.info(f"Loaded {len(parcels):,} CAMA records for {county} County")

        except Exception as e:
            logger.error(f"Error loading CAMA data for {county}: {e}")
            self._county_cache[county] = {}

    def clear_cache(self, county: Optional[str] = None):
        """Clear cached CAMA data."""
        if county:
            self._county_cache.pop(county, None)
        else:
            self._county_cache.clear()


# Singleton instance for reuse
_cama_loader: Optional[NHCAMALoader] = None


def get_cama_loader(gisdata_path: str = "/app/gisdata/NH") -> NHCAMALoader:
    """Get singleton CAMA loader instance."""
    global _cama_loader
    if _cama_loader is None:
        _cama_loader = NHCAMALoader(gisdata_path)
    return _cama_loader


def get_nh_parcel_cama_data(town_name: str, pid: str, gisdata_path: str = "/app/gisdata/NH") -> Optional[Dict]:
    """
    Get CAMA assessment data for a NH parcel.

    Args:
        town_name: Town name (e.g., "Brentwood")
        pid: Parcel ID (e.g., "201.001.000")
        gisdata_path: Path to NH CAMA data directory

    Returns:
        Dictionary of CAMA assessment fields or None
    """
    from leads.services import get_nh_county_for_town

    county = get_nh_county_for_town(town_name)
    if not county:
        logger.warning(f"Unknown NH town: {town_name}")
        return None

    loader = get_cama_loader(gisdata_path)
    return loader.get_cama_data_for_parcel(county, pid)
