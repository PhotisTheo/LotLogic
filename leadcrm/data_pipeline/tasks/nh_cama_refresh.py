"""
Celery task for automated NH CAMA data refresh.

Downloads updated CAMA assessment files from NH GRANIT quarterly
and refreshes the local gisdata cache.
"""

import logging
import os
import shutil
import zipfile
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

import requests
from celery import shared_task
from django.conf import settings

logger = logging.getLogger(__name__)

# NH GRANIT CAMA file URLs (updated quarterly)
NH_CAMA_BASE_URL = "https://www.granit.unh.edu/cgi-bin/load_file?PATH=/media/nhgeodata/NHDOT/CADdata/parcels"

NH_COUNTIES = [
    "Belknap", "Carroll", "Cheshire", "Coos", "Grafton",
    "Hillsborough", "Merrimack", "Rockingham", "Strafford", "Sullivan"
]


@shared_task(bind=True, name="nh_cama_refresh")
def refresh_nh_cama_data(self, force: bool = False) -> dict:
    """
    Download and install updated NH CAMA assessment data.

    Args:
        force: If True, download even if files are recent

    Returns:
        Dictionary with status and results
    """
    logger.info("Starting NH CAMA data refresh...")

    results = {
        "started_at": datetime.now().isoformat(),
        "counties_updated": [],
        "counties_failed": [],
        "total_downloaded": 0,
        "errors": []
    }

    gisdata_dir = Path(settings.GISDATA_DIR) / "NH"
    gisdata_dir.mkdir(parents=True, exist_ok=True)

    for county in NH_COUNTIES:
        try:
            logger.info(f"Processing {county} County CAMA data...")

            # Check if update is needed
            existing_file = gisdata_dir / f"{county}_ParcelsCAMA.zip"
            if not force and existing_file.exists():
                # Check file age
                age_days = (datetime.now().timestamp() - existing_file.stat().st_mtime) / 86400
                if age_days < 90:  # Less than 90 days old
                    logger.info(f"Skipping {county} - file is recent ({age_days:.0f} days old)")
                    continue

            # Download county CAMA file
            success, error = download_county_cama(county, gisdata_dir)

            if success:
                results["counties_updated"].append(county)
                results["total_downloaded"] += 1
                logger.info(f"✓ Successfully updated {county} County CAMA data")
            else:
                results["counties_failed"].append(county)
                results["errors"].append(f"{county}: {error}")
                logger.error(f"✗ Failed to update {county}: {error}")

        except Exception as e:
            logger.exception(f"Error processing {county} County")
            results["counties_failed"].append(county)
            results["errors"].append(f"{county}: {str(e)}")

    # Clear CAMA cache to force reload
    try:
        from data_pipeline.sources.nh_cama import get_cama_loader
        cama_loader = get_cama_loader()
        cama_loader.clear_cache()
        logger.info("Cleared CAMA cache")
    except Exception as e:
        logger.warning(f"Could not clear CAMA cache: {e}")

    results["completed_at"] = datetime.now().isoformat()
    results["success"] = len(results["counties_failed"]) == 0

    logger.info(f"NH CAMA refresh completed: {results['total_downloaded']} counties updated")
    return results


def download_county_cama(county: str, dest_dir: Path) -> Tuple[bool, str]:
    """
    Download and extract CAMA file for a specific county.

    Args:
        county: County name (e.g., "Rockingham")
        dest_dir: Destination directory for files

    Returns:
        Tuple of (success: bool, error_message: str)
    """
    try:
        # Construct download URL
        # Note: NH GRANIT URLs may need adjustment based on their current structure
        filename = f"{county}_ParcelsCAMA.zip"
        url = f"{NH_CAMA_BASE_URL}/{filename}"

        logger.info(f"Downloading {county} CAMA from: {url}")

        # Download with timeout
        response = requests.get(url, timeout=300, stream=True)
        response.raise_for_status()

        # Save zip file
        zip_path = dest_dir / filename
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded {zip_path.stat().st_size / (1024*1024):.1f} MB")

        # Extract zip file
        extract_dir = dest_dir / f"{county}_ParcelsCAMA"

        # Remove old directory if exists
        if extract_dir.exists():
            shutil.rmtree(extract_dir)

        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        logger.info(f"Extracted CAMA data to {extract_dir}")

        return True, ""

    except requests.exceptions.RequestException as e:
        return False, f"Download failed: {str(e)}"
    except zipfile.BadZipFile as e:
        return False, f"Invalid zip file: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"


@shared_task(name="nh_cama_check_updates")
def check_for_cama_updates() -> dict:
    """
    Check if NH CAMA updates are available without downloading.

    Returns:
        Dictionary with update status for each county
    """
    logger.info("Checking for NH CAMA updates...")

    status = {}
    gisdata_dir = Path(settings.GISDATA_DIR) / "NH"

    for county in NH_COUNTIES:
        try:
            existing_file = gisdata_dir / f"{county}_ParcelsCAMA.zip"

            if not existing_file.exists():
                status[county] = {"status": "missing", "action": "download_needed"}
            else:
                age_days = (datetime.now().timestamp() - existing_file.stat().st_mtime) / 86400
                size_mb = existing_file.stat().st_size / (1024 * 1024)

                if age_days > 180:  # Older than 6 months
                    status[county] = {
                        "status": "outdated",
                        "age_days": int(age_days),
                        "size_mb": round(size_mb, 1),
                        "action": "update_recommended"
                    }
                else:
                    status[county] = {
                        "status": "current",
                        "age_days": int(age_days),
                        "size_mb": round(size_mb, 1),
                        "action": "none"
                    }

        except Exception as e:
            status[county] = {"status": "error", "error": str(e)}

    return status
