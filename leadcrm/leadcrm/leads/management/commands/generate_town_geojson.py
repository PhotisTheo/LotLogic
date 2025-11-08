"""
Django management command to pre-generate GeoJSON files for all Massachusetts towns.

This dramatically improves map loading performance by:
1. Converting shapefiles to GeoJSON once (instead of on every request)
2. Pre-computing coordinate transformations (State Plane -> WGS84)
3. Serving static files from S3/CDN instead of processing on-demand

Usage:
    python manage.py generate_town_geojson [--towns TOWN_ID1,TOWN_ID2] [--upload-s3] [--output-dir DIR]

Examples:
    # Generate GeoJSON for all towns to local directory
    python manage.py generate_town_geojson

    # Generate for specific towns only
    python manage.py generate_town_geojson --towns 45,157,285

    # Generate and upload to S3
    python manage.py generate_town_geojson --upload-s3

    # Custom output directory
    python manage.py generate_town_geojson --output-dir /tmp/geojson
"""

import json
import logging
from pathlib import Path
from typing import Optional

from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Pre-generate GeoJSON files for Massachusetts towns to improve map loading performance"

    def add_arguments(self, parser):
        parser.add_argument(
            '--towns',
            type=str,
            help='Comma-separated list of town IDs to process (default: all towns)',
        )
        parser.add_argument(
            '--output-dir',
            type=str,
            default='static/geojson/towns',
            help='Output directory for GeoJSON files (default: static/geojson/towns)',
        )
        parser.add_argument(
            '--upload-s3',
            action='store_true',
            help='Upload generated files to S3 (requires AWS credentials configured)',
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit number of parcels per town (for testing)',
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Regenerate files even if they already exist',
        )

    def handle(self, *args, **options):
        from leads.services import (
            _get_massgis_town,
            get_massgis_catalog,
        )

        towns_filter = options.get('towns')
        output_dir = Path(options['output_dir'])
        upload_s3 = options.get('upload_s3', False)
        limit = options.get('limit')
        force = options.get('force', False)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        self.stdout.write(f"Output directory: {output_dir.absolute()}")

        # Determine which towns to process
        if towns_filter:
            town_ids = [int(tid.strip()) for tid in towns_filter.split(',')]
            self.stdout.write(f"Processing {len(town_ids)} specific towns: {town_ids}")
        else:
            massgis_catalog = get_massgis_catalog()
            town_ids = sorted(massgis_catalog.keys())
            self.stdout.write(f"Processing all {len(town_ids)} Massachusetts towns")

        # Stats
        success_count = 0
        skip_count = 0
        error_count = 0

        for town_id in town_ids:
            try:
                town = _get_massgis_town(town_id)
                safe_name = town.name.replace(' ', '_').replace('/', '_')
                output_file = output_dir / f"town_{town_id}_{safe_name}.geojson"

                # Skip if already exists and not forcing
                if output_file.exists() and not force:
                    self.stdout.write(self.style.WARNING(f"⏭️  Skipping {town.name} (ID: {town_id}) - file exists"))
                    skip_count += 1
                    continue

                self.stdout.write(f"📍 Processing {town.name} (ID: {town_id})...")

                parcels = self._load_parcels_for_town(town_id, limit)

                if not parcels:
                    self.stdout.write(self.style.WARNING(f"⚠️  No parcels returned for {town.name}"))
                    continue

                features = []
                for parcel in parcels:
                    geometry_latlng = parcel.get('geometry') or []
                    if not geometry_latlng:
                        continue

                    # Convert Leaflet-friendly [lat, lng] pairs back to GeoJSON [lng, lat]
                    coordinates = [[
                        [lng, lat] for lat, lng in geometry_latlng
                    ]]

                    if coordinates[0] and coordinates[0][0] != coordinates[0][-1]:
                        coordinates[0].append(list(coordinates[0][0]))

                    properties = dict(parcel)
                    properties.pop('geometry', None)

                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": coordinates,
                        },
                        "properties": properties,
                    }
                    features.append(feature)

                parcel_count = len(features)
                if parcel_count == 0:
                    self.stdout.write(self.style.WARNING(f"⚠️  No valid parcel geometries for {town.name}"))
                    continue

                geojson = {
                    "type": "FeatureCollection",
                    "features": features,
                    "metadata": {
                        "town_id": town_id,
                        "town_name": town.name,
                        "parcel_count": parcel_count,
                        "generated_by": "generate_town_geojson management command",
                    },
                }

                # Write to file
                with open(output_file, 'w') as f:
                    json.dump(geojson, f, separators=(',', ':'))  # Compact JSON

                file_size_mb = output_file.stat().st_size / 1024 / 1024
                self.stdout.write(
                    self.style.SUCCESS(
                        f"✅ {town.name}: {parcel_count} parcels, {file_size_mb:.2f} MB"
                    )
                )

                success_count += 1

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"❌ Error processing town {town_id}: {e}")
                )
                logger.exception(f"Error generating GeoJSON for town {town_id}")
                error_count += 1
                continue

        # Summary
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.SUCCESS(f"✅ Successfully generated: {success_count} towns"))
        if skip_count > 0:
            self.stdout.write(self.style.WARNING(f"⏭️  Skipped (already exist): {skip_count} towns"))
        if error_count > 0:
            self.stdout.write(self.style.ERROR(f"❌ Failed: {error_count} towns"))
        self.stdout.write("=" * 60)

        # Upload to S3 if requested
        if upload_s3:
            self.stdout.write("\n📤 Uploading to S3...")
            uploaded = self._upload_to_s3(output_dir)
            self.stdout.write(self.style.SUCCESS(f"✅ Uploaded {uploaded} files to S3"))

    def _load_parcels_for_town(self, town_id: int, limit: Optional[int]) -> list[dict]:
        """Load all parcel records for a town using the same logic as the API."""
        import shapefile
        from collections import defaultdict

        from leads.services import (
            _clean_string,
            _classify_use_code,
            _compose_owner_address,
            _ensure_massgis_dataset,
            _find_taxpar_shapefile,
            _format_address,
            _get_massgis_town,
            _get_use_description,
            _is_absentee,
            _load_assess_records,
            _load_usecode_lookup,
            _should_replace_assess_record,
            _summarize_unit_records,
            calculate_equity_metrics,
            massgis_stateplane_to_wgs84,
        )

        town = _get_massgis_town(town_id)
        dataset_dir = Path(_ensure_massgis_dataset(town))
        tax_par_path = _find_taxpar_shapefile(dataset_dir)

        sf = shapefile.Reader(str(tax_par_path))
        field_names = [field[0] for field in sf.fields[1:]]

        assess_records = _load_assess_records(str(dataset_dir))
        usecode_lookup = _load_usecode_lookup(str(dataset_dir))

        assess_index: dict[str, dict] = {}
        unit_records_map: dict[str, list] = defaultdict(list)
        for record in assess_records:
            for key_name in ("LOC_ID", "MAP_PAR_ID", "PID", "GIS_ID"):
                key_value = _clean_string(record.get(key_name))
                if not key_value:
                    continue
                unit_records_map[key_value].append(record)
                existing = assess_index.get(key_value)
                if existing is None or _should_replace_assess_record(record, existing):
                    assess_index[key_value] = record

        parcels: list[dict] = []

        for shape_record in sf.shapeRecords():
            if limit is not None and len(parcels) >= limit:
                break

            shape = shape_record.shape
            if not shape.points:
                continue

            attributes = dict(zip(field_names, shape_record.record))

            assess_data = None
            unit_records = None
            lookup_keys = [
                _clean_string(attributes.get("LOC_ID")),
                _clean_string(attributes.get("MAP_PAR_ID")),
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

            x_coords = [p[0] for p in shape.points]
            y_coords = [p[1] for p in shape.points]
            centroid_x = sum(x_coords) / len(x_coords)
            centroid_y = sum(y_coords) / len(y_coords)
            lng, lat = massgis_stateplane_to_wgs84(centroid_x, centroid_y)

            site_addr = _clean_string(attributes.get("SITE_ADDR")) or _clean_string(attributes.get("LOC_ADDR"))
            if not site_addr:
                fallback_source = (
                    _clean_string(attributes.get("MAP_PAR_ID"))
                    or _clean_string(attributes.get("LOC_ID"))
                )
                if fallback_source:
                    site_addr = f"Parcel {fallback_source}"
                    attributes["SITE_ADDR"] = site_addr
                else:
                    continue

            if not attributes.get("SITE_CITY"):
                attributes["SITE_CITY"] = town.name.title()

            polygon_coords = []
            for point in shape.points:
                point_lng, point_lat = massgis_stateplane_to_wgs84(point[0], point[1])
                polygon_coords.append([point_lat, point_lng])

            use_code = attributes.get("USE_CODE", "")
            use_desc = _get_use_description(use_code, usecode_lookup)
            property_category = _classify_use_code(use_code)
            is_absentee = _is_absentee(attributes)
            equity_percent, _, _, _, _, _ = calculate_equity_metrics(attributes)

            parcel = {
                "loc_id": attributes.get("LOC_ID", ""),
                "town_id": town_id,
                "town_name": town.name,
                "address": _format_address(attributes),
                "owner": attributes.get("OWNER1") or attributes.get("OWNER_NAME", "Unknown"),
                "owner_address": _compose_owner_address(attributes),
                "total_value": attributes.get("TOTAL_VAL"),
                "land_value": attributes.get("LAND_VAL"),
                "building_value": attributes.get("BLDG_VAL"),
                "property_type": use_desc,
                "property_category": property_category,
                "use_code": use_code,
                "use_description": use_desc,
                "style": _clean_string(attributes.get("STYLE")),
                "year_built": attributes.get("YEAR_BUILT"),
                "units": attributes.get("UNITS"),
                "lot_size": attributes.get("LOT_SIZE"),
                "lot_units": _clean_string(attributes.get("LOT_UNITS")),
                "zoning": _clean_string(attributes.get("ZONING")),
                "zone": _clean_string(attributes.get("ZONE")),
                "absentee": is_absentee,
                "equity_percent": equity_percent,
                "last_sale_price": attributes.get("LS_PRICE"),
                "last_sale_date": _clean_string(attributes.get("LS_DATE")),
                "site_city": _clean_string(attributes.get("SITE_CITY")) or _clean_string(attributes.get("CITY")),
                "site_zip": _clean_string(attributes.get("SITE_ZIP")) or _clean_string(attributes.get("ZIP")),
                "city": _clean_string(attributes.get("SITE_CITY")) or _clean_string(attributes.get("CITY")) or town.name.title(),
                "zip": _clean_string(attributes.get("SITE_ZIP")) or _clean_string(attributes.get("ZIP")),
                "value_display": None,
                "centroid": {"lat": lat, "lng": lng},
                "geometry": polygon_coords,
                "units_detail": _summarize_unit_records(unit_records) if unit_records else None,
            }

            total_value = parcel.get("total_value")
            if total_value:
                parcel["value_display"] = f"${float(total_value):,.0f}"

            parcels.append(parcel)

        return parcels

    def _upload_to_s3(self, output_dir: Path) -> int:
        """Upload generated GeoJSON files to S3"""
        if not settings.USE_S3:
            self.stdout.write(self.style.ERROR("S3 not configured. Set AWS credentials in environment."))
            return 0

        try:
            import boto3
            from botocore.exceptions import ClientError

            s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region_name=settings.AWS_S3_REGION_NAME,
            )

            bucket = settings.AWS_STORAGE_BUCKET_NAME
            uploaded = 0

            for geojson_file in output_dir.glob("*.geojson"):
                s3_key = f"geojson/towns/{geojson_file.name}"

                try:
                    s3_client.upload_file(
                        str(geojson_file),
                        bucket,
                        s3_key,
                        ExtraArgs={
                            'ContentType': 'application/geo+json',
                            'CacheControl': 'public, max-age=31536000',  # 1 year cache
                        }
                    )
                    self.stdout.write(f"  ✅ Uploaded: {s3_key}")
                    uploaded += 1
                except ClientError as e:
                    self.stdout.write(self.style.ERROR(f"  ❌ Failed to upload {geojson_file.name}: {e}"))
                    continue

            return uploaded

        except ImportError:
            self.stdout.write(self.style.ERROR("boto3 not installed. Run: pip install boto3"))
            return 0
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error uploading to S3: {e}"))
            logger.exception("S3 upload error")
            return 0
