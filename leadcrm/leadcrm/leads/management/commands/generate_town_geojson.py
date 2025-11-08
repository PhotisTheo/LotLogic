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

from django.core.management.base import BaseCommand
from django.conf import settings

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
            _ensure_massgis_dataset,
            _find_taxpar_shapefile,
            massgis_stateplane_to_wgs84,
            MASSGIS_TOWNS,
        )
        import shapefile

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
            town_ids = sorted(MASSGIS_TOWNS.keys())
            self.stdout.write(f"Processing all {len(town_ids)} Massachusetts towns")

        # Stats
        success_count = 0
        skip_count = 0
        error_count = 0

        for town_id in town_ids:
            try:
                town = _get_massgis_town(town_id)
                output_file = output_dir / f"town_{town_id}_{town.name.replace(' ', '_')}.geojson"

                # Skip if already exists and not forcing
                if output_file.exists() and not force:
                    self.stdout.write(self.style.WARNING(f"⏭️  Skipping {town.name} (ID: {town_id}) - file exists"))
                    skip_count += 1
                    continue

                self.stdout.write(f"📍 Processing {town.name} (ID: {town_id})...")

                # Get dataset and shapefile
                dataset_dir = _ensure_massgis_dataset(town)
                tax_par_path = _find_taxpar_shapefile(Path(dataset_dir))

                # Read shapefile
                sf = shapefile.Reader(str(tax_par_path))
                field_names = [field[0] for field in sf.fields[1:]]

                # Build GeoJSON
                features = []
                parcel_count = 0

                for shape_record in sf.iterShapeRecords():
                    if limit and parcel_count >= limit:
                        break

                    shape = shape_record.shape
                    record = shape_record.record

                    # Skip invalid shapes
                    if not shape or not hasattr(shape, 'points') or not shape.points:
                        continue

                    # Convert coordinates from State Plane to WGS84
                    wgs84_points = []
                    for point in shape.points:
                        try:
                            lng, lat = massgis_stateplane_to_wgs84(point[0], point[1])
                            wgs84_points.append([lng, lat])
                        except Exception as e:
                            logger.warning(f"Error converting coordinates for {town.name}: {e}")
                            continue

                    if not wgs84_points:
                        continue

                    # Close polygon if needed
                    if wgs84_points[0] != wgs84_points[-1]:
                        wgs84_points.append(wgs84_points[0])

                    # Build properties from shapefile attributes
                    properties = {}
                    for i, field_name in enumerate(field_names):
                        try:
                            value = record[i]
                            # Convert to JSON-serializable types
                            if value is None:
                                properties[field_name] = None
                            elif isinstance(value, (str, int, float, bool)):
                                properties[field_name] = value
                            else:
                                properties[field_name] = str(value)
                        except (IndexError, Exception) as e:
                            logger.warning(f"Error reading field {field_name}: {e}")
                            continue

                    # Add town metadata
                    properties['town_id'] = town_id
                    properties['town_name'] = town.name

                    # Create GeoJSON feature
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [wgs84_points]
                        },
                        "properties": properties
                    }

                    features.append(feature)
                    parcel_count += 1

                sf.close()

                # Create GeoJSON FeatureCollection
                geojson = {
                    "type": "FeatureCollection",
                    "features": features,
                    "metadata": {
                        "town_id": town_id,
                        "town_name": town.name,
                        "parcel_count": parcel_count,
                        "generated_by": "generate_town_geojson management command"
                    }
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
