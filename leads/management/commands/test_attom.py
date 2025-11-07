"""
Management command to test ATTOM API and see actual response structure.
Usage: python manage.py test_attom <town_id> <loc_id>
"""
import json
from django.core.management.base import BaseCommand
from leads.services import get_massgis_parcel_detail
from leads.attom_service import fetch_attom_data_for_address


class Command(BaseCommand):
    help = 'Test ATTOM API for a specific parcel and display the response structure'

    def add_arguments(self, parser):
        parser.add_argument('town_id', type=int, help='MassGIS Town ID')
        parser.add_argument('loc_id', type=str, help='Parcel Location ID')

    def handle(self, *args, **options):
        town_id = options['town_id']
        loc_id = options['loc_id']

        self.stdout.write(f"\nTesting ATTOM API for parcel: {town_id} / {loc_id}\n")
        self.stdout.write("="*80)

        try:
            # Get parcel details from MassGIS
            parcel = get_massgis_parcel_detail(town_id, loc_id)

            self.stdout.write(f"\nParcel Address:")
            self.stdout.write(f"  {parcel.site_address}")
            self.stdout.write(f"  {parcel.site_city}, {parcel.site_zip}")

            if not parcel.site_address or not parcel.site_city:
                self.stdout.write(self.style.ERROR("\nERROR: Parcel is missing address information"))
                return

            # Construct address for ATTOM
            address1 = parcel.site_address
            address2 = f"{parcel.site_city}, {parcel.site_zip}" if parcel.site_zip else parcel.site_city

            self.stdout.write(f"\nFetching from ATTOM API...")
            self.stdout.write(f"  Address 1: {address1}")
            self.stdout.write(f"  Address 2: {address2}\n")

            # Fetch from ATTOM
            attom_data = fetch_attom_data_for_address(address1, address2)

            if not attom_data or not attom_data.get("raw_response"):
                self.stdout.write(self.style.ERROR("\nNo data returned from ATTOM API"))
                return

            raw = attom_data.get("raw_response", {})

            self.stdout.write(self.style.SUCCESS("\n✓ ATTOM API Response Received\n"))
            self.stdout.write("="*80)

            # Show top-level structure
            self.stdout.write(f"\nResponse Status: {raw.get('status', {}).get('msg', 'N/A')}")
            self.stdout.write(f"Property Count: {len(raw.get('property', []))}")

            if raw.get('property'):
                prop = raw['property'][0]

                self.stdout.write(f"\n\nTop-level keys in property object:")
                for key in sorted(prop.keys()):
                    value_type = type(prop[key]).__name__
                    self.stdout.write(f"  - {key:30} ({value_type})")

                # Show foreclosure structure
                if 'foreclosure' in prop:
                    self.stdout.write(f"\n\nForeclosure data structure:")
                    fc = prop['foreclosure']
                    if isinstance(fc, dict):
                        for key in sorted(fc.keys()):
                            self.stdout.write(f"  - {key}: {fc[key]}")
                    else:
                        self.stdout.write(f"  Type: {type(fc)}")
                        self.stdout.write(f"  Value: {fc}")

                # Show mortgage structure
                if 'mortgage' in prop:
                    self.stdout.write(f"\n\nMortgage data structure:")
                    mg = prop['mortgage']
                    if isinstance(mg, list):
                        self.stdout.write(f"  Count: {len(mg)} mortgages")
                        if mg:
                            self.stdout.write(f"  First mortgage keys:")
                            for key in sorted(mg[0].keys()):
                                self.stdout.write(f"    - {key}: {mg[0][key]}")
                    else:
                        self.stdout.write(f"  Type: {type(mg)}")

                # Show assessment structure
                if 'assessment' in prop:
                    self.stdout.write(f"\n\nAssessment data structure:")
                    asmt = prop['assessment']
                    if isinstance(asmt, dict):
                        for key in sorted(asmt.keys()):
                            self.stdout.write(f"  - {key}: {asmt[key]}")

                # Show tax structure
                if 'tax' in prop:
                    self.stdout.write(f"\n\nTax data structure:")
                    tax = prop['tax']
                    if isinstance(tax, dict):
                        for key in sorted(tax.keys()):
                            self.stdout.write(f"  - {key}: {tax[key]}")

                # Save full JSON to file for inspection
                output_file = f"/tmp/attom_response_{town_id}_{loc_id}.json"
                with open(output_file, 'w') as f:
                    json.dump(raw, f, indent=2, default=str)

                self.stdout.write(f"\n\n✓ Full response saved to: {output_file}")
                self.stdout.write("="*80 + "\n")

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\nERROR: {type(e).__name__}: {e}"))
            import traceback
            traceback.print_exc()
