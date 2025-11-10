"""
Management command to test various ATTOM API endpoints for propensity to default score.
Usage: python manage.py test_propensity <town_id> <loc_id>
"""
import json
import urllib.request
import urllib.parse
import urllib.error
from django.core.management.base import BaseCommand
from django.conf import settings
from leads.services import get_massgis_parcel_detail


class Command(BaseCommand):
    help = 'Test various ATTOM API endpoints to find propensity to default score'

    def add_arguments(self, parser):
        parser.add_argument('town_id', type=int, help='MassGIS Town ID')
        parser.add_argument('loc_id', type=str, help='Parcel Location ID')

    def handle(self, *args, **options):
        town_id = options['town_id']
        loc_id = options['loc_id']

        api_key = settings.ATTOM_API_KEY
        if not api_key:
            self.stdout.write(self.style.ERROR("ATTOM_API_KEY not found in settings"))
            return

        self.stdout.write(f"\nTesting ATTOM API endpoints for propensity score")
        self.stdout.write(f"Parcel: {town_id} / {loc_id}\n")
        self.stdout.write("="*80)

        try:
            # Get parcel details from MassGIS
            parcel = get_massgis_parcel_detail(town_id, loc_id)

            if not parcel.site_address or not parcel.site_city:
                self.stdout.write(self.style.ERROR("\nERROR: Parcel is missing address information"))
                return

            address1 = parcel.site_address
            address2 = f"{parcel.site_city}, {parcel.site_zip}" if parcel.site_zip else parcel.site_city

            self.stdout.write(f"\nAddress:")
            self.stdout.write(f"  {address1}")
            self.stdout.write(f"  {address2}\n")

            # List of endpoints to try
            endpoints_to_test = [
                # Property endpoints with different variations
                "/property/detail",
                "/property/expandedprofile",
                "/property/basicprofile",
                "/property/snapshot",

                # AVM/Risk endpoints
                "/avm/detail",
                "/avm/homeequity",

                # Assessment endpoints
                "/assessment/detail",
                "/assessment/snapshot",

                # Sales endpoints
                "/saleshistory/detail",
                "/sale/detail",

                # Direct risk/propensity attempts
                "/property/propensity",
                "/property/risk",
                "/propensity/detail",
                "/risk/detail",
                "/default/propensity",
            ]

            results = []

            for endpoint in endpoints_to_test:
                self.stdout.write(f"\nTesting: {endpoint}")
                self.stdout.write("-" * 80)

                result = self._test_endpoint(endpoint, address1, address2, api_key)
                results.append({
                    "endpoint": endpoint,
                    "success": result["success"],
                    "has_propensity": result["has_propensity"],
                    "status_code": result["status_code"],
                    "message": result["message"]
                })

                if result["success"]:
                    self.stdout.write(self.style.SUCCESS(f"  ✓ {result['message']}"))

                    if result["has_propensity"]:
                        self.stdout.write(self.style.SUCCESS(f"  🎯 PROPENSITY DATA FOUND!"))
                        self.stdout.write(f"  Response preview: {json.dumps(result['data'], indent=2)[:500]}")

                        # Save full response
                        output_file = f"/tmp/attom_propensity_{endpoint.replace('/', '_')}.json"
                        with open(output_file, 'w') as f:
                            json.dump(result['full_response'], f, indent=2, default=str)
                        self.stdout.write(f"  Full response saved to: {output_file}")
                    else:
                        # Check what keys are available
                        if result.get("keys"):
                            self.stdout.write(f"  Available keys: {', '.join(result['keys'][:10])}")
                else:
                    self.stdout.write(self.style.WARNING(f"  ✗ {result['message']}"))

            # Summary
            self.stdout.write("\n" + "="*80)
            self.stdout.write("\nSUMMARY:")
            self.stdout.write(f"  Total endpoints tested: {len(results)}")
            self.stdout.write(f"  Successful responses: {sum(1 for r in results if r['success'])}")
            self.stdout.write(f"  Found propensity data: {sum(1 for r in results if r['has_propensity'])}")

            if any(r['has_propensity'] for r in results):
                self.stdout.write(self.style.SUCCESS("\n✓ Propensity data is available!"))
                self.stdout.write("\nEndpoints with propensity data:")
                for r in results:
                    if r['has_propensity']:
                        self.stdout.write(f"  - {r['endpoint']}")
            else:
                self.stdout.write(self.style.WARNING("\n✗ No propensity data found in any endpoint"))
                self.stdout.write("\nThis may mean:")
                self.stdout.write("  1. Propensity score requires a premium ATTOM subscription")
                self.stdout.write("  2. It's only available via bulk data/ATTOM Cloud")
                self.stdout.write("  3. It requires special query parameters we haven't tried")

            self.stdout.write("="*80 + "\n")

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\nERROR: {type(e).__name__}: {e}"))
            import traceback
            traceback.print_exc()

    def _test_endpoint(self, endpoint, address1, address2, api_key):
        """Test a single ATTOM endpoint and check for propensity data."""
        base_url = "https://api.gateway.attomdata.com"

        # Build URL with query parameters
        params = urllib.parse.urlencode({
            "address1": address1,
            "address2": address2,
        })
        url = f"{base_url}{endpoint}?{params}"

        # Create request with headers
        req = urllib.request.Request(url)
        req.add_header("accept", "application/json")
        req.add_header("apikey", api_key)

        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                status_code = response.status
                body = response.read().decode('utf-8')
                data = json.loads(body)

                # Check for propensity data in various places
                has_propensity = False
                propensity_data = None
                keys = []

                if data.get("property"):
                    prop = data["property"][0] if isinstance(data["property"], list) else data["property"]
                    keys = list(prop.keys()) if isinstance(prop, dict) else []

                    # Search for propensity in various keys
                    for key in ["propensity", "propensityToDefault", "defaultRisk", "risk", "score"]:
                        if key in prop:
                            has_propensity = True
                            propensity_data = prop[key]
                            break

                    # Also check in nested structures
                    if not has_propensity:
                        json_str = json.dumps(prop).lower()
                        if "propensity" in json_str or "defaultrisk" in json_str:
                            has_propensity = True
                            propensity_data = "Found in nested data (see full response)"

                return {
                    "success": True,
                    "has_propensity": has_propensity,
                    "status_code": status_code,
                    "message": f"Response received ({len(data.get('property', []))} properties)",
                    "data": propensity_data,
                    "full_response": data,
                    "keys": keys
                }

        except urllib.error.HTTPError as e:
            status_code = e.code
            if status_code == 404:
                return {
                    "success": False,
                    "has_propensity": False,
                    "status_code": status_code,
                    "message": "Endpoint not found (404)"
                }
            elif status_code == 401:
                return {
                    "success": False,
                    "has_propensity": False,
                    "status_code": status_code,
                    "message": "Unauthorized - API key may not have access (401)"
                }
            else:
                error_body = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
                return {
                    "success": False,
                    "has_propensity": False,
                    "status_code": status_code,
                    "message": f"HTTP {status_code}: {error_body[:100]}"
                }
        except urllib.error.URLError as e:
            return {
                "success": False,
                "has_propensity": False,
                "status_code": None,
                "message": f"URL Error: {str(e)[:100]}"
            }
        except Exception as e:
            return {
                "success": False,
                "has_propensity": False,
                "status_code": None,
                "message": f"Error: {type(e).__name__}: {str(e)[:100]}"
            }
