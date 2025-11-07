#!/usr/bin/env python3
"""
Test script for CourtListener API integration.
Tests the party name search functionality.
"""

import os
import sys
import django

# Setup Django
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leadcrm.settings')
django.setup()

from leads.lien_legal_service import search_courtlistener_by_name
from django.conf import settings

def test_courtlistener_api():
    """Test CourtListener API with a sample search"""

    print("=" * 70)
    print("CourtListener API Test")
    print("=" * 70)

    # Check if API key is configured
    api_key = getattr(settings, "COURTLISTENER_API_KEY", None)
    if not api_key:
        print("\n❌ ERROR: COURTLISTENER_API_KEY not configured in settings")
        print("Please add COURTLISTENER_API_KEY to your .env file")
        return False

    print(f"\n✓ API key found: {api_key[:10]}...{api_key[-4:]}")

    # Test with a sample name search
    test_name = "Smith"  # Common name, should return some results

    print(f"\n🔍 Searching for cases involving: '{test_name}'")
    print("Searching MA Bankruptcy Court and MA District Court...")
    print("-" * 70)

    try:
        results = search_courtlistener_by_name(test_name, state="MA", limit=5)

        if not results:
            print("\n⚠️  No results found")
            print("This could mean:")
            print("  - No cases found for this name")
            print("  - API authentication issue")
            print("  - API endpoint or parameters incorrect")
            return False

        print(f"\n✓ Found {len(results)} case(s):\n")

        for i, case in enumerate(results, 1):
            print(f"Case {i}:")
            print(f"  Case Number: {case.get('case_number', 'N/A')}")
            print(f"  Court: {case.get('court', 'N/A')}")
            print(f"  Type: {case.get('action_type', 'N/A')}")
            print(f"  Status: {case.get('status', 'N/A')}")
            print(f"  Filed: {case.get('filing_date', 'N/A')}")
            print(f"  Description: {case.get('description', 'N/A')[:60]}...")
            print(f"  URL: {case.get('source_url', 'N/A')}")
            print()

        print("=" * 70)
        print("✓ CourtListener API integration is working correctly!")
        print("=" * 70)
        return True

    except Exception as e:
        print(f"\n❌ ERROR during API search:")
        print(f"   {str(e)}")
        print("\nFull error details:")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_courtlistener_api()
    sys.exit(0 if success else 1)
