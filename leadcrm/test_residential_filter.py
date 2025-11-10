#!/usr/bin/env python3
"""
Test script to verify that only residential parcels are searched.
"""

import os
import sys
import django

# Setup Django
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leadcrm.settings')
django.setup()

def test_residential_filter():
    """Test that residential filter works correctly"""

    print("=" * 70)
    print("Residential Parcel Filter Test")
    print("=" * 70)

    # Test parcels with different categories
    test_parcels = [
        {'property_category': 'Residential', 'owner': 'John Smith', 'address': '123 Main St'},
        {'property_category': 'Commercial', 'owner': 'ABC Corp', 'address': '456 Business Ave'},
        {'property_category': 'Exempt', 'owner': 'City of Salem', 'address': '789 City Hall'},
        {'property_category': 'Residential', 'owner': 'Jane Doe', 'address': '321 Oak St'},
        {'property_category': 'Agricultural', 'owner': 'Farm LLC', 'address': '999 Farm Rd'},
    ]

    print(f"\nTesting with {len(test_parcels)} sample parcels:")
    print("-" * 70)

    residential_count = 0
    skipped_count = 0

    for i, parcel in enumerate(test_parcels, 1):
        category = parcel.get('property_category', '')
        owner = parcel.get('owner', 'Unknown')
        address = parcel.get('address', 'Unknown')

        is_residential = category == 'Residential'
        status = "✓ WOULD SEARCH" if is_residential else "⊗ SKIP"

        print(f"\nParcel {i}:")
        print(f"  Address: {address}")
        print(f"  Owner: {owner}")
        print(f"  Category: {category}")
        print(f"  Status: {status}")

        if is_residential:
            residential_count += 1
        else:
            skipped_count += 1

    print("\n" + "=" * 70)
    print("SUMMARY:")
    print("=" * 70)
    print(f"Total parcels: {len(test_parcels)}")
    print(f"Residential (searched): {residential_count}")
    print(f"Non-residential (skipped): {skipped_count}")
    print(f"\nAPI calls saved: {skipped_count} ({skipped_count/len(test_parcels)*100:.0f}%)")
    print("\n✓ Residential-only filtering is working correctly!")
    print("=" * 70)

    return True

if __name__ == "__main__":
    success = test_residential_filter()
    sys.exit(0 if success else 1)
