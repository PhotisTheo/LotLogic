#!/usr/bin/env python3
"""
Test script to simulate fetching liens/legal actions for a parcel.
This mimics what happens when you view a parcel detail page.
"""

import os
import sys
import django

# Setup Django
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leadcrm.settings')
django.setup()

from leads.lien_legal_service import search_all_sources
from leads.models import LienRecord, LegalAction
from django.contrib.auth import get_user_model

User = get_user_model()

def test_parcel_lien_search():
    """Test searching for liens/legal actions for a sample parcel"""

    print("=" * 70)
    print("Parcel Lien/Legal Action Search Test")
    print("=" * 70)

    # Test data - simulating a parcel owner
    test_owner = "Smith Properties LLC"
    test_address = "123 Main Street"
    test_town = "Salem"
    test_county = "Essex"

    print(f"\nSearching for liens and legal actions:")
    print(f"  Owner: {test_owner}")
    print(f"  Address: {test_address}")
    print(f"  Town: {test_town}")
    print(f"  County: {test_county}")
    print("-" * 70)

    try:
        # Search all sources
        legal_actions, liens = search_all_sources(
            owner_name=test_owner,
            address=test_address,
            town_name=test_town,
            county=test_county
        )

        print(f"\n📋 Search Results:")
        print(f"  Legal Actions: {len(legal_actions)}")
        print(f"  Liens: {len(liens)}")
        print()

        # Show legal actions
        if legal_actions:
            print("=" * 70)
            print("LEGAL ACTIONS FOUND:")
            print("=" * 70)

            real_actions = [a for a in legal_actions if a.get('case_number') != 'MANUAL_LOOKUP']
            manual_actions = [a for a in legal_actions if a.get('case_number') == 'MANUAL_LOOKUP']

            if real_actions:
                print(f"\n✓ {len(real_actions)} real case(s) from CourtListener:")
                for i, action in enumerate(real_actions, 1):
                    print(f"\n  Case {i}:")
                    print(f"    Case Number: {action.get('case_number')}")
                    print(f"    Court: {action.get('court')}")
                    print(f"    Type: {action.get('action_type')}")
                    print(f"    Status: {action.get('status')}")
                    print(f"    Filed: {action.get('filing_date')}")
                    print(f"    Description: {action.get('description', '')[:60]}...")
                    print(f"    Would be saved: YES ✓")

            if manual_actions:
                print(f"\n⚠️  {len(manual_actions)} manual lookup guide(s):")
                for action in manual_actions:
                    print(f"    - {action.get('source')}")
                    print(f"      Would be saved: NO (filtered out)")
        else:
            print("No legal actions found.")

        # Show liens
        if liens:
            print("\n" + "=" * 70)
            print("LIENS FOUND:")
            print("=" * 70)

            # Check which would actually be saved
            real_liens = []
            manual_liens = []

            for lien in liens:
                source = lien.get('source', '')
                notes = lien.get('notes', '')
                has_data = bool(lien.get('recording_date') or lien.get('amount') or lien.get('book_number'))

                if "Manual Lookup" in source or "Check these sources" in notes:
                    manual_liens.append(lien)
                elif has_data:
                    real_liens.append(lien)
                else:
                    manual_liens.append(lien)

            if real_liens:
                print(f"\n✓ {len(real_liens)} real lien(s) with data:")
                for i, lien in enumerate(real_liens, 1):
                    print(f"\n  Lien {i}:")
                    print(f"    Type: {lien.get('lien_type')}")
                    print(f"    Holder: {lien.get('lien_holder')}")
                    print(f"    Amount: ${lien.get('amount', 'N/A')}")
                    print(f"    Date: {lien.get('recording_date', 'N/A')}")
                    print(f"    Source: {lien.get('source')}")
                    print(f"    Would be saved: YES ✓")

            if manual_liens:
                print(f"\n⚠️  {len(manual_liens)} manual lookup guide(s):")
                for lien in manual_liens:
                    print(f"    - {lien.get('source')}")
                    print(f"      Would be saved: NO (filtered out)")
        else:
            print("\nNo liens found.")

        print("\n" + "=" * 70)
        print("SUMMARY:")
        print("=" * 70)

        real_action_count = len([a for a in legal_actions if a.get('case_number') != 'MANUAL_LOOKUP'])
        real_lien_count = len([l for l in liens if not ("Manual Lookup" in l.get('source', '') or "Check these sources" in l.get('notes', ''))])

        print(f"✓ Records that would be saved to database:")
        print(f"    Legal Actions: {real_action_count}")
        print(f"    Liens: {real_lien_count}")
        print(f"\n✓ Manual lookup guides (not saved): {len(legal_actions) - real_action_count + len(liens) - real_lien_count}")
        print("\n✓ Test completed successfully!")
        print("=" * 70)

        return True

    except Exception as e:
        print(f"\n❌ ERROR during search:")
        print(f"   {str(e)}")
        print("\nFull error details:")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_parcel_lien_search()
    sys.exit(0 if success else 1)
