#!/usr/bin/env python3
"""
Cleanup script to remove generic placeholder lien and legal action records.
These were created by mistake and contain no real data - just manual lookup guidance.
"""

import os
import sys
import django

# Setup Django
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leadcrm.settings')
django.setup()

from leads.models import LienRecord, LegalAction

def cleanup_placeholder_records():
    """Remove generic placeholder records that aren't real liens/actions"""

    print("Cleaning up placeholder lien and legal action records...")

    # Delete lien records that are generic guidance (contain "Manual Lookup" or have no real data)
    placeholder_liens = LienRecord.objects.filter(
        source__icontains="Manual Lookup"
    ) | LienRecord.objects.filter(
        notes__icontains="Check these sources"
    ) | LienRecord.objects.filter(
        lien_holder="Unknown"
    ) | LienRecord.objects.filter(
        lien_holder__icontains="Check source"
    )

    lien_count = placeholder_liens.count()
    print(f"Found {lien_count} placeholder lien records")

    if lien_count > 0:
        placeholder_liens.delete()
        print(f"Deleted {lien_count} placeholder lien records")

    # Delete legal action records that are generic guidance (case_number = "MANUAL_LOOKUP")
    placeholder_actions = LegalAction.objects.filter(
        case_number="MANUAL_LOOKUP"
    )

    action_count = placeholder_actions.count()
    print(f"Found {action_count} placeholder legal action records")

    if action_count > 0:
        placeholder_actions.delete()
        print(f"Deleted {action_count} placeholder legal action records")

    # Show remaining counts
    remaining_liens = LienRecord.objects.count()
    remaining_actions = LegalAction.objects.count()

    print(f"\nRemaining records:")
    print(f"  - Liens: {remaining_liens}")
    print(f"  - Legal Actions: {remaining_actions}")

    print("\nCleanup complete!")

if __name__ == "__main__":
    cleanup_placeholder_records()
