#!/usr/bin/env python
"""
Quick test script for Newburyport scraping.
Run this in Railway shell: python test_newburyport.py
"""
import os
import sys
import django

# Setup Django
sys.path.insert(0, '/app/leadcrm')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leadcrm.settings')
django.setup()

from data_pipeline.jobs.task_queue import run_registry_task
from data_pipeline.town_registry_map import get_registry_for_town

def test_newburyport():
    """Test scraping a single Newburyport parcel."""

    town_id = 184  # Newburyport
    loc_id = '0141000000100000'  # Sample parcel

    print(f"🧪 Testing scraper on Newburyport (town_id={town_id})")
    print(f"📍 Parcel LOC_ID: {loc_id}")
    print("-" * 60)

    # Get registry for this town
    registry_id = get_registry_for_town(town_id)
    if not registry_id:
        print(f"❌ No registry mapping found for town {town_id}")
        return

    print(f"📚 Registry: {registry_id}")
    print(f"🔄 Queueing scraping task...")

    # Queue async task
    task = run_registry_task.delay(
        config={'registry_id': registry_id},
        loc_id=f"{town_id}-{loc_id}",
        force_refresh=True
    )

    print(f"✅ Task queued: {task.id}")
    print(f"⏳ Waiting for task to complete...")

    # Wait for result (timeout after 60 seconds)
    try:
        result = task.get(timeout=60)
        print(f"✅ Task completed successfully!")
        print(f"📊 Result: {result}")
    except Exception as e:
        print(f"❌ Task failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_newburyport()
