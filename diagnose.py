#!/usr/bin/env python
"""Standalone diagnostic script for ParcelMarketValue data."""

import os
import sys
import django

# Setup Django
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'leadcrm'))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leadcrm.settings')
django.setup()

from django.db.models import Count, Avg, Max, Q
from leads.models import ParcelMarketValue
from leads.services import get_massgis_catalog

print("=" * 80)
print("PARCEL MARKET VALUE DIAGNOSTIC REPORT")
print("=" * 80)
print()

# 1. Overall statistics
total = ParcelMarketValue.objects.count()
print(f"📊 OVERALL STATISTICS")
print(f"   Total ParcelMarketValue records: {total:,}")

if total == 0:
    print()
    print("❌ CRITICAL: No ParcelMarketValue records found!")
    print("   This means the compute_market_values command did not create any records.")
    print("   The comp computation may have failed or not been run.")
    print()
    sys.exit(1)

# 2. Comp distribution
with_comps = ParcelMarketValue.objects.filter(comparable_count__gt=0).count()
without_comps = ParcelMarketValue.objects.filter(comparable_count=0).count()

print()
print(f"🔍 COMP DISTRIBUTION")
print(f"   Parcels WITH comps: {with_comps:,} ({with_comps/total*100:.1f}%)")
print(f"   Parcels WITHOUT comps: {without_comps:,} ({without_comps/total*100:.1f}%)")

# 3. Average comp statistics
avg_stats = ParcelMarketValue.objects.aggregate(
    avg_comps=Avg('comparable_count'),
    max_comps=Max('comparable_count'),
    avg_market_value=Avg('market_value'),
    avg_confidence=Avg('valuation_confidence')
)

print()
print(f"📈 VALUE STATISTICS")
print(f"   Average comps per parcel: {avg_stats['avg_comps']:.2f}")
print(f"   Maximum comps found: {avg_stats['max_comps']}")
if avg_stats['avg_market_value']:
    print(f"   Average market value: ${avg_stats['avg_market_value']:,.0f}")
if avg_stats['avg_confidence']:
    print(f"   Average confidence: {avg_stats['avg_confidence']*100:.1f}%")

# 4. Town-level breakdown
print()
print(f"🏘️  TOWN-LEVEL BREAKDOWN (Top 15 by record count)")

catalog = get_massgis_catalog()
town_stats = ParcelMarketValue.objects.values('town_id').annotate(
    total_parcels=Count('id'),
    parcels_with_comps=Count('id', filter=Q(comparable_count__gt=0)),
    avg_comps=Avg('comparable_count'),
    avg_value=Avg('market_value')
).order_by('-total_parcels')[:15]

for stat in town_stats:
    town_name = catalog.get(stat['town_id']).name if stat['town_id'] in catalog else f"Town {stat['town_id']}"
    pct_with_comps = stat['parcels_with_comps'] / stat['total_parcels'] * 100 if stat['total_parcels'] > 0 else 0
    avg_val_str = f"${stat['avg_value']:,.0f}" if stat['avg_value'] else "N/A"
    print(
        f"   {town_name:20s}: {stat['total_parcels']:5,} parcels | "
        f"{stat['parcels_with_comps']:5,} ({pct_with_comps:5.1f}%) with comps | "
        f"avg {stat['avg_comps']:.1f} comps | avg value {avg_val_str}"
    )

# 5. Sample records WITH comps
print()
print(f"✅ SAMPLE RECORDS WITH COMPS (up to 5)")
for p in ParcelMarketValue.objects.filter(comparable_count__gt=0).order_by('-comparable_count')[:5]:
    town_name = catalog.get(p.town_id).name if p.town_id in catalog else f"Town {p.town_id}"
    psf_str = f"${p.market_value_per_sqft:,.0f}/sqft" if p.market_value_per_sqft else "$/sqft N/A"
    conf_str = f"{p.valuation_confidence*100:.0f}%" if p.valuation_confidence else "N/A"
    print(
        f"   {town_name}/{p.loc_id}: ${p.market_value:,.0f} | "
        f"{p.comparable_count} comps | confidence {conf_str} | {psf_str}"
    )

# 6. Sample records WITHOUT comps
print()
print(f"❌ SAMPLE RECORDS WITHOUT COMPS (up to 5)")
for p in ParcelMarketValue.objects.filter(comparable_count=0)[:5]:
    town_name = catalog.get(p.town_id).name if p.town_id in catalog else f"Town {p.town_id}"
    hedonic_note = f" (hedonic: ${p.hedonic_value:,.0f})" if p.hedonic_value else ""
    conf_str = f"{p.valuation_confidence*100:.0f}%" if p.valuation_confidence else "N/A"
    print(
        f"   {town_name}/{p.loc_id}: ${p.market_value:,.0f}{hedonic_note} | "
        f"0 comps | confidence {conf_str}"
    )

# 7. Recent updates
print()
print(f"🕐 RECENT UPDATES")
latest = ParcelMarketValue.objects.order_by('-valued_at').first()
oldest = ParcelMarketValue.objects.order_by('valued_at').first()
if latest and oldest:
    print(f"   Most recent valuation: {latest.valued_at}")
    print(f"   Oldest valuation: {oldest.valued_at}")
    print(f"   Model version: {latest.model_version}")

# 8. Issues and recommendations
print()
print("=" * 80)
print("ISSUES & RECOMMENDATIONS")
print("=" * 80)

if without_comps / total > 0.5:
    print(f"⚠️  HIGH: {without_comps/total*100:.1f}% of parcels have NO comps")
    print("   → This is expected for small towns with few sales")
    print("   → Consider implementing cross-town comp matching")
    print()

if avg_stats['avg_comps'] < 3:
    print(f"⚠️  MEDIUM: Average comps per parcel is only {avg_stats['avg_comps']:.1f}")
    print("   → Target is 5 comps per parcel")
    print("   → Increase lookback_days or allow cross-town matching")
    print()

print("✅ Diagnostic complete!")
print()
