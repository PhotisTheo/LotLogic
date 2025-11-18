"""Diagnostic command to check ParcelMarketValue data and identify issues."""

from django.core.management.base import BaseCommand
from django.db.models import Count, Avg, Max, Min, Q
from ...models import ParcelMarketValue
from ...services import get_massgis_catalog


class Command(BaseCommand):
    help = "Diagnose ParcelMarketValue data to identify why market values aren't showing"

    def handle(self, *args, **options):
        self.stdout.write(self.style.WARNING("=" * 80))
        self.stdout.write(self.style.WARNING("PARCEL MARKET VALUE DIAGNOSTIC REPORT"))
        self.stdout.write(self.style.WARNING("=" * 80))
        self.stdout.write("")

        # 1. Overall statistics
        total = ParcelMarketValue.objects.count()
        self.stdout.write(self.style.NOTICE(f"📊 OVERALL STATISTICS"))
        self.stdout.write(f"   Total ParcelMarketValue records: {total:,}")

        if total == 0:
            self.stdout.write(self.style.ERROR(""))
            self.stdout.write(self.style.ERROR("❌ CRITICAL: No ParcelMarketValue records found!"))
            self.stdout.write(self.style.ERROR("   This means the compute_market_values command did not create any records."))
            self.stdout.write(self.style.ERROR("   The comp computation may have failed or not been run."))
            self.stdout.write("")
            return

        # 2. Comp distribution
        with_comps = ParcelMarketValue.objects.filter(comparable_count__gt=0).count()
        without_comps = ParcelMarketValue.objects.filter(comparable_count=0).count()

        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"🔍 COMP DISTRIBUTION"))
        self.stdout.write(f"   Parcels WITH comps: {with_comps:,} ({with_comps/total*100:.1f}%)")
        self.stdout.write(f"   Parcels WITHOUT comps: {without_comps:,} ({without_comps/total*100:.1f}%)")

        # 3. Average comp statistics
        avg_stats = ParcelMarketValue.objects.aggregate(
            avg_comps=Avg('comparable_count'),
            max_comps=Max('comparable_count'),
            avg_market_value=Avg('market_value'),
            avg_confidence=Avg('valuation_confidence')
        )

        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"📈 VALUE STATISTICS"))
        self.stdout.write(f"   Average comps per parcel: {avg_stats['avg_comps']:.2f}")
        self.stdout.write(f"   Maximum comps found: {avg_stats['max_comps']}")
        self.stdout.write(f"   Average market value: ${avg_stats['avg_market_value']:,.0f}" if avg_stats['avg_market_value'] else "   Average market value: N/A")
        self.stdout.write(f"   Average confidence: {avg_stats['avg_confidence']*100:.1f}%" if avg_stats['avg_confidence'] else "   Average confidence: N/A")

        # 4. Town-level breakdown
        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"🏘️  TOWN-LEVEL BREAKDOWN (Top 20 by record count)"))

        catalog = get_massgis_catalog()
        town_stats = ParcelMarketValue.objects.values('town_id').annotate(
            total_parcels=Count('id'),
            parcels_with_comps=Count('id', filter=Q(comparable_count__gt=0)),
            avg_comps=Avg('comparable_count'),
            avg_value=Avg('market_value')
        ).order_by('-total_parcels')[:20]

        for stat in town_stats:
            town_name = catalog.get(stat['town_id']).name if stat['town_id'] in catalog else f"Town {stat['town_id']}"
            pct_with_comps = stat['parcels_with_comps'] / stat['total_parcels'] * 100 if stat['total_parcels'] > 0 else 0
            self.stdout.write(
                f"   {town_name:20s}: {stat['total_parcels']:5,} parcels | "
                f"{stat['parcels_with_comps']:5,} ({pct_with_comps:5.1f}%) with comps | "
                f"avg {stat['avg_comps']:.1f} comps | "
                f"avg value ${stat['avg_value']:,.0f}" if stat['avg_value'] else f"avg value N/A"
            )

        # 5. Sample records WITH comps
        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"✅ SAMPLE RECORDS WITH COMPS (up to 5)"))
        for p in ParcelMarketValue.objects.filter(comparable_count__gt=0).order_by('-comparable_count')[:5]:
            town_name = catalog.get(p.town_id).name if p.town_id in catalog else f"Town {p.town_id}"
            self.stdout.write(
                f"   {town_name}/{p.loc_id}: ${p.market_value:,.0f} | "
                f"{p.comparable_count} comps | "
                f"confidence {p.valuation_confidence*100:.0f}% | "
                f"${p.market_value_per_sqft:,.0f}/sqft" if p.market_value_per_sqft else "$/sqft N/A"
            )
            # Show comp details from payload
            if p.payload and 'comps' in p.payload:
                comps = p.payload['comps'][:3]  # Show first 3 comps
                for i, comp in enumerate(comps, 1):
                    sale_price = comp.get('sale_price', 'N/A')
                    sale_date = comp.get('sale_date', 'N/A')
                    psf = comp.get('psf', 'N/A')
                    self.stdout.write(f"      Comp {i}: ${sale_price:,.0f} ({sale_date}) @ ${psf}/sqft" if psf != 'N/A' else f"      Comp {i}: ${sale_price:,.0f} ({sale_date})")

        # 6. Sample records WITHOUT comps
        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"❌ SAMPLE RECORDS WITHOUT COMPS (up to 5)"))
        for p in ParcelMarketValue.objects.filter(comparable_count=0)[:5]:
            town_name = catalog.get(p.town_id).name if p.town_id in catalog else f"Town {p.town_id}"
            hedonic_note = ""
            if p.hedonic_value:
                hedonic_note = f" (hedonic: ${p.hedonic_value:,.0f})"
            self.stdout.write(
                f"   {town_name}/{p.loc_id}: ${p.market_value:,.0f}{hedonic_note} | "
                f"0 comps | "
                f"confidence {p.valuation_confidence*100:.0f}%" if p.valuation_confidence else "confidence N/A"
            )

        # 7. Methodology breakdown
        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"🔬 METHODOLOGY BREAKDOWN"))
        methodology_stats = ParcelMarketValue.objects.values('methodology').annotate(
            count=Count('id')
        )
        for stat in methodology_stats:
            self.stdout.write(f"   {stat['methodology']}: {stat['count']:,} parcels")

        # 8. Recent updates
        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"🕐 RECENT UPDATES"))
        latest = ParcelMarketValue.objects.order_by('-valued_at').first()
        oldest = ParcelMarketValue.objects.order_by('valued_at').first()
        if latest and oldest:
            self.stdout.write(f"   Most recent valuation: {latest.valued_at}")
            self.stdout.write(f"   Oldest valuation: {oldest.valued_at}")
            self.stdout.write(f"   Model version: {latest.model_version}")

        # 9. Issues and recommendations
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("=" * 80))
        self.stdout.write(self.style.WARNING("ISSUES & RECOMMENDATIONS"))
        self.stdout.write(self.style.WARNING("=" * 80))

        issues_found = False

        if without_comps / total > 0.5:
            issues_found = True
            self.stdout.write(self.style.ERROR(f"⚠️  HIGH: {without_comps/total*100:.1f}% of parcels have NO comps"))
            self.stdout.write("   → This is expected for small towns with few sales")
            self.stdout.write("   → Consider implementing cross-town comp matching")

        if avg_stats['avg_comps'] < 3:
            issues_found = True
            self.stdout.write(self.style.ERROR(f"⚠️  MEDIUM: Average comps per parcel is only {avg_stats['avg_comps']:.1f}"))
            self.stdout.write("   → Target is 5 comps per parcel")
            self.stdout.write("   → Increase lookback_days or allow cross-town matching")

        if not issues_found:
            self.stdout.write(self.style.SUCCESS("✅ No major issues detected"))
            self.stdout.write("   If market values aren't showing in the UI, the issue may be:")
            self.stdout.write("   1. Cache issue - try clearing browser cache")
            self.stdout.write("   2. Specific parcels you're viewing don't have valuations")
            self.stdout.write("   3. Frontend code not properly rendering the values")

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("Diagnostic complete!"))
