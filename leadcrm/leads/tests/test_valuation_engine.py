from datetime import datetime, timedelta

from django.test import SimpleTestCase

from leads.valuation_engine import ParcelValuationEngine


def _sale_date(days_ago: int) -> str:
    return (datetime.utcnow() - timedelta(days=days_ago)).strftime("%Y-%m-%d")


class ParcelValuationEngineTests(SimpleTestCase):
    def setUp(self) -> None:
        self.engine = ParcelValuationEngine(lookback_days=365, target_comp_count=3)

    def test_hybrid_model_generates_market_value(self) -> None:
        records = [
            {
                "LOC_ID": "PARCEL001",
                "TOTAL_VAL": 320000,
                "LOT_SIZE": 6000,
                "BLD_AREA": 1600,
                "USE_CODE": "101",
                "STYLE": "Colonial",
                "LS_PRICE": 350000,
                "LS_DATE": _sale_date(60),
            },
            {
                "LOC_ID": "PARCEL002",
                "TOTAL_VAL": 300000,
                "LOT_SIZE": 5800,
                "BLD_AREA": 1500,
                "USE_CODE": "101",
                "STYLE": "Colonial",
                "LS_PRICE": 365000,
                "LS_DATE": _sale_date(120),
            },
            {
                "LOC_ID": "PARCEL003",
                "TOTAL_VAL": 295000,
                "LOT_SIZE": 6200,
                "BLD_AREA": 1550,
                "USE_CODE": "101",
                "STYLE": "Cape",
                "LS_PRICE": 340000,
                "LS_DATE": _sale_date(200),
            },
        ]

        clean = self.engine.build_clean_records(records)
        valuations, model, stats = self.engine.compute(clean)

        self.assertIsNotNone(model)
        self.assertIsNotNone(stats.global_psf)
        subject = next((val for val in valuations if val.loc_id == "PARCEL001"), None)
        self.assertIsNotNone(subject)
        self.assertIsNotNone(subject.market_value)
        self.assertGreater(subject.comparable_count, 0)
        self.assertIsNotNone(subject.hedonic_value)
        self.assertGreater(subject.confidence or 0, 0)
