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
            {
                "LOC_ID": "PARCEL004",
                "TOTAL_VAL": 280000,
                "LOT_SIZE": 6400,
                "BLD_AREA": 1500,
                "USE_CODE": "101",
                "STYLE": "Colonial",
                "LS_PRICE": 330000,
                "LS_DATE": _sale_date(250),
            },
            {
                "LOC_ID": "PARCEL005",
                "TOTAL_VAL": 360000,
                "LOT_SIZE": 6100,
                "BLD_AREA": 1700,
                "USE_CODE": "101",
                "STYLE": "Colonial",
                "LS_PRICE": 370000,
                "LS_DATE": _sale_date(300),
            },
            {
                "LOC_ID": "PARCEL006",
                "TOTAL_VAL": 305000,
                "LOT_SIZE": 6300,
                "BLD_AREA": 1525,
                "USE_CODE": "101",
                "STYLE": "Ranch",
                "LS_PRICE": 355000,
                "LS_DATE": _sale_date(320),
            },
            {
                "LOC_ID": "PARCEL007",
                "TOTAL_VAL": 315000,
                "LOT_SIZE": 6500,
                "BLD_AREA": 1650,
                "USE_CODE": "101",
                "STYLE": "Colonial",
                "LS_PRICE": 360000,
                "LS_DATE": _sale_date(330),
            },
            {
                "LOC_ID": "PARCEL008",
                "TOTAL_VAL": 300000,
                "LOT_SIZE": 6000,
                "BLD_AREA": 1580,
                "USE_CODE": "101",
                "STYLE": "Cape",
                "LS_PRICE": 345000,
                "LS_DATE": _sale_date(340),
            },
            {
                "LOC_ID": "PARCEL009",
                "TOTAL_VAL": 285000,
                "LOT_SIZE": 5900,
                "BLD_AREA": 1480,
                "USE_CODE": "101",
                "STYLE": "Colonial",
                "LS_PRICE": 325000,
                "LS_DATE": _sale_date(350),
            },
            {
                "LOC_ID": "PARCEL010",
                "TOTAL_VAL": 275000,
                "LOT_SIZE": 5800,
                "BLD_AREA": 1400,
                "USE_CODE": "101",
                "STYLE": "Ranch",
                "LS_PRICE": 315000,
                "LS_DATE": _sale_date(360),
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
