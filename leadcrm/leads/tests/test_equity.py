from django.test import SimpleTestCase

from leads.services import calculate_equity_metrics


class EquityMetricTests(SimpleTestCase):
    def test_market_value_overrides_assessed_total(self) -> None:
        record = {
            "MARKET_VALUE": 450000,
            "TOTAL_VAL": 300000,
            "LS_PRICE": 200000,
            "LS_DATE": "2018-06-01",
        }

        equity_percent, balance, equity_value, *_ = calculate_equity_metrics(record)

        self.assertIsNotNone(equity_percent)
        self.assertGreater(equity_percent or 0, 0)
        # Equity value should be based on market value (450k - balance)
        self.assertGreater((equity_value or 0), 200000)
