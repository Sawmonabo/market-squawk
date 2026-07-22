from __future__ import annotations

from decimal import Decimal
import math
import unittest

from market_squawk import finance


class FinanceParityContracts(unittest.TestCase):
    def test_exported_rust_kernels_match_golden_finance_results(self) -> None:
        returns = finance.simple_returns([100.0, 110.0, 99.0], [1, 2, 3], "USD")
        self.assertEqual(len(returns), 2)
        self.assertAlmostEqual(returns[0], 0.1)
        self.assertAlmostEqual(returns[1], -0.1)
        self.assertAlmostEqual(finance.cumulative_return(returns), -0.01)
        self.assertAlmostEqual(
            finance.total_returns(
                [Decimal("100.00"), Decimal("102.00")],
                [Decimal("1.00")],
                [1, 2],
                "USD",
            )[0],
            0.03,
        )

        volatility = finance.volatility([0.01, -0.02, 0.03], periods_per_year=252)
        self.assertAlmostEqual(volatility, 0.025166114784235832 * math.sqrt(252))
        drawdown = finance.maximum_drawdown([100.0, 120.0, 90.0, 121.0], [1, 2, 3, 4], "USD")
        self.assertEqual(drawdown, {"magnitude": 0.25, "peak_index": 1, "trough_index": 2, "recovery_index": 3})
        self.assertAlmostEqual(finance.correlation([1.0, 2.0, 3.0], [3.0, 2.0, 1.0]), -1.0)
        self.assertEqual(finance.historical_var([1.0, 4.0, 2.0, 3.0], 0.75), 3.0)
        self.assertAlmostEqual(finance.expected_shortfall([1.0, 4.0, 2.0, 3.0], 0.75), 4.0)

    def test_statistical_boundary_rejects_nonfinite_values(self) -> None:
        with self.assertRaises(ValueError):
            finance.cumulative_return([0.1, float("nan")])


if __name__ == "__main__":
    unittest.main()
