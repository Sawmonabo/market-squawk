from __future__ import annotations

from decimal import Decimal
import math
import unittest

from market_squawk import finance


class FinanceParityContracts(unittest.TestCase):
    def test_exported_rust_kernels_match_golden_finance_results(self) -> None:
        returns = finance.simple_returns([100.0, 110.0, 99.0], [1, 2, 3], "USD")
        self.assertEqual(len(returns.values), 2)
        self.assertAlmostEqual(returns.values[0], 0.1)
        self.assertAlmostEqual(returns.values[1], -0.1)
        self.assertEqual(returns.policy.currency, "USD")
        self.assertEqual(returns.policy.time_axis, "unix-nanoseconds-utc:adjacent-observations")
        self.assertAlmostEqual(finance.cumulative_return(returns.values).value, -0.01)
        self.assertAlmostEqual(
            finance.total_returns(
                [Decimal("100.00"), Decimal("102.00")],
                [Decimal("1.00")],
                [1, 2],
                "USD",
            ).values[0],
            0.03,
        )

        volatility = finance.volatility([0.01, -0.02, 0.03], periods_per_year=252)
        self.assertAlmostEqual(volatility.value, 0.025166114784235832 * math.sqrt(252))
        self.assertEqual(volatility.policy.variance, "sample")
        self.assertEqual(volatility.policy.annualization_periods, 252)
        drawdown = finance.maximum_drawdown([100.0, 120.0, 90.0, 121.0], [1, 2, 3, 4], "USD")
        self.assertEqual(
            (drawdown.magnitude, drawdown.peak_index, drawdown.trough_index, drawdown.recovery_index),
            (0.25, 1, 2, 3),
        )
        self.assertAlmostEqual(
            finance.correlation([1.0, 2.0, 3.0], [3.0, 2.0, 1.0]).value, -1.0
        )
        self.assertEqual(finance.historical_var([1.0, 4.0, 2.0, 3.0], 0.75).value, 3.0)
        self.assertAlmostEqual(
            finance.expected_shortfall([1.0, 4.0, 2.0, 3.0], 0.75).value, 4.0
        )

    def test_statistical_boundary_rejects_nonfinite_values(self) -> None:
        with self.assertRaises(ValueError):
            finance.cumulative_return([0.1, float("nan")])

        class OversizedSequence:
            accessed = False

            def __len__(self) -> int:
                return finance.MAX_ANALYTIC_VALUES + 1

            def __getitem__(self, _index: int) -> float:
                self.accessed = True
                raise AssertionError("oversized sequence was materialized")

        oversized = OversizedSequence()
        with self.assertRaises(ValueError):
            finance._native.compound_returns(oversized)
        self.assertFalse(oversized.accessed)


if __name__ == "__main__":
    unittest.main()
