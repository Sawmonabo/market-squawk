from __future__ import annotations

from decimal import Decimal
import math
import unittest

from market_squawk import finance


class FinanceParityContracts(unittest.TestCase):
    def test_exported_rust_kernels_match_golden_finance_results(self) -> None:
        context = finance.OperationContext(60_000, 1_000_000)
        returns = finance.simple_returns(
            [Decimal("100.0"), Decimal("110.0"), Decimal("99.0")],
            [1, 2, 3],
            "USD",
            context=context,
        )
        self.assertEqual(len(returns.values), 2)
        self.assertAlmostEqual(returns.values[0], 0.1)
        self.assertAlmostEqual(returns.values[1], -0.1)
        self.assertEqual(returns.policy.currency, "USD")
        self.assertEqual(returns.policy.time_axis, "unix-nanoseconds-utc:adjacent-observations")
        self.assertAlmostEqual(
            finance.cumulative_return(returns.values, context=context).value, -0.01
        )
        self.assertAlmostEqual(
            finance.total_returns(
                [Decimal("100.00"), Decimal("102.00")],
                [Decimal("1.00")],
                [1, 2],
                "USD",
                context=context,
            ).values[0],
            0.03,
        )

        volatility = finance.volatility(
            [0.01, -0.02, 0.03], periods_per_year=252, context=context
        )
        self.assertAlmostEqual(volatility.value, 0.025166114784235832 * math.sqrt(252))
        self.assertEqual(volatility.policy.variance, "sample")
        self.assertEqual(volatility.policy.annualization_periods, 252)
        drawdown = finance.maximum_drawdown(
            [Decimal("100"), Decimal("120"), Decimal("90"), Decimal("121")],
            [1, 2, 3, 4],
            "USD",
            context=context,
        )
        self.assertEqual(
            (drawdown.magnitude, drawdown.peak_index, drawdown.trough_index, drawdown.recovery_index),
            (0.25, 1, 2, 3),
        )
        self.assertAlmostEqual(
            finance.correlation(
                [1.0, 2.0, 3.0], [3.0, 2.0, 1.0], context=context
            ).value,
            -1.0,
        )
        self.assertEqual(
            finance.historical_var([1.0, 4.0, 2.0, 3.0], 0.75, context=context).value,
            3.0,
        )
        self.assertAlmostEqual(
            finance.expected_shortfall(
                [1.0, 4.0, 2.0, 3.0], 0.75, context=context
            ).value,
            4.0,
        )

    def test_statistical_boundary_rejects_nonfinite_values(self) -> None:
        context = finance.OperationContext(60_000, 1_000_000)
        with self.assertRaises(ValueError):
            finance.cumulative_return([0.1, float("nan")], context=context)

        with self.assertRaises(TypeError):
            finance.total_returns([100.0, 110.0], [0.0], [1, 2], "USD", context=context)

        cancelled = finance.OperationContext(60_000, 1_000_000)
        cancelled.cancel()
        with self.assertRaises(ValueError):
            finance.cumulative_return([0.1], context=cancelled)

        class OversizedSequence:
            accessed = False

            def __len__(self) -> int:
                return finance.MAX_ANALYTIC_VALUES + 1

            def __getitem__(self, _index: int) -> float:
                self.accessed = True
                raise AssertionError("oversized sequence was materialized")

        oversized = OversizedSequence()
        with self.assertRaises(ValueError):
            finance._native.compound_returns(oversized, context)
        self.assertFalse(oversized.accessed)


if __name__ == "__main__":
    unittest.main()
