"""Typed Python facade over the bounded Rust batch analytics kernels."""

from __future__ import annotations

from decimal import Decimal
from typing import Sequence, TypedDict

from . import _native


class FeatureContract(TypedDict):
    name: str
    version: int
    input_schema_sha256: str
    semantic_sha256: str


def simple_returns(prices: Sequence[float], timestamps: Sequence[int], currency: str) -> list[float]:
    return _native.price_returns(list(prices), list(timestamps), currency)


def total_returns(
    prices: Sequence[Decimal],
    distributions: Sequence[Decimal],
    timestamps: Sequence[int],
    currency: str,
) -> list[float]:
    return _native.exact_total_returns(
        [str(value) for value in prices],
        [str(value) for value in distributions],
        list(timestamps),
        currency,
    )


def cumulative_return(returns: Sequence[float]) -> float:
    return _native.compound_returns(list(returns))


def volatility(returns: Sequence[float], *, periods_per_year: int) -> float:
    return _native.return_volatility(list(returns), periods_per_year)


def maximum_drawdown(prices: Sequence[float], timestamps: Sequence[int], currency: str) -> dict[str, int | float | None]:
    magnitude, peak, trough, recovery = _native.drawdown(list(prices), list(timestamps), currency)
    return {
        "magnitude": magnitude,
        "peak_index": peak,
        "trough_index": trough,
        "recovery_index": recovery,
    }


def correlation(left: Sequence[float], right: Sequence[float]) -> float:
    return _native.pearson_correlation(list(left), list(right))


def historical_var(losses: Sequence[float], confidence: float) -> float:
    return _native.value_at_risk(list(losses), confidence)


def expected_shortfall(losses: Sequence[float], confidence: float) -> float:
    return _native.expected_shortfall(list(losses), confidence)


def feature_contracts() -> list[FeatureContract]:
    return [
        {
            "name": name,
            "version": version,
            "input_schema_sha256": input_schema,
            "semantic_sha256": semantic,
        }
        for name, version, input_schema, semantic in _native.canonical_feature_contracts()
    ]
