"""Typed Python facade over the bounded Rust batch analytics kernels."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence, TypedDict

from . import _native


MAX_ANALYTIC_VALUES = 1_000_000
MAX_ANALYTIC_RETAINED_BYTES = 64 * 1024 * 1024
MAX_DECIMAL_TEXT_BYTES = 128


class FeatureContract(TypedDict):
    name: str
    version: int
    input_schema_sha256: str
    semantic_sha256: str


@dataclass(frozen=True)
class AnalyticalPolicy:
    """Explicit units, time semantics, and statistical choices for one result."""

    input_unit: str
    output_unit: str
    missing_value: str = "reject"
    currency: str | None = None
    time_axis: str | None = None
    variance: str | None = None
    annualization_periods: int | None = None
    quantile: str | None = None
    confidence: float | None = None
    aggregation: str | None = None


@dataclass(frozen=True)
class SeriesResult:
    values: tuple[float, ...]
    policy: AnalyticalPolicy


@dataclass(frozen=True)
class ScalarResult:
    value: float
    policy: AnalyticalPolicy


@dataclass(frozen=True)
class DrawdownResult:
    magnitude: float
    peak_index: int
    trough_index: int
    recovery_index: int | None
    policy: AnalyticalPolicy


def simple_returns(
    prices: Sequence[float], timestamps: Sequence[int], currency: str
) -> SeriesResult:
    values = _native.price_returns(_bounded(prices), _bounded(timestamps), currency)
    return SeriesResult(
        tuple(values),
        AnalyticalPolicy(
            input_unit="currency",
            output_unit="return:unit",
            currency=currency,
            time_axis="unix-nanoseconds-utc:adjacent-observations",
        ),
    )


def total_returns(
    prices: Sequence[Decimal],
    distributions: Sequence[Decimal],
    timestamps: Sequence[int],
    currency: str,
) -> SeriesResult:
    price_values = _decimal_strings(prices)
    distribution_values = _decimal_strings(distributions)
    values = _native.exact_total_returns(
        price_values, distribution_values, _bounded(timestamps), currency
    )
    return SeriesResult(
        tuple(values),
        AnalyticalPolicy(
            input_unit="decimal-currency",
            output_unit="return:unit",
            currency=currency,
            time_axis="unix-nanoseconds-utc:adjacent-observations",
        ),
    )


def cumulative_return(returns: Sequence[float]) -> ScalarResult:
    return ScalarResult(
        _native.compound_returns(_bounded(returns)),
        AnalyticalPolicy(
            input_unit="return:unit",
            output_unit="return:unit",
            aggregation="compounded",
        ),
    )


def volatility(returns: Sequence[float], *, periods_per_year: int) -> ScalarResult:
    return ScalarResult(
        _native.return_volatility(_bounded(returns), periods_per_year),
        AnalyticalPolicy(
            input_unit="return:unit",
            output_unit="return:unit",
            variance="sample",
            annualization_periods=periods_per_year,
        ),
    )


def maximum_drawdown(
    prices: Sequence[float], timestamps: Sequence[int], currency: str
) -> DrawdownResult:
    magnitude, peak, trough, recovery = _native.drawdown(
        _bounded(prices), _bounded(timestamps), currency
    )
    return DrawdownResult(
        magnitude,
        peak,
        trough,
        recovery,
        AnalyticalPolicy(
            input_unit="currency",
            output_unit="return:unit",
            currency=currency,
            time_axis="unix-nanoseconds-utc:ordered-observations",
        ),
    )


def correlation(left: Sequence[float], right: Sequence[float]) -> ScalarResult:
    return ScalarResult(
        _native.pearson_correlation(_bounded(left), _bounded(right)),
        AnalyticalPolicy(
            input_unit="return:unit",
            output_unit="correlation-coefficient",
            variance="sample",
        ),
    )


def historical_var(losses: Sequence[float], confidence: float) -> ScalarResult:
    return ScalarResult(
        _native.value_at_risk(_bounded(losses), confidence),
        AnalyticalPolicy(
            input_unit="return:unit",
            output_unit="loss:return:unit",
            quantile="historical-nearest-rank",
            confidence=confidence,
        ),
    )


def expected_shortfall(losses: Sequence[float], confidence: float) -> ScalarResult:
    return ScalarResult(
        _native.expected_shortfall(_bounded(losses), confidence),
        AnalyticalPolicy(
            input_unit="return:unit",
            output_unit="loss:return:unit",
            quantile="discrete-tail-mean",
            confidence=confidence,
        ),
    )


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


def _bounded(values: Sequence[object]) -> Sequence[object]:
    try:
        count = len(values)
    except (TypeError, OverflowError) as error:
        raise ValueError("financial input must be a sized sequence") from error
    if not 0 <= count <= MAX_ANALYTIC_VALUES:
        raise ValueError("financial input exceeds its retained-value bound")
    return values


def _decimal_strings(values: Sequence[Decimal]) -> list[str]:
    admitted = _bounded(values)
    if len(admitted) * MAX_DECIMAL_TEXT_BYTES > MAX_ANALYTIC_RETAINED_BYTES:
        raise ValueError("decimal input exceeds its retained-byte bound")
    encoded: list[str] = []
    retained_bytes = 0
    for value in admitted:
        text = str(value)
        text_bytes = len(text.encode("utf-8"))
        retained_bytes += text_bytes
        if (
            not text
            or text_bytes > MAX_DECIMAL_TEXT_BYTES
            or retained_bytes > MAX_ANALYTIC_RETAINED_BYTES
        ):
            raise ValueError("decimal input exceeds its text bound")
        encoded.append(text)
    return encoded
