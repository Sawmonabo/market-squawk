//! Exact fundamental, valuation, free-cash-flow, and earnings-surprise kernels.

use rust_decimal::Decimal;

use crate::batch::checked_decimal_ratio;
use crate::{AnalyticsError, DecimalPolicy, ExactDecimalResult, ExactDecimalUnit, MonetaryValue};

/// Minimal exact financial period consumed by reusable fundamental kernels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FundamentalPeriod {
    revenue: MonetaryValue,
    operating_income: MonetaryValue,
    operating_cash_flow: MonetaryValue,
    capital_expenditure: MonetaryValue,
}

impl FundamentalPeriod {
    /// Constructs one invariant-preserving period using nonnegative capex-as-outflow convention.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies or negative capital expenditure.
    pub fn try_new(
        revenue: MonetaryValue,
        operating_income: MonetaryValue,
        operating_cash_flow: MonetaryValue,
        capital_expenditure: MonetaryValue,
    ) -> Result<Self, AnalyticsError> {
        ensure_same_measurement(revenue, operating_income)?;
        ensure_same_measurement(revenue, operating_cash_flow)?;
        ensure_same_measurement(revenue, capital_expenditure)?;
        if capital_expenditure.money().amount() < Decimal::ZERO {
            return Err(AnalyticsError::NegativeCapitalExpenditure);
        }
        Ok(Self {
            revenue,
            operating_income,
            operating_cash_flow,
            capital_expenditure,
        })
    }

    /// Returns revenue.
    #[must_use]
    pub const fn revenue(self) -> MonetaryValue {
        self.revenue
    }

    /// Returns operating income.
    #[must_use]
    pub const fn operating_income(self) -> MonetaryValue {
        self.operating_income
    }

    /// Returns operating cash flow.
    #[must_use]
    pub const fn operating_cash_flow(self) -> MonetaryValue {
        self.operating_cash_flow
    }

    /// Returns capital expenditure represented as a nonnegative cash outflow amount.
    #[must_use]
    pub const fn capital_expenditure(self) -> MonetaryValue {
        self.capital_expenditure
    }

    /// Computes `operating_cash_flow - capital_expenditure` exactly.
    ///
    /// # Errors
    ///
    /// Rejects currency mismatch or unrepresentable exact subtraction.
    pub fn free_cash_flow(self) -> Result<MonetaryValue, AnalyticsError> {
        ensure_same_measurement(self.operating_cash_flow, self.capital_expenditure)?;
        let money = self
            .operating_cash_flow
            .money()
            .checked_sub(self.capital_expenditure.money())
            .map_err(|_| AnalyticsError::DecimalArithmetic)?;
        Ok(MonetaryValue::new(money, self.operating_cash_flow.basis()))
    }
}

/// Computes period-over-period growth `(current - prior) / abs(prior)`.
///
/// # Errors
///
/// Rejects currency mismatch, zero prior value, checked arithmetic failure, or unsupported policy.
pub fn fundamental_growth(
    current: MonetaryValue,
    prior: MonetaryValue,
    policy: DecimalPolicy,
) -> Result<ExactDecimalResult, AnalyticsError> {
    ensure_same_measurement(current, prior)?;
    let change = current
        .money()
        .checked_sub(prior.money())
        .map_err(|_| AnalyticsError::DecimalArithmetic)?;
    ratio_result(
        change.amount(),
        prior.money().amount().abs(),
        ExactDecimalUnit::Rate,
        policy,
    )
}

/// Computes a component margin `component / denominator` with explicit decimal rounding.
///
/// # Errors
///
/// Rejects currency mismatch, zero denominator, or checked decimal failure.
pub fn margin(
    component: MonetaryValue,
    denominator: MonetaryValue,
    policy: DecimalPolicy,
) -> Result<ExactDecimalResult, AnalyticsError> {
    ensure_same_measurement(component, denominator)?;
    ratio_result(
        component.money().amount(),
        denominator.money().amount(),
        ExactDecimalUnit::Ratio,
        policy,
    )
}

/// Computes an exact-money valuation multiple `market_value / metric`.
///
/// # Errors
///
/// Rejects currency mismatch, zero metric, or checked decimal failure.
pub fn valuation_multiple(
    market_value: MonetaryValue,
    metric: MonetaryValue,
    policy: DecimalPolicy,
) -> Result<ExactDecimalResult, AnalyticsError> {
    ensure_same_measurement(market_value, metric)?;
    ratio_result(
        market_value.money().amount(),
        metric.money().amount(),
        ExactDecimalUnit::Ratio,
        policy,
    )
}

/// Computes free-cash-flow yield `free_cash_flow / market_value`.
///
/// # Errors
///
/// Rejects currency mismatch, zero market value, or checked decimal failure.
pub fn free_cash_flow_yield(
    free_cash_flow: MonetaryValue,
    market_value: MonetaryValue,
    policy: DecimalPolicy,
) -> Result<ExactDecimalResult, AnalyticsError> {
    ensure_same_measurement(free_cash_flow, market_value)?;
    ratio_result(
        free_cash_flow.money().amount(),
        market_value.money().amount(),
        ExactDecimalUnit::Rate,
        policy,
    )
}

/// Computes normalized earnings surprise `(actual - consensus) / abs(consensus)`.
///
/// # Errors
///
/// Rejects currency mismatch, zero consensus, or checked decimal failure.
pub fn earnings_surprise(
    actual: MonetaryValue,
    consensus: MonetaryValue,
    policy: DecimalPolicy,
) -> Result<ExactDecimalResult, AnalyticsError> {
    ensure_same_measurement(actual, consensus)?;
    let difference = actual
        .money()
        .checked_sub(consensus.money())
        .map_err(|_| AnalyticsError::DecimalArithmetic)?;
    ratio_result(
        difference.amount(),
        consensus.money().amount().abs(),
        ExactDecimalUnit::Ratio,
        policy,
    )
}

fn ensure_same_measurement(
    left: MonetaryValue,
    right: MonetaryValue,
) -> Result<(), AnalyticsError> {
    if left.money().currency() != right.money().currency() {
        return Err(AnalyticsError::CurrencyMismatch);
    }
    if left.basis() != right.basis() {
        return Err(AnalyticsError::MeasurementUnitMismatch);
    }
    Ok(())
}

fn ratio_result(
    numerator: Decimal,
    denominator: Decimal,
    unit: ExactDecimalUnit,
    policy: DecimalPolicy,
) -> Result<ExactDecimalResult, AnalyticsError> {
    Ok(ExactDecimalResult::new(
        checked_decimal_ratio(numerator, denominator, policy)?,
        unit,
        policy,
    ))
}
