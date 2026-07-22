//! Exact fundamental, valuation, free-cash-flow, and earnings-surprise kernels.

use market_squawk_domain::Money;
use rust_decimal::Decimal;

use crate::batch::checked_decimal_ratio;
use crate::{AnalyticsError, DecimalPolicy};

/// Minimal exact financial period consumed by reusable fundamental kernels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FundamentalPeriod {
    revenue: Money,
    operating_income: Money,
    operating_cash_flow: Money,
    capital_expenditure: Money,
}

impl FundamentalPeriod {
    /// Constructs one invariant-preserving period using nonnegative capex-as-outflow convention.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies or negative capital expenditure.
    pub fn try_new(
        revenue: Money,
        operating_income: Money,
        operating_cash_flow: Money,
        capital_expenditure: Money,
    ) -> Result<Self, AnalyticsError> {
        let currency = revenue.currency();
        if operating_income.currency() != currency
            || operating_cash_flow.currency() != currency
            || capital_expenditure.currency() != currency
        {
            return Err(AnalyticsError::CurrencyMismatch);
        }
        if capital_expenditure.amount() < Decimal::ZERO {
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
    pub const fn revenue(self) -> Money {
        self.revenue
    }

    /// Returns operating income.
    #[must_use]
    pub const fn operating_income(self) -> Money {
        self.operating_income
    }

    /// Returns operating cash flow.
    #[must_use]
    pub const fn operating_cash_flow(self) -> Money {
        self.operating_cash_flow
    }

    /// Returns capital expenditure represented as a nonnegative cash outflow amount.
    #[must_use]
    pub const fn capital_expenditure(self) -> Money {
        self.capital_expenditure
    }

    /// Computes `operating_cash_flow - capital_expenditure` exactly.
    ///
    /// # Errors
    ///
    /// Rejects currency mismatch or unrepresentable exact subtraction.
    pub fn free_cash_flow(self) -> Result<Money, AnalyticsError> {
        ensure_same_currency(self.operating_cash_flow, self.capital_expenditure)?;
        self.operating_cash_flow
            .checked_sub(self.capital_expenditure)
            .map_err(|_| AnalyticsError::DecimalArithmetic)
    }
}

/// Computes period-over-period growth `(current - prior) / abs(prior)`.
///
/// # Errors
///
/// Rejects currency mismatch, zero prior value, checked arithmetic failure, or unsupported policy.
pub fn fundamental_growth(
    current: Money,
    prior: Money,
    policy: DecimalPolicy,
) -> Result<Decimal, AnalyticsError> {
    ensure_same_currency(current, prior)?;
    let change = current
        .checked_sub(prior)
        .map_err(|_| AnalyticsError::DecimalArithmetic)?;
    checked_decimal_ratio(change.amount(), prior.amount().abs(), policy)
}

/// Computes a component margin `component / denominator` with explicit decimal rounding.
///
/// # Errors
///
/// Rejects currency mismatch, zero denominator, or checked decimal failure.
pub fn margin(
    component: Money,
    denominator: Money,
    policy: DecimalPolicy,
) -> Result<Decimal, AnalyticsError> {
    ensure_same_currency(component, denominator)?;
    checked_decimal_ratio(component.amount(), denominator.amount(), policy)
}

/// Computes an exact-money valuation multiple `market_value / metric`.
///
/// # Errors
///
/// Rejects currency mismatch, zero metric, or checked decimal failure.
pub fn valuation_multiple(
    market_value: Money,
    metric: Money,
    policy: DecimalPolicy,
) -> Result<Decimal, AnalyticsError> {
    ensure_same_currency(market_value, metric)?;
    checked_decimal_ratio(market_value.amount(), metric.amount(), policy)
}

/// Computes free-cash-flow yield `free_cash_flow / market_value`.
///
/// # Errors
///
/// Rejects currency mismatch, zero market value, or checked decimal failure.
pub fn free_cash_flow_yield(
    free_cash_flow: Money,
    market_value: Money,
    policy: DecimalPolicy,
) -> Result<Decimal, AnalyticsError> {
    margin(free_cash_flow, market_value, policy)
}

/// Computes normalized earnings surprise `(actual - consensus) / abs(consensus)`.
///
/// # Errors
///
/// Rejects currency mismatch, zero consensus, or checked decimal failure.
pub fn earnings_surprise(
    actual: Money,
    consensus: Money,
    policy: DecimalPolicy,
) -> Result<Decimal, AnalyticsError> {
    ensure_same_currency(actual, consensus)?;
    let difference = actual
        .checked_sub(consensus)
        .map_err(|_| AnalyticsError::DecimalArithmetic)?;
    checked_decimal_ratio(difference.amount(), consensus.amount().abs(), policy)
}

fn ensure_same_currency(left: Money, right: Money) -> Result<(), AnalyticsError> {
    if left.currency() == right.currency() {
        Ok(())
    } else {
        Err(AnalyticsError::CurrencyMismatch)
    }
}
