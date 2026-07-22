//! Policy-explicit exact time- and money-weighted performance.

use std::num::NonZeroU32;

use market_squawk_analytics::{ExactDecimalScale, ExactRate};
use market_squawk_domain::{Money, Timestamp};
use rust_decimal::Decimal;

use crate::{
    PortfolioError, PortfolioLimits, PortfolioRevision, PortfolioRevisionId, checked_decimal_add,
    checked_decimal_div, checked_decimal_mul, checked_decimal_sub,
};

/// Boundary convention for external cash flows in subperiod returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CashFlowTiming {
    /// Cash is economically present throughout the period.
    StartOfPeriod,
    /// Cash enters or exits after the period return is earned.
    EndOfPeriod,
}

/// Explicit money-weighted calculation method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MoneyWeightedMethod {
    /// Exact Modified Dietz with timing weights fixed by [`CashFlowTiming`].
    ModifiedDietz,
}

/// Versioned performance policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PerformancePolicy {
    cash_flow_timing: CashFlowTiming,
    money_weighted_method: MoneyWeightedMethod,
    version: NonZeroU32,
}

impl PerformancePolicy {
    /// Constructs an explicit versioned policy.
    pub const fn new(
        cash_flow_timing: CashFlowTiming,
        money_weighted_method: MoneyWeightedMethod,
        version: NonZeroU32,
    ) -> Self {
        Self {
            cash_flow_timing,
            money_weighted_method,
            version,
        }
    }

    /// Returns the flow timing convention.
    pub const fn cash_flow_timing(self) -> CashFlowTiming {
        self.cash_flow_timing
    }

    /// Returns the money-weighted method.
    pub const fn money_weighted_method(self) -> MoneyWeightedMethod {
        self.money_weighted_method
    }

    /// Returns semantic policy version.
    pub const fn version(self) -> NonZeroU32 {
        self.version
    }
}

/// One ordered valuation subperiod and its signed external flow.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PerformancePeriod {
    ends_at: Timestamp,
    opening_value: Money,
    closing_value: Money,
    external_flow: Money,
}

impl PerformancePeriod {
    /// Constructs a subperiod in one currency with positive opening value.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies or nonpositive opening value.
    pub fn try_new(
        ends_at: Timestamp,
        opening_value: Money,
        closing_value: Money,
        external_flow: Money,
    ) -> Result<Self, PortfolioError> {
        if opening_value.amount() <= Decimal::ZERO
            || opening_value.currency() != closing_value.currency()
            || opening_value.currency() != external_flow.currency()
        {
            return Err(PortfolioError::InvalidPolicy);
        }
        Ok(Self {
            ends_at,
            opening_value,
            closing_value,
            external_flow,
        })
    }

    /// Returns period end.
    pub const fn ends_at(self) -> Timestamp {
        self.ends_at
    }

    /// Returns opening valuation.
    pub const fn opening_value(self) -> Money {
        self.opening_value
    }

    /// Returns closing valuation before timing interpretation.
    pub const fn closing_value(self) -> Money {
        self.closing_value
    }

    /// Returns signed external cash flow.
    pub const fn external_flow(self) -> Money {
        self.external_flow
    }
}

/// Revision-bound exact performance output.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PerformanceReport {
    revision_id: PortfolioRevisionId,
    policy: PerformancePolicy,
    time_weighted_return: ExactRate,
    money_weighted_return: ExactRate,
    periods: usize,
}

impl PerformanceReport {
    /// Calculates exact TWR and Modified Dietz MWR under one explicit policy.
    ///
    /// # Errors
    ///
    /// Rejects empty/excessive, unordered, mixed-currency, or arithmetically invalid periods.
    pub fn try_calculate(
        revision: &PortfolioRevision,
        periods: &[PerformancePeriod],
        policy: PerformancePolicy,
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if periods.is_empty() || periods.len() > limits.max_history {
            return Err(PortfolioError::LimitExceeded {
                resource: "performance history",
                observed: periods.len(),
                limit: limits.max_history,
            });
        }
        if periods
            .windows(2)
            .any(|window| window[0].ends_at >= window[1].ends_at)
            || periods.iter().any(|period| {
                period.opening_value.currency() != revision.base_currency()
                    || period.closing_value.currency() != revision.base_currency()
                    || period.external_flow.currency() != revision.base_currency()
            })
        {
            return Err(PortfolioError::InvalidPolicy);
        }
        let growth = periods.iter().try_fold(Decimal::ONE, |growth, period| {
            let (numerator, denominator) = match policy.cash_flow_timing {
                CashFlowTiming::StartOfPeriod => (
                    period.closing_value.amount(),
                    checked_decimal_add(
                        period.opening_value.amount(),
                        period.external_flow.amount(),
                    )?,
                ),
                CashFlowTiming::EndOfPeriod => (
                    checked_decimal_sub(
                        period.closing_value.amount(),
                        period.external_flow.amount(),
                    )?,
                    period.opening_value.amount(),
                ),
            };
            checked_decimal_mul(growth, checked_decimal_div(numerator, denominator)?)
        })?;
        let time_weighted = checked_decimal_sub(growth, Decimal::ONE)?;
        let opening = periods
            .first()
            .map(|period| period.opening_value.amount())
            .ok_or(PortfolioError::InvalidPolicy)?;
        let closing = periods
            .last()
            .map(|period| period.closing_value.amount())
            .ok_or(PortfolioError::InvalidPolicy)?;
        let flows = periods.iter().try_fold(Decimal::ZERO, |total, period| {
            checked_decimal_add(total, period.external_flow.amount())
        })?;
        let weighted_flows = match policy.cash_flow_timing {
            CashFlowTiming::StartOfPeriod => flows,
            CashFlowTiming::EndOfPeriod => Decimal::ZERO,
        };
        let money_weighted = checked_decimal_div(
            checked_decimal_sub(checked_decimal_sub(closing, opening)?, flows)?,
            checked_decimal_add(opening, weighted_flows)?,
        )?;
        Ok(Self {
            revision_id: revision.id(),
            policy,
            time_weighted_return: ExactRate::try_new(time_weighted, ExactDecimalScale::Unit)
                .map_err(|_| PortfolioError::Analytics)?,
            money_weighted_return: ExactRate::try_new(money_weighted, ExactDecimalScale::Unit)
                .map_err(|_| PortfolioError::Analytics)?,
            periods: periods.len(),
        })
    }

    /// Returns bound immutable revision identity.
    pub const fn revision_id(self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns calculation policy.
    pub const fn policy(self) -> PerformancePolicy {
        self.policy
    }

    /// Returns geometrically linked subperiod return.
    pub const fn time_weighted_return(self) -> ExactRate {
        self.time_weighted_return
    }

    /// Returns exact Modified Dietz return.
    pub const fn money_weighted_return(self) -> ExactRate {
        self.money_weighted_return
    }

    /// Returns contributing period count.
    pub const fn periods(self) -> usize {
        self.periods
    }
}
