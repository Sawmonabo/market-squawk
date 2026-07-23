//! Constrained revision-bound rebalance proposals with no execution authority.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use market_squawk_analytics::ExactRate;
use market_squawk_domain::{InstrumentId, Money};
use rust_decimal::Decimal;

use crate::{
    PortfolioError, PortfolioLimits, PortfolioRevision, PortfolioRevisionId, checked_decimal_add,
    checked_decimal_div, checked_decimal_mul, checked_decimal_sub,
};

/// One desired instrument allocation weight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RebalanceTarget {
    instrument_id: InstrumentId,
    target_weight: ExactRate,
}

impl RebalanceTarget {
    /// Constructs a target in the closed interval `[0, 1]`.
    ///
    /// # Errors
    ///
    /// Rejects negative or above-total weights.
    pub fn try_new(
        instrument_id: InstrumentId,
        target_weight: ExactRate,
    ) -> Result<Self, PortfolioError> {
        if target_weight.value() < Decimal::ZERO || target_weight.value() > Decimal::ONE {
            return Err(PortfolioError::InvalidPolicy);
        }
        Ok(Self {
            instrument_id,
            target_weight,
        })
    }
}

/// Caller input for constrained proposal generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RebalanceConstraintInput {
    /// Maximum proposed instrument adjustments.
    pub max_proposals: NonZeroUsize,
    /// Maximum one-way turnover as a fraction of total account value.
    pub max_turnover: ExactRate,
    /// Minimum cash retained after every proposal.
    pub minimum_cash: Money,
    /// Whether a target may cross through zero into a short value.
    pub allow_short: bool,
}

/// Validated rebalance constraints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RebalanceConstraints {
    max_proposals: usize,
    max_turnover: ExactRate,
    minimum_cash: Money,
    allow_short: bool,
}

impl RebalanceConstraints {
    /// Validates proposal count, turnover, and cash floor.
    ///
    /// # Errors
    ///
    /// Rejects negative/excessive turnover or a negative cash floor.
    pub fn try_new(input: RebalanceConstraintInput) -> Result<Self, PortfolioError> {
        if input.max_turnover.value() < Decimal::ZERO
            || input.max_turnover.value() > Decimal::ONE
            || input.minimum_cash.amount().is_sign_negative()
        {
            return Err(PortfolioError::InvalidPolicy);
        }
        Ok(Self {
            max_proposals: input.max_proposals.get(),
            max_turnover: input.max_turnover,
            minimum_cash: input.minimum_cash,
            allow_short: input.allow_short,
        })
    }
}

/// One signed value adjustment proposal, not an order or approval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposedTrade {
    instrument_id: InstrumentId,
    value_change: Money,
}

impl ProposedTrade {
    /// Returns canonical instrument identity.
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns signed desired value change; positive buys and negative sells remain proposals.
    pub const fn value_change(self) -> Money {
        self.value_change
    }
}

/// Bounded proposal set bound to the current portfolio revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceProposal {
    revision_id: PortfolioRevisionId,
    trades: Vec<ProposedTrade>,
    projected_cash: Money,
    turnover: ExactRate,
    constrained: bool,
}

impl RebalanceProposal {
    /// Calculates a deterministic cash- and turnover-constrained proposal.
    ///
    /// The result carries no order, approval, reservation, dispatch, or live adapter capability.
    /// If exact targets conflict with constraints, all nonzero desired deltas are scaled
    /// proportionally and `constrained()` is true.
    ///
    /// # Errors
    ///
    /// Rejects missing/duplicate targets, weights not totaling one, currencies, or bounds.
    pub fn try_calculate(
        revision: &PortfolioRevision,
        targets: &[RebalanceTarget],
        constraints: RebalanceConstraints,
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if targets.is_empty()
            || targets.len() > limits.max_instruments
            || targets.len() > constraints.max_proposals
        {
            return Err(PortfolioError::LimitExceeded {
                resource: "rebalance targets",
                observed: targets.len(),
                limit: limits.max_instruments.min(constraints.max_proposals),
            });
        }
        if constraints.minimum_cash.currency() != revision.base_currency() {
            return Err(PortfolioError::CurrencyMismatch);
        }
        let unique = targets
            .iter()
            .map(|target| target.instrument_id)
            .collect::<BTreeSet<_>>();
        if unique.len() != targets.len()
            || targets
                .iter()
                .any(|target| revision.position(target.instrument_id).is_none())
        {
            return Err(PortfolioError::InvalidDimension);
        }
        let weight_total = targets.iter().try_fold(Decimal::ZERO, |total, target| {
            checked_decimal_add(total, target.target_weight.value())
        })?;
        if weight_total != Decimal::ONE {
            return Err(PortfolioError::InvalidPolicy);
        }
        let total_value =
            checked_decimal_add(revision.cash().amount(), revision.market_value().amount())?;
        if total_value <= Decimal::ZERO {
            return Err(PortfolioError::InvalidPolicy);
        }
        let mut desired = targets
            .iter()
            .map(|target| {
                let current = revision
                    .position(target.instrument_id)
                    .ok_or(PortfolioError::InvalidDimension)?
                    .market_value()
                    .amount();
                let target_value = checked_decimal_mul(total_value, target.target_weight.value())?;
                Ok((
                    target.instrument_id,
                    current,
                    checked_decimal_sub(target_value, current)?,
                ))
            })
            .collect::<Result<Vec<_>, PortfolioError>>()?;
        let gross_traded_value = desired
            .iter()
            .try_fold(Decimal::ZERO, |total, (_, _, delta)| {
                checked_decimal_add(total, delta.abs())
            })?;
        // One-way turnover is half of gross buys plus sells. This definition remains stable when
        // current cash makes the proposal net-buying or a cash floor leaves it unbalanced; external
        // cash flows are not part of this proposal contract and therefore require no adjustment.
        let one_way_traded_value = checked_decimal_div(gross_traded_value, Decimal::from(2_u32))?;
        let turnover_limit = checked_decimal_mul(total_value, constraints.max_turnover.value())?;
        let sales = desired
            .iter()
            .try_fold(Decimal::ZERO, |total, (_, _, delta)| {
                if delta.is_sign_negative() {
                    checked_decimal_add(total, delta.abs())
                } else {
                    Ok(total)
                }
            })?;
        let buys = desired
            .iter()
            .try_fold(Decimal::ZERO, |total, (_, _, delta)| {
                if delta.is_sign_positive() {
                    checked_decimal_add(total, *delta)
                } else {
                    Ok(total)
                }
            })?;
        let buy_capacity = checked_decimal_sub(
            checked_decimal_add(revision.cash().amount(), sales)?,
            constraints.minimum_cash.amount(),
        )?
        .max(Decimal::ZERO);
        let turnover_scale = if one_way_traded_value > turnover_limit {
            checked_decimal_div(turnover_limit, one_way_traded_value)?
        } else {
            Decimal::ONE
        };
        let buy_scale = if buys > buy_capacity {
            checked_decimal_div(buy_capacity, buys)?
        } else {
            Decimal::ONE
        };
        let scale = turnover_scale.min(buy_scale);
        let constrained = scale < Decimal::ONE;
        let mut trades = Vec::new();
        for (instrument_id, current, delta) in desired.drain(..) {
            let scaled = checked_decimal_mul(delta, scale)?;
            if scaled.is_zero() {
                continue;
            }
            if !constraints.allow_short && checked_decimal_add(current, scaled)?.is_sign_negative()
            {
                return Err(PortfolioError::InvalidPolicy);
            }
            trades.push(ProposedTrade {
                instrument_id,
                value_change: Money::new(scaled, revision.base_currency()),
            });
        }
        if trades.len() > constraints.max_proposals || trades.len() > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "rebalance proposals",
                observed: trades.len(),
                limit: constraints.max_proposals.min(limits.max_results),
            });
        }
        let net_change = trades.iter().try_fold(Decimal::ZERO, |total, trade| {
            checked_decimal_add(total, trade.value_change.amount())
        })?;
        let projected_cash = Money::new(
            checked_decimal_sub(revision.cash().amount(), net_change)?,
            revision.base_currency(),
        );
        if projected_cash.amount() < constraints.minimum_cash.amount() {
            return Err(PortfolioError::InvalidPolicy);
        }
        let actual_gross_traded_value = trades.iter().try_fold(Decimal::ZERO, |total, trade| {
            checked_decimal_add(total, trade.value_change.amount().abs())
        })?;
        let actual_one_way_traded_value =
            checked_decimal_div(actual_gross_traded_value, Decimal::from(2_u32))?;
        Ok(Self {
            revision_id: revision.id(),
            trades,
            projected_cash,
            turnover: ExactRate::try_new(
                checked_decimal_div(actual_one_way_traded_value, total_value)?,
                market_squawk_analytics::ExactDecimalScale::Unit,
            )
            .map_err(|_| PortfolioError::Analytics)?,
            constrained,
        })
    }

    /// Returns bound immutable revision identity.
    pub const fn revision_id(&self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns bounded signed value proposals.
    pub fn trades(&self) -> &[ProposedTrade] {
        &self.trades
    }

    /// Returns cash after applying every proposed value change.
    pub const fn projected_cash(&self) -> Money {
        self.projected_cash
    }

    /// Returns one-way turnover fraction.
    pub const fn turnover(&self) -> ExactRate {
        self.turnover
    }

    /// Returns whether constraints required proportional scaling.
    pub const fn constrained(&self) -> bool {
        self.constrained
    }
}
