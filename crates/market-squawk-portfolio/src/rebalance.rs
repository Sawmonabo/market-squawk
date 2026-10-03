//! Constrained revision-bound rebalance proposals with no execution authority.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use market_squawk_analytics::{ExactDecimalScale, ExactRate};
use market_squawk_domain::{InstrumentId, Money};
use num_bigint::{BigInt, Sign};
use rust_decimal::Decimal;

use crate::{PortfolioError, PortfolioLimits, PortfolioRevision, PortfolioRevisionId};

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
    /// Whether existing negative holdings may remain short in a constrained proposal.
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
    /// proportionally, subject to conservative decimal rounding, and `constrained()` is true.
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
        let holdings = revision
            .positions()
            .iter()
            .map(|position| (position.instrument_id(), position.market_value()))
            .collect::<Vec<_>>();
        let calculation = RebalanceCalculation::try_calculate(
            revision.cash(),
            &holdings,
            targets,
            constraints,
            limits,
        )?;
        Ok(Self {
            revision_id: revision.id(),
            trades: calculation.trades,
            projected_cash: calculation.projected_cash,
            turnover: calculation.turnover,
            constrained: calculation.constrained,
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

    /// Returns whether policy constraints or representational rounding changed a target.
    pub const fn constrained(&self) -> bool {
        self.constrained
    }
}

/// Reusable value calculation without a fabricated ledger revision or execution authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalanceCalculation {
    total_value: Money,
    trades: Vec<ProposedTrade>,
    projected_cash: Money,
    turnover: ExactRate,
    constrained: bool,
}

impl RebalanceCalculation {
    /// Calculates progress toward explicit allocations of total wealth, including cash.
    ///
    /// Every holding requires one target. Exact integer intermediates bind the proportional
    /// scale to net cash consumption and half-gross turnover. Nonterminating amounts are rounded
    /// toward zero at the greatest jointly representable Decimal precision; any cash-rounding
    /// residual first reduces purchases. Existing shorts may remain only with `allow_short`.
    /// `constrained` includes both policy limits and representational rounding. Turnover is
    /// reported to 28 decimal places, nearest-even; its limit is checked before rounding.
    ///
    /// # Errors
    ///
    /// Rejects incomplete/duplicate targets, currency mismatches, infeasible constraints,
    /// unrepresentable financial totals, and admitted resource bounds.
    pub fn try_calculate(
        cash: Money,
        holdings: &[(InstrumentId, Money)],
        targets: &[RebalanceTarget],
        constraints: RebalanceConstraints,
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if targets.is_empty() || targets.len() > limits.max_instruments {
            return Err(PortfolioError::LimitExceeded {
                resource: "rebalance targets",
                observed: targets.len(),
                limit: limits.max_instruments,
            });
        }
        if holdings.len() != targets.len() {
            return Err(PortfolioError::InvalidDimension);
        }
        if constraints.minimum_cash.currency() != cash.currency()
            || holdings
                .iter()
                .any(|(_, value)| value.currency() != cash.currency())
        {
            return Err(PortfolioError::CurrencyMismatch);
        }
        let unit = BigInt::from(10_u8).pow(Decimal::MAX_SCALE);
        let zero = BigInt::from(0_u8);
        let mut weights = BTreeMap::new();
        for target in targets {
            if weights
                .insert(target.instrument_id, units(target.target_weight.value()))
                .is_some()
            {
                return Err(PortfolioError::InvalidDimension);
            }
        }
        if weights.values().sum::<BigInt>() != unit {
            return Err(PortfolioError::InvalidPolicy);
        }
        let cash_units = units(cash.amount());
        let total = holdings.iter().fold(cash_units.clone(), |sum, (_, value)| {
            sum + units(value.amount())
        });
        if total <= zero {
            return Err(PortfolioError::InvalidPolicy);
        }
        let total_value = Money::new(
            decimal(&total).ok_or(PortfolioError::Arithmetic)?,
            cash.currency(),
        );
        let mut desired = Vec::new();
        desired
            .try_reserve_exact(holdings.len())
            .map_err(|_| PortfolioError::Arithmetic)?;
        for &(instrument_id, current) in holdings {
            let weight = weights
                .remove(&instrument_id)
                .ok_or(PortfolioError::InvalidDimension)?;
            let current = units(current.amount());
            let delta = &total * weight - &current * &unit;
            desired.push((instrument_id, current, delta));
        }
        // Canonical order makes residual allocation independent of request/holding order.
        desired.sort_unstable_by_key(|(id, _, _)| *id);
        let gross = desired
            .iter()
            .map(|(_, _, delta)| magnitude(delta))
            .sum::<BigInt>();
        let net = desired.iter().map(|(_, _, delta)| delta).sum::<BigInt>();
        let gross_limit_numerator = &total * units(constraints.max_turnover.value()) * 2_u8;
        let floor = units(constraints.minimum_cash.amount());
        let available = &cash_units - &floor;
        let (mut numerator, mut denominator) = (BigInt::from(1_u8), BigInt::from(1_u8));
        if gross > gross_limit_numerator {
            numerator = gross_limit_numerator.clone();
            denominator = gross.clone();
        }
        if net > zero && &net * &numerator > &available * &unit * &denominator {
            numerator = &available * &unit;
            denominator = net.clone();
        }
        if numerator < zero || &available * &unit * &denominator < &net * &numerator {
            return Err(PortfolioError::InvalidPolicy);
        }
        if !constraints.allow_short
            && desired.iter().any(|(_, current, delta)| {
                current * &unit * &denominator + delta * &numerator < zero
            })
        {
            return Err(PortfolioError::InvalidPolicy);
        }
        // Decimal has exactly 29 supported scales. Search those representations, never an
        // epsilon-based financial tolerance or an arbitrary retry limit. Integer intermediates
        // remain bounded by Decimal input width and the admitted holding count.
        for scale in (0..=Decimal::MAX_SCALE).rev() {
            let quantum = BigInt::from(10_u8).pow(Decimal::MAX_SCALE - scale);
            let divisor = &unit * &denominator * &quantum;
            let mut changes = desired
                .iter()
                .map(|(_, _, delta)| delta * &numerator / &divisor * &quantum)
                .collect::<Vec<_>>();
            let mut deficit = changes.iter().sum::<BigInt>() - &available;
            // Truncating sales can leave a tiny cash deficit. Reduce purchases by whole output
            // quanta, retaining the freed cash instead of overstating funded purchases.
            if deficit > zero {
                for change in &mut changes {
                    if change.sign() == Sign::Plus {
                        let reduction = ((&deficit + &quantum - 1_u8) / &quantum * &quantum)
                            .min(change.clone());
                        *change -= &reduction;
                        deficit -= reduction;
                        if deficit <= zero {
                            break;
                        }
                    }
                }
            }
            // A net-selling account can start with negative cash. If truncation alone prevents
            // covering it, complete a final sale quantum only when the exact turnover budget
            // and the no-short policy admit it. This is also disclosed as constrained rounding.
            if deficit > zero {
                for ((_, current, delta), change) in desired.iter().zip(&mut changes) {
                    if delta.sign() == Sign::Minus {
                        let extra = (&deficit + &quantum - 1_u8) / &quantum * &quantum;
                        if current + &*change >= extra {
                            *change -= &extra;
                            break;
                        }
                    }
                }
            }
            let actual_gross = changes.iter().map(magnitude).sum::<BigInt>();
            let projected_cash = &cash_units - changes.iter().sum::<BigInt>();
            if projected_cash < floor || &actual_gross * &unit > gross_limit_numerator {
                continue;
            }
            let Some(projected_cash) = decimal(&projected_cash) else {
                continue;
            };
            let mut trades = Vec::new();
            trades
                .try_reserve_exact(changes.len())
                .map_err(|_| PortfolioError::Arithmetic)?;
            let mut constrained = false;
            let mut representable = true;
            for ((instrument_id, current, desired_delta), change) in desired.iter().zip(&changes) {
                let projected = current + change;
                if (!constraints.allow_short && projected < zero) || decimal(&projected).is_none() {
                    representable = false;
                    break;
                }
                let Some(amount) = decimal(change) else {
                    representable = false;
                    break;
                };
                constrained |= change * &unit != *desired_delta;
                if !amount.is_zero() {
                    trades.push(ProposedTrade {
                        instrument_id: *instrument_id,
                        value_change: Money::new(amount, cash.currency()),
                    });
                }
            }
            if !representable {
                continue;
            }
            if trades.len() > constraints.max_proposals || trades.len() > limits.max_results {
                return Err(PortfolioError::LimitExceeded {
                    resource: "rebalance proposals",
                    observed: trades.len(),
                    limit: constraints.max_proposals.min(limits.max_results),
                });
            }
            let turnover = rounded_rate(&actual_gross, &(&total * 2_u8), &unit)?;
            return Ok(Self {
                total_value,
                trades,
                projected_cash: Money::new(projected_cash, cash.currency()),
                turnover: ExactRate::try_new(turnover, ExactDecimalScale::Unit)
                    .map_err(|_| PortfolioError::Analytics)?,
                constrained,
            });
        }
        Err(PortfolioError::Arithmetic)
    }

    /// Returns original holdings plus cash, in the account currency.
    pub const fn total_value(&self) -> Money {
        self.total_value
    }
    /// Returns signed hypothetical value changes in canonical instrument order.
    pub fn trades(&self) -> &[ProposedTrade] {
        &self.trades
    }
    /// Returns exactly conserved cash after the value changes.
    pub const fn projected_cash(&self) -> Money {
        self.projected_cash
    }
    /// Returns half-gross turnover, rounded nearest-even to Decimal precision.
    pub const fn turnover(&self) -> ExactRate {
        self.turnover
    }
    /// Returns whether a policy constraint or decimal rounding changed any desired allocation.
    pub const fn constrained(&self) -> bool {
        self.constrained
    }
}

fn units(value: Decimal) -> BigInt {
    BigInt::from(value.mantissa()) * BigInt::from(10_u8).pow(Decimal::MAX_SCALE - value.scale())
}

fn magnitude(value: &BigInt) -> BigInt {
    if value.sign() == Sign::Minus {
        -value
    } else {
        value.clone()
    }
}

fn decimal(value: &BigInt) -> Option<Decimal> {
    let mut mantissa = value.clone();
    let mut scale = Decimal::MAX_SCALE;
    while scale > 0 && &mantissa % 10_u8 == BigInt::from(0_u8) {
        mantissa /= 10_u8;
        scale -= 1;
    }
    Decimal::try_from_i128_with_scale(i128::try_from(mantissa).ok()?, scale).ok()
}

fn rounded_rate(
    numerator: &BigInt,
    denominator: &BigInt,
    unit: &BigInt,
) -> Result<Decimal, PortfolioError> {
    let scaled = numerator * unit;
    let mut quotient = &scaled / denominator;
    let remainder = scaled % denominator;
    if &remainder * 2_u8 > *denominator || (&remainder * 2_u8 == *denominator && quotient.bit(0)) {
        quotient += 1_u8;
    }
    decimal(&quotient).ok_or(PortfolioError::Arithmetic)
}
