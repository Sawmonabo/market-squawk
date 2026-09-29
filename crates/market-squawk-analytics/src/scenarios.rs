//! Exact portfolio exposure, attribution, and composable scenario-stress kernels.

use std::collections::BTreeMap;

use market_squawk_domain::{Currency, Money};
use rust_decimal::Decimal;

use crate::batch::{validate_count, validate_identifier};
use crate::{AnalyticsError, ExactRate, MonetaryBasis, MonetaryValue};

/// One exact portfolio amount and realized/forecast return assigned to a named dimension.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioAllocation {
    dimension: String,
    market_value: MonetaryValue,
    return_rate: ExactRate,
}

impl PortfolioAllocation {
    /// Constructs a dimensioned allocation.
    ///
    /// # Errors
    ///
    /// Rejects an empty, oversized, or non-canonical dimension identifier.
    pub fn try_new(
        dimension: &str,
        market_value: MonetaryValue,
        return_rate: ExactRate,
    ) -> Result<Self, AnalyticsError> {
        validate_identifier(dimension)?;
        Ok(Self {
            dimension: dimension.to_owned(),
            market_value,
            return_rate,
        })
    }

    /// Returns dimension identifier.
    #[must_use]
    pub fn dimension(&self) -> &str {
        &self.dimension
    }

    /// Returns exact market value.
    #[must_use]
    pub const fn market_value(&self) -> MonetaryValue {
        self.market_value
    }

    /// Returns exact realized or forecast return rate.
    #[must_use]
    pub const fn return_rate(&self) -> ExactRate {
        self.return_rate
    }
}

/// One exact named contribution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributionContribution {
    dimension: String,
    amount: MonetaryValue,
}

impl AttributionContribution {
    /// Returns dimension identifier.
    #[must_use]
    pub fn dimension(&self) -> &str {
        &self.dimension
    }

    /// Returns exact contribution amount.
    #[must_use]
    pub const fn amount(&self) -> MonetaryValue {
        self.amount
    }
}

/// Exact ordered attribution or stress result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PortfolioAttribution {
    contributions: Box<[AttributionContribution]>,
    total: MonetaryValue,
}

impl PortfolioAttribution {
    /// Returns contributions in allocation input order.
    #[must_use]
    pub fn contributions(&self) -> &[AttributionContribution] {
        &self.contributions
    }

    /// Returns exact total.
    #[must_use]
    pub const fn total(&self) -> MonetaryValue {
        self.total
    }
}

/// One exact scenario shock assigned to a named portfolio dimension.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioShock {
    dimension: String,
    return_shock: ExactRate,
}

impl ScenarioShock {
    /// Constructs one exact shock.
    ///
    /// # Errors
    ///
    /// Rejects an invalid dimension identifier or a price change below -100%.
    pub fn try_new(dimension: &str, return_shock: ExactRate) -> Result<Self, AnalyticsError> {
        validate_identifier(dimension)?;
        if return_shock.value() < -Decimal::ONE {
            return Err(AnalyticsError::ReturnBelowFloor);
        }
        Ok(Self {
            dimension: dimension.to_owned(),
            return_shock,
        })
    }

    /// Returns dimension identifier.
    #[must_use]
    pub fn dimension(&self) -> &str {
        &self.dimension
    }

    /// Returns exact return shock.
    #[must_use]
    pub const fn return_shock(&self) -> ExactRate {
        self.return_shock
    }
}

/// Rule for multiple shocks targeting the same dimension.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShockComposition {
    /// Sum shocks: `r1 + r2 + ...`.
    Additive,
    /// Apply sequentially: `(1 + r1) * (1 + r2) * ... - 1`.
    Compounded,
}

/// Exact gross and net portfolio exposure in one currency and measurement basis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortfolioExposure {
    net: MonetaryValue,
    gross: MonetaryValue,
}

impl PortfolioExposure {
    /// Starts an exact exposure total from one signed position value.
    #[must_use]
    pub fn from_value(value: MonetaryValue) -> Self {
        Self {
            net: value,
            gross: MonetaryValue::new(
                Money::new(value.money().amount().abs(), value.money().currency()),
                value.basis(),
            ),
        }
    }

    /// Adds one position without retaining individual allocations.
    ///
    /// # Errors
    ///
    /// Rejects mixed currencies/bases and unrepresentable exact net or gross addition.
    pub fn checked_add(self, value: MonetaryValue) -> Result<Self, AnalyticsError> {
        if value.money().currency() != self.net.money().currency() {
            return Err(AnalyticsError::CurrencyMismatch);
        }
        if value.basis() != self.net.basis() {
            return Err(AnalyticsError::MeasurementUnitMismatch);
        }
        let net = self
            .net
            .money()
            .checked_add(value.money())
            .map_err(|_| AnalyticsError::DecimalArithmetic)?;
        let gross = self
            .gross
            .money()
            .checked_add(Money::new(
                value.money().amount().abs(),
                value.money().currency(),
            ))
            .map_err(|_| AnalyticsError::DecimalArithmetic)?;
        Ok(Self {
            net: MonetaryValue::new(net, self.net.basis()),
            gross: MonetaryValue::new(gross, self.net.basis()),
        })
    }

    /// Returns signed net exposure.
    #[must_use]
    pub const fn net(self) -> MonetaryValue {
        self.net
    }

    /// Returns absolute gross exposure.
    #[must_use]
    pub const fn gross(self) -> MonetaryValue {
        self.gross
    }
}

/// Computes exact net and absolute gross exposure.
///
/// # Errors
///
/// Rejects empty/excessive input, mixed currencies/bases, or unrepresentable exact addition.
pub fn portfolio_exposure(
    allocations: &[PortfolioAllocation],
) -> Result<PortfolioExposure, AnalyticsError> {
    validate_count(allocations.len(), 1)?;
    allocations[1..].iter().try_fold(
        PortfolioExposure::from_value(allocations[0].market_value),
        |exposure, allocation| exposure.checked_add(allocation.market_value),
    )
}

/// Computes exact contribution `market_value * return_rate` for every allocation.
///
/// # Errors
///
/// Rejects empty/excessive input, mixed currencies/bases, or unrepresentable exact arithmetic.
pub fn portfolio_attribution(
    allocations: &[PortfolioAllocation],
) -> Result<PortfolioAttribution, AnalyticsError> {
    validate_count(allocations.len(), 1)?;
    let (currency, basis) = common_measurement(allocations)?;
    let contributions = allocations
        .iter()
        .map(|allocation| {
            allocation
                .market_value
                .money()
                .checked_mul_decimal(allocation.return_rate.value())
                .map(|amount| AttributionContribution {
                    dimension: allocation.dimension.clone(),
                    amount: MonetaryValue::new(amount, basis),
                })
                .map_err(|_| AnalyticsError::DecimalArithmetic)
        })
        .collect::<Result<Vec<_>, _>>()?;
    attribution_from_contributions(contributions, currency, basis)
}

/// Applies all shocks with an explicit composition rule and exact currency arithmetic.
///
/// Allocations without a shock contribute zero. Every supplied shock must map to at least one
/// allocation, preventing silent misspelling or stale scenario dimensions.
///
/// # Errors
///
/// Rejects empty/excessive allocations, excessive shocks, mixed currencies, unmapped shocks,
/// a composed price change below -100%, or unrepresentable exact composition/money arithmetic.
pub fn scenario_impact(
    allocations: &[PortfolioAllocation],
    shocks: &[ScenarioShock],
    composition: ShockComposition,
) -> Result<PortfolioAttribution, AnalyticsError> {
    validate_count(allocations.len(), 1)?;
    if shocks.len() > crate::MAX_BATCH_OBSERVATIONS {
        return Err(AnalyticsError::ObservationLimitExceeded);
    }
    let (currency, basis) = common_measurement(allocations)?;
    let mut by_dimension = BTreeMap::<&str, Vec<Decimal>>::new();
    for allocation in allocations {
        by_dimension.entry(&allocation.dimension).or_default();
    }
    for shock in shocks {
        by_dimension
            .get_mut(shock.dimension.as_str())
            .ok_or(AnalyticsError::UnknownShockDimension)?
            .push(shock.return_shock.value());
    }
    let rates = by_dimension
        .into_iter()
        .map(|(dimension, values)| {
            compose_shocks(values.into_iter(), composition, currency).map(|rate| (dimension, rate))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let contributions = allocations
        .iter()
        .map(|allocation| {
            let rate = rates
                .get(allocation.dimension.as_str())
                .copied()
                .ok_or(AnalyticsError::UnknownShockDimension)?;
            allocation
                .market_value
                .money()
                .checked_mul_decimal(rate)
                .map(|amount| AttributionContribution {
                    dimension: allocation.dimension.clone(),
                    amount: MonetaryValue::new(amount, basis),
                })
                .map_err(|_| AnalyticsError::DecimalArithmetic)
        })
        .collect::<Result<Vec<_>, _>>()?;
    attribution_from_contributions(contributions, currency, basis)
}

fn common_measurement(
    allocations: &[PortfolioAllocation],
) -> Result<(Currency, MonetaryBasis), AnalyticsError> {
    let currency = allocations[0].market_value.money().currency();
    let basis = allocations[0].market_value.basis();
    for allocation in allocations {
        if allocation.market_value.money().currency() != currency {
            return Err(AnalyticsError::CurrencyMismatch);
        }
        if allocation.market_value.basis() != basis {
            return Err(AnalyticsError::MeasurementUnitMismatch);
        }
    }
    Ok((currency, basis))
}

fn attribution_from_contributions(
    contributions: Vec<AttributionContribution>,
    currency: Currency,
    basis: MonetaryBasis,
) -> Result<PortfolioAttribution, AnalyticsError> {
    let total = contributions.iter().try_fold(
        Money::new(Decimal::ZERO, currency),
        |total, contribution| {
            total
                .checked_add(contribution.amount.money())
                .map_err(|_| AnalyticsError::DecimalArithmetic)
        },
    )?;
    Ok(PortfolioAttribution {
        contributions: contributions.into_boxed_slice(),
        total: MonetaryValue::new(total, basis),
    })
}

fn compose_shocks(
    mut shocks: impl Iterator<Item = Decimal>,
    composition: ShockComposition,
    currency: Currency,
) -> Result<Decimal, AnalyticsError> {
    // Apply the shocks to a one-unit reference price so composition reuses Money's exact
    // operators. Decimal's checked operators alone permit precision loss and underflow.
    let unit_price = Money::new(Decimal::ONE, currency);
    let rate = match composition {
        ShockComposition::Additive => shocks
            .try_fold(Money::new(Decimal::ZERO, currency), |total, shock| {
                total
                    .checked_add(Money::new(shock, currency))
                    .map_err(|_| AnalyticsError::DecimalArithmetic)
            })
            .map(|value| value.amount()),
        ShockComposition::Compounded => shocks
            .try_fold(unit_price, |price, shock| {
                unit_price
                    .checked_add(Money::new(shock, currency))
                    .and_then(|shock_price| price.checked_mul_decimal(shock_price.amount()))
                    .map_err(|_| AnalyticsError::DecimalArithmetic)
            })?
            .checked_sub(unit_price)
            .map(|value| value.amount())
            .map_err(|_| AnalyticsError::DecimalArithmetic),
    }?;
    if rate < -Decimal::ONE {
        return Err(AnalyticsError::ReturnBelowFloor);
    }
    Ok(rate)
}
