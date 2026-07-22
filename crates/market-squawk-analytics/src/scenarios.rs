//! Exact portfolio exposure, attribution, and composable scenario-stress kernels.

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
    /// Rejects an invalid dimension identifier.
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
    let (currency, basis) = common_measurement(allocations)?;
    let (net, gross) = allocations.iter().try_fold(
        (
            Money::new(Decimal::ZERO, currency),
            Money::new(Decimal::ZERO, currency),
        ),
        |(net, gross), allocation| {
            let market_value = allocation.market_value.money();
            let absolute = Money::new(market_value.amount().abs(), currency);
            let net = net
                .checked_add(market_value)
                .map_err(|_| AnalyticsError::DecimalArithmetic)?;
            let gross = gross
                .checked_add(absolute)
                .map_err(|_| AnalyticsError::DecimalArithmetic)?;
            Ok::<_, AnalyticsError>((net, gross))
        },
    )?;
    Ok(PortfolioExposure {
        net: MonetaryValue::new(net, basis),
        gross: MonetaryValue::new(gross, basis),
    })
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
/// Rejects empty/excessive allocations, excessive shocks, mixed currencies, unmapped shocks, or
/// unrepresentable exact composition/money arithmetic.
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
    if shocks.iter().any(|shock| {
        !allocations
            .iter()
            .any(|allocation| allocation.dimension == shock.dimension)
    }) {
        return Err(AnalyticsError::UnknownShockDimension);
    }
    let contributions = allocations
        .iter()
        .map(|allocation| {
            let rate = compose_shocks(
                shocks
                    .iter()
                    .filter(|shock| shock.dimension == allocation.dimension)
                    .map(|shock| shock.return_shock.value()),
                composition,
            )?;
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
) -> Result<Decimal, AnalyticsError> {
    match composition {
        ShockComposition::Additive => shocks
            .try_fold(Decimal::ZERO, |total, shock| {
                total
                    .checked_add(shock)
                    .ok_or(AnalyticsError::DecimalArithmetic)
            })
            .map(|value| value.normalize()),
        ShockComposition::Compounded => shocks
            .try_fold(Decimal::ONE, |factor, shock| {
                Decimal::ONE
                    .checked_add(shock)
                    .and_then(|shock_factor| factor.checked_mul(shock_factor))
                    .ok_or(AnalyticsError::DecimalArithmetic)
            })?
            .checked_sub(Decimal::ONE)
            .map(|value| value.normalize())
            .ok_or(AnalyticsError::DecimalArithmetic),
    }
}
