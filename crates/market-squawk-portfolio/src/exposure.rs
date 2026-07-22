//! Revision-bound allocation and multi-dimensional exposure.

use std::collections::{BTreeMap, BTreeSet};

use market_squawk_analytics::{
    ExactDecimalScale, ExactRate, FeatureKey, MonetaryBasis, MonetaryValue, PortfolioAllocation,
    portfolio_exposure,
};
use market_squawk_domain::{Currency, InstrumentId, Money, SourceIdentifier, VenueId};
use rust_decimal::Decimal;

use crate::{
    PortfolioError, PortfolioLimits, PortfolioRevision, PortfolioRevisionId, checked_decimal_mul,
};

/// Exact factor loading tied to the Task 12 canonical feature key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FactorLoading {
    key: FeatureKey,
    loading: ExactRate,
}

impl FactorLoading {
    /// Constructs a factor loading.
    ///
    /// # Errors
    ///
    /// Rejects no already-valid Task 12 value; the result type preserves API symmetry.
    pub const fn try_new(key: FeatureKey, loading: ExactRate) -> Result<Self, PortfolioError> {
        Ok(Self { key, loading })
    }

    /// Returns canonical feature key.
    pub const fn key(&self) -> &FeatureKey {
        &self.key
    }

    /// Returns exact dimensionless loading.
    pub const fn loading(&self) -> ExactRate {
        self.loading
    }
}

/// Point-in-time instrument classification used by exposure aggregation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstrumentClassification {
    instrument_id: InstrumentId,
    sector: SourceIdentifier,
    issuer: SourceIdentifier,
    venue: VenueId,
    currency: Currency,
    factors: Vec<FactorLoading>,
}

impl InstrumentClassification {
    /// Constructs one bounded classification with duplicate-free factors.
    ///
    /// # Errors
    ///
    /// Rejects duplicate factor keys.
    pub fn try_new(
        instrument_id: InstrumentId,
        sector: SourceIdentifier,
        issuer: SourceIdentifier,
        venue: VenueId,
        currency: Currency,
        mut factors: Vec<FactorLoading>,
    ) -> Result<Self, PortfolioError> {
        factors.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if factors.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(PortfolioError::InvalidDimension);
        }
        Ok(Self {
            instrument_id,
            sector,
            issuer,
            venue,
            currency,
            factors,
        })
    }

    /// Returns canonical instrument identity.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns source-authored sector identity.
    pub const fn sector(&self) -> &SourceIdentifier {
        &self.sector
    }

    /// Returns source-authored issuer identity.
    pub const fn issuer(&self) -> &SourceIdentifier {
        &self.issuer
    }

    /// Returns canonical venue identity.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns instrument denomination.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns Task 12 factor loadings.
    pub fn factors(&self) -> &[FactorLoading] {
        &self.factors
    }
}

/// One exact named exposure amount.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExposureLine {
    dimension: String,
    amount: Money,
}

impl ExposureLine {
    /// Returns canonical report-local dimension label.
    pub fn dimension(&self) -> &str {
        &self.dimension
    }

    /// Returns signed exact exposure.
    pub const fn amount(&self) -> Money {
        self.amount
    }
}

/// Complete revision-bound instrument, sector, factor, currency, issuer, and venue exposure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExposureReport {
    revision_id: PortfolioRevisionId,
    instrument: Vec<ExposureLine>,
    sector: Vec<ExposureLine>,
    factor: Vec<ExposureLine>,
    currency: Vec<ExposureLine>,
    issuer: Vec<ExposureLine>,
    venue: Vec<ExposureLine>,
    allocation_total: Money,
    net: Money,
    gross: Money,
}

impl ExposureReport {
    /// Aggregates every required dimension and uses the Task 12 exact exposure kernel.
    ///
    /// # Errors
    ///
    /// Rejects absent/duplicate classifications, excessive output, or checked arithmetic failure.
    pub fn try_calculate(
        revision: &PortfolioRevision,
        classifications: &[InstrumentClassification],
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if classifications.len() > limits.max_instruments {
            return Err(PortfolioError::LimitExceeded {
                resource: "classifications",
                observed: classifications.len(),
                limit: limits.max_instruments,
            });
        }
        let by_instrument = classifications
            .iter()
            .map(|classification| (classification.instrument_id, classification))
            .collect::<BTreeMap<_, _>>();
        if by_instrument.len() != classifications.len()
            || revision
                .positions()
                .iter()
                .any(|position| !by_instrument.contains_key(&position.instrument_id()))
        {
            return Err(PortfolioError::InvalidDimension);
        }
        let mut instrument = Vec::new();
        let mut sector = BTreeMap::new();
        let mut factor = BTreeMap::new();
        let mut currency = BTreeMap::new();
        let mut issuer = BTreeMap::new();
        let mut venue = BTreeMap::new();
        let mut allocations = Vec::new();
        for position in revision.positions() {
            let classification = by_instrument
                .get(&position.instrument_id())
                .ok_or(PortfolioError::InvalidDimension)?;
            let value = position.market_value();
            let dimension = instrument_dimension(position.instrument_id());
            instrument.push(ExposureLine {
                dimension: dimension.clone(),
                amount: value,
            });
            allocations.push(
                PortfolioAllocation::try_new(
                    &dimension,
                    MonetaryValue::new(value, MonetaryBasis::Total),
                    ExactRate::try_new(Decimal::ZERO, ExactDecimalScale::Unit)
                        .map_err(|_| PortfolioError::Analytics)?,
                )
                .map_err(|_| PortfolioError::Analytics)?,
            );
            aggregate(&mut sector, classification.sector.as_str(), value)?;
            aggregate(
                &mut currency,
                &classification.currency.as_str().to_ascii_lowercase(),
                value,
            )?;
            aggregate(&mut issuer, classification.issuer.as_str(), value)?;
            aggregate(
                &mut venue,
                &classification.venue.as_str().to_ascii_lowercase(),
                value,
            )?;
            for loading in &classification.factors {
                let amount = Money::new(
                    checked_decimal_mul(value.amount(), loading.loading.value())?,
                    value.currency(),
                );
                aggregate(
                    &mut factor,
                    &format!("{}-{}", loading.key.name(), loading.key.version()),
                    amount,
                )?;
            }
        }
        let factor_keys = classifications
            .iter()
            .flat_map(|classification| classification.factors.iter().map(|factor| &factor.key))
            .collect::<BTreeSet<_>>();
        if factor_keys.len() > limits.max_factors {
            return Err(PortfolioError::LimitExceeded {
                resource: "factors",
                observed: factor_keys.len(),
                limit: limits.max_factors,
            });
        }
        let total_lines = instrument
            .len()
            .saturating_add(sector.len())
            .saturating_add(factor.len())
            .saturating_add(currency.len())
            .saturating_add(issuer.len())
            .saturating_add(venue.len());
        if total_lines > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "exposure results",
                observed: total_lines,
                limit: limits.max_results,
            });
        }
        let kernel = portfolio_exposure(&allocations).map_err(|_| PortfolioError::Analytics)?;
        let allocation_total = allocations.iter().try_fold(
            Money::new(Decimal::ZERO, revision.base_currency()),
            |total, allocation| {
                total
                    .checked_add(allocation.market_value().money())
                    .map_err(|_| PortfolioError::Arithmetic)
            },
        )?;
        Ok(Self {
            revision_id: revision.id(),
            instrument,
            sector: lines(sector),
            factor: lines(factor),
            currency: lines(currency),
            issuer: lines(issuer),
            venue: lines(venue),
            allocation_total,
            net: kernel.net().money(),
            gross: kernel.gross().money(),
        })
    }

    /// Returns immutable revision identity.
    pub const fn revision_id(&self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns instrument exposures.
    pub fn instrument(&self) -> &[ExposureLine] {
        &self.instrument
    }

    /// Returns sector exposures.
    pub fn sector(&self) -> &[ExposureLine] {
        &self.sector
    }

    /// Returns factor exposures.
    pub fn factor(&self) -> &[ExposureLine] {
        &self.factor
    }

    /// Returns currency exposures.
    pub fn currency(&self) -> &[ExposureLine] {
        &self.currency
    }

    /// Returns issuer exposures.
    pub fn issuer(&self) -> &[ExposureLine] {
        &self.issuer
    }

    /// Returns venue exposures.
    pub fn venue(&self) -> &[ExposureLine] {
        &self.venue
    }

    /// Returns signed allocation total.
    pub const fn allocation_total(&self) -> Money {
        self.allocation_total
    }

    /// Returns Task 12 net exposure.
    pub const fn net(&self) -> Money {
        self.net
    }

    /// Returns Task 12 gross exposure.
    pub const fn gross(&self) -> Money {
        self.gross
    }
}

pub(crate) fn instrument_dimension(instrument_id: InstrumentId) -> String {
    format!("instrument-{instrument_id}")
}

fn aggregate(
    values: &mut BTreeMap<String, Money>,
    key: &str,
    amount: Money,
) -> Result<(), PortfolioError> {
    if let Some(current) = values.get(key).copied() {
        values.insert(
            key.to_owned(),
            current
                .checked_add(amount)
                .map_err(|_| PortfolioError::Arithmetic)?,
        );
    } else {
        values.insert(key.to_owned(), amount);
    }
    Ok(())
}

fn lines(values: BTreeMap<String, Money>) -> Vec<ExposureLine> {
    values
        .into_iter()
        .map(|(dimension, amount)| ExposureLine { dimension, amount })
        .collect()
}
