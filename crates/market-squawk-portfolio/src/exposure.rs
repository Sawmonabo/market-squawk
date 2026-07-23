//! Revision-bound allocation and multi-dimensional exposure.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use market_squawk_analytics::{
    ExactDecimalScale, ExactRate, FeatureKey, MAX_FEATURE_NAME_BYTES, MonetaryBasis, MonetaryValue,
    PortfolioAllocation, portfolio_exposure,
};
use market_squawk_data::Sha256Digest;
use market_squawk_domain::{Currency, InstrumentId, Money, SourceIdentifier, VenueId};
use rust_decimal::Decimal;

use crate::{
    PortfolioAnalyticsEvidence, PortfolioError, PortfolioLimits, PortfolioRevision,
    PortfolioRevisionId, admit_retained_bytes, checked_decimal_mul, checked_usize_add,
    checked_usize_mul,
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
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if factors.len() > limits.max_factors {
            return Err(PortfolioError::LimitExceeded {
                resource: "classification factors",
                observed: factors.len(),
                limit: limits.max_factors,
            });
        }
        let retained_bytes = [
            std::mem::size_of::<Self>(),
            sector.retained_bytes(),
            issuer.retained_bytes(),
            venue.retained_bytes(),
            checked_usize_mul(factors.capacity(), std::mem::size_of::<FactorLoading>())?,
            checked_usize_mul(factors.len(), MAX_FEATURE_NAME_BYTES)?,
        ]
        .into_iter()
        .try_fold(0_usize, checked_usize_add)?;
        admit_retained_bytes(retained_bytes, limits)?;
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
    analytics_evidence_digest: Sha256Digest,
    instrument: Vec<ExposureLine>,
    sector: Vec<ExposureLine>,
    factor: Vec<ExposureLine>,
    currency: Vec<ExposureLine>,
    issuer: Vec<ExposureLine>,
    venue: Vec<ExposureLine>,
    allocation_total: Money,
    net: Money,
    gross: Money,
    retained_bytes: usize,
}

impl ExposureReport {
    /// Aggregates every required dimension and uses the Task 12 exact exposure kernel.
    ///
    /// # Errors
    ///
    /// Rejects absent/duplicate classifications, excessive output, or checked arithmetic failure.
    pub fn try_calculate(
        revision: &PortfolioRevision,
        analytics_evidence: &PortfolioAnalyticsEvidence,
        classifications: &[InstrumentClassification],
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        let report_through = revision.evidence().as_of();
        analytics_evidence.validate_report(revision, report_through, report_through)?;
        let positions = revision.positions().len();
        if classifications.len() > limits.max_instruments || positions > limits.max_instruments {
            return Err(PortfolioError::LimitExceeded {
                resource: "classifications",
                observed: classifications.len().max(positions),
                limit: limits.max_instruments,
            });
        }
        let factor_occurrences =
            classifications
                .iter()
                .try_fold(0_usize, |total, classification| {
                    if classification.factors.len() > limits.max_factors {
                        return Err(PortfolioError::LimitExceeded {
                            resource: "classification factors",
                            observed: classification.factors.len(),
                            limit: limits.max_factors,
                        });
                    }
                    checked_usize_add(total, classification.factors.len())
                })?;
        if factor_occurrences > limits.max_factors {
            return Err(PortfolioError::LimitExceeded {
                resource: "factor occurrences",
                observed: factor_occurrences,
                limit: limits.max_factors,
            });
        }
        if factor_occurrences > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "factor occurrences",
                observed: factor_occurrences,
                limit: limits.max_results,
            });
        }
        let worst_lines = checked_usize_add(checked_usize_mul(positions, 5)?, factor_occurrences)?;
        if worst_lines > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "exposure results",
                observed: worst_lines,
                limit: limits.max_results,
            });
        }
        let factor_index_rows = factor_occurrences;
        let work_rows = [
            worst_lines,
            positions,
            classifications.len(),
            factor_index_rows,
        ]
        .into_iter()
        .try_fold(0_usize, checked_usize_add)?;
        if work_rows > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "exposure work",
                observed: work_rows,
                limit: limits.max_results,
            });
        }
        admit_retained_bytes(
            exposure_retained_preflight(positions, factor_occurrences, worst_lines)?,
            limits,
        )?;
        let mut factor_keys = Vec::new();
        factor_keys
            .try_reserve_exact(factor_occurrences)
            .map_err(|_| PortfolioError::AllocationFailed)?;
        factor_keys.extend(
            classifications
                .iter()
                .flat_map(|classification| classification.factors.iter().map(|factor| &factor.key)),
        );
        factor_keys.sort_unstable();
        factor_keys.dedup();
        if factor_keys.len() > limits.max_factors {
            return Err(PortfolioError::LimitExceeded {
                resource: "factors",
                observed: factor_keys.len(),
                limit: limits.max_factors,
            });
        }
        let mut by_instrument = Vec::new();
        by_instrument
            .try_reserve_exact(classifications.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        by_instrument.extend(classifications);
        by_instrument.sort_unstable_by_key(|classification| classification.instrument_id);
        if by_instrument
            .windows(2)
            .any(|pair| pair[0].instrument_id == pair[1].instrument_id)
            || revision.positions().iter().any(|position| {
                by_instrument
                    .binary_search_by_key(&position.instrument_id(), |classification| {
                        classification.instrument_id
                    })
                    .is_err()
            })
        {
            return Err(PortfolioError::InvalidDimension);
        }
        let mut instrument = Vec::new();
        instrument
            .try_reserve_exact(positions)
            .map_err(|_| PortfolioError::AllocationFailed)?;
        let mut sector = BTreeMap::new();
        let mut factor = BTreeMap::new();
        let mut currency = BTreeMap::new();
        let mut issuer = BTreeMap::new();
        let mut venue = BTreeMap::new();
        let mut allocations = Vec::new();
        allocations
            .try_reserve_exact(positions)
            .map_err(|_| PortfolioError::AllocationFailed)?;
        for position in revision.positions() {
            let classification = by_instrument
                .binary_search_by_key(&position.instrument_id(), |classification| {
                    classification.instrument_id
                })
                .ok()
                .and_then(|index| by_instrument.get(index))
                .ok_or(PortfolioError::InvalidDimension)?;
            let value = position.market_value();
            let dimension = try_instrument_dimension(position.instrument_id())?;
            allocations.push(
                PortfolioAllocation::try_new(
                    &dimension,
                    MonetaryValue::new(value, MonetaryBasis::Total),
                    ExactRate::try_new(Decimal::ZERO, ExactDecimalScale::Unit)
                        .map_err(|_| PortfolioError::Analytics)?,
                )
                .map_err(|_| PortfolioError::Analytics)?,
            );
            instrument.push(ExposureLine {
                dimension,
                amount: value,
            });
            aggregate(&mut sector, classification.sector.as_str(), value)?;
            aggregate_owned(
                &mut currency,
                try_ascii_lowercase(classification.currency.as_str())?,
                value,
            )?;
            aggregate(&mut issuer, classification.issuer.as_str(), value)?;
            aggregate_owned(
                &mut venue,
                try_ascii_lowercase(classification.venue.as_str())?,
                value,
            )?;
            for loading in &classification.factors {
                let amount = Money::new(
                    checked_decimal_mul(value.amount(), loading.loading.value())?,
                    value.currency(),
                );
                aggregate_owned(&mut factor, try_factor_dimension(&loading.key)?, amount)?;
            }
        }
        let total_lines = [
            instrument.len(),
            sector.len(),
            factor.len(),
            currency.len(),
            issuer.len(),
            venue.len(),
        ]
        .into_iter()
        .try_fold(0_usize, checked_usize_add)?;
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
        let sector = lines(sector)?;
        let factor = lines(factor)?;
        let currency = lines(currency)?;
        let issuer = lines(issuer)?;
        let venue = lines(venue)?;
        let retained_bytes = exposure_retained_bytes([
            (&instrument, instrument.capacity()),
            (&sector, sector.capacity()),
            (&factor, factor.capacity()),
            (&currency, currency.capacity()),
            (&issuer, issuer.capacity()),
            (&venue, venue.capacity()),
        ])?;
        admit_retained_bytes(retained_bytes, limits)?;
        Ok(Self {
            revision_id: revision.id(),
            analytics_evidence_digest: analytics_evidence.semantic_digest(),
            instrument,
            sector,
            factor,
            currency,
            issuer,
            venue,
            allocation_total,
            net: kernel.net().money(),
            gross: kernel.gross().money(),
            retained_bytes,
        })
    }

    /// Returns immutable revision identity.
    pub const fn revision_id(&self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns the exact point-in-time analytics authority digest.
    pub const fn analytics_evidence_digest(&self) -> Sha256Digest {
        self.analytics_evidence_digest
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

    /// Returns exact Rust-visible bytes retained by this report.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

pub(crate) fn try_instrument_dimension(
    instrument_id: InstrumentId,
) -> Result<String, PortfolioError> {
    let mut dimension = String::new();
    dimension
        .try_reserve_exact("instrument-".len() + 36)
        .map_err(|_| PortfolioError::AllocationFailed)?;
    write!(&mut dimension, "instrument-{instrument_id}")
        .map_err(|_| PortfolioError::AllocationFailed)?;
    Ok(dimension)
}

fn aggregate(
    values: &mut BTreeMap<String, Money>,
    key: &str,
    amount: Money,
) -> Result<(), PortfolioError> {
    if let Some(current) = values.get_mut(key) {
        *current = current
            .checked_add(amount)
            .map_err(|_| PortfolioError::Arithmetic)?;
    } else {
        values.insert(try_owned_string(key)?, amount);
    }
    Ok(())
}

fn aggregate_owned(
    values: &mut BTreeMap<String, Money>,
    key: String,
    amount: Money,
) -> Result<(), PortfolioError> {
    if let Some(current) = values.get_mut(key.as_str()) {
        *current = current
            .checked_add(amount)
            .map_err(|_| PortfolioError::Arithmetic)?;
    } else {
        values.insert(key, amount);
    }
    Ok(())
}

fn lines(values: BTreeMap<String, Money>) -> Result<Vec<ExposureLine>, PortfolioError> {
    let mut lines = Vec::new();
    lines
        .try_reserve_exact(values.len())
        .map_err(|_| PortfolioError::AllocationFailed)?;
    lines.extend(
        values
            .into_iter()
            .map(|(dimension, amount)| ExposureLine { dimension, amount }),
    );
    Ok(lines)
}

fn try_owned_string(value: &str) -> Result<String, PortfolioError> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| PortfolioError::AllocationFailed)?;
    owned.push_str(value);
    Ok(owned)
}

fn try_ascii_lowercase(value: &str) -> Result<String, PortfolioError> {
    let mut lowercase = String::new();
    lowercase
        .try_reserve_exact(value.len())
        .map_err(|_| PortfolioError::AllocationFailed)?;
    lowercase.extend(
        value
            .bytes()
            .map(|byte| char::from(byte.to_ascii_lowercase())),
    );
    Ok(lowercase)
}

fn try_factor_dimension(key: &FeatureKey) -> Result<String, PortfolioError> {
    let capacity = checked_usize_add(key.name().len(), 11)?;
    let mut dimension = String::new();
    dimension
        .try_reserve_exact(capacity)
        .map_err(|_| PortfolioError::AllocationFailed)?;
    write!(&mut dimension, "{}-{}", key.name(), key.version())
        .map_err(|_| PortfolioError::AllocationFailed)?;
    Ok(dimension)
}

fn exposure_retained_preflight(
    positions: usize,
    factor_occurrences: usize,
    worst_lines: usize,
) -> Result<usize, PortfolioError> {
    let fixed_and_lines = checked_usize_add(
        std::mem::size_of::<ExposureReport>(),
        checked_usize_mul(worst_lines, std::mem::size_of::<ExposureLine>())?,
    )?;
    [
        fixed_and_lines,
        checked_usize_mul(positions, "instrument-".len() + 36)?,
        checked_usize_mul(positions, SourceIdentifier::MAX_LENGTH)?,
        checked_usize_mul(positions, 3)?,
        checked_usize_mul(positions, SourceIdentifier::MAX_LENGTH)?,
        checked_usize_mul(positions, VenueId::MAX_LENGTH)?,
        checked_usize_mul(factor_occurrences, MAX_FEATURE_NAME_BYTES + 11)?,
    ]
    .into_iter()
    .try_fold(0_usize, checked_usize_add)
}

fn exposure_retained_bytes(groups: [(&[ExposureLine], usize); 6]) -> Result<usize, PortfolioError> {
    groups.into_iter().try_fold(
        std::mem::size_of::<ExposureReport>(),
        |retained, (lines, capacity)| {
            let retained = checked_usize_add(
                retained,
                checked_usize_mul(capacity, std::mem::size_of::<ExposureLine>())?,
            )?;
            lines.iter().try_fold(retained, |retained, line| {
                checked_usize_add(retained, line.dimension.capacity())
            })
        },
    )
}
