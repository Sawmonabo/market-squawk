//! Immutable, manifest-pinned analytical inputs and feature contracts.

use market_squawk_analytics::{
    AnalyticsError, BatchFeatureCatalog, DatedMoney, DatedStatisticalInput, DecimalPolicy,
    FactorObservation, PortfolioAllocation, ScenarioShock, ShockComposition, factor_regression,
    scenario_impact, simple_returns, total_returns, valuation_multiple,
};
use market_squawk_data::PinnedDataset;
use market_squawk_domain::{
    DataQuality, InstrumentId, Money, SourceId, SourceIdentifier, Timestamp,
};
use thiserror::Error;

const MAXIMUM_REGISTERED_ANALYSIS_DATASETS: usize = 4_096;

/// Exact requestable scope of one already selected point-in-time analytical input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalysisDatasetScope {
    instruments: Box<[InstrumentId]>,
    starts_at: Timestamp,
    ends_at: Timestamp,
    sources: Box<[SourceId]>,
    qualities: Box<[DataQuality]>,
}

impl AnalysisDatasetScope {
    /// Constructs canonical source, instrument, time, and quality evidence.
    ///
    /// # Errors
    ///
    /// Rejects an empty or duplicate source/quality set, duplicate instruments, or an empty time
    /// interval.
    pub fn try_new(
        mut instruments: Vec<InstrumentId>,
        starts_at: Timestamp,
        ends_at: Timestamp,
        mut sources: Vec<SourceId>,
        mut qualities: Vec<DataQuality>,
    ) -> Result<Self, AnalysisCatalogError> {
        if starts_at >= ends_at || sources.is_empty() || qualities.is_empty() {
            return Err(AnalysisCatalogError::InvalidScope);
        }
        instruments.sort_unstable();
        if instruments.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AnalysisCatalogError::InvalidScope);
        }
        sources.sort_unstable();
        if sources.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AnalysisCatalogError::InvalidScope);
        }
        qualities.sort_unstable_by_key(|quality| quality_rank(*quality));
        if qualities.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AnalysisCatalogError::InvalidScope);
        }
        Ok(Self {
            instruments: instruments.into_boxed_slice(),
            starts_at,
            ends_at,
            sources: sources.into_boxed_slice(),
            qualities: qualities.into_boxed_slice(),
        })
    }

    pub(super) fn instruments(&self) -> &[InstrumentId] {
        &self.instruments
    }

    pub(super) const fn starts_at(&self) -> Timestamp {
        self.starts_at
    }

    pub(super) const fn ends_at(&self) -> Timestamp {
        self.ends_at
    }

    pub(super) fn sources(&self) -> &[SourceId] {
        &self.sources
    }

    pub(super) fn qualities(&self) -> &[DataQuality] {
        &self.qualities
    }
}

const fn quality_rank(quality: DataQuality) -> u8 {
    match quality {
        DataQuality::DirectVerified => 0,
        DataQuality::DirectUnverified => 1,
        DataQuality::OfficialDelayed => 2,
        DataQuality::Aggregated => 3,
        DataQuality::Indicative => 4,
        DataQuality::Modeled => 5,
        DataQuality::Estimated => 6,
        DataQuality::Stale => 7,
        DataQuality::Quarantined => 8,
    }
}

/// Exact price-only or price-plus-distribution return input.
#[derive(Clone, Debug, PartialEq)]
pub enum ReturnAnalysisInput {
    /// Statistical prices for simple holding-period returns.
    Simple(Box<[DatedStatisticalInput]>),
    /// Exact prices and interval distributions for total holding-period returns.
    Total {
        /// Strictly increasing exact prices.
        prices: Box<[DatedMoney]>,
        /// Cash distributions corresponding to each holding interval.
        distributions: Box<[Money]>,
    },
}

impl ReturnAnalysisInput {
    pub(super) fn calculate(
        &self,
    ) -> Result<market_squawk_analytics::StatisticalSeries, AnalyticsError> {
        match self {
            Self::Simple(prices) => simple_returns(prices),
            Self::Total {
                prices,
                distributions,
            } => total_returns(prices, distributions),
        }
    }

    fn validate_time(&self, scope: &AnalysisDatasetScope) -> Result<(), AnalysisCatalogError> {
        let within_scope = match self {
            Self::Simple(prices) => prices
                .iter()
                .all(|price| price.at() >= scope.starts_at() && price.at() <= scope.ends_at()),
            Self::Total { prices, .. } => prices
                .iter()
                .all(|price| price.at() >= scope.starts_at() && price.at() <= scope.ends_at()),
        };
        within_scope
            .then_some(())
            .ok_or(AnalysisCatalogError::InvalidScope)
    }
}

/// Ordered factor names and response/factor rows for one regression.
#[derive(Clone, Debug, PartialEq)]
pub struct FactorAnalysisInput {
    names: Box<[SourceIdentifier]>,
    observations: Box<[FactorObservation]>,
}

impl FactorAnalysisInput {
    /// Binds ordered factor identities to rows of the same width.
    pub fn try_new(
        names: Vec<SourceIdentifier>,
        observations: Vec<FactorObservation>,
    ) -> Result<Self, AnalysisCatalogError> {
        if names.is_empty()
            || observations.is_empty()
            || observations
                .iter()
                .any(|observation| observation.factors().len() != names.len())
            || names
                .iter()
                .enumerate()
                .any(|(index, name)| names[index + 1..].contains(name))
        {
            return Err(AnalysisCatalogError::InvalidFactorInput);
        }
        factor_regression(&observations)?;
        Ok(Self {
            names: names.into_boxed_slice(),
            observations: observations.into_boxed_slice(),
        })
    }

    pub(super) fn names(&self) -> &[SourceIdentifier] {
        &self.names
    }

    pub(super) fn observations(&self) -> &[FactorObservation] {
        &self.observations
    }
}

/// Exact monetary inputs and rounding policy for one valuation multiple.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValuationAnalysisInput {
    market_value: market_squawk_analytics::MonetaryValue,
    metric: market_squawk_analytics::MonetaryValue,
    policy: DecimalPolicy,
}

impl ValuationAnalysisInput {
    /// Validates one exact valuation-multiple input.
    pub fn try_new(
        market_value: market_squawk_analytics::MonetaryValue,
        metric: market_squawk_analytics::MonetaryValue,
        policy: DecimalPolicy,
    ) -> Result<Self, AnalysisCatalogError> {
        valuation_multiple(market_value, metric, policy)?;
        Ok(Self {
            market_value,
            metric,
            policy,
        })
    }

    pub(super) fn calculate(
        self,
    ) -> Result<market_squawk_analytics::ExactDecimalResult, AnalyticsError> {
        valuation_multiple(self.market_value, self.metric, self.policy)
    }
}

/// Exact allocations and shocks for one scenario calculation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioAnalysisInput {
    allocations: Box<[PortfolioAllocation]>,
    shocks: Box<[ScenarioShock]>,
    composition: ShockComposition,
}

impl ScenarioAnalysisInput {
    /// Validates one scenario through the production exact-money kernel.
    pub fn try_new(
        allocations: Vec<PortfolioAllocation>,
        shocks: Vec<ScenarioShock>,
        composition: ShockComposition,
    ) -> Result<Self, AnalysisCatalogError> {
        scenario_impact(&allocations, &shocks, composition)?;
        Ok(Self {
            allocations: allocations.into_boxed_slice(),
            shocks: shocks.into_boxed_slice(),
            composition,
        })
    }

    pub(super) fn calculate(
        &self,
    ) -> Result<market_squawk_analytics::PortfolioAttribution, AnalyticsError> {
        scenario_impact(&self.allocations, &self.shocks, self.composition)
    }
}

/// Manifest-pinned typed inputs for the analytical operations currently supported by one dataset.
#[derive(Clone, Debug)]
pub struct AnalysisDataset {
    pinned: PinnedDataset,
    scope: AnalysisDatasetScope,
    returns: Option<ReturnAnalysisInput>,
    factors: Option<FactorAnalysisInput>,
    valuation: Option<ValuationAnalysisInput>,
    scenario: Option<ScenarioAnalysisInput>,
}

impl AnalysisDataset {
    /// Registers typed analytical inputs under one catalog-resolved immutable generation.
    ///
    /// # Errors
    ///
    /// At least one kernel input must be present. Every present input is evaluated once during
    /// registration so an invalid dataset never becomes requestable.
    pub fn try_new(
        pinned: PinnedDataset,
        scope: AnalysisDatasetScope,
        returns: Option<ReturnAnalysisInput>,
        factors: Option<FactorAnalysisInput>,
        valuation: Option<ValuationAnalysisInput>,
        scenario: Option<ScenarioAnalysisInput>,
    ) -> Result<Self, AnalysisCatalogError> {
        if returns.is_none() && factors.is_none() && valuation.is_none() && scenario.is_none() {
            return Err(AnalysisCatalogError::EmptyDataset);
        }
        if let Some(input) = &returns {
            input.validate_time(&scope)?;
            input.calculate()?;
        }
        if let Some(input) = &factors {
            factor_regression(input.observations())?;
        }
        if let Some(input) = valuation {
            input.calculate()?;
        }
        if let Some(input) = &scenario {
            input.calculate()?;
        }
        Ok(Self {
            pinned,
            scope,
            returns,
            factors,
            valuation,
            scenario,
        })
    }

    pub(super) const fn pinned(&self) -> &PinnedDataset {
        &self.pinned
    }

    pub(super) const fn scope(&self) -> &AnalysisDatasetScope {
        &self.scope
    }

    pub(super) const fn returns(&self) -> Option<&ReturnAnalysisInput> {
        self.returns.as_ref()
    }

    pub(super) const fn factors(&self) -> Option<&FactorAnalysisInput> {
        self.factors.as_ref()
    }

    pub(super) const fn valuation(&self) -> Option<ValuationAnalysisInput> {
        self.valuation
    }

    pub(super) const fn scenario(&self) -> Option<&ScenarioAnalysisInput> {
        self.scenario.as_ref()
    }
}

/// Immutable application catalog used by all read-only analysis operations.
#[derive(Clone, Debug)]
pub struct AnalysisCatalog {
    datasets: Box<[AnalysisDataset]>,
    feature_catalog: BatchFeatureCatalog,
}

impl AnalysisCatalog {
    /// Canonicalizes a bounded set with one active immutable generation per dataset identity.
    pub fn try_new(
        mut datasets: Vec<AnalysisDataset>,
        feature_catalog: BatchFeatureCatalog,
    ) -> Result<Self, AnalysisCatalogError> {
        if datasets.len() > MAXIMUM_REGISTERED_ANALYSIS_DATASETS {
            return Err(AnalysisCatalogError::Capacity);
        }
        datasets.sort_unstable_by(|left, right| {
            left.pinned()
                .manifest()
                .dataset_id()
                .as_str()
                .cmp(right.pinned().manifest().dataset_id().as_str())
        });
        if datasets.windows(2).any(|pair| {
            pair[0].pinned().manifest().dataset_id() == pair[1].pinned().manifest().dataset_id()
        }) {
            return Err(AnalysisCatalogError::DuplicateDataset);
        }
        Ok(Self {
            datasets: datasets.into_boxed_slice(),
            feature_catalog,
        })
    }

    pub(super) fn dataset(&self, id: &str) -> Option<&AnalysisDataset> {
        self.datasets
            .binary_search_by(|candidate| {
                candidate.pinned().manifest().dataset_id().as_str().cmp(id)
            })
            .ok()
            .and_then(|index| self.datasets.get(index))
    }

    pub(super) const fn feature_catalog(&self) -> &BatchFeatureCatalog {
        &self.feature_catalog
    }
}

/// Invalid immutable analytical catalog construction.
#[derive(Clone, Debug, Error, PartialEq)]
pub enum AnalysisCatalogError {
    /// Scope evidence is empty, duplicated, or temporally invalid.
    #[error("analysis dataset scope is invalid")]
    InvalidScope,
    /// Factor identities do not match the registered observation width.
    #[error("analysis factor input is invalid")]
    InvalidFactorInput,
    /// No analytical kernel can consume the registration.
    #[error("analysis dataset has no registered kernel input")]
    EmptyDataset,
    /// Too many immutable generations were registered.
    #[error("analysis catalog capacity was exceeded")]
    Capacity,
    /// More than one active generation claimed one request dataset identity.
    #[error("analysis catalog contains duplicate dataset identities")]
    DuplicateDataset,
    /// A production analytical kernel rejected the input.
    #[error("analysis kernel rejected the registered input: {0}")]
    Analytics(#[from] AnalyticsError),
}
