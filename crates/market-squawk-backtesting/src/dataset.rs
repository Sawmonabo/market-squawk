//! Non-forgeable point-in-time dataset admission and canonical event-time observations.

use std::mem::size_of;
use std::num::NonZeroU32;

use market_squawk_data::{
    DatasetManifestRef, DatasetSchemaRegistry, PinnedInstrumentDefinitions, PinnedQueryOutput,
    Sha256Digest,
};
use market_squawk_domain::{
    BasisPoints, Denomination, InstrumentExecutionTerms, InstrumentId, PriceTicks, QuantityLots,
    SourceIdentifier, Timestamp,
};
use sha2::{Digest as _, Sha256};

use crate::engine::BacktestError;

mod admission;

pub use admission::{
    AVAILABLE_AT_COMPONENT, DEPTH_COMPONENT, EVENT_AT_COMPONENT, MID_PRICE_COMPONENT,
    SPREAD_COMPONENT, STALE_AT_COMPONENT, UNIVERSE_COMPONENT,
};

const HARD_MAX_OBSERVATIONS: usize = 1_000_000;
const HARD_MAX_PENDING_INTENTS: usize = 65_536;
const HARD_MAX_FILLS: usize = 1_000_000;
const HARD_MAX_RETAINED_BYTES: usize = 512 * 1024 * 1024;

/// Historical eligibility carried by the exact Task 11 universe at an observation cutoff.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HistoricalUniverseStatus {
    /// The instrument belonged to the historical universe at this cutoff.
    Eligible,
    /// The instrument was outside the historical universe at this cutoff.
    Ineligible,
    /// The instrument was terminally delisted at or before this cutoff.
    Delisted,
}

/// One finite research feature available to a strategy at the current cutoff.
#[derive(Clone, Debug, PartialEq)]
pub struct ResearchFeatureValue {
    pub(crate) name: SourceIdentifier,
    version: NonZeroU32,
    value: f64,
}

impl ResearchFeatureValue {
    /// Constructs a named finite feature value.
    pub fn try_new(
        name: SourceIdentifier,
        version: NonZeroU32,
        value: f64,
    ) -> Result<Self, BacktestError> {
        if !value.is_finite() {
            return Err(BacktestError::InvalidObservation);
        }
        Ok(Self {
            name,
            version,
            value,
        })
    }

    /// Returns the stable feature name.
    #[must_use]
    pub const fn name(&self) -> &SourceIdentifier {
        &self.name
    }

    /// Returns the nonzero producer semantic version.
    #[must_use]
    pub const fn version(&self) -> NonZeroU32 {
        self.version
    }

    /// Returns the finite feature value.
    #[must_use]
    pub const fn value(&self) -> f64 {
        self.value
    }
}

/// Internal point-in-time observation input produced only by pinned admission or test fixtures.
#[derive(Clone, Debug)]
pub(crate) struct BacktestObservationInput {
    pub execution_terms: InstrumentExecutionTerms,
    pub event_at: Timestamp,
    pub available_at: Timestamp,
    pub decision_at: Timestamp,
    pub stale_at: Timestamp,
    pub mid_price: Option<PriceTicks>,
    pub spread_basis_points: BasisPoints,
    pub executable_depth: QuantityLots,
    pub universe: HistoricalUniverseStatus,
    pub features: Vec<ResearchFeatureValue>,
    pub lineage_digest: Sha256Digest,
}

/// One manifest-bound observation with conservative availability and freshness semantics.
#[derive(Clone, Debug)]
pub struct BacktestObservation {
    pub(crate) execution_terms: InstrumentExecutionTerms,
    event_at: Timestamp,
    available_at: Timestamp,
    pub(crate) decision_at: Timestamp,
    pub(crate) stale_at: Timestamp,
    pub(crate) mid_price: Option<PriceTicks>,
    pub(crate) spread_basis_points: BasisPoints,
    pub(crate) executable_depth: QuantityLots,
    pub(crate) universe: HistoricalUniverseStatus,
    pub(crate) features: Box<[ResearchFeatureValue]>,
    pub(crate) lineage_digest: Sha256Digest,
}

impl BacktestObservation {
    pub(crate) fn try_new(mut input: BacktestObservationInput) -> Result<Self, BacktestError> {
        if input.event_at > input.available_at
            || input.available_at > input.decision_at
            || input.stale_at < input.decision_at
            || !(0..=10_000).contains(&input.spread_basis_points.get())
            || input.mid_price.is_some_and(|price| price.get() <= 0)
            || input.lineage_digest.bytes() == [0; 32]
        {
            return Err(BacktestError::InvalidObservation);
        }
        input
            .features
            .sort_unstable_by(|left, right| left.name.cmp(&right.name));
        if input
            .features
            .windows(2)
            .any(|pair| pair[0].name == pair[1].name)
        {
            return Err(BacktestError::InvalidObservation);
        }
        Ok(Self {
            execution_terms: input.execution_terms,
            event_at: input.event_at,
            available_at: input.available_at,
            decision_at: input.decision_at,
            stale_at: input.stale_at,
            mid_price: input.mid_price,
            spread_basis_points: input.spread_basis_points,
            executable_depth: input.executable_depth,
            universe: input.universe,
            features: input.features.into_boxed_slice(),
            lineage_digest: input.lineage_digest,
        })
    }

    /// Returns the stable instrument identity.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.execution_terms.instrument_id()
    }

    /// Returns the event timestamp retained by the PIT producer.
    #[must_use]
    pub const fn event_at(&self) -> Timestamp {
        self.event_at
    }

    /// Returns when the observation first became available to research decisions.
    #[must_use]
    pub const fn available_at(&self) -> Timestamp {
        self.available_at
    }

    /// Returns the exact decision cutoff.
    #[must_use]
    pub const fn decision_at(&self) -> Timestamp {
        self.decision_at
    }

    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            .saturating_add(size_of::<ResearchFeatureValue>().saturating_mul(self.features.len()))
            .saturating_add(
                self.features
                    .iter()
                    .map(|feature| feature.name.as_str().len())
                    .sum::<usize>(),
            )
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BacktestDatasetInput {
    pub manifest: DatasetManifestRef,
    pub object_graph_digest: Sha256Digest,
    pub point_in_time_content: Sha256Digest,
    pub point_in_time_audit: Sha256Digest,
    pub instrument_definition_content: Sha256Digest,
    pub instrument_definition_audit: Sha256Digest,
    pub observations: Vec<BacktestObservation>,
}

/// Validated, canonically ordered point-in-time research stream.
#[derive(Clone, Debug)]
pub struct BacktestDataset {
    pub(crate) manifest: DatasetManifestRef,
    object_graph_digest: Sha256Digest,
    pub(crate) point_in_time_content: Sha256Digest,
    pub(crate) point_in_time_audit: Sha256Digest,
    pub(crate) observations: Box<[BacktestObservation]>,
    pub(crate) identity: Sha256Digest,
    pub(crate) retained_bytes: usize,
}

impl BacktestDataset {
    /// Admits only owned non-forgeable query and instrument-definition receipts.
    pub fn try_from_pinned_query(
        output: PinnedQueryOutput,
        instrument_definitions: PinnedInstrumentDefinitions,
        limits: BacktestLimits,
    ) -> Result<Self, BacktestError> {
        admission::from_pinned_query(output, instrument_definitions, limits)
    }

    pub(crate) fn try_new(mut input: BacktestDatasetInput) -> Result<Self, BacktestError> {
        let feature_schema = DatasetSchemaRegistry::local().canonical_feature_labels()?;
        if input.manifest.schema() != &feature_schema
            || input.observations.is_empty()
            || [
                input.object_graph_digest,
                input.point_in_time_content,
                input.point_in_time_audit,
                input.instrument_definition_content,
                input.instrument_definition_audit,
            ]
            .into_iter()
            .any(|digest| digest.bytes() == [0; 32])
        {
            return Err(BacktestError::InvalidDataset);
        }
        input.observations.sort_unstable_by(|left, right| {
            left.decision_at
                .cmp(&right.decision_at)
                .then_with(|| left.instrument_id().cmp(&right.instrument_id()))
                .then_with(|| left.lineage_digest.cmp(&right.lineage_digest))
        });
        if input.observations.windows(2).any(|pair| {
            pair[0].decision_at == pair[1].decision_at
                && pair[0].instrument_id() == pair[1].instrument_id()
        }) {
            return Err(BacktestError::InvalidDataset);
        }
        let retained_bytes = input.observations.iter().try_fold(
            size_of::<Self>()
                .checked_add(input.manifest.dataset_id().as_str().len())
                .ok_or(BacktestError::LimitExceeded)?,
            |total, observation| {
                total
                    .checked_add(observation.retained_bytes())
                    .ok_or(BacktestError::LimitExceeded)
            },
        )?;
        let identity = dataset_identity(&input);
        Ok(Self {
            manifest: input.manifest,
            object_graph_digest: input.object_graph_digest,
            point_in_time_content: input.point_in_time_content,
            point_in_time_audit: input.point_in_time_audit,
            observations: input.observations.into_boxed_slice(),
            identity,
            retained_bytes,
        })
    }

    /// Returns the complete exact research input identity.
    #[must_use]
    pub const fn identity(&self) -> Sha256Digest {
        self.identity
    }

    /// Returns the immutable Task 11 manifest generation.
    #[must_use]
    pub const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }

    /// Returns the exact catalog-resolved generation and object-graph identity.
    #[must_use]
    pub const fn object_graph_digest(&self) -> Sha256Digest {
        self.object_graph_digest
    }

    /// Returns the exact point-in-time content identity minted by the query authority.
    #[must_use]
    pub const fn point_in_time_content(&self) -> Sha256Digest {
        self.point_in_time_content
    }

    /// Returns the exact point-in-time audit identity minted by the query authority.
    #[must_use]
    pub const fn point_in_time_audit(&self) -> Sha256Digest {
        self.point_in_time_audit
    }
}

fn dataset_identity(input: &BacktestDatasetInput) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/backtest-dataset/v2");
    update_text(&mut hash, input.manifest.dataset_id().as_str());
    hash.update(input.manifest.manifest_version().to_be_bytes());
    hash.update(input.manifest.content_hash().bytes());
    hash.update(input.object_graph_digest.bytes());
    hash.update(input.point_in_time_content.bytes());
    hash.update(input.point_in_time_audit.bytes());
    hash.update(input.instrument_definition_content.bytes());
    hash.update(input.instrument_definition_audit.bytes());
    hash.update((input.observations.len() as u64).to_be_bytes());
    for observation in &input.observations {
        update_execution_terms(&mut hash, observation.execution_terms);
        hash.update(observation.event_at.unix_nanos().to_be_bytes());
        hash.update(observation.available_at.unix_nanos().to_be_bytes());
        hash.update(observation.decision_at.unix_nanos().to_be_bytes());
        hash.update(observation.stale_at.unix_nanos().to_be_bytes());
        match observation.mid_price {
            Some(price) => {
                hash.update([1]);
                hash.update(price.get().to_be_bytes());
            }
            None => hash.update([0]),
        }
        hash.update(observation.spread_basis_points.get().to_be_bytes());
        hash.update(observation.executable_depth.get().to_be_bytes());
        hash.update([match observation.universe {
            HistoricalUniverseStatus::Eligible => 0,
            HistoricalUniverseStatus::Ineligible => 1,
            HistoricalUniverseStatus::Delisted => 2,
        }]);
        hash.update(observation.lineage_digest.bytes());
        hash.update((observation.features.len() as u64).to_be_bytes());
        for feature in &observation.features {
            update_text(&mut hash, feature.name.as_str());
            hash.update(feature.version.get().to_be_bytes());
            hash.update(feature.value.to_bits().to_be_bytes());
        }
    }
    Sha256Digest::new(hash.finalize().into())
}

fn update_execution_terms(hash: &mut Sha256, terms: InstrumentExecutionTerms) {
    hash.update(terms.instrument_id().as_uuid().as_bytes());
    hash.update(terms.definition_revision().get().to_be_bytes());
    update_decimal(hash, terms.price_tick().as_decimal());
    update_decimal(hash, terms.lot_size().as_decimal());
    update_text(hash, terms.quote_currency().as_str());
    match terms.settlement_denomination() {
        Denomination::Currency(currency) => {
            hash.update([0]);
            update_text(hash, currency.as_str());
        }
        Denomination::Asset(instrument_id) => {
            hash.update([1]);
            hash.update(instrument_id.as_uuid().as_bytes());
        }
    }
    update_decimal(hash, terms.contract_multiplier());
}

fn update_decimal(hash: &mut Sha256, value: rust_decimal::Decimal) {
    let normalized = value.normalize();
    hash.update(normalized.mantissa().to_be_bytes());
    hash.update(normalized.scale().to_be_bytes());
}

fn update_text(hash: &mut Sha256, value: &str) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value.as_bytes());
}

/// Caller-selected engine resource ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BacktestLimitsInput {
    pub max_observations: usize,
    pub max_pending_intents: usize,
    pub max_fills: usize,
    pub max_retained_bytes: usize,
}

/// Validated bounded engine limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BacktestLimits {
    pub(crate) max_observations: usize,
    pub(crate) max_pending_intents: usize,
    pub(crate) max_fills: usize,
    pub(crate) max_retained_bytes: usize,
}

impl BacktestLimits {
    /// Validates positive limits against fixed process ceilings.
    pub fn try_new(input: BacktestLimitsInput) -> Result<Self, BacktestError> {
        let valid = input.max_observations > 0
            && input.max_observations <= HARD_MAX_OBSERVATIONS
            && input.max_pending_intents > 0
            && input.max_pending_intents <= HARD_MAX_PENDING_INTENTS
            && input.max_fills > 0
            && input.max_fills <= HARD_MAX_FILLS
            && input.max_retained_bytes > 0
            && input.max_retained_bytes <= HARD_MAX_RETAINED_BYTES;
        if !valid {
            return Err(BacktestError::InvalidLimits);
        }
        Ok(Self {
            max_observations: input.max_observations,
            max_pending_intents: input.max_pending_intents,
            max_fills: input.max_fills,
            max_retained_bytes: input.max_retained_bytes,
        })
    }
}
