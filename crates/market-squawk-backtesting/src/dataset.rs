//! Non-forgeable point-in-time dataset admission and canonical event-time observations.

use std::mem::size_of;
use std::num::NonZeroU32;

use market_squawk_data::{
    CompleteMarketBarHistoryOutput, DatasetManifestRef, DatasetSchemaRegistry,
    FeatureDatasetInputCoordinateHandle, FeatureDatasetInputEpochCursor,
    PinnedInstrumentDefinitions, PinnedQueryOutput, Sha256Digest,
};
use market_squawk_domain::{
    BasisPoints, Denomination, HistoricalStudyBasis, HistoricalStudyLimitation,
    InstrumentExecutionTerms, InstrumentId, Money, PriceTicks, QuantityLots, SourceIdentifier,
    Timestamp,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::engine::BacktestError;

mod admission;
pub(crate) mod history_store;
pub(crate) mod observation_store;

pub use admission::{
    AVAILABLE_AT_COMPONENT, BacktestDailyHistoryAdmission, DEPTH_COMPONENT, EVENT_AT_COMPONENT,
    MID_PRICE_COMPONENT, SPREAD_COMPONENT, STALE_AT_COMPONENT, UNIVERSE_COMPONENT,
};

const HARD_MAX_OBSERVATIONS: usize = 1_000_000;
const HARD_MAX_PENDING_INTENTS: usize = 65_536;
const HARD_MAX_FILLS: usize = 1_000_000;
const HARD_MAX_RETAINED_BYTES: usize = 512 * 1024 * 1024;

/// Compact qualification of a historical study. This descriptor is untrusted provenance;
/// only admission from the sealed data output establishes the corresponding source authority.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BacktestStudyQualification {
    basis: HistoricalStudyBasis,
    snapshot_as_of: Timestamp,
    source_snapshot_digest: [u8; 32],
    decision_lag_nanos: Option<i64>,
    limitation_mask: u8,
}

impl BacktestStudyQualification {
    pub fn try_new(
        basis: HistoricalStudyBasis,
        snapshot_as_of: Timestamp,
        source_snapshot_digest: Sha256Digest,
        decision_lag_nanos: Option<i64>,
        limitations: &[HistoricalStudyLimitation],
    ) -> Result<Self, BacktestError> {
        let mut mask = 0;
        for limitation in limitations {
            let bit = match limitation {
                HistoricalStudyLimitation::HistoricalRevisionCoverageUnproven => 1,
                HistoricalStudyLimitation::LaterVintageInputs => 2,
                HistoricalStudyLimitation::PresentDayFixedCohort => 4,
                HistoricalStudyLimitation::SimulatedAvailability => 8,
            };
            if mask & bit != 0 {
                return Err(BacktestError::InvalidDataset);
            }
            mask |= bit;
        }
        let value = Self {
            basis,
            snapshot_as_of,
            source_snapshot_digest: source_snapshot_digest.bytes(),
            decision_lag_nanos,
            limitation_mask: mask,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(self) -> Result<(), BacktestError> {
        let valid = match self.basis {
            HistoricalStudyBasis::HistoricalAsKnown => {
                self.decision_lag_nanos.is_none() && self.limitation_mask == 4
            }
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => {
                self.decision_lag_nanos.is_some_and(|lag| lag >= 0) && self.limitation_mask == 15
            }
        };
        if !valid || self.source_snapshot_digest == [0; 32] {
            return Err(BacktestError::InvalidDataset);
        }
        Ok(())
    }

    pub const fn basis(self) -> HistoricalStudyBasis {
        self.basis
    }
    pub const fn snapshot_as_of(self) -> Timestamp {
        self.snapshot_as_of
    }
    pub const fn source_snapshot_digest(self) -> Sha256Digest {
        Sha256Digest::new(self.source_snapshot_digest)
    }
    pub const fn decision_lag_nanos(self) -> Option<i64> {
        self.decision_lag_nanos
    }
    pub const fn limitations(self) -> &'static [HistoricalStudyLimitation] {
        match self.basis {
            HistoricalStudyBasis::HistoricalAsKnown => {
                &[HistoricalStudyLimitation::PresentDayFixedCohort]
            }
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => &[
                HistoricalStudyLimitation::HistoricalRevisionCoverageUnproven,
                HistoricalStudyLimitation::LaterVintageInputs,
                HistoricalStudyLimitation::PresentDayFixedCohort,
                HistoricalStudyLimitation::SimulatedAvailability,
            ],
        }
    }

    pub(crate) fn admits_clocks(
        self,
        origin: Timestamp,
        available_at: Timestamp,
        source_selection_as_of: Timestamp,
        decision_at: Timestamp,
    ) -> bool {
        if self.validate().is_err()
            || origin > decision_at
            || origin > available_at
            || available_at > source_selection_as_of
            || source_selection_as_of > self.snapshot_as_of
            || decision_at > self.snapshot_as_of
        {
            return false;
        }
        match self.basis {
            HistoricalStudyBasis::HistoricalAsKnown => source_selection_as_of <= decision_at,
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => {
                source_selection_as_of == self.snapshot_as_of
                    && self
                        .decision_lag_nanos
                        .is_some_and(|lag| origin.checked_add_nanos(lag).ok() == Some(decision_at))
            }
        }
    }

    pub(crate) fn hash_into(self, hash: &mut Sha256) {
        hash.update([match self.basis {
            HistoricalStudyBasis::HistoricalAsKnown => 1,
            HistoricalStudyBasis::RetrospectiveFrozenSnapshot => 2,
        }]);
        hash.update(self.snapshot_as_of.unix_nanos().to_be_bytes());
        hash.update(self.source_snapshot_digest);
        hash.update([self.limitation_mask]);
        match self.decision_lag_nanos {
            Some(lag) => {
                hash.update([1]);
                hash.update(lag.to_be_bytes());
            }
            None => hash.update([0]),
        }
    }
}

/// Financial observation basis used by simulated research execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BacktestExecutionBasis {
    /// Point-in-time quotes with observed spread and executable depth.
    ObservedQuoteDepth,
    /// Later-known complete raw daily bars, isolated from the signal information set.
    CompletedDailyBar,
}

/// One raw realized daily bar admitted only from the durable complete-history reader.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(crate) struct BacktestDailyBar {
    pub(crate) execution_terms: InstrumentExecutionTerms,
    /// Provider period start or original named regular-session open; never a synthesized candle.
    pub(crate) starts_at: Timestamp,
    pub(crate) ends_at: Timestamp,
    pub(crate) available_at: Timestamp,
    pub(crate) close: Money,
    pub(crate) traded_volume: rust_decimal::Decimal,
    #[serde(with = "history_store::digest_wire")]
    pub(crate) lineage_digest: Sha256Digest,
}

#[derive(Clone, Debug)]
pub(crate) struct NominalOutcomeSource {
    pub(crate) instrument: InstrumentId,
    pub(crate) manifest: DatasetManifestRef,
    pub(crate) knowledge_cutoff: Timestamp,
}

#[derive(Clone, Debug)]
pub(crate) struct BacktestDailyHistory {
    pub(crate) nominal_sources: Box<[NominalOutcomeSource]>,
    pub(crate) bars: history_store::DailyBarStore,
    pub(crate) digest: Sha256Digest,
    pub(crate) available_at: Timestamp,
}

/// Historical eligibility carried by the exact Task 11 universe at an observation cutoff.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum HistoricalUniverseStatus {
    /// The instrument belonged to the historical universe at this cutoff.
    Eligible,
    /// The instrument was outside the historical universe at this cutoff.
    Ineligible,
    /// The instrument was terminally delisted at or before this cutoff.
    Delisted,
}

/// One finite research feature available to a strategy at the current cutoff.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    #[serde(with = "history_store::digest_wire")]
    pub(crate) lineage_digest: Sha256Digest,
    /// Source-verified financial origin and fixed target, distinct from provider event time.
    /// Only sealed feature-epoch admission can populate this for completed-close studies.
    pub(crate) financial_target: Option<(Timestamp, Timestamp)>,
    pub(crate) source_selection_as_of: Timestamp,
    pub(crate) market_reference: Option<Money>,
    #[serde(skip)]
    pub(crate) input_coordinate: Option<Box<FeatureDatasetInputCoordinateHandle>>,
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
            financial_target: None,
            source_selection_as_of: input.decision_at,
            market_reference: None,
            input_coordinate: None,
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
            .saturating_add(
                self.input_coordinate
                    .as_ref()
                    .map_or(0, |coordinate| coordinate.retained_bytes()),
            )
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
    pub(crate) observations: observation_store::ObservationStore,
    pub(crate) identity: Sha256Digest,
    pub(crate) retained_bytes: usize,
    pub(crate) daily_history: Option<BacktestDailyHistory>,
    pub(crate) study_qualification: Option<BacktestStudyQualification>,
}

impl BacktestDataset {
    /// Admits only owned non-forgeable query and instrument-definition receipts.
    pub fn try_from_pinned_query(
        output: PinnedQueryOutput,
        instrument_definitions: PinnedInstrumentDefinitions,
        limits: BacktestLimits,
    ) -> Result<Self, BacktestError> {
        admission::from_pinned_query(output, &instrument_definitions, limits, false)
    }

    /// Binds a separate complete raw daily outcome stream to an already admitted PIT signal set.
    /// The private reader receipts and historical definition authority are consumed directly;
    /// caller-authored bars, latest substitutions and adjusted-price reconstruction are rejected.
    /// These bars never become signal features or historical-as-known evidence.
    pub fn with_complete_daily_history(
        self,
        histories: Vec<CompleteMarketBarHistoryOutput>,
        instrument_definitions: PinnedInstrumentDefinitions,
        admitted_at: Timestamp,
        limits: BacktestLimits,
    ) -> Result<Self, BacktestError> {
        admission::with_complete_daily_history(
            self,
            histories,
            instrument_definitions,
            admitted_at,
            limits,
        )
    }

    /// Admits the original PIT signal query and a separate raw realized-outcome stream with one
    /// exact historical instrument-definition receipt. Outcome bars remain hidden from signals.
    pub fn try_from_pinned_query_with_complete_daily_history(
        output: PinnedQueryOutput,
        instrument_definitions: PinnedInstrumentDefinitions,
        histories: Vec<CompleteMarketBarHistoryOutput>,
        admitted_at: Timestamp,
        limits: BacktestLimits,
    ) -> Result<Self, BacktestError> {
        let dataset = admission::from_pinned_query(output, &instrument_definitions, limits, true)?;
        admission::with_complete_daily_history(
            dataset,
            histories,
            instrument_definitions,
            admitted_at,
            limits,
        )
    }

    /// Starts sealed scoring admission and accepts each realized instrument history separately.
    pub fn begin_study_input_epochs(
        output: FeatureDatasetInputEpochCursor,
        instrument_definitions: PinnedInstrumentDefinitions,
        admitted_at: Timestamp,
        limits: BacktestLimits,
    ) -> Result<BacktestDailyHistoryAdmission, BacktestError> {
        let dataset = admission::from_study_input_epochs(
            output,
            &instrument_definitions,
            admitted_at,
            limits,
        )?;
        BacktestDailyHistoryAdmission::new(dataset, instrument_definitions, admitted_at, limits)
    }

    /// Admits a complete label-free scoring population and separately authenticated raw outcomes.
    /// Neither caller-authored epoch vectors nor label-selected feature queries can enter here.
    pub fn try_from_study_input_epochs(
        output: FeatureDatasetInputEpochCursor,
        instrument_definitions: PinnedInstrumentDefinitions,
        histories: Vec<CompleteMarketBarHistoryOutput>,
        admitted_at: Timestamp,
        limits: BacktestLimits,
    ) -> Result<Self, BacktestError> {
        let dataset = admission::from_study_input_epochs(
            output,
            &instrument_definitions,
            admitted_at,
            limits,
        )?;
        admission::with_complete_daily_history(
            dataset,
            histories,
            instrument_definitions,
            admitted_at,
            limits,
        )
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
        let observations = observation_store::ObservationStore::from_observations(std::mem::take(
            &mut input.observations,
        ))?;
        Self::from_store(input, observations)
    }

    pub(crate) fn from_store(
        input: BacktestDatasetInput,
        observations: observation_store::ObservationStore,
    ) -> Result<Self, BacktestError> {
        let identity = dataset_identity(&input, &observations)?;
        // SQLite caches and the bounded decode page, rather than complete historical bytes.
        let retained_bytes = 64 * 1024
            + observations
                .iter()
                .take(128)
                .try_fold(0usize, |total, value| {
                    total
                        .checked_add(value?.retained_bytes())
                        .ok_or(BacktestError::LimitExceeded)
                })?;
        Ok(Self {
            manifest: input.manifest,
            object_graph_digest: input.object_graph_digest,
            point_in_time_content: input.point_in_time_content,
            point_in_time_audit: input.point_in_time_audit,
            observations,
            identity,
            retained_bytes,
            daily_history: None,
            study_qualification: None,
        })
    }

    /// Returns the complete exact research input identity.
    #[must_use]
    pub const fn identity(&self) -> Sha256Digest {
        self.identity
    }

    /// Qualification authenticated by the sealed study-epoch reader, when present.
    pub const fn study_qualification(&self) -> Option<BacktestStudyQualification> {
        self.study_qualification
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

    /// Returns whether execution uses observed quote depth or a separate completed daily stream.
    #[must_use]
    pub const fn execution_basis(&self) -> BacktestExecutionBasis {
        if self.daily_history.is_some() {
            BacktestExecutionBasis::CompletedDailyBar
        } else {
            BacktestExecutionBasis::ObservedQuoteDepth
        }
    }

    /// Returns the exact complete raw-history identity when daily execution is selected.
    #[must_use]
    pub fn raw_execution_history_digest(&self) -> Option<Sha256Digest> {
        self.daily_history.as_ref().map(|history| history.digest)
    }
}

fn dataset_identity(
    input: &BacktestDatasetInput,
    observations: &observation_store::ObservationStore,
) -> Result<Sha256Digest, BacktestError> {
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
    hash.update((observations.len() as u64).to_be_bytes());
    for observation in observations.iter() {
        let observation = observation?;
        update_execution_terms(&mut hash, observation.execution_terms);
        hash.update(observation.event_at.unix_nanos().to_be_bytes());
        hash.update(observation.available_at.unix_nanos().to_be_bytes());
        hash.update(
            observation
                .source_selection_as_of
                .unix_nanos()
                .to_be_bytes(),
        );
        hash.update(observation.decision_at.unix_nanos().to_be_bytes());
        hash.update(observation.stale_at.unix_nanos().to_be_bytes());
        match observation.financial_target {
            Some((origin, target)) => {
                hash.update([1]);
                hash.update(origin.unix_nanos().to_be_bytes());
                hash.update(target.unix_nanos().to_be_bytes());
            }
            None => hash.update([0]),
        }
        match observation.mid_price {
            Some(price) => {
                hash.update([1]);
                hash.update(price.get().to_be_bytes());
            }
            None => hash.update([0]),
        }
        match observation.market_reference {
            Some(price) => {
                hash.update([1]);
                update_decimal(&mut hash, price.amount());
                update_text(&mut hash, price.currency().as_str());
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
    Ok(Sha256Digest::new(hash.finalize().into()))
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
