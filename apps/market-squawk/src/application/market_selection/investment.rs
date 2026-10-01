//! Typed, fail-closed market evidence for one selected investment instrument.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_data::{
    AuthorizedMarketEventUse, CatalogLimit, InstrumentDefinitionReadCapability,
    MarketDataInstrumentPopulationQuery, MarketDataInstrumentPopulationSelection,
    MarketDataInstrumentReadCapability, MarketEventUseRequest, PinnedInstrumentDefinitions,
    ProviderMarketEventPointInTimeSelection, ProviderMarketEventSelectedCandidate,
    ProviderMarketEventSelectionCompleteness, ResearchUse, ResearchUseLimits,
};

use market_squawk_domain::{
    AssetClass, ConnectionGeneration, Currency, DataQuality, DigestAlgorithm, EvidenceDigest,
    ExecutionEligibility, InstrumentDefinition, InstrumentExecutionTerms, InstrumentId,
    LiveEventClass, LiveProvenance, MarketDataInstrumentDefinition, MarketDepth, MarketEvent,
    SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_live::{
    LastTradeSnapshot, LiveFeatureSetSnapshot, LiveFeatureSnapshot, OrderLevelPriceProjection,
    SnapshotCompleteness, StreamSnapshot,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::SourceMetadata;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use super::{
    BudgetAvailability, CandidateAdmissionState, CandidateCapabilities, CandidateHealth,
    CandidateIdentity, CandidateIntegrity, CandidateTimestamps, DowngradePolicy, FreshnessBasis,
    FreshnessRequirement, HealthState, IntegrityState, MarketCoverage, MarketOperation,
    MarketOperationSet, MarketSelectionPolicy, MarketSelectionReceipt, MarketSelectionRequest,
    ObservationTiming, ProviderBudgetSnapshot, RequestPriority, RightsAdmission,
    SelectedMarketSource, SourceCandidate, select_market_source,
};
use crate::application::market_runtime::{
    MarketDisplaySnapshotLease, MarketKrakenPriceProjectionLease,
};
use crate::application::research::{
    MarketEventPointInTimeReceipt, MarketEventPointInTimeSelector, MarketEventReadError,
    map_catalog_error, map_durable_market_ingest_error, map_market_definition_read_error,
    map_point_in_time_read_error,
};
use crate::live_source::display_market::DisplayMarketReadObservation;
use crate::research_service::ResearchService;

/// A caller supplied evidence that does not exactly match the selected receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketInvestmentReadError {
    ExecutionOperationForbidden,
    SelectedSourceMismatch,
    InvalidFinancialTerms,
    AmbiguousFeatureEvidence,
    EvidenceIdentityEncoding,
}

impl fmt::Display for MarketInvestmentReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecutionOperationForbidden => {
                formatter.write_str("investment evidence cannot carry execution authority")
            }
            Self::SelectedSourceMismatch => formatter
                .write_str("market evidence does not match the exact selected source generation"),
            Self::InvalidFinancialTerms => {
                formatter.write_str("market mark cannot be represented by the admitted terms")
            }
            Self::AmbiguousFeatureEvidence => {
                formatter.write_str("more than one feature set matches the exact source generation")
            }
            Self::EvidenceIdentityEncoding => {
                formatter.write_str("market mark evidence cannot be represented canonically")
            }
        }
    }
}

impl std::error::Error for MarketInvestmentReadError {}

/// Truthful reason one selected source cannot currently produce an investment mark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketInvestmentUnavailableReason {
    NoEligibleSource,
    NoFreshLastTradeOrMidpoint,
    DurablePitEvidenceNotEstablished,
}

/// Why feature evidence is absent without borrowing evidence from another source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketFeatureUnavailableReason {
    SourceDoesNotPublishLiveFeatures,
    IncompleteSnapshot,
    NoExactSourceGeneration,
    AvailableAfterSelection,
    IncompleteValueSet,
}

/// Exact feature evidence, or a typed reason it is unavailable.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MarketFeatureEvidence<'source> {
    Available(&'source LiveFeatureSetSnapshot),
    Unavailable(MarketFeatureUnavailableReason),
}

/// Code-owned mark choice. Neither variant is an order, target, or recommendation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MarketInvestmentMarkBasis {
    FreshLastTrade,
    FreshBidAskMidpoint,
}

/// Borrowed exact evidence from which the selected mark was computed.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MarketInvestmentMarkEvidence<'source> {
    LiveTrade(&'source LastTradeSnapshot),
    LiveBook(&'source StreamSnapshot),
    DisplayTrade(&'source DisplayMarketReadObservation),
    DisplayQuote(&'source DisplayMarketReadObservation),
    KrakenPriceProjection(&'source OrderLevelPriceProjection),
    Durable(&'source ProviderMarketEventSelectedCandidate),
}

/// Exact decimal mark and currency backed by one retained durable source observation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MarketInvestmentMark<'source> {
    value: Decimal,
    currency: Currency,
    basis: MarketInvestmentMarkBasis,
    evidence_identity: EvidenceDigest,
    fresh_until: Option<Timestamp>,
    evidence: MarketInvestmentMarkEvidence<'source>,
}

impl<'source> MarketInvestmentMark<'source> {
    pub(crate) const fn value(self) -> Decimal {
        self.value
    }

    pub(crate) const fn currency(self) -> Currency {
        self.currency
    }

    pub(crate) const fn basis(self) -> MarketInvestmentMarkBasis {
        self.basis
    }

    /// Returns the versioned identity of the exact mark, source selection, and retained evidence.
    pub(crate) const fn evidence_identity(self) -> EvidenceDigest {
        self.evidence_identity
    }

    /// Returns the inclusive source-specific freshness deadline when one is retained.
    /// Deadline-requiring consumers must reject `None` rather than infer a value.
    pub(crate) const fn fresh_until(self) -> Option<Timestamp> {
        self.fresh_until
    }

    pub(crate) const fn evidence(self) -> MarketInvestmentMarkEvidence<'source> {
        self.evidence
    }
}

/// One native live stream plus immutable publication facts used only for mismatch rejection.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LiveMarketInvestmentSource<'source> {
    surface_id: &'source SourceIdentifier,
    provider: &'source SourceIdentifier,
    stream: &'source StreamSnapshot,
    features: &'source LiveFeatureSnapshot,
    definition: &'source InstrumentDefinition,
    published_at: Timestamp,
}

impl<'source> LiveMarketInvestmentSource<'source> {
    pub(crate) const fn new(
        surface_id: &'source SourceIdentifier,
        provider: &'source SourceIdentifier,
        stream: &'source StreamSnapshot,
        features: &'source LiveFeatureSnapshot,
        definition: &'source InstrumentDefinition,
        published_at: Timestamp,
    ) -> Self {
        Self {
            surface_id,
            provider,
            stream,
            features,
            definition,
            published_at,
        }
    }
}

/// The exact retained hot source selected by the existing unified resolver.
///
/// Hot leases remain display-only. Only the durable variant carries catalog-reconstructed
/// point-in-time authority for an investment observation.
#[derive(Clone, Copy, Debug)]
pub(crate) enum SelectedMarketInvestmentSource<'source> {
    Live(LiveMarketInvestmentSource<'source>),
    Display {
        snapshot: &'source MarketDisplaySnapshotLease,
        definition: &'source MarketDataInstrumentDefinition,
    },
    Kraken(&'source MarketKrakenPriceProjectionLease),
    Durable(&'source MarketInvestmentReadReceipt),
}

/// Typed non-executable observation for an analysis compositor.
///
/// Every field is private; construction requires the complete durable source and definition join.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MarketInvestmentObservation<'receipt, 'source> {
    selected: SelectedMarketSource<'receipt>,
    selection_digest: EvidenceDigest,
    selected_at: Timestamp,
    mark: MarketInvestmentMark<'source>,
    features: MarketFeatureEvidence<'source>,
    publication: &'source ProviderMarketEventPointInTimeSelection,
}

impl<'receipt, 'source> MarketInvestmentObservation<'receipt, 'source> {
    pub(crate) const fn instrument_id(self) -> InstrumentId {
        self.selected.candidate().identity().instrument_id()
    }

    pub(crate) const fn selected_source(self) -> SelectedMarketSource<'receipt> {
        self.selected
    }

    pub(crate) const fn selection_digest(self) -> EvidenceDigest {
        self.selection_digest
    }

    pub(crate) const fn selected_at(self) -> Timestamp {
        self.selected_at
    }

    pub(crate) const fn freshness_age_nanos(self) -> u64 {
        self.selected.freshness_age_nanos()
    }

    pub(crate) const fn generation(self) -> Option<ConnectionGeneration> {
        self.selected
            .candidate()
            .admission()
            .integrity()
            .generation()
    }

    pub(crate) const fn mark(self) -> MarketInvestmentMark<'source> {
        self.mark
    }

    pub(crate) const fn timestamps(self) -> CandidateTimestamps {
        self.selected.candidate().timestamps()
    }

    pub(crate) const fn quality(self) -> DataQuality {
        self.selected.candidate().capabilities().quality()
    }

    pub(crate) const fn depth(self) -> Option<MarketDepth> {
        self.selected.candidate().capabilities().depth()
    }

    pub(crate) const fn coverage(self) -> MarketCoverage {
        self.selected.candidate().capabilities().coverage()
    }

    pub(crate) const fn integrity(self) -> IntegrityState {
        self.selected.candidate().admission().integrity().state()
    }

    pub(crate) const fn features(self) -> MarketFeatureEvidence<'source> {
        self.features
    }

    pub(crate) const fn publication(self) -> &'source ProviderMarketEventPointInTimeSelection {
        self.publication
    }
}

/// Complete single-instrument result without partial or fabricated market evidence.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MarketInvestmentRead<'receipt, 'source> {
    Available(MarketInvestmentObservation<'receipt, 'source>),
    Unavailable(MarketInvestmentUnavailableReason),
}

/// Verifies that an immutable selected receipt names the exact retained connection generation.
pub(crate) fn selected_generation_matches(
    selected: SelectedMarketSource<'_>,
    actual: ConnectionGeneration,
) -> bool {
    selected.candidate().admission().integrity().generation() == Some(actual)
}

/// Requires the exact durable receipt; hot-only observations retain an explicit PIT gap.
pub(crate) fn read_market_investment_observation<'receipt, 'source>(
    receipt: &'receipt MarketSelectionReceipt,
    source: Option<SelectedMarketInvestmentSource<'source>>,
) -> Result<MarketInvestmentRead<'receipt, 'source>, MarketInvestmentReadError> {
    if receipt.request().operation().requires_execution_quality() {
        return Err(MarketInvestmentReadError::ExecutionOperationForbidden);
    }
    if let Some(SelectedMarketInvestmentSource::Durable(durable)) = source {
        if receipt != durable.selection() {
            return Err(MarketInvestmentReadError::SelectedSourceMismatch);
        }
        return durable
            .observation_with_receipt(receipt)
            .map(MarketInvestmentRead::Available);
    }
    let (selected, source) = match (receipt.selected(), source) {
        (None, None) => {
            return Ok(MarketInvestmentRead::Unavailable(
                MarketInvestmentUnavailableReason::NoEligibleSource,
            ));
        }
        (Some(selected), Some(source)) => (selected, source),
        (None, Some(_)) | (Some(_), None) => {
            return Err(MarketInvestmentReadError::SelectedSourceMismatch);
        }
    };
    validate_hot_source_matches_selection(selected, source, receipt.selected_at())?;
    Ok(MarketInvestmentRead::Unavailable(
        MarketInvestmentUnavailableReason::DurablePitEvidenceNotEstablished,
    ))
}

fn validate_hot_source_matches_selection(
    selected: SelectedMarketSource<'_>,
    source: SelectedMarketInvestmentSource<'_>,
    selected_at: Timestamp,
) -> Result<(), MarketInvestmentReadError> {
    match source {
        SelectedMarketInvestmentSource::Live(source) => {
            validate_live_source_matches_selection(selected, source)
        }
        SelectedMarketInvestmentSource::Display {
            snapshot,
            definition,
        } => validate_display_source_matches_selection(selected, snapshot, definition, selected_at),
        SelectedMarketInvestmentSource::Kraken(snapshot) => {
            validate_kraken_source_matches_selection(selected, snapshot, selected_at)
        }
        SelectedMarketInvestmentSource::Durable(_) => {
            Err(MarketInvestmentReadError::SelectedSourceMismatch)
        }
    }
}

fn validate_live_source_matches_selection(
    selected: SelectedMarketSource<'_>,
    source: LiveMarketInvestmentSource<'_>,
) -> Result<(), MarketInvestmentReadError> {
    let identity = selected.candidate().identity();
    let stream = source.stream;
    let timestamps = selected.candidate().timestamps();
    if source.provider != identity.provider()
        || source.surface_id != identity.observation_id()
        || stream.source() != identity.source_id()
        || Some(stream.venue()) != identity.venue_id()
        || stream.instrument() != identity.instrument_id()
        || stream.provider_product() != identity.product()
        || stream.provider_channel() != identity.feed()
        || !selected_generation_matches(selected, stream.connection_generation())
        || source.definition.instrument_id() != identity.instrument_id()
        || timestamps.source_timestamp() != stream.source_timestamp()
        || timestamps.effective_at() != stream.source_timestamp().unwrap_or(stream.received_at())
        || timestamps.received_at() != stream.received_at()
        || timestamps.available_at() != stream.evaluated_at()
        || timestamps.ingested_at() != source.published_at
    {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    }
    if source.features.set_dimension().completeness() == SnapshotCompleteness::Complete {
        let mut matching_features = source.features.sets().iter().filter(|features| {
            features.source() == stream.source()
                && features.venue() == stream.venue()
                && features.instrument() == stream.instrument()
                && features.provider_product() == stream.provider_product()
                && features.provider_channel() == stream.provider_channel()
                && features.connection_generation() == stream.connection_generation()
                && selected_generation_matches(selected, features.connection_generation())
        });
        let _ = matching_features.next();
        if matching_features.next().is_some() {
            return Err(MarketInvestmentReadError::AmbiguousFeatureEvidence);
        }
    }
    Ok(())
}

fn validate_display_source_matches_selection(
    selected: SelectedMarketSource<'_>,
    snapshot: &MarketDisplaySnapshotLease,
    definition: &MarketDataInstrumentDefinition,
    selected_at: Timestamp,
) -> Result<(), MarketInvestmentReadError> {
    let identity = selected.candidate().identity();
    let key = snapshot.lease().key();
    if snapshot.metadata().provider() != identity.provider()
        || snapshot.surface_id() != identity.observation_id()
        || key.source_id() != identity.source_id()
        || Some(key.venue_id()) != identity.venue_id()
        || key.instrument_id() != identity.instrument_id()
        || definition.instrument_id() != identity.instrument_id()
        || !snapshot.matches_definition(definition)
        || definition.effective_interval().starts_at() > selected_at
        || definition
            .effective_interval()
            .ends_at()
            .is_some_and(|ends_at| selected_at >= ends_at)
        || !selected_generation_matches(selected, key.generation())
    {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    }
    Ok(())
}

fn validate_kraken_source_matches_selection(
    selected: SelectedMarketSource<'_>,
    snapshot: &MarketKrakenPriceProjectionLease,
    selected_at: Timestamp,
) -> Result<(), MarketInvestmentReadError> {
    let identity = selected.candidate().identity();
    let key = snapshot.key();
    let projection = snapshot.projection();
    let live = snapshot
        .metadata()
        .coverage()
        .live()
        .ok_or(MarketInvestmentReadError::SelectedSourceMismatch)?;
    let terms = snapshot.execution_terms();
    if snapshot.metadata().provider() != identity.provider()
        || snapshot.surface_id() != identity.observation_id()
        || live.provider_product() != identity.product()
        || live.provider_channel() != identity.feed()
        || key.source_id() != identity.source_id()
        || Some(key.venue_id()) != identity.venue_id()
        || key.instrument_id() != identity.instrument_id()
        || !selected_generation_matches(selected, key.generation())
        || projection.route().generation() != key.generation()
        || terms.instrument_id() != key.instrument_id()
        || projection.received_at() > projection.available_at()
        || projection.available_at() > selected_at
    {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    }
    Ok(())
}

const MAX_INVESTMENT_MARKET_CANDIDATES: usize = 256;
const MAX_SOURCE_EVENT_TIES: usize = 32;

/// Compact value reference to an exact durable read. Deserialization grants no authority: every
/// use reconstructs the original cutoff and compares all commitments against the catalog.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MarketInvestmentReadReference {
    instrument_id: InstrumentId,
    source_cutoff_unix_nanos: String,
    maximum_mark_age_nanos: String,
    evidence_digest: String,
    source_selection_digest: String,
    rights_input_digest: String,
    publication_selection_digest: String,
    definition_selection_digest: String,
    price_authority_digest: String,
    source_scope_digests: Option<Vec<String>>,
}

impl MarketInvestmentReadReference {
    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) fn source_cutoff(&self) -> Result<Timestamp, ServiceError> {
        let value = self
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if value <= 0 || value.to_string() != self.source_cutoff_unix_nanos {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Timestamp::from_unix_nanos(value))
    }

    pub(crate) fn maximum_mark_age_nanos(&self) -> Result<u64, ServiceError> {
        let value = self
            .maximum_mark_age_nanos
            .parse::<u64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if value == 0 || value > i64::MAX as u64 || value.to_string() != self.maximum_mark_age_nanos
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(value)
    }

    fn validate(&self) -> Result<(), ServiceError> {
        self.source_cutoff()?;
        self.maximum_mark_age_nanos()?;
        for digest in [
            &self.evidence_digest,
            &self.source_selection_digest,
            &self.rights_input_digest,
            &self.publication_selection_digest,
            &self.definition_selection_digest,
            &self.price_authority_digest,
        ]
        .into_iter()
        .chain(self.source_scope_digests.iter().flatten())
        {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || digest.bytes().all(|byte| byte == b'0')
            {
                return Err(ServiceError::InvalidRequest);
            }
        }
        if self.source_scope_digests.as_ref().is_some_and(|scope| {
            scope.is_empty()
                || scope.len() > MAX_INVESTMENT_MARKET_CANDIDATES
                || scope
                    .iter()
                    .enumerate()
                    .any(|(index, value)| scope[..index].contains(value))
        }) {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(())
    }
}

/// Owned exact selected publication, definition authority and checked financial mark.
/// It contains no live qualification, order or reservation authority.
#[derive(Debug)]
pub(crate) struct MarketInvestmentReadReceipt {
    selection: MarketSelectionReceipt,
    source: DurableInvestmentSource,
    definition_digest: EvidenceDigest,
    price_authority_digest: [u8; 32],
    instrument_definitions: Option<PinnedInstrumentDefinitions>,
    market_definitions: MarketDataInstrumentPopulationSelection,
    source_scope: Option<Box<[SourceId]>>,
    evidence_digest: EvidenceDigest,
    authorization: AuthorizedMarketEventUse,
    authorized_at: Timestamp,
}

#[derive(Debug)]
struct DurableInvestmentSource {
    receipt: MarketEventPointInTimeReceipt,
    terms: Option<InstrumentExecutionTerms>,
    instrument_id: InstrumentId,
    currency: Currency,
    asset_class: AssetClass,
    value: Decimal,
    basis: MarketInvestmentMarkBasis,
    fresh_until: Timestamp,
    observation_id: SourceIdentifier,
    rights_input_digest: EvidenceDigest,
}

impl MarketInvestmentReadReceipt {
    pub(crate) fn reference(&self) -> MarketInvestmentReadReference {
        let hex = crate::application::domain_support::encode_hex;
        MarketInvestmentReadReference {
            instrument_id: self.source.instrument_id,
            source_cutoff_unix_nanos: self.selection.selected_at().unix_nanos().to_string(),
            maximum_mark_age_nanos: self
                .selection
                .request()
                .freshness()
                .maximum_age_nanos()
                .to_string(),
            evidence_digest: hex(self.evidence_digest.bytes()),
            source_selection_digest: hex(self.selection.source_evidence_digest().bytes()),
            rights_input_digest: hex(self.source.rights_input_digest.bytes()),
            publication_selection_digest: hex(self.publication().selection_digest().bytes()),
            definition_selection_digest: hex(self.definition_digest.bytes()),
            price_authority_digest: hex(self.price_authority_digest),
            source_scope_digests: self
                .source_scope
                .as_ref()
                .map(|scope| scope.iter().map(source_scope_digest).collect()),
        }
    }

    pub(crate) const fn selection(&self) -> &MarketSelectionReceipt {
        &self.selection
    }

    pub(crate) const fn publication(&self) -> &ProviderMarketEventPointInTimeSelection {
        self.source.receipt.selection()
    }

    pub(crate) const fn execution_terms(&self) -> Option<InstrumentExecutionTerms> {
        self.source.terms
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.source.instrument_id
    }

    pub(crate) const fn currency(&self) -> Currency {
        self.source.currency
    }

    pub(crate) const fn asset_class(&self) -> AssetClass {
        self.source.asset_class
    }

    pub(crate) const fn instrument_definitions(&self) -> Option<&PinnedInstrumentDefinitions> {
        self.instrument_definitions.as_ref()
    }

    pub(crate) const fn market_definitions(&self) -> &MarketDataInstrumentPopulationSelection {
        &self.market_definitions
    }

    pub(crate) const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    /// Actual current admission of this read, independent from the retained financial cutoff.
    pub(crate) const fn authorized_at(&self) -> Timestamp {
        self.authorized_at
    }

    pub(crate) fn authorization_expires_at(&self) -> Timestamp {
        self.authorization.expires_at()
    }

    pub(crate) fn authorization_decision_digest(&self) -> [u8; 32] {
        self.authorization.decision_digest().bytes()
    }

    pub(crate) fn event(&self) -> Result<&MarketEvent, MarketInvestmentReadError> {
        Ok(single_candidate(self.publication())?.event())
    }

    /// A valuation input must first come from its genuine qualified producer. This exact join
    /// cannot upgrade this research mark into a qualified valuation or execution input.
    pub(crate) fn bind_valuation_input(
        &self,
        input: market_squawk_valuation::ValuationInput,
    ) -> Result<market_squawk_valuation::ValuationInput, market_squawk_valuation::FairValueError>
    {
        input.bind_selected_market_publication(self.publication())
    }

    pub(crate) fn observation(
        &self,
    ) -> Result<MarketInvestmentObservation<'_, '_>, MarketInvestmentReadError> {
        self.observation_with_receipt(&self.selection)
    }

    fn observation_with_receipt<'receipt, 'source>(
        &'source self,
        receipt: &'receipt MarketSelectionReceipt,
    ) -> Result<MarketInvestmentObservation<'receipt, 'source>, MarketInvestmentReadError> {
        if current_market_time().map_err(|_| MarketInvestmentReadError::SelectedSourceMismatch)?
            >= self.authorization.expires_at()
        {
            return Err(MarketInvestmentReadError::SelectedSourceMismatch);
        }
        let selected = receipt
            .selected()
            .ok_or(MarketInvestmentReadError::SelectedSourceMismatch)?;
        let candidate = single_candidate(self.publication())?;
        if receipt != &self.selection
            || selected.candidate().identity().observation_id() != &self.source.observation_id
        {
            return Err(MarketInvestmentReadError::SelectedSourceMismatch);
        }
        Ok(MarketInvestmentObservation {
            selected,
            selection_digest: receipt.source_evidence_digest(),
            selected_at: receipt.selected_at(),
            mark: MarketInvestmentMark {
                value: self.source.value,
                currency: self.source.currency,
                basis: self.source.basis,
                evidence_identity: self.evidence_digest,
                fresh_until: Some(self.source.fresh_until),
                evidence: MarketInvestmentMarkEvidence::Durable(candidate),
            },
            features: MarketFeatureEvidence::Unavailable(
                MarketFeatureUnavailableReason::NoExactSourceGeneration,
            ),
            publication: self.publication(),
        })
    }
}

/// Bounded provider-neutral current and historical market reads over the existing durable routes.
/// The same reader uses the caller's cutoff for events, publication and instrument definitions.
#[derive(Clone, Debug)]
pub(crate) struct MarketInvestmentReadCapability {
    research: Arc<ResearchService>,
    definitions: InstrumentDefinitionReadCapability,
    market_definitions: MarketDataInstrumentReadCapability,
    maximum_mark_age_nanos: u64,
}

impl MarketInvestmentReadCapability {
    /// Uses a producer-validated financial age policy without changing source-specific ceilings.
    pub(crate) fn with_maximum_mark_age_nanos(
        &self,
        maximum_mark_age_nanos: u64,
    ) -> Result<Self, ServiceError> {
        Self::try_new(
            Arc::clone(&self.research),
            self.definitions.clone(),
            self.market_definitions.clone(),
            maximum_mark_age_nanos,
        )
    }

    pub(crate) fn try_new(
        research: Arc<ResearchService>,
        definitions: InstrumentDefinitionReadCapability,
        market_definitions: MarketDataInstrumentReadCapability,
        maximum_mark_age_nanos: u64,
    ) -> Result<Self, ServiceError> {
        if maximum_mark_age_nanos == 0 || maximum_mark_age_nanos > i64::MAX as u64 {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(Self {
            research,
            definitions,
            market_definitions,
            maximum_mark_age_nanos,
        })
    }

    pub(crate) async fn read(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<MarketInvestmentReadReceipt>, ServiceError> {
        self.read_scoped(instrument_id, as_of, None, deadline, cancellation)
            .await
    }

    /// Restricts a historical read to the study's exact registered source scope.
    pub(crate) async fn read_for_sources(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        sources: &[SourceId],
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<MarketInvestmentReadReceipt>, ServiceError> {
        if sources.is_empty()
            || sources.len() > MAX_INVESTMENT_MARKET_CANDIDATES
            || sources.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ServiceError::InvalidRequest);
        }
        self.read_scoped(instrument_id, as_of, Some(sources), deadline, cancellation)
            .await
    }

    async fn read_scoped(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        source_scope: Option<&[SourceId]>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<MarketInvestmentReadReceipt>, ServiceError> {
        check_market_read(as_of, deadline, &cancellation)?;
        let population = self
            .market_definitions
            .pin_population_as_of(
                MarketDataInstrumentPopulationQuery::try_new(vec![instrument_id], as_of, as_of)
                    .map_err(|_| ServiceError::InvalidRequest)?,
                deadline,
                &cancellation,
            )
            .map_err(map_market_definition_read_error)?;
        let [definition] = population.records() else {
            return Ok(None);
        };
        if !population.exclusions().is_empty() {
            return Ok(None);
        }
        // Native monetary prices remain independent of execution terms. Retain a separate
        // optional pin for sizing when an original definition actually existed at this cutoff.
        let mut terms: Option<PinnedInstrumentDefinitions> = None;
        let mut terms_checked = false;
        let routes = self.durable_routes(instrument_id, as_of, deadline, &cancellation)?;
        let mut sources = Vec::new();
        let mut candidates = Vec::new();
        for route in routes.into_iter().filter(|route| {
            source_scope.is_none_or(|scope| scope.binary_search(route.source_surface()).is_ok())
        }) {
            let selector = MarketEventPointInTimeSelector::new(
                Arc::clone(&self.research),
                route.dataset().clone(),
                route.source_surface().clone(),
            );
            for event_kind in [LiveEventClass::Quote, LiveEventClass::Trade] {
                check_market_read(as_of, deadline, &cancellation)?;
                let child = cancellation.child_token();
                let read = selector.select_current(
                    instrument_id,
                    route.venue_id().clone(),
                    event_kind,
                    as_of,
                    as_of,
                    MAX_SOURCE_EVENT_TIES,
                    deadline,
                    child.clone(),
                );
                let selected = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => { child.cancel(); return Err(ServiceError::Cancelled); }
                    result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), read) => {
                        match result {
                            Ok(Ok(value)) => value,
                            Ok(Err(error)) => return Err(map_market_event_read_error(error)),
                            Err(_) => { child.cancel(); return Err(ServiceError::DeadlineExceeded); }
                        }
                    }
                };
                let Some(receipt) = selected else {
                    continue;
                };
                let Ok(candidate) = single_candidate(receipt.selection()) else {
                    continue;
                };
                let provenance = market_provenance(candidate.event());
                let Some(source_at) = provenance.source_timestamp() else {
                    continue;
                };
                // Archived snapshots can predate the current reference interval. Exclude
                // ineligible marks before requiring the current reference at every mark clock.
                let Some(metadata) = self
                    .research
                    .analytical()
                    .retained_source_metadata(
                        provenance.binding().source_id(),
                        provenance.binding().metadata_revision(),
                        as_of,
                        deadline,
                        &cancellation,
                    )
                    .map_err(map_durable_market_ingest_error)?
                else {
                    continue;
                };
                if provenance.binding().metadata_revision() != metadata.revision()
                    || !metadata.is_effective_at(source_at)
                    || !metadata.is_effective_at(as_of)
                    || provenance.binding().source_id() != metadata.source_id()
                    || provenance.binding().instrument_id() != Some(instrument_id)
                    || provenance.binding().venue_id() != route.venue_id()
                    || matches!(
                        provenance.recorded_quality(),
                        DataQuality::Modeled
                            | DataQuality::Estimated
                            | DataQuality::Stale
                            | DataQuality::Quarantined
                    )
                {
                    continue;
                }
                let freshness = metadata.freshness_policy();
                let age = self
                    .maximum_mark_age_nanos
                    .min(freshness.max_source_age_nanos())
                    .min(freshness.max_market_age_nanos());
                let mut fresh_until = source_at
                    .checked_add_nanos(i64::try_from(age).map_err(|_| ServiceError::InvalidResult)?)
                    .map_err(|_| ServiceError::InvalidResult)?;
                for until in [
                    metadata.authorization().inclusive_authorization_deadline(),
                    metadata.coverage().inclusive_coverage_deadline(),
                ]
                .into_iter()
                .flatten()
                {
                    fresh_until = fresh_until.min(until);
                }
                if fresh_until < as_of || receipt.selection().commit_available_at() > fresh_until {
                    continue;
                }
                let native_price = match candidate.event() {
                    MarketEvent::MarketDataQuote(quote) => {
                        validate_native_reference(
                            quote.reference(),
                            definition,
                            provenance,
                            as_of,
                        )?;
                        true
                    }
                    MarketEvent::MarketDataTrade(trade) => {
                        validate_native_reference(
                            trade.reference(),
                            definition,
                            provenance,
                            as_of,
                        )?;
                        true
                    }
                    MarketEvent::Quote(_) | MarketEvent::Trade(_) => false,
                    _ => continue,
                };
                if !terms_checked {
                    terms = self
                        .definitions
                        .pin_optional(
                            instrument_id,
                            definition.definition().asset_class(),
                            as_of,
                            CatalogLimit::new(MAX_INVESTMENT_MARKET_CANDIDATES)
                                .map_err(|_| ServiceError::Internal)?,
                            deadline,
                            &cancellation,
                        )
                        .map_err(map_catalog_error)?;
                    terms_checked = true;
                }
                let execution_terms = terms.as_ref().and_then(|definitions| {
                    let original = definitions.execution_terms_at(instrument_id, source_at)?;
                    (definitions.execution_terms_at(instrument_id, provenance.received_at())
                        == Some(original)
                        && definitions.execution_terms_at(instrument_id, as_of) == Some(original)
                        && original.instrument_id() == instrument_id)
                        .then_some(original)
                });
                let execution_terms = if native_price {
                    execution_terms.filter(|original| {
                        original.quote_currency() == definition.definition().quote_currency()
                    })
                } else {
                    if execution_terms.is_some_and(|original| {
                        original.quote_currency() != definition.definition().quote_currency()
                    }) {
                        return Err(ServiceError::InvalidResult);
                    }
                    execution_terms
                };
                // Legacy ticks cannot be interpreted without their exact original scale. A native
                // decimal mark remains usable when independently retained sizing terms are absent
                // or do not cover the same identity, asset family, currency and full observation interval.
                if !native_price && execution_terms.is_none() {
                    continue;
                }
                let (authorization, authorized_at) = match self
                    .authorize_local_analysis(receipt.selection(), deadline, &cancellation)
                    .await
                {
                    Ok(admitted) => admitted,
                    Err(ServiceError::Unauthorized) => continue,
                    Err(error) => return Err(error),
                };
                let Some((value, basis)) = market_mark(candidate.event(), execution_terms)? else {
                    continue;
                };
                if sources.len() == MAX_INVESTMENT_MARKET_CANDIDATES {
                    return Err(ServiceError::ResourceExhausted);
                }
                sources
                    .try_reserve(1)
                    .map_err(|_| ServiceError::ResourceExhausted)?;
                candidates
                    .try_reserve(1)
                    .map_err(|_| ServiceError::ResourceExhausted)?;
                let observation_id = SourceIdentifier::try_from(format!(
                    "durable-market-{}",
                    crate::application::domain_support::encode_hex(
                        candidate.coordinate().coordinate_digest().bytes()
                    )
                ))
                .map_err(|_| ServiceError::InvalidResult)?;
                candidates.push(durable_candidate(
                    route.venue_id(),
                    &metadata,
                    &receipt,
                    definition,
                    &observation_id,
                    as_of,
                    &authorization,
                    authorized_at,
                )?);
                sources.push(DurableInvestmentSource {
                    receipt,
                    terms: execution_terms,
                    instrument_id,
                    currency: definition.definition().quote_currency(),
                    asset_class: definition.definition().asset_class(),
                    value,
                    basis,
                    fresh_until,
                    observation_id,
                    rights_input_digest: authorization.rights_input_digest(),
                });
                // The candidate retains exact decision facts and a rights-input digest only. A full
                // current authorization is retained exclusively for the selected final read.
                drop(authorization);
            }
        }
        let request = MarketSelectionRequest::try_new(
            definition.definition().asset_class(),
            MarketOperation::ResearchAnalysis,
            ObservationTiming::Stored,
            None,
            DataQuality::DirectUnverified,
            MarketCoverage::SingleVenue,
            FreshnessRequirement::try_new(
                as_of,
                FreshnessBasis::Source,
                self.maximum_mark_age_nanos,
            )
            .map_err(|_| ServiceError::InvalidRequest)?,
            RequestPriority::Foreground,
            DowngradePolicy::try_new(
                &[],
                &[],
                &[
                    DataQuality::OfficialDelayed,
                    DataQuality::Aggregated,
                    DataQuality::Indicative,
                ],
                &[
                    MarketCoverage::MultiVenuePartial,
                    MarketCoverage::Consolidated,
                ],
                None,
            )
            .map_err(|_| ServiceError::Internal)?,
            Some(definition.revision_digest()),
        )
        .map_err(|_| ServiceError::InvalidRequest)?
        .with_authorization_at(current_market_time()?)
        .map_err(|_| ServiceError::InvalidRequest)?;
        let selection = select_market_source(
            MarketSelectionPolicy::v1(MAX_INVESTMENT_MARKET_CANDIDATES)
                .map_err(|_| ServiceError::Internal)?,
            request,
            candidates,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let Some(selected) = selection.selected() else {
            return Ok(None);
        };
        let selected_id = selected.candidate().identity().observation_id();
        let index = sources
            .iter()
            .position(|source| &source.observation_id == selected_id)
            .ok_or(ServiceError::InvalidResult)?;
        let source = sources.swap_remove(index);
        let (authorization, authorized_at) = self
            .authorize_local_analysis(source.receipt.selection(), deadline, &cancellation)
            .await?;
        if authorization.rights_input_digest() != source.rights_input_digest {
            return Err(ServiceError::InvalidResult);
        }
        let instrument_definitions = terms.filter(|_| source.terms.is_some());
        let mut price_authority = Sha256::new();
        price_authority.update(b"market-squawk/investment-price-authority/v1\0");
        let event = single_candidate(source.receipt.selection())
            .map_err(|_| ServiceError::InvalidResult)?
            .event();
        match event {
            MarketEvent::MarketDataQuote(_) | MarketEvent::MarketDataTrade(_) => {
                // Optional execution terms never replace native monetary price authority.
                price_authority.update([2]);
                price_authority.update(population.receipt_digest().bytes());
                let reference = match event {
                    MarketEvent::MarketDataQuote(value) => value.reference(),
                    MarketEvent::MarketDataTrade(value) => value.reference(),
                    _ => return Err(ServiceError::InvalidResult),
                };
                price_authority.update(
                    serde_json::to_vec(reference).map_err(|_| ServiceError::InvalidResult)?,
                );
            }
            MarketEvent::Quote(_) | MarketEvent::Trade(_) => {
                let definitions = instrument_definitions
                    .as_ref()
                    .ok_or(ServiceError::InvalidResult)?;
                price_authority.update([1]);
                price_authority.update(definitions.audit_identity().bytes());
                price_authority.update(
                    serde_json::to_vec(&source.terms).map_err(|_| ServiceError::InvalidResult)?,
                );
            }
            _ => return Err(ServiceError::InvalidResult),
        }
        let mut receipt = MarketInvestmentReadReceipt {
            selection,
            source,
            definition_digest: population.receipt_digest(),
            price_authority_digest: price_authority.finalize().into(),
            instrument_definitions,
            market_definitions: population,
            source_scope: source_scope.map(|scope| scope.to_vec().into_boxed_slice()),
            evidence_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [0; 32]),
            authorization,
            authorized_at,
        };
        receipt.evidence_digest = durable_mark_digest(&receipt)?;
        check_market_read(as_of, deadline, &cancellation)?;
        Ok(Some(receipt))
    }

    pub(crate) async fn authorize_local_analysis(
        &self,
        selection: &ProviderMarketEventPointInTimeSelection,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(AuthorizedMarketEventUse, Timestamp), ServiceError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if cancellation.is_cancelled() {
            return Err(ServiceError::Cancelled);
        }
        if remaining.is_zero() {
            return Err(ServiceError::DeadlineExceeded);
        }
        let limits = ResearchUseLimits::try_new(
            1,
            4096,
            8192,
            4096,
            4 * 1024 * 1024,
            remaining.min(Duration::from_secs(5)),
            Duration::from_secs(300),
        )
        .map_err(|_| ServiceError::InvalidRequest)?;
        let candidate = single_candidate(selection).map_err(|_| ServiceError::InvalidResult)?;
        let authorization = self
            .research
            .authorize_market_event_use(
                MarketEventUseRequest::try_new(
                    selection.commit().clone(),
                    vec![candidate.coordinate().clone()],
                    ResearchUse::LocalAnalysis,
                    limits,
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
                deadline,
                cancellation,
            )
            .await
            .map_err(crate::application::research::corporate_actions::map_research_error)?
            .map_err(crate::application::research::map_research_use_error)?;
        let admitted_at = current_market_time()?;
        check_market_read(admitted_at, deadline, cancellation)?;
        if authorization.research_use() != ResearchUse::LocalAnalysis
            || authorization.commit() != selection.commit()
            || admitted_at >= authorization.expires_at()
        {
            return Err(ServiceError::Unauthorized);
        }
        Ok((authorization, admitted_at))
    }

    fn durable_routes(
        &self,
        instrument_id: InstrumentId,
        as_of: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Vec<market_squawk_data::ProviderMarketEventDurableRoute>, ServiceError> {
        let mut routes = Vec::new();
        let mut after = None;
        loop {
            check_market_read(as_of, deadline, cancellation)?;
            let page = self
                .research
                .analytical()
                .provider_market_event_durable_routes(
                    instrument_id,
                    &[LiveEventClass::Quote, LiveEventClass::Trade],
                    as_of,
                    as_of,
                    after.as_ref(),
                    MAX_INVESTMENT_MARKET_CANDIDATES,
                    deadline,
                    cancellation,
                )
                .map_err(map_durable_market_ingest_error)?;
            check_market_read(as_of, deadline, cancellation)?;
            let exhausted = page.len() < MAX_INVESTMENT_MARKET_CANDIDATES;
            after = page.last().cloned();
            routes
                .try_reserve(page.len())
                .map_err(|_| ServiceError::ResourceExhausted)?;
            routes.extend(page);
            if exhausted {
                break;
            }
        }
        Ok(routes)
    }

    /// Reopens exact raw/native/committed evidence and rejects changed source selection or terms.
    pub(crate) async fn recheck(
        &self,
        expected: &MarketInvestmentReadReceipt,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ServiceError> {
        let as_of = expected.selection.selected_at();
        let current = self
            .read_scoped(
                expected.instrument_id(),
                as_of,
                expected.source_scope.as_deref(),
                deadline,
                cancellation,
            )
            .await?
            .ok_or(ServiceError::Unavailable)?;
        if current.selection.source_evidence_digest() != expected.selection.source_evidence_digest()
            || current.evidence_digest != expected.evidence_digest
            || current.publication() != expected.publication()
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(())
    }

    /// Reopens a retained reference at its original financial and publication cutoff. Current
    /// product-token membership and current connection state cannot substitute a newer read.
    pub(crate) async fn read_reference(
        &self,
        expected: &MarketInvestmentReadReference,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<MarketInvestmentReadReceipt, ServiceError> {
        expected.validate()?;
        let as_of = expected.source_cutoff()?;
        check_market_read(as_of, deadline, &cancellation)?;
        let reader = self.with_maximum_mark_age_nanos(expected.maximum_mark_age_nanos()?)?;
        let source_scope = if let Some(digests) = &expected.source_scope_digests {
            let routes =
                self.durable_routes(expected.instrument_id(), as_of, deadline, &cancellation)?;
            let mut sources = Vec::new();
            sources
                .try_reserve_exact(digests.len())
                .map_err(|_| ServiceError::ResourceExhausted)?;
            for digest in digests {
                let mut matches = routes
                    .iter()
                    .map(|route| route.source_surface())
                    .filter(|source| source_scope_digest(source) == *digest);
                let source = matches.next().ok_or(ServiceError::Unavailable)?;
                if matches.any(|candidate| candidate != source) {
                    return Err(ServiceError::InvalidResult);
                }
                sources.push(source.clone());
            }
            if sources.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(ServiceError::InvalidRequest);
            }
            Some(sources)
        } else {
            None
        };
        let receipt = reader
            .read_scoped(
                expected.instrument_id(),
                as_of,
                source_scope.as_deref(),
                deadline,
                cancellation,
            )
            .await?
            .ok_or(ServiceError::Unavailable)?;
        if receipt.reference() != *expected {
            return Err(ServiceError::InvalidResult);
        }
        Ok(receipt)
    }
}

fn source_scope_digest(source: &SourceId) -> String {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/market-source-scope/v1\0");
    digest.update(source.as_str().as_bytes());
    crate::application::domain_support::encode_hex(digest.finalize().into())
}

fn single_candidate(
    selection: &ProviderMarketEventPointInTimeSelection,
) -> Result<&ProviderMarketEventSelectedCandidate, MarketInvestmentReadError> {
    if selection.completeness() != ProviderMarketEventSelectionCompleteness::Complete {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    }
    let [source] = selection.sources() else {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    };
    let [candidate] = source.tied_candidates() else {
        return Err(MarketInvestmentReadError::SelectedSourceMismatch);
    };
    Ok(candidate)
}

fn market_provenance(event: &MarketEvent) -> &LiveProvenance {
    match event {
        MarketEvent::Trade(event) => event.provenance(),
        MarketEvent::Quote(event) => event.provenance(),
        MarketEvent::MarketDataQuote(event) => event.provenance(),
        MarketEvent::MarketDataTrade(event) => event.provenance(),
        MarketEvent::MarketDataBook(event) => event.provenance(),
        MarketEvent::MarketDataChart(event) => event.provenance(),
        MarketEvent::MarketDataScreener(event) => event.provenance(),
        MarketEvent::BookSnapshot(event) => event.provenance(),
        MarketEvent::BookDelta(event) => event.provenance(),
        MarketEvent::Auction(event) => event.provenance(),
        MarketEvent::TradingHalt(event) => event.provenance(),
        MarketEvent::InstrumentStatus(event) => event.provenance(),
        MarketEvent::CorporateAction(event) => event.provenance(),
    }
}

fn validate_native_reference(
    reference: &market_squawk_domain::MarketDataReference,
    definition: &market_squawk_data::MarketDataInstrumentRecord,
    provenance: &LiveProvenance,
    knowledge_at: Timestamp,
) -> Result<(), ServiceError> {
    if reference.definition_digest() != definition.revision_digest()
        || provenance.instrument_id() != Some(reference.instrument_id())
        || definition.published_at() > knowledge_at
    {
        return Err(ServiceError::InvalidResult);
    }
    for at in [
        provenance
            .source_timestamp()
            .ok_or(ServiceError::InvalidResult)?,
        provenance.received_at(),
        knowledge_at,
    ] {
        reference
            .validate_definition_at(definition.definition(), at)
            .map_err(|_| ServiceError::InvalidResult)?;
    }
    Ok(())
}

fn market_mark(
    event: &MarketEvent,
    terms: Option<InstrumentExecutionTerms>,
) -> Result<Option<(Decimal, MarketInvestmentMarkBasis)>, ServiceError> {
    let price = |ticks: market_squawk_domain::PriceTicks| {
        ticks
            .checked_to_decimal(terms.ok_or(ServiceError::InvalidResult)?.price_tick())
            .map_err(|_| ServiceError::InvalidResult)
    };
    let (value, basis) = match event {
        MarketEvent::MarketDataTrade(trade) => (
            trade.price().amount(),
            MarketInvestmentMarkBasis::FreshLastTrade,
        ),
        MarketEvent::MarketDataQuote(quote) => {
            let (Some(bid), Some(ask)) = (quote.bid(), quote.ask()) else {
                return Ok(None);
            };
            let (bid, ask) = (bid.price().amount(), ask.price().amount());
            if bid <= Decimal::ZERO || bid > ask {
                return Ok(None);
            }
            (
                bid.checked_add(ask)
                    .and_then(|sum| sum.checked_div(Decimal::TWO))
                    .ok_or(ServiceError::InvalidResult)?,
                MarketInvestmentMarkBasis::FreshBidAskMidpoint,
            )
        }
        MarketEvent::Trade(trade) => (
            price(trade.price())?,
            MarketInvestmentMarkBasis::FreshLastTrade,
        ),
        MarketEvent::Quote(quote) => {
            let (Some(bid), Some(ask)) = (quote.bid(), quote.ask()) else {
                return Ok(None);
            };
            let (bid, ask) = (price(bid.price())?, price(ask.price())?);
            if bid <= Decimal::ZERO || bid > ask {
                return Ok(None);
            }
            (
                bid.checked_add(ask)
                    .and_then(|sum| sum.checked_div(Decimal::TWO))
                    .ok_or(ServiceError::InvalidResult)?,
                MarketInvestmentMarkBasis::FreshBidAskMidpoint,
            )
        }
        _ => return Ok(None),
    };
    Ok((value > Decimal::ZERO).then_some((value, basis)))
}

fn durable_candidate(
    venue_id: &VenueId,
    metadata: &SourceMetadata,
    receipt: &MarketEventPointInTimeReceipt,
    definition: &market_squawk_data::MarketDataInstrumentRecord,
    observation_id: &SourceIdentifier,
    as_of: Timestamp,
    authorization: &AuthorizedMarketEventUse,
    authorized_at: Timestamp,
) -> Result<SourceCandidate, ServiceError> {
    let candidate =
        single_candidate(receipt.selection()).map_err(|_| ServiceError::InvalidResult)?;
    let provenance = market_provenance(candidate.event());
    let live = metadata
        .coverage()
        .live()
        .ok_or(ServiceError::InvalidResult)?;
    let operations = MarketOperationSet::try_new(&[
        MarketOperation::ResearchAnalysis,
        MarketOperation::PortfolioMark,
        MarketOperation::ModelInput,
        MarketOperation::Backtest,
    ])
    .map_err(|_| ServiceError::Internal)?;
    let rights = RightsAdmission::try_admitted(
        SourceIdentifier::try_from(format!(
            "research-use-{}",
            crate::application::domain_support::encode_hex(authorization.decision_digest().bytes())
        ))
        .map_err(|_| ServiceError::InvalidResult)?,
        operations,
        authorized_at,
        authorized_at,
        Some(
            authorization
                .expires_at()
                .checked_sub_nanos(1)
                .map_err(|_| ServiceError::InvalidResult)?,
        ),
    )
    .map_err(|_| ServiceError::InvalidResult)?;
    // The archive's recorded DirectVerified label is not a live qualification receipt.
    let quality = match provenance.recorded_quality() {
        DataQuality::DirectVerified => DataQuality::DirectUnverified,
        quality => quality,
    };
    if live.provider_product() != provenance.binding().provider_product()
        || live.provider_channel() != provenance.binding().provider_channel()
        || !live
            .rules()
            .iter()
            .any(|rule| rule.event_class() == provenance.binding().event_class())
    {
        return Err(ServiceError::InvalidResult);
    }
    let quality = if super::requirements::quality_preference(quality)
        > super::requirements::quality_preference(metadata.quality_ceiling())
    {
        metadata.quality_ceiling()
    } else {
        quality
    };
    let topology = metadata.coverage().topology();
    let coverage = if topology.is_consolidated() {
        MarketCoverage::Consolidated
    } else if topology.is_partial() {
        MarketCoverage::MultiVenuePartial
    } else {
        MarketCoverage::SingleVenue
    };
    SourceCandidate::try_new(
        CandidateIdentity::new(
            metadata.provider().clone(),
            provenance.binding().provider_product().clone(),
            provenance.binding().provider_channel().clone(),
            metadata.source_id().clone(),
            Some(venue_id.clone()),
            definition.definition().instrument_id(),
            observation_id.clone(),
            Some(definition.revision_digest()),
        ),
        CandidateCapabilities::try_new(
            definition.definition().asset_class(),
            operations,
            ObservationTiming::Stored,
            matches!(
                candidate.event(),
                MarketEvent::Quote(_) | MarketEvent::MarketDataQuote(_)
            )
            .then_some(MarketDepth::TopOfBook),
            quality,
            coverage,
        )
        .map_err(|_| ServiceError::InvalidResult)?,
        CandidateTimestamps::try_new(
            provenance
                .source_timestamp()
                .ok_or(ServiceError::InvalidResult)?,
            provenance.source_timestamp(),
            provenance.received_at(),
            provenance
                .available_at()
                .max(candidate.coordinate().origin_committed_at()),
            receipt.selection().commit_available_at(),
        )
        .map_err(|_| ServiceError::InvalidResult)?,
        CandidateAdmissionState::new(
            CandidateHealth::new(HealthState::Degraded, provenance.available_at()),
            ProviderBudgetSnapshot::try_new(BudgetAvailability::NotRequired, None, None, as_of)
                .map_err(|_| ServiceError::InvalidResult)?,
            rights,
            CandidateIntegrity::new(
                IntegrityState::Unverified,
                Some(provenance.connection_generation()),
                candidate.coordinate().origin_committed_at(),
            ),
            ExecutionEligibility::Ineligible,
        ),
    )
    .map_err(|_| ServiceError::InvalidResult)
}

fn durable_mark_digest(
    receipt: &MarketInvestmentReadReceipt,
) -> Result<EvidenceDigest, ServiceError> {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/durable-investment-mark/v1\0");
    hash.update(receipt.selection.source_evidence_digest().bytes());
    hash.update(receipt.source.rights_input_digest.bytes());
    hash.update(receipt.publication().selection_digest().bytes());
    hash.update(receipt.definition_digest.bytes());
    hash.update(receipt.price_authority_digest);
    // Independent sizing authority is separately committed even for a native monetary price.
    match receipt.instrument_definitions.as_ref() {
        Some(definitions) => {
            hash.update([1]);
            hash.update(definitions.content_identity().bytes());
            hash.update(definitions.audit_identity().bytes());
        }
        None => hash.update([0]),
    }
    hash.update(
        serde_json::to_vec(&receipt.source.terms).map_err(|_| ServiceError::InvalidResult)?,
    );
    hash.update(receipt.source.value.normalize().serialize());
    hash.update([match receipt.source.basis {
        MarketInvestmentMarkBasis::FreshLastTrade => 1,
        MarketInvestmentMarkBasis::FreshBidAskMidpoint => 2,
    }]);
    hash.update(receipt.source.fresh_until.unix_nanos().to_be_bytes());
    if let Some(scope) = &receipt.source_scope {
        for source in scope {
            hash.update((source.as_str().len() as u64).to_be_bytes());
            hash.update(source.as_str().as_bytes());
        }
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

pub(crate) fn map_market_event_read_error(error: MarketEventReadError) -> ServiceError {
    match error {
        MarketEventReadError::Ingest(error) => map_durable_market_ingest_error(error),
        MarketEventReadError::PointInTime(error) => map_point_in_time_read_error(error),
        MarketEventReadError::DurableGenerationInvalid
        | MarketEventReadError::RestartInvalid
        | MarketEventReadError::PointInTimeInvalid
        | MarketEventReadError::DurableReadConflict => ServiceError::InvalidResult,
    }
}

fn current_market_time() -> Result<Timestamp, ServiceError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?
        .as_nanos();
    Ok(Timestamp::from_unix_nanos(
        i64::try_from(nanos).map_err(|_| ServiceError::Unavailable)?,
    ))
}

fn check_market_read(
    as_of: Timestamp,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ServiceError> {
    if as_of.unix_nanos() <= 0 {
        Err(ServiceError::InvalidRequest)
    } else if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
