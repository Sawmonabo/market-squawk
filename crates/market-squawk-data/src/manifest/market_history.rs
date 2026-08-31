//! Durable, completeness-bound market-bar history publication and restart selection.

use std::{fmt, time::Instant};

use market_squawk_domain::{
    AssetClass, AvailabilityEvidence as ResearchAvailabilityEvidence, BarTimestampBasis, Currency,
    DataQuality, DigestAlgorithm, EvidenceDigest, InstrumentId, MarketBarAdjustment,
    MarketBarObservation, MarketBarSessionKind, MarketDataInstrumentDefinition, PayloadReference,
    ProviderInstrumentId, ResearchObservation, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    AvailabilityEvidence as ExtractionAvailabilityEvidence, CanonicalObservationPayload,
    ExtractionBatch, MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS, ProviderCaptureSemanticBinding,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::catalog::load_pinned;
use super::{
    DatasetId, DatasetManifestRef, ManifestCatalogError, ManifestPlan, PinnedDataset, Sha256Digest,
};
use crate::catalog::{PreparedProviderCaptureBinding, load_provider_capture_for_run};
use crate::schema::{DatasetSchemaRef, DatasetSchemaRegistry};
use crate::{ArtifactRecord, DatasetManifestRecord, IngestRunRecord};

const MARKET_BAR_HISTORY_RECEIPT_VERSION: u16 = 1;
const MAX_GENERATION_MARKET_BAR_HISTORY_INPUTS: usize = 4_096;
const RECEIPT_DIGEST_DOMAIN: &[u8] = b"market-squawk/market-bar-history-publication/v1";
const EXPECTED_TIMESTAMP_SET_DOMAIN: &[u8] =
    b"market-squawk/market-bar-history-expected-timestamps/v1";
const BAR_SET_DOMAIN: &[u8] = b"market-squawk/market-bar-history-normalized-bars/v1";
const CURRENT_RESEARCH_ADMISSION: &str = "current_research_only";
const CURRENT_RESEARCH_REASON: &str = "local_first_observed_without_provider_publication_time";
const ALPACA_HISTORY_GRAPH_PURPOSE: &str = "alpaca-iex-historical-bars-and-calendar/v1";
const ALPACA_HISTORY_SOURCE: &str = "alpaca-basic-iex-market-data";
const ALPACA_HISTORY_ASSET_CLASSES: &str = "equity,fund";
const ALPACA_HISTORY_VENUE: &str = "iex";
const ALPACA_HISTORY_FEED: &str = "iex";
const ALPACA_HISTORY_INTERVAL: &str = "1Day";
const ALPACA_HISTORY_ADJUSTMENT: &str = "all";
const ALPACA_HISTORY_TIMESTAMP_BASIS: &str = "period_start";
const ALPACA_HISTORY_SESSION_KIND: &str = "provider_defined";
const ALPACA_HISTORY_SESSION_RULESET: &str = "alpaca-v3-iex-utc-range-returned-dates-v2";
const ALPACA_HISTORY_CURRENCY: &str = "USD";
const ALPACA_HISTORY_SELECTION_POLICY_VERSION: u16 = 1;
const ALPACA_HISTORY_POLICY_DOMAIN: &[u8] = b"market-squawk/alpaca-history-selection-policy/v1";
const ALPACA_HISTORY_SELECTION_DOMAIN: &[u8] = b"market-squawk/alpaca-history-selection/v1";
const LATEST_CANONICAL_HISTORY_WINDOW_SELECTION_DOMAIN: &[u8] =
    b"market-squawk/latest-canonical-market-bar-history-window-selection/v1";

/// Opaque, versioned policy for canonical durable market-history selection.
///
/// V1 resolves only the complete Alpaca Basic/IEX daily adjusted product. Provider, account,
/// symbol, feed, venue, adjustment, timestamp, and session coordinates are code-owned and cannot
/// be supplied through this value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketHistorySelectionPolicy {
    version: u16,
}

impl MarketHistorySelectionPolicy {
    /// Complete daily adjusted history under the sole supported V1 durable policy.
    pub const COMPLETE_DAILY_ADJUSTED_V1: Self = Self {
        version: ALPACA_HISTORY_SELECTION_POLICY_VERSION,
    };

    /// Returns the stable policy version bound into selection evidence.
    pub const fn version(self) -> u16 {
        self.version
    }

    const fn is_supported(self) -> bool {
        self.version == ALPACA_HISTORY_SELECTION_POLICY_VERSION
    }
}

/// Provider-neutral request for the latest complete durable history window known at one cutoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LatestCanonicalMarketBarHistoryWindowRequest {
    instrument_id: InstrumentId,
    selection_policy: MarketHistorySelectionPolicy,
    knowledge_cutoff: Timestamp,
}

impl LatestCanonicalMarketBarHistoryWindowRequest {
    /// Constructs one latest-window lookup under the code-owned daily adjusted policy.
    ///
    /// # Errors
    ///
    /// Rejects an unsupported policy version.
    pub fn try_new(
        instrument_id: InstrumentId,
        selection_policy: MarketHistorySelectionPolicy,
        knowledge_cutoff: Timestamp,
    ) -> Result<Self, ManifestCatalogError> {
        if !selection_policy.is_supported() {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        Ok(Self {
            instrument_id,
            selection_policy,
            knowledge_cutoff,
        })
    }

    /// Returns the canonical instrument coordinate.
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the sole code-owned, versioned selection policy.
    pub const fn selection_policy(self) -> MarketHistorySelectionPolicy {
        self.selection_policy
    }

    /// Returns the inclusive trusted internal knowledge cutoff.
    pub const fn knowledge_cutoff(self) -> Timestamp {
        self.knowledge_cutoff
    }
}

/// Precommit proof reconstructed from normalized rows and a hash-bound provider request graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MarketBarHistoryPublicationCandidate {
    binding_digest: Sha256Digest,
    source_id: SourceId,
    capture_receipt_digest: Sha256Digest,
    capture_content_digest: Sha256Digest,
    capture_observation_digest: Sha256Digest,
    provider_dataset: SourceIdentifier,
    instrument_id: InstrumentId,
    instrument_revision_digest: Sha256Digest,
    admitted_plan_digest: Sha256Digest,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_ruleset: SourceIdentifier,
    graph_purpose: SourceIdentifier,
    requested_start: Timestamp,
    requested_end: Timestamp,
    coverage_first: Timestamp,
    coverage_last: Timestamp,
    coverage_last_complete: Timestamp,
    expected_bar_count: usize,
    expected_timestamp_set_digest: Sha256Digest,
    bar_set_digest: Sha256Digest,
    completeness_evidence_digest: Sha256Digest,
    market_bar_component_ordinal: u16,
    market_bar_component_content_digest: Sha256Digest,
    market_bar_component_page_count: u16,
    session_calendar_component_ordinal: u16,
    session_calendar_component_content_digest: Sha256Digest,
    session_calendar_component_page_count: u16,
    currency: Currency,
    max_available_at: Timestamp,
    max_received_at: Timestamp,
    max_ingested_at: Timestamp,
}

impl MarketBarHistoryPublicationCandidate {
    /// Validates the typed capture semantic against every normalized record before publication.
    pub(crate) fn try_from_batch(
        batch: &ExtractionBatch,
        observations: &[ResearchObservation],
        prepared: Option<&PreparedProviderCaptureBinding>,
    ) -> Result<Option<Self>, ManifestCatalogError> {
        let Some(prepared) = prepared else {
            return Ok(None);
        };
        let capture = prepared.evidence.capture();
        let binding = match capture.semantic_binding() {
            Some(ProviderCaptureSemanticBinding::CompleteMarketBarHistoryV1(binding)) => binding,
            None if capture.terminal()
                == ProviderCaptureTerminalDisposition::CompleteRequestGraph
                && !observations.is_empty()
                && observations.iter().all(|observation| {
                    matches!(observation, ResearchObservation::MarketBar(_))
                }) =>
            {
                return Err(ManifestCatalogError::MarketBarHistoryMismatch);
            }
            None => return Ok(None),
        };
        if capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
            || capture.request_graph_components().len() != 2
            || batch.records().len() != observations.len()
            || observations.len() != binding.expected_provider_timestamps().len()
            || observations.is_empty()
            || observations.len() > MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
            || capture.source_id() != batch.request().object().source_id()
            || capture.dataset() != batch.request().object().dataset()
            || binding.graph_purpose().as_str() != ALPACA_HISTORY_GRAPH_PURPOSE
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        let components = capture.request_graph_components();
        let market_bar_component = components
            .get(usize::from(binding.market_bar_component_ordinal()))
            .filter(|component| component.ordinal() == binding.market_bar_component_ordinal())
            .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
        let session_calendar_component = components
            .get(usize::from(binding.session_calendar_component_ordinal()))
            .filter(|component| component.ordinal() == binding.session_calendar_component_ordinal())
            .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
        if binding.market_bar_component_ordinal() != 0
            || binding.session_calendar_component_ordinal() != 1
            || market_bar_component.terminal()
                != ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
            || session_calendar_component.terminal()
                != ProviderCaptureTerminalDisposition::StandaloneResponse
            || market_bar_component.dataset() != capture.dataset()
            || session_calendar_component.dataset() != capture.dataset()
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        let market_bar_page_start = usize::from(market_bar_component.first_page_ordinal());
        let market_bar_page_end = market_bar_page_start
            .checked_add(usize::from(market_bar_component.page_count().get()))
            .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
        let market_bar_pages = capture
            .pages()
            .get(market_bar_page_start..market_bar_page_end)
            .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
        if batch.request().object().evidence().content_digest()
            != market_bar_component.content_digest()
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }

        let mut coverage_first = None;
        let mut coverage_last = None;
        let mut coverage_last_complete = None;
        let mut currency = None;
        let mut common_session = None;
        let mut max_available_at = None;
        let mut max_received_at = None;
        let mut max_ingested_at = None;
        let mut bar_set = Sha256::new();
        bar_set.update(BAR_SET_DOMAIN);
        bar_set.update((observations.len() as u64).to_be_bytes());

        for ((record, observation), expected_timestamp) in batch
            .records()
            .iter()
            .zip(observations)
            .zip(binding.expected_provider_timestamps())
        {
            let ResearchObservation::MarketBar(bar) = observation else {
                return Err(ManifestCatalogError::MarketBarHistoryMismatch);
            };
            let context = bar.context();
            let provenance = context.provenance();
            let time = context.time();
            let semantics = bar.time_semantics();
            let provider_timestamp = semantics.provider_timestamp();
            let available_at = match record.availability() {
                ExtractionAvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at,
                ExtractionAvailabilityEvidence::Observed { .. }
                | ExtractionAvailabilityEvidence::Inferred { .. }
                | ExtractionAvailabilityEvidence::Unknown => {
                    return Err(ManifestCatalogError::MarketBarHistoryMismatch);
                }
            };
            let observation_available_at = match provenance.availability() {
                ResearchAvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at,
                ResearchAvailabilityEvidence::Evidenced { .. }
                | ResearchAvailabilityEvidence::Inferred { .. }
                | ResearchAvailabilityEvidence::Unknown => {
                    return Err(ManifestCatalogError::MarketBarHistoryMismatch);
                }
            };
            let provider_page_matches = match provenance.payload_reference() {
                PayloadReference::ContentHash(payload_hash)
                    if payload_hash.algorithm() == DigestAlgorithm::Sha256 =>
                {
                    market_bar_pages.iter().any(|page| {
                        page.body_digest().bytes() == payload_hash.digest()
                            && page.received_at() == provenance.received_at()
                    })
                }
                PayloadReference::ContentHash(_) | PayloadReference::SourceReference(_) => false,
            };
            if provider_timestamp != *expected_timestamp
                || record.effective_time().exact_timestamp() != Some(provider_timestamp)
                || record.published_time().is_some()
                || record.superseded_time().is_some()
                || time.published().is_some()
                || time.superseded().is_some()
                || provenance.instrument_id() != Some(binding.instrument_id())
                || provenance.venue_id() != Some(binding.venue_id())
                || provenance.source_id() != capture.source_id()
                || provenance.source_timestamp() != Some(provider_timestamp)
                || provenance.quality() != DataQuality::Aggregated
                || bar.provider_instrument_id() != binding.provider_instrument_id()
                || bar.feed() != binding.feed()
                || bar.interval() != binding.interval()
                || bar.adjustment() != binding.adjustment()
                || semantics.timestamp_basis() != binding.timestamp_basis()
                || semantics.session().kind() != binding.session_kind()
                || semantics.session().ruleset() != binding.session_ruleset()
                || semantics.session().evidence() != binding.completeness_evidence()
                || provider_timestamp < binding.requested_start()
                || provider_timestamp > binding.requested_end()
                || semantics.period_end_exclusive() > binding.requested_end()
                || available_at != observation_available_at
                || !provider_page_matches
                || available_at != provenance.received_at()
                || provenance.received_at() > provenance.ingested_at()
                || available_at > provenance.ingested_at()
            {
                return Err(ManifestCatalogError::MarketBarHistoryMismatch);
            }
            if let Some(session) = common_session.as_ref() {
                if session != semantics.session() {
                    return Err(ManifestCatalogError::MarketBarHistoryMismatch);
                }
            } else {
                common_session = Some(semantics.session().clone());
            }
            if let Some(expected_currency) = currency {
                if expected_currency != bar.currency() {
                    return Err(ManifestCatalogError::MarketBarHistoryMismatch);
                }
            } else {
                currency = Some(bar.currency());
            }
            coverage_first.get_or_insert(provider_timestamp);
            coverage_last = Some(provider_timestamp);
            coverage_last_complete = Some(
                coverage_last_complete
                    .map_or(semantics.period_end_exclusive(), |current: Timestamp| {
                        current.max(semantics.period_end_exclusive())
                    }),
            );
            max_available_at = Some(
                max_available_at
                    .map_or(available_at, |current: Timestamp| current.max(available_at)),
            );
            max_received_at = Some(
                max_received_at.map_or(provenance.received_at(), |current: Timestamp| {
                    current.max(provenance.received_at())
                }),
            );
            max_ingested_at = Some(
                max_ingested_at.map_or(provenance.ingested_at(), |current: Timestamp| {
                    current.max(provenance.ingested_at())
                }),
            );
            let semantic_payload = CanonicalObservationPayload::try_from_observation(observation)
                .map_err(|_| ManifestCatalogError::MarketBarHistoryMismatch)?;
            bar_set.update(provider_timestamp.unix_nanos().to_be_bytes());
            // The payload identity binds the exact normalized OHLCV/trade-count/VWAP/session
            // semantics while deliberately excluding revision assignment and provenance clocks.
            // The immutable origin object and capture digests separately bind finalized rows and
            // provider bytes, so this set digest remains reproducible after restart.
            hash_evidence(&mut bar_set, semantic_payload.identity());
        }

        let expected_timestamp_set_digest =
            expected_timestamp_set_digest(binding.expected_provider_timestamps())?;
        let bar_set_digest = nonzero_sha256(bar_set.finalize().into())?;
        Ok(Some(Self {
            binding_digest: sha256_evidence(prepared.binding_digest())?,
            source_id: capture.source_id().clone(),
            capture_receipt_digest: sha256_evidence(
                prepared.evidence.sealed_capture_receipt_digest(),
            )?,
            capture_content_digest: sha256_evidence(capture.content_digest())?,
            capture_observation_digest: sha256_evidence(capture.observation_digest())?,
            provider_dataset: capture.dataset().clone(),
            instrument_id: binding.instrument_id(),
            instrument_revision_digest: sha256_evidence(binding.instrument_revision_digest())?,
            admitted_plan_digest: sha256_evidence(binding.admitted_plan_digest())?,
            provider_instrument_id: binding.provider_instrument_id().clone(),
            venue_id: binding.venue_id().clone(),
            feed: binding.feed().clone(),
            interval: binding.interval().clone(),
            adjustment: binding.adjustment(),
            timestamp_basis: binding.timestamp_basis(),
            session_kind: binding.session_kind(),
            session_ruleset: binding.session_ruleset().clone(),
            graph_purpose: binding.graph_purpose().clone(),
            requested_start: binding.requested_start(),
            requested_end: binding.requested_end(),
            coverage_first: coverage_first.ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?,
            coverage_last: coverage_last.ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?,
            coverage_last_complete: coverage_last_complete
                .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?,
            expected_bar_count: observations.len(),
            expected_timestamp_set_digest,
            bar_set_digest,
            completeness_evidence_digest: sha256_evidence(binding.completeness_evidence())?,
            market_bar_component_ordinal: binding.market_bar_component_ordinal(),
            market_bar_component_content_digest: sha256_evidence(
                market_bar_component.content_digest(),
            )?,
            market_bar_component_page_count: market_bar_component.page_count().get(),
            session_calendar_component_ordinal: binding.session_calendar_component_ordinal(),
            session_calendar_component_content_digest: sha256_evidence(
                session_calendar_component.content_digest(),
            )?,
            session_calendar_component_page_count: session_calendar_component.page_count().get(),
            currency: currency.ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?,
            max_available_at: max_available_at
                .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?,
            max_received_at: max_received_at
                .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?,
            max_ingested_at: max_ingested_at
                .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?,
        }))
    }
}

/// Provider-neutral immutable-read request for one exact canonical daily history window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalMarketBarHistoryRequest {
    instrument_id: InstrumentId,
    requested_start: Timestamp,
    requested_end: Timestamp,
    selection_policy: MarketHistorySelectionPolicy,
    knowledge_cutoff: Timestamp,
    exact_manifest: Option<DatasetManifestRef>,
}

impl CanonicalMarketBarHistoryRequest {
    /// Selects the latest durable series for one exact normalized inclusive provider range.
    ///
    /// # Errors
    ///
    /// Rejects an empty or reversed window or an unsupported policy version.
    pub fn try_latest(
        instrument_id: InstrumentId,
        requested_start: Timestamp,
        requested_end: Timestamp,
        selection_policy: MarketHistorySelectionPolicy,
        knowledge_cutoff: Timestamp,
    ) -> Result<Self, ManifestCatalogError> {
        Self::try_new(
            instrument_id,
            requested_start,
            requested_end,
            selection_policy,
            knowledge_cutoff,
            None,
        )
    }

    /// Selects a durable series only through the supplied immutable generation.
    ///
    /// This constructor is retained for internal replay and tests; it never falls back to latest.
    ///
    /// # Errors
    ///
    /// Rejects an empty or reversed window or an unsupported policy version.
    pub fn try_exact(
        instrument_id: InstrumentId,
        requested_start: Timestamp,
        requested_end: Timestamp,
        selection_policy: MarketHistorySelectionPolicy,
        knowledge_cutoff: Timestamp,
        manifest: DatasetManifestRef,
    ) -> Result<Self, ManifestCatalogError> {
        Self::try_new(
            instrument_id,
            requested_start,
            requested_end,
            selection_policy,
            knowledge_cutoff,
            Some(manifest),
        )
    }

    fn try_new(
        instrument_id: InstrumentId,
        requested_start: Timestamp,
        requested_end: Timestamp,
        selection_policy: MarketHistorySelectionPolicy,
        knowledge_cutoff: Timestamp,
        exact_manifest: Option<DatasetManifestRef>,
    ) -> Result<Self, ManifestCatalogError> {
        if requested_start >= requested_end || !selection_policy.is_supported() {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        Ok(Self {
            instrument_id,
            requested_start,
            requested_end,
            selection_policy,
            knowledge_cutoff,
            exact_manifest,
        })
    }

    /// Returns the canonical instrument coordinate.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact normalized inclusive provider range.
    pub const fn requested_range(&self) -> (Timestamp, Timestamp) {
        (self.requested_start, self.requested_end)
    }

    /// Returns the sole code-owned, versioned selection policy.
    pub const fn selection_policy(&self) -> MarketHistorySelectionPolicy {
        self.selection_policy
    }

    /// Returns the inclusive trusted internal knowledge cutoff.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the optional exact immutable generation pin used for replay or tests.
    pub const fn exact_manifest(&self) -> Option<&DatasetManifestRef> {
        self.exact_manifest.as_ref()
    }
}

/// Opaque catalog selection that hands the exact immutable request to the existing reader.
#[derive(Clone, Eq, PartialEq)]
pub struct LatestCanonicalMarketBarHistoryWindowSelection {
    exact_request: CanonicalMarketBarHistoryRequest,
    lookup_digest: Sha256Digest,
}

impl LatestCanonicalMarketBarHistoryWindowSelection {
    /// Returns the canonical instrument coordinate.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.exact_request.instrument_id()
    }

    /// Returns the exact selected provider-neutral financial window.
    pub const fn requested_range(&self) -> (Timestamp, Timestamp) {
        self.exact_request.requested_range()
    }

    /// Returns the code-owned policy that selected this window.
    pub const fn selection_policy(&self) -> MarketHistorySelectionPolicy {
        self.exact_request.selection_policy()
    }

    /// Returns the inclusive trusted knowledge cutoff used by selection.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.exact_request.knowledge_cutoff()
    }

    /// Returns the digest binding the policy, cutoff, complete window, and immutable evidence.
    pub const fn lookup_digest(&self) -> Sha256Digest {
        self.lookup_digest
    }

    /// Returns the exact internally manifest-pinned request for the existing durable reader.
    pub const fn exact_request(&self) -> &CanonicalMarketBarHistoryRequest {
        &self.exact_request
    }

    /// Transfers the exact internally manifest-pinned request to the existing durable reader.
    pub fn into_exact_request(self) -> CanonicalMarketBarHistoryRequest {
        self.exact_request
    }
}

impl fmt::Debug for LatestCanonicalMarketBarHistoryWindowSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LatestCanonicalMarketBarHistoryWindowSelection")
            .field("instrument_id", &self.instrument_id())
            .field("requested_range", &self.requested_range())
            .field("policy_version", &self.selection_policy().version())
            .field("knowledge_cutoff", &self.knowledge_cutoff())
            .field("lookup_digest", &self.lookup_digest)
            .field("immutable_generation", &"[OPAQUE]")
            .finish()
    }
}

/// Exact current-research selection request for one fixed provider history series and window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteMarketBarHistoryRequest {
    instrument_id: InstrumentId,
    requested_start: Timestamp,
    requested_end: Timestamp,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_ruleset: SourceIdentifier,
    knowledge_cutoff: Timestamp,
    exact_manifest: Option<DatasetManifestRef>,
}

impl CompleteMarketBarHistoryRequest {
    /// Selects the latest complete generation for exactly these typed coordinates at the cutoff.
    ///
    /// # Errors
    ///
    /// Rejects an empty or reversed requested window.
    #[allow(
        clippy::too_many_arguments,
        reason = "the request must name every non-interchangeable market-bar series coordinate"
    )]
    pub fn try_latest(
        instrument_id: InstrumentId,
        requested_start: Timestamp,
        requested_end: Timestamp,
        provider_instrument_id: ProviderInstrumentId,
        venue_id: VenueId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        adjustment: MarketBarAdjustment,
        timestamp_basis: BarTimestampBasis,
        session_kind: MarketBarSessionKind,
        session_ruleset: SourceIdentifier,
        knowledge_cutoff: Timestamp,
    ) -> Result<Self, ManifestCatalogError> {
        Self::try_new(
            instrument_id,
            requested_start,
            requested_end,
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            adjustment,
            timestamp_basis,
            session_kind,
            session_ruleset,
            knowledge_cutoff,
            None,
        )
    }

    /// Selects these typed coordinates only from the supplied immutable generation.
    ///
    /// No latest-generation fallback is allowed.
    ///
    /// # Errors
    ///
    /// Rejects an empty or reversed requested window.
    #[allow(
        clippy::too_many_arguments,
        reason = "the request must name every non-interchangeable market-bar series coordinate"
    )]
    pub fn try_exact(
        instrument_id: InstrumentId,
        requested_start: Timestamp,
        requested_end: Timestamp,
        provider_instrument_id: ProviderInstrumentId,
        venue_id: VenueId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        adjustment: MarketBarAdjustment,
        timestamp_basis: BarTimestampBasis,
        session_kind: MarketBarSessionKind,
        session_ruleset: SourceIdentifier,
        knowledge_cutoff: Timestamp,
        manifest: DatasetManifestRef,
    ) -> Result<Self, ManifestCatalogError> {
        Self::try_new(
            instrument_id,
            requested_start,
            requested_end,
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            adjustment,
            timestamp_basis,
            session_kind,
            session_ruleset,
            knowledge_cutoff,
            Some(manifest),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the request must name every non-interchangeable market-bar series coordinate"
    )]
    fn try_new(
        instrument_id: InstrumentId,
        requested_start: Timestamp,
        requested_end: Timestamp,
        provider_instrument_id: ProviderInstrumentId,
        venue_id: VenueId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        adjustment: MarketBarAdjustment,
        timestamp_basis: BarTimestampBasis,
        session_kind: MarketBarSessionKind,
        session_ruleset: SourceIdentifier,
        knowledge_cutoff: Timestamp,
        exact_manifest: Option<DatasetManifestRef>,
    ) -> Result<Self, ManifestCatalogError> {
        if requested_start >= requested_end {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        Ok(Self {
            instrument_id,
            requested_start,
            requested_end,
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            adjustment,
            timestamp_basis,
            session_kind,
            session_ruleset,
            knowledge_cutoff,
            exact_manifest,
        })
    }

    /// Returns the canonical instrument coordinate.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact inclusive provider request window.
    pub const fn requested_range(&self) -> (Timestamp, Timestamp) {
        (self.requested_start, self.requested_end)
    }

    /// Returns the provider-native instrument coordinate.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the venue coordinate.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the provider feed coordinate.
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }

    /// Returns the bar interval coordinate.
    pub const fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }

    /// Returns the provider adjustment coordinate.
    pub const fn adjustment(&self) -> MarketBarAdjustment {
        self.adjustment
    }

    /// Returns the provider timestamp anchor coordinate.
    pub const fn timestamp_basis(&self) -> BarTimestampBasis {
        self.timestamp_basis
    }

    /// Returns the session-family coordinate.
    pub const fn session_kind(&self) -> MarketBarSessionKind {
        self.session_kind
    }

    /// Returns the stable session-ruleset coordinate.
    pub const fn session_ruleset(&self) -> &SourceIdentifier {
        &self.session_ruleset
    }

    /// Returns the inclusive local-knowledge cutoff.
    pub const fn knowledge_cutoff(&self) -> Timestamp {
        self.knowledge_cutoff
    }

    /// Returns the optional exact immutable generation pin.
    pub const fn exact_manifest(&self) -> Option<&DatasetManifestRef> {
        self.exact_manifest.as_ref()
    }

    fn matches_receipt(&self, receipt: &MarketBarHistoryPublicationReceipt) -> bool {
        receipt.instrument_id() == self.instrument_id
            && receipt.requested_range() == self.requested_range()
            && receipt.provider_instrument_id() == &self.provider_instrument_id
            && receipt.venue_id() == &self.venue_id
            && receipt.feed() == &self.feed
            && receipt.interval() == &self.interval
            && receipt.adjustment() == self.adjustment
            && receipt.timestamp_basis() == self.timestamp_basis
            && receipt.session_kind() == self.session_kind
            && receipt.session_ruleset() == &self.session_ruleset
    }
}

/// Versioned immutable receipt for one exactly complete provider request window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketBarHistoryPublicationReceipt {
    receipt_digest: Sha256Digest,
    origin_manifest: DatasetManifestRef,
    origin_run_id: Uuid,
    origin_artifact_id: Uuid,
    origin_object_ordinal: u16,
    source_id: SourceId,
    binding_digest: Sha256Digest,
    capture_receipt_digest: Sha256Digest,
    capture_content_digest: Sha256Digest,
    capture_observation_digest: Sha256Digest,
    capture_recorded_at: Timestamp,
    provider_dataset: SourceIdentifier,
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    instrument_revision_digest: Sha256Digest,
    admitted_plan_digest: Sha256Digest,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_ruleset: SourceIdentifier,
    graph_purpose: SourceIdentifier,
    requested_start: Timestamp,
    requested_end: Timestamp,
    coverage_first: Timestamp,
    coverage_last: Timestamp,
    coverage_last_complete: Timestamp,
    expected_bar_count: usize,
    expected_provider_timestamps: Box<[Timestamp]>,
    expected_timestamp_set_digest: Sha256Digest,
    bar_set_digest: Sha256Digest,
    completeness_evidence_digest: Sha256Digest,
    market_bar_component_ordinal: u16,
    market_bar_component_content_digest: Sha256Digest,
    market_bar_component_page_count: u16,
    session_calendar_component_ordinal: u16,
    session_calendar_component_content_digest: Sha256Digest,
    session_calendar_component_page_count: u16,
    currency: Currency,
    max_available_at: Timestamp,
    max_received_at: Timestamp,
    max_ingested_at: Timestamp,
    published_at: Timestamp,
}

impl MarketBarHistoryPublicationReceipt {
    /// Returns the hash of the canonical versioned receipt JSON.
    pub const fn receipt_digest(&self) -> Sha256Digest {
        self.receipt_digest
    }

    /// Returns the original immutable generation that published this complete window.
    pub const fn origin_manifest(&self) -> &DatasetManifestRef {
        &self.origin_manifest
    }

    /// Returns the ingest run that owned the exact provider capture and publication.
    pub const fn origin_run_id(&self) -> Uuid {
        self.origin_run_id
    }

    /// Returns the exact origin Parquet artifact.
    pub const fn origin_artifact_id(&self) -> Uuid {
        self.origin_artifact_id
    }

    /// Returns the exact object ordinal within the origin manifest.
    pub const fn origin_object_ordinal(&self) -> u16 {
        self.origin_object_ordinal
    }

    /// Returns the source authority namespace.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the sealed provider-capture lineage identity.
    pub const fn capture_receipt_digest(&self) -> Sha256Digest {
        self.capture_receipt_digest
    }

    /// Returns the outer complete request-graph content and observation identities.
    pub const fn capture_graph_digests(&self) -> (Sha256Digest, Sha256Digest) {
        (self.capture_content_digest, self.capture_observation_digest)
    }

    /// Returns when the exact sealed provider capture entered the authoritative catalog.
    pub const fn capture_recorded_at(&self) -> Timestamp {
        self.capture_recorded_at
    }

    /// Returns the complete provider request-graph dataset.
    pub const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }

    /// Returns the canonical instrument.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact canonical asset class bound by the admitted instrument revision.
    pub const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    /// Returns the exact canonical instrument revision selected by the admitted plan.
    pub const fn instrument_revision_digest(&self) -> Sha256Digest {
        self.instrument_revision_digest
    }

    /// Returns the exact admitted plan identity bound into the provider request graph.
    pub const fn admitted_plan_digest(&self) -> Sha256Digest {
        self.admitted_plan_digest
    }

    /// Returns the provider-native instrument.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the venue.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the provider feed.
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }

    /// Returns the provider bar interval.
    pub const fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }

    /// Returns the provider adjustment policy.
    pub const fn adjustment(&self) -> MarketBarAdjustment {
        self.adjustment
    }

    /// Returns the provider timestamp anchor.
    pub const fn timestamp_basis(&self) -> BarTimestampBasis {
        self.timestamp_basis
    }

    /// Returns the exact session class.
    pub const fn session_kind(&self) -> MarketBarSessionKind {
        self.session_kind
    }

    /// Returns the stable session-ruleset identity.
    pub const fn session_ruleset(&self) -> &SourceIdentifier {
        &self.session_ruleset
    }

    /// Returns the versioned request-graph purpose enforced by the history vertical.
    pub const fn graph_purpose(&self) -> &SourceIdentifier {
        &self.graph_purpose
    }

    /// Returns the single quote currency proven across every bar in the window.
    pub const fn currency(&self) -> Currency {
        self.currency
    }

    /// Returns the inclusive requested provider range.
    pub const fn requested_range(&self) -> (Timestamp, Timestamp) {
        (self.requested_start, self.requested_end)
    }

    /// Returns the exact returned provider-timestamp coverage.
    pub const fn coverage(&self) -> (Timestamp, Timestamp, Timestamp) {
        (
            self.coverage_first,
            self.coverage_last,
            self.coverage_last_complete,
        )
    }

    /// Returns the exact expected and returned bar count (proven equal at publication).
    pub const fn bar_count(&self) -> usize {
        self.expected_bar_count
    }

    /// Returns the exact ordered provider timestamps proven against the market calendar.
    pub fn expected_provider_timestamps(&self) -> &[Timestamp] {
        &self.expected_provider_timestamps
    }

    /// Returns the exact expected-provider-timestamp-set identity.
    pub const fn expected_timestamp_set_digest(&self) -> Sha256Digest {
        self.expected_timestamp_set_digest
    }

    /// Returns the exact normalized bar-set identity.
    pub const fn bar_set_digest(&self) -> Sha256Digest {
        self.bar_set_digest
    }

    /// Returns the plan-specific calendar/session completeness evidence digest.
    pub const fn completeness_evidence_digest(&self) -> Sha256Digest {
        self.completeness_evidence_digest
    }

    /// Returns the exact bar-component ordinal, content digest, and provider page count.
    pub const fn market_bar_component(&self) -> (u16, Sha256Digest, u16) {
        (
            self.market_bar_component_ordinal,
            self.market_bar_component_content_digest,
            self.market_bar_component_page_count,
        )
    }

    /// Returns the exact calendar-component ordinal, content digest, and provider page count.
    pub const fn session_calendar_component(&self) -> (u16, Sha256Digest, u16) {
        (
            self.session_calendar_component_ordinal,
            self.session_calendar_component_content_digest,
            self.session_calendar_component_page_count,
        )
    }

    /// Returns the greatest conservative availability, receive, and ingest clocks.
    pub const fn knowledge_clocks(&self) -> (Timestamp, Timestamp, Timestamp) {
        (
            self.max_available_at,
            self.max_received_at,
            self.max_ingested_at,
        )
    }

    /// Returns when the immutable origin generation was published locally.
    pub const fn published_at(&self) -> Timestamp {
        self.published_at
    }

    /// History first observed locally is never admitted as historical-as-known evidence.
    pub const fn point_in_time_eligible(&self) -> bool {
        false
    }

    /// Complete history is eligible for current research after every retained clock is known.
    pub const fn current_research_eligible(&self) -> bool {
        true
    }

    /// History first observed locally is never admitted to backtests.
    pub const fn backtest_eligible(&self) -> bool {
        false
    }

    /// Retrospective model training remains disabled without provider publication chronology.
    pub const fn retrospective_training_eligible(&self) -> bool {
        false
    }

    /// Returns the fixed fail-closed admission reason.
    pub const fn admission_reason(&self) -> &'static str {
        CURRENT_RESEARCH_REASON
    }

    pub(crate) fn validate_selected_bars(
        &self,
        bars: &[MarketBarObservation],
    ) -> Result<(), ManifestCatalogError> {
        if bars.len() != self.expected_bar_count
            || expected_timestamp_set_digest(&self.expected_provider_timestamps)?
                != self.expected_timestamp_set_digest
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        let mut bar_set = Sha256::new();
        bar_set.update(BAR_SET_DOMAIN);
        bar_set.update((bars.len() as u64).to_be_bytes());
        let mut max_available_at = None;
        let mut max_received_at = None;
        let mut max_ingested_at = None;
        for (bar, expected_timestamp) in bars.iter().zip(self.expected_provider_timestamps.iter()) {
            let context = bar.context();
            let provenance = context.provenance();
            let semantics = bar.time_semantics();
            let provider_timestamp = semantics.provider_timestamp();
            let available_at = match provenance.availability() {
                ResearchAvailabilityEvidence::LocalFirstObserved { observed_at } => *observed_at,
                ResearchAvailabilityEvidence::Evidenced { .. }
                | ResearchAvailabilityEvidence::Inferred { .. }
                | ResearchAvailabilityEvidence::Unknown => {
                    return Err(ManifestCatalogError::MarketBarHistoryMismatch);
                }
            };
            if provider_timestamp != *expected_timestamp
                || context.time().effective().exact_timestamp() != Some(provider_timestamp)
                || context.time().published().is_some()
                || context.time().superseded().is_some()
                || provenance.instrument_id() != Some(self.instrument_id)
                || provenance.source_id() != &self.source_id
                || provenance.venue_id() != Some(&self.venue_id)
                || provenance.source_timestamp() != Some(provider_timestamp)
                || provenance.quality() != DataQuality::Aggregated
                || bar.provider_instrument_id() != &self.provider_instrument_id
                || bar.feed() != &self.feed
                || bar.interval() != &self.interval
                || bar.adjustment() != self.adjustment
                || semantics.timestamp_basis() != self.timestamp_basis
                || semantics.session().kind() != self.session_kind
                || semantics.session().ruleset() != &self.session_ruleset
                || semantics.session().evidence().algorithm() != DigestAlgorithm::Sha256
                || semantics.session().evidence().bytes()
                    != self.completeness_evidence_digest.bytes()
                || bar.currency() != self.currency
                || provider_timestamp < self.requested_start
                || provider_timestamp > self.requested_end
                || semantics.period_end_exclusive() > self.requested_end
                || available_at != provenance.received_at()
                || provenance.received_at() > provenance.ingested_at()
            {
                return Err(ManifestCatalogError::MarketBarHistoryMismatch);
            }
            max_available_at = Some(
                max_available_at
                    .map_or(available_at, |current: Timestamp| current.max(available_at)),
            );
            max_received_at = Some(
                max_received_at.map_or(provenance.received_at(), |current: Timestamp| {
                    current.max(provenance.received_at())
                }),
            );
            max_ingested_at = Some(
                max_ingested_at.map_or(provenance.ingested_at(), |current: Timestamp| {
                    current.max(provenance.ingested_at())
                }),
            );
            let observation = ResearchObservation::MarketBar(bar.clone());
            let semantic_payload = CanonicalObservationPayload::try_from_observation(&observation)
                .map_err(|_| ManifestCatalogError::MarketBarHistoryMismatch)?;
            bar_set.update(provider_timestamp.unix_nanos().to_be_bytes());
            hash_evidence(&mut bar_set, semantic_payload.identity());
        }
        if max_available_at != Some(self.max_available_at)
            || max_received_at != Some(self.max_received_at)
            || max_ingested_at != Some(self.max_ingested_at)
            || nonzero_sha256(bar_set.finalize().into())? != self.bar_set_digest
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        Ok(())
    }
}

/// Restart-safe selection of one complete window under an immutable descendant generation.
#[derive(Debug)]
pub struct CompleteMarketBarHistorySelection {
    pinned: PinnedDataset,
    receipt: MarketBarHistoryPublicationReceipt,
    policy_digest: Sha256Digest,
    selection_digest: Sha256Digest,
}

impl CompleteMarketBarHistorySelection {
    /// Returns the exact selected generation. It may inherit the receipt through append/compaction.
    pub const fn pinned(&self) -> &PinnedDataset {
        &self.pinned
    }

    /// Returns the exact complete-window receipt linked into the selected generation.
    pub const fn receipt(&self) -> &MarketBarHistoryPublicationReceipt {
        &self.receipt
    }

    /// Returns the version of the code-owned Alpaca/IEX daily adjusted-series policy.
    pub const fn policy_version(&self) -> u16 {
        ALPACA_HISTORY_SELECTION_POLICY_VERSION
    }

    /// Returns the exact backend series-policy identity used for this selection.
    pub const fn policy_digest(&self) -> Sha256Digest {
        self.policy_digest
    }

    /// Returns the digest binding request coordinates, cutoff, descendant, and origin receipt.
    pub const fn selection_digest(&self) -> Sha256Digest {
        self.selection_digest
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct MarketBarHistoryReceiptWire {
    receipt_version: u16,
    origin_dataset_id: String,
    origin_manifest_version: u64,
    origin_schema_name: String,
    origin_schema_version: u16,
    origin_schema_fingerprint: [u8; 32],
    origin_manifest_content_hash: [u8; 32],
    origin_run_id: Uuid,
    origin_anchor_manifest_id: Uuid,
    origin_artifact_id: Uuid,
    origin_object_ordinal: u16,
    source_id: SourceId,
    binding_digest: [u8; 32],
    capture_receipt_digest: [u8; 32],
    capture_content_digest: [u8; 32],
    capture_observation_digest: [u8; 32],
    capture_recorded_at_ns: i64,
    provider_dataset: SourceIdentifier,
    instrument_id: InstrumentId,
    asset_class: AssetClass,
    instrument_revision_digest: [u8; 32],
    admitted_plan_digest: [u8; 32],
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_ruleset: SourceIdentifier,
    graph_purpose: SourceIdentifier,
    currency: Currency,
    requested_start_ns: i64,
    requested_end_ns: i64,
    coverage_first_ns: i64,
    coverage_last_ns: i64,
    coverage_last_complete_ns: i64,
    expected_bar_count: u32,
    returned_bar_count: u32,
    expected_timestamp_set_digest: [u8; 32],
    bar_set_digest: [u8; 32],
    completeness_evidence_digest: [u8; 32],
    market_bar_component_ordinal: u16,
    market_bar_component_content_digest: [u8; 32],
    market_bar_component_page_count: u16,
    session_calendar_component_ordinal: u16,
    session_calendar_component_content_digest: [u8; 32],
    session_calendar_component_page_count: u16,
    max_available_at_ns: i64,
    max_received_at_ns: i64,
    max_ingested_at_ns: i64,
    published_at_ns: i64,
    admission_class: String,
    current_research_eligible: bool,
    point_in_time_eligible: bool,
    backtest_eligible: bool,
    retrospective_training_eligible: bool,
    admission_reason: String,
}

fn expected_timestamp_set_digest(
    timestamps: &[Timestamp],
) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(EXPECTED_TIMESTAMP_SET_DOMAIN);
    hash.update((timestamps.len() as u64).to_be_bytes());
    for timestamp in timestamps {
        hash.update(timestamp.unix_nanos().to_be_bytes());
    }
    nonzero_sha256(hash.finalize().into())
}

fn sha256_evidence(evidence: EvidenceDigest) -> Result<Sha256Digest, ManifestCatalogError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    nonzero_sha256(evidence.bytes())
}

fn nonzero_sha256(bytes: [u8; 32]) -> Result<Sha256Digest, ManifestCatalogError> {
    if bytes == [0; 32] {
        Err(ManifestCatalogError::MarketBarHistoryMismatch)
    } else {
        Ok(Sha256Digest::new(bytes))
    }
}

fn require_canonical_history_schema(schema: &DatasetSchemaRef) -> Result<(), ManifestCatalogError> {
    let canonical = DatasetSchemaRegistry::local().canonical_research_observations()?;
    if schema == &canonical {
        Ok(())
    } else {
        Err(ManifestCatalogError::MarketBarHistoryMismatch)
    }
}

fn validate_exact_instrument_revision(
    connection: &Connection,
    revision_digest: Sha256Digest,
    instrument_id: InstrumentId,
    source_id: &SourceId,
    provider_instrument_id: &ProviderInstrumentId,
    currency: Currency,
    requested_start: Timestamp,
    requested_end: Timestamp,
    admitted_at: Timestamp,
) -> Result<AssetClass, ManifestCatalogError> {
    let stored: Option<(String, i64, i64, Option<i64>)> = connection
        .query_row(
            "SELECT definition_json, published_at_ns, effective_start_ns, effective_end_ns
             FROM market_data_instrument_revisions
             WHERE revision_digest=?1
               AND instrument_id=?2
               AND published_at_ns<=?3
               AND effective_start_ns<=?4
               AND (effective_end_ns IS NULL OR ?5<effective_end_ns)",
            params![
                revision_digest.bytes(),
                instrument_id.to_string(),
                admitted_at.unix_nanos(),
                requested_start.unix_nanos(),
                requested_end.unix_nanos(),
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((definition_json, published_at_ns, effective_start_ns, effective_end_ns)) = stored
    else {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    };
    let computed_digest: [u8; 32] = Sha256::digest(definition_json.as_bytes()).into();
    let definition: MarketDataInstrumentDefinition =
        serde_json::from_str(&definition_json).map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let asset_class = definition.asset_class();
    let provider_identity =
        definition.provider_identity_at(source_id, provider_instrument_id, requested_start);
    if computed_digest != revision_digest.bytes()
        || serde_json::to_string(&definition).map_err(|_| ManifestCatalogError::CorruptCatalog)?
            != definition_json
        || definition.instrument_id() != instrument_id
        || !matches!(asset_class, AssetClass::Equity | AssetClass::Fund)
        || definition.quote_currency() != currency
        || provider_identity.is_none_or(|identity| {
            identity.instrument_id() != instrument_id
                || identity.validity().starts_at() > requested_start
                || identity
                    .validity()
                    .ends_at()
                    .is_some_and(|end| requested_end >= end)
        })
        || definition.effective_interval().starts_at().unix_nanos() != effective_start_ns
        || definition
            .effective_interval()
            .ends_at()
            .map(|end| end.unix_nanos())
            != effective_end_ns
        || published_at_ns > admitted_at.unix_nanos()
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok(asset_class)
}

fn hash_evidence(hash: &mut Sha256, evidence: EvidenceDigest) {
    hash.update([match evidence.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(evidence.bytes());
}

fn hash_text(hash: &mut Sha256, value: &str) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value.as_bytes());
}

fn history_policy_digest() -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(ALPACA_HISTORY_POLICY_DOMAIN);
    hash.update(ALPACA_HISTORY_SELECTION_POLICY_VERSION.to_be_bytes());
    for value in [
        ALPACA_HISTORY_SOURCE,
        ALPACA_HISTORY_ASSET_CLASSES,
        ALPACA_HISTORY_VENUE,
        ALPACA_HISTORY_FEED,
        ALPACA_HISTORY_INTERVAL,
        ALPACA_HISTORY_ADJUSTMENT,
        ALPACA_HISTORY_TIMESTAMP_BASIS,
        ALPACA_HISTORY_SESSION_KIND,
        ALPACA_HISTORY_SESSION_RULESET,
        ALPACA_HISTORY_GRAPH_PURPOSE,
        ALPACA_HISTORY_CURRENCY,
    ] {
        hash_text(&mut hash, value);
    }
    nonzero_sha256(hash.finalize().into())
}

fn history_selection_digest(
    policy_digest: Sha256Digest,
    request: &CompleteMarketBarHistoryRequest,
    manifest: &DatasetManifestRef,
    publication_digest: Sha256Digest,
) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(ALPACA_HISTORY_SELECTION_DOMAIN);
    hash.update(policy_digest.bytes());
    hash_text(&mut hash, &request.instrument_id().to_string());
    let (requested_start, requested_end) = request.requested_range();
    hash.update(requested_start.unix_nanos().to_be_bytes());
    hash.update(requested_end.unix_nanos().to_be_bytes());
    hash_text(&mut hash, request.provider_instrument_id().as_str());
    hash_text(&mut hash, request.venue_id().as_str());
    hash_text(&mut hash, request.feed().as_str());
    hash_text(&mut hash, request.interval().as_str());
    hash_text(&mut hash, adjustment_name(request.adjustment()));
    hash_text(&mut hash, timestamp_basis_name(request.timestamp_basis()));
    hash_text(&mut hash, session_kind_name(request.session_kind()));
    hash_text(&mut hash, request.session_ruleset().as_str());
    hash.update(request.knowledge_cutoff().unix_nanos().to_be_bytes());
    hash_text(&mut hash, manifest.dataset_id().as_str());
    hash.update(manifest.manifest_version().to_be_bytes());
    hash_text(&mut hash, manifest.schema().name());
    hash.update(manifest.schema_version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
    hash.update(publication_digest.bytes());
    nonzero_sha256(hash.finalize().into())
}

fn latest_canonical_history_window_selection_digest(
    request: &LatestCanonicalMarketBarHistoryWindowRequest,
    candidate: LatestCanonicalMarketBarHistoryWindowCandidate,
    selection: &CompleteMarketBarHistorySelection,
) -> Result<Sha256Digest, ManifestCatalogError> {
    let manifest = selection.pinned().manifest();
    let receipt = selection.receipt();
    let mut hash = Sha256::new();
    hash.update(LATEST_CANONICAL_HISTORY_WINDOW_SELECTION_DOMAIN);
    hash.update(selection.policy_digest().bytes());
    hash_text(&mut hash, &request.instrument_id().to_string());
    hash.update(request.selection_policy().version().to_be_bytes());
    hash.update(request.knowledge_cutoff().unix_nanos().to_be_bytes());
    let (requested_start, requested_end) = candidate.requested_range();
    hash.update(requested_start.unix_nanos().to_be_bytes());
    hash.update(requested_end.unix_nanos().to_be_bytes());
    let (coverage_first, coverage_last, coverage_last_complete) = candidate.coverage();
    hash.update(coverage_first.unix_nanos().to_be_bytes());
    hash.update(coverage_last.unix_nanos().to_be_bytes());
    hash.update(coverage_last_complete.unix_nanos().to_be_bytes());
    hash.update(
        u64::try_from(candidate.expected_bar_count)
            .map_err(|_| ManifestCatalogError::CountOverflow)?
            .to_be_bytes(),
    );
    hash_text(&mut hash, manifest.dataset_id().as_str());
    hash.update(manifest.manifest_version().to_be_bytes());
    hash_text(&mut hash, manifest.schema().name());
    hash.update(manifest.schema_version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
    hash.update(receipt.receipt_digest().bytes());
    hash.update(selection.selection_digest().bytes());
    nonzero_sha256(hash.finalize().into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the transaction binds run, artifact, manifest, schema, and typed history evidence"
)]
pub(super) fn insert_generation_market_bar_history_inputs(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
    plan: &ManifestPlan,
    artifact: &ArtifactRecord,
    anchor: &DatasetManifestRecord,
    schema: &DatasetSchemaRef,
    source_input: Option<&IngestRunRecord>,
    candidate: Option<&MarketBarHistoryPublicationCandidate>,
) -> Result<(), ManifestCatalogError> {
    if generation_sequence <= 0 {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let kind: String = transaction.query_row(
        "SELECT generation_kind FROM analytical_generations WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let inherited_count: i64 = transaction.query_row(
        "SELECT COUNT(*)
         FROM analytical_generation_parents AS edge
         JOIN analytical_generations AS child
           ON child.dataset_id=edge.child_dataset_id
          AND child.manifest_version=edge.child_manifest_version
         JOIN analytical_generation_market_bar_history_inputs AS parent_input
           ON parent_input.generation_sequence=edge.parent_generation_sequence
         WHERE child.generation_sequence=?1
           AND child.generation_kind IN ('ingest', 'compaction', 'derived')",
        [generation_sequence],
        |row| row.get(0),
    )?;
    if inherited_count < 0
        || usize::try_from(inherited_count)
            .ok()
            .is_none_or(|count| count > MAX_GENERATION_MARKET_BAR_HISTORY_INPUTS)
        || (kind == "ingest" && inherited_count > 0 && candidate.is_none())
        || (candidate.is_some() && (kind != "ingest" || source_input.is_none()))
    {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    if let Some(candidate) = candidate {
        let source_input = source_input.ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
        if candidate.max_ingested_at > anchor.created_at()
            || candidate.source_id != *source_input.source_id()
        {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        let asset_class = validate_exact_instrument_revision(
            transaction,
            candidate.instrument_revision_digest,
            candidate.instrument_id,
            &candidate.source_id,
            &candidate.provider_instrument_id,
            candidate.currency,
            candidate.requested_start,
            candidate.requested_end,
            source_input.requested_at(),
        )?;
        validate_inherited_series(transaction, generation_sequence, candidate, asset_class)?;
        insert_market_bar_history_publication(
            transaction,
            generation_sequence,
            plan,
            artifact,
            anchor,
            schema,
            source_input,
            candidate,
            asset_class,
        )?;
    }

    propagate_generation_market_bar_history_inputs(transaction, generation_sequence)
}

pub(crate) fn propagate_generation_market_bar_history_inputs(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
) -> Result<(), ManifestCatalogError> {
    if generation_sequence <= 0 {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let kind: String = transaction.query_row(
        "SELECT generation_kind FROM analytical_generations WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    if !matches!(kind.as_str(), "ingest" | "compaction" | "derived") {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let inserted = transaction.execute(
        "INSERT INTO analytical_generation_market_bar_history_inputs
         (generation_sequence, input_ordinal, publication_receipt_digest)
         WITH candidates AS (
             SELECT parent_input.publication_receipt_digest
             FROM analytical_generation_parents AS edge
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             JOIN analytical_generation_market_bar_history_inputs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
               AND child.generation_kind IN ('ingest', 'compaction', 'derived')
             UNION
             SELECT publication.publication_receipt_digest
             FROM market_bar_history_publications AS publication
             WHERE publication.origin_generation_sequence=?1
         )
         SELECT ?1,
                ROW_NUMBER() OVER (ORDER BY publication_receipt_digest) - 1,
                publication_receipt_digest
         FROM candidates
         ORDER BY publication_receipt_digest
         LIMIT ?2",
        params![
            generation_sequence,
            i64::try_from(MAX_GENERATION_MARKET_BAR_HISTORY_INPUTS)
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
        ],
    )?;
    let expected: i64 = transaction.query_row(
        "WITH candidates AS (
             SELECT parent_input.publication_receipt_digest
             FROM analytical_generation_parents AS edge
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             JOIN analytical_generation_market_bar_history_inputs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
               AND child.generation_kind IN ('ingest', 'compaction', 'derived')
             UNION
             SELECT publication.publication_receipt_digest
             FROM market_bar_history_publications AS publication
             WHERE publication.origin_generation_sequence=?1
         ) SELECT COUNT(*) FROM candidates",
        [generation_sequence],
        |row| row.get(0),
    )?;
    if expected < 0
        || usize::try_from(expected).ok().is_none_or(|count| {
            count > MAX_GENERATION_MARKET_BAR_HISTORY_INPUTS || count != inserted
        })
    {
        return Err(ManifestCatalogError::MarketBarHistoryInputLimitExceeded {
            max: MAX_GENERATION_MARKET_BAR_HISTORY_INPUTS,
        });
    }
    Ok(())
}

fn validate_inherited_series(
    transaction: &Connection,
    generation_sequence: i64,
    candidate: &MarketBarHistoryPublicationCandidate,
    asset_class: AssetClass,
) -> Result<(), ManifestCatalogError> {
    let mismatches: i64 = transaction.query_row(
        "SELECT COUNT(*)
         FROM analytical_generation_parents AS edge
         JOIN analytical_generations AS child
           ON child.dataset_id=edge.child_dataset_id
          AND child.manifest_version=edge.child_manifest_version
         JOIN analytical_generation_market_bar_history_inputs AS parent_input
           ON parent_input.generation_sequence=edge.parent_generation_sequence
         JOIN market_bar_history_publications AS publication
           USING (publication_receipt_digest)
         WHERE child.generation_sequence=?1
           AND child.generation_kind IN ('ingest', 'compaction')
           AND (
               publication.source_id<>?2
               OR publication.instrument_id<>?3
               OR publication.asset_class<>?14
               OR publication.provider_instrument_id<>?4
               OR publication.venue_id<>?5
               OR publication.feed<>?6
               OR publication.bar_interval<>?7
               OR publication.adjustment<>?8
               OR publication.timestamp_basis<>?9
               OR publication.session_kind<>?10
               OR publication.session_ruleset<>?11
               OR publication.graph_purpose<>?12
               OR publication.currency<>?13
           )",
        params![
            generation_sequence,
            candidate.source_id.as_str(),
            candidate.instrument_id.to_string(),
            candidate.provider_instrument_id.as_str(),
            candidate.venue_id.as_str(),
            candidate.feed.as_str(),
            candidate.interval.as_str(),
            adjustment_name(candidate.adjustment),
            timestamp_basis_name(candidate.timestamp_basis),
            session_kind_name(candidate.session_kind),
            candidate.session_ruleset.as_str(),
            candidate.graph_purpose.as_str(),
            candidate.currency.as_str(),
            asset_class_name(asset_class),
        ],
        |row| row.get(0),
    )?;
    if mismatches == 0 {
        Ok(())
    } else {
        Err(ManifestCatalogError::MarketBarHistoryMismatch)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the immutable receipt binds every origin authority explicitly"
)]
fn insert_market_bar_history_publication(
    transaction: &Transaction<'_>,
    generation_sequence: i64,
    plan: &ManifestPlan,
    artifact: &ArtifactRecord,
    anchor: &DatasetManifestRecord,
    schema: &DatasetSchemaRef,
    source_input: &IngestRunRecord,
    candidate: &MarketBarHistoryPublicationCandidate,
    asset_class: AssetClass,
) -> Result<(), ManifestCatalogError> {
    require_canonical_history_schema(schema)?;
    let origin_object_ordinal = plan
        .objects()
        .len()
        .checked_sub(1)
        .and_then(|ordinal| u16::try_from(ordinal).ok())
        .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
    let origin_manifest_version: i64 = transaction.query_row(
        "SELECT manifest_version FROM analytical_generations WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let origin_manifest_version = u64::try_from(origin_manifest_version)
        .ok()
        .filter(|version| *version > 0)
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    let capture_recorded_at_ns: i64 = transaction
        .query_row(
            "SELECT capture.recorded_at_ns
             FROM provider_capture_bindings AS binding
             JOIN provider_raw_observations AS capture
               ON capture.capture_observation_digest=binding.capture_observation_digest
             WHERE binding.binding_digest=?1
               AND capture.capture_content_digest=?2
               AND capture.capture_observation_digest=?3
               AND capture.source_id=?4
               AND capture.provider_dataset=?5
               AND capture.terminal_disposition='complete_request_graph'",
            params![
                candidate.binding_digest.bytes(),
                candidate.capture_content_digest.bytes(),
                candidate.capture_observation_digest.bytes(),
                candidate.source_id.as_str(),
                candidate.provider_dataset.as_str(),
            ],
            |row| row.get(0),
        )
        .optional()?
        .ok_or(ManifestCatalogError::MarketBarHistoryMismatch)?;
    if capture_recorded_at_ns > anchor.created_at().unix_nanos() {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let expected_bar_count = u32::try_from(candidate.expected_bar_count)
        .map_err(|_| ManifestCatalogError::MarketBarHistoryMismatch)?;
    let wire = MarketBarHistoryReceiptWire {
        receipt_version: MARKET_BAR_HISTORY_RECEIPT_VERSION,
        origin_dataset_id: plan.dataset_id().as_str().to_owned(),
        origin_manifest_version,
        origin_schema_name: schema.name().to_owned(),
        origin_schema_version: schema.version().get(),
        origin_schema_fingerprint: schema.fingerprint(),
        origin_manifest_content_hash: plan.content_hash().bytes(),
        origin_run_id: source_input.run_id(),
        origin_anchor_manifest_id: anchor.manifest_id(),
        origin_artifact_id: artifact.artifact_id(),
        origin_object_ordinal,
        source_id: candidate.source_id.clone(),
        binding_digest: candidate.binding_digest.bytes(),
        capture_receipt_digest: candidate.capture_receipt_digest.bytes(),
        capture_content_digest: candidate.capture_content_digest.bytes(),
        capture_observation_digest: candidate.capture_observation_digest.bytes(),
        capture_recorded_at_ns,
        provider_dataset: candidate.provider_dataset.clone(),
        instrument_id: candidate.instrument_id,
        asset_class,
        instrument_revision_digest: candidate.instrument_revision_digest.bytes(),
        admitted_plan_digest: candidate.admitted_plan_digest.bytes(),
        provider_instrument_id: candidate.provider_instrument_id.clone(),
        venue_id: candidate.venue_id.clone(),
        feed: candidate.feed.clone(),
        interval: candidate.interval.clone(),
        adjustment: candidate.adjustment,
        timestamp_basis: candidate.timestamp_basis,
        session_kind: candidate.session_kind,
        session_ruleset: candidate.session_ruleset.clone(),
        graph_purpose: candidate.graph_purpose.clone(),
        currency: candidate.currency,
        requested_start_ns: candidate.requested_start.unix_nanos(),
        requested_end_ns: candidate.requested_end.unix_nanos(),
        coverage_first_ns: candidate.coverage_first.unix_nanos(),
        coverage_last_ns: candidate.coverage_last.unix_nanos(),
        coverage_last_complete_ns: candidate.coverage_last_complete.unix_nanos(),
        expected_bar_count,
        returned_bar_count: expected_bar_count,
        expected_timestamp_set_digest: candidate.expected_timestamp_set_digest.bytes(),
        bar_set_digest: candidate.bar_set_digest.bytes(),
        completeness_evidence_digest: candidate.completeness_evidence_digest.bytes(),
        market_bar_component_ordinal: candidate.market_bar_component_ordinal,
        market_bar_component_content_digest: candidate.market_bar_component_content_digest.bytes(),
        market_bar_component_page_count: candidate.market_bar_component_page_count,
        session_calendar_component_ordinal: candidate.session_calendar_component_ordinal,
        session_calendar_component_content_digest: candidate
            .session_calendar_component_content_digest
            .bytes(),
        session_calendar_component_page_count: candidate.session_calendar_component_page_count,
        max_available_at_ns: candidate.max_available_at.unix_nanos(),
        max_received_at_ns: candidate.max_received_at.unix_nanos(),
        max_ingested_at_ns: candidate.max_ingested_at.unix_nanos(),
        published_at_ns: anchor.created_at().unix_nanos(),
        admission_class: CURRENT_RESEARCH_ADMISSION.to_owned(),
        current_research_eligible: true,
        point_in_time_eligible: false,
        backtest_eligible: false,
        retrospective_training_eligible: false,
        admission_reason: CURRENT_RESEARCH_REASON.to_owned(),
    };
    let receipt_json =
        serde_json::to_string(&wire).map_err(|_| ManifestCatalogError::MarketBarHistoryMismatch)?;
    let publication_receipt_digest = receipt_digest(receipt_json.as_bytes())?;
    transaction.execute(
        "INSERT INTO market_bar_history_publications
         (publication_receipt_digest, receipt_version, origin_generation_sequence,
          origin_run_id, origin_anchor_manifest_id, origin_artifact_id, origin_object_ordinal,
          source_id, binding_digest, capture_receipt_digest, capture_content_digest,
          capture_observation_digest, capture_recorded_at_ns, provider_dataset, instrument_id,
          instrument_revision_digest, admitted_plan_digest, provider_instrument_id, venue_id,
          feed, bar_interval, adjustment, timestamp_basis, session_kind, session_ruleset,
          graph_purpose, currency, requested_start_ns, requested_end_ns, coverage_first_ns, coverage_last_ns,
          coverage_last_complete_ns, expected_bar_count, returned_bar_count,
          expected_timestamp_set_digest, bar_set_digest, completeness_evidence_digest,
          market_bar_component_ordinal, market_bar_component_content_digest,
          market_bar_component_page_count, session_calendar_component_ordinal,
          session_calendar_component_content_digest, session_calendar_component_page_count,
          max_available_at_ns, max_received_at_ns, max_ingested_at_ns, published_at_ns,
          admission_class, current_research_eligible, point_in_time_eligible,
          backtest_eligible, retrospective_training_eligible, admission_reason, receipt_json,
          asset_class)
         VALUES (
          ?1, 1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15,
          ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29,
          ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40, ?41, ?42, ?43,
          ?44, ?45, ?46, 'current_research_only', 1, 0, 0, 0, ?47, ?48, ?49)",
        params![
            publication_receipt_digest.bytes(),
            generation_sequence,
            source_input.run_id().to_string(),
            anchor.manifest_id().to_string(),
            artifact.artifact_id().to_string(),
            i64::from(origin_object_ordinal),
            candidate.source_id.as_str(),
            candidate.binding_digest.bytes(),
            candidate.capture_receipt_digest.bytes(),
            candidate.capture_content_digest.bytes(),
            candidate.capture_observation_digest.bytes(),
            capture_recorded_at_ns,
            candidate.provider_dataset.as_str(),
            candidate.instrument_id.to_string(),
            candidate.instrument_revision_digest.bytes(),
            candidate.admitted_plan_digest.bytes(),
            candidate.provider_instrument_id.as_str(),
            candidate.venue_id.as_str(),
            candidate.feed.as_str(),
            candidate.interval.as_str(),
            adjustment_name(candidate.adjustment),
            timestamp_basis_name(candidate.timestamp_basis),
            session_kind_name(candidate.session_kind),
            candidate.session_ruleset.as_str(),
            candidate.graph_purpose.as_str(),
            candidate.currency.as_str(),
            candidate.requested_start.unix_nanos(),
            candidate.requested_end.unix_nanos(),
            candidate.coverage_first.unix_nanos(),
            candidate.coverage_last.unix_nanos(),
            candidate.coverage_last_complete.unix_nanos(),
            i64::from(expected_bar_count),
            i64::from(expected_bar_count),
            candidate.expected_timestamp_set_digest.bytes(),
            candidate.bar_set_digest.bytes(),
            candidate.completeness_evidence_digest.bytes(),
            i64::from(candidate.market_bar_component_ordinal),
            candidate.market_bar_component_content_digest.bytes(),
            i64::from(candidate.market_bar_component_page_count),
            i64::from(candidate.session_calendar_component_ordinal),
            candidate.session_calendar_component_content_digest.bytes(),
            i64::from(candidate.session_calendar_component_page_count),
            candidate.max_available_at.unix_nanos(),
            candidate.max_received_at.unix_nanos(),
            candidate.max_ingested_at.unix_nanos(),
            anchor.created_at().unix_nanos(),
            CURRENT_RESEARCH_REASON,
            receipt_json,
            asset_class_name(asset_class),
        ],
    )?;
    Ok(())
}

fn receipt_digest(receipt_json: &[u8]) -> Result<Sha256Digest, ManifestCatalogError> {
    let mut hash = Sha256::new();
    hash.update(RECEIPT_DIGEST_DOMAIN);
    hash.update((receipt_json.len() as u64).to_be_bytes());
    hash.update(receipt_json);
    nonzero_sha256(hash.finalize().into())
}

fn adjustment_name(value: MarketBarAdjustment) -> &'static str {
    match value {
        MarketBarAdjustment::Raw => "raw",
        MarketBarAdjustment::Split => "split",
        MarketBarAdjustment::Dividend => "dividend",
        MarketBarAdjustment::SpinOff => "spin_off",
        MarketBarAdjustment::All => "all",
    }
}

fn asset_class_name(value: AssetClass) -> &'static str {
    match value {
        AssetClass::Equity => "equity",
        AssetClass::Fund => "fund",
        AssetClass::FixedIncome
        | AssetClass::Option
        | AssetClass::Future
        | AssetClass::ForeignExchange
        | AssetClass::Crypto
        | AssetClass::Commodity
        | AssetClass::Index
        | AssetClass::Cash => "unsupported",
    }
}

fn timestamp_basis_name(value: BarTimestampBasis) -> &'static str {
    match value {
        BarTimestampBasis::PeriodStart => "period_start",
        BarTimestampBasis::PeriodEnd => "period_end",
    }
}

fn session_kind_name(value: MarketBarSessionKind) -> &'static str {
    match value {
        MarketBarSessionKind::Regular => "regular",
        MarketBarSessionKind::Extended => "extended",
        MarketBarSessionKind::Continuous => "continuous",
        MarketBarSessionKind::ProviderDefined => "provider_defined",
    }
}

pub(super) fn generation_market_bar_history_inputs_match_manifest(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<bool, ManifestCatalogError> {
    let (generation_sequence, kind): (i64, String) = connection.query_row(
        "SELECT generation_sequence, generation_kind
         FROM analytical_generations
         WHERE dataset_id=?1 AND manifest_version=?2",
        params![
            manifest.dataset_id().as_str(),
            i64::try_from(manifest.manifest_version())
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
        ],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let expected: i64 = connection.query_row(
        "WITH candidates AS (
             SELECT parent_input.publication_receipt_digest
             FROM analytical_generation_parents AS edge
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             JOIN analytical_generation_market_bar_history_inputs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
               AND child.generation_kind IN ('ingest', 'compaction', 'derived')
             UNION
             SELECT publication.publication_receipt_digest
             FROM market_bar_history_publications AS publication
             WHERE publication.origin_generation_sequence=?1
         ) SELECT COUNT(*) FROM candidates",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let actual: i64 = connection.query_row(
        "SELECT COUNT(*)
         FROM analytical_generation_market_bar_history_inputs
         WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let ordinal_shape: bool = connection.query_row(
        "SELECT COUNT(*)=0 OR (MIN(input_ordinal)=0 AND MAX(input_ordinal)=COUNT(*)-1)
         FROM analytical_generation_market_bar_history_inputs
         WHERE generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let canonical_ordinal_order: bool = connection.query_row(
        "SELECT NOT EXISTS(
             SELECT 1 FROM (
                 SELECT input_ordinal,
                        ROW_NUMBER() OVER (ORDER BY publication_receipt_digest) - 1
                            AS expected_ordinal
                 FROM analytical_generation_market_bar_history_inputs
                 WHERE generation_sequence=?1
             )
             WHERE input_ordinal<>expected_ordinal
         )",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let exact: bool = connection.query_row(
        "WITH candidates AS (
             SELECT parent_input.publication_receipt_digest
             FROM analytical_generation_parents AS edge
             JOIN analytical_generations AS child
               ON child.dataset_id=edge.child_dataset_id
              AND child.manifest_version=edge.child_manifest_version
             JOIN analytical_generation_market_bar_history_inputs AS parent_input
               ON parent_input.generation_sequence=edge.parent_generation_sequence
             WHERE child.generation_sequence=?1
               AND child.generation_kind IN ('ingest', 'compaction', 'derived')
             UNION
             SELECT publication.publication_receipt_digest
             FROM market_bar_history_publications AS publication
             WHERE publication.origin_generation_sequence=?1
         )
         SELECT NOT EXISTS(
             SELECT publication_receipt_digest FROM candidates
             EXCEPT
             SELECT publication_receipt_digest
             FROM analytical_generation_market_bar_history_inputs
             WHERE generation_sequence=?1
         ) AND NOT EXISTS(
             SELECT publication_receipt_digest
             FROM analytical_generation_market_bar_history_inputs
             WHERE generation_sequence=?1
             EXCEPT
             SELECT publication_receipt_digest FROM candidates
         )",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let count = usize::try_from(actual).ok();
    Ok(matches!(kind.as_str(), "ingest" | "compaction" | "derived")
        && expected == actual
        && count.is_some_and(|count| count <= MAX_GENERATION_MARKET_BAR_HISTORY_INPUTS)
        && ordinal_shape
        && canonical_ordinal_order
        && exact)
}

pub(super) fn generation_market_bar_history_candidate_matches(
    connection: &Connection,
    manifest: &DatasetManifestRef,
    candidate: Option<&MarketBarHistoryPublicationCandidate>,
) -> Result<bool, ManifestCatalogError> {
    if !generation_market_bar_history_inputs_match_manifest(connection, manifest)? {
        return Ok(false);
    }
    let generation_sequence: i64 = connection.query_row(
        "SELECT generation_sequence FROM analytical_generations
         WHERE dataset_id=?1 AND manifest_version=?2",
        params![
            manifest.dataset_id().as_str(),
            i64::try_from(manifest.manifest_version())
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
        ],
        |row| row.get(0),
    )?;
    let origin_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM market_bar_history_publications
         WHERE origin_generation_sequence=?1",
        [generation_sequence],
        |row| row.get(0),
    )?;
    let Some(candidate) = candidate else {
        return Ok(origin_count == 0);
    };
    if origin_count != 1 {
        return Ok(false);
    }
    let matches: bool = connection.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM market_bar_history_publications
             WHERE origin_generation_sequence=?1
               AND source_id=?2
               AND capture_receipt_digest=?3
               AND capture_content_digest=?4
               AND capture_observation_digest=?5
               AND provider_dataset=?6
               AND instrument_id=?7
               AND instrument_revision_digest=?8
               AND admitted_plan_digest=?9
               AND provider_instrument_id=?10
               AND venue_id=?11
               AND feed=?12
               AND bar_interval=?13
               AND adjustment=?14
               AND timestamp_basis=?15
               AND session_kind=?16
               AND session_ruleset=?17
               AND graph_purpose=?18
               AND currency=?19
               AND requested_start_ns=?20
               AND requested_end_ns=?21
               AND coverage_first_ns=?22
               AND coverage_last_ns=?23
               AND coverage_last_complete_ns=?24
               AND expected_bar_count=?25
               AND returned_bar_count=?25
               AND expected_timestamp_set_digest=?26
               AND bar_set_digest=?27
               AND completeness_evidence_digest=?28
               AND market_bar_component_ordinal=?29
               AND market_bar_component_content_digest=?30
               AND market_bar_component_page_count=?31
               AND session_calendar_component_ordinal=?32
               AND session_calendar_component_content_digest=?33
               AND session_calendar_component_page_count=?34
               AND max_available_at_ns=?35
               AND max_received_at_ns=?36
               AND max_ingested_at_ns=?37
               AND admission_class='current_research_only'
               AND current_research_eligible=1
               AND point_in_time_eligible=0
               AND backtest_eligible=0
               AND retrospective_training_eligible=0
               AND admission_reason='local_first_observed_without_provider_publication_time'
         )",
        params![
            generation_sequence,
            candidate.source_id.as_str(),
            candidate.capture_receipt_digest.bytes(),
            candidate.capture_content_digest.bytes(),
            candidate.capture_observation_digest.bytes(),
            candidate.provider_dataset.as_str(),
            candidate.instrument_id.to_string(),
            candidate.instrument_revision_digest.bytes(),
            candidate.admitted_plan_digest.bytes(),
            candidate.provider_instrument_id.as_str(),
            candidate.venue_id.as_str(),
            candidate.feed.as_str(),
            candidate.interval.as_str(),
            adjustment_name(candidate.adjustment),
            timestamp_basis_name(candidate.timestamp_basis),
            session_kind_name(candidate.session_kind),
            candidate.session_ruleset.as_str(),
            candidate.graph_purpose.as_str(),
            candidate.currency.as_str(),
            candidate.requested_start.unix_nanos(),
            candidate.requested_end.unix_nanos(),
            candidate.coverage_first.unix_nanos(),
            candidate.coverage_last.unix_nanos(),
            candidate.coverage_last_complete.unix_nanos(),
            i64::try_from(candidate.expected_bar_count)
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
            candidate.expected_timestamp_set_digest.bytes(),
            candidate.bar_set_digest.bytes(),
            candidate.completeness_evidence_digest.bytes(),
            i64::from(candidate.market_bar_component_ordinal),
            candidate.market_bar_component_content_digest.bytes(),
            i64::from(candidate.market_bar_component_page_count),
            i64::from(candidate.session_calendar_component_ordinal),
            candidate.session_calendar_component_content_digest.bytes(),
            i64::from(candidate.session_calendar_component_page_count),
            candidate.max_available_at.unix_nanos(),
            candidate.max_received_at.unix_nanos(),
            candidate.max_ingested_at.unix_nanos(),
        ],
        |row| row.get(0),
    )?;
    Ok(matches)
}

struct StoredCanonicalMarketBarHistorySeries {
    source_id: String,
    asset_class: String,
    provider_instrument_id: String,
    venue_id: String,
    feed: String,
    interval: String,
    adjustment: String,
    timestamp_basis: String,
    session_kind: String,
    session_ruleset: String,
    graph_purpose: String,
    currency: String,
}

fn resolve_canonical_market_bar_history_series(
    connection: &Connection,
    request: &CanonicalMarketBarHistoryRequest,
    canonical_schema: &DatasetSchemaRef,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<CompleteMarketBarHistoryRequest>, ManifestCatalogError> {
    let zero_digest = [0_u8; 32];
    let exact = request.exact_manifest();
    let exact_enabled = i64::from(exact.is_some());
    let exact_dataset = exact.map_or("", |manifest| manifest.dataset_id().as_str());
    let exact_version = exact
        .map(|manifest| i64::try_from(manifest.manifest_version()))
        .transpose()
        .map_err(|_| ManifestCatalogError::CountOverflow)?
        .unwrap_or(0);
    let exact_schema_name = exact.map_or("", |manifest| manifest.schema().name());
    let exact_schema_version =
        exact.map_or(0, |manifest| i64::from(manifest.schema_version().get()));
    let exact_fingerprint = exact.map_or(zero_digest, |manifest| manifest.schema().fingerprint());
    let exact_content = exact.map_or(zero_digest, |manifest| manifest.content_hash().bytes());
    let mut statement = connection.prepare(
        "SELECT DISTINCT
                publication.source_id,
                publication.asset_class,
                publication.provider_instrument_id,
                publication.venue_id,
                publication.feed,
                publication.bar_interval,
                publication.adjustment,
                publication.timestamp_basis,
                publication.session_kind,
                publication.session_ruleset,
                publication.graph_purpose,
                publication.currency
         FROM analytical_generations AS selected_generation
         JOIN dataset_manifests AS selected_manifest
           ON selected_manifest.manifest_id=selected_generation.anchor_manifest_id
         JOIN artifacts AS selected_artifact
           ON selected_artifact.artifact_id=selected_manifest.artifact_id
         JOIN ingest_runs AS selected_run
           ON selected_run.run_id=selected_artifact.run_id
         JOIN analytical_generation_market_bar_history_inputs AS history_input
           ON history_input.generation_sequence=selected_generation.generation_sequence
         JOIN market_bar_history_publications AS publication
           USING (publication_receipt_digest)
         JOIN ingest_runs AS origin_run
           ON origin_run.run_id=publication.origin_run_id
         JOIN provider_capture_bindings AS binding
           ON binding.binding_digest=publication.binding_digest
         JOIN provider_raw_observations AS capture
           ON capture.capture_observation_digest=binding.capture_observation_digest
         JOIN analytical_generation_provider_capture_bindings AS selected_capture
           ON selected_capture.generation_sequence=selected_generation.generation_sequence
          AND selected_capture.binding_digest=publication.binding_digest
         WHERE publication.instrument_id=?1
           AND publication.requested_start_ns=?13
           AND publication.requested_end_ns=?14
           AND selected_generation.schema_name=?10
           AND selected_generation.schema_version=?11
           AND selected_generation.schema_fingerprint=?12
           AND (?3=0 OR (
                selected_generation.dataset_id=?4
                AND selected_generation.manifest_version=?5
                AND selected_generation.schema_name=?6
                AND selected_generation.schema_version=?7
                AND selected_generation.schema_fingerprint=?8
                AND selected_generation.content_hash=?9
           ))
           AND publication.source_id='alpaca-basic-iex-market-data'
           AND publication.venue_id='iex'
           AND publication.feed='iex'
           AND publication.bar_interval='1Day'
           AND publication.adjustment='all'
           AND publication.timestamp_basis='period_start'
           AND publication.session_kind='provider_defined'
           AND publication.session_ruleset='alpaca-v3-iex-utc-range-returned-dates-v2'
           AND publication.graph_purpose='alpaca-iex-historical-bars-and-calendar/v1'
           AND publication.asset_class IN ('equity', 'fund')
           AND publication.currency='USD'
           AND selected_generation.created_at_ns<=?2
           AND selected_manifest.created_at_ns<=?2
           AND selected_artifact.created_at_ns<=?2
           AND selected_run.state='succeeded'
           AND selected_run.operation='persist'
           AND selected_run.source_id=publication.source_id
           AND selected_run.requested_at_ns<=?2
           AND selected_run.completed_at_ns<=?2
           AND origin_run.state='succeeded'
           AND origin_run.operation='persist'
           AND origin_run.source_id=publication.source_id
           AND origin_run.requested_at_ns<=?2
           AND origin_run.completed_at_ns<=?2
           AND capture.recorded_at_ns<=?2
           AND publication.capture_recorded_at_ns<=?2
           AND publication.max_available_at_ns<=?2
           AND publication.max_received_at_ns<=?2
           AND publication.max_ingested_at_ns<=?2
           AND publication.published_at_ns<=?2
           AND publication.admission_class='current_research_only'
           AND publication.current_research_eligible=1
           AND publication.point_in_time_eligible=0
           AND publication.backtest_eligible=0
           AND publication.retrospective_training_eligible=0
         ORDER BY publication.source_id,
                  publication.asset_class,
                  publication.provider_instrument_id,
                  publication.venue_id,
                  publication.feed,
                  publication.bar_interval,
                  publication.adjustment,
                  publication.timestamp_basis,
                  publication.session_kind,
                  publication.session_ruleset,
                  publication.graph_purpose,
                  publication.currency
         LIMIT 2",
    )?;
    let rows = statement.query_map(
        params![
            request.instrument_id().to_string(),
            request.knowledge_cutoff().unix_nanos(),
            exact_enabled,
            exact_dataset,
            exact_version,
            exact_schema_name,
            exact_schema_version,
            exact_fingerprint.as_slice(),
            exact_content.as_slice(),
            canonical_schema.name(),
            i64::from(canonical_schema.version().get()),
            canonical_schema.fingerprint().as_slice(),
            request.requested_range().0.unix_nanos(),
            request.requested_range().1.unix_nanos(),
        ],
        |row| {
            Ok(StoredCanonicalMarketBarHistorySeries {
                source_id: row.get(0)?,
                asset_class: row.get(1)?,
                provider_instrument_id: row.get(2)?,
                venue_id: row.get(3)?,
                feed: row.get(4)?,
                interval: row.get(5)?,
                adjustment: row.get(6)?,
                timestamp_basis: row.get(7)?,
                session_kind: row.get(8)?,
                session_ruleset: row.get(9)?,
                graph_purpose: row.get(10)?,
                currency: row.get(11)?,
            })
        },
    )?;
    let mut series = Vec::new();
    series
        .try_reserve_exact(2)
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    for row in rows {
        check_operation(deadline, cancellation)?;
        series.push(row?);
    }
    drop(statement);
    if series.len() > 1 {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let Some(series) = series.pop() else {
        return Ok(None);
    };
    if series.source_id != ALPACA_HISTORY_SOURCE
        || !matches!(series.asset_class.as_str(), "equity" | "fund")
        || series.venue_id != ALPACA_HISTORY_VENUE
        || series.feed != ALPACA_HISTORY_FEED
        || series.interval != ALPACA_HISTORY_INTERVAL
        || series.adjustment != ALPACA_HISTORY_ADJUSTMENT
        || series.timestamp_basis != ALPACA_HISTORY_TIMESTAMP_BASIS
        || series.session_kind != ALPACA_HISTORY_SESSION_KIND
        || series.session_ruleset != ALPACA_HISTORY_SESSION_RULESET
        || series.graph_purpose != ALPACA_HISTORY_GRAPH_PURPOSE
        || series.currency != ALPACA_HISTORY_CURRENCY
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let provider_instrument_id =
        ProviderInstrumentId::try_from(series.provider_instrument_id.as_str())
            .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let venue_id = VenueId::try_from(series.venue_id.as_str())
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let feed = SourceIdentifier::try_from(series.feed.as_str())
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let interval = SourceIdentifier::try_from(series.interval.as_str())
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let session_ruleset = SourceIdentifier::try_from(series.session_ruleset.as_str())
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let (requested_start, requested_end) = request.requested_range();
    let resolved = match request.exact_manifest() {
        Some(manifest) => CompleteMarketBarHistoryRequest::try_exact(
            request.instrument_id(),
            requested_start,
            requested_end,
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            MarketBarAdjustment::All,
            BarTimestampBasis::PeriodStart,
            MarketBarSessionKind::ProviderDefined,
            session_ruleset,
            request.knowledge_cutoff(),
            manifest.clone(),
        ),
        None => CompleteMarketBarHistoryRequest::try_latest(
            request.instrument_id(),
            requested_start,
            requested_end,
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            MarketBarAdjustment::All,
            BarTimestampBasis::PeriodStart,
            MarketBarSessionKind::ProviderDefined,
            session_ruleset,
            request.knowledge_cutoff(),
        ),
    }?;
    Ok(Some(resolved))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LatestCanonicalMarketBarHistoryWindowCandidate {
    requested_start: Timestamp,
    requested_end: Timestamp,
    coverage_first: Timestamp,
    coverage_last: Timestamp,
    coverage_last_complete: Timestamp,
    expected_bar_count: usize,
}

impl LatestCanonicalMarketBarHistoryWindowCandidate {
    fn try_new(
        requested_start_ns: i64,
        requested_end_ns: i64,
        coverage_first_ns: i64,
        coverage_last_ns: i64,
        coverage_last_complete_ns: i64,
        expected_bar_count: i64,
        knowledge_cutoff: Timestamp,
    ) -> Result<Self, ManifestCatalogError> {
        let requested_start = Timestamp::from_unix_nanos(requested_start_ns);
        let requested_end = Timestamp::from_unix_nanos(requested_end_ns);
        let coverage_first = Timestamp::from_unix_nanos(coverage_first_ns);
        let coverage_last = Timestamp::from_unix_nanos(coverage_last_ns);
        let coverage_last_complete = Timestamp::from_unix_nanos(coverage_last_complete_ns);
        let expected_bar_count = usize::try_from(expected_bar_count)
            .ok()
            .filter(|count| *count > 0 && *count <= MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS)
            .ok_or(ManifestCatalogError::CorruptCatalog)?;
        if requested_start >= requested_end
            || requested_end > knowledge_cutoff
            || coverage_first < requested_start
            || coverage_last < coverage_first
            || coverage_last > requested_end
            || coverage_last_complete < coverage_last
            || coverage_last_complete > requested_end
            || coverage_last_complete > knowledge_cutoff
        {
            return Err(ManifestCatalogError::CorruptCatalog);
        }
        Ok(Self {
            requested_start,
            requested_end,
            coverage_first,
            coverage_last,
            coverage_last_complete,
            expected_bar_count,
        })
    }

    const fn requested_range(self) -> (Timestamp, Timestamp) {
        (self.requested_start, self.requested_end)
    }

    const fn coverage(self) -> (Timestamp, Timestamp, Timestamp) {
        (
            self.coverage_first,
            self.coverage_last,
            self.coverage_last_complete,
        )
    }

    const fn has_same_selection_rank(self, other: Self) -> bool {
        self.coverage_last_complete.unix_nanos() == other.coverage_last_complete.unix_nanos()
            && self.requested_end.unix_nanos() == other.requested_end.unix_nanos()
            && self.expected_bar_count == other.expected_bar_count
            && self.requested_start.unix_nanos() == other.requested_start.unix_nanos()
    }
}

pub(super) fn select_latest_canonical_market_bar_history_window(
    connection: &Connection,
    max_objects: usize,
    request: &LatestCanonicalMarketBarHistoryWindowRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<LatestCanonicalMarketBarHistoryWindowSelection>, ManifestCatalogError> {
    check_operation(deadline, cancellation)?;
    if !request.selection_policy().is_supported() {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let canonical_schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
    let mut statement = connection.prepare(
        "SELECT DISTINCT
                publication.requested_start_ns,
                publication.requested_end_ns,
                publication.coverage_first_ns,
                publication.coverage_last_ns,
                publication.coverage_last_complete_ns,
                publication.expected_bar_count
         FROM analytical_generations AS selected_generation
         JOIN dataset_manifests AS selected_manifest
           ON selected_manifest.manifest_id=selected_generation.anchor_manifest_id
         JOIN artifacts AS selected_artifact
           ON selected_artifact.artifact_id=selected_manifest.artifact_id
         JOIN ingest_runs AS selected_run
           ON selected_run.run_id=selected_artifact.run_id
         JOIN analytical_generation_market_bar_history_inputs AS history_input
           ON history_input.generation_sequence=selected_generation.generation_sequence
         JOIN market_bar_history_publications AS publication
           USING (publication_receipt_digest)
         JOIN ingest_runs AS origin_run
           ON origin_run.run_id=publication.origin_run_id
         JOIN provider_capture_bindings AS binding
           ON binding.binding_digest=publication.binding_digest
         JOIN provider_raw_observations AS capture
           ON capture.capture_observation_digest=binding.capture_observation_digest
         JOIN analytical_generation_provider_capture_bindings AS selected_capture
           ON selected_capture.generation_sequence=selected_generation.generation_sequence
          AND selected_capture.binding_digest=publication.binding_digest
         WHERE publication.instrument_id=?1
           AND selected_generation.schema_name=?3
           AND selected_generation.schema_version=?4
           AND selected_generation.schema_fingerprint=?5
           AND publication.source_id='alpaca-basic-iex-market-data'
           AND publication.venue_id='iex'
           AND publication.feed='iex'
           AND publication.bar_interval='1Day'
           AND publication.adjustment='all'
           AND publication.timestamp_basis='period_start'
           AND publication.session_kind='provider_defined'
           AND publication.session_ruleset='alpaca-v3-iex-utc-range-returned-dates-v2'
           AND publication.graph_purpose='alpaca-iex-historical-bars-and-calendar/v1'
           AND publication.asset_class IN ('equity', 'fund')
           AND publication.currency='USD'
           AND publication.requested_end_ns<=?2
           AND publication.coverage_last_complete_ns<=?2
           AND publication.expected_bar_count=publication.returned_bar_count
           AND selected_generation.created_at_ns<=?2
           AND selected_manifest.created_at_ns<=?2
           AND selected_artifact.created_at_ns<=?2
           AND selected_run.state='succeeded'
           AND selected_run.operation='persist'
           AND selected_run.source_id=publication.source_id
           AND selected_run.requested_at_ns<=?2
           AND selected_run.completed_at_ns<=?2
           AND origin_run.state='succeeded'
           AND origin_run.operation='persist'
           AND origin_run.source_id=publication.source_id
           AND origin_run.requested_at_ns<=?2
           AND origin_run.completed_at_ns<=?2
           AND capture.recorded_at_ns<=?2
           AND publication.capture_recorded_at_ns<=?2
           AND publication.max_available_at_ns<=?2
           AND publication.max_received_at_ns<=?2
           AND publication.max_ingested_at_ns<=?2
           AND publication.published_at_ns<=?2
           AND publication.admission_class='current_research_only'
           AND publication.current_research_eligible=1
           AND publication.point_in_time_eligible=0
           AND publication.backtest_eligible=0
           AND publication.retrospective_training_eligible=0
         ORDER BY publication.coverage_last_complete_ns DESC,
                  publication.requested_end_ns DESC,
                  publication.expected_bar_count DESC,
                  publication.requested_start_ns ASC,
                  publication.coverage_first_ns ASC,
                  publication.coverage_last_ns DESC
         LIMIT 2",
    )?;
    let rows = statement.query_map(
        params![
            request.instrument_id().to_string(),
            request.knowledge_cutoff().unix_nanos(),
            canonical_schema.name(),
            i64::from(canonical_schema.version().get()),
            canonical_schema.fingerprint().as_slice(),
        ],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        },
    )?;
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(2)
        .map_err(|_| ManifestCatalogError::CountOverflow)?;
    for row in rows {
        check_operation(deadline, cancellation)?;
        let (start, end, first, last, last_complete, count) = row?;
        candidates.push(LatestCanonicalMarketBarHistoryWindowCandidate::try_new(
            start,
            end,
            first,
            last,
            last_complete,
            count,
            request.knowledge_cutoff(),
        )?);
    }
    drop(statement);
    if candidates
        .get(1)
        .is_some_and(|other| candidates[0].has_same_selection_rank(*other))
    {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let Some(candidate) = candidates.into_iter().next() else {
        return Ok(None);
    };
    let (requested_start, requested_end) = candidate.requested_range();
    let canonical_request = CanonicalMarketBarHistoryRequest::try_latest(
        request.instrument_id(),
        requested_start,
        requested_end,
        request.selection_policy(),
        request.knowledge_cutoff(),
    )?;
    check_operation(deadline, cancellation)?;
    let validated = select_canonical_market_bar_history(
        connection,
        max_objects,
        &canonical_request,
        deadline,
        cancellation,
    )?
    .ok_or(ManifestCatalogError::CorruptCatalog)?;
    let receipt = validated.receipt();
    if receipt.instrument_id() != request.instrument_id()
        || receipt.requested_range() != candidate.requested_range()
        || receipt.coverage() != candidate.coverage()
        || receipt.bar_count() != candidate.expected_bar_count
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let exact_request = CanonicalMarketBarHistoryRequest::try_exact(
        request.instrument_id(),
        requested_start,
        requested_end,
        request.selection_policy(),
        request.knowledge_cutoff(),
        validated.pinned().manifest().clone(),
    )?;
    let lookup_digest =
        latest_canonical_history_window_selection_digest(request, candidate, &validated)?;
    Ok(Some(LatestCanonicalMarketBarHistoryWindowSelection {
        exact_request,
        lookup_digest,
    }))
}

pub(super) fn select_canonical_market_bar_history(
    connection: &Connection,
    max_objects: usize,
    request: &CanonicalMarketBarHistoryRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<CompleteMarketBarHistorySelection>, ManifestCatalogError> {
    check_operation(deadline, cancellation)?;
    if !request.selection_policy().is_supported() {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let canonical_schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
    if request
        .exact_manifest()
        .is_some_and(|manifest| manifest.schema() != &canonical_schema)
    {
        return Err(ManifestCatalogError::MarketBarHistoryMismatch);
    }
    let Some(resolved) = resolve_canonical_market_bar_history_series(
        connection,
        request,
        &canonical_schema,
        deadline,
        cancellation,
    )?
    else {
        return Ok(None);
    };
    check_operation(deadline, cancellation)?;
    select_complete_market_bar_history(connection, max_objects, &resolved, deadline, cancellation)?
        .map(Some)
        .ok_or(ManifestCatalogError::CorruptCatalog)
}

fn ensure_unambiguous_history_series(
    connection: &Connection,
    request: &CompleteMarketBarHistoryRequest,
    canonical_schema: &DatasetSchemaRef,
) -> Result<(), ManifestCatalogError> {
    let zero_digest = [0_u8; 32];
    let exact = request.exact_manifest();
    let exact_enabled = i64::from(exact.is_some());
    let exact_dataset = exact.map_or("", |manifest| manifest.dataset_id().as_str());
    let exact_version = exact
        .map(|manifest| i64::try_from(manifest.manifest_version()))
        .transpose()
        .map_err(|_| ManifestCatalogError::CountOverflow)?
        .unwrap_or(0);
    let exact_schema_name = exact.map_or("", |manifest| manifest.schema().name());
    let exact_schema_version =
        exact.map_or(0, |manifest| i64::from(manifest.schema_version().get()));
    let exact_fingerprint = exact.map_or(zero_digest, |manifest| manifest.schema().fingerprint());
    let exact_content = exact.map_or(zero_digest, |manifest| manifest.content_hash().bytes());
    let series_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM (
             SELECT publication.asset_class
             FROM analytical_generations AS selected_generation
             JOIN dataset_manifests AS selected_manifest
               ON selected_manifest.manifest_id=selected_generation.anchor_manifest_id
             JOIN artifacts AS selected_artifact
               ON selected_artifact.artifact_id=selected_manifest.artifact_id
             JOIN ingest_runs AS selected_run
               ON selected_run.run_id=selected_artifact.run_id
             JOIN analytical_generation_market_bar_history_inputs AS history_input
               ON history_input.generation_sequence=selected_generation.generation_sequence
             JOIN market_bar_history_publications AS publication
               USING (publication_receipt_digest)
             JOIN ingest_runs AS origin_run
               ON origin_run.run_id=publication.origin_run_id
             JOIN provider_capture_bindings AS binding
               ON binding.binding_digest=publication.binding_digest
             JOIN provider_raw_observations AS capture
               ON capture.capture_observation_digest=binding.capture_observation_digest
             JOIN analytical_generation_provider_capture_bindings AS selected_capture
               ON selected_capture.generation_sequence=selected_generation.generation_sequence
              AND selected_capture.binding_digest=publication.binding_digest
             WHERE publication.instrument_id=?1
               AND publication.requested_start_ns=?13
               AND publication.requested_end_ns=?14
               AND publication.provider_instrument_id=?15
               AND publication.venue_id=?16
               AND publication.feed=?17
               AND publication.bar_interval=?18
               AND publication.adjustment=?19
               AND publication.timestamp_basis=?20
               AND publication.session_kind=?21
               AND publication.session_ruleset=?22
               AND selected_generation.schema_name=?10
               AND selected_generation.schema_version=?11
               AND selected_generation.schema_fingerprint=?12
               AND (?3=0 OR (
                    selected_generation.dataset_id=?4
                    AND selected_generation.manifest_version=?5
                    AND selected_generation.schema_name=?6
                    AND selected_generation.schema_version=?7
                    AND selected_generation.schema_fingerprint=?8
                    AND selected_generation.content_hash=?9
               ))
               AND publication.source_id='alpaca-basic-iex-market-data'
               AND publication.graph_purpose='alpaca-iex-historical-bars-and-calendar/v1'
               AND publication.asset_class IN ('equity', 'fund')
               AND publication.currency='USD'
               AND selected_generation.created_at_ns<=?2
               AND selected_manifest.created_at_ns<=?2
               AND selected_artifact.created_at_ns<=?2
               AND selected_run.state='succeeded'
               AND selected_run.operation='persist'
               AND selected_run.source_id=publication.source_id
               AND selected_run.requested_at_ns<=?2
               AND selected_run.completed_at_ns<=?2
               AND origin_run.state='succeeded'
               AND origin_run.operation='persist'
               AND origin_run.source_id=publication.source_id
               AND origin_run.requested_at_ns<=?2
               AND origin_run.completed_at_ns<=?2
               AND capture.recorded_at_ns<=?2
               AND publication.capture_recorded_at_ns<=?2
               AND publication.max_available_at_ns<=?2
               AND publication.max_received_at_ns<=?2
               AND publication.max_ingested_at_ns<=?2
               AND publication.published_at_ns<=?2
               AND publication.admission_class='current_research_only'
               AND publication.current_research_eligible=1
               AND publication.point_in_time_eligible=0
               AND publication.backtest_eligible=0
               AND publication.retrospective_training_eligible=0
             GROUP BY publication.asset_class
             LIMIT 2
         )",
        params![
            request.instrument_id().to_string(),
            request.knowledge_cutoff().unix_nanos(),
            exact_enabled,
            exact_dataset,
            exact_version,
            exact_schema_name,
            exact_schema_version,
            exact_fingerprint.as_slice(),
            exact_content.as_slice(),
            canonical_schema.name(),
            i64::from(canonical_schema.version().get()),
            canonical_schema.fingerprint().as_slice(),
            request.requested_range().0.unix_nanos(),
            request.requested_range().1.unix_nanos(),
            request.provider_instrument_id().as_str(),
            request.venue_id().as_str(),
            request.feed().as_str(),
            request.interval().as_str(),
            adjustment_name(request.adjustment()),
            timestamp_basis_name(request.timestamp_basis()),
            session_kind_name(request.session_kind()),
            request.session_ruleset().as_str(),
        ],
        |row| row.get(0),
    )?;
    if series_count <= 1 {
        Ok(())
    } else {
        Err(ManifestCatalogError::MarketBarHistoryMismatch)
    }
}

pub(super) fn select_complete_market_bar_history(
    connection: &Connection,
    max_objects: usize,
    request: &CompleteMarketBarHistoryRequest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<CompleteMarketBarHistorySelection>, ManifestCatalogError> {
    check_operation(deadline, cancellation)?;
    let canonical_schema = DatasetSchemaRegistry::local().canonical_research_observations()?;
    ensure_unambiguous_history_series(connection, request, &canonical_schema)?;
    let selected = if let Some(exact) = request.exact_manifest() {
        if exact.schema() != &canonical_schema {
            return Err(ManifestCatalogError::MarketBarHistoryMismatch);
        }
        let digest = connection
            .query_row(
                "SELECT publication.publication_receipt_digest
                 FROM analytical_generations AS selected_generation
                 JOIN dataset_manifests AS selected_manifest
                   ON selected_manifest.manifest_id=selected_generation.anchor_manifest_id
                 JOIN artifacts AS selected_artifact
                   ON selected_artifact.artifact_id=selected_manifest.artifact_id
                 JOIN ingest_runs AS selected_run
                   ON selected_run.run_id=selected_artifact.run_id
                 JOIN analytical_generation_market_bar_history_inputs AS history_input
                   ON history_input.generation_sequence=selected_generation.generation_sequence
                 JOIN market_bar_history_publications AS publication
                   USING (publication_receipt_digest)
                 JOIN ingest_runs AS origin_run
                   ON origin_run.run_id=publication.origin_run_id
                 JOIN provider_capture_bindings AS binding
                   ON binding.binding_digest=publication.binding_digest
                 JOIN provider_raw_observations AS capture
                   ON capture.capture_observation_digest=binding.capture_observation_digest
                 JOIN analytical_generation_provider_capture_bindings AS selected_capture
                   ON selected_capture.generation_sequence=selected_generation.generation_sequence
                  AND selected_capture.binding_digest=publication.binding_digest
                 WHERE selected_generation.dataset_id=?1
                   AND selected_generation.manifest_version=?2
                   AND selected_generation.schema_name=?3
                   AND selected_generation.schema_version=?4
                   AND selected_generation.schema_fingerprint=?5
                   AND selected_generation.content_hash=?6
                   AND publication.instrument_id=?7
                   AND publication.requested_start_ns=?9
                   AND publication.requested_end_ns=?10
                   AND publication.provider_instrument_id=?11
                   AND publication.venue_id=?12
                   AND publication.feed=?13
                   AND publication.bar_interval=?14
                   AND publication.adjustment=?15
                   AND publication.timestamp_basis=?16
                   AND publication.session_kind=?17
                   AND publication.session_ruleset=?18
                   AND publication.source_id='alpaca-basic-iex-market-data'
                   AND publication.graph_purpose='alpaca-iex-historical-bars-and-calendar/v1'
                   AND publication.asset_class IN ('equity', 'fund')
                   AND publication.currency='USD'
                   AND selected_generation.created_at_ns<=?8
                   AND selected_manifest.created_at_ns<=?8
                   AND selected_artifact.created_at_ns<=?8
                   AND selected_run.state='succeeded'
                   AND selected_run.operation='persist'
                   AND selected_run.source_id=publication.source_id
                   AND selected_run.requested_at_ns<=?8
                   AND selected_run.completed_at_ns<=?8
                   AND origin_run.state='succeeded'
                   AND origin_run.operation='persist'
                   AND origin_run.source_id=publication.source_id
                   AND origin_run.requested_at_ns<=?8
                   AND origin_run.completed_at_ns<=?8
                   AND capture.recorded_at_ns<=?8
                   AND publication.capture_recorded_at_ns<=?8
                   AND publication.max_available_at_ns<=?8
                   AND publication.max_received_at_ns<=?8
                   AND publication.max_ingested_at_ns<=?8
                   AND publication.published_at_ns<=?8
                   AND publication.admission_class='current_research_only'
                   AND publication.current_research_eligible=1
                   AND publication.point_in_time_eligible=0
                   AND publication.backtest_eligible=0
                   AND publication.retrospective_training_eligible=0
                 ORDER BY publication.published_at_ns DESC,
                          publication.origin_generation_sequence DESC,
                          publication.publication_receipt_digest DESC
                 LIMIT 1",
                params![
                    exact.dataset_id().as_str(),
                    i64::try_from(exact.manifest_version())
                        .map_err(|_| ManifestCatalogError::CountOverflow)?,
                    exact.schema().name(),
                    i64::from(exact.schema_version().get()),
                    exact.schema().fingerprint().as_slice(),
                    exact.content_hash().bytes(),
                    request.instrument_id().to_string(),
                    request.knowledge_cutoff().unix_nanos(),
                    request.requested_range().0.unix_nanos(),
                    request.requested_range().1.unix_nanos(),
                    request.provider_instrument_id().as_str(),
                    request.venue_id().as_str(),
                    request.feed().as_str(),
                    request.interval().as_str(),
                    adjustment_name(request.adjustment()),
                    timestamp_basis_name(request.timestamp_basis()),
                    session_kind_name(request.session_kind()),
                    request.session_ruleset().as_str(),
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(|digest| parse_sha256(&digest))
            .transpose()?;
        digest.map(|digest| (exact.clone(), digest))
    } else {
        connection
            .query_row(
                "SELECT selected_generation.dataset_id,
                        selected_generation.manifest_version,
                        selected_generation.schema_name,
                        selected_generation.schema_version,
                        selected_generation.schema_fingerprint,
                        selected_generation.content_hash,
                        publication.publication_receipt_digest
                 FROM analytical_generations AS selected_generation
                 JOIN dataset_manifests AS selected_manifest
                   ON selected_manifest.manifest_id=selected_generation.anchor_manifest_id
                 JOIN artifacts AS selected_artifact
                   ON selected_artifact.artifact_id=selected_manifest.artifact_id
                 JOIN ingest_runs AS selected_run
                   ON selected_run.run_id=selected_artifact.run_id
                 JOIN analytical_generation_market_bar_history_inputs AS history_input
                   ON history_input.generation_sequence=selected_generation.generation_sequence
                 JOIN market_bar_history_publications AS publication
                   USING (publication_receipt_digest)
                 JOIN ingest_runs AS origin_run
                   ON origin_run.run_id=publication.origin_run_id
                 JOIN provider_capture_bindings AS binding
                   ON binding.binding_digest=publication.binding_digest
                 JOIN provider_raw_observations AS capture
                   ON capture.capture_observation_digest=binding.capture_observation_digest
                 JOIN analytical_generation_provider_capture_bindings AS selected_capture
                   ON selected_capture.generation_sequence=selected_generation.generation_sequence
                  AND selected_capture.binding_digest=publication.binding_digest
                 WHERE publication.instrument_id=?1
                   AND selected_generation.schema_name=?3
                   AND selected_generation.schema_version=?4
                   AND selected_generation.schema_fingerprint=?5
                   AND publication.source_id='alpaca-basic-iex-market-data'
                   AND publication.requested_start_ns=?6
                   AND publication.requested_end_ns=?7
                   AND publication.provider_instrument_id=?8
                   AND publication.venue_id=?9
                   AND publication.feed=?10
                   AND publication.bar_interval=?11
                   AND publication.adjustment=?12
                   AND publication.timestamp_basis=?13
                   AND publication.session_kind=?14
                   AND publication.session_ruleset=?15
                   AND publication.graph_purpose='alpaca-iex-historical-bars-and-calendar/v1'
                   AND publication.asset_class IN ('equity', 'fund')
                   AND publication.currency='USD'
                   AND selected_generation.created_at_ns<=?2
                   AND selected_manifest.created_at_ns<=?2
                   AND selected_artifact.created_at_ns<=?2
                   AND selected_run.state='succeeded'
                   AND selected_run.operation='persist'
                   AND selected_run.source_id=publication.source_id
                   AND selected_run.requested_at_ns<=?2
                   AND selected_run.completed_at_ns<=?2
                   AND origin_run.state='succeeded'
                   AND origin_run.operation='persist'
                   AND origin_run.source_id=publication.source_id
                   AND origin_run.requested_at_ns<=?2
                   AND origin_run.completed_at_ns<=?2
                   AND capture.recorded_at_ns<=?2
                   AND publication.capture_recorded_at_ns<=?2
                   AND publication.max_available_at_ns<=?2
                   AND publication.max_received_at_ns<=?2
                   AND publication.max_ingested_at_ns<=?2
                   AND publication.published_at_ns<=?2
                   AND publication.admission_class='current_research_only'
                   AND publication.current_research_eligible=1
                   AND publication.point_in_time_eligible=0
                   AND publication.backtest_eligible=0
                   AND publication.retrospective_training_eligible=0
                 ORDER BY publication.published_at_ns DESC,
                          publication.origin_generation_sequence DESC,
                          publication.publication_receipt_digest DESC,
                          selected_generation.created_at_ns DESC,
                          selected_generation.generation_sequence DESC
                 LIMIT 1",
                params![
                    request.instrument_id().to_string(),
                    request.knowledge_cutoff().unix_nanos(),
                    canonical_schema.name(),
                    i64::from(canonical_schema.version().get()),
                    canonical_schema.fingerprint().as_slice(),
                    request.requested_range().0.unix_nanos(),
                    request.requested_range().1.unix_nanos(),
                    request.provider_instrument_id().as_str(),
                    request.venue_id().as_str(),
                    request.feed().as_str(),
                    request.interval().as_str(),
                    adjustment_name(request.adjustment()),
                    timestamp_basis_name(request.timestamp_basis()),
                    session_kind_name(request.session_kind()),
                    request.session_ruleset().as_str(),
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, Vec<u8>>(4)?,
                        row.get::<_, Vec<u8>>(5)?,
                        row.get::<_, Vec<u8>>(6)?,
                    ))
                },
            )
            .optional()?
            .map(
                |(dataset, version, schema_name, schema_version, fingerprint, content, receipt)| {
                    let schema_version = u16::try_from(schema_version)
                        .ok()
                        .and_then(|value| market_squawk_domain::SchemaVersion::new(value).ok())
                        .ok_or(ManifestCatalogError::CorruptCatalog)?;
                    let schema = DatasetSchemaRef::try_new(
                        schema_name,
                        schema_version,
                        parse_digest_array(&fingerprint)?,
                    )?;
                    require_canonical_history_schema(&schema)?;
                    let manifest = DatasetManifestRef::try_new_with_schema(
                        DatasetId::try_from(dataset.as_str())?,
                        u64::try_from(version)
                            .ok()
                            .filter(|value| *value > 0)
                            .ok_or(ManifestCatalogError::CorruptCatalog)?,
                        schema,
                        parse_sha256(&content)?,
                    )?;
                    Ok::<(DatasetManifestRef, Sha256Digest), ManifestCatalogError>((
                        manifest,
                        parse_sha256(&receipt)?,
                    ))
                },
            )
            .transpose()?
    };
    let Some((manifest, publication_digest)) = selected else {
        return Ok(None);
    };
    check_operation(deadline, cancellation)?;
    let pinned = load_pinned(connection, &manifest, max_objects)?;
    let receipt = load_market_bar_history_receipt(
        connection,
        publication_digest,
        request.instrument_id(),
        request.knowledge_cutoff(),
    )?;
    if !request.matches_receipt(&receipt) {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    check_operation(deadline, cancellation)?;
    let policy_digest = history_policy_digest()?;
    let selection_digest =
        history_selection_digest(policy_digest, request, &manifest, publication_digest)?;
    Ok(Some(CompleteMarketBarHistorySelection {
        pinned,
        receipt,
        policy_digest,
        selection_digest,
    }))
}

fn load_market_bar_history_receipt(
    connection: &Connection,
    publication_digest: Sha256Digest,
    expected_instrument: InstrumentId,
    cutoff: Timestamp,
) -> Result<MarketBarHistoryPublicationReceipt, ManifestCatalogError> {
    let (receipt_json, origin_requested_at_ns): (String, i64) = connection
        .query_row(
            "SELECT publication.receipt_json, origin_run.requested_at_ns
             FROM market_bar_history_publications AS publication
             JOIN analytical_generations AS origin_generation
               ON origin_generation.generation_sequence=publication.origin_generation_sequence
             JOIN analytical_generation_source_inputs AS source_input
               ON source_input.generation_sequence=origin_generation.generation_sequence
              AND source_input.run_id=publication.origin_run_id
              AND source_input.source_id=publication.source_id
             JOIN ingest_runs AS origin_run
               ON origin_run.run_id=publication.origin_run_id
             JOIN dataset_manifests AS anchor
               ON anchor.manifest_id=publication.origin_anchor_manifest_id
              AND anchor.manifest_id=origin_generation.anchor_manifest_id
             JOIN artifacts AS artifact
               ON artifact.artifact_id=publication.origin_artifact_id
              AND artifact.artifact_id=anchor.artifact_id
              AND artifact.run_id=origin_run.run_id
             JOIN analytical_generation_objects AS object
               ON object.dataset_id=origin_generation.dataset_id
              AND object.manifest_version=origin_generation.manifest_version
              AND object.ordinal=publication.origin_object_ordinal
              AND object.artifact_id=artifact.artifact_id
              AND object.row_count=publication.returned_bar_count
             JOIN analytical_generation_provider_capture_bindings AS capture_input
               ON capture_input.generation_sequence=origin_generation.generation_sequence
              AND capture_input.binding_digest=publication.binding_digest
              AND capture_input.run_id=origin_run.run_id
              AND capture_input.source_id=publication.source_id
             JOIN provider_capture_bindings AS binding
               ON binding.binding_digest=publication.binding_digest
             JOIN provider_raw_observations AS capture
               ON capture.capture_observation_digest=binding.capture_observation_digest
              AND capture.capture_content_digest=publication.capture_content_digest
              AND capture.capture_observation_digest=publication.capture_observation_digest
              AND capture.recorded_at_ns=publication.capture_recorded_at_ns
              AND capture.source_id=publication.source_id
              AND capture.provider_dataset=publication.provider_dataset
              AND capture.terminal_disposition='complete_request_graph'
             JOIN market_data_instrument_revisions AS instrument_revision
               ON instrument_revision.revision_digest=publication.instrument_revision_digest
              AND instrument_revision.instrument_id=publication.instrument_id
             WHERE publication.publication_receipt_digest=?1
               AND origin_run.state='succeeded'
               AND origin_run.operation='persist'
               AND origin_run.source_id=publication.source_id",
            [publication_digest.bytes()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    if receipt_digest(receipt_json.as_bytes())? != publication_digest {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let wire: MarketBarHistoryReceiptWire =
        serde_json::from_str(&receipt_json).map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let asset_class = validate_exact_instrument_revision(
        connection,
        nonzero_sha256(wire.instrument_revision_digest)?,
        wire.instrument_id,
        &wire.source_id,
        &wire.provider_instrument_id,
        wire.currency,
        Timestamp::from_unix_nanos(wire.requested_start_ns),
        Timestamp::from_unix_nanos(wire.requested_end_ns),
        Timestamp::from_unix_nanos(origin_requested_at_ns),
    )?;
    if asset_class != wire.asset_class {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    if !publication_wire_matches_row(connection, publication_digest, &wire, &receipt_json)? {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let persisted_capture = load_provider_capture_for_run(connection, wire.origin_run_id)?
        .ok_or(ManifestCatalogError::CorruptCatalog)?;
    if sha256_evidence(persisted_capture.sealed_capture_receipt_digest())?.bytes()
        != wire.capture_receipt_digest
        || sha256_evidence(persisted_capture.binding_digest())?.bytes() != wire.binding_digest
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let expected_provider_timestamps =
        validate_capture_against_wire(persisted_capture.capture(), &wire)?;
    receipt_from_wire(
        publication_digest,
        wire,
        expected_provider_timestamps,
        expected_instrument,
        cutoff,
    )
}

fn publication_wire_matches_row(
    connection: &Connection,
    publication_digest: Sha256Digest,
    wire: &MarketBarHistoryReceiptWire,
    receipt_json: &str,
) -> Result<bool, ManifestCatalogError> {
    let matches = connection.query_row(
        "SELECT EXISTS(
             SELECT 1
             FROM market_bar_history_publications AS publication
             JOIN analytical_generations AS generation
               ON generation.generation_sequence=publication.origin_generation_sequence
             WHERE publication.publication_receipt_digest=?1
               AND publication.receipt_version=?2
               AND generation.dataset_id=?3
               AND generation.manifest_version=?4
               AND generation.schema_name=?5
               AND generation.schema_version=?6
               AND generation.schema_fingerprint=?7
               AND generation.content_hash=?8
               AND publication.origin_run_id=?9
               AND publication.origin_anchor_manifest_id=?10
               AND publication.origin_artifact_id=?11
               AND publication.origin_object_ordinal=?12
               AND publication.source_id=?13
               AND publication.capture_receipt_digest=?14
               AND publication.capture_content_digest=?15
               AND publication.capture_observation_digest=?16
               AND publication.capture_recorded_at_ns=?17
               AND publication.provider_dataset=?18
               AND publication.instrument_id=?19
               AND publication.asset_class=?59
               AND publication.instrument_revision_digest=?20
               AND publication.admitted_plan_digest=?21
               AND publication.provider_instrument_id=?22
               AND publication.venue_id=?23
               AND publication.feed=?24
               AND publication.bar_interval=?25
               AND publication.adjustment=?26
               AND publication.timestamp_basis=?27
               AND publication.session_kind=?28
               AND publication.session_ruleset=?29
               AND publication.graph_purpose=?30
               AND publication.currency=?31
               AND publication.requested_start_ns=?32
               AND publication.requested_end_ns=?33
               AND publication.coverage_first_ns=?34
               AND publication.coverage_last_ns=?35
               AND publication.coverage_last_complete_ns=?36
               AND publication.expected_bar_count=?37
               AND publication.returned_bar_count=?38
               AND publication.expected_timestamp_set_digest=?39
               AND publication.bar_set_digest=?40
               AND publication.completeness_evidence_digest=?41
               AND publication.market_bar_component_ordinal=?42
               AND publication.market_bar_component_content_digest=?43
               AND publication.market_bar_component_page_count=?44
               AND publication.session_calendar_component_ordinal=?45
               AND publication.session_calendar_component_content_digest=?46
               AND publication.session_calendar_component_page_count=?47
               AND publication.max_available_at_ns=?48
               AND publication.max_received_at_ns=?49
               AND publication.max_ingested_at_ns=?50
               AND publication.published_at_ns=?51
               AND publication.admission_class=?52
               AND publication.current_research_eligible=?53
               AND publication.point_in_time_eligible=?54
               AND publication.backtest_eligible=?55
               AND publication.retrospective_training_eligible=?56
               AND publication.admission_reason=?57
               AND publication.receipt_json=?58
               AND publication.binding_digest=?60
         )",
        params![
            publication_digest.bytes(),
            i64::from(wire.receipt_version),
            wire.origin_dataset_id,
            i64::try_from(wire.origin_manifest_version)
                .map_err(|_| ManifestCatalogError::CountOverflow)?,
            wire.origin_schema_name,
            i64::from(wire.origin_schema_version),
            wire.origin_schema_fingerprint,
            wire.origin_manifest_content_hash,
            wire.origin_run_id.to_string(),
            wire.origin_anchor_manifest_id.to_string(),
            wire.origin_artifact_id.to_string(),
            i64::from(wire.origin_object_ordinal),
            wire.source_id.as_str(),
            wire.capture_receipt_digest,
            wire.capture_content_digest,
            wire.capture_observation_digest,
            wire.capture_recorded_at_ns,
            wire.provider_dataset.as_str(),
            wire.instrument_id.to_string(),
            wire.instrument_revision_digest,
            wire.admitted_plan_digest,
            wire.provider_instrument_id.as_str(),
            wire.venue_id.as_str(),
            wire.feed.as_str(),
            wire.interval.as_str(),
            adjustment_name(wire.adjustment),
            timestamp_basis_name(wire.timestamp_basis),
            session_kind_name(wire.session_kind),
            wire.session_ruleset.as_str(),
            wire.graph_purpose.as_str(),
            wire.currency.as_str(),
            wire.requested_start_ns,
            wire.requested_end_ns,
            wire.coverage_first_ns,
            wire.coverage_last_ns,
            wire.coverage_last_complete_ns,
            i64::from(wire.expected_bar_count),
            i64::from(wire.returned_bar_count),
            wire.expected_timestamp_set_digest,
            wire.bar_set_digest,
            wire.completeness_evidence_digest,
            i64::from(wire.market_bar_component_ordinal),
            wire.market_bar_component_content_digest,
            i64::from(wire.market_bar_component_page_count),
            i64::from(wire.session_calendar_component_ordinal),
            wire.session_calendar_component_content_digest,
            i64::from(wire.session_calendar_component_page_count),
            wire.max_available_at_ns,
            wire.max_received_at_ns,
            wire.max_ingested_at_ns,
            wire.published_at_ns,
            wire.admission_class,
            wire.current_research_eligible,
            wire.point_in_time_eligible,
            wire.backtest_eligible,
            wire.retrospective_training_eligible,
            wire.admission_reason,
            receipt_json,
            asset_class_name(wire.asset_class),
            wire.binding_digest,
        ],
        |row| row.get(0),
    )?;
    Ok(matches)
}

fn validate_capture_against_wire(
    capture: &ProviderCaptureSetReceipt,
    wire: &MarketBarHistoryReceiptWire,
) -> Result<Box<[Timestamp]>, ManifestCatalogError> {
    let Some(ProviderCaptureSemanticBinding::CompleteMarketBarHistoryV1(binding)) =
        capture.semantic_binding()
    else {
        return Err(ManifestCatalogError::CorruptCatalog);
    };
    let components = capture.request_graph_components();
    let market_bar_component = components.get(usize::from(wire.market_bar_component_ordinal));
    let session_calendar_component =
        components.get(usize::from(wire.session_calendar_component_ordinal));
    if capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || components.len() != 2
        || capture.source_id() != &wire.source_id
        || capture.dataset() != &wire.provider_dataset
        || capture.content_digest().bytes() != wire.capture_content_digest
        || capture.observation_digest().bytes() != wire.capture_observation_digest
        || binding.instrument_id() != wire.instrument_id
        || binding.instrument_revision_digest().bytes() != wire.instrument_revision_digest
        || binding.admitted_plan_digest().bytes() != wire.admitted_plan_digest
        || binding.provider_instrument_id() != &wire.provider_instrument_id
        || binding.venue_id() != &wire.venue_id
        || binding.feed() != &wire.feed
        || binding.interval() != &wire.interval
        || binding.adjustment() != wire.adjustment
        || binding.timestamp_basis() != wire.timestamp_basis
        || binding.session_kind() != wire.session_kind
        || binding.session_ruleset() != &wire.session_ruleset
        || binding.graph_purpose() != &wire.graph_purpose
        || binding.requested_start().unix_nanos() != wire.requested_start_ns
        || binding.requested_end().unix_nanos() != wire.requested_end_ns
        || binding.expected_provider_timestamps().len()
            != usize::try_from(wire.expected_bar_count)
                .map_err(|_| ManifestCatalogError::CorruptCatalog)?
        || expected_timestamp_set_digest(binding.expected_provider_timestamps())?.bytes()
            != wire.expected_timestamp_set_digest
        || binding.completeness_evidence().bytes() != wire.completeness_evidence_digest
        || binding.market_bar_component_ordinal() != wire.market_bar_component_ordinal
        || binding.session_calendar_component_ordinal() != wire.session_calendar_component_ordinal
        || binding
            .expected_provider_timestamps()
            .first()
            .map(|timestamp| timestamp.unix_nanos())
            != Some(wire.coverage_first_ns)
        || binding
            .expected_provider_timestamps()
            .last()
            .map(|timestamp| timestamp.unix_nanos())
            != Some(wire.coverage_last_ns)
        || market_bar_component.is_none_or(|component| {
            component.ordinal() != wire.market_bar_component_ordinal
                || component.dataset() != capture.dataset()
                || component.terminal()
                    != ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
                || component.content_digest().bytes() != wire.market_bar_component_content_digest
                || component.page_count().get() != wire.market_bar_component_page_count
        })
        || session_calendar_component.is_none_or(|component| {
            component.ordinal() != wire.session_calendar_component_ordinal
                || component.dataset() != capture.dataset()
                || component.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
                || component.content_digest().bytes()
                    != wire.session_calendar_component_content_digest
                || component.page_count().get() != wire.session_calendar_component_page_count
        })
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    let mut expected_provider_timestamps = Vec::new();
    expected_provider_timestamps
        .try_reserve_exact(binding.expected_provider_timestamps().len())
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    expected_provider_timestamps.extend_from_slice(binding.expected_provider_timestamps());
    Ok(expected_provider_timestamps.into_boxed_slice())
}

fn receipt_from_wire(
    receipt_digest: Sha256Digest,
    wire: MarketBarHistoryReceiptWire,
    expected_provider_timestamps: Box<[Timestamp]>,
    expected_instrument: InstrumentId,
    cutoff: Timestamp,
) -> Result<MarketBarHistoryPublicationReceipt, ManifestCatalogError> {
    let schema_version = market_squawk_domain::SchemaVersion::new(wire.origin_schema_version)
        .map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let schema = DatasetSchemaRef::try_new(
        &wire.origin_schema_name,
        schema_version,
        wire.origin_schema_fingerprint,
    )?;
    require_canonical_history_schema(&schema).map_err(|_| ManifestCatalogError::CorruptCatalog)?;
    let origin_manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from(wire.origin_dataset_id.as_str())?,
        wire.origin_manifest_version,
        schema,
        nonzero_sha256(wire.origin_manifest_content_hash)?,
    )?;
    let max_available_at = Timestamp::from_unix_nanos(wire.max_available_at_ns);
    let max_received_at = Timestamp::from_unix_nanos(wire.max_received_at_ns);
    let max_ingested_at = Timestamp::from_unix_nanos(wire.max_ingested_at_ns);
    let published_at = Timestamp::from_unix_nanos(wire.published_at_ns);
    if wire.receipt_version != MARKET_BAR_HISTORY_RECEIPT_VERSION
        || wire.instrument_id != expected_instrument
        || !matches!(wire.asset_class, AssetClass::Equity | AssetClass::Fund)
        || wire.source_id.as_str() != ALPACA_HISTORY_SOURCE
        || wire.venue_id.as_str() != ALPACA_HISTORY_VENUE
        || wire.feed.as_str() != ALPACA_HISTORY_FEED
        || wire.interval.as_str() != ALPACA_HISTORY_INTERVAL
        || adjustment_name(wire.adjustment) != ALPACA_HISTORY_ADJUSTMENT
        || timestamp_basis_name(wire.timestamp_basis) != ALPACA_HISTORY_TIMESTAMP_BASIS
        || session_kind_name(wire.session_kind) != ALPACA_HISTORY_SESSION_KIND
        || wire.session_ruleset.as_str() != ALPACA_HISTORY_SESSION_RULESET
        || wire.graph_purpose.as_str() != ALPACA_HISTORY_GRAPH_PURPOSE
        || wire.currency.as_str() != ALPACA_HISTORY_CURRENCY
        || wire.expected_bar_count == 0
        || wire.expected_bar_count != wire.returned_bar_count
        || usize::try_from(wire.expected_bar_count)
            .ok()
            .is_none_or(|count| count > MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS)
        || wire.requested_start_ns >= wire.requested_end_ns
        || wire.coverage_first_ns < wire.requested_start_ns
        || wire.coverage_last_ns < wire.coverage_first_ns
        || wire.coverage_last_ns > wire.requested_end_ns
        || wire.coverage_last_complete_ns < wire.coverage_last_ns
        || wire.coverage_last_complete_ns > wire.requested_end_ns
        || max_available_at > max_ingested_at
        || max_received_at > max_ingested_at
        || max_ingested_at > published_at
        || max_available_at > cutoff
        || max_received_at > cutoff
        || max_ingested_at > cutoff
        || published_at > cutoff
        || wire.capture_recorded_at_ns > cutoff.unix_nanos()
        || wire.admission_class != CURRENT_RESEARCH_ADMISSION
        || !wire.current_research_eligible
        || wire.point_in_time_eligible
        || wire.backtest_eligible
        || wire.retrospective_training_eligible
        || wire.admission_reason != CURRENT_RESEARCH_REASON
    {
        return Err(ManifestCatalogError::CorruptCatalog);
    }
    Ok(MarketBarHistoryPublicationReceipt {
        receipt_digest,
        origin_manifest,
        origin_run_id: wire.origin_run_id,
        origin_artifact_id: wire.origin_artifact_id,
        origin_object_ordinal: wire.origin_object_ordinal,
        source_id: wire.source_id,
        binding_digest: nonzero_sha256(wire.binding_digest)?,
        capture_receipt_digest: nonzero_sha256(wire.capture_receipt_digest)?,
        capture_content_digest: nonzero_sha256(wire.capture_content_digest)?,
        capture_observation_digest: nonzero_sha256(wire.capture_observation_digest)?,
        capture_recorded_at: Timestamp::from_unix_nanos(wire.capture_recorded_at_ns),
        provider_dataset: wire.provider_dataset,
        instrument_id: wire.instrument_id,
        asset_class: wire.asset_class,
        instrument_revision_digest: nonzero_sha256(wire.instrument_revision_digest)?,
        admitted_plan_digest: nonzero_sha256(wire.admitted_plan_digest)?,
        provider_instrument_id: wire.provider_instrument_id,
        venue_id: wire.venue_id,
        feed: wire.feed,
        interval: wire.interval,
        adjustment: wire.adjustment,
        timestamp_basis: wire.timestamp_basis,
        session_kind: wire.session_kind,
        session_ruleset: wire.session_ruleset,
        graph_purpose: wire.graph_purpose,
        requested_start: Timestamp::from_unix_nanos(wire.requested_start_ns),
        requested_end: Timestamp::from_unix_nanos(wire.requested_end_ns),
        coverage_first: Timestamp::from_unix_nanos(wire.coverage_first_ns),
        coverage_last: Timestamp::from_unix_nanos(wire.coverage_last_ns),
        coverage_last_complete: Timestamp::from_unix_nanos(wire.coverage_last_complete_ns),
        expected_bar_count: usize::try_from(wire.expected_bar_count)
            .map_err(|_| ManifestCatalogError::CorruptCatalog)?,
        expected_provider_timestamps,
        expected_timestamp_set_digest: nonzero_sha256(wire.expected_timestamp_set_digest)?,
        bar_set_digest: nonzero_sha256(wire.bar_set_digest)?,
        completeness_evidence_digest: nonzero_sha256(wire.completeness_evidence_digest)?,
        market_bar_component_ordinal: wire.market_bar_component_ordinal,
        market_bar_component_content_digest: nonzero_sha256(
            wire.market_bar_component_content_digest,
        )?,
        market_bar_component_page_count: wire.market_bar_component_page_count,
        session_calendar_component_ordinal: wire.session_calendar_component_ordinal,
        session_calendar_component_content_digest: nonzero_sha256(
            wire.session_calendar_component_content_digest,
        )?,
        session_calendar_component_page_count: wire.session_calendar_component_page_count,
        currency: wire.currency,
        max_available_at,
        max_received_at,
        max_ingested_at,
        published_at,
    })
}

fn parse_digest_array(value: &[u8]) -> Result<[u8; 32], ManifestCatalogError> {
    value
        .try_into()
        .map_err(|_| ManifestCatalogError::CorruptCatalog)
}

fn parse_sha256(value: &[u8]) -> Result<Sha256Digest, ManifestCatalogError> {
    nonzero_sha256(parse_digest_array(value)?)
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ManifestCatalogError> {
    if cancellation.is_cancelled() {
        Err(ManifestCatalogError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ManifestCatalogError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
