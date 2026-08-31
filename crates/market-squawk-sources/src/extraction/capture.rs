//! Bounded, source-neutral provider response capture receipts.

mod market_event;

pub use market_event::{
    MAX_PROVIDER_MARKET_EVENT_BATCH_BYTES, MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS,
    PROVIDER_MARKET_EVENT_SCHEMA_VERSION, ProviderCompositeResponseEventBindingDigest,
    ProviderCompositeResponseEventRowCoordinate, ProviderEventMicrobatchBindingDigest,
    ProviderEventMicrobatchRowFrame, ProviderEventMicrobatchRowFrameEvidence,
    ProviderMarketEventBatch, ProviderMarketEventContentIdentity,
    ProviderMarketEventNativeLineageBatch, ProviderMarketEventNativeLineageRowEvidenceRef,
    ProviderPublicationBindingDigest, ProviderPublicationBindingKind,
    ProviderResponseMarketEventBindingDigest, ProviderResponseMarketEventRowFrameEvidence,
    SealedProviderCompositeResponseEventBinding, SealedProviderEventMicrobatchBinding,
    SealedProviderPublicationBinding, SealedProviderResponseMarketEventBinding,
    verify_provider_market_event_native_lineage_batch_evidence,
};

use std::mem::size_of;
use std::num::NonZeroU16;
use std::sync::Arc;

use bytes::Bytes;
use market_squawk_domain::{
    BarTimestampBasis, DigestAlgorithm, EvidenceDigest, InstrumentId, MarketBarAdjustment,
    MarketBarSessionKind, MetadataRevision, ProviderInstrumentId, SourceId, SourceIdentifier,
    Timestamp, VenueId,
};
use market_squawk_platform::{
    RawCaptureRecord, SealedResearchJournalSegmentClaim, SealedResearchJournalSegmentReceipt,
    SealedResearchJournalStore, SealedResearchJournalStoreError,
};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

use super::batch::{ExtractionBatch, ExtractionContentIdentity};
use super::contracts::MAX_EXTRACTION_RECORDS;
use super::native_lineage::{
    ProviderNativeLineageBatch, ProviderNativeLineageImplementation, ProviderNativeLineageSchema,
};
use crate::bounded::BoundedVec;

/// Maximum exact response pages admitted to one provider capture set.
pub const MAX_PROVIDER_CAPTURE_PAGES: usize = 64;
/// Maximum exact provider body bytes in one page/raw `MSJ1` frame.
pub const MAX_PROVIDER_CAPTURE_PAGE_BYTES: u64 =
    market_squawk_platform::RawCaptureRecord::MAX_LIVE_PAYLOAD_BYTES as u64;
/// Maximum aggregate exact provider body bytes admitted to one capture set.
///
/// This remains conservatively sealable below the 512 MiB `MSJ1` bound even under worst-case JSON
/// byte-array expansion in the committed raw-envelope wire.
pub const MAX_PROVIDER_CAPTURE_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum exact event frames admitted to one application-defined live microbatch.
pub const MAX_PROVIDER_EVENT_MICROBATCH_FRAMES: usize = 4_096;
/// Maximum aggregate source-frame bytes admitted to one live event microbatch.
pub const MAX_PROVIDER_EVENT_MICROBATCH_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum exact expected provider timestamps retained by one complete market-bar history graph.
pub const MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS: usize = 10_000;
/// Maximum architecture-independent timestamp bytes retained by that exact expected set.
pub const MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMP_BYTES: usize =
    MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS * size_of::<i64>();

/// Exact terminal condition observed after the last ordered response page.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderCaptureTerminalDisposition {
    /// The request contract defines one response and no pagination tokens are valid.
    StandaloneResponse,
    /// A paginated provider response explicitly returned no next-page token.
    ExhaustedWithoutNextPage,
    /// Every independently complete request in an ordered request graph was captured.
    CompleteRequestGraph,
}

impl ProviderCaptureTerminalDisposition {
    const fn tag(self) -> &'static [u8] {
        match self {
            Self::StandaloneResponse => b"standalone_response",
            Self::ExhaustedWithoutNextPage => b"exhausted_without_next_page",
            Self::CompleteRequestGraph => b"complete_request_graph",
        }
    }
}

/// Version-one semantic proof attached only to a complete market-bar history request graph.
///
/// The exact expected provider timestamp set is retained directly rather than hidden behind an
/// identifier. That lets publication validate requested-range completeness after restart without
/// reopening provider transport or trusting a sidecar.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteMarketBarHistoryV1 {
    requested_start: Timestamp,
    requested_end: Timestamp,
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_ruleset: SourceIdentifier,
    graph_purpose: SourceIdentifier,
    market_bar_component_ordinal: u16,
    session_calendar_component_ordinal: u16,
    expected_provider_timestamps: Box<[Timestamp]>,
    completeness_evidence: EvidenceDigest,
}

impl CompleteMarketBarHistoryV1 {
    /// Constructs a bounded, nonempty, strictly ordered exact expected-session set.
    #[allow(
        clippy::too_many_arguments,
        reason = "requested coordinates and stable series identity remain explicit"
    )]
    pub fn try_new(
        requested_start: Timestamp,
        requested_end: Timestamp,
        instrument_id: InstrumentId,
        instrument_revision_digest: EvidenceDigest,
        admitted_plan_digest: EvidenceDigest,
        provider_instrument_id: ProviderInstrumentId,
        venue_id: VenueId,
        feed: SourceIdentifier,
        interval: SourceIdentifier,
        adjustment: MarketBarAdjustment,
        timestamp_basis: BarTimestampBasis,
        session_kind: MarketBarSessionKind,
        session_ruleset: SourceIdentifier,
        graph_purpose: SourceIdentifier,
        market_bar_component_ordinal: u16,
        session_calendar_component_ordinal: u16,
        expected_provider_timestamps: Vec<Timestamp>,
        completeness_evidence: EvidenceDigest,
    ) -> Result<Self, ProviderCaptureError> {
        if requested_start >= requested_end
            || expected_provider_timestamps.is_empty()
            || expected_provider_timestamps.len() > MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS
            || expected_provider_timestamps
                .len()
                .checked_mul(size_of::<i64>())
                .is_none_or(|bytes| bytes > MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMP_BYTES)
            || completeness_evidence.algorithm() != DigestAlgorithm::Sha256
            || completeness_evidence.bytes() == [0; 32]
            || instrument_revision_digest.algorithm() != DigestAlgorithm::Sha256
            || instrument_revision_digest.bytes() == [0; 32]
            || admitted_plan_digest.algorithm() != DigestAlgorithm::Sha256
            || admitted_plan_digest.bytes() == [0; 32]
            || market_bar_component_ordinal != 0
            || session_calendar_component_ordinal != 1
        {
            return Err(ProviderCaptureError::InvalidMarketBarHistorySemantics);
        }
        let mut previous = None;
        for timestamp in &expected_provider_timestamps {
            if *timestamp < requested_start
                || *timestamp > requested_end
                || previous.is_some_and(|prior| prior >= *timestamp)
            {
                return Err(ProviderCaptureError::InvalidMarketBarHistorySemantics);
            }
            previous = Some(*timestamp);
        }
        Ok(Self {
            requested_start,
            requested_end,
            instrument_id,
            instrument_revision_digest,
            admitted_plan_digest,
            provider_instrument_id,
            venue_id,
            feed,
            interval,
            adjustment,
            timestamp_basis,
            session_kind,
            session_ruleset,
            graph_purpose,
            market_bar_component_ordinal,
            session_calendar_component_ordinal,
            expected_provider_timestamps: expected_provider_timestamps.into_boxed_slice(),
            completeness_evidence,
        })
    }

    /// Returns the inclusive provider request start.
    pub const fn requested_start(&self) -> Timestamp {
        self.requested_start
    }

    /// Returns the inclusive provider request end.
    pub const fn requested_end(&self) -> Timestamp {
        self.requested_end
    }

    /// Returns the canonical instrument expected in every bar.
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the exact canonical instrument-definition revision used by plan admission.
    pub const fn instrument_revision_digest(&self) -> EvidenceDigest {
        self.instrument_revision_digest
    }

    /// Returns the exact immutable application plan that admitted this capture.
    pub const fn admitted_plan_digest(&self) -> EvidenceDigest {
        self.admitted_plan_digest
    }

    /// Returns the exact provider-native instrument identity.
    pub const fn provider_instrument_id(&self) -> &ProviderInstrumentId {
        &self.provider_instrument_id
    }

    /// Returns the exact venue expected in every bar.
    pub const fn venue_id(&self) -> &VenueId {
        &self.venue_id
    }

    /// Returns the exact provider feed.
    pub const fn feed(&self) -> &SourceIdentifier {
        &self.feed
    }

    /// Returns the exact provider bar interval.
    pub const fn interval(&self) -> &SourceIdentifier {
        &self.interval
    }

    /// Returns the corporate-action adjustment contract.
    pub const fn adjustment(&self) -> MarketBarAdjustment {
        self.adjustment
    }

    /// Returns which exact boundary anchors provider timestamps.
    pub const fn timestamp_basis(&self) -> BarTimestampBasis {
        self.timestamp_basis
    }

    /// Returns the source-neutral session class shared by every expected bar.
    pub const fn session_kind(&self) -> MarketBarSessionKind {
        self.session_kind
    }

    /// Returns the exact session-ruleset identity shared by every expected bar.
    pub const fn session_ruleset(&self) -> &SourceIdentifier {
        &self.session_ruleset
    }

    /// Returns the versioned complete-request-graph composition purpose.
    pub const fn graph_purpose(&self) -> &SourceIdentifier {
        &self.graph_purpose
    }

    /// Returns the request-graph component containing the exact market-bar responses.
    pub const fn market_bar_component_ordinal(&self) -> u16 {
        self.market_bar_component_ordinal
    }

    /// Returns the request-graph component containing the session-calendar response.
    pub const fn session_calendar_component_ordinal(&self) -> u16 {
        self.session_calendar_component_ordinal
    }

    /// Returns every expected provider timestamp in strict ascending order.
    pub fn expected_provider_timestamps(&self) -> &[Timestamp] {
        &self.expected_provider_timestamps
    }

    /// Returns provider/calendar evidence that established exact set equality.
    pub const fn completeness_evidence(&self) -> EvidenceDigest {
        self.completeness_evidence
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CompleteMarketBarHistoryV1Wire {
    requested_start: Timestamp,
    requested_end: Timestamp,
    instrument_id: InstrumentId,
    instrument_revision_digest: EvidenceDigest,
    admitted_plan_digest: EvidenceDigest,
    provider_instrument_id: ProviderInstrumentId,
    venue_id: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_ruleset: SourceIdentifier,
    graph_purpose: SourceIdentifier,
    market_bar_component_ordinal: u16,
    session_calendar_component_ordinal: u16,
    expected_provider_timestamps: BoundedVec<Timestamp, MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS>,
    completeness_evidence: EvidenceDigest,
}

impl<'de> Deserialize<'de> for CompleteMarketBarHistoryV1 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = CompleteMarketBarHistoryV1Wire::deserialize(deserializer)?;
        Self::try_new(
            wire.requested_start,
            wire.requested_end,
            wire.instrument_id,
            wire.instrument_revision_digest,
            wire.admitted_plan_digest,
            wire.provider_instrument_id,
            wire.venue_id,
            wire.feed,
            wire.interval,
            wire.adjustment,
            wire.timestamp_basis,
            wire.session_kind,
            wire.session_ruleset,
            wire.graph_purpose,
            wire.market_bar_component_ordinal,
            wire.session_calendar_component_ordinal,
            wire.expected_provider_timestamps.into_vec(),
            wire.completeness_evidence,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Closed semantic binding carried by a complete provider request graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", content = "receipt", rename_all = "snake_case")]
pub enum ProviderCaptureSemanticBinding {
    /// Exact requested and expected daily market-bar history coordinates.
    CompleteMarketBarHistoryV1(CompleteMarketBarHistoryV1),
}

/// One independently complete request component retained inside an ordered request graph.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCaptureRequestGraphComponent {
    ordinal: u16,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    request_set_identity: EvidenceDigest,
    terminal: ProviderCaptureTerminalDisposition,
    first_page_ordinal: u16,
    page_count: NonZeroU16,
    total_body_bytes: u64,
    content_digest: EvidenceDigest,
    observation_digest: EvidenceDigest,
}

impl ProviderCaptureRequestGraphComponent {
    /// Returns this component's contiguous zero-based graph ordinal.
    pub const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Returns the exact registered source that produced this component.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source metadata revision that governed this component.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the exact provider dataset addressed by this component.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the complete request semantics identity of this component.
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }

    /// Returns how this independently complete component terminated.
    pub const fn terminal(&self) -> ProviderCaptureTerminalDisposition {
        self.terminal
    }

    /// Returns the first flattened page ordinal owned by this component.
    pub const fn first_page_ordinal(&self) -> u16 {
        self.first_page_ordinal
    }

    /// Returns the nonzero number of flattened pages owned by this component.
    pub const fn page_count(&self) -> NonZeroU16 {
        self.page_count
    }

    /// Returns this component's checked provider-body byte total.
    pub const fn total_body_bytes(&self) -> u64 {
        self.total_body_bytes
    }

    /// Returns this component's stable content identity.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns this component's content-and-receive-time identity.
    pub const fn observation_digest(&self) -> EvidenceDigest {
        self.observation_digest
    }
}

/// One exact ordered provider response page, without retaining its raw body in row metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCapturePageReceipt {
    ordinal: u16,
    request_identity: EvidenceDigest,
    request_page_token_digest: Option<EvidenceDigest>,
    response_next_page_token_digest: Option<EvidenceDigest>,
    http_status: u16,
    body_bytes: u64,
    body_digest: EvidenceDigest,
    received_at: Timestamp,
}

impl ProviderCapturePageReceipt {
    /// Constructs one bounded page receipt from already verified exact response facts.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact page evidence remains explicit"
    )]
    pub fn try_new(
        ordinal: u16,
        request_identity: EvidenceDigest,
        request_page_token_digest: Option<EvidenceDigest>,
        response_next_page_token_digest: Option<EvidenceDigest>,
        http_status: u16,
        body_bytes: u64,
        body_digest: EvidenceDigest,
        received_at: Timestamp,
    ) -> Result<Self, ProviderCaptureError> {
        if usize::from(ordinal) >= MAX_PROVIDER_CAPTURE_PAGES {
            return Err(ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_CAPTURE_PAGES,
            });
        }
        if !(200..=299).contains(&http_status) {
            return Err(ProviderCaptureError::UnsuccessfulHttpStatus(http_status));
        }
        if body_bytes == 0 || body_bytes > MAX_PROVIDER_CAPTURE_PAGE_BYTES {
            return Err(ProviderCaptureError::ByteLimitExceeded {
                max: MAX_PROVIDER_CAPTURE_PAGE_BYTES,
            });
        }
        require_sha256_identity(request_identity)?;
        require_sha256_identity(body_digest)?;
        if let Some(digest) = request_page_token_digest {
            require_sha256_identity(digest)?;
        }
        if let Some(digest) = response_next_page_token_digest {
            require_sha256_identity(digest)?;
        }
        Ok(Self {
            ordinal,
            request_identity,
            request_page_token_digest,
            response_next_page_token_digest,
            http_status,
            body_bytes,
            body_digest,
            received_at,
        })
    }

    /// Returns the contiguous zero-based page ordinal.
    pub const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Returns the digest of the exact authorized request semantics for this page.
    pub const fn request_identity(&self) -> EvidenceDigest {
        self.request_identity
    }

    /// Returns the digest of the page token sent with this exact request.
    pub const fn request_page_token_digest(&self) -> Option<EvidenceDigest> {
        self.request_page_token_digest
    }

    /// Returns the digest of the provider-returned next token, when present.
    pub const fn response_next_page_token_digest(&self) -> Option<EvidenceDigest> {
        self.response_next_page_token_digest
    }

    /// Returns the accepted provider HTTP status.
    pub const fn http_status(&self) -> u16 {
        self.http_status
    }

    /// Returns the exact provider body byte count.
    pub const fn body_bytes(&self) -> u64 {
        self.body_bytes
    }

    /// Returns the SHA-256 digest of the exact provider body.
    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }

    /// Returns the socket-boundary time at which the complete body was observed.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    fn with_ordinal(&self, ordinal: u16) -> Self {
        Self {
            ordinal,
            request_identity: self.request_identity,
            request_page_token_digest: self.request_page_token_digest,
            response_next_page_token_digest: self.response_next_page_token_digest,
            http_status: self.http_status,
            body_bytes: self.body_bytes,
            body_digest: self.body_digest,
            received_at: self.received_at,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCapturePageReceiptWire {
    ordinal: u16,
    request_identity: EvidenceDigest,
    request_page_token_digest: Option<EvidenceDigest>,
    response_next_page_token_digest: Option<EvidenceDigest>,
    http_status: u16,
    body_bytes: u64,
    body_digest: EvidenceDigest,
    received_at: Timestamp,
}

impl<'de> Deserialize<'de> for ProviderCapturePageReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderCapturePageReceiptWire::deserialize(deserializer)?;
        Self::try_new(
            wire.ordinal,
            wire.request_identity,
            wire.request_page_token_digest,
            wire.response_next_page_token_digest,
            wire.http_status,
            wire.body_bytes,
            wire.body_digest,
            wire.received_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Completed bounded provider capture set with stable content and observation identities.
///
/// `content_digest` excludes receive time and every local storage coordinate. It is stable across
/// exact retries of the same provider request/pages. `observation_digest` adds every receive time
/// while still remaining independent of a particular physical store.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCaptureSetReceipt {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    request_set_identity: EvidenceDigest,
    terminal: ProviderCaptureTerminalDisposition,
    total_body_bytes: u64,
    pages: Box<[ProviderCapturePageReceipt]>,
    request_graph_components: Box<[ProviderCaptureRequestGraphComponent]>,
    semantic_binding: Option<ProviderCaptureSemanticBinding>,
    content_digest: EvidenceDigest,
    observation_digest: EvidenceDigest,
}

impl ProviderCaptureSetReceipt {
    /// Validates ordering, page-token continuity, terminal state, and aggregate bounds.
    pub fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        request_set_identity: EvidenceDigest,
        terminal: ProviderCaptureTerminalDisposition,
        pages: Vec<ProviderCapturePageReceipt>,
    ) -> Result<Self, ProviderCaptureError> {
        Self::try_new_with_request_graph(
            source_id,
            metadata_revision,
            dataset,
            request_set_identity,
            terminal,
            pages,
            Vec::new(),
            None,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "request-graph framing remains explicit"
    )]
    fn try_new_with_request_graph(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        request_set_identity: EvidenceDigest,
        terminal: ProviderCaptureTerminalDisposition,
        pages: Vec<ProviderCapturePageReceipt>,
        request_graph_components: Vec<ProviderCaptureRequestGraphComponent>,
        semantic_binding: Option<ProviderCaptureSemanticBinding>,
    ) -> Result<Self, ProviderCaptureError> {
        require_sha256_identity(request_set_identity)?;
        if pages.is_empty() {
            return Err(ProviderCaptureError::EmptyCaptureSet);
        }
        if pages.len() > MAX_PROVIDER_CAPTURE_PAGES {
            return Err(ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_CAPTURE_PAGES,
            });
        }
        let mut total_body_bytes = 0_u64;
        for (expected_ordinal, page) in pages.iter().enumerate() {
            let expected_ordinal = u16::try_from(expected_ordinal).map_err(|_| {
                ProviderCaptureError::PageLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_PAGES,
                }
            })?;
            if page.ordinal != expected_ordinal {
                return Err(ProviderCaptureError::PageOrderingInvalid);
            }
            total_body_bytes = total_body_bytes.checked_add(page.body_bytes).ok_or(
                ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_BYTES,
                },
            )?;
            if total_body_bytes > MAX_PROVIDER_CAPTURE_BYTES {
                return Err(ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_BYTES,
                });
            }
        }
        match terminal {
            ProviderCaptureTerminalDisposition::StandaloneResponse
            | ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage => {
                if !request_graph_components.is_empty() || semantic_binding.is_some() {
                    return Err(ProviderCaptureError::RequestGraphInvalid);
                }
                validate_page_chain(terminal, &pages)?;
            }
            ProviderCaptureTerminalDisposition::CompleteRequestGraph => {
                validate_request_graph_components(
                    &source_id,
                    &metadata_revision,
                    &pages,
                    &request_graph_components,
                )?;
                if let Some(semantic_binding) = semantic_binding.as_ref() {
                    validate_semantic_graph_binding(
                        semantic_binding,
                        &source_id,
                        &metadata_revision,
                        &dataset,
                        request_set_identity,
                        &request_graph_components,
                    )?;
                }
            }
        }
        let content_digest = if terminal == ProviderCaptureTerminalDisposition::CompleteRequestGraph
        {
            request_graph_content_digest(
                &source_id,
                &metadata_revision,
                &dataset,
                request_set_identity,
                total_body_bytes,
                &pages,
                &request_graph_components,
                semantic_binding.as_ref(),
            )
        } else {
            capture_content_digest(
                &source_id,
                &metadata_revision,
                &dataset,
                request_set_identity,
                terminal,
                total_body_bytes,
                &pages,
            )
        };
        let observation_digest =
            if terminal == ProviderCaptureTerminalDisposition::CompleteRequestGraph {
                request_graph_observation_digest(content_digest, &request_graph_components)
            } else {
                capture_observation_digest(content_digest, &pages)
            };
        Ok(Self {
            source_id,
            metadata_revision,
            dataset,
            request_set_identity,
            terminal,
            total_body_bytes,
            pages: pages.into_boxed_slice(),
            request_graph_components: request_graph_components.into_boxed_slice(),
            semantic_binding,
            content_digest,
            observation_digest,
        })
    }

    /// Returns exact source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns exact source metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns exact provider dataset identity.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the identity of the complete provider request/plan semantics.
    pub const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }

    /// Returns the exact provider terminal disposition.
    pub const fn terminal(&self) -> ProviderCaptureTerminalDisposition {
        self.terminal
    }

    /// Returns the checked aggregate provider body byte count.
    pub const fn total_body_bytes(&self) -> u64 {
        self.total_body_bytes
    }

    /// Returns exact ordered page receipts.
    pub fn pages(&self) -> &[ProviderCapturePageReceipt] {
        &self.pages
    }

    /// Returns ordered component framing for a complete request graph, otherwise an empty slice.
    pub fn request_graph_components(&self) -> &[ProviderCaptureRequestGraphComponent] {
        &self.request_graph_components
    }

    /// Returns the typed complete-request semantic proof, when this graph carries one.
    pub const fn semantic_binding(&self) -> Option<&ProviderCaptureSemanticBinding> {
        self.semantic_binding.as_ref()
    }

    /// Returns the stable provider-content identity, excluding receive/storage facts.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the identity that additionally binds every exact receive time.
    pub const fn observation_digest(&self) -> EvidenceDigest {
        self.observation_digest
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderCaptureSetReceiptWire {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    request_set_identity: EvidenceDigest,
    terminal: ProviderCaptureTerminalDisposition,
    total_body_bytes: u64,
    pages: BoundedVec<ProviderCapturePageReceipt, MAX_PROVIDER_CAPTURE_PAGES>,
    request_graph_components:
        BoundedVec<ProviderCaptureRequestGraphComponent, MAX_PROVIDER_CAPTURE_PAGES>,
    semantic_binding: Option<ProviderCaptureSemanticBinding>,
    content_digest: EvidenceDigest,
    observation_digest: EvidenceDigest,
}

impl<'de> Deserialize<'de> for ProviderCaptureSetReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderCaptureSetReceiptWire::deserialize(deserializer)?;
        let rebuilt = Self::try_new_with_request_graph(
            wire.source_id,
            wire.metadata_revision,
            wire.dataset,
            wire.request_set_identity,
            wire.terminal,
            wire.pages.into_vec(),
            wire.request_graph_components.into_vec(),
            wire.semantic_binding,
        )
        .map_err(serde::de::Error::custom)?;
        if rebuilt.total_body_bytes != wire.total_body_bytes
            || rebuilt.content_digest != wire.content_digest
            || rebuilt.observation_digest != wire.observation_digest
        {
            return Err(serde::de::Error::custom(
                ProviderCaptureError::ReceiptBindingMismatch,
            ));
        }
        Ok(rebuilt)
    }
}

/// Complete in-memory provider capture ready for durable sealing.
///
/// This value owns only the source-neutral receipt and the exact bounded raw response records.
/// Provider credentials, request clients, rate permits, and account/session authorities cannot be
/// attached to it. Construction proves the receipt and raw bytes describe the same ordered page
/// set before either can cross the durable storage boundary.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderCaptureMaterial {
    receipt: ProviderCaptureSetReceipt,
    records: Box<[RawCaptureRecord]>,
}

impl ProviderCaptureMaterial {
    /// Binds one completed capture receipt to its exact ordered raw response records.
    pub fn try_new(
        receipt: ProviderCaptureSetReceipt,
        records: Vec<RawCaptureRecord>,
    ) -> Result<Self, ProviderCaptureError> {
        if records.len() != receipt.pages().len() {
            return Err(ProviderCaptureError::MaterialBindingMismatch);
        }
        let mut total_body_bytes = 0_u64;
        let mut previous_received_at = None;
        for (page, record) in receipt.pages().iter().zip(&records) {
            let expected_source =
                if receipt.terminal == ProviderCaptureTerminalDisposition::CompleteRequestGraph {
                    let component = request_graph_component_for_page(
                        &receipt.request_graph_components,
                        page.ordinal,
                    )
                    .ok_or(ProviderCaptureError::MaterialBindingMismatch)?;
                    if component.first_page_ordinal == page.ordinal {
                        previous_received_at = None;
                    }
                    component.source_id.as_str()
                } else {
                    receipt.source_id().as_str()
                };
            let expected_sequence = Some(u64::from(page.ordinal()));
            let received_at = record
                .received_at()
                .timestamp_nanos_opt()
                .ok_or(ProviderCaptureError::MaterialBindingMismatch)?;
            let payload_bytes = u64::try_from(record.payload().len())
                .map_err(|_| ProviderCaptureError::MaterialBindingMismatch)?;
            let payload_digest = EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(record.payload()).into(),
            );
            total_body_bytes = total_body_bytes.checked_add(payload_bytes).ok_or(
                ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_BYTES,
                },
            )?;
            if total_body_bytes > MAX_PROVIDER_CAPTURE_BYTES {
                return Err(ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_BYTES,
                });
            }
            if previous_received_at.is_some_and(|previous| previous > page.received_at())
                || record.source() != expected_source
                || record.event_id().is_nil()
                || record.connection_id().is_nil()
                || record.source_sequence() != expected_sequence
                || payload_bytes != page.body_bytes()
                || payload_digest != page.body_digest()
                || received_at != page.received_at().unix_nanos()
            {
                return Err(ProviderCaptureError::MaterialBindingMismatch);
            }
            previous_received_at = Some(page.received_at());
        }
        if total_body_bytes != receipt.total_body_bytes() {
            return Err(ProviderCaptureError::MaterialBindingMismatch);
        }
        Ok(Self {
            receipt,
            records: records.into_boxed_slice(),
        })
    }

    /// Combines independently complete captures into one ordered complete request graph.
    ///
    /// The caller supplies the graph owner's source authority, provider dataset, and request-set
    /// identity for the complete graph. The first component must share that owner authority;
    /// later independently governed components retain their own source and metadata revisions.
    /// Each component remains independently framed by its original dataset, request-set identity,
    /// terminal condition, page-token evidence, content digest, and observation digest. Flattened
    /// pages and raw records receive fresh contiguous ordinals solely for the one sealed segment;
    /// exact provider body bytes, request identities, receive times, and local event/connection
    /// identities are preserved.
    ///
    /// # Errors
    ///
    /// Rejects fewer than two components, nested request graphs, a first component that does not
    /// match the graph owner, invalid graph identity, nonmonotonic request ordering, and aggregate
    /// page/byte bounds above the existing capture-set ceilings.
    pub fn try_combine_request_graph(
        owner_source_id: SourceId,
        owner_metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        request_set_identity: EvidenceDigest,
        components: Vec<Self>,
    ) -> Result<Self, ProviderCaptureError> {
        Self::try_combine_request_graph_inner(
            owner_source_id,
            owner_metadata_revision,
            dataset,
            request_set_identity,
            components,
            None,
        )
    }

    /// Combines a complete request graph with one typed, hash-bound semantic proof.
    ///
    /// The semantic is retained inside the canonical capture receipt and therefore follows the
    /// same raw-storage, run-lineage, generation-lineage, and restart verification path as its
    /// exact provider response components. The graph request identity is derived here from the
    /// semantic's versioned purpose and the exact ordered component receipts; callers cannot
    /// supply or accidentally drift that authority-critical hash.
    pub fn try_combine_request_graph_with_semantic(
        owner_source_id: SourceId,
        owner_metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        components: Vec<Self>,
        semantic_binding: ProviderCaptureSemanticBinding,
    ) -> Result<Self, ProviderCaptureError> {
        if components.len() < 2 {
            return Err(ProviderCaptureError::RequestGraphInvalid);
        }
        if components.len() > MAX_PROVIDER_CAPTURE_PAGES {
            return Err(ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_CAPTURE_PAGES,
            });
        }
        let purpose = match &semantic_binding {
            ProviderCaptureSemanticBinding::CompleteMarketBarHistoryV1(binding) => {
                binding.graph_purpose()
            }
        };
        let request_set_identity = semantic_material_request_graph_identity(
            &owner_source_id,
            &owner_metadata_revision,
            &dataset,
            purpose,
            &components,
        );
        Self::try_combine_request_graph_inner(
            owner_source_id,
            owner_metadata_revision,
            dataset,
            request_set_identity,
            components,
            Some(semantic_binding),
        )
    }

    fn try_combine_request_graph_inner(
        owner_source_id: SourceId,
        owner_metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        request_set_identity: EvidenceDigest,
        components: Vec<Self>,
        semantic_binding: Option<ProviderCaptureSemanticBinding>,
    ) -> Result<Self, ProviderCaptureError> {
        require_sha256_identity(request_set_identity)?;
        if components.len() < 2 {
            return Err(ProviderCaptureError::RequestGraphInvalid);
        }
        if components.len() > MAX_PROVIDER_CAPTURE_PAGES {
            return Err(ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_CAPTURE_PAGES,
            });
        }
        let first = components
            .first()
            .ok_or(ProviderCaptureError::RequestGraphInvalid)?;
        if first.receipt.source_id != owner_source_id
            || first.receipt.metadata_revision != owner_metadata_revision
        {
            return Err(ProviderCaptureError::RequestGraphComponentMismatch);
        }
        let total_page_count = components.iter().try_fold(0_usize, |total, component| {
            if component.receipt.terminal
                == ProviderCaptureTerminalDisposition::CompleteRequestGraph
                || !component.receipt.request_graph_components.is_empty()
            {
                return Err(ProviderCaptureError::NestedRequestGraph);
            }
            total.checked_add(component.receipt.pages.len()).ok_or(
                ProviderCaptureError::PageLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_PAGES,
                },
            )
        })?;
        if total_page_count > MAX_PROVIDER_CAPTURE_PAGES {
            return Err(ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_CAPTURE_PAGES,
            });
        }

        let mut pages = Vec::new();
        pages
            .try_reserve_exact(total_page_count)
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(total_page_count)
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;
        let mut framing = Vec::new();
        framing
            .try_reserve_exact(components.len())
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;

        for (component_ordinal, component) in components.into_iter().enumerate() {
            let Self {
                receipt,
                records: component_records,
            } = component;
            let first_page_ordinal = u16::try_from(pages.len()).map_err(|_| {
                ProviderCaptureError::PageLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_PAGES,
                }
            })?;
            let page_count = NonZeroU16::new(u16::try_from(receipt.pages.len()).map_err(|_| {
                ProviderCaptureError::PageLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_PAGES,
                }
            })?)
            .ok_or(ProviderCaptureError::RequestGraphInvalid)?;
            framing.push(ProviderCaptureRequestGraphComponent {
                ordinal: u16::try_from(component_ordinal).map_err(|_| {
                    ProviderCaptureError::PageLimitExceeded {
                        max: MAX_PROVIDER_CAPTURE_PAGES,
                    }
                })?,
                source_id: receipt.source_id.clone(),
                metadata_revision: receipt.metadata_revision.clone(),
                dataset: receipt.dataset.clone(),
                request_set_identity: receipt.request_set_identity,
                terminal: receipt.terminal,
                first_page_ordinal,
                page_count,
                total_body_bytes: receipt.total_body_bytes,
                content_digest: receipt.content_digest,
                observation_digest: receipt.observation_digest,
            });
            for (page, record) in receipt.pages.iter().zip(component_records.into_vec()) {
                let ordinal = u16::try_from(pages.len()).map_err(|_| {
                    ProviderCaptureError::PageLimitExceeded {
                        max: MAX_PROVIDER_CAPTURE_PAGES,
                    }
                })?;
                pages.push(page.with_ordinal(ordinal));
                records.push(
                    RawCaptureRecord::try_new_live(
                        record.event_id(),
                        Arc::from(record.source()),
                        record.connection_id(),
                        Some(u64::from(ordinal)),
                        record.exchange_at(),
                        record.received_at(),
                        Bytes::copy_from_slice(record.payload()),
                    )
                    .map_err(|_| ProviderCaptureError::MaterialBindingMismatch)?,
                );
            }
        }
        let receipt = ProviderCaptureSetReceipt::try_new_with_request_graph(
            owner_source_id,
            owner_metadata_revision,
            dataset,
            request_set_identity,
            ProviderCaptureTerminalDisposition::CompleteRequestGraph,
            pages,
            framing,
            semantic_binding,
        )?;
        Self::try_new(receipt, records)
    }

    /// Returns the exact completed provider capture receipt.
    pub const fn receipt(&self) -> &ProviderCaptureSetReceipt {
        &self.receipt
    }

    /// Returns the exact ordered raw provider response records.
    pub fn records(&self) -> &[RawCaptureRecord] {
        &self.records
    }

    /// Splits this one-use material into a whole-capture expectation and its physical seal request.
    pub fn into_whole_seal_parts(
        self,
    ) -> (ProviderCaptureSealExpectation, ProviderCaptureSealRequest) {
        self.into_seal_parts(ProviderCaptureSealDisposition::Whole)
    }

    /// Splits a complete request graph into a component-token expectation and physical seal request.
    pub fn into_component_seal_parts(
        self,
    ) -> Result<(ProviderCaptureSealExpectation, ProviderCaptureSealRequest), ProviderCaptureError>
    {
        if self.receipt.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
            || self.receipt.request_graph_components().is_empty()
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(self.into_seal_parts(ProviderCaptureSealDisposition::Components))
    }

    fn into_seal_parts(
        self,
        disposition: ProviderCaptureSealDisposition,
    ) -> (ProviderCaptureSealExpectation, ProviderCaptureSealRequest) {
        let witness = Arc::new(ProviderCaptureSealWitness(()));
        (
            ProviderCaptureSealExpectation {
                witness: Arc::clone(&witness),
                disposition,
            },
            ProviderCaptureSealRequest {
                witness,
                payload: ProviderCaptureSealPayload::ResponseSet {
                    receipt: self.receipt,
                    records: self.records,
                },
            },
        )
    }
}

/// Exact source event evidence for one frame in an application-defined live microbatch.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEventMicrobatchFrameReceipt {
    ordinal: u16,
    event_id: [u8; 16],
    connection_id: [u8; 16],
    source_sequence: Option<u64>,
    exchange_at: Option<Timestamp>,
    received_at: Timestamp,
    payload_bytes: u64,
    payload_digest: EvidenceDigest,
}

impl ProviderEventMicrobatchFrameReceipt {
    /// Returns the exact contiguous physical-order ordinal inside this microbatch.
    pub const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Returns the exact locally assigned event UUID bytes retained in the raw envelope.
    pub const fn event_id(&self) -> [u8; 16] {
        self.event_id
    }

    /// Returns the exact connection UUID bytes retained in the raw envelope.
    pub const fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    /// Returns the provider/source sequence when the stream supplied one.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }

    /// Returns the source-authored event time when the stream supplied one.
    pub const fn exchange_at(&self) -> Option<Timestamp> {
        self.exchange_at
    }

    /// Returns the exact socket-boundary receive time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the exact provider frame byte count.
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Returns the SHA-256 digest of the exact provider frame bytes.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    fn validate(&self, expected_ordinal: usize) -> Result<(), ProviderCaptureError> {
        if usize::from(self.ordinal) != expected_ordinal
            || self.event_id == [0; 16]
            || self.connection_id == [0; 16]
            || self.payload_bytes == 0
            || self.payload_bytes > MAX_PROVIDER_CAPTURE_PAGE_BYTES
        {
            return Err(ProviderCaptureError::MaterialBindingMismatch);
        }
        require_sha256_identity(self.payload_digest)
    }
}

/// Bounded evidence for one application-defined unit of ordered live source events.
///
/// This receipt deliberately has no HTTP status, pagination token, terminal disposition, or
/// provider-completeness claim. Its boundary is chosen by application scheduling and is bound into
/// `stream_identity`; frame order is exact evidence, not an assertion that the source stream was
/// complete before or after the microbatch.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderEventMicrobatchReceipt {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    stream_identity: SourceIdentifier,
    total_payload_bytes: u64,
    frames: Box<[ProviderEventMicrobatchFrameReceipt]>,
    content_digest: EvidenceDigest,
    observation_digest: EvidenceDigest,
}

impl ProviderEventMicrobatchReceipt {
    fn try_from_parts(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        stream_identity: SourceIdentifier,
        frames: Vec<ProviderEventMicrobatchFrameReceipt>,
    ) -> Result<Self, ProviderCaptureError> {
        if frames.is_empty() {
            return Err(ProviderCaptureError::EmptyCaptureSet);
        }
        if frames.len() > MAX_PROVIDER_EVENT_MICROBATCH_FRAMES {
            return Err(ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_EVENT_MICROBATCH_FRAMES,
            });
        }
        let mut total_payload_bytes = 0_u64;
        for (ordinal, frame) in frames.iter().enumerate() {
            frame.validate(ordinal)?;
            total_payload_bytes = total_payload_bytes.checked_add(frame.payload_bytes).ok_or(
                ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_EVENT_MICROBATCH_BYTES,
                },
            )?;
            if total_payload_bytes > MAX_PROVIDER_EVENT_MICROBATCH_BYTES {
                return Err(ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_EVENT_MICROBATCH_BYTES,
                });
            }
        }
        let content_digest = event_microbatch_content_digest(
            &source_id,
            &metadata_revision,
            &dataset,
            &stream_identity,
            total_payload_bytes,
            &frames,
        );
        let observation_digest = event_microbatch_observation_digest(content_digest, &frames);
        Ok(Self {
            source_id,
            metadata_revision,
            dataset,
            stream_identity,
            total_payload_bytes,
            frames: frames.into_boxed_slice(),
            content_digest,
            observation_digest,
        })
    }

    /// Returns the exact source authority identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact source-metadata revision used to interpret these frames.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the canonical provider dataset addressed by this stream.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the exact adapter/application-defined stream boundary identity.
    pub const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }

    /// Returns the checked aggregate exact provider-frame bytes.
    pub const fn total_payload_bytes(&self) -> u64 {
        self.total_payload_bytes
    }

    /// Returns the ordered exact frame receipts.
    pub fn frames(&self) -> &[ProviderEventMicrobatchFrameReceipt] {
        &self.frames
    }

    /// Returns stable source-content identity excluding local event/connection/receive evidence.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns identity over content plus exact event, connection, and receive evidence.
    pub const fn observation_digest(&self) -> EvidenceDigest {
        self.observation_digest
    }

    fn validate(&self) -> Result<(), ProviderCaptureError> {
        if self.frames.is_empty() {
            return Err(ProviderCaptureError::EmptyCaptureSet);
        }
        if self.frames.len() > MAX_PROVIDER_EVENT_MICROBATCH_FRAMES {
            return Err(ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_EVENT_MICROBATCH_FRAMES,
            });
        }
        let mut total_payload_bytes = 0_u64;
        for (ordinal, frame) in self.frames.iter().enumerate() {
            frame.validate(ordinal)?;
            total_payload_bytes = total_payload_bytes.checked_add(frame.payload_bytes).ok_or(
                ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_EVENT_MICROBATCH_BYTES,
                },
            )?;
            if total_payload_bytes > MAX_PROVIDER_EVENT_MICROBATCH_BYTES {
                return Err(ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_EVENT_MICROBATCH_BYTES,
                });
            }
        }
        let content_digest = event_microbatch_content_digest(
            &self.source_id,
            &self.metadata_revision,
            &self.dataset,
            &self.stream_identity,
            total_payload_bytes,
            &self.frames,
        );
        let observation_digest = event_microbatch_observation_digest(content_digest, &self.frames);
        if total_payload_bytes != self.total_payload_bytes
            || content_digest != self.content_digest
            || observation_digest != self.observation_digest
        {
            return Err(ProviderCaptureError::ReceiptBindingMismatch);
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderEventMicrobatchReceiptWire {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    dataset: SourceIdentifier,
    stream_identity: SourceIdentifier,
    total_payload_bytes: u64,
    frames: crate::bounded::BoundedVec<
        ProviderEventMicrobatchFrameReceipt,
        MAX_PROVIDER_EVENT_MICROBATCH_FRAMES,
    >,
    content_digest: EvidenceDigest,
    observation_digest: EvidenceDigest,
}

impl<'de> Deserialize<'de> for ProviderEventMicrobatchReceipt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderEventMicrobatchReceiptWire::deserialize(deserializer)?;
        let rebuilt = Self::try_from_parts(
            wire.source_id,
            wire.metadata_revision,
            wire.dataset,
            wire.stream_identity,
            wire.frames.into_vec(),
        )
        .map_err(serde::de::Error::custom)?;
        if rebuilt.total_payload_bytes != wire.total_payload_bytes
            || rebuilt.content_digest != wire.content_digest
            || rebuilt.observation_digest != wire.observation_digest
        {
            return Err(serde::de::Error::custom(
                ProviderCaptureError::ReceiptBindingMismatch,
            ));
        }
        Ok(rebuilt)
    }
}

/// Complete bounded live-event microbatch ready for the common durable seal operation.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderEventMicrobatchMaterial {
    receipt: ProviderEventMicrobatchReceipt,
    records: Box<[RawCaptureRecord]>,
}

impl ProviderEventMicrobatchMaterial {
    /// Binds exact source frames to a non-HTTP application-defined microbatch receipt.
    pub fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        dataset: SourceIdentifier,
        stream_identity: SourceIdentifier,
        records: Vec<RawCaptureRecord>,
    ) -> Result<Self, ProviderCaptureError> {
        if records.is_empty() {
            return Err(ProviderCaptureError::EmptyCaptureSet);
        }
        if records.len() > MAX_PROVIDER_EVENT_MICROBATCH_FRAMES {
            return Err(ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_EVENT_MICROBATCH_FRAMES,
            });
        }
        let mut frames = Vec::new();
        frames
            .try_reserve_exact(records.len())
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;
        for (ordinal, record) in records.iter().enumerate() {
            if record.source() != source_id.as_str() {
                return Err(ProviderCaptureError::MaterialBindingMismatch);
            }
            let payload_bytes = u64::try_from(record.payload().len())
                .map_err(|_| ProviderCaptureError::MaterialBindingMismatch)?;
            let exchange_at = record
                .exchange_at()
                .map(|timestamp| {
                    timestamp
                        .timestamp_nanos_opt()
                        .map(Timestamp::from_unix_nanos)
                        .ok_or(ProviderCaptureError::MaterialBindingMismatch)
                })
                .transpose()?;
            let received_at = record
                .received_at()
                .timestamp_nanos_opt()
                .map(Timestamp::from_unix_nanos)
                .ok_or(ProviderCaptureError::MaterialBindingMismatch)?;
            frames.push(ProviderEventMicrobatchFrameReceipt {
                ordinal: u16::try_from(ordinal)
                    .map_err(|_| ProviderCaptureError::MaterialBindingMismatch)?,
                event_id: *record.event_id().as_bytes(),
                connection_id: *record.connection_id().as_bytes(),
                source_sequence: record.source_sequence(),
                exchange_at,
                received_at,
                payload_bytes,
                payload_digest: EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(record.payload()).into(),
                ),
            });
        }
        let receipt = ProviderEventMicrobatchReceipt::try_from_parts(
            source_id,
            metadata_revision,
            dataset,
            stream_identity,
            frames,
        )?;
        Ok(Self {
            receipt,
            records: records.into_boxed_slice(),
        })
    }

    /// Returns cloneable logical evidence before this material is consumed into its seal request.
    pub const fn receipt(&self) -> &ProviderEventMicrobatchReceipt {
        &self.receipt
    }

    /// Returns the exact bounded source frames before this material is consumed.
    pub fn records(&self) -> &[RawCaptureRecord] {
        &self.records
    }

    /// Splits this one-use material into its typed expectation and the common physical request.
    pub fn into_sealing_parts(
        self,
    ) -> (
        ProviderEventMicrobatchSealExpectation,
        ProviderCaptureSealRequest,
    ) {
        let witness = Arc::new(ProviderCaptureSealWitness(()));
        (
            ProviderEventMicrobatchSealExpectation {
                witness: Arc::clone(&witness),
            },
            ProviderCaptureSealRequest {
                witness,
                payload: ProviderCaptureSealPayload::EventMicrobatch {
                    receipt: self.receipt,
                    records: self.records,
                },
            },
        )
    }
}

#[derive(Debug)]
struct ProviderCaptureSealWitness(());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProviderCaptureSealDisposition {
    Whole,
    Components,
}

/// Opaque one-use expectation for the exact physical seal request split from capture material.
///
/// This value is deliberately non-cloneable and non-serializable. It shares only a private
/// process-local witness with its request; no logical receipt comparison can substitute another
/// successfully sealed capture.
#[derive(Debug)]
pub struct ProviderCaptureSealExpectation {
    witness: Arc<ProviderCaptureSealWitness>,
    disposition: ProviderCaptureSealDisposition,
}

impl ProviderCaptureSealExpectation {
    /// Consumes this expectation and the sealer's opaque result into one exclusive live authority.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<RejoinedProviderCapture, ProviderCaptureError> {
        if !Arc::ptr_eq(&self.witness, &sealed.witness) {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let SealedProviderCapturePayload::ResponseSet(receipt) = sealed.payload else {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        };
        match self.disposition {
            ProviderCaptureSealDisposition::Whole => {
                Ok(RejoinedProviderCapture::Whole(ProviderWholeCaptureToken {
                    receipt,
                }))
            }
            ProviderCaptureSealDisposition::Components => {
                let capture = receipt.capture();
                if capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
                    || capture.request_graph_components().is_empty()
                {
                    return Err(ProviderCaptureError::SealedBindingMismatch);
                }
                let receipt = Arc::new(receipt);
                let mut tokens = Vec::new();
                tokens
                    .try_reserve_exact(receipt.capture().request_graph_components().len())
                    .map_err(|_| ProviderCaptureError::AllocationFailed)?;
                for component in receipt.capture().request_graph_components() {
                    tokens.push(ProviderCaptureComponentToken {
                        receipt: Arc::clone(&receipt),
                        ordinal: component.ordinal(),
                    });
                }
                Ok(RejoinedProviderCapture::Components(
                    ProviderCaptureComponentTokenSet {
                        tokens: tokens.into_boxed_slice(),
                    },
                ))
            }
        }
    }
}

/// Opaque one-use expectation for the exact live-event microbatch seal request.
#[derive(Debug)]
pub struct ProviderEventMicrobatchSealExpectation {
    witness: Arc<ProviderCaptureSealWitness>,
}

impl ProviderEventMicrobatchSealExpectation {
    /// Consumes the sealer result only when it carries this exact process-local witness and kind.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<ProviderEventMicrobatchToken, ProviderCaptureError> {
        if !Arc::ptr_eq(&self.witness, &sealed.witness) {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let SealedProviderCapturePayload::EventMicrobatch(receipt) = sealed.payload else {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        };
        receipt.capture().validate()?;
        Ok(ProviderEventMicrobatchToken { receipt })
    }
}

#[derive(Debug)]
enum ProviderCaptureSealPayload {
    ResponseSet {
        receipt: ProviderCaptureSetReceipt,
        records: Box<[RawCaptureRecord]>,
    },
    EventMicrobatch {
        receipt: ProviderEventMicrobatchReceipt,
        records: Box<[RawCaptureRecord]>,
    },
}

/// Opaque one-use request consumed only by the application-owned physical sealer.
#[derive(Debug)]
pub struct ProviderCaptureSealRequest {
    witness: Arc<ProviderCaptureSealWitness>,
    payload: ProviderCaptureSealPayload,
}

impl ProviderCaptureSealRequest {
    /// Seals the exact records and moves the private process-local witness into the result.
    ///
    /// Production composition exposes this operation only through `ResearchService`; adapter tests
    /// exercise the same consuming capability directly against an ephemeral store.
    pub fn seal(
        self,
        store: &SealedResearchJournalStore,
    ) -> Result<SealedProviderCaptureMaterial, ProviderCaptureMaterialSealError> {
        let payload = match self.payload {
            ProviderCaptureSealPayload::ResponseSet { receipt, records } => {
                let segment = store.seal(&records)?;
                SealedProviderCapturePayload::ResponseSet(
                    SealedProviderCaptureSetReceipt::try_bind(receipt, segment)?,
                )
            }
            ProviderCaptureSealPayload::EventMicrobatch { receipt, records } => {
                let segment = store.seal(&records)?;
                SealedProviderCapturePayload::EventMicrobatch(
                    SealedProviderEventMicrobatchReceipt::try_bind(receipt, segment)?,
                )
            }
        };
        Ok(SealedProviderCaptureMaterial {
            witness: self.witness,
            payload,
        })
    }
}

#[derive(Debug)]
enum SealedProviderCapturePayload {
    ResponseSet(SealedProviderCaptureSetReceipt),
    EventMicrobatch(SealedProviderEventMicrobatchReceipt),
}

/// Opaque one-use physical result returned by the sole seal operation.
#[derive(Debug)]
pub struct SealedProviderCaptureMaterial {
    witness: Arc<ProviderCaptureSealWitness>,
    payload: SealedProviderCapturePayload,
}

/// Exclusive post-seal authority: either the whole capture or its closed component token set.
#[derive(Debug)]
pub enum RejoinedProviderCapture {
    /// One indivisible whole-capture authority.
    Whole(ProviderWholeCaptureToken),
    /// One exact non-reusable token per complete request-graph component.
    Components(ProviderCaptureComponentTokenSet),
}

impl RejoinedProviderCapture {
    /// Consumes this closed result as a whole-capture token.
    pub fn try_into_whole(self) -> Result<ProviderWholeCaptureToken, ProviderCaptureError> {
        match self {
            Self::Whole(token) => Ok(token),
            Self::Components(_) => Err(ProviderCaptureError::SealedBindingMismatch),
        }
    }

    /// Consumes this closed result as a request-graph component token set.
    pub fn try_into_components(
        self,
    ) -> Result<ProviderCaptureComponentTokenSet, ProviderCaptureError> {
        match self {
            Self::Components(tokens) => Ok(tokens),
            Self::Whole(_) => Err(ProviderCaptureError::SealedBindingMismatch),
        }
    }
}

/// Exclusive one-use authority for one complete sealed capture.
#[derive(Debug)]
pub struct ProviderWholeCaptureToken {
    receipt: SealedProviderCaptureSetReceipt,
}

impl ProviderWholeCaptureToken {
    /// Returns cloneable persisted/restart evidence; this value cannot authorize another binding.
    pub fn persisted_receipt(&self) -> &SealedProviderCaptureSetReceipt {
        &self.receipt
    }
}

/// Closed exact ordinal-indexed component authorities for one complete sealed request graph.
#[derive(Debug)]
pub struct ProviderCaptureComponentTokenSet {
    tokens: Box<[ProviderCaptureComponentToken]>,
}

impl ProviderCaptureComponentTokenSet {
    /// Returns the exact number of complete graph components.
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Returns whether the graph unexpectedly contained no components.
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Consumes the set into one non-cloneable token per exact component ordinal.
    pub fn into_tokens(self) -> Box<[ProviderCaptureComponentToken]> {
        self.tokens
    }
}

/// Exclusive one-use authority for one exact complete request-graph component.
#[derive(Debug)]
pub struct ProviderCaptureComponentToken {
    receipt: Arc<SealedProviderCaptureSetReceipt>,
    ordinal: u16,
}

impl ProviderCaptureComponentToken {
    /// Returns the adapter-selected exact component ordinal carried by this token.
    pub const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Returns persisted/restart evidence without cloning the complete graph receipt.
    pub fn persisted_receipt(&self) -> &SealedProviderCaptureSetReceipt {
        &self.receipt
    }
}

/// Failure while durably sealing already validated provider capture material.
#[derive(Debug, Error)]
pub enum ProviderCaptureMaterialSealError {
    /// The sole sealed research-segment store rejected or could not persist the exact records.
    #[error("provider capture material could not be sealed")]
    Store(#[from] SealedResearchJournalStoreError),
    /// The store's verified physical frame receipt did not bind back to the capture receipt.
    #[error("sealed provider capture material does not match its receipt")]
    Capture(#[from] ProviderCaptureError),
}

/// Final receipt binding provider observations to one verified immutable physical segment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SealedProviderCaptureSetReceipt {
    capture: ProviderCaptureSetReceipt,
    segment: SealedResearchJournalSegmentReceipt,
    receipt_digest: EvidenceDigest,
}

impl SealedProviderCaptureSetReceipt {
    /// Rebinds persisted capture evidence to a freshly verified immutable frame receipt.
    ///
    /// This returns cloneable restart evidence only; it does not recreate any one-use live
    /// publication authority.
    pub fn try_bind(
        capture: ProviderCaptureSetReceipt,
        segment: SealedResearchJournalSegmentReceipt,
    ) -> Result<Self, ProviderCaptureError> {
        if capture.pages.len() != segment.frames().len() {
            return Err(ProviderCaptureError::PhysicalReceiptMismatch);
        }
        for (page, frame) in capture.pages.iter().zip(segment.frames()) {
            if frame.ordinal() != u32::from(page.ordinal)
                || frame.provider_payload_bytes() != page.body_bytes
                || frame.provider_payload_digest() != page.body_digest
                || frame.received_at() != page.received_at
                || frame.source_sequence() != Some(u64::from(page.ordinal))
            {
                return Err(ProviderCaptureError::PhysicalReceiptMismatch);
            }
        }
        let receipt_digest = sealed_provider_capture_receipt_digest(
            capture.observation_digest,
            segment.physical_receipt_digest(),
        );
        Ok(Self {
            capture,
            segment,
            receipt_digest,
        })
    }

    /// Returns the provider request/page observation receipt.
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }

    /// Returns the verified immutable `MSJ1` physical receipt.
    pub const fn segment(&self) -> &SealedResearchJournalSegmentReceipt {
        &self.segment
    }

    /// Returns the digest binding observed times and the sealed physical receipt.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

fn sealed_provider_capture_receipt_digest(
    capture_observation_digest: EvidenceDigest,
    physical_receipt_digest: EvidenceDigest,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/sealed-provider-capture-receipt/v1");
    hash_digest(&mut hash, capture_observation_digest);
    hash_digest(&mut hash, physical_receipt_digest);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

/// Persistable evidence binding one live-event microbatch to one immutable physical segment.
///
/// This cloneable receipt is restart evidence only. The non-cloneable
/// [`ProviderEventMicrobatchToken`] remains the sole live authority for downstream admission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SealedProviderEventMicrobatchReceipt {
    capture: ProviderEventMicrobatchReceipt,
    segment: SealedResearchJournalSegmentReceipt,
    receipt_digest: EvidenceDigest,
}

impl SealedProviderEventMicrobatchReceipt {
    fn try_bind(
        capture: ProviderEventMicrobatchReceipt,
        segment: SealedResearchJournalSegmentReceipt,
    ) -> Result<Self, ProviderCaptureError> {
        capture.validate()?;
        if capture.frames().len() != segment.frames().len() {
            return Err(ProviderCaptureError::PhysicalReceiptMismatch);
        }
        for (event, frame) in capture.frames().iter().zip(segment.frames()) {
            if frame.ordinal() != u32::from(event.ordinal())
                || frame.provider_payload_bytes() != event.payload_bytes()
                || frame.provider_payload_digest() != event.payload_digest()
                || frame.received_at() != event.received_at()
                || frame.source_sequence() != event.source_sequence()
            {
                return Err(ProviderCaptureError::PhysicalReceiptMismatch);
            }
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/sealed-provider-event-microbatch-receipt/v1");
        hash_digest(&mut hash, capture.observation_digest());
        hash_digest(&mut hash, segment.physical_receipt_digest());
        let receipt_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
        Ok(Self {
            capture,
            segment,
            receipt_digest,
        })
    }

    /// Returns the exact bounded logical event-microbatch evidence.
    pub const fn capture(&self) -> &ProviderEventMicrobatchReceipt {
        &self.capture
    }

    /// Returns the verified immutable physical journal receipt.
    pub const fn segment(&self) -> &SealedResearchJournalSegmentReceipt {
        &self.segment
    }

    /// Returns the digest joining logical observations to the physical object.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Exclusive one-use live authority for one exact sealed event microbatch.
#[derive(Debug)]
pub struct ProviderEventMicrobatchToken {
    receipt: SealedProviderEventMicrobatchReceipt,
}

impl ProviderEventMicrobatchToken {
    /// Returns cloneable persisted/restart evidence; it cannot authorize another live admission.
    pub const fn persisted_receipt(&self) -> &SealedProviderEventMicrobatchReceipt {
        &self.receipt
    }
}

/// Exact portion of one sealed provider capture consumed by a normalized extraction batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCaptureScope {
    /// The extraction consumes the complete sealed response or request graph.
    Whole,
    /// The extraction consumes one exact component of a complete sealed request graph.
    RequestGraphComponent {
        /// Adapter-supplied zero-based graph component ordinal.
        ordinal: u16,
    },
}

/// Physical layout retained by one consuming sealed-provider binding.
///
/// This is deliberately orthogonal to the durable capture-unit kind and the platform raw-object
/// kind. It says only how this binding selects and maps the already sealed physical evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCaptureBindingLayout {
    /// One physical journal segment contains the whole logical capture.
    WholeSingleSegment,
    /// One physical journal segment contains the graph from which one component is selected.
    RequestGraphComponent,
    /// One ordered physical segment per logical capture page, with no combined reseal.
    OrderedSegments,
}

/// Exact canonical-row mapping to one logical capture page and immutable physical frame.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderCaptureRowFrame {
    canonical_row_ordinal: u32,
    capture_page_ordinal: u16,
    segment_ordinal: u16,
    physical_frame_ordinal: u32,
    page_body_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl ProviderCaptureRowFrame {
    /// Returns the contiguous zero-based canonical row ordinal.
    pub const fn canonical_row_ordinal(&self) -> u32 {
        self.canonical_row_ordinal
    }

    /// Returns the exact page ordinal in the complete logical capture.
    pub const fn capture_page_ordinal(&self) -> u16 {
        self.capture_page_ordinal
    }

    /// Returns the ordered immutable physical segment ordinal.
    pub const fn segment_ordinal(&self) -> u16 {
        self.segment_ordinal
    }

    /// Returns the exact frame ordinal inside that immutable physical segment.
    pub const fn physical_frame_ordinal(&self) -> u32 {
        self.physical_frame_ordinal
    }

    /// Returns the exact provider-body digest shared by logical page and physical frame.
    pub const fn page_body_digest(&self) -> EvidenceDigest {
        self.page_body_digest
    }

    /// Returns the exact socket-boundary receive time shared by page and frame.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the exact source sequence retained in the immutable raw frame.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }
}

/// Checked value-only restart projection of one canonical-row to physical-frame mapping.
///
/// This projection cannot construct a sealed binding or live token. It is accepted only by
/// [`ProviderCaptureBindingDigest::verify_evidence`] as caller-supplied persisted evidence.
#[derive(Debug, Eq, PartialEq)]
pub struct ProviderCaptureRowFrameEvidence {
    canonical_row_ordinal: u32,
    capture_page_ordinal: u16,
    segment_ordinal: u16,
    physical_frame_ordinal: u32,
    page_body_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl ProviderCaptureRowFrameEvidence {
    /// Validates exact bounded row, page, segment, frame, digest, clock, and sequence evidence.
    #[allow(
        clippy::too_many_arguments,
        reason = "persisted row/frame evidence remains explicit"
    )]
    pub fn try_new(
        canonical_row_ordinal: u32,
        capture_page_ordinal: u16,
        segment_ordinal: u16,
        physical_frame_ordinal: u32,
        page_body_digest: EvidenceDigest,
        received_at: Timestamp,
        source_sequence: Option<u64>,
    ) -> Result<Self, ProviderCaptureError> {
        if usize::try_from(canonical_row_ordinal)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            >= MAX_EXTRACTION_RECORDS
            || usize::from(capture_page_ordinal) >= MAX_PROVIDER_CAPTURE_PAGES
            || usize::from(segment_ordinal) >= MAX_PROVIDER_CAPTURE_PAGES
            || usize::try_from(physical_frame_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
                >= MAX_PROVIDER_CAPTURE_PAGES
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        require_sha256_identity(page_body_digest)?;
        Ok(Self {
            canonical_row_ordinal,
            capture_page_ordinal,
            segment_ordinal,
            physical_frame_ordinal,
            page_body_digest,
            received_at,
            source_sequence,
        })
    }

    fn digest_projection(&self) -> ProviderCaptureRowFrameDigestProjection {
        ProviderCaptureRowFrameDigestProjection {
            canonical_row_ordinal: self.canonical_row_ordinal,
            capture_page_ordinal: self.capture_page_ordinal,
            segment_ordinal: self.segment_ordinal,
            physical_frame_ordinal: self.physical_frame_ordinal,
            page_body_digest: self.page_body_digest,
            received_at: self.received_at,
            source_sequence: self.source_sequence,
        }
    }
}

/// Checked borrowed restart projection of one exact sources capture and platform physical claim.
///
/// The platform claim remains borrowed and independently non-authoritative. This projection can
/// only participate in value verification and cannot recreate a sealed receipt or live token.
#[derive(Debug)]
pub struct ProviderCapturePhysicalClaimEvidenceRef<'a> {
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    claim: &'a SealedResearchJournalSegmentClaim,
}

impl<'a> ProviderCapturePhysicalClaimEvidenceRef<'a> {
    /// Validates one bounded borrowed physical claim projection without copying frames or bytes.
    pub fn try_new(
        capture_content_digest: EvidenceDigest,
        capture_observation_digest: EvidenceDigest,
        sealed_capture_receipt_digest: EvidenceDigest,
        claim: &'a SealedResearchJournalSegmentClaim,
    ) -> Result<Self, ProviderCaptureError> {
        require_sha256_identity(capture_content_digest)?;
        require_sha256_identity(capture_observation_digest)?;
        require_sha256_identity(sealed_capture_receipt_digest)?;
        require_sha256_identity(claim.content_digest())?;
        require_sha256_identity(claim.physical_receipt_digest())?;
        if claim.relative_reference().is_empty()
            || claim.size_bytes() == 0
            || claim.frames().is_empty()
            || claim.frames().len() > MAX_PROVIDER_CAPTURE_PAGES
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        for (expected_ordinal, frame) in claim.frames().iter().enumerate() {
            if frame.ordinal()
                != u32::try_from(expected_ordinal)
                    .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
                || frame.framed_bytes() == 0
                || frame.provider_payload_bytes() == 0
                || frame.provider_payload_bytes() > MAX_PROVIDER_CAPTURE_PAGE_BYTES
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            require_sha256_identity(frame.provider_payload_digest())?;
        }
        if sealed_provider_capture_receipt_digest(
            capture_observation_digest,
            claim.physical_receipt_digest(),
        ) != sealed_capture_receipt_digest
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(Self {
            capture_content_digest,
            capture_observation_digest,
            sealed_capture_receipt_digest,
            claim,
        })
    }

    fn digest_projection(&self) -> ProviderCapturePhysicalClaimDigestProjection<'a> {
        ProviderCapturePhysicalClaimDigestProjection {
            capture_content_digest: self.capture_content_digest,
            capture_observation_digest: self.capture_observation_digest,
            sealed_capture_receipt_digest: self.sealed_capture_receipt_digest,
            claim: self.claim,
        }
    }
}

/// Source-neutral one-use authority joining one logical capture to ordered standalone seals.
#[derive(Debug)]
pub struct ProviderOrderedCaptureSegments {
    root_capture: ProviderCaptureSetReceipt,
    segments: Box<[ProviderWholeCaptureToken]>,
    receipt_digest: EvidenceDigest,
}

impl ProviderOrderedCaptureSegments {
    /// Consumes exact standalone page tokens into one ordered logical multi-segment capture.
    pub fn try_rejoin(
        root_capture: ProviderCaptureSetReceipt,
        segments: Vec<ProviderWholeCaptureToken>,
    ) -> Result<Self, ProviderCaptureError> {
        if segments.is_empty()
            || segments.len() != root_capture.pages().len()
            || segments.len() > MAX_PROVIDER_CAPTURE_PAGES
            || root_capture.terminal()
                != ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        for (ordinal, (root_page, token)) in root_capture.pages().iter().zip(&segments).enumerate()
        {
            let ordinal =
                u16::try_from(ordinal).map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
            let sealed = token.persisted_receipt();
            let [segment_page] = sealed.capture().pages() else {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            };
            let [frame] = sealed.segment().frames() else {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            };
            if root_page.ordinal() != ordinal
                || sealed.capture().source_id() != root_capture.source_id()
                || sealed.capture().metadata_revision() != root_capture.metadata_revision()
                || sealed.capture().request_set_identity() != root_page.request_identity()
                || sealed.capture().terminal()
                    != ProviderCaptureTerminalDisposition::StandaloneResponse
                || segment_page.ordinal() != 0
                || segment_page.request_identity() != root_page.request_identity()
                || segment_page.http_status() != root_page.http_status()
                || segment_page.body_bytes() != root_page.body_bytes()
                || segment_page.body_digest() != root_page.body_digest()
                || segment_page.received_at() != root_page.received_at()
                || frame.ordinal() != 0
                || frame.provider_payload_bytes() != root_page.body_bytes()
                || frame.provider_payload_digest() != root_page.body_digest()
                || frame.received_at() != root_page.received_at()
                || frame.source_sequence() != Some(0)
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
        }
        let receipt_digest = ordered_provider_capture_segments_digest(
            root_capture.observation_digest(),
            segments.iter().map(|token| {
                let sealed = token.persisted_receipt();
                (
                    sealed.receipt_digest(),
                    sealed.segment().physical_receipt_digest(),
                )
            }),
        );
        Ok(Self {
            root_capture,
            segments: segments.into_boxed_slice(),
            receipt_digest,
        })
    }

    /// Returns the complete logical capture spanned by the ordered physical segments.
    pub const fn root_capture(&self) -> &ProviderCaptureSetReceipt {
        &self.root_capture
    }

    /// Returns the digest binding root observation identity and all ordered physical receipts.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }

    /// Returns the exact number of ordered physical segments.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Returns persisted evidence for one ordered physical segment.
    pub fn persisted_segment_receipt(
        &self,
        ordinal: usize,
    ) -> Option<&SealedProviderCaptureSetReceipt> {
        self.segments
            .get(ordinal)
            .map(ProviderWholeCaptureToken::persisted_receipt)
    }
}

fn ordered_provider_capture_segments_digest(
    root_observation_digest: EvidenceDigest,
    segments: impl Iterator<Item = (EvidenceDigest, EvidenceDigest)>,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/ordered-provider-capture-segments/v1");
    hash_digest(&mut digest, root_observation_digest);
    for (sealed_capture_receipt_digest, physical_receipt_digest) in segments {
        hash_digest(&mut digest, sealed_capture_receipt_digest);
        hash_digest(&mut digest, physical_receipt_digest);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

#[derive(Debug)]
enum ProviderCaptureBindingAuthority {
    Whole(ProviderWholeCaptureToken),
    Component(ProviderCaptureComponentToken),
    Ordered(ProviderOrderedCaptureSegments),
}

const PROVIDER_CAPTURE_BINDING_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/sealed-provider-capture-binding/evidence/v1";
const PROVIDER_CAPTURE_BINDING_DIGEST_VERSION: u16 = 1;

/// Copyable evidence identity of one complete sealed provider-capture binding projection.
///
/// This digest is persistable evidence, not live authority. It has no deserialization or public
/// construction surface and cannot recreate the non-cloneable binding that minted it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderCaptureBindingDigest(EvidenceDigest);

impl ProviderCaptureBindingDigest {
    /// Returns the algorithm-qualified digest for durable evidence persistence.
    pub const fn evidence(self) -> EvidenceDigest {
        self.0
    }

    /// Verifies persisted value evidence without reconstructing a binding or live authority.
    ///
    /// The expected digest is loaded by the caller. Common code validates every bounded input and
    /// compares the same private canonical projection used by live binding construction. It never
    /// returns a computed replacement digest.
    #[allow(
        clippy::too_many_arguments,
        reason = "persisted sealed-binding evidence remains explicit"
    )]
    pub fn verify_evidence(
        expected_digest: EvidenceDigest,
        root_capture: &ProviderCaptureSetReceipt,
        sealed_capture_receipt_digest: EvidenceDigest,
        scope: ProviderCaptureScope,
        layout: ProviderCaptureBindingLayout,
        extraction_content_digest: EvidenceDigest,
        extraction_record_count: usize,
        record_count: usize,
        native_schema_version: u16,
        native_implementation: ProviderNativeLineageImplementation,
        native_schema_fingerprint: EvidenceDigest,
        native_batch_digest: EvidenceDigest,
        native_row_count: usize,
        row_frames: &[ProviderCaptureRowFrameEvidence],
        physical_claims: &[ProviderCapturePhysicalClaimEvidenceRef<'_>],
    ) -> Result<(), ProviderCaptureError> {
        require_sha256_identity(expected_digest)?;
        let header = ProviderCaptureBindingDigestHeader {
            sealed_capture_receipt_digest,
            scope,
            layout,
            extraction_content_digest,
            extraction_record_count,
            record_count,
            native_schema_version,
            native_implementation,
            native_schema_fingerprint,
            native_batch_digest,
            native_row_count,
        };
        validate_provider_capture_binding_evidence(
            root_capture,
            header,
            row_frames,
            physical_claims,
        )?;
        let observed = compute_provider_capture_binding_digest(
            root_capture,
            header,
            row_frames.len(),
            |ordinal| {
                row_frames
                    .get(ordinal)
                    .map(ProviderCaptureRowFrameEvidence::digest_projection)
            },
            physical_claims.len(),
            |ordinal| {
                physical_claims
                    .get(ordinal)
                    .map(ProviderCapturePhysicalClaimEvidenceRef::digest_projection)
            },
        )?;
        if observed.evidence() != expected_digest {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }
}

/// Non-reusable binding between one normalized extraction batch and its sealed raw evidence.
///
/// The binding deliberately has no clone or serialization surface. It is minted only after the
/// exact batch object is rejoined to either the complete capture or one adapter-selected request
/// graph component. Durable publication authority consumes this value; provider adapters cannot
/// manufacture a second storage or manifest authority from it.
#[derive(Debug)]
pub struct SealedProviderCaptureBinding {
    authority: ProviderCaptureBindingAuthority,
    batch: ExtractionBatch,
    native_lineage: ProviderNativeLineageBatch,
    content_identity: ExtractionContentIdentity,
    record_count: usize,
    row_frames: Box<[ProviderCaptureRowFrame]>,
    evidence_digest: ProviderCaptureBindingDigest,
}

impl SealedProviderCaptureBinding {
    /// Binds a batch whose source object consumes the complete sealed capture.
    pub fn try_whole(
        token: ProviderWholeCaptureToken,
        batch: ExtractionBatch,
        native_lineage: ProviderNativeLineageBatch,
        row_capture_page_ordinals: Vec<u16>,
    ) -> Result<Self, ProviderCaptureError> {
        validate_whole_capture(&token, &batch)?;
        let receipt = token.persisted_receipt();
        let row_frames =
            single_segment_row_frames(receipt, &batch, &row_capture_page_ordinals, None)?;
        Self::finish(
            ProviderCaptureBindingAuthority::Whole(token),
            batch,
            native_lineage,
            row_frames,
        )
    }

    /// Binds a batch to one exact adapter-selected complete request-graph component.
    ///
    /// The ordinal is authoritative input from the adapter-specific continuation. This method
    /// never scans graph components by digest. It rebuilds the selected component's local page
    /// chain, byte total, content identity, observation identity, and object capture identity.
    pub fn try_component(
        token: ProviderCaptureComponentToken,
        batch: ExtractionBatch,
        native_lineage: ProviderNativeLineageBatch,
        row_capture_page_ordinals: Vec<u16>,
    ) -> Result<Self, ProviderCaptureError> {
        let (first_page, end_page) = validate_component_capture(&token, &batch)?;
        let receipt = token.persisted_receipt();
        let row_frames = single_segment_row_frames(
            receipt,
            &batch,
            &row_capture_page_ordinals,
            Some((first_page, end_page)),
        )?;
        Self::finish(
            ProviderCaptureBindingAuthority::Component(token),
            batch,
            native_lineage,
            row_frames,
        )
    }

    /// Binds a batch to one source-neutral ordered multi-segment logical capture.
    pub fn try_ordered_segments(
        token: ProviderOrderedCaptureSegments,
        batch: ExtractionBatch,
        native_lineage: ProviderNativeLineageBatch,
        row_capture_page_ordinals: Vec<u16>,
    ) -> Result<Self, ProviderCaptureError> {
        validate_ordered_capture(&token, &batch)?;
        let row_frames = ordered_segment_row_frames(&token, &batch, &row_capture_page_ordinals)?;
        Self::finish(
            ProviderCaptureBindingAuthority::Ordered(token),
            batch,
            native_lineage,
            row_frames,
        )
    }

    fn finish(
        authority: ProviderCaptureBindingAuthority,
        batch: ExtractionBatch,
        native_lineage: ProviderNativeLineageBatch,
        row_frames: Box<[ProviderCaptureRowFrame]>,
    ) -> Result<Self, ProviderCaptureError> {
        native_lineage
            .validate(&batch)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
        let content_identity = ExtractionContentIdentity::try_from_batch(&batch)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
        let record_count = batch.records().len();
        if content_identity.record_count() != record_count || row_frames.len() != record_count {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let evidence_digest = provider_capture_binding_digest(
            &authority,
            content_identity,
            record_count,
            &native_lineage,
            &row_frames,
        )?;
        Ok(Self {
            authority,
            batch,
            native_lineage,
            content_identity,
            record_count,
            row_frames,
            evidence_digest,
        })
    }

    /// Revalidates canonical/native alignment, content identity, and exact page/frame mapping.
    pub fn validate(&self) -> Result<(), ProviderCaptureError> {
        if self.record_count != self.batch.records().len()
            || self.content_identity
                != ExtractionContentIdentity::try_from_batch(&self.batch)
                    .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            || self.native_lineage.validate(&self.batch).is_err()
            || self.row_frames.len() != self.record_count
            || self.row_frames.iter().enumerate().any(|(ordinal, frame)| {
                frame.canonical_row_ordinal != u32::try_from(ordinal).unwrap_or(u32::MAX)
            })
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let mut row_capture_page_ordinals = Vec::new();
        row_capture_page_ordinals
            .try_reserve_exact(self.row_frames.len())
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;
        row_capture_page_ordinals.extend(
            self.row_frames
                .iter()
                .map(|frame| frame.capture_page_ordinal),
        );
        let expected_row_frames = match &self.authority {
            ProviderCaptureBindingAuthority::Whole(token) => {
                validate_whole_capture(token, &self.batch)?;
                single_segment_row_frames(
                    token.persisted_receipt(),
                    &self.batch,
                    &row_capture_page_ordinals,
                    None,
                )?
            }
            ProviderCaptureBindingAuthority::Component(token) => {
                let page_range = validate_component_capture(token, &self.batch)?;
                single_segment_row_frames(
                    token.persisted_receipt(),
                    &self.batch,
                    &row_capture_page_ordinals,
                    Some(page_range),
                )?
            }
            ProviderCaptureBindingAuthority::Ordered(token) => {
                validate_ordered_capture(token, &self.batch)?;
                ordered_segment_row_frames(token, &self.batch, &row_capture_page_ordinals)?
            }
        };
        if expected_row_frames.as_ref() != self.row_frames.as_ref() {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        if provider_capture_binding_digest(
            &self.authority,
            self.content_identity,
            self.record_count,
            &self.native_lineage,
            &self.row_frames,
        )? != self.evidence_digest
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        Ok(())
    }

    /// Returns the exact canonical batch retained inside this one-use authority.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns the exact aligned provider-native lineage retained inside this authority.
    pub const fn native_lineage(&self) -> &ProviderNativeLineageBatch {
        &self.native_lineage
    }

    /// Returns the recomputable semantic identity of the retained canonical batch.
    pub const fn content_identity(&self) -> ExtractionContentIdentity {
        self.content_identity
    }

    /// Returns the exact canonical record count bound at construction.
    pub const fn record_count(&self) -> usize {
        self.record_count
    }

    /// Returns the contiguous exact canonical-row to logical-page/physical-frame map.
    pub const fn row_frames(&self) -> &[ProviderCaptureRowFrame] {
        &self.row_frames
    }

    /// Returns the cached, recomputable evidence digest of the complete binding projection.
    pub const fn evidence_digest(&self) -> ProviderCaptureBindingDigest {
        self.evidence_digest
    }

    /// Returns persisted logical capture evidence; it cannot remint live authority.
    pub fn capture_evidence(&self) -> &ProviderCaptureSetReceipt {
        match &self.authority {
            ProviderCaptureBindingAuthority::Whole(token) => token.receipt.capture(),
            ProviderCaptureBindingAuthority::Component(token) => token.receipt.capture(),
            ProviderCaptureBindingAuthority::Ordered(token) => token.root_capture(),
        }
    }

    /// Returns the digest binding the logical observation to its exact physical seal(s).
    pub fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        match &self.authority {
            ProviderCaptureBindingAuthority::Whole(token) => token.receipt.receipt_digest(),
            ProviderCaptureBindingAuthority::Component(token) => token.receipt.receipt_digest(),
            ProviderCaptureBindingAuthority::Ordered(token) => token.receipt_digest(),
        }
    }

    /// Returns exact persisted evidence for one ordered physical segment.
    pub fn persisted_segment_receipt(
        &self,
        ordinal: usize,
    ) -> Option<&SealedProviderCaptureSetReceipt> {
        match &self.authority {
            ProviderCaptureBindingAuthority::Whole(token) => {
                (ordinal == 0).then_some(&token.receipt)
            }
            ProviderCaptureBindingAuthority::Component(token) => {
                (ordinal == 0).then_some(token.persisted_receipt())
            }
            ProviderCaptureBindingAuthority::Ordered(token) => {
                token.persisted_segment_receipt(ordinal)
            }
        }
    }

    /// Returns whether the bound batch consumes the whole capture or one exact component.
    pub const fn scope(&self) -> ProviderCaptureScope {
        match &self.authority {
            ProviderCaptureBindingAuthority::Whole(_)
            | ProviderCaptureBindingAuthority::Ordered(_) => ProviderCaptureScope::Whole,
            ProviderCaptureBindingAuthority::Component(token) => {
                ProviderCaptureScope::RequestGraphComponent {
                    ordinal: token.ordinal,
                }
            }
        }
    }

    /// Returns the exact physical layout retained by this binding.
    pub const fn layout(&self) -> ProviderCaptureBindingLayout {
        match self.authority {
            ProviderCaptureBindingAuthority::Whole(_) => {
                ProviderCaptureBindingLayout::WholeSingleSegment
            }
            ProviderCaptureBindingAuthority::Component(_) => {
                ProviderCaptureBindingLayout::RequestGraphComponent
            }
            ProviderCaptureBindingAuthority::Ordered(_) => {
                ProviderCaptureBindingLayout::OrderedSegments
            }
        }
    }

    /// Returns the exact request-graph component ordinal, when component-scoped.
    pub const fn component_ordinal(&self) -> Option<u16> {
        match self.scope() {
            ProviderCaptureScope::Whole => None,
            ProviderCaptureScope::RequestGraphComponent { ordinal } => Some(ordinal),
        }
    }
}

#[derive(Clone, Copy)]
struct ProviderCaptureBindingDigestHeader {
    sealed_capture_receipt_digest: EvidenceDigest,
    scope: ProviderCaptureScope,
    layout: ProviderCaptureBindingLayout,
    extraction_content_digest: EvidenceDigest,
    extraction_record_count: usize,
    record_count: usize,
    native_schema_version: u16,
    native_implementation: ProviderNativeLineageImplementation,
    native_schema_fingerprint: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    native_row_count: usize,
}

#[derive(Clone, Copy)]
struct ProviderCaptureRowFrameDigestProjection {
    canonical_row_ordinal: u32,
    capture_page_ordinal: u16,
    segment_ordinal: u16,
    physical_frame_ordinal: u32,
    page_body_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl From<&ProviderCaptureRowFrame> for ProviderCaptureRowFrameDigestProjection {
    fn from(frame: &ProviderCaptureRowFrame) -> Self {
        Self {
            canonical_row_ordinal: frame.canonical_row_ordinal,
            capture_page_ordinal: frame.capture_page_ordinal,
            segment_ordinal: frame.segment_ordinal,
            physical_frame_ordinal: frame.physical_frame_ordinal,
            page_body_digest: frame.page_body_digest,
            received_at: frame.received_at,
            source_sequence: frame.source_sequence,
        }
    }
}

#[derive(Clone, Copy)]
struct ProviderCapturePhysicalClaimDigestProjection<'a> {
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    claim: &'a SealedResearchJournalSegmentClaim,
}

fn provider_capture_binding_digest(
    authority: &ProviderCaptureBindingAuthority,
    content_identity: ExtractionContentIdentity,
    record_count: usize,
    native_lineage: &ProviderNativeLineageBatch,
    row_frames: &[ProviderCaptureRowFrame],
) -> Result<ProviderCaptureBindingDigest, ProviderCaptureError> {
    let native_schema = native_lineage.schema();
    let (scope, layout) = binding_scope_layout(authority);
    let header = ProviderCaptureBindingDigestHeader {
        sealed_capture_receipt_digest: binding_sealed_capture_digest(authority),
        scope,
        layout,
        extraction_content_digest: content_identity.digest(),
        extraction_record_count: content_identity.record_count(),
        record_count,
        native_schema_version: native_schema.version(),
        native_implementation: native_schema.implementation(),
        native_schema_fingerprint: native_schema.fingerprint(),
        native_batch_digest: native_lineage.batch_digest(),
        native_row_count: native_lineage.rows().len(),
    };
    let segment_count = binding_segment_count(authority);
    compute_provider_capture_binding_digest(
        binding_root_capture(authority),
        header,
        row_frames.len(),
        |ordinal| {
            row_frames
                .get(ordinal)
                .map(ProviderCaptureRowFrameDigestProjection::from)
        },
        segment_count,
        |ordinal| {
            binding_segment_receipt(authority, ordinal).map(|receipt| {
                ProviderCapturePhysicalClaimDigestProjection {
                    capture_content_digest: receipt.capture().content_digest(),
                    capture_observation_digest: receipt.capture().observation_digest(),
                    sealed_capture_receipt_digest: receipt.receipt_digest(),
                    claim: receipt.segment().claim(),
                }
            })
        },
    )
}

fn compute_provider_capture_binding_digest<'claim, RowAt, ClaimAt>(
    root_capture: &ProviderCaptureSetReceipt,
    header: ProviderCaptureBindingDigestHeader,
    row_frame_count: usize,
    mut row_frame_at: RowAt,
    physical_claim_count: usize,
    mut physical_claim_at: ClaimAt,
) -> Result<ProviderCaptureBindingDigest, ProviderCaptureError>
where
    RowAt: FnMut(usize) -> Option<ProviderCaptureRowFrameDigestProjection>,
    ClaimAt: FnMut(usize) -> Option<ProviderCapturePhysicalClaimDigestProjection<'claim>>,
{
    let mut digest = Sha256::new();
    hash_binding_field(&mut digest, PROVIDER_CAPTURE_BINDING_DIGEST_DOMAIN)?;
    digest.update(PROVIDER_CAPTURE_BINDING_DIGEST_VERSION.to_be_bytes());

    hash_binding_field(&mut digest, root_capture.source_id().as_str().as_bytes())?;
    hash_binding_field(
        &mut digest,
        root_capture
            .metadata_revision()
            .as_source_identifier()
            .as_str()
            .as_bytes(),
    )?;
    hash_binding_field(&mut digest, root_capture.dataset().as_str().as_bytes())?;
    hash_digest(&mut digest, root_capture.request_set_identity());
    hash_binding_field(&mut digest, root_capture.terminal().tag())?;
    digest.update(root_capture.total_body_bytes().to_be_bytes());
    hash_digest(&mut digest, root_capture.content_digest());
    hash_digest(&mut digest, root_capture.observation_digest());

    hash_binding_length(&mut digest, root_capture.pages().len())?;
    for page in root_capture.pages() {
        digest.update(page.ordinal().to_be_bytes());
        hash_digest(&mut digest, page.request_identity());
        hash_optional_digest(&mut digest, page.request_page_token_digest());
        hash_optional_digest(&mut digest, page.response_next_page_token_digest());
        digest.update(page.http_status().to_be_bytes());
        digest.update(page.body_bytes().to_be_bytes());
        hash_digest(&mut digest, page.body_digest());
        digest.update(page.received_at().unix_nanos().to_be_bytes());
    }

    hash_binding_length(&mut digest, root_capture.request_graph_components().len())?;
    for component in root_capture.request_graph_components() {
        digest.update(component.ordinal().to_be_bytes());
        hash_binding_field(&mut digest, component.source_id().as_str().as_bytes())?;
        hash_binding_field(
            &mut digest,
            component
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        )?;
        hash_binding_field(&mut digest, component.dataset().as_str().as_bytes())?;
        hash_digest(&mut digest, component.request_set_identity());
        hash_binding_field(&mut digest, component.terminal().tag())?;
        digest.update(component.first_page_ordinal().to_be_bytes());
        digest.update(component.page_count().get().to_be_bytes());
        digest.update(component.total_body_bytes().to_be_bytes());
        hash_digest(&mut digest, component.content_digest());
        hash_digest(&mut digest, component.observation_digest());
    }

    hash_digest(&mut digest, header.sealed_capture_receipt_digest);
    hash_binding_scope_layout(&mut digest, header.scope, header.layout)?;
    hash_digest(&mut digest, header.extraction_content_digest);
    hash_binding_length(&mut digest, header.extraction_record_count)?;
    hash_binding_length(&mut digest, header.record_count)?;
    digest.update(header.native_schema_version.to_be_bytes());
    digest.update([header.native_implementation.tag()]);
    hash_digest(&mut digest, header.native_schema_fingerprint);
    hash_digest(&mut digest, header.native_batch_digest);
    hash_binding_length(&mut digest, header.native_row_count)?;

    hash_binding_length(&mut digest, row_frame_count)?;
    for ordinal in 0..row_frame_count {
        let frame = row_frame_at(ordinal).ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        digest.update(frame.canonical_row_ordinal.to_be_bytes());
        digest.update(frame.capture_page_ordinal.to_be_bytes());
        digest.update(frame.segment_ordinal.to_be_bytes());
        digest.update(frame.physical_frame_ordinal.to_be_bytes());
        hash_digest(&mut digest, frame.page_body_digest);
        digest.update(frame.received_at.unix_nanos().to_be_bytes());
        hash_binding_optional_u64(&mut digest, frame.source_sequence);
    }

    hash_binding_length(&mut digest, physical_claim_count)?;
    for ordinal in 0..physical_claim_count {
        hash_binding_length(&mut digest, ordinal)?;
        let physical =
            physical_claim_at(ordinal).ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        hash_digest(&mut digest, physical.capture_content_digest);
        hash_digest(&mut digest, physical.capture_observation_digest);
        hash_digest(&mut digest, physical.sealed_capture_receipt_digest);
        hash_binding_field(&mut digest, physical.claim.relative_reference().as_bytes())?;
        hash_digest(&mut digest, physical.claim.content_digest());
        digest.update(physical.claim.size_bytes().to_be_bytes());
        hash_digest(&mut digest, physical.claim.physical_receipt_digest());
        hash_binding_length(&mut digest, physical.claim.frames().len())?;
        for frame in physical.claim.frames() {
            digest.update(frame.ordinal().to_be_bytes());
            digest.update(frame.offset().to_be_bytes());
            digest.update(frame.framed_bytes().to_be_bytes());
            digest.update(frame.provider_payload_bytes().to_be_bytes());
            hash_digest(&mut digest, frame.provider_payload_digest());
            digest.update(frame.received_at().unix_nanos().to_be_bytes());
            hash_binding_optional_u64(&mut digest, frame.source_sequence());
        }
    }

    Ok(ProviderCaptureBindingDigest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    )))
}

fn hash_binding_scope_layout(
    digest: &mut Sha256,
    scope: ProviderCaptureScope,
    layout: ProviderCaptureBindingLayout,
) -> Result<(), ProviderCaptureError> {
    match (scope, layout) {
        (ProviderCaptureScope::Whole, ProviderCaptureBindingLayout::WholeSingleSegment) => {
            digest.update([1]);
            digest.update([1]);
            digest.update([0]);
        }
        (
            ProviderCaptureScope::RequestGraphComponent { ordinal },
            ProviderCaptureBindingLayout::RequestGraphComponent,
        ) => {
            digest.update([2]);
            digest.update([2]);
            digest.update([1]);
            digest.update(ordinal.to_be_bytes());
        }
        (ProviderCaptureScope::Whole, ProviderCaptureBindingLayout::OrderedSegments) => {
            digest.update([1]);
            digest.update([3]);
            digest.update([0]);
        }
        _ => return Err(ProviderCaptureError::SealedBindingMismatch),
    }
    Ok(())
}

fn validate_provider_capture_binding_evidence(
    root_capture: &ProviderCaptureSetReceipt,
    header: ProviderCaptureBindingDigestHeader,
    row_frames: &[ProviderCaptureRowFrameEvidence],
    physical_claims: &[ProviderCapturePhysicalClaimEvidenceRef<'_>],
) -> Result<(), ProviderCaptureError> {
    require_sha256_identity(header.sealed_capture_receipt_digest)?;
    require_sha256_identity(header.extraction_content_digest)?;
    require_sha256_identity(header.native_schema_fingerprint)?;
    require_sha256_identity(header.native_batch_digest)?;
    let native_schema =
        ProviderNativeLineageSchema::for_implementation(header.native_implementation);
    if native_schema.version() != header.native_schema_version
        || native_schema.fingerprint() != header.native_schema_fingerprint
        || header.extraction_record_count != header.record_count
        || header.native_row_count != header.record_count
        || row_frames.len() != header.record_count
        || header.record_count > MAX_EXTRACTION_RECORDS
        || physical_claims.is_empty()
        || physical_claims.len() > MAX_PROVIDER_CAPTURE_PAGES
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }

    let selected_page_range = match (header.scope, header.layout) {
        (ProviderCaptureScope::Whole, ProviderCaptureBindingLayout::WholeSingleSegment) => {
            if physical_claims.len() != 1 {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            None
        }
        (
            ProviderCaptureScope::RequestGraphComponent { ordinal },
            ProviderCaptureBindingLayout::RequestGraphComponent,
        ) => {
            if physical_claims.len() != 1
                || root_capture.terminal()
                    != ProviderCaptureTerminalDisposition::CompleteRequestGraph
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            let component = root_capture
                .request_graph_components()
                .get(usize::from(ordinal))
                .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
            if component.ordinal() != ordinal
                || component.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            let first = usize::from(component.first_page_ordinal());
            let end = first
                .checked_add(usize::from(component.page_count().get()))
                .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
            root_capture
                .pages()
                .get(first..end)
                .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
            Some((first, end))
        }
        (ProviderCaptureScope::Whole, ProviderCaptureBindingLayout::OrderedSegments) => {
            if root_capture.terminal()
                != ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage
                || physical_claims.len() != root_capture.pages().len()
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            None
        }
        _ => return Err(ProviderCaptureError::SealedBindingMismatch),
    };

    match header.layout {
        ProviderCaptureBindingLayout::WholeSingleSegment
        | ProviderCaptureBindingLayout::RequestGraphComponent => {
            let physical = &physical_claims[0];
            if physical.capture_content_digest != root_capture.content_digest()
                || physical.capture_observation_digest != root_capture.observation_digest()
                || physical.sealed_capture_receipt_digest != header.sealed_capture_receipt_digest
                || physical.claim.frames().len() != root_capture.pages().len()
            {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
            for (page, frame) in root_capture.pages().iter().zip(physical.claim.frames()) {
                if frame.ordinal() != u32::from(page.ordinal())
                    || frame.provider_payload_bytes() != page.body_bytes()
                    || frame.provider_payload_digest() != page.body_digest()
                    || frame.received_at() != page.received_at()
                    || frame.source_sequence() != Some(u64::from(page.ordinal()))
                {
                    return Err(ProviderCaptureError::SealedBindingMismatch);
                }
            }
        }
        ProviderCaptureBindingLayout::OrderedSegments => {
            for (page, physical) in root_capture.pages().iter().zip(physical_claims) {
                let [frame] = physical.claim.frames() else {
                    return Err(ProviderCaptureError::SealedBindingMismatch);
                };
                if frame.ordinal() != 0
                    || frame.provider_payload_bytes() != page.body_bytes()
                    || frame.provider_payload_digest() != page.body_digest()
                    || frame.received_at() != page.received_at()
                    || frame.source_sequence() != Some(0)
                {
                    return Err(ProviderCaptureError::SealedBindingMismatch);
                }
            }
            let ordered_digest = ordered_provider_capture_segments_digest(
                root_capture.observation_digest(),
                physical_claims.iter().map(|physical| {
                    (
                        physical.sealed_capture_receipt_digest,
                        physical.claim.physical_receipt_digest(),
                    )
                }),
            );
            if ordered_digest != header.sealed_capture_receipt_digest {
                return Err(ProviderCaptureError::SealedBindingMismatch);
            }
        }
    }

    for (expected_ordinal, row) in row_frames.iter().enumerate() {
        let expected_ordinal = u32::try_from(expected_ordinal)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
        let page_index = usize::from(row.capture_page_ordinal);
        let page = root_capture
            .pages()
            .get(page_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        if selected_page_range.is_some_and(|(first, end)| page_index < first || page_index >= end) {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let (expected_segment_ordinal, expected_physical_frame_ordinal, expected_sequence) =
            match header.layout {
                ProviderCaptureBindingLayout::WholeSingleSegment
                | ProviderCaptureBindingLayout::RequestGraphComponent => (
                    0,
                    u32::from(row.capture_page_ordinal),
                    Some(u64::from(row.capture_page_ordinal)),
                ),
                ProviderCaptureBindingLayout::OrderedSegments => {
                    (row.capture_page_ordinal, 0, Some(0))
                }
            };
        let physical = physical_claims
            .get(usize::from(expected_segment_ordinal))
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let physical_frame = physical
            .claim
            .frames()
            .get(
                usize::try_from(expected_physical_frame_ordinal)
                    .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
            )
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        if row.canonical_row_ordinal != expected_ordinal
            || row.segment_ordinal != expected_segment_ordinal
            || row.physical_frame_ordinal != expected_physical_frame_ordinal
            || row.page_body_digest != page.body_digest()
            || row.received_at != page.received_at()
            || row.source_sequence != expected_sequence
            || physical_frame.ordinal() != expected_physical_frame_ordinal
            || physical_frame.provider_payload_bytes() != page.body_bytes()
            || physical_frame.provider_payload_digest() != page.body_digest()
            || physical_frame.received_at() != page.received_at()
            || physical_frame.source_sequence() != expected_sequence
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    }
    Ok(())
}

fn binding_root_capture(authority: &ProviderCaptureBindingAuthority) -> &ProviderCaptureSetReceipt {
    match authority {
        ProviderCaptureBindingAuthority::Whole(token) => token.receipt.capture(),
        ProviderCaptureBindingAuthority::Component(token) => token.receipt.capture(),
        ProviderCaptureBindingAuthority::Ordered(token) => token.root_capture(),
    }
}

fn binding_scope_layout(
    authority: &ProviderCaptureBindingAuthority,
) -> (ProviderCaptureScope, ProviderCaptureBindingLayout) {
    match authority {
        ProviderCaptureBindingAuthority::Whole(_) => (
            ProviderCaptureScope::Whole,
            ProviderCaptureBindingLayout::WholeSingleSegment,
        ),
        ProviderCaptureBindingAuthority::Component(token) => (
            ProviderCaptureScope::RequestGraphComponent {
                ordinal: token.ordinal,
            },
            ProviderCaptureBindingLayout::RequestGraphComponent,
        ),
        ProviderCaptureBindingAuthority::Ordered(_) => (
            ProviderCaptureScope::Whole,
            ProviderCaptureBindingLayout::OrderedSegments,
        ),
    }
}

fn binding_sealed_capture_digest(authority: &ProviderCaptureBindingAuthority) -> EvidenceDigest {
    match authority {
        ProviderCaptureBindingAuthority::Whole(token) => token.receipt.receipt_digest(),
        ProviderCaptureBindingAuthority::Component(token) => token.receipt.receipt_digest(),
        ProviderCaptureBindingAuthority::Ordered(token) => token.receipt_digest(),
    }
}

fn binding_segment_count(authority: &ProviderCaptureBindingAuthority) -> usize {
    match authority {
        ProviderCaptureBindingAuthority::Whole(_)
        | ProviderCaptureBindingAuthority::Component(_) => 1,
        ProviderCaptureBindingAuthority::Ordered(token) => token.segment_count(),
    }
}

fn binding_segment_receipt(
    authority: &ProviderCaptureBindingAuthority,
    ordinal: usize,
) -> Option<&SealedProviderCaptureSetReceipt> {
    match authority {
        ProviderCaptureBindingAuthority::Whole(token) => {
            (ordinal == 0).then_some(token.persisted_receipt())
        }
        ProviderCaptureBindingAuthority::Component(token) => {
            (ordinal == 0).then_some(token.persisted_receipt())
        }
        ProviderCaptureBindingAuthority::Ordered(token) => token.persisted_segment_receipt(ordinal),
    }
}

fn hash_binding_length(digest: &mut Sha256, length: usize) -> Result<(), ProviderCaptureError> {
    digest.update(
        u64::try_from(length)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?
            .to_be_bytes(),
    );
    Ok(())
}

fn hash_binding_field(digest: &mut Sha256, value: &[u8]) -> Result<(), ProviderCaptureError> {
    hash_binding_length(digest, value.len())?;
    digest.update(value);
    Ok(())
}

fn hash_binding_optional_u64(digest: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            digest.update([1]);
            digest.update(value.to_be_bytes());
        }
        None => digest.update([0]),
    }
}

fn validate_whole_capture(
    token: &ProviderWholeCaptureToken,
    batch: &ExtractionBatch,
) -> Result<(), ProviderCaptureError> {
    let receipt = token.persisted_receipt();
    let capture = receipt.capture();
    let object = batch.request().object();
    let expected_identity = SourceObjectCaptureIdentity::try_from_capture(capture)?;
    if receipt.receipt_digest().bytes() == [0; 32]
        || object.source_id() != capture.source_id()
        || object.metadata_revision() != capture.metadata_revision()
        || object.dataset() != capture.dataset()
        || object.capture_identity() != expected_identity
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    Ok(())
}

fn validate_component_capture(
    token: &ProviderCaptureComponentToken,
    batch: &ExtractionBatch,
) -> Result<(usize, usize), ProviderCaptureError> {
    let receipt = token.persisted_receipt();
    let capture = receipt.capture();
    if capture.terminal() != ProviderCaptureTerminalDisposition::CompleteRequestGraph
        || receipt.receipt_digest().bytes() == [0; 32]
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let component = capture
        .request_graph_components()
        .get(usize::from(token.ordinal))
        .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
    if component.ordinal() != token.ordinal
        || component.terminal() == ProviderCaptureTerminalDisposition::CompleteRequestGraph
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let first_page = usize::from(component.first_page_ordinal());
    let page_count = usize::from(component.page_count().get());
    let end_page = first_page
        .checked_add(page_count)
        .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
    let selected_pages = capture
        .pages()
        .get(first_page..end_page)
        .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
    let mut local_pages = Vec::new();
    local_pages
        .try_reserve_exact(page_count)
        .map_err(|_| ProviderCaptureError::AllocationFailed)?;
    let mut total_body_bytes = 0_u64;
    for (local_ordinal, page) in selected_pages.iter().enumerate() {
        let local_ordinal = u16::try_from(local_ordinal)
            .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
        total_body_bytes = total_body_bytes
            .checked_add(page.body_bytes())
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        local_pages.push(page.with_ordinal(local_ordinal));
    }
    validate_page_chain(component.terminal(), &local_pages)?;
    let content_digest = capture_content_digest(
        component.source_id(),
        component.metadata_revision(),
        component.dataset(),
        component.request_set_identity(),
        component.terminal(),
        total_body_bytes,
        &local_pages,
    );
    let observation_digest = capture_observation_digest(content_digest, &local_pages);
    let expected_identity = SourceObjectCaptureIdentity::Paged {
        content_digest,
        page_count: component.page_count(),
        terminal: component.terminal(),
    };
    let object = batch.request().object();
    if total_body_bytes != component.total_body_bytes()
        || content_digest != component.content_digest()
        || observation_digest != component.observation_digest()
        || object.source_id() != component.source_id()
        || object.metadata_revision() != component.metadata_revision()
        || object.dataset() != component.dataset()
        || object.capture_identity() != expected_identity
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    Ok((first_page, end_page))
}

fn validate_ordered_capture(
    token: &ProviderOrderedCaptureSegments,
    batch: &ExtractionBatch,
) -> Result<(), ProviderCaptureError> {
    let capture = token.root_capture();
    let object = batch.request().object();
    let expected_identity = SourceObjectCaptureIdentity::try_from_capture(capture)?;
    if token.receipt_digest().bytes() == [0; 32]
        || object.source_id() != capture.source_id()
        || object.metadata_revision() != capture.metadata_revision()
        || object.dataset() != capture.dataset()
        || object.capture_identity() != expected_identity
    {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    // Rejoining already checked every standalone seal. Rechecking each frame here prevents a
    // future representation change from weakening the retained ordered-segment invariant.
    if token.segments.len() != capture.pages().len() {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    for (ordinal, (root_page, segment)) in capture.pages().iter().zip(&token.segments).enumerate() {
        let ordinal =
            u16::try_from(ordinal).map_err(|_| ProviderCaptureError::SealedBindingMismatch)?;
        let sealed = segment.persisted_receipt();
        let [segment_page] = sealed.capture().pages() else {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        };
        let [frame] = sealed.segment().frames() else {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        };
        if root_page.ordinal() != ordinal
            || sealed.capture().source_id() != capture.source_id()
            || sealed.capture().metadata_revision() != capture.metadata_revision()
            || sealed.capture().request_set_identity() != root_page.request_identity()
            || sealed.capture().terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
            || segment_page.ordinal() != 0
            || segment_page.request_identity() != root_page.request_identity()
            || segment_page.http_status() != root_page.http_status()
            || segment_page.body_bytes() != root_page.body_bytes()
            || segment_page.body_digest() != root_page.body_digest()
            || segment_page.received_at() != root_page.received_at()
            || frame.ordinal() != 0
            || frame.provider_payload_bytes() != root_page.body_bytes()
            || frame.provider_payload_digest() != root_page.body_digest()
            || frame.received_at() != root_page.received_at()
            || frame.source_sequence() != Some(0)
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
    }
    Ok(())
}

fn single_segment_row_frames(
    receipt: &SealedProviderCaptureSetReceipt,
    batch: &ExtractionBatch,
    row_capture_page_ordinals: &[u16],
    allowed_page_range: Option<(usize, usize)>,
) -> Result<Box<[ProviderCaptureRowFrame]>, ProviderCaptureError> {
    if row_capture_page_ordinals.len() != batch.records().len() {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut mappings = Vec::new();
    mappings
        .try_reserve_exact(row_capture_page_ordinals.len())
        .map_err(|_| ProviderCaptureError::AllocationFailed)?;
    for (row_ordinal, capture_page_ordinal) in row_capture_page_ordinals.iter().copied().enumerate()
    {
        let page_index = usize::from(capture_page_ordinal);
        if allowed_page_range.is_some_and(|(start, end)| page_index < start || page_index >= end) {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        let page = receipt
            .capture()
            .pages()
            .get(page_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let frame = receipt
            .segment()
            .frames()
            .get(page_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        if page.ordinal() != capture_page_ordinal
            || frame.ordinal() != u32::from(capture_page_ordinal)
            || frame.provider_payload_bytes() != page.body_bytes()
            || frame.provider_payload_digest() != page.body_digest()
            || frame.received_at() != page.received_at()
            || frame.source_sequence() != Some(u64::from(capture_page_ordinal))
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        mappings.push(ProviderCaptureRowFrame {
            canonical_row_ordinal: u32::try_from(row_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
            capture_page_ordinal,
            segment_ordinal: 0,
            physical_frame_ordinal: frame.ordinal(),
            page_body_digest: page.body_digest(),
            received_at: page.received_at(),
            source_sequence: frame.source_sequence(),
        });
    }
    Ok(mappings.into_boxed_slice())
}

fn ordered_segment_row_frames(
    token: &ProviderOrderedCaptureSegments,
    batch: &ExtractionBatch,
    row_capture_page_ordinals: &[u16],
) -> Result<Box<[ProviderCaptureRowFrame]>, ProviderCaptureError> {
    if row_capture_page_ordinals.len() != batch.records().len() {
        return Err(ProviderCaptureError::SealedBindingMismatch);
    }
    let mut mappings = Vec::new();
    mappings
        .try_reserve_exact(row_capture_page_ordinals.len())
        .map_err(|_| ProviderCaptureError::AllocationFailed)?;
    for (row_ordinal, capture_page_ordinal) in row_capture_page_ordinals.iter().copied().enumerate()
    {
        let segment_index = usize::from(capture_page_ordinal);
        let page = token
            .root_capture
            .pages()
            .get(segment_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let sealed = token
            .persisted_segment_receipt(segment_index)
            .ok_or(ProviderCaptureError::SealedBindingMismatch)?;
        let [frame] = sealed.segment().frames() else {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        };
        if page.ordinal() != capture_page_ordinal
            || frame.ordinal() != 0
            || frame.provider_payload_bytes() != page.body_bytes()
            || frame.provider_payload_digest() != page.body_digest()
            || frame.received_at() != page.received_at()
            || frame.source_sequence() != Some(0)
        {
            return Err(ProviderCaptureError::SealedBindingMismatch);
        }
        mappings.push(ProviderCaptureRowFrame {
            canonical_row_ordinal: u32::try_from(row_ordinal)
                .map_err(|_| ProviderCaptureError::SealedBindingMismatch)?,
            capture_page_ordinal,
            segment_ordinal: capture_page_ordinal,
            physical_frame_ordinal: 0,
            page_body_digest: page.body_digest(),
            received_at: page.received_at(),
            source_sequence: frame.source_sequence(),
        });
    }
    Ok(mappings.into_boxed_slice())
}

/// Capture lineage attached to a discovered source object without embedding provider bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum SourceObjectCaptureIdentity {
    /// The source object is one non-paginated exact provider response or local exact object.
    Standalone,
    /// The source object is bound to a complete provider response capture set.
    Paged {
        /// Stable provider-content identity excluding local receive/storage facts.
        content_digest: EvidenceDigest,
        /// Exact nonzero page count bound into the capture receipt.
        page_count: NonZeroU16,
        /// Exact terminal disposition bound into the capture receipt.
        terminal: ProviderCaptureTerminalDisposition,
    },
}

impl SourceObjectCaptureIdentity {
    /// Constructs strict captured-response lineage from a completed provider capture set.
    pub fn try_from_capture(
        capture: &ProviderCaptureSetReceipt,
    ) -> Result<Self, ProviderCaptureError> {
        let page_count = NonZeroU16::new(u16::try_from(capture.pages.len()).map_err(|_| {
            ProviderCaptureError::PageLimitExceeded {
                max: MAX_PROVIDER_CAPTURE_PAGES,
            }
        })?)
        .ok_or(ProviderCaptureError::EmptyCaptureSet)?;
        Ok(Self::Paged {
            content_digest: capture.content_digest,
            page_count,
            terminal: capture.terminal,
        })
    }

    /// Returns the stable capture-set content identity for captured response lineage.
    pub const fn paged_content_digest(self) -> Option<EvidenceDigest> {
        match self {
            Self::Standalone => None,
            Self::Paged { content_digest, .. } => Some(content_digest),
        }
    }

    pub(crate) fn validate(self) -> Result<Self, ProviderCaptureError> {
        if let Self::Paged {
            content_digest,
            page_count,
            ..
        } = self
        {
            require_sha256_identity(content_digest)?;
            if usize::from(page_count.get()) > MAX_PROVIDER_CAPTURE_PAGES {
                return Err(ProviderCaptureError::PageLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_PAGES,
                });
            }
        }
        Ok(self)
    }

    pub(crate) fn hash_into(self, hash: &mut Sha256) {
        match self {
            Self::Standalone => hash.update(b"standalone"),
            Self::Paged {
                content_digest,
                page_count,
                terminal,
            } => {
                hash.update(b"paged");
                hash_digest(hash, content_digest);
                hash.update(page_count.get().to_be_bytes());
                hash.update(terminal.tag());
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum SourceObjectCaptureIdentityWire {
    Standalone,
    Paged {
        content_digest: EvidenceDigest,
        page_count: NonZeroU16,
        terminal: ProviderCaptureTerminalDisposition,
    },
}

impl<'de> Deserialize<'de> for SourceObjectCaptureIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let identity = match SourceObjectCaptureIdentityWire::deserialize(deserializer)? {
            SourceObjectCaptureIdentityWire::Standalone => Self::Standalone,
            SourceObjectCaptureIdentityWire::Paged {
                content_digest,
                page_count,
                terminal,
            } => Self::Paged {
                content_digest,
                page_count,
                terminal,
            },
        };
        identity.validate().map_err(serde::de::Error::custom)
    }
}

/// Provider capture-set invariant failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProviderCaptureError {
    /// At least one exact provider response is required.
    #[error("provider capture set is empty")]
    EmptyCaptureSet,
    /// Page count or ordinal exceeds the global bound.
    #[error("provider capture page count exceeds maximum {max}")]
    PageLimitExceeded {
        /// Global maximum.
        max: usize,
    },
    /// Aggregate or individual page bytes exceed the global bound.
    #[error("provider capture bytes exceed maximum {max}")]
    ByteLimitExceeded {
        /// Global maximum.
        max: u64,
    },
    /// Only successful provider response pages can become research content.
    #[error("provider capture HTTP status {0} is not successful")]
    UnsuccessfulHttpStatus(u16),
    /// An exact request/body/token identity is not nonzero SHA-256.
    #[error("provider capture identity must be nonzero SHA-256")]
    InvalidDigest,
    /// Page ordinals are not contiguous from zero in response order.
    #[error("provider capture page ordering is invalid")]
    PageOrderingInvalid,
    /// A request token does not match the previous response's exact next token.
    #[error("provider capture page-token chain is invalid")]
    PageTokenChainInvalid,
    /// Terminal disposition conflicts with final page/token facts.
    #[error("provider capture terminal disposition is invalid")]
    TerminalDispositionInvalid,
    /// Serialized derived totals or digests do not rebuild exactly.
    #[error("provider capture receipt binding does not match")]
    ReceiptBindingMismatch,
    /// Provider page facts do not map one-to-one to the sealed `MSJ1` physical frames.
    #[error("provider capture does not match its sealed physical receipt")]
    PhysicalReceiptMismatch,
    /// Raw response records do not map one-to-one to the provider page receipt.
    #[error("provider capture material does not match its page receipt")]
    MaterialBindingMismatch,
    /// A sealed whole-capture or request-component receipt does not match its normalized batch.
    #[error("sealed provider capture does not match its extraction batch and scope")]
    SealedBindingMismatch,
    /// A bounded allocation for capture framing could not be reserved.
    #[error("provider capture allocation failed")]
    AllocationFailed,
    /// Complete request-graph framing is missing, discontinuous, or internally inconsistent.
    #[error("provider capture request graph is invalid")]
    RequestGraphInvalid,
    /// The first component does not establish the request graph's canonical owner authority.
    #[error("provider capture request-graph root component does not match owner authority")]
    RequestGraphComponentMismatch,
    /// A complete request graph cannot be used as another graph's component.
    #[error("nested provider capture request graphs are not admitted")]
    NestedRequestGraph,
    /// Complete market-bar history semantic coordinates are invalid or unbounded.
    #[error("complete market-bar history capture semantics are invalid")]
    InvalidMarketBarHistorySemantics,
}

fn require_sha256_identity(digest: EvidenceDigest) -> Result<(), ProviderCaptureError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes().iter().all(|byte| *byte == 0)
    {
        Err(ProviderCaptureError::InvalidDigest)
    } else {
        Ok(())
    }
}

fn validate_page_chain(
    terminal: ProviderCaptureTerminalDisposition,
    pages: &[ProviderCapturePageReceipt],
) -> Result<(), ProviderCaptureError> {
    if terminal == ProviderCaptureTerminalDisposition::CompleteRequestGraph {
        return Err(ProviderCaptureError::RequestGraphInvalid);
    }
    let mut previous_next_token = None;
    for (ordinal, page) in pages.iter().enumerate() {
        if page.request_page_token_digest != previous_next_token {
            return Err(ProviderCaptureError::PageTokenChainInvalid);
        }
        if ordinal + 1 < pages.len() && page.response_next_page_token_digest.is_none() {
            return Err(ProviderCaptureError::PageTokenChainInvalid);
        }
        previous_next_token = page.response_next_page_token_digest;
    }
    let last = pages.last().ok_or(ProviderCaptureError::EmptyCaptureSet)?;
    if last.response_next_page_token_digest.is_some() {
        return Err(ProviderCaptureError::TerminalDispositionInvalid);
    }
    if terminal == ProviderCaptureTerminalDisposition::StandaloneResponse
        && (pages.len() != 1 || last.request_page_token_digest.is_some())
    {
        return Err(ProviderCaptureError::TerminalDispositionInvalid);
    }
    Ok(())
}

fn request_graph_component_for_page(
    components: &[ProviderCaptureRequestGraphComponent],
    page_ordinal: u16,
) -> Option<&ProviderCaptureRequestGraphComponent> {
    let index = components
        .partition_point(|component| component.first_page_ordinal <= page_ordinal)
        .checked_sub(1)?;
    let component = components.get(index)?;
    let end = component
        .first_page_ordinal
        .checked_add(component.page_count.get())?;
    (page_ordinal < end).then_some(component)
}

fn validate_request_graph_components(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    pages: &[ProviderCapturePageReceipt],
    components: &[ProviderCaptureRequestGraphComponent],
) -> Result<(), ProviderCaptureError> {
    if components.len() < 2 || components.len() > MAX_PROVIDER_CAPTURE_PAGES {
        return Err(ProviderCaptureError::RequestGraphInvalid);
    }
    let mut expected_first_page = 0_usize;
    for (expected_component_ordinal, component) in components.iter().enumerate() {
        if usize::from(component.ordinal) != expected_component_ordinal
            || usize::from(component.first_page_ordinal) != expected_first_page
            || component.terminal == ProviderCaptureTerminalDisposition::CompleteRequestGraph
            || (expected_component_ordinal == 0
                && (&component.source_id != source_id
                    || &component.metadata_revision != metadata_revision))
        {
            return Err(ProviderCaptureError::RequestGraphInvalid);
        }
        require_sha256_identity(component.request_set_identity)?;
        require_sha256_identity(component.content_digest)?;
        require_sha256_identity(component.observation_digest)?;
        let page_count = usize::from(component.page_count.get());
        let end = expected_first_page
            .checked_add(page_count)
            .ok_or(ProviderCaptureError::RequestGraphInvalid)?;
        let component_pages = pages
            .get(expected_first_page..end)
            .ok_or(ProviderCaptureError::RequestGraphInvalid)?;
        let mut local_pages = Vec::new();
        local_pages
            .try_reserve_exact(page_count)
            .map_err(|_| ProviderCaptureError::AllocationFailed)?;
        let mut total_body_bytes = 0_u64;
        for (local_ordinal, page) in component_pages.iter().enumerate() {
            total_body_bytes = total_body_bytes.checked_add(page.body_bytes).ok_or(
                ProviderCaptureError::ByteLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_BYTES,
                },
            )?;
            local_pages.push(page.with_ordinal(u16::try_from(local_ordinal).map_err(|_| {
                ProviderCaptureError::PageLimitExceeded {
                    max: MAX_PROVIDER_CAPTURE_PAGES,
                }
            })?));
        }
        validate_page_chain(component.terminal, &local_pages)?;
        let content_digest = capture_content_digest(
            &component.source_id,
            &component.metadata_revision,
            &component.dataset,
            component.request_set_identity,
            component.terminal,
            total_body_bytes,
            &local_pages,
        );
        let observation_digest = capture_observation_digest(content_digest, &local_pages);
        if total_body_bytes != component.total_body_bytes
            || content_digest != component.content_digest
            || observation_digest != component.observation_digest
        {
            return Err(ProviderCaptureError::RequestGraphInvalid);
        }
        expected_first_page = end;
    }
    if expected_first_page != pages.len() {
        return Err(ProviderCaptureError::RequestGraphInvalid);
    }
    Ok(())
}

fn validate_semantic_graph_binding(
    semantic_binding: &ProviderCaptureSemanticBinding,
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    request_set_identity: EvidenceDigest,
    components: &[ProviderCaptureRequestGraphComponent],
) -> Result<(), ProviderCaptureError> {
    let ProviderCaptureSemanticBinding::CompleteMarketBarHistoryV1(binding) = semantic_binding;
    let market_bar_component = components
        .get(usize::from(binding.market_bar_component_ordinal))
        .ok_or(ProviderCaptureError::RequestGraphInvalid)?;
    let session_calendar_component = components
        .get(usize::from(binding.session_calendar_component_ordinal))
        .ok_or(ProviderCaptureError::RequestGraphInvalid)?;
    if market_bar_component.ordinal != binding.market_bar_component_ordinal
        || session_calendar_component.ordinal != binding.session_calendar_component_ordinal
        || request_set_identity
            != semantic_request_graph_identity(
                source_id,
                metadata_revision,
                dataset,
                &binding.graph_purpose,
                components,
            )
    {
        return Err(ProviderCaptureError::RequestGraphInvalid);
    }
    Ok(())
}

fn semantic_request_graph_identity(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    purpose: &SourceIdentifier,
    components: &[ProviderCaptureRequestGraphComponent],
) -> EvidenceDigest {
    request_graph_identity_from_fields(
        source_id,
        metadata_revision,
        dataset,
        purpose,
        components.len(),
        components.iter().map(|component| {
            (
                &component.source_id,
                &component.metadata_revision,
                &component.dataset,
                component.request_set_identity,
            )
        }),
    )
}

fn semantic_material_request_graph_identity(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    purpose: &SourceIdentifier,
    components: &[ProviderCaptureMaterial],
) -> EvidenceDigest {
    request_graph_identity_from_fields(
        source_id,
        metadata_revision,
        dataset,
        purpose,
        components.len(),
        components.iter().map(|component| {
            (
                component.receipt().source_id(),
                component.receipt().metadata_revision(),
                component.receipt().dataset(),
                component.receipt().request_set_identity(),
            )
        }),
    )
}

fn request_graph_identity_from_fields<'a>(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    purpose: &SourceIdentifier,
    component_count: usize,
    components: impl IntoIterator<
        Item = (
            &'a SourceId,
            &'a MetadataRevision,
            &'a SourceIdentifier,
            EvidenceDigest,
        ),
    >,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/provider-request-graph-composition/v1\0");
    hash_field(&mut hash, purpose.as_str().as_bytes());
    hash_field(&mut hash, source_id.as_str().as_bytes());
    hash_field(
        &mut hash,
        metadata_revision.as_source_identifier().as_str().as_bytes(),
    );
    hash_field(&mut hash, dataset.as_str().as_bytes());
    hash.update((component_count as u64).to_be_bytes());
    for (
        component_source_id,
        component_metadata_revision,
        component_dataset,
        component_request_identity,
    ) in components
    {
        hash_field(&mut hash, component_source_id.as_str().as_bytes());
        hash_field(
            &mut hash,
            component_metadata_revision
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        hash_field(&mut hash, component_dataset.as_str().as_bytes());
        hash.update(component_request_identity.bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn event_microbatch_content_digest(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    stream_identity: &SourceIdentifier,
    total_payload_bytes: u64,
    frames: &[ProviderEventMicrobatchFrameReceipt],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/provider-event-microbatch-content/v1");
    hash_field(&mut hash, source_id.as_str().as_bytes());
    hash_field(
        &mut hash,
        metadata_revision.as_source_identifier().as_str().as_bytes(),
    );
    hash_field(&mut hash, dataset.as_str().as_bytes());
    hash_field(&mut hash, stream_identity.as_str().as_bytes());
    hash.update(total_payload_bytes.to_be_bytes());
    hash.update((frames.len() as u64).to_be_bytes());
    for frame in frames {
        hash.update(frame.ordinal.to_be_bytes());
        match frame.source_sequence {
            Some(sequence) => {
                hash.update([1]);
                hash.update(sequence.to_be_bytes());
            }
            None => hash.update([0]),
        }
        match frame.exchange_at {
            Some(exchange_at) => {
                hash.update([1]);
                hash.update(exchange_at.unix_nanos().to_be_bytes());
            }
            None => hash.update([0]),
        }
        hash.update(frame.payload_bytes.to_be_bytes());
        hash_digest(&mut hash, frame.payload_digest);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn event_microbatch_observation_digest(
    content_digest: EvidenceDigest,
    frames: &[ProviderEventMicrobatchFrameReceipt],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/provider-event-microbatch-observation/v1");
    hash_digest(&mut hash, content_digest);
    for frame in frames {
        hash.update(frame.ordinal.to_be_bytes());
        hash.update(frame.event_id);
        hash.update(frame.connection_id);
        hash.update(frame.received_at.unix_nanos().to_be_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "all stable capture coordinates are bound"
)]
fn capture_content_digest(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    request_set_identity: EvidenceDigest,
    terminal: ProviderCaptureTerminalDisposition,
    total_body_bytes: u64,
    pages: &[ProviderCapturePageReceipt],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/provider-capture-content/v1");
    hash_field(&mut hash, source_id.as_str().as_bytes());
    hash_field(
        &mut hash,
        metadata_revision.as_source_identifier().as_str().as_bytes(),
    );
    hash_field(&mut hash, dataset.as_str().as_bytes());
    hash_digest(&mut hash, request_set_identity);
    hash.update(terminal.tag());
    hash.update(total_body_bytes.to_be_bytes());
    hash.update((pages.len() as u64).to_be_bytes());
    for page in pages {
        hash.update(page.ordinal.to_be_bytes());
        hash_digest(&mut hash, page.request_identity);
        hash_optional_digest(&mut hash, page.request_page_token_digest);
        hash_optional_digest(&mut hash, page.response_next_page_token_digest);
        hash.update(page.http_status.to_be_bytes());
        hash.update(page.body_bytes.to_be_bytes());
        hash_digest(&mut hash, page.body_digest);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

#[allow(
    clippy::too_many_arguments,
    reason = "all stable request-graph coordinates are bound"
)]
fn request_graph_content_digest(
    source_id: &SourceId,
    metadata_revision: &MetadataRevision,
    dataset: &SourceIdentifier,
    request_set_identity: EvidenceDigest,
    total_body_bytes: u64,
    pages: &[ProviderCapturePageReceipt],
    components: &[ProviderCaptureRequestGraphComponent],
    semantic_binding: Option<&ProviderCaptureSemanticBinding>,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(if semantic_binding.is_some() {
        b"market-squawk/provider-capture-request-graph-content/v2".as_slice()
    } else {
        b"market-squawk/provider-capture-request-graph-content/v1".as_slice()
    });
    hash_field(&mut hash, source_id.as_str().as_bytes());
    hash_field(
        &mut hash,
        metadata_revision.as_source_identifier().as_str().as_bytes(),
    );
    hash_field(&mut hash, dataset.as_str().as_bytes());
    hash_digest(&mut hash, request_set_identity);
    hash.update(ProviderCaptureTerminalDisposition::CompleteRequestGraph.tag());
    hash.update(total_body_bytes.to_be_bytes());
    hash.update((components.len() as u64).to_be_bytes());
    for component in components {
        hash.update(component.ordinal.to_be_bytes());
        hash_field(&mut hash, component.source_id.as_str().as_bytes());
        hash_field(
            &mut hash,
            component
                .metadata_revision
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        hash_field(&mut hash, component.dataset.as_str().as_bytes());
        hash_digest(&mut hash, component.request_set_identity);
        hash.update(component.terminal.tag());
        hash.update(component.first_page_ordinal.to_be_bytes());
        hash.update(component.page_count.get().to_be_bytes());
        hash.update(component.total_body_bytes.to_be_bytes());
        hash_digest(&mut hash, component.content_digest);
    }
    if let Some(semantic_binding) = semantic_binding {
        hash_semantic_binding(&mut hash, semantic_binding);
    }
    hash.update((pages.len() as u64).to_be_bytes());
    for page in pages {
        hash.update(page.ordinal.to_be_bytes());
        hash_digest(&mut hash, page.request_identity);
        hash_optional_digest(&mut hash, page.request_page_token_digest);
        hash_optional_digest(&mut hash, page.response_next_page_token_digest);
        hash.update(page.http_status.to_be_bytes());
        hash.update(page.body_bytes.to_be_bytes());
        hash_digest(&mut hash, page.body_digest);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_semantic_binding(hash: &mut Sha256, semantic_binding: &ProviderCaptureSemanticBinding) {
    let ProviderCaptureSemanticBinding::CompleteMarketBarHistoryV1(binding) = semantic_binding;
    hash.update([1]);
    hash.update(b"complete_market_bar_history_v1");
    hash.update(binding.requested_start.unix_nanos().to_be_bytes());
    hash.update(binding.requested_end.unix_nanos().to_be_bytes());
    hash.update(binding.instrument_id.as_uuid().as_bytes());
    hash_digest(hash, binding.instrument_revision_digest);
    hash_digest(hash, binding.admitted_plan_digest);
    hash_field(hash, binding.provider_instrument_id.as_str().as_bytes());
    hash_field(hash, binding.venue_id.as_str().as_bytes());
    hash_field(hash, binding.feed.as_str().as_bytes());
    hash_field(hash, binding.interval.as_str().as_bytes());
    hash.update([match binding.adjustment {
        MarketBarAdjustment::Raw => 1,
        MarketBarAdjustment::Split => 2,
        MarketBarAdjustment::Dividend => 3,
        MarketBarAdjustment::SpinOff => 4,
        MarketBarAdjustment::All => 5,
    }]);
    hash.update([match binding.timestamp_basis {
        BarTimestampBasis::PeriodStart => 1,
        BarTimestampBasis::PeriodEnd => 2,
    }]);
    hash.update([match binding.session_kind {
        MarketBarSessionKind::Regular => 1,
        MarketBarSessionKind::Extended => 2,
        MarketBarSessionKind::Continuous => 3,
        MarketBarSessionKind::ProviderDefined => 4,
    }]);
    hash_field(hash, binding.session_ruleset.as_str().as_bytes());
    hash_field(hash, binding.graph_purpose.as_str().as_bytes());
    hash.update(binding.market_bar_component_ordinal.to_be_bytes());
    hash.update(binding.session_calendar_component_ordinal.to_be_bytes());
    hash.update(
        u64::try_from(binding.expected_provider_timestamps.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for timestamp in &binding.expected_provider_timestamps {
        hash.update(timestamp.unix_nanos().to_be_bytes());
    }
    hash_digest(hash, binding.completeness_evidence);
}

fn capture_observation_digest(
    content_digest: EvidenceDigest,
    pages: &[ProviderCapturePageReceipt],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/provider-capture-observation/v1");
    hash_digest(&mut hash, content_digest);
    for page in pages {
        hash.update(page.ordinal.to_be_bytes());
        hash.update(page.received_at.unix_nanos().to_be_bytes());
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn request_graph_observation_digest(
    content_digest: EvidenceDigest,
    components: &[ProviderCaptureRequestGraphComponent],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/provider-capture-request-graph-observation/v1");
    hash_digest(&mut hash, content_digest);
    for component in components {
        hash.update(component.ordinal.to_be_bytes());
        hash_field(&mut hash, component.source_id.as_str().as_bytes());
        hash_field(
            &mut hash,
            component
                .metadata_revision
                .as_source_identifier()
                .as_str()
                .as_bytes(),
        );
        hash_digest(&mut hash, component.observation_digest);
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_optional_digest(hash: &mut Sha256, digest: Option<EvidenceDigest>) {
    match digest {
        Some(digest) => {
            hash.update([1]);
            hash_digest(hash, digest);
        }
        None => hash.update([0]),
    }
}

fn hash_digest(hash: &mut Sha256, digest: EvidenceDigest) {
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
}

fn hash_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use market_squawk_domain::{
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
        SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::{LocalPaths, RawCaptureRecord};
    use sha2::{Digest as _, Sha256};
    use static_assertions::assert_not_impl_any;

    use super::{
        ProviderCaptureBindingDigest, ProviderCaptureError, ProviderCaptureMaterial,
        ProviderCapturePageReceipt, ProviderCapturePhysicalClaimEvidenceRef,
        ProviderCaptureRowFrameEvidence, ProviderCaptureScope, ProviderCaptureSetReceipt,
        ProviderCaptureTerminalDisposition, SealedProviderCaptureBinding,
    };
    use crate::{
        AvailabilityEvidence, DiscoveryRequest, ExtractionBatch, ExtractionError, ExtractionRecord,
        ExtractionRequest, MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES, ProviderEventMicrobatchMaterial,
        ProviderEventMicrobatchToken, ProviderNativeLineageBatch,
        ProviderNativeLineageBatchBuilder, ProviderNativeLineageError,
        ProviderNativeLineageImplementation, ProviderNativeLineageRowEvidenceRef, SourceObject,
        verify_provider_native_lineage_batch_evidence,
    };
    use bytes::Bytes;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

    static TEMPORARY_DIRECTORY_ORDINAL: AtomicU64 = AtomicU64::new(0);

    struct TemporaryDirectory(PathBuf);

    impl TemporaryDirectory {
        fn new() -> Self {
            let ordinal = TEMPORARY_DIRECTORY_ORDINAL.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "market-squawk-provider-capture-binding-{}-{ordinal}",
                std::process::id()
            )))
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TemporaryDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn digest(byte: u8) -> EvidenceDigest {
        EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32])
    }

    fn record(
        source: &str,
        ordinal: u16,
        received_at: Timestamp,
        payload: Vec<u8>,
    ) -> Result<RawCaptureRecord, serde_json::Error> {
        serde_json::from_value(serde_json::json!({
            "event_id": format!("00000000-0000-0000-0000-{:012x}", u64::from(ordinal) + 1),
            "source": source,
            "connection_id": "00000000-0000-0000-0000-000000000064",
            "source_sequence": u64::from(ordinal),
            "exchange_at": null,
            "received_at": format!(
                "1970-01-01T00:00:00.{:09}Z",
                received_at.unix_nanos()
            ),
            "payload": payload,
        }))
    }

    fn capture(received_offset: i64) -> Result<ProviderCaptureSetReceipt, ProviderCaptureError> {
        let token = digest(9);
        ProviderCaptureSetReceipt::try_new(
            SourceId::try_from("fixture").expect("fixture source"),
            MetadataRevision::new(
                SourceIdentifier::try_from("fixture-r1").expect("fixture revision"),
            ),
            SourceIdentifier::try_from("fixture-dataset").expect("fixture dataset"),
            digest(1),
            ProviderCaptureTerminalDisposition::ExhaustedWithoutNextPage,
            vec![
                ProviderCapturePageReceipt::try_new(
                    0,
                    digest(2),
                    None,
                    Some(token),
                    200,
                    10,
                    digest(3),
                    Timestamp::from_unix_nanos(100 + received_offset),
                )?,
                ProviderCapturePageReceipt::try_new(
                    1,
                    digest(4),
                    Some(token),
                    None,
                    200,
                    20,
                    digest(5),
                    Timestamp::from_unix_nanos(200 + received_offset),
                )?,
            ],
        )
    }

    #[test]
    fn capture_digest_excludes_receive_time_and_rejects_nonterminal_or_unordered_pages()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = capture(0)?;
        let retried = capture(50)?;
        assert_eq!(first.content_digest(), retried.content_digest());
        assert_ne!(first.observation_digest(), retried.observation_digest());

        let records = first
            .pages()
            .iter()
            .map(|page| {
                let payload = vec![page.ordinal() as u8 + 1; page.body_bytes() as usize];
                record(
                    first.source_id().as_str(),
                    page.ordinal(),
                    page.received_at(),
                    payload,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let pages = records
            .iter()
            .enumerate()
            .map(|(ordinal, record)| {
                let original = &first.pages()[ordinal];
                ProviderCapturePageReceipt::try_new(
                    u16::try_from(ordinal)?,
                    original.request_identity(),
                    original.request_page_token_digest(),
                    original.response_next_page_token_digest(),
                    original.http_status(),
                    u64::try_from(record.payload().len())?,
                    EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        Sha256::digest(record.payload()).into(),
                    ),
                    original.received_at(),
                )
                .map_err(Into::into)
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        let material_receipt = ProviderCaptureSetReceipt::try_new(
            first.source_id().clone(),
            first.metadata_revision().clone(),
            first.dataset().clone(),
            first.request_set_identity(),
            first.terminal(),
            pages,
        )?;
        let wrong_source = material_receipt
            .pages()
            .iter()
            .map(|page| {
                record(
                    "another-source",
                    page.ordinal(),
                    page.received_at(),
                    records[usize::from(page.ordinal())].payload().to_vec(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary_evidence = ExactPayloadEvidence::from_content_digest(digest(30));
        let primary_expected_bytes = Some(999);
        let discovery = DiscoveryRequest::try_new(
            material_receipt.dataset().clone(),
            None,
            NonZeroU16::MIN,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let object = SourceObject::try_new(
            material_receipt.source_id().clone(),
            material_receipt.metadata_revision().clone(),
            &discovery,
            SourceIdentifier::try_from("fixture-object")?,
            SourceIdentifier::try_from("application-json")?,
            primary_evidence.clone(),
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
            None,
            primary_expected_bytes,
        )?;
        let request = ExtractionRequest::try_new(
            object,
            NonZeroU32::MIN,
            NonZeroU64::new(100_000).ok_or("fixture byte bound")?,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let payload = Bytes::from_static(b"normalized");
        let record_evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(&payload).into(),
        ));
        let batch = ExtractionBatch::try_new(
            &request,
            vec![ExtractionRecord::try_new(
                &request,
                SourceIdentifier::try_from("schema-v1")?,
                record_evidence.clone(),
                Timestamp::from_unix_nanos(2),
                None,
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: Timestamp::from_unix_nanos(3),
                },
                SourceIdentifier::try_from("record-r1")?,
                None,
                payload.clone(),
            )?],
        )?;
        let material = ProviderCaptureMaterial::try_new(material_receipt.clone(), records)?;
        assert_eq!(material.receipt(), &material_receipt);
        assert_eq!(material.records().len(), 2);
        assert_eq!(
            ProviderCaptureMaterial::try_new(material_receipt.clone(), wrong_source),
            Err(ProviderCaptureError::MaterialBindingMismatch)
        );

        let second_payload = b"secondary".to_vec();
        let second_received_at = Timestamp::from_unix_nanos(300);
        let second_source_id = SourceId::try_from("fixture-secondary")?;
        let second_metadata_revision =
            MetadataRevision::new(SourceIdentifier::try_from("fixture-secondary-r1")?);
        let second_receipt = ProviderCaptureSetReceipt::try_new(
            second_source_id.clone(),
            second_metadata_revision.clone(),
            SourceIdentifier::try_from("fixture-metadata")?,
            digest(31),
            ProviderCaptureTerminalDisposition::StandaloneResponse,
            vec![ProviderCapturePageReceipt::try_new(
                0,
                digest(32),
                None,
                None,
                200,
                u64::try_from(second_payload.len())?,
                EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    Sha256::digest(&second_payload).into(),
                ),
                second_received_at,
            )?],
        )?;
        assert!(matches!(
            batch.clone().try_bind_provider_capture(&second_receipt),
            Err(ExtractionError::SourceBindingMismatch)
        ));
        assert_eq!(
            ProviderCaptureMaterial::try_new(
                second_receipt.clone(),
                vec![record(
                    material_receipt.source_id().as_str(),
                    0,
                    second_received_at,
                    second_payload.clone(),
                )?],
            ),
            Err(ProviderCaptureError::MaterialBindingMismatch)
        );
        let second = ProviderCaptureMaterial::try_new(
            second_receipt,
            vec![record(
                second_source_id.as_str(),
                0,
                second_received_at,
                second_payload,
            )?],
        )?;
        let graph = ProviderCaptureMaterial::try_combine_request_graph(
            material_receipt.source_id().clone(),
            material_receipt.metadata_revision().clone(),
            material_receipt.dataset().clone(),
            digest(33),
            vec![material, second],
        )?;
        assert_eq!(
            graph.receipt().terminal(),
            ProviderCaptureTerminalDisposition::CompleteRequestGraph
        );
        assert_eq!(graph.receipt().request_graph_components().len(), 2);
        assert_eq!(graph.receipt().pages().len(), 3);
        assert_eq!(graph.records()[2].payload(), b"secondary");
        assert_eq!(
            graph.receipt().request_graph_components()[1].source_id(),
            &second_source_id
        );
        assert_eq!(
            graph.receipt().request_graph_components()[1].metadata_revision(),
            &second_metadata_revision
        );
        assert_eq!(graph.records()[2].source(), second_source_id.as_str());
        let reopened_graph: ProviderCaptureSetReceipt =
            serde_json::from_slice(&serde_json::to_vec(graph.receipt())?)?;
        assert_eq!(&reopened_graph, graph.receipt());
        let rebound = batch.try_bind_provider_capture(graph.receipt())?;
        assert_eq!(rebound.request().object().evidence(), &primary_evidence);
        assert_eq!(
            rebound.request().object().expected_bytes(),
            primary_expected_bytes
        );
        assert_eq!(rebound.records()[0].evidence(), &record_evidence);
        assert_eq!(rebound.records()[0].payload(), &payload);
        assert_eq!(
            rebound.request().object().capture_identity(),
            crate::SourceObjectCaptureIdentity::try_from_capture(graph.receipt())?
        );

        let component = &graph.receipt().request_graph_components()[1];
        let component_identity = crate::SourceObjectCaptureIdentity::Paged {
            content_digest: component.content_digest(),
            page_count: component.page_count(),
            terminal: component.terminal(),
        };
        let component_discovery = DiscoveryRequest::try_new(
            component.dataset().clone(),
            None,
            NonZeroU16::MIN,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let component_object = SourceObject::try_new_with_capture_identity(
            component.source_id().clone(),
            component.metadata_revision().clone(),
            &component_discovery,
            SourceIdentifier::try_from("fixture-component-object")?,
            SourceIdentifier::try_from("application-json")?,
            ExactPayloadEvidence::from_content_digest(component.content_digest()),
            component_identity,
            EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
            None,
            AvailabilityEvidence::LocalFirstObserved {
                observed_at: Timestamp::from_unix_nanos(3),
            },
            Some(component.total_body_bytes()),
        )?;
        let component_request = ExtractionRequest::try_new(
            component_object,
            NonZeroU32::MIN,
            NonZeroU64::new(100_000).ok_or("fixture byte bound")?,
            Timestamp::from_unix_nanos(1_000),
        )?;
        let component_payload = Bytes::from_static(b"secondary-normalized");
        let component_record_evidence =
            ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                Sha256::digest(&component_payload).into(),
            ));
        let component_batch = ExtractionBatch::try_new(
            &component_request,
            vec![ExtractionRecord::try_new(
                &component_request,
                SourceIdentifier::try_from("schema-v1")?,
                component_record_evidence,
                Timestamp::from_unix_nanos(2),
                None,
                AvailabilityEvidence::LocalFirstObserved {
                    observed_at: Timestamp::from_unix_nanos(3),
                },
                SourceIdentifier::try_from("record-r1")?,
                None,
                component_payload,
            )?],
        )?;
        let temporary = TemporaryDirectory::new();
        let paths = LocalPaths::prepare(temporary.path())?;
        let store = paths.sealed_research_journal_store()?;
        let (graph_expectation, graph_seal_request) = graph.into_component_seal_parts()?;
        let sealed = graph_seal_request.seal(&store)?;
        let mut component_tokens = graph_expectation
            .try_rejoin(sealed)?
            .try_into_components()?
            .into_tokens()
            .into_vec()
            .into_iter();
        let component_zero = component_tokens.next().ok_or("component zero")?;
        let component_one = component_tokens.next().ok_or("component one")?;
        assert!(component_tokens.next().is_none());
        let build_native_lineage = |batch: &ExtractionBatch| {
            let mut builder = ProviderNativeLineageBatchBuilder::try_new(
                ProviderNativeLineageImplementation::BlsTimeseriesV1,
                batch,
            )?;
            builder.try_push(&"provider-native-row")?;
            builder.finish()
        };
        let wrong_native_lineage = build_native_lineage(&component_batch)?;
        assert!(matches!(
            SealedProviderCaptureBinding::try_component(
                component_zero,
                component_batch.clone(),
                wrong_native_lineage,
                vec![2],
            ),
            Err(ProviderCaptureError::SealedBindingMismatch)
        ));
        let native_lineage = build_native_lineage(&component_batch)?;
        let component_binding = SealedProviderCaptureBinding::try_component(
            component_one,
            component_batch.clone(),
            native_lineage,
            vec![2],
        )?;
        assert_eq!(
            component_binding.scope(),
            ProviderCaptureScope::RequestGraphComponent { ordinal: 1 }
        );
        assert_eq!(component_binding.component_ordinal(), Some(1));
        assert_eq!(component_binding.record_count(), 1);
        assert_eq!(component_binding.row_frames()[0].capture_page_ordinal(), 2);
        let binding_digest = component_binding.evidence_digest();
        assert_eq!(binding_digest, component_binding.evidence_digest());
        assert_eq!(
            binding_digest.evidence().algorithm(),
            DigestAlgorithm::Sha256
        );
        assert_eq!(
            binding_digest.evidence().bytes(),
            [
                0xc8, 0xb6, 0xba, 0x0a, 0xd3, 0xfd, 0x27, 0x8f, 0x8a, 0xec, 0x74, 0x83, 0xc4, 0xf7,
                0x48, 0x18, 0xbb, 0xe7, 0xa8, 0xab, 0x46, 0x8b, 0x75, 0x2d, 0x2c, 0xfc, 0xd1, 0xa8,
                0x0d, 0x75, 0x76, 0xe8,
            ]
        );
        assert_ne!(
            binding_digest.evidence(),
            component_binding.capture_evidence().observation_digest()
        );
        assert_ne!(
            binding_digest.evidence(),
            component_binding.sealed_capture_receipt_digest()
        );
        assert_ne!(
            binding_digest.evidence(),
            component_binding.native_lineage().batch_digest()
        );
        let persisted_row = &component_binding.row_frames()[0];
        let persisted_row_frames = [ProviderCaptureRowFrameEvidence::try_new(
            persisted_row.canonical_row_ordinal(),
            persisted_row.capture_page_ordinal(),
            persisted_row.segment_ordinal(),
            persisted_row.physical_frame_ordinal(),
            persisted_row.page_body_digest(),
            persisted_row.received_at(),
            persisted_row.source_sequence(),
        )?];
        let persisted_segment = component_binding
            .persisted_segment_receipt(0)
            .ok_or("persisted component segment")?;
        let persisted_physical_claims = [ProviderCapturePhysicalClaimEvidenceRef::try_new(
            persisted_segment.capture().content_digest(),
            persisted_segment.capture().observation_digest(),
            persisted_segment.receipt_digest(),
            persisted_segment.segment().claim(),
        )?];
        let persisted_content = component_binding.content_identity();
        let persisted_native = component_binding.native_lineage();
        let persisted_native_schema = persisted_native.schema();
        let persisted_native_row = &persisted_native.rows()[0];
        let persisted_native_rows = [ProviderNativeLineageRowEvidenceRef::try_new(
            persisted_native_row.ordinal(),
            persisted_native_row.canonical_record_digest(),
            persisted_native_row.semantic_payload(),
            persisted_native_row.semantic_payload_digest(),
        )?];
        verify_provider_native_lineage_batch_evidence(
            persisted_native.batch_digest(),
            persisted_native_schema.version(),
            persisted_native_schema.implementation(),
            persisted_native_schema.fingerprint(),
            persisted_content.digest(),
            persisted_content.record_count(),
            &persisted_native_rows,
            None,
        )?;
        assert_eq!(
            verify_provider_native_lineage_batch_evidence(
                digest(90),
                persisted_native_schema.version(),
                persisted_native_schema.implementation(),
                persisted_native_schema.fingerprint(),
                persisted_content.digest(),
                persisted_content.record_count(),
                &persisted_native_rows,
                None,
            ),
            Err(ProviderNativeLineageError::AlignmentMismatch)
        );
        ProviderCaptureBindingDigest::verify_evidence(
            binding_digest.evidence(),
            component_binding.capture_evidence(),
            component_binding.sealed_capture_receipt_digest(),
            component_binding.scope(),
            component_binding.layout(),
            persisted_content.digest(),
            persisted_content.record_count(),
            component_binding.record_count(),
            persisted_native_schema.version(),
            persisted_native_schema.implementation(),
            persisted_native_schema.fingerprint(),
            persisted_native.batch_digest(),
            persisted_native.rows().len(),
            &persisted_row_frames,
            &persisted_physical_claims,
        )?;
        assert_eq!(
            ProviderCaptureBindingDigest::verify_evidence(
                digest(91),
                component_binding.capture_evidence(),
                component_binding.sealed_capture_receipt_digest(),
                component_binding.scope(),
                component_binding.layout(),
                persisted_content.digest(),
                persisted_content.record_count(),
                component_binding.record_count(),
                persisted_native_schema.version(),
                persisted_native_schema.implementation(),
                persisted_native_schema.fingerprint(),
                persisted_native.batch_digest(),
                persisted_native.rows().len(),
                &persisted_row_frames,
                &persisted_physical_claims,
            ),
            Err(ProviderCaptureError::SealedBindingMismatch)
        );
        component_binding.validate()?;
        assert_not_impl_any!(ProviderCaptureBindingDigest: serde::Serialize, serde::de::DeserializeOwned);
        assert_not_impl_any!(ProviderCaptureRowFrameEvidence: Clone, serde::Serialize, serde::de::DeserializeOwned);
        assert_not_impl_any!(ProviderCapturePhysicalClaimEvidenceRef<'static>: Clone, serde::Serialize, serde::de::DeserializeOwned);
        assert_not_impl_any!(ProviderNativeLineageRowEvidenceRef<'static>: Clone, serde::Serialize, serde::de::DeserializeOwned);
        assert_not_impl_any!(SealedProviderCaptureBinding: Clone, serde::Serialize);

        let native_lineage = build_native_lineage(&component_batch)?;
        let replay_native_digest = build_native_lineage(&component_batch)?.batch_digest();
        assert_eq!(native_lineage.batch_digest(), replay_native_digest);
        assert_eq!(native_lineage.schema().version(), 2);
        assert_eq!(
            native_lineage.schema().implementation(),
            ProviderNativeLineageImplementation::BlsTimeseriesV1
        );
        assert_eq!(native_lineage.rows().len(), component_batch.records().len());
        assert_eq!(native_lineage.rows()[0].ordinal(), 0);
        assert_eq!(
            native_lineage.rows()[0].canonical_record_digest(),
            component_batch.records()[0].evidence().content_digest()
        );
        assert_eq!(
            serde_json::from_slice::<String>(native_lineage.rows()[0].semantic_payload())?,
            "provider-native-row"
        );
        native_lineage.validate(&component_batch)?;
        let incomplete_native_lineage = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::BlsTimeseriesV1,
            &component_batch,
        )?;
        assert_eq!(
            incomplete_native_lineage.finish(),
            Err(ProviderNativeLineageError::RowCountMismatch {
                expected: 1,
                observed: 0,
            })
        );
        let mut oversized_native_lineage = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::BlsTimeseriesV1,
            &component_batch,
        )?;
        let oversized_semantics = "x".repeat(MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES);
        assert_eq!(
            oversized_native_lineage.try_push(&oversized_semantics),
            Err(ProviderNativeLineageError::RowByteLimitExceeded {
                ordinal: 0,
                max: MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES,
            })
        );
        assert_not_impl_any!(ProviderNativeLineageBatchBuilder<'static>: Clone, serde::Serialize);
        assert_not_impl_any!(ProviderNativeLineageBatch: Clone, serde::Serialize);

        let event_records = vec![
            record(
                "fixture-live",
                0,
                Timestamp::from_unix_nanos(400),
                vec![41, 42],
            )?,
            record(
                "fixture-live",
                1,
                Timestamp::from_unix_nanos(500),
                vec![51, 52, 53],
            )?,
        ];
        let event_material = ProviderEventMicrobatchMaterial::try_new(
            SourceId::try_from("fixture-live")?,
            MetadataRevision::new(SourceIdentifier::try_from("fixture-live-r1")?),
            SourceIdentifier::try_from("fixture-live-dataset")?,
            SourceIdentifier::try_from("fixture-live-stream")?,
            event_records,
        )?;
        let expected_event_receipt = event_material.receipt().clone();
        let (event_expectation, event_seal_request) = event_material.into_sealing_parts();
        let event_token = event_expectation.try_rejoin(event_seal_request.seal(&store)?)?;
        assert_eq!(
            event_token.persisted_receipt().capture(),
            &expected_event_receipt
        );
        assert_eq!(event_token.persisted_receipt().segment().frames().len(), 2);
        assert_ne!(
            event_token.persisted_receipt().receipt_digest().bytes(),
            [0; 32]
        );
        assert_not_impl_any!(ProviderEventMicrobatchToken: Clone, serde::Serialize);

        let mismatch_material = ProviderEventMicrobatchMaterial::try_new(
            SourceId::try_from("fixture-live")?,
            MetadataRevision::new(SourceIdentifier::try_from("fixture-live-r1")?),
            SourceIdentifier::try_from("fixture-live-dataset")?,
            SourceIdentifier::try_from("fixture-live-stream")?,
            vec![record(
                "fixture-live",
                2,
                Timestamp::from_unix_nanos(600),
                vec![61],
            )?],
        )?;
        let (mismatch_expectation, _mismatch_request) = mismatch_material.into_sealing_parts();
        let crosswire_material = ProviderEventMicrobatchMaterial::try_new(
            SourceId::try_from("fixture-live")?,
            MetadataRevision::new(SourceIdentifier::try_from("fixture-live-r1")?),
            SourceIdentifier::try_from("fixture-live-dataset")?,
            SourceIdentifier::try_from("fixture-live-stream")?,
            vec![record(
                "fixture-live",
                3,
                Timestamp::from_unix_nanos(700),
                vec![71],
            )?],
        )?;
        let (_crosswire_expectation, crosswire_request) = crosswire_material.into_sealing_parts();
        assert!(matches!(
            mismatch_expectation.try_rejoin(crosswire_request.seal(&store)?),
            Err(ProviderCaptureError::SealedBindingMismatch)
        ));

        let mut reversed = first.pages().to_vec();
        reversed.reverse();
        assert_eq!(
            ProviderCaptureSetReceipt::try_new(
                first.source_id().clone(),
                first.metadata_revision().clone(),
                first.dataset().clone(),
                first.request_set_identity(),
                first.terminal(),
                reversed,
            ),
            Err(ProviderCaptureError::PageOrderingInvalid)
        );
        let mut unterminated = first.pages().to_vec();
        unterminated[1] = ProviderCapturePageReceipt::try_new(
            1,
            digest(4),
            Some(digest(9)),
            Some(digest(10)),
            200,
            20,
            digest(5),
            Timestamp::from_unix_nanos(200),
        )?;
        assert_eq!(
            ProviderCaptureSetReceipt::try_new(
                first.source_id().clone(),
                first.metadata_revision().clone(),
                first.dataset().clone(),
                first.request_set_identity(),
                first.terminal(),
                unterminated,
            ),
            Err(ProviderCaptureError::TerminalDispositionInvalid)
        );
        Ok(())
    }
}
