//! Bounded, source-neutral provider response capture receipts.

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
    RawCaptureRecord, SealedResearchJournalSegmentReceipt, SealedResearchJournalStore,
    SealedResearchJournalStoreError,
};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

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
            if receipt.terminal == ProviderCaptureTerminalDisposition::CompleteRequestGraph
                && receipt
                    .request_graph_components
                    .iter()
                    .any(|component| component.first_page_ordinal == page.ordinal)
            {
                previous_received_at = None;
            }
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
                || record.source() != receipt.source_id().as_str()
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
    /// The caller supplies the provider dataset and request-set identity for the complete graph.
    /// Each component remains independently framed by its original dataset, request-set identity,
    /// terminal condition, page-token evidence, content digest, and observation digest. Flattened
    /// pages and raw records receive fresh contiguous ordinals solely for the one sealed segment;
    /// exact provider body bytes, request identities, receive times, and local event/connection
    /// identities are preserved.
    ///
    /// # Errors
    ///
    /// Rejects fewer than two components, nested request graphs, source or metadata-revision
    /// mismatches, invalid graph identity, nonmonotonic request ordering, and aggregate page/byte
    /// bounds above the existing capture-set ceilings.
    pub fn try_combine_request_graph(
        dataset: SourceIdentifier,
        request_set_identity: EvidenceDigest,
        components: Vec<Self>,
    ) -> Result<Self, ProviderCaptureError> {
        Self::try_combine_request_graph_inner(dataset, request_set_identity, components, None)
    }

    /// Combines a complete request graph with one typed, hash-bound semantic proof.
    ///
    /// The semantic is retained inside the canonical capture receipt and therefore follows the
    /// same raw-storage, run-lineage, generation-lineage, and restart verification path as its
    /// exact provider response components. The graph request identity is derived here from the
    /// semantic's versioned purpose and the exact ordered component receipts; callers cannot
    /// supply or accidentally drift that authority-critical hash.
    pub fn try_combine_request_graph_with_semantic(
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
        let first = components
            .first()
            .ok_or(ProviderCaptureError::RequestGraphInvalid)?;
        let purpose = match &semantic_binding {
            ProviderCaptureSemanticBinding::CompleteMarketBarHistoryV1(binding) => {
                binding.graph_purpose()
            }
        };
        let request_set_identity = semantic_material_request_graph_identity(
            first.receipt.source_id(),
            first.receipt.metadata_revision(),
            &dataset,
            purpose,
            &components,
        );
        Self::try_combine_request_graph_inner(
            dataset,
            request_set_identity,
            components,
            Some(semantic_binding),
        )
    }

    fn try_combine_request_graph_inner(
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
        let source_id = first.receipt.source_id.clone();
        let metadata_revision = first.receipt.metadata_revision.clone();
        let total_page_count = components.iter().try_fold(0_usize, |total, component| {
            if component.receipt.source_id != source_id
                || component.receipt.metadata_revision != metadata_revision
            {
                return Err(ProviderCaptureError::RequestGraphComponentMismatch);
            }
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
            source_id,
            metadata_revision,
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

    /// Consumes and durably seals the already validated exact provider response material.
    pub fn seal(
        self,
        store: &SealedResearchJournalStore,
    ) -> Result<SealedProviderCaptureSetReceipt, ProviderCaptureMaterialSealError> {
        let segment = store.seal(&self.records)?;
        SealedProviderCaptureSetReceipt::try_bind(self.receipt, segment).map_err(Into::into)
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
    /// Binds one completed capture to the exact sealed frame mapping.
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
            {
                return Err(ProviderCaptureError::PhysicalReceiptMismatch);
            }
        }
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/sealed-provider-capture-receipt/v1");
        hash_digest(&mut hash, capture.observation_digest);
        hash_digest(&mut hash, segment.physical_receipt_digest());
        let receipt_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into());
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
    /// A bounded allocation for capture framing could not be reserved.
    #[error("provider capture allocation failed")]
    AllocationFailed,
    /// Complete request-graph framing is missing, discontinuous, or internally inconsistent.
    #[error("provider capture request graph is invalid")]
    RequestGraphInvalid,
    /// A component belongs to a different source or metadata revision.
    #[error("provider capture request-graph component does not share source authority")]
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
            source_id,
            metadata_revision,
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
        components
            .iter()
            .map(|component| (&component.dataset, component.request_set_identity)),
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
    components: impl IntoIterator<Item = (&'a SourceIdentifier, EvidenceDigest)>,
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
    for (component_dataset, component_request_identity) in components {
        hash_field(&mut hash, component_dataset.as_str().as_bytes());
        hash.update(component_request_identity.bytes());
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
    use market_squawk_domain::{
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
        SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::RawCaptureRecord;
    use sha2::{Digest as _, Sha256};

    use super::{
        ProviderCaptureError, ProviderCaptureMaterial, ProviderCapturePageReceipt,
        ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    };
    use crate::{
        AvailabilityEvidence, DiscoveryRequest, ExtractionBatch, ExtractionError, ExtractionRecord,
        ExtractionRequest, SourceObject,
    };
    use bytes::Bytes;
    use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

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
        let second_receipt = ProviderCaptureSetReceipt::try_new(
            material_receipt.source_id().clone(),
            material_receipt.metadata_revision().clone(),
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
        let second = ProviderCaptureMaterial::try_new(
            second_receipt,
            vec![record(
                material_receipt.source_id().as_str(),
                0,
                second_received_at,
                second_payload,
            )?],
        )?;
        let graph = ProviderCaptureMaterial::try_combine_request_graph(
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
