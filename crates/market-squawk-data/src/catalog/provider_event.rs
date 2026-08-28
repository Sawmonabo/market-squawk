//! Durable typed live-event publication evidence and composite snapshot/event edges.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use market_squawk_platform::SealedResearchJournalSegmentClaim;
use market_squawk_sources::{
    MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS, MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES,
    PROVIDER_MARKET_EVENT_SCHEMA_VERSION, ProviderCaptureSetReceipt,
    ProviderCompositeResponseEventBindingDigest, ProviderEventMicrobatchBindingDigest,
    ProviderEventMicrobatchReceipt, ProviderEventMicrobatchRowFrameEvidence,
    ProviderMarketEventNativeLineageRowEvidenceRef, ProviderNativeLineageBatchSidecarEvidenceRef,
    ProviderResponseMarketEventBindingDigest, ProviderResponseMarketEventRowFrameEvidence,
    SealedProviderCompositeResponseEventBinding, SealedProviderEventMicrobatchBinding,
    SealedProviderPublicationBinding, SealedProviderResponseMarketEventBinding, SourceMetadata,
    verify_provider_market_event_native_lineage_batch_evidence,
};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::provider_capture::{
    MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES, MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS,
    native_implementation_name, parse_native_implementation, parse_source_sequence,
    raw_claim_digest, source_sequence_blob,
};
use super::storage::{append_audit, parse_digest, sha256};
use super::{Catalog, CatalogError};

const EVENT_BINDING_FORMAT_VERSION: i64 = 1;
const MAX_EVENT_NATIVE_BYTES: usize = 64 * 1024 * 1024;
const MAX_EVENT_CLAIM_JSON_BYTES: usize = 2 * 1024 * 1024;
const EVENT_ROW_MAPPING_DIGEST_DOMAIN: &[u8] = b"market-squawk/provider-event-binding/row-map/v1";

/// One exact persisted canonical/native/logical-frame/physical-frame coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderEventBindingRow {
    canonical_row_ordinal: u32,
    canonical_event_digest: EvidenceDigest,
    native_semantic_payload: Vec<u8>,
    native_semantic_digest: EvidenceDigest,
    event_frame_ordinal: u16,
    physical_frame_ordinal: u32,
    event_id: [u8; 16],
    connection_id: [u8; 16],
    payload_digest: EvidenceDigest,
    exchange_at: Option<Timestamp>,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl PersistedProviderEventBindingRow {
    /// Returns the contiguous canonical event ordinal.
    pub const fn canonical_row_ordinal(&self) -> u32 {
        self.canonical_row_ordinal
    }

    /// Returns SHA-256 of the exact canonical typed-event JSON.
    pub const fn canonical_event_digest(&self) -> EvidenceDigest {
        self.canonical_event_digest
    }

    /// Returns exact bounded provider-native row semantics.
    pub fn native_semantic_payload(&self) -> &[u8] {
        &self.native_semantic_payload
    }

    /// Returns SHA-256 of the exact provider-native row semantics.
    pub const fn native_semantic_digest(&self) -> EvidenceDigest {
        self.native_semantic_digest
    }

    /// Returns the exact logical stream-frame ordinal.
    pub const fn event_frame_ordinal(&self) -> u16 {
        self.event_frame_ordinal
    }

    /// Returns the exact immutable journal frame ordinal.
    pub const fn physical_frame_ordinal(&self) -> u32 {
        self.physical_frame_ordinal
    }

    /// Returns the locally assigned source-event identity.
    pub const fn event_id(&self) -> [u8; 16] {
        self.event_id
    }

    /// Returns the exact connection-generation identity.
    pub const fn connection_id(&self) -> [u8; 16] {
        self.connection_id
    }

    /// Returns SHA-256 of the exact provider frame bytes.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    /// Returns the source-authored event time when supplied.
    pub const fn exchange_at(&self) -> Option<Timestamp> {
        self.exchange_at
    }

    /// Returns the exact local socket-boundary receive time.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the source sequence when supplied.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }
}

/// Value-only provider-native live-event schema retained across restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderEventNativeLineage {
    schema_version: u16,
    implementation: String,
    row_count: usize,
    batch_digest: EvidenceDigest,
    batch_sidecar: Option<Vec<u8>>,
    batch_sidecar_digest: Option<EvidenceDigest>,
}

impl PersistedProviderEventNativeLineage {
    /// Returns the common typed-event lineage schema version.
    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    /// Returns the exact closed provider-native implementation.
    pub fn implementation(&self) -> &str {
        &self.implementation
    }

    /// Returns the exact native row count.
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the common-owned native batch digest.
    pub const fn batch_digest(&self) -> EvidenceDigest {
        self.batch_digest
    }

    /// Returns optional exact batch-level provider-native semantics.
    pub fn batch_sidecar_semantic_payload(&self) -> Option<&[u8]> {
        self.batch_sidecar.as_deref()
    }

    /// Returns SHA-256 of optional batch-level provider-native semantics.
    pub const fn batch_sidecar_semantic_payload_digest(&self) -> Option<EvidenceDigest> {
        self.batch_sidecar_digest
    }
}

/// Historical value evidence for one sealed typed live-event microbatch.
///
/// This value cannot recreate process-local publication authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderEventBindingEvidence {
    binding_digest: EvidenceDigest,
    capture: ProviderEventMicrobatchReceipt,
    sealed_event_receipt_digest: EvidenceDigest,
    canonical_schema_fingerprint: EvidenceDigest,
    canonical_content_digest: EvidenceDigest,
    canonical_event_count: usize,
    native_lineage: PersistedProviderEventNativeLineage,
    row_mapping_digest: EvidenceDigest,
    rows: Vec<PersistedProviderEventBindingRow>,
    raw_claim_digest: EvidenceDigest,
    physical_claim: SealedResearchJournalSegmentClaim,
}

impl PersistedProviderEventBindingEvidence {
    /// Returns the exact common-owned event binding digest.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    /// Returns exact logical stream-microbatch evidence.
    pub const fn capture(&self) -> &ProviderEventMicrobatchReceipt {
        &self.capture
    }

    /// Returns the digest joining logical event evidence to its immutable raw object.
    pub const fn sealed_event_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_event_receipt_digest
    }

    /// Returns the code-owned canonical typed-event schema fingerprint.
    pub const fn canonical_schema_fingerprint(&self) -> EvidenceDigest {
        self.canonical_schema_fingerprint
    }

    /// Returns deterministic identity over the exact ordered canonical events.
    pub const fn canonical_content_digest(&self) -> EvidenceDigest {
        self.canonical_content_digest
    }

    /// Returns the exact canonical event count.
    pub const fn canonical_event_count(&self) -> usize {
        self.canonical_event_count
    }

    /// Returns provider-native typed-event lineage evidence.
    pub const fn native_lineage(&self) -> &PersistedProviderEventNativeLineage {
        &self.native_lineage
    }

    /// Returns compact identity over every row/frame coordinate.
    pub const fn row_mapping_digest(&self) -> EvidenceDigest {
        self.row_mapping_digest
    }

    /// Returns exact ordered row/frame/native evidence.
    pub fn rows(&self) -> &[PersistedProviderEventBindingRow] {
        &self.rows
    }

    /// Returns the canonical serialized-claim digest.
    pub const fn raw_claim_digest(&self) -> EvidenceDigest {
        self.raw_claim_digest
    }

    /// Returns the immutable raw journal claim.
    pub const fn physical_claim(&self) -> &SealedResearchJournalSegmentClaim {
        &self.physical_claim
    }

    pub(crate) fn verify_integrity(&self) -> Result<(), CatalogError> {
        if self.canonical_event_count == 0
            || self.canonical_event_count > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
            || self.canonical_event_count != self.rows.len()
            || self.native_lineage.row_count != self.rows.len()
            || self.capture.frames().is_empty()
            || self.capture.frames().len() != self.physical_claim.frames().len()
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let claim_json = serde_json::to_vec(&self.physical_claim)?;
        if claim_json.len() > MAX_EVENT_CLAIM_JSON_BYTES
            || raw_claim_digest(&claim_json) != self.raw_claim_digest
            || self.physical_claim.frames().len() != self.capture.frames().len()
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let mut native_bytes = self
            .native_lineage
            .batch_sidecar
            .as_ref()
            .map_or(0, Vec::len);
        if native_bytes > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let mut native_rows = Vec::new();
        native_rows
            .try_reserve_exact(self.rows.len())
            .map_err(|_| CatalogError::Allocation)?;
        let mut row_frames = Vec::new();
        row_frames
            .try_reserve_exact(self.rows.len())
            .map_err(|_| CatalogError::Allocation)?;
        for (ordinal, row) in self.rows.iter().enumerate() {
            native_bytes = native_bytes
                .checked_add(row.native_semantic_payload.len())
                .ok_or(CatalogError::ProviderEventMismatch)?;
            if native_bytes > MAX_EVENT_NATIVE_BYTES
                || row.canonical_row_ordinal
                    != u32::try_from(ordinal).map_err(|_| CatalogError::ProviderEventMismatch)?
                || row.native_semantic_payload.is_empty()
                || sha256_evidence(&row.native_semantic_payload) != row.native_semantic_digest
            {
                return Err(CatalogError::ProviderEventMismatch);
            }
            let logical = self
                .capture
                .frames()
                .get(usize::from(row.event_frame_ordinal))
                .ok_or(CatalogError::ProviderEventMismatch)?;
            let physical = self
                .physical_claim
                .frames()
                .get(
                    usize::try_from(row.physical_frame_ordinal)
                        .map_err(|_| CatalogError::ProviderEventMismatch)?,
                )
                .ok_or(CatalogError::ProviderEventMismatch)?;
            if logical.event_id() != row.event_id
                || logical.connection_id() != row.connection_id
                || logical.payload_digest() != row.payload_digest
                || logical.exchange_at() != row.exchange_at
                || logical.received_at() != row.received_at
                || logical.source_sequence() != row.source_sequence
                || physical.ordinal() != row.physical_frame_ordinal
                || physical.provider_payload_digest() != row.payload_digest
                || physical.received_at() != row.received_at
                || physical.source_sequence() != row.source_sequence
            {
                return Err(CatalogError::ProviderEventMismatch);
            }
            native_rows.push(
                ProviderMarketEventNativeLineageRowEvidenceRef::try_new(
                    row.canonical_row_ordinal,
                    row.canonical_event_digest,
                    &row.native_semantic_payload,
                    row.native_semantic_digest,
                )
                .map_err(|_| CatalogError::ProviderEventMismatch)?,
            );
            row_frames.push(
                ProviderEventMicrobatchRowFrameEvidence::try_new(
                    row.canonical_row_ordinal,
                    row.event_frame_ordinal,
                    row.physical_frame_ordinal,
                    row.event_id,
                    row.connection_id,
                    row.payload_digest,
                    row.exchange_at,
                    row.received_at,
                    row.source_sequence,
                )
                .map_err(|_| CatalogError::ProviderEventMismatch)?,
            );
        }
        let sidecar = match (
            self.native_lineage.batch_sidecar.as_deref(),
            self.native_lineage.batch_sidecar_digest,
        ) {
            (Some(payload), Some(digest)) => Some(
                ProviderNativeLineageBatchSidecarEvidenceRef::try_new(payload, digest)
                    .map_err(|_| CatalogError::ProviderEventMismatch)?,
            ),
            (None, None) => None,
            _ => return Err(CatalogError::ProviderEventMismatch),
        };
        let implementation = parse_native_implementation(&self.native_lineage.implementation)?;
        verify_provider_market_event_native_lineage_batch_evidence(
            self.native_lineage.batch_digest,
            self.native_lineage.schema_version,
            implementation,
            self.canonical_schema_fingerprint,
            self.canonical_content_digest,
            self.canonical_event_count,
            &native_rows,
            sidecar.as_ref(),
        )
        .map_err(|_| CatalogError::ProviderEventMismatch)?;
        ProviderEventMicrobatchBindingDigest::verify_evidence(
            self.binding_digest,
            &self.capture,
            self.sealed_event_receipt_digest,
            &self.physical_claim,
            self.canonical_schema_fingerprint,
            self.canonical_content_digest,
            self.canonical_event_count,
            implementation,
            self.native_lineage.batch_digest,
            self.native_lineage.row_count,
            &row_frames,
        )
        .map_err(|_| CatalogError::ProviderEventMismatch)?;
        if event_row_mapping_digest(&self.rows)? != self.row_mapping_digest {
            return Err(CatalogError::ProviderEventMismatch);
        }
        Ok(())
    }
}

/// One exact persisted typed HTTP-response event row coordinate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderResponseMarketEventBindingRow {
    canonical_row_ordinal: u32,
    canonical_event_digest: EvidenceDigest,
    native_semantic_payload: Vec<u8>,
    native_semantic_digest: EvidenceDigest,
    capture_page_ordinal: u16,
    physical_frame_ordinal: u32,
    payload_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl PersistedProviderResponseMarketEventBindingRow {
    pub const fn canonical_row_ordinal(&self) -> u32 {
        self.canonical_row_ordinal
    }
    pub const fn canonical_event_digest(&self) -> EvidenceDigest {
        self.canonical_event_digest
    }
    pub fn native_semantic_payload(&self) -> &[u8] {
        &self.native_semantic_payload
    }
    pub const fn native_semantic_digest(&self) -> EvidenceDigest {
        self.native_semantic_digest
    }
    pub const fn capture_page_ordinal(&self) -> u16 {
        self.capture_page_ordinal
    }
    pub const fn physical_frame_ordinal(&self) -> u32 {
        self.physical_frame_ordinal
    }
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }
}

/// Historical value evidence for typed canonical events decoded from one sealed HTTP response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedProviderResponseMarketEventBindingEvidence {
    binding_digest: EvidenceDigest,
    capture: ProviderCaptureSetReceipt,
    sealed_capture_receipt_digest: EvidenceDigest,
    canonical_schema_fingerprint: EvidenceDigest,
    canonical_content_digest: EvidenceDigest,
    canonical_event_count: usize,
    native_lineage: PersistedProviderEventNativeLineage,
    row_mapping_digest: EvidenceDigest,
    rows: Vec<PersistedProviderResponseMarketEventBindingRow>,
    raw_claim_digest: EvidenceDigest,
    physical_claim: SealedResearchJournalSegmentClaim,
}

impl PersistedProviderResponseMarketEventBindingEvidence {
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
    pub const fn capture(&self) -> &ProviderCaptureSetReceipt {
        &self.capture
    }
    pub const fn sealed_capture_receipt_digest(&self) -> EvidenceDigest {
        self.sealed_capture_receipt_digest
    }
    pub const fn canonical_schema_fingerprint(&self) -> EvidenceDigest {
        self.canonical_schema_fingerprint
    }
    pub const fn canonical_content_digest(&self) -> EvidenceDigest {
        self.canonical_content_digest
    }
    pub const fn canonical_event_count(&self) -> usize {
        self.canonical_event_count
    }
    pub const fn native_lineage(&self) -> &PersistedProviderEventNativeLineage {
        &self.native_lineage
    }
    pub const fn row_mapping_digest(&self) -> EvidenceDigest {
        self.row_mapping_digest
    }
    pub fn rows(&self) -> &[PersistedProviderResponseMarketEventBindingRow] {
        &self.rows
    }
    pub const fn raw_claim_digest(&self) -> EvidenceDigest {
        self.raw_claim_digest
    }
    pub const fn physical_claim(&self) -> &SealedResearchJournalSegmentClaim {
        &self.physical_claim
    }

    pub(crate) fn verify_integrity(&self) -> Result<(), CatalogError> {
        if self.canonical_event_count == 0
            || self.canonical_event_count > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
            || self.canonical_event_count != self.rows.len()
            || self.native_lineage.row_count != self.rows.len()
            || self.capture.pages().is_empty()
            || self.capture.pages().len() != self.physical_claim.frames().len()
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let claim_json = serde_json::to_vec(&self.physical_claim)?;
        if claim_json.len() > MAX_EVENT_CLAIM_JSON_BYTES
            || raw_claim_digest(&claim_json) != self.raw_claim_digest
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let mut native_bytes = self
            .native_lineage
            .batch_sidecar
            .as_ref()
            .map_or(0, Vec::len);
        if native_bytes > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let mut native_rows = Vec::new();
        native_rows
            .try_reserve_exact(self.rows.len())
            .map_err(|_| CatalogError::Allocation)?;
        let mut row_frames = Vec::new();
        row_frames
            .try_reserve_exact(self.rows.len())
            .map_err(|_| CatalogError::Allocation)?;
        for (ordinal, row) in self.rows.iter().enumerate() {
            native_bytes = native_bytes
                .checked_add(row.native_semantic_payload.len())
                .ok_or(CatalogError::ProviderEventMismatch)?;
            if native_bytes > MAX_EVENT_NATIVE_BYTES
                || row.canonical_row_ordinal
                    != u32::try_from(ordinal).map_err(|_| CatalogError::ProviderEventMismatch)?
                || row.native_semantic_payload.is_empty()
                || sha256_evidence(&row.native_semantic_payload) != row.native_semantic_digest
            {
                return Err(CatalogError::ProviderEventMismatch);
            }
            let page = self
                .capture
                .pages()
                .get(usize::from(row.capture_page_ordinal))
                .ok_or(CatalogError::ProviderEventMismatch)?;
            let frame = self
                .physical_claim
                .frames()
                .get(
                    usize::try_from(row.physical_frame_ordinal)
                        .map_err(|_| CatalogError::ProviderEventMismatch)?,
                )
                .ok_or(CatalogError::ProviderEventMismatch)?;
            if page.body_digest() != row.payload_digest
                || page.received_at() != row.received_at
                || frame.ordinal() != row.physical_frame_ordinal
                || frame.provider_payload_digest() != row.payload_digest
                || frame.received_at() != row.received_at
                || frame.source_sequence() != row.source_sequence
            {
                return Err(CatalogError::ProviderEventMismatch);
            }
            native_rows.push(
                ProviderMarketEventNativeLineageRowEvidenceRef::try_new(
                    row.canonical_row_ordinal,
                    row.canonical_event_digest,
                    &row.native_semantic_payload,
                    row.native_semantic_digest,
                )
                .map_err(|_| CatalogError::ProviderEventMismatch)?,
            );
            row_frames.push(
                ProviderResponseMarketEventRowFrameEvidence::try_new(
                    row.canonical_row_ordinal,
                    row.capture_page_ordinal,
                    0,
                    row.physical_frame_ordinal,
                    row.payload_digest,
                    row.received_at,
                    row.source_sequence,
                )
                .map_err(|_| CatalogError::ProviderEventMismatch)?,
            );
        }
        let sidecar = persisted_sidecar(&self.native_lineage)?;
        let implementation = parse_native_implementation(&self.native_lineage.implementation)?;
        verify_provider_market_event_native_lineage_batch_evidence(
            self.native_lineage.batch_digest,
            self.native_lineage.schema_version,
            implementation,
            self.canonical_schema_fingerprint,
            self.canonical_content_digest,
            self.canonical_event_count,
            &native_rows,
            sidecar.as_ref(),
        )
        .map_err(|_| CatalogError::ProviderEventMismatch)?;
        ProviderResponseMarketEventBindingDigest::verify_evidence(
            self.binding_digest,
            &self.capture,
            self.sealed_capture_receipt_digest,
            &self.physical_claim,
            self.canonical_schema_fingerprint,
            self.canonical_content_digest,
            self.canonical_event_count,
            implementation,
            self.native_lineage.batch_digest,
            self.native_lineage.row_count,
            &row_frames,
        )
        .map_err(|_| CatalogError::ProviderEventMismatch)?;
        if response_event_row_mapping_digest(&self.rows)? != self.row_mapping_digest {
            return Err(CatalogError::ProviderEventMismatch);
        }
        Ok(())
    }
}

/// Historical tagged provider publication evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PersistedProviderPublicationEvidence {
    /// Typed canonical market events decoded from one sealed HTTP response.
    ResponseMarketEvent(PersistedProviderResponseMarketEventBindingEvidence),
    /// A pure ordered live-event microbatch.
    EventMicrobatch(PersistedProviderEventBindingEvidence),
    /// An exact response snapshot followed by one ordered live-event microbatch.
    CompositeResponseEvent {
        /// Complete response snapshot evidence.
        response: PersistedProviderResponseMarketEventBindingEvidence,
        /// Complete event microbatch evidence.
        event: PersistedProviderEventBindingEvidence,
        /// Digest joining the two independently verified sub-bindings.
        composite_binding_digest: EvidenceDigest,
    },
}

impl PersistedProviderPublicationEvidence {
    /// Returns the kind-qualified publication digest retained by a run/generation.
    pub const fn publication_digest(&self) -> EvidenceDigest {
        match self {
            Self::ResponseMarketEvent(response) => response.binding_digest,
            Self::EventMicrobatch(event) => event.binding_digest,
            Self::CompositeResponseEvent {
                composite_binding_digest,
                ..
            } => *composite_binding_digest,
        }
    }

    /// Returns the closed durable publication kind stored in schema metadata and the catalog.
    pub const fn publication_kind(&self) -> &'static str {
        match self {
            Self::ResponseMarketEvent(_) => "response_market_event",
            Self::EventMicrobatch(_) => "event_microbatch",
            Self::CompositeResponseEvent { .. } => "composite_response_event",
        }
    }

    /// Returns the exact event sub-binding.
    pub const fn event(&self) -> Option<&PersistedProviderEventBindingEvidence> {
        match self {
            Self::ResponseMarketEvent(_) => None,
            Self::EventMicrobatch(event) | Self::CompositeResponseEvent { event, .. } => {
                Some(event)
            }
        }
    }

    /// Returns the response sub-binding only for a composite publication.
    pub const fn response(&self) -> Option<&PersistedProviderResponseMarketEventBindingEvidence> {
        match self {
            Self::ResponseMarketEvent(response) | Self::CompositeResponseEvent { response, .. } => {
                Some(response)
            }
            Self::EventMicrobatch(_) => None,
        }
    }

    pub(crate) fn verify_integrity(&self) -> Result<(), CatalogError> {
        match self {
            Self::ResponseMarketEvent(response) => response.verify_integrity(),
            Self::EventMicrobatch(event) => event.verify_integrity(),
            Self::CompositeResponseEvent {
                response,
                event,
                composite_binding_digest,
            } => {
                response.verify_integrity()?;
                event.verify_integrity()?;
                if response.capture().source_id() != event.capture().source_id()
                    || response.capture().metadata_revision() != event.capture().metadata_revision()
                {
                    return Err(CatalogError::ProviderEventMismatch);
                }
                ProviderCompositeResponseEventBindingDigest::verify_evidence(
                    *composite_binding_digest,
                    response.binding_digest(),
                    event.binding_digest(),
                    response.canonical_event_count(),
                    event.canonical_event_count,
                )
                .map_err(|_| CatalogError::ProviderEventMismatch)
            }
        }
    }
}

/// Value-only prepared publication consumed by the sole SQLite publication transaction.
#[derive(Debug)]
pub(crate) enum PreparedProviderPublicationBinding {
    ResponseMarketEvent(PreparedProviderResponseMarketEventBinding),
    EventMicrobatch(PreparedProviderEventBinding),
    CompositeResponseEvent {
        response: PreparedProviderResponseMarketEventBinding,
        event: PreparedProviderEventBinding,
        composite_binding_digest: EvidenceDigest,
        response_row_count: usize,
    },
}

impl PreparedProviderPublicationBinding {
    pub(crate) fn try_from_live(
        binding: &SealedProviderPublicationBinding,
    ) -> Result<Self, CatalogError> {
        match binding {
            SealedProviderPublicationBinding::ResponseSet(_) => {
                Err(CatalogError::ProviderEventMismatch)
            }
            SealedProviderPublicationBinding::ResponseMarketEvent(binding) => {
                Ok(Self::ResponseMarketEvent(
                    PreparedProviderResponseMarketEventBinding::try_from_live(binding)?,
                ))
            }
            SealedProviderPublicationBinding::EventMicrobatch(binding) => Ok(
                Self::EventMicrobatch(PreparedProviderEventBinding::try_from_live(binding)?),
            ),
            SealedProviderPublicationBinding::CompositeResponseEvent(binding) => {
                prepare_composite(binding)
            }
        }
    }

    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        match self {
            Self::ResponseMarketEvent(binding) => binding.evidence.binding_digest,
            Self::EventMicrobatch(binding) => binding.evidence.binding_digest,
            Self::CompositeResponseEvent {
                composite_binding_digest,
                ..
            } => *composite_binding_digest,
        }
    }

    pub(crate) fn matches_persisted(
        &self,
        persisted: &PersistedProviderPublicationEvidence,
    ) -> bool {
        match (self, persisted) {
            (
                Self::ResponseMarketEvent(prepared),
                PersistedProviderPublicationEvidence::ResponseMarketEvent(stored),
            ) => prepared.evidence == *stored,
            (
                Self::EventMicrobatch(prepared),
                PersistedProviderPublicationEvidence::EventMicrobatch(stored),
            ) => prepared.evidence == *stored,
            (
                Self::CompositeResponseEvent {
                    response,
                    event,
                    composite_binding_digest,
                    ..
                },
                PersistedProviderPublicationEvidence::CompositeResponseEvent {
                    response: stored_response,
                    event: stored_event,
                    composite_binding_digest: stored_digest,
                },
            ) => {
                response.evidence == *stored_response
                    && event.evidence == *stored_event
                    && composite_binding_digest == stored_digest
            }
            _ => false,
        }
    }
}

#[derive(Debug)]
pub(crate) struct PreparedProviderResponseMarketEventBinding {
    evidence: PersistedProviderResponseMarketEventBindingEvidence,
}

impl PreparedProviderResponseMarketEventBinding {
    fn try_from_live(
        binding: &SealedProviderResponseMarketEventBinding,
    ) -> Result<Self, CatalogError> {
        binding
            .validate()
            .map_err(|_| CatalogError::ProviderEventMismatch)?;
        let record_count = binding.record_count();
        if record_count == 0
            || record_count > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
            || record_count != binding.batch().events().len()
            || record_count != binding.native_lineage().rows().len()
            || record_count != binding.row_frames().len()
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let claim = binding.persisted_receipt().segment().claim().clone();
        let claim_json = serde_json::to_vec(&claim)?;
        if claim_json.len() > MAX_EVENT_CLAIM_JSON_BYTES {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        let native = binding.native_lineage();
        let sidecar = native.batch_sidecar().map(|sidecar| sidecar.to_vec());
        let mut native_bytes = sidecar.as_ref().map_or(0, Vec::len);
        let mut rows = Vec::new();
        rows.try_reserve_exact(record_count)
            .map_err(|_| CatalogError::Allocation)?;
        for (ordinal, (native_row, coordinate)) in
            native.rows().iter().zip(binding.row_frames()).enumerate()
        {
            native_bytes = native_bytes
                .checked_add(native_row.len())
                .ok_or(CatalogError::ProviderEventMismatch)?;
            if native_bytes > MAX_EVENT_NATIVE_BYTES {
                return Err(CatalogError::ResultByteLimitExceeded);
            }
            let mut native_payload = Vec::new();
            native_payload
                .try_reserve_exact(native_row.len())
                .map_err(|_| CatalogError::Allocation)?;
            native_payload.extend_from_slice(native_row);
            rows.push(PersistedProviderResponseMarketEventBindingRow {
                canonical_row_ordinal: u32::try_from(ordinal)
                    .map_err(|_| CatalogError::ProviderEventMismatch)?,
                canonical_event_digest: binding
                    .batch()
                    .canonical_event_digest(ordinal)
                    .ok_or(CatalogError::ProviderEventMismatch)?,
                native_semantic_payload: native_payload,
                native_semantic_digest: native
                    .row_digest(ordinal)
                    .ok_or(CatalogError::ProviderEventMismatch)?,
                capture_page_ordinal: coordinate.capture_page_ordinal(),
                physical_frame_ordinal: coordinate.physical_frame_ordinal(),
                payload_digest: coordinate.page_body_digest(),
                received_at: coordinate.received_at(),
                source_sequence: coordinate.source_sequence(),
            });
        }
        let content = binding.content_identity();
        let evidence = PersistedProviderResponseMarketEventBindingEvidence {
            binding_digest: binding.evidence_digest().evidence(),
            capture: binding.capture_evidence().clone(),
            sealed_capture_receipt_digest: binding.sealed_receipt_digest(),
            canonical_schema_fingerprint: content.schema_fingerprint(),
            canonical_content_digest: content.content_digest(),
            canonical_event_count: content.event_count(),
            native_lineage: PersistedProviderEventNativeLineage {
                schema_version: PROVIDER_MARKET_EVENT_SCHEMA_VERSION,
                implementation: native_implementation_name(native.implementation()).to_owned(),
                row_count: native.rows().len(),
                batch_digest: native.batch_digest(),
                batch_sidecar: sidecar,
                batch_sidecar_digest: native.batch_sidecar_digest(),
            },
            row_mapping_digest: response_event_row_mapping_digest(&rows)?,
            rows,
            raw_claim_digest: raw_claim_digest(&claim_json),
            physical_claim: claim,
        };
        evidence.verify_integrity()?;
        Ok(Self { evidence })
    }
}

#[derive(Debug)]
pub(crate) struct PreparedProviderEventBinding {
    evidence: PersistedProviderEventBindingEvidence,
}

impl PreparedProviderEventBinding {
    fn try_from_live(binding: &SealedProviderEventMicrobatchBinding) -> Result<Self, CatalogError> {
        binding
            .validate()
            .map_err(|_| CatalogError::ProviderEventMismatch)?;
        let record_count = binding.record_count();
        if record_count == 0
            || record_count > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS
            || record_count != binding.batch().events().len()
            || record_count != binding.native_lineage().rows().len()
            || record_count != binding.row_frames().len()
        {
            return Err(CatalogError::ProviderEventMismatch);
        }
        let claim = binding.persisted_receipt().segment().claim().clone();
        let claim_json = serde_json::to_vec(&claim)?;
        if claim_json.len() > MAX_EVENT_CLAIM_JSON_BYTES {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        let native = binding.native_lineage();
        let sidecar = native.batch_sidecar().map(|sidecar| sidecar.to_vec());
        let mut native_bytes = sidecar.as_ref().map_or(0, Vec::len);
        let mut rows = Vec::new();
        rows.try_reserve_exact(record_count)
            .map_err(|_| CatalogError::Allocation)?;
        for (ordinal, (native_row, coordinate)) in
            native.rows().iter().zip(binding.row_frames()).enumerate()
        {
            native_bytes = native_bytes
                .checked_add(native_row.len())
                .ok_or(CatalogError::ProviderEventMismatch)?;
            if native_bytes > MAX_EVENT_NATIVE_BYTES {
                return Err(CatalogError::ResultByteLimitExceeded);
            }
            let mut native_payload = Vec::new();
            native_payload
                .try_reserve_exact(native_row.len())
                .map_err(|_| CatalogError::Allocation)?;
            native_payload.extend_from_slice(native_row);
            rows.push(PersistedProviderEventBindingRow {
                canonical_row_ordinal: u32::try_from(ordinal)
                    .map_err(|_| CatalogError::ProviderEventMismatch)?,
                canonical_event_digest: binding
                    .batch()
                    .canonical_event_digest(ordinal)
                    .ok_or(CatalogError::ProviderEventMismatch)?,
                native_semantic_payload: native_payload,
                native_semantic_digest: native
                    .row_digest(ordinal)
                    .ok_or(CatalogError::ProviderEventMismatch)?,
                event_frame_ordinal: coordinate.event_frame_ordinal(),
                physical_frame_ordinal: coordinate.physical_frame_ordinal(),
                event_id: coordinate.event_id(),
                connection_id: coordinate.connection_id(),
                payload_digest: coordinate.payload_digest(),
                exchange_at: coordinate.exchange_at(),
                received_at: coordinate.received_at(),
                source_sequence: coordinate.source_sequence(),
            });
        }
        let content = binding.content_identity();
        let evidence = PersistedProviderEventBindingEvidence {
            binding_digest: binding.evidence_digest().evidence(),
            capture: binding.capture_evidence().clone(),
            sealed_event_receipt_digest: binding.sealed_receipt_digest(),
            canonical_schema_fingerprint: content.schema_fingerprint(),
            canonical_content_digest: content.content_digest(),
            canonical_event_count: content.event_count(),
            native_lineage: PersistedProviderEventNativeLineage {
                schema_version: PROVIDER_MARKET_EVENT_SCHEMA_VERSION,
                implementation: native_implementation_name(native.implementation()).to_owned(),
                row_count: native.rows().len(),
                batch_digest: native.batch_digest(),
                batch_sidecar: sidecar,
                batch_sidecar_digest: native.batch_sidecar_digest(),
            },
            row_mapping_digest: event_row_mapping_digest(&rows)?,
            rows,
            raw_claim_digest: raw_claim_digest(&claim_json),
            physical_claim: claim,
        };
        evidence.verify_integrity()?;
        Ok(Self { evidence })
    }
}

fn prepare_composite(
    binding: &SealedProviderCompositeResponseEventBinding,
) -> Result<PreparedProviderPublicationBinding, CatalogError> {
    let response = PreparedProviderResponseMarketEventBinding::try_from_live(binding.response())?;
    let event = PreparedProviderEventBinding::try_from_live(binding.event())?;
    let response_row_count = binding.response().record_count();
    ProviderCompositeResponseEventBindingDigest::verify_evidence(
        binding.evidence_digest().evidence(),
        binding.response().evidence_digest().evidence(),
        binding.event().evidence_digest().evidence(),
        response_row_count,
        binding.event().record_count(),
    )
    .map_err(|_| CatalogError::ProviderEventMismatch)?;
    Ok(PreparedProviderPublicationBinding::CompositeResponseEvent {
        response,
        event,
        composite_binding_digest: binding.evidence_digest().evidence(),
        response_row_count,
    })
}

impl Catalog {
    /// Reopens and verifies one exact typed event sub-binding after restart.
    pub fn provider_event_binding_evidence(
        &self,
        binding_digest: EvidenceDigest,
    ) -> Result<Option<PersistedProviderEventBindingEvidence>, CatalogError> {
        load_provider_event_binding_evidence(&self.connection, binding_digest)
    }

    /// Reopens and verifies one exact tagged event/composite publication after restart.
    pub fn provider_publication_evidence(
        &self,
        publication_digest: EvidenceDigest,
    ) -> Result<Option<PersistedProviderPublicationEvidence>, CatalogError> {
        load_provider_publication_evidence(&self.connection, publication_digest)
    }

    pub(crate) fn provider_publication_for_run(
        &self,
        run_id: Uuid,
    ) -> Result<Option<PersistedProviderPublicationEvidence>, CatalogError> {
        load_provider_publication_for_run(&self.connection, run_id)
    }
}

pub(crate) fn retain_prepared_provider_publication_binding(
    connection: &Transaction<'_>,
    run_id: Uuid,
    prepared: &PreparedProviderPublicationBinding,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    match prepared {
        PreparedProviderPublicationBinding::ResponseMarketEvent(response) => {
            retain_response_event_binding_evidence(connection, run_id, response, recorded_at)?;
            associate_provider_publication(
                connection,
                run_id,
                response.evidence.capture.source_id().as_str(),
                "response_market_event",
                response.evidence.binding_digest,
                Some(response.evidence.binding_digest),
                None,
                None,
                recorded_at,
            )?;
        }
        PreparedProviderPublicationBinding::EventMicrobatch(event) => {
            retain_event_binding_evidence(connection, run_id, event, recorded_at)?;
            associate_provider_publication(
                connection,
                run_id,
                event.evidence.capture.source_id().as_str(),
                "event_microbatch",
                event.evidence.binding_digest,
                None,
                Some(event.evidence.binding_digest),
                None,
                recorded_at,
            )?;
        }
        PreparedProviderPublicationBinding::CompositeResponseEvent {
            response,
            event,
            composite_binding_digest,
            response_row_count,
        } => {
            retain_response_event_binding_evidence(connection, run_id, response, recorded_at)?;
            retain_event_binding_evidence(connection, run_id, event, recorded_at)?;
            let event_count = event.evidence.canonical_event_count;
            let inserted = connection.execute(
                "INSERT OR IGNORE INTO provider_composite_response_event_bindings
                 (composite_binding_digest, response_binding_digest, event_binding_digest,
                  response_row_count, event_row_count, recorded_at_ns)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    digest_bytes(*composite_binding_digest),
                    digest_bytes(response.evidence.binding_digest),
                    digest_bytes(event.evidence.binding_digest),
                    to_i64(*response_row_count)?,
                    to_i64(event_count)?,
                    recorded_at.unix_nanos(),
                ],
            )?;
            if inserted > 1 {
                return Err(CatalogError::ProviderEventConflict);
            }
            associate_provider_publication(
                connection,
                run_id,
                event.evidence.capture.source_id().as_str(),
                "composite_response_event",
                *composite_binding_digest,
                Some(response.evidence.binding_digest),
                Some(event.evidence.binding_digest),
                Some(*composite_binding_digest),
                recorded_at,
            )?;
        }
    }
    let retained = load_provider_publication_evidence(connection, prepared.publication_digest())?
        .ok_or(CatalogError::ProviderEventConflict)?;
    retained.verify_integrity()?;
    if retained.publication_digest() != prepared.publication_digest() {
        return Err(CatalogError::ProviderEventConflict);
    }
    Ok(())
}

fn retain_response_event_binding_evidence(
    connection: &Transaction<'_>,
    run_id: Uuid,
    prepared: &PreparedProviderResponseMarketEventBinding,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let evidence = &prepared.evidence;
    evidence.verify_integrity()?;
    validate_response_event_source_revision(connection, run_id, &evidence.capture)?;
    require_raw_claim_capacity(
        connection,
        evidence.raw_claim_digest,
        &evidence.physical_claim,
    )?;
    insert_response_event_capture(connection, evidence, recorded_at)?;
    insert_response_event_binding(connection, evidence, recorded_at)?;
    let retained =
        load_provider_response_event_binding_evidence(connection, evidence.binding_digest)?
            .ok_or(CatalogError::ProviderEventConflict)?;
    if retained != *evidence {
        return Err(CatalogError::ProviderEventConflict);
    }
    Ok(())
}

fn retain_event_binding_evidence(
    connection: &Transaction<'_>,
    run_id: Uuid,
    prepared: &PreparedProviderEventBinding,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let evidence = &prepared.evidence;
    evidence.verify_integrity()?;
    validate_event_source_revision(connection, run_id, &evidence.capture)?;
    require_event_claim_capacity(connection, evidence)?;
    insert_event_capture(connection, evidence, recorded_at)?;
    insert_event_binding(connection, evidence, recorded_at)?;
    let retained = load_provider_event_binding_evidence(connection, evidence.binding_digest)?
        .ok_or(CatalogError::ProviderEventConflict)?;
    if retained != *evidence {
        return Err(CatalogError::ProviderEventConflict);
    }
    Ok(())
}

fn validate_event_source_revision(
    connection: &Connection,
    run_id: Uuid,
    capture: &ProviderEventMicrobatchReceipt,
) -> Result<(), CatalogError> {
    validate_source_revision_identity(
        connection,
        run_id,
        capture.source_id().as_str(),
        capture.metadata_revision().as_source_identifier().as_str(),
    )
}

fn validate_response_event_source_revision(
    connection: &Connection,
    run_id: Uuid,
    capture: &ProviderCaptureSetReceipt,
) -> Result<(), CatalogError> {
    validate_source_revision_identity(
        connection,
        run_id,
        capture.source_id().as_str(),
        capture.metadata_revision().as_source_identifier().as_str(),
    )
}

fn validate_source_revision_identity(
    connection: &Connection,
    run_id: Uuid,
    capture_source_id: &str,
    capture_revision: &str,
) -> Result<(), CatalogError> {
    let (run_source, revision_digest, metadata_json): (String, Vec<u8>, String) = connection
        .query_row(
            "SELECT run.source_id, source.current_revision_digest, revision.metadata_json
             FROM ingest_runs AS run
             JOIN sources AS source ON source.source_id=run.source_id
             JOIN source_revisions AS revision
               ON revision.source_id=source.source_id
              AND revision.revision_digest=source.current_revision_digest
             WHERE run.run_id=?1 AND run.state='reserved'",
            [run_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    if run_source != capture_source_id
        || revision_digest.as_slice() != sha256(metadata_json.as_bytes())
    {
        return Err(CatalogError::ProviderEventMismatch);
    }
    let source: SourceMetadata = serde_json::from_str(&metadata_json)?;
    if source.source_id().as_str() != capture_source_id
        || source.revision().as_source_identifier().as_str() != capture_revision
    {
        return Err(CatalogError::ProviderEventMismatch);
    }
    Ok(())
}

fn insert_response_event_capture(
    connection: &Connection,
    evidence: &PersistedProviderResponseMarketEventBindingEvidence,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let capture = &evidence.capture;
    let capture_json = serde_json::to_string(capture)?;
    if capture_json.len() > MAX_EVENT_CLAIM_JSON_BYTES {
        return Err(CatalogError::ResultByteLimitExceeded);
    }
    let source_revision_digest: Vec<u8> = connection.query_row(
        "SELECT current_revision_digest FROM sources WHERE source_id=?1",
        [capture.source_id().as_str()],
        |row| row.get(0),
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO provider_raw_observations
         (capture_observation_digest, capture_content_digest, source_id,
          source_revision_digest, metadata_revision, provider_dataset,
          request_set_identity, terminal_disposition, page_count, total_body_bytes,
          capture_json, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            digest_bytes(capture.observation_digest()),
            digest_bytes(capture.content_digest()),
            capture.source_id().as_str(),
            source_revision_digest,
            capture.metadata_revision().as_source_identifier().as_str(),
            capture.dataset().as_str(),
            digest_bytes(capture.request_set_identity()),
            capture_terminal_name(capture.terminal()),
            to_i64(capture.pages().len())?,
            to_i64(capture.total_body_bytes())?,
            capture_json,
            recorded_at.unix_nanos(),
        ],
    )?;
    for page in capture.pages() {
        connection.execute(
            "INSERT OR IGNORE INTO provider_raw_observation_pages
             (capture_observation_digest, page_ordinal, request_identity,
              request_page_token_digest, response_next_page_token_digest, http_status,
              body_bytes, body_digest, received_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                digest_bytes(capture.observation_digest()),
                i64::from(page.ordinal()),
                digest_bytes(page.request_identity()),
                page.request_page_token_digest().map(digest_bytes),
                page.response_next_page_token_digest().map(digest_bytes),
                i64::from(page.http_status()),
                to_i64(page.body_bytes())?,
                digest_bytes(page.body_digest()),
                page.received_at().unix_nanos(),
            ],
        )?;
    }
    let claim = &evidence.physical_claim;
    let claim_json = serde_json::to_string(claim)?;
    if claim_json.len() > MAX_EVENT_CLAIM_JSON_BYTES
        || raw_claim_digest(claim_json.as_bytes()) != evidence.raw_claim_digest
    {
        return Err(CatalogError::ProviderEventMismatch);
    }
    connection.execute(
        "INSERT OR IGNORE INTO sealed_raw_objects
         (raw_claim_digest, physical_receipt_digest, relative_reference,
          content_digest, size_bytes, frame_count, raw_claim_json, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            digest_bytes(evidence.raw_claim_digest),
            digest_bytes(claim.physical_receipt_digest()),
            claim.relative_reference(),
            digest_bytes(claim.content_digest()),
            to_i64(claim.size_bytes())?,
            to_i64(claim.frames().len())?,
            claim_json,
            recorded_at.unix_nanos(),
        ],
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO provider_raw_observation_objects
         (capture_observation_digest, input_ordinal, raw_claim_digest,
          physical_receipt_digest, object_capture_content_digest,
          object_capture_observation_digest, capture_receipt_digest)
         VALUES (?1, 0, ?2, ?3, ?4, ?5, ?6)",
        params![
            digest_bytes(capture.observation_digest()),
            digest_bytes(evidence.raw_claim_digest),
            digest_bytes(claim.physical_receipt_digest()),
            digest_bytes(capture.content_digest()),
            digest_bytes(capture.observation_digest()),
            digest_bytes(evidence.sealed_capture_receipt_digest),
        ],
    )?;
    for frame in claim.frames() {
        connection.execute(
            "INSERT OR IGNORE INTO provider_raw_observation_frames
             (capture_observation_digest, observation_unit_ordinal,
              raw_object_input_ordinal, raw_claim_digest, physical_receipt_digest,
              raw_unit_ordinal, frame_offset, framed_bytes, provider_payload_bytes,
              provider_payload_digest, received_at_ns, source_sequence)
             VALUES (?1, ?2, 0, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                digest_bytes(capture.observation_digest()),
                i64::from(frame.ordinal()),
                digest_bytes(evidence.raw_claim_digest),
                digest_bytes(claim.physical_receipt_digest()),
                i64::from(frame.ordinal()),
                to_i64(frame.offset())?,
                to_i64(frame.framed_bytes())?,
                to_i64(frame.provider_payload_bytes())?,
                digest_bytes(frame.provider_payload_digest()),
                frame.received_at().unix_nanos(),
                source_sequence_blob(frame.source_sequence()),
            ],
        )?;
    }
    Ok(())
}

fn insert_response_event_binding(
    connection: &Connection,
    evidence: &PersistedProviderResponseMarketEventBindingEvidence,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    connection.execute(
        "INSERT OR IGNORE INTO provider_response_market_event_bindings
         (response_event_binding_digest, binding_format_version,
          capture_observation_digest, sealed_capture_receipt_digest,
          canonical_schema_fingerprint, canonical_content_digest,
          canonical_event_count, row_mapping_digest, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            digest_bytes(evidence.binding_digest),
            EVENT_BINDING_FORMAT_VERSION,
            digest_bytes(evidence.capture.observation_digest()),
            digest_bytes(evidence.sealed_capture_receipt_digest),
            digest_bytes(evidence.canonical_schema_fingerprint),
            digest_bytes(evidence.canonical_content_digest),
            to_i64(evidence.canonical_event_count)?,
            digest_bytes(evidence.row_mapping_digest),
            recorded_at.unix_nanos(),
        ],
    )?;
    let native = &evidence.native_lineage;
    connection.execute(
        "INSERT OR IGNORE INTO provider_response_market_event_binding_native_lineage
         (response_event_binding_digest, schema_version, implementation, row_count,
          batch_digest, batch_sidecar_payload, batch_sidecar_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            digest_bytes(evidence.binding_digest),
            i64::from(native.schema_version),
            native.implementation,
            to_i64(native.row_count)?,
            digest_bytes(native.batch_digest),
            native.batch_sidecar.as_deref(),
            native.batch_sidecar_digest.map(digest_bytes),
        ],
    )?;
    for row in &evidence.rows {
        connection.execute(
            "INSERT OR IGNORE INTO provider_response_market_event_binding_rows
             (response_event_binding_digest, capture_observation_digest,
              canonical_row_ordinal, canonical_event_digest, native_semantic_payload,
              native_semantic_digest, capture_page_ordinal, physical_frame_ordinal,
              payload_digest, received_at_ns, source_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                digest_bytes(evidence.binding_digest),
                digest_bytes(evidence.capture.observation_digest()),
                i64::from(row.canonical_row_ordinal),
                digest_bytes(row.canonical_event_digest),
                row.native_semantic_payload,
                digest_bytes(row.native_semantic_digest),
                i64::from(row.capture_page_ordinal),
                i64::from(row.physical_frame_ordinal),
                digest_bytes(row.payload_digest),
                row.received_at.unix_nanos(),
                source_sequence_blob(row.source_sequence),
            ],
        )?;
    }
    Ok(())
}

fn require_event_claim_capacity(
    connection: &Connection,
    evidence: &PersistedProviderEventBindingEvidence,
) -> Result<(), CatalogError> {
    require_raw_claim_capacity(
        connection,
        evidence.raw_claim_digest,
        &evidence.physical_claim,
    )
}

fn require_raw_claim_capacity(
    connection: &Connection,
    raw_claim_digest: EvidenceDigest,
    physical_claim: &SealedResearchJournalSegmentClaim,
) -> Result<(), CatalogError> {
    let exists: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sealed_raw_objects WHERE raw_claim_digest=?1)",
        [digest_bytes(raw_claim_digest)],
        |row| row.get(0),
    )?;
    if exists {
        return Ok(());
    }
    let (retained, retained_bytes): (i64, i64) = connection.query_row(
        "SELECT physical_claims, physical_bytes
         FROM provider_capture_recovery_capacity WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let retained = usize::try_from(retained).map_err(|_| CatalogError::CorruptCatalog)?;
    let retained_bytes = u64::try_from(retained_bytes).map_err(|_| CatalogError::CorruptCatalog)?;
    if retained
        .checked_add(1)
        .is_none_or(|total| total > MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS)
        || retained_bytes
            .checked_add(physical_claim.size_bytes())
            .is_none_or(|total| total > MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES)
    {
        return Err(CatalogError::ProviderCaptureCapacityExceeded {
            max_claims: MAX_PROVIDER_CAPTURE_PHYSICAL_CLAIMS,
            max_bytes: MAX_PROVIDER_CAPTURE_PHYSICAL_BYTES,
        });
    }
    Ok(())
}

fn insert_event_capture(
    connection: &Connection,
    evidence: &PersistedProviderEventBindingEvidence,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let capture = &evidence.capture;
    let capture_json = serde_json::to_string(capture)?;
    if capture_json.len() > MAX_EVENT_CLAIM_JSON_BYTES {
        return Err(CatalogError::ResultByteLimitExceeded);
    }
    let source_revision_digest: Vec<u8> = connection.query_row(
        "SELECT current_revision_digest FROM sources WHERE source_id=?1",
        [capture.source_id().as_str()],
        |row| row.get(0),
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO provider_event_microbatches
         (event_observation_digest, event_content_digest, source_id, source_revision_digest,
          dataset, stream_identity, frame_count, total_payload_bytes, capture_json, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            digest_bytes(capture.observation_digest()),
            digest_bytes(capture.content_digest()),
            capture.source_id().as_str(),
            source_revision_digest,
            capture.dataset().as_str(),
            capture.stream_identity().as_str(),
            to_i64(capture.frames().len())?,
            to_i64(capture.total_payload_bytes())?,
            capture_json,
            recorded_at.unix_nanos(),
        ],
    )?;
    for frame in capture.frames() {
        connection.execute(
            "INSERT OR IGNORE INTO provider_event_microbatch_frames
             (event_observation_digest, event_frame_ordinal, event_id, connection_id,
              source_sequence, exchange_at_ns, received_at_ns, payload_bytes, payload_digest)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                digest_bytes(capture.observation_digest()),
                i64::from(frame.ordinal()),
                frame.event_id().as_slice(),
                frame.connection_id().as_slice(),
                source_sequence_blob(frame.source_sequence()),
                frame.exchange_at().map(Timestamp::unix_nanos),
                frame.received_at().unix_nanos(),
                to_i64(frame.payload_bytes())?,
                digest_bytes(frame.payload_digest()),
            ],
        )?;
    }
    let claim = &evidence.physical_claim;
    let claim_json = serde_json::to_string(claim)?;
    connection.execute(
        "INSERT OR IGNORE INTO sealed_raw_objects
         (raw_claim_digest, physical_receipt_digest, relative_reference,
          content_digest, size_bytes, frame_count, raw_claim_json, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            digest_bytes(evidence.raw_claim_digest),
            digest_bytes(claim.physical_receipt_digest()),
            claim.relative_reference(),
            digest_bytes(claim.content_digest()),
            to_i64(claim.size_bytes())?,
            to_i64(claim.frames().len())?,
            claim_json,
            recorded_at.unix_nanos(),
        ],
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO provider_event_microbatch_objects
         (event_observation_digest, raw_claim_digest, physical_receipt_digest,
          sealed_event_receipt_digest) VALUES (?1, ?2, ?3, ?4)",
        params![
            digest_bytes(capture.observation_digest()),
            digest_bytes(evidence.raw_claim_digest),
            digest_bytes(claim.physical_receipt_digest()),
            digest_bytes(evidence.sealed_event_receipt_digest),
        ],
    )?;
    Ok(())
}

fn insert_event_binding(
    connection: &Connection,
    evidence: &PersistedProviderEventBindingEvidence,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    connection.execute(
        "INSERT OR IGNORE INTO provider_event_bindings
         (event_binding_digest, binding_format_version, event_observation_digest,
          sealed_event_receipt_digest, canonical_schema_fingerprint,
          canonical_content_digest, canonical_event_count, row_mapping_digest, recorded_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            digest_bytes(evidence.binding_digest),
            EVENT_BINDING_FORMAT_VERSION,
            digest_bytes(evidence.capture.observation_digest()),
            digest_bytes(evidence.sealed_event_receipt_digest),
            digest_bytes(evidence.canonical_schema_fingerprint),
            digest_bytes(evidence.canonical_content_digest),
            to_i64(evidence.canonical_event_count)?,
            digest_bytes(evidence.row_mapping_digest),
            recorded_at.unix_nanos(),
        ],
    )?;
    let native = &evidence.native_lineage;
    connection.execute(
        "INSERT OR IGNORE INTO provider_event_binding_native_lineage
         (event_binding_digest, schema_version, implementation, row_count, batch_digest,
          batch_sidecar_payload, batch_sidecar_digest)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            digest_bytes(evidence.binding_digest),
            i64::from(native.schema_version),
            native.implementation,
            to_i64(native.row_count)?,
            digest_bytes(native.batch_digest),
            native.batch_sidecar.as_deref(),
            native.batch_sidecar_digest.map(digest_bytes),
        ],
    )?;
    for row in &evidence.rows {
        connection.execute(
            "INSERT OR IGNORE INTO provider_event_binding_rows
             (event_binding_digest, event_observation_digest, canonical_row_ordinal,
              canonical_event_digest, native_semantic_payload, native_semantic_digest,
              event_frame_ordinal, physical_frame_ordinal, event_id, connection_id,
              payload_digest, exchange_at_ns, received_at_ns, source_sequence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                digest_bytes(evidence.binding_digest),
                digest_bytes(evidence.capture.observation_digest()),
                i64::from(row.canonical_row_ordinal),
                digest_bytes(row.canonical_event_digest),
                row.native_semantic_payload,
                digest_bytes(row.native_semantic_digest),
                i64::from(row.event_frame_ordinal),
                i64::from(row.physical_frame_ordinal),
                row.event_id.as_slice(),
                row.connection_id.as_slice(),
                digest_bytes(row.payload_digest),
                row.exchange_at.map(Timestamp::unix_nanos),
                row.received_at.unix_nanos(),
                source_sequence_blob(row.source_sequence),
            ],
        )?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the closed publication tag and each independently verified edge stay explicit"
)]
fn associate_provider_publication(
    connection: &Transaction<'_>,
    run_id: Uuid,
    source_id: &str,
    kind: &str,
    publication_digest: EvidenceDigest,
    response_digest: Option<EvidenceDigest>,
    event_digest: Option<EvidenceDigest>,
    composite_digest: Option<EvidenceDigest>,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let used: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM ingest_run_provider_publication_bindings
            WHERE publication_digest=?1
               OR (?2 IS NOT NULL AND response_binding_digest=?2)
               OR (?3 IS NOT NULL AND event_binding_digest=?3)
         )",
        params![
            digest_bytes(publication_digest),
            response_digest.map(digest_bytes),
            event_digest.map(digest_bytes),
        ],
        |row| row.get(0),
    )?;
    if used {
        return Err(CatalogError::ProviderEventConflict);
    }
    let inserted = connection.execute(
        "INSERT INTO ingest_run_provider_publication_bindings
         (run_id, input_ordinal, publication_digest, publication_kind, source_id,
          response_binding_digest, event_binding_digest, composite_binding_digest)
         VALUES (?1, 0, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            run_id.to_string(),
            digest_bytes(publication_digest),
            kind,
            source_id,
            response_digest.map(digest_bytes),
            event_digest.map(digest_bytes),
            composite_digest.map(digest_bytes),
        ],
    )?;
    if inserted != 1 {
        return Err(CatalogError::ProviderEventConflict);
    }
    append_audit(
        connection,
        "provider-event-publication.retained",
        &run_id.to_string(),
        publication_digest.bytes(),
        recorded_at,
    )?;
    Ok(())
}

pub(crate) fn load_provider_publication_for_run(
    connection: &Connection,
    run_id: Uuid,
) -> Result<Option<PersistedProviderPublicationEvidence>, CatalogError> {
    let digest = connection
        .query_row(
            "SELECT publication_digest FROM ingest_run_provider_publication_bindings
             WHERE run_id=?1",
            [run_id.to_string()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()?
        .map(|value| parse_digest(1, &value))
        .transpose()?;
    digest
        .map(|digest| load_provider_publication_evidence(connection, digest))
        .transpose()
        .map(Option::flatten)
}

fn load_provider_publication_evidence(
    connection: &Connection,
    publication_digest: EvidenceDigest,
) -> Result<Option<PersistedProviderPublicationEvidence>, CatalogError> {
    type PublicationHeader = (String, Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>);
    let header: Option<PublicationHeader> = connection
        .query_row(
            "SELECT publication_kind, response_binding_digest, event_binding_digest,
                    composite_binding_digest
             FROM ingest_run_provider_publication_bindings WHERE publication_digest=?1",
            [digest_bytes(publication_digest)],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((kind, response, event, composite)) = header else {
        return Ok(None);
    };
    let evidence = match (kind.as_str(), response, composite) {
        ("response_market_event", Some(response), None) if event.is_none() => {
            let response_digest = parse_digest(1, &response)?;
            if response_digest != publication_digest {
                return Err(CatalogError::CorruptCatalog);
            }
            let response =
                load_provider_response_event_binding_evidence(connection, response_digest)?
                    .ok_or(CatalogError::CorruptCatalog)?;
            PersistedProviderPublicationEvidence::ResponseMarketEvent(response)
        }
        ("event_microbatch", None, None) => {
            let event_digest = event
                .as_deref()
                .map(|value| parse_digest(1, value))
                .transpose()?
                .ok_or(CatalogError::CorruptCatalog)?;
            if publication_digest != event_digest {
                return Err(CatalogError::CorruptCatalog);
            }
            let event = load_provider_event_binding_evidence(connection, event_digest)?
                .ok_or(CatalogError::CorruptCatalog)?;
            PersistedProviderPublicationEvidence::EventMicrobatch(event)
        }
        ("composite_response_event", Some(response), Some(composite)) => {
            let response_digest = parse_digest(1, &response)?;
            let composite_digest = parse_digest(1, &composite)?;
            let event_digest = event
                .as_deref()
                .map(|value| parse_digest(1, value))
                .transpose()?
                .ok_or(CatalogError::CorruptCatalog)?;
            if composite_digest != publication_digest {
                return Err(CatalogError::CorruptCatalog);
            }
            let response =
                load_provider_response_event_binding_evidence(connection, response_digest)?
                    .ok_or(CatalogError::CorruptCatalog)?;
            let event = load_provider_event_binding_evidence(connection, event_digest)?
                .ok_or(CatalogError::CorruptCatalog)?;
            PersistedProviderPublicationEvidence::CompositeResponseEvent {
                response,
                event,
                composite_binding_digest: composite_digest,
            }
        }
        _ => return Err(CatalogError::CorruptCatalog),
    };
    evidence.verify_integrity()?;
    Ok(Some(evidence))
}

fn load_provider_response_event_binding_evidence(
    connection: &Connection,
    binding_digest: EvidenceDigest,
) -> Result<Option<PersistedProviderResponseMarketEventBindingEvidence>, CatalogError> {
    type Header = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, i64, Vec<u8>);
    let header: Option<Header> = connection
        .query_row(
            "SELECT capture_observation_digest, sealed_capture_receipt_digest,
                    canonical_schema_fingerprint, canonical_content_digest,
                    canonical_event_count, row_mapping_digest
             FROM provider_response_market_event_bindings
             WHERE response_event_binding_digest=?1",
            [digest_bytes(binding_digest)],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((observation, sealed, schema, content, event_count, row_map)) = header else {
        return Ok(None);
    };
    let observation_digest = parse_digest(1, &observation)?;
    let capture_json: String = connection.query_row(
        "SELECT capture_json FROM provider_raw_observations
         WHERE capture_observation_digest=?1",
        [digest_bytes(observation_digest)],
        |row| row.get(0),
    )?;
    if capture_json.len() > MAX_EVENT_CLAIM_JSON_BYTES {
        return Err(CatalogError::ResultByteLimitExceeded);
    }
    let capture: ProviderCaptureSetReceipt = serde_json::from_str(&capture_json)?;
    if capture.observation_digest() != observation_digest {
        return Err(CatalogError::CorruptCatalog);
    }
    type NativeHeader = (i64, String, i64, Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>);
    let native: NativeHeader = connection.query_row(
        "SELECT schema_version, implementation, row_count, batch_digest,
                batch_sidecar_payload, batch_sidecar_digest
         FROM provider_response_market_event_binding_native_lineage
         WHERE response_event_binding_digest=?1",
        [digest_bytes(binding_digest)],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    if native.4.is_some() != native.5.is_some()
        || native
            .4
            .as_ref()
            .is_some_and(|value| value.len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES)
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let row_count = usize::try_from(event_count).map_err(|_| CatalogError::CorruptCatalog)?;
    if row_count == 0 || row_count > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS {
        return Err(CatalogError::ResultRowLimitExceeded);
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .map_err(|_| CatalogError::Allocation)?;
    let mut statement = connection.prepare(
        "SELECT canonical_row_ordinal, canonical_event_digest, native_semantic_payload,
                native_semantic_digest, capture_page_ordinal, physical_frame_ordinal,
                payload_digest, received_at_ns, source_sequence
         FROM provider_response_market_event_binding_rows
         WHERE response_event_binding_digest=?1 ORDER BY canonical_row_ordinal",
    )?;
    let mut sqlite_rows = statement.query([digest_bytes(binding_digest)])?;
    let mut native_bytes = native.4.as_ref().map_or(0, Vec::len);
    while let Some(row) = sqlite_rows.next()? {
        if rows.len() == row_count {
            return Err(CatalogError::ResultRowLimitExceeded);
        }
        let native_payload: Vec<u8> = row.get(2)?;
        native_bytes = native_bytes
            .checked_add(native_payload.len())
            .ok_or(CatalogError::CorruptCatalog)?;
        if native_bytes > MAX_EVENT_NATIVE_BYTES {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        rows.push(PersistedProviderResponseMarketEventBindingRow {
            canonical_row_ordinal: u32::try_from(row.get::<_, i64>(0)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            canonical_event_digest: parse_digest(1, &row.get::<_, Vec<u8>>(1)?)?,
            native_semantic_payload: native_payload,
            native_semantic_digest: parse_digest(1, &row.get::<_, Vec<u8>>(3)?)?,
            capture_page_ordinal: u16::try_from(row.get::<_, i64>(4)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            physical_frame_ordinal: u32::try_from(row.get::<_, i64>(5)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            payload_digest: parse_digest(1, &row.get::<_, Vec<u8>>(6)?)?,
            received_at: Timestamp::from_unix_nanos(row.get(7)?),
            source_sequence: parse_source_sequence(row.get(8)?),
        });
    }
    if rows.len() != row_count {
        return Err(CatalogError::CorruptCatalog);
    }
    let (raw_claim, claim_json): (Vec<u8>, String) = connection.query_row(
        "SELECT edge.raw_claim_digest, object.raw_claim_json
         FROM provider_raw_observation_objects AS edge
         JOIN sealed_raw_objects AS object
           ON object.raw_claim_digest=edge.raw_claim_digest
          AND object.physical_receipt_digest=edge.physical_receipt_digest
         WHERE edge.capture_observation_digest=?1 AND edge.input_ordinal=0",
        [digest_bytes(observation_digest)],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if claim_json.len() > MAX_EVENT_CLAIM_JSON_BYTES {
        return Err(CatalogError::ResultByteLimitExceeded);
    }
    let evidence = PersistedProviderResponseMarketEventBindingEvidence {
        binding_digest,
        capture,
        sealed_capture_receipt_digest: parse_digest(1, &sealed)?,
        canonical_schema_fingerprint: parse_digest(1, &schema)?,
        canonical_content_digest: parse_digest(1, &content)?,
        canonical_event_count: row_count,
        native_lineage: PersistedProviderEventNativeLineage {
            schema_version: u16::try_from(native.0).map_err(|_| CatalogError::CorruptCatalog)?,
            implementation: native.1,
            row_count: usize::try_from(native.2).map_err(|_| CatalogError::CorruptCatalog)?,
            batch_digest: parse_digest(1, &native.3)?,
            batch_sidecar: native.4,
            batch_sidecar_digest: native.5.map(|value| parse_digest(1, &value)).transpose()?,
        },
        row_mapping_digest: parse_digest(1, &row_map)?,
        rows,
        raw_claim_digest: parse_digest(1, &raw_claim)?,
        physical_claim: serde_json::from_str(&claim_json)?,
    };
    evidence.verify_integrity()?;
    Ok(Some(evidence))
}

fn load_provider_event_binding_evidence(
    connection: &Connection,
    binding_digest: EvidenceDigest,
) -> Result<Option<PersistedProviderEventBindingEvidence>, CatalogError> {
    type Header = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, i64, Vec<u8>);
    let header: Option<Header> = connection
        .query_row(
            "SELECT event_observation_digest, sealed_event_receipt_digest,
                    canonical_schema_fingerprint, canonical_content_digest,
                    canonical_event_count, row_mapping_digest
             FROM provider_event_bindings WHERE event_binding_digest=?1",
            [digest_bytes(binding_digest)],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((observation, sealed, schema, content, event_count, row_map)) = header else {
        return Ok(None);
    };
    let observation_digest = parse_digest(1, &observation)?;
    let capture_json: String = connection.query_row(
        "SELECT capture_json FROM provider_event_microbatches
         WHERE event_observation_digest=?1",
        [digest_bytes(observation_digest)],
        |row| row.get(0),
    )?;
    if capture_json.len() > MAX_EVENT_CLAIM_JSON_BYTES {
        return Err(CatalogError::ResultByteLimitExceeded);
    }
    let capture: ProviderEventMicrobatchReceipt = serde_json::from_str(&capture_json)?;
    if capture.observation_digest() != observation_digest {
        return Err(CatalogError::CorruptCatalog);
    }
    type NativeHeader = (i64, String, i64, Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>);
    let native: NativeHeader = connection.query_row(
        "SELECT schema_version, implementation, row_count, batch_digest,
                batch_sidecar_payload, batch_sidecar_digest
         FROM provider_event_binding_native_lineage WHERE event_binding_digest=?1",
        [digest_bytes(binding_digest)],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;
    if native.4.is_some() != native.5.is_some()
        || native
            .4
            .as_ref()
            .is_some_and(|value| value.len() > MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES)
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let row_count = usize::try_from(event_count).map_err(|_| CatalogError::CorruptCatalog)?;
    if row_count == 0 || row_count > MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS {
        return Err(CatalogError::ResultRowLimitExceeded);
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .map_err(|_| CatalogError::Allocation)?;
    let mut statement = connection.prepare(
        "SELECT canonical_row_ordinal, canonical_event_digest, native_semantic_payload,
                native_semantic_digest, event_frame_ordinal, physical_frame_ordinal,
                event_id, connection_id, payload_digest, exchange_at_ns, received_at_ns,
                source_sequence
         FROM provider_event_binding_rows WHERE event_binding_digest=?1
         ORDER BY canonical_row_ordinal",
    )?;
    let mut sqlite_rows = statement.query([digest_bytes(binding_digest)])?;
    let mut native_bytes = native.4.as_ref().map_or(0, Vec::len);
    while let Some(row) = sqlite_rows.next()? {
        if rows.len() == row_count {
            return Err(CatalogError::ResultRowLimitExceeded);
        }
        let native_payload: Vec<u8> = row.get(2)?;
        native_bytes = native_bytes
            .checked_add(native_payload.len())
            .ok_or(CatalogError::CorruptCatalog)?;
        if native_bytes > MAX_EVENT_NATIVE_BYTES {
            return Err(CatalogError::ResultByteLimitExceeded);
        }
        rows.push(PersistedProviderEventBindingRow {
            canonical_row_ordinal: u32::try_from(row.get::<_, i64>(0)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            canonical_event_digest: parse_digest(1, &row.get::<_, Vec<u8>>(1)?)?,
            native_semantic_payload: native_payload,
            native_semantic_digest: parse_digest(1, &row.get::<_, Vec<u8>>(3)?)?,
            event_frame_ordinal: u16::try_from(row.get::<_, i64>(4)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            physical_frame_ordinal: u32::try_from(row.get::<_, i64>(5)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            event_id: row
                .get::<_, Vec<u8>>(6)?
                .try_into()
                .map_err(|_| CatalogError::CorruptCatalog)?,
            connection_id: row
                .get::<_, Vec<u8>>(7)?
                .try_into()
                .map_err(|_| CatalogError::CorruptCatalog)?,
            payload_digest: parse_digest(1, &row.get::<_, Vec<u8>>(8)?)?,
            exchange_at: row
                .get::<_, Option<i64>>(9)?
                .map(Timestamp::from_unix_nanos),
            received_at: Timestamp::from_unix_nanos(row.get(10)?),
            source_sequence: parse_source_sequence(row.get(11)?),
        });
    }
    if rows.len() != row_count {
        return Err(CatalogError::CorruptCatalog);
    }
    let (raw_claim, claim_json): (Vec<u8>, String) = connection.query_row(
        "SELECT edge.raw_claim_digest, object.raw_claim_json
         FROM provider_event_microbatch_objects AS edge
         JOIN sealed_raw_objects AS object
           ON object.raw_claim_digest=edge.raw_claim_digest
          AND object.physical_receipt_digest=edge.physical_receipt_digest
         WHERE edge.event_observation_digest=?1",
        [digest_bytes(observation_digest)],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if claim_json.len() > MAX_EVENT_CLAIM_JSON_BYTES {
        return Err(CatalogError::ResultByteLimitExceeded);
    }
    let evidence = PersistedProviderEventBindingEvidence {
        binding_digest,
        capture,
        sealed_event_receipt_digest: parse_digest(1, &sealed)?,
        canonical_schema_fingerprint: parse_digest(1, &schema)?,
        canonical_content_digest: parse_digest(1, &content)?,
        canonical_event_count: row_count,
        native_lineage: PersistedProviderEventNativeLineage {
            schema_version: u16::try_from(native.0).map_err(|_| CatalogError::CorruptCatalog)?,
            implementation: native.1,
            row_count: usize::try_from(native.2).map_err(|_| CatalogError::CorruptCatalog)?,
            batch_digest: parse_digest(1, &native.3)?,
            batch_sidecar: native.4,
            batch_sidecar_digest: native.5.map(|value| parse_digest(1, &value)).transpose()?,
        },
        row_mapping_digest: parse_digest(1, &row_map)?,
        rows,
        raw_claim_digest: parse_digest(1, &raw_claim)?,
        physical_claim: serde_json::from_str(&claim_json)?,
    };
    evidence.verify_integrity()?;
    Ok(Some(evidence))
}

fn event_row_mapping_digest(
    rows: &[PersistedProviderEventBindingRow],
) -> Result<EvidenceDigest, CatalogError> {
    let mut hash = Sha256::new();
    hash_field(&mut hash, EVENT_ROW_MAPPING_DIGEST_DOMAIN)?;
    hash.update(
        u64::try_from(rows.len())
            .map_err(|_| CatalogError::InvalidRecord)?
            .to_be_bytes(),
    );
    for row in rows {
        hash.update(row.canonical_row_ordinal.to_be_bytes());
        hash.update(row.canonical_event_digest.bytes());
        hash.update(row.native_semantic_digest.bytes());
        hash.update(row.event_frame_ordinal.to_be_bytes());
        hash.update(row.physical_frame_ordinal.to_be_bytes());
        hash.update(row.event_id);
        hash.update(row.connection_id);
        hash.update(row.payload_digest.bytes());
        match row.exchange_at {
            Some(timestamp) => {
                hash.update([1]);
                hash.update(timestamp.unix_nanos().to_be_bytes());
            }
            None => hash.update([0]),
        }
        hash.update(row.received_at.unix_nanos().to_be_bytes());
        match row.source_sequence {
            Some(sequence) => {
                hash.update([1]);
                hash.update(sequence.to_be_bytes());
            }
            None => hash.update([0]),
        }
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn response_event_row_mapping_digest(
    rows: &[PersistedProviderResponseMarketEventBindingRow],
) -> Result<EvidenceDigest, CatalogError> {
    let mut hash = Sha256::new();
    hash_field(
        &mut hash,
        b"market-squawk/provider-response-market-event-binding/row-map/v1",
    )?;
    hash.update(
        u64::try_from(rows.len())
            .map_err(|_| CatalogError::InvalidRecord)?
            .to_be_bytes(),
    );
    for row in rows {
        hash.update(row.canonical_row_ordinal.to_be_bytes());
        hash.update(row.canonical_event_digest.bytes());
        hash.update(row.native_semantic_digest.bytes());
        hash.update(row.capture_page_ordinal.to_be_bytes());
        hash.update(row.physical_frame_ordinal.to_be_bytes());
        hash.update(row.payload_digest.bytes());
        hash.update(row.received_at.unix_nanos().to_be_bytes());
        match row.source_sequence {
            Some(sequence) => {
                hash.update([1]);
                hash.update(sequence.to_be_bytes());
            }
            None => hash.update([0]),
        }
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn persisted_sidecar<'a>(
    native: &'a PersistedProviderEventNativeLineage,
) -> Result<Option<ProviderNativeLineageBatchSidecarEvidenceRef<'a>>, CatalogError> {
    match (native.batch_sidecar.as_deref(), native.batch_sidecar_digest) {
        (Some(payload), Some(digest)) => Ok(Some(
            ProviderNativeLineageBatchSidecarEvidenceRef::try_new(payload, digest)
                .map_err(|_| CatalogError::ProviderEventMismatch)?,
        )),
        (None, None) => Ok(None),
        _ => Err(CatalogError::ProviderEventMismatch),
    }
}

const fn capture_terminal_name(
    terminal: market_squawk_sources::ProviderCaptureTerminalDisposition,
) -> &'static str {
    use market_squawk_sources::ProviderCaptureTerminalDisposition as Terminal;
    match terminal {
        Terminal::StandaloneResponse => "standalone_response",
        Terminal::ExhaustedWithoutNextPage => "exhausted_without_next_page",
        Terminal::CompleteRequestGraph => "complete_request_graph",
    }
}

fn sha256_evidence(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn hash_field(hash: &mut Sha256, value: &[u8]) -> Result<(), CatalogError> {
    hash.update(
        u64::try_from(value.len())
            .map_err(|_| CatalogError::InvalidRecord)?
            .to_be_bytes(),
    );
    hash.update(value);
    Ok(())
}

const fn digest_bytes(digest: EvidenceDigest) -> [u8; 32] {
    digest.bytes()
}

fn to_i64<T>(value: T) -> Result<i64, CatalogError>
where
    i64: TryFrom<T>,
{
    i64::try_from(value).map_err(|_| CatalogError::InvalidRecord)
}
