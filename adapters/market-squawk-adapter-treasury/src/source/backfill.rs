//! Restart-safe, page-sealed acquisition for Treasury daily-rate all-history feeds.

use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{SealedResearchJournalSegmentClaim, SealedResearchJournalStore};
use market_squawk_sources::{
    CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryRequest, ExtractionBatch, ExtractionContentIdentity,
    ExtractionRecord, ExtractionRequest, ExtractionSourceError, MAX_EXTRACTION_BATCH_BYTES,
    MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, ProviderCaptureMaterial,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
    SourceObject,
};
use serde::{Deserialize, Serialize, Serializer};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

use crate::client::system_timestamp;
use crate::vertical::TreasuryExtractionAccountingInput;
use crate::{
    FiscalDataParseLimits, TreasuryDailyRatePage, TreasuryDailyRatePaginationTracker,
    TreasuryDatasetDescriptor, TreasuryDatasetFamily, TreasuryDatasetPeriod,
    TreasuryExtractionAccounting, TreasuryPublicationMode, TreasurySourceError,
};

use super::lineage::{ObjectKind, ParsedObjectId, invalid_protocol, source_object};
use super::normalize::canonical_daily_rate_records;
use super::{TreasurySource, TreasurySourceConfig};

const CHECKPOINT_POLICY_VERSION: &str = "treasury-daily-rate-all-history-checkpoint-v1";
const MAX_ALL_HISTORY_PAGES: usize = 1_024;
const MAX_ALL_HISTORY_RAW_BODY_BYTES: u64 = MAX_EXTRACTION_BATCH_BYTES;
// The strict five-family schema has at most 28 values per row; 32 leaves explicit additive headroom.
const MAX_ALL_HISTORY_CANONICAL_POINTS: u64 = 3_200_000;
const MAX_CHECKPOINT_JSON_BYTES: usize = 16 * 1024 * 1024;

/// Persisted, non-authoritative claim for one parsed and durably sealed all-history page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TreasuryAllHistoryPersistedPage {
    page_number: u64,
    returned_source_rows: u64,
    canonical_points: u64,
    raw_body_bytes: u64,
    request_digest: EvidenceDigest,
    payload_digest: EvidenceDigest,
    received_at: Timestamp,
    provider_published_at: Timestamp,
    terminal: bool,
    canonical_normalized_at: Option<Timestamp>,
    canonical_content_digest: Option<EvidenceDigest>,
    discovery_request: DiscoveryRequest,
    source_object: SourceObject,
    capture: ProviderCaptureSetReceipt,
    sealed_segment_claim: Option<SealedResearchJournalSegmentClaim>,
    sealed_receipt_digest: Option<EvidenceDigest>,
}

/// Bounded, persistable progress claim for one exact Treasury all-history query.
///
/// This value intentionally has no `Deserialize` implementation. Its JSON restore path is byte
/// bounded, rebuilds every structural identity, then requires the source to reopen every sealed
/// journal claim before it can issue another provider request. Deserialized claims never become
/// capture authority by themselves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreasuryAllHistoryCheckpoint {
    policy_version: &'static str,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    descriptor: TreasuryDatasetDescriptor,
    activation_intent_digest: EvidenceDigest,
    owner_use_attestation_digest: EvidenceDigest,
    next_page: u64,
    accepted_source_rows: u64,
    canonical_points: u64,
    raw_body_bytes: u64,
    first_received_at: Option<Timestamp>,
    last_received_at: Option<Timestamp>,
    terminal_observed: bool,
    pages: Box<[TreasuryAllHistoryPersistedPage]>,
    checkpoint_digest: EvidenceDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TreasuryAllHistoryCheckpointWire {
    policy_version: Box<str>,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    provider_dataset: SourceIdentifier,
    activation_intent_digest: EvidenceDigest,
    owner_use_attestation_digest: EvidenceDigest,
    next_page: u64,
    accepted_source_rows: u64,
    canonical_points: u64,
    raw_body_bytes: u64,
    first_received_at: Option<Timestamp>,
    last_received_at: Option<Timestamp>,
    terminal_observed: bool,
    pages: Vec<TreasuryAllHistoryPersistedPage>,
    checkpoint_digest: EvidenceDigest,
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct TreasuryAllHistoryCheckpointRef<'a> {
    policy_version: &'static str,
    source_id: &'a SourceId,
    metadata_revision: &'a MetadataRevision,
    provider_dataset: &'a SourceIdentifier,
    activation_intent_digest: EvidenceDigest,
    owner_use_attestation_digest: EvidenceDigest,
    next_page: u64,
    accepted_source_rows: u64,
    canonical_points: u64,
    raw_body_bytes: u64,
    first_received_at: Option<Timestamp>,
    last_received_at: Option<Timestamp>,
    terminal_observed: bool,
    pages: &'a [TreasuryAllHistoryPersistedPage],
    checkpoint_digest: EvidenceDigest,
}

impl Serialize for TreasuryAllHistoryCheckpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        TreasuryAllHistoryCheckpointRef {
            policy_version: self.policy_version,
            source_id: &self.source_id,
            metadata_revision: &self.metadata_revision,
            provider_dataset: self.descriptor.provider_dataset(),
            activation_intent_digest: self.activation_intent_digest,
            owner_use_attestation_digest: self.owner_use_attestation_digest,
            next_page: self.next_page,
            accepted_source_rows: self.accepted_source_rows,
            canonical_points: self.canonical_points,
            raw_body_bytes: self.raw_body_bytes,
            first_received_at: self.first_received_at,
            last_received_at: self.last_received_at,
            terminal_observed: self.terminal_observed,
            pages: &self.pages,
            checkpoint_digest: self.checkpoint_digest,
        }
        .serialize(serializer)
    }
}

impl TreasuryAllHistoryCheckpoint {
    fn initial(
        source: &TreasurySource,
        descriptor: TreasuryDatasetDescriptor,
    ) -> Result<Self, TreasurySourceError> {
        let mut checkpoint = Self {
            policy_version: CHECKPOINT_POLICY_VERSION,
            source_id: source.metadata.source_id().clone(),
            metadata_revision: source.metadata.revision().clone(),
            descriptor,
            activation_intent_digest: source.activation.intent_digest(),
            owner_use_attestation_digest: source.activation.owner_use().attestation_digest(),
            next_page: 0,
            accepted_source_rows: 0,
            canonical_points: 0,
            raw_body_bytes: 0,
            first_received_at: None,
            last_received_at: None,
            terminal_observed: false,
            pages: Box::new([]),
            checkpoint_digest: empty_digest(),
        };
        checkpoint.checkpoint_digest = checkpoint.compute_digest()?;
        Ok(checkpoint)
    }

    /// Encodes the bounded non-authoritative progress claim for transactional persistence.
    pub fn to_json(&self) -> Result<Vec<u8>, TreasurySourceError> {
        self.validate_structure()?;
        let encoded =
            serde_json::to_vec(self).map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
        if encoded.len() > MAX_CHECKPOINT_JSON_BYTES {
            return Err(TreasurySourceError::InvalidBackfillCheckpoint);
        }
        Ok(encoded)
    }

    fn from_json(source: &TreasurySource, encoded: &[u8]) -> Result<Self, TreasurySourceError> {
        if encoded.is_empty() || encoded.len() > MAX_CHECKPOINT_JSON_BYTES {
            return Err(TreasurySourceError::InvalidBackfillCheckpoint);
        }
        let wire: TreasuryAllHistoryCheckpointWire = serde_json::from_slice(encoded)
            .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
        if wire.policy_version.as_ref() != CHECKPOINT_POLICY_VERSION
            || wire.pages.len() > MAX_ALL_HISTORY_PAGES
        {
            return Err(TreasurySourceError::InvalidBackfillCheckpoint);
        }
        let descriptor = source
            .activation
            .catalog()
            .dataset(&wire.provider_dataset)
            .cloned()
            .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
        let checkpoint = Self {
            policy_version: CHECKPOINT_POLICY_VERSION,
            source_id: wire.source_id,
            metadata_revision: wire.metadata_revision,
            descriptor,
            activation_intent_digest: wire.activation_intent_digest,
            owner_use_attestation_digest: wire.owner_use_attestation_digest,
            next_page: wire.next_page,
            accepted_source_rows: wire.accepted_source_rows,
            canonical_points: wire.canonical_points,
            raw_body_bytes: wire.raw_body_bytes,
            first_received_at: wire.first_received_at,
            last_received_at: wire.last_received_at,
            terminal_observed: wire.terminal_observed,
            pages: wire.pages.into_boxed_slice(),
            checkpoint_digest: wire.checkpoint_digest,
        };
        checkpoint.validate_structure()?;
        Ok(checkpoint)
    }

    fn validate_structure(&self) -> Result<(), TreasurySourceError> {
        if self.policy_version != CHECKPOINT_POLICY_VERSION
            || self.descriptor.period() != TreasuryDatasetPeriod::AllHistory
            || self.descriptor.publication_mode() != TreasuryPublicationMode::ResumableBackfill
            || !matches!(
                self.descriptor.family(),
                TreasuryDatasetFamily::DailyRate(_)
            )
            || self.activation_intent_digest.algorithm() != DigestAlgorithm::Sha256
            || self.owner_use_attestation_digest.algorithm() != DigestAlgorithm::Sha256
            || self.activation_intent_digest.bytes() == [0; 32]
            || self.owner_use_attestation_digest.bytes() == [0; 32]
            || self.pages.len() > MAX_ALL_HISTORY_PAGES
            || self.next_page
                != u64::try_from(self.pages.len())
                    .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?
            || !within_source_row_limit(self.accepted_source_rows)
            || self.canonical_points > MAX_ALL_HISTORY_CANONICAL_POINTS
            || self.raw_body_bytes > MAX_ALL_HISTORY_RAW_BODY_BYTES
        {
            return Err(TreasurySourceError::InvalidBackfillCheckpoint);
        }
        let mut source_rows = 0_u64;
        let mut canonical_points = 0_u64;
        let mut raw_body_bytes = 0_u64;
        let mut first_received_at = None;
        let mut last_received_at = None;
        let mut payloads = Vec::new();
        for (expected_page, page) in self.pages.iter().enumerate() {
            let expected_page = u64::try_from(expected_page)
                .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
            if page.page_number != expected_page
                || page.request_digest.algorithm() != DigestAlgorithm::Sha256
                || page.payload_digest.algorithm() != DigestAlgorithm::Sha256
                || page.sealed_receipt_digest.is_none_or(|digest| {
                    digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
                })
                || page.raw_body_bytes == 0
                || page.discovery_request.dataset() != self.descriptor.provider_dataset()
                || page.discovery_request.effective_at().is_some()
                || page.discovery_request.max_results() != 1
                || page.received_at > page.discovery_request.deadline()
                || page.source_object.source_id() != &self.source_id
                || page.source_object.metadata_revision() != &self.metadata_revision
                || page.source_object.dataset() != self.descriptor.provider_dataset()
                || page.source_object.discovery_request_id() != page.discovery_request.request_id()
                || page.source_object.media_type().as_str() != "application/atom+xml"
                || page.source_object.capture_identity()
                    != market_squawk_sources::SourceObjectCaptureIdentity::Standalone
                || page.source_object.evidence().content_digest() != page.payload_digest
                || page.source_object.expected_bytes() != Some(page.raw_body_bytes)
                || page.capture.source_id() != &self.source_id
                || page.capture.metadata_revision() != &self.metadata_revision
                || page.capture.dataset() != self.descriptor.provider_dataset()
                || page.capture.request_set_identity() != page.request_digest
                || page.capture.terminal() != ProviderCaptureTerminalDisposition::StandaloneResponse
                || page.capture.pages().len() != 1
                || page.capture.total_body_bytes() != page.raw_body_bytes
                || page.capture.pages()[0].ordinal() != 0
                || page.capture.pages()[0].request_identity() != page.request_digest
                || page.capture.pages()[0]
                    .request_page_token_digest()
                    .is_some()
                || page.capture.pages()[0]
                    .response_next_page_token_digest()
                    .is_some()
                || page.capture.pages()[0].body_bytes() != page.raw_body_bytes
                || page.capture.pages()[0].body_digest() != page.payload_digest
                || page.capture.pages()[0].received_at() != page.received_at
                || page.sealed_segment_claim.as_ref().is_none_or(|claim| {
                    claim.frames().len() != 1
                        || claim.frames()[0].ordinal() != 0
                        || claim.frames()[0].provider_payload_bytes() != page.raw_body_bytes
                        || claim.frames()[0].provider_payload_digest() != page.payload_digest
                        || claim.frames()[0].received_at() != page.received_at
                })
                || page.terminal != (expected_page + 1 == self.next_page && self.terminal_observed)
                || (!page.terminal && page.returned_source_rows == 0)
                || (page.terminal && (page.returned_source_rows != 0 || page.canonical_points != 0))
                || (!page.terminal && page.canonical_points < page.returned_source_rows)
                || page.terminal
                    == (page.canonical_normalized_at.is_some()
                        && page.canonical_content_digest.is_some())
                || page
                    .canonical_normalized_at
                    .is_some_and(|normalized_at| page.terminal || normalized_at < page.received_at)
                || page.canonical_content_digest.is_some_and(|digest| {
                    page.terminal
                        || digest.algorithm() != DigestAlgorithm::Sha256
                        || digest.bytes() == [0; 32]
                })
                || last_received_at.is_some_and(|previous| page.received_at < previous)
                || payloads.contains(&page.payload_digest)
            {
                return Err(TreasurySourceError::InvalidBackfillCheckpoint);
            }
            let parsed = ParsedObjectId::parse(page.source_object.object_id())
                .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
            parsed
                .verify_request(page.request_digest.bytes())
                .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
            if parsed.kind != ObjectKind::DailyRate
                || u64::try_from(parsed.page_number).ok() != Some(page.page_number)
                || parsed.payload_digest != page.payload_digest.bytes()
            {
                return Err(TreasurySourceError::InvalidBackfillCheckpoint);
            }
            source_rows = source_rows
                .checked_add(page.returned_source_rows)
                .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
            canonical_points = canonical_points
                .checked_add(page.canonical_points)
                .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
            raw_body_bytes = raw_body_bytes
                .checked_add(page.raw_body_bytes)
                .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
            first_received_at.get_or_insert(page.received_at);
            last_received_at = Some(page.received_at);
            payloads.push(page.payload_digest);
        }
        if source_rows != self.accepted_source_rows
            || canonical_points != self.canonical_points
            || raw_body_bytes != self.raw_body_bytes
            || first_received_at != self.first_received_at
            || last_received_at != self.last_received_at
            || self.terminal_observed != self.pages.last().is_some_and(|page| page.terminal)
            || self.compute_digest()? != self.checkpoint_digest
        {
            return Err(TreasurySourceError::InvalidBackfillCheckpoint);
        }
        Ok(())
    }

    fn compute_digest(&self) -> Result<EvidenceDigest, TreasurySourceError> {
        let wire = serde_json::to_vec(&(
            self.policy_version,
            &self.source_id,
            &self.metadata_revision,
            &self.descriptor,
            self.activation_intent_digest,
            self.owner_use_attestation_digest,
            self.next_page,
            self.accepted_source_rows,
            self.canonical_points,
            self.raw_body_bytes,
            self.first_received_at,
            self.last_received_at,
            self.terminal_observed,
            &self.pages,
        ))
        .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
        Ok(domain_digest(
            b"market-squawk/treasury-all-history-checkpoint/v1\0",
            &wire,
        ))
    }

    /// Returns the exact all-history dataset being acquired.
    pub const fn descriptor(&self) -> &TreasuryDatasetDescriptor {
        &self.descriptor
    }

    /// Returns the next zero-based provider page that may be requested.
    pub const fn next_page(&self) -> u64 {
        self.next_page
    }

    /// Returns source rows admitted through the last durably sealed page.
    pub const fn accepted_source_rows(&self) -> u64 {
        self.accepted_source_rows
    }

    /// Returns canonical scalar observations prepared through the last sealed data page.
    pub const fn canonical_points(&self) -> u64 {
        self.canonical_points
    }

    /// Returns checked aggregate raw provider bytes through the last sealed page.
    pub const fn raw_body_bytes(&self) -> u64 {
        self.raw_body_bytes
    }

    /// Returns whether the provider-defined empty terminal page has been durably sealed.
    pub const fn terminal_observed(&self) -> bool {
        self.terminal_observed
    }

    /// Returns the stable identity of this exact progress state.
    pub const fn checkpoint_digest(&self) -> EvidenceDigest {
        self.checkpoint_digest
    }
}

/// Runtime authority for issuing only the next exact page of one all-history query.
///
/// The application must persist each returned checkpoint with its ingest-stage transition under
/// the existing SQLite compare-and-swap authority. This provider-local type prevents in-process
/// out-of-order admission; the root catalog prevents two restored workers from committing the
/// same prior checkpoint concurrently.
#[derive(Debug)]
pub struct TreasuryAllHistoryBackfill {
    checkpoint: TreasuryAllHistoryCheckpoint,
    tracker: TreasuryDailyRatePaginationTracker,
    verified_seals: Vec<SealedProviderCaptureSetReceipt>,
}

impl TreasuryAllHistoryBackfill {
    /// Returns the persistable progress claim after each accepted page.
    pub const fn checkpoint(&self) -> &TreasuryAllHistoryCheckpoint {
        &self.checkpoint
    }

    /// Returns a completion receipt only after the empty terminal response was parsed and sealed.
    pub fn acquisition_completion(
        &self,
    ) -> Result<TreasuryAllHistoryAcquisitionCompletion, TreasurySourceError> {
        if !self.checkpoint.terminal_observed
            || self.verified_seals.len() != self.checkpoint.pages.len()
        {
            return Err(TreasurySourceError::BackfillIncomplete);
        }
        TreasuryAllHistoryAcquisitionCompletion::try_new(&self.checkpoint, &self.verified_seals)
    }

    /// Advances the checkpoint only when the exact fetched response has a verified durable seal.
    pub fn accept_sealed_page(
        &mut self,
        mut admission: TreasuryAllHistoryPageAdmission,
        sealed: SealedProviderCaptureSetReceipt,
    ) -> Result<(), TreasurySourceError> {
        if self.checkpoint.terminal_observed
            || self.verified_seals.len() != self.checkpoint.pages.len()
            || admission.base_checkpoint_digest != self.checkpoint.checkpoint_digest
            || admission.expected_capture != *sealed.capture()
            || admission.persisted.page_number != self.checkpoint.next_page
            || admission.persisted.capture != *sealed.capture()
            || admission.persisted.sealed_segment_claim.is_some()
            || admission.persisted.sealed_receipt_digest.is_some()
        {
            return Err(TreasurySourceError::InvalidBackfillCheckpoint);
        }
        admission.persisted.sealed_segment_claim = Some(sealed.segment().claim().clone());
        admission.persisted.sealed_receipt_digest = Some(sealed.receipt_digest());
        let next_source_rows = self
            .checkpoint
            .accepted_source_rows
            .checked_add(admission.persisted.returned_source_rows)
            .filter(|value| within_source_row_limit(*value))
            .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
        let next_points = self
            .checkpoint
            .canonical_points
            .checked_add(admission.persisted.canonical_points)
            .filter(|value| *value <= MAX_ALL_HISTORY_CANONICAL_POINTS)
            .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
        let next_bytes = self
            .checkpoint
            .raw_body_bytes
            .checked_add(admission.persisted.raw_body_bytes)
            .filter(|value| *value <= MAX_ALL_HISTORY_RAW_BODY_BYTES)
            .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
        if self.checkpoint.pages.len() == MAX_ALL_HISTORY_PAGES {
            return Err(TreasurySourceError::InvalidBackfillCheckpoint);
        }
        let mut next_checkpoint = self.checkpoint.clone();
        let mut pages = next_checkpoint.pages.to_vec();
        pages.push(admission.persisted);
        next_checkpoint.accepted_source_rows = next_source_rows;
        next_checkpoint.canonical_points = next_points;
        next_checkpoint.raw_body_bytes = next_bytes;
        next_checkpoint.first_received_at.get_or_insert(
            pages
                .last()
                .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?
                .received_at,
        );
        next_checkpoint.last_received_at = pages.last().map(|page| page.received_at);
        next_checkpoint.terminal_observed = pages.last().is_some_and(|page| page.terminal);
        next_checkpoint.next_page = next_checkpoint
            .next_page
            .checked_add(1)
            .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
        next_checkpoint.pages = pages.into_boxed_slice();
        next_checkpoint.checkpoint_digest = next_checkpoint.compute_digest()?;
        next_checkpoint.validate_structure()?;
        self.verified_seals
            .try_reserve(1)
            .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
        self.checkpoint = next_checkpoint;
        self.tracker = admission.next_tracker;
        self.verified_seals.push(sealed);
        Ok(())
    }
}

/// Canonical page output prepared from the same exact response that is awaiting raw sealing.
#[derive(Debug)]
pub struct TreasuryAllHistoryCanonicalPage {
    batch: ExtractionBatch,
    accounting: TreasuryExtractionAccounting,
    normalized_at: Timestamp,
    content_identity: ExtractionContentIdentity,
}

impl TreasuryAllHistoryCanonicalPage {
    /// Returns the source-neutral canonical extraction batch.
    pub const fn batch(&self) -> &ExtractionBatch {
        &self.batch
    }

    /// Returns exact page-local source-row, canonical-point, byte, and clock accounting.
    pub const fn accounting(&self) -> &TreasuryExtractionAccounting {
        &self.accounting
    }

    /// Returns the exact local normalization clock persisted for deterministic replay.
    pub const fn normalized_at(&self) -> Timestamp {
        self.normalized_at
    }

    /// Returns the request-attempt-independent semantic identity of the canonical page.
    pub const fn content_identity(&self) -> ExtractionContentIdentity {
        self.content_identity
    }

    /// Consumes the page for normal analytical staging.
    pub fn into_batch(self) -> ExtractionBatch {
        self.batch
    }
}

/// One fetched and cross-page-validated provider response awaiting durable raw sealing.
#[derive(Debug)]
pub struct TreasuryAllHistoryFetchedPage {
    canonical: Option<TreasuryAllHistoryCanonicalPage>,
    capture: ProviderCaptureMaterial,
    admission: TreasuryAllHistoryPageAdmission,
}

impl TreasuryAllHistoryFetchedPage {
    /// Returns `true` only for the empty provider-defined terminal response.
    pub const fn terminal(&self) -> bool {
        self.admission.persisted.terminal
    }

    /// Returns the canonical page; terminal evidence deliberately has no analytical batch.
    pub const fn canonical(&self) -> Option<&TreasuryAllHistoryCanonicalPage> {
        self.canonical.as_ref()
    }

    /// Returns the exact raw response material that must be sealed before checkpoint advance.
    pub const fn capture_material(&self) -> &ProviderCaptureMaterial {
        &self.capture
    }

    /// Splits the exact response into explicit root-owned phases.
    ///
    /// The application seals `ProviderCaptureMaterial` first, stages the optional canonical batch,
    /// then passes the seal and admission to `accept_sealed_page` while compare-and-swap persisting
    /// the resulting checkpoint with the same ingest-stage transition. Terminal pages have no
    /// canonical batch but still require the identical seal/checkpoint transaction.
    pub fn into_parts(
        self,
    ) -> (
        Option<TreasuryAllHistoryCanonicalPage>,
        ProviderCaptureMaterial,
        TreasuryAllHistoryPageAdmission,
    ) {
        (self.canonical, self.capture, self.admission)
    }
}

/// One-shot page admission bound to the checkpoint state used to issue its exact request.
#[derive(Debug)]
pub struct TreasuryAllHistoryPageAdmission {
    base_checkpoint_digest: EvidenceDigest,
    expected_capture: ProviderCaptureSetReceipt,
    persisted: TreasuryAllHistoryPersistedPage,
    next_tracker: TreasuryDailyRatePaginationTracker,
}

/// Authoritative completion proof for a sealed, canonically reproducible acquisition chain.
///
/// This receipt deliberately does not claim that canonical records were durably staged or that an
/// analytical generation was published. It proves that every raw page, including the empty
/// terminal response, can be reopened and normalized to the exact retained content identity. The
/// application must bind these expectations to its own committed generation authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TreasuryAllHistoryAcquisitionCompletion {
    checkpoint: TreasuryAllHistoryCheckpoint,
    sealed_pages: Box<[SealedProviderCaptureSetReceipt]>,
    completion_digest: EvidenceDigest,
    provider_snapshot_isolation_claimed: bool,
}

impl TreasuryAllHistoryAcquisitionCompletion {
    fn try_new(
        checkpoint: &TreasuryAllHistoryCheckpoint,
        sealed_pages: &[SealedProviderCaptureSetReceipt],
    ) -> Result<Self, TreasurySourceError> {
        checkpoint.validate_structure()?;
        if !checkpoint.terminal_observed
            || checkpoint.canonical_points == 0
            || sealed_pages.len() != checkpoint.pages.len()
            || sealed_pages
                .iter()
                .zip(checkpoint.pages.iter())
                .any(|(sealed, page)| {
                    sealed.capture() != &page.capture
                        || Some(sealed.segment().claim()) != page.sealed_segment_claim.as_ref()
                        || Some(sealed.receipt_digest()) != page.sealed_receipt_digest
                })
        {
            return Err(TreasurySourceError::BackfillIncomplete);
        }
        let seal_digests = sealed_pages
            .iter()
            .map(SealedProviderCaptureSetReceipt::receipt_digest)
            .collect::<Vec<_>>();
        let wire = serde_json::to_vec(&(checkpoint.checkpoint_digest, &seal_digests, false))
            .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
        Ok(Self {
            checkpoint: checkpoint.clone(),
            sealed_pages: sealed_pages.to_vec().into_boxed_slice(),
            completion_digest: domain_digest(
                b"market-squawk/treasury-all-history-completion/v1\0",
                &wire,
            ),
            provider_snapshot_isolation_claimed: false,
        })
    }

    /// Returns the exact completed all-history dataset descriptor.
    pub const fn descriptor(&self) -> &TreasuryDatasetDescriptor {
        &self.checkpoint.descriptor
    }

    /// Returns the exact source generation that produced the acquisition chain.
    pub const fn source_id(&self) -> &SourceId {
        &self.checkpoint.source_id
    }

    /// Returns the exact source-metadata revision that produced the acquisition chain.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.checkpoint.metadata_revision
    }

    /// Returns the owner-authorized activation identity.
    pub const fn activation_intent_digest(&self) -> EvidenceDigest {
        self.checkpoint.activation_intent_digest
    }

    /// Returns the owner-use attestation identity enforced during retrieval.
    pub const fn owner_use_attestation_digest(&self) -> EvidenceDigest {
        self.checkpoint.owner_use_attestation_digest
    }

    /// Returns the count of data pages plus the retained empty terminal page.
    pub fn response_count(&self) -> u64 {
        self.checkpoint.next_page
    }

    /// Returns the exact source rows accepted across nonterminal pages.
    pub const fn source_rows(&self) -> u64 {
        self.checkpoint.accepted_source_rows
    }

    /// Returns the exact canonical scalar count prepared across nonterminal pages.
    pub const fn canonical_points(&self) -> u64 {
        self.checkpoint.canonical_points
    }

    /// Returns checked aggregate provider response bytes including the empty terminal XML body.
    pub const fn raw_body_bytes(&self) -> u64 {
        self.checkpoint.raw_body_bytes
    }

    /// Returns the first and final local receive clocks for the observed response chain.
    pub const fn receive_window(&self) -> Option<(Timestamp, Timestamp)> {
        match (
            self.checkpoint.first_received_at,
            self.checkpoint.last_received_at,
        ) {
            (Some(first), Some(last)) => Some((first, last)),
            _ => None,
        }
    }

    /// Returns exact payload identities in provider page order, including terminal evidence.
    pub fn payload_digests(&self) -> impl ExactSizeIterator<Item = EvidenceDigest> + '_ {
        self.checkpoint.pages.iter().map(|page| page.payload_digest)
    }

    /// Returns deterministic canonical content identities for data pages in provider order.
    pub fn canonical_content_digests(&self) -> impl Iterator<Item = EvidenceDigest> + '_ {
        self.checkpoint
            .pages
            .iter()
            .filter_map(|page| page.canonical_content_digest)
    }

    /// Returns every exact discovered response object, including the empty terminal response.
    pub fn source_objects(&self) -> impl ExactSizeIterator<Item = &SourceObject> {
        self.checkpoint.pages.iter().map(|page| &page.source_object)
    }

    /// Returns only data-bearing objects that contributed canonical analytical rows.
    pub fn data_source_objects(&self) -> impl Iterator<Item = &SourceObject> {
        self.checkpoint
            .pages
            .iter()
            .filter(|page| !page.terminal)
            .map(|page| &page.source_object)
    }

    /// Returns the retained empty response that proves provider-defined termination.
    pub fn terminal_source_object(&self) -> Option<&SourceObject> {
        self.checkpoint
            .pages
            .last()
            .filter(|page| page.terminal)
            .map(|page| &page.source_object)
    }

    /// Returns verified sealed-page receipts for retained-capture catalog admission.
    pub fn sealed_pages(&self) -> &[SealedProviderCaptureSetReceipt] {
        &self.sealed_pages
    }

    /// Returns the stable completion identity used by immutable publication receipts.
    pub const fn completion_digest(&self) -> EvidenceDigest {
        self.completion_digest
    }

    /// Treasury does not promise snapshot isolation across live all-history pages.
    pub const fn provider_snapshot_isolation_claimed(&self) -> bool {
        self.provider_snapshot_isolation_claimed
    }
}

impl TreasurySource {
    /// Starts a bounded all-history session for one exact owner-authorized configured dataset.
    pub fn start_all_history_backfill(
        &self,
        dataset: &SourceIdentifier,
    ) -> Result<TreasuryAllHistoryBackfill, TreasurySourceError> {
        let query = all_history_query(self, dataset)?;
        let descriptor = self
            .activation
            .catalog()
            .dataset(dataset)
            .cloned()
            .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
        let checkpoint = TreasuryAllHistoryCheckpoint::initial(self, descriptor)?;
        let tracker = TreasuryDailyRatePaginationTracker::try_new(
            query,
            MAX_ALL_HISTORY_PAGES,
            MAX_EXTRACTION_RECORDS,
        )?;
        Ok(TreasuryAllHistoryBackfill {
            checkpoint,
            tracker,
            verified_seals: Vec::new(),
        })
    }

    /// Restores progress only after reopening and reparsing every exact retained raw page.
    pub fn restore_all_history_backfill(
        &self,
        encoded_checkpoint: &[u8],
        store: &SealedResearchJournalStore,
    ) -> Result<TreasuryAllHistoryBackfill, TreasurySourceError> {
        let checkpoint = TreasuryAllHistoryCheckpoint::from_json(self, encoded_checkpoint)?;
        validate_checkpoint_source(self, &checkpoint)?;
        let query = all_history_query(self, checkpoint.descriptor.provider_dataset())?;
        let mut tracker = TreasuryDailyRatePaginationTracker::try_new(
            query,
            MAX_ALL_HISTORY_PAGES,
            MAX_EXTRACTION_RECORDS,
        )?;
        let limits = FiscalDataParseLimits::production_defaults();
        let mut verified_seals = Vec::new();
        verified_seals
            .try_reserve_exact(checkpoint.pages.len())
            .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
        for persisted in &checkpoint.pages {
            let claim = persisted
                .sealed_segment_claim
                .as_ref()
                .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
            let segment = store
                .open_verified_claim(claim)
                .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
            let sealed = SealedProviderCaptureSetReceipt::try_bind(
                persisted.capture.clone(),
                segment.receipt().clone(),
            )
            .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
            if Some(sealed.receipt_digest()) != persisted.sealed_receipt_digest
                || segment.records().len() != 1
                || ProviderCaptureMaterial::try_new(
                    persisted.capture.clone(),
                    segment.records().to_vec(),
                )
                .is_err()
            {
                return Err(TreasurySourceError::InvalidBackfillCheckpoint);
            }
            let page_number = usize::try_from(persisted.page_number)
                .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
            let request = query.page(page_number)?;
            let raw = &segment.records()[0];
            let page = TreasuryDailyRatePage::parse(raw.payload(), &request, limits)?;
            validate_replayed_page(persisted, &page)?;
            let expected_object = source_object(
                &self.metadata,
                &persisted.discovery_request,
                &request,
                raw.payload(),
                persisted.received_at,
                "application/atom+xml",
                ObjectKind::DailyRate,
            )
            .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
            if expected_object != persisted.source_object {
                return Err(TreasurySourceError::InvalidBackfillCheckpoint);
            }
            if persisted.terminal {
                if persisted.canonical_normalized_at.is_some()
                    || persisted.canonical_content_digest.is_some()
                {
                    return Err(TreasurySourceError::InvalidBackfillCheckpoint);
                }
            } else {
                let normalized_at = persisted
                    .canonical_normalized_at
                    .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)?;
                let canonical = prepare_canonical_page(
                    self,
                    CanonicalPagePreparation {
                        descriptor: &checkpoint.descriptor,
                        object: expected_object,
                        page: &page,
                        received_at: persisted.received_at,
                        normalized_at,
                        raw_body_bytes: raw.payload().len(),
                        deadline: persisted.discovery_request.deadline(),
                        capture: &persisted.capture,
                    },
                )
                .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
                if Some(canonical.content_identity().digest()) != persisted.canonical_content_digest
                    || u64::try_from(canonical.content_identity().record_count()).ok()
                        != Some(persisted.canonical_points)
                {
                    return Err(TreasurySourceError::InvalidBackfillCheckpoint);
                }
            }
            let terminal = tracker.accept(&page)?;
            if terminal != persisted.terminal {
                return Err(TreasurySourceError::InvalidBackfillCheckpoint);
            }
            verified_seals.push(sealed);
        }
        if tracker_terminal_matches_checkpoint(&checkpoint, &verified_seals) {
            Ok(TreasuryAllHistoryBackfill {
                checkpoint,
                tracker,
                verified_seals,
            })
        } else {
            Err(TreasurySourceError::InvalidBackfillCheckpoint)
        }
    }

    /// Fetches and prepares exactly the next page without advancing durable progress.
    pub async fn fetch_next_all_history_page(
        &self,
        backfill: &TreasuryAllHistoryBackfill,
        authority: market_squawk_sources::ExtractionAuthority,
        discovery: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> Result<TreasuryAllHistoryFetchedPage, ExtractionSourceError> {
        validate_checkpoint_source(self, &backfill.checkpoint).map_err(|_| invalid_protocol())?;
        if backfill.checkpoint.terminal_observed
            || backfill.verified_seals.len() != backfill.checkpoint.pages.len()
            || discovery.effective_at().is_some()
            || discovery.dataset() != backfill.checkpoint.descriptor.provider_dataset()
            || discovery.max_results() != 1
        {
            return Err(invalid_protocol());
        }
        let query = all_history_query(self, discovery.dataset()).map_err(|_| invalid_protocol())?;
        let page_number =
            usize::try_from(backfill.checkpoint.next_page).map_err(|_| invalid_protocol())?;
        let page_request = query.page(page_number).map_err(|_| invalid_protocol())?;
        let retrieved = self
            .fetch_daily_rate_page(
                &authority,
                &page_request,
                FiscalDataParseLimits::production_defaults(),
                discovery.deadline(),
                &cancellation,
            )
            .await?;
        let mut next_tracker = backfill.tracker.clone();
        let terminal = next_tracker
            .accept(retrieved.page())
            .map_err(|_| invalid_protocol())?;
        let source_rows = retrieved.page().observations().len();
        let canonical_points = page_canonical_points(retrieved.page())?;
        let raw_body_bytes =
            u64::try_from(retrieved.exact_payload().len()).map_err(|_| invalid_protocol())?;
        backfill
            .checkpoint
            .accepted_source_rows
            .checked_add(u64::try_from(source_rows).map_err(|_| invalid_protocol())?)
            .filter(|value| within_source_row_limit(*value))
            .ok_or_else(invalid_protocol)?;
        backfill
            .checkpoint
            .canonical_points
            .checked_add(canonical_points)
            .filter(|value| *value <= MAX_ALL_HISTORY_CANONICAL_POINTS)
            .ok_or_else(invalid_protocol)?;
        backfill
            .checkpoint
            .raw_body_bytes
            .checked_add(raw_body_bytes)
            .filter(|value| *value <= MAX_ALL_HISTORY_RAW_BODY_BYTES)
            .ok_or_else(invalid_protocol)?;
        let object = source_object(
            &self.metadata,
            &discovery,
            &page_request,
            retrieved.exact_payload(),
            retrieved.received_at(),
            "application/atom+xml",
            ObjectKind::DailyRate,
        )?;
        let expected_capture = retrieved.capture_material().receipt().clone();
        let canonical = if terminal {
            None
        } else {
            let normalized_at = system_timestamp().map_err(super::map_adapter_error)?;
            Some(prepare_canonical_page(
                self,
                CanonicalPagePreparation {
                    descriptor: &backfill.checkpoint.descriptor,
                    object: object.clone(),
                    page: retrieved.page(),
                    received_at: retrieved.received_at(),
                    normalized_at,
                    raw_body_bytes: retrieved.exact_payload().len(),
                    deadline: discovery.deadline(),
                    capture: &expected_capture,
                },
            )?)
        };
        let persisted = TreasuryAllHistoryPersistedPage {
            page_number: backfill.checkpoint.next_page,
            returned_source_rows: u64::try_from(source_rows).map_err(|_| invalid_protocol())?,
            canonical_points,
            raw_body_bytes,
            request_digest: sha256(page_request.request_digest()),
            payload_digest: sha256(retrieved.page().response_payload_digest()),
            received_at: retrieved.received_at(),
            provider_published_at: retrieved.page().feed_published_at(),
            terminal,
            canonical_normalized_at: canonical.as_ref().map(|page| page.normalized_at),
            canonical_content_digest: canonical
                .as_ref()
                .map(|page| page.content_identity.digest()),
            discovery_request: discovery,
            source_object: object,
            capture: expected_capture.clone(),
            sealed_segment_claim: None,
            sealed_receipt_digest: None,
        };
        let (_, _, _, capture) = retrieved.into_parts();
        Ok(TreasuryAllHistoryFetchedPage {
            canonical,
            capture,
            admission: TreasuryAllHistoryPageAdmission {
                base_checkpoint_digest: backfill.checkpoint.checkpoint_digest,
                expected_capture,
                persisted,
                next_tracker,
            },
        })
    }
}

struct CanonicalPagePreparation<'a> {
    descriptor: &'a TreasuryDatasetDescriptor,
    object: SourceObject,
    page: &'a TreasuryDailyRatePage,
    received_at: Timestamp,
    normalized_at: Timestamp,
    raw_body_bytes: usize,
    deadline: Timestamp,
    capture: &'a ProviderCaptureSetReceipt,
}

fn prepare_canonical_page(
    source: &TreasurySource,
    input: CanonicalPagePreparation<'_>,
) -> Result<TreasuryAllHistoryCanonicalPage, ExtractionSourceError> {
    let CanonicalPagePreparation {
        descriptor,
        object,
        page,
        received_at,
        normalized_at,
        raw_body_bytes,
        deadline,
        capture,
    } = input;
    let canonical =
        canonical_daily_rate_records(&source.metadata, page, received_at, normalized_at)
            .map_err(super::map_adapter_error)?;
    let record_limit =
        NonZeroU32::new(u32::try_from(canonical.len()).map_err(|_| invalid_protocol())?)
            .ok_or_else(invalid_protocol)?;
    let byte_limit =
        NonZeroU64::new(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES).ok_or_else(invalid_protocol)?;
    let request = ExtractionRequest::try_new(object, record_limit, byte_limit, deadline)?;
    let schema = SourceIdentifier::try_from(CURRENT_RESEARCH_RECORD_SCHEMA)
        .map_err(|_| invalid_protocol())?;
    let records = canonical
        .into_iter()
        .map(|record| {
            ExtractionRecord::try_new_with_time(
                &request,
                schema.clone(),
                record.evidence,
                record.effective,
                record.published,
                record.availability,
                record.revision,
                None,
                record.payload,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let accounting = TreasuryExtractionAccounting::try_new(TreasuryExtractionAccountingInput {
        descriptor: descriptor.clone(),
        page_number: page.page_number(),
        returned_source_rows: page.observations().len(),
        canonical_points: records.len(),
        raw_body_bytes,
        query_digest: page.query_digest(),
        request_digest: page.request_digest(),
        payload_digest: page.response_payload_digest(),
        received_at,
        provider_published_at: Some(page.feed_published_at()),
        terminal_for_query: false,
    })
    .map_err(|_| invalid_protocol())?;
    let batch = ExtractionBatch::try_new(&request, records)?.try_bind_provider_capture(capture)?;
    let content_identity = ExtractionContentIdentity::try_from_batch(&batch)?;
    Ok(TreasuryAllHistoryCanonicalPage {
        batch,
        accounting,
        normalized_at,
        content_identity,
    })
}

fn validate_checkpoint_source(
    source: &TreasurySource,
    checkpoint: &TreasuryAllHistoryCheckpoint,
) -> Result<(), TreasurySourceError> {
    checkpoint.validate_structure()?;
    if checkpoint.source_id != *source.metadata.source_id()
        || checkpoint.metadata_revision != *source.metadata.revision()
        || checkpoint.activation_intent_digest != source.activation.intent_digest()
        || checkpoint.owner_use_attestation_digest
            != source.activation.owner_use().attestation_digest()
        || source
            .activation
            .catalog()
            .dataset(checkpoint.descriptor.provider_dataset())
            != Some(&checkpoint.descriptor)
        || !source.activation.authorizes_private_research()
    {
        return Err(TreasurySourceError::InvalidBackfillCheckpoint);
    }
    Ok(())
}

fn all_history_query<'a>(
    source: &'a TreasurySource,
    dataset: &SourceIdentifier,
) -> Result<&'a crate::TreasuryDailyRateQuery, TreasurySourceError> {
    let TreasurySourceConfig::DailyRates(config) = &source.config else {
        return Err(TreasurySourceError::InvalidBackfillCheckpoint);
    };
    config
        .query(dataset)
        .filter(|query| query.is_all_history())
        .ok_or(TreasurySourceError::InvalidBackfillCheckpoint)
}

fn validate_replayed_page(
    persisted: &TreasuryAllHistoryPersistedPage,
    page: &TreasuryDailyRatePage,
) -> Result<(), TreasurySourceError> {
    let source_rows = u64::try_from(page.observations().len())
        .map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
    let canonical_points =
        page_canonical_points(page).map_err(|_| TreasurySourceError::InvalidBackfillCheckpoint)?;
    if u64::try_from(page.page_number()).ok() != Some(persisted.page_number)
        || source_rows != persisted.returned_source_rows
        || canonical_points != persisted.canonical_points
        || sha256(page.request_digest()) != persisted.request_digest
        || sha256(page.response_payload_digest()) != persisted.payload_digest
        || page.feed_published_at() != persisted.provider_published_at
        || page.is_terminal() != persisted.terminal
    {
        return Err(TreasurySourceError::InvalidBackfillCheckpoint);
    }
    Ok(())
}

fn tracker_terminal_matches_checkpoint(
    checkpoint: &TreasuryAllHistoryCheckpoint,
    seals: &[SealedProviderCaptureSetReceipt],
) -> bool {
    seals.len() == checkpoint.pages.len()
        && checkpoint.terminal_observed == checkpoint.pages.last().is_some_and(|page| page.terminal)
}

fn page_canonical_points(page: &TreasuryDailyRatePage) -> Result<u64, ExtractionSourceError> {
    page.observations()
        .iter()
        .try_fold(0_u64, |total, observation| {
            let points = u64::try_from(observation.metric_points().count())
                .map_err(|_| invalid_protocol())?;
            total.checked_add(points).ok_or_else(invalid_protocol)
        })
}

fn within_source_row_limit(value: u64) -> bool {
    usize::try_from(value).is_ok_and(|value| value <= MAX_EXTRACTION_RECORDS)
}

const fn sha256(bytes: [u8; 32]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, bytes)
}

fn empty_digest() -> EvidenceDigest {
    sha256(Sha256::digest([]).into())
}

fn domain_digest(domain: &[u8], wire: &[u8]) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(wire);
    sha256(digest.finalize().into())
}
