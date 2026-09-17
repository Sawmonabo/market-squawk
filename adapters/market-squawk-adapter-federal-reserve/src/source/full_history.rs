//! One actual H.15 file, deterministic native partitions, and bounded canonical iteration.
//!
//! No HTTP page is invented. The retained original checkpoint is an inert locator; reopening
//! physically verifies the same raw and transport objects before parsing or normalization.

use super::*;
use market_squawk_platform::{
    ResearchObjectAdmission, ResearchObjectClaim, ResearchObjectControl,
    ResearchObjectControlPoint, SealedResearchJournalStore, SealedResearchJournalStoreError,
    VerifiedResearchObject,
};
use market_squawk_sources::{
    CanonicalPartitionExpectation, LogicalItemRange, LogicalObjectRole, LogicalPartitionFamily,
    LogicalPartitionSetAdmission, PendingLogicalPartitionSet, ProviderLogicalPublicationError,
    ProviderLogicalTerminalInput, SealedLogicalObjectInput,
    SealedProviderLogicalPublicationBinding,
};
use serde::{Deserialize, Serialize};
use std::io::{Read as _, Write as _};
use std::num::{NonZeroU32, NonZeroU64};

const VERSION: u16 = 1;
const ROWS_PER_PARTITION: u32 = 256;
const MAX_ROWS: u64 = 262_144;
const MAX_PARTITIONS: u32 = 1_024;
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_FRAME_BYTES: u64 = 32 * 1024;
const MAX_PARTITION_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CHECKPOINT_BYTES: usize = 128 * 1024;
const NATIVE_DOMAIN: &[u8] = b"market-squawk/board-full-history/native/v1";
const ROW_MAP_DOMAIN: &[u8] = b"market-squawk/board-full-history/row-map/v1";
const CANONICAL_DOMAIN: &[u8] = b"market-squawk/board-full-history/canonical-partition/v1";

/// Failure before genuine original-file, complete-partition, or canonical authority exists.
#[derive(Debug, Error)]
pub enum BoardFullHistoryError {
    /// Existing extraction authority or actual source transport failed.
    #[error(transparent)]
    Source(#[from] ExtractionSourceError),
    /// Existing Board profile or source policy failed.
    #[error(transparent)]
    Board(#[from] BoardSourceError),
    /// The strict original file did not preserve its declared native semantics.
    #[error(transparent)]
    Parse(#[from] BoardAdapterError),
    /// Shared logical storage failed or was cancelled under its original control.
    #[error(transparent)]
    Store(#[from] SealedResearchJournalStoreError),
    /// Shared aligned partition closure failed.
    #[error(transparent)]
    Logical(#[from] ProviderLogicalPublicationError),
    /// Shared extraction/native lineage normalization failed.
    #[error(transparent)]
    Extraction(#[from] BoardExtractionError),
    /// Bounded original source or deterministic partition identity did not agree.
    #[error("Board full-history original source or partition evidence is inconsistent")]
    InvalidEvidence,
    /// A source frame or retained recipe exceeded the closed admission.
    #[error("Board full-history source or partition exceeded its bounded admission")]
    ResourceBound,
    /// A logical object could not be read or written completely.
    #[error("Board full-history controlled logical object I/O failed")]
    Io(#[from] std::io::Error),
}

impl From<market_squawk_sources::ExtractionError> for BoardFullHistoryError {
    fn from(error: market_squawk_sources::ExtractionError) -> Self {
        Self::Source(error.into())
    }
}
impl From<market_squawk_sources::ProviderNativeLineageError> for BoardFullHistoryError {
    fn from(error: market_squawk_sources::ProviderNativeLineageError) -> Self {
        Self::Extraction(error.into())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OriginalWire {
    version: u16,
    metadata: SourceMetadata,
    receipt: BoardHttpReceipt,
    ingested_at: Timestamp,
    original_deadline: Timestamp,
    native_schema_digest: [u8; 32],
    normalized_content_digest: [u8; 32],
    observation_count: u64,
    missing_observation_count: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OriginalCheckpoint {
    version: u16,
    raw: ResearchObjectClaim,
    transport: ResearchObjectClaim,
    original_digest: EvidenceDigest,
}

/// Actual single modified HTTP file awaiting the existing application-owned raw seal.
#[derive(Debug)]
pub struct BoardFullHistoryPending {
    metadata: SourceMetadata,
    retrieved: crate::transport::BoardRetrievedBody,
    deadline: Timestamp,
}

/// Original sealed file and native parse, retained before partition staging begins.
#[derive(Debug)]
pub struct BoardFullHistoryOriginal {
    wire: OriginalWire,
    parsed: ParsedBoardDataset,
    objects: Vec<SealedLogicalObjectInput>,
    original_digest: EvidenceDigest,
}

impl BoardSource {
    /// Uses this same installed Board metadata, public transport, rate authority and runtime.
    /// Its ordinary dashboard profile and one-batch extraction contract are preserved.
    pub async fn retrieve_h15_full_history(
        &self,
        authority: &ExtractionAuthority,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<BoardFullHistoryPending, BoardFullHistoryError> {
        self.validate_authority(authority)?;
        ensure_deadline(deadline)?;
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()?;
        Self::validate_metadata(&self.metadata, &profile)?;
        let result = self
            .client
            .fetch_raw(
                &self.metadata,
                authority,
                &profile,
                None,
                deadline,
                cancellation,
            )
            .await;
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(failure) => {
                self.record_failure(&failure)?;
                return Err(failure.error.into());
            }
        };
        self.validate_authority(authority)?;
        ensure_deadline(deadline)?;
        let crate::transport::BoardRawRetrievalOutcome::Modified(retrieved) = outcome else {
            return Err(BoardFullHistoryError::InvalidEvidence);
        };
        {
            let receipt = retrieved.receipt();
            let mut health = self.health.lock().map_err(|_| invalid(()))?;
            health.requests_total = health.requests_total.saturating_add(1);
            health.modified_responses_total = health.modified_responses_total.saturating_add(1);
            health.last_status = Some(receipt.status());
            health.last_response_bytes = receipt.body_bytes();
            health.last_response_digest = Some(receipt.body_digest());
            health.last_received_at = Some(receipt.received_at());
            health.last_latency_nanos = receipt.latency_nanos();
            health.last_retry_after_present = false;
        }
        Ok(BoardFullHistoryPending {
            metadata: self.metadata.clone(),
            retrieved: *retrieved,
            deadline,
        })
    }
}

impl BoardFullHistoryPending {
    /// Must run on the existing owned research I/O lane. Exact original objects are sealed once;
    /// persist `checkpoint_bytes()` in the existing operation checkpoint before partitioning.
    pub fn seal_original(
        self,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<BoardFullHistoryOriginal, BoardFullHistoryError> {
        checkpoint(control)?;
        let (bytes, receipt) = self.retrieved.into_parts();
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()?;
        let parsed = crate::parse::parse_csv_controlled(
            profile.contract(),
            &bytes,
            profile.parse_limits(),
            control,
        )?;
        checkpoint(control)?;
        let ingested_at = system_timestamp()?;
        let wire = OriginalWire {
            version: VERSION,
            metadata: self.metadata,
            receipt,
            ingested_at,
            original_deadline: self.deadline,
            native_schema_digest: parsed.native_schema_digest(),
            normalized_content_digest: parsed.normalized_content_digest(),
            observation_count: parsed.observation_count(),
            missing_observation_count: parsed.missing_observation_count(),
        };
        validate_original(&wire, &parsed, &bytes)?;
        let encoded = serde_json::to_vec(&wire).map_err(invalid)?;
        if encoded.len() > MAX_CHECKPOINT_BYTES {
            return Err(BoardFullHistoryError::ResourceBound);
        }
        let original_digest = sha(&encoded);
        let raw = seal_object(store, control, &bytes, MAX_FILE_BYTES)?;
        let transport = seal_object(store, control, &encoded, MAX_CHECKPOINT_BYTES as u64)?;
        let objects = vec![
            SealedLogicalObjectInput::try_from_verified(
                LogicalObjectRole::ProviderPayload,
                0,
                sha(&bytes),
                raw,
                control,
            )?,
            SealedLogicalObjectInput::try_from_verified(
                LogicalObjectRole::Catalog,
                1,
                original_digest,
                transport,
                control,
            )?,
        ];
        Ok(BoardFullHistoryOriginal {
            wire,
            parsed,
            objects,
            original_digest,
        })
    }
}

impl BoardFullHistoryOriginal {
    /// Inert original locator for the existing durable operation checkpoint, never live custody.
    pub fn checkpoint_bytes(&self) -> Result<Box<[u8]>, BoardFullHistoryError> {
        let [raw, transport] = self.objects.as_slice() else {
            return Err(invalid(()));
        };
        let encoded = serde_json::to_vec(&OriginalCheckpoint {
            version: VERSION,
            raw: raw.object().claim().clone(),
            transport: transport.object().claim().clone(),
            original_digest: self.original_digest,
        })
        .map_err(invalid)?;
        if encoded.len() > MAX_CHECKPOINT_BYTES {
            return Err(BoardFullHistoryError::ResourceBound);
        }
        Ok(encoded.into_boxed_slice())
    }

    /// Reopens the exact source-owned checkpoint after its owning catalog session is read.
    /// No HTTP call, current source file, changed local clock, or claimed physical receipt is used.
    pub fn reopen_checkpoint(
        bytes: &[u8],
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<Self, BoardFullHistoryError> {
        let reopened = reopen_original_checkpoint(bytes, store, control)?;
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()?;
        let parsed = crate::parse::parse_csv_controlled(
            profile.contract(),
            &reopened.body,
            profile.parse_limits(),
            control,
        )?;
        validate_original(&reopened.wire, &parsed, &reopened.body)?;
        Ok(Self {
            wire: reopened.wire,
            parsed,
            objects: reopened.objects,
            original_digest: reopened.original_digest,
        })
    }

    /// Physically authenticates the same original but reconstructs only complete partitions
    /// covering eleven exact 10y dates. This is read evidence, never complete publication input.
    pub fn reopen_annual_checkpoint(
        bytes: &[u8],
        dates: &[market_squawk_domain::CalendarDate; 11],
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<BoardFullHistorySelectedReplay, BoardFullHistoryError> {
        let reopened = reopen_original_checkpoint(bytes, store, control)?;
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()?;
        let descriptor = crate::h15_treasury_constant_maturities_dashboard_series()
            .iter()
            .find(|series| series.slot() == "10y")
            .ok_or_else(|| invalid(()))?;
        let series = profile
            .contract()
            .series_scope()
            .exact_series()
            .and_then(|series| {
                series
                    .iter()
                    .find(|series| series.series_name() == descriptor.provider_series_name())
            })
            .ok_or_else(|| invalid(()))?;
        let selected = crate::parse::parse_csv_selected_partitions(
            profile.contract(),
            &reopened.body,
            profile.parse_limits(),
            reopened.wire.observation_count,
            series.unique_id(),
            dates,
            ROWS_PER_PARTITION,
            control,
        )?;
        if selected.parsed.native_schema_digest() != reopened.wire.native_schema_digest
            || selected.parsed.source_payload_digest() != reopened.wire.receipt.body_digest()
            || selected.parsed.observation_count() != selected.global_ordinals.len() as u64
            || selected.global_ordinals.is_empty()
            || selected.global_ordinals.len() > 11 * ROWS_PER_PARTITION as usize
        {
            return Err(invalid(()));
        }
        let first = selected.global_ordinals[0];
        let mut cursor = BoardFullHistoryCanonicalCursor::new(reopened.wire, selected.parsed)?;
        cursor.selected_ordinals = Some(selected.global_ordinals);
        cursor.next_ordinal = first;
        checkpoint(control)?;
        Ok(BoardFullHistorySelectedReplay {
            cursor,
            objects: reopened.objects,
            original_digest: reopened.original_digest,
        })
    }

    /// Exact registered source declaration that produced the original file.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.wire.metadata
    }
    /// Live original receipts required by the sole catalog custody transaction.
    pub fn objects(&self) -> &[SealedLogicalObjectInput] {
        &self.objects
    }
    /// Code-owned complete native partition schema used by exact catalog selection.
    pub fn native_schema_digest(&self) -> EvidenceDigest {
        sha(NATIVE_DOMAIN)
    }
    /// Source-defined complete native partition schema, independent of acquired values.
    pub fn native_partition_schema_digest() -> EvidenceDigest {
        sha(NATIVE_DOMAIN)
    }
    /// Reuses the exact original parse without staging or writing any new object.
    pub fn into_canonical_cursor(
        self,
    ) -> Result<BoardFullHistoryCanonicalCursor, BoardFullHistoryError> {
        BoardFullHistoryCanonicalCursor::new(self.wire, self.parsed)
    }
    /// Exact whole-file transport/native receipt, independently of later publication clocks.
    pub const fn original_digest(&self) -> EvidenceDigest {
        self.original_digest
    }
    /// Actual source receipt clock, never a historical observation date.
    pub const fn received_at(&self) -> Timestamp {
        self.wire.receipt.received_at()
    }

    /// Stages exact native semantics and row maps in deterministic 256-row partitions. Canonical
    /// payloads are generated one partition at a time and discarded after hashing; the returned
    /// cursor repeats that same private source normalization under the original retained clocks.
    pub fn prepare(
        self,
        canonical_schema: EvidenceDigest,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<BoardPreparedFullHistory, BoardFullHistoryError> {
        let Self {
            wire,
            parsed,
            objects,
            original_digest,
        } = self;
        let original_checkpoint = checkpoint_from_objects(&objects, original_digest)?;
        let mut cursor = BoardFullHistoryCanonicalCursor::new(wire, parsed)?;
        let admission = LogicalPartitionSetAdmission::try_new(
            ResearchObjectAdmission::try_new(MAX_PARTITION_BYTES, 16)?,
            MAX_PARTITIONS,
            ROWS_PER_PARTITION,
            MAX_FRAME_BYTES,
        )?;
        let mut native = PendingLogicalPartitionSet::begin(
            LogicalPartitionFamily::ProviderNative,
            sha(NATIVE_DOMAIN),
            admission,
            0,
        )?;
        let mut row_map = PendingLogicalPartitionSet::begin(
            LogicalPartitionFamily::CanonicalRowMap,
            sha(ROW_MAP_DOMAIN),
            admission,
            0,
        )?;
        let mut expectations = Vec::new();
        let result = (|| -> Result<(), BoardFullHistoryError> {
            while let Some(partition) = cursor.next_partition(control)? {
                for (offset, (native_bytes, row_bytes)) in partition
                    .native_rows
                    .iter()
                    .zip(partition.row_maps.iter())
                    .enumerate()
                {
                    checkpoint(control)?;
                    let ordinal = partition.range.first_ordinal() + offset as u64;
                    let a = native.stage_frame(
                        store,
                        control,
                        ordinal,
                        native_bytes,
                        sha(native_bytes),
                    )?;
                    let b =
                        row_map.stage_frame(store, control, ordinal, row_bytes, sha(row_bytes))?;
                    if a.partition_ordinal() != partition.ordinal
                        || b.partition_ordinal() != partition.ordinal
                        || a.partition_item_ordinal() != offset as u32
                        || b.partition_item_ordinal() != offset as u32
                    {
                        return Err(invalid(()));
                    }
                }
                expectations.push(CanonicalPartitionExpectation::try_new(
                    partition.ordinal,
                    partition.range,
                    canonical_schema,
                    partition.digest,
                    partition.ordinal,
                    partition.ordinal,
                )?);
                native.seal_current_partition(store, control)?;
                row_map.seal_current_partition(store, control)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            let _ = native.abort(store);
            let _ = row_map.abort(store);
            return Err(error);
        }
        let native = native.finish(store, control)?;
        let row_map = row_map.finish(store, control)?;
        if expectations.len() != native.partitions().len()
            || expectations.len() != row_map.partitions().len()
            || expectations
                .iter()
                .zip(native.partitions())
                .zip(row_map.partitions())
                .any(|((a, b), c)| {
                    a.row_range() != b.item_range() || a.row_range() != c.item_range()
                })
        {
            return Err(invalid(()));
        }
        let mut partitions = native.into_partitions().into_vec();
        partitions.extend(row_map.into_partitions().into_vec());
        let terminal = ProviderLogicalTerminalInput {
            source_id: cursor.wire.metadata.source_id().clone(),
            source_revision_digest: sha(
                &serde_json::to_vec(&cursor.wire.metadata).map_err(invalid)?
            ),
            execution_attempt_digest: Some(original_digest),
            provider_terminal_evidence_digest: original_digest,
            total_decoded_events: 0,
            total_canonical_rows: cursor.wire.observation_count,
            total_logical_object_bytes: objects
                .iter()
                .map(|object| object.object().size_bytes())
                .sum(),
        };
        cursor.reset();
        let binding = SealedProviderLogicalPublicationBinding::try_new(
            terminal,
            &[
                LogicalPartitionFamily::ProviderNative,
                LogicalPartitionFamily::CanonicalRowMap,
            ],
            objects,
            partitions,
            expectations,
        )?;
        Ok(BoardPreparedFullHistory {
            cursor,
            binding,
            original_checkpoint,
            original_digest,
        })
    }
}

/// Exact physically reopened original with a bounded, source-selected subset of complete
/// partitions. It cannot produce a prepared publication or attest a whole-dataset replay.
#[derive(Debug)]
pub struct BoardFullHistorySelectedReplay {
    cursor: BoardFullHistoryCanonicalCursor,
    objects: Vec<SealedLogicalObjectInput>,
    original_digest: EvidenceDigest,
}
impl BoardFullHistorySelectedReplay {
    /// Exact original registered source declaration.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.cursor.wire.metadata
    }
    /// Physically authenticated original raw and transport objects.
    pub fn objects(&self) -> &[SealedLogicalObjectInput] {
        &self.objects
    }
    /// Exact whole original transport/native identity.
    pub const fn original_digest(&self) -> EvidenceDigest {
        self.original_digest
    }
    /// Full original row count authenticated by the original receipt and exact CSV date scan.
    pub const fn original_observation_count(&self) -> u64 {
        self.cursor.wire.observation_count
    }
    /// Replays only the selected complete partitions, with their original global ordinals.
    pub fn into_canonical_cursor(self) -> BoardFullHistoryCanonicalCursor {
        self.cursor
    }
}

/// Complete source-owned logical evidence and its bounded canonical iterator.
#[derive(Debug)]
pub struct BoardPreparedFullHistory {
    cursor: BoardFullHistoryCanonicalCursor,
    binding: SealedProviderLogicalPublicationBinding,
    original_checkpoint: Box<[u8]>,
    original_digest: EvidenceDigest,
}

impl BoardPreparedFullHistory {
    /// Exact source metadata retained from the real acquisition.
    pub const fn metadata(&self) -> &SourceMetadata {
        &self.cursor.wire.metadata
    }
    /// Exact original checkpoint must already be durable before publishing a terminal plan.
    pub fn original_checkpoint(&self) -> &[u8] {
        &self.original_checkpoint
    }
    /// Whole original transport/native identity.
    pub const fn original_digest(&self) -> EvidenceDigest {
        self.original_digest
    }
    /// Actual complete logical binding used as both final rights payload and ingest identity.
    pub const fn binding(&self) -> &SealedProviderLogicalPublicationBinding {
        &self.binding
    }
    /// Consumes all original logical custody exactly once for the existing atomic data publisher.
    pub fn into_parts(
        self,
    ) -> (
        BoardFullHistoryCanonicalCursor,
        SealedProviderLogicalPublicationBinding,
        Box<[u8]>,
    ) {
        (self.cursor, self.binding, self.original_checkpoint)
    }
}

/// Holds only the bounded native parse plus one source normalization position.
#[derive(Debug)]
pub struct BoardFullHistoryCanonicalCursor {
    wire: OriginalWire,
    parsed: ParsedBoardDataset,
    request: ExtractionRequest,
    series_index: usize,
    row_index: usize,
    next_ordinal: u64,
    processed_rows: u64,
    selected_ordinals: Option<Vec<u64>>,
}

/// At most 256 rows with exact native and raw-file coordinate equality.
#[derive(Debug)]
pub struct BoardFullHistoryCanonicalPartition {
    ordinal: u32,
    range: LogicalItemRange,
    batch: ExtractionBatch,
    native: ProviderNativeLineageBatch,
    revisions: ExtractionRevisionPlan,
    native_rows: Vec<Vec<u8>>,
    row_maps: Vec<Vec<u8>>,
    digest: EvidenceDigest,
}

impl BoardFullHistoryCanonicalPartition {
    /// Exact canonical range in the complete original file's series-major order.
    pub const fn range(&self) -> LogicalItemRange {
        self.range
    }
    /// Contiguous partition ordinal.
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }
    /// Source-owned typed payload commitment checked by the terminal expectation.
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
    /// Physically checks each retained native or row-map frame against the original file's
    /// deterministic normalization. The caller must obtain the exact claim from its catalog.
    pub fn verify_retained_frames(
        &self,
        family: LogicalPartitionFamily,
        object: &mut VerifiedResearchObject,
        control: &dyn ResearchObjectControl,
    ) -> Result<(), BoardFullHistoryError> {
        let rows = match family {
            LogicalPartitionFamily::ProviderNative => &self.native_rows,
            LogicalPartitionFamily::CanonicalRowMap => &self.row_maps,
            _ => return Err(invalid(())),
        };
        let mut expected_bytes = 0_u64;
        for (offset, expected) in rows.iter().enumerate() {
            checkpoint(control)?;
            let mut header = [0_u8; 16];
            object.read_exact(&mut header)?;
            let ordinal = u64::from_le_bytes(header[..8].try_into().map_err(invalid)?);
            let size = u64::from_le_bytes(header[8..].try_into().map_err(invalid)?);
            if ordinal != self.range.first_ordinal() + offset as u64
                || size != expected.len() as u64
            {
                return Err(invalid(()));
            }
            let mut bytes = vec![0_u8; expected.len()];
            object.read_exact(&mut bytes)?;
            if bytes != *expected {
                return Err(invalid(()));
            }
            expected_bytes = expected_bytes
                .checked_add(16 + size)
                .ok_or_else(|| invalid(()))?;
        }
        let mut probe = [0_u8; 1];
        if object.read(&mut probe)? != 0 || object.size_bytes() != expected_bytes {
            return Err(invalid(()));
        }
        checkpoint(control)
    }
    /// Fixed schema for an exact source-owned native or row-map family.
    pub fn evidence_schema(
        family: LogicalPartitionFamily,
    ) -> Result<EvidenceDigest, BoardFullHistoryError> {
        match family {
            LogicalPartitionFamily::ProviderNative => Ok(sha(NATIVE_DOMAIN)),
            LogicalPartitionFamily::CanonicalRowMap => Ok(sha(ROW_MAP_DOMAIN)),
            _ => Err(invalid(())),
        }
    }
    /// Bounded canonical batch, original native equality, and existing local revision authority.
    pub fn into_parts(
        self,
    ) -> (
        ExtractionBatch,
        ProviderNativeLineageBatch,
        ExtractionRevisionPlan,
    ) {
        (self.batch, self.native, self.revisions)
    }
}

impl BoardFullHistoryCanonicalCursor {
    fn new(wire: OriginalWire, parsed: ParsedBoardDataset) -> Result<Self, BoardFullHistoryError> {
        let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()?;
        let discovery = DiscoveryRequest::try_new(
            profile.dataset().clone(),
            None,
            NonZeroU16::new(1).ok_or_else(|| invalid(()))?,
            wire.original_deadline,
        )?;
        let body_digest = wire.receipt.body_digest();
        let object = SourceObject::try_new_with_availability(
            wire.metadata.source_id().clone(),
            wire.metadata.revision().clone(),
            &discovery,
            identifier(format!(
                "federal-reserve-board-file:{}:{}:{}",
                profile.contract().family().as_str(),
                lower_hex(profile.contract().contract_digest()),
                lower_hex(body_digest)
            ))?,
            identifier(media_type(profile.contract().format()))?,
            ExactPayloadEvidence::with_version_pinned_locator(
                EvidenceDigest::new(DigestAlgorithm::Sha256, body_digest),
                VersionPinnedSourceLocator::new(
                    identifier(format!(
                        "federal-reserve-board-request:{}",
                        lower_hex(profile.contract().request().request_digest())
                    ))?,
                    identifier(lower_hex(body_digest))?,
                ),
            ),
            EffectiveInterval::new(wire.receipt.received_at(), None).map_err(invalid)?,
            None,
            market_squawk_sources::AvailabilityEvidence::LocalFirstObserved {
                observed_at: wire.receipt.received_at(),
            },
            Some(wire.receipt.body_bytes()),
        )?;
        let request = ExtractionRequest::try_new(
            object,
            NonZeroU32::new(ROWS_PER_PARTITION).ok_or_else(|| invalid(()))?,
            NonZeroU64::new(MAX_PARTITION_BYTES).ok_or_else(|| invalid(()))?,
            wire.original_deadline,
        )?;
        Ok(Self {
            wire,
            parsed,
            request,
            series_index: 0,
            row_index: 0,
            next_ordinal: 0,
            processed_rows: 0,
            selected_ordinals: None,
        })
    }
    fn reset(&mut self) {
        self.series_index = 0;
        self.row_index = 0;
        self.next_ordinal = 0;
        self.processed_rows = 0;
    }
    /// Produces exactly the next fixed source partition, checking cancellation for every row.
    pub fn next_partition(
        &mut self,
        control: &dyn ResearchObjectControl,
    ) -> Result<Option<BoardFullHistoryCanonicalPartition>, BoardFullHistoryError> {
        checkpoint(control)?;
        if self.processed_rows == self.parsed.observation_count() {
            return Ok(None);
        }
        let first = self.next_ordinal;
        let mut accumulator = ExtractionBatchAccumulator::try_new(&self.request)?;
        let mut native_rows = Vec::new();
        let mut row_maps = Vec::new();
        let mut digest = Sha256::new();
        digest.update(CANONICAL_DOMAIN);
        digest.update(first.to_be_bytes());
        while native_rows.len() < ROWS_PER_PARTITION as usize
            && self.next_ordinal < self.wire.observation_count
        {
            checkpoint(control)?;
            if self.selected_ordinals.as_ref().is_some_and(|ordinals| {
                ordinals.get(self.processed_rows as usize) != Some(&self.next_ordinal)
            }) {
                return Err(invalid(()));
            }
            while self
                .parsed
                .series()
                .get(self.series_index)
                .is_some_and(|series| self.row_index == series.observations().len())
            {
                self.series_index += 1;
                self.row_index = 0;
            }
            let series = self
                .parsed
                .series()
                .get(self.series_index)
                .ok_or_else(|| invalid(()))?;
            let observation = series
                .observations()
                .get(self.row_index)
                .ok_or_else(|| invalid(()))?;
            let record = canonical_record(
                &self.wire.metadata,
                &self.parsed,
                series,
                observation.period(),
                observation.value(),
                observation.row_digest(),
                &self.wire.receipt,
                self.wire.ingested_at,
            )?;
            let native = serde_json::to_value(&BoardH15NativeLineageRowV1 {
                series_unique_id: series.unique_id(),
                series_name: series.series_name(),
                series_description: series.description(),
                series_unit: series.unit(),
                series_multiplier: series.multiplier(),
                series_currency: series.currency(),
                series_frequency: series.frequency(),
                series_lifecycle: series.lifecycle(),
                series_dimensions: series.dimensions(),
                period: observation.period(),
                value: observation.value(),
                observation_dimensions: observation.dimensions(),
            })
            .map_err(invalid)?;
            let native = serde_json::to_vec(&native).map_err(invalid)?;
            let row_map = serde_json::to_vec(&RowMap {
                global_ordinal: self.next_ordinal,
                series_index: if self.selected_ordinals.is_some() {
                    (self.next_ordinal / (self.wire.observation_count / 11)) as u32
                } else {
                    self.series_index as u32
                },
                observation_index: if self.selected_ordinals.is_some() {
                    (self.next_ordinal % (self.wire.observation_count / 11)) as u32
                } else {
                    self.row_index as u32
                },
                original_body_digest: self.wire.receipt.body_digest(),
                native_row_digest: observation.row_digest(),
                canonical_payload_digest: sha(&record.payload),
            })
            .map_err(invalid)?;
            if native.len() as u64 > MAX_FRAME_BYTES
                || row_map.len() as u64 > MAX_FRAME_BYTES
                || record.payload.len() as u64 > MAX_FRAME_BYTES
            {
                return Err(BoardFullHistoryError::ResourceBound);
            }
            digest.update(self.next_ordinal.to_be_bytes());
            digest.update(sha(&record.payload).bytes());
            accumulator.push(market_squawk_sources::ExtractionRecord::try_new_with_time(
                &self.request,
                identifier(market_squawk_sources::CURRENT_RESEARCH_RECORD_SCHEMA)?,
                record.evidence,
                record.effective,
                None,
                record.availability,
                record.revision,
                None,
                record.payload,
            )?)?;
            native_rows.push(native);
            row_maps.push(row_map);
            self.next_ordinal += 1;
            self.row_index += 1;
            self.processed_rows += 1;
        }
        if let Some(ordinals) = &self.selected_ordinals {
            self.next_ordinal = ordinals
                .get(self.processed_rows as usize)
                .copied()
                .unwrap_or(self.wire.observation_count);
        }
        let batch = accumulator.finish()?;
        let mut native = ProviderNativeLineageBatchBuilder::try_new(
            ProviderNativeLineageImplementation::FederalReserveH15V1,
            &batch,
        )?;
        native.try_set_batch_sidecar(&BoardH15NativeLineageBatchV1 {
            native_contract_version: BOARD_NATIVE_CONTRACT_VERSION,
            release: self.parsed.release(),
            family: self.parsed.family(),
            format: self.parsed.format(),
            frequency: self.parsed.frequency(),
            route_lifecycle: self.parsed.route_lifecycle(),
            native_schema_digest: self.parsed.native_schema_digest(),
            sdmx_header: self.parsed.sdmx_header(),
            artifacts: self.parsed.artifacts(),
        })?;
        for row in &native_rows {
            let value: serde_json::Value = serde_json::from_slice(row).map_err(invalid)?;
            native.try_push(&value)?;
        }
        let native = native.finish()?;
        let revisions =
            ExtractionRevisionPlan::locally_observed_with_native_lineage(batch.records().len())
                .map_err(invalid)?;
        let count = NonZeroU32::new(native_rows.len() as u32).ok_or_else(|| invalid(()))?;
        digest.update(count.get().to_be_bytes());
        Ok(Some(BoardFullHistoryCanonicalPartition {
            ordinal: u32::try_from(first / u64::from(ROWS_PER_PARTITION)).map_err(invalid)?,
            range: LogicalItemRange::try_new(first, count)?,
            batch,
            native,
            revisions,
            native_rows,
            row_maps,
            digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        }))
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct RowMap {
    global_ordinal: u64,
    series_index: u32,
    observation_index: u32,
    original_body_digest: [u8; 32],
    native_row_digest: [u8; 32],
    canonical_payload_digest: EvidenceDigest,
}

fn validate_original(
    wire: &OriginalWire,
    parsed: &ParsedBoardDataset,
    bytes: &[u8],
) -> Result<(), BoardFullHistoryError> {
    validate_original_envelope(wire, bytes)?;
    let receipt = &wire.receipt;
    if parsed.request_digest() != receipt.contract_request_digest()
        || parsed.contract_digest() != receipt.contract_digest()
        || parsed.source_payload_digest() != receipt.body_digest()
        || parsed.native_schema_digest() != wire.native_schema_digest
        || parsed.normalized_content_digest() != wire.normalized_content_digest
        || parsed.observation_count() != wire.observation_count
        || !(1..=MAX_ROWS).contains(&wire.observation_count)
        || parsed.missing_observation_count() != wire.missing_observation_count
        || parsed.series().len() != 11
    {
        return Err(invalid(()));
    }
    Ok(())
}

/// The complete normalized-content commitment remains authenticated in OriginalWire. A selected
/// read proves its needed canonical partitions against the committed publication separately.
fn validate_original_envelope(
    wire: &OriginalWire,
    bytes: &[u8],
) -> Result<(), BoardFullHistoryError> {
    let profile = BoardDatasetProfile::h15_treasury_constant_maturities_full_history()?;
    BoardSource::validate_metadata(&wire.metadata, &profile)?;
    let receipt = &wire.receipt;
    let validators = crate::BoardHttpValidators::try_new(
        receipt.validators().etag().map(<[u8]>::to_vec),
        receipt.validators().last_modified().map(str::to_owned),
    )?;
    if wire.version != VERSION
        || bytes.is_empty()
        || bytes.len() as u64 > MAX_FILE_BYTES
        || !wire.metadata.is_effective_at(receipt.request_started_at())
        || !matches!(
            receipt.content_type(),
            "text/csv" | "application/csv" | "application/octet-stream"
        )
        || receipt.status() != 200
        || receipt.conditional().is_some()
        || validators != *receipt.validators()
        || receipt.contract_digest() != profile.contract().contract_digest()
        || receipt.contract_request_digest() != profile.contract().request().request_digest()
        || receipt.request_digest()
            != crate::transport::request_identity(receipt.contract_request_digest(), None)
        || receipt.body_digest() != sha(bytes).bytes()
        || receipt.body_bytes() != bytes.len() as u64
        || receipt
            .declared_body_bytes()
            .is_some_and(|declared| declared != receipt.body_bytes())
        || receipt.request_started_at() > receipt.received_at()
        || receipt.received_at() > wire.ingested_at
        || wire.ingested_at > wire.original_deadline
        || !(1..=MAX_ROWS).contains(&wire.observation_count)
        || wire.observation_count % 11 != 0
        || wire.missing_observation_count > wire.observation_count
    {
        return Err(invalid(()));
    }
    Ok(())
}

struct ReopenedBoardOriginal {
    wire: OriginalWire,
    body: Vec<u8>,
    objects: Vec<SealedLogicalObjectInput>,
    original_digest: EvidenceDigest,
}

fn reopen_original_checkpoint(
    bytes: &[u8],
    store: &SealedResearchJournalStore,
    control: &dyn ResearchObjectControl,
) -> Result<ReopenedBoardOriginal, BoardFullHistoryError> {
    checkpoint(control)?;
    if bytes.is_empty() || bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(invalid(()));
    }
    let reference: OriginalCheckpoint = serde_json::from_slice(bytes).map_err(invalid)?;
    if reference.version != VERSION || serde_json::to_vec(&reference).map_err(invalid)? != bytes {
        return Err(invalid(()));
    }
    let mut raw = store.open_verified_logical_object_claim(&reference.raw, control)?;
    let mut transport = store.open_verified_logical_object_claim(&reference.transport, control)?;
    let encoded = read_bounded(&mut transport, MAX_CHECKPOINT_BYTES as u64, control)?;
    if sha(&encoded) != reference.original_digest {
        return Err(invalid(()));
    }
    let wire: OriginalWire = serde_json::from_slice(&encoded).map_err(invalid)?;
    if serde_json::to_vec(&wire).map_err(invalid)? != encoded {
        return Err(invalid(()));
    }
    let body = read_bounded(&mut raw, MAX_FILE_BYTES, control)?;
    validate_original_envelope(&wire, &body)?;
    let objects = vec![
        SealedLogicalObjectInput::try_from_verified(
            LogicalObjectRole::ProviderPayload,
            0,
            sha(&body),
            raw,
            control,
        )?,
        SealedLogicalObjectInput::try_from_verified(
            LogicalObjectRole::Catalog,
            1,
            reference.original_digest,
            transport,
            control,
        )?,
    ];
    Ok(ReopenedBoardOriginal {
        wire,
        body,
        objects,
        original_digest: reference.original_digest,
    })
}

fn checkpoint_from_objects(
    objects: &[SealedLogicalObjectInput],
    original_digest: EvidenceDigest,
) -> Result<Box<[u8]>, BoardFullHistoryError> {
    let [raw, transport] = objects else {
        return Err(invalid(()));
    };
    let bytes = serde_json::to_vec(&OriginalCheckpoint {
        version: VERSION,
        raw: raw.object().claim().clone(),
        transport: transport.object().claim().clone(),
        original_digest,
    })
    .map_err(invalid)?;
    if bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(BoardFullHistoryError::ResourceBound);
    }
    Ok(bytes.into_boxed_slice())
}
fn checkpoint(control: &dyn ResearchObjectControl) -> Result<(), BoardFullHistoryError> {
    control
        .checkpoint(ResearchObjectControlPoint::BeforeVerification)
        .map_err(SealedResearchJournalStoreError::ObjectControl)?;
    Ok(())
}
fn seal_object(
    store: &SealedResearchJournalStore,
    control: &dyn ResearchObjectControl,
    bytes: &[u8],
    maximum: u64,
) -> Result<VerifiedResearchObject, BoardFullHistoryError> {
    let mut pending = store.begin_logical_object(ResearchObjectAdmission::try_new(maximum, 16)?)?;
    let result = (|| -> Result<(), BoardFullHistoryError> {
        for chunk in bytes.chunks(64 * 1024) {
            checkpoint(control)?;
            pending.write_all(chunk)?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        let _ = store.abort_logical_object(pending);
        return Err(error);
    }
    Ok(store.finish_logical_object(pending, control)?)
}
fn read_bounded(
    object: &mut VerifiedResearchObject,
    maximum: u64,
    control: &dyn ResearchObjectControl,
) -> Result<Vec<u8>, BoardFullHistoryError> {
    if object.size_bytes() == 0 || object.size_bytes() > maximum {
        return Err(BoardFullHistoryError::ResourceBound);
    }
    let size = usize::try_from(object.size_bytes()).map_err(invalid)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(invalid)?;
    let mut chunk = [0_u8; 64 * 1024];
    while bytes.len() < size {
        checkpoint(control)?;
        let count = object.read(&mut chunk)?;
        if count == 0 {
            return Err(invalid(()));
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    if bytes.len() != size {
        return Err(invalid(()));
    }
    Ok(bytes)
}
fn sha(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}
fn invalid<T>(_: T) -> BoardFullHistoryError {
    BoardFullHistoryError::InvalidEvidence
}
