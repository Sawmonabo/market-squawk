//! Streamed logical-object and ordered-partition publication authority.
//!
//! This module is the common production-scale sibling to bounded response captures. Provider
//! adapters describe exact semantics and terminal completeness; the application owns physical
//! staging and immutable publication. Value claims remain restart evidence only. The final
//! noncloneable binding is minted only from live, store-issued logical-object receipts.

use std::{
    collections::BTreeSet,
    io::{Read, Write},
    num::NonZeroU32,
};

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceId};
use market_squawk_platform::{
    PendingResearchObject, ResearchObjectAdmission, ResearchObjectCheckpointClaim,
    ResearchObjectClaim, ResearchObjectControl, ResearchObjectReceipt, SealedResearchJournalStore,
    SealedResearchJournalStoreError, VerifiedResearchObject,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Maximum logical objects bound by one provider publication.
pub const MAX_PROVIDER_LOGICAL_OBJECTS: usize = 64;
/// Maximum evidence partitions across all families in one provider publication.
pub const MAX_PROVIDER_LOGICAL_PARTITIONS: usize = 4_096;
/// Maximum canonical Parquet partitions expected by one immutable generation.
pub const MAX_PROVIDER_CANONICAL_PARTITIONS: usize = 1_024;
/// Maximum encoded catalog metadata admitted across one complete logical publication.
pub const MAX_PROVIDER_LOGICAL_CATALOG_BYTES: usize = 256 * 1024 * 1024;
/// Exact frame header bytes: little-endian item ordinal followed by little-endian payload length.
pub const LOGICAL_PARTITION_FRAME_HEADER_BYTES: u64 = 16;

const LOGICAL_OBJECT_SET_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/object-set/v1";
const LOGICAL_PARTITION_SEMANTIC_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/partition-semantic/v1";
const LOGICAL_PARTITION_SET_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/partition-set/v1";
const CANONICAL_PARTITION_SET_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/canonical-partition-set/v1";
const LOGICAL_TERMINAL_RECEIPT_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/terminal/v1";
const LOGICAL_PUBLICATION_BINDING_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/binding/v1";
const LOGICAL_PARTITION_CHECKPOINT_DOMAIN: &[u8] =
    b"market-squawk/provider-logical-publication/checkpoint/v1";

/// Closed role of one application-sealed source object.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalObjectRole {
    /// Small selected provider catalog or descriptor object.
    Catalog,
    /// Provider-delivered payload, including a compressed bulk object.
    ProviderPayload,
    /// Deterministically expanded provider payload, such as an IEX PCAP.
    ExpandedPayload,
    /// One component in a complete official provider surface.
    ProviderComponent,
}

impl LogicalObjectRole {
    const fn tag(self) -> u8 {
        match self {
            Self::Catalog => 1,
            Self::ProviderPayload => 2,
            Self::ExpandedPayload => 3,
            Self::ProviderComponent => 4,
        }
    }
}

/// Closed evidence-partition families shared by large provider verticals.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalPartitionFamily {
    /// Exact decoded provider events, before derivation.
    DecodedEvent,
    /// Exact provider-native semantics aligned to canonical output rows.
    ProviderNative,
    /// Exact canonical-row to provider-evidence coordinates, including one-to-many mappings.
    CanonicalRowMap,
    /// Exact identity-resolution assertions emitted by a reference parser.
    ResolverAssertion,
    /// Exact resolver decisions emitted from those assertions.
    ResolverOutcome,
    /// Exact unresolved or contradictory identity evidence retained fail-closed.
    ResolverConflict,
}

impl LogicalPartitionFamily {
    const fn tag(self) -> u8 {
        match self {
            Self::DecodedEvent => 1,
            Self::ProviderNative => 2,
            Self::CanonicalRowMap => 3,
            Self::ResolverAssertion => 4,
            Self::ResolverOutcome => 5,
            Self::ResolverConflict => 6,
        }
    }
}

/// Contiguous global item range represented by one bounded partition.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalItemRange {
    first_ordinal: u64,
    item_count: NonZeroU32,
}

impl LogicalItemRange {
    /// Constructs a nonempty range whose exclusive end fits in `u64`.
    pub fn try_new(
        first_ordinal: u64,
        item_count: NonZeroU32,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        first_ordinal
            .checked_add(u64::from(item_count.get()))
            .ok_or(ProviderLogicalPublicationError::OrdinalOverflow)?;
        Ok(Self {
            first_ordinal,
            item_count,
        })
    }

    /// Returns the first global item ordinal.
    pub const fn first_ordinal(self) -> u64 {
        self.first_ordinal
    }

    /// Returns the nonzero item count.
    pub const fn item_count(self) -> NonZeroU32 {
        self.item_count
    }

    /// Returns the exclusive global item ordinal.
    pub fn end_exclusive(self) -> Result<u64, ProviderLogicalPublicationError> {
        self.first_ordinal
            .checked_add(u64::from(self.item_count.get()))
            .ok_or(ProviderLogicalPublicationError::OrdinalOverflow)
    }
}

/// Code-owned bounds for one streamed partition set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogicalPartitionSetAdmission {
    object: ResearchObjectAdmission,
    maximum_partitions: u32,
    maximum_items_per_partition: u32,
    maximum_frame_bytes: u64,
}

impl LogicalPartitionSetAdmission {
    /// Constructs explicit nonzero limits below common format and publication ceilings.
    pub fn try_new(
        object: ResearchObjectAdmission,
        maximum_partitions: u32,
        maximum_items_per_partition: u32,
        maximum_frame_bytes: u64,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        if maximum_partitions == 0
            || usize::try_from(maximum_partitions)
                .ok()
                .is_none_or(|value| value > MAX_PROVIDER_LOGICAL_PARTITIONS)
            || maximum_items_per_partition == 0
            || maximum_frame_bytes == 0
            || maximum_frame_bytes
                .checked_add(LOGICAL_PARTITION_FRAME_HEADER_BYTES)
                .is_none_or(|bytes| bytes > object.maximum_bytes())
        {
            return Err(ProviderLogicalPublicationError::InvalidAdmission);
        }
        Ok(Self {
            object,
            maximum_partitions,
            maximum_items_per_partition,
            maximum_frame_bytes,
        })
    }

    /// Returns the underlying logical-object admission.
    pub const fn object(self) -> ResearchObjectAdmission {
        self.object
    }

    /// Returns the maximum partition count.
    pub const fn maximum_partitions(self) -> u32 {
        self.maximum_partitions
    }

    /// Returns the maximum item count per partition.
    pub const fn maximum_items_per_partition(self) -> u32 {
        self.maximum_items_per_partition
    }

    /// Returns the maximum payload bytes in one framed item.
    pub const fn maximum_frame_bytes(self) -> u64 {
        self.maximum_frame_bytes
    }
}

/// Stable coordinate returned immediately after one complete framed item is staged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedLogicalItemCoordinate {
    family: LogicalPartitionFamily,
    partition_ordinal: u32,
    partition_item_ordinal: u32,
    global_item_ordinal: u64,
    frame_digest: EvidenceDigest,
}

impl StagedLogicalItemCoordinate {
    /// Returns the evidence family.
    pub const fn family(self) -> LogicalPartitionFamily {
        self.family
    }

    /// Returns the zero-based partition ordinal.
    pub const fn partition_ordinal(self) -> u32 {
        self.partition_ordinal
    }

    /// Returns the zero-based item ordinal within the partition.
    pub const fn partition_item_ordinal(self) -> u32 {
        self.partition_item_ordinal
    }

    /// Returns the global item ordinal supplied to `stage_frame`.
    pub const fn global_item_ordinal(self) -> u64 {
        self.global_item_ordinal
    }

    /// Returns the SHA-256 digest of the exact payload bytes, excluding the frame header.
    pub const fn frame_digest(self) -> EvidenceDigest {
        self.frame_digest
    }
}

/// Live application-owned logical object with its closed semantic role.
#[derive(Debug)]
pub struct SealedLogicalObjectInput {
    role: LogicalObjectRole,
    ordinal: u32,
    semantic_identity: EvidenceDigest,
    object: ResearchObjectReceipt,
}

impl SealedLogicalObjectInput {
    /// Re-verifies and binds one live immutable object to an exact ordered provider role.
    pub fn try_from_verified(
        role: LogicalObjectRole,
        ordinal: u32,
        semantic_identity: EvidenceDigest,
        object: VerifiedResearchObject,
        control: &dyn ResearchObjectControl,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        validate_sha256(semantic_identity)?;
        let object = object.reverify_for_commit(control)?;
        if object.size_bytes() == 0 {
            return Err(ProviderLogicalPublicationError::EmptyLogicalObject);
        }
        Ok(Self {
            role,
            ordinal,
            semantic_identity,
            object,
        })
    }

    /// Returns the closed object role.
    pub const fn role(&self) -> LogicalObjectRole {
        self.role
    }

    /// Returns its zero-based order in the complete raw object graph.
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns provider/application semantic identity for the exact object.
    pub const fn semantic_identity(&self) -> EvidenceDigest {
        self.semantic_identity
    }

    /// Returns the live physical object receipt.
    pub const fn object(&self) -> &ResearchObjectReceipt {
        &self.object
    }
}

/// One live, bounded evidence partition.
#[derive(Debug)]
pub struct SealedLogicalPartitionInput {
    family: LogicalPartitionFamily,
    partition_ordinal: u32,
    item_range: LogicalItemRange,
    schema_identity: EvidenceDigest,
    semantic_digest: EvidenceDigest,
    object: ResearchObjectReceipt,
}

impl SealedLogicalPartitionInput {
    fn try_from_verified(
        family: LogicalPartitionFamily,
        partition_ordinal: u32,
        item_range: LogicalItemRange,
        schema_identity: EvidenceDigest,
        object: ResearchObjectReceipt,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        validate_sha256(schema_identity)?;
        let semantic_digest = partition_semantic_digest(
            family,
            partition_ordinal,
            item_range,
            schema_identity,
            &object,
        );
        Ok(Self {
            family,
            partition_ordinal,
            item_range,
            schema_identity,
            semantic_digest,
            object,
        })
    }

    /// Returns the evidence family.
    pub const fn family(&self) -> LogicalPartitionFamily {
        self.family
    }

    /// Returns the zero-based ordinal within its family.
    pub const fn partition_ordinal(&self) -> u32 {
        self.partition_ordinal
    }

    /// Returns the exact represented global item range.
    pub const fn item_range(&self) -> LogicalItemRange {
        self.item_range
    }

    /// Returns the code/provider-owned schema identity.
    pub const fn schema_identity(&self) -> EvidenceDigest {
        self.schema_identity
    }

    /// Returns the digest binding family, range, schema, and exact physical object.
    pub const fn semantic_digest(&self) -> EvidenceDigest {
        self.semantic_digest
    }

    /// Returns the live physical object receipt.
    pub const fn object(&self) -> &ResearchObjectReceipt {
        &self.object
    }
}

/// Persistable value-only evidence for one completed logical partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SealedLogicalPartitionClaim {
    family: LogicalPartitionFamily,
    partition_ordinal: u32,
    item_range: LogicalItemRange,
    schema_identity: EvidenceDigest,
    semantic_digest: EvidenceDigest,
    object: ResearchObjectClaim,
}

impl SealedLogicalPartitionClaim {
    fn from_input(input: &SealedLogicalPartitionInput) -> Self {
        Self {
            family: input.family,
            partition_ordinal: input.partition_ordinal,
            item_range: input.item_range,
            schema_identity: input.schema_identity,
            semantic_digest: input.semantic_digest,
            object: input.object.claim().clone(),
        }
    }

    /// Returns the persistable physical object claim.
    pub const fn object(&self) -> &ResearchObjectClaim {
        &self.object
    }

    /// Returns the evidence family.
    pub const fn family(&self) -> LogicalPartitionFamily {
        self.family
    }

    /// Returns the zero-based ordinal within the family.
    pub const fn partition_ordinal(&self) -> u32 {
        self.partition_ordinal
    }

    /// Returns the represented global item range.
    pub const fn item_range(&self) -> LogicalItemRange {
        self.item_range
    }

    /// Returns the exact schema identity.
    pub const fn schema_identity(&self) -> EvidenceDigest {
        self.schema_identity
    }

    /// Returns the exact semantic partition digest.
    pub const fn semantic_digest(&self) -> EvidenceDigest {
        self.semantic_digest
    }
}

/// Expected immutable canonical partition, aligned to native and row-map partitions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalPartitionExpectation {
    partition_ordinal: u32,
    row_range: LogicalItemRange,
    schema_identity: EvidenceDigest,
    semantic_digest: EvidenceDigest,
    aligned_native_partition: u32,
    aligned_row_map_partition: u32,
}

impl CanonicalPartitionExpectation {
    /// Constructs one exact data-layer staging expectation.
    pub fn try_new(
        partition_ordinal: u32,
        row_range: LogicalItemRange,
        schema_identity: EvidenceDigest,
        semantic_digest: EvidenceDigest,
        aligned_native_partition: u32,
        aligned_row_map_partition: u32,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        validate_sha256(schema_identity)?;
        validate_sha256(semantic_digest)?;
        Ok(Self {
            partition_ordinal,
            row_range,
            schema_identity,
            semantic_digest,
            aligned_native_partition,
            aligned_row_map_partition,
        })
    }

    /// Returns the zero-based canonical generation-partition ordinal.
    pub const fn partition_ordinal(&self) -> u32 {
        self.partition_ordinal
    }

    /// Returns its exact global canonical row range.
    pub const fn row_range(&self) -> LogicalItemRange {
        self.row_range
    }

    /// Returns the canonical schema identity.
    pub const fn schema_identity(&self) -> EvidenceDigest {
        self.schema_identity
    }

    /// Returns the exact staged canonical content/lineage expectation.
    pub const fn semantic_digest(&self) -> EvidenceDigest {
        self.semantic_digest
    }

    /// Returns the aligned provider-native partition ordinal.
    pub const fn aligned_native_partition(&self) -> u32 {
        self.aligned_native_partition
    }

    /// Returns the aligned row-map partition ordinal.
    pub const fn aligned_row_map_partition(&self) -> u32 {
        self.aligned_row_map_partition
    }
}

/// Provider-specific terminal evidence supplied before common closure verification.
#[derive(Clone, Debug)]
pub struct ProviderLogicalTerminalInput {
    /// Exact source whose provider evidence is represented.
    pub source_id: SourceId,
    /// Exact source schema/contract/revision identity.
    pub source_revision_digest: EvidenceDigest,
    /// Optional exact execution/capture-attempt identity.
    pub execution_attempt_digest: Option<EvidenceDigest>,
    /// Digest of the provider-specific terminal parse/completeness/continuity payload.
    pub provider_terminal_evidence_digest: EvidenceDigest,
    /// Exact decoded-event count, zero when this vertical has no event family.
    pub total_decoded_events: u64,
    /// Exact canonical row count across ordered canonical partitions.
    pub total_canonical_rows: u64,
    /// Exact byte count across the complete logical-object graph.
    pub total_logical_object_bytes: u64,
}

/// Terminal whole-publication receipt after common ordered-set closure succeeds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLogicalTerminalReceipt {
    source_id: SourceId,
    source_revision_digest: EvidenceDigest,
    execution_attempt_digest: Option<EvidenceDigest>,
    provider_terminal_evidence_digest: EvidenceDigest,
    raw_object_set_digest: EvidenceDigest,
    evidence_partition_set_digest: EvidenceDigest,
    canonical_partition_set_digest: EvidenceDigest,
    total_decoded_events: u64,
    total_canonical_rows: u64,
    total_logical_object_bytes: u64,
    receipt_digest: EvidenceDigest,
}

impl ProviderLogicalTerminalReceipt {
    /// Returns the exact source.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns source schema/contract/revision identity.
    pub const fn source_revision_digest(&self) -> EvidenceDigest {
        self.source_revision_digest
    }

    /// Returns optional execution/capture-attempt identity.
    pub const fn execution_attempt_digest(&self) -> Option<EvidenceDigest> {
        self.execution_attempt_digest
    }

    /// Returns the provider-specific terminal evidence digest.
    pub const fn provider_terminal_evidence_digest(&self) -> EvidenceDigest {
        self.provider_terminal_evidence_digest
    }

    /// Returns the complete ordered raw-object set digest.
    pub const fn raw_object_set_digest(&self) -> EvidenceDigest {
        self.raw_object_set_digest
    }

    /// Returns the complete ordered evidence-partition set digest.
    pub const fn evidence_partition_set_digest(&self) -> EvidenceDigest {
        self.evidence_partition_set_digest
    }

    /// Returns the complete ordered canonical-partition expectation digest.
    pub const fn canonical_partition_set_digest(&self) -> EvidenceDigest {
        self.canonical_partition_set_digest
    }

    /// Returns the exact decoded-event count.
    pub const fn total_decoded_events(&self) -> u64 {
        self.total_decoded_events
    }

    /// Returns the exact canonical row count.
    pub const fn total_canonical_rows(&self) -> u64 {
        self.total_canonical_rows
    }

    /// Returns bytes across every raw logical object.
    pub const fn total_logical_object_bytes(&self) -> u64 {
        self.total_logical_object_bytes
    }

    /// Returns the terminal common receipt digest.
    pub const fn receipt_digest(&self) -> EvidenceDigest {
        self.receipt_digest
    }
}

/// Noncloneable final authority joining logical objects, evidence partitions, and canonical plans.
pub struct SealedProviderLogicalPublicationBinding {
    terminal: ProviderLogicalTerminalReceipt,
    objects: Box<[SealedLogicalObjectInput]>,
    partitions: Box<[SealedLogicalPartitionInput]>,
    canonical_partitions: Box<[CanonicalPartitionExpectation]>,
    binding_digest: EvidenceDigest,
}

impl std::fmt::Debug for SealedProviderLogicalPublicationBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SealedProviderLogicalPublicationBinding")
            .field("source_id", self.terminal.source_id())
            .field("objects", &self.objects.len())
            .field("partitions", &self.partitions.len())
            .field("canonical_partitions", &self.canonical_partitions.len())
            .field("binding_digest", &self.binding_digest)
            .finish()
    }
}

impl SealedProviderLogicalPublicationBinding {
    /// Validates exact ordered closure and consumes all live common publication evidence.
    pub fn try_new(
        terminal: ProviderLogicalTerminalInput,
        required_partition_families: &[LogicalPartitionFamily],
        objects: Vec<SealedLogicalObjectInput>,
        partitions: Vec<SealedLogicalPartitionInput>,
        canonical_partitions: Vec<CanonicalPartitionExpectation>,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        validate_sha256(terminal.source_revision_digest)?;
        if let Some(attempt) = terminal.execution_attempt_digest {
            validate_sha256(attempt)?;
        }
        validate_sha256(terminal.provider_terminal_evidence_digest)?;
        validate_required_families(required_partition_families, &partitions)?;
        validate_objects(&objects)?;
        validate_partitions(&partitions)?;
        validate_canonical_partitions(&canonical_partitions, &partitions)?;
        let encoded_claim_bytes = objects
            .iter()
            .map(|object| object.object().claim())
            .chain(
                partitions
                    .iter()
                    .map(|partition| partition.object().claim()),
            )
            .try_fold(0usize, |total, claim| {
                let encoded = serde_json::to_vec(claim)
                    .map_err(|_| ProviderLogicalPublicationError::StateConflict)?;
                total
                    .checked_add(encoded.len())
                    .filter(|bytes| *bytes <= MAX_PROVIDER_LOGICAL_CATALOG_BYTES)
                    .ok_or(ProviderLogicalPublicationError::CatalogMetadataLimitExceeded)
            })?;
        debug_assert!(encoded_claim_bytes <= MAX_PROVIDER_LOGICAL_CATALOG_BYTES);
        let total_logical_object_bytes = objects.iter().try_fold(0_u64, |total, object| {
            total
                .checked_add(object.object.size_bytes())
                .ok_or(ProviderLogicalPublicationError::CountOverflow)
        })?;
        if total_logical_object_bytes != terminal.total_logical_object_bytes {
            return Err(ProviderLogicalPublicationError::TerminalCountMismatch);
        }
        let total_canonical_rows =
            canonical_partitions
                .iter()
                .try_fold(0_u64, |total, partition| {
                    total
                        .checked_add(u64::from(partition.row_range.item_count.get()))
                        .ok_or(ProviderLogicalPublicationError::CountOverflow)
                })?;
        if total_canonical_rows != terminal.total_canonical_rows
            || family_item_count(&partitions, LogicalPartitionFamily::ProviderNative)?
                != total_canonical_rows
            || family_item_count(&partitions, LogicalPartitionFamily::CanonicalRowMap)?
                != total_canonical_rows
            || family_item_count(&partitions, LogicalPartitionFamily::DecodedEvent)?
                != terminal.total_decoded_events
        {
            return Err(ProviderLogicalPublicationError::TerminalCountMismatch);
        }

        let raw_object_set_digest = object_set_digest(&objects);
        let evidence_partition_set_digest = partition_set_digest(&partitions);
        let canonical_partition_set_digest = canonical_set_digest(&canonical_partitions);
        let mut receipt = ProviderLogicalTerminalReceipt {
            source_id: terminal.source_id,
            source_revision_digest: terminal.source_revision_digest,
            execution_attempt_digest: terminal.execution_attempt_digest,
            provider_terminal_evidence_digest: terminal.provider_terminal_evidence_digest,
            raw_object_set_digest,
            evidence_partition_set_digest,
            canonical_partition_set_digest,
            total_decoded_events: terminal.total_decoded_events,
            total_canonical_rows,
            total_logical_object_bytes,
            receipt_digest: empty_sha256(),
        };
        receipt.receipt_digest = terminal_receipt_digest(&receipt);
        let binding_digest = publication_binding_digest(&receipt);
        Ok(Self {
            terminal: receipt,
            objects: objects.into_boxed_slice(),
            partitions: partitions.into_boxed_slice(),
            canonical_partitions: canonical_partitions.into_boxed_slice(),
            binding_digest,
        })
    }

    /// Returns the terminal whole-publication receipt.
    pub const fn terminal(&self) -> &ProviderLogicalTerminalReceipt {
        &self.terminal
    }

    /// Returns ordered live raw-object inputs.
    pub const fn objects(&self) -> &[SealedLogicalObjectInput] {
        &self.objects
    }

    /// Returns ordered live evidence partitions.
    pub const fn partitions(&self) -> &[SealedLogicalPartitionInput] {
        &self.partitions
    }

    /// Returns ordered canonical staging expectations.
    pub const fn canonical_partitions(&self) -> &[CanonicalPartitionExpectation] {
        &self.canonical_partitions
    }

    /// Returns the final common binding digest.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }

    /// Consumes the noncloneable authority into its data-layer publication parts.
    pub fn into_parts(
        self,
    ) -> (
        ProviderLogicalTerminalReceipt,
        Box<[SealedLogicalObjectInput]>,
        Box<[SealedLogicalPartitionInput]>,
        Box<[CanonicalPartitionExpectation]>,
    ) {
        (
            self.terminal,
            self.objects,
            self.partitions,
            self.canonical_partitions,
        )
    }
}

/// Value-only restart checkpoint for one partition set.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LogicalPartitionSetCheckpoint {
    family: LogicalPartitionFamily,
    schema_identity: EvidenceDigest,
    maximum_object_bytes: u64,
    maximum_object_chunks: u32,
    object_integrity_chunk_bytes: u64,
    maximum_partitions: u32,
    maximum_items_per_partition: u32,
    maximum_frame_bytes: u64,
    first_item_ordinal: u64,
    next_item_ordinal: u64,
    next_partition_ordinal: u32,
    completed: Box<[SealedLogicalPartitionClaim]>,
    current: Option<ResearchObjectCheckpointClaim>,
    current_first_ordinal: u64,
    current_items: u32,
    current_bytes: u64,
    checkpoint_digest: EvidenceDigest,
}

impl LogicalPartitionSetCheckpoint {
    /// Returns the underlying pending-stage checkpoint, when the current partition is nonempty.
    pub const fn pending_object(&self) -> Option<&ResearchObjectCheckpointClaim> {
        self.current.as_ref()
    }

    /// Returns completed immutable partition claims.
    pub const fn completed_partitions(&self) -> &[SealedLogicalPartitionClaim] {
        &self.completed
    }

    /// Returns the digest binding the complete partition-set checkpoint.
    pub const fn checkpoint_digest(&self) -> EvidenceDigest {
        self.checkpoint_digest
    }
}

/// Noncloneable owner of one bounded streamed evidence-partition set.
pub struct PendingLogicalPartitionSet {
    family: LogicalPartitionFamily,
    schema_identity: EvidenceDigest,
    admission: LogicalPartitionSetAdmission,
    first_item_ordinal: u64,
    next_item_ordinal: u64,
    next_partition_ordinal: u32,
    completed: Vec<SealedLogicalPartitionInput>,
    current: Option<PendingResearchObject>,
    current_first_ordinal: u64,
    current_items: u32,
    current_bytes: u64,
    poisoned: bool,
}

impl std::fmt::Debug for PendingLogicalPartitionSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingLogicalPartitionSet")
            .field("family", &self.family)
            .field("completed", &self.completed.len())
            .field("current_items", &self.current_items)
            .field("current_bytes", &self.current_bytes)
            .field("next_item_ordinal", &self.next_item_ordinal)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

impl PendingLogicalPartitionSet {
    /// Begins an empty set. The first physical stage is created lazily on the first item.
    pub fn begin(
        family: LogicalPartitionFamily,
        schema_identity: EvidenceDigest,
        admission: LogicalPartitionSetAdmission,
        first_item_ordinal: u64,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        validate_sha256(schema_identity)?;
        Ok(Self {
            family,
            schema_identity,
            admission,
            first_item_ordinal,
            next_item_ordinal: first_item_ordinal,
            next_partition_ordinal: 0,
            completed: Vec::new(),
            current: None,
            current_first_ordinal: first_item_ordinal,
            current_items: 0,
            current_bytes: 0,
            poisoned: false,
        })
    }

    /// Restores a value checkpoint only after exact raw-object and frame verification.
    pub fn resume(
        checkpoint: LogicalPartitionSetCheckpoint,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<Self, ProviderLogicalPublicationError> {
        validate_partition_set_checkpoint(&checkpoint)?;
        let object = ResearchObjectAdmission::try_new(
            checkpoint.maximum_object_bytes,
            usize::try_from(checkpoint.maximum_object_chunks)
                .map_err(|_| ProviderLogicalPublicationError::InvalidCheckpoint)?,
        )?;
        if object.integrity_chunk_bytes() != checkpoint.object_integrity_chunk_bytes {
            return Err(ProviderLogicalPublicationError::InvalidCheckpoint);
        }
        let admission = LogicalPartitionSetAdmission::try_new(
            object,
            checkpoint.maximum_partitions,
            checkpoint.maximum_items_per_partition,
            checkpoint.maximum_frame_bytes,
        )?;
        let mut completed = Vec::new();
        completed
            .try_reserve_exact(checkpoint.completed.len())
            .map_err(|_| ProviderLogicalPublicationError::Allocation)?;
        for claimed in &checkpoint.completed {
            let mut verified =
                store.open_verified_logical_object_claim(&claimed.object, control)?;
            verify_partition_frames(
                &mut verified,
                claimed.item_range,
                admission.maximum_frame_bytes,
            )?;
            let receipt = verified.reverify_for_commit(control)?;
            let input = SealedLogicalPartitionInput::try_from_verified(
                claimed.family,
                claimed.partition_ordinal,
                claimed.item_range,
                claimed.schema_identity,
                receipt,
            )?;
            if input.semantic_digest != claimed.semantic_digest {
                return Err(ProviderLogicalPublicationError::InvalidCheckpoint);
            }
            completed.push(input);
        }
        let current = match checkpoint.current.as_ref() {
            Some(claim) => Some(store.resume_logical_object(object, claim)?),
            None => None,
        };
        Ok(Self {
            family: checkpoint.family,
            schema_identity: checkpoint.schema_identity,
            admission,
            first_item_ordinal: checkpoint.first_item_ordinal,
            next_item_ordinal: checkpoint.next_item_ordinal,
            next_partition_ordinal: checkpoint.next_partition_ordinal,
            completed,
            current,
            current_first_ordinal: checkpoint.current_first_ordinal,
            current_items: checkpoint.current_items,
            current_bytes: checkpoint.current_bytes,
            poisoned: false,
        })
    }

    /// Appends one exact `{ordinal LE, length LE, payload}` frame and returns its coordinate.
    pub fn stage_frame(
        &mut self,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
        item_ordinal: u64,
        bytes: &[u8],
        semantic_digest: EvidenceDigest,
    ) -> Result<StagedLogicalItemCoordinate, ProviderLogicalPublicationError> {
        self.ensure_writable()?;
        if item_ordinal != self.next_item_ordinal || bytes.is_empty() {
            return Err(ProviderLogicalPublicationError::NonContiguousItems);
        }
        let payload_bytes = u64::try_from(bytes.len())
            .map_err(|_| ProviderLogicalPublicationError::FrameLimitExceeded)?;
        if payload_bytes > self.admission.maximum_frame_bytes {
            return Err(ProviderLogicalPublicationError::FrameLimitExceeded);
        }
        let actual_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into());
        if semantic_digest != actual_digest {
            return Err(ProviderLogicalPublicationError::FrameDigestMismatch);
        }
        let frame_bytes = payload_bytes
            .checked_add(LOGICAL_PARTITION_FRAME_HEADER_BYTES)
            .ok_or(ProviderLogicalPublicationError::FrameLimitExceeded)?;
        let would_exceed = self.current_items == self.admission.maximum_items_per_partition
            || self
                .current_bytes
                .checked_add(frame_bytes)
                .is_none_or(|bytes| bytes > self.admission.object.maximum_bytes());
        if self.current_items > 0 && would_exceed {
            self.seal_current(store, control)?;
        }
        if self.next_partition_ordinal >= self.admission.maximum_partitions {
            return Err(ProviderLogicalPublicationError::PartitionLimitExceeded);
        }
        if self.current.is_none() {
            self.current = Some(store.begin_logical_object(self.admission.object)?);
            self.current_first_ordinal = item_ordinal;
            self.current_items = 0;
            self.current_bytes = 0;
        }
        if self
            .current_bytes
            .checked_add(frame_bytes)
            .is_none_or(|bytes| bytes > self.admission.object.maximum_bytes())
        {
            return Err(ProviderLogicalPublicationError::FrameLimitExceeded);
        }
        let partition_item_ordinal = self.current_items;
        let pending = self
            .current
            .as_mut()
            .ok_or(ProviderLogicalPublicationError::StateConflict)?;
        self.poisoned = true;
        pending.write_all(&item_ordinal.to_le_bytes())?;
        pending.write_all(&payload_bytes.to_le_bytes())?;
        pending.write_all(bytes)?;
        self.current_items = self
            .current_items
            .checked_add(1)
            .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        self.current_bytes = self
            .current_bytes
            .checked_add(frame_bytes)
            .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        self.next_item_ordinal = self
            .next_item_ordinal
            .checked_add(1)
            .ok_or(ProviderLogicalPublicationError::OrdinalOverflow)?;
        self.poisoned = false;
        Ok(StagedLogicalItemCoordinate {
            family: self.family,
            partition_ordinal: self.next_partition_ordinal,
            partition_item_ordinal,
            global_item_ordinal: item_ordinal,
            frame_digest: semantic_digest,
        })
    }

    /// Returns whether one next payload fits the current partition without an automatic roll.
    ///
    /// Multi-family producers use this before staging a canonical row so every aligned family can
    /// be sealed at the same row boundary.
    pub fn current_partition_accepts(
        &self,
        payload_bytes: u64,
    ) -> Result<bool, ProviderLogicalPublicationError> {
        self.ensure_writable()?;
        if payload_bytes == 0 || payload_bytes > self.admission.maximum_frame_bytes {
            return Err(ProviderLogicalPublicationError::FrameLimitExceeded);
        }
        let frame_bytes = payload_bytes
            .checked_add(LOGICAL_PARTITION_FRAME_HEADER_BYTES)
            .ok_or(ProviderLogicalPublicationError::FrameLimitExceeded)?;
        Ok(
            self.current_items < self.admission.maximum_items_per_partition
                && self
                    .current_bytes
                    .checked_add(frame_bytes)
                    .is_some_and(|bytes| bytes <= self.admission.object.maximum_bytes()),
        )
    }

    /// Returns the current partition ordinal, including an empty lazily staged partition.
    pub const fn current_partition_ordinal(&self) -> u32 {
        self.next_partition_ordinal
    }

    /// Returns the current nonempty global item range.
    pub fn current_partition_range(
        &self,
    ) -> Result<Option<LogicalItemRange>, ProviderLogicalPublicationError> {
        NonZeroU32::new(self.current_items)
            .map(|count| LogicalItemRange::try_new(self.current_first_ordinal, count))
            .transpose()
    }

    /// Explicitly closes a nonempty current partition at an application-coordinated boundary.
    pub fn seal_current_partition(
        &mut self,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<(), ProviderLogicalPublicationError> {
        self.ensure_writable()?;
        self.seal_current(store, control)
    }

    /// Synchronizes the current stage and returns complete value-only restart evidence.
    pub fn checkpoint(
        &mut self,
        store: &SealedResearchJournalStore,
    ) -> Result<LogicalPartitionSetCheckpoint, ProviderLogicalPublicationError> {
        self.ensure_writable()?;
        let current = match self.current.as_mut() {
            Some(pending) => Some(store.checkpoint_logical_object(pending)?),
            None => None,
        };
        let mut completed = Vec::new();
        completed
            .try_reserve_exact(self.completed.len())
            .map_err(|_| ProviderLogicalPublicationError::Allocation)?;
        completed.extend(
            self.completed
                .iter()
                .map(SealedLogicalPartitionClaim::from_input),
        );
        let mut checkpoint = LogicalPartitionSetCheckpoint {
            family: self.family,
            schema_identity: self.schema_identity,
            maximum_object_bytes: self.admission.object.maximum_bytes(),
            maximum_object_chunks: u32::try_from(self.admission.object.maximum_chunks())
                .map_err(|_| ProviderLogicalPublicationError::InvalidAdmission)?,
            object_integrity_chunk_bytes: self.admission.object.integrity_chunk_bytes(),
            maximum_partitions: self.admission.maximum_partitions,
            maximum_items_per_partition: self.admission.maximum_items_per_partition,
            maximum_frame_bytes: self.admission.maximum_frame_bytes,
            first_item_ordinal: self.first_item_ordinal,
            next_item_ordinal: self.next_item_ordinal,
            next_partition_ordinal: self.next_partition_ordinal,
            completed: completed.into_boxed_slice(),
            current,
            current_first_ordinal: self.current_first_ordinal,
            current_items: self.current_items,
            current_bytes: self.current_bytes,
            checkpoint_digest: empty_sha256(),
        };
        checkpoint.checkpoint_digest = partition_set_checkpoint_digest(&checkpoint);
        validate_partition_set_checkpoint(&checkpoint)?;
        Ok(checkpoint)
    }

    /// Finishes the current partition and returns ordered immutable partition receipts.
    pub fn finish(
        mut self,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<SealedLogicalPartitionSet, ProviderLogicalPublicationError> {
        self.ensure_writable()?;
        self.seal_current(store, control)?;
        if self.completed.is_empty() {
            return Err(ProviderLogicalPublicationError::EmptyPartitionSet);
        }
        validate_partitions(&self.completed)?;
        let set_digest = partition_set_digest(&self.completed);
        Ok(SealedLogicalPartitionSet {
            family: self.family,
            schema_identity: self.schema_identity,
            first_item_ordinal: self.first_item_ordinal,
            next_item_ordinal: self.next_item_ordinal,
            partitions: self.completed.into_boxed_slice(),
            set_digest,
        })
    }

    /// Cancels the current writable stage. Completed immutable objects remain unreferenced and
    /// are quarantined by ordinary raw-object recovery.
    pub fn abort(
        mut self,
        store: &SealedResearchJournalStore,
    ) -> Result<(), ProviderLogicalPublicationError> {
        if let Some(pending) = self.current.take() {
            store.abort_logical_object(pending)?;
        }
        Ok(())
    }

    fn ensure_writable(&self) -> Result<(), ProviderLogicalPublicationError> {
        if self.poisoned {
            Err(ProviderLogicalPublicationError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn seal_current(
        &mut self,
        store: &SealedResearchJournalStore,
        control: &dyn ResearchObjectControl,
    ) -> Result<(), ProviderLogicalPublicationError> {
        let Some(pending) = self.current.take() else {
            return Ok(());
        };
        if self.current_items == 0 {
            store.abort_logical_object(pending)?;
            self.current_bytes = 0;
            return Ok(());
        }
        let item_count = NonZeroU32::new(self.current_items)
            .ok_or(ProviderLogicalPublicationError::EmptyPartitionSet)?;
        let range = LogicalItemRange::try_new(self.current_first_ordinal, item_count)?;
        let mut verified = store.finish_logical_object(pending, control)?;
        verify_partition_frames(&mut verified, range, self.admission.maximum_frame_bytes)?;
        let receipt = verified.reverify_for_commit(control)?;
        let partition = SealedLogicalPartitionInput::try_from_verified(
            self.family,
            self.next_partition_ordinal,
            range,
            self.schema_identity,
            receipt,
        )?;
        self.completed
            .try_reserve(1)
            .map_err(|_| ProviderLogicalPublicationError::Allocation)?;
        self.completed.push(partition);
        self.next_partition_ordinal = self
            .next_partition_ordinal
            .checked_add(1)
            .ok_or(ProviderLogicalPublicationError::PartitionLimitExceeded)?;
        self.current_first_ordinal = self.next_item_ordinal;
        self.current_items = 0;
        self.current_bytes = 0;
        Ok(())
    }
}

/// Completed noncloneable partition-set owner.
#[derive(Debug)]
pub struct SealedLogicalPartitionSet {
    family: LogicalPartitionFamily,
    schema_identity: EvidenceDigest,
    first_item_ordinal: u64,
    next_item_ordinal: u64,
    partitions: Box<[SealedLogicalPartitionInput]>,
    set_digest: EvidenceDigest,
}

impl SealedLogicalPartitionSet {
    /// Returns the evidence family.
    pub const fn family(&self) -> LogicalPartitionFamily {
        self.family
    }

    /// Returns the schema identity.
    pub const fn schema_identity(&self) -> EvidenceDigest {
        self.schema_identity
    }

    /// Returns the first global item ordinal.
    pub const fn first_item_ordinal(&self) -> u64 {
        self.first_item_ordinal
    }

    /// Returns the exclusive global item ordinal.
    pub const fn next_item_ordinal(&self) -> u64 {
        self.next_item_ordinal
    }

    /// Returns ordered immutable partitions.
    pub const fn partitions(&self) -> &[SealedLogicalPartitionInput] {
        &self.partitions
    }

    /// Returns the complete ordered partition-set digest.
    pub const fn set_digest(&self) -> EvidenceDigest {
        self.set_digest
    }

    /// Consumes the set into publication inputs.
    pub fn into_partitions(self) -> Box<[SealedLogicalPartitionInput]> {
        self.partitions
    }
}

/// Invalid logical-object or partition-publication state.
#[derive(Debug, Error)]
pub enum ProviderLogicalPublicationError {
    /// Logical-object store operation failed.
    #[error(transparent)]
    ObjectStore(#[from] SealedResearchJournalStoreError),
    /// Framed staging I/O failed.
    #[error("logical partition staging I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Admission is zero, inconsistent, or exceeds a common ceiling.
    #[error("logical partition admission is invalid")]
    InvalidAdmission,
    /// Allocation under an admitted bound failed.
    #[error("logical publication bounded allocation failed")]
    Allocation,
    /// A SHA-256 evidence digest was required.
    #[error("logical publication evidence digest is invalid")]
    InvalidDigest,
    /// One provider logical object was empty.
    #[error("provider logical object must not be empty")]
    EmptyLogicalObject,
    /// The partition set contained no items.
    #[error("logical partition set must not be empty")]
    EmptyPartitionSet,
    /// A frame exceeded the admitted per-item or object byte ceiling.
    #[error("logical partition frame limit was exceeded")]
    FrameLimitExceeded,
    /// The caller-supplied digest did not match exact frame payload bytes.
    #[error("logical partition frame digest does not match its bytes")]
    FrameDigestMismatch,
    /// Items or partitions were missing, reordered, duplicated, or overlapping.
    #[error("logical publication order is not contiguous")]
    NonContiguousItems,
    /// The admitted partition count was exceeded.
    #[error("logical publication partition limit was exceeded")]
    PartitionLimitExceeded,
    /// An item ordinal overflowed.
    #[error("logical publication item ordinal overflowed")]
    OrdinalOverflow,
    /// A count or byte total overflowed.
    #[error("logical publication count overflowed")]
    CountOverflow,
    /// Encoded publication metadata exceeded the complete-generation ceiling.
    #[error("logical publication catalog metadata limit was exceeded")]
    CatalogMetadataLimitExceeded,
    /// A restart checkpoint was structurally or cryptographically inconsistent.
    #[error("logical partition checkpoint is invalid")]
    InvalidCheckpoint,
    /// Required and provided partition-family closure differed.
    #[error("logical publication partition-family closure is incomplete")]
    PartitionFamilyMismatch,
    /// Canonical, native, and row-map partition alignment differed.
    #[error("logical publication canonical/native/row-map alignment is invalid")]
    PartitionAlignmentMismatch,
    /// Terminal provider totals did not equal the exact common closure.
    #[error("logical publication terminal totals do not match exact evidence")]
    TerminalCountMismatch,
    /// A prior partial write made the current stage unusable except for abort/recovery.
    #[error("logical partition stage is poisoned after a partial write")]
    Poisoned,
    /// Internal single-owner state was inconsistent.
    #[error("logical publication state is inconsistent")]
    StateConflict,
}

fn validate_sha256(digest: EvidenceDigest) -> Result<(), ProviderLogicalPublicationError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0_u8; 32] {
        Err(ProviderLogicalPublicationError::InvalidDigest)
    } else {
        Ok(())
    }
}

fn validate_objects(
    objects: &[SealedLogicalObjectInput],
) -> Result<(), ProviderLogicalPublicationError> {
    if objects.is_empty() || objects.len() > MAX_PROVIDER_LOGICAL_OBJECTS {
        return Err(ProviderLogicalPublicationError::PartitionLimitExceeded);
    }
    for (ordinal, object) in objects.iter().enumerate() {
        if object.ordinal
            != u32::try_from(ordinal)
                .map_err(|_| ProviderLogicalPublicationError::OrdinalOverflow)?
        {
            return Err(ProviderLogicalPublicationError::NonContiguousItems);
        }
        validate_sha256(object.semantic_identity)?;
        if object.object.size_bytes() == 0 {
            return Err(ProviderLogicalPublicationError::EmptyLogicalObject);
        }
    }
    Ok(())
}

fn validate_partitions(
    partitions: &[SealedLogicalPartitionInput],
) -> Result<(), ProviderLogicalPublicationError> {
    if partitions.len() > MAX_PROVIDER_LOGICAL_PARTITIONS {
        return Err(ProviderLogicalPublicationError::PartitionLimitExceeded);
    }
    let mut prior_family = None;
    let mut expected_partition = 0_u32;
    let mut expected_item = None;
    for partition in partitions {
        validate_sha256(partition.schema_identity)?;
        let expected_semantic = partition_semantic_digest(
            partition.family,
            partition.partition_ordinal,
            partition.item_range,
            partition.schema_identity,
            &partition.object,
        );
        if expected_semantic != partition.semantic_digest {
            return Err(ProviderLogicalPublicationError::InvalidDigest);
        }
        if prior_family != Some(partition.family) {
            if prior_family.is_some_and(|prior| prior >= partition.family) {
                return Err(ProviderLogicalPublicationError::NonContiguousItems);
            }
            prior_family = Some(partition.family);
            expected_partition = 0;
            expected_item = Some(partition.item_range.first_ordinal);
        }
        if partition.partition_ordinal != expected_partition
            || expected_item != Some(partition.item_range.first_ordinal)
        {
            return Err(ProviderLogicalPublicationError::NonContiguousItems);
        }
        expected_partition = expected_partition
            .checked_add(1)
            .ok_or(ProviderLogicalPublicationError::OrdinalOverflow)?;
        expected_item = Some(partition.item_range.end_exclusive()?);
    }
    Ok(())
}

fn validate_required_families(
    required: &[LogicalPartitionFamily],
    partitions: &[SealedLogicalPartitionInput],
) -> Result<(), ProviderLogicalPublicationError> {
    if required.is_empty() || required.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ProviderLogicalPublicationError::PartitionFamilyMismatch);
    }
    let required = required.iter().copied().collect::<BTreeSet<_>>();
    let provided = partitions
        .iter()
        .map(|partition| partition.family)
        .collect::<BTreeSet<_>>();
    if required != provided {
        return Err(ProviderLogicalPublicationError::PartitionFamilyMismatch);
    }
    Ok(())
}

fn validate_canonical_partitions(
    canonical: &[CanonicalPartitionExpectation],
    partitions: &[SealedLogicalPartitionInput],
) -> Result<(), ProviderLogicalPublicationError> {
    if canonical.len() > MAX_PROVIDER_CANONICAL_PARTITIONS {
        return Err(ProviderLogicalPublicationError::PartitionLimitExceeded);
    }
    let native = partitions
        .iter()
        .filter(|partition| partition.family == LogicalPartitionFamily::ProviderNative)
        .collect::<Vec<_>>();
    let row_maps = partitions
        .iter()
        .filter(|partition| partition.family == LogicalPartitionFamily::CanonicalRowMap)
        .collect::<Vec<_>>();
    let mut expected_row = canonical
        .first()
        .map(|partition| partition.row_range.first_ordinal);
    for (ordinal, expected) in canonical.iter().enumerate() {
        validate_sha256(expected.schema_identity)?;
        validate_sha256(expected.semantic_digest)?;
        if expected.partition_ordinal
            != u32::try_from(ordinal)
                .map_err(|_| ProviderLogicalPublicationError::OrdinalOverflow)?
            || expected_row != Some(expected.row_range.first_ordinal)
        {
            return Err(ProviderLogicalPublicationError::NonContiguousItems);
        }
        let native = native
            .get(
                usize::try_from(expected.aligned_native_partition)
                    .map_err(|_| ProviderLogicalPublicationError::PartitionAlignmentMismatch)?,
            )
            .ok_or(ProviderLogicalPublicationError::PartitionAlignmentMismatch)?;
        let row_map = row_maps
            .get(
                usize::try_from(expected.aligned_row_map_partition)
                    .map_err(|_| ProviderLogicalPublicationError::PartitionAlignmentMismatch)?,
            )
            .ok_or(ProviderLogicalPublicationError::PartitionAlignmentMismatch)?;
        if native.partition_ordinal != expected.aligned_native_partition
            || row_map.partition_ordinal != expected.aligned_row_map_partition
            || native.item_range != expected.row_range
            || row_map.item_range != expected.row_range
        {
            return Err(ProviderLogicalPublicationError::PartitionAlignmentMismatch);
        }
        expected_row = Some(expected.row_range.end_exclusive()?);
    }
    if canonical.is_empty() != (native.is_empty() && row_maps.is_empty())
        || canonical.len() != native.len()
        || canonical.len() != row_maps.len()
    {
        return Err(ProviderLogicalPublicationError::PartitionAlignmentMismatch);
    }
    Ok(())
}

fn family_item_count(
    partitions: &[SealedLogicalPartitionInput],
    family: LogicalPartitionFamily,
) -> Result<u64, ProviderLogicalPublicationError> {
    partitions
        .iter()
        .filter(|partition| partition.family == family)
        .try_fold(0_u64, |total, partition| {
            total
                .checked_add(u64::from(partition.item_range.item_count.get()))
                .ok_or(ProviderLogicalPublicationError::CountOverflow)
        })
}

fn verify_partition_frames(
    object: &mut VerifiedResearchObject,
    range: LogicalItemRange,
    maximum_frame_bytes: u64,
) -> Result<(), ProviderLogicalPublicationError> {
    let mut expected_ordinal = range.first_ordinal;
    let end = range.end_exclusive()?;
    let mut observed_bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    while expected_ordinal < end {
        let mut header = [0_u8; 16];
        object.read_exact(&mut header)?;
        observed_bytes = observed_bytes
            .checked_add(LOGICAL_PARTITION_FRAME_HEADER_BYTES)
            .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        let ordinal = u64::from_le_bytes(
            header[..8]
                .try_into()
                .map_err(|_| ProviderLogicalPublicationError::StateConflict)?,
        );
        let payload_bytes = u64::from_le_bytes(
            header[8..]
                .try_into()
                .map_err(|_| ProviderLogicalPublicationError::StateConflict)?,
        );
        if ordinal != expected_ordinal || payload_bytes == 0 || payload_bytes > maximum_frame_bytes
        {
            return Err(ProviderLogicalPublicationError::NonContiguousItems);
        }
        let mut remaining = payload_bytes;
        while remaining > 0 {
            let read = usize::try_from(remaining)
                .unwrap_or(usize::MAX)
                .min(buffer.len());
            object.read_exact(&mut buffer[..read])?;
            remaining = remaining
                .checked_sub(
                    u64::try_from(read)
                        .map_err(|_| ProviderLogicalPublicationError::CountOverflow)?,
                )
                .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        }
        observed_bytes = observed_bytes
            .checked_add(payload_bytes)
            .ok_or(ProviderLogicalPublicationError::CountOverflow)?;
        expected_ordinal = expected_ordinal
            .checked_add(1)
            .ok_or(ProviderLogicalPublicationError::OrdinalOverflow)?;
    }
    let mut probe = [0_u8; 1];
    if object.read(&mut probe)? != 0 || observed_bytes != object.size_bytes() {
        return Err(ProviderLogicalPublicationError::NonContiguousItems);
    }
    Ok(())
}

fn validate_partition_set_checkpoint(
    checkpoint: &LogicalPartitionSetCheckpoint,
) -> Result<(), ProviderLogicalPublicationError> {
    validate_sha256(checkpoint.schema_identity)?;
    if checkpoint.checkpoint_digest != partition_set_checkpoint_digest(checkpoint)
        || checkpoint.completed.len() > MAX_PROVIDER_LOGICAL_PARTITIONS
        || usize::try_from(checkpoint.next_partition_ordinal).ok()
            != Some(checkpoint.completed.len())
        || checkpoint.current_items > checkpoint.maximum_items_per_partition
        || checkpoint.current_bytes > checkpoint.maximum_object_bytes
        || checkpoint.current.is_none() != (checkpoint.current_items == 0)
        || checkpoint
            .current
            .as_ref()
            .is_some_and(|claim| claim.size_bytes() != checkpoint.current_bytes)
    {
        return Err(ProviderLogicalPublicationError::InvalidCheckpoint);
    }
    let completed_items = checkpoint
        .completed
        .iter()
        .try_fold(0_u64, |total, partition| {
            if partition.family != checkpoint.family
                || partition.schema_identity != checkpoint.schema_identity
            {
                return Err(ProviderLogicalPublicationError::InvalidCheckpoint);
            }
            total
                .checked_add(u64::from(partition.item_range.item_count.get()))
                .ok_or(ProviderLogicalPublicationError::CountOverflow)
        })?;
    let expected_next = checkpoint
        .first_item_ordinal
        .checked_add(completed_items)
        .and_then(|ordinal| ordinal.checked_add(u64::from(checkpoint.current_items)))
        .ok_or(ProviderLogicalPublicationError::OrdinalOverflow)?;
    if expected_next != checkpoint.next_item_ordinal
        || checkpoint.current_first_ordinal
            != checkpoint
                .next_item_ordinal
                .checked_sub(u64::from(checkpoint.current_items))
                .ok_or(ProviderLogicalPublicationError::InvalidCheckpoint)?
    {
        return Err(ProviderLogicalPublicationError::InvalidCheckpoint);
    }
    Ok(())
}

fn object_set_digest(objects: &[SealedLogicalObjectInput]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_OBJECT_SET_DOMAIN);
    hash.update((objects.len() as u64).to_be_bytes());
    for object in objects {
        hash.update(object.role.tag().to_be_bytes());
        hash.update(object.ordinal.to_be_bytes());
        hash_digest(&mut hash, object.semantic_identity);
        hash_digest(&mut hash, object.object.content_digest());
        hash.update(object.object.size_bytes().to_be_bytes());
        hash_digest(&mut hash, object.object.claim().physical_receipt_digest());
    }
    sha256(hash)
}

fn partition_semantic_digest(
    family: LogicalPartitionFamily,
    partition_ordinal: u32,
    range: LogicalItemRange,
    schema_identity: EvidenceDigest,
    object: &ResearchObjectReceipt,
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_PARTITION_SEMANTIC_DOMAIN);
    hash.update(family.tag().to_be_bytes());
    hash.update(partition_ordinal.to_be_bytes());
    hash.update(range.first_ordinal.to_be_bytes());
    hash.update(range.item_count.get().to_be_bytes());
    hash_digest(&mut hash, schema_identity);
    hash_digest(&mut hash, object.content_digest());
    hash.update(object.size_bytes().to_be_bytes());
    hash_digest(&mut hash, object.claim().physical_receipt_digest());
    sha256(hash)
}

fn partition_set_digest(partitions: &[SealedLogicalPartitionInput]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_PARTITION_SET_DOMAIN);
    hash.update((partitions.len() as u64).to_be_bytes());
    for partition in partitions {
        hash.update(partition.family.tag().to_be_bytes());
        hash.update(partition.partition_ordinal.to_be_bytes());
        hash.update(partition.item_range.first_ordinal.to_be_bytes());
        hash.update(partition.item_range.item_count.get().to_be_bytes());
        hash_digest(&mut hash, partition.schema_identity);
        hash_digest(&mut hash, partition.semantic_digest);
    }
    sha256(hash)
}

fn canonical_set_digest(partitions: &[CanonicalPartitionExpectation]) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(CANONICAL_PARTITION_SET_DOMAIN);
    hash.update((partitions.len() as u64).to_be_bytes());
    for partition in partitions {
        hash.update(partition.partition_ordinal.to_be_bytes());
        hash.update(partition.row_range.first_ordinal.to_be_bytes());
        hash.update(partition.row_range.item_count.get().to_be_bytes());
        hash_digest(&mut hash, partition.schema_identity);
        hash_digest(&mut hash, partition.semantic_digest);
        hash.update(partition.aligned_native_partition.to_be_bytes());
        hash.update(partition.aligned_row_map_partition.to_be_bytes());
    }
    sha256(hash)
}

fn terminal_receipt_digest(receipt: &ProviderLogicalTerminalReceipt) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_TERMINAL_RECEIPT_DOMAIN);
    hash_field(&mut hash, receipt.source_id.as_str().as_bytes());
    hash_digest(&mut hash, receipt.source_revision_digest);
    hash_optional_digest(&mut hash, receipt.execution_attempt_digest);
    hash_digest(&mut hash, receipt.provider_terminal_evidence_digest);
    hash_digest(&mut hash, receipt.raw_object_set_digest);
    hash_digest(&mut hash, receipt.evidence_partition_set_digest);
    hash_digest(&mut hash, receipt.canonical_partition_set_digest);
    hash.update(receipt.total_decoded_events.to_be_bytes());
    hash.update(receipt.total_canonical_rows.to_be_bytes());
    hash.update(receipt.total_logical_object_bytes.to_be_bytes());
    sha256(hash)
}

fn publication_binding_digest(receipt: &ProviderLogicalTerminalReceipt) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_PUBLICATION_BINDING_DOMAIN);
    hash_digest(&mut hash, receipt.receipt_digest);
    hash_digest(&mut hash, receipt.raw_object_set_digest);
    hash_digest(&mut hash, receipt.evidence_partition_set_digest);
    hash_digest(&mut hash, receipt.canonical_partition_set_digest);
    sha256(hash)
}

fn partition_set_checkpoint_digest(checkpoint: &LogicalPartitionSetCheckpoint) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(LOGICAL_PARTITION_CHECKPOINT_DOMAIN);
    hash.update(checkpoint.family.tag().to_be_bytes());
    hash_digest(&mut hash, checkpoint.schema_identity);
    hash.update(checkpoint.maximum_object_bytes.to_be_bytes());
    hash.update(checkpoint.maximum_object_chunks.to_be_bytes());
    hash.update(checkpoint.object_integrity_chunk_bytes.to_be_bytes());
    hash.update(checkpoint.maximum_partitions.to_be_bytes());
    hash.update(checkpoint.maximum_items_per_partition.to_be_bytes());
    hash.update(checkpoint.maximum_frame_bytes.to_be_bytes());
    hash.update(checkpoint.first_item_ordinal.to_be_bytes());
    hash.update(checkpoint.next_item_ordinal.to_be_bytes());
    hash.update(checkpoint.next_partition_ordinal.to_be_bytes());
    hash.update((checkpoint.completed.len() as u64).to_be_bytes());
    for partition in &checkpoint.completed {
        hash.update(partition.family.tag().to_be_bytes());
        hash.update(partition.partition_ordinal.to_be_bytes());
        hash.update(partition.item_range.first_ordinal.to_be_bytes());
        hash.update(partition.item_range.item_count.get().to_be_bytes());
        hash_digest(&mut hash, partition.schema_identity);
        hash_digest(&mut hash, partition.semantic_digest);
        hash_digest(&mut hash, partition.object.content_digest());
        hash_digest(&mut hash, partition.object.physical_receipt_digest());
    }
    match &checkpoint.current {
        Some(current) => {
            hash.update([1]);
            hash_field(&mut hash, current.staging_reference().as_bytes());
            hash.update(current.size_bytes().to_be_bytes());
            hash_digest(&mut hash, current.prefix_digest());
        }
        None => hash.update([0]),
    }
    hash.update(checkpoint.current_first_ordinal.to_be_bytes());
    hash.update(checkpoint.current_items.to_be_bytes());
    hash.update(checkpoint.current_bytes.to_be_bytes());
    sha256(hash)
}

fn hash_field(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn hash_digest(hash: &mut Sha256, digest: EvidenceDigest) {
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
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

fn sha256(hash: Sha256) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn empty_sha256() -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest([]).into())
}
