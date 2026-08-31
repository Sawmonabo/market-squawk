//! Resumable, chunk-verified, content-addressed logical raw objects.
//!
//! `MSJ1` remains the bounded raw-record journal format. This sibling branch stores one logical raw
//! body without forcing it into an in-memory journal frame. A value claim is never authority:
//! authority is issued only after the store verifies the complete immutable object.

use std::{
    fmt,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::MutexGuard,
};

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{SeqAccess, Visitor},
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::{
    FileIdentity, MAX_RECOVERY_ENTRIES, MAX_SEALED_BYTES, RecoveryControl,
    SealedResearchJournalRecoveryReport, SealedResearchJournalSegmentClaim,
    SealedResearchJournalStore, SealedResearchJournalStoreError, bounded_entries, digest_hex,
    ensure_directory, hash_digest, hash_field, hash_file_bounded_with_control, is_lower_hex,
    lock_pending_stage, opened_file_metadata, quarantine_no_replace, quarantine_stage_no_replace,
    sync_directory, try_string_from_parts, validate_private_regular_file,
    validate_private_regular_file_links, validate_unclaimed_msj_with_control,
};

const FORMAT_INTEGRITY_CHUNK_BYTES: u64 = 16 * 1024 * 1024;
const FORMAT_MAX_CHUNKS: usize = 4_096;
const FORMAT_MAX_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
const MAX_STAGE_REFERENCE_BYTES: usize = 64;
const LOGICAL_OBJECT_SUFFIX: &str = ".mro";
const LOGICAL_STAGE_SUFFIX: &str = ".mro.stage";
const JOURNAL_OBJECT_SUFFIX: &str = ".msj";
const JOURNAL_STAGE_SUFFIX: &str = ".msj.stage";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RawObjectKind {
    JournalSegment,
    LogicalObject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinkedStageDisposition {
    PublishedFinal,
    Quarantined,
}

enum LinkedStageTarget {
    PublishedFinal {
        shard: Dir,
        filename: String,
        read_only_transition: Option<PreparedReadOnlyLink>,
    },
    Quarantined {
        quarantine_name: String,
    },
}

struct PreparedReadOnlyLink {
    file: File,
    identity: FileIdentity,
    size_bytes: u64,
    expected_links: u64,
    read_only_permissions: Option<std::fs::Permissions>,
}

#[derive(Clone, Copy)]
struct LinkedStageEvidence<'control> {
    kind: RawObjectKind,
    size_bytes: u64,
    identity: FileIdentity,
    content_digest: EvidenceDigest,
    control: &'control dyn ResearchObjectControl,
}

impl RawObjectKind {
    const fn object_suffix(self) -> &'static str {
        match self {
            Self::JournalSegment => JOURNAL_OBJECT_SUFFIX,
            Self::LogicalObject => LOGICAL_OBJECT_SUFFIX,
        }
    }

    const fn maximum_bytes(self) -> u64 {
        match self {
            Self::JournalSegment => MAX_SEALED_BYTES,
            Self::LogicalObject => FORMAT_MAX_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RetainedRawObject {
    kind: RawObjectKind,
    content_digest: EvidenceDigest,
    physical_receipt_digest: EvidenceDigest,
    retained_units: usize,
}

/// Provider-owned limits for one logical raw object, below immutable format ceilings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResearchObjectAdmission {
    maximum_bytes: u64,
    maximum_chunks: usize,
    integrity_chunk_bytes: u64,
}

impl ResearchObjectAdmission {
    /// Constructs nonzero provider limits strictly below the 64 GiB / 4,096-chunk format ceiling.
    pub fn try_new(
        maximum_bytes: u64,
        maximum_chunks: usize,
    ) -> Result<Self, SealedResearchJournalStoreError> {
        Self::try_new_with_chunk_bytes(maximum_bytes, maximum_chunks, FORMAT_INTEGRITY_CHUNK_BYTES)
    }

    fn try_new_with_chunk_bytes(
        maximum_bytes: u64,
        maximum_chunks: usize,
        integrity_chunk_bytes: u64,
    ) -> Result<Self, SealedResearchJournalStoreError> {
        let admitted_by_chunks = u64::try_from(maximum_chunks)
            .ok()
            .and_then(|chunks| chunks.checked_mul(integrity_chunk_bytes));
        if maximum_bytes == 0
            || maximum_bytes >= FORMAT_MAX_BYTES
            || maximum_chunks == 0
            || maximum_chunks >= FORMAT_MAX_CHUNKS
            || integrity_chunk_bytes == 0
            || integrity_chunk_bytes > FORMAT_INTEGRITY_CHUNK_BYTES
            || (!cfg!(test) && integrity_chunk_bytes != FORMAT_INTEGRITY_CHUNK_BYTES)
            || admitted_by_chunks.is_none_or(|bytes| maximum_bytes > bytes)
        {
            return Err(SealedResearchJournalStoreError::InvalidObjectAdmission);
        }
        Ok(Self {
            maximum_bytes,
            maximum_chunks,
            integrity_chunk_bytes,
        })
    }

    #[cfg(test)]
    fn try_new_for_test(
        maximum_bytes: u64,
        maximum_chunks: usize,
        integrity_chunk_bytes: u64,
    ) -> Result<Self, SealedResearchJournalStoreError> {
        Self::try_new_with_chunk_bytes(maximum_bytes, maximum_chunks, integrity_chunk_bytes)
    }

    /// Returns the exact provider-owned byte ceiling.
    pub const fn maximum_bytes(self) -> u64 {
        self.maximum_bytes
    }

    /// Returns the exact provider-owned integrity-chunk ceiling.
    pub const fn maximum_chunks(self) -> usize {
        self.maximum_chunks
    }

    /// Returns the fixed integrity-chunk size bound to this admission.
    pub const fn integrity_chunk_bytes(self) -> u64 {
        self.integrity_chunk_bytes
    }
}

/// Exact physical mapping of one integrity chunk in a logical raw object.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchObjectChunkReceipt {
    ordinal: u32,
    offset: u64,
    size_bytes: u64,
    content_digest: EvidenceDigest,
}

impl ResearchObjectChunkReceipt {
    /// Returns the zero-based integrity-chunk ordinal.
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns the exact object-relative chunk offset.
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the exact retained bytes in this chunk.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns the SHA-256 digest of this exact chunk.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }
}

/// Persistable, value-only claim for one synchronized staging prefix.
///
/// Deserialization is bounded and never grants write, resume, or publication authority. Resume
/// reopens the named stage without following links and re-hashes the complete prefix.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchObjectCheckpointClaim {
    staging_reference: Box<str>,
    stage_device: u64,
    stage_inode: u64,
    maximum_bytes: u64,
    maximum_chunks: u32,
    integrity_chunk_bytes: u64,
    size_bytes: u64,
    completed_chunks: Box<[ResearchObjectChunkReceipt]>,
    partial_chunk_bytes: u64,
    partial_chunk_digest: Option<EvidenceDigest>,
    prefix_digest: EvidenceDigest,
    checkpoint_digest: EvidenceDigest,
}

impl ResearchObjectCheckpointClaim {
    /// Returns the opaque capability-relative stage reference.
    pub fn staging_reference(&self) -> &str {
        &self.staging_reference
    }

    /// Returns exact synchronized prefix bytes.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns the provider-owned byte ceiling bound to this checkpoint.
    pub const fn maximum_bytes(&self) -> u64 {
        self.maximum_bytes
    }

    /// Returns the provider-owned chunk ceiling bound to this checkpoint.
    pub const fn maximum_chunks(&self) -> u32 {
        self.maximum_chunks
    }

    /// Returns the integrity-chunk size bound into this immutable claim.
    pub const fn integrity_chunk_bytes(&self) -> u64 {
        self.integrity_chunk_bytes
    }

    /// Returns complete fixed-size chunks preceding any partial tail.
    pub fn completed_chunks(&self) -> &[ResearchObjectChunkReceipt] {
        &self.completed_chunks
    }

    /// Returns the digest of every synchronized prefix byte.
    pub const fn prefix_digest(&self) -> EvidenceDigest {
        self.prefix_digest
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchObjectCheckpointClaimWire {
    #[serde(deserialize_with = "deserialize_bounded_stage_reference")]
    staging_reference: Box<str>,
    stage_device: u64,
    stage_inode: u64,
    maximum_bytes: u64,
    maximum_chunks: u32,
    integrity_chunk_bytes: u64,
    size_bytes: u64,
    #[serde(deserialize_with = "deserialize_bounded_object_chunks")]
    completed_chunks: Box<[ResearchObjectChunkReceipt]>,
    partial_chunk_bytes: u64,
    partial_chunk_digest: Option<EvidenceDigest>,
    prefix_digest: EvidenceDigest,
    checkpoint_digest: EvidenceDigest,
}

impl<'de> Deserialize<'de> for ResearchObjectCheckpointClaim {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ResearchObjectCheckpointClaimWire::deserialize(deserializer)?;
        let claim = Self {
            staging_reference: wire.staging_reference,
            stage_device: wire.stage_device,
            stage_inode: wire.stage_inode,
            maximum_bytes: wire.maximum_bytes,
            maximum_chunks: wire.maximum_chunks,
            integrity_chunk_bytes: wire.integrity_chunk_bytes,
            size_bytes: wire.size_bytes,
            completed_chunks: wire.completed_chunks,
            partial_chunk_bytes: wire.partial_chunk_bytes,
            partial_chunk_digest: wire.partial_chunk_digest,
            prefix_digest: wire.prefix_digest,
            checkpoint_digest: wire.checkpoint_digest,
        };
        validate_checkpoint_claim(&claim).map_err(serde::de::Error::custom)?;
        Ok(claim)
    }
}

/// Persistable, non-authoritative value claim for one immutable logical raw object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchObjectClaim {
    relative_reference: Box<str>,
    content_digest: EvidenceDigest,
    size_bytes: u64,
    integrity_chunk_bytes: u64,
    chunks: Box<[ResearchObjectChunkReceipt]>,
    physical_receipt_digest: EvidenceDigest,
}

impl ResearchObjectClaim {
    /// Returns the capability-relative content-addressed object reference.
    pub fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    /// Returns the SHA-256 digest of the complete exact object.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the exact object length.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns the integrity-chunk size bound into this immutable claim.
    pub const fn integrity_chunk_bytes(&self) -> u64 {
        self.integrity_chunk_bytes
    }

    /// Returns the ordered fixed-integrity chunk mapping.
    pub fn chunks(&self) -> &[ResearchObjectChunkReceipt] {
        &self.chunks
    }

    /// Returns the digest binding the reference, complete bytes, and every chunk coordinate.
    pub const fn physical_receipt_digest(&self) -> EvidenceDigest {
        self.physical_receipt_digest
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResearchObjectClaimWire {
    #[serde(deserialize_with = "deserialize_bounded_object_reference")]
    relative_reference: Box<str>,
    content_digest: EvidenceDigest,
    size_bytes: u64,
    integrity_chunk_bytes: u64,
    #[serde(deserialize_with = "deserialize_bounded_object_chunks")]
    chunks: Box<[ResearchObjectChunkReceipt]>,
    physical_receipt_digest: EvidenceDigest,
}

impl<'de> Deserialize<'de> for ResearchObjectClaim {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ResearchObjectClaimWire::deserialize(deserializer)?;
        let claim = Self {
            relative_reference: wire.relative_reference,
            content_digest: wire.content_digest,
            size_bytes: wire.size_bytes,
            integrity_chunk_bytes: wire.integrity_chunk_bytes,
            chunks: wire.chunks,
            physical_receipt_digest: wire.physical_receipt_digest,
        };
        validate_object_claim(&claim).map_err(serde::de::Error::custom)?;
        Ok(claim)
    }
}

/// Restart-persistable raw-object claim across the journal-segment and logical-object formats.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SealedResearchRawClaim {
    /// One bounded `MSJ1` raw-record journal segment.
    JournalSegment(SealedResearchJournalSegmentClaim),
    /// One streamed, chunk-verified logical raw body.
    LogicalObject(ResearchObjectClaim),
}

/// Non-forgeable receipt issued only by a successful finish or exact verified reopen.
///
/// The type intentionally has no public constructor and no `Deserialize` implementation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchObjectReceipt {
    claim: ResearchObjectClaim,
}

impl ResearchObjectReceipt {
    /// Returns the persistable non-authoritative restart claim.
    pub const fn claim(&self) -> &ResearchObjectClaim {
        &self.claim
    }

    /// Returns the SHA-256 digest of the complete exact object.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.claim.content_digest()
    }

    /// Returns the exact object length.
    pub const fn size_bytes(&self) -> u64 {
        self.claim.size_bytes()
    }

    /// Returns ordered physical chunk receipts.
    pub fn chunks(&self) -> &[ResearchObjectChunkReceipt] {
        self.claim.chunks()
    }
}

/// Cooperative control points for bounded logical research-object operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResearchObjectControlPoint {
    /// Before complete stage verification begins.
    BeforeVerification,
    /// Before one bounded verification read.
    BeforeVerificationChunk {
        /// Exact verified offset before the read.
        offset_bytes: u64,
    },
    /// Immediately before immutable publication or verified authority release.
    BeforeCommit,
    /// Before one catalog claim is verified by a streaming recovery session.
    BeforeRecoveryClaim {
        /// Claims already verified and retained by this session.
        observed_claims: usize,
    },
    /// Before one bounded filesystem entry is inspected during recovery finish.
    BeforeRecoveryEntry {
        /// Filesystem entries already charged by this session.
        inspected_entries: usize,
    },
    /// Immediately before recovery performs the first mutation for one verified entry.
    BeforeRecoveryMutation {
        /// Filesystem entries already charged by this session.
        inspected_entries: usize,
    },
    /// After the caller ends catalog observation and before orphan reconciliation begins.
    BeforeRecoveryFinish {
        /// Complete claim count observed by this session.
        observed_claims: usize,
    },
}

/// Caller-owned cancellation, deadline, or trusted-control failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ResearchObjectControlError {
    /// The caller cancelled the controlled operation.
    #[error("logical research-object operation was cancelled")]
    Cancelled,
    /// The caller's monotonic deadline elapsed during the controlled operation.
    #[error("logical research-object operation deadline was exceeded")]
    DeadlineExceeded,
    /// Trusted control state could not be established for the operation.
    #[error("logical research-object operation control is unavailable")]
    Unavailable,
}

/// Caller-owned cooperative control for verification, publication, and recovery.
pub trait ResearchObjectControl {
    /// Checks whether the operation may continue at this control point.
    fn checkpoint(
        &self,
        point: ResearchObjectControlPoint,
    ) -> Result<(), ResearchObjectControlError>;
}

/// Caller-owned hard bounds for one streaming raw-catalog recovery session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedResearchRecoveryAdmission {
    maximum_claims: usize,
    maximum_entries: usize,
}

impl SealedResearchRecoveryAdmission {
    /// Constructs nonzero claim and filesystem budgets within the fixed recovery ceiling.
    pub fn try_new(
        maximum_claims: usize,
        maximum_entries: usize,
    ) -> Result<Self, SealedResearchJournalStoreError> {
        if maximum_claims == 0
            || maximum_entries == 0
            || maximum_claims > MAX_RECOVERY_ENTRIES
            || maximum_entries > MAX_RECOVERY_ENTRIES
            || maximum_claims
                .checked_add(maximum_entries)
                .is_none_or(|total| total > MAX_RECOVERY_ENTRIES)
        {
            return Err(SealedResearchJournalStoreError::InvalidRecoveryAdmission);
        }
        Ok(Self {
            maximum_claims,
            maximum_entries,
        })
    }

    /// Returns the complete catalog-claim budget.
    pub const fn maximum_claims(self) -> usize {
        self.maximum_claims
    }

    /// Returns the complete filesystem-entry budget.
    pub const fn maximum_entries(self) -> usize {
        self.maximum_entries
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecoverySessionState {
    Observing,
    Aborted,
}

/// Noncloneable single-owner recovery authority for one complete catalog scan.
pub struct SealedResearchRecoverySession<'store, 'control> {
    store: &'store SealedResearchJournalStore,
    _operation: MutexGuard<'store, ()>,
    control: &'control dyn ResearchObjectControl,
    admission: SealedResearchRecoveryAdmission,
    retained: Vec<RetainedRawObject>,
    observed_claims: usize,
    state: RecoverySessionState,
}

impl fmt::Debug for SealedResearchRecoverySession<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SealedResearchRecoverySession")
            .field("maximum_claims", &self.admission.maximum_claims)
            .field("maximum_entries", &self.admission.maximum_entries)
            .field("observed_claims", &self.observed_claims)
            .field("state", &self.state)
            .finish()
    }
}

/// Noncloneable writable owner of one capability-confined logical-object stage.
pub struct PendingResearchObject {
    admission: ResearchObjectAdmission,
    staging: Dir,
    stage_name: Box<str>,
    file: File,
    identity: FileIdentity,
    store_owner_identity: FileIdentity,
    size_bytes: u64,
    completed_chunks: Vec<ResearchObjectChunkReceipt>,
    whole_hasher: Sha256,
    partial_hasher: Sha256,
    partial_chunk_bytes: u64,
}

impl fmt::Debug for PendingResearchObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingResearchObject")
            .field("stage", &"[CAPABILITY-RELATIVE STAGE]")
            .field("size_bytes", &self.size_bytes)
            .field("completed_chunks", &self.completed_chunks.len())
            .field("partial_chunk_bytes", &self.partial_chunk_bytes)
            .finish()
    }
}

impl PendingResearchObject {
    /// Writes admitted bytes after charging the complete call against byte/chunk limits.
    ///
    /// Like [`Write::write`], a positive partial result means the caller must retry only the
    /// remaining suffix. Admission and allocation failures occur before any byte is written.
    pub fn write_admitted(
        &mut self,
        bytes: &[u8],
    ) -> Result<usize, SealedResearchJournalStoreError> {
        self.validate_identity()?;
        let requested = u64::try_from(bytes.len()).map_err(|_| {
            SealedResearchJournalStoreError::ObjectByteLimitExceeded {
                max: self.admission.maximum_bytes,
            }
        })?;
        let next_size = self.size_bytes.checked_add(requested).ok_or(
            SealedResearchJournalStoreError::ObjectByteLimitExceeded {
                max: self.admission.maximum_bytes,
            },
        )?;
        if next_size > self.admission.maximum_bytes {
            return Err(SealedResearchJournalStoreError::ObjectByteLimitExceeded {
                max: self.admission.maximum_bytes,
            });
        }
        let required_chunks = chunk_count(next_size, self.admission.integrity_chunk_bytes)?;
        if required_chunks > self.admission.maximum_chunks {
            return Err(SealedResearchJournalStoreError::ObjectChunkLimitExceeded {
                max: self.admission.maximum_chunks,
            });
        }
        let completed_after = usize::try_from(next_size / self.admission.integrity_chunk_bytes)
            .map_err(
                |_| SealedResearchJournalStoreError::ObjectChunkLimitExceeded {
                    max: self.admission.maximum_chunks,
                },
            )?;
        if completed_after > self.completed_chunks.len() {
            self.completed_chunks
                .try_reserve(completed_after - self.completed_chunks.len())
                .map_err(|_| SealedResearchJournalStoreError::ObjectAllocationFailed)?;
        }

        let mut remaining = bytes;
        let mut accepted = 0_usize;
        while !remaining.is_empty() {
            let chunk_remaining = self
                .admission
                .integrity_chunk_bytes
                .checked_sub(self.partial_chunk_bytes)
                .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
            let attempt = remaining.len().min(
                usize::try_from(chunk_remaining)
                    .map_err(|_| SealedResearchJournalStoreError::ObjectReceiptMismatch)?,
            );
            let written = match self.file.write(&remaining[..attempt]) {
                Ok(0) if accepted > 0 => return Ok(accepted),
                Ok(written) => written,
                Err(_source) if accepted > 0 => return Ok(accepted),
                Err(source) => {
                    return Err(SealedResearchJournalStoreError::io(
                        "failed to write logical research-object stage",
                        source,
                    ));
                }
            };
            if written == 0 {
                return Err(SealedResearchJournalStoreError::io(
                    "failed to write logical research-object stage",
                    std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "logical research-object stage accepted zero bytes",
                    ),
                ));
            }
            let written_bytes = u64::try_from(written)
                .map_err(|_| SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
            self.whole_hasher.update(&remaining[..written]);
            self.partial_hasher.update(&remaining[..written]);
            self.size_bytes = self.size_bytes.checked_add(written_bytes).ok_or(
                SealedResearchJournalStoreError::ObjectByteLimitExceeded {
                    max: self.admission.maximum_bytes,
                },
            )?;
            self.partial_chunk_bytes = self
                .partial_chunk_bytes
                .checked_add(written_bytes)
                .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
            accepted = accepted
                .checked_add(written)
                .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
            remaining = &remaining[written..];
            if self.partial_chunk_bytes == self.admission.integrity_chunk_bytes {
                let ordinal = self.completed_chunks.len();
                let offset = u64::try_from(ordinal)
                    .ok()
                    .and_then(|value| value.checked_mul(self.admission.integrity_chunk_bytes))
                    .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
                let digest = EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    std::mem::take(&mut self.partial_hasher).finalize().into(),
                );
                self.completed_chunks.push(ResearchObjectChunkReceipt {
                    ordinal: u32::try_from(ordinal).map_err(|_| {
                        SealedResearchJournalStoreError::ObjectChunkLimitExceeded {
                            max: self.admission.maximum_chunks,
                        }
                    })?,
                    offset,
                    size_bytes: self.admission.integrity_chunk_bytes,
                    content_digest: digest,
                });
                self.partial_chunk_bytes = 0;
            }
        }
        Ok(accepted)
    }

    fn validate_identity(&self) -> Result<(), SealedResearchJournalStoreError> {
        let named = self
            .staging
            .symlink_metadata(&*self.stage_name)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to inspect logical research-object stage",
                    source,
                )
            })?;
        let opened = opened_file_metadata(&self.file)?;
        validate_private_regular_file_with_links(&named, Some(self.size_bytes), Some(1))?;
        validate_private_regular_file_with_links(&opened, Some(self.size_bytes), Some(1))?;
        if FileIdentity::from_metadata(&named) != self.identity
            || FileIdentity::from_metadata(&opened) != self.identity
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(())
    }
}

impl Write for PendingResearchObject {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.write_admitted(buffer).map_err(std::io::Error::other)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

/// Opaque, fully verified descriptor for one immutable logical raw object.
///
/// It is intentionally noncloneable and non-Serde. The descriptor exposes only bounded
/// `Read + Seek`; no path, mutable handle, or arbitrary-reader constructor is available.
pub struct VerifiedResearchObject {
    receipt: ResearchObjectReceipt,
    object_directory: Dir,
    filename: Box<str>,
    file: File,
    identity: FileIdentity,
    position: u64,
}

impl fmt::Debug for VerifiedResearchObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedResearchObject")
            .field("content_digest", &self.receipt.content_digest())
            .field("size_bytes", &self.receipt.size_bytes())
            .field("descriptor", &"[VERIFIED READ-ONLY DESCRIPTOR]")
            .field("position", &self.position)
            .finish()
    }
}

impl VerifiedResearchObject {
    /// Returns the SHA-256 digest bound to this exact verified descriptor.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.receipt.content_digest()
    }

    /// Returns the exact byte length bound to this verified descriptor.
    pub const fn size_bytes(&self) -> u64 {
        self.receipt.size_bytes()
    }

    /// Consumes the view, re-hashes the same descriptor, and returns commit-ready authority.
    ///
    /// Downstream code can read a role-specific view, then call this immediately before its own
    /// sink commit. The descriptor, final name, size, chunks, and whole digest are all checked
    /// again without exposing the file or path.
    pub fn reverify_for_commit(
        mut self,
        control: &dyn ResearchObjectControl,
    ) -> Result<ResearchObjectReceipt, SealedResearchJournalStoreError> {
        control.checkpoint(ResearchObjectControlPoint::BeforeVerification)?;
        verify_opened_object_with_control(
            &self.object_directory,
            &self.filename,
            &mut self.file,
            self.identity,
            &self.receipt.claim,
            Some(control),
        )?;
        control.checkpoint(ResearchObjectControlPoint::BeforeCommit)?;
        Ok(self.receipt)
    }
}

impl Read for VerifiedResearchObject {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.receipt.size_bytes().saturating_sub(self.position);
        let admitted = buffer
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        if admitted == 0 {
            return Ok(0);
        }
        let read = self.file.read(&mut buffer[..admitted])?;
        self.position = self
            .position
            .checked_add(u64::try_from(read).map_err(std::io::Error::other)?)
            .ok_or_else(|| std::io::Error::other("verified object position overflowed"))?;
        Ok(read)
    }
}

impl Seek for VerifiedResearchObject {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        let length = i128::from(self.receipt.size_bytes());
        let current = i128::from(self.position);
        let requested = match position {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::Current(offset) => current
                .checked_add(i128::from(offset))
                .ok_or_else(|| std::io::Error::other("verified object seek overflowed"))?,
            SeekFrom::End(offset) => length
                .checked_add(i128::from(offset))
                .ok_or_else(|| std::io::Error::other("verified object seek overflowed"))?,
        };
        if !(0..=length).contains(&requested) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "verified object seek exceeds object bounds",
            ));
        }
        let requested = u64::try_from(requested).map_err(std::io::Error::other)?;
        let actual = self.file.seek(SeekFrom::Start(requested))?;
        if actual != requested {
            return Err(std::io::Error::other(
                "verified object descriptor returned a different seek position",
            ));
        }
        self.position = actual;
        Ok(actual)
    }
}

impl SealedResearchJournalStore {
    /// Starts one new capability-confined logical raw object under explicit provider limits.
    pub fn begin_logical_object(
        &self,
        admission: ResearchObjectAdmission,
    ) -> Result<PendingResearchObject, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        let stage_name = format!("{}{LOGICAL_STAGE_SUFFIX}", Uuid::new_v4());
        let file = open_new_stage(&self.staging, &stage_name)?;
        lock_pending_stage(&file)?;
        let metadata = opened_file_metadata(&file)?;
        validate_private_regular_file_with_links(&metadata, Some(0), Some(1))?;
        let identity = FileIdentity::from_metadata(&metadata);
        let named = self
            .staging
            .symlink_metadata(&stage_name)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to inspect new logical research-object stage",
                    source,
                )
            })?;
        validate_private_regular_file_with_links(&named, Some(0), Some(1))?;
        if FileIdentity::from_metadata(&named) != identity {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        sync_directory(&self.staging)?;
        Ok(PendingResearchObject {
            admission,
            staging: self.staging.try_clone().map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to retain logical research-object staging capability",
                    source,
                )
            })?,
            stage_name: stage_name.into_boxed_str(),
            file,
            identity,
            store_owner_identity: self.owner_identity,
            size_bytes: 0,
            completed_chunks: Vec::new(),
            whole_hasher: Sha256::new(),
            partial_hasher: Sha256::new(),
            partial_chunk_bytes: 0,
        })
    }

    /// Flushes and synchronizes a stage, then returns a value-only restart claim.
    pub fn checkpoint_logical_object(
        &self,
        pending: &mut PendingResearchObject,
    ) -> Result<ResearchObjectCheckpointClaim, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        self.validate_pending_owner(pending)?;
        pending.file.flush().map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to flush logical research-object checkpoint",
                source,
            )
        })?;
        pending.file.sync_all().map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to synchronize logical research-object checkpoint",
                source,
            )
        })?;
        pending.validate_identity()?;
        sync_directory(&pending.staging)?;

        let completed_chunks = clone_chunks(&pending.completed_chunks)?.into_boxed_slice();
        let partial_chunk_digest = (pending.partial_chunk_bytes > 0).then(|| {
            EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                pending.partial_hasher.clone().finalize().into(),
            )
        });
        let prefix_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            pending.whole_hasher.clone().finalize().into(),
        );
        let maximum_chunks = u32::try_from(pending.admission.maximum_chunks)
            .map_err(|_| SealedResearchJournalStoreError::InvalidObjectAdmission)?;
        let mut claim = ResearchObjectCheckpointClaim {
            staging_reference: pending.stage_name.clone(),
            stage_device: pending.identity.device,
            stage_inode: pending.identity.inode,
            maximum_bytes: pending.admission.maximum_bytes,
            maximum_chunks,
            integrity_chunk_bytes: pending.admission.integrity_chunk_bytes,
            size_bytes: pending.size_bytes,
            completed_chunks,
            partial_chunk_bytes: pending.partial_chunk_bytes,
            partial_chunk_digest,
            prefix_digest,
            checkpoint_digest: empty_sha256(),
        };
        claim.checkpoint_digest = checkpoint_digest(&claim);
        validate_checkpoint_claim(&claim)?;
        Ok(claim)
    }

    /// Resumes only after re-hashing the complete synchronized prefix named by a value claim.
    pub fn resume_logical_object(
        &self,
        admission: ResearchObjectAdmission,
        claim: &ResearchObjectCheckpointClaim,
    ) -> Result<PendingResearchObject, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        validate_checkpoint_claim(claim)?;
        if admission.maximum_bytes != claim.maximum_bytes
            || u32::try_from(admission.maximum_chunks).ok() != Some(claim.maximum_chunks)
            || admission.integrity_chunk_bytes != claim.integrity_chunk_bytes
        {
            return Err(SealedResearchJournalStoreError::ObjectCheckpointMismatch);
        }
        match self.resume_logical_object_inner(admission, claim) {
            Ok(pending) => Ok(pending),
            Err(error @ SealedResearchJournalStoreError::ObjectStageActive) => Err(error),
            Err(error) => {
                match self.staging.symlink_metadata(&*claim.staging_reference) {
                    Ok(metadata) if metadata.file_type().is_file() => {
                        let quarantine_name =
                            try_string_from_parts(&["staging-", &claim.staging_reference])?;
                        quarantine_stage_no_replace(
                            &self.staging,
                            &claim.staging_reference,
                            &self.quarantine,
                            &quarantine_name,
                            FORMAT_MAX_BYTES,
                            1,
                            None,
                        )?;
                    }
                    Ok(_metadata) => {
                        return Err(SealedResearchJournalStoreError::StateConflict);
                    }
                    Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                    Err(source) => {
                        return Err(SealedResearchJournalStoreError::io(
                            "failed to inspect rejected logical research-object stage",
                            source,
                        ));
                    }
                }
                Err(error)
            }
        }
    }

    /// Consumes and removes one unpublished logical-object stage after exact owner validation.
    ///
    /// A linked or identity-changed stage is rejected. Published immutable objects are never
    /// removed by this cancellation path.
    pub fn abort_logical_object(
        &self,
        pending: PendingResearchObject,
    ) -> Result<(), SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        self.validate_pending_owner(&pending)?;
        pending.validate_identity()?;
        let stage_name = pending.stage_name.clone();
        let identity = pending.identity;
        let size_bytes = pending.size_bytes;
        drop(pending);
        let named = self
            .staging
            .symlink_metadata(&*stage_name)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to inspect aborted logical research-object stage",
                    source,
                )
            })?;
        validate_private_regular_file_with_links(&named, Some(size_bytes), Some(1))?;
        if FileIdentity::from_metadata(&named) != identity {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        self.staging.remove_file(&*stage_name).map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to remove aborted logical research-object stage",
                source,
            )
        })?;
        sync_directory(&self.staging)
    }

    /// Finishes, fully re-verifies, and publishes a content-addressed `.mro` without replacement.
    ///
    /// Control is checked throughout verification and immediately before the final-link attempt.
    /// Once that link exists, synchronization, stage retirement, and exact reopen proceed without
    /// another cancellation check so a committed object is never reported as pre-commit state.
    pub fn finish_logical_object(
        &self,
        mut pending: PendingResearchObject,
        control: &dyn ResearchObjectControl,
    ) -> Result<VerifiedResearchObject, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        self.validate_pending_owner(&pending)?;
        pending.file.flush().map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to flush logical research-object stage",
                source,
            )
        })?;
        pending.file.sync_all().map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to synchronize logical research-object stage",
                source,
            )
        })?;
        pending.validate_identity()?;
        sync_directory(&pending.staging)?;

        control.checkpoint(ResearchObjectControlPoint::BeforeVerification)?;
        let rehashed = rehash_prefix(
            &mut pending.file,
            pending.size_bytes,
            pending.admission.integrity_chunk_bytes,
            pending.admission.maximum_chunks,
            Some(control),
        )?;
        let incremental_digest = EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            pending.whole_hasher.clone().finalize().into(),
        );
        if rehashed.prefix_digest != incremental_digest
            || rehashed.completed_chunks != pending.completed_chunks
            || rehashed.partial_chunk_bytes != pending.partial_chunk_bytes
            || rehashed.partial_chunk_digest
                != (pending.partial_chunk_bytes > 0).then(|| {
                    EvidenceDigest::new(
                        DigestAlgorithm::Sha256,
                        pending.partial_hasher.clone().finalize().into(),
                    )
                })
        {
            return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
        }
        let chunks = rehashed.into_all_chunks()?.into_boxed_slice();
        let content_digest = incremental_digest;
        let hex = digest_hex(content_digest);
        let shard_name = &hex[..2];
        let filename = format!("{hex}{LOGICAL_OBJECT_SUFFIX}");
        let relative_reference = format!("objects/sha256/{shard_name}/{filename}");
        let physical_receipt_digest = object_receipt_digest(
            &relative_reference,
            content_digest,
            pending.size_bytes,
            pending.admission.integrity_chunk_bytes,
            &chunks,
        );
        let claim = ResearchObjectClaim {
            relative_reference: relative_reference.into_boxed_str(),
            content_digest,
            size_bytes: pending.size_bytes,
            integrity_chunk_bytes: pending.admission.integrity_chunk_bytes,
            chunks,
            physical_receipt_digest,
        };
        validate_object_claim(&claim)?;
        let receipt = ResearchObjectReceipt {
            claim: clone_object_claim(&claim)?,
        };

        pending.validate_identity()?;
        let shard = ensure_directory(&self.objects, shard_name)?;
        let stage_name = pending.stage_name.clone();
        let published_identity = pending.identity;
        pending.validate_identity()?;
        control.checkpoint(ResearchObjectControlPoint::BeforeCommit)?;
        let published_new = match self.staging.hard_link(&*stage_name, &shard, &filename) {
            Ok(()) => true,
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = open_verified_object_from_shard(&shard, &filename, &claim)?;
                if existing.receipt != receipt {
                    return Err(SealedResearchJournalStoreError::StateConflict);
                }
                false
            }
            Err(source) => {
                return Err(SealedResearchJournalStoreError::io(
                    "failed to publish logical research object without replacement",
                    source,
                ));
            }
        };

        if published_new {
            let completed = (|| {
                let published = shard.symlink_metadata(&filename).map_err(|source| {
                    SealedResearchJournalStoreError::io(
                        "failed to inspect newly linked logical research object",
                        source,
                    )
                })?;
                validate_private_regular_file_with_links(
                    &published,
                    Some(claim.size_bytes),
                    Some(2),
                )?;
                if FileIdentity::from_metadata(&published) != published_identity {
                    return Err(SealedResearchJournalStoreError::StateConflict);
                }
                sync_directory(&shard)?;
                prepare_read_only_link(&shard, &filename, claim.size_bytes, published_identity, 2)?
                    .complete(&shard, &filename)?;
                sync_directory(&shard)?;
                drop(pending);
                self.staging.remove_file(&*stage_name).map_err(|source| {
                    SealedResearchJournalStoreError::io(
                        "failed to retire linked logical research-object stage",
                        source,
                    )
                })?;
                sync_directory(&self.staging)?;
                let verified = open_verified_object_from_shard(&shard, &filename, &claim)?;
                if verified.receipt != receipt {
                    return Err(SealedResearchJournalStoreError::StateConflict);
                }
                Ok(verified)
            })();
            return completed
                .map_err(|_error| SealedResearchJournalStoreError::RawPublicationIndeterminate);
        }

        drop(pending);
        self.staging.remove_file(&*stage_name).map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to retire published logical research-object stage",
                source,
            )
        })?;
        sync_directory(&self.staging)?;
        let verified = open_verified_object_from_shard(&shard, &filename, &claim)?;
        if verified.receipt != receipt {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(verified)
    }

    /// Opens a store-issued receipt through a new independently verified read-only descriptor.
    pub fn open_verified_logical_object(
        &self,
        receipt: &ResearchObjectReceipt,
        control: &dyn ResearchObjectControl,
    ) -> Result<VerifiedResearchObject, SealedResearchJournalStoreError> {
        self.open_verified_logical_object_claim(receipt.claim(), control)
    }

    /// Upgrades a bounded value claim only after exact whole-object and chunk verification.
    pub fn open_verified_logical_object_claim(
        &self,
        claim: &ResearchObjectClaim,
        control: &dyn ResearchObjectControl,
    ) -> Result<VerifiedResearchObject, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        control.checkpoint(ResearchObjectControlPoint::BeforeVerification)?;
        let verified = self.open_verified_logical_claim_inner_with_control(claim, Some(control))?;
        control.checkpoint(ResearchObjectControlPoint::BeforeCommit)?;
        Ok(verified)
    }

    /// Begins one bounded, streaming recovery scan while retaining exclusive store authority.
    ///
    /// The caller must observe every authoritative catalog claim and consume [`finish`](
    /// SealedResearchRecoverySession::finish) to attest that the catalog scan reached its end.
    /// Dropping the session never starts orphan reconciliation.
    pub fn begin_recovery<'store, 'control>(
        &'store self,
        admission: SealedResearchRecoveryAdmission,
        control: &'control dyn ResearchObjectControl,
    ) -> Result<SealedResearchRecoverySession<'store, 'control>, SealedResearchJournalStoreError>
    {
        let operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(admission.maximum_claims)
            .map_err(|_| SealedResearchJournalStoreError::ObjectAllocationFailed)?;
        Ok(SealedResearchRecoverySession {
            store: self,
            _operation: operation,
            control,
            admission,
            retained,
            observed_claims: 0,
            state: RecoverySessionState::Observing,
        })
    }

    fn finish_recovery(
        &self,
        retained: &[RetainedRawObject],
        admission: SealedResearchRecoveryAdmission,
        control: &dyn ResearchObjectControl,
    ) -> Result<SealedResearchJournalRecoveryReport, SealedResearchJournalStoreError> {
        self.validate_owner()?;
        let mut inspected_entries = 0_usize;
        let staging_entries = bounded_entries(
            &self.staging,
            &mut inspected_entries,
            admission.maximum_entries,
            control,
        )?;
        let mut quarantined_staging = Vec::new();
        quarantined_staging
            .try_reserve_exact(staging_entries.len())
            .map_err(|_| SealedResearchJournalStoreError::ObjectAllocationFailed)?;
        for entry in staging_entries {
            control.checkpoint(ResearchObjectControlPoint::BeforeRecoveryEntry {
                inspected_entries,
            })?;
            let name = entry.name;
            let entry = entry.entry;
            let kind = raw_stage_kind(&name)?;
            if !entry
                .file_type()
                .map_err(|source| {
                    SealedResearchJournalStoreError::io(
                        "failed to inspect raw-object staging entry type",
                        source,
                    )
                })?
                .is_file()
            {
                return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
            }
            let metadata = self.staging.symlink_metadata(&name).map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to inspect raw-object staging link state",
                    source,
                )
            })?;
            match cap_fs_ext::MetadataExt::nlink(&metadata) {
                1 => {
                    let quarantine_name = try_string_from_parts(&["staging-", &name])?;
                    quarantine_stage_no_replace(
                        &self.staging,
                        &name,
                        &self.quarantine,
                        &quarantine_name,
                        kind.maximum_bytes(),
                        1,
                        Some(RecoveryControl {
                            control,
                            inspected_entries,
                        }),
                    )?;
                    quarantined_staging.push(name);
                }
                2 => {
                    if self.reconcile_linked_stage(
                        kind,
                        &name,
                        RecoveryControl {
                            control,
                            inspected_entries,
                        },
                    )? == LinkedStageDisposition::Quarantined
                    {
                        quarantined_staging.push(name);
                    }
                }
                _ => {
                    return Err(SealedResearchJournalStoreError::RawPublicationIndeterminate);
                }
            }
        }

        let mut quarantined_objects = Vec::new();
        let shard_entries = bounded_entries(
            &self.objects,
            &mut inspected_entries,
            admission.maximum_entries,
            control,
        )?;
        for shard_entry in shard_entries {
            control.checkpoint(ResearchObjectControlPoint::BeforeRecoveryEntry {
                inspected_entries,
            })?;
            let shard = shard_entry.name;
            let shard_entry = shard_entry.entry;
            if !is_lower_hex(&shard, 2)
                || !shard_entry
                    .file_type()
                    .map_err(|source| {
                        SealedResearchJournalStoreError::io(
                            "failed to inspect raw-object shard type",
                            source,
                        )
                    })?
                    .is_dir()
            {
                return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
            }
            let shard_directory = self.objects.open_dir_nofollow(&shard).map_err(|source| {
                SealedResearchJournalStoreError::io("failed to open raw-object shard", source)
            })?;
            let file_entries = bounded_entries(
                &shard_directory,
                &mut inspected_entries,
                admission.maximum_entries,
                control,
            )?;
            quarantined_objects
                .try_reserve(file_entries.len())
                .map_err(|_| SealedResearchJournalStoreError::ObjectAllocationFailed)?;
            for file_entry in file_entries {
                control.checkpoint(ResearchObjectControlPoint::BeforeRecoveryEntry {
                    inspected_entries,
                })?;
                let filename = file_entry.name;
                let file_entry = file_entry.entry;
                let (kind, hex) = raw_object_kind_and_hex(&filename)?;
                if !is_lower_hex(hex, 64)
                    || !hex.starts_with(&shard)
                    || !file_entry
                        .file_type()
                        .map_err(|source| {
                            SealedResearchJournalStoreError::io(
                                "failed to inspect sealed raw-object type",
                                source,
                            )
                        })?
                        .is_file()
                {
                    return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
                }
                let digest_bytes = decode_sha256_hex(hex)?;
                if retained_raw_object_exists(retained, kind, &digest_bytes) {
                    continue;
                }
                let reference =
                    try_string_from_parts(&["objects/sha256/", &shard, "/", &filename])?;
                let quarantine_name = try_string_from_parts(&["object-", &filename])?;
                quarantine_no_replace(
                    &shard_directory,
                    &filename,
                    &self.quarantine,
                    &quarantine_name,
                    kind.maximum_bytes(),
                    Some(RecoveryControl {
                        control,
                        inspected_entries,
                    }),
                )?;
                quarantined_objects.push(reference);
            }
        }
        let quarantine_entries = bounded_entries(
            &self.quarantine,
            &mut inspected_entries,
            admission.maximum_entries,
            control,
        )?;
        let mut retained_journal_segments = 0_usize;
        let mut retained_raw_records = 0_usize;
        let mut retained_logical_objects = 0_usize;
        let mut retained_logical_object_chunks = 0_usize;
        for retained_object in retained {
            match retained_object.kind {
                RawObjectKind::JournalSegment => {
                    retained_journal_segments = retained_journal_segments
                        .checked_add(1)
                        .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)?;
                    retained_raw_records = retained_raw_records
                        .checked_add(retained_object.retained_units)
                        .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)?;
                }
                RawObjectKind::LogicalObject => {
                    retained_logical_objects = retained_logical_objects
                        .checked_add(1)
                        .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)?;
                    retained_logical_object_chunks = retained_logical_object_chunks
                        .checked_add(retained_object.retained_units)
                        .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)?;
                }
            }
        }
        Ok(SealedResearchJournalRecoveryReport {
            quarantined_staging,
            quarantined_objects,
            retained_quarantine_entries: quarantine_entries.len(),
            retained_journal_segments,
            retained_raw_records,
            retained_logical_objects,
            retained_logical_object_chunks,
        })
    }

    fn reconcile_linked_stage(
        &self,
        kind: RawObjectKind,
        stage_name: &str,
        recovery: RecoveryControl<'_>,
    ) -> Result<LinkedStageDisposition, SealedResearchJournalStoreError> {
        let mut stage = open_locked_linked_stage(&self.staging, stage_name, kind)
            .map_err(|_error| SealedResearchJournalStoreError::RawPublicationIndeterminate)?;
        let prepared = (|| {
            let stage_named = self
                .staging
                .symlink_metadata(stage_name)
                .map_err(|source| {
                    SealedResearchJournalStoreError::io(
                        "failed to inspect linked raw-object stage",
                        source,
                    )
                })?;
            let stage_opened = opened_file_metadata(&stage)?;
            let size_bytes = stage_opened.len();
            if size_bytes > kind.maximum_bytes() {
                return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
            }
            validate_linked_file_state(kind, &stage_named, Some(size_bytes))?;
            validate_linked_file_state(kind, &stage_opened, Some(size_bytes))?;
            let identity = FileIdentity::from_metadata(&stage_opened);
            if FileIdentity::from_metadata(&stage_named) != identity {
                return Err(SealedResearchJournalStoreError::StateConflict);
            }
            let content_digest = hash_file_bounded_with_control(
                &mut stage,
                size_bytes,
                kind.maximum_bytes(),
                Some(recovery.control),
            )?;
            if kind == RawObjectKind::JournalSegment {
                validate_unclaimed_msj_with_control(&stage, size_bytes, Some(recovery.control))?;
            }
            let evidence = LinkedStageEvidence {
                kind,
                size_bytes,
                identity,
                content_digest,
                control: recovery.control,
            };
            let quarantine_name = try_string_from_parts(&["staging-", stage_name])?;
            let quarantine_matches =
                self.linked_candidate_matches(&self.quarantine, &quarantine_name, evidence)?;

            let hex = try_digest_hex(content_digest)?;
            let shard_name = &hex[..2];
            let filename = try_string_from_parts(&[&hex, kind.object_suffix()])?;
            let shard = match self.objects.open_dir_nofollow(shard_name) {
                Ok(shard) => Some(shard),
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => None,
                Err(source) => {
                    return Err(SealedResearchJournalStoreError::io(
                        "failed to open linked raw-object shard",
                        source,
                    ));
                }
            };
            let final_matches = match shard.as_ref() {
                Some(shard) => self.linked_candidate_matches(shard, &filename, evidence)?,
                None => false,
            };
            let target = match (quarantine_matches, final_matches, shard) {
                (true, false, _) => LinkedStageTarget::Quarantined { quarantine_name },
                (false, true, Some(shard)) => {
                    let read_only_transition = if kind == RawObjectKind::LogicalObject {
                        Some(prepare_read_only_link(
                            &shard, &filename, size_bytes, identity, 2,
                        )?)
                    } else {
                        None
                    };
                    LinkedStageTarget::PublishedFinal {
                        shard,
                        filename,
                        read_only_transition,
                    }
                }
                _ => return Err(SealedResearchJournalStoreError::StateConflict),
            };
            let stage_named_after =
                self.staging
                    .symlink_metadata(stage_name)
                    .map_err(|source| {
                        SealedResearchJournalStoreError::io(
                            "failed to re-inspect linked raw-object stage",
                            source,
                        )
                    })?;
            let stage_opened_after = opened_file_metadata(&stage)?;
            if FileIdentity::from_metadata(&stage_named_after) != identity
                || FileIdentity::from_metadata(&stage_opened_after) != identity
                || stage_opened_after.len() != size_bytes
                || cap_fs_ext::MetadataExt::nlink(&stage_named_after) != 2
                || cap_fs_ext::MetadataExt::nlink(&stage_opened_after) != 2
            {
                return Err(SealedResearchJournalStoreError::StateConflict);
            }
            match &target {
                LinkedStageTarget::PublishedFinal { shard, .. } => sync_directory(shard)?,
                LinkedStageTarget::Quarantined { .. } => sync_directory(&self.quarantine)?,
            }
            Ok((target, size_bytes, identity))
        })();
        let (mut target, size_bytes, identity) = prepared.map_err(linked_preparation_error)?;
        recovery.before_mutation()?;
        let reconciled = (|| -> Result<LinkedStageDisposition, SealedResearchJournalStoreError> {
            if let LinkedStageTarget::PublishedFinal {
                shard,
                filename,
                read_only_transition,
            } = &mut target
                && let Some(transition) = read_only_transition.take()
            {
                transition.complete(shard, filename)?;
                sync_directory(shard)?;
            }
            drop(stage);
            self.staging.remove_file(stage_name).map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to retire reconciled raw-object stage",
                    source,
                )
            })?;
            sync_directory(&self.staging)?;
            match target {
                LinkedStageTarget::PublishedFinal {
                    shard, filename, ..
                } => {
                    sync_directory(&shard)?;
                    verify_reconciled_link_metadata(
                        kind, &shard, &filename, size_bytes, identity, true,
                    )?;
                    Ok(LinkedStageDisposition::PublishedFinal)
                }
                LinkedStageTarget::Quarantined { quarantine_name } => {
                    sync_directory(&self.quarantine)?;
                    verify_reconciled_link_metadata(
                        kind,
                        &self.quarantine,
                        &quarantine_name,
                        size_bytes,
                        identity,
                        false,
                    )?;
                    Ok(LinkedStageDisposition::Quarantined)
                }
            }
        })();
        reconciled.map_err(|_error| SealedResearchJournalStoreError::RawPublicationIndeterminate)
    }

    fn linked_candidate_matches(
        &self,
        directory: &Dir,
        name: &str,
        evidence: LinkedStageEvidence<'_>,
    ) -> Result<bool, SealedResearchJournalStoreError> {
        let LinkedStageEvidence {
            kind,
            size_bytes,
            identity,
            content_digest,
            control,
        } = evidence;
        let named = match directory.symlink_metadata(name) {
            Ok(named) => named,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(source) => {
                return Err(SealedResearchJournalStoreError::io(
                    "failed to inspect linked raw-object target",
                    source,
                ));
            }
        };
        if FileIdentity::from_metadata(&named) != identity {
            return Ok(false);
        }
        validate_linked_file_state(kind, &named, Some(size_bytes))?;
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let mut file = directory
            .open_with(name, &options)
            .map(cap_std::fs::File::into_std)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to open linked raw-object target",
                    source,
                )
            })?;
        let opened = opened_file_metadata(&file)?;
        validate_linked_file_state(kind, &opened, Some(size_bytes))?;
        if FileIdentity::from_metadata(&opened) != identity
            || hash_file_bounded_with_control(
                &mut file,
                size_bytes,
                kind.maximum_bytes(),
                Some(control),
            )? != content_digest
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        if kind == RawObjectKind::JournalSegment {
            validate_unclaimed_msj_with_control(&file, size_bytes, Some(control))?;
        }
        let named_after = directory.symlink_metadata(name).map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to re-inspect linked raw-object target",
                source,
            )
        })?;
        let opened_after = opened_file_metadata(&file)?;
        validate_linked_file_state(kind, &named_after, Some(size_bytes))?;
        validate_linked_file_state(kind, &opened_after, Some(size_bytes))?;
        if FileIdentity::from_metadata(&named_after) != identity
            || FileIdentity::from_metadata(&opened_after) != identity
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(true)
    }

    fn open_verified_logical_claim_inner_with_control(
        &self,
        claim: &ResearchObjectClaim,
        control: Option<&dyn ResearchObjectControl>,
    ) -> Result<VerifiedResearchObject, SealedResearchJournalStoreError> {
        validate_object_claim(claim)?;
        let hex = digest_hex(claim.content_digest);
        let shard = self
            .objects
            .open_dir_nofollow(&hex[..2])
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to open logical research-object shard",
                    source,
                )
            })?;
        let filename = format!("{hex}{LOGICAL_OBJECT_SUFFIX}");
        open_verified_object_from_shard_with_control(&shard, &filename, claim, control)
    }

    fn validate_pending_owner(
        &self,
        pending: &PendingResearchObject,
    ) -> Result<(), SealedResearchJournalStoreError> {
        if pending.store_owner_identity != self.owner_identity {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(())
    }

    fn resume_logical_object_inner(
        &self,
        admission: ResearchObjectAdmission,
        claim: &ResearchObjectCheckpointClaim,
    ) -> Result<PendingResearchObject, SealedResearchJournalStoreError> {
        let named = self
            .staging
            .symlink_metadata(&*claim.staging_reference)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to inspect logical research-object checkpoint stage",
                    source,
                )
            })?;
        validate_private_regular_file_with_links(&named, Some(claim.size_bytes), Some(1))?;
        let identity = FileIdentity::from_metadata(&named);
        if identity.device != claim.stage_device || identity.inode != claim.stage_inode {
            return Err(SealedResearchJournalStoreError::ObjectCheckpointMismatch);
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).follow(FollowSymlinks::No);
        let mut file = self
            .staging
            .open_with(&*claim.staging_reference, &options)
            .map(cap_std::fs::File::into_std)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to open logical research-object checkpoint stage",
                    source,
                )
            })?;
        lock_pending_stage(&file)?;
        let opened = opened_file_metadata(&file)?;
        validate_private_regular_file_with_links(&opened, Some(claim.size_bytes), Some(1))?;
        if FileIdentity::from_metadata(&opened) != identity {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        let rehashed = rehash_prefix(
            &mut file,
            claim.size_bytes,
            admission.integrity_chunk_bytes,
            admission.maximum_chunks,
            None,
        )?;
        if rehashed.prefix_digest != claim.prefix_digest
            || rehashed.completed_chunks.as_slice() != claim.completed_chunks.as_ref()
            || rehashed.partial_chunk_bytes != claim.partial_chunk_bytes
            || rehashed.partial_chunk_digest != claim.partial_chunk_digest
        {
            return Err(SealedResearchJournalStoreError::ObjectCheckpointMismatch);
        }
        let named_after = self
            .staging
            .symlink_metadata(&*claim.staging_reference)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to re-inspect logical research-object checkpoint stage",
                    source,
                )
            })?;
        let opened_after = opened_file_metadata(&file)?;
        validate_private_regular_file_with_links(&named_after, Some(claim.size_bytes), Some(1))?;
        validate_private_regular_file_with_links(&opened_after, Some(claim.size_bytes), Some(1))?;
        if FileIdentity::from_metadata(&named_after) != identity
            || FileIdentity::from_metadata(&opened_after) != identity
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        file.seek(SeekFrom::End(0)).map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to seek resumed logical research-object stage",
                source,
            )
        })?;
        Ok(PendingResearchObject {
            admission,
            staging: self.staging.try_clone().map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to retain resumed logical-object staging capability",
                    source,
                )
            })?,
            stage_name: claim.staging_reference.clone(),
            file,
            identity,
            store_owner_identity: self.owner_identity,
            size_bytes: claim.size_bytes,
            completed_chunks: rehashed.completed_chunks,
            whole_hasher: rehashed.whole_hasher,
            partial_hasher: rehashed.partial_hasher,
            partial_chunk_bytes: rehashed.partial_chunk_bytes,
        })
    }
}

impl SealedResearchRecoverySession<'_, '_> {
    /// Verifies one authoritative claim and retains only its fixed-size digest evidence.
    ///
    /// A failed claim or control check permanently aborts this session. No staging or object
    /// namespace is reconciled until [`finish`](Self::finish) consumes the complete scan.
    pub fn observe_claim(
        &mut self,
        claim: &SealedResearchRawClaim,
    ) -> Result<(), SealedResearchJournalStoreError> {
        if self.state != RecoverySessionState::Observing {
            return Err(SealedResearchJournalStoreError::RecoverySessionAborted);
        }
        let result = self.observe_claim_inner(claim);
        if result.is_err() {
            self.state = RecoverySessionState::Aborted;
        }
        result
    }

    /// Attests end-of-catalog, deduplicates verified evidence, and reconciles orphan state.
    pub fn finish(
        mut self,
    ) -> Result<SealedResearchJournalRecoveryReport, SealedResearchJournalStoreError> {
        if self.state != RecoverySessionState::Observing {
            return Err(SealedResearchJournalStoreError::RecoverySessionAborted);
        }
        let prepared = (|| {
            self.control
                .checkpoint(ResearchObjectControlPoint::BeforeRecoveryFinish {
                    observed_claims: self.observed_claims,
                })?;
            compact_retained_raw_objects(&mut self.retained)
        })();
        if let Err(error) = prepared {
            self.state = RecoverySessionState::Aborted;
            return Err(error);
        }
        self.store
            .finish_recovery(&self.retained, self.admission, self.control)
    }

    fn observe_claim_inner(
        &mut self,
        claim: &SealedResearchRawClaim,
    ) -> Result<(), SealedResearchJournalStoreError> {
        if self.observed_claims >= self.admission.maximum_claims
            || self.retained.len() >= self.retained.capacity()
        {
            return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
        }
        self.control
            .checkpoint(ResearchObjectControlPoint::BeforeRecoveryClaim {
                observed_claims: self.observed_claims,
            })?;
        let retained_object = match claim {
            SealedResearchRawClaim::JournalSegment(claim) => {
                let verified = self
                    .store
                    .open_verified_claim_inner_with_control(claim, Some(self.control))?;
                RetainedRawObject {
                    kind: RawObjectKind::JournalSegment,
                    content_digest: verified.receipt().content_digest(),
                    physical_receipt_digest: verified.receipt().physical_receipt_digest(),
                    retained_units: verified.receipt().frames().len(),
                }
            }
            SealedResearchRawClaim::LogicalObject(claim) => {
                let verified = self
                    .store
                    .open_verified_logical_claim_inner_with_control(claim, Some(self.control))?;
                RetainedRawObject {
                    kind: RawObjectKind::LogicalObject,
                    content_digest: verified.receipt.content_digest(),
                    physical_receipt_digest: verified.receipt.claim().physical_receipt_digest(),
                    retained_units: verified.receipt.chunks().len(),
                }
            }
        };
        self.retained.push(retained_object);
        self.observed_claims = self
            .observed_claims
            .checked_add(1)
            .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)?;
        Ok(())
    }
}

fn linked_preparation_error(
    error: SealedResearchJournalStoreError,
) -> SealedResearchJournalStoreError {
    match error {
        error @ (SealedResearchJournalStoreError::ObjectControl(_)
        | SealedResearchJournalStoreError::ObjectAllocationFailed) => error,
        _ => SealedResearchJournalStoreError::RawPublicationIndeterminate,
    }
}

fn validate_linked_file_state(
    kind: RawObjectKind,
    metadata: &cap_std::fs::Metadata,
    expected_bytes: Option<u64>,
) -> Result<(), SealedResearchJournalStoreError> {
    match kind {
        RawObjectKind::JournalSegment => {
            validate_private_regular_file_links(metadata, expected_bytes, 2)
        }
        RawObjectKind::LogicalObject => {
            validate_private_regular_file_with_links(metadata, expected_bytes, Some(2))
                .or_else(|_| validate_read_only_regular_file(metadata, expected_bytes, Some(2)))
        }
    }
}

fn open_locked_linked_stage(
    directory: &Dir,
    name: &str,
    kind: RawObjectKind,
) -> Result<File, SealedResearchJournalStoreError> {
    let named = directory.symlink_metadata(name).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to inspect linked raw-object stage", source)
    })?;
    validate_linked_file_state(kind, &named, None)?;
    let identity = FileIdentity::from_metadata(&named);
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = directory
        .open_with(name, &options)
        .map(cap_std::fs::File::into_std)
        .map_err(|source| {
            SealedResearchJournalStoreError::io("failed to open linked raw-object stage", source)
        })?;
    lock_pending_stage(&file)?;
    let named_after = directory.symlink_metadata(name).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to re-inspect linked raw-object stage", source)
    })?;
    let opened = opened_file_metadata(&file)?;
    validate_linked_file_state(kind, &named_after, None)?;
    validate_linked_file_state(kind, &opened, None)?;
    if FileIdentity::from_metadata(&named_after) != identity
        || FileIdentity::from_metadata(&opened) != identity
    {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    Ok(file)
}

fn verify_reconciled_link_metadata(
    kind: RawObjectKind,
    directory: &Dir,
    name: &str,
    size_bytes: u64,
    identity: FileIdentity,
    published_final: bool,
) -> Result<(), SealedResearchJournalStoreError> {
    let named = directory.symlink_metadata(name).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to inspect reconciled raw-object link", source)
    })?;
    validate_reconciled_metadata(kind, &named, size_bytes, published_final)?;
    if FileIdentity::from_metadata(&named) != identity {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = directory
        .open_with(name, &options)
        .map(cap_std::fs::File::into_std)
        .map_err(|source| {
            SealedResearchJournalStoreError::io("failed to open reconciled raw-object link", source)
        })?;
    let opened = opened_file_metadata(&file)?;
    validate_reconciled_metadata(kind, &opened, size_bytes, published_final)?;
    if FileIdentity::from_metadata(&opened) != identity {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    let named_after = directory.symlink_metadata(name).map_err(|source| {
        SealedResearchJournalStoreError::io(
            "failed to re-inspect reconciled raw-object link",
            source,
        )
    })?;
    let opened_after = opened_file_metadata(&file)?;
    validate_reconciled_metadata(kind, &named_after, size_bytes, published_final)?;
    validate_reconciled_metadata(kind, &opened_after, size_bytes, published_final)?;
    if FileIdentity::from_metadata(&named_after) != identity
        || FileIdentity::from_metadata(&opened_after) != identity
    {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    Ok(())
}

fn validate_reconciled_metadata(
    kind: RawObjectKind,
    metadata: &cap_std::fs::Metadata,
    size_bytes: u64,
    published_final: bool,
) -> Result<(), SealedResearchJournalStoreError> {
    if published_final && kind == RawObjectKind::LogicalObject {
        validate_read_only_regular_file(metadata, Some(size_bytes), Some(1))
    } else if kind == RawObjectKind::LogicalObject {
        validate_private_regular_file_with_links(metadata, Some(size_bytes), Some(1))
            .or_else(|_| validate_read_only_regular_file(metadata, Some(size_bytes), Some(1)))
    } else {
        validate_private_regular_file(metadata, Some(size_bytes))
    }
}

fn try_digest_hex(digest: EvidenceDigest) -> Result<String, SealedResearchJournalStoreError> {
    const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";

    let mut encoded = String::new();
    encoded
        .try_reserve_exact(64)
        .map_err(|_| SealedResearchJournalStoreError::ObjectAllocationFailed)?;
    for byte in digest.bytes() {
        encoded.push(char::from(LOWER_HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(LOWER_HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn compact_retained_raw_objects(
    retained: &mut Vec<RetainedRawObject>,
) -> Result<(), SealedResearchJournalStoreError> {
    retained.sort_unstable_by(|left, right| {
        left.kind.cmp(&right.kind).then_with(|| {
            left.content_digest
                .bytes()
                .cmp(&right.content_digest.bytes())
        })
    });
    let mut unique = 0_usize;
    for read in 0..retained.len() {
        let candidate = retained[read];
        if unique > 0 {
            let existing = retained[unique - 1];
            if existing.kind == candidate.kind
                && existing.content_digest == candidate.content_digest
            {
                if existing.physical_receipt_digest != candidate.physical_receipt_digest
                    || existing.retained_units != candidate.retained_units
                {
                    return Err(SealedResearchJournalStoreError::StateConflict);
                }
                continue;
            }
        }
        retained[unique] = candidate;
        unique += 1;
    }
    retained.truncate(unique);
    Ok(())
}

fn retained_raw_object_exists(
    retained: &[RetainedRawObject],
    kind: RawObjectKind,
    digest_bytes: &[u8; 32],
) -> bool {
    retained
        .binary_search_by(|candidate| {
            candidate
                .kind
                .cmp(&kind)
                .then_with(|| candidate.content_digest.bytes().cmp(digest_bytes))
        })
        .is_ok()
}

fn raw_stage_kind(name: &str) -> Result<RawObjectKind, SealedResearchJournalStoreError> {
    if name.ends_with(LOGICAL_STAGE_SUFFIX) {
        validate_stage_reference(name)
            .map_err(|_| SealedResearchJournalStoreError::RecoveryStateInvalid)?;
        return Ok(RawObjectKind::LogicalObject);
    }
    let Some(uuid) = name.strip_suffix(JOURNAL_STAGE_SUFFIX) else {
        return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
    };
    if !is_canonical_lower_uuid(uuid) || name.len() > MAX_STAGE_REFERENCE_BYTES {
        return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
    }
    Ok(RawObjectKind::JournalSegment)
}

fn raw_object_kind_and_hex(
    filename: &str,
) -> Result<(RawObjectKind, &str), SealedResearchJournalStoreError> {
    if let Some(hex) = filename.strip_suffix(JOURNAL_OBJECT_SUFFIX) {
        Ok((RawObjectKind::JournalSegment, hex))
    } else if let Some(hex) = filename.strip_suffix(LOGICAL_OBJECT_SUFFIX) {
        Ok((RawObjectKind::LogicalObject, hex))
    } else {
        Err(SealedResearchJournalStoreError::RecoveryStateInvalid)
    }
}

fn decode_sha256_hex(hex: &str) -> Result<[u8; 32], SealedResearchJournalStoreError> {
    if !is_lower_hex(hex, 64) {
        return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
    }
    let mut bytes = [0_u8; 32];
    let encoded = hex.as_bytes();
    for (index, byte) in bytes.iter_mut().enumerate() {
        let high = decode_lower_hex_nibble(encoded[index * 2])?;
        let low = decode_lower_hex_nibble(encoded[index * 2 + 1])?;
        *byte = (high << 4) | low;
    }
    Ok(bytes)
}

fn decode_lower_hex_nibble(byte: u8) -> Result<u8, SealedResearchJournalStoreError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(SealedResearchJournalStoreError::RecoveryStateInvalid),
    }
}

fn is_canonical_lower_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

struct RehashedPrefix {
    prefix_digest: EvidenceDigest,
    completed_chunks: Vec<ResearchObjectChunkReceipt>,
    partial_chunk_bytes: u64,
    partial_chunk_digest: Option<EvidenceDigest>,
    whole_hasher: Sha256,
    partial_hasher: Sha256,
    integrity_chunk_bytes: u64,
}

impl RehashedPrefix {
    fn into_all_chunks(
        mut self,
    ) -> Result<Vec<ResearchObjectChunkReceipt>, SealedResearchJournalStoreError> {
        if self.partial_chunk_bytes > 0 {
            self.completed_chunks
                .try_reserve(1)
                .map_err(|_| SealedResearchJournalStoreError::ObjectAllocationFailed)?;
            let ordinal = self.completed_chunks.len();
            let offset = u64::try_from(ordinal)
                .ok()
                .and_then(|value| value.checked_mul(self.integrity_chunk_bytes))
                .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
            self.completed_chunks.push(ResearchObjectChunkReceipt {
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| SealedResearchJournalStoreError::ObjectReceiptMismatch)?,
                offset,
                size_bytes: self.partial_chunk_bytes,
                content_digest: self
                    .partial_chunk_digest
                    .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?,
            });
        }
        Ok(self.completed_chunks)
    }
}

fn open_new_stage(staging: &Dir, name: &str) -> Result<File, SealedResearchJournalStoreError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create_new(true)
        .follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    staging
        .open_with(name, &options)
        .map(cap_std::fs::File::into_std)
        .map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to create logical research-object stage",
                source,
            )
        })
}

fn rehash_prefix(
    file: &mut File,
    expected_bytes: u64,
    integrity_chunk_bytes: u64,
    maximum_chunks: usize,
    control: Option<&dyn ResearchObjectControl>,
) -> Result<RehashedPrefix, SealedResearchJournalStoreError> {
    let required_chunks = chunk_count(expected_bytes, integrity_chunk_bytes)?;
    if expected_bytes > FORMAT_MAX_BYTES || required_chunks > maximum_chunks {
        return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
    }
    file.seek(SeekFrom::Start(0)).map_err(|source| {
        SealedResearchJournalStoreError::io(
            "failed to rewind logical research-object descriptor",
            source,
        )
    })?;
    let mut completed_chunks = Vec::new();
    completed_chunks
        .try_reserve(required_chunks)
        .map_err(|_| SealedResearchJournalStoreError::ObjectAllocationFailed)?;
    let mut whole_hasher = Sha256::new();
    let mut partial_hasher = Sha256::new();
    let mut partial_chunk_bytes = 0_u64;
    let mut observed = 0_u64;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    while observed < expected_bytes {
        if let Some(control) = control {
            control.checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
                offset_bytes: observed,
            })?;
        }
        let remaining = expected_bytes
            .checked_sub(observed)
            .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
        let chunk_remaining = integrity_chunk_bytes
            .checked_sub(partial_chunk_bytes)
            .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
        let attempt = usize::try_from(remaining.min(chunk_remaining))
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = file.read(&mut buffer[..attempt]).map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to verify logical research-object bytes",
                source,
            )
        })?;
        if read == 0 {
            return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
        }
        let read_bytes = u64::try_from(read)
            .map_err(|_| SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
        whole_hasher.update(&buffer[..read]);
        partial_hasher.update(&buffer[..read]);
        observed = observed
            .checked_add(read_bytes)
            .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
        partial_chunk_bytes = partial_chunk_bytes
            .checked_add(read_bytes)
            .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
        if partial_chunk_bytes == integrity_chunk_bytes {
            let ordinal = completed_chunks.len();
            let offset = u64::try_from(ordinal)
                .ok()
                .and_then(|value| value.checked_mul(integrity_chunk_bytes))
                .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
            completed_chunks.push(ResearchObjectChunkReceipt {
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| SealedResearchJournalStoreError::ObjectReceiptMismatch)?,
                offset,
                size_bytes: integrity_chunk_bytes,
                content_digest: EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    std::mem::take(&mut partial_hasher).finalize().into(),
                ),
            });
            partial_chunk_bytes = 0;
        }
    }
    if let Some(control) = control {
        control.checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
            offset_bytes: observed,
        })?;
    }
    if file.read(&mut buffer[..1]).map_err(|source| {
        SealedResearchJournalStoreError::io(
            "failed to probe logical research-object length",
            source,
        )
    })? != 0
    {
        return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
    }
    let prefix_digest = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        whole_hasher.clone().finalize().into(),
    );
    let partial_chunk_digest = (partial_chunk_bytes > 0).then(|| {
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            partial_hasher.clone().finalize().into(),
        )
    });
    Ok(RehashedPrefix {
        prefix_digest,
        completed_chunks,
        partial_chunk_bytes,
        partial_chunk_digest,
        whole_hasher,
        partial_hasher,
        integrity_chunk_bytes,
    })
}

fn open_verified_object_from_shard(
    shard: &Dir,
    filename: &str,
    claim: &ResearchObjectClaim,
) -> Result<VerifiedResearchObject, SealedResearchJournalStoreError> {
    open_verified_object_from_shard_with_control(shard, filename, claim, None)
}

fn open_verified_object_from_shard_with_control(
    shard: &Dir,
    filename: &str,
    claim: &ResearchObjectClaim,
    control: Option<&dyn ResearchObjectControl>,
) -> Result<VerifiedResearchObject, SealedResearchJournalStoreError> {
    let named = shard.symlink_metadata(filename).map_err(|source| {
        SealedResearchJournalStoreError::io(
            "failed to inspect sealed logical research object",
            source,
        )
    })?;
    validate_read_only_regular_file(&named, Some(claim.size_bytes), Some(1))?;
    let identity = FileIdentity::from_metadata(&named);
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let mut file = shard
        .open_with(filename, &options)
        .map(cap_std::fs::File::into_std)
        .map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to open sealed logical research object",
                source,
            )
        })?;
    verify_opened_object_with_control(shard, filename, &mut file, identity, claim, control)?;
    file.seek(SeekFrom::Start(0)).map_err(|source| {
        SealedResearchJournalStoreError::io(
            "failed to rewind verified logical research object",
            source,
        )
    })?;
    Ok(VerifiedResearchObject {
        receipt: ResearchObjectReceipt {
            claim: clone_object_claim(claim)?,
        },
        object_directory: shard.try_clone().map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to retain logical research-object directory",
                source,
            )
        })?,
        filename: filename.into(),
        file,
        identity,
        position: 0,
    })
}

fn verify_opened_object_with_control(
    shard: &Dir,
    filename: &str,
    file: &mut File,
    identity: FileIdentity,
    claim: &ResearchObjectClaim,
    control: Option<&dyn ResearchObjectControl>,
) -> Result<(), SealedResearchJournalStoreError> {
    validate_object_claim(claim)?;
    let opened = opened_file_metadata(file)?;
    validate_read_only_regular_file(&opened, Some(claim.size_bytes), Some(1))?;
    if FileIdentity::from_metadata(&opened) != identity {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    let rehashed = rehash_prefix(
        file,
        claim.size_bytes,
        claim.integrity_chunk_bytes,
        FORMAT_MAX_CHUNKS,
        control,
    )?;
    if rehashed.prefix_digest != claim.content_digest
        || rehashed.into_all_chunks()?.as_slice() != claim.chunks.as_ref()
    {
        return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
    }
    let named_after = shard.symlink_metadata(filename).map_err(|source| {
        SealedResearchJournalStoreError::io(
            "failed to re-inspect sealed logical research object",
            source,
        )
    })?;
    let opened_after = opened_file_metadata(file)?;
    validate_read_only_regular_file(&named_after, Some(claim.size_bytes), Some(1))?;
    validate_read_only_regular_file(&opened_after, Some(claim.size_bytes), Some(1))?;
    if FileIdentity::from_metadata(&named_after) != identity
        || FileIdentity::from_metadata(&opened_after) != identity
    {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    Ok(())
}

fn prepare_read_only_link(
    shard: &Dir,
    filename: &str,
    size_bytes: u64,
    identity: FileIdentity,
    expected_links: u64,
) -> Result<PreparedReadOnlyLink, SealedResearchJournalStoreError> {
    let named = shard.symlink_metadata(filename).map_err(|source| {
        SealedResearchJournalStoreError::io(
            "failed to inspect linked logical research object",
            source,
        )
    })?;
    let already_read_only =
        validate_read_only_regular_file(&named, Some(size_bytes), Some(expected_links)).is_ok();
    if !already_read_only {
        validate_private_regular_file_with_links(&named, Some(size_bytes), Some(expected_links))?;
    }
    if FileIdentity::from_metadata(&named) != identity {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    if !already_read_only {
        options.write(true);
    }
    let file = shard
        .open_with(filename, &options)
        .map(cap_std::fs::File::into_std)
        .map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to open linked logical research object",
                source,
            )
        })?;
    let opened = opened_file_metadata(&file)?;
    if already_read_only {
        validate_read_only_regular_file(&opened, Some(size_bytes), Some(expected_links))?;
    } else {
        validate_private_regular_file_with_links(&opened, Some(size_bytes), Some(expected_links))?;
    }
    if FileIdentity::from_metadata(&opened) != identity {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    let named_after = shard.symlink_metadata(filename).map_err(|source| {
        SealedResearchJournalStoreError::io(
            "failed to re-inspect linked logical research object",
            source,
        )
    })?;
    if already_read_only {
        validate_read_only_regular_file(&named_after, Some(size_bytes), Some(expected_links))?;
    } else {
        validate_private_regular_file_with_links(
            &named_after,
            Some(size_bytes),
            Some(expected_links),
        )?;
    }
    if FileIdentity::from_metadata(&named_after) != identity {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    let read_only_permissions = if already_read_only {
        None
    } else {
        let mut permissions = file
            .metadata()
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to inspect logical research-object permissions",
                    source,
                )
            })?
            .permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            permissions.set_mode(0o400);
        }
        #[cfg(windows)]
        permissions.set_readonly(true);
        #[cfg(not(any(unix, windows)))]
        return Err(SealedResearchJournalStoreError::StateConflict);
        Some(permissions)
    };
    Ok(PreparedReadOnlyLink {
        file,
        identity,
        size_bytes,
        expected_links,
        read_only_permissions,
    })
}

impl PreparedReadOnlyLink {
    fn complete(self, shard: &Dir, filename: &str) -> Result<(), SealedResearchJournalStoreError> {
        let Some(permissions) = self.read_only_permissions else {
            return Ok(());
        };
        self.file.set_permissions(permissions).map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to make logical research object read-only",
                source,
            )
        })?;
        self.file.sync_all().map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to synchronize read-only logical research object",
                source,
            )
        })?;
        let named = shard.symlink_metadata(filename).map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to re-inspect read-only logical research object",
                source,
            )
        })?;
        let opened = opened_file_metadata(&self.file)?;
        validate_read_only_regular_file(&named, Some(self.size_bytes), Some(self.expected_links))?;
        validate_read_only_regular_file(&opened, Some(self.size_bytes), Some(self.expected_links))?;
        if FileIdentity::from_metadata(&named) != self.identity
            || FileIdentity::from_metadata(&opened) != self.identity
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(())
    }
}

fn validate_read_only_regular_file(
    metadata: &cap_std::fs::Metadata,
    expected_bytes: Option<u64>,
    expected_links: Option<u64>,
) -> Result<(), SealedResearchJournalStoreError> {
    validate_confined_regular_file(metadata, expected_bytes, expected_links, true)
}

fn validate_private_regular_file_with_links(
    metadata: &cap_std::fs::Metadata,
    expected_bytes: Option<u64>,
    expected_links: Option<u64>,
) -> Result<(), SealedResearchJournalStoreError> {
    validate_confined_regular_file(metadata, expected_bytes, expected_links, false)
}

fn validate_confined_regular_file(
    metadata: &cap_std::fs::Metadata,
    expected_bytes: Option<u64>,
    expected_links: Option<u64>,
    require_read_only: bool,
) -> Result<(), SealedResearchJournalStoreError> {
    use cap_fs_ext::MetadataExt as _;

    if !metadata.is_file()
        || expected_bytes.is_some_and(|bytes| metadata.len() != bytes)
        || expected_links.is_some_and(|links| metadata.nlink() != links)
    {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    #[cfg(unix)]
    {
        use cap_std::fs::PermissionsExt as _;

        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0
            || (require_read_only && (mode & 0o222 != 0 || mode & 0o400 == 0))
            || (!require_read_only && mode & 0o600 != 0o600)
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        use cap_std::fs::MetadataExt as _;

        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || require_read_only != metadata.permissions().readonly()
        {
            Err(SealedResearchJournalStoreError::StateConflict)
        } else {
            Ok(())
        }
    }
    #[cfg(not(any(unix, windows)))]
    Err(SealedResearchJournalStoreError::StateConflict)
}

fn validate_checkpoint_claim(
    claim: &ResearchObjectCheckpointClaim,
) -> Result<(), SealedResearchJournalStoreError> {
    validate_stage_reference(&claim.staging_reference)?;
    let maximum_chunks = usize::try_from(claim.maximum_chunks)
        .map_err(|_| SealedResearchJournalStoreError::ObjectCheckpointMismatch)?;
    let admission = ResearchObjectAdmission::try_new_with_chunk_bytes(
        claim.maximum_bytes,
        maximum_chunks,
        claim.integrity_chunk_bytes,
    )
    .map_err(|_| SealedResearchJournalStoreError::ObjectCheckpointMismatch)?;
    if claim.size_bytes > admission.maximum_bytes
        || claim.prefix_digest.algorithm() != DigestAlgorithm::Sha256
        || claim.completed_chunks.len() > admission.maximum_chunks
        || claim.partial_chunk_bytes >= admission.integrity_chunk_bytes
        || (claim.partial_chunk_bytes == 0) != claim.partial_chunk_digest.is_none()
        || claim
            .partial_chunk_digest
            .is_some_and(|digest| digest.algorithm() != DigestAlgorithm::Sha256)
    {
        return Err(SealedResearchJournalStoreError::ObjectCheckpointMismatch);
    }
    validate_complete_chunks(&claim.completed_chunks, admission.integrity_chunk_bytes)?;
    let completed_bytes = u64::try_from(claim.completed_chunks.len())
        .ok()
        .and_then(|chunks| chunks.checked_mul(admission.integrity_chunk_bytes))
        .ok_or(SealedResearchJournalStoreError::ObjectCheckpointMismatch)?;
    if completed_bytes
        .checked_add(claim.partial_chunk_bytes)
        .ok_or(SealedResearchJournalStoreError::ObjectCheckpointMismatch)?
        != claim.size_bytes
        || checkpoint_digest(claim) != claim.checkpoint_digest
    {
        return Err(SealedResearchJournalStoreError::ObjectCheckpointMismatch);
    }
    Ok(())
}

fn validate_object_claim(
    claim: &ResearchObjectClaim,
) -> Result<(), SealedResearchJournalStoreError> {
    if claim.size_bytes > FORMAT_MAX_BYTES
        || claim.integrity_chunk_bytes == 0
        || claim.integrity_chunk_bytes > FORMAT_INTEGRITY_CHUNK_BYTES
        || (!cfg!(test) && claim.integrity_chunk_bytes != FORMAT_INTEGRITY_CHUNK_BYTES)
        || claim.chunks.len() > FORMAT_MAX_CHUNKS
        || claim.content_digest.algorithm() != DigestAlgorithm::Sha256
    {
        return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
    }
    let expected_chunks = chunk_count(claim.size_bytes, claim.integrity_chunk_bytes)?;
    if claim.chunks.len() != expected_chunks {
        return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
    }
    let mut expected_offset = 0_u64;
    for (ordinal, chunk) in claim.chunks.iter().enumerate() {
        let final_chunk = ordinal + 1 == claim.chunks.len();
        if chunk.ordinal
            != u32::try_from(ordinal)
                .map_err(|_| SealedResearchJournalStoreError::ObjectReceiptMismatch)?
            || chunk.offset != expected_offset
            || chunk.size_bytes == 0
            || chunk.size_bytes > claim.integrity_chunk_bytes
            || (!final_chunk && chunk.size_bytes != claim.integrity_chunk_bytes)
            || chunk.content_digest.algorithm() != DigestAlgorithm::Sha256
        {
            return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
        }
        expected_offset = expected_offset
            .checked_add(chunk.size_bytes)
            .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?;
    }
    if expected_offset != claim.size_bytes {
        return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
    }
    let hex = digest_hex(claim.content_digest);
    let expected_reference = format!("objects/sha256/{}/{hex}.mro", &hex[..2]);
    if claim.relative_reference.as_ref() != expected_reference
        || claim.physical_receipt_digest
            != object_receipt_digest(
                &expected_reference,
                claim.content_digest,
                claim.size_bytes,
                claim.integrity_chunk_bytes,
                &claim.chunks,
            )
    {
        return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
    }
    Ok(())
}

fn validate_complete_chunks(
    chunks: &[ResearchObjectChunkReceipt],
    integrity_chunk_bytes: u64,
) -> Result<(), SealedResearchJournalStoreError> {
    for (ordinal, chunk) in chunks.iter().enumerate() {
        let expected_offset = u64::try_from(ordinal)
            .ok()
            .and_then(|value| value.checked_mul(integrity_chunk_bytes))
            .ok_or(SealedResearchJournalStoreError::ObjectCheckpointMismatch)?;
        if chunk.ordinal
            != u32::try_from(ordinal)
                .map_err(|_| SealedResearchJournalStoreError::ObjectCheckpointMismatch)?
            || chunk.offset != expected_offset
            || chunk.size_bytes != integrity_chunk_bytes
            || chunk.content_digest.algorithm() != DigestAlgorithm::Sha256
        {
            return Err(SealedResearchJournalStoreError::ObjectCheckpointMismatch);
        }
    }
    Ok(())
}

fn checkpoint_digest(claim: &ResearchObjectCheckpointClaim) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/research-object-checkpoint/v1");
    hash_field(&mut hash, claim.staging_reference.as_bytes());
    hash.update(claim.stage_device.to_be_bytes());
    hash.update(claim.stage_inode.to_be_bytes());
    hash.update(claim.maximum_bytes.to_be_bytes());
    hash.update(claim.maximum_chunks.to_be_bytes());
    hash.update(claim.integrity_chunk_bytes.to_be_bytes());
    hash.update(claim.size_bytes.to_be_bytes());
    hash.update((claim.completed_chunks.len() as u64).to_be_bytes());
    hash_chunks(&mut hash, &claim.completed_chunks);
    hash.update(claim.partial_chunk_bytes.to_be_bytes());
    match claim.partial_chunk_digest {
        Some(digest) => {
            hash.update([1]);
            hash_digest(&mut hash, digest);
        }
        None => hash.update([0]),
    }
    hash_digest(&mut hash, claim.prefix_digest);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn object_receipt_digest(
    reference: &str,
    content_digest: EvidenceDigest,
    size_bytes: u64,
    integrity_chunk_bytes: u64,
    chunks: &[ResearchObjectChunkReceipt],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/research-logical-object-receipt/v1");
    hash_field(&mut hash, reference.as_bytes());
    hash_digest(&mut hash, content_digest);
    hash.update(size_bytes.to_be_bytes());
    hash.update(integrity_chunk_bytes.to_be_bytes());
    hash.update((chunks.len() as u64).to_be_bytes());
    hash_chunks(&mut hash, chunks);
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn hash_chunks(hash: &mut Sha256, chunks: &[ResearchObjectChunkReceipt]) {
    for chunk in chunks {
        hash.update(chunk.ordinal.to_be_bytes());
        hash.update(chunk.offset.to_be_bytes());
        hash.update(chunk.size_bytes.to_be_bytes());
        hash_digest(hash, chunk.content_digest);
    }
}

fn empty_sha256() -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest([]).into())
}

fn chunk_count(
    size_bytes: u64,
    integrity_chunk_bytes: u64,
) -> Result<usize, SealedResearchJournalStoreError> {
    if integrity_chunk_bytes == 0 {
        return Err(SealedResearchJournalStoreError::InvalidObjectAdmission);
    }
    if size_bytes == 0 {
        return Ok(0);
    }
    usize::try_from(
        size_bytes
            .checked_sub(1)
            .and_then(|bytes| bytes.checked_div(integrity_chunk_bytes))
            .and_then(|chunks| chunks.checked_add(1))
            .ok_or(SealedResearchJournalStoreError::ObjectReceiptMismatch)?,
    )
    .map_err(|_| SealedResearchJournalStoreError::ObjectReceiptMismatch)
}

fn clone_chunks(
    chunks: &[ResearchObjectChunkReceipt],
) -> Result<Vec<ResearchObjectChunkReceipt>, SealedResearchJournalStoreError> {
    let mut cloned = Vec::new();
    cloned
        .try_reserve_exact(chunks.len())
        .map_err(|_| SealedResearchJournalStoreError::ObjectAllocationFailed)?;
    cloned.extend_from_slice(chunks);
    Ok(cloned)
}

fn clone_object_claim(
    claim: &ResearchObjectClaim,
) -> Result<ResearchObjectClaim, SealedResearchJournalStoreError> {
    Ok(ResearchObjectClaim {
        relative_reference: claim.relative_reference.clone(),
        content_digest: claim.content_digest,
        size_bytes: claim.size_bytes,
        integrity_chunk_bytes: claim.integrity_chunk_bytes,
        chunks: clone_chunks(&claim.chunks)?.into_boxed_slice(),
        physical_receipt_digest: claim.physical_receipt_digest,
    })
}

fn validate_stage_reference(reference: &str) -> Result<(), SealedResearchJournalStoreError> {
    let Some(uuid) = reference.strip_suffix(LOGICAL_STAGE_SUFFIX) else {
        return Err(SealedResearchJournalStoreError::ObjectCheckpointMismatch);
    };
    if !is_canonical_lower_uuid(uuid) || reference.len() > MAX_STAGE_REFERENCE_BYTES {
        return Err(SealedResearchJournalStoreError::ObjectCheckpointMismatch);
    }
    Ok(())
}

fn deserialize_bounded_stage_reference<'de, D>(deserializer: D) -> Result<Box<str>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ReferenceVisitor;

    impl Visitor<'_> for ReferenceVisitor {
        type Value = Box<str>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "a logical-object stage reference of at most {MAX_STAGE_REFERENCE_BYTES} bytes"
            )
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > MAX_STAGE_REFERENCE_BYTES {
                Err(E::custom("logical-object stage reference bound exceeded"))
            } else {
                Ok(value.into())
            }
        }
    }

    deserializer.deserialize_str(ReferenceVisitor)
}

fn deserialize_bounded_object_reference<'de, D>(deserializer: D) -> Result<Box<str>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ReferenceVisitor;

    impl Visitor<'_> for ReferenceVisitor {
        type Value = Box<str>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "a bounded content-addressed logical-object reference"
            )
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > 128 {
                Err(E::custom("logical-object reference bound exceeded"))
            } else {
                Ok(value.into())
            }
        }
    }

    deserializer.deserialize_str(ReferenceVisitor)
}

fn deserialize_bounded_object_chunks<'de, D>(
    deserializer: D,
) -> Result<Box<[ResearchObjectChunkReceipt]>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ChunksVisitor;

    impl<'de> Visitor<'de> for ChunksVisitor {
        type Value = Box<[ResearchObjectChunkReceipt]>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {FORMAT_MAX_CHUNKS} logical-object integrity chunks"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut chunks = Vec::new();
            chunks
                .try_reserve(sequence.size_hint().unwrap_or(0).min(FORMAT_MAX_CHUNKS))
                .map_err(|_| serde::de::Error::custom("logical-object chunk allocation failed"))?;
            while chunks.len() < FORMAT_MAX_CHUNKS {
                match sequence.next_element()? {
                    Some(chunk) => {
                        chunks.try_reserve(1).map_err(|_| {
                            serde::de::Error::custom("logical-object chunk allocation failed")
                        })?;
                        chunks.push(chunk);
                    }
                    None => return Ok(chunks.into_boxed_slice()),
                }
            }
            if sequence
                .next_element::<ResearchObjectChunkReceipt>()?
                .is_some()
            {
                return Err(serde::de::Error::custom(
                    "logical-object integrity-chunk bound exceeded",
                ));
            }
            Ok(chunks.into_boxed_slice())
        }
    }

    deserializer.deserialize_seq(ChunksVisitor)
}

#[cfg(test)]
mod tests {
    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
        sync::Arc,
    };

    use cap_std::{ambient_authority, fs::Dir};
    use sha2::{Digest as _, Sha256};
    use tempfile::tempdir;

    use super::{
        ResearchObjectAdmission, ResearchObjectControl, ResearchObjectControlError,
        ResearchObjectControlPoint, SealedResearchRawClaim, SealedResearchRecoveryAdmission,
    };
    use crate::journal::{SealedResearchJournalStore, SealedResearchJournalStoreError};

    struct Allow;

    impl ResearchObjectControl for Allow {
        fn checkpoint(
            &self,
            _point: ResearchObjectControlPoint,
        ) -> Result<(), ResearchObjectControlError> {
            Ok(())
        }
    }

    struct CancelVerificationAt(u64);

    impl ResearchObjectControl for CancelVerificationAt {
        fn checkpoint(
            &self,
            point: ResearchObjectControlPoint,
        ) -> Result<(), ResearchObjectControlError> {
            if matches!(
                point,
                ResearchObjectControlPoint::BeforeVerificationChunk { offset_bytes }
                    if offset_bytes == self.0
            ) {
                Err(ResearchObjectControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }

    struct CancelBeforeCommit;

    impl ResearchObjectControl for CancelBeforeCommit {
        fn checkpoint(
            &self,
            point: ResearchObjectControlPoint,
        ) -> Result<(), ResearchObjectControlError> {
            if point == ResearchObjectControlPoint::BeforeCommit {
                Err(ResearchObjectControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn logical_object_resumes_exact_chunks_and_quarantines_a_changed_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempdir()?;
        let journal = Arc::new(Dir::open_ambient_dir(
            temporary.path(),
            ambient_authority(),
        )?);
        let admission = ResearchObjectAdmission::try_new_for_test(32, 8, 4)?;

        let store = SealedResearchJournalStore::try_from_journal_directory(Arc::clone(&journal))?;
        let mut pending = store.begin_logical_object(admission)?;
        pending.write_all(b"abcdef")?;
        let checkpoint = store.checkpoint_logical_object(&mut pending)?;
        assert!(store.resume_logical_object(admission, &checkpoint).is_err());
        assert_eq!(
            std::fs::read_dir(temporary.path().join("research-segments/quarantine"))?.count(),
            0
        );
        drop(pending);
        drop(store);
        let checkpoint = serde_json::from_slice(&serde_json::to_vec(&checkpoint)?)?;

        let store = SealedResearchJournalStore::try_from_journal_directory(Arc::clone(&journal))?;
        let mut resumed = store.resume_logical_object(admission, &checkpoint)?;
        resumed.write_all(b"ghijklmn")?;
        let mut verified = store.finish_logical_object(resumed, &Allow)?;
        assert_eq!(verified.size_bytes(), 14);

        let mut first = [0_u8; 5];
        verified.read_exact(&mut first)?;
        assert_eq!(&first, b"abcde");
        verified.seek(SeekFrom::Start(8))?;
        let mut tail = Vec::new();
        verified.read_to_end(&mut tail)?;
        assert_eq!(tail, b"ijklmn");
        let receipt = verified.reverify_for_commit(&Allow)?;
        assert_eq!(receipt.chunks().len(), 4);

        assert!(matches!(
            store.open_verified_logical_object_claim(receipt.claim(), &CancelVerificationAt(4)),
            Err(SealedResearchJournalStoreError::ObjectControl(
                ResearchObjectControlError::Cancelled
            ))
        ));
        let verified = store.open_verified_logical_object(&receipt, &Allow)?;
        assert!(matches!(
            verified.reverify_for_commit(&CancelBeforeCommit),
            Err(SealedResearchJournalStoreError::ObjectControl(
                ResearchObjectControlError::Cancelled
            ))
        ));
        assert_eq!(
            store
                .open_verified_logical_object_claim(receipt.claim(), &Allow)?
                .reverify_for_commit(&Allow)?,
            receipt
        );

        let mut duplicate = store.begin_logical_object(admission)?;
        duplicate.write_all(b"abcdefghijklmn")?;
        assert_eq!(
            store
                .finish_logical_object(duplicate, &Allow)?
                .reverify_for_commit(&Allow)?,
            receipt
        );

        let interrupted_bytes = b"post-link-crash";
        let mut interrupted = store.begin_logical_object(admission)?;
        interrupted.write_all(interrupted_bytes)?;
        let interrupted_checkpoint = store.checkpoint_logical_object(&mut interrupted)?;
        let interrupted_hex = format!("{:x}", Sha256::digest(interrupted_bytes));
        let interrupted_stage = temporary
            .path()
            .join("research-segments/staging")
            .join(interrupted_checkpoint.staging_reference());
        let interrupted_shard = temporary
            .path()
            .join("research-segments/objects/sha256")
            .join(&interrupted_hex[..2]);
        std::fs::create_dir_all(&interrupted_shard)?;
        let interrupted_final = interrupted_shard.join(format!("{interrupted_hex}.mro"));
        std::fs::hard_link(&interrupted_stage, &interrupted_final)?;
        let mut interrupted_permissions = std::fs::metadata(&interrupted_final)?.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            interrupted_permissions.set_mode(0o400);
        }
        #[cfg(windows)]
        interrupted_permissions.set_readonly(true);
        std::fs::set_permissions(&interrupted_final, interrupted_permissions)?;

        let mut quarantined = store.begin_logical_object(admission)?;
        quarantined.write_all(b"quarantine-crash")?;
        let quarantined_checkpoint = store.checkpoint_logical_object(&mut quarantined)?;
        let quarantined_stage = temporary
            .path()
            .join("research-segments/staging")
            .join(quarantined_checkpoint.staging_reference());
        std::fs::hard_link(
            &quarantined_stage,
            temporary
                .path()
                .join("research-segments/quarantine")
                .join(format!(
                    "staging-{}",
                    quarantined_checkpoint.staging_reference()
                )),
        )?;
        drop(interrupted);
        drop(quarantined);
        drop(store);

        let store = SealedResearchJournalStore::try_from_journal_directory(Arc::clone(&journal))?;
        let generic = SealedResearchRawClaim::LogicalObject(receipt.claim().clone());
        let cancel_at_terminal_probe = CancelVerificationAt(receipt.size_bytes());
        let mut cancelled = store.begin_recovery(
            SealedResearchRecoveryAdmission::try_new(4, 32)?,
            &cancel_at_terminal_probe,
        )?;
        assert!(matches!(
            cancelled.observe_claim(&generic),
            Err(SealedResearchJournalStoreError::ObjectControl(
                ResearchObjectControlError::Cancelled
            ))
        ));
        assert!(matches!(
            cancelled.finish(),
            Err(SealedResearchJournalStoreError::RecoverySessionAborted)
        ));
        let mut recovery =
            store.begin_recovery(SealedResearchRecoveryAdmission::try_new(4, 32)?, &Allow)?;
        recovery.observe_claim(&generic)?;
        let recovery = recovery.finish()?;
        assert_eq!(recovery.retained_journal_segments(), 0);
        assert_eq!(recovery.retained_raw_records(), 0);
        assert_eq!(recovery.retained_logical_objects(), 1);
        assert_eq!(recovery.retained_logical_object_chunks(), 4);
        assert_eq!(
            recovery.quarantined_staging(),
            &[String::from(quarantined_checkpoint.staging_reference())]
        );
        assert_eq!(recovery.quarantined_objects().len(), 1);

        let mut corrupt = store.begin_logical_object(admission)?;
        corrupt.write_all(b"abcdefghijkl")?;
        let corrupt_checkpoint = store.checkpoint_logical_object(&mut corrupt)?;
        drop(corrupt);
        let stage = temporary
            .path()
            .join("research-segments/staging")
            .join(corrupt_checkpoint.staging_reference());
        let mut stage = OpenOptions::new().write(true).open(stage)?;
        stage.write_all(b"X")?;
        stage.sync_all()?;
        drop(stage);
        assert!(
            store
                .resume_logical_object(admission, &corrupt_checkpoint)
                .is_err()
        );
        assert_eq!(
            std::fs::read_dir(temporary.path().join("research-segments/quarantine"))?.count(),
            3
        );
        Ok(())
    }
}
