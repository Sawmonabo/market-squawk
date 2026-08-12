//! Sealed, content-addressed `MSJ1` research segments.
//!
//! This is deliberately separate from [`super::JournalWriter`]. A live append is diagnostic
//! buffering and never produces a durable authority receipt. This store returns a receipt only
//! after the complete immutable segment has been flushed, synchronized, replay-validated,
//! hashed, and published without replacement.

use std::{
    collections::BTreeSet,
    fmt,
    fs::File,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{SeqAccess, Visitor},
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use uuid::Uuid;

use super::{CURRENT_MAGIC, JournalError, JournalReader, write_current_frame};
use crate::RawCaptureRecord;

const STORE_DIRECTORY: &str = "research-segments";
const STAGING_DIRECTORY: &str = "staging";
const OBJECTS_DIRECTORY: &str = "objects";
const SHA256_DIRECTORY: &str = "sha256";
const QUARANTINE_DIRECTORY: &str = "quarantine";
const OWNER_LOCK_FILE: &str = ".owner.lock";
const MAX_SEALED_FRAMES: usize = 4_096;
const MAX_SEALED_BYTES: u64 = 512 * 1024 * 1024;
const MAX_RECOVERY_ENTRIES: usize = 100_000;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
const MAX_CLAIM_REFERENCE_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

impl FileIdentity {
    fn from_metadata(metadata: &cap_std::fs::Metadata) -> Self {
        Self {
            device: cap_fs_ext::MetadataExt::dev(metadata),
            inode: cap_fs_ext::MetadataExt::ino(metadata),
        }
    }
}

/// Physical mapping of one exact raw provider body inside a sealed `MSJ1` segment.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SealedResearchJournalFrameReceipt {
    ordinal: u32,
    offset: u64,
    framed_bytes: u64,
    provider_payload_bytes: u64,
    provider_payload_digest: EvidenceDigest,
    received_at: Timestamp,
    source_sequence: Option<u64>,
}

impl SealedResearchJournalFrameReceipt {
    /// Returns the zero-based frame ordinal.
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }

    /// Returns the byte offset of the frame length prefix.
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the complete length-prefix, CRC, and serialized-envelope byte count.
    pub const fn framed_bytes(&self) -> u64 {
        self.framed_bytes
    }

    /// Returns the exact provider body length inside the raw envelope.
    pub const fn provider_payload_bytes(&self) -> u64 {
        self.provider_payload_bytes
    }

    /// Returns the SHA-256 digest of the exact provider body bytes.
    pub const fn provider_payload_digest(&self) -> EvidenceDigest {
        self.provider_payload_digest
    }

    /// Returns the socket-boundary observation time persisted in the raw envelope.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the provider/source sequence retained in the raw envelope.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }
}

/// Persistable, non-authoritative claim describing one expected sealed `MSJ1` object.
///
/// Deserialization remains bounded, but does not grant authority. Only a successful store seal or
/// verified reopen upgrades this claim to [`SealedResearchJournalSegmentReceipt`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SealedResearchJournalSegmentClaim {
    relative_reference: Box<str>,
    content_digest: EvidenceDigest,
    size_bytes: u64,
    frames: Box<[SealedResearchJournalFrameReceipt]>,
    physical_receipt_digest: EvidenceDigest,
}

impl SealedResearchJournalSegmentClaim {
    /// Returns the capability-relative immutable object reference.
    pub fn relative_reference(&self) -> &str {
        &self.relative_reference
    }

    /// Returns the SHA-256 digest of every exact `MSJ1` byte.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    /// Returns the exact sealed object length.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns the ordered physical frame mapping.
    pub fn frames(&self) -> &[SealedResearchJournalFrameReceipt] {
        &self.frames
    }

    /// Returns the digest binding object location, bytes, and every physical frame coordinate.
    pub const fn physical_receipt_digest(&self) -> EvidenceDigest {
        self.physical_receipt_digest
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SealedResearchJournalSegmentClaimWire {
    #[serde(deserialize_with = "deserialize_bounded_reference")]
    relative_reference: Box<str>,
    content_digest: EvidenceDigest,
    size_bytes: u64,
    #[serde(deserialize_with = "deserialize_bounded_frames")]
    frames: Box<[SealedResearchJournalFrameReceipt]>,
    physical_receipt_digest: EvidenceDigest,
}

fn deserialize_bounded_reference<'de, D>(deserializer: D) -> Result<Box<str>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ReferenceVisitor;

    impl Visitor<'_> for ReferenceVisitor {
        type Value = Box<str>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "a sealed segment reference of at most {MAX_CLAIM_REFERENCE_BYTES} UTF-8 bytes"
            )
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > MAX_CLAIM_REFERENCE_BYTES {
                Err(E::custom("sealed segment reference bound exceeded"))
            } else {
                Ok(Box::from(value))
            }
        }
    }

    deserializer.deserialize_str(ReferenceVisitor)
}

impl<'de> Deserialize<'de> for SealedResearchJournalSegmentClaim {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SealedResearchJournalSegmentClaimWire::deserialize(deserializer)?;
        let claim = Self {
            relative_reference: wire.relative_reference,
            content_digest: wire.content_digest,
            size_bytes: wire.size_bytes,
            frames: wire.frames,
            physical_receipt_digest: wire.physical_receipt_digest,
        };
        validate_claim_shape(&claim).map_err(serde::de::Error::custom)?;
        Ok(claim)
    }
}

fn deserialize_bounded_frames<'de, D>(
    deserializer: D,
) -> Result<Box<[SealedResearchJournalFrameReceipt]>, D::Error>
where
    D: Deserializer<'de>,
{
    struct FramesVisitor;

    impl<'de> Visitor<'de> for FramesVisitor {
        type Value = Box<[SealedResearchJournalFrameReceipt]>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "at most {MAX_SEALED_FRAMES} sealed research journal frames"
            )
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut frames =
                Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_SEALED_FRAMES));
            while frames.len() < MAX_SEALED_FRAMES {
                match sequence.next_element()? {
                    Some(frame) => frames.push(frame),
                    None => return Ok(frames.into_boxed_slice()),
                }
            }
            if sequence
                .next_element::<SealedResearchJournalFrameReceipt>()?
                .is_some()
            {
                return Err(serde::de::Error::custom(
                    "sealed research journal frame bound exceeded",
                ));
            }
            Ok(frames.into_boxed_slice())
        }
    }

    deserializer.deserialize_seq(FramesVisitor)
}

/// Non-forgeable receipt for one durably sealed or exactly reopened `MSJ1` object.
///
/// The type intentionally has no public constructor and no `Deserialize` implementation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SealedResearchJournalSegmentReceipt {
    claim: SealedResearchJournalSegmentClaim,
}

impl SealedResearchJournalSegmentReceipt {
    /// Returns a persistable non-authoritative claim for restart recovery.
    pub const fn claim(&self) -> &SealedResearchJournalSegmentClaim {
        &self.claim
    }

    /// Returns the capability-relative immutable object reference.
    pub fn relative_reference(&self) -> &str {
        self.claim.relative_reference()
    }

    /// Returns the SHA-256 digest of every exact `MSJ1` byte.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.claim.content_digest()
    }

    /// Returns the exact sealed object length.
    pub const fn size_bytes(&self) -> u64 {
        self.claim.size_bytes()
    }

    /// Returns the ordered physical frame mapping.
    pub fn frames(&self) -> &[SealedResearchJournalFrameReceipt] {
        self.claim.frames()
    }

    /// Returns the digest binding object location, bytes, and every physical frame coordinate.
    pub const fn physical_receipt_digest(&self) -> EvidenceDigest {
        self.claim.physical_receipt_digest()
    }
}

/// Verified records reopened from the exact object named by a sealed receipt.
#[derive(Debug)]
pub struct SealedResearchJournalSegment {
    receipt: SealedResearchJournalSegmentReceipt,
    records: Box<[RawCaptureRecord]>,
}

impl SealedResearchJournalSegment {
    /// Returns the store-verified receipt.
    pub const fn receipt(&self) -> &SealedResearchJournalSegmentReceipt {
        &self.receipt
    }

    /// Returns the bounded exact raw envelopes in physical frame order.
    pub fn records(&self) -> &[RawCaptureRecord] {
        &self.records
    }

    /// Consumes the verified handle and returns its bounded raw envelopes.
    pub fn into_records(self) -> Box<[RawCaptureRecord]> {
        self.records
    }
}

/// Conservative startup-recovery result. Quarantine entries are retained, never deleted here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedResearchJournalRecoveryReport {
    quarantined_staging: Box<[Box<str>]>,
    quarantined_objects: Box<[Box<str>]>,
    retained_quarantine_entries: usize,
}

impl SealedResearchJournalRecoveryReport {
    /// Returns staging filenames moved out of the writable namespace.
    pub fn quarantined_staging(&self) -> &[Box<str>] {
        &self.quarantined_staging
    }

    /// Returns unreferenced immutable object references moved to quarantine.
    pub fn quarantined_objects(&self) -> &[Box<str>] {
        &self.quarantined_objects
    }

    /// Returns the complete bounded quarantine entry count after recovery.
    pub const fn retained_quarantine_entries(&self) -> usize {
        self.retained_quarantine_entries
    }
}

/// Sealed research-segment store failure.
#[derive(Debug, Error)]
pub enum SealedResearchJournalStoreError {
    /// Prepared journal capability is required for mutation or verified reopen.
    #[error("prepared journal capability is unavailable")]
    PreparedCapabilityRequired,
    /// Another process already owns the single research-segment store authority.
    #[error("research-segment store already has an active owner")]
    AlreadyOwned,
    /// A sealed segment must contain at least one exact provider response.
    #[error("research-segment store refuses an empty segment")]
    EmptySegment,
    /// A sealed segment exceeded its fixed frame-count ceiling.
    #[error("research-segment frame count exceeds maximum {max}")]
    FrameLimitExceeded {
        /// Fixed store ceiling.
        max: usize,
    },
    /// A sealed segment exceeded its fixed aggregate byte ceiling.
    #[error("research-segment bytes exceed maximum {max}")]
    ByteLimitExceeded {
        /// Fixed store ceiling.
        max: u64,
    },
    /// A receive time cannot be represented by the source-neutral timestamp contract.
    #[error("research-segment receive time is outside signed Unix nanoseconds")]
    InvalidReceiveTimestamp,
    /// Persisted receipt fields or exact reopened bytes disagree.
    #[error("research-segment receipt does not match the sealed object")]
    ReceiptMismatch,
    /// Existing immutable or quarantine state conflicts with an exact retry.
    #[error("research-segment immutable state conflicts with the requested operation")]
    StateConflict,
    /// Startup recovery encountered malformed, ambiguous, or excessive state and stopped.
    #[error("research-segment recovery encountered unrecognized or excessive state")]
    RecoveryStateInvalid,
    /// In-process operation serialization was poisoned.
    #[error("research-segment operation lock is poisoned")]
    OperationLockPoisoned,
    /// Existing or newly written bytes are not a valid `MSJ1` journal.
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// A capability-confined filesystem operation failed.
    #[error("{context}: {source}")]
    Io {
        /// Non-secret operation description.
        context: &'static str,
        /// Underlying filesystem failure.
        #[source]
        source: std::io::Error,
    },
}

impl SealedResearchJournalStoreError {
    fn io(context: &'static str, source: std::io::Error) -> Self {
        Self::Io { context, source }
    }
}

/// Single-owner sealed `MSJ1` research-segment authority.
///
/// The retained filesystem lock prevents two store instances from racing publication or startup
/// recovery. The in-process mutex serializes seal/open/recovery on this owner. Recovery must be
/// called only after the caller has supplied the complete authoritative catalog receipt set.
#[derive(Debug)]
pub struct SealedResearchJournalStore {
    root: Arc<Dir>,
    staging: Dir,
    objects: Dir,
    quarantine: Dir,
    _owner_lock: File,
    owner_identity: FileIdentity,
    operation: Mutex<()>,
}

impl SealedResearchJournalStore {
    pub(crate) fn try_from_journal_directory(
        journal: Arc<Dir>,
    ) -> Result<Self, SealedResearchJournalStoreError> {
        let root = ensure_directory(&journal, STORE_DIRECTORY)?;
        let staging = ensure_directory(&root, STAGING_DIRECTORY)?;
        let objects = ensure_directory(&root, OBJECTS_DIRECTORY)?;
        let objects = ensure_directory(&objects, SHA256_DIRECTORY)?;
        let quarantine = ensure_directory(&root, QUARANTINE_DIRECTORY)?;
        let owner_lock = open_private_file(&root, OWNER_LOCK_FILE, false)?;
        let owner_named = root.symlink_metadata(OWNER_LOCK_FILE).map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to inspect research-segment owner lock",
                source,
            )
        })?;
        let owner_opened = owner_lock.metadata().map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to inspect opened research-segment owner lock",
                source,
            )
        })?;
        validate_private_regular_file(&owner_named, None)?;
        validate_private_regular_file(&owner_opened, None)?;
        let owner_identity = FileIdentity::from_metadata(&owner_named);
        if FileIdentity::from_metadata(&owner_opened) != owner_identity {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        let owner_lock = owner_lock.into_std();
        owner_lock.try_lock_exclusive().map_err(|source| {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                SealedResearchJournalStoreError::AlreadyOwned
            } else {
                SealedResearchJournalStoreError::io(
                    "failed to acquire research-segment owner lock",
                    source,
                )
            }
        })?;
        Ok(Self {
            root: Arc::new(root),
            staging,
            objects,
            quarantine,
            _owner_lock: owner_lock,
            owner_identity,
            operation: Mutex::new(()),
        })
    }

    /// Seals ordered raw provider responses into an immutable content-addressed `MSJ1` object.
    ///
    /// A returned receipt proves the stage was flushed and synchronized, the complete file was
    /// hashed and replay-validated, and the final name was published without replacement.
    pub fn seal(
        &self,
        records: &[RawCaptureRecord],
    ) -> Result<SealedResearchJournalSegmentReceipt, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        self.seal_inner(records)
    }

    /// Opens the exact object named by `receipt`, then re-hashes and replay-validates every frame.
    pub fn open_verified(
        &self,
        receipt: &SealedResearchJournalSegmentReceipt,
    ) -> Result<SealedResearchJournalSegment, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        self.open_verified_claim_inner(receipt.claim())
    }

    /// Reopens a bounded persisted claim and returns authority only after complete verification.
    pub fn open_verified_claim(
        &self,
        claim: &SealedResearchJournalSegmentClaim,
    ) -> Result<SealedResearchJournalSegment, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        self.open_verified_claim_inner(claim)
    }

    /// Quarantines all incomplete stages and all final objects absent from the complete catalog.
    ///
    /// Every supplied authoritative receipt is verified before any mutation. Quarantine content
    /// is retained and reported; this recovery boundary never deletes evidence or guesses whether
    /// malformed entries are safe.
    pub fn recover_after_catalog_scan(
        &self,
        authoritative: &[SealedResearchJournalSegmentClaim],
    ) -> Result<SealedResearchJournalRecoveryReport, SealedResearchJournalStoreError> {
        let _operation = self
            .operation
            .lock()
            .map_err(|_| SealedResearchJournalStoreError::OperationLockPoisoned)?;
        self.validate_owner()?;
        if authoritative.len() > MAX_RECOVERY_ENTRIES {
            return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
        }
        let mut inspected_entries = authoritative.len();
        let mut retained = BTreeSet::new();
        for claim in authoritative {
            let verified = self.open_verified_claim_inner(claim)?;
            retained.insert(digest_hex(verified.receipt().content_digest()));
        }

        let mut quarantined_staging = Vec::new();
        let staging_entries = bounded_entries(&self.staging)?;
        charge_recovery_entries(&mut inspected_entries, staging_entries.len())?;
        for entry in staging_entries {
            let name = portable_name(&entry.file_name())?;
            if !entry
                .file_type()
                .map_err(|source| {
                    SealedResearchJournalStoreError::io(
                        "failed to inspect staging entry type",
                        source,
                    )
                })?
                .is_file()
            {
                return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
            }
            let quarantine_name = format!("staging-{name}");
            quarantine_no_replace(&self.staging, &name, &self.quarantine, &quarantine_name)?;
            quarantined_staging.push(Box::<str>::from(name));
        }

        let mut quarantined_objects = Vec::new();
        let shard_entries = bounded_entries(&self.objects)?;
        charge_recovery_entries(&mut inspected_entries, shard_entries.len())?;
        for shard_entry in shard_entries {
            let shard = portable_name(&shard_entry.file_name())?;
            if !is_lower_hex(&shard, 2)
                || !shard_entry
                    .file_type()
                    .map_err(|source| {
                        SealedResearchJournalStoreError::io(
                            "failed to inspect object shard type",
                            source,
                        )
                    })?
                    .is_dir()
            {
                return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
            }
            let shard_directory = self.objects.open_dir_nofollow(&shard).map_err(|source| {
                SealedResearchJournalStoreError::io("failed to open object shard", source)
            })?;
            let file_entries = bounded_entries(&shard_directory)?;
            charge_recovery_entries(&mut inspected_entries, file_entries.len())?;
            for file_entry in file_entries {
                let filename = portable_name(&file_entry.file_name())?;
                let Some(hex) = filename.strip_suffix(".msj") else {
                    return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
                };
                if !is_lower_hex(hex, 64)
                    || !hex.starts_with(&shard)
                    || !file_entry
                        .file_type()
                        .map_err(|source| {
                            SealedResearchJournalStoreError::io(
                                "failed to inspect sealed object type",
                                source,
                            )
                        })?
                        .is_file()
                {
                    return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
                }
                if retained.contains(hex) {
                    continue;
                }
                let reference = format!("objects/sha256/{shard}/{filename}");
                let quarantine_name = format!("object-{filename}");
                quarantine_no_replace(
                    &shard_directory,
                    &filename,
                    &self.quarantine,
                    &quarantine_name,
                )?;
                quarantined_objects.push(Box::<str>::from(reference));
            }
            sync_directory(&shard_directory)?;
        }
        sync_directory(&self.staging)?;
        sync_directory(&self.quarantine)?;
        let quarantine_entries = bounded_entries(&self.quarantine)?;
        charge_recovery_entries(&mut inspected_entries, quarantine_entries.len())?;
        let retained_quarantine_entries = quarantine_entries.len();
        Ok(SealedResearchJournalRecoveryReport {
            quarantined_staging: quarantined_staging.into_boxed_slice(),
            quarantined_objects: quarantined_objects.into_boxed_slice(),
            retained_quarantine_entries,
        })
    }

    fn validate_owner(&self) -> Result<(), SealedResearchJournalStoreError> {
        let named = self
            .root
            .symlink_metadata(OWNER_LOCK_FILE)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to revalidate research-segment owner lock",
                    source,
                )
            })?;
        let opened = opened_file_metadata(&self._owner_lock)?;
        validate_private_regular_file(&named, None)?;
        validate_private_regular_file(&opened, None)?;
        if FileIdentity::from_metadata(&named) != self.owner_identity
            || FileIdentity::from_metadata(&opened) != self.owner_identity
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(())
    }

    fn seal_inner(
        &self,
        records: &[RawCaptureRecord],
    ) -> Result<SealedResearchJournalSegmentReceipt, SealedResearchJournalStoreError> {
        if records.is_empty() {
            return Err(SealedResearchJournalStoreError::EmptySegment);
        }
        if records.len() > MAX_SEALED_FRAMES {
            return Err(SealedResearchJournalStoreError::FrameLimitExceeded {
                max: MAX_SEALED_FRAMES,
            });
        }
        let stage_name = format!("{}.msj.stage", Uuid::new_v4());
        let stage = open_private_file(&self.staging, &stage_name, true)?;
        let result = self.write_validate_publish_stage(stage, &stage_name, records);
        if result.is_err() {
            match self.staging.remove_file(&stage_name) {
                Ok(()) => {
                    let _ignored_sync_failure = sync_directory(&self.staging);
                }
                Err(source) if source.kind() == std::io::ErrorKind::NotFound => {}
                Err(_source) => {}
            }
        }
        result
    }

    fn write_validate_publish_stage(
        &self,
        stage: cap_std::fs::File,
        stage_name: &str,
        records: &[RawCaptureRecord],
    ) -> Result<SealedResearchJournalSegmentReceipt, SealedResearchJournalStoreError> {
        let initial_metadata = stage.metadata().map_err(|source| {
            SealedResearchJournalStoreError::io("failed to inspect new MSJ1 stage", source)
        })?;
        validate_private_regular_file(&initial_metadata, Some(0))?;
        let stage_identity = FileIdentity::from_metadata(&initial_metadata);
        let mut writer = BufWriter::new(stage.into_std());
        writer.write_all(CURRENT_MAGIC).map_err(|source| {
            SealedResearchJournalStoreError::io("failed to write MSJ1 header", source)
        })?;
        let mut offset = u64::try_from(CURRENT_MAGIC.len()).map_err(|_| {
            SealedResearchJournalStoreError::ByteLimitExceeded {
                max: MAX_SEALED_BYTES,
            }
        })?;
        let mut frames = Vec::with_capacity(records.len());
        for (ordinal, record) in records.iter().enumerate() {
            let written = write_current_frame(&mut writer, record)?;
            let serialized = u64::try_from(written.serialized_payload_bytes).map_err(|_| {
                SealedResearchJournalStoreError::ByteLimitExceeded {
                    max: MAX_SEALED_BYTES,
                }
            })?;
            let framed_bytes = serialized.checked_add(8).ok_or(
                SealedResearchJournalStoreError::ByteLimitExceeded {
                    max: MAX_SEALED_BYTES,
                },
            )?;
            let next = offset.checked_add(framed_bytes).ok_or(
                SealedResearchJournalStoreError::ByteLimitExceeded {
                    max: MAX_SEALED_BYTES,
                },
            )?;
            if next > MAX_SEALED_BYTES {
                return Err(SealedResearchJournalStoreError::ByteLimitExceeded {
                    max: MAX_SEALED_BYTES,
                });
            }
            let received_at = record
                .received_at()
                .timestamp_nanos_opt()
                .map(Timestamp::from_unix_nanos)
                .ok_or(SealedResearchJournalStoreError::InvalidReceiveTimestamp)?;
            frames.push(SealedResearchJournalFrameReceipt {
                ordinal: u32::try_from(ordinal).map_err(|_| {
                    SealedResearchJournalStoreError::FrameLimitExceeded {
                        max: MAX_SEALED_FRAMES,
                    }
                })?,
                offset,
                framed_bytes,
                provider_payload_bytes: u64::try_from(record.payload().len()).map_err(|_| {
                    SealedResearchJournalStoreError::ByteLimitExceeded {
                        max: MAX_SEALED_BYTES,
                    }
                })?,
                provider_payload_digest: sha256(record.payload()),
                received_at,
                source_sequence: record.source_sequence(),
            });
            offset = next;
        }
        writer.flush().map_err(|source| {
            SealedResearchJournalStoreError::io("failed to flush sealed MSJ1 stage", source)
        })?;
        writer.get_ref().sync_all().map_err(|source| {
            SealedResearchJournalStoreError::io("failed to synchronize sealed MSJ1 stage", source)
        })?;
        let mut stage = writer.into_inner().map_err(|error| {
            SealedResearchJournalStoreError::io(
                "failed to release sealed MSJ1 buffer",
                error.into_error(),
            )
        })?;
        let size_bytes = stage
            .metadata()
            .map_err(|source| {
                SealedResearchJournalStoreError::io("failed to inspect sealed MSJ1 stage", source)
            })?
            .len();
        if size_bytes != offset || size_bytes > MAX_SEALED_BYTES {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        let stage_named = self
            .staging
            .symlink_metadata(stage_name)
            .map_err(|source| {
                SealedResearchJournalStoreError::io("failed to inspect named MSJ1 stage", source)
            })?;
        let stage_opened = opened_file_metadata(&stage)?;
        validate_private_regular_file(&stage_named, Some(size_bytes))?;
        validate_private_regular_file(&stage_opened, Some(size_bytes))?;
        if FileIdentity::from_metadata(&stage_named) != stage_identity
            || FileIdentity::from_metadata(&stage_opened) != stage_identity
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        let content_digest = hash_file(&mut stage, size_bytes)?;
        validate_exact_records(&stage, records)?;

        let hex = digest_hex(content_digest);
        let shard_name = &hex[..2];
        let filename = format!("{hex}.msj");
        let relative_reference = format!("objects/sha256/{shard_name}/{filename}");
        let frames = frames.into_boxed_slice();
        let physical_receipt_digest =
            physical_receipt_digest(&relative_reference, content_digest, size_bytes, &frames);
        let claim = SealedResearchJournalSegmentClaim {
            relative_reference: relative_reference.into_boxed_str(),
            content_digest,
            size_bytes,
            frames,
            physical_receipt_digest,
        };
        let receipt = SealedResearchJournalSegmentReceipt {
            claim: claim.clone(),
        };

        let shard = ensure_directory(&self.objects, shard_name)?;
        match self.staging.hard_link(stage_name, &shard, &filename) {
            Ok(()) => {
                let published = shard.symlink_metadata(&filename).map_err(|source| {
                    SealedResearchJournalStoreError::io(
                        "failed to inspect newly linked MSJ1 object",
                        source,
                    )
                })?;
                if !published.file_type().is_file()
                    || published.len() != size_bytes
                    || FileIdentity::from_metadata(&published) != stage_identity
                {
                    return Err(SealedResearchJournalStoreError::StateConflict);
                }
                sync_directory(&shard)?;
            }
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = self.open_verified_from_shard(&shard, &filename, &claim)?;
                if existing.receipt() != &receipt {
                    return Err(SealedResearchJournalStoreError::StateConflict);
                }
            }
            Err(source) => {
                return Err(SealedResearchJournalStoreError::io(
                    "failed to publish sealed MSJ1 object without replacement",
                    source,
                ));
            }
        }
        drop(stage);
        self.staging.remove_file(stage_name).map_err(|source| {
            SealedResearchJournalStoreError::io("failed to remove published MSJ1 stage", source)
        })?;
        sync_directory(&self.staging)?;
        let verified = self.open_verified_from_shard(&shard, &filename, &claim)?;
        if verified.receipt() != &receipt {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(verified.receipt)
    }

    fn open_verified_claim_inner(
        &self,
        claim: &SealedResearchJournalSegmentClaim,
    ) -> Result<SealedResearchJournalSegment, SealedResearchJournalStoreError> {
        validate_claim_shape(claim)?;
        let hex = digest_hex(claim.content_digest);
        let shard = self
            .objects
            .open_dir_nofollow(&hex[..2])
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to open sealed MSJ1 object shard",
                    source,
                )
            })?;
        let filename = format!("{hex}.msj");
        self.open_verified_from_shard(&shard, &filename, claim)
    }

    fn open_verified_from_shard(
        &self,
        shard: &Dir,
        filename: &str,
        claim: &SealedResearchJournalSegmentClaim,
    ) -> Result<SealedResearchJournalSegment, SealedResearchJournalStoreError> {
        let named = shard.symlink_metadata(filename).map_err(|source| {
            SealedResearchJournalStoreError::io("failed to inspect sealed MSJ1 object", source)
        })?;
        validate_private_regular_file(&named, Some(claim.size_bytes))?;
        let identity = FileIdentity::from_metadata(&named);
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let opened = shard.open_with(filename, &options).map_err(|source| {
            SealedResearchJournalStoreError::io("failed to open sealed MSJ1 object", source)
        })?;
        let opened_metadata = opened.metadata().map_err(|source| {
            SealedResearchJournalStoreError::io("failed to inspect opened MSJ1 object", source)
        })?;
        validate_private_regular_file(&opened_metadata, Some(claim.size_bytes))?;
        if FileIdentity::from_metadata(&opened_metadata) != identity {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        let identity_handle = opened.try_clone().map_err(|source| {
            SealedResearchJournalStoreError::io("failed to retain MSJ1 identity handle", source)
        })?;
        let mut file = opened.into_std();
        if file
            .metadata()
            .map_err(|source| {
                SealedResearchJournalStoreError::io("failed to inspect opened MSJ1 object", source)
            })?
            .len()
            != claim.size_bytes
            || hash_file(&mut file, claim.size_bytes)? != claim.content_digest
        {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        validate_current_magic(&mut file)?;
        let records = JournalReader::new(file).read_all_bounded(
            claim.frames.len(),
            claim.size_bytes.saturating_sub(CURRENT_MAGIC.len() as u64),
        )?;
        if records.len() != claim.frames.len() {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        validate_frame_receipts(&records, claim)?;
        let named_after = shard.symlink_metadata(filename).map_err(|source| {
            SealedResearchJournalStoreError::io("failed to re-inspect sealed MSJ1 object", source)
        })?;
        let opened_after = identity_handle.metadata().map_err(|source| {
            SealedResearchJournalStoreError::io("failed to re-inspect opened MSJ1 identity", source)
        })?;
        validate_private_regular_file(&named_after, Some(claim.size_bytes))?;
        validate_private_regular_file(&opened_after, Some(claim.size_bytes))?;
        if FileIdentity::from_metadata(&named_after) != identity
            || FileIdentity::from_metadata(&opened_after) != identity
        {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(SealedResearchJournalSegment {
            receipt: SealedResearchJournalSegmentReceipt {
                claim: claim.clone(),
            },
            records: records.into_boxed_slice(),
        })
    }
}

fn validate_frame_receipts(
    records: &[RawCaptureRecord],
    claim: &SealedResearchJournalSegmentClaim,
) -> Result<(), SealedResearchJournalStoreError> {
    let mut offset = u64::try_from(CURRENT_MAGIC.len())
        .map_err(|_| SealedResearchJournalStoreError::ReceiptMismatch)?;
    for (ordinal, (record, frame)) in records.iter().zip(claim.frames.iter()).enumerate() {
        let serialized = serialized_record_bytes(record)?;
        let framed_bytes = serialized
            .checked_add(8)
            .ok_or(SealedResearchJournalStoreError::ReceiptMismatch)?;
        let received_at = record
            .received_at()
            .timestamp_nanos_opt()
            .map(Timestamp::from_unix_nanos)
            .ok_or(SealedResearchJournalStoreError::InvalidReceiveTimestamp)?;
        if frame.ordinal
            != u32::try_from(ordinal).map_err(|_| {
                SealedResearchJournalStoreError::FrameLimitExceeded {
                    max: MAX_SEALED_FRAMES,
                }
            })?
            || frame.offset != offset
            || frame.framed_bytes != framed_bytes
            || frame.provider_payload_bytes
                != u64::try_from(record.payload().len())
                    .map_err(|_| SealedResearchJournalStoreError::ReceiptMismatch)?
            || frame.provider_payload_digest != sha256(record.payload())
            || frame.received_at != received_at
            || frame.source_sequence != record.source_sequence()
        {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        offset = offset
            .checked_add(framed_bytes)
            .ok_or(SealedResearchJournalStoreError::ReceiptMismatch)?;
    }
    if offset != claim.size_bytes {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    Ok(())
}

fn validate_claim_shape(
    claim: &SealedResearchJournalSegmentClaim,
) -> Result<(), SealedResearchJournalStoreError> {
    if claim.frames.is_empty()
        || claim.frames.len() > MAX_SEALED_FRAMES
        || claim.size_bytes > MAX_SEALED_BYTES
        || claim.content_digest.algorithm() != DigestAlgorithm::Sha256
    {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    let mut expected_offset = u64::try_from(CURRENT_MAGIC.len())
        .map_err(|_| SealedResearchJournalStoreError::ReceiptMismatch)?;
    for (ordinal, frame) in claim.frames.iter().enumerate() {
        if frame.ordinal
            != u32::try_from(ordinal)
                .map_err(|_| SealedResearchJournalStoreError::ReceiptMismatch)?
            || frame.offset != expected_offset
            || frame.framed_bytes < 8
            || frame.provider_payload_bytes > MAX_SEALED_BYTES
            || frame.provider_payload_digest.algorithm() != DigestAlgorithm::Sha256
        {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        expected_offset = expected_offset
            .checked_add(frame.framed_bytes)
            .ok_or(SealedResearchJournalStoreError::ReceiptMismatch)?;
        if expected_offset > MAX_SEALED_BYTES {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
    }
    if expected_offset != claim.size_bytes {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    let hex = digest_hex(claim.content_digest);
    let expected = format!("objects/sha256/{}/{hex}.msj", &hex[..2]);
    if claim.relative_reference.as_ref() != expected
        || claim.physical_receipt_digest
            != physical_receipt_digest(
                &expected,
                claim.content_digest,
                claim.size_bytes,
                &claim.frames,
            )
    {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    Ok(())
}

fn validate_exact_records(
    file: &File,
    expected: &[RawCaptureRecord],
) -> Result<(), SealedResearchJournalStoreError> {
    let mut clone = file.try_clone().map_err(|source| {
        SealedResearchJournalStoreError::io("failed to clone sealed MSJ1 stage", source)
    })?;
    clone.seek(SeekFrom::Start(0)).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to rewind sealed MSJ1 stage", source)
    })?;
    validate_current_magic(&mut clone)?;
    let actual = JournalReader::new(clone).read_all_bounded(
        expected.len(),
        MAX_SEALED_BYTES.saturating_sub(CURRENT_MAGIC.len() as u64),
    )?;
    if actual != expected {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    Ok(())
}

fn validate_current_magic(file: &mut File) -> Result<(), SealedResearchJournalStoreError> {
    file.seek(SeekFrom::Start(0)).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to rewind sealed MSJ1 header", source)
    })?;
    let mut magic = [0_u8; 4];
    file.read_exact(&mut magic).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to read sealed MSJ1 header", source)
    })?;
    if &magic != CURRENT_MAGIC {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    file.seek(SeekFrom::Start(0)).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to rewind validated MSJ1 object", source)
    })?;
    Ok(())
}

fn serialized_record_bytes(
    record: &RawCaptureRecord,
) -> Result<u64, SealedResearchJournalStoreError> {
    let mut sink = std::io::sink();
    let written = write_current_frame(&mut sink, record)?;
    u64::try_from(written.serialized_payload_bytes)
        .map_err(|_| SealedResearchJournalStoreError::ReceiptMismatch)
}

fn physical_receipt_digest(
    reference: &str,
    content_digest: EvidenceDigest,
    size_bytes: u64,
    frames: &[SealedResearchJournalFrameReceipt],
) -> EvidenceDigest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/sealed-research-msj1-receipt/v1");
    hash_field(&mut hash, reference.as_bytes());
    hash_digest(&mut hash, content_digest);
    hash.update(size_bytes.to_be_bytes());
    hash.update((frames.len() as u64).to_be_bytes());
    for frame in frames {
        hash.update(frame.ordinal.to_be_bytes());
        hash.update(frame.offset.to_be_bytes());
        hash.update(frame.framed_bytes.to_be_bytes());
        hash.update(frame.provider_payload_bytes.to_be_bytes());
        hash_digest(&mut hash, frame.provider_payload_digest);
        hash.update(frame.received_at.unix_nanos().to_be_bytes());
        match frame.source_sequence {
            Some(sequence) => {
                hash.update([1]);
                hash.update(sequence.to_be_bytes());
            }
            None => hash.update([0]),
        }
    }
    EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into())
}

fn sha256(bytes: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(bytes).into())
}

fn hash_file(
    file: &mut File,
    expected_bytes: u64,
) -> Result<EvidenceDigest, SealedResearchJournalStoreError> {
    file.seek(SeekFrom::Start(0)).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to rewind sealed MSJ1 bytes", source)
    })?;
    let mut hash = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    loop {
        let count = file.read(&mut buffer).map_err(|source| {
            SealedResearchJournalStoreError::io("failed to hash sealed MSJ1 bytes", source)
        })?;
        if count == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(count).map_err(|_| {
                SealedResearchJournalStoreError::ByteLimitExceeded {
                    max: MAX_SEALED_BYTES,
                }
            })?)
            .ok_or(SealedResearchJournalStoreError::ByteLimitExceeded {
                max: MAX_SEALED_BYTES,
            })?;
        if observed > expected_bytes || observed > MAX_SEALED_BYTES {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        hash.update(&buffer[..count]);
    }
    if observed != expected_bytes {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn hash_digest(hash: &mut Sha256, digest: EvidenceDigest) {
    hash.update([match digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    hash.update(digest.bytes());
}

fn hash_field(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
}

fn digest_hex(digest: EvidenceDigest) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in digest.bytes() {
        use std::fmt::Write as _;
        let _infallible = write!(&mut encoded, "{byte:02x}");
    }
    encoded
}

fn ensure_directory(parent: &Dir, name: &str) -> Result<Dir, SealedResearchJournalStoreError> {
    match parent.open_dir_nofollow(name) {
        Ok(directory) => Ok(directory),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            match parent.create_dir(name) {
                Ok(()) => sync_directory(parent)?,
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(source) => {
                    return Err(SealedResearchJournalStoreError::io(
                        "failed to create research-segment directory",
                        source,
                    ));
                }
            }
            parent.open_dir_nofollow(name).map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to open research-segment directory",
                    source,
                )
            })
        }
        Err(source) => Err(SealedResearchJournalStoreError::io(
            "failed to open research-segment directory",
            source,
        )),
    }
}

fn open_private_file(
    parent: &Dir,
    name: &str,
    create_new: bool,
) -> Result<cap_std::fs::File, SealedResearchJournalStoreError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).follow(FollowSymlinks::No);
    if create_new {
        options.create_new(true);
    } else {
        options.create(true);
    }
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    parent.open_with(name, &options).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to open private research-segment file", source)
    })
}

fn sync_directory(directory: &Dir) -> Result<(), SealedResearchJournalStoreError> {
    #[cfg(unix)]
    {
        directory
            .try_clone()
            .map_err(|source| {
                SealedResearchJournalStoreError::io("failed to clone directory handle", source)
            })?
            .into_std_file()
            .sync_all()
            .map_err(|source| {
                SealedResearchJournalStoreError::io("failed to synchronize directory entry", source)
            })
    }
    #[cfg(windows)]
    {
        let _directory = directory;
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _directory = directory;
        Err(SealedResearchJournalStoreError::StateConflict)
    }
}

fn bounded_entries(
    directory: &Dir,
) -> Result<Vec<cap_std::fs::DirEntry>, SealedResearchJournalStoreError> {
    let mut entries = Vec::new();
    let iterator = directory.entries().map_err(|source| {
        SealedResearchJournalStoreError::io("failed to enumerate research-segment state", source)
    })?;
    for entry in iterator {
        if entries.len() >= MAX_RECOVERY_ENTRIES {
            return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
        }
        entries.push(entry.map_err(|source| {
            SealedResearchJournalStoreError::io(
                "failed to read research-segment directory entry",
                source,
            )
        })?);
    }
    entries.sort_by_key(cap_std::fs::DirEntry::file_name);
    Ok(entries)
}

fn charge_recovery_entries(
    observed: &mut usize,
    additional: usize,
) -> Result<(), SealedResearchJournalStoreError> {
    *observed = observed
        .checked_add(additional)
        .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)?;
    if *observed > MAX_RECOVERY_ENTRIES {
        Err(SealedResearchJournalStoreError::RecoveryStateInvalid)
    } else {
        Ok(())
    }
}

fn portable_name(name: &std::ffi::OsStr) -> Result<String, SealedResearchJournalStoreError> {
    name.to_str()
        .filter(|value| {
            !value.is_empty()
                && *value != "."
                && *value != ".."
                && value.len() <= 255
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
        })
        .map(str::to_owned)
        .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)
}

fn is_lower_hex(value: &str, exact_length: usize) -> bool {
    value.len() == exact_length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn quarantine_no_replace(
    source_directory: &Dir,
    source_name: &str,
    quarantine: &Dir,
    quarantine_name: &str,
) -> Result<(), SealedResearchJournalStoreError> {
    match source_directory.hard_link(source_name, quarantine, quarantine_name) {
        Ok(()) => sync_directory(quarantine)?,
        Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
            if !exact_files_match(source_directory, source_name, quarantine, quarantine_name)? {
                return Err(SealedResearchJournalStoreError::StateConflict);
            }
        }
        Err(source) => {
            return Err(SealedResearchJournalStoreError::io(
                "failed to quarantine research-segment state without replacement",
                source,
            ));
        }
    }
    source_directory
        .remove_file(source_name)
        .map_err(|source| {
            SealedResearchJournalStoreError::io("failed to remove quarantined source entry", source)
        })?;
    sync_directory(source_directory)
}

fn exact_files_match(
    left_directory: &Dir,
    left_name: &str,
    right_directory: &Dir,
    right_name: &str,
) -> Result<bool, SealedResearchJournalStoreError> {
    let left_metadata = left_directory
        .symlink_metadata(left_name)
        .map_err(|source| {
            SealedResearchJournalStoreError::io("failed to inspect quarantine source", source)
        })?;
    let right_metadata = right_directory
        .symlink_metadata(right_name)
        .map_err(|source| {
            SealedResearchJournalStoreError::io("failed to inspect quarantine target", source)
        })?;
    if !left_metadata.file_type().is_file()
        || !right_metadata.file_type().is_file()
        || left_metadata.len() != right_metadata.len()
        || left_metadata.len() > MAX_SEALED_BYTES
    {
        return Ok(false);
    }
    let mut left = open_readonly(left_directory, left_name)?;
    let mut right = open_readonly(right_directory, right_name)?;
    Ok(hash_file(&mut left, left_metadata.len())? == hash_file(&mut right, right_metadata.len())?)
}

fn open_readonly(directory: &Dir, name: &str) -> Result<File, SealedResearchJournalStoreError> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    directory
        .open_with(Path::new(name), &options)
        .map(cap_std::fs::File::into_std)
        .map_err(|source| {
            SealedResearchJournalStoreError::io("failed to open research-segment file", source)
        })
}

fn opened_file_metadata(
    file: &File,
) -> Result<cap_std::fs::Metadata, SealedResearchJournalStoreError> {
    cap_std::fs::File::from_std(file.try_clone().map_err(|source| {
        SealedResearchJournalStoreError::io("failed to clone research-segment handle", source)
    })?)
    .metadata()
    .map_err(|source| {
        SealedResearchJournalStoreError::io("failed to inspect research-segment handle", source)
    })
}

fn validate_private_regular_file(
    metadata: &cap_std::fs::Metadata,
    expected_bytes: Option<u64>,
) -> Result<(), SealedResearchJournalStoreError> {
    if !metadata.is_file()
        || expected_bytes.is_some_and(|expected| metadata.len() != expected)
        || cap_fs_ext::MetadataExt::nlink(metadata) != 1
    {
        return Err(SealedResearchJournalStoreError::StateConflict);
    }
    #[cfg(unix)]
    {
        use cap_std::fs::PermissionsExt as _;

        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        Ok(())
    }
    #[cfg(windows)]
    {
        use cap_std::fs::MetadataExt as _;

        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            Err(SealedResearchJournalStoreError::StateConflict)
        } else {
            Ok(())
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(SealedResearchJournalStoreError::StateConflict)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use cap_std::{ambient_authority, fs::Dir};
    use chrono::{DateTime, Utc};
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::SealedResearchJournalStore;
    use crate::RawCaptureRecord;

    #[test]
    fn sealed_segment_reopens_exact_bytes_and_recovery_retains_only_catalog_objects()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempdir()?;
        let journal = Arc::new(Dir::open_ambient_dir(
            temporary.path(),
            ambient_authority(),
        )?);
        let store = SealedResearchJournalStore::try_from_journal_directory(journal)?;
        let observed = "2026-08-11T14:30:01Z".parse::<DateTime<Utc>>()?;
        let first = RawCaptureRecord::try_new_live(
            Uuid::new_v4(),
            Arc::from("research.fixture"),
            Uuid::new_v4(),
            Some(0),
            None,
            observed,
            Bytes::from_static(br#"{"page":1}"#),
        )?;
        let receipt = store.seal(std::slice::from_ref(&first))?;
        let reopened = store.open_verified(&receipt)?;
        assert_eq!(reopened.records(), std::slice::from_ref(&first));
        assert_eq!(reopened.records()[0].payload(), br#"{"page":1}"#);

        let second = RawCaptureRecord::try_new_live(
            Uuid::new_v4(),
            Arc::from("research.fixture"),
            Uuid::new_v4(),
            Some(0),
            None,
            observed,
            Bytes::from_static(br#"{"page":2}"#),
        )?;
        let orphan = store.seal(std::slice::from_ref(&second))?;
        let recovery = store.recover_after_catalog_scan(std::slice::from_ref(receipt.claim()))?;
        assert!(recovery.quarantined_staging().is_empty());
        assert_eq!(
            recovery.quarantined_objects(),
            &[Box::<str>::from(orphan.relative_reference())]
        );
        assert_eq!(
            store.open_verified(&receipt)?.records()[0].payload(),
            first.payload()
        );
        assert!(store.open_verified(&orphan).is_err());
        Ok(())
    }
}
