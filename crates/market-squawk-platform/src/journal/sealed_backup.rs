//! Exact native-byte backup through the existing sealed raw store authority.

use std::{
    cell::Cell,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::MutexGuard,
};

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::super::{
    FileIdentity, RecoveryControlledReader, SealedResearchJournalSegmentClaim, ensure_directory,
    hash_file_bounded_with_control, lock_pending_stage, opened_file_metadata,
    serialized_record_bytes, sha256_with_control, sync_directory, validate_claim_shape,
    validate_private_regular_file,
};
use super::{
    FORMAT_MAX_CHUNKS, HASH_BUFFER_BYTES, RawObjectKind, ResearchObjectControl,
    ResearchObjectControlError, ResearchObjectControlPoint, SealedResearchJournalStore,
    SealedResearchJournalStoreError, SealedResearchRawClaim, open_new_stage,
    prepare_read_only_link, rehash_prefix, try_digest_hex, validate_object_claim,
    validate_reconciled_metadata,
};
use crate::journal::{JournalFormat, JournalReader};

type Result<T> = std::result::Result<T, SealedResearchJournalStoreError>;

impl SealedResearchJournalStore {
    /// Writes exactly the original native bytes bound by a catalog raw claim.
    ///
    /// The caller owns archive framing and must discard its incomplete output on error. This
    /// verifies physical frame/chunk claims before writing and hashes the actual exported bytes.
    /// Memory holds at most one decoded native frame plus fixed-size I/O/verification buffers.
    pub fn write_backup_claim(
        &self,
        claim: &SealedResearchRawClaim,
        writer: &mut dyn Write,
        control: &dyn ResearchObjectControl,
    ) -> Result<()> {
        let _operation = self.backup_operation(control)?;
        let terms = BackupTerms::validate(claim)?;
        let hex = try_digest_hex(terms.digest)?;
        let shard = self
            .objects
            .open_dir_nofollow(&hex[..2])
            .map_err(|source| {
                SealedResearchJournalStoreError::io("failed to open backup raw shard", source)
            })?;
        let filename = format!("{hex}{}", terms.kind.object_suffix());
        let (mut file, identity) = open_backup_object(&shard, &filename, terms)?;
        verify_backup_contents(&mut file, claim, terms, control)?;
        rewind(&mut file)?;
        copy_exact_backup(&mut file, writer, terms, control)?;
        validate_backup_identity(&shard, &filename, &file, identity, terms, true)?;
        self.validate_owner()?;
        control.checkpoint(ResearchObjectControlPoint::BeforeCommit)?;
        Ok(())
    }

    /// Restores exactly one claim's native bytes without consuming the following archive item.
    ///
    /// Publication uses the existing store's single owner, staging namespace, no-replace hard
    /// link, and crash-recovery names. Every claim is reverified before publication. Cancellation
    /// is honored until final-link creation; subsequent failures report indeterminate publication.
    pub fn restore_backup_claim(
        &self,
        claim: &SealedResearchRawClaim,
        reader: &mut dyn Read,
        control: &dyn ResearchObjectControl,
    ) -> Result<()> {
        let _operation = self.backup_operation(control)?;
        let terms = BackupTerms::validate(claim)?;
        let stage_name = format!("{}{}.stage", Uuid::new_v4(), terms.kind.object_suffix());
        let mut stage = open_new_stage(&self.staging, &stage_name)?;
        lock_pending_stage(&stage)?;
        let initial = opened_file_metadata(&stage)?;
        validate_private_regular_file(&initial, Some(0))?;
        let identity = FileIdentity::from_metadata(&initial);
        let result = (|| {
            copy_exact_backup(reader, &mut stage, terms, control)?;
            stage.sync_all().map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to synchronize backup raw stage",
                    source,
                )
            })?;
            validate_backup_identity(&self.staging, &stage_name, &stage, identity, terms, false)?;
            sync_directory(&self.staging)?;
            verify_backup_contents(&mut stage, claim, terms, control)?;
            validate_backup_identity(&self.staging, &stage_name, &stage, identity, terms, false)?;
            self.validate_owner()?;
            let hex = try_digest_hex(terms.digest)?;
            let shard = ensure_directory(&self.objects, &hex[..2])?;
            let filename = format!("{hex}{}", terms.kind.object_suffix());
            control.checkpoint(ResearchObjectControlPoint::BeforeCommit)?;
            match self.staging.hard_link(&stage_name, &shard, &filename) {
                Ok(()) => {
                    let completed = (|| {
                        // Publication has crossed its commit boundary. Complete only bounded
                        // identity, permissions, and durability work without cancellation.
                        let published = shard.symlink_metadata(&filename).map_err(|source| {
                            SealedResearchJournalStoreError::io(
                                "failed to inspect restored raw link",
                                source,
                            )
                        })?;
                        super::validate_linked_file_state(
                            terms.kind,
                            &published,
                            Some(terms.bytes),
                        )?;
                        if FileIdentity::from_metadata(&published) != identity {
                            return Err(SealedResearchJournalStoreError::StateConflict);
                        }
                        sync_directory(&shard)?;
                        if terms.kind == RawObjectKind::LogicalObject {
                            prepare_read_only_link(&shard, &filename, terms.bytes, identity, 2)?
                                .complete(&shard, &filename)?;
                            sync_directory(&shard)?;
                        }
                        drop(stage);
                        self.staging.remove_file(&stage_name).map_err(|source| {
                            SealedResearchJournalStoreError::io(
                                "failed to retire restored raw stage",
                                source,
                            )
                        })?;
                        sync_directory(&self.staging)?;
                        super::verify_reconciled_link_metadata(
                            terms.kind,
                            &shard,
                            &filename,
                            terms.bytes,
                            identity,
                            true,
                        )
                    })();
                    completed
                        .map_err(|_| SealedResearchJournalStoreError::RawPublicationIndeterminate)
                }
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                    let (mut existing, existing_identity) =
                        open_backup_object(&shard, &filename, terms)?;
                    verify_backup_contents(&mut existing, claim, terms, control)?;
                    validate_backup_identity(
                        &shard,
                        &filename,
                        &existing,
                        existing_identity,
                        terms,
                        true,
                    )?;
                    control.checkpoint(ResearchObjectControlPoint::BeforeCommit)?;
                    validate_backup_identity(
                        &self.staging,
                        &stage_name,
                        &stage,
                        identity,
                        terms,
                        false,
                    )?;
                    drop(stage);
                    self.staging.remove_file(&stage_name).map_err(|source| {
                        SealedResearchJournalStoreError::io(
                            "failed to retire duplicate backup raw stage",
                            source,
                        )
                    })?;
                    sync_directory(&self.staging)
                }
                Err(source) => Err(SealedResearchJournalStoreError::io(
                    "failed to publish restored raw object",
                    source,
                )),
            }
        })();
        // Only our unchanged, unpublished private stage may be removed. A linked or ambiguous
        // stage remains for existing store recovery; never remove a possibly committed object.
        if result.is_err()
            && !matches!(
                &result,
                Err(SealedResearchJournalStoreError::RawPublicationIndeterminate)
            )
            && let Ok(named) = self.staging.symlink_metadata(&stage_name)
            && FileIdentity::from_metadata(&named) == identity
            && validate_private_regular_file(&named, None).is_ok()
            && self.staging.remove_file(&stage_name).is_ok()
        {
            let _ignored_sync_failure = sync_directory(&self.staging);
        }
        result
    }

    fn backup_operation<'store>(
        &'store self,
        control: &dyn ResearchObjectControl,
    ) -> Result<MutexGuard<'store, ()>> {
        control.checkpoint(ResearchObjectControlPoint::BeforeVerification)?;
        let operation = match self.operation.try_lock() {
            Ok(operation) => operation,
            Err(std::sync::TryLockError::WouldBlock) => {
                return Err(ResearchObjectControlError::Unavailable.into());
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(SealedResearchJournalStoreError::OperationLockPoisoned);
            }
        };
        self.validate_owner()?;
        Ok(operation)
    }
}

#[derive(Clone, Copy)]
struct BackupTerms {
    kind: RawObjectKind,
    bytes: u64,
    digest: EvidenceDigest,
}

impl BackupTerms {
    fn validate(claim: &SealedResearchRawClaim) -> Result<Self> {
        match claim {
            SealedResearchRawClaim::JournalSegment(claim) => {
                validate_claim_shape(claim)?;
                Ok(Self {
                    kind: RawObjectKind::JournalSegment,
                    bytes: claim.size_bytes(),
                    digest: claim.content_digest(),
                })
            }
            SealedResearchRawClaim::LogicalObject(claim) => {
                validate_object_claim(claim)?;
                Ok(Self {
                    kind: RawObjectKind::LogicalObject,
                    bytes: claim.size_bytes(),
                    digest: claim.content_digest(),
                })
            }
        }
    }
}

fn rewind(file: &mut File) -> Result<()> {
    file.seek(SeekFrom::Start(0)).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to rewind backup raw object", source)
    })?;
    Ok(())
}

fn open_backup_object(
    directory: &Dir,
    name: &str,
    terms: BackupTerms,
) -> Result<(File, FileIdentity)> {
    let named = directory.symlink_metadata(name).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to inspect backup raw object", source)
    })?;
    validate_reconciled_metadata(terms.kind, &named, terms.bytes, true)?;
    let identity = FileIdentity::from_metadata(&named);
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = directory
        .open_with(name, &options)
        .map(cap_std::fs::File::into_std)
        .map_err(|source| {
            SealedResearchJournalStoreError::io("failed to open backup raw object", source)
        })?;
    validate_backup_identity(directory, name, &file, identity, terms, true)?;
    Ok((file, identity))
}

fn validate_backup_identity(
    directory: &Dir,
    name: &str,
    file: &File,
    identity: FileIdentity,
    terms: BackupTerms,
    published: bool,
) -> Result<()> {
    let named = directory.symlink_metadata(name).map_err(|source| {
        SealedResearchJournalStoreError::io("failed to revalidate backup raw object", source)
    })?;
    let opened = opened_file_metadata(file)?;
    for metadata in [&named, &opened] {
        if published {
            validate_reconciled_metadata(terms.kind, metadata, terms.bytes, true)?;
        } else {
            validate_private_regular_file(metadata, Some(terms.bytes))?;
        }
        if FileIdentity::from_metadata(metadata) != identity {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
    }
    Ok(())
}

fn copy_exact_backup(
    reader: &mut dyn Read,
    writer: &mut dyn Write,
    terms: BackupTerms,
    control: &dyn ResearchObjectControl,
) -> Result<()> {
    let mut buffer = [0_u8; HASH_BUFFER_BYTES];
    let mut observed = 0_u64;
    let mut hash = Sha256::new();
    while observed < terms.bytes {
        control.checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
            offset_bytes: observed,
        })?;
        let attempt = usize::try_from(terms.bytes - observed)
            .unwrap_or(usize::MAX)
            .min(buffer.len());
        let read = match reader.read(&mut buffer[..attempt]) {
            Ok(read) => read,
            Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(source) => {
                return Err(SealedResearchJournalStoreError::io(
                    "failed to read backup raw bytes",
                    source,
                ));
            }
        };
        if read == 0 {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        let mut written = 0;
        while written < read {
            control.checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
                offset_bytes: observed + written as u64,
            })?;
            match writer.write(&buffer[written..read]) {
                Ok(0) => {
                    return Err(SealedResearchJournalStoreError::io(
                        "failed to write backup raw bytes",
                        std::io::ErrorKind::WriteZero.into(),
                    ));
                }
                Ok(count) => written += count,
                Err(source) if source.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(source) => {
                    return Err(SealedResearchJournalStoreError::io(
                        "failed to write backup raw bytes",
                        source,
                    ));
                }
            }
        }
        hash.update(&buffer[..read]);
        observed += read as u64;
    }
    control.checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
        offset_bytes: observed,
    })?;
    if EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()) != terms.digest {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    Ok(())
}

fn verify_backup_contents(
    file: &mut File,
    claim: &SealedResearchRawClaim,
    terms: BackupTerms,
    control: &dyn ResearchObjectControl,
) -> Result<()> {
    match claim {
        SealedResearchRawClaim::LogicalObject(claim) => {
            let rehashed = rehash_prefix(
                file,
                terms.bytes,
                claim.integrity_chunk_bytes(),
                FORMAT_MAX_CHUNKS,
                Some(control),
            )?;
            if rehashed.prefix_digest != terms.digest
                || rehashed.into_all_chunks()?.as_slice() != claim.chunks()
            {
                return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
            }
        }
        SealedResearchRawClaim::JournalSegment(claim) => {
            verify_backup_journal_contents(file, claim, terms, control)?;
        }
    }
    Ok(())
}

fn verify_backup_journal_contents(
    file: &mut File,
    claim: &SealedResearchJournalSegmentClaim,
    terms: BackupTerms,
    control: &dyn ResearchObjectControl,
) -> Result<()> {
    if hash_file_bounded_with_control(file, terms.bytes, terms.kind.maximum_bytes(), Some(control))?
        != terms.digest
    {
        return Err(SealedResearchJournalStoreError::ReceiptMismatch);
    }
    rewind(file)?;
    let failure = Cell::new(None);
    let checkpoint = |offset_bytes| {
        control
            .checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk { offset_bytes })
            .map_err(|error| {
                failure.set(Some(error));
                std::io::Error::other("backup journal verification stopped")
            })
    };
    let mut reader = JournalReader::new(RecoveryControlledReader {
        inner: file,
        control,
        failure: &failure,
        observed_bytes: 0,
    });
    let verified = (|| -> Result<()> {
        if reader.ensure_format()? != JournalFormat::MarketSquawkMsj1 {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        let mut payload_bytes = 0;
        for frame in claim.frames() {
            let offset = reader.offset;
            let record = reader
                .next_record_bounded_inner(frame.framed_bytes(), Some(&checkpoint))?
                .ok_or(SealedResearchJournalStoreError::ReceiptMismatch)?;
            let timestamp = record
                .received_at()
                .timestamp_nanos_opt()
                .map(Timestamp::from_unix_nanos)
                .ok_or(SealedResearchJournalStoreError::InvalidReceiveTimestamp)?;
            if offset != frame.offset()
                || reader.offset - offset != frame.framed_bytes()
                || serialized_record_bytes(&record, Some(control))?.checked_add(8)
                    != Some(frame.framed_bytes())
                || record.payload().len() as u64 != frame.provider_payload_bytes()
                || sha256_with_control(record.payload(), &mut payload_bytes, Some(control))?
                    != frame.provider_payload_digest()
                || timestamp != frame.received_at()
                || record.source_sequence() != frame.source_sequence()
            {
                return Err(SealedResearchJournalStoreError::ReceiptMismatch);
            }
        }
        if reader.offset != terms.bytes
            || reader
                .next_record_bounded_inner(0, Some(&checkpoint))?
                .is_some()
        {
            return Err(SealedResearchJournalStoreError::ReceiptMismatch);
        }
        Ok(())
    })();
    if let Some(error) = failure.get() {
        return Err(error.into());
    }
    verified?;

    Ok(())
}
