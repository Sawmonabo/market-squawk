//! Retained directory and hash cursors. Work budgets apply to a turn, never to store lifetime.
use super::*;
use cap_std::fs::ReadDir;

/// Finite filesystem-entry and hashing work admitted in one maintenance turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedResearchRecoveryAdmission {
    maximum_entries: usize,
    maximum_bytes: u64,
}
impl SealedResearchRecoveryAdmission {
    /// Constructs nonzero per-turn budgets, independent of the retained inventory size.
    pub fn try_new(
        maximum_entries: usize,
        maximum_bytes: u64,
    ) -> Result<Self, SealedResearchJournalStoreError> {
        if maximum_entries == 0 || maximum_bytes == 0 {
            return Err(SealedResearchJournalStoreError::InvalidRecoveryAdmission);
        }
        Ok(Self {
            maximum_entries,
            maximum_bytes,
        })
    }
    /// Returns the maximum directory entries inspected in this turn.
    pub const fn maximum_entries(self) -> usize {
        self.maximum_entries
    }
    /// Returns the maximum physical bytes hashed in this turn.
    pub const fn maximum_bytes(self) -> u64 {
        self.maximum_bytes
    }
}
impl Default for SealedResearchRecoveryAdmission {
    fn default() -> Self {
        Self {
            maximum_entries: 128,
            maximum_bytes: 8 * 1024 * 1024,
        }
    }
}
/// Per-turn observations. Completion means this directory pass ended, not authority for a claim.
#[derive(Debug, Eq, PartialEq)]
pub struct SealedResearchRecoveryTurn {
    report: SealedResearchJournalRecoveryReport,
    complete: bool,
}
impl SealedResearchRecoveryTurn {
    /// Returns only this turn's verified and quarantined entries.
    pub const fn report(&self) -> &SealedResearchJournalRecoveryReport {
        &self.report
    }
    /// Returns whether this pass reached the end of every directory.
    pub const fn complete(&self) -> bool {
        self.complete
    }
}
#[derive(Debug)]
enum Phase {
    Staging,
    Objects,
    Quarantine,
    Complete,
}
#[derive(Debug)]
struct HashingObject {
    directory: Dir,
    name: String,
    reference: String,
    kind: RawObjectKind,
    file: File,
    identity: FileIdentity,
    size: u64,
    modified: cap_std::time::SystemTime,
    offset: u64,
    hasher: Sha256,
    claim: Option<SealedResearchRawClaim>,
    linked_stage: bool,
}
/// One owner-bound pass; retains a bounded number of directory descriptors and one hash state.
/// No filesystem or catalog lock survives an `advance` call.
#[derive(Debug)]
pub struct SealedResearchRecoverySession {
    owner: Arc<RawStoreOwner>,
    phase: Phase,
    entries: ReadDir,
    shard: Option<(String, Dir, ReadDir)>,
    hashing: Option<HashingObject>,
    pending_entry: Option<cap_std::fs::DirEntry>,
}
impl SealedResearchJournalStore {
    /// Starts a retained pass without inspecting the inventory or acquiring the catalog writer.
    pub fn begin_recovery(
        &self,
    ) -> Result<SealedResearchRecoverySession, SealedResearchJournalStoreError> {
        self.validate_owner()?;
        Ok(SealedResearchRecoverySession {
            owner: Arc::clone(&self.owner),
            phase: Phase::Staging,
            entries: entries(&self.staging)?,
            shard: None,
            hashing: None,
            pending_entry: None,
        })
    }
}
impl SealedResearchRecoverySession {
    /// Advances one bounded turn. Membership must be read freshly after the live-pin check;
    /// returning `None` authorizes orphan quarantine only while this turn owns store exclusion.
    pub fn advance(
        &mut self,
        store: &SealedResearchJournalStore,
        admission: SealedResearchRecoveryAdmission,
        control: &dyn ResearchObjectControl,
        mut membership: impl FnMut(
            SealedResearchRawObjectKind,
            EvidenceDigest,
        )
            -> Result<Option<SealedResearchRawClaim>, ResearchObjectControlError>,
    ) -> Result<SealedResearchRecoveryTurn, SealedResearchJournalStoreError> {
        control.checkpoint(ResearchObjectControlPoint::BeforeRecoveryEntry {
            inspected_entries: 0,
        })?;
        if !Arc::ptr_eq(&self.owner, &store.owner) {
            return Err(SealedResearchJournalStoreError::StateConflict);
        }
        let _operation = store
            .operation
            .try_lock()
            .map_err(|_| ResearchObjectControlError::Unavailable)?;
        let _recovery = store
            .recovery_exclusion
            .try_write()
            .map_err(|_| ResearchObjectControlError::Unavailable)?;
        store.validate_owner()?;
        let mut report = SealedResearchJournalRecoveryReport {
            quarantined_staging: Vec::new(),
            quarantined_objects: Vec::new(),
            retained_quarantine_entries: 0,
            retained_journal_segments: 0,
            retained_raw_records: 0,
            retained_logical_objects: 0,
            retained_logical_object_chunks: 0,
        };
        let mut inspected = 0;
        let mut hashed = 0;
        'work: loop {
            match control.checkpoint(ResearchObjectControlPoint::BeforeRecoveryEntry {
                inspected_entries: inspected,
            }) {
                Ok(()) => (),
                Err(ResearchObjectControlError::DeadlineExceeded) => break,
                Err(error) => return Err(error.into()),
            }
            if let Some(pending) = self.hashing.as_mut() {
                // A selected reopen/publication since the previous turn can now own this object.
                if store.owner.pinned(&pending.reference)? {
                    self.hashing = None;
                    continue;
                }
                pending.validate()?;
                let mut buffer = [0u8; HASH_BUFFER_BYTES];
                while pending.offset < pending.size && hashed < admission.maximum_bytes {
                    match control.checkpoint(ResearchObjectControlPoint::BeforeVerificationChunk {
                        offset_bytes: pending.offset,
                    }) {
                        Ok(()) => (),
                        Err(ResearchObjectControlError::DeadlineExceeded) => break 'work,
                        Err(error) => return Err(error.into()),
                    }
                    let length = (pending.size - pending.offset)
                        .min(admission.maximum_bytes - hashed)
                        .min(HASH_BUFFER_BYTES as u64) as usize;
                    pending
                        .file
                        .read_exact(&mut buffer[..length])
                        .map_err(|source| {
                            SealedResearchJournalStoreError::io(
                                "failed to hash retained raw-object recovery descriptor",
                                source,
                            )
                        })?;
                    pending.hasher.update(&buffer[..length]);
                    pending.offset += length as u64;
                    hashed += length as u64;
                }
                if pending.offset != pending.size {
                    break;
                }
                pending.validate()?;
                let digest = EvidenceDigest::new(
                    DigestAlgorithm::Sha256,
                    pending.hasher.clone().finalize().into(),
                );
                if pending.linked_stage {
                    let disposition = store.reconcile_linked_stage(
                        pending.kind,
                        &pending.name,
                        &pending.file,
                        pending.identity,
                        digest,
                        RecoveryControl {
                            control,
                            inspected_entries: inspected,
                        },
                    )?;
                    let pending = self
                        .hashing
                        .take()
                        .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)?;
                    if disposition == LinkedStageDisposition::Quarantined {
                        report.quarantined_staging.push(pending.name);
                    }
                } else {
                    let pending = self
                        .hashing
                        .take()
                        .ok_or(SealedResearchJournalStoreError::RecoveryStateInvalid)?;
                    let expected = match pending.claim.as_ref() {
                        Some(SealedResearchRawClaim::JournalSegment(claim)) => {
                            report.retained_journal_segments += 1;
                            report.retained_raw_records += claim.frames().len();
                            claim.content_digest()
                        }
                        Some(SealedResearchRawClaim::LogicalObject(claim)) => {
                            report.retained_logical_objects += 1;
                            report.retained_logical_object_chunks += claim.chunks().len();
                            claim.content_digest()
                        }
                        None => return Err(SealedResearchJournalStoreError::RecoveryStateInvalid),
                    };
                    if digest != expected {
                        return Err(SealedResearchJournalStoreError::ObjectReceiptMismatch);
                    }
                }
            }
            if inspected >= admission.maximum_entries || hashed >= admission.maximum_bytes {
                break;
            }
            match self.phase {
                Phase::Staging => {
                    let Some(entry) = next_pending(&mut self.pending_entry, &mut self.entries)?
                    else {
                        self.entries = entries(&store.objects)?;
                        self.phase = Phase::Objects;
                        continue;
                    };
                    inspected += 1;
                    let name = super::super::try_portable_name(&entry.file_name())?;
                    let kind = raw_stage_kind(&name)?;
                    let reference = format!("staging/{name}");
                    if store.owner.pinned(&reference)? {
                        continue;
                    }
                    if !entry
                        .file_type()
                        .map_err(|source| {
                            SealedResearchJournalStoreError::io(
                                "failed to inspect raw stage type",
                                source,
                            )
                        })?
                        .is_file()
                    {
                        return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
                    }
                    let metadata = store.staging.symlink_metadata(&name).map_err(|source| {
                        SealedResearchJournalStoreError::io("failed to inspect raw stage", source)
                    })?;
                    match cap_fs_ext::MetadataExt::nlink(&metadata) {
                        1 => {
                            if let Err(error) = quarantine_stage_no_replace(
                                &store.staging,
                                &name,
                                &store.quarantine,
                                &format!("staging-{name}"),
                                kind.maximum_bytes(),
                                1,
                                Some(RecoveryControl {
                                    control,
                                    inspected_entries: inspected,
                                }),
                            ) {
                                self.pending_entry = Some(entry);
                                return Err(error);
                            }
                            report.quarantined_staging.push(name);
                        }
                        2 => {
                            let file = open_locked_linked_stage(&store.staging, &name, kind)?;
                            self.hashing = Some(HashingObject::new(
                                &store.staging,
                                name,
                                reference,
                                kind,
                                file,
                                None,
                                true,
                            )?);
                        }
                        _ => {
                            return Err(
                                SealedResearchJournalStoreError::RawPublicationIndeterminate,
                            );
                        }
                    }
                }
                Phase::Objects => {
                    if let Some((shard_name, shard, iterator)) = self.shard.as_mut() {
                        let Some(entry) = next_pending(&mut self.pending_entry, iterator)? else {
                            self.shard = None;
                            continue;
                        };
                        inspected += 1;
                        let name = super::super::try_portable_name(&entry.file_name())?;
                        let (kind, hex) = raw_object_kind_and_hex(&name)?;
                        if !is_lower_hex(hex, 64)
                            || !hex.starts_with(shard_name.as_str())
                            || !entry
                                .file_type()
                                .map_err(|source| {
                                    SealedResearchJournalStoreError::io(
                                        "failed to inspect raw object type",
                                        source,
                                    )
                                })?
                                .is_file()
                        {
                            return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
                        }
                        let digest =
                            EvidenceDigest::new(DigestAlgorithm::Sha256, decode_sha256_hex(hex)?);
                        let reference = format!("objects/sha256/{shard_name}/{name}");
                        if store.owner.pinned(&reference)? {
                            continue;
                        }
                        let claim = match membership(kind, digest) {
                            Ok(claim) => claim,
                            Err(error) => {
                                self.pending_entry = Some(entry);
                                return Err(error.into());
                            }
                        };
                        if let Some(claim) = claim {
                            let (claim_kind, claim_digest, size) = match &claim {
                                SealedResearchRawClaim::JournalSegment(claim) => {
                                    super::super::validate_claim_shape(claim)?;
                                    (
                                        RawObjectKind::JournalSegment,
                                        claim.content_digest(),
                                        claim.size_bytes(),
                                    )
                                }
                                SealedResearchRawClaim::LogicalObject(claim) => {
                                    validate_object_claim(claim)?;
                                    (
                                        RawObjectKind::LogicalObject,
                                        claim.content_digest(),
                                        claim.size_bytes(),
                                    )
                                }
                            };
                            if claim_kind != kind || claim_digest != digest {
                                return Err(SealedResearchJournalStoreError::ReceiptMismatch);
                            }
                            let mut options = OpenOptions::new();
                            options.read(true).follow(FollowSymlinks::No);
                            let file = shard
                                .open_with(&name, &options)
                                .map(cap_std::fs::File::into_std)
                                .map_err(|source| {
                                    SealedResearchJournalStoreError::io(
                                        "failed to open raw object for bounded recovery",
                                        source,
                                    )
                                })?;
                            let pending = HashingObject::new(
                                shard,
                                name,
                                reference,
                                kind,
                                file,
                                Some(claim),
                                false,
                            )?;
                            if pending.size != size {
                                return Err(SealedResearchJournalStoreError::ReceiptMismatch);
                            }
                            self.hashing = Some(pending);
                        } else {
                            // A live producer would still hold its pin, or already have committed
                            // its exact claim before the fresh membership read above.
                            if let Err(error) = quarantine_no_replace(
                                shard,
                                &name,
                                &store.quarantine,
                                &format!("object-{name}-{}", Uuid::new_v4()),
                                kind.maximum_bytes(),
                                Some(RecoveryControl {
                                    control,
                                    inspected_entries: inspected,
                                }),
                            ) {
                                self.pending_entry = Some(entry);
                                return Err(error);
                            }
                            report.quarantined_objects.push(reference);
                        }
                    } else {
                        let Some(entry) = next_pending(&mut self.pending_entry, &mut self.entries)?
                        else {
                            self.entries = entries(&store.quarantine)?;
                            self.phase = Phase::Quarantine;
                            continue;
                        };
                        inspected += 1;
                        let name = super::super::try_portable_name(&entry.file_name())?;
                        if !is_lower_hex(&name, 2)
                            || !entry
                                .file_type()
                                .map_err(|source| {
                                    SealedResearchJournalStoreError::io(
                                        "failed to inspect raw shard type",
                                        source,
                                    )
                                })?
                                .is_dir()
                        {
                            return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
                        }
                        let shard = store.objects.open_dir_nofollow(&name).map_err(|source| {
                            SealedResearchJournalStoreError::io("failed to open raw shard", source)
                        })?;
                        let iterator = entries(&shard)?;
                        self.shard = Some((name, shard, iterator));
                    }
                }
                Phase::Quarantine => {
                    let Some(entry) = next_pending(&mut self.pending_entry, &mut self.entries)?
                    else {
                        self.phase = Phase::Complete;
                        break;
                    };
                    inspected += 1;
                    super::super::try_portable_name(&entry.file_name())?;
                    if !entry
                        .file_type()
                        .map_err(|source| {
                            SealedResearchJournalStoreError::io(
                                "failed to inspect raw quarantine entry",
                                source,
                            )
                        })?
                        .is_file()
                    {
                        return Err(SealedResearchJournalStoreError::RecoveryStateInvalid);
                    }
                    report.retained_quarantine_entries += 1;
                }
                Phase::Complete => break,
            }
        }
        Ok(SealedResearchRecoveryTurn {
            report,
            complete: matches!(self.phase, Phase::Complete),
        })
    }
}
impl HashingObject {
    fn new(
        directory: &Dir,
        name: String,
        reference: String,
        kind: RawObjectKind,
        file: File,
        claim: Option<SealedResearchRawClaim>,
        linked_stage: bool,
    ) -> Result<Self, SealedResearchJournalStoreError> {
        let metadata = opened_file_metadata(&file)?;
        let result = Self {
            directory: directory.try_clone().map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to retain raw recovery directory",
                    source,
                )
            })?,
            name,
            reference,
            kind,
            identity: FileIdentity::from_metadata(&metadata),
            size: metadata.len(),
            modified: metadata.modified().map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to read raw recovery modification time",
                    source,
                )
            })?,
            file,
            offset: 0,
            hasher: Sha256::new(),
            claim,
            linked_stage,
        };
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> Result<(), SealedResearchJournalStoreError> {
        let named = self
            .directory
            .symlink_metadata(&self.name)
            .map_err(|source| {
                SealedResearchJournalStoreError::io(
                    "failed to inspect retained raw recovery name",
                    source,
                )
            })?;
        let opened = opened_file_metadata(&self.file)?;
        for metadata in [&named, &opened] {
            if self.size > self.kind.maximum_bytes()
                || metadata.modified().map_err(|source| {
                    SealedResearchJournalStoreError::io(
                        "failed to verify raw recovery modification time",
                        source,
                    )
                })? != self.modified
                || FileIdentity::from_metadata(metadata) != self.identity
            {
                return Err(SealedResearchJournalStoreError::StateConflict);
            }
            if self.linked_stage {
                validate_linked_file_state(self.kind, metadata, Some(self.size))?;
            } else {
                validate_reconciled_metadata(self.kind, metadata, self.size, true)?;
            }
        }
        Ok(())
    }
}
fn entries(directory: &Dir) -> Result<ReadDir, SealedResearchJournalStoreError> {
    directory.entries().map_err(|source| {
        SealedResearchJournalStoreError::io("failed to enumerate raw recovery directory", source)
    })
}
fn next(
    entries: &mut ReadDir,
) -> Result<Option<cap_std::fs::DirEntry>, SealedResearchJournalStoreError> {
    entries.next().transpose().map_err(|source| {
        SealedResearchJournalStoreError::io("failed to read raw recovery entry", source)
    })
}

fn next_pending(
    pending: &mut Option<cap_std::fs::DirEntry>,
    entries: &mut ReadDir,
) -> Result<Option<cap_std::fs::DirEntry>, SealedResearchJournalStoreError> {
    if pending.is_some() {
        Ok(pending.take())
    } else {
        next(entries)
    }
}
