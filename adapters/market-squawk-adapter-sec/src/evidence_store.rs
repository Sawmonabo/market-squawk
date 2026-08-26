//! Capability-scoped immutable raw-evidence persistence.

use std::fmt::Write as _;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, Timestamp};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MAX_RAW_EVIDENCE_READ_BYTES: u64 = 64 * 1024 * 1024;

/// Secret-free identity returned after a streamed response is durably sealed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RawEvidenceReceipt {
    evidence: EvidenceDigest,
    size_bytes: u64,
}

/// Bounded capability-scoped content writer used by immutable provider-local generations.
pub(crate) struct RawEvidenceContentWriter<'a> {
    store: &'a RawEvidenceStore,
    scratch: RawEvidenceScratch<'a>,
    digest: Sha256,
    observed: u64,
    maximum: u64,
    deadline: Timestamp,
}

impl RawEvidenceContentWriter<'_> {
    /// Appends one bounded chunk while preserving a streaming SHA-256 identity.
    pub(crate) fn write_bytes(
        &mut self,
        bytes: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<(), RawEvidenceError> {
        check_deadline(cancellation, self.deadline)?;
        let increment =
            u64::try_from(bytes.len()).map_err(|_| RawEvidenceError::WriteLimitExceeded)?;
        let next = self
            .observed
            .checked_add(increment)
            .ok_or(RawEvidenceError::WriteLimitExceeded)?;
        if next > self.maximum {
            return Err(RawEvidenceError::WriteLimitExceeded);
        }
        self.scratch.file_mut()?.write_all(bytes)?;
        self.digest.update(bytes);
        self.observed = next;
        Ok(())
    }

    /// Returns bytes durably staged so far.
    pub(crate) const fn observed_bytes(&self) -> u64 {
        self.observed
    }

    /// Flushes, rereads, seals read-only, and atomically publishes this exact content object.
    pub(crate) fn seal(
        mut self,
        cancellation: &CancellationToken,
    ) -> Result<RawEvidenceReceipt, RawEvidenceError> {
        check_deadline(cancellation, self.deadline)?;
        if self.observed == 0 {
            return Err(RawEvidenceError::LengthMismatch);
        }
        self.scratch.file_mut()?.sync_all()?;
        let evidence = EvidenceDigest::new(DigestAlgorithm::Sha256, self.digest.finalize().into());
        verify_reader_before(
            self.scratch.file_mut()?,
            evidence,
            self.observed,
            self.deadline,
            cancellation,
        )?;
        seal_readonly(self.scratch.file_mut()?)?;
        self.scratch.file_mut()?.sync_all()?;
        check_deadline(cancellation, self.deadline)?;
        let final_name = evidence_name(evidence)?;
        match self
            .store
            .directory
            .hard_link(&self.scratch.name, &self.store.directory, &final_name)
        {
            Ok(()) => {
                self.store.observe_final_link(cancellation);
                #[cfg(windows)]
                sync_new_link_metadata(self.scratch.file_mut()?)?;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let mut existing = self.store.open_named(&final_name, self.observed)?;
                verify_reader_before(
                    &mut existing,
                    evidence,
                    self.observed,
                    self.deadline,
                    cancellation,
                )?;
                self.store.observe_identical_final();
            }
            Err(error) => return Err(error.into()),
        }
        sync_publication_directory(&self.store.directory)?;
        self.store.observe_directory_synced();
        Ok(RawEvidenceReceipt {
            evidence,
            size_bytes: self.observed,
        })
    }
}

impl RawEvidenceReceipt {
    pub(crate) const fn new(evidence: EvidenceDigest, size_bytes: u64) -> Self {
        Self {
            evidence,
            size_bytes,
        }
    }

    /// Returns exact SHA-256 evidence identity.
    pub(crate) const fn evidence(self) -> EvidenceDigest {
        self.evidence
    }

    /// Returns exact streamed byte length.
    pub(crate) const fn size_bytes(self) -> u64 {
        self.size_bytes
    }
}

/// Capability-scoped, content-addressed raw SEC evidence store.
#[derive(Debug)]
pub struct RawEvidenceStore {
    directory: Dir,
    #[cfg(test)]
    publication_probe: Option<Arc<PublicationCommitTestProbe>>,
}

/// Capability-scoped temporary file used only for bounded external validation.
pub(crate) struct RawEvidenceScratch<'a> {
    directory: &'a Dir,
    name: String,
    file: Option<std::fs::File>,
}

impl RawEvidenceScratch<'_> {
    pub(crate) fn file_mut(&mut self) -> Result<&mut std::fs::File, RawEvidenceError> {
        if self.file.is_none() {
            let mut options = OpenOptions::new();
            options.read(true).write(true);
            options.follow(FollowSymlinks::No);
            self.file = Some(self.directory.open_with(&self.name, &options)?.into_std());
        }
        self.file
            .as_mut()
            .ok_or(RawEvidenceError::VerificationFailed)
    }

    pub(crate) fn rewind(&mut self) -> Result<(), RawEvidenceError> {
        self.file_mut()?.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub(crate) fn sync_and_rewind(&mut self) -> Result<(), RawEvidenceError> {
        self.file_mut()?.sync_all()?;
        self.rewind()
    }

    /// Flushes and closes this descriptor without deleting its confined scratch inode.
    ///
    /// A later [`Self::file_mut`] call reopens it without following links. Bounded external merge
    /// passes can therefore retain many runs while holding only a fixed number of descriptors.
    pub(crate) fn sync_and_close(&mut self) -> Result<(), RawEvidenceError> {
        self.file_mut()?.sync_all()?;
        drop(self.file.take());
        Ok(())
    }

    /// Releases only the descriptor for bounded scratch-file fan-out.
    ///
    /// Scratch bytes are not publication authority; the final reader must still sync and verify
    /// them before creating any immutable object.
    pub(crate) fn close_descriptor(&mut self) {
        drop(self.file.take());
    }
}

impl Drop for RawEvidenceScratch<'_> {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ignored = self.directory.remove_file(&self.name);
    }
}

impl RawEvidenceStore {
    /// Adopts an already-authorized directory capability without ambient path access.
    pub const fn new(directory: Dir) -> Self {
        Self {
            directory,
            #[cfg(test)]
            publication_probe: None,
        }
    }

    /// Creates a private same-capability scratch inode removed automatically on every exit path.
    pub(crate) fn create_scratch(&self) -> Result<RawEvidenceScratch<'_>, RawEvidenceError> {
        let name = format!(".sec-validation-{}.tmp", Uuid::new_v4());
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let file = self.directory.open_with(&name, &options)?.into_std();
        Ok(RawEvidenceScratch {
            directory: &self.directory,
            name,
            file: Some(file),
        })
    }

    /// Begins one bounded streaming content object under an absolute publication deadline.
    pub(crate) fn create_content_writer(
        &self,
        maximum: u64,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<RawEvidenceContentWriter<'_>, RawEvidenceError> {
        if maximum == 0 {
            return Err(RawEvidenceError::WriteLimitExceeded);
        }
        check_deadline(cancellation, deadline)?;
        Ok(RawEvidenceContentWriter {
            store: self,
            scratch: self.create_scratch()?,
            digest: Sha256::new(),
            observed: 0,
            maximum,
            deadline,
        })
    }

    #[cfg(test)]
    fn new_with_publication_probe(
        directory: Dir,
        publication_probe: Arc<PublicationCommitTestProbe>,
    ) -> Self {
        Self {
            directory,
            publication_probe: Some(publication_probe),
        }
    }

    /// Persists exact bytes under their SHA-256 content identity.
    ///
    /// Publication writes and fsyncs a private staging inode, then atomically creates the final
    /// name with a same-directory hard link. Existing content is accepted only after byte and hash
    /// verification, making retries and restart reconciliation idempotent.
    pub fn persist(&self, bytes: &[u8]) -> Result<EvidenceDigest, RawEvidenceError> {
        self.persist_cancellable(bytes, &CancellationToken::new())
    }

    /// Persists exact bytes while checkpointing cooperative cancellation between bounded chunks.
    pub fn persist_cancellable(
        &self,
        bytes: &[u8],
        cancellation: &CancellationToken,
    ) -> Result<EvidenceDigest, RawEvidenceError> {
        check_cancelled(cancellation)?;
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let evidence = EvidenceDigest::new(DigestAlgorithm::Sha256, digest);
        let final_name = evidence_name(evidence)?;
        let expected_bytes =
            u64::try_from(bytes.len()).map_err(|_| RawEvidenceError::ReadLimitExceeded)?;
        match self.read_named_verified(&final_name, evidence, expected_bytes, cancellation) {
            Ok(existing) if existing.as_slice() == bytes => {
                self.observe_identical_final();
                sync_publication_directory(&self.directory)?;
                self.observe_directory_synced();
                return Ok(evidence);
            }
            Ok(_) => return Err(RawEvidenceError::ContentConflict),
            Err(RawEvidenceError::Io(error)) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let staging_name = format!(".sec-evidence-{}.tmp", Uuid::new_v4());
        let cleanup = StagingCleanup {
            directory: &self.directory,
            name: &staging_name,
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut staging = self
            .directory
            .open_with(&staging_name, &options)?
            .into_std();
        for chunk in bytes.chunks(64 * 1024) {
            check_cancelled(cancellation)?;
            staging.write_all(chunk)?;
        }
        check_cancelled(cancellation)?;
        staging.sync_all()?;
        staging.seek(SeekFrom::Start(0))?;
        let mut verified = Vec::new();
        verified
            .try_reserve(bytes.len())
            .map_err(|_| RawEvidenceError::AllocationFailed)?;
        read_chunks(&mut staging, &mut verified, cancellation)?;
        if verified.as_slice() != bytes || Sha256::digest(&verified).as_slice() != digest {
            return Err(RawEvidenceError::VerificationFailed);
        }
        seal_readonly(&staging)?;
        staging.sync_all()?;

        check_cancelled(cancellation)?;
        match self
            .directory
            .hard_link(&staging_name, &self.directory, &final_name)
        {
            Ok(()) => {
                self.observe_final_link(cancellation);
                #[cfg(windows)]
                sync_new_link_metadata(&staging)?;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let commit_cancellation = CancellationToken::new();
                if self
                    .read_named_verified(
                        &final_name,
                        evidence,
                        expected_bytes,
                        &commit_cancellation,
                    )?
                    .as_slice()
                    != bytes
                {
                    return Err(RawEvidenceError::ContentConflict);
                }
                self.observe_identical_final();
            }
            Err(error) => return Err(error.into()),
        }
        sync_publication_directory(&self.directory)?;
        self.observe_directory_synced();
        drop(cleanup);
        Ok(evidence)
    }

    /// Streams bounded response chunks into one private staging inode and atomically seals them.
    ///
    /// The receiver is deliberately synchronous: callers feed its bounded channel from async
    /// transport while this method runs on the adapter's admitted blocking pool. The archive is
    /// never assembled in memory. `expected_bytes` binds a provider `Content-Length` when one was
    /// supplied; `max_bytes` remains authoritative when it was not.
    pub(crate) fn persist_stream_receiver(
        &self,
        mut receiver: mpsc::Receiver<Bytes>,
        expected_bytes: Option<u64>,
        max_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<RawEvidenceReceipt, RawEvidenceError> {
        if max_bytes == 0 || expected_bytes.is_some_and(|length| length > max_bytes) {
            return Err(RawEvidenceError::WriteLimitExceeded);
        }
        check_cancelled(cancellation)?;
        let staging_name = format!(".sec-evidence-{}.tmp", Uuid::new_v4());
        let cleanup = StagingCleanup {
            directory: &self.directory,
            name: &staging_name,
        };
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        options.follow(FollowSymlinks::No);
        configure_private_creation(&mut options);
        let mut staging = self
            .directory
            .open_with(&staging_name, &options)?
            .into_std();
        let mut digest = Sha256::new();
        let mut observed = 0_u64;
        while let Some(chunk) = receiver.blocking_recv() {
            check_cancelled(cancellation)?;
            let chunk_bytes =
                u64::try_from(chunk.len()).map_err(|_| RawEvidenceError::WriteLimitExceeded)?;
            observed = observed
                .checked_add(chunk_bytes)
                .ok_or(RawEvidenceError::WriteLimitExceeded)?;
            if observed > max_bytes || expected_bytes.is_some_and(|length| observed > length) {
                return Err(RawEvidenceError::WriteLimitExceeded);
            }
            staging.write_all(&chunk)?;
            digest.update(&chunk);
        }
        check_cancelled(cancellation)?;
        if observed == 0 || expected_bytes.is_some_and(|length| length != observed) {
            return Err(RawEvidenceError::LengthMismatch);
        }
        staging.sync_all()?;
        let evidence = EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into());
        staging.seek(SeekFrom::Start(0))?;
        verify_reader(&mut staging, evidence, observed, cancellation)?;
        seal_readonly(&staging)?;
        staging.sync_all()?;
        check_cancelled(cancellation)?;
        let final_name = evidence_name(evidence)?;
        match self
            .directory
            .hard_link(&staging_name, &self.directory, &final_name)
        {
            Ok(()) => {
                self.observe_final_link(cancellation);
                #[cfg(windows)]
                sync_new_link_metadata(&staging)?;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                let commit_cancellation = CancellationToken::new();
                let mut existing = self.open_named(&final_name, observed)?;
                verify_reader(&mut existing, evidence, observed, &commit_cancellation)?;
                self.observe_identical_final();
            }
            Err(error) => return Err(error.into()),
        }
        sync_publication_directory(&self.directory)?;
        self.observe_directory_synced();
        drop(cleanup);
        Ok(RawEvidenceReceipt {
            evidence,
            size_bytes: observed,
        })
    }

    /// Reads and re-verifies exact evidence after restart.
    pub fn read_verified(&self, evidence: &EvidenceDigest) -> Result<Vec<u8>, RawEvidenceError> {
        self.read_verified_bounded(evidence, MAX_RAW_EVIDENCE_READ_BYTES)
    }

    /// Reads exact evidence only when its current regular-file size is within `max_bytes`.
    pub fn read_verified_bounded(
        &self,
        evidence: &EvidenceDigest,
        max_bytes: u64,
    ) -> Result<Vec<u8>, RawEvidenceError> {
        self.read_verified_bounded_cancellable(evidence, max_bytes, &CancellationToken::new())
    }

    /// Reads exact evidence with a hard byte ceiling and bounded cancellation checkpoints.
    pub fn read_verified_bounded_cancellable(
        &self,
        evidence: &EvidenceDigest,
        max_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, RawEvidenceError> {
        self.read_named_verified(
            &evidence_name(*evidence)?,
            *evidence,
            max_bytes,
            cancellation,
        )
    }

    /// Opens one exact large raw object after streaming size and digest verification.
    ///
    /// The returned descriptor is already positioned at byte zero. It remains a confined file
    /// capability and lets ZIP readers seek without copying an archive into memory.
    pub fn open_verified(
        &self,
        evidence: &EvidenceDigest,
        expected_bytes: u64,
        max_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<std::fs::File, RawEvidenceError> {
        if expected_bytes == 0 || expected_bytes > max_bytes {
            return Err(RawEvidenceError::ReadLimitExceeded);
        }
        let mut file = self.open_named(&evidence_name(*evidence)?, max_bytes)?;
        verify_reader(&mut file, *evidence, expected_bytes, cancellation)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }

    /// Opens exact large evidence only when streaming verification completes before `deadline`.
    pub fn open_verified_before(
        &self,
        evidence: &EvidenceDigest,
        expected_bytes: u64,
        max_bytes: u64,
        deadline: Timestamp,
        cancellation: &CancellationToken,
    ) -> Result<std::fs::File, RawEvidenceError> {
        check_deadline(cancellation, deadline)?;
        if expected_bytes == 0 || expected_bytes > max_bytes {
            return Err(RawEvidenceError::ReadLimitExceeded);
        }
        let mut file = self.open_named(&evidence_name(*evidence)?, max_bytes)?;
        verify_reader_before(&mut file, *evidence, expected_bytes, deadline, cancellation)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }

    /// Reopens a previously verified immutable object for indexed reads.
    ///
    /// Generation recovery must first hash the complete object. Subsequent indexed reads require
    /// its exact content-addressed name, byte length, regular-file type, and read-only mode; every
    /// selected frame remains independently authenticated by a digest from a freshly verified
    /// content-addressed index page.
    pub(crate) fn open_sealed_readonly(
        &self,
        evidence: &EvidenceDigest,
        expected_bytes: u64,
        max_bytes: u64,
    ) -> Result<std::fs::File, RawEvidenceError> {
        if expected_bytes == 0 || expected_bytes > max_bytes {
            return Err(RawEvidenceError::ReadLimitExceeded);
        }
        let name = evidence_name(*evidence)?;
        let file = self.open_named(&name, max_bytes)?;
        let metadata = file.metadata()?;
        if metadata.len() != expected_bytes || !metadata.permissions().readonly() {
            return Err(RawEvidenceError::VerificationFailed);
        }
        Ok(file)
    }

    fn read_named_verified(
        &self,
        name: &str,
        evidence: EvidenceDigest,
        max_bytes: u64,
        cancellation: &CancellationToken,
    ) -> Result<Vec<u8>, RawEvidenceError> {
        check_cancelled(cancellation)?;
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        let file = self.directory.open_with(name, &options)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || !self.directory.symlink_metadata(name)?.is_file() {
            return Err(RawEvidenceError::NotRegularFile);
        }
        if metadata.len() > max_bytes {
            return Err(RawEvidenceError::ReadLimitExceeded);
        }
        let mut bytes = Vec::new();
        read_chunks(
            &mut file.into_std().take(max_bytes.saturating_add(1)),
            &mut bytes,
            cancellation,
        )?;
        if u64::try_from(bytes.len()).map_or(true, |length| length > max_bytes) {
            return Err(RawEvidenceError::ReadLimitExceeded);
        }
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != evidence.bytes() {
            return Err(RawEvidenceError::VerificationFailed);
        }
        Ok(bytes)
    }

    fn open_named(&self, name: &str, max_bytes: u64) -> Result<std::fs::File, RawEvidenceError> {
        let mut options = OpenOptions::new();
        options.read(true);
        options.follow(FollowSymlinks::No);
        let file = self.directory.open_with(name, &options)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || !self.directory.symlink_metadata(name)?.is_file() {
            return Err(RawEvidenceError::NotRegularFile);
        }
        if metadata.len() > max_bytes {
            return Err(RawEvidenceError::ReadLimitExceeded);
        }
        Ok(file.into_std())
    }

    #[cfg(test)]
    fn observe_final_link(&self, cancellation: &CancellationToken) {
        if let Some(probe) = &self.publication_probe {
            probe.final_link_published(cancellation);
        }
    }

    #[cfg(not(test))]
    fn observe_final_link(&self, _cancellation: &CancellationToken) {}

    #[cfg(test)]
    fn observe_identical_final(&self) {
        if let Some(probe) = &self.publication_probe {
            probe.record(PublicationCommitEvent::IdenticalFinalObserved);
        }
    }

    #[cfg(not(test))]
    fn observe_identical_final(&self) {}

    #[cfg(test)]
    fn observe_directory_synced(&self) {
        if let Some(probe) = &self.publication_probe {
            probe.record(PublicationCommitEvent::ParentDirectorySynced);
        }
    }

    #[cfg(not(test))]
    fn observe_directory_synced(&self) {}
}

#[cfg(unix)]
pub(crate) fn sync_publication_directory(directory: &Dir) -> Result<(), std::io::Error> {
    use cap_std::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    directory
        .open_with(".", &options)
        .map(cap_std::fs::File::into_std)
        .and_then(|opened| opened.sync_all())
}

#[cfg(all(not(unix), not(windows)))]
pub(crate) fn sync_publication_directory(directory: &Dir) -> Result<(), std::io::Error> {
    directory.try_clone()?.into_std_file().sync_all()
}

#[cfg(windows)]
pub(crate) fn sync_publication_directory(_directory: &Dir) -> Result<(), std::io::Error> {
    // Windows has no safe portable equivalent to fsync for a directory handle.
    Ok(())
}

#[cfg(windows)]
pub(crate) fn sync_new_link_metadata(file: &std::fs::File) -> Result<(), std::io::Error> {
    // Re-flush the writable staging handle after publishing its final hard link.
    file.sync_all()
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PublicationCommitEvent {
    FinalLinkPublished,
    IdenticalFinalObserved,
    ParentDirectorySynced,
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct PublicationCommitTestProbe {
    cancel_after_link: AtomicBool,
    events: Mutex<Vec<PublicationCommitEvent>>,
}

#[cfg(test)]
impl PublicationCommitTestProbe {
    fn cancel_after_first_link() -> Self {
        Self {
            cancel_after_link: AtomicBool::new(true),
            events: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn final_link_published(&self, cancellation: &CancellationToken) {
        self.record(PublicationCommitEvent::FinalLinkPublished);
        if self.cancel_after_link.swap(false, Ordering::AcqRel) {
            cancellation.cancel();
        }
    }

    pub(crate) fn record(&self, event: PublicationCommitEvent) {
        match self.events.lock() {
            Ok(mut events) => events.push(event),
            Err(poisoned) => poisoned.into_inner().push(event),
        }
    }

    fn events(&self) -> Vec<PublicationCommitEvent> {
        match self.events.lock() {
            Ok(events) => events.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    fn clear_events(&self) {
        match self.events.lock() {
            Ok(mut events) => events.clear(),
            Err(poisoned) => poisoned.into_inner().clear(),
        }
    }
}

fn read_chunks(
    reader: &mut impl Read,
    bytes: &mut Vec<u8>,
    cancellation: &CancellationToken,
) -> Result<(), RawEvidenceError> {
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check_cancelled(cancellation)?;
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(());
        }
        bytes
            .try_reserve(read)
            .map_err(|_| RawEvidenceError::AllocationFailed)?;
        bytes.extend_from_slice(&buffer[..read]);
    }
}

fn verify_reader(
    reader: &mut (impl Read + Seek),
    evidence: EvidenceDigest,
    expected_bytes: u64,
    cancellation: &CancellationToken,
) -> Result<(), RawEvidenceError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 {
        return Err(RawEvidenceError::UnsupportedDigest);
    }
    reader.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check_cancelled(cancellation)?;
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| RawEvidenceError::ReadLimitExceeded)?)
            .ok_or(RawEvidenceError::ReadLimitExceeded)?;
        if observed > expected_bytes {
            return Err(RawEvidenceError::LengthMismatch);
        }
        digest.update(&buffer[..read]);
    }
    let actual: [u8; 32] = digest.finalize().into();
    if observed != expected_bytes || actual != evidence.bytes() {
        return Err(RawEvidenceError::VerificationFailed);
    }
    Ok(())
}

fn verify_reader_before(
    reader: &mut (impl Read + Seek),
    evidence: EvidenceDigest,
    expected_bytes: u64,
    deadline: Timestamp,
    cancellation: &CancellationToken,
) -> Result<(), RawEvidenceError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 {
        return Err(RawEvidenceError::UnsupportedDigest);
    }
    reader.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut observed = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        check_deadline(cancellation, deadline)?;
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        observed = observed
            .checked_add(u64::try_from(read).map_err(|_| RawEvidenceError::ReadLimitExceeded)?)
            .ok_or(RawEvidenceError::ReadLimitExceeded)?;
        if observed > expected_bytes {
            return Err(RawEvidenceError::LengthMismatch);
        }
        digest.update(&buffer[..read]);
    }
    let actual: [u8; 32] = digest.finalize().into();
    if observed != expected_bytes || actual != evidence.bytes() {
        return Err(RawEvidenceError::VerificationFailed);
    }
    Ok(())
}

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), RawEvidenceError> {
    if cancellation.is_cancelled() {
        Err(RawEvidenceError::Cancelled)
    } else {
        Ok(())
    }
}

fn check_deadline(
    cancellation: &CancellationToken,
    deadline: Timestamp,
) -> Result<(), RawEvidenceError> {
    check_cancelled(cancellation)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RawEvidenceError::ClockUnavailable)?;
    let seconds = i64::try_from(now.as_secs()).map_err(|_| RawEvidenceError::ClockUnavailable)?;
    let nanos = seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i64::from(now.subsec_nanos())))
        .ok_or(RawEvidenceError::ClockUnavailable)?;
    if nanos >= deadline.unix_nanos() {
        Err(RawEvidenceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn evidence_name(evidence: EvidenceDigest) -> Result<String, RawEvidenceError> {
    if evidence.algorithm() != DigestAlgorithm::Sha256 {
        return Err(RawEvidenceError::UnsupportedDigest);
    }
    let mut name = String::with_capacity(64 + ".raw".len());
    for byte in evidence.bytes() {
        write!(&mut name, "{byte:02x}").map_err(|_| RawEvidenceError::NameFormatting)?;
    }
    name.push_str(".raw");
    Ok(name)
}

struct StagingCleanup<'a> {
    directory: &'a Dir,
    name: &'a str,
}

impl Drop for StagingCleanup<'_> {
    fn drop(&mut self) {
        let _ignored = self.directory.remove_file(self.name);
    }
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

fn seal_readonly(file: &std::fs::File) -> Result<(), std::io::Error> {
    let mut permissions = file.metadata()?.permissions();
    permissions.set_readonly(true);
    file.set_permissions(permissions)
}

/// Immutable raw-evidence persistence failure.
#[derive(Debug, Error)]
pub enum RawEvidenceError {
    /// Filesystem I/O failed.
    #[error("raw evidence I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Only SHA-256 identities are admitted for this store version.
    #[error("raw evidence digest algorithm is unsupported")]
    UnsupportedDigest,
    /// An existing content address did not contain the expected exact bytes.
    #[error("raw evidence content-address conflict")]
    ContentConflict,
    /// A content digest or reread check failed.
    #[error("raw evidence verification failed")]
    VerificationFailed,
    /// The current evidence object exceeds the caller's memory bound.
    #[error("raw evidence exceeds its read bound")]
    ReadLimitExceeded,
    /// A streamed response exceeded its configured write bound.
    #[error("raw evidence exceeds its streamed write bound")]
    WriteLimitExceeded,
    /// Provider length metadata and streamed response bytes disagree.
    #[error("raw evidence streamed length mismatches provider metadata")]
    LengthMismatch,
    /// The content-addressed path was not a regular file.
    #[error("raw evidence path is not a regular file")]
    NotRegularFile,
    /// Formatting an in-memory content name failed.
    #[error("raw evidence name formatting failed")]
    NameFormatting,
    /// The caller cancelled before immutable publication completed.
    #[error("raw evidence operation was cancelled")]
    Cancelled,
    /// The caller's absolute operation deadline elapsed.
    #[error("raw evidence operation exceeded its deadline")]
    DeadlineExceeded,
    /// The wall clock could not be represented for deadline enforcement.
    #[error("raw evidence system clock is unavailable")]
    ClockUnavailable,
    /// A bounded allocation could not be reserved.
    #[error("raw evidence allocation failed")]
    AllocationFailed,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use cap_std::{ambient_authority, fs::Dir};
    use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};

    use super::{PublicationCommitEvent, PublicationCommitTestProbe, RawEvidenceStore};
    use crate::{SecHttpValidators, SecRepresentationLimits, SecRepresentationRegistry};

    #[test]
    fn release_blocking_remediation_publication_commit_is_durable_for_both_stores()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let raw_path = temporary.path().join("raw");
        let representations_path = temporary.path().join("representations");
        std::fs::create_dir(&raw_path)?;
        std::fs::create_dir(&representations_path)?;

        let raw_probe = Arc::new(PublicationCommitTestProbe::cancel_after_first_link());
        let raw_store = RawEvidenceStore::new_with_publication_probe(
            Dir::open_ambient_dir(&raw_path, ambient_authority())?,
            Arc::clone(&raw_probe),
        );
        let raw_cancellation = tokio_util::sync::CancellationToken::new();
        let raw_evidence =
            raw_store.persist_cancellable(b"durable SEC publication", &raw_cancellation)?;
        assert!(raw_cancellation.is_cancelled());
        assert_eq!(
            raw_probe.events(),
            vec![
                PublicationCommitEvent::FinalLinkPublished,
                PublicationCommitEvent::ParentDirectorySynced,
            ]
        );

        raw_probe.clear_events();
        assert_eq!(raw_store.persist(b"durable SEC publication")?, raw_evidence);
        assert_eq!(
            raw_probe.events(),
            vec![
                PublicationCommitEvent::IdenticalFinalObserved,
                PublicationCommitEvent::ParentDirectorySynced,
            ]
        );

        let representation_probe = Arc::new(PublicationCommitTestProbe::cancel_after_first_link());
        let registry = SecRepresentationRegistry::open_with_publication_probe(
            Dir::open_ambient_dir(&representations_path, ambient_authority())?,
            SecRepresentationLimits::production_defaults(),
            Arc::clone(&representation_probe),
        )?;
        let representation_cancellation = tokio_util::sync::CancellationToken::new();
        let locator = "https://data.sec.gov/submissions/CIK0000320193.json";
        let representation = registry.record_success_cancellable(
            locator,
            EvidenceDigest::new(DigestAlgorithm::Sha256, [7; 32]),
            24,
            SecHttpValidators::default(),
            &representation_cancellation,
        )?;
        assert!(representation_cancellation.is_cancelled());
        assert_eq!(representation.retrieval_revision(), 1);
        assert_eq!(
            representation_probe.events(),
            vec![
                PublicationCommitEvent::FinalLinkPublished,
                PublicationCommitEvent::ParentDirectorySynced,
            ]
        );
        drop(registry);

        let restarted = SecRepresentationRegistry::open(
            Dir::open_ambient_dir(&representations_path, ambient_authority())?,
            SecRepresentationLimits::production_defaults(),
        )?;
        let restored = restarted.record_not_modified(locator, SecHttpValidators::default())?;
        assert_eq!(restored, representation);
        Ok(())
    }
}
