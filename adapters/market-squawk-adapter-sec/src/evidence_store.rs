//! Capability-scoped immutable raw-evidence persistence.

use std::fmt::Write as _;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::{Arc, Mutex};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const MAX_RAW_EVIDENCE_READ_BYTES: u64 = 64 * 1024 * 1024;

/// Capability-scoped, content-addressed raw SEC evidence store.
#[derive(Debug)]
pub struct RawEvidenceStore {
    directory: Dir,
    #[cfg(test)]
    publication_probe: Option<Arc<PublicationCommitTestProbe>>,
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

        check_cancelled(cancellation)?;
        match self
            .directory
            .hard_link(&staging_name, &self.directory, &final_name)
        {
            Ok(()) => self.observe_final_link(cancellation),
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

#[cfg(not(unix))]
pub(crate) fn sync_publication_directory(directory: &Dir) -> Result<(), std::io::Error> {
    directory.try_clone()?.into_std_file().sync_all()
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

fn check_cancelled(cancellation: &CancellationToken) -> Result<(), RawEvidenceError> {
    if cancellation.is_cancelled() {
        Err(RawEvidenceError::Cancelled)
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
    /// The content-addressed path was not a regular file.
    #[error("raw evidence path is not a regular file")]
    NotRegularFile,
    /// Formatting an in-memory content name failed.
    #[error("raw evidence name formatting failed")]
    NameFormatting,
    /// The caller cancelled before immutable publication completed.
    #[error("raw evidence operation was cancelled")]
    Cancelled,
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
