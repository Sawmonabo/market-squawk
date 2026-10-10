//! Stopped original repository and audit custody for the workspace SourceData component.
use super::*;
use cap_fs_ext::{FollowSymlinks, MetadataExt as _, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_adapter_paper::{
    PaperCheckpointBackup, PaperCheckpointBackupLease, PaperCheckpointRepository,
};
use market_squawk_platform::{ArtifactRoot, ControlRoot};
use sha2::{Digest as _, Sha256};
use std::{
    fs::File,
    io::{Read as _, Seek as _, SeekFrom, Write},
    num::{NonZeroU64, NonZeroUsize},
    sync::Mutex as FileMutex,
};
use tokio::sync::OwnedMutexGuard;

/// Least-authority owner; cannot start, stop, or submit execution.
pub(crate) struct PaperStoppedBackupAuthority {
    controller: Arc<PaperController>,
    research: Arc<crate::ResearchService>,
    checkpoint_root: ArtifactRoot,
    control_root: ControlRoot,
    maximum_checkpoint_bytes: NonZeroUsize,
    maximum_audit_bytes: NonZeroU64,
}
impl fmt::Debug for PaperStoppedBackupAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PaperStoppedBackupAuthority([ORIGINAL STOPPED OWNER])")
    }
}
impl PaperApplicationServices {
    pub(crate) fn stopped_backup_authority(
        &self,
        research: Arc<crate::ResearchService>,
        checkpoint_root: ArtifactRoot,
        control_root: ControlRoot,
        maximum_checkpoint_bytes: NonZeroUsize,
        maximum_audit_bytes: NonZeroU64,
    ) -> Arc<PaperStoppedBackupAuthority> {
        Arc::new(PaperStoppedBackupAuthority {
            controller: Arc::clone(&self.controller),
            research,
            checkpoint_root,
            control_root,
            maximum_checkpoint_bytes,
            maximum_audit_bytes,
        })
    }
}
impl PaperStoppedBackupAuthority {
    pub(crate) async fn retain(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PaperStoppedBackupLease, ServiceError> {
        check_backup(deadline, cancellation)?;
        let owner = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(ServiceError::Cancelled),
            _ = tokio::time::sleep_until(deadline.into()) => return Err(ServiceError::DeadlineExceeded),
            owner = Arc::clone(&self.controller.owner_gate).lock_owned() => owner,
        };
        {
            let state = bounded_lock(&self.controller.state, deadline, cancellation).await?;
            if !matches!(&*state, PaperState::Stopped { .. }) {
                return Err(ServiceError::Unavailable);
            }
        }
        let owner = Arc::new(owner);
        let checkpoint_root = self.checkpoint_root.clone();
        let control_root = self.control_root.clone();
        let maximum_checkpoint_bytes = self.maximum_checkpoint_bytes;
        let maximum_audit_bytes = self.maximum_audit_bytes;
        let retained = self
            .research
            .run_owned_research_io(deadline, cancellation, move |cancel| {
                check_backup(deadline, &cancel)?;
                let checkpoint = PaperCheckpointRepository::retain_stopped_backup(
                    &checkpoint_root,
                    maximum_checkpoint_bytes,
                )
                .map_err(|_| ServiceError::Unavailable)?;
                let directory = control_root
                    .try_clone_directory()
                    .map_err(|_| ServiceError::Unavailable)?;
                let mut audits = Vec::new();
                let mut remaining = maximum_audit_bytes.get();
                for kind in PaperAuditBackupKind::ALL {
                    if let Some(audit) = PaperAuditBackupFile::retain(
                        &directory, kind, remaining, deadline, &cancel,
                    )? {
                        remaining = remaining
                            .checked_sub(audit.bytes())
                            .ok_or(ServiceError::ResourceExhausted)?;
                        audits.push(audit);
                    }
                }
                // Once paper has published events, both mandatory stream owners have existed.
                // A missing stream cannot be represented as an empty historical audit.
                if checkpoint
                    .checkpoint()
                    .is_some_and(|value| value.sequence() > 0)
                    && audits.len() != PaperAuditBackupKind::ALL.len()
                {
                    return Err(ServiceError::InvalidResult);
                }
                check_backup(deadline, &cancel)?;
                Ok((owner, checkpoint, audits))
            })
            .await
            .map_err(|_| ServiceError::Unavailable)??;
        check_backup(deadline, cancellation)?;
        Ok(PaperStoppedBackupLease {
            _owner: retained.0,
            checkpoint: Arc::new(retained.1),
            audits: Arc::new(retained.2),
            control_root: self.control_root.clone(),
            research: Arc::clone(&self.research),
        })
    }
}

/// Holds the real start fence, original repository lock and each existing audit writer lock.
pub(crate) struct PaperStoppedBackupLease {
    _owner: Arc<OwnedMutexGuard<()>>,
    checkpoint: Arc<PaperCheckpointBackupLease>,
    audits: Arc<Vec<PaperAuditBackupFile>>,
    control_root: ControlRoot,
    research: Arc<crate::ResearchService>,
}
impl fmt::Debug for PaperStoppedBackupLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PaperStoppedBackupLease([STOPPED REPOSITORY AND ORIGINAL AUDITS LOCKED])")
    }
}
impl PaperStoppedBackupLease {
    /// Moves only original lock custody into supervised I/O, without retaining its worker owner.
    pub(crate) fn stream_custody(&self) -> PaperStoppedBackupStreamCustody {
        PaperStoppedBackupStreamCustody {
            _owner: Arc::clone(&self._owner),
            checkpoint: Arc::clone(&self.checkpoint),
            audits: Arc::clone(&self.audits),
        }
    }
    pub(crate) fn checkpoint(&self) -> Option<&PaperCheckpointBackup> {
        self.checkpoint.checkpoint()
    }
    pub(crate) fn audits(&self) -> &[PaperAuditBackupFile] {
        &self.audits
    }
    pub(crate) async fn revalidate(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        check_backup(deadline, cancellation)?;
        let owner = Arc::clone(&self._owner);
        let checkpoint = Arc::clone(&self.checkpoint);
        let audits = Arc::clone(&self.audits);
        let root = self.control_root.clone();
        self.research
            .run_owned_research_io(deadline, cancellation, move |cancel| {
                let _owner = owner;
                check_backup(deadline, &cancel)?;
                checkpoint
                    .revalidate()
                    .map_err(|_| ServiceError::Unavailable)?;
                let directory = root
                    .try_clone_directory()
                    .map_err(|_| ServiceError::Unavailable)?;
                for kind in PaperAuditBackupKind::ALL {
                    if let Some(audit) = audits.iter().find(|audit| audit.kind == kind) {
                        audit.verify_path(&directory)?;
                        audit.write_to(&mut std::io::sink(), deadline, &cancel)?;
                        audit.verify_path(&directory)?;
                    } else {
                        match directory.symlink_metadata(kind.file_name()) {
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            _ => return Err(ServiceError::Unavailable),
                        }
                    }
                }
                check_backup(deadline, &cancel)
            })
            .await
            .map_err(|_| ServiceError::Unavailable)?
    }
}

/// Cloneable custody of the original stopped run for the existing supervised stream worker.
/// No research service, control owner or worker handle is retained, so cancellation cannot
/// release the financial locks early or form a research-worker ownership cycle.
#[derive(Clone)]
pub(crate) struct PaperStoppedBackupStreamCustody {
    _owner: Arc<OwnedMutexGuard<()>>,
    checkpoint: Arc<PaperCheckpointBackupLease>,
    audits: Arc<Vec<PaperAuditBackupFile>>,
}
impl PaperStoppedBackupStreamCustody {
    pub(crate) fn checkpoint(&self) -> Option<&PaperCheckpointBackup> {
        self.checkpoint.checkpoint()
    }
    pub(crate) fn audits(&self) -> &[PaperAuditBackupFile] {
        &self.audits
    }
}

/// Closed file identities; no archive-authored path is admitted by the producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PaperAuditBackupKind {
    Execution,
    State,
}
impl PaperAuditBackupKind {
    const ALL: [Self; 2] = [Self::Execution, Self::State];
    pub(crate) const fn file_name(self) -> &'static str {
        match self {
            Self::Execution => "paper-execution-audit-v2.jsonl",
            Self::State => "paper-state-audit-v1.jsonl",
        }
    }
}

/// Original append-only bytes remain locked and are copied in bounded chunks, never rewritten.
pub(crate) struct PaperAuditBackupFile {
    kind: PaperAuditBackupKind,
    file: FileMutex<File>,
    directory: Dir,
    identity: (u64, u64),
    bytes: u64,
    digest: [u8; 32],
}
impl fmt::Debug for PaperAuditBackupFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PaperAuditBackupFile")
            .field("kind", &self.kind)
            .field("bytes", &self.bytes)
            .finish()
    }
}
impl PaperAuditBackupFile {
    fn retain(
        directory: &Dir,
        kind: PaperAuditBackupKind,
        maximum_bytes: u64,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>, ServiceError> {
        check_backup(deadline, cancellation)?;
        match directory.symlink_metadata(kind.file_name()) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Ok(metadata) if metadata.is_file() => {}
            _ => return Err(ServiceError::Unavailable),
        }
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let file = directory
            .open_with(kind.file_name(), &options)
            .map_err(|_| ServiceError::Unavailable)?;
        let metadata = file.metadata().map_err(|_| ServiceError::Unavailable)?;
        if !metadata.is_file() {
            return Err(ServiceError::Unavailable);
        }
        if metadata.len() > maximum_bytes {
            return Err(ServiceError::ResourceExhausted);
        }
        let mut file = file.into_std();
        fs2::FileExt::try_lock_exclusive(&file).map_err(|_| ServiceError::Unavailable)?;
        let digest = copy_audit(
            &mut file,
            metadata.len(),
            &mut std::io::sink(),
            deadline,
            cancellation,
        )?;
        let audit = Self {
            kind,
            file: FileMutex::new(file),
            directory: directory
                .try_clone()
                .map_err(|_| ServiceError::Unavailable)?,
            identity: (metadata.dev(), metadata.ino()),
            bytes: metadata.len(),
            digest,
        };
        audit.verify_path(directory)?;
        Ok(Some(audit))
    }
    pub(crate) const fn kind(&self) -> PaperAuditBackupKind {
        self.kind
    }
    pub(crate) const fn file_name(&self) -> &'static str {
        self.kind.file_name()
    }
    pub(crate) const fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(crate) const fn digest(&self) -> [u8; 32] {
        self.digest
    }
    fn verify_path(&self, directory: &Dir) -> Result<(), ServiceError> {
        let metadata = directory
            .symlink_metadata(self.file_name())
            .map_err(|_| ServiceError::Unavailable)?;
        if !metadata.is_file()
            || (metadata.dev(), metadata.ino()) != self.identity
            || metadata.len() != self.bytes
        {
            return Err(ServiceError::Unavailable);
        }
        Ok(())
    }
    pub(crate) fn write_to(
        &self,
        writer: &mut dyn Write,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), ServiceError> {
        check_backup(deadline, cancellation)?;
        self.verify_path(&self.directory)?;
        let mut file = self
            .file
            .try_lock()
            .map_err(|_| ServiceError::Unavailable)?;
        if copy_audit(&mut file, self.bytes, writer, deadline, cancellation)? != self.digest {
            return Err(ServiceError::InvalidResult);
        }
        self.verify_path(&self.directory)?;
        check_backup(deadline, cancellation)
    }
}
fn copy_audit(
    file: &mut File,
    length: u64,
    writer: &mut dyn Write,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<[u8; 32], ServiceError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| ServiceError::Unavailable)?;
    let mut remaining = length;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    while remaining != 0 {
        check_backup(deadline, cancellation)?;
        let capacity = usize::try_from(remaining.min(buffer.len() as u64))
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let read = file
            .read(&mut buffer[..capacity])
            .map_err(|_| ServiceError::Unavailable)?;
        if read == 0 {
            return Err(ServiceError::InvalidResult);
        }
        hash.update(&buffer[..read]);
        writer
            .write_all(&buffer[..read])
            .map_err(|_| ServiceError::Unavailable)?;
        remaining -= read as u64;
    }
    check_backup(deadline, cancellation)?;
    if file
        .read(&mut buffer[..1])
        .map_err(|_| ServiceError::Unavailable)?
        != 0
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(hash.finalize().into())
}
fn check_backup(deadline: Instant, cancellation: &CancellationToken) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}
