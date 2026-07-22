//! Capability-confined immutable inventory and content-addressed publication.

use std::io::{Read as _, Write as _};
use std::path::Path;
use std::sync::Mutex;

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_data::Sha256Digest;

use super::ExperimentError;
use super::model::{
    BacktestArtifact, ExperimentLimits, TrialCompletion, TrialCompletionInput, TrialFailure,
    TrialId, TrialRecord, TrialSpec, TrialStatus,
};
use super::wire::{
    decode_reservation, decode_terminal, digest_bytes, encode_hex, encode_reservation,
    encode_terminal,
};

const NAMESPACE: &str = "backtesting/v1";
const RESERVATIONS: &str = "backtesting/v1/reservations";
const TERMINALS: &str = "backtesting/v1/terminals";
const ARTIFACTS: &str = "backtesting/v1/artifacts/sha256";

/// Non-cloneable proof that a trial identity was durably reserved before execution.
#[derive(Debug)]
pub struct TrialReservation {
    spec: TrialSpec,
    record_digest: Sha256Digest,
}

/// Capability-confined immutable experiment inventory and artifact publisher.
#[derive(Debug)]
pub struct ExperimentInventory {
    root: Dir,
    limits: ExperimentLimits,
    writer: Mutex<()>,
}

impl ExperimentInventory {
    /// Initializes or reopens the fixed no-follow inventory namespace.
    pub fn try_new(root: Dir, limits: ExperimentLimits) -> Result<Self, ExperimentError> {
        for path in [
            "backtesting",
            NAMESPACE,
            RESERVATIONS,
            TERMINALS,
            "backtesting/v1/artifacts",
            ARTIFACTS,
        ] {
            ensure_directory(&root, Path::new(path))?;
        }
        Ok(Self {
            root,
            limits,
            writer: Mutex::new(()),
        })
    }

    pub(crate) const fn limits(&self) -> ExperimentLimits {
        self.limits
    }

    /// Durably reserves one identity before execution and never overwrites a prior trial.
    pub fn reserve(&self, spec: TrialSpec) -> Result<TrialReservation, ExperimentError> {
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        if count_records(&self.root, RESERVATIONS, self.limits.max_trials)?
            >= self.limits.max_trials
        {
            return Err(ExperimentError::LimitExceeded);
        }
        let bytes = encode_reservation(&spec)?;
        if bytes.len() > self.limits.max_record_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        let path = reservation_path(spec.id());
        publish_immutable(&self.root, &path, &bytes, ExistingPolicy::Reject)?;
        Ok(TrialReservation {
            spec,
            record_digest: digest_bytes(&bytes),
        })
    }

    /// Publishes a bounded content-addressed detailed result. Exact retries are idempotent.
    pub fn publish_artifact(&self, bytes: &[u8]) -> Result<BacktestArtifact, ExperimentError> {
        if bytes.is_empty() || bytes.len() > self.limits.max_artifact_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        let digest = digest_bytes(bytes);
        let hex = encode_hex(digest.bytes());
        let prefix = hex.get(..2).ok_or(ExperimentError::Encoding)?;
        let parent = format!("{ARTIFACTS}/{prefix}");
        ensure_directory(&self.root, Path::new(&parent))?;
        let reference = format!("{parent}/{hex}.json");
        publish_immutable(
            &self.root,
            Path::new(&reference),
            bytes,
            ExistingPolicy::AcceptExact,
        )?;
        Ok(BacktestArtifact {
            reference: reference.into_boxed_str(),
            digest,
            byte_count: u64::try_from(bytes.len()).map_err(|_| ExperimentError::LimitExceeded)?,
        })
    }

    /// Commits the sole immutable successful terminal for a reserved trial.
    pub fn complete(
        &self,
        reservation: TrialReservation,
        input: TrialCompletionInput,
    ) -> Result<TrialRecord, ExperimentError> {
        let completion = TrialCompletion::try_new(input, self.limits)?;
        self.commit_terminal(reservation, TrialStatus::Completed(completion))
    }

    /// Commits the sole immutable failed terminal for a reserved trial.
    pub fn fail(
        &self,
        reservation: TrialReservation,
        failure: TrialFailure,
    ) -> Result<TrialRecord, ExperimentError> {
        self.commit_terminal(reservation, TrialStatus::Failed(failure))
    }

    /// Loads one bounded reservation and its optional immutable terminal.
    pub fn trial(&self, id: TrialId) -> Result<TrialRecord, ExperimentError> {
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        let reservation = read_bounded(
            &self.root,
            &reservation_path(id),
            self.limits.max_record_bytes,
        )?;
        let spec = decode_reservation(&reservation)?;
        if spec.id() != id {
            return Err(ExperimentError::CorruptRecord);
        }
        let status = match read_optional_bounded(
            &self.root,
            &terminal_path(id),
            self.limits.max_record_bytes,
        )? {
            Some(bytes) => decode_terminal(&bytes, id, self.limits)?,
            None => TrialStatus::Reserved,
        };
        Ok(TrialRecord { spec, status })
    }

    fn commit_terminal(
        &self,
        reservation: TrialReservation,
        status: TrialStatus,
    ) -> Result<TrialRecord, ExperimentError> {
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        let reservation_bytes = read_bounded(
            &self.root,
            &reservation_path(reservation.spec.id()),
            self.limits.max_record_bytes,
        )?;
        if digest_bytes(&reservation_bytes) != reservation.record_digest
            || decode_reservation(&reservation_bytes)? != reservation.spec
        {
            return Err(ExperimentError::CorruptRecord);
        }
        let terminal = encode_terminal(reservation.spec.id(), &status)?;
        if terminal.len() > self.limits.max_record_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        publish_immutable(
            &self.root,
            &terminal_path(reservation.spec.id()),
            &terminal,
            ExistingPolicy::Reject,
        )?;
        Ok(TrialRecord {
            spec: reservation.spec,
            status,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExistingPolicy {
    Reject,
    AcceptExact,
}

fn publish_immutable(
    root: &Dir,
    final_path: &Path,
    bytes: &[u8],
    policy: ExistingPolicy,
) -> Result<(), ExperimentError> {
    if let Some(existing) = read_optional_bounded(root, final_path, bytes.len().max(1))? {
        return if policy == ExistingPolicy::AcceptExact && existing == bytes {
            Ok(())
        } else {
            Err(ExperimentError::TrialAlreadyExists)
        };
    }
    let file_name = final_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(ExperimentError::Encoding)?;
    let stage_path = final_path.with_file_name(format!(".{file_name}.pending"));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let mut stage = match root.open_with(&stage_path, &options) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_bounded(root, &stage_path, bytes.len().max(1))?;
            if existing != bytes {
                return Err(ExperimentError::CorruptRecord);
            }
            root.open_with(&stage_path, &read_options())?
        }
        Err(error) => return Err(ExperimentError::Io(error)),
    };
    if stage.metadata()?.len() == 0 {
        stage.write_all(bytes)?;
        stage.sync_all()?;
    }
    drop(stage);
    match root.hard_link(&stage_path, root, final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_bounded(root, final_path, bytes.len().max(1))?;
            if existing != bytes || policy == ExistingPolicy::Reject {
                return Err(ExperimentError::TrialAlreadyExists);
            }
        }
        Err(error) => return Err(ExperimentError::Io(error)),
    }
    synchronize_parent(root, final_path)?;
    match root.remove_file(&stage_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(ExperimentError::Io(error)),
    }
    synchronize_parent(root, final_path)?;
    if read_bounded(root, final_path, bytes.len().max(1))? != bytes {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(())
}

fn ensure_directory(root: &Dir, path: &Path) -> Result<(), ExperimentError> {
    let mut current = root.try_clone()?;
    for component in path.components() {
        let name = match component {
            std::path::Component::Normal(name) => name,
            _ => return Err(ExperimentError::Encoding),
        };
        current = match current.open_dir_nofollow(name) {
            Ok(directory) => directory,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                current.create_dir(name)?;
                current.open_dir_nofollow(name)?
            }
            Err(error) => return Err(ExperimentError::Io(error)),
        };
    }
    Ok(())
}

fn count_records(root: &Dir, path: &str, limit: usize) -> Result<usize, ExperimentError> {
    let directory = root.open_dir_nofollow(path)?;
    let mut count = 0_usize;
    for entry in directory.entries()? {
        let entry = entry?;
        if entry.file_name().to_string_lossy().ends_with(".json") {
            count = count.checked_add(1).ok_or(ExperimentError::LimitExceeded)?;
            if count > limit {
                return Err(ExperimentError::LimitExceeded);
            }
        }
    }
    Ok(count)
}

fn read_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options
}

fn read_bounded(root: &Dir, path: &Path, maximum: usize) -> Result<Vec<u8>, ExperimentError> {
    let mut file = root.open_with(path, &read_options())?;
    let metadata = file.metadata()?;
    let size = usize::try_from(metadata.len()).map_err(|_| ExperimentError::LimitExceeded)?;
    if !metadata.is_file() || size == 0 || size > maximum {
        return Err(ExperimentError::CorruptRecord);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| ExperimentError::LimitExceeded)?;
    bytes.resize(size, 0);
    file.read_exact(&mut bytes)?;
    let mut trailing = [0_u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(bytes)
}

fn read_optional_bounded(
    root: &Dir,
    path: &Path,
    maximum: usize,
) -> Result<Option<Vec<u8>>, ExperimentError> {
    match read_bounded(root, path, maximum) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(ExperimentError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn synchronize_parent(root: &Dir, path: &Path) -> Result<(), ExperimentError> {
    let parent = path.parent().ok_or(ExperimentError::Encoding)?;
    root.open_dir_nofollow(parent)?.into_std_file().sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn synchronize_parent(_root: &Dir, _path: &Path) -> Result<(), ExperimentError> {
    Err(ExperimentError::Unavailable)
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) {
    use cap_std::fs::OpenOptionsExt as _;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn configure_private_creation(_options: &mut OpenOptions) {}

fn reservation_path(id: TrialId) -> std::path::PathBuf {
    Path::new(RESERVATIONS).join(format!("{}.json", encode_hex(id.digest().bytes())))
}

fn terminal_path(id: TrialId) -> std::path::PathBuf {
    Path::new(TERMINALS).join(format!("{}.json", encode_hex(id.digest().bytes())))
}
