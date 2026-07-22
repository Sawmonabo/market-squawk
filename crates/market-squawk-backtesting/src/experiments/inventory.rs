//! Capability-confined immutable inventory and content-addressed publication.

use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use market_squawk_data::Sha256Digest;
use market_squawk_domain::Timestamp;
use serde::{Deserialize, Serialize};

use super::ExperimentError;
use super::cohort::{
    BacktestCohortEvaluation, BacktestCohortEvaluationId, decode_evaluation, encode_evaluation,
};
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
const ATTEMPTS: &str = "backtesting/v1/attempts";
const COHORTS: &str = "backtesting/v1/cohorts";
const ARTIFACTS: &str = "backtesting/v1/artifacts/sha256";
const DEFAULT_LEASE_NANOS: i64 = 60 * 60 * 1_000_000_000;
const MAX_ATTEMPTS_PER_TRIAL: usize = 1_024;
const MAX_STAGE_NAME_ATTEMPTS: usize = 32;

static NEXT_STAGE_NONCE: AtomicU64 = AtomicU64::new(1);

/// Non-cloneable proof that a trial identity was durably reserved before execution.
#[derive(Debug)]
pub struct TrialReservation {
    spec: TrialSpec,
    record_digest: Sha256Digest,
    attempt: u64,
    attempt_digest: Sha256Digest,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AttemptRecord {
    schema_version: u16,
    trial_id: String,
    reservation_digest: String,
    attempt: u64,
    acquired_at: i64,
    expires_at: i64,
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
            ATTEMPTS,
            COHORTS,
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

    /// Durably reserves one identity and acquires a recoverable process-independent attempt lease.
    pub(crate) fn reserve(&self, spec: TrialSpec) -> Result<TrialReservation, ExperimentError> {
        self.reserve_at(spec, current_timestamp()?, DEFAULT_LEASE_NANOS)
    }

    pub(crate) fn reserve_at(
        &self,
        spec: TrialSpec,
        acquired_at: Timestamp,
        lease_nanos: i64,
    ) -> Result<TrialReservation, ExperimentError> {
        if lease_nanos <= 0 {
            return Err(ExperimentError::InvalidLease);
        }
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        let bytes = encode_reservation(&spec)?;
        if bytes.len() > self.limits.max_record_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        let path = reservation_path(spec.id());
        if read_optional_bounded(&self.root, &path, self.limits.max_record_bytes)?.is_none()
            && count_records(&self.root, RESERVATIONS, self.limits.max_trials)?
                >= self.limits.max_trials
        {
            return Err(ExperimentError::LimitExceeded);
        }
        publish_immutable(&self.root, &path, &bytes, ExistingPolicy::AcceptExact)?;
        if read_optional_bounded(
            &self.root,
            &legacy_terminal_path(spec.id()),
            self.limits.max_record_bytes,
        )?
        .is_some()
        {
            return Err(ExperimentError::TrialAlreadyExists);
        }
        self.acquire_attempt(spec, digest_bytes(&bytes), acquired_at, lease_nanos)
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

    pub(crate) fn publish_cohort_evaluation(
        &self,
        evaluation: &BacktestCohortEvaluation,
    ) -> Result<(), ExperimentError> {
        let bytes = encode_evaluation(evaluation)?;
        if bytes.len() > self.limits.max_record_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        publish_immutable(
            &self.root,
            &cohort_path(evaluation.id()),
            &bytes,
            ExistingPolicy::AcceptExact,
        )
    }

    /// Loads and revalidates one append-only cohort decision record.
    pub fn cohort_evaluation(
        &self,
        id: BacktestCohortEvaluationId,
    ) -> Result<BacktestCohortEvaluation, ExperimentError> {
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        let bytes = read_bounded(&self.root, &cohort_path(id), self.limits.max_record_bytes)?;
        decode_evaluation(&bytes, id)
    }

    /// Commits the sole immutable successful terminal for a reserved trial.
    pub(crate) fn complete(
        &self,
        reservation: TrialReservation,
        input: TrialCompletionInput,
    ) -> Result<TrialRecord, ExperimentError> {
        let completion = TrialCompletion::try_new(input, self.limits)?;
        self.commit_terminal(reservation, TrialStatus::Completed(completion))
    }

    /// Commits the sole immutable failed terminal for a reserved trial.
    pub(crate) fn fail(
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
        let legacy = read_optional_bounded(
            &self.root,
            &legacy_terminal_path(id),
            self.limits.max_record_bytes,
        )?;
        let status = if let Some(bytes) = legacy {
            decode_terminal(&bytes, id, self.limits)?
        } else if let Some(attempt) = latest_attempt(&self.root, id, self.limits.max_record_bytes)?
        {
            match read_optional_bounded(
                &self.root,
                &attempt_terminal_path(id, attempt.attempt),
                self.limits.max_record_bytes,
            )? {
                Some(bytes) => decode_terminal(&bytes, id, self.limits)?,
                None => TrialStatus::Reserved,
            }
        } else {
            TrialStatus::Reserved
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
        let completed_at = current_timestamp()?;
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
        let attempt_bytes = read_bounded(
            &self.root,
            &attempt_path(reservation.spec.id(), reservation.attempt),
            self.limits.max_record_bytes,
        )?;
        let attempt = decode_attempt(&attempt_bytes, reservation.spec.id())?;
        if digest_bytes(&attempt_bytes) != reservation.attempt_digest
            || attempt.attempt != reservation.attempt
            || attempt.reservation_digest != encode_hex(reservation.record_digest.bytes())
            || latest_attempt(
                &self.root,
                reservation.spec.id(),
                self.limits.max_record_bytes,
            )?
            .map(|value| value.attempt)
                != Some(reservation.attempt)
            || completed_at.unix_nanos() < attempt.acquired_at
            || completed_at.unix_nanos() >= attempt.expires_at
        {
            return Err(ExperimentError::ReservationLeaseLost);
        }
        let terminal = encode_terminal(reservation.spec.id(), &status)?;
        if terminal.len() > self.limits.max_record_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        let terminal_parent = terminal_attempt_parent(reservation.spec.id());
        ensure_directory(&self.root, &terminal_parent)?;
        publish_immutable(
            &self.root,
            &attempt_terminal_path(reservation.spec.id(), reservation.attempt),
            &terminal,
            ExistingPolicy::Reject,
        )?;
        if latest_attempt(
            &self.root,
            reservation.spec.id(),
            self.limits.max_record_bytes,
        )?
        .map(|value| value.attempt)
            != Some(reservation.attempt)
        {
            return Err(ExperimentError::ReservationLeaseLost);
        }
        Ok(TrialRecord {
            spec: reservation.spec,
            status,
        })
    }

    fn acquire_attempt(
        &self,
        spec: TrialSpec,
        record_digest: Sha256Digest,
        acquired_at: Timestamp,
        lease_nanos: i64,
    ) -> Result<TrialReservation, ExperimentError> {
        let trial_id = spec.id();
        let parent = attempt_parent(trial_id);
        ensure_directory(&self.root, &parent)?;
        for _ in 0..3 {
            let latest = latest_attempt(&self.root, trial_id, self.limits.max_record_bytes)?;
            if let Some(attempt) = &latest
                && read_optional_bounded(
                    &self.root,
                    &attempt_terminal_path(trial_id, attempt.attempt),
                    self.limits.max_record_bytes,
                )?
                .is_some()
            {
                return Err(ExperimentError::TrialAlreadyExists);
            }
            if latest
                .as_ref()
                .is_some_and(|attempt| attempt.expires_at > acquired_at.unix_nanos())
            {
                return Err(ExperimentError::TrialInProgress);
            }
            let number = latest.map_or(1, |attempt| attempt.attempt.saturating_add(1));
            if number == 0
                || usize::try_from(number).map_or(true, |value| value > MAX_ATTEMPTS_PER_TRIAL)
            {
                return Err(ExperimentError::LimitExceeded);
            }
            let expires_at = acquired_at
                .unix_nanos()
                .checked_add(lease_nanos)
                .ok_or(ExperimentError::InvalidLease)?;
            let record = AttemptRecord {
                schema_version: 1,
                trial_id: encode_hex(trial_id.digest().bytes()),
                reservation_digest: encode_hex(record_digest.bytes()),
                attempt: number,
                acquired_at: acquired_at.unix_nanos(),
                expires_at,
            };
            let attempt_bytes =
                serde_json::to_vec(&record).map_err(|_| ExperimentError::Encoding)?;
            if attempt_bytes.len() > self.limits.max_record_bytes {
                return Err(ExperimentError::LimitExceeded);
            }
            match publish_immutable(
                &self.root,
                &attempt_path(trial_id, number),
                &attempt_bytes,
                ExistingPolicy::Reject,
            ) {
                Ok(()) => {
                    return Ok(TrialReservation {
                        spec,
                        record_digest,
                        attempt: number,
                        attempt_digest: digest_bytes(&attempt_bytes),
                    });
                }
                Err(ExperimentError::TrialAlreadyExists) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(ExperimentError::TrialInProgress)
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
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let (stage_path, mut stage) = create_unique_stage(root, final_path, bytes, &options)?;
    if let Err(error) = stage.write_all(bytes).and_then(|()| stage.sync_all()) {
        drop(stage);
        remove_stage(root, &stage_path)?;
        return Err(ExperimentError::Io(error));
    }
    drop(stage);
    match root.hard_link(&stage_path, root, final_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_bounded(root, final_path, bytes.len().max(1))?;
            if existing != bytes || policy == ExistingPolicy::Reject {
                remove_stage(root, &stage_path)?;
                return Err(ExperimentError::TrialAlreadyExists);
            }
        }
        Err(error) => {
            remove_stage(root, &stage_path)?;
            return Err(ExperimentError::Io(error));
        }
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

fn create_unique_stage(
    root: &Dir,
    final_path: &Path,
    bytes: &[u8],
    options: &OpenOptions,
) -> Result<(PathBuf, cap_std::fs::File), ExperimentError> {
    let file_name = final_path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or(ExperimentError::Encoding)?;
    let digest = encode_hex(digest_bytes(bytes).bytes());
    for _ in 0..MAX_STAGE_NAME_ATTEMPTS {
        let nonce = NEXT_STAGE_NONCE.fetch_add(1, Ordering::Relaxed);
        let stage_path = final_path.with_file_name(format!(
            ".{file_name}.{}.{nonce}.{digest}.pending",
            std::process::id()
        ));
        match root.open_with(&stage_path, options) {
            Ok(file) => return Ok((stage_path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(ExperimentError::Io(error)),
        }
    }
    Err(ExperimentError::Unavailable)
}

fn remove_stage(root: &Dir, stage_path: &Path) -> Result<(), ExperimentError> {
    match root.remove_file(stage_path) {
        Ok(()) => synchronize_parent(root, stage_path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ExperimentError::Io(error)),
    }
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

fn legacy_terminal_path(id: TrialId) -> std::path::PathBuf {
    Path::new(TERMINALS).join(format!("{}.json", encode_hex(id.digest().bytes())))
}

fn terminal_attempt_parent(id: TrialId) -> PathBuf {
    Path::new(TERMINALS).join(encode_hex(id.digest().bytes()))
}

fn attempt_terminal_path(id: TrialId, attempt: u64) -> PathBuf {
    terminal_attempt_parent(id).join(format!("{attempt:020}.json"))
}

fn cohort_path(id: BacktestCohortEvaluationId) -> PathBuf {
    Path::new(COHORTS).join(format!("{}.json", encode_hex(id.digest().bytes())))
}

fn attempt_parent(id: TrialId) -> PathBuf {
    Path::new(ATTEMPTS).join(encode_hex(id.digest().bytes()))
}

fn attempt_path(id: TrialId, attempt: u64) -> PathBuf {
    attempt_parent(id).join(format!("{attempt:020}.json"))
}

fn latest_attempt(
    root: &Dir,
    id: TrialId,
    maximum: usize,
) -> Result<Option<AttemptRecord>, ExperimentError> {
    let directory = root.open_dir_nofollow(attempt_parent(id))?;
    let mut latest = None::<AttemptRecord>;
    let mut count = 0_usize;
    for entry in directory.entries()? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(ExperimentError::CorruptRecord);
        };
        if !name.ends_with(".json") {
            continue;
        }
        count = count.checked_add(1).ok_or(ExperimentError::LimitExceeded)?;
        if count > MAX_ATTEMPTS_PER_TRIAL {
            return Err(ExperimentError::LimitExceeded);
        }
        let bytes = read_bounded(root, &attempt_parent(id).join(name), maximum)?;
        let record = decode_attempt(&bytes, id)?;
        if latest
            .as_ref()
            .is_none_or(|current| record.attempt > current.attempt)
        {
            latest = Some(record);
        }
    }
    Ok(latest)
}

fn decode_attempt(bytes: &[u8], id: TrialId) -> Result<AttemptRecord, ExperimentError> {
    let record: AttemptRecord =
        serde_json::from_slice(bytes).map_err(|_| ExperimentError::CorruptRecord)?;
    if record.schema_version != 1
        || record.trial_id != encode_hex(id.digest().bytes())
        || record.attempt == 0
        || record.acquired_at >= record.expires_at
    {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(record)
}

fn current_timestamp() -> Result<Timestamp, ExperimentError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ExperimentError::Unavailable)?
        .as_nanos();
    let nanos = i64::try_from(nanos).map_err(|_| ExperimentError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
