//! Capability-confined immutable inventory and content-addressed publication.

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use cap_fs_ext::{DirExt as _, FollowSymlinks, OpenOptionsFollowExt as _};
use cap_std::fs::{Dir, OpenOptions};
use fs2::FileExt as _;
use market_squawk_data::Sha256Digest;
use market_squawk_domain::Timestamp;
use serde::{Deserialize, Serialize};

use super::ExperimentError;
use super::cohort::{
    BacktestCohortEvaluation, BacktestCohortEvaluationId, decode_evaluation, encode_evaluation,
};
use super::model::{
    BacktestArtifact, ExperimentLimits, TrialCompletion, TrialCompletionInput, TrialFailure,
    TrialId, TrialIdentityVersion, TrialRecord, TrialSpec, TrialStatus,
};
use super::wire::{
    decode_reservation, decode_terminal, digest_bytes, encode_hex, encode_reservation,
    encode_terminal,
};

const NAMESPACE: &str = "backtesting/v1";
const RESERVATIONS: &str = "backtesting/v1/reservations";
const TERMINALS: &str = "backtesting/v1/terminals";
const ATTEMPTS: &str = "backtesting/v1/attempts";
const PENDING: &str = "backtesting/v1/pending";
const COHORTS: &str = "backtesting/v1/cohorts";
const ARTIFACTS: &str = "backtesting/v1/artifacts/sha256";
const AUTHORITY_LOCK: &str = "backtesting/v1/inventory.lock";
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
    active_attempts: Arc<Mutex<BTreeSet<(TrialId, u64)>>>,
}

impl Drop for TrialReservation {
    fn drop(&mut self) {
        if let Ok(mut active) = self.active_attempts.lock() {
            active.remove(&(self.spec.id(), self.attempt));
        }
    }
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
    _authority_lock: File,
    active_attempts: Arc<Mutex<BTreeSet<(TrialId, u64)>>>,
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
            PENDING,
            COHORTS,
            "backtesting/v1/artifacts",
            ARTIFACTS,
        ] {
            ensure_directory(&root, Path::new(path))?;
        }
        let authority_lock = acquire_authority_lock(&root)?;
        Ok(Self {
            root,
            limits,
            _authority_lock: authority_lock,
            active_attempts: Arc::new(Mutex::new(BTreeSet::new())),
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
        if spec.identity_version() != TrialIdentityVersion::V3 {
            return Err(ExperimentError::InvalidSpec);
        }
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
            return Err(match spec.identity_version() {
                TrialIdentityVersion::V1 | TrialIdentityVersion::V2 => {
                    ExperimentError::TrialAlreadyExists
                }
                TrialIdentityVersion::V3 => ExperimentError::CorruptRecord,
            });
        }
        self.acquire_attempt(spec, digest_bytes(&bytes), acquired_at, lease_nanos)
    }

    pub(crate) fn prepare_artifact(
        &self,
        bytes: &[u8],
    ) -> Result<BacktestArtifact, ExperimentError> {
        if bytes.is_empty() || bytes.len() > self.limits.max_artifact_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        let digest = digest_bytes(bytes);
        let hex = encode_hex(digest.bytes());
        let prefix = hex.get(..2).ok_or(ExperimentError::Encoding)?;
        Ok(BacktestArtifact {
            reference: format!("{ARTIFACTS}/{prefix}/{hex}.json").into_boxed_str(),
            digest,
            byte_count: u64::try_from(bytes.len()).map_err(|_| ExperimentError::LimitExceeded)?,
        })
    }

    /// Reads one content-addressed backtest report through the inventory's confined artifact root.
    ///
    /// The caller supplies only the immutable digest and exact retained byte count. The inventory
    /// derives the capability-relative location, refuses oversized or mismatched content, and
    /// never returns a filesystem path.
    pub fn read_artifact(
        &self,
        digest: Sha256Digest,
        byte_count: u64,
    ) -> Result<Vec<u8>, ExperimentError> {
        let artifact = BacktestArtifact {
            reference: artifact_reference(digest)?.into_boxed_str(),
            digest,
            byte_count,
        };
        let (path, maximum) = self.validate_artifact_authority(&artifact)?;
        let bytes = read_bounded(&self.root, &path, maximum)?;
        validate_artifact_bytes(&bytes, &artifact)?;
        Ok(bytes)
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
        completion: TrialCompletion,
        artifact_bytes: &[u8],
    ) -> Result<TrialRecord, ExperimentError> {
        if reservation.spec.identity_version() != TrialIdentityVersion::V3 {
            return Err(ExperimentError::CorruptRecord);
        }
        let expected_artifact = self.prepare_artifact(artifact_bytes)?;
        if completion.artifact() != &expected_artifact {
            return Err(ExperimentError::InvalidCompletion);
        }
        let status = TrialStatus::Completed(completion);
        let terminal = encode_terminal(reservation.spec.id(), &status)?;
        if terminal.len() > self.limits.max_record_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        self.validate_active_attempt(&reservation)?;

        let pending_path = pending_artifact_path(reservation.spec.id(), reservation.attempt);
        ensure_directory(
            &self.root,
            pending_path.parent().ok_or(ExperimentError::Encoding)?,
        )?;
        publish_or_confirm_exact(
            &self.root,
            &pending_path,
            artifact_bytes,
            ExistingPolicy::AcceptExact,
        )?;
        self.validate_active_attempt(&reservation)?;

        let terminal_path = attempt_terminal_path(reservation.spec.id(), reservation.attempt);
        ensure_directory(&self.root, &terminal_attempt_parent(reservation.spec.id()))?;
        publish_or_confirm_exact(
            &self.root,
            &terminal_path,
            &terminal,
            ExistingPolicy::Reject,
        )?;

        let artifact_path = Path::new(expected_artifact.reference());
        ensure_directory(
            &self.root,
            artifact_path.parent().ok_or(ExperimentError::Encoding)?,
        )?;
        publish_or_confirm_exact(
            &self.root,
            artifact_path,
            artifact_bytes,
            ExistingPolicy::AcceptExact,
        )?;
        cleanup_optional(&self.root, &pending_path);
        let id = reservation.spec.id();
        drop(_guard);
        self.trial(id)
    }

    pub(crate) fn prepare_completion(
        &self,
        reservation: &TrialReservation,
        input: TrialCompletionInput,
    ) -> Result<TrialCompletion, ExperimentError> {
        if reservation.spec.identity_version() != TrialIdentityVersion::V3 {
            return Err(ExperimentError::CorruptRecord);
        }
        let completion = TrialCompletion::try_new(input, self.limits)?;
        let terminal = encode_terminal(
            reservation.spec.id(),
            &TrialStatus::Completed(completion.clone()),
        )?;
        if terminal.len() > self.limits.max_record_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        Ok(completion)
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
        let reservation_digest = digest_bytes(&reservation);
        let identity_version = spec.identity_version();
        let latest = latest_attempt(
            &self.root,
            id,
            reservation_digest,
            self.limits.max_record_bytes,
        )?;
        let attempt_terminal = authoritative_attempt_terminal(
            &self.root,
            id,
            identity_version,
            latest.as_ref(),
            self.limits.max_record_bytes,
        )?;
        let legacy = read_optional_bounded(
            &self.root,
            &legacy_terminal_path(id),
            self.limits.max_record_bytes,
        )?;
        let status = match identity_version {
            TrialIdentityVersion::V1 | TrialIdentityVersion::V2 => {
                if let Some(bytes) = legacy {
                    let decoded = decode_terminal(
                        &bytes,
                        id,
                        identity_version.terminal_schema_version(),
                        self.limits,
                    )?;
                    self.validate_published_terminal(decoded)?
                } else {
                    TrialStatus::Reserved
                }
            }
            TrialIdentityVersion::V3 => {
                if legacy.is_some() {
                    return Err(ExperimentError::CorruptRecord);
                }
                match (latest, attempt_terminal) {
                    (Some(attempt), Some(bytes)) => {
                        let decoded = decode_terminal(
                            &bytes,
                            id,
                            identity_version.terminal_schema_version(),
                            self.limits,
                        )?;
                        self.reconcile_terminal_artifact(id, attempt.attempt, decoded)?
                    }
                    (Some(_), None) | (None, None) => TrialStatus::Reserved,
                    (None, Some(_)) => return Err(ExperimentError::CorruptRecord),
                }
            }
        };
        Ok(TrialRecord { spec, status })
    }

    fn validate_published_terminal(
        &self,
        decoded: super::wire::DecodedTerminal,
    ) -> Result<TrialStatus, ExperimentError> {
        let TrialStatus::Completed(completion) = &decoded.status else {
            return Ok(decoded.status);
        };
        let (final_path, maximum) = self.validate_artifact_authority(completion.artifact())?;
        let final_bytes = read_optional_bounded(&self.root, &final_path, maximum)?
            .ok_or(ExperimentError::CorruptRecord)?;
        validate_artifact_bytes(&final_bytes, completion.artifact())?;
        Ok(decoded.status)
    }

    fn reconcile_terminal_artifact(
        &self,
        id: TrialId,
        attempt: u64,
        decoded: super::wire::DecodedTerminal,
    ) -> Result<TrialStatus, ExperimentError> {
        let TrialStatus::Completed(completion) = &decoded.status else {
            return Ok(decoded.status);
        };
        let artifact = completion.artifact();
        let (final_path, maximum) = self.validate_artifact_authority(artifact)?;
        let pending_path = pending_artifact_path(id, attempt);
        let final_bytes = read_optional_bounded(&self.root, &final_path, maximum)?;
        let pending_bytes = read_optional_bounded(&self.root, &pending_path, maximum)?;
        if let Some(bytes) = &final_bytes {
            validate_artifact_bytes(bytes, artifact)?;
            if let Some(pending) = &pending_bytes {
                validate_artifact_bytes(pending, artifact)?;
            }
            cleanup_optional(&self.root, &pending_path);
            return Ok(decoded.status);
        }
        let pending = pending_bytes.ok_or(ExperimentError::CorruptRecord)?;
        validate_artifact_bytes(&pending, artifact)?;
        ensure_directory(
            &self.root,
            final_path.parent().ok_or(ExperimentError::Encoding)?,
        )?;
        publish_or_confirm_exact(
            &self.root,
            &final_path,
            &pending,
            ExistingPolicy::AcceptExact,
        )?;
        cleanup_optional(&self.root, &pending_path);
        Ok(decoded.status)
    }

    fn validate_artifact_authority(
        &self,
        artifact: &BacktestArtifact,
    ) -> Result<(PathBuf, usize), ExperimentError> {
        let expected_reference = artifact_reference(artifact.digest())?;
        if artifact.reference() != expected_reference {
            return Err(ExperimentError::CorruptRecord);
        }
        let maximum =
            usize::try_from(artifact.byte_count()).map_err(|_| ExperimentError::CorruptRecord)?;
        if maximum == 0 || maximum > self.limits.max_artifact_bytes {
            return Err(ExperimentError::CorruptRecord);
        }
        Ok((PathBuf::from(artifact.reference()), maximum))
    }

    fn commit_terminal(
        &self,
        reservation: TrialReservation,
        status: TrialStatus,
    ) -> Result<TrialRecord, ExperimentError> {
        if reservation.spec.identity_version() != TrialIdentityVersion::V3 {
            return Err(ExperimentError::CorruptRecord);
        }
        let _guard = self
            .writer
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?;
        self.validate_active_attempt(&reservation)?;
        let terminal = encode_terminal(reservation.spec.id(), &status)?;
        if terminal.len() > self.limits.max_record_bytes {
            return Err(ExperimentError::LimitExceeded);
        }
        let terminal_parent = terminal_attempt_parent(reservation.spec.id());
        ensure_directory(&self.root, &terminal_parent)?;
        publish_or_confirm_exact(
            &self.root,
            &attempt_terminal_path(reservation.spec.id(), reservation.attempt),
            &terminal,
            ExistingPolicy::Reject,
        )?;
        let id = reservation.spec.id();
        drop(_guard);
        self.trial(id)
    }

    fn validate_active_attempt(
        &self,
        reservation: &TrialReservation,
    ) -> Result<(), ExperimentError> {
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
            || !self
                .active_attempts
                .lock()
                .map_err(|_| ExperimentError::Unavailable)?
                .contains(&(reservation.spec.id(), reservation.attempt))
            || latest_attempt(
                &self.root,
                reservation.spec.id(),
                reservation.record_digest,
                self.limits.max_record_bytes,
            )?
            .map(|value| value.attempt)
                != Some(reservation.attempt)
            || completed_at.unix_nanos() < attempt.acquired_at
        {
            return Err(ExperimentError::ReservationLeaseLost);
        }
        Ok(())
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
        if self
            .active_attempts
            .lock()
            .map_err(|_| ExperimentError::Unavailable)?
            .iter()
            .any(|(active_id, _)| *active_id == trial_id)
        {
            return Err(ExperimentError::TrialInProgress);
        }
        for _ in 0..3 {
            let latest = latest_attempt(
                &self.root,
                trial_id,
                record_digest,
                self.limits.max_record_bytes,
            )?;
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
            if let Some(attempt) = &latest {
                remove_optional(
                    &self.root,
                    &pending_artifact_path(trial_id, attempt.attempt),
                )?;
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
                    self.active_attempts
                        .lock()
                        .map_err(|_| ExperimentError::Unavailable)?
                        .insert((trial_id, number));
                    return Ok(TrialReservation {
                        spec,
                        record_digest,
                        attempt: number,
                        attempt_digest: digest_bytes(&attempt_bytes),
                        active_attempts: Arc::clone(&self.active_attempts),
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
    match root.hard_link(&stage_path, root, final_path) {
        Ok(()) =>
        {
            #[cfg(windows)]
            if let Err(error) = stage.sync_all() {
                drop(stage);
                remove_stage(root, &stage_path)?;
                return Err(ExperimentError::Io(error));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_bounded(root, final_path, bytes.len().max(1))?;
            if existing != bytes || policy == ExistingPolicy::Reject {
                drop(stage);
                remove_stage(root, &stage_path)?;
                return Err(ExperimentError::TrialAlreadyExists);
            }
        }
        Err(error) => {
            drop(stage);
            remove_stage(root, &stage_path)?;
            return Err(ExperimentError::Io(error));
        }
    }
    drop(stage);
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

fn publish_or_confirm_exact(
    root: &Dir,
    final_path: &Path,
    bytes: &[u8],
    policy: ExistingPolicy,
) -> Result<(), ExperimentError> {
    match publish_immutable(root, final_path, bytes, policy) {
        Ok(()) => Ok(()),
        Err(error) => match read_optional_bounded(root, final_path, bytes.len().max(1)) {
            Ok(Some(existing)) if existing == bytes => Ok(()),
            _ => Err(error),
        },
    }
}

fn validate_artifact_bytes(
    bytes: &[u8],
    artifact: &BacktestArtifact,
) -> Result<(), ExperimentError> {
    if u64::try_from(bytes.len()).map_err(|_| ExperimentError::CorruptRecord)?
        != artifact.byte_count()
        || digest_bytes(bytes) != artifact.digest()
    {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(())
}

fn artifact_reference(digest: Sha256Digest) -> Result<String, ExperimentError> {
    let hex = encode_hex(digest.bytes());
    let prefix = hex.get(..2).ok_or(ExperimentError::Encoding)?;
    Ok(format!("{ARTIFACTS}/{prefix}/{hex}.json"))
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

fn remove_optional(root: &Dir, path: &Path) -> Result<(), ExperimentError> {
    match root.remove_file(path) {
        Ok(()) => synchronize_parent(root, path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ExperimentError::Io(error)),
    }
}

fn cleanup_optional(root: &Dir, path: &Path) {
    match remove_optional(root, path) {
        Ok(()) | Err(_) => {}
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
    use cap_std::fs::OpenOptionsExt as _;

    let parent = path.parent().ok_or(ExperimentError::Encoding)?;
    let directory = root.open_dir_nofollow(parent)?;
    let mut options = OpenOptions::new();
    options.read(true);
    options.follow(FollowSymlinks::No);
    options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC);
    directory.open_with(".", &options)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn synchronize_parent(_root: &Dir, _path: &Path) -> Result<(), ExperimentError> {
    // The writable staged file is re-flushed after its final hard link is published.
    Ok(())
}

#[cfg(all(not(unix), not(windows)))]
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

fn acquire_authority_lock(root: &Dir) -> Result<File, ExperimentError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    options.follow(FollowSymlinks::No);
    configure_private_creation(&mut options);
    let lock = root.open_with(AUTHORITY_LOCK, &options)?.into_std();
    validate_authority_lock(&lock)?;
    lock.try_lock_exclusive()
        .map_err(|_| ExperimentError::Unavailable)?;
    validate_authority_lock(&lock)?;
    Ok(lock)
}

#[cfg(unix)]
fn validate_authority_lock(lock: &File) -> Result<(), ExperimentError> {
    use std::os::unix::fs::MetadataExt as _;

    let metadata = lock.metadata()?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.mode() & 0o077 != 0 {
        return Err(ExperimentError::Unavailable);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_authority_lock(lock: &File) -> Result<(), ExperimentError> {
    if !lock.metadata()?.is_file() {
        return Err(ExperimentError::Unavailable);
    }
    Ok(())
}

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

fn pending_artifact_path(id: TrialId, attempt: u64) -> PathBuf {
    Path::new(PENDING)
        .join(encode_hex(id.digest().bytes()))
        .join(format!("{attempt:020}.json"))
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

fn authoritative_attempt_terminal(
    root: &Dir,
    id: TrialId,
    identity_version: TrialIdentityVersion,
    latest_attempt: Option<&AttemptRecord>,
    maximum: usize,
) -> Result<Option<Vec<u8>>, ExperimentError> {
    let parent = terminal_attempt_parent(id);
    let directory = match root.open_dir_nofollow(&parent) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ExperimentError::Io(error)),
    };
    let mut count = 0_usize;
    let mut candidate = None::<u64>;
    let mut invalid = false;
    for entry in directory.entries()? {
        let entry = entry?;
        count = count.checked_add(1).ok_or(ExperimentError::LimitExceeded)?;
        if count > MAX_ATTEMPTS_PER_TRIAL {
            return Err(ExperimentError::LimitExceeded);
        }
        let number = entry
            .file_name()
            .to_str()
            .and_then(decode_attempt_terminal_name);
        if !entry.file_type()?.is_file() || number.is_none() || candidate.is_some() {
            invalid = true;
            continue;
        }
        candidate = number;
    }
    if count == 0 {
        return Ok(None);
    }
    if identity_version != TrialIdentityVersion::V3 || invalid {
        return Err(ExperimentError::CorruptRecord);
    }
    let number = candidate.ok_or(ExperimentError::CorruptRecord)?;
    if latest_attempt.map(|attempt| attempt.attempt) != Some(number) {
        return Err(ExperimentError::CorruptRecord);
    }
    read_bounded(root, &attempt_terminal_path(id, number), maximum).map(Some)
}

fn decode_attempt_terminal_name(name: &str) -> Option<u64> {
    let number = name.strip_suffix(".json")?;
    if number.len() != 20 || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let number = number.parse::<u64>().ok()?;
    (number != 0
        && usize::try_from(number)
            .ok()
            .is_some_and(|number| number <= MAX_ATTEMPTS_PER_TRIAL))
    .then_some(number)
}

fn latest_attempt(
    root: &Dir,
    id: TrialId,
    reservation_digest: Sha256Digest,
    maximum: usize,
) -> Result<Option<AttemptRecord>, ExperimentError> {
    let directory = match root.open_dir_nofollow(attempt_parent(id)) {
        Ok(directory) => directory,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(ExperimentError::Io(error)),
    };
    let mut latest = None::<AttemptRecord>;
    let mut count = 0_usize;
    let mut seen = [0_u64; MAX_ATTEMPTS_PER_TRIAL.div_ceil(64)];
    let expected_reservation_digest = encode_hex(reservation_digest.bytes());
    for entry in directory.entries()? {
        let entry = entry?;
        count = count.checked_add(1).ok_or(ExperimentError::LimitExceeded)?;
        if count > MAX_ATTEMPTS_PER_TRIAL {
            return Err(ExperimentError::LimitExceeded);
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(ExperimentError::CorruptRecord);
        };
        if !entry.file_type()?.is_file() {
            return Err(ExperimentError::CorruptRecord);
        }
        let number = decode_attempt_terminal_name(name).ok_or(ExperimentError::CorruptRecord)?;
        let bytes = read_bounded(root, &attempt_parent(id).join(name), maximum)?;
        let record = decode_attempt(&bytes, id)?;
        if record.attempt != number || record.reservation_digest != expected_reservation_digest {
            return Err(ExperimentError::CorruptRecord);
        }
        let index = usize::try_from(number - 1).map_err(|_| ExperimentError::CorruptRecord)?;
        let word = index / 64;
        let mask = 1_u64 << (index % 64);
        if seen[word] & mask != 0 {
            return Err(ExperimentError::CorruptRecord);
        }
        seen[word] |= mask;
        if latest
            .as_ref()
            .is_none_or(|current| record.attempt > current.attempt)
        {
            latest = Some(record);
        }
    }
    let Some(latest) = latest else {
        return Ok(None);
    };
    let latest_number =
        usize::try_from(latest.attempt).map_err(|_| ExperimentError::CorruptRecord)?;
    if count != latest_number
        || (0..latest_number).any(|index| seen[index / 64] & (1_u64 << (index % 64)) == 0)
    {
        return Err(ExperimentError::CorruptRecord);
    }
    Ok(Some(latest))
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
