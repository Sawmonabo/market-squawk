//! Staged, approval-bound program update and automatic rollback authority.

use std::{fmt, num::NonZeroU64, sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::Mutex;

const MAXIMUM_VERSION_BYTES: usize = 128;

/// Monotonic program-selector generation, independent of workspace/data generations.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ProgramGeneration(NonZeroU64);

impl ProgramGeneration {
    /// Creates a nonzero generation.
    pub fn try_new(value: u64) -> Result<Self, UpdateError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(UpdateError::InvalidGeneration)
    }

    /// Returns the generation value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    fn next(self) -> Result<Self, UpdateError> {
        self.get()
            .checked_add(1)
            .ok_or(UpdateError::GenerationExhausted)
            .and_then(Self::try_new)
    }
}

/// Exact downloaded candidate admitted by trusted metadata and digest/length verification.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StagedUpdateCandidate {
    version: String,
    trusted_metadata_sha256: [u8; 32],
    manifest_sha256: [u8; 32],
    bundle_sha256: [u8; 32],
    bundle_bytes: u64,
    minimum_schema_version: u32,
    maximum_schema_version: u32,
}

impl StagedUpdateCandidate {
    /// Constructs candidate evidence only at the crate-private trusted update adapter boundary.
    #[allow(
        clippy::too_many_arguments,
        reason = "every update admission claim is independently verified"
    )]
    pub(crate) fn try_from_trusted_metadata(
        version: impl Into<String>,
        trusted_metadata_sha256: [u8; 32],
        manifest_sha256: [u8; 32],
        bundle_sha256: [u8; 32],
        bundle_bytes: u64,
        minimum_schema_version: u32,
        maximum_schema_version: u32,
    ) -> Result<Self, UpdateError> {
        let version = version.into();
        if version.is_empty()
            || version.len() > MAXIMUM_VERSION_BYTES
            || version.chars().any(char::is_control)
            || trusted_metadata_sha256 == [0; 32]
            || manifest_sha256 == [0; 32]
            || bundle_sha256 == [0; 32]
            || bundle_bytes == 0
            || minimum_schema_version == 0
            || minimum_schema_version > maximum_schema_version
        {
            return Err(UpdateError::InvalidCandidate);
        }
        Ok(Self {
            version,
            trusted_metadata_sha256,
            manifest_sha256,
            bundle_sha256,
            bundle_bytes,
            minimum_schema_version,
            maximum_schema_version,
        })
    }
}

/// Current runtime and storage facts used for compatibility preflight.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UpdateActivitySnapshot {
    schema_version: u32,
    available_disk_bytes: u64,
    required_disk_bytes: u64,
    running_mutation_jobs: u32,
    paper_execution_active: bool,
    execution_reconciliation_pending: bool,
}

impl UpdateActivitySnapshot {
    /// Creates exact preflight facts from the current application authorities.
    #[allow(
        clippy::too_many_arguments,
        reason = "every update blocker is explicit"
    )]
    pub const fn new(
        schema_version: u32,
        available_disk_bytes: u64,
        required_disk_bytes: u64,
        running_mutation_jobs: u32,
        paper_execution_active: bool,
        execution_reconciliation_pending: bool,
    ) -> Self {
        Self {
            schema_version,
            available_disk_bytes,
            required_disk_bytes,
            running_mutation_jobs,
            paper_execution_active,
            execution_reconciliation_pending,
        }
    }

    fn compatible(self, candidate: &StagedUpdateCandidate) -> bool {
        self.schema_version >= candidate.minimum_schema_version
            && self.schema_version <= candidate.maximum_schema_version
            && self.available_disk_bytes >= self.required_disk_bytes
            && self.running_mutation_jobs == 0
            && !self.paper_execution_active
            && !self.execution_reconciliation_pending
    }
}

/// Digest-bound staged-update preview.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UpdatePreview {
    current_generation: ProgramGeneration,
    candidate: StagedUpdateCandidate,
    activity: UpdateActivitySnapshot,
    can_approve: bool,
    preview_sha256: [u8; 32],
}

impl UpdatePreview {
    /// Returns whether compatibility, disk, activity, and reconciliation preflight passed.
    #[must_use]
    pub const fn can_approve(&self) -> bool {
        self.can_approve
    }

    /// Consumes this exact successful preflight into explicit approval.
    pub fn try_approve(self) -> Result<UpdateApproval, UpdateError> {
        if !self.can_approve {
            return Err(UpdateError::PreflightBlocked);
        }
        Ok(UpdateApproval {
            current_generation: self.current_generation,
            candidate: self.candidate,
            preview_sha256: self.preview_sha256,
        })
    }
}

/// Approval bound to an exact staged candidate and current program generation.
#[derive(Clone, Debug)]
pub struct UpdateApproval {
    current_generation: ProgramGeneration,
    candidate: StagedUpdateCandidate,
    preview_sha256: [u8; 32],
}

/// Platform lifecycle boundary that owns selector mutation, restart, health, and rollback.
#[async_trait]
pub trait UpdateActivation: fmt::Debug + Send + Sync {
    /// Drains and reconciles application mutation authorities before selector activation.
    async fn drain_and_reconcile(&self, deadline: std::time::Instant) -> Result<(), UpdateError>;

    /// Activates the already verified immutable candidate generation.
    async fn activate(
        &self,
        candidate: &StagedUpdateCandidate,
        attempted_generation: ProgramGeneration,
    ) -> Result<(), UpdateError>;

    /// Restarts and proves the attempted program generation healthy.
    async fn restart_and_health_check(
        &self,
        generation: ProgramGeneration,
    ) -> Result<(), UpdateError>;

    /// Revalidates and activates the retained known-good program under a newer generation.
    async fn rollback_known_good(&self, generation: ProgramGeneration) -> Result<(), UpdateError>;
}

/// Durable update-transition journal committed before a generation becomes current.
pub trait UpdateJournal: fmt::Debug + Send + Sync {
    /// Appends one terminal activation or rollback record.
    fn append(&self, record: &UpdateTransitionRecord) -> Result<(), UpdateError>;
}

/// Stable terminal state of one update attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateTransitionState {
    Activated,
    RolledBack,
}

/// Durable evidence for a staged update activation attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UpdateTransitionRecord {
    previous_generation: ProgramGeneration,
    attempted_generation: ProgramGeneration,
    active_generation: ProgramGeneration,
    candidate_manifest_sha256: [u8; 32],
    preview_sha256: [u8; 32],
    state: UpdateTransitionState,
}

/// Client-visible update receipt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct UpdateReceipt {
    active_generation: ProgramGeneration,
    attempted_generation: ProgramGeneration,
}

impl UpdateReceipt {
    /// Returns the only current program generation after activation or rollback.
    #[must_use]
    pub const fn active_generation(self) -> ProgramGeneration {
        self.active_generation
    }
}

/// Complete update result, including automatic rollback.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdateOutcome {
    Activated(UpdateReceipt),
    RolledBack(UpdateReceipt),
}

/// Single-writer staged update authority.
pub struct TrustedUpdateAuthority {
    state: Mutex<UpdateLifecycleState>,
    journal: Arc<dyn UpdateJournal>,
}

#[derive(Debug)]
struct UpdateLifecycleState {
    generation: ProgramGeneration,
    fenced: bool,
}

impl TrustedUpdateAuthority {
    /// Restores the current program generation and durable journal authority.
    #[must_use]
    pub fn new(generation: ProgramGeneration, journal: Arc<dyn UpdateJournal>) -> Self {
        Self {
            state: Mutex::new(UpdateLifecycleState {
                generation,
                fenced: false,
            }),
            journal,
        }
    }

    /// Returns the current durable program generation when lifecycle recovery is not fenced.
    pub fn current(&self) -> Result<ProgramGeneration, UpdateError> {
        let state = self
            .state
            .try_lock()
            .map_err(|_| UpdateError::AuthorityBusy)?;
        if state.fenced {
            return Err(UpdateError::AuthorityFenced);
        }
        Ok(state.generation)
    }

    /// Builds a compatibility preflight over one trusted, fully downloaded candidate.
    pub fn preview(
        &self,
        candidate: StagedUpdateCandidate,
        activity: UpdateActivitySnapshot,
    ) -> Result<UpdatePreview, UpdateError> {
        let state = self
            .state
            .try_lock()
            .map_err(|_| UpdateError::AuthorityBusy)?;
        if state.fenced {
            return Err(UpdateError::AuthorityFenced);
        }
        let current_generation = state.generation;
        let can_approve = activity.compatible(&candidate);
        let encoded = serde_json::to_vec(&(
            "market-squawk-update-preview-v1",
            current_generation,
            &candidate,
            activity,
            can_approve,
        ))
        .map_err(|_| UpdateError::Encoding)?;
        Ok(UpdatePreview {
            current_generation,
            candidate,
            activity,
            can_approve,
            preview_sha256: Sha256::digest(encoded).into(),
        })
    }

    /// Activates one explicitly approved candidate or rolls back automatically.
    pub async fn activate(
        &self,
        approval: UpdateApproval,
        activation: &dyn UpdateActivation,
        timeout: Duration,
    ) -> Result<UpdateOutcome, UpdateError> {
        if timeout.is_zero() || timeout > Duration::from_secs(10 * 60) {
            return Err(UpdateError::InvalidTimeout);
        }
        let mut state = self.state.lock().await;
        if state.fenced || state.generation != approval.current_generation {
            return Err(UpdateError::StaleApproval);
        }
        let deadline = std::time::Instant::now()
            .checked_add(timeout)
            .ok_or(UpdateError::InvalidTimeout)?;
        state.fenced = true;
        if let Err(error) = activation.drain_and_reconcile(deadline).await {
            state.fenced = false;
            return Err(error);
        }
        let attempted = state.generation.next()?;
        let activated = activation
            .activate(&approval.candidate, attempted)
            .await
            .is_ok()
            && activation.restart_and_health_check(attempted).await.is_ok();
        if activated {
            let record = UpdateTransitionRecord {
                previous_generation: state.generation,
                attempted_generation: attempted,
                active_generation: attempted,
                candidate_manifest_sha256: approval.candidate.manifest_sha256,
                preview_sha256: approval.preview_sha256,
                state: UpdateTransitionState::Activated,
            };
            if self.journal.append(&record).is_ok() {
                state.generation = attempted;
                state.fenced = false;
                return Ok(UpdateOutcome::Activated(UpdateReceipt {
                    active_generation: attempted,
                    attempted_generation: attempted,
                }));
            }
        }
        rollback_after_update_failure(&mut state, attempted, &approval, activation, &*self.journal)
            .await
    }
}

async fn rollback_after_update_failure(
    state: &mut UpdateLifecycleState,
    attempted: ProgramGeneration,
    approval: &UpdateApproval,
    activation: &dyn UpdateActivation,
    journal: &dyn UpdateJournal,
) -> Result<UpdateOutcome, UpdateError> {
    let rollback = attempted.next()?;
    activation
        .rollback_known_good(rollback)
        .await
        .map_err(|_| UpdateError::RollbackFailed)?;
    let record = UpdateTransitionRecord {
        previous_generation: state.generation,
        attempted_generation: attempted,
        active_generation: rollback,
        candidate_manifest_sha256: approval.candidate.manifest_sha256,
        preview_sha256: approval.preview_sha256,
        state: UpdateTransitionState::RolledBack,
    };
    journal.append(&record)?;
    state.generation = rollback;
    state.fenced = false;
    Ok(UpdateOutcome::RolledBack(UpdateReceipt {
        active_generation: rollback,
        attempted_generation: attempted,
    }))
}

impl fmt::Debug for TrustedUpdateAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TrustedUpdateAuthority([STAGED PROGRAM AUTHORITY])")
    }
}

/// Typed staged-update failure without installer paths or sensitive metadata.
#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("program generation must be nonzero")]
    InvalidGeneration,
    #[error("program generation is exhausted")]
    GenerationExhausted,
    #[error("trusted update candidate is invalid")]
    InvalidCandidate,
    #[error("update authority is busy")]
    AuthorityBusy,
    #[error("update authority is fenced pending explicit lifecycle recovery")]
    AuthorityFenced,
    #[error("update compatibility preflight is blocked")]
    PreflightBlocked,
    #[error("update approval is stale")]
    StaleApproval,
    #[error("update timeout is invalid")]
    InvalidTimeout,
    #[error("update evidence could not be encoded")]
    Encoding,
    #[error("update activation failed")]
    ActivationFailed,
    #[error("program restart or health check failed")]
    HealthCheckFailed,
    #[error("known-good program rollback failed")]
    RollbackFailed,
    #[error("update transition journal is unavailable")]
    JournalUnavailable,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex as StandardMutex,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Debug, Default)]
    struct RecordingJournal(StandardMutex<Vec<UpdateTransitionRecord>>);

    impl UpdateJournal for RecordingJournal {
        fn append(&self, record: &UpdateTransitionRecord) -> Result<(), UpdateError> {
            self.0
                .lock()
                .map_err(|_| UpdateError::JournalUnavailable)?
                .push(record.clone());
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct FailingFirstJournal {
        attempts: AtomicUsize,
        records: StandardMutex<Vec<UpdateTransitionRecord>>,
    }

    impl UpdateJournal for FailingFirstJournal {
        fn append(&self, record: &UpdateTransitionRecord) -> Result<(), UpdateError> {
            if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
                return Err(UpdateError::JournalUnavailable);
            }
            self.records
                .lock()
                .map_err(|_| UpdateError::JournalUnavailable)?
                .push(record.clone());
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailingActivation;

    #[async_trait]
    impl UpdateActivation for FailingActivation {
        async fn drain_and_reconcile(
            &self,
            _deadline: std::time::Instant,
        ) -> Result<(), UpdateError> {
            Ok(())
        }

        async fn activate(
            &self,
            _candidate: &StagedUpdateCandidate,
            _attempted_generation: ProgramGeneration,
        ) -> Result<(), UpdateError> {
            Ok(())
        }

        async fn restart_and_health_check(
            &self,
            _generation: ProgramGeneration,
        ) -> Result<(), UpdateError> {
            Err(UpdateError::HealthCheckFailed)
        }

        async fn rollback_known_good(
            &self,
            _generation: ProgramGeneration,
        ) -> Result<(), UpdateError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn failed_candidate_health_rolls_back_under_a_newer_program_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = Arc::new(RecordingJournal::default());
        let authority =
            TrustedUpdateAuthority::new(ProgramGeneration::try_new(10)?, journal.clone());
        let candidate = StagedUpdateCandidate::try_from_trusted_metadata(
            "1.1.0", [1; 32], [2; 32], [3; 32], 1024, 1, 1,
        )?;
        let preview = authority.preview(
            candidate,
            UpdateActivitySnapshot::new(1, 2048, 1024, 0, false, false),
        )?;

        let outcome = authority
            .activate(
                preview.try_approve()?,
                &FailingActivation,
                Duration::from_secs(1),
            )
            .await?;

        let UpdateOutcome::RolledBack(receipt) = outcome else {
            return Err("expected automatic rollback".into());
        };
        assert_eq!(receipt.active_generation().get(), 12);
        assert_eq!(journal.0.lock().map_err(|_| "journal")?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn activation_journal_failure_rolls_back_and_records_the_new_generation()
    -> Result<(), Box<dyn std::error::Error>> {
        let journal = Arc::new(FailingFirstJournal::default());
        let authority =
            TrustedUpdateAuthority::new(ProgramGeneration::try_new(10)?, journal.clone());
        let candidate = StagedUpdateCandidate::try_from_trusted_metadata(
            "1.1.0", [1; 32], [2; 32], [3; 32], 1024, 1, 1,
        )?;
        let preview = authority.preview(
            candidate,
            UpdateActivitySnapshot::new(1, 2048, 1024, 0, false, false),
        )?;

        let outcome = authority
            .activate(
                preview.try_approve()?,
                &HealthyActivation,
                Duration::from_secs(1),
            )
            .await?;

        assert!(matches!(outcome, UpdateOutcome::RolledBack(_)));
        assert_eq!(authority.current()?.get(), 12);
        let records = journal.records.lock().map_err(|_| "journal")?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].state, UpdateTransitionState::RolledBack);
        Ok(())
    }

    #[derive(Debug)]
    struct HealthyActivation;

    #[async_trait]
    impl UpdateActivation for HealthyActivation {
        async fn drain_and_reconcile(
            &self,
            _deadline: std::time::Instant,
        ) -> Result<(), UpdateError> {
            Ok(())
        }

        async fn activate(
            &self,
            _candidate: &StagedUpdateCandidate,
            _attempted_generation: ProgramGeneration,
        ) -> Result<(), UpdateError> {
            Ok(())
        }

        async fn restart_and_health_check(
            &self,
            _generation: ProgramGeneration,
        ) -> Result<(), UpdateError> {
            Ok(())
        }

        async fn rollback_known_good(
            &self,
            _generation: ProgramGeneration,
        ) -> Result<(), UpdateError> {
            Ok(())
        }
    }
}
