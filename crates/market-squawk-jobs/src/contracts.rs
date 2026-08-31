use std::{num::NonZeroU64, str::FromStr};

use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_services::RequestId;
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

mod api;
mod lifecycle;

pub use api::*;
pub use lifecycle::*;

const MAXIMUM_RECOVERY_PAGE_ITEMS: usize = 1_024;
const MAXIMUM_EVENT_PAGE_ITEMS: usize = 4_096;
const MAXIMUM_JOB_ARTIFACTS: usize = 64;
const MAXIMUM_JOB_ARTIFACT_BYTES: usize = 1_073_741_824;
const MAXIMUM_JOB_ATTEMPTS: u64 = 64;

/// Invalid durable-job contract input.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum JobContractError {
    /// UUID identities must not use the nil value.
    #[error("job identity must not be nil")]
    NilIdentity,
    /// Text was not a valid UUID.
    #[error("job identity is not a valid UUID")]
    InvalidIdentityText,
    /// A job generation is one-based.
    #[error("job generation must be nonzero")]
    ZeroGeneration,
    /// A generation or sequence cannot advance without overflow.
    #[error("job counter overflow")]
    CounterOverflow,
    /// A requested page size was zero or exceeded its hard ceiling.
    #[error("job page limit is invalid")]
    InvalidPageLimit,
    /// A job attempt ceiling was zero or exceeded the code-owned maximum.
    #[error("job attempt limit is invalid")]
    InvalidAttemptLimit,
    /// Completed progress exceeded the declared total.
    #[error("completed job units exceed total units")]
    ProgressExceedsTotal,
    /// A page contained more records than the caller admitted.
    #[error("job page exceeds its admitted limit")]
    PageLimitExceeded,
    /// A completed snapshot omitted its immutable result reference.
    #[error("completed job snapshot requires a result reference")]
    MissingResult,
    /// A failed snapshot omitted its typed failure.
    #[error("failed job snapshot requires failure evidence")]
    MissingFailure,
    /// A nonterminal snapshot claimed terminal result or failure evidence.
    #[error("job snapshot contains incompatible terminal evidence")]
    UnexpectedTerminalEvidence,
    /// Result artifacts exceeded the count or aggregate-byte ceiling.
    #[error("job result artifacts exceed admitted bounds")]
    ArtifactLimitExceeded,
}

/// Stable identity for one durable job.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct JobId(Uuid);

impl JobId {
    /// Creates a non-nil job identity.
    pub fn try_from_uuid(value: Uuid) -> Result<Self, JobContractError> {
        if value.is_nil() {
            Err(JobContractError::NilIdentity)
        } else {
            Ok(Self(value))
        }
    }

    /// Parses a non-nil UUID job identity.
    pub fn try_from_str(value: &str) -> Result<Self, JobContractError> {
        value.parse()
    }

    /// Returns the UUID value.
    #[must_use]
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl FromStr for JobId {
    type Err = JobContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(value).map_err(|_| JobContractError::InvalidIdentityText)?;
        Self::try_from_uuid(uuid)
    }
}

/// One-based execution generation for a durable job.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct JobGeneration(NonZeroU64);

impl JobGeneration {
    /// Creates a one-based job generation.
    pub fn try_new(value: u64) -> Result<Self, JobContractError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(JobContractError::ZeroGeneration)
    }

    /// Returns the one-based generation.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Advances to the next generation without overflow.
    pub fn checked_next(self) -> Result<Self, JobContractError> {
        self.get()
            .checked_add(1)
            .ok_or(JobContractError::CounterOverflow)
            .and_then(Self::try_new)
    }
}

/// Monotonic event sequence within one job generation. Zero is the initial cursor.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct JobEventSequence(u64);

impl JobEventSequence {
    /// Creates a sequence value. Zero represents no event observed yet.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the sequence value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Advances to the next sequence without overflow.
    pub fn checked_next(self) -> Result<Self, JobContractError> {
        self.get()
            .checked_add(1)
            .map(Self)
            .ok_or(JobContractError::CounterOverflow)
    }
}

/// Finite ceiling on durable execution generations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct JobAttemptLimit(NonZeroU64);

impl JobAttemptLimit {
    /// Admits between one and 64 execution generations.
    pub fn try_new(value: u64) -> Result<Self, JobContractError> {
        match NonZeroU64::new(value) {
            Some(value) if value.get() <= MAXIMUM_JOB_ATTEMPTS => Ok(Self(value)),
            _ => Err(JobContractError::InvalidAttemptLimit),
        }
    }

    /// Returns the admitted generation ceiling.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

macro_rules! page_limit {
    ($name:ident, $maximum:expr, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct $name(usize);

        impl $name {
            /// Creates a positive limit no larger than its code-owned ceiling.
            pub fn try_new(value: usize) -> Result<Self, JobContractError> {
                if value == 0 || value > $maximum {
                    Err(JobContractError::InvalidPageLimit)
                } else {
                    Ok(Self(value))
                }
            }

            /// Returns the admitted item count.
            #[must_use]
            pub const fn get(self) -> usize {
                self.0
            }
        }
    };
}

page_limit!(
    RecoveryPageLimit,
    MAXIMUM_RECOVERY_PAGE_ITEMS,
    "Maximum records returned by one nonterminal-recovery page."
);
page_limit!(
    JobEventPageLimit,
    MAXIMUM_EVENT_PAGE_ITEMS,
    "Maximum records returned by one job-event page."
);
page_limit!(
    JobListPageLimit,
    MAXIMUM_RECOVERY_PAGE_ITEMS,
    "Maximum latest-generation snapshots returned by one job-list page."
);

/// Opaque continuation returned by nonterminal recovery scans.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct RecoveryCursor(SourceIdentifier);

impl RecoveryCursor {
    /// Wraps an already validated opaque cursor.
    #[must_use]
    pub const fn new(value: SourceIdentifier) -> Self {
        Self(value)
    }

    /// Returns the opaque cursor value.
    #[must_use]
    pub const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.0
    }
}

/// Opaque continuation returned by bounded latest-job lists.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct JobListCursor(SourceIdentifier);

impl JobListCursor {
    /// Wraps a repository-minted opaque continuation.
    #[must_use]
    pub const fn new(value: SourceIdentifier) -> Self {
        Self(value)
    }

    pub(crate) const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.0
    }
}

/// Durable lifecycle state. Completed, failed, cancelled, and interrupted end a generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Accepted and waiting for owned execution capacity.
    Queued,
    /// Validating immutable inputs and acquiring bounded resources.
    Preparing,
    /// Runner owns the current generation.
    Running,
    /// Runner requires an explicit application confirmation.
    AwaitingConfirmation,
    /// Cooperative cancellation has been durably requested.
    Cancelling,
    /// Immutable domain result authority published successfully.
    Completed,
    /// Execution terminated with typed failure evidence.
    Failed,
    /// Cooperative cancellation and cleanup completed.
    Cancelled,
    /// Process or host interruption ended the prior generation.
    Interrupted,
    /// A new generation is applying its recovery policy.
    Recovering,
}

impl JobState {
    /// Returns whether the current execution generation cannot transition again.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted
        )
    }
}

/// Transport-neutral origin retained for audit and authorization correlation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobOrigin {
    workspace: SourceIdentifier,
    client: SourceIdentifier,
}

impl JobOrigin {
    /// Binds an admitted workspace and initiating client identity.
    #[must_use]
    pub const fn new(workspace: SourceIdentifier, client: SourceIdentifier) -> Self {
        Self { workspace, client }
    }

    /// Admitted workspace identity.
    #[must_use]
    pub const fn workspace(&self) -> &SourceIdentifier {
        &self.workspace
    }

    /// Initiating client identity.
    #[must_use]
    pub const fn client(&self) -> &SourceIdentifier {
        &self.client
    }
}

/// Immutable path-free input identity retained for replay and recovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmittedJobInput {
    authority: SourceIdentifier,
    identity: SourceIdentifier,
    digest: EvidenceDigest,
}

impl AdmittedJobInput {
    /// Binds the owning authority, immutable identity, and exact content digest.
    #[must_use]
    pub const fn new(
        authority: SourceIdentifier,
        identity: SourceIdentifier,
        digest: EvidenceDigest,
    ) -> Self {
        Self {
            authority,
            identity,
            digest,
        }
    }

    /// Input-owning application authority.
    #[must_use]
    pub const fn authority(&self) -> &SourceIdentifier {
        &self.authority
    }

    /// Immutable input identity.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Exact input content digest.
    #[must_use]
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Exact authority image used when a job was admitted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobAuthoritySnapshot {
    authority: SourceIdentifier,
    identity: SourceIdentifier,
    digest: EvidenceDigest,
    captured_at: Timestamp,
}

impl JobAuthoritySnapshot {
    /// Binds one immutable authority snapshot and observation time.
    #[must_use]
    pub const fn new(
        authority: SourceIdentifier,
        identity: SourceIdentifier,
        digest: EvidenceDigest,
        captured_at: Timestamp,
    ) -> Self {
        Self {
            authority,
            identity,
            digest,
            captured_at,
        }
    }

    /// Authority that minted this snapshot.
    #[must_use]
    pub const fn authority(&self) -> &SourceIdentifier {
        &self.authority
    }

    /// Immutable authority-image identity.
    #[must_use]
    pub const fn identity(&self) -> &SourceIdentifier {
        &self.identity
    }

    /// Exact authority-image digest.
    #[must_use]
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }

    /// Time the authority image was captured.
    #[must_use]
    pub const fn captured_at(&self) -> Timestamp {
        self.captured_at
    }
}

/// Immutable job specification admitted before persistence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdmittedJobSpec {
    id: JobId,
    generation: JobGeneration,
    kind: SourceIdentifier,
    origin: JobOrigin,
    request_id: RequestId,
    input: AdmittedJobInput,
    authority: JobAuthoritySnapshot,
    attempt_limit: JobAttemptLimit,
    admitted_at: Timestamp,
}

impl AdmittedJobSpec {
    /// Creates an immutable path-free specification with a finite attempt ceiling.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        id: JobId,
        generation: JobGeneration,
        kind: SourceIdentifier,
        origin: JobOrigin,
        request_id: RequestId,
        input: AdmittedJobInput,
        authority: JobAuthoritySnapshot,
        attempt_limit: JobAttemptLimit,
        admitted_at: Timestamp,
    ) -> Result<Self, JobContractError> {
        if generation.get() > attempt_limit.get() {
            return Err(JobContractError::InvalidAttemptLimit);
        }
        Ok(Self {
            id,
            generation,
            kind,
            origin,
            request_id,
            input,
            authority,
            attempt_limit,
            admitted_at,
        })
    }

    /// Stable job identity.
    #[must_use]
    pub const fn id(&self) -> JobId {
        self.id
    }

    /// Current admitted generation.
    #[must_use]
    pub const fn generation(&self) -> JobGeneration {
        self.generation
    }

    /// Stable registered runner kind.
    #[must_use]
    pub const fn kind(&self) -> &SourceIdentifier {
        &self.kind
    }

    /// Initiating transport-neutral request identity.
    #[must_use]
    pub const fn request_id(&self) -> &RequestId {
        &self.request_id
    }

    /// Workspace and initiating client identity.
    #[must_use]
    pub const fn origin(&self) -> &JobOrigin {
        &self.origin
    }

    /// Immutable admitted input authority.
    #[must_use]
    pub const fn input(&self) -> &AdmittedJobInput {
        &self.input
    }

    /// Exact authority image used at admission.
    #[must_use]
    pub const fn authority(&self) -> &JobAuthoritySnapshot {
        &self.authority
    }

    /// Finite execution-generation ceiling.
    #[must_use]
    pub const fn attempt_limit(&self) -> JobAttemptLimit {
        self.attempt_limit
    }

    /// Durable admission timestamp.
    #[must_use]
    pub const fn admitted_at(&self) -> Timestamp {
        self.admitted_at
    }

    pub(crate) fn next_generation(&self, admitted_at: Timestamp) -> Result<Self, JobContractError> {
        let generation = self.generation.checked_next()?;
        Self::try_new(
            self.id,
            generation,
            self.kind.clone(),
            self.origin.clone(),
            self.request_id.clone(),
            self.input.clone(),
            self.authority.clone(),
            self.attempt_limit,
            admitted_at,
        )
    }
}
