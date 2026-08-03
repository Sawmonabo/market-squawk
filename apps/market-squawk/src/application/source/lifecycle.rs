//! Closed credential-free source lifecycle command and receipt boundary.

use std::{fmt, num::NonZeroU64, time::Instant};

use async_trait::async_trait;
use market_squawk_domain::{
    ConnectionGeneration, CoverageStatus, DataQuality, DigestAlgorithm, EvidenceDigest,
    SourceIdentifier, StreamIntegrityState, Timestamp,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Closed source lifecycle action implemented by the sole runtime/onboarding owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceLifecycleAction {
    /// Starts an already admitted source configuration.
    Start,
    /// Stops runtime activity without deleting registration or retained data.
    Stop,
    /// Retries the currently blocked phase under its existing provider budget.
    Retry,
    /// Invalidates the current generation and establishes a verified successor.
    Resynchronize,
    /// Revalidates credentials, rights, availability, and runtime readiness without starting.
    Verify,
    /// Activates an already prepared public-configuration generation.
    Reconfigure,
    /// Revokes runtime authority and performs the selected local cleanup contract.
    Remove,
}

/// Caller input for one exact source lifecycle command.
pub struct SourceLifecycleCommandInput {
    /// Code-owned provider surface.
    pub provider: SourceIdentifier,
    /// Requested closed action.
    pub action: SourceLifecycleAction,
    /// Exact state revision required for compare-and-apply.
    pub expected_state_revision: NonZeroU64,
    /// Exact current live generation required by retry or resynchronization.
    pub expected_generation: Option<ConnectionGeneration>,
    /// Existing onboarding session resolved privately by the authority.
    pub onboarding_session_id: Option<Uuid>,
    /// Digest of an already prepared public configuration; never raw configuration or credentials.
    pub public_configuration_digest: Option<EvidenceDigest>,
    /// Bounded code/reason identifier for an operator-directed transition.
    pub reason: Option<SourceIdentifier>,
    /// Request-owned cancellation authority.
    pub cancellation: CancellationToken,
    /// Monotonic deadline that the implementation may narrow but never extend.
    pub deadline: Instant,
}

impl fmt::Debug for SourceLifecycleCommandInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceLifecycleCommandInput")
            .field("provider", &self.provider)
            .field("action", &self.action)
            .field("expected_state_revision", &self.expected_state_revision)
            .field("expected_generation", &self.expected_generation)
            .field("onboarding_session_id", &self.onboarding_session_id)
            .field(
                "has_public_configuration_digest",
                &self.public_configuration_digest.is_some(),
            )
            .field("reason", &self.reason)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Validated lifecycle command containing no credential, lease, path, or network authority.
pub struct SourceLifecycleCommand {
    provider: SourceIdentifier,
    action: SourceLifecycleAction,
    expected_state_revision: NonZeroU64,
    expected_generation: Option<ConnectionGeneration>,
    onboarding_session_id: Option<Uuid>,
    public_configuration_digest: Option<EvidenceDigest>,
    reason: Option<SourceIdentifier>,
    cancellation: CancellationToken,
    deadline: Instant,
}

impl SourceLifecycleCommand {
    /// Validates the action-specific precondition matrix before authority dispatch.
    pub fn try_new(input: SourceLifecycleCommandInput) -> Result<Self, SourceLifecycleError> {
        if input.deadline <= Instant::now() {
            return Err(SourceLifecycleError::DeadlineExceeded);
        }
        let valid = match input.action {
            SourceLifecycleAction::Start | SourceLifecycleAction::Verify => {
                input.expected_generation.is_none()
                    && input.public_configuration_digest.is_none()
                    && input.reason.is_none()
            }
            SourceLifecycleAction::Reconfigure => {
                input.onboarding_session_id.is_some()
                    && input.public_configuration_digest.is_some()
                    && input.reason.is_none()
            }
            SourceLifecycleAction::Retry => {
                input.onboarding_session_id.is_none()
                    && input.public_configuration_digest.is_none()
                    && input.reason.is_some()
            }
            SourceLifecycleAction::Resynchronize => {
                input.expected_generation.is_some()
                    && input.onboarding_session_id.is_none()
                    && input.public_configuration_digest.is_none()
                    && input.reason.is_some()
            }
            SourceLifecycleAction::Stop | SourceLifecycleAction::Remove => {
                input.onboarding_session_id.is_none()
                    && input.public_configuration_digest.is_none()
                    && input.reason.is_some()
            }
        };
        if !valid {
            return Err(SourceLifecycleError::InvalidRequest);
        }
        if input.public_configuration_digest.is_some_and(|digest| {
            digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32]
        }) {
            return Err(SourceLifecycleError::InvalidRequest);
        }
        Ok(Self {
            provider: input.provider,
            action: input.action,
            expected_state_revision: input.expected_state_revision,
            expected_generation: input.expected_generation,
            onboarding_session_id: input.onboarding_session_id,
            public_configuration_digest: input.public_configuration_digest,
            reason: input.reason,
            cancellation: input.cancellation,
            deadline: input.deadline,
        })
    }

    /// Returns the exact code-owned provider surface.
    pub const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the closed requested action.
    pub const fn action(&self) -> SourceLifecycleAction {
        self.action
    }

    /// Returns the compare-and-apply state revision.
    pub const fn expected_state_revision(&self) -> NonZeroU64 {
        self.expected_state_revision
    }

    /// Returns the exact expected live generation when required.
    pub const fn expected_generation(&self) -> Option<ConnectionGeneration> {
        self.expected_generation
    }

    /// Returns the onboarding session identity when the action resolves prepared authority.
    pub const fn onboarding_session_id(&self) -> Option<Uuid> {
        self.onboarding_session_id
    }

    /// Returns the already prepared public-configuration digest for reconfiguration.
    pub const fn public_configuration_digest(&self) -> Option<EvidenceDigest> {
        self.public_configuration_digest
    }

    /// Returns the bounded transition reason when required.
    pub const fn reason(&self) -> Option<&SourceIdentifier> {
        self.reason.as_ref()
    }

    /// Returns request cancellation propagated to the sole authority.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Returns the absolute monotonic deadline.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl fmt::Debug for SourceLifecycleCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceLifecycleCommand")
            .field("provider", &self.provider)
            .field("action", &self.action)
            .field("expected_state_revision", &self.expected_state_revision)
            .field("expected_generation", &self.expected_generation)
            .field("onboarding_session_id", &self.onboarding_session_id)
            .field(
                "has_public_configuration_digest",
                &self.public_configuration_digest.is_some(),
            )
            .field("reason", &self.reason)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Safe disposition of one lifecycle command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceLifecycleDisposition {
    /// The requested transition was durably applied.
    Applied,
    /// The exact request had already been applied.
    Replay,
    /// Current evidence rejected the request without changing authority.
    Rejected,
    /// State is indeterminate and must be reconciled before another mutation.
    ReconciliationRequired,
}

/// Safe source lifecycle state after an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceLifecycleState {
    /// Registered or configured but not running.
    Stopped,
    /// Runtime admission or initial synchronization is in progress.
    Starting,
    /// A current runtime generation is active.
    Active,
    /// Integrity recovery is establishing a successor generation.
    Resynchronizing,
    /// Current authority is blocked pending an explicit next action.
    Blocked,
    /// Runtime authority and selected configuration have been removed.
    Removed,
}

/// Provider-budget state safe for user-facing lifecycle results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceRateBudgetState {
    /// A provider request may be attempted now.
    Available,
    /// Retry is prohibited until the exact provider deadline.
    CoolingDown { until: Timestamp },
    /// The provider budget is unavailable and no retry may be scheduled.
    Unavailable,
    /// No exact provider-budget observation is exposed by the current authority boundary.
    Indeterminate,
}

/// Authorization and rights state safe for public results.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceAuthorizationState {
    /// Current credential and rights evidence is admitted for the selected operation.
    Admitted,
    /// Verification or rights admission remains pending.
    Pending,
    /// Current evidence explicitly blocks the operation.
    Blocked,
    /// No authorization is required for this public/local source.
    NotRequired,
}

/// Source availability derived from current provider/runtime evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceAvailabilityState {
    /// The source is currently available for its admitted operation.
    Available,
    /// Provider evidence is delayed or temporarily unavailable.
    TemporarilyUnavailable,
    /// Configuration or authority has been removed.
    Removed,
    /// Current evidence cannot establish availability safely.
    Indeterminate,
}

/// Stable public blocker classification; internal errors and provider payloads remain private.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceLifecycleBlocker {
    /// Credentials require user action or verification.
    Credential,
    /// Rights or authorization does not admit the requested operation.
    Rights,
    /// Provider rate budget prohibits an attempt.
    RateBudget,
    /// Stream continuity or checksum evidence requires resynchronization.
    Integrity,
    /// Provider or transport is unavailable.
    ProviderAvailability,
    /// Durable local state requires reconciliation.
    Reconciliation,
    /// A newer state revision or generation already owns authority.
    StalePrecondition,
}

/// Immutable rights/availability evidence identity carried by a lifecycle receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceRightsEvidence {
    evidence_id: SourceIdentifier,
    digest: EvidenceDigest,
    effective_at: Timestamp,
    expires_at: Option<Timestamp>,
}

impl SourceRightsEvidence {
    /// Constructs time-ordered immutable rights evidence.
    pub fn try_new(
        evidence_id: SourceIdentifier,
        digest: EvidenceDigest,
        effective_at: Timestamp,
        expires_at: Option<Timestamp>,
    ) -> Result<Self, SourceLifecycleError> {
        if !valid_sha256(digest) || expires_at.is_some_and(|expires| expires <= effective_at) {
            return Err(SourceLifecycleError::InvalidResult);
        }
        Ok(Self {
            evidence_id,
            digest,
            effective_at,
            expires_at,
        })
    }

    /// Returns stable evidence identity.
    pub const fn evidence_id(&self) -> &SourceIdentifier {
        &self.evidence_id
    }

    /// Returns exact evidence digest.
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }

    /// Returns the first effective instant.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns immutable expiry when one exists.
    pub const fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }
}

/// Construction input for one safe immutable lifecycle receipt.
#[derive(Clone, Eq, PartialEq)]
pub struct SourceLifecycleReceiptInput {
    /// Idempotency/audit identity of the applied operation.
    pub operation_id: SourceIdentifier,
    /// Exact provider surface.
    pub provider: SourceIdentifier,
    /// Requested action.
    pub action: SourceLifecycleAction,
    /// Applied/replay/rejected/reconciliation disposition.
    pub disposition: SourceLifecycleDisposition,
    /// Resulting public lifecycle state.
    pub state: SourceLifecycleState,
    /// State revision after the operation.
    pub state_revision: NonZeroU64,
    /// Generation before the operation when a live generation existed.
    pub previous_generation: Option<ConnectionGeneration>,
    /// Current generation after the operation when one exists.
    pub current_generation: Option<ConnectionGeneration>,
    /// Exact callable research-runtime generation identity when this is not a live stream.
    pub runtime_generation_digest: Option<EvidenceDigest>,
    /// Current coverage conclusion.
    pub coverage: Option<CoverageStatus>,
    /// Current stream-integrity conclusion.
    pub integrity: Option<StreamIntegrityState>,
    /// Current data-quality conclusion.
    pub quality: Option<DataQuality>,
    /// Shared provider-rate state.
    pub rate_budget: SourceRateBudgetState,
    /// Authorization/rights state.
    pub authorization: SourceAuthorizationState,
    /// Provider/runtime availability state.
    pub availability: SourceAvailabilityState,
    /// Immutable rights evidence when applicable.
    pub rights_evidence: Option<SourceRightsEvidence>,
    /// Current blocker when the result is not immediately usable.
    pub blocker: Option<SourceLifecycleBlocker>,
    /// Public configuration digest after start/reconfigure when applicable.
    pub public_configuration_digest: Option<EvidenceDigest>,
    /// Trusted observation time of the result.
    pub observed_at: Timestamp,
}

/// Secret-free lifecycle receipt returned by the sole source owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceLifecycleReceipt(SourceLifecycleReceiptInput);

impl SourceLifecycleReceipt {
    /// Validates generation and blocker consistency before publication.
    pub fn try_new(input: SourceLifecycleReceiptInput) -> Result<Self, SourceLifecycleError> {
        if input.action == SourceLifecycleAction::Resynchronize
            && input.disposition == SourceLifecycleDisposition::Applied
            && !matches!(
                (input.previous_generation, input.current_generation),
                (Some(previous), Some(current)) if current.get() > previous.get()
            )
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        if matches!(
            input.disposition,
            SourceLifecycleDisposition::Rejected
                | SourceLifecycleDisposition::ReconciliationRequired
        ) != input.blocker.is_some()
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        let live_evidence = [
            input.current_generation.is_some(),
            input.coverage.is_some(),
            input.integrity.is_some(),
            input.quality.is_some(),
        ];
        if live_evidence.iter().any(|present| *present)
            && !live_evidence.iter().all(|present| *present)
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        if input.state == SourceLifecycleState::Active
            && (input.current_generation.is_some() == input.runtime_generation_digest.is_some())
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        if input.state == SourceLifecycleState::Removed
            && (input.current_generation.is_some() || input.runtime_generation_digest.is_some())
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        if input
            .runtime_generation_digest
            .is_some_and(|digest| !valid_sha256(digest))
            || input
                .public_configuration_digest
                .is_some_and(|digest| !valid_sha256(digest))
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        Ok(Self(input))
    }

    /// Returns complete safe receipt fields for typed integration-owned projection.
    pub const fn fields(&self) -> &SourceLifecycleReceiptInput {
        &self.0
    }
}

/// Construction input for an exact credential-free lifecycle status read.
#[derive(Clone, Eq, PartialEq)]
pub struct SourceLifecycleStatusInput {
    /// Exact code-owned provider surface.
    pub provider: SourceIdentifier,
    /// Current compare-and-apply revision clients must use for their next mutation.
    pub state_revision: NonZeroU64,
    /// Current durable lifecycle state.
    pub state: SourceLifecycleState,
    /// Exact current live generation when this is a live stream.
    pub current_generation: Option<ConnectionGeneration>,
    /// Exact callable research-runtime generation when this is a research source.
    pub runtime_generation_digest: Option<EvidenceDigest>,
    /// Current public configuration identity when retained.
    pub public_configuration_digest: Option<EvidenceDigest>,
    /// Current fail-closed blocker when one exists.
    pub blocker: Option<SourceLifecycleBlocker>,
    /// Trusted observation time.
    pub observed_at: Timestamp,
}

/// Exact lifecycle status exposed through an exact-scoped `Source.GetStatus` read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceLifecycleStatus(SourceLifecycleStatusInput);

impl SourceLifecycleStatus {
    /// Validates active-runtime identity and blocker consistency.
    pub fn try_new(input: SourceLifecycleStatusInput) -> Result<Self, SourceLifecycleError> {
        if input.state == SourceLifecycleState::Active
            && (input.current_generation.is_some() == input.runtime_generation_digest.is_some())
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        if input.state != SourceLifecycleState::Active
            && (input.current_generation.is_some() || input.runtime_generation_digest.is_some())
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        if (input.state == SourceLifecycleState::Blocked) != input.blocker.is_some() {
            return Err(SourceLifecycleError::InvalidResult);
        }
        if input
            .runtime_generation_digest
            .is_some_and(|digest| !valid_sha256(digest))
            || input
                .public_configuration_digest
                .is_some_and(|digest| !valid_sha256(digest))
        {
            return Err(SourceLifecycleError::InvalidResult);
        }
        Ok(Self(input))
    }

    /// Returns complete safe fields for integration-owned projection.
    pub const fn fields(&self) -> &SourceLifecycleStatusInput {
        &self.0
    }
}

impl fmt::Debug for SourceLifecycleStatusInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceLifecycleStatusInput")
            .field("provider", &self.provider)
            .field("state_revision", &self.state_revision)
            .field("state", &self.state)
            .field("current_generation", &self.current_generation)
            .field(
                "has_runtime_generation_digest",
                &self.runtime_generation_digest.is_some(),
            )
            .field(
                "has_public_configuration_digest",
                &self.public_configuration_digest.is_some(),
            )
            .field("blocker", &self.blocker)
            .field("observed_at", &self.observed_at)
            .finish()
    }
}

impl fmt::Debug for SourceLifecycleReceiptInput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceLifecycleReceiptInput")
            .field("operation_id", &self.operation_id)
            .field("provider", &self.provider)
            .field("action", &self.action)
            .field("disposition", &self.disposition)
            .field("state", &self.state)
            .field("state_revision", &self.state_revision)
            .field("previous_generation", &self.previous_generation)
            .field("current_generation", &self.current_generation)
            .field(
                "has_runtime_generation_digest",
                &self.runtime_generation_digest.is_some(),
            )
            .field("coverage", &self.coverage)
            .field("integrity", &self.integrity)
            .field("quality", &self.quality)
            .field("rate_budget", &self.rate_budget)
            .field("authorization", &self.authorization)
            .field("availability", &self.availability)
            .field("rights_evidence", &self.rights_evidence)
            .field("blocker", &self.blocker)
            .field(
                "has_public_configuration_digest",
                &self.public_configuration_digest.is_some(),
            )
            .field("observed_at", &self.observed_at)
            .finish()
    }
}

/// Bounded source-lifecycle boundary failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SourceLifecycleError {
    /// Action-specific request fields are inconsistent.
    #[error("source lifecycle request is invalid")]
    InvalidRequest,
    /// Authority returned inconsistent public evidence.
    #[error("source lifecycle result is invalid")]
    InvalidResult,
    /// Exact source or prepared configuration does not exist.
    #[error("source lifecycle target was not found")]
    NotFound,
    /// State revision or generation precondition is stale.
    #[error("source lifecycle precondition conflicts with current state")]
    Conflict,
    /// Current credential or rights evidence does not authorize the operation.
    #[error("source lifecycle operation is unauthorized")]
    Unauthorized,
    /// Provider budget currently prohibits the operation.
    #[error("source lifecycle provider budget is unavailable")]
    RateLimited,
    /// Request cancellation won the lifecycle race.
    #[error("source lifecycle operation was cancelled")]
    Cancelled,
    /// Request deadline elapsed.
    #[error("source lifecycle operation deadline elapsed")]
    DeadlineExceeded,
    /// Source owner is not currently available.
    #[error("source lifecycle authority is unavailable")]
    Unavailable,
    /// Durable state must be reconciled before another mutation.
    #[error("source lifecycle reconciliation is required")]
    ReconciliationRequired,
    /// Internal authority failed without caller-safe detail.
    #[error("source lifecycle operation failed")]
    Internal,
}

/// Sole source lifecycle owner injected by live/paper application composition.
#[async_trait]
pub trait SourceLifecycleAuthority: Send + Sync {
    /// Returns the exact number of currently active source runtimes when the owner can sample a
    /// coherent synchronous view.
    ///
    /// Implementations without a synchronous runtime-activity authority fail closed. Operations
    /// preflight must never infer zero from that absence.
    fn active_source_count(&self) -> Result<usize, SourceLifecycleError> {
        Err(SourceLifecycleError::Unavailable)
    }

    /// Returns the exact current lifecycle revision and runtime identity for one provider.
    async fn status(
        &self,
        provider: &SourceIdentifier,
        cancellation: &CancellationToken,
        deadline: Instant,
    ) -> Result<SourceLifecycleStatus, SourceLifecycleError>;

    /// Executes one compare-and-apply command and returns only safe immutable evidence.
    async fn execute(
        &self,
        command: SourceLifecycleCommand,
    ) -> Result<SourceLifecycleReceipt, SourceLifecycleError>;
}

fn valid_sha256(digest: EvidenceDigest) -> bool {
    digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes() != [0; 32]
}
