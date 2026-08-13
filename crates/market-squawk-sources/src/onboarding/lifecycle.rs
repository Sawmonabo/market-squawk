//! Generation-bound provider onboarding authority.

use market_squawk_domain::{EvidenceDigest, SourceIdentifier, Timestamp};
use market_squawk_platform::{SecretGeneration, SecretMutationPlan, SecretRef};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use super::{
    AuthoritySet, ProviderCapability, ProviderCapabilityError, ProviderCapabilityRevision,
    RightsAdmissionState, RuntimeVerificationContext, RuntimeVerificationEvidence, SetupMode,
};
use crate::onboarding::capability::nonzero_digest;

const MAX_RETAINED_GENERATIONS: usize = 256;
const MAX_OPERATION_RETRY_BUDGET: u8 = 8;

/// Non-secret issuer, audience, resource, and account bindings observed by a verifier.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityBindings {
    issuer_digest: Option<EvidenceDigest>,
    audience_digest: Option<EvidenceDigest>,
    resource_digest: Option<EvidenceDigest>,
    account_digest: Option<EvidenceDigest>,
}

impl AuthorityBindings {
    /// Constructs exact non-secret authority bindings.
    pub const fn new(
        issuer_digest: Option<EvidenceDigest>,
        audience_digest: Option<EvidenceDigest>,
        resource_digest: Option<EvidenceDigest>,
        account_digest: Option<EvidenceDigest>,
    ) -> Self {
        Self {
            issuer_digest,
            audience_digest,
            resource_digest,
            account_digest,
        }
    }

    /// Returns the issuer binding, when the provider exposes one.
    pub const fn issuer_digest(self) -> Option<EvidenceDigest> {
        self.issuer_digest
    }

    /// Returns the audience binding, when the provider exposes one.
    pub const fn audience_digest(self) -> Option<EvidenceDigest> {
        self.audience_digest
    }

    /// Returns the resource binding, when the provider exposes one.
    pub const fn resource_digest(self) -> Option<EvidenceDigest> {
        self.resource_digest
    }

    /// Returns the provider account or portfolio binding, when applicable.
    pub const fn account_digest(self) -> Option<EvidenceDigest> {
        self.account_digest
    }

    fn validate(self) -> Result<(), OnboardingStateError> {
        if [
            self.issuer_digest,
            self.audience_digest,
            self.resource_digest,
            self.account_digest,
        ]
        .into_iter()
        .flatten()
        .all(nonzero_digest)
        {
            Ok(())
        } else {
            Err(OnboardingStateError::InvalidEvidence)
        }
    }
}

/// Validated input for one exact least-privilege verification.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityVerificationInput {
    /// Authority requested by the onboarding session.
    pub requested: AuthoritySet,
    /// Authority observed by the non-mutating provider verifier.
    pub observed: AuthoritySet,
    /// Digest of provider restrictions not expressible as authority names.
    pub restrictions_digest: EvidenceDigest,
    /// Non-secret issuer/audience/resource/account bindings.
    pub bindings: AuthorityBindings,
    /// Wall-clock time at which the provider response was verified.
    pub verified_at: Timestamp,
    /// Credential or verifier-result expiry, when known.
    pub expires_at: Option<Timestamp>,
    /// Exact code-owned verifier revision.
    pub verifier_revision: SourceIdentifier,
    /// Explicit limit on what this verifier result establishes.
    pub assurance_limitation: SourceIdentifier,
    /// Digest of the redacted verification evidence.
    pub evidence_digest: EvidenceDigest,
}

/// Exact requested and observed provider authority admitted for one generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(into = "AuthorityVerificationInput")]
pub struct AuthorityVerification {
    requested: AuthoritySet,
    observed: AuthoritySet,
    restrictions_digest: EvidenceDigest,
    bindings: AuthorityBindings,
    verified_at: Timestamp,
    expires_at: Option<Timestamp>,
    verifier_revision: SourceIdentifier,
    assurance_limitation: SourceIdentifier,
    evidence_digest: EvidenceDigest,
}

impl AuthorityVerification {
    /// Verifies exact authority and binds the result to one capability revision.
    pub fn try_new(
        capability: &ProviderCapability,
        input: AuthorityVerificationInput,
    ) -> Result<Self, OnboardingStateError> {
        if input.requested != input.observed
            || !capability
                .minimum_authority()
                .is_subset_of(&input.requested)
            || !input.requested.is_subset_of(capability.maximum_authority())
            || input.verifier_revision != *capability.verifier_revision()
            || !nonzero_digest(input.restrictions_digest)
            || !nonzero_digest(input.evidence_digest)
            || input
                .expires_at
                .is_some_and(|expires_at| expires_at <= input.verified_at)
        {
            return Err(OnboardingStateError::AuthorityDenied);
        }
        input.bindings.validate()?;
        Ok(Self {
            requested: input.requested,
            observed: input.observed,
            restrictions_digest: input.restrictions_digest,
            bindings: input.bindings,
            verified_at: input.verified_at,
            expires_at: input.expires_at,
            verifier_revision: input.verifier_revision,
            assurance_limitation: input.assurance_limitation,
            evidence_digest: input.evidence_digest,
        })
    }

    /// Returns the exact requested authority.
    pub const fn requested(&self) -> &AuthoritySet {
        &self.requested
    }

    /// Returns the exact observed authority.
    pub const fn observed(&self) -> &AuthoritySet {
        &self.observed
    }

    /// Returns the provider-restriction digest.
    pub const fn restrictions_digest(&self) -> EvidenceDigest {
        self.restrictions_digest
    }

    /// Returns the non-secret authority bindings.
    pub const fn bindings(&self) -> AuthorityBindings {
        self.bindings
    }

    /// Returns the verification time.
    pub const fn verified_at(&self) -> Timestamp {
        self.verified_at
    }

    /// Returns the provider expiry, when known.
    pub const fn expires_at(&self) -> Option<Timestamp> {
        self.expires_at
    }

    /// Returns the exact verifier revision.
    pub const fn verifier_revision(&self) -> &SourceIdentifier {
        &self.verifier_revision
    }

    /// Returns the verifier's explicit assurance limitation.
    pub const fn assurance_limitation(&self) -> &SourceIdentifier {
        &self.assurance_limitation
    }

    /// Returns the redacted evidence digest.
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    fn revalidate(&self, capability: &ProviderCapability) -> Result<(), OnboardingStateError> {
        let rebuilt = Self::try_new(capability, self.clone().into())?;
        if rebuilt == *self {
            Ok(())
        } else {
            Err(OnboardingStateError::InvalidEvidence)
        }
    }

    fn valid_at(&self, observed_at: Timestamp) -> bool {
        self.verified_at <= observed_at
            && self
                .expires_at
                .is_none_or(|expires_at| observed_at < expires_at)
    }
}

impl From<AuthorityVerification> for AuthorityVerificationInput {
    fn from(verification: AuthorityVerification) -> Self {
        Self {
            requested: verification.requested,
            observed: verification.observed,
            restrictions_digest: verification.restrictions_digest,
            bindings: verification.bindings,
            verified_at: verification.verified_at,
            expires_at: verification.expires_at,
            verifier_revision: verification.verifier_revision,
            assurance_limitation: verification.assurance_limitation,
            evidence_digest: verification.evidence_digest,
        }
    }
}

impl<'de> Deserialize<'de> for AuthorityVerification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = AuthorityVerificationInput::deserialize(deserializer)?;
        if input.requested != input.observed
            || !nonzero_digest(input.restrictions_digest)
            || !nonzero_digest(input.evidence_digest)
            || input
                .expires_at
                .is_some_and(|expires_at| expires_at <= input.verified_at)
        {
            return Err(serde::de::Error::custom("invalid authority verification"));
        }
        input
            .bindings
            .validate()
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            requested: input.requested,
            observed: input.observed,
            restrictions_digest: input.restrictions_digest,
            bindings: input.bindings,
            verified_at: input.verified_at,
            expires_at: input.expires_at,
            verifier_revision: input.verifier_revision,
            assurance_limitation: input.assurance_limitation,
            evidence_digest: input.evidence_digest,
        })
    }
}

/// Durable onboarding state exposed to control-plane callers.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingState {
    /// Required capability evidence or runtime availability is absent.
    Unavailable,
    /// A no-secret surface is admitted but not yet activated.
    AnonymousAvailable,
    /// A provider-controlled action must be completed by the user.
    UserActionRequired,
    /// A credential was imported but has not been protocol-validated.
    CredentialImportedUnverified,
    /// Credential structure or protocol exchange was validated.
    ProtocolValidated,
    /// One exact credential generation is stored but unverified.
    StoredUnverified,
    /// A durable exact store plan may require local reconciliation.
    SecretReconciliationRequired,
    /// Requested authority exactly matched observed authority.
    VerifiedLeastPrivilege,
    /// Rights and rate-policy admission remain incomplete.
    RightsAdmissionPending,
    /// Runtime verification or final activation remains incomplete.
    RuntimeVerificationPending,
    /// The exact active generation is admitted for scoped use.
    ActiveScoped,
    /// The active credential reached its provider-verification renewal boundary.
    RenewalRequired,
    /// Evidence change invalidated activation until review.
    RefreshRequired,
    /// A candidate generation is being prepared while the prior remains active.
    RotationPending,
    /// Remote invalidation of an old credential is not confirmed.
    RevocationUnconfirmed,
    /// A mutating remote operation has unknown completion state.
    IndeterminateRemoteState,
    /// Exact local or remote cleanup remains required.
    CleanupRequired,
    /// An authority, rights, policy, or cancellation gate blocks activation.
    Blocked,
}

impl OnboardingState {
    /// Returns the stable catalog representation.
    pub const fn database_name(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::AnonymousAvailable => "anonymous_available",
            Self::UserActionRequired => "user_action_required",
            Self::CredentialImportedUnverified => "credential_imported_unverified",
            Self::ProtocolValidated => "protocol_validated",
            Self::StoredUnverified => "stored_unverified",
            Self::SecretReconciliationRequired => "secret_reconciliation_required",
            Self::VerifiedLeastPrivilege => "verified_least_privilege",
            Self::RightsAdmissionPending => "rights_admission_pending",
            Self::RuntimeVerificationPending => "runtime_verification_pending",
            Self::ActiveScoped => "active_scoped",
            Self::RenewalRequired => "renewal_required",
            Self::RefreshRequired => "refresh_required",
            Self::RotationPending => "rotation_pending",
            Self::RevocationUnconfirmed => "revocation_unconfirmed",
            Self::IndeterminateRemoteState => "indeterminate_remote_state",
            Self::CleanupRequired => "cleanup_required",
            Self::Blocked => "blocked",
        }
    }
}

/// Per-generation credential lifecycle state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialGenerationState {
    /// Catalog authority reserved this generation before secret storage.
    Reserved,
    /// Exact backend selection was durably planned before any mutation.
    StorePlanned,
    /// The planned target may exist and requires exact reconciliation.
    StoreReconciliationRequired,
    /// The exact generation was stored but not authority-verified.
    StoredUnverified,
    /// Least-privilege authority was verified.
    VerifiedLeastPrivilege,
    /// This exact generation is current execution authority.
    ActiveScoped,
    /// A newer generation is active and this one is retained for safe cleanup.
    SupersededRetained,
    /// This generation completed the required retirement facts.
    Retired,
    /// This generation is an immutable catalog tombstone.
    Tombstoned,
    /// A never-stored candidate was abandoned without external effect.
    AbandonedNoEffect,
    /// Exact cleanup of this generation is incomplete.
    CleanupRequired,
}

/// Result of a remote old-credential revocation attempt.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteRevocationOutcome {
    /// The provider confirmed remote revocation.
    Confirmed,
    /// The provider confirmed that the credential no longer exists.
    NotFound,
    /// The admitted provider surface has no remote revocation operation.
    Unsupported,
    /// The provider returned a determinate failure.
    Failed,
    /// A mutating attempt may have completed but cannot yet be reconciled.
    Indeterminate,
}

/// Result of exact local deletion for one opaque generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalDeletionOutcome {
    /// The exact local item was deleted.
    Deleted,
    /// The exact local item was already absent.
    NotFound,
    /// The backend returned a determinate failure.
    Failed,
    /// Deletion may have completed but cannot yet be reconciled.
    Indeterminate,
}

/// Exact no-debt outcome when clearing a planned secret mutation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretStoreClearOutcome {
    /// The exact planned target was confirmed absent.
    Absent,
    /// The exact planned target was deleted.
    Deleted,
}

/// Stable event class used by catalog indexing and audit summaries.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingEventKind {
    /// User-provided credential material entered the secure-store boundary.
    CredentialImported,
    /// Exact backend selection was persisted before secret mutation.
    SecretStorePlanned,
    /// The exact planned target requires foreground reconciliation.
    SecretStoreReconciliationRequired,
    /// A planned target was confirmed absent or deleted.
    SecretStoreCleared,
    /// The admitted credential protocol completed validation.
    ProtocolValidated,
    /// The exact opaque secret reference was durably stored.
    CredentialStored,
    /// Exact requested and observed authority was admitted.
    AuthorityVerified,
    /// Requested-use rights were admitted.
    RightsAdmitted,
    /// The exact rate policy was admitted.
    RatePolicyAdmitted,
    /// A bounded runtime verification succeeded.
    RuntimeVerified,
    /// The exact generation or anonymous surface became active.
    Activate,
    /// The active credential reached its verified renewal boundary.
    RenewalRequired,
    /// A higher candidate generation was reserved.
    BeginRotation,
    /// A reserved candidate was abandoned before any external mutation.
    CandidateCancelledNoEffect,
    /// Authority moved atomically to the prepared candidate.
    Cutover,
    /// Remote revocation produced a separately recorded outcome.
    RemoteRevocation,
    /// Exact local secret deletion produced a separately recorded outcome.
    LocalDeletion,
    /// A superseded generation completed retirement.
    Retire,
    /// A retired generation became an immutable tombstone.
    Tombstone,
    /// Capability evidence requires review.
    RefreshRequired,
    /// Runtime or capability evidence made the surface unavailable.
    Unavailable,
    /// An external mutation has indeterminate completion.
    IndeterminateRemoteState,
    /// Exact cleanup remains required.
    CleanupRequired,
    /// Runtime activation was quarantined and all retained secret authority was revoked.
    ActivationQuarantined,
    /// A policy or authority gate blocked the session.
    Blocked,
    /// Cancellation permanently blocked later activation.
    Cancelled,
}

impl OnboardingEventKind {
    /// Returns the stable catalog representation.
    pub const fn database_name(self) -> &'static str {
        match self {
            Self::CredentialImported => "credential_imported",
            Self::SecretStorePlanned => "secret_store_planned",
            Self::SecretStoreReconciliationRequired => "secret_store_reconciliation_required",
            Self::SecretStoreCleared => "secret_store_cleared",
            Self::ProtocolValidated => "protocol_validated",
            Self::CredentialStored => "credential_stored",
            Self::AuthorityVerified => "authority_verified",
            Self::RightsAdmitted => "rights_admitted",
            Self::RatePolicyAdmitted => "rate_policy_admitted",
            Self::RuntimeVerified => "runtime_verified",
            Self::Activate => "activate",
            Self::RenewalRequired => "renewal_required",
            Self::BeginRotation => "begin_rotation",
            Self::CandidateCancelledNoEffect => "candidate_cancelled_no_effect",
            Self::Cutover => "cutover",
            Self::RemoteRevocation => "remote_revocation",
            Self::LocalDeletion => "local_deletion",
            Self::Retire => "retire",
            Self::Tombstone => "tombstone",
            Self::RefreshRequired => "refresh_required",
            Self::Unavailable => "unavailable",
            Self::IndeterminateRemoteState => "indeterminate_remote_state",
            Self::CleanupRequired => "cleanup_required",
            Self::ActivationQuarantined => "activation_quarantined",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
        }
    }
}

/// One non-secret, generation-bound onboarding transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OnboardingEvent {
    /// Secret material crossed directly into the secure-store operation.
    CredentialImported {
        /// Reserved generation receiving the credential.
        generation: SecretGeneration,
        /// Digest of redacted import evidence.
        evidence_digest: EvidenceDigest,
    },
    /// Freezes one exact backend and locator before credential bytes can be written.
    SecretStorePlanned {
        /// Durable non-secret exact mutation plan.
        plan: SecretMutationPlan,
        /// Digest of redacted import and planning evidence.
        evidence_digest: EvidenceDigest,
    },
    /// Records that a durable plan may have been applied but was not catalog-committed.
    SecretStoreReconciliationRequired {
        /// Exact candidate generation.
        generation: SecretGeneration,
        /// Digest of redacted reconciliation evidence.
        evidence_digest: EvidenceDigest,
    },
    /// Clears a durable plan only after exact absence or deletion is known.
    SecretStoreCleared {
        /// Exact candidate generation.
        generation: SecretGeneration,
        /// Exact planned target.
        reference: SecretRef,
        /// Determinate no-debt outcome.
        outcome: SecretStoreClearOutcome,
    },
    /// The provider-specific credential protocol was validated.
    ProtocolValidated {
        /// Generation being validated.
        generation: SecretGeneration,
        /// Digest of redacted protocol evidence.
        evidence_digest: EvidenceDigest,
    },
    /// Exact secure storage completed.
    CredentialStored {
        /// Opaque backend reference; never credential material.
        reference: SecretRef,
    },
    /// Exact authority verification completed.
    AuthorityVerified {
        /// Validated requested-versus-observed authority.
        verification: Box<AuthorityVerification>,
    },
    /// Requested-use rights were admitted.
    RightsAdmitted {
        /// `None` identifies a no-secret surface.
        generation: Option<SecretGeneration>,
        /// Exact rights-decision digest.
        decision_digest: EvidenceDigest,
    },
    /// Endpoint-class rate policy was admitted.
    RatePolicyAdmitted {
        /// `None` identifies a no-secret surface.
        generation: Option<SecretGeneration>,
        /// Exact admitted policy evidence digest.
        policy_digest: EvidenceDigest,
    },
    /// A bounded provider runtime verification succeeded.
    RuntimeVerified {
        /// `None` identifies a no-secret surface.
        generation: Option<SecretGeneration>,
        /// Closed full runtime evidence, or the legacy digest wrapper for other profiles.
        evidence: RuntimeVerificationEvidence,
    },
    /// Activates only an already fully admitted generation or anonymous surface.
    Activate {
        /// `None` identifies a no-secret surface.
        generation: Option<SecretGeneration>,
    },
    /// Records the exact expiry that suspended active credential authority.
    RenewalRequired {
        /// Exact active generation requiring replacement.
        generation: SecretGeneration,
        /// Exclusive expiry retained by its authority verification.
        expires_at: Timestamp,
        /// Digest of the non-secret renewal decision.
        evidence_digest: EvidenceDigest,
    },
    /// Reserves a higher candidate while retaining active authority.
    BeginRotation {
        /// Exact next credential generation.
        candidate_generation: SecretGeneration,
        /// Unique owner of the bounded rotation operation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_owner: Option<SourceIdentifier>,
        /// Fixed wall-clock counterpart to the operation's monotonic runtime deadline.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deadline_at: Option<Timestamp>,
        /// Maximum retry count for this exact operation.
        #[serde(default)]
        retry_budget: u8,
    },
    /// Cancels a reserved candidate only when no plan, reference, or mutation debt exists.
    CandidateCancelledNoEffect {
        /// Exact candidate generation.
        generation: SecretGeneration,
        /// Digest of the no-effect cancellation decision.
        evidence_digest: EvidenceDigest,
    },
    /// Moves authority to a fully admitted candidate.
    Cutover {
        /// Exact currently active generation.
        prior_generation: SecretGeneration,
        /// Exact fully admitted candidate generation.
        candidate_generation: SecretGeneration,
    },
    /// Records remote revocation independently of local deletion.
    RemoteRevocation {
        /// Superseded generation.
        generation: SecretGeneration,
        /// Typed provider result.
        outcome: RemoteRevocationOutcome,
        /// Digest of redacted provider evidence.
        evidence_digest: EvidenceDigest,
    },
    /// Records local deletion independently of remote revocation.
    LocalDeletion {
        /// Exact opaque generation targeted.
        generation: SecretGeneration,
        /// Typed secret-store result.
        outcome: LocalDeletionOutcome,
    },
    /// Retires a superseded generation after exact local cleanup.
    Retire {
        /// Exact superseded generation.
        generation: SecretGeneration,
    },
    /// Creates an immutable terminal tombstone.
    Tombstone {
        /// Exact retired generation.
        generation: SecretGeneration,
    },
    /// Invalidates activation after an evidence change.
    RefreshRequired {
        /// Exact evidence-change digest.
        evidence_digest: EvidenceDigest,
    },
    /// Marks an unadmitted or runtime-unavailable surface.
    Unavailable {
        /// Exact availability evidence digest.
        evidence_digest: EvidenceDigest,
    },
    /// Records an unresolved mutating remote result.
    IndeterminateRemoteState {
        /// Affected credential generation, when one exists.
        generation: Option<SecretGeneration>,
        /// Digest of redacted reconciliation evidence.
        evidence_digest: EvidenceDigest,
    },
    /// Records exact cleanup debt.
    CleanupRequired {
        /// Affected credential generation, when one exists.
        generation: Option<SecretGeneration>,
        /// Digest of redacted cleanup evidence.
        evidence_digest: EvidenceDigest,
    },
    /// Revokes runtime activation and establishes cleanup authority for every retained secret.
    ActivationQuarantined {
        /// Digest of the exact quarantined runtime or activation-recipe state.
        evidence_digest: EvidenceDigest,
    },
    /// Blocks activation on an admitted policy decision.
    Blocked {
        /// Digest of the non-secret blocking decision.
        evidence_digest: EvidenceDigest,
    },
    /// Records terminal operation cancellation.
    Cancelled {
        /// Digest of non-secret cancellation evidence.
        evidence_digest: EvidenceDigest,
    },
}

impl OnboardingEvent {
    /// Returns the stable event class.
    pub const fn kind(&self) -> OnboardingEventKind {
        match self {
            Self::CredentialImported { .. } => OnboardingEventKind::CredentialImported,
            Self::SecretStorePlanned { .. } => OnboardingEventKind::SecretStorePlanned,
            Self::SecretStoreReconciliationRequired { .. } => {
                OnboardingEventKind::SecretStoreReconciliationRequired
            }
            Self::SecretStoreCleared { .. } => OnboardingEventKind::SecretStoreCleared,
            Self::ProtocolValidated { .. } => OnboardingEventKind::ProtocolValidated,
            Self::CredentialStored { .. } => OnboardingEventKind::CredentialStored,
            Self::AuthorityVerified { .. } => OnboardingEventKind::AuthorityVerified,
            Self::RightsAdmitted { .. } => OnboardingEventKind::RightsAdmitted,
            Self::RatePolicyAdmitted { .. } => OnboardingEventKind::RatePolicyAdmitted,
            Self::RuntimeVerified { .. } => OnboardingEventKind::RuntimeVerified,
            Self::Activate { .. } => OnboardingEventKind::Activate,
            Self::RenewalRequired { .. } => OnboardingEventKind::RenewalRequired,
            Self::BeginRotation { .. } => OnboardingEventKind::BeginRotation,
            Self::CandidateCancelledNoEffect { .. } => {
                OnboardingEventKind::CandidateCancelledNoEffect
            }
            Self::Cutover { .. } => OnboardingEventKind::Cutover,
            Self::RemoteRevocation { .. } => OnboardingEventKind::RemoteRevocation,
            Self::LocalDeletion { .. } => OnboardingEventKind::LocalDeletion,
            Self::Retire { .. } => OnboardingEventKind::Retire,
            Self::Tombstone { .. } => OnboardingEventKind::Tombstone,
            Self::RefreshRequired { .. } => OnboardingEventKind::RefreshRequired,
            Self::Unavailable { .. } => OnboardingEventKind::Unavailable,
            Self::IndeterminateRemoteState { .. } => OnboardingEventKind::IndeterminateRemoteState,
            Self::CleanupRequired { .. } => OnboardingEventKind::CleanupRequired,
            Self::ActivationQuarantined { .. } => OnboardingEventKind::ActivationQuarantined,
            Self::Blocked { .. } => OnboardingEventKind::Blocked,
            Self::Cancelled { .. } => OnboardingEventKind::Cancelled,
        }
    }

    /// Returns canonical validated JSON for append-only catalog storage.
    pub fn canonical_json(&self) -> Result<Vec<u8>, OnboardingStateError> {
        serde_json::to_vec(self).map_err(|_| OnboardingStateError::Serialization)
    }

    /// Parses bounded catalog JSON; state application performs capability revalidation.
    pub fn try_from_json(bytes: &[u8]) -> Result<Self, OnboardingStateError> {
        if bytes.len() > 65_536 {
            return Err(OnboardingStateError::ResourceLimit);
        }
        serde_json::from_slice(bytes).map_err(|_| OnboardingStateError::Serialization)
    }

    /// Returns the primary generation affected by this event.
    pub const fn generation(&self) -> Option<SecretGeneration> {
        match self {
            Self::CredentialImported { generation, .. }
            | Self::SecretStoreReconciliationRequired { generation, .. }
            | Self::SecretStoreCleared { generation, .. }
            | Self::CandidateCancelledNoEffect { generation, .. }
            | Self::ProtocolValidated { generation, .. }
            | Self::RemoteRevocation { generation, .. }
            | Self::LocalDeletion { generation, .. }
            | Self::Retire { generation }
            | Self::Tombstone { generation }
            | Self::RenewalRequired { generation, .. } => Some(*generation),
            Self::CredentialStored { reference } => Some(reference.generation()),
            Self::SecretStorePlanned { plan, .. } => Some(plan.target().generation()),
            Self::AuthorityVerified { .. } => None,
            Self::RightsAdmitted { generation, .. }
            | Self::RatePolicyAdmitted { generation, .. }
            | Self::RuntimeVerified { generation, .. }
            | Self::Activate { generation }
            | Self::IndeterminateRemoteState { generation, .. }
            | Self::CleanupRequired { generation, .. } => *generation,
            Self::BeginRotation {
                candidate_generation,
                operation_owner: _,
                deadline_at: _,
                retry_budget: _,
            } => Some(*candidate_generation),
            Self::Cutover {
                candidate_generation,
                ..
            } => Some(*candidate_generation),
            Self::RefreshRequired { .. }
            | Self::Unavailable { .. }
            | Self::ActivationQuarantined { .. }
            | Self::Blocked { .. }
            | Self::Cancelled { .. } => None,
        }
    }

    /// Returns the prior generation named by a cutover.
    pub const fn prior_generation(&self) -> Option<SecretGeneration> {
        match self {
            Self::Cutover {
                prior_generation, ..
            } => Some(*prior_generation),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct GenerationRecord {
    generation: SecretGeneration,
    state: CredentialGenerationState,
    store_plan: Option<SecretMutationPlan>,
    reference: Option<SecretRef>,
    verification: Option<AuthorityVerification>,
    rights_digest: Option<EvidenceDigest>,
    rate_policy_digest: Option<EvidenceDigest>,
    runtime_evidence: Option<RuntimeVerificationEvidence>,
    remote_revocation: Option<RemoteRevocationOutcome>,
    local_deletion: Option<LocalDeletionOutcome>,
}

impl GenerationRecord {
    const fn reserved(generation: SecretGeneration) -> Self {
        Self {
            generation,
            state: CredentialGenerationState::Reserved,
            store_plan: None,
            reference: None,
            verification: None,
            rights_digest: None,
            rate_policy_digest: None,
            runtime_evidence: None,
            remote_revocation: None,
            local_deletion: None,
        }
    }

    fn fully_admitted(&self, capability: &ProviderCapability, observed_at: Timestamp) -> bool {
        let authority_is_current = match self.runtime_evidence.as_ref() {
            Some(RuntimeVerificationEvidence::AlpacaPaperIexDoctorReceiptV1(_)) => {
                self.verification.is_some()
            }
            _ => self
                .verification
                .as_ref()
                .is_some_and(|verification| verification.valid_at(observed_at)),
        };
        self.store_plan.is_none()
            && self.reference.is_some()
            && authority_is_current
            && self.rights_digest.is_some()
            && self.rate_policy_digest == Some(capability.rate_policy().evidence_digest())
            && self
                .runtime_evidence
                .as_ref()
                .is_some_and(|evidence| evidence.admits_activation_at(observed_at))
    }
}

/// Pure, replayable onboarding authority for one capability revision and requested use.
#[derive(Clone, Debug)]
pub struct OnboardingLifecycle {
    surface_id: SourceIdentifier,
    capability_revision: ProviderCapabilityRevision,
    capability_digest: EvidenceDigest,
    setup_mode: SetupMode,
    requested_authority: AuthoritySet,
    runtime_verification_context: Option<RuntimeVerificationContext>,
    state: OnboardingState,
    generations: Vec<GenerationRecord>,
    active_generation: Option<SecretGeneration>,
    candidate_generation: Option<SecretGeneration>,
    rotation_operation_owner: Option<SourceIdentifier>,
    rotation_deadline_at: Option<Timestamp>,
    rotation_retry_budget: u8,
    rotation_started_from_renewal: bool,
    anonymous_rights_digest: Option<EvidenceDigest>,
    anonymous_rate_policy_digest: Option<EvidenceDigest>,
    anonymous_runtime_evidence: Option<RuntimeVerificationEvidence>,
    cancelled: bool,
}

impl OnboardingLifecycle {
    /// Reserves the first generation or creates a no-secret pending admission.
    pub fn reserve(
        capability: &ProviderCapability,
        requested_authority: AuthoritySet,
    ) -> Result<Self, OnboardingStateError> {
        Self::reserve_inner(capability, requested_authority, None)
    }

    /// Reserves onboarding with immutable session/configuration coordinates for typed evidence.
    pub fn reserve_with_runtime_verification_context(
        capability: &ProviderCapability,
        requested_authority: AuthoritySet,
        runtime_verification_context: RuntimeVerificationContext,
    ) -> Result<Self, OnboardingStateError> {
        Self::reserve_inner(
            capability,
            requested_authority,
            Some(runtime_verification_context),
        )
    }

    fn reserve_inner(
        capability: &ProviderCapability,
        requested_authority: AuthoritySet,
        runtime_verification_context: Option<RuntimeVerificationContext>,
    ) -> Result<Self, OnboardingStateError> {
        if !capability
            .minimum_authority()
            .is_subset_of(&requested_authority)
            || !requested_authority.is_subset_of(capability.maximum_authority())
        {
            return Err(OnboardingStateError::AuthorityDenied);
        }
        let credentialed = capability.setup_mode() != SetupMode::NoCredential;
        let mut generations = Vec::new();
        let candidate_generation = if credentialed {
            generations
                .try_reserve_exact(1)
                .map_err(|_| OnboardingStateError::Allocation)?;
            let generation =
                SecretGeneration::new(1).map_err(|_| OnboardingStateError::InvalidTransition)?;
            generations.push(GenerationRecord::reserved(generation));
            Some(generation)
        } else {
            None
        };
        let state = if capability.rights_state() == RightsAdmissionState::Blocked {
            OnboardingState::Blocked
        } else if credentialed {
            OnboardingState::UserActionRequired
        } else {
            OnboardingState::AnonymousAvailable
        };
        Ok(Self {
            surface_id: capability.surface_id().clone(),
            capability_revision: capability.revision(),
            capability_digest: capability.content_digest(),
            setup_mode: capability.setup_mode(),
            requested_authority,
            runtime_verification_context,
            state,
            generations,
            active_generation: None,
            candidate_generation,
            rotation_operation_owner: None,
            rotation_deadline_at: None,
            rotation_retry_budget: 0,
            rotation_started_from_renewal: false,
            anonymous_rights_digest: None,
            anonymous_rate_policy_digest: None,
            anonymous_runtime_evidence: None,
            cancelled: false,
        })
    }

    /// Applies one validated event and returns the resulting durable state.
    pub fn apply(
        &mut self,
        capability: &ProviderCapability,
        event: OnboardingEvent,
        observed_at: Timestamp,
    ) -> Result<OnboardingState, OnboardingStateError> {
        self.ensure_capability(capability)?;
        if self.state == OnboardingState::RotationPending
            && rotation_progress_event(&event)
            && self
                .rotation_deadline_at
                .is_some_and(|deadline| observed_at >= deadline)
        {
            return Err(OnboardingStateError::DeadlineExceeded);
        }
        let runtime_renewal_allowed = self.state == OnboardingState::RenewalRequired
            && matches!(&event, OnboardingEvent::RuntimeVerified { .. });
        if matches!(
            self.state,
            OnboardingState::RenewalRequired
                | OnboardingState::RefreshRequired
                | OnboardingState::Unavailable
                | OnboardingState::SecretReconciliationRequired
                | OnboardingState::IndeterminateRemoteState
                | OnboardingState::CleanupRequired
                | OnboardingState::Blocked
        ) && !runtime_renewal_allowed
            && !matches!(
                event,
                OnboardingEvent::RefreshRequired { .. }
                    | OnboardingEvent::Unavailable { .. }
                    | OnboardingEvent::Blocked { .. }
                    | OnboardingEvent::Cancelled { .. }
                    | OnboardingEvent::CleanupRequired { .. }
                    | OnboardingEvent::ActivationQuarantined { .. }
                    | OnboardingEvent::IndeterminateRemoteState { .. }
                    | OnboardingEvent::RemoteRevocation { .. }
                    | OnboardingEvent::LocalDeletion { .. }
                    | OnboardingEvent::Retire { .. }
                    | OnboardingEvent::Tombstone { .. }
                    | OnboardingEvent::BeginRotation { .. }
                    | OnboardingEvent::RenewalRequired { .. }
                    | OnboardingEvent::SecretStoreReconciliationRequired { .. }
                    | OnboardingEvent::SecretStoreCleared { .. }
                    | OnboardingEvent::CredentialStored { .. }
                    | OnboardingEvent::CandidateCancelledNoEffect { .. }
            )
        {
            return Err(OnboardingStateError::InvalidTransition);
        }
        match event {
            OnboardingEvent::CredentialImported {
                generation,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                self.require_candidate(generation)?;
                let record = self.generation_mut(generation)?;
                if record.state != CredentialGenerationState::Reserved
                    || record.store_plan.is_some()
                    || record.reference.is_some()
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                self.state = self.pending_state(OnboardingState::CredentialImportedUnverified);
            }
            OnboardingEvent::SecretStorePlanned {
                plan,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                let generation = plan.target().generation();
                self.require_candidate(generation)?;
                let record = self.generation_mut(generation)?;
                if record.state != CredentialGenerationState::Reserved
                    || record.store_plan.is_some()
                    || record.reference.is_some()
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                record.store_plan = Some(plan);
                record.state = CredentialGenerationState::StorePlanned;
                self.state = self.pending_state(OnboardingState::CredentialImportedUnverified);
            }
            OnboardingEvent::SecretStoreReconciliationRequired {
                generation,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                self.require_candidate(generation)?;
                let record = self.generation_mut(generation)?;
                if !matches!(
                    record.state,
                    CredentialGenerationState::StorePlanned
                        | CredentialGenerationState::StoreReconciliationRequired
                ) || record.store_plan.is_none()
                    || record.reference.is_some()
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                record.state = CredentialGenerationState::StoreReconciliationRequired;
                self.state = OnboardingState::SecretReconciliationRequired;
            }
            OnboardingEvent::SecretStoreCleared {
                generation,
                reference,
                outcome: _,
            } => {
                let is_candidate = self.candidate_generation == Some(generation);
                let quarantined_cleanup =
                    !is_candidate && self.state == OnboardingState::CleanupRequired;
                if !is_candidate && !quarantined_cleanup {
                    return Err(OnboardingStateError::GenerationMismatch);
                }
                {
                    let record = self.generation_mut(generation)?;
                    if !matches!(
                        record.state,
                        CredentialGenerationState::StorePlanned
                            | CredentialGenerationState::StoreReconciliationRequired
                            | CredentialGenerationState::CleanupRequired
                    ) || record
                        .store_plan
                        .as_ref()
                        .is_none_or(|plan| plan.target() != &reference)
                        || record.reference.is_some()
                    {
                        return Err(OnboardingStateError::InvalidTransition);
                    }
                    record.store_plan = None;
                    record.state = if quarantined_cleanup {
                        CredentialGenerationState::AbandonedNoEffect
                    } else {
                        CredentialGenerationState::Reserved
                    };
                }
                self.state = if quarantined_cleanup {
                    self.cleanup_completion_state()
                } else {
                    self.pending_state(OnboardingState::UserActionRequired)
                };
            }
            OnboardingEvent::ProtocolValidated {
                generation,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                self.require_candidate(generation)?;
                let record = self.generation(generation)?;
                if record.state != CredentialGenerationState::Reserved
                    || record.store_plan.is_some()
                    || record.reference.is_some()
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                self.state = self.pending_state(OnboardingState::ProtocolValidated);
            }
            OnboardingEvent::CredentialStored { reference } => {
                let generation = reference.generation();
                self.require_candidate(generation)?;
                if self.active_generation.is_some()
                    && !capability.lifecycle_support().overlap_cutover()
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                let record = self.generation_mut(generation)?;
                let planned = record
                    .store_plan
                    .as_ref()
                    .is_some_and(|plan| plan.target() == &reference);
                let legacy_unplanned = record.store_plan.is_none()
                    && record.state == CredentialGenerationState::Reserved;
                if (!matches!(
                    record.state,
                    CredentialGenerationState::StorePlanned
                        | CredentialGenerationState::StoreReconciliationRequired
                        | CredentialGenerationState::CleanupRequired
                ) && !legacy_unplanned)
                    || (!planned && !legacy_unplanned)
                    || record.reference.is_some()
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                record.store_plan = None;
                record.reference = Some(reference);
                record.state = CredentialGenerationState::StoredUnverified;
                self.state = self.pending_state(OnboardingState::StoredUnverified);
            }
            OnboardingEvent::AuthorityVerified { verification } => {
                let generation = self
                    .candidate_generation
                    .ok_or(OnboardingStateError::InvalidTransition)?;
                verification.revalidate(capability)?;
                if verification.requested() != &self.requested_authority
                    || !verification.valid_at(observed_at)
                {
                    return Err(OnboardingStateError::AuthorityDenied);
                }
                let record = self.generation_mut(generation)?;
                if record.state != CredentialGenerationState::StoredUnverified
                    || record.verification.is_some()
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                record.verification = Some(*verification);
                record.state = CredentialGenerationState::VerifiedLeastPrivilege;
                self.state = self.pending_state(OnboardingState::VerifiedLeastPrivilege);
            }
            OnboardingEvent::RightsAdmitted {
                generation,
                decision_digest,
            } => {
                require_digest(decision_digest)?;
                if capability.rights_state() == RightsAdmissionState::Blocked {
                    return Err(OnboardingStateError::RightsDenied);
                }
                if let Some(generation) = generation {
                    self.require_candidate(generation)?;
                    let record = self.generation_mut(generation)?;
                    require_verified(record, observed_at)?;
                    record.rights_digest = Some(decision_digest);
                    self.state = self.pending_progress_state(generation)?;
                } else {
                    self.require_anonymous()?;
                    self.anonymous_rights_digest = Some(decision_digest);
                    self.state = self.anonymous_progress_state();
                }
            }
            OnboardingEvent::RatePolicyAdmitted {
                generation,
                policy_digest,
            } => {
                if policy_digest != capability.rate_policy().evidence_digest()
                    || !nonzero_digest(policy_digest)
                {
                    return Err(OnboardingStateError::EvidenceMismatch);
                }
                if let Some(generation) = generation {
                    self.require_candidate(generation)?;
                    let record = self.generation_mut(generation)?;
                    require_verified(record, observed_at)?;
                    record.rate_policy_digest = Some(policy_digest);
                    self.state = self.pending_progress_state(generation)?;
                } else {
                    self.require_anonymous()?;
                    self.anonymous_rate_policy_digest = Some(policy_digest);
                    self.state = self.anonymous_progress_state();
                }
            }
            OnboardingEvent::RuntimeVerified {
                generation,
                evidence,
            } => {
                if let Some(generation) = generation {
                    let targets_active_generation = self.active_generation == Some(generation);
                    if targets_active_generation && self.state != OnboardingState::RenewalRequired {
                        return Err(OnboardingStateError::InvalidTransition);
                    }
                    let renewal = targets_active_generation;
                    if renewal {
                        self.validate_runtime_renewal(
                            capability,
                            generation,
                            &evidence,
                            observed_at,
                        )?;
                        self.generation_mut(generation)?.runtime_evidence = Some(evidence);
                        self.state = OnboardingState::ActiveScoped;
                    } else {
                        self.require_candidate(generation)?;
                        self.validate_initial_runtime_evidence(
                            capability,
                            Some(generation),
                            &evidence,
                            observed_at,
                        )?;
                        let record = self.generation_mut(generation)?;
                        require_verified(record, observed_at)?;
                        if record.rights_digest.is_none()
                            || record.rate_policy_digest.is_none()
                            || record.runtime_evidence.is_some()
                        {
                            return Err(OnboardingStateError::InvalidTransition);
                        }
                        record.runtime_evidence = Some(evidence);
                        self.state =
                            self.pending_state(OnboardingState::RuntimeVerificationPending);
                    }
                } else {
                    self.require_anonymous()?;
                    if self.anonymous_rights_digest.is_none()
                        || self.anonymous_rate_policy_digest.is_none()
                        || self.anonymous_runtime_evidence.is_some()
                    {
                        return Err(OnboardingStateError::InvalidTransition);
                    }
                    self.validate_initial_runtime_evidence(
                        capability,
                        None,
                        &evidence,
                        observed_at,
                    )?;
                    self.anonymous_runtime_evidence = Some(evidence);
                    self.state = OnboardingState::RuntimeVerificationPending;
                }
            }
            OnboardingEvent::Activate { generation } => {
                if let Some(generation) = generation {
                    self.require_candidate(generation)?;
                    if self.active_generation.is_some() {
                        return Err(OnboardingStateError::InvalidTransition);
                    }
                    let record = self.generation_mut(generation)?;
                    if !record.fully_admitted(capability, observed_at) {
                        return Err(OnboardingStateError::InvalidTransition);
                    }
                    record.state = CredentialGenerationState::ActiveScoped;
                    self.active_generation = Some(generation);
                    self.candidate_generation = None;
                } else {
                    self.require_anonymous()?;
                    if self.anonymous_rights_digest.is_none()
                        || self.anonymous_rate_policy_digest.is_none()
                        || self
                            .anonymous_runtime_evidence
                            .as_ref()
                            .is_none_or(|evidence| !evidence.admits_activation_at(observed_at))
                    {
                        return Err(OnboardingStateError::InvalidTransition);
                    }
                }
                self.state = OnboardingState::ActiveScoped;
            }
            OnboardingEvent::RenewalRequired {
                generation,
                expires_at,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                let verification = self
                    .generation_verification(generation)
                    .ok_or(OnboardingStateError::InvalidTransition)?;
                let currentness_deadline = self
                    .generation_alpaca_paper_iex_doctor_receipt(generation)
                    .map(super::AlpacaPaperIexDoctorReceiptV1::exclusive_expires_at)
                    .or_else(|| verification.expires_at());
                if self.state != OnboardingState::ActiveScoped
                    || self.active_generation != Some(generation)
                    || currentness_deadline != Some(expires_at)
                    || observed_at < expires_at
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                self.state = OnboardingState::RenewalRequired;
            }
            OnboardingEvent::BeginRotation {
                candidate_generation,
                operation_owner,
                deadline_at,
                retry_budget,
            } => {
                let started_from_renewal = self.state == OnboardingState::RenewalRequired;
                if !capability.lifecycle_support().rotation()
                    || !matches!(
                        self.state,
                        OnboardingState::ActiveScoped | OnboardingState::RenewalRequired
                    )
                    || self.candidate_generation.is_some()
                    || retry_budget > MAX_OPERATION_RETRY_BUDGET
                    || deadline_at.is_some_and(|deadline| deadline <= observed_at)
                    || capability.rate_policy().enforcement_policy().is_some()
                        && (operation_owner.is_none() || deadline_at.is_none())
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                let current = self
                    .generations
                    .last()
                    .ok_or(OnboardingStateError::InvalidTransition)?
                    .generation;
                let expected = current
                    .get()
                    .checked_add(1)
                    .ok_or(OnboardingStateError::ResourceLimit)?;
                if candidate_generation.get() != expected
                    || self.generations.len() == MAX_RETAINED_GENERATIONS
                {
                    return Err(OnboardingStateError::ResourceLimit);
                }
                self.generations
                    .try_reserve_exact(1)
                    .map_err(|_| OnboardingStateError::Allocation)?;
                self.generations
                    .push(GenerationRecord::reserved(candidate_generation));
                self.candidate_generation = Some(candidate_generation);
                self.rotation_operation_owner = operation_owner;
                self.rotation_deadline_at = deadline_at;
                self.rotation_retry_budget = retry_budget;
                self.rotation_started_from_renewal = started_from_renewal;
                self.state = OnboardingState::RotationPending;
            }
            OnboardingEvent::CandidateCancelledNoEffect {
                generation,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                self.require_candidate(generation)?;
                let renewal_required = self.rotation_started_from_renewal;
                let active_exists = self.active_generation.is_some();
                let record = self.generation_mut(generation)?;
                if record.state != CredentialGenerationState::Reserved
                    || record.store_plan.is_some()
                    || record.reference.is_some()
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                record.state = CredentialGenerationState::AbandonedNoEffect;
                self.candidate_generation = None;
                self.clear_rotation_operation();
                self.state = if !active_exists {
                    OnboardingState::Blocked
                } else if renewal_required {
                    OnboardingState::RenewalRequired
                } else {
                    OnboardingState::ActiveScoped
                };
            }
            OnboardingEvent::Cutover {
                prior_generation,
                candidate_generation,
            } => {
                let overlap = capability.lifecycle_support().overlap_cutover();
                let prior_ready = if overlap {
                    self.active_generation == Some(prior_generation)
                        && self.generation(prior_generation)?.state
                            == CredentialGenerationState::ActiveScoped
                } else {
                    self.active_generation.is_none()
                        && self.generation(prior_generation)?.state
                            == CredentialGenerationState::SupersededRetained
                };
                if self.state != OnboardingState::RotationPending
                    || self.candidate_generation != Some(candidate_generation)
                    || !prior_ready
                    || !self
                        .generation(candidate_generation)?
                        .fully_admitted(capability, observed_at)
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                if overlap {
                    self.generation_mut(prior_generation)?.state =
                        CredentialGenerationState::SupersededRetained;
                }
                self.generation_mut(candidate_generation)?.state =
                    CredentialGenerationState::ActiveScoped;
                self.active_generation = Some(candidate_generation);
                self.candidate_generation = None;
                self.clear_rotation_operation();
                self.state = OnboardingState::ActiveScoped;
            }
            OnboardingEvent::RemoteRevocation {
                generation,
                outcome,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                let supports_remote_revocation = capability.lifecycle_support().remote_revocation();
                if (!supports_remote_revocation && outcome != RemoteRevocationOutcome::Unsupported)
                    || (supports_remote_revocation
                        && outcome == RemoteRevocationOutcome::Unsupported)
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                let generation_state = self.generation(generation)?.state;
                if self.active_generation == Some(generation) {
                    if self.candidate_generation.is_none()
                        || capability.lifecycle_support().overlap_cutover()
                        || !supports_remote_revocation
                        || generation_state != CredentialGenerationState::ActiveScoped
                    {
                        return Err(OnboardingStateError::InvalidTransition);
                    }
                    let record = self.generation_mut(generation)?;
                    record.remote_revocation = Some(outcome);
                    self.state = match outcome {
                        RemoteRevocationOutcome::Confirmed | RemoteRevocationOutcome::NotFound => {
                            record.state = CredentialGenerationState::SupersededRetained;
                            self.active_generation = None;
                            OnboardingState::RotationPending
                        }
                        RemoteRevocationOutcome::Unsupported => OnboardingState::RefreshRequired,
                        RemoteRevocationOutcome::Failed => OnboardingState::RevocationUnconfirmed,
                        RemoteRevocationOutcome::Indeterminate => {
                            OnboardingState::IndeterminateRemoteState
                        }
                    };
                } else if !matches!(
                    generation_state,
                    CredentialGenerationState::SupersededRetained
                        | CredentialGenerationState::CleanupRequired
                ) {
                    return Err(OnboardingStateError::InvalidTransition);
                } else {
                    self.generation_mut(generation)?.remote_revocation = Some(outcome);
                    self.state = if self.active_generation.is_none() {
                        OnboardingState::CleanupRequired
                    } else {
                        match (outcome, generation_state) {
                            (RemoteRevocationOutcome::Indeterminate, _) => {
                                OnboardingState::IndeterminateRemoteState
                            }
                            (_, CredentialGenerationState::CleanupRequired) => {
                                OnboardingState::CleanupRequired
                            }
                            (
                                RemoteRevocationOutcome::Confirmed
                                | RemoteRevocationOutcome::NotFound,
                                _,
                            ) => OnboardingState::ActiveScoped,
                            (RemoteRevocationOutcome::Unsupported, _)
                                if supports_remote_revocation =>
                            {
                                OnboardingState::RefreshRequired
                            }
                            (RemoteRevocationOutcome::Unsupported, _) => {
                                OnboardingState::ActiveScoped
                            }
                            (RemoteRevocationOutcome::Failed, _) => {
                                OnboardingState::RevocationUnconfirmed
                            }
                        }
                    };
                }
            }
            OnboardingEvent::LocalDeletion {
                generation,
                outcome,
            } => {
                if self.active_generation == Some(generation)
                    || !matches!(
                        self.generation(generation)?.state,
                        CredentialGenerationState::SupersededRetained
                            | CredentialGenerationState::CleanupRequired
                    )
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                let active_exists = self.active_generation.is_some();
                let record = self.generation_mut(generation)?;
                record.local_deletion = Some(outcome);
                if matches!(
                    outcome,
                    LocalDeletionOutcome::Failed | LocalDeletionOutcome::Indeterminate
                ) {
                    record.state = CredentialGenerationState::CleanupRequired;
                    self.state = OnboardingState::CleanupRequired;
                } else {
                    record.state = CredentialGenerationState::SupersededRetained;
                    self.state = if !active_exists {
                        OnboardingState::CleanupRequired
                    } else {
                        match record.remote_revocation {
                            Some(RemoteRevocationOutcome::Failed) => {
                                OnboardingState::RevocationUnconfirmed
                            }
                            Some(RemoteRevocationOutcome::Indeterminate) => {
                                OnboardingState::IndeterminateRemoteState
                            }
                            _ => OnboardingState::ActiveScoped,
                        }
                    };
                }
            }
            OnboardingEvent::Retire { generation } => {
                let record = self.generation_mut(generation)?;
                if record.state != CredentialGenerationState::SupersededRetained
                    || !matches!(
                        record.local_deletion,
                        Some(LocalDeletionOutcome::Deleted | LocalDeletionOutcome::NotFound)
                    )
                    || !matches!(
                        record.remote_revocation,
                        Some(
                            RemoteRevocationOutcome::Confirmed
                                | RemoteRevocationOutcome::NotFound
                                | RemoteRevocationOutcome::Unsupported
                        )
                    )
                {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                record.state = CredentialGenerationState::Retired;
            }
            OnboardingEvent::Tombstone { generation } => {
                let record = self.generation_mut(generation)?;
                if record.state != CredentialGenerationState::Retired {
                    return Err(OnboardingStateError::InvalidTransition);
                }
                record.state = CredentialGenerationState::Tombstoned;
                if self.candidate_generation == Some(generation) {
                    let renewal_required = self.rotation_started_from_renewal;
                    self.candidate_generation = None;
                    self.clear_rotation_operation();
                    self.state = if self.active_generation.is_some() {
                        if renewal_required {
                            OnboardingState::RenewalRequired
                        } else {
                            OnboardingState::ActiveScoped
                        }
                    } else {
                        OnboardingState::Blocked
                    };
                } else if self.active_generation.is_some() {
                    self.state = OnboardingState::ActiveScoped;
                } else {
                    self.state = self.cleanup_completion_state();
                }
            }
            OnboardingEvent::RefreshRequired { evidence_digest } => {
                require_digest(evidence_digest)?;
                self.state = OnboardingState::RefreshRequired;
            }
            OnboardingEvent::Unavailable { evidence_digest } => {
                require_digest(evidence_digest)?;
                self.state = OnboardingState::Unavailable;
            }
            OnboardingEvent::IndeterminateRemoteState {
                generation,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                if let Some(generation) = generation {
                    self.generation(generation)?;
                }
                self.state = OnboardingState::IndeterminateRemoteState;
            }
            OnboardingEvent::CleanupRequired {
                generation,
                evidence_digest,
            } => {
                require_digest(evidence_digest)?;
                if let Some(generation) = generation {
                    let active_is_target = self.active_generation == Some(generation);
                    let record = self.generation_mut(generation)?;
                    if record.reference.is_none() && record.store_plan.is_none() {
                        return Err(OnboardingStateError::InvalidTransition);
                    }
                    record.state = CredentialGenerationState::CleanupRequired;
                    if active_is_target {
                        self.active_generation = None;
                    }
                }
                self.state = OnboardingState::CleanupRequired;
            }
            OnboardingEvent::ActivationQuarantined { evidence_digest } => {
                require_digest(evidence_digest)?;
                let mut cleanup_required = false;
                for record in &mut self.generations {
                    match record.state {
                        CredentialGenerationState::Reserved => {
                            if record.reference.is_some() || record.store_plan.is_some() {
                                return Err(OnboardingStateError::InvalidTransition);
                            }
                            record.state = CredentialGenerationState::AbandonedNoEffect;
                        }
                        CredentialGenerationState::StorePlanned
                        | CredentialGenerationState::StoreReconciliationRequired
                        | CredentialGenerationState::StoredUnverified
                        | CredentialGenerationState::VerifiedLeastPrivilege
                        | CredentialGenerationState::ActiveScoped
                        | CredentialGenerationState::SupersededRetained
                        | CredentialGenerationState::CleanupRequired => {
                            if record.reference.is_none() && record.store_plan.is_none() {
                                return Err(OnboardingStateError::InvalidTransition);
                            }
                            record.state = CredentialGenerationState::CleanupRequired;
                            cleanup_required = true;
                        }
                        CredentialGenerationState::Retired
                        | CredentialGenerationState::Tombstoned
                        | CredentialGenerationState::AbandonedNoEffect => {}
                    }
                }
                self.active_generation = None;
                self.candidate_generation = None;
                self.clear_rotation_operation();
                self.state = if cleanup_required {
                    OnboardingState::CleanupRequired
                } else {
                    OnboardingState::Blocked
                };
            }
            OnboardingEvent::Blocked { evidence_digest } => {
                require_digest(evidence_digest)?;
                self.state = OnboardingState::Blocked;
            }
            OnboardingEvent::Cancelled { evidence_digest } => {
                require_digest(evidence_digest)?;
                self.cancelled = true;
                self.state = OnboardingState::Blocked;
            }
        }
        Ok(self.state)
    }

    /// Returns the provider/surface identity.
    pub const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    /// Returns the exact code-owned capability revision.
    pub const fn capability_revision(&self) -> ProviderCapabilityRevision {
        self.capability_revision
    }

    /// Returns the canonical capability digest.
    pub const fn capability_digest(&self) -> EvidenceDigest {
        self.capability_digest
    }

    /// Returns the exact admitted setup mode.
    pub const fn setup_mode(&self) -> SetupMode {
        self.setup_mode
    }

    /// Returns the exact requested authority.
    pub const fn requested_authority(&self) -> &AuthoritySet {
        &self.requested_authority
    }

    /// Returns immutable reservation coordinates used to fence typed runtime evidence.
    pub const fn runtime_verification_context(&self) -> Option<&RuntimeVerificationContext> {
        self.runtime_verification_context.as_ref()
    }

    /// Returns the current durable state.
    pub const fn state(&self) -> OnboardingState {
        self.state
    }

    /// Returns the exact active credential generation.
    pub const fn active_generation(&self) -> Option<SecretGeneration> {
        self.active_generation
    }

    /// Returns the exact reserved candidate generation.
    pub const fn candidate_generation(&self) -> Option<SecretGeneration> {
        self.candidate_generation
    }

    /// Returns the next contiguous credential generation without reserving it.
    pub fn next_generation(&self) -> Result<SecretGeneration, OnboardingStateError> {
        let next = match self.generations.last() {
            Some(record) => record
                .generation
                .get()
                .checked_add(1)
                .ok_or(OnboardingStateError::ResourceLimit)?,
            None => 1,
        };
        SecretGeneration::new(next).map_err(|_| OnboardingStateError::ResourceLimit)
    }

    /// Returns the current rotation-operation owner.
    pub const fn rotation_operation_owner(&self) -> Option<&SourceIdentifier> {
        self.rotation_operation_owner.as_ref()
    }

    /// Returns the fixed wall-clock deadline for the current rotation.
    pub const fn rotation_deadline_at(&self) -> Option<Timestamp> {
        self.rotation_deadline_at
    }

    /// Returns the bounded retry ceiling for the current rotation.
    pub const fn rotation_retry_budget(&self) -> Option<u8> {
        if self.candidate_generation.is_some() {
            Some(self.rotation_retry_budget)
        } else {
            None
        }
    }

    /// Returns the retained state for one exact generation.
    pub fn generation_state(
        &self,
        generation: SecretGeneration,
    ) -> Option<CredentialGenerationState> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .map(|record| record.state)
    }

    /// Returns the opaque secret reference for one retained generation.
    pub fn generation_reference(&self, generation: SecretGeneration) -> Option<&SecretRef> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.reference.as_ref())
    }

    /// Returns the durable exact mutation plan for an unfinished candidate store.
    pub fn generation_store_plan(
        &self,
        generation: SecretGeneration,
    ) -> Option<&SecretMutationPlan> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.store_plan.as_ref())
    }

    /// Returns the exact local target that may require reconciliation or deletion.
    pub fn generation_cleanup_reference(&self, generation: SecretGeneration) -> Option<&SecretRef> {
        self.generation_reference(generation).or_else(|| {
            self.generation_store_plan(generation)
                .map(SecretMutationPlan::target)
        })
    }

    /// Returns retained least-privilege verification for one exact generation.
    pub fn generation_verification(
        &self,
        generation: SecretGeneration,
    ) -> Option<&AuthorityVerification> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.verification.as_ref())
    }

    /// Returns the retained rights admission for one exact generation.
    pub fn generation_rights_digest(&self, generation: SecretGeneration) -> Option<EvidenceDigest> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.rights_digest)
    }

    /// Returns the retained rate-policy admission for one exact generation.
    pub fn generation_rate_policy_digest(
        &self,
        generation: SecretGeneration,
    ) -> Option<EvidenceDigest> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.rate_policy_digest)
    }

    /// Returns the retained runtime verification for one exact generation.
    pub fn generation_runtime_digest(
        &self,
        generation: SecretGeneration,
    ) -> Option<EvidenceDigest> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.runtime_evidence.as_ref())
            .map(RuntimeVerificationEvidence::evidence_digest)
    }

    /// Returns the complete retained runtime verification for one exact generation.
    pub fn generation_runtime_evidence(
        &self,
        generation: SecretGeneration,
    ) -> Option<&RuntimeVerificationEvidence> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.runtime_evidence.as_ref())
    }

    /// Returns the typed Alpaca Paper/IEX doctor receipt for one exact generation.
    pub fn generation_alpaca_paper_iex_doctor_receipt(
        &self,
        generation: SecretGeneration,
    ) -> Option<&super::AlpacaPaperIexDoctorReceiptV1> {
        self.generation_runtime_evidence(generation)
            .and_then(RuntimeVerificationEvidence::alpaca_paper_iex_receipt)
    }

    /// Revalidates the exact active generation's complete admission at a trusted read time.
    pub fn active_generation_is_fully_admitted(
        &self,
        capability: &ProviderCapability,
        observed_at: Timestamp,
    ) -> Result<bool, OnboardingStateError> {
        self.ensure_capability(capability)?;
        let Some(generation) = self.active_generation else {
            return Ok(false);
        };
        Ok(self.state == OnboardingState::ActiveScoped
            && self
                .generation(generation)?
                .fully_admitted(capability, observed_at))
    }

    /// Returns the no-credential rights admission.
    pub const fn anonymous_rights_digest(&self) -> Option<EvidenceDigest> {
        self.anonymous_rights_digest
    }

    /// Returns the no-credential rate-policy admission.
    pub const fn anonymous_rate_policy_digest(&self) -> Option<EvidenceDigest> {
        self.anonymous_rate_policy_digest
    }

    /// Returns the no-credential runtime verification.
    pub fn anonymous_runtime_digest(&self) -> Option<EvidenceDigest> {
        self.anonymous_runtime_evidence
            .as_ref()
            .map(RuntimeVerificationEvidence::evidence_digest)
    }

    /// Returns the complete no-credential runtime verification.
    pub const fn anonymous_runtime_evidence(&self) -> Option<&RuntimeVerificationEvidence> {
        self.anonymous_runtime_evidence.as_ref()
    }

    /// Returns whether terminal cancellation was durably recorded after cleanup.
    pub const fn cancellation_recorded(&self) -> bool {
        self.cancelled
    }

    /// Returns the latest separately retained remote-revocation result.
    pub fn generation_remote_revocation(
        &self,
        generation: SecretGeneration,
    ) -> Option<RemoteRevocationOutcome> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.remote_revocation)
    }

    /// Returns the latest separately retained exact local-deletion result.
    pub fn generation_local_deletion(
        &self,
        generation: SecretGeneration,
    ) -> Option<LocalDeletionOutcome> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .and_then(|record| record.local_deletion)
    }

    /// Iterates every retained generation and state in ascending generation order.
    pub fn generation_states(
        &self,
    ) -> impl ExactSizeIterator<Item = (SecretGeneration, CredentialGenerationState)> + '_ {
        self.generations
            .iter()
            .map(|record| (record.generation, record.state))
    }

    /// Returns the exact rights-decision digest retained for the active authority.
    pub fn admitted_rights_digest(&self) -> Option<EvidenceDigest> {
        if let Some(generation) = self.active_generation {
            return self
                .generations
                .iter()
                .find(|record| record.generation == generation)
                .and_then(|record| record.rights_digest);
        }
        self.anonymous_rights_digest
    }

    /// Returns whether this exact generation currently has scoped authority.
    pub fn generation_is_active_scoped(&self, generation: SecretGeneration) -> bool {
        !matches!(
            self.state,
            OnboardingState::Unavailable
                | OnboardingState::RenewalRequired
                | OnboardingState::RefreshRequired
                | OnboardingState::IndeterminateRemoteState
                | OnboardingState::Blocked
        ) && self.active_generation == Some(generation)
            && self.generation_state(generation) == Some(CredentialGenerationState::ActiveScoped)
    }

    fn ensure_capability(
        &self,
        capability: &ProviderCapability,
    ) -> Result<(), OnboardingStateError> {
        if capability.surface_id() == &self.surface_id
            && capability.revision() == self.capability_revision
            && capability.content_digest() == self.capability_digest
            && capability.setup_mode() == self.setup_mode
        {
            Ok(())
        } else {
            Err(OnboardingStateError::CapabilityMismatch)
        }
    }

    fn require_candidate(&self, generation: SecretGeneration) -> Result<(), OnboardingStateError> {
        if self.candidate_generation == Some(generation) {
            Ok(())
        } else {
            Err(OnboardingStateError::GenerationMismatch)
        }
    }

    fn require_anonymous(&self) -> Result<(), OnboardingStateError> {
        if self.setup_mode == SetupMode::NoCredential
            && self.active_generation.is_none()
            && self.candidate_generation.is_none()
        {
            Ok(())
        } else {
            Err(OnboardingStateError::GenerationMismatch)
        }
    }

    fn generation(
        &self,
        generation: SecretGeneration,
    ) -> Result<&GenerationRecord, OnboardingStateError> {
        self.generations
            .iter()
            .find(|record| record.generation == generation)
            .ok_or(OnboardingStateError::GenerationMismatch)
    }

    fn generation_mut(
        &mut self,
        generation: SecretGeneration,
    ) -> Result<&mut GenerationRecord, OnboardingStateError> {
        self.generations
            .iter_mut()
            .find(|record| record.generation == generation)
            .ok_or(OnboardingStateError::GenerationMismatch)
    }

    fn pending_state(&self, ordinary: OnboardingState) -> OnboardingState {
        if self.active_generation.is_some() {
            OnboardingState::RotationPending
        } else {
            ordinary
        }
    }

    fn pending_progress_state(
        &self,
        generation: SecretGeneration,
    ) -> Result<OnboardingState, OnboardingStateError> {
        let record = self.generation(generation)?;
        let ordinary = if record.rights_digest.is_some() && record.rate_policy_digest.is_some() {
            OnboardingState::RuntimeVerificationPending
        } else {
            OnboardingState::RightsAdmissionPending
        };
        Ok(self.pending_state(ordinary))
    }

    fn anonymous_progress_state(&self) -> OnboardingState {
        if self.anonymous_rights_digest.is_some() && self.anonymous_rate_policy_digest.is_some() {
            OnboardingState::RuntimeVerificationPending
        } else {
            OnboardingState::RightsAdmissionPending
        }
    }

    fn validate_initial_runtime_evidence(
        &self,
        capability: &ProviderCapability,
        generation: Option<SecretGeneration>,
        evidence: &RuntimeVerificationEvidence,
        observed_at: Timestamp,
    ) -> Result<(), OnboardingStateError> {
        evidence
            .revalidate()
            .map_err(|_| OnboardingStateError::InvalidEvidence)?;
        if !evidence.admits_activation_at(observed_at) {
            return Err(OnboardingStateError::InvalidEvidence);
        }
        match evidence {
            RuntimeVerificationEvidence::DigestV1(_) => {
                if self.surface_id.as_str() == super::ALPACA_BASIC_MARKET_DATA_SURFACE_ID {
                    Err(OnboardingStateError::EvidenceMismatch)
                } else {
                    Ok(())
                }
            }
            RuntimeVerificationEvidence::AlpacaPaperIexDoctorReceiptV1(receipt) => {
                let generation = generation.ok_or(OnboardingStateError::GenerationMismatch)?;
                if receipt.predecessor_digest().is_some() {
                    return Err(OnboardingStateError::InvalidEvidence);
                }
                self.validate_alpaca_receipt_binding(capability, generation, receipt)
            }
        }
    }

    fn validate_runtime_renewal(
        &self,
        capability: &ProviderCapability,
        generation: SecretGeneration,
        evidence: &RuntimeVerificationEvidence,
        observed_at: Timestamp,
    ) -> Result<(), OnboardingStateError> {
        evidence
            .revalidate()
            .map_err(|_| OnboardingStateError::InvalidEvidence)?;
        let next = evidence
            .alpaca_paper_iex_receipt()
            .ok_or(OnboardingStateError::EvidenceMismatch)?;
        let record = self.generation(generation)?;
        if self.state != OnboardingState::RenewalRequired
            || self.active_generation != Some(generation)
            || self.candidate_generation.is_some()
            || record.state != CredentialGenerationState::ActiveScoped
            || !evidence.admits_activation_at(observed_at)
        {
            return Err(OnboardingStateError::InvalidTransition);
        }
        self.validate_alpaca_receipt_binding(capability, generation, next)?;
        let prior = record
            .runtime_evidence
            .as_ref()
            .and_then(RuntimeVerificationEvidence::alpaca_paper_iex_receipt)
            .ok_or(OnboardingStateError::EvidenceMismatch)?;
        if next.predecessor_digest() != Some(prior.receipt_sha256())
            || next.verified_at() <= prior.verified_at()
            || !next.same_authority_as(prior)
        {
            return Err(OnboardingStateError::EvidenceMismatch);
        }
        Ok(())
    }

    fn validate_alpaca_receipt_binding(
        &self,
        capability: &ProviderCapability,
        generation: SecretGeneration,
        receipt: &super::AlpacaPaperIexDoctorReceiptV1,
    ) -> Result<(), OnboardingStateError> {
        let record = self.generation(generation)?;
        let context = self
            .runtime_verification_context
            .as_ref()
            .ok_or(OnboardingStateError::EvidenceMismatch)?;
        let principal_matches = record
            .verification
            .as_ref()
            .and_then(|verification| verification.bindings().account_digest())
            == Some(receipt.market_data_principal_sha256());
        let authority_is_nonexpiring = record
            .verification
            .as_ref()
            .is_some_and(|verification| verification.expires_at().is_none());
        if receipt.surface_id() != &self.surface_id
            || receipt.surface_id() != capability.surface_id()
            || receipt.generation() != generation
            || receipt.capability_revision() != self.capability_revision
            || receipt.capability_revision() != capability.revision()
            || receipt.capability_digest() != self.capability_digest
            || receipt.capability_digest() != capability.content_digest()
            || receipt.session_identifier() != context.session_identifier()
            || receipt.public_configuration_digest() != context.public_configuration_digest()
            || record.rights_digest != Some(receipt.rights_decision_digest())
            || record.rate_policy_digest != Some(receipt.rate_policy_digest())
            || receipt.rate_policy_digest() != capability.rate_policy().evidence_digest()
            || !principal_matches
            || !authority_is_nonexpiring
        {
            return Err(OnboardingStateError::EvidenceMismatch);
        }
        Ok(())
    }

    fn clear_rotation_operation(&mut self) {
        self.rotation_operation_owner = None;
        self.rotation_deadline_at = None;
        self.rotation_retry_budget = 0;
        self.rotation_started_from_renewal = false;
    }

    fn cleanup_completion_state(&self) -> OnboardingState {
        if self
            .generations
            .iter()
            .any(|record| record.state == CredentialGenerationState::CleanupRequired)
        {
            OnboardingState::CleanupRequired
        } else if self.active_generation.is_some() {
            OnboardingState::ActiveScoped
        } else {
            OnboardingState::Blocked
        }
    }
}

/// Provider onboarding validation or transition failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum OnboardingStateError {
    /// A capability record failed validation.
    #[error("provider capability is invalid")]
    Capability(#[from] ProviderCapabilityError),
    /// The supplied capability differs from the reserved immutable revision.
    #[error("provider capability does not match the onboarding reservation")]
    CapabilityMismatch,
    /// Requested and observed authority was not an exact admitted match.
    #[error("provider authority was denied")]
    AuthorityDenied,
    /// Requested-use rights were blocked.
    #[error("provider rights were denied")]
    RightsDenied,
    /// An event named the wrong credential generation.
    #[error("credential generation does not match lifecycle authority")]
    GenerationMismatch,
    /// Evidence was empty or structurally invalid.
    #[error("provider onboarding evidence is invalid")]
    InvalidEvidence,
    /// Evidence did not match the code-owned capability.
    #[error("provider onboarding evidence does not match capability authority")]
    EvidenceMismatch,
    /// The event is not legal from the current state.
    #[error("provider onboarding transition is invalid")]
    InvalidTransition,
    /// The bounded rotation operation elapsed before this transition.
    #[error("provider onboarding operation deadline elapsed")]
    DeadlineExceeded,
    /// A bounded state or event ceiling was exceeded.
    #[error("provider onboarding resource limit was exceeded")]
    ResourceLimit,
    /// A checked allocation failed.
    #[error("provider onboarding allocation failed")]
    Allocation,
    /// Durable non-secret event serialization failed.
    #[error("provider onboarding serialization failed")]
    Serialization,
}

fn require_verified(
    record: &GenerationRecord,
    observed_at: Timestamp,
) -> Result<(), OnboardingStateError> {
    if record.state == CredentialGenerationState::VerifiedLeastPrivilege
        && record
            .verification
            .as_ref()
            .is_some_and(|verification| verification.valid_at(observed_at))
    {
        Ok(())
    } else {
        Err(OnboardingStateError::InvalidTransition)
    }
}

fn require_digest(digest: EvidenceDigest) -> Result<(), OnboardingStateError> {
    if nonzero_digest(digest) {
        Ok(())
    } else {
        Err(OnboardingStateError::InvalidEvidence)
    }
}

fn rotation_progress_event(event: &OnboardingEvent) -> bool {
    matches!(
        event,
        OnboardingEvent::CredentialImported { .. }
            | OnboardingEvent::SecretStorePlanned { .. }
            | OnboardingEvent::ProtocolValidated { .. }
            | OnboardingEvent::CredentialStored { .. }
            | OnboardingEvent::AuthorityVerified { .. }
            | OnboardingEvent::RightsAdmitted { .. }
            | OnboardingEvent::RatePolicyAdmitted { .. }
            | OnboardingEvent::RuntimeVerified { .. }
            | OnboardingEvent::Activate { .. }
            | OnboardingEvent::Cutover { .. }
    )
}
