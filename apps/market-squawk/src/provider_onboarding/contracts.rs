//! Secret-free status contracts shared by local portal and CLI transports.

use market_squawk_adapter_bls::BlsSeriesMetadataInput;
use market_squawk_data::ResumedProviderOnboarding;
use market_squawk_domain::{
    CalendarDate, DataQuality, EvidenceDigest, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{SecretGeneration, SecretRef};
use market_squawk_sources::{
    CapabilityRegistrationOutcome, DataUseOperation, DataUseRight, OnboardingState,
    OperationAdmission, ProfileEvidence, ProfileReleaseState, ProviderCapabilityRevision,
    ProviderOnboardingProfile, ProviderPublicConfiguration, Requirement, ZeroFeeStatus,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Serializable code-owned profile facts for CLI and portal clients.
#[derive(Clone, Debug, Serialize)]
pub struct ProviderProfileView {
    id: &'static str,
    display_name: &'static str,
    zero_fee: ZeroFeeStatus,
    account_requirement: Requirement,
    credential_requirement: Requirement,
    administrative_contact_requirement: Requirement,
    release_state: ProfileReleaseState,
    official_handoff_url: &'static str,
    handoff_instruction: &'static str,
    permissions: &'static [&'static str],
    coverage: &'static str,
    quality_ceiling: DataQuality,
    rights: Vec<DataUseRight>,
    rights_duties: &'static [&'static str],
    rights_decision_digest: EvidenceDigest,
    persistence_evidence: Option<ProfileEvidence>,
    rotation: &'static str,
    revocation: &'static str,
    recovery: &'static [&'static str],
    evidence: Vec<ProfileEvidence>,
}

impl ProviderProfileView {
    /// Returns the stable code-owned surface identity.
    pub const fn id(&self) -> &'static str {
        self.id
    }

    /// Returns the current release gate.
    pub const fn release_state(&self) -> ProfileReleaseState {
        self.release_state
    }

    /// Returns the exact official handoff.
    pub const fn official_handoff_url(&self) -> &'static str {
        self.official_handoff_url
    }
}

impl From<&ProviderOnboardingProfile> for ProviderProfileView {
    fn from(profile: &ProviderOnboardingProfile) -> Self {
        let (account, credential, administrative_contact) = profile.requirements();
        let (handoff_url, handoff_instruction) = profile.handoff();
        let (coverage, quality_ceiling) = profile.coverage();
        let (rights, rights_duties) = profile.rights();
        let (rotation, revocation, recovery) = profile.lifecycle();
        Self {
            id: profile.id(),
            display_name: profile.display_name(),
            zero_fee: profile.zero_fee(),
            account_requirement: account,
            credential_requirement: credential,
            administrative_contact_requirement: administrative_contact,
            release_state: profile.release_state(),
            official_handoff_url: handoff_url,
            handoff_instruction,
            permissions: profile.permissions(),
            coverage,
            quality_ceiling,
            rights: rights.to_vec(),
            rights_duties,
            rights_decision_digest: profile.rights_decision_digest(),
            persistence_evidence: profile.persistence_evidence(),
            rotation,
            revocation,
            recovery,
            evidence: profile.evidence().to_vec(),
        }
    }
}

/// Idempotent profile registration disposition exposed without catalog authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderProfileRegistrationOutcome {
    /// The exact code-owned capability revision was inserted.
    Inserted,
    /// The exact same revision and canonical bytes were already retained.
    Replay,
}

/// Secret-free result of registering one exact code-owned provider profile.
#[derive(Clone, Debug, Serialize)]
pub struct ProviderProfileRegistration {
    profile: ProviderProfileView,
    outcome: ProviderProfileRegistrationOutcome,
}

impl ProviderProfileRegistration {
    pub(super) fn new(
        profile: ProviderProfileView,
        outcome: CapabilityRegistrationOutcome,
    ) -> Self {
        Self {
            profile,
            outcome: match outcome {
                CapabilityRegistrationOutcome::Inserted => {
                    ProviderProfileRegistrationOutcome::Inserted
                }
                CapabilityRegistrationOutcome::Replay => ProviderProfileRegistrationOutcome::Replay,
            },
        }
    }

    /// Returns the complete registered code-owned profile.
    pub const fn profile(&self) -> &ProviderProfileView {
        &self.profile
    }

    /// Returns whether registration inserted or replayed exact retained bytes.
    pub const fn outcome(&self) -> ProviderProfileRegistrationOutcome {
        self.outcome
    }
}

/// Next bounded action exposed to a local caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OnboardingNextAction {
    /// Complete the exact provider-controlled handoff.
    CompleteProviderHandoff,
    /// Import one provider-created secret through the write-only endpoint.
    ImportSecret,
    /// Verify the securely stored credential and activate its exact admitted authority.
    VerifyAndActivate,
    /// Refresh the named mutable official evidence.
    RefreshEvidence,
    /// Resolve the exact rights conflict before credential handling.
    ResolveRights,
    /// Recover the provider condition, then create a new immutable session.
    StartNewSession,
    /// No further setup action is required.
    Active,
    /// The session is terminally blocked or cancelled.
    None,
}

/// Closed provider-specific configuration accepted by the local onboarding portal.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum ProviderPortalActivationRequest {
    /// SEC EDGAR needs only the declared contact already retained by onboarding.
    Sec,
    /// BLS needs explicit user-verified series semantics and a bounded year range.
    Bls {
        /// Exact series semantics; units and frequency are never inferred.
        series: Vec<BlsSeriesMetadataInput>,
        /// Inclusive first observation year.
        start_year: u16,
        /// Inclusive final observation year.
        end_year: u16,
    },
    /// Treasury Fiscal Data average-interest-rate query.
    TreasuryFiscal {
        /// Inclusive first record date.
        first_record_date: CalendarDate,
        /// Inclusive final record date.
        last_record_date: CalendarDate,
        /// Bounded provider page size.
        page_size: u16,
    },
}

/// Secret-free evidence returned after durable adapter registration succeeds.
#[derive(Clone, Debug, Serialize)]
pub struct ProviderPortalActivationView {
    profile: SourceIdentifier,
    session_id: Uuid,
    capability_revision: u64,
    capability_digest: EvidenceDigest,
    rights_decision_digest: EvidenceDigest,
    persistence_evidence: Option<ProfileEvidence>,
    public_configuration_digest: EvidenceDigest,
    credential_generation: Option<u64>,
    verification_expires_at: Option<Timestamp>,
    issued_at: Timestamp,
}

impl ProviderPortalActivationView {
    pub(crate) fn from_lease(profile: SourceIdentifier, lease: &ProviderActivationLease) -> Self {
        Self {
            profile,
            session_id: lease.session_id(),
            capability_revision: lease.capability_revision().get(),
            capability_digest: lease.capability_digest(),
            rights_decision_digest: lease.rights_decision_digest(),
            persistence_evidence: lease.persistence_evidence(),
            public_configuration_digest: lease.public_configuration_digest(),
            credential_generation: lease.generation().map(SecretGeneration::get),
            verification_expires_at: lease.verification_expires_at(),
            issued_at: lease.issued_at(),
        }
    }
}

/// Secret-free durable status returned by every service and portal operation.
#[derive(Clone, Debug, Serialize)]
pub struct OnboardingSessionView {
    session_id: Uuid,
    surface_id: String,
    state: OnboardingState,
    next_action: OnboardingNextAction,
    credential_stored: bool,
    public_configuration: ProviderPublicConfiguration,
    official_handoff_url: &'static str,
    handoff_instruction: &'static str,
    recovery: &'static [&'static str],
}

/// Immutable in-process authority to construct one exact activated provider adapter.
///
/// The lease contains only non-secret catalog evidence and an opaque backend reference. Its
/// private fields prevent callers from manufacturing activation authority from a profile name or
/// provider quality ceiling.
#[derive(Clone)]
pub struct ProviderActivationLease {
    session_id: Uuid,
    surface_id: SourceIdentifier,
    capability_revision: ProviderCapabilityRevision,
    capability_digest: EvidenceDigest,
    rights_decision_digest: EvidenceDigest,
    rights: Vec<DataUseRight>,
    persistence_evidence: Option<ProfileEvidence>,
    public_configuration_digest: EvidenceDigest,
    public_configuration: ProviderPublicConfiguration,
    generation: Option<SecretGeneration>,
    secret_reference: Option<SecretRef>,
    verification_expires_at: Option<Timestamp>,
    issued_at: Timestamp,
}

impl ProviderActivationLease {
    pub(super) fn new(input: ProviderActivationLeaseInput) -> Self {
        Self {
            session_id: input.session_id,
            surface_id: input.surface_id,
            capability_revision: input.capability_revision,
            capability_digest: input.capability_digest,
            rights_decision_digest: input.rights_decision_digest,
            rights: input.rights,
            persistence_evidence: input.persistence_evidence,
            public_configuration_digest: input.public_configuration_digest,
            public_configuration: input.public_configuration,
            generation: input.generation,
            secret_reference: input.secret_reference,
            verification_expires_at: input.verification_expires_at,
            issued_at: input.issued_at,
        }
    }

    /// Returns the durable onboarding session bound into this lease.
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the exact built-in provider surface.
    pub const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    /// Returns the exact code-owned capability revision.
    pub const fn capability_revision(&self) -> ProviderCapabilityRevision {
        self.capability_revision
    }

    /// Returns the exact canonical capability digest.
    pub const fn capability_digest(&self) -> EvidenceDigest {
        self.capability_digest
    }

    /// Returns the exact code-owned rights decision admitted by the durable lifecycle.
    pub const fn rights_decision_digest(&self) -> EvidenceDigest {
        self.rights_decision_digest
    }

    /// Returns whether the admitted decision authorizes one exact data-use operation.
    pub fn admits(&self, operation: DataUseOperation) -> bool {
        self.rights.iter().any(|right| {
            right.operation() == operation && right.admission() == OperationAdmission::Admitted
        })
    }

    /// Returns exact official evidence selected to authorize durable persistence, when admitted.
    pub const fn persistence_evidence(&self) -> Option<ProfileEvidence> {
        self.persistence_evidence
    }

    /// Returns the exact public-configuration digest retained by the catalog.
    pub const fn public_configuration_digest(&self) -> EvidenceDigest {
        self.public_configuration_digest
    }

    /// Returns the recovered non-secret configuration needed by provider constructors.
    pub const fn public_configuration(&self) -> &ProviderPublicConfiguration {
        &self.public_configuration
    }

    /// Returns the activated credential generation, when this surface uses one.
    pub const fn generation(&self) -> Option<SecretGeneration> {
        self.generation
    }

    /// Returns the opaque backend reference without exposing credential bytes.
    pub const fn secret_reference(&self) -> Option<&SecretRef> {
        self.secret_reference.as_ref()
    }

    /// Returns the exclusive provider-verification expiry, when one was observed.
    pub const fn verification_expires_at(&self) -> Option<Timestamp> {
        self.verification_expires_at
    }

    /// Returns the trusted local instant when the lease was issued.
    pub const fn issued_at(&self) -> Timestamp {
        self.issued_at
    }
}

impl std::fmt::Debug for ProviderActivationLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProviderActivationLease")
            .field("session_id", &self.session_id)
            .field("surface_id", &self.surface_id)
            .field("capability_revision", &self.capability_revision)
            .field("rights_decision_digest", &self.rights_decision_digest)
            .field("generation", &self.generation)
            .field("secret_reference", &"[OPAQUE]")
            .field("verification_expires_at", &self.verification_expires_at)
            .field("issued_at", &self.issued_at)
            .finish()
    }
}

pub(super) struct ProviderActivationLeaseInput {
    pub session_id: Uuid,
    pub surface_id: SourceIdentifier,
    pub capability_revision: ProviderCapabilityRevision,
    pub capability_digest: EvidenceDigest,
    pub rights_decision_digest: EvidenceDigest,
    pub rights: Vec<DataUseRight>,
    pub persistence_evidence: Option<ProfileEvidence>,
    pub public_configuration_digest: EvidenceDigest,
    pub public_configuration: ProviderPublicConfiguration,
    pub generation: Option<SecretGeneration>,
    pub secret_reference: Option<SecretRef>,
    pub verification_expires_at: Option<Timestamp>,
    pub issued_at: Timestamp,
}

impl OnboardingSessionView {
    /// Returns the durable session identity.
    pub const fn session_id(&self) -> Uuid {
        self.session_id
    }

    /// Returns the exact code-owned surface identity.
    pub fn surface_id(&self) -> &str {
        &self.surface_id
    }

    /// Returns the durable lifecycle state.
    pub const fn state(&self) -> OnboardingState {
        self.state
    }

    /// Returns the next bounded action.
    pub const fn next_action(&self) -> OnboardingNextAction {
        self.next_action
    }

    /// Returns whether an opaque secret reference—not secret bytes—is retained.
    pub const fn credential_stored(&self) -> bool {
        self.credential_stored
    }

    /// Returns recovered public configuration suitable for adapter construction.
    pub const fn public_configuration(&self) -> &ProviderPublicConfiguration {
        &self.public_configuration
    }
}

pub(super) fn session_view(
    profile: &ProviderOnboardingProfile,
    resumed: &ResumedProviderOnboarding,
) -> OnboardingSessionView {
    let lifecycle = resumed.lifecycle();
    let generation = lifecycle
        .active_generation()
        .or_else(|| lifecycle.candidate_generation());
    let credential_stored = generation
        .and_then(|generation| lifecycle.generation_reference(generation))
        .is_some();
    let next_action = match lifecycle.state() {
        OnboardingState::UserActionRequired => OnboardingNextAction::ImportSecret,
        OnboardingState::StoredUnverified => OnboardingNextAction::VerifyAndActivate,
        OnboardingState::RefreshRequired => OnboardingNextAction::RefreshEvidence,
        OnboardingState::Unavailable => OnboardingNextAction::StartNewSession,
        OnboardingState::ActiveScoped => OnboardingNextAction::Active,
        OnboardingState::Blocked
            if profile.release_state() == ProfileReleaseState::RightsBlocked =>
        {
            OnboardingNextAction::ResolveRights
        }
        OnboardingState::Blocked => OnboardingNextAction::None,
        _ => OnboardingNextAction::CompleteProviderHandoff,
    };
    let (official_handoff_url, handoff_instruction) = profile.handoff();
    let (_, _, recovery) = profile.lifecycle();
    OnboardingSessionView {
        session_id: resumed.reservation().session_id(),
        surface_id: profile.id().to_owned(),
        state: lifecycle.state(),
        next_action,
        credential_stored,
        public_configuration: resumed.public_configuration().clone(),
        official_handoff_url,
        handoff_instruction,
        recovery,
    }
}
