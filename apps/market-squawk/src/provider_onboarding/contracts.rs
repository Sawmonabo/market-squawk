//! Secret-free status contracts shared by local portal and CLI transports.

use market_squawk_data::ResumedProviderOnboarding;
use market_squawk_domain::DataQuality;
use market_squawk_sources::{
    CapabilityRegistrationOutcome, DataUseRight, OnboardingState, ProfileEvidence,
    ProfileReleaseState, ProviderOnboardingProfile, ProviderPublicConfiguration, Requirement,
    ZeroFeeStatus,
};
use serde::Serialize;
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
        OnboardingState::UserActionRequired if credential_stored => {
            OnboardingNextAction::CompleteProviderHandoff
        }
        OnboardingState::UserActionRequired => OnboardingNextAction::ImportSecret,
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
