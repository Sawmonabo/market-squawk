//! Code-owned onboarding facts for one exact provider surface.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use market_squawk_domain::{DataQuality, EvidenceDigest};
use serde::Serialize;
use thiserror::Error;

use super::{CredentialKind, ProviderCapability, RightsAdmissionState, SetupMode};
use crate::{EndpointPolicy, HttpRequestBounds, NetworkPolicyError};

const MAX_PROFILES: usize = 32;

/// Whether an external account, secret, or declared contact is needed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Requirement {
    /// The surface does not require this item.
    NotRequired,
    /// The user must supply non-secret identifying information.
    RequiredNonSecret,
    /// The provider controls creation and the user imports the result.
    RequiredProviderControlled,
}

/// Evidence-backed statement about zero-fee availability.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ZeroFeeStatus {
    /// Official evidence affirmatively states the exact surface is free.
    Confirmed,
    /// The technical route requires no credential, but a durable zero-fee commitment was not found.
    NoCredentialFeeNotEstablished,
    /// A free account is documented, but downstream product rights remain blocked.
    FreeAccountRightsBlocked,
    /// This is a local Market Squawk capability with no external service charge.
    Local,
    /// Price was not separately established by the reviewed evidence.
    NotSeparatelyEstablished,
}

/// User-visible activation mechanism admitted for one surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileActivationMode {
    /// No secret is collected; admitted checks can be automated locally.
    NoCredential,
    /// The provider creates a secret and Market Squawk imports it write-only.
    ManualSecretImport,
    /// The capability operates only on local user-owned state.
    Local,
}

/// Current release gate for one exact provider surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileReleaseState {
    /// The admitted operations can proceed after the configured probe succeeds.
    Available,
    /// Retrieval/display may proceed while durable or derivative operations remain closed.
    RightsLimited,
    /// Mutable official evidence must be refreshed before activation.
    RefreshRequired,
    /// An affirmative rights conflict blocks activation.
    RightsBlocked,
}

/// One data operation whose rights are evaluated independently.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DataUseOperation {
    /// Retrieve data from the source.
    Retrieve,
    /// Display data locally.
    Display,
    /// Persist data in Market Squawk datasets.
    Persist,
    /// Use data as model or feature input.
    ModelTraining,
    /// Export data or derived datasets.
    Export,
    /// Redistribute source data.
    Redistribute,
}

impl DataUseOperation {
    /// Returns the stable evidence representation.
    pub const fn evidence_name(self) -> &'static str {
        match self {
            Self::Retrieve => "retrieve",
            Self::Display => "display",
            Self::Persist => "persist",
            Self::ModelTraining => "model_training",
            Self::Export => "export",
            Self::Redistribute => "redistribute",
        }
    }
}

/// Rights decision for one operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationAdmission {
    /// The reviewed evidence admits this exact operation with the listed duties.
    Admitted,
    /// Evidence is incomplete; the operation remains closed.
    Pending,
    /// Reviewed evidence conflicts with the operation.
    Blocked,
}

impl OperationAdmission {
    /// Returns the stable evidence representation.
    pub const fn evidence_name(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::Pending => "pending",
            Self::Blocked => "blocked",
        }
    }
}

/// Exact rights disposition for one operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct DataUseRight {
    operation: DataUseOperation,
    admission: OperationAdmission,
}

impl DataUseRight {
    /// Constructs one operation decision.
    pub const fn new(operation: DataUseOperation, admission: OperationAdmission) -> Self {
        Self {
            operation,
            admission,
        }
    }

    /// Returns the operation.
    pub const fn operation(self) -> DataUseOperation {
        self.operation
    }

    /// Returns the decision.
    pub const fn admission(self) -> OperationAdmission {
        self.admission
    }
}

/// Static verification transport. Callers cannot supply a target URL.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeTransport {
    /// A deterministic local capability check.
    Local,
    /// A bounded HTTP GET.
    HttpGet,
    /// A bounded HTTP JSON POST with a code-owned body.
    HttpPostJson,
}

/// Fixed, non-mutating verification request and its semantic expectation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerificationProbe {
    transport: ProbeTransport,
    endpoint: Option<&'static str>,
    body: Option<&'static str>,
    semantic_expectation: &'static str,
    endpoint_policy: Option<EndpointPolicy>,
}

impl VerificationProbe {
    /// Constructs a deterministic local check.
    pub(crate) const fn local(semantic_expectation: &'static str) -> Self {
        Self {
            transport: ProbeTransport::Local,
            endpoint: None,
            body: None,
            semantic_expectation,
            endpoint_policy: None,
        }
    }

    /// Constructs a fixed bounded network check.
    pub(crate) fn network(
        transport: ProbeTransport,
        endpoint: &'static str,
        body: Option<&'static str>,
    ) -> Result<Self, NetworkPolicyError> {
        if transport == ProbeTransport::Local
            || (transport == ProbeTransport::HttpGet && body.is_some())
            || (transport == ProbeTransport::HttpPostJson && body.is_none())
        {
            return Err(NetworkPolicyError::InvalidRequestBounds);
        }
        let seconds =
            |value| NonZeroU64::new(value).ok_or(NetworkPolicyError::InvalidRequestBounds);
        let bounds = HttpRequestBounds::try_new(
            seconds(5_000_000_000)?,
            seconds(10_000_000_000)?,
            seconds(10_000_000_000)?,
            0,
            seconds(1024 * 1024)?,
        )?;
        Ok(Self {
            transport,
            endpoint: Some(endpoint),
            body,
            semantic_expectation: "successful bounded response with the provider's expected schema",
            endpoint_policy: Some(EndpointPolicy::try_new_with_bounds([endpoint], bounds)?),
        })
    }

    /// Returns the transport.
    pub const fn transport(&self) -> ProbeTransport {
        self.transport
    }

    /// Returns the fixed target, if this is a network probe.
    pub const fn endpoint(&self) -> Option<&'static str> {
        self.endpoint
    }

    /// Returns the fixed request body, if any.
    pub const fn body(&self) -> Option<&'static str> {
        self.body
    }

    /// Returns what a successful response establishes.
    pub const fn semantic_expectation(&self) -> &'static str {
        self.semantic_expectation
    }

    /// Returns the immutable endpoint policy.
    pub const fn endpoint_policy(&self) -> Option<&EndpointPolicy> {
        self.endpoint_policy.as_ref()
    }
}

/// One official source supporting a profile decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ProfileEvidence {
    source_id: &'static str,
    official_url: &'static str,
    reviewed_on: &'static str,
    content_digest: Option<EvidenceDigest>,
    refresh_required: bool,
}

impl ProfileEvidence {
    /// Constructs one code-owned evidence reference.
    pub(crate) const fn new(
        source_id: &'static str,
        official_url: &'static str,
        reviewed_on: &'static str,
        content_digest: Option<EvidenceDigest>,
        refresh_required: bool,
    ) -> Self {
        Self {
            source_id,
            official_url,
            reviewed_on,
            content_digest,
            refresh_required,
        }
    }

    /// Returns the stable research source identity.
    pub const fn source_id(self) -> &'static str {
        self.source_id
    }

    /// Returns the exact official source URL.
    pub const fn official_url(self) -> &'static str {
        self.official_url
    }

    /// Returns the last substantive review date.
    pub const fn reviewed_on(self) -> &'static str {
        self.reviewed_on
    }

    /// Returns the captured content digest when the source was captured successfully.
    pub const fn content_digest(self) -> Option<EvidenceDigest> {
        self.content_digest
    }

    /// Returns whether mutable official content must be refreshed before activation.
    pub const fn refresh_required(self) -> bool {
        self.refresh_required
    }
}

/// Complete immutable onboarding profile for an exact provider surface.
#[derive(Clone, Debug)]
pub struct ProviderOnboardingProfile {
    id: &'static str,
    display_name: &'static str,
    capability: ProviderCapability,
    zero_fee: ZeroFeeStatus,
    account: Requirement,
    credential: Requirement,
    administrative_contact: Requirement,
    activation_mode: ProfileActivationMode,
    release_state: ProfileReleaseState,
    handoff_url: &'static str,
    handoff_instruction: &'static str,
    permissions: &'static [&'static str],
    coverage: &'static str,
    quality_ceiling: DataQuality,
    probe: VerificationProbe,
    rights: &'static [DataUseRight],
    rights_duties: &'static [&'static str],
    rotation: &'static str,
    revocation: &'static str,
    recovery: &'static [&'static str],
    evidence: &'static [ProfileEvidence],
}

/// Construction input used only by the built-in profile catalog.
pub(crate) struct ProviderOnboardingProfileInput {
    pub id: &'static str,
    pub display_name: &'static str,
    pub capability: ProviderCapability,
    pub zero_fee: ZeroFeeStatus,
    pub account: Requirement,
    pub credential: Requirement,
    pub administrative_contact: Requirement,
    pub activation_mode: ProfileActivationMode,
    pub release_state: ProfileReleaseState,
    pub handoff_url: &'static str,
    pub handoff_instruction: &'static str,
    pub permissions: &'static [&'static str],
    pub coverage: &'static str,
    pub quality_ceiling: DataQuality,
    pub probe: VerificationProbe,
    pub rights: &'static [DataUseRight],
    pub rights_duties: &'static [&'static str],
    pub rotation: &'static str,
    pub revocation: &'static str,
    pub recovery: &'static [&'static str],
    pub evidence: &'static [ProfileEvidence],
}

impl ProviderOnboardingProfile {
    pub(crate) fn try_new(
        input: ProviderOnboardingProfileInput,
    ) -> Result<Self, ProviderProfileError> {
        let credentialed = input.capability.credential_kind() != CredentialKind::None;
        if input.id.is_empty()
            || input.display_name.is_empty()
            || input.id != input.capability.surface_id().as_str()
            || input.handoff_url != input.capability.official_entry_uri()
            || input.evidence.is_empty()
            || input.rights.len() != 6
            || input
                .rights
                .windows(2)
                .any(|pair| pair[0].operation >= pair[1].operation)
            || credentialed != (input.credential == Requirement::RequiredProviderControlled)
            || credentialed != (input.activation_mode == ProfileActivationMode::ManualSecretImport)
            || (input.release_state == ProfileReleaseState::RightsBlocked
                && input.capability.rights_state() != RightsAdmissionState::Blocked)
            || (input.release_state == ProfileReleaseState::Available
                && input.capability.rights_state() != RightsAdmissionState::AdmittedScoped)
            || (input.capability.setup_mode() == SetupMode::NoCredential
                && input.credential != Requirement::NotRequired)
        {
            return Err(ProviderProfileError::InvalidProfile);
        }
        Ok(Self {
            id: input.id,
            display_name: input.display_name,
            capability: input.capability,
            zero_fee: input.zero_fee,
            account: input.account,
            credential: input.credential,
            administrative_contact: input.administrative_contact,
            activation_mode: input.activation_mode,
            release_state: input.release_state,
            handoff_url: input.handoff_url,
            handoff_instruction: input.handoff_instruction,
            permissions: input.permissions,
            coverage: input.coverage,
            quality_ceiling: input.quality_ceiling,
            probe: input.probe,
            rights: input.rights,
            rights_duties: input.rights_duties,
            rotation: input.rotation,
            revocation: input.revocation,
            recovery: input.recovery,
            evidence: input.evidence,
        })
    }

    /// Returns the stable portal identity.
    pub const fn id(&self) -> &'static str {
        self.id
    }

    /// Returns the user-facing name.
    pub const fn display_name(&self) -> &'static str {
        self.display_name
    }

    /// Returns the exact catalog capability.
    pub const fn capability(&self) -> &ProviderCapability {
        &self.capability
    }

    /// Returns the zero-fee evidence classification.
    pub const fn zero_fee(&self) -> ZeroFeeStatus {
        self.zero_fee
    }

    /// Returns account, credential, and contact requirements.
    pub const fn requirements(&self) -> (Requirement, Requirement, Requirement) {
        (self.account, self.credential, self.administrative_contact)
    }

    /// Returns the activation mode.
    pub const fn activation_mode(&self) -> ProfileActivationMode {
        self.activation_mode
    }

    /// Returns the release gate.
    pub const fn release_state(&self) -> ProfileReleaseState {
        self.release_state
    }

    /// Returns the exact official handoff.
    pub const fn handoff(&self) -> (&'static str, &'static str) {
        (self.handoff_url, self.handoff_instruction)
    }

    /// Returns the exact requested permissions.
    pub const fn permissions(&self) -> &'static [&'static str] {
        self.permissions
    }

    /// Returns coverage and its maximum data-quality classification.
    pub const fn coverage(&self) -> (&'static str, DataQuality) {
        (self.coverage, self.quality_ceiling)
    }

    /// Returns the fixed verification probe.
    pub const fn probe(&self) -> &VerificationProbe {
        &self.probe
    }

    /// Returns operation-specific rights and duties.
    pub const fn rights(&self) -> (&'static [DataUseRight], &'static [&'static str]) {
        (self.rights, self.rights_duties)
    }

    /// Returns rotation, revocation, and recovery guidance.
    pub const fn lifecycle(&self) -> (&'static str, &'static str, &'static [&'static str]) {
        (self.rotation, self.revocation, self.recovery)
    }

    /// Returns exact official evidence and review dates.
    pub const fn evidence(&self) -> &'static [ProfileEvidence] {
        self.evidence
    }
}

/// Bounded immutable registry of built-in profiles.
#[derive(Debug)]
pub struct ProviderProfileRegistry {
    profiles: BTreeMap<&'static str, ProviderOnboardingProfile>,
}

impl ProviderProfileRegistry {
    pub(crate) fn try_new(
        profiles: Vec<ProviderOnboardingProfile>,
    ) -> Result<Self, ProviderProfileError> {
        if profiles.is_empty() || profiles.len() > MAX_PROFILES {
            return Err(ProviderProfileError::InvalidProfile);
        }
        let mut retained = BTreeMap::new();
        for profile in profiles {
            if retained.insert(profile.id(), profile).is_some() {
                return Err(ProviderProfileError::DuplicateProfile);
            }
        }
        Ok(Self { profiles: retained })
    }

    /// Returns one exact profile.
    pub fn get(&self, id: &str) -> Option<&ProviderOnboardingProfile> {
        self.profiles.get(id)
    }

    /// Iterates in stable profile-ID order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &ProviderOnboardingProfile> {
        self.profiles.values()
    }
}

/// Built-in profile construction failure.
#[derive(Debug, Error)]
pub enum ProviderProfileError {
    /// One built-in record violated its cross-field contract.
    #[error("provider onboarding profile is invalid")]
    InvalidProfile,
    /// Two built-in records used the same stable profile identity.
    #[error("provider onboarding profile identity is duplicated")]
    DuplicateProfile,
    /// The underlying generation-bound capability was invalid.
    #[error(transparent)]
    Capability(#[from] super::ProviderCapabilityError),
    /// A fixed provider target failed the endpoint policy.
    #[error(transparent)]
    Network(#[from] NetworkPolicyError),
    /// A bounded provider identity was invalid.
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
}
