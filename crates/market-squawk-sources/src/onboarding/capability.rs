//! Versioned code-owned provider capability records.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

const MAX_AUTHORITIES: usize = 64;
const MAX_EVIDENCE_BINDINGS: usize = 32;
const MAX_PROVIDER_SURFACES: usize = 64;
const MAX_REVISIONS_PER_SURFACE: usize = 256;
const MAX_OFFICIAL_URI_BYTES: usize = 2_048;

/// One evidence-admitted setup mode for an exact provider surface.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupMode {
    /// No secret is required.
    NoCredential,
    /// The user creates a provider API credential and imports it locally.
    ManualApiKeyImport,
    /// Native authorization code using an external browser and PKCE.
    OAuthAuthorizationCodePkce,
    /// OAuth device authorization.
    OAuthDevice,
    /// Provider-admitted dynamic OAuth client registration.
    DynamicClientRegistration,
}

impl SetupMode {
    /// Returns the stable catalog representation.
    pub const fn database_name(self) -> &'static str {
        match self {
            Self::NoCredential => "no_credential",
            Self::ManualApiKeyImport => "manual_api_key_import",
            Self::OAuthAuthorizationCodePkce => "oauth_authorization_code_pkce",
            Self::OAuthDevice => "oauth_device",
            Self::DynamicClientRegistration => "dynamic_client_registration",
        }
    }
}

/// Provider-controlled human boundary for the selected setup mode.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanBoundary {
    /// No provider-controlled user action is required.
    None,
    /// Login, consent, key issuance, or equivalent remains provider-controlled.
    ProviderControlled,
}

/// Secret shape stored for one provider surface.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialKind {
    /// The surface has no secret.
    None,
    /// One API credential value.
    ApiKey,
    /// An identifier plus secret pair encoded inside one bounded secret value.
    ApiKeyPair,
    /// OAuth token-set material.
    OAuthTokenSet,
    /// Dynamic-registration credentials.
    DynamicClientRegistration,
}

/// Rights admission attached to the code-owned provider record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RightsAdmissionState {
    /// Exact requested operations have scope-bound rights admission.
    AdmittedScoped,
    /// Rights evidence is incomplete and activation must remain pending.
    Pending,
    /// Current evidence affirmatively blocks the requested product use.
    Blocked,
}

/// Nonzero monotonic revision of one provider-surface record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ProviderCapabilityRevision(NonZeroU64);

impl ProviderCapabilityRevision {
    /// Constructs a nonzero record revision.
    pub fn new(value: u64) -> Result<Self, ProviderCapabilityError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ProviderCapabilityError::InvalidRecord)
    }

    /// Returns the portable revision value.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Canonical sorted set of bounded authority names.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct AuthoritySet(Box<[SourceIdentifier]>);

impl AuthoritySet {
    /// Sorts and admits a unique bounded authority set.
    pub fn try_new(
        mut authorities: Vec<SourceIdentifier>,
    ) -> Result<Self, ProviderCapabilityError> {
        if authorities.len() > MAX_AUTHORITIES {
            return Err(ProviderCapabilityError::ResourceLimit);
        }
        authorities.sort();
        if authorities.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ProviderCapabilityError::InvalidRecord);
        }
        Ok(Self(authorities.into_boxed_slice()))
    }

    /// Returns whether the set is empty.
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the canonical authorities.
    pub fn as_slice(&self) -> &[SourceIdentifier] {
        &self.0
    }

    /// Returns whether every authority is present in `other`.
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.0
            .iter()
            .all(|authority| other.0.binary_search(authority).is_ok())
    }
}

impl<'de> Deserialize<'de> for AuthoritySet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let authorities = Vec::<SourceIdentifier>::deserialize(deserializer)?;
        Self::try_new(authorities).map_err(serde::de::Error::custom)
    }
}

/// Exact source document and response-body digest supporting a capability.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceBinding {
    source_id: SourceIdentifier,
    digest: EvidenceDigest,
}

impl EvidenceBinding {
    /// Constructs a source/digest binding.
    pub const fn new(source_id: SourceIdentifier, digest: EvidenceDigest) -> Self {
        Self { source_id, digest }
    }

    /// Returns the evidence source identity.
    pub const fn source_id(&self) -> &SourceIdentifier {
        &self.source_id
    }

    /// Returns the exact response or stable-content digest.
    pub const fn digest(&self) -> EvidenceDigest {
        self.digest
    }
}

/// Evidence-bound provider/product/protocol/endpoint rate-policy identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RatePolicyDescriptor {
    policy_id: SourceIdentifier,
    evidence_digest: EvidenceDigest,
    unknown_is_conservative: bool,
}

impl RatePolicyDescriptor {
    /// Constructs a rate-policy descriptor.
    pub fn try_new(
        policy_id: SourceIdentifier,
        evidence_digest: EvidenceDigest,
        unknown_is_conservative: bool,
    ) -> Result<Self, ProviderCapabilityError> {
        let descriptor = Self {
            policy_id,
            evidence_digest,
            unknown_is_conservative,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// Returns the full versioned policy identity.
    pub const fn policy_id(&self) -> &SourceIdentifier {
        &self.policy_id
    }

    /// Returns the exact policy evidence digest.
    pub const fn evidence_digest(&self) -> EvidenceDigest {
        self.evidence_digest
    }

    fn validate(&self) -> Result<(), ProviderCapabilityError> {
        if self.unknown_is_conservative && nonzero_digest(self.evidence_digest) {
            Ok(())
        } else {
            Err(ProviderCapabilityError::InvalidRecord)
        }
    }
}

/// Provider-admitted credential lifecycle operations.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleSupport {
    rotation: bool,
    remote_revocation: bool,
    overlap_cutover: bool,
}

impl LifecycleSupport {
    /// Constructs exact lifecycle support flags.
    pub const fn new(rotation: bool, remote_revocation: bool, overlap_cutover: bool) -> Self {
        Self {
            rotation,
            remote_revocation,
            overlap_cutover,
        }
    }

    /// Returns whether candidate-generation rotation is supported.
    pub const fn rotation(self) -> bool {
        self.rotation
    }

    /// Returns whether remote revocation is documented.
    pub const fn remote_revocation(self) -> bool {
        self.remote_revocation
    }

    /// Returns whether prior and candidate generations may overlap until cutover.
    pub const fn overlap_cutover(self) -> bool {
        self.overlap_cutover
    }

    fn validate(self) -> Result<(), ProviderCapabilityError> {
        if (self.overlap_cutover && !self.rotation)
            || (self.rotation && !self.overlap_cutover && !self.remote_revocation)
        {
            Err(ProviderCapabilityError::InvalidRecord)
        } else {
            Ok(())
        }
    }
}

/// Validated input for one provider-surface capability revision.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCapabilityInput {
    /// Stable provider/surface identity.
    pub surface_id: SourceIdentifier,
    /// Monotonic code-owned revision.
    pub revision: ProviderCapabilityRevision,
    /// Exact admitted setup mode.
    pub setup_mode: SetupMode,
    /// Exact static official provider entry URI.
    pub official_entry_uri: String,
    /// Provider-controlled human-action boundary.
    pub human_boundary: HumanBoundary,
    /// Credential shape for this mode.
    pub credential_kind: CredentialKind,
    /// Required minimum authority.
    pub minimum_authority: AuthoritySet,
    /// Maximum accepted authority.
    pub maximum_authority: AuthoritySet,
    /// Exact non-mutating verifier revision.
    pub verifier_revision: SourceIdentifier,
    /// Endpoint-class-specific rate policy.
    pub rate_policy: RatePolicyDescriptor,
    /// Rights state for product use.
    pub rights_state: RightsAdmissionState,
    /// Supported lifecycle operations.
    pub lifecycle_support: LifecycleSupport,
    /// Exact supporting sources and digests.
    pub evidence: Vec<EvidenceBinding>,
    /// Stable trigger invalidated by evidence or provider-surface change.
    pub refresh_trigger: SourceIdentifier,
}

/// Immutable validated provider-surface capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCapability {
    surface_id: SourceIdentifier,
    revision: ProviderCapabilityRevision,
    setup_mode: SetupMode,
    official_entry_uri: String,
    human_boundary: HumanBoundary,
    credential_kind: CredentialKind,
    minimum_authority: AuthoritySet,
    maximum_authority: AuthoritySet,
    verifier_revision: SourceIdentifier,
    rate_policy: RatePolicyDescriptor,
    rights_state: RightsAdmissionState,
    lifecycle_support: LifecycleSupport,
    evidence: Box<[EvidenceBinding]>,
    refresh_trigger: SourceIdentifier,
    content_digest: EvidenceDigest,
}

impl ProviderCapability {
    /// Validates a complete record and computes its canonical SHA-256 identity.
    pub fn try_new(mut input: ProviderCapabilityInput) -> Result<Self, ProviderCapabilityError> {
        validate_official_uri(&input.official_entry_uri)?;
        input.rate_policy.validate()?;
        input.lifecycle_support.validate()?;
        if !input
            .minimum_authority
            .is_subset_of(&input.maximum_authority)
            || input.evidence.is_empty()
            || input.evidence.len() > MAX_EVIDENCE_BINDINGS
        {
            return Err(ProviderCapabilityError::InvalidRecord);
        }
        validate_mode_contract(&input)?;
        input
            .evidence
            .sort_by(|left, right| left.source_id.cmp(&right.source_id));
        if input
            .evidence
            .windows(2)
            .any(|pair| pair[0].source_id == pair[1].source_id)
            || input
                .evidence
                .iter()
                .any(|evidence| !nonzero_digest(evidence.digest))
        {
            return Err(ProviderCapabilityError::InvalidRecord);
        }
        let canonical =
            serde_json::to_vec(&input).map_err(|_| ProviderCapabilityError::Serialization)?;
        let content_digest =
            EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(canonical).into());
        Ok(Self {
            surface_id: input.surface_id,
            revision: input.revision,
            setup_mode: input.setup_mode,
            official_entry_uri: input.official_entry_uri,
            human_boundary: input.human_boundary,
            credential_kind: input.credential_kind,
            minimum_authority: input.minimum_authority,
            maximum_authority: input.maximum_authority,
            verifier_revision: input.verifier_revision,
            rate_policy: input.rate_policy,
            rights_state: input.rights_state,
            lifecycle_support: input.lifecycle_support,
            evidence: input.evidence.into_boxed_slice(),
            refresh_trigger: input.refresh_trigger,
            content_digest,
        })
    }

    /// Revalidates canonical JSON loaded from durable non-secret state.
    pub fn try_from_json(bytes: &[u8]) -> Result<Self, ProviderCapabilityError> {
        let input =
            serde_json::from_slice(bytes).map_err(|_| ProviderCapabilityError::Serialization)?;
        Self::try_new(input)
    }

    /// Returns canonical validated JSON for durable catalog storage.
    pub fn canonical_json(&self) -> Result<Vec<u8>, ProviderCapabilityError> {
        serde_json::to_vec(&self.to_input()).map_err(|_| ProviderCapabilityError::Serialization)
    }

    /// Returns the provider/surface identity.
    pub const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    /// Returns the one-based record revision.
    pub const fn revision(&self) -> ProviderCapabilityRevision {
        self.revision
    }

    /// Returns the only admitted setup mode.
    pub const fn setup_mode(&self) -> SetupMode {
        self.setup_mode
    }

    /// Returns the exact static official provider entry URI.
    pub fn official_entry_uri(&self) -> &str {
        &self.official_entry_uri
    }

    /// Returns the human-action boundary.
    pub const fn human_boundary(&self) -> HumanBoundary {
        self.human_boundary
    }

    /// Returns the credential shape.
    pub const fn credential_kind(&self) -> CredentialKind {
        self.credential_kind
    }

    /// Returns required minimum authority.
    pub const fn minimum_authority(&self) -> &AuthoritySet {
        &self.minimum_authority
    }

    /// Returns maximum accepted authority.
    pub const fn maximum_authority(&self) -> &AuthoritySet {
        &self.maximum_authority
    }

    /// Returns the exact verifier revision.
    pub const fn verifier_revision(&self) -> &SourceIdentifier {
        &self.verifier_revision
    }

    /// Returns the rate-policy binding.
    pub const fn rate_policy(&self) -> &RatePolicyDescriptor {
        &self.rate_policy
    }

    /// Returns rights state.
    pub const fn rights_state(&self) -> RightsAdmissionState {
        self.rights_state
    }

    /// Returns lifecycle support.
    pub const fn lifecycle_support(&self) -> LifecycleSupport {
        self.lifecycle_support
    }

    /// Returns exact supporting evidence.
    pub fn evidence(&self) -> &[EvidenceBinding] {
        &self.evidence
    }

    /// Returns the evidence-refresh trigger.
    pub const fn refresh_trigger(&self) -> &SourceIdentifier {
        &self.refresh_trigger
    }

    /// Returns canonical record identity.
    pub const fn content_digest(&self) -> EvidenceDigest {
        self.content_digest
    }

    fn to_input(&self) -> ProviderCapabilityInput {
        ProviderCapabilityInput {
            surface_id: self.surface_id.clone(),
            revision: self.revision,
            setup_mode: self.setup_mode,
            official_entry_uri: self.official_entry_uri.clone(),
            human_boundary: self.human_boundary,
            credential_kind: self.credential_kind,
            minimum_authority: self.minimum_authority.clone(),
            maximum_authority: self.maximum_authority.clone(),
            verifier_revision: self.verifier_revision.clone(),
            rate_policy: self.rate_policy.clone(),
            rights_state: self.rights_state,
            lifecycle_support: self.lifecycle_support,
            evidence: self.evidence.to_vec(),
            refresh_trigger: self.refresh_trigger.clone(),
        }
    }
}

/// Runtime observation that can disable or narrow code-owned authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeCapabilityObservation {
    setup_mode: SetupMode,
    maximum_authority: AuthoritySet,
    available: bool,
}

impl RuntimeCapabilityObservation {
    /// Constructs a runtime observation without granting it authority.
    pub fn try_new(
        setup_mode: SetupMode,
        maximum_authority: AuthoritySet,
        available: bool,
    ) -> Result<Self, ProviderCapabilityError> {
        if !available && !maximum_authority.is_empty() {
            return Err(ProviderCapabilityError::InvalidRecord);
        }
        Ok(Self {
            setup_mode,
            maximum_authority,
            available,
        })
    }
}

/// Runtime-narrowed view of a code-owned provider record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeProviderCapability {
    code_owned: ProviderCapability,
    maximum_authority: AuthoritySet,
    available: bool,
}

impl RuntimeProviderCapability {
    /// Returns the maximum authority after runtime narrowing.
    pub const fn maximum_authority(&self) -> &AuthoritySet {
        &self.maximum_authority
    }

    /// Returns whether runtime evidence left the surface available.
    pub const fn available(&self) -> bool {
        self.available
    }

    /// Returns the exact code-owned capability identity.
    pub const fn code_owned(&self) -> &ProviderCapability {
        &self.code_owned
    }
}

/// Result of registering a capability revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityRegistrationOutcome {
    /// A new contiguous revision was retained.
    Inserted,
    /// The exact same revision and bytes were already retained.
    Replay,
}

/// Bounded in-memory registry retaining every code-owned revision.
#[derive(Debug, Default)]
pub struct ProviderCapabilityRegistry {
    records: BTreeMap<SourceIdentifier, Vec<ProviderCapability>>,
}

impl ProviderCapabilityRegistry {
    /// Constructs an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers revision one or the next contiguous revision.
    pub fn register(
        &mut self,
        capability: ProviderCapability,
    ) -> Result<CapabilityRegistrationOutcome, ProviderCapabilityError> {
        if let Some(revisions) = self.records.get_mut(capability.surface_id()) {
            let current = revisions
                .last()
                .ok_or(ProviderCapabilityError::InvalidRecord)?;
            if capability.revision() == current.revision() {
                return if capability == *current {
                    Ok(CapabilityRegistrationOutcome::Replay)
                } else {
                    Err(ProviderCapabilityError::RevisionConflict)
                };
            }
            let expected = current
                .revision()
                .get()
                .checked_add(1)
                .ok_or(ProviderCapabilityError::ResourceLimit)?;
            if capability.revision().get() != expected {
                return Err(ProviderCapabilityError::RevisionGap);
            }
            if revisions.len() == MAX_REVISIONS_PER_SURFACE {
                return Err(ProviderCapabilityError::ResourceLimit);
            }
            revisions
                .try_reserve_exact(1)
                .map_err(|_| ProviderCapabilityError::Allocation)?;
            revisions.push(capability);
            return Ok(CapabilityRegistrationOutcome::Inserted);
        }
        if capability.revision().get() != 1 {
            return Err(ProviderCapabilityError::RevisionGap);
        }
        if self.records.len() == MAX_PROVIDER_SURFACES {
            return Err(ProviderCapabilityError::ResourceLimit);
        }
        let mut revisions = Vec::new();
        revisions
            .try_reserve_exact(1)
            .map_err(|_| ProviderCapabilityError::Allocation)?;
        let surface_id = capability.surface_id().clone();
        revisions.push(capability);
        self.records.insert(surface_id, revisions);
        Ok(CapabilityRegistrationOutcome::Inserted)
    }

    /// Returns the current code-owned revision for one surface.
    pub fn current(&self, surface_id: &SourceIdentifier) -> Option<&ProviderCapability> {
        self.records
            .get(surface_id)
            .and_then(|records| records.last())
    }

    /// Applies runtime availability and authority only when it narrows the current record.
    pub fn narrow_current(
        &self,
        surface_id: &SourceIdentifier,
        observation: RuntimeCapabilityObservation,
    ) -> Result<RuntimeProviderCapability, ProviderCapabilityError> {
        let code_owned = self
            .current(surface_id)
            .ok_or(ProviderCapabilityError::UnknownSurface)?;
        if observation.setup_mode != code_owned.setup_mode()
            || !observation
                .maximum_authority
                .is_subset_of(code_owned.maximum_authority())
            || (observation.available
                && !code_owned
                    .minimum_authority()
                    .is_subset_of(&observation.maximum_authority))
        {
            return Err(ProviderCapabilityError::RuntimeBroadening);
        }
        Ok(RuntimeProviderCapability {
            code_owned: code_owned.clone(),
            maximum_authority: observation.maximum_authority,
            available: observation.available,
        })
    }
}

/// Provider capability construction, registry, or narrowing failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProviderCapabilityError {
    /// A field combination, URI, authority set, or digest is invalid.
    #[error("provider capability record is invalid")]
    InvalidRecord,
    /// A bounded record or registry ceiling was exceeded.
    #[error("provider capability resource limit was exceeded")]
    ResourceLimit,
    /// A checked allocation failed.
    #[error("provider capability allocation failed")]
    Allocation,
    /// Canonical JSON could not be encoded or decoded.
    #[error("provider capability serialization failed")]
    Serialization,
    /// A new record skipped or moved behind the required contiguous revision.
    #[error("provider capability revision is not contiguous")]
    RevisionGap,
    /// The same revision named different immutable evidence.
    #[error("provider capability revision conflicts with retained evidence")]
    RevisionConflict,
    /// Runtime discovery attempted to enable or broaden code-owned authority.
    #[error("runtime provider metadata attempted to broaden authority")]
    RuntimeBroadening,
    /// The requested provider surface is not registered.
    #[error("provider capability surface is unknown")]
    UnknownSurface,
}

fn validate_official_uri(value: &str) -> Result<(), ProviderCapabilityError> {
    if value.is_empty() || value.len() > MAX_OFFICIAL_URI_BYTES {
        return Err(ProviderCapabilityError::InvalidRecord);
    }
    let parsed = Url::parse(value).map_err(|_| ProviderCapabilityError::InvalidRecord)?;
    if parsed.scheme() != "https"
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.as_str() != value
    {
        return Err(ProviderCapabilityError::InvalidRecord);
    }
    Ok(())
}

fn validate_mode_contract(input: &ProviderCapabilityInput) -> Result<(), ProviderCapabilityError> {
    let anonymous = input.setup_mode == SetupMode::NoCredential;
    if anonymous
        && (input.human_boundary != HumanBoundary::None
            || input.credential_kind != CredentialKind::None
            || !input.minimum_authority.is_empty()
            || !input.maximum_authority.is_empty())
    {
        return Err(ProviderCapabilityError::InvalidRecord);
    }
    if !anonymous
        && (input.human_boundary != HumanBoundary::ProviderControlled
            || input.credential_kind == CredentialKind::None
            || input.minimum_authority.is_empty())
    {
        return Err(ProviderCapabilityError::InvalidRecord);
    }
    Ok(())
}

pub(super) fn nonzero_digest(digest: EvidenceDigest) -> bool {
    digest.bytes() != [0; 32]
}
