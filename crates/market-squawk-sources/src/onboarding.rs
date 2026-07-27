//! Evidence-bound provider capabilities and onboarding lifecycle authority.

/// Canonical onboarding and application surface identifier for the FRED/ALFRED API capability.
pub const FRED_ALFRED_API_SURFACE_ID: &str = "fred-alfred.api-v1-v2";

mod built_in_profiles;
mod capability;
mod lifecycle;
mod profile;
mod public_configuration;

pub use built_in_profiles::{TREASURY_DAILY_RATES_PROBE_YEAR, built_in_provider_profiles};
pub use capability::{
    AuthoritySet, CapabilityRegistrationOutcome, CredentialKind, EvidenceBinding, HumanBoundary,
    LifecycleSupport, ProviderCapability, ProviderCapabilityError, ProviderCapabilityInput,
    ProviderCapabilityRegistry, ProviderCapabilityRevision, RatePolicyDescriptor,
    RightsAdmissionState, RuntimeCapabilityObservation, RuntimeProviderCapability, SetupMode,
};
pub use lifecycle::{
    AuthorityBindings, AuthorityVerification, AuthorityVerificationInput,
    CredentialGenerationState, LocalDeletionOutcome, OnboardingEvent, OnboardingEventKind,
    OnboardingLifecycle, OnboardingState, OnboardingStateError, RemoteRevocationOutcome,
    SecretStoreClearOutcome,
};
pub use profile::{
    DataUseOperation, DataUseRight, OperationAdmission, ProbeTransport, ProfileActivationMode,
    ProfileEvidence, ProfileReleaseState, ProviderOnboardingProfile, ProviderProfileError,
    ProviderProfileRegistry, Requirement, VerificationProbe, ZeroFeeStatus,
};
pub use public_configuration::{
    MAX_PROVIDER_PUBLIC_CONFIGURATION_BYTES, MAX_PROVIDER_PUBLIC_CONFIGURATION_FIELDS,
    ProviderPublicConfiguration, PublicConfigurationError,
};

#[cfg(test)]
mod tests;
