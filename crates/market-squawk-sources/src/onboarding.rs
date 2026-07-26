//! Evidence-bound provider capabilities and onboarding lifecycle authority.

mod built_in_profiles;
mod capability;
mod lifecycle;
mod profile;
mod public_configuration;

pub use built_in_profiles::built_in_provider_profiles;
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
