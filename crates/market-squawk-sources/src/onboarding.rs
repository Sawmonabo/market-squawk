//! Evidence-bound provider capabilities and onboarding lifecycle authority.

mod capability;
mod lifecycle;

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
};

#[cfg(test)]
mod tests;
