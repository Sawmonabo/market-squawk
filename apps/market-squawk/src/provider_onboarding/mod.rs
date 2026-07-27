//! Local provider onboarding application service and hardened loopback portal.

mod contracts;
mod portal;
mod service;

pub(crate) use contracts::{
    FredPortalEvidenceInput, FredPortalGrantInput, FredPortalServiceEvidenceInput,
    FredPortalServicePermissionChannelInput, FredPortalServicePermissionInput,
    FredPortalServiceReviewInput,
};
pub use contracts::{
    OnboardingNextAction, OnboardingSessionView, ProviderActivationLease,
    ProviderPortalActivationRequest, ProviderPortalActivationView, ProviderProfileRegistration,
    ProviderProfileRegistrationOutcome, ProviderProfileView, SecCikInput, SecCikInputError,
};
pub use portal::{
    ProviderOnboardingPortal, ProviderPortalActivationAuthority, ProviderPortalActivationError,
    ProviderPortalConfig, ProviderPortalError,
};
pub(crate) use service::{
    AcquiredFredTermsDocument, ProviderOnboardingMutationAuthority,
    ProviderRuntimeStartupAdmissions,
};
pub use service::{ProviderOnboardingError, ProviderOnboardingService, StartOnboardingRequest};
