//! Local provider onboarding application service and hardened loopback portal.

mod contracts;
mod portal;
mod service;

pub use contracts::{
    OnboardingNextAction, OnboardingSessionView, ProviderActivationLease,
    ProviderPortalActivationRequest, ProviderPortalActivationView, ProviderProfileRegistration,
    ProviderProfileRegistrationOutcome, ProviderProfileView,
};
pub use portal::{
    ProviderOnboardingPortal, ProviderPortalActivationAuthority, ProviderPortalActivationError,
    ProviderPortalConfig, ProviderPortalError,
};
pub use service::{ProviderOnboardingError, ProviderOnboardingService, StartOnboardingRequest};
pub(crate) use service::{ProviderOnboardingMutationAuthority, ProviderRuntimeStartupAdmissions};
