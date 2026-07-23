//! Local provider onboarding application service and hardened loopback portal.

mod contracts;
mod portal;
mod service;

pub use contracts::{
    OnboardingNextAction, OnboardingSessionView, ProviderProfileRegistration,
    ProviderProfileRegistrationOutcome, ProviderProfileView,
};
pub use portal::{ProviderOnboardingPortal, ProviderPortalConfig, ProviderPortalError};
pub use service::{ProviderOnboardingError, ProviderOnboardingService, StartOnboardingRequest};
