//! Local provider onboarding application service and hardened loopback portal.

mod contracts;
pub(crate) mod credential_bundle;
mod credential_bundle_delegation;
mod portal;
mod service;

pub use credential_bundle::{
    AlpacaCredentialRealm, PROVIDER_CREDENTIAL_BUNDLE_SCHEMA, ProviderCredentialBundle,
    ProviderCredentialBundleParseError, ProviderCredentialConfiguration, ProviderCredentialValue,
    ProviderCredentialValues, parse_provider_credential_bundle_file,
};
pub use credential_bundle_delegation::{
    ProviderCredentialBundleDelegation, ProviderCredentialBundleDelegationError,
    ProviderCredentialBundleProvider, ProviderCredentialDelegationDisposition,
    ProviderCredentialDelegationResult, ProviderCredentialProfileUnavailableReason,
    delegate_provider_credential_bundle,
};

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
