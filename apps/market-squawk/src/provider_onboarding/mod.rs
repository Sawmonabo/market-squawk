//! Local provider onboarding application service and hardened loopback portal.

mod contracts;
pub(crate) mod credential_bundle;
mod credential_bundle_delegation;
mod portal;
mod schwab_market_doctor;
mod schwab_market_doctor_probe;
mod schwab_oauth_installation;
mod schwab_oauth_runtime;
mod service;

pub(crate) const SCHWAB_MARKET_DATA_SURFACE_ID: &str = "schwab.trader-api-market-data";

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
    FredPortalServiceReviewInput, SchwabOAuthLifecycleAction, SchwabOAuthLifecycleView,
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
pub(crate) use schwab_oauth_installation::{
    InstallationSchwabOAuthBrowser, InstallationSchwabOAuthIdentity,
    InstallationSchwabOAuthTlsAcceptor, apply_installation_trust_action,
};
pub use schwab_oauth_installation::{
    SchwabOAuthInstallationCapabilityError, SchwabOAuthInstallationTrustAction,
    SchwabOAuthInstallationTrustState,
};
pub(crate) use schwab_oauth_runtime::{
    SchwabOAuthBrowserError, SchwabOAuthMarketAuthority, SchwabOAuthMarketDrain,
    SchwabOAuthMarketDrainError, SchwabOAuthMarketDrainFuture, SchwabOAuthPublicationEpoch,
    SchwabOAuthRuntime, SchwabOAuthRuntimeConfiguration, SchwabOAuthRuntimeError,
};
pub(crate) use service::{
    AcquiredFredTermsDocument, ProviderOnboardingMutationAuthority,
    ProviderRuntimeStartupAdmissions,
};
pub use service::{ProviderOnboardingError, ProviderOnboardingService, StartOnboardingRequest};
