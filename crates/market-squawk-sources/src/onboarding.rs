//! Evidence-bound provider capabilities and onboarding lifecycle authority.

/// Canonical onboarding and application surface identifier for the FRED/ALFRED API capability.
pub const FRED_ALFRED_API_SURFACE_ID: &str = "fred-alfred.api-v1-v2";

mod built_in_profiles;
mod capability;
mod lifecycle;
mod profile;
mod public_configuration;
mod runtime_verification;
mod source_authority;

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
pub use runtime_verification::{
    ALPACA_BASIC_MARKET_DATA_SURFACE_ID, ALPACA_PAPER_IEX_DOCTOR_RECEIPT_SCHEMA,
    AlpacaDoctorAdditionalCapability, AlpacaDoctorBatchObservation,
    AlpacaDoctorCalendarObservation, AlpacaDoctorCapabilityEvidence, AlpacaDoctorCredentialRealm,
    AlpacaDoctorHistoricalObservation, AlpacaDoctorHistoricalPageEvidence,
    AlpacaDoctorHttpEvidence, AlpacaDoctorProbeEvidence, AlpacaDoctorQuoteObservation,
    AlpacaDoctorRateEvidence, AlpacaDoctorStreamObservation, AlpacaPaperIexDoctorReceiptInput,
    AlpacaPaperIexDoctorReceiptV1, AlpacaRateLimitField, AlpacaRetryAfterEvidence,
    MAX_ALPACA_PAPER_IEX_DOCTOR_RECEIPT_BYTES, MAX_SCHWAB_MARKET_DATA_DOCTOR_RECEIPT_BYTES,
    RuntimeCapabilityDisposition, RuntimeVerificationContext, RuntimeVerificationDigestV1,
    RuntimeVerificationEvidence, RuntimeVerificationEvidenceError,
    SCHWAB_MARKET_DATA_DOCTOR_RECEIPT_SCHEMA, SCHWAB_MARKET_DATA_SURFACE_ID,
    SchwabMarketDataDoctorObservation, SchwabMarketDataDoctorReceiptInput,
    SchwabMarketDataDoctorReceiptV1, SchwabMarketDataFamily, SchwabMarketDataFamilyEvidence,
    SchwabUserPreferenceDoctorEvidence,
};
pub use source_authority::{
    FASB_XBRL_TAXONOMY_AUTHORITY, FASB_XBRL_TAXONOMY_RATE_SCOPE, FASB_XBRL_TAXONOMY_SOURCE_ID,
    FILING_TAXONOMY_SOURCE_AUTHORITIES, FilingTaxonomyAuthorityContractError,
    FilingTaxonomyAuthorityLookupError, FilingTaxonomyLocator, FilingTaxonomyRequestHeaderClass,
    FilingTaxonomySourceAuthority, ResolvedFilingTaxonomyAuthority, SEC_EDGAR_AUTHORITY,
    SEC_EDGAR_PROFILE_ID, SEC_EDGAR_RATE_SCOPE, SEC_EDGAR_SOURCE_ID,
    W3C_XML_SCHEMA_STANDARDS_AUTHORITY, W3C_XML_SCHEMA_STANDARDS_RATE_SCOPE,
    W3C_XML_SCHEMA_STANDARDS_SOURCE_ID, XBRL_INTERNATIONAL_STANDARDS_AUTHORITY,
    XBRL_INTERNATIONAL_STANDARDS_RATE_SCOPE, XBRL_INTERNATIONAL_STANDARDS_SOURCE_ID,
    XBRL_US_LEGACY_TAXONOMY_AUTHORITY, XBRL_US_LEGACY_TAXONOMY_RATE_SCOPE,
    XBRL_US_LEGACY_TAXONOMY_SOURCE_ID, resolve_filing_taxonomy_authority,
    route_filing_taxonomy_physical_locator,
};

#[cfg(test)]
mod tests;
