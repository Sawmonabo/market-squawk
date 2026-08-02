//! Evidence-reviewed built-in onboarding profiles.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_domain::{DataQuality, DigestAlgorithm, EvidenceDigest, SourceIdentifier};

use super::{
    AuthoritySet, CredentialKind, EvidenceBinding, FRED_ALFRED_API_SURFACE_ID, HumanBoundary,
    LifecycleSupport, ProviderCapability, ProviderCapabilityInput, ProviderCapabilityRevision,
    RatePolicyDescriptor, RightsAdmissionState, SetupMode,
};
use crate::onboarding::profile::{
    DataUseOperation, DataUseRight, OperationAdmission, ProbeTransport, ProfileActivationMode,
    ProfileEvidence, ProfileReleaseState, ProviderOnboardingProfile,
    ProviderOnboardingProfileInput, ProviderProfileError, ProviderProfileRegistry, Requirement,
    VerificationProbe, ZeroFeeStatus,
};
use crate::{
    BackoffPolicy, BudgetScope, BudgetWindowSemantics, ProviderBudgetPolicy, ProviderBudgetWindow,
};

const REVIEW_DATE: &str = "2026-07-23";
const PROJECT_HANDOFF: &str = "https://github.com/Sawmonabo/market-squawk";
const SECOND_NANOS: u64 = 1_000_000_000;
const MINUTE_NANOS: u64 = 60 * SECOND_NANOS;
const DAY_NANOS: u64 = 86_400 * SECOND_NANOS;
const LEGACY_REPORT_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0x55, 0xb7, 0xf0, 0x38, 0x50, 0x15, 0xfb, 0xd3, 0x18, 0xc8, 0x77, 0xf9, 0x9e, 0x32, 0x9c,
        0x31, 0x98, 0x02, 0x4a, 0x31, 0xfd, 0x8f, 0x05, 0xcb, 0x9e, 0x7e, 0x12, 0xe7, 0x66, 0x31,
        0x80, 0xcb,
    ],
);
const PROVIDER_RELEASE_REPORT_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0x74, 0xb4, 0x3e, 0x5e, 0x35, 0xf2, 0x47, 0xf5, 0x62, 0x54, 0x0e, 0x65, 0x7a, 0xcc, 0x1a,
        0x9f, 0xd4, 0xd8, 0xe9, 0xf0, 0xde, 0x0c, 0x8a, 0x1b, 0x63, 0xeb, 0x4a, 0xa3, 0x6a, 0x9a,
        0x73, 0xcf,
    ],
);
const COINBASE_DIRECT_COMPOSITION_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0xdf, 0x90, 0xf3, 0xc1, 0x53, 0x0e, 0x9a, 0xc6, 0xd4, 0x4f, 0x89, 0x48, 0x00, 0x10, 0x1f,
        0xdf, 0x61, 0xf9, 0x3a, 0x8b, 0x93, 0x33, 0xd3, 0x25, 0x6c, 0xb5, 0x77, 0xd7, 0x2f, 0x8e,
        0x75, 0x90,
    ],
);
const TREASURY_DAILY_RATES_AUTHORITY_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0x43, 0x17, 0x26, 0xc4, 0x84, 0x3c, 0x75, 0x7b, 0xe6, 0x5e, 0x79, 0x8c, 0x64, 0x9c, 0x50,
        0xfb, 0x14, 0x88, 0x1c, 0xc5, 0xfa, 0x05, 0xe3, 0x7c, 0xce, 0xd7, 0x68, 0x8e, 0x1d, 0x5e,
        0x7f, 0x92,
    ],
);
const FRED_SELF_HOSTED_AUTHORITY_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0x49, 0x41, 0xa3, 0x30, 0x88, 0xab, 0x87, 0x26, 0x42, 0x77, 0x9e, 0x3b, 0x7f, 0x67, 0x88,
        0x12, 0xd4, 0xed, 0x7d, 0x1a, 0xf0, 0x43, 0x42, 0x05, 0x4d, 0x0a, 0x0c, 0x93, 0xbf, 0x7f,
        0x81, 0xb4,
    ],
);
const FRED_TERMS_MANIFEST_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0xc2, 0xf5, 0x35, 0x99, 0x30, 0x7d, 0x77, 0x39, 0x87, 0x99, 0xcc, 0xda, 0x16, 0xec, 0xa3,
        0x4c, 0x78, 0xdf, 0xa2, 0x16, 0xb9, 0x83, 0xae, 0x95, 0xda, 0xab, 0xae, 0x68, 0x94, 0x98,
        0x2e, 0x77,
    ],
);
const FRED_UNRATE_RIGHTS_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0xa7, 0x6a, 0xb0, 0x03, 0x4e, 0xba, 0x4b, 0x87, 0x8b, 0x00, 0xa6, 0xec, 0xf2, 0x9f, 0x02,
        0x62, 0x70, 0x63, 0x9d, 0x23, 0x0c, 0x07, 0x4a, 0x29, 0x15, 0x2a, 0x1b, 0x29, 0x89, 0xff,
        0x7f, 0x0a,
    ],
);
const SEC_PUBLIC_API_AUTHORITY_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0xf4, 0x25, 0x65, 0x04, 0x19, 0x56, 0xc1, 0x33, 0x45, 0xae, 0xc3, 0xa3, 0xb5, 0x5e, 0x52,
        0x83, 0x4d, 0xc1, 0x5a, 0x78, 0x97, 0xe9, 0x26, 0xf6, 0x90, 0xc7, 0x56, 0xd1, 0x0d, 0x2f,
        0x4a, 0x80,
    ],
);
const BLS_PUBLIC_V1_AUTHORITY_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0x8c, 0xd0, 0x23, 0x7b, 0x36, 0x23, 0x23, 0x79, 0x58, 0x65, 0x10, 0xcc, 0x5c, 0x3b, 0xd3,
        0x7d, 0x6c, 0x64, 0xf9, 0x7b, 0x89, 0x79, 0xab, 0x4a, 0x23, 0xa7, 0x80, 0x28, 0x1a, 0x08,
        0xf1, 0x82,
    ],
);
const COINBASE_DIRECT_PROFILE: &str = "coinbase.exchange-direct-market-data";
const SEC_PROFILE: &str = "sec.edgar-public";
const BLS_PUBLIC_V1_PROFILE: &str = "bls.v1-unregistered";
const FRED_PROFILE: &str = FRED_ALFRED_API_SURFACE_ID;
const TREASURY_DAILY_RATES_PROFILE: &str = "treasury.daily-rates-xml";
const TREASURY_FISCAL_PROFILE: &str = "treasury.fiscal-data";
const SEC_PUBLIC_API_AUTHORITY_SOURCE: &str = "MSQ-SEC-EDGAR-PUBLIC-API-AUTHORITY-2026-07-26";
const BLS_PUBLIC_V1_AUTHORITY_SOURCE: &str = "MSQ-BLS-PUBLIC-V1-AUTHORITY-2026-07-26";
const TREASURY_DAILY_RATES_AUTHORITY_SOURCE: &str =
    "MSQ-TREASURY-DAILY-RATES-RELEASE-AUTHORITY-2026-07-26";
// Revision 4 is already durable catalog authority. Reconstruct its source identifier byte-for-byte
// without reusing its superseded product terminology for the current profile.
const FRED_REVISION_FOUR_AUTHORITY_SOURCE: &str =
    concat!("MSQ-FRED-ALFRED-LOCAL-", "FIRST-AUTHORITY-2026-07-26");
const FRED_SELF_HOSTED_AUTHORITY_SOURCE: &str = "MSQ-FRED-ALFRED-SELF-HOSTED-AUTHORITY-2026-07-26";
const FRED_TERMS_MANIFEST_SOURCE: &str = "MSQ-FRED-RIGHTS-MANIFEST-2026-07-26";
const FRED_UNRATE_RIGHTS_SOURCE: &str = "MSQ-FRED-UNRATE-PUBLIC-DOMAIN-2026-07-26";
/// Code-owned completed year used by the bounded Treasury daily-rate onboarding probe.
pub const TREASURY_DAILY_RATES_PROBE_YEAR: u16 = 2025;

const RIGHTS_LIMITED: &[DataUseRight] = &[
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::ModelTraining, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::Redistribute, OperationAdmission::Pending),
];
const RIGHTS_BLS: &[DataUseRight] = &[
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Admitted),
    DataUseRight::new(
        DataUseOperation::ModelTraining,
        OperationAdmission::Admitted,
    ),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::Redistribute, OperationAdmission::Pending),
];
const RIGHTS_FRED_TWO_GATE: &[DataUseRight] = &[
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::ModelTraining, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::Redistribute, OperationAdmission::Blocked),
];
const RIGHTS_ALL: &[DataUseRight] = &[
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Admitted),
    DataUseRight::new(
        DataUseOperation::ModelTraining,
        OperationAdmission::Admitted,
    ),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Redistribute, OperationAdmission::Admitted),
];
const RIGHTS_USER_OWNED: &[DataUseRight] = &[
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Admitted),
    DataUseRight::new(
        DataUseOperation::ModelTraining,
        OperationAdmission::Admitted,
    ),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Redistribute, OperationAdmission::Pending),
];

const EXCHANGE_DUTIES: &[&str] = &[
    "preserve exact provider and venue provenance",
    "do not admit persistence, modeling, export, or redistribution without a later rights decision",
];
const BLS_DUTIES: &[&str] = &[
    "retain BLS provenance and access date",
    "preserve the required disclaimer and truthful representation",
    "enforce the exact tier limits and third-party-rights boundary",
];
const FRED_SELF_HOSTED_DUTIES: &[&str] = &[
    "use only the official FRED API for programmatic access",
    "require exact written St. Louis Fed service permission for every durable or training operation",
    "require independent exact public-domain or owner permission for every selected series",
    "bind raw Bank evidence to an explicit local review decision and finite revalidation deadline",
    "retain exact series, vintage, provider, payload, and terms-bundle provenance",
    "keep durable use, export, and redistribution closed whenever either authority gate is absent",
];
const LOCAL_DUTIES: &[&str] = &[
    "retain the imported source record",
    "the user remains responsible for rights they do not own",
];
const NO_DUTIES: &[&str] = &[];
const COMMON_RECOVERY: &[&str] = &[
    "retry only after the reported deadline or provider-health condition clears",
    "resume the durable session by its opaque session identifier",
    "cancel the session if the requested scope or source is no longer intended",
];
const REFRESH_RECOVERY: &[&str] = &[
    "refresh the named official evidence sources and publish a new contiguous profile revision",
    "resume only after the catalog contains the newly admitted revision",
];
const FRED_SELF_HOSTED_RECOVERY: &[&str] = &[
    "refresh the exact FRED terms bundle before its local revalidation deadline",
    "import exact written St. Louis Fed permission and complete a hash-bound local review",
    "replace expired or incomplete Bank or exact-series evidence before durable activation",
    "resume the provider credential through the explicit foreground secret boundary",
];
const LOCAL_RECOVERY: &[&str] = &[
    "repair the selected local path or input record",
    "restart the bounded import or paper-account operation",
];

const COINBASE_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        "DOC-001",
        "https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api",
        REVIEW_DATE,
        None,
        false,
    ),
    ProfileEvidence::new(
        "DOC-007",
        "https://docs.cdp.coinbase.com/coinbase-app/api-architecture/rate-limiting",
        REVIEW_DATE,
        None,
        false,
    ),
];
const COINBASE_DIRECT_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        "CB-DIRECT-AUTH",
        "https://docs.cdp.coinbase.com/exchange/rest-api/authentication",
        "2026-07-25",
        None,
        false,
    ),
    ProfileEvidence::new(
        "CB-DIRECT-WS-AUTH",
        "https://docs.cdp.coinbase.com/exchange/websocket-feed/authentication",
        "2026-07-25",
        None,
        false,
    ),
    ProfileEvidence::new(
        "CB-DIRECT-CONNECTIONS",
        "https://help.coinbase.com/en/exchange/managing-my-account/market-data-connections",
        "2026-07-25",
        None,
        false,
    ),
    ProfileEvidence::new(
        "CB-EXCHANGE-REST-RATE-LIMITS",
        "https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits",
        "2026-07-25",
        None,
        false,
    ),
    ProfileEvidence::new(
        "CB-EXCHANGE-WS-RATE-LIMITS",
        "https://docs.cdp.coinbase.com/exchange/websocket-feed/rate-limits",
        "2026-07-25",
        None,
        false,
    ),
    ProfileEvidence::new(
        "CB-MARKET-DATA-TERMS",
        "https://www.coinbase.com/legal/market_data",
        "2026-07-25",
        None,
        false,
    ),
];
const KRAKEN_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        "DOC-011",
        "https://docs.kraken.com/exchange/guides/overview",
        REVIEW_DATE,
        None,
        false,
    ),
    ProfileEvidence::new(
        "DOC-016",
        "https://support.kraken.com/articles/206548367-what-are-the-api-rate-limits-",
        REVIEW_DATE,
        None,
        false,
    ),
];
const SEC_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SEC_PUBLIC_API_AUTHORITY_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/v1.0.0/docs/research/providers/sec-edgar-public-api-2026-07-26.md",
        "2026-07-26",
        Some(SEC_PUBLIC_API_AUTHORITY_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "DOC-019",
        "https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
        "2026-07-25",
        None,
        true,
    ),
    ProfileEvidence::new(
        "SEC-FAIR-ACCESS",
        "https://www.sec.gov/search-filings/edgar-search-assistance/accessing-edgar-data",
        "2026-07-25",
        None,
        true,
    ),
    ProfileEvidence::new(
        "DOC-020",
        "https://www.sec.gov/about/webmaster-frequently-asked-questions",
        "2026-07-25",
        None,
        true,
    ),
];
const FRED_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        FRED_TERMS_MANIFEST_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/v1.0.0/docs/verification/fred-rights-decision.json",
        "2026-07-26",
        Some(FRED_TERMS_MANIFEST_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        FRED_UNRATE_RIGHTS_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/v1.0.0/docs/verification/fred-unrate-public-domain-rights.json",
        "2026-07-26",
        Some(FRED_UNRATE_RIGHTS_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        FRED_SELF_HOSTED_AUTHORITY_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/v1.0.0/docs/research/providers/2026-07-26-fred-alfred-self-hosted-api-authority.md",
        "2026-07-26",
        Some(FRED_SELF_HOSTED_AUTHORITY_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "FRED-PERMISSIONS-CONTACT",
        "https://fred.stlouisfed.org/contactus/",
        "2026-07-26",
        None,
        true,
    ),
    ProfileEvidence::new(
        "DOC-021",
        "https://fred.stlouisfed.org/docs/api/api_key.html",
        REVIEW_DATE,
        None,
        false,
    ),
    ProfileEvidence::new(
        "DOC-023",
        "https://fredhelp.stlouisfed.org/fred/account/fred-account-features/register/",
        REVIEW_DATE,
        None,
        false,
    ),
    ProfileEvidence::new(
        "DOC-025",
        "https://fred.stlouisfed.org/legal/",
        "2026-07-25",
        None,
        true,
    ),
];
const BLS_V1_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        BLS_PUBLIC_V1_AUTHORITY_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/v1.0.0/docs/research/providers/bls-public-data-api-2026-07-21.md",
        "2026-07-26",
        Some(BLS_PUBLIC_V1_AUTHORITY_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "DOC-026",
        "https://www.bls.gov/developers/api_faqs.htm",
        "2026-07-25",
        None,
        true,
    ),
    ProfileEvidence::new(
        "DOC-029",
        "https://www.bls.gov/developers/termsOfService.htm",
        "2026-07-25",
        None,
        true,
    ),
    ProfileEvidence::new(
        "BLS-CONTENT-ORIGIN",
        "https://www.bls.gov/opub/copyright-information.htm",
        "2026-07-25",
        None,
        true,
    ),
];
const BLS_V2_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        "DOC-026",
        "https://www.bls.gov/developers/api_faqs.htm",
        "2026-07-25",
        None,
        true,
    ),
    ProfileEvidence::new(
        "DOC-027",
        "https://data.bls.gov/registrationEngine/",
        "2026-07-25",
        None,
        false,
    ),
    ProfileEvidence::new(
        "DOC-028",
        "https://www.bls.gov/developers/api_signature_v2.htm",
        "2026-07-25",
        None,
        true,
    ),
    ProfileEvidence::new(
        "DOC-029",
        "https://www.bls.gov/developers/termsOfService.htm",
        "2026-07-25",
        None,
        true,
    ),
    ProfileEvidence::new(
        "BLS-CONTENT-ORIGIN",
        "https://www.bls.gov/opub/copyright-information.htm",
        "2026-07-25",
        None,
        true,
    ),
];
const TREASURY_XML_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        "DOC-030",
        "https://home.treasury.gov/treasury-daily-interest-rate-xml-feed",
        "2026-07-26",
        None,
        false,
    ),
    ProfileEvidence::new(
        "TREASURY-DATA-GOV-NOMINAL-PAR-CC0",
        "https://catalog.data.gov/dataset/interest-rate-statistics-daily-treasury-yield-curve-rates",
        "2026-07-26",
        None,
        false,
    ),
    ProfileEvidence::new(
        "TREASURY-DATA-GOV-BILL-RATES-CC0",
        "https://catalog.data.gov/dataset/interest-rate-statistics-daily-treasury-bill-rates",
        "2026-07-26",
        None,
        false,
    ),
    ProfileEvidence::new(
        "TREASURY-DATA-GOV-LONG-TERM-CC0",
        "https://catalog.data.gov/dataset/daily-treasury-long-term-rate-data",
        "2026-07-26",
        None,
        false,
    ),
    ProfileEvidence::new(
        "TREASURY-DATA-GOV-REAL-PAR-CC0",
        "https://catalog.data.gov/dataset/daily-treasury-real-yield-curve-rates",
        "2026-07-26",
        None,
        false,
    ),
    ProfileEvidence::new(
        "TREASURY-DATA-GOV-REAL-LONG-TERM-CC0",
        "https://catalog.data.gov/dataset/daily-treasury-real-long-term-rates",
        "2026-07-26",
        None,
        false,
    ),
    ProfileEvidence::new(
        "CC0-1.0-LEGAL-CODE",
        "https://creativecommons.org/publicdomain/zero/1.0/legalcode",
        "2026-07-26",
        None,
        false,
    ),
    ProfileEvidence::new(
        TREASURY_DAILY_RATES_AUTHORITY_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/bf3a9099db391d51efab9fe839741003bc6b546a/docs/research/providers/2026-07-26-treasury-daily-rates-release-authority.md",
        "2026-07-26",
        Some(TREASURY_DAILY_RATES_AUTHORITY_DIGEST),
        false,
    ),
];
const FISCAL_EVIDENCE: &[ProfileEvidence] = &[ProfileEvidence::new(
    "DOC-031",
    "https://fiscaldata.treasury.gov/api-documentation/",
    REVIEW_DATE,
    Some(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [
            0xa9, 0xfd, 0x1a, 0xbe, 0x46, 0x38, 0x16, 0x4a, 0x71, 0x16, 0x42, 0xb9, 0x3c, 0x43,
            0x60, 0x6c, 0x47, 0xd3, 0x02, 0x9b, 0x5d, 0x52, 0x67, 0x93, 0x35, 0xa4, 0x6f, 0x88,
            0xdd, 0xd4, 0xaf, 0xc2,
        ],
    )),
    false,
)];
const LOCAL_EVIDENCE: &[ProfileEvidence] = &[ProfileEvidence::new(
    "market-squawk-local-capability-v1",
    PROJECT_HANDOFF,
    REVIEW_DATE,
    Some(LEGACY_REPORT_DIGEST),
    false,
)];

struct BuiltInSpec {
    id: &'static str,
    display_name: &'static str,
    official_entry: &'static str,
    setup: ProfileActivationMode,
    zero_fee: ZeroFeeStatus,
    account: Requirement,
    contact: Requirement,
    release: ProfileReleaseState,
    rights_state: RightsAdmissionState,
    authority: Option<&'static str>,
    permissions: &'static [&'static str],
    coverage: &'static str,
    quality: DataQuality,
    probe: VerificationProbe,
    rights: &'static [DataUseRight],
    duties: &'static [&'static str],
    persistence_evidence_source_id: Option<&'static str>,
    rotation: &'static str,
    revocation: &'static str,
    recovery: &'static [&'static str],
    evidence: &'static [ProfileEvidence],
    rate_policy: &'static str,
    refresh_trigger: &'static str,
    handoff_instruction: &'static str,
}

/// Builds the complete bounded registry used by the local portal and CLI services.
pub fn built_in_provider_profiles() -> Result<ProviderProfileRegistry, ProviderProfileError> {
    ProviderProfileRegistry::try_new(vec![
        build(coinbase()?)?,
        build(coinbase_direct()?)?,
        build(kraken()?)?,
        build(sec()?)?,
        build(fred()?)?,
        build(bls_v1()?)?,
        build(bls_v2()?)?,
        build(treasury_xml()?)?,
        build(fiscal_data()?)?,
        build(local_files())?,
        build(portfolio_imports())?,
        build(paper_execution())?,
    ])
}

fn build(spec: BuiltInSpec) -> Result<ProviderOnboardingProfile, ProviderProfileError> {
    let credentialed = spec.setup == ProfileActivationMode::ManualSecretImport;
    let prior_credential_kind = if credentialed {
        CredentialKind::ApiKey
    } else {
        CredentialKind::None
    };
    let historical_rights_state = if spec.id == FRED_PROFILE {
        RightsAdmissionState::Blocked
    } else {
        spec.rights_state
    };
    let legacy_capability = build_capability_with_rights_state(
        &spec,
        ProviderCapabilityRevision::new(1)?,
        prior_credential_kind,
        RatePolicyDescriptor::try_new(
            SourceIdentifier::try_from(spec.rate_policy)?,
            LEGACY_REPORT_DIGEST,
            true,
        )?,
        historical_rights_state,
    )?;
    let revision_two = build_capability_with_rights_state(
        &spec,
        ProviderCapabilityRevision::new(2)?,
        prior_credential_kind,
        RatePolicyDescriptor::try_new_enforced(
            SourceIdentifier::try_from(spec.rate_policy)?,
            LEGACY_REPORT_DIGEST,
            true,
            ProviderCapabilityRevision::new(1)?,
            SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
            LEGACY_REPORT_DIGEST,
            built_in_budget(&spec, false)?,
            spec.probe.transport() != ProbeTransport::Local,
        )?,
        historical_rights_state,
    )?;
    let (historical_capabilities, capability) = if spec.id == COINBASE_DIRECT_PROFILE {
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            CredentialKind::ApiKeySecretPassphrase,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(spec.rate_policy)?,
                COINBASE_DIRECT_COMPOSITION_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                COINBASE_DIRECT_COMPOSITION_DIGEST,
                built_in_budget(&spec, true)?,
                true,
            )?,
        )?;
        (vec![legacy_capability, revision_two], current)
    } else if spec.id == TREASURY_DAILY_RATES_PROFILE {
        let revision_three = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                spec.probe.transport() != ProbeTransport::Local,
            )?,
        )?;
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(4)?,
            prior_credential_kind,
            revision_three.rate_policy().clone(),
        )?;
        (
            vec![legacy_capability, revision_two, revision_three],
            current,
        )
    } else if spec.id == TREASURY_FISCAL_PROFILE {
        let revision_three = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                true,
            )?,
        )?;
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(4)?,
            prior_credential_kind,
            revision_three.rate_policy().clone(),
        )?;
        (
            vec![legacy_capability, revision_two, revision_three],
            current,
        )
    } else if spec.id == FRED_PROFILE {
        let revision_three = build_capability_with_rights_state(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                prior_fred_budget()?,
                true,
            )?,
            RightsAdmissionState::Blocked,
        )?;
        let revision_four = build_capability(
            &spec,
            ProviderCapabilityRevision::new(4)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                true,
            )?,
        )?;
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(5)?,
            prior_credential_kind,
            revision_four.rate_policy().clone(),
        )?;
        (
            vec![
                legacy_capability,
                revision_two,
                revision_three,
                revision_four,
            ],
            current,
        )
    } else if matches!(spec.id, SEC_PROFILE | BLS_PUBLIC_V1_PROFILE) {
        let revision_three = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                true,
            )?,
        )?;
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(4)?,
            prior_credential_kind,
            revision_three.rate_policy().clone(),
        )?;
        (
            vec![legacy_capability, revision_two, revision_three],
            current,
        )
    } else if has_provider_release_revision(spec.id) {
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                spec.probe.transport() != ProbeTransport::Local,
            )?,
        )?;
        (vec![legacy_capability, revision_two], current)
    } else {
        (vec![legacy_capability], revision_two)
    };
    ProviderOnboardingProfile::try_new(ProviderOnboardingProfileInput {
        id: spec.id,
        display_name: spec.display_name,
        historical_capabilities,
        capability,
        zero_fee: spec.zero_fee,
        account: spec.account,
        credential: if credentialed {
            Requirement::RequiredProviderControlled
        } else {
            Requirement::NotRequired
        },
        administrative_contact: spec.contact,
        activation_mode: spec.setup,
        release_state: spec.release,
        handoff_url: spec.official_entry,
        handoff_instruction: spec.handoff_instruction,
        permissions: spec.permissions,
        coverage: spec.coverage,
        quality_ceiling: spec.quality,
        probe: spec.probe,
        rights: spec.rights,
        rights_duties: spec.duties,
        persistence_evidence_source_id: spec.persistence_evidence_source_id,
        rotation: spec.rotation,
        revocation: spec.revocation,
        recovery: spec.recovery,
        evidence: spec.evidence,
    })
}

fn build_capability(
    spec: &BuiltInSpec,
    revision: ProviderCapabilityRevision,
    credential_kind: CredentialKind,
    rate_policy: RatePolicyDescriptor,
) -> Result<ProviderCapability, ProviderProfileError> {
    build_capability_with_rights_state(
        spec,
        revision,
        credential_kind,
        rate_policy,
        spec.rights_state,
    )
}

fn build_capability_with_rights_state(
    spec: &BuiltInSpec,
    revision: ProviderCapabilityRevision,
    credential_kind: CredentialKind,
    rate_policy: RatePolicyDescriptor,
    rights_state: RightsAdmissionState,
) -> Result<ProviderCapability, ProviderProfileError> {
    let credentialed = spec.setup == ProfileActivationMode::ManualSecretImport;
    let authority = spec.authority.map(SourceIdentifier::try_from).transpose()?;
    let minimum_authority =
        AuthoritySet::try_new(authority.clone().into_iter().collect::<Vec<_>>())?;
    let maximum_authority = AuthoritySet::try_new(authority.into_iter().collect::<Vec<_>>())?;
    Ok(ProviderCapability::try_new(ProviderCapabilityInput {
        surface_id: SourceIdentifier::try_from(spec.id)?,
        revision,
        setup_mode: if credentialed {
            SetupMode::ManualApiKeyImport
        } else {
            SetupMode::NoCredential
        },
        official_entry_uri: spec.official_entry.to_owned(),
        human_boundary: if credentialed {
            HumanBoundary::ProviderControlled
        } else {
            HumanBoundary::None
        },
        credential_kind,
        minimum_authority,
        maximum_authority,
        verifier_revision: SourceIdentifier::try_from(format!(
            "{}.probe.v{}",
            spec.id,
            if (spec.id == TREASURY_DAILY_RATES_PROFILE && revision.get() >= 3)
                || (spec.id == TREASURY_FISCAL_PROFILE && revision.get() >= 4)
            {
                2
            } else {
                1
            }
        ))?,
        rate_policy,
        rights_state,
        lifecycle_support: if matches!(
            spec.id,
            "bls.v2-registered" | "coinbase.exchange-direct-market-data"
        ) {
            LifecycleSupport::new(true, false, true)
        } else {
            LifecycleSupport::new(false, false, false)
        },
        evidence: capability_evidence(spec, revision)?,
        refresh_trigger: SourceIdentifier::try_from(
            if spec.id == SEC_PROFILE && revision.get() >= 4 {
                SEC_PUBLIC_API_AUTHORITY_SOURCE
            } else if spec.id == BLS_PUBLIC_V1_PROFILE && revision.get() >= 4 {
                BLS_PUBLIC_V1_AUTHORITY_SOURCE
            } else if spec.id == TREASURY_DAILY_RATES_PROFILE && revision.get() >= 4 {
                "TREASURY-XML-AUTHORITY-2026-07-26"
            } else if spec.id == FRED_PROFILE && revision.get() >= 4 {
                FRED_TERMS_MANIFEST_SOURCE
            } else {
                spec.refresh_trigger
            },
        )?,
    })?)
}

fn capability_evidence(
    spec: &BuiltInSpec,
    revision: ProviderCapabilityRevision,
) -> Result<Vec<EvidenceBinding>, ProviderProfileError> {
    let (report_source, report_digest) =
        if revision.get() >= 3 && has_provider_release_revision(spec.id) {
            (
                "MSQ-PROVIDER-RELEASE-EVIDENCE-2026-07-25",
                PROVIDER_RELEASE_REPORT_DIGEST,
            )
        } else {
            ("MSQ-ONBOARDING-REPORT-2026-07-23", LEGACY_REPORT_DIGEST)
        };
    let mut evidence = vec![EvidenceBinding::new(
        SourceIdentifier::try_from(report_source)?,
        report_digest,
    )];
    if spec.id == COINBASE_DIRECT_PROFILE && revision.get() >= 3 {
        evidence.push(EvidenceBinding::new(
            SourceIdentifier::try_from("MSQ-COINBASE-DIRECT-COMPOSITION-AUDIT-2026-07-25")?,
            COINBASE_DIRECT_COMPOSITION_DIGEST,
        ));
    }
    if spec.id == TREASURY_DAILY_RATES_PROFILE && revision.get() >= 4 {
        evidence.push(EvidenceBinding::new(
            SourceIdentifier::try_from(TREASURY_DAILY_RATES_AUTHORITY_SOURCE)?,
            TREASURY_DAILY_RATES_AUTHORITY_DIGEST,
        ));
    }
    if spec.id == SEC_PROFILE && revision.get() >= 4 {
        evidence.push(EvidenceBinding::new(
            SourceIdentifier::try_from(SEC_PUBLIC_API_AUTHORITY_SOURCE)?,
            SEC_PUBLIC_API_AUTHORITY_DIGEST,
        ));
    }
    if spec.id == BLS_PUBLIC_V1_PROFILE && revision.get() >= 4 {
        evidence.push(EvidenceBinding::new(
            SourceIdentifier::try_from(BLS_PUBLIC_V1_AUTHORITY_SOURCE)?,
            BLS_PUBLIC_V1_AUTHORITY_DIGEST,
        ));
    }
    if spec.id == FRED_PROFILE && revision.get() >= 4 {
        let authority_source = if revision.get() == 4 {
            FRED_REVISION_FOUR_AUTHORITY_SOURCE
        } else {
            FRED_SELF_HOSTED_AUTHORITY_SOURCE
        };
        evidence.push(EvidenceBinding::new(
            SourceIdentifier::try_from(authority_source)?,
            FRED_SELF_HOSTED_AUTHORITY_DIGEST,
        ));
        evidence.push(EvidenceBinding::new(
            SourceIdentifier::try_from(FRED_TERMS_MANIFEST_SOURCE)?,
            FRED_TERMS_MANIFEST_DIGEST,
        ));
    }
    Ok(evidence)
}

fn has_provider_release_revision(profile_id: &str) -> bool {
    matches!(
        profile_id,
        "sec.edgar-public"
            | "fred-alfred.api-v1-v2"
            | "bls.v1-unregistered"
            | "bls.v2-registered"
            | "treasury.daily-rates-xml"
            | "treasury.fiscal-data"
    )
}

fn current_rate_policy(spec: &BuiltInSpec) -> &'static str {
    if spec.id == "fred-alfred.api-v1-v2" {
        "fred-alfred.api-v1-v2.rate-policy.v2"
    } else {
        spec.rate_policy
    }
}

fn built_in_budget(
    spec: &BuiltInSpec,
    current_revision: bool,
) -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    let backoff =
        BackoffPolicy::try_new(nonzero_u64(SECOND_NANOS)?, nonzero_u64(MINUTE_NANOS)?, 0)?;
    match spec.id {
        "sec.edgar-public" => simple_budget("us-sec-edgar", None, 8, SECOND_NANOS, 4, backoff),
        "bls.v1-unregistered" => bls_budget(None, 25, backoff),
        "bls.v2-registered" => bls_budget(Some("bls.registered-onboarding"), 500, backoff),
        FRED_PROFILE if current_revision => fred_budget(backoff, "fred.onboarding-api-key"),
        "fred-alfred.api-v1-v2" => simple_budget(
            "fred",
            Some("fred.onboarding-rights-blocked"),
            120,
            MINUTE_NANOS,
            2,
            backoff,
        ),
        "treasury.daily-rates-xml" | "treasury.fiscal-data" => {
            simple_budget("us-treasury", None, 100, MINUTE_NANOS, 2, backoff)
        }
        "coinbase.public-market-data" => {
            simple_budget("coinbase", None, 1, MINUTE_NANOS, 1, backoff)
        }
        // Market Squawk's conservative combined safety ceiling for Direct network operations.
        // Coinbase documents distinct per-IP and account/message limits; this is not presented as
        // one provider-published aggregate contract.
        COINBASE_DIRECT_PROFILE => simple_budget(
            "coinbase-exchange",
            Some(if current_revision {
                "coinbase.exchange-direct.account-template"
            } else {
                "coinbase.exchange-direct.default-account"
            }),
            8,
            SECOND_NANOS,
            2,
            backoff,
        ),
        "kraken.spot-public-market-data" => {
            simple_budget("kraken", None, 1, MINUTE_NANOS, 1, backoff)
        }
        "local.files" | "local.portfolio-imports" | "local.paper-execution" => {
            simple_budget("market-squawk-local", None, 1, SECOND_NANOS, 1, backoff)
        }
        _ => Err(ProviderProfileError::InvalidProfile),
    }
}

fn simple_budget(
    provider: &str,
    account: Option<&str>,
    requests: u32,
    window_nanos: u64,
    concurrency: u16,
    backoff: BackoffPolicy,
) -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    let provider = SourceIdentifier::try_from(provider)?;
    let scope = match account {
        Some(account) => {
            BudgetScope::with_authorization_account(provider, SourceIdentifier::try_from(account)?)
        }
        None => BudgetScope::new(provider),
    };
    Ok(ProviderBudgetPolicy::try_new(
        scope,
        NonZeroU32::new(requests).ok_or(ProviderProfileError::InvalidProfile)?,
        nonzero_u64(window_nanos)?,
        NonZeroU16::new(concurrency).ok_or(ProviderProfileError::InvalidProfile)?,
        backoff,
    )?)
}

fn prior_fred_budget() -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    fred_budget(
        BackoffPolicy::try_new(nonzero_u64(SECOND_NANOS)?, nonzero_u64(MINUTE_NANOS)?, 0)?,
        "fred.onboarding-rights-blocked",
    )
}

fn fred_budget(
    backoff: BackoffPolicy,
    account: &str,
) -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    // The one-second window is Market Squawk's conservative ceiling for the combined v1/v2
    // profile. The retained official evidence is specific to v2, so it is not represented as a
    // provider-published v1 limit.
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(2).ok_or(ProviderProfileError::InvalidProfile)?,
            nonzero_u64(SECOND_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(120).ok_or(ProviderProfileError::InvalidProfile)?,
            nonzero_u64(MINUTE_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )?,
    ];
    Ok(ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(
            SourceIdentifier::try_from("fred")?,
            SourceIdentifier::try_from(account)?,
        ),
        &windows,
        NonZeroU16::new(2).ok_or(ProviderProfileError::InvalidProfile)?,
        backoff,
    )?)
}

fn bls_budget(
    account: Option<&str>,
    daily_requests: u32,
    backoff: BackoffPolicy,
) -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    let provider = SourceIdentifier::try_from("us-bls")?;
    let scope = match account {
        Some(account) => {
            BudgetScope::with_authorization_account(provider, SourceIdentifier::try_from(account)?)
        }
        None => BudgetScope::new(provider),
    };
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(50).ok_or(ProviderProfileError::InvalidProfile)?,
            nonzero_u64(10 * SECOND_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(daily_requests).ok_or(ProviderProfileError::InvalidProfile)?,
            nonzero_u64(DAY_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )?,
    ];
    Ok(ProviderBudgetPolicy::try_new_conjunctive(
        scope,
        &windows,
        NonZeroU16::new(2).ok_or(ProviderProfileError::InvalidProfile)?,
        backoff,
    )?)
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, ProviderProfileError> {
    NonZeroU64::new(value).ok_or(ProviderProfileError::InvalidProfile)
}

fn coinbase() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: "coinbase.public-market-data",
        display_name: "Coinbase Advanced Trade public market data",
        official_entry: "https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/rest-api",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NoCredentialFeeNotEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RightsLimited,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Coinbase Advanced Trade public products and market data; not consolidated",
        quality: DataQuality::DirectUnverified,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://api.coinbase.com/api/v3/brokerage/market/products/BTC-USD",
            None,
        )?,
        rights: RIGHTS_LIMITED,
        duties: EXCHANGE_DUTIES,
        persistence_evidence_source_id: None,
        rotation: "not applicable: this surface has no credential",
        revocation: "not applicable: this surface has no credential",
        recovery: COMMON_RECOVERY,
        evidence: COINBASE_EVIDENCE,
        rate_policy: "coinbase.public-market-data.rate-policy.v1",
        refresh_trigger: "CB-PUBLIC",
        handoff_instruction: "No account or key is requested; continue with the bounded public probe.",
    })
}

fn coinbase_direct() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: "coinbase.exchange-direct-market-data",
        display_name: "Coinbase Exchange Direct Market Data",
        official_entry: "https://help.coinbase.com/en/exchange/managing-my-account/how-to-create-an-api-key",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RightsLimited,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("coinbase.exchange.market-data.read"),
        permissions: &["view"],
        coverage: "Authenticated Coinbase Exchange ws-direct full-channel products; single venue and not consolidated",
        quality: DataQuality::DirectVerified,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://api.exchange.coinbase.com/users/self/verify",
            None,
        )?,
        rights: RIGHTS_LIMITED,
        duties: EXCHANGE_DUTIES,
        persistence_evidence_source_id: None,
        rotation: "create a replacement View-only Exchange key and import the complete envelope as a higher generation",
        revocation: "delete the exact Exchange key remotely, then delete the exact local generation",
        recovery: COMMON_RECOVERY,
        evidence: COINBASE_DIRECT_EVIDENCE,
        rate_policy: "coinbase.exchange-direct-market-data.rate-policy.v1",
        refresh_trigger: "CB-EXCHANGE-DIRECT",
        handoff_instruction: "Create a Coinbase Exchange API key with View permission only, then import one version-1 envelope containing api_key, passphrase, and signing_secret.",
    })
}

fn kraken() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: "kraken.spot-public-market-data",
        display_name: "Kraken Spot public market data",
        official_entry: "https://docs.kraken.com/exchange/guides/overview",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NoCredentialFeeNotEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RightsLimited,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Kraken Spot venue public instruments and books; not consolidated",
        quality: DataQuality::DirectUnverified,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://api.kraken.com/0/public/SystemStatus",
            None,
        )?,
        rights: RIGHTS_LIMITED,
        duties: EXCHANGE_DUTIES,
        persistence_evidence_source_id: None,
        rotation: "not applicable: this surface has no credential",
        revocation: "not applicable: this surface has no credential",
        recovery: COMMON_RECOVERY,
        evidence: KRAKEN_EVIDENCE,
        rate_policy: "kraken.spot-public-market-data.rate-policy.v1",
        refresh_trigger: "KR-PUBLIC",
        handoff_instruction: "No account or key is requested; continue with the bounded public probe.",
    })
}

fn sec() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: SEC_PROFILE,
        display_name: "SEC EDGAR submissions and XBRL",
        official_entry: "https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::NotRequired,
        contact: Requirement::RequiredNonSecret,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Public EDGAR submissions and company facts only; other sec.gov assets excluded",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://data.sec.gov/submissions/CIK0000320193.json",
            None,
        )?,
        rights: RIGHTS_ALL,
        duties: &[
            "declare an application/company and administrative contact in User-Agent",
            "retain public EDGAR provenance and enforce the aggregate fair-access limit",
        ],
        persistence_evidence_source_id: Some(SEC_PUBLIC_API_AUTHORITY_SOURCE),
        rotation: "update the declared non-secret contact when administrative ownership changes",
        revocation: "remove the source configuration locally",
        recovery: COMMON_RECOVERY,
        evidence: SEC_EVIDENCE,
        rate_policy: "sec.edgar-public.aggregate-rate-policy.v1",
        refresh_trigger: "SEC",
        handoff_instruction: "Provide a truthful non-secret organization and monitored administrative email, then continue with the bounded public probe.",
    })
}

fn fred() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: FRED_PROFILE,
        display_name: "FRED and ALFRED API v1",
        official_entry: "https://fred.stlouisfed.org/docs/api/api_key.html",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RightsLimited,
        rights_state: RightsAdmissionState::Pending,
        authority: Some("fred.series.read"),
        permissions: &["series.read"],
        coverage: "Bounded ephemeral FRED/ALFRED retrieval; durable use requires exact written St. Louis Fed service permission plus independent exact-series authority",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network_secret_query(
            ProbeTransport::HttpGet,
            "https://api.stlouisfed.org/fred/series",
            &[("series_id", "UNRATE"), ("file_type", "json")],
            "api_key",
            32,
        )?,
        rights: RIGHTS_FRED_TWO_GATE,
        duties: FRED_SELF_HOSTED_DUTIES,
        persistence_evidence_source_id: None,
        rotation: "create a replacement provider key and import it as a higher generation",
        revocation: "delete the exact provider key remotely, then delete the exact local generation",
        recovery: FRED_SELF_HOSTED_RECOVERY,
        evidence: FRED_EVIDENCE,
        rate_policy: "fred-alfred.api-v1-v2.rate-policy.v1",
        refresh_trigger: "FRED",
        handoff_instruction: "Create a zero-fee provider API key for ephemeral access. Durable activation additionally requires exact written St. Louis Fed permission and exact-series authority.",
    })
}

fn bls_v1() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: BLS_PUBLIC_V1_PROFILE,
        display_name: "BLS public API v1 unregistered",
        official_entry: "https://www.bls.gov/developers/api_signature.htm",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "BLS v1 series within the lower unregistered request limits",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network(
            ProbeTransport::HttpPostJson,
            "https://api.bls.gov/publicAPI/v1/timeseries/data/",
            Some(r#"{"seriesid":["LNS14000000"],"startyear":"2025","endyear":"2025"}"#),
        )?,
        rights: RIGHTS_BLS,
        duties: BLS_DUTIES,
        persistence_evidence_source_id: Some(BLS_PUBLIC_V1_AUTHORITY_SOURCE),
        rotation: "not applicable: this surface has no credential",
        revocation: "not applicable: this surface has no credential",
        recovery: COMMON_RECOVERY,
        evidence: BLS_V1_EVIDENCE,
        rate_policy: "bls.v1-unregistered.rate-policy.v1",
        refresh_trigger: "BLS-V1",
        handoff_instruction: "No account or key is required; continue with the bounded public v1 probe.",
    })
}

fn bls_v2() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: "bls.v2-registered",
        display_name: "BLS public API v2 registered",
        official_entry: "https://data.bls.gov/registrationEngine/",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::NotSeparatelyEstablished,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::RequiredNonSecret,
        release: ProfileReleaseState::RefreshRequired,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("bls.timeseries.read"),
        permissions: &["timeseries.read"],
        coverage: "BLS v2 series within registered-tier limits and annual key renewal",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network(
            ProbeTransport::HttpPostJson,
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            Some(r#"{"seriesid":["LNS14000000"],"startyear":"2025","endyear":"2025"}"#),
        )?,
        rights: RIGHTS_BLS,
        duties: BLS_DUTIES,
        persistence_evidence_source_id: Some("DOC-029"),
        rotation: "renew at least annually and import the emailed replacement as a higher generation",
        revocation: "delete the exact local generation; no reviewed remote key-revocation API exists",
        recovery: REFRESH_RECOVERY,
        evidence: BLS_V2_EVIDENCE,
        rate_policy: "bls.v2-registered.rate-policy.v1",
        refresh_trigger: "BLS-V2",
        handoff_instruction: "Complete organization/email, CAPTCHA, terms, and emailed-key retrieval on the official page, then import the key write-only.",
    })
}

fn treasury_xml() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: TREASURY_DAILY_RATES_PROFILE,
        display_name: "U.S. Treasury daily interest-rate XML",
        official_entry: "https://home.treasury.gov/treasury-daily-interest-rate-xml-feed",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "All five Treasury daily-rate families: daily_treasury_yield_curve, daily_treasury_bill_rates, daily_treasury_long_term_rate, daily_treasury_real_yield_curve, daily_treasury_real_long_term",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network_exact_public_query(
            ProbeTransport::HttpGet,
            "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml",
            "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=daily_treasury_yield_curve&field_tdr_date_value=2025",
            &[
                ("data", "daily_treasury_yield_curve"),
                ("field_tdr_date_value", "2025"),
            ],
        )?,
        rights: RIGHTS_ALL,
        duties: &[
            "apply CC0 1.0 only to the five identified Treasury daily-rate datasets",
            "retain the dataset family, official URL, retrieval time, payload digest, provider record identity, and publication and effective-time provenance",
            "exclude Treasury seals, trademarks, unrelated website media, and third-party materials from this dataset-level admission",
        ],
        persistence_evidence_source_id: Some(TREASURY_DAILY_RATES_AUTHORITY_SOURCE),
        rotation: "not applicable: this surface has no credential",
        revocation: "not applicable: this surface has no credential",
        recovery: COMMON_RECOVERY,
        evidence: TREASURY_XML_EVIDENCE,
        rate_policy: "treasury.daily-rates-xml.rate-policy.v1",
        refresh_trigger: "TREASURY-XML",
        handoff_instruction: "No account or token is required; continue with the bounded public daily-rate probe.",
    })
}

fn fiscal_data() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: TREASURY_FISCAL_PROFILE,
        display_name: "U.S. Treasury Fiscal Data API",
        official_entry: "https://fiscaldata.treasury.gov/api-documentation/",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Fiscal Data API datasets with dataset/version-specific provenance",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network_exact_public_query(
            ProbeTransport::HttpGet,
            "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates",
            "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?page%5Bsize%5D=1",
            &[("page[size]", "1")],
        )?,
        rights: RIGHTS_ALL,
        duties: &[
            "bind the license and source lineage to the exact Fiscal Data dataset and version",
        ],
        persistence_evidence_source_id: Some("DOC-031"),
        rotation: "not applicable: this surface has no credential",
        revocation: "not applicable: this surface has no credential",
        recovery: COMMON_RECOVERY,
        evidence: FISCAL_EVIDENCE,
        rate_policy: "treasury.fiscal-data.rate-policy.v1",
        refresh_trigger: "FISCAL",
        handoff_instruction: "No account or token is requested; continue with the bounded API probe.",
    })
}

fn local_files() -> BuiltInSpec {
    BuiltInSpec {
        id: "local.files",
        display_name: "Local files",
        official_entry: PROJECT_HANDOFF,
        setup: ProfileActivationMode::Local,
        zero_fee: ZeroFeeStatus::Local,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "User-selected CSV, TSV, JSON, NDJSON, XML, Excel, and Parquet inputs",
        quality: DataQuality::DirectUnverified,
        probe: VerificationProbe::local(
            "local file adapter is installed and input remains caller-selected",
        ),
        rights: RIGHTS_USER_OWNED,
        duties: LOCAL_DUTIES,
        persistence_evidence_source_id: Some("market-squawk-local-capability-v1"),
        rotation: "not applicable: this surface has no credential",
        revocation: "remove the local source registration; source files remain user-owned",
        recovery: LOCAL_RECOVERY,
        evidence: LOCAL_EVIDENCE,
        rate_policy: "local.files.resource-policy.v1",
        refresh_trigger: "LOCAL-FILES",
        handoff_instruction: "Choose a file later through the bounded file-import command; no path is accepted by the portal.",
    }
}

fn portfolio_imports() -> BuiltInSpec {
    BuiltInSpec {
        id: "local.portfolio-imports",
        display_name: "Portfolio holdings and transactions imports",
        official_entry: PROJECT_HANDOFF,
        setup: ProfileActivationMode::Local,
        zero_fee: ZeroFeeStatus::Local,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "User-owned holdings, transactions, cash flows, and cost-basis exports",
        quality: DataQuality::DirectUnverified,
        probe: VerificationProbe::local(
            "portfolio import adapter and reconciliation service are installed",
        ),
        rights: RIGHTS_USER_OWNED,
        duties: LOCAL_DUTIES,
        persistence_evidence_source_id: Some("market-squawk-local-capability-v1"),
        rotation: "not applicable: this surface has no credential",
        revocation: "remove the import registration; preserve source records required for reconciliation",
        recovery: LOCAL_RECOVERY,
        evidence: LOCAL_EVIDENCE,
        rate_policy: "local.portfolio-imports.resource-policy.v1",
        refresh_trigger: "LOCAL-PORTFOLIO",
        handoff_instruction: "Select a supported broker or portfolio export later; the portal accepts no filesystem path.",
    }
}

fn paper_execution() -> BuiltInSpec {
    BuiltInSpec {
        id: "local.paper-execution",
        display_name: "Local paper execution",
        official_entry: PROJECT_HANDOFF,
        setup: ProfileActivationMode::Local,
        zero_fee: ZeroFeeStatus::Local,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Local simulated orders, fills, balances, positions, fees, latency, and slippage",
        quality: DataQuality::Modeled,
        probe: VerificationProbe::local(
            "paper adapter and centralized risk authority are installed",
        ),
        rights: RIGHTS_ALL,
        duties: NO_DUTIES,
        persistence_evidence_source_id: Some("market-squawk-local-capability-v1"),
        rotation: "not applicable: this surface has no credential",
        revocation: "disable the paper account through controlled execution authority",
        recovery: LOCAL_RECOVERY,
        evidence: LOCAL_EVIDENCE,
        rate_policy: "local.paper-execution.resource-policy.v1",
        refresh_trigger: "LOCAL-PAPER",
        handoff_instruction: "No external account or key is requested; all orders remain under local risk authority.",
    }
}
