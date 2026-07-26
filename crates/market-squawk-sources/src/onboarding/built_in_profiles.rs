//! Evidence-reviewed built-in onboarding profiles.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_domain::{DataQuality, DigestAlgorithm, EvidenceDigest, SourceIdentifier};

use super::{
    AuthoritySet, CredentialKind, EvidenceBinding, HumanBoundary, LifecycleSupport,
    ProviderCapability, ProviderCapabilityInput, ProviderCapabilityRevision, RatePolicyDescriptor,
    RightsAdmissionState, SetupMode,
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
const REPORT_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0x55, 0xb7, 0xf0, 0x38, 0x50, 0x15, 0xfb, 0xd3, 0x18, 0xc8, 0x77, 0xf9, 0x9e, 0x32, 0x9c,
        0x31, 0x98, 0x02, 0x4a, 0x31, 0xfd, 0x8f, 0x05, 0xcb, 0x9e, 0x7e, 0x12, 0xe7, 0x66, 0x31,
        0x80, 0xcb,
    ],
);

const RIGHTS_LIMITED: &[DataUseRight] = &[
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::ModelTraining, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Pending),
    DataUseRight::new(DataUseOperation::Redistribute, OperationAdmission::Pending),
];
const RIGHTS_BLOCKED: &[DataUseRight] = &[
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Blocked),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Blocked),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Blocked),
    DataUseRight::new(DataUseOperation::ModelTraining, OperationAdmission::Blocked),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Blocked),
    DataUseRight::new(DataUseOperation::Redistribute, OperationAdmission::Blocked),
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
const RIGHTS_RECOVERY: &[&str] = &[
    "record a qualified scope-specific rights decision before importing a key",
    "obtain any required series-owner permission and publish a new contiguous profile revision",
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
        "DOC-019",
        "https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
        REVIEW_DATE,
        None,
        true,
    ),
    ProfileEvidence::new(
        "DOC-020",
        "https://www.sec.gov/about/webmaster-frequently-asked-questions",
        REVIEW_DATE,
        None,
        true,
    ),
];
const FRED_EVIDENCE: &[ProfileEvidence] = &[
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
        REVIEW_DATE,
        None,
        false,
    ),
];
const BLS_V1_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        "DOC-026",
        "https://www.bls.gov/developers/api_FAQs.htm",
        REVIEW_DATE,
        None,
        true,
    ),
    ProfileEvidence::new(
        "DOC-029",
        "https://www.bls.gov/developers/termsOfService.htm",
        REVIEW_DATE,
        None,
        true,
    ),
];
const BLS_V2_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        "DOC-027",
        "https://data.bls.gov/registrationEngine/",
        REVIEW_DATE,
        None,
        false,
    ),
    ProfileEvidence::new(
        "DOC-028",
        "https://www.bls.gov/developers/api_signature_v2.htm",
        REVIEW_DATE,
        None,
        true,
    ),
    ProfileEvidence::new(
        "DOC-029",
        "https://www.bls.gov/developers/termsOfService.htm",
        REVIEW_DATE,
        None,
        true,
    ),
];
const TREASURY_XML_EVIDENCE: &[ProfileEvidence] = &[ProfileEvidence::new(
    "DOC-030",
    "https://home.treasury.gov/treasury-daily-interest-rate-xml-feed",
    REVIEW_DATE,
    None,
    false,
)];
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
    Some(REPORT_DIGEST),
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
    let legacy_capability = build_capability(
        &spec,
        ProviderCapabilityRevision::new(1)?,
        RatePolicyDescriptor::try_new(
            SourceIdentifier::try_from(spec.rate_policy)?,
            REPORT_DIGEST,
            true,
        )?,
    )?;
    let capability = build_capability(
        &spec,
        ProviderCapabilityRevision::new(2)?,
        RatePolicyDescriptor::try_new_enforced(
            SourceIdentifier::try_from(spec.rate_policy)?,
            REPORT_DIGEST,
            true,
            ProviderCapabilityRevision::new(1)?,
            SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
            REPORT_DIGEST,
            built_in_budget(&spec)?,
            spec.probe.transport() != ProbeTransport::Local,
        )?,
    )?;
    let credentialed = spec.setup == ProfileActivationMode::ManualSecretImport;
    ProviderOnboardingProfile::try_new(ProviderOnboardingProfileInput {
        id: spec.id,
        display_name: spec.display_name,
        historical_capabilities: vec![legacy_capability],
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
    rate_policy: RatePolicyDescriptor,
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
        credential_kind: if credentialed {
            CredentialKind::ApiKey
        } else {
            CredentialKind::None
        },
        minimum_authority,
        maximum_authority,
        verifier_revision: SourceIdentifier::try_from(format!("{}.probe.v1", spec.id))?,
        rate_policy,
        rights_state: spec.rights_state,
        lifecycle_support: if spec.id == "bls.v2-registered" {
            LifecycleSupport::new(true, false, true)
        } else {
            LifecycleSupport::new(false, false, false)
        },
        evidence: vec![EvidenceBinding::new(
            SourceIdentifier::try_from("MSQ-ONBOARDING-REPORT-2026-07-23")?,
            REPORT_DIGEST,
        )],
        refresh_trigger: SourceIdentifier::try_from(spec.refresh_trigger)?,
    })?)
}

fn built_in_budget(spec: &BuiltInSpec) -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    let backoff =
        BackoffPolicy::try_new(nonzero_u64(SECOND_NANOS)?, nonzero_u64(MINUTE_NANOS)?, 0)?;
    match spec.id {
        "sec.edgar-public" => simple_budget("us-sec-edgar", None, 8, SECOND_NANOS, 4, backoff),
        "bls.v1-unregistered" => bls_budget(None, 25, backoff),
        "bls.v2-registered" => bls_budget(Some("bls.registered-onboarding"), 500, backoff),
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
        id: "sec.edgar-public",
        display_name: "SEC EDGAR submissions and XBRL",
        official_entry: "https://www.sec.gov/search-filings/edgar-application-programming-interfaces",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::NotRequired,
        contact: Requirement::RequiredNonSecret,
        release: ProfileReleaseState::RefreshRequired,
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
        persistence_evidence_source_id: Some("DOC-019"),
        rotation: "update the declared non-secret contact when administrative ownership changes",
        revocation: "remove the source configuration locally",
        recovery: REFRESH_RECOVERY,
        evidence: SEC_EVIDENCE,
        rate_policy: "sec.edgar-public.aggregate-rate-policy.v1",
        refresh_trigger: "SEC",
        handoff_instruction: "Provide a valid non-secret organization and administrative email after evidence refresh.",
    })
}

fn fred() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: "fred-alfred.api-v1-v2",
        display_name: "FRED and ALFRED API v1/v2",
        official_entry: "https://fred.stlouisfed.org/docs/api/api_key.html",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::FreeAccountRightsBlocked,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RightsBlocked,
        rights_state: RightsAdmissionState::Blocked,
        authority: Some("fred.series.read"),
        permissions: &["series.read"],
        coverage: "FRED series and ALFRED vintages; third-party series may carry separate rights",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://api.stlouisfed.org/fred/series",
            None,
        )?,
        rights: RIGHTS_BLOCKED,
        duties: &[
            "a successful key check does not admit storage, modeling, export, or AI-facing use",
        ],
        persistence_evidence_source_id: None,
        rotation: "create a replacement provider key and import it as a higher generation",
        revocation: "delete the exact provider key remotely, then delete the exact local generation",
        recovery: RIGHTS_RECOVERY,
        evidence: FRED_EVIDENCE,
        rate_policy: "fred-alfred.api-v1-v2.rate-policy.v1",
        refresh_trigger: "FRED",
        handoff_instruction: "Use the official key page only after qualified rights admission; Market Squawk cannot issue this key.",
    })
}

fn bls_v1() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: "bls.v1-unregistered",
        display_name: "BLS public API v1 unregistered",
        official_entry: "https://www.bls.gov/developers/api_signature.htm",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NotSeparatelyEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RefreshRequired,
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
        persistence_evidence_source_id: Some("DOC-029"),
        rotation: "not applicable: this surface has no credential",
        revocation: "not applicable: this surface has no credential",
        recovery: REFRESH_RECOVERY,
        evidence: BLS_V1_EVIDENCE,
        rate_policy: "bls.v1-unregistered.rate-policy.v1",
        refresh_trigger: "BLS-V1",
        handoff_instruction: "No account or key is requested; activation waits for evidence refresh.",
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
        id: "treasury.daily-rates-xml",
        display_name: "U.S. Treasury daily interest-rate XML",
        official_entry: "https://home.treasury.gov/treasury-daily-interest-rate-xml-feed",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NotSeparatelyEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RightsLimited,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Treasury daily interest-rate XML families; durable publication remains closed",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml",
            None,
        )?,
        rights: RIGHTS_LIMITED,
        duties: &["do not inherit Fiscal Data rights onto this separate XML surface"],
        persistence_evidence_source_id: None,
        rotation: "not applicable: this surface has no credential",
        revocation: "not applicable: this surface has no credential",
        recovery: COMMON_RECOVERY,
        evidence: TREASURY_XML_EVIDENCE,
        rate_policy: "treasury.daily-rates-xml.rate-policy.v1",
        refresh_trigger: "TREASURY-XML",
        handoff_instruction: "No account or token is requested; durable use remains rights-limited.",
    })
}

fn fiscal_data() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: "treasury.fiscal-data",
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
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/avg_interest_rates",
            None,
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
