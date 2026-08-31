//! Evidence-reviewed built-in onboarding profiles.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_domain::{DataQuality, DigestAlgorithm, EvidenceDigest, SourceIdentifier};

use super::{
    AuthoritySet, CredentialKind, EvidenceBinding, FRED_ALFRED_API_SURFACE_ID, HumanBoundary,
    LifecycleSupport, ProviderCapability, ProviderCapabilityInput, ProviderCapabilityRevision,
    RatePolicyDescriptor, RightsAdmissionState, SEC_EDGAR_AUTHORITY, SEC_EDGAR_PROFILE_ID,
    SetupMode,
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
const HOUR_NANOS: u64 = 60 * MINUTE_NANOS;
const DAY_NANOS: u64 = 86_400 * SECOND_NANOS;
const SCHWAB_MARKET_DATA_DOCTOR_ATTEMPTS: u32 = 20;
const SCHWAB_MARKET_DATA_DOCTOR_WINDOW_NANOS: u64 = 15 * MINUTE_NANOS;
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
const SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST: EvidenceDigest = EvidenceDigest::new(
    DigestAlgorithm::Sha256,
    [
        0x7a, 0x1f, 0x05, 0xd3, 0xf6, 0x48, 0x04, 0xea, 0xa8, 0x23, 0xde, 0x65, 0x92, 0x3c, 0x87,
        0x5e, 0x10, 0x75, 0xa4, 0x80, 0x18, 0x7c, 0xe4, 0x4a, 0xec, 0x94, 0xe1, 0xf6, 0xb9, 0xc0,
        0x8f, 0xfa,
    ],
);
const COINBASE_DIRECT_PROFILE: &str = "coinbase.exchange-direct-market-data";
const BLS_PUBLIC_V1_PROFILE: &str = "bls.v1-unregistered";
const FRED_PROFILE: &str = FRED_ALFRED_API_SURFACE_ID;
const ALPACA_BASIC_PROFILE: &str = "alpaca.basic-market-data";
const NASDAQ_REFERENCE_PROFILE: &str = "nasdaq-trader-symbol-directory-reference";
const SCHWAB_MARKET_DATA_PROFILE: &str = "schwab.trader-api-market-data";
const SCHWAB_USER_PREFERENCE_PROBE_URL: &str = "https://api.schwabapi.com/trader/v1/userPreference";
const YAHOO_ENRICHMENT_PROFILE: &str = "yahoo-finance.experimental-enrichment";
const IEX_HIST_PROFILE: &str = "iex.hist-feed-files";
const OCC_REFERENCE_PROFILE: &str = "occ.options-reference";
const CBOE_REFERENCE_PROFILE: &str = "cboe.options-reference";
const BEA_PROFILE: &str = "bea.api-data";
const CENSUS_PROFILE: &str = "census.data-api";
const EIA_PROFILE: &str = "eia.api-v2";
const FEDERAL_RESERVE_BOARD_PROFILE: &str = "federal-reserve-board.data-download-program";
const FEDERAL_RESERVE_BOARD_H15_PROBE_BASE_URL: &str =
    "https://www.federalreserve.gov/datadownload/Output.aspx";
pub(super) const FEDERAL_RESERVE_BOARD_H15_PROBE_URL: &str = concat!(
    "https://www.federalreserve.gov/datadownload/Output.aspx?filetype=csv&label=include&",
    "lastobs=10&layout=seriescolumn&rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&",
    "type=package"
);
const TIINGO_PROFILE: &str = "tiingo.starter-eod-nav";
const KRAKEN_L3_PROFILE: &str = "kraken.spot-authenticated-level3-market-data";
const TREASURY_DAILY_RATES_PROFILE: &str = "treasury.daily-rates-xml";
const TREASURY_FISCAL_PROFILE: &str = "treasury.fiscal-data";
const SEC_PUBLIC_API_AUTHORITY_SOURCE: &str = "MSQ-SEC-EDGAR-PUBLIC-API-AUTHORITY-2026-07-26";
const BLS_PUBLIC_V1_AUTHORITY_SOURCE: &str = "MSQ-BLS-PUBLIC-V1-AUTHORITY-2026-07-26";
const TREASURY_DAILY_RATES_AUTHORITY_SOURCE: &str =
    "MSQ-TREASURY-DAILY-RATES-RELEASE-AUTHORITY-2026-07-26";
const SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE: &str =
    "MSQ-SELECTED-MARKET-DATA-ARCHITECTURE-2026-08-11";
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
const RIGHTS_LOCAL_PERSONAL_RESEARCH: &[DataUseRight] = &[
    DataUseRight::new(DataUseOperation::Retrieve, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Display, OperationAdmission::Admitted),
    DataUseRight::new(DataUseOperation::Persist, OperationAdmission::Admitted),
    DataUseRight::new(
        DataUseOperation::ModelTraining,
        OperationAdmission::Admitted,
    ),
    DataUseRight::new(DataUseOperation::Export, OperationAdmission::Blocked),
    DataUseRight::new(DataUseOperation::Redistribute, OperationAdmission::Blocked),
];
const EXCHANGE_DUTIES: &[&str] = &[
    "preserve exact provider and venue provenance",
    "do not admit persistence, modeling, export, or redistribution without a later rights decision",
];
const PRIVATE_CRYPTO_RESEARCH_DUTIES: &[&str] = &[
    "preserve exact provider, venue, product, channel, generation, and raw-payload provenance",
    "restrict persisted source data and every transformed or model-derived artifact to owner-local personal research use",
    "prohibit sale, export, redistribution, third-party serving, and commercial exploitation of source data or derived datasets",
    "never infer account, position, order, funds, money-movement, or execution authority from a market-data surface",
];
const BLS_DUTIES: &[&str] = &[
    "retain BLS provenance and access date",
    "preserve the required disclaimer and truthful representation",
    "enforce the exact tier limits and third-party-rights boundary",
];
const FRED_PERSONAL_RESEARCH_DUTIES: &[&str] = &[
    "use only the official FRED API for programmatic access",
    "bind every acquisition and publication to the exact configured dataset, series, and vintage interval",
    "retain exact provider, payload, receipt, publication-clock, and immutable lineage evidence",
    "restrict persisted source data and every derived artifact to owner-local personal research and prohibit sale, export, redistribution, or third-party serving",
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
const FRED_RECOVERY: &[&str] = &[
    "rotate and import the API key through the foreground secret boundary when the provider rejects the current credential",
    "resume only the exact admitted dataset, series, and vintage interval through the shared bounded acquisition lane",
    "complete publication from sealed raw evidence or reacquire an incomplete provider page chain",
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
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
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
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
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
const ALPACA_BASIC_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "ALPACA-PAPER-ONLY",
        "https://docs.alpaca.markets/us/docs/paper-trading",
        "2026-08-11",
        None,
        false,
    ),
    ProfileEvidence::new(
        "ALPACA-BASIC-COVERAGE",
        "https://docs.alpaca.markets/us/docs/about-market-data-api",
        "2026-08-11",
        None,
        false,
    ),
    ProfileEvidence::new(
        "ALPACA-IEX-LATEST-QUOTE",
        "https://docs.alpaca.markets/us/v1.4.2/reference/stocklatestquotesingle-1",
        "2026-08-09",
        None,
        false,
    ),
    ProfileEvidence::new(
        "ALPACA-MARKET-DATA-STREAMING",
        "https://docs.alpaca.markets/us/v1.1/docs/streaming-market-data",
        "2026-08-11",
        None,
        false,
    ),
    ProfileEvidence::new(
        "ALPACA-REDISTRIBUTION-GUIDANCE",
        "https://alpaca.markets/support/redistribute-alpaca-api",
        "2026-08-09",
        None,
        true,
    ),
];
const NASDAQ_REFERENCE_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "NASDAQ-SYMBOL-DIRECTORY-DEFINITIONS",
        "https://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "NASDAQ-LISTED-DIRECTORY",
        "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "NASDAQ-OTHER-LISTED-DIRECTORY",
        "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
        "2026-08-11",
        None,
        true,
    ),
];
const SCHWAB_MARKET_DATA_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "SCHWAB-TRADER-API-INDIVIDUAL",
        "https://developer.schwab.com/products/trader-api--individual",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "SCHWAB-MARKET-DATA-PRODUCTION-DOCUMENTATION",
        "https://contentdelivery.schwab.com/api/content/rtcontent/asset/market-data-production--trader-api--individual--documentation",
        "2026-08-11",
        None,
        true,
    ),
];
const YAHOO_ENRICHMENT_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "YFINANCE-API-REFERENCE",
        "https://ranaroussi.github.io/yfinance/reference/index.html",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "YAHOO-FINANCE-EXCHANGE-DELAYS",
        "https://help.yahoo.com/kb/finance/article-exchanges-data-delays-sln2310.html",
        "2026-08-11",
        None,
        true,
    ),
];
const IEX_HIST_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "IEX-HIST-MARKET-DATA",
        "https://iextrading.com/trading/market-data/index.html",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "IEX-TOPS-SPECIFICATION",
        "https://www.iex.io/documents/tops-v1-66",
        "2026-08-11",
        None,
        true,
    ),
];
const OCC_REFERENCE_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "OCC-DIRECTORY-OF-LISTED-PRODUCTS",
        "https://www.theocc.com/market-data/market-data-reports/series-and-trading-data/directory-of-listed-products",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "OCC-INFORMATION-MEMOS",
        "https://infomemo.theocc.com/infomemo/search-memo",
        "2026-08-11",
        None,
        true,
    ),
];
const CBOE_REFERENCE_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "CBOE-US-OPTIONS-REFERENCE-DATA",
        "https://www.cboe.com/markets/us/options/market-statistics/reference-data",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "CBOE-TITANIUM-SYMBOLOGY",
        "https://www.cboe.com/document/tech-spec/document/technical-specifications/cboe-titanium-u.s.-equitiesoptionsfutures-symbology-reference",
        "2026-08-11",
        None,
        true,
    ),
];
const BEA_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "BEA-API-USER-GUIDE",
        "https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "BEA-CORRECTION-POLICY",
        "https://www.bea.gov/about/policies-and-information/correction",
        "2026-08-11",
        None,
        true,
    ),
];
const CENSUS_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "CENSUS-DATA-API-GUIDE",
        "https://www.census.gov/data/developers/guidance/api-user-guide.html",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "CENSUS-AVAILABLE-DATA",
        "https://www.census.gov/data/developers/guidance/api-user-guide.Available_Data.html",
        "2026-08-11",
        None,
        true,
    ),
];
const EIA_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "EIA-API-V2-TECHNICAL-DOCUMENTATION",
        "https://www.eia.gov/opendata/documentation.php",
        "2026-08-11",
        None,
        true,
    ),
];
const FEDERAL_RESERVE_BOARD_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "FEDERAL-RESERVE-BOARD-DDP",
        "https://www.federalreserve.gov/datadownload/",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "FEDERAL-RESERVE-BOARD-DDP-HELP",
        "https://www.federalreserve.gov/datadownload/help/",
        "2026-08-11",
        None,
        true,
    ),
];
const TIINGO_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "TIINGO-EOD-DOCUMENTATION",
        "https://www.tiingo.com/documentation/end-of-day",
        "2026-08-11",
        None,
        true,
    ),
    ProfileEvidence::new(
        "TIINGO-STARTER-PRICING",
        "https://www.tiingo.com/about/pricing",
        "2026-08-11",
        None,
        true,
    ),
];
const KRAKEN_L3_EVIDENCE: &[ProfileEvidence] = &[
    ProfileEvidence::new(
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
    ),
    ProfileEvidence::new(
        "KRAKEN-L3-CHANNEL",
        "https://docs.kraken.com/api/docs/websocket-v2/level3/",
        "2026-08-09",
        None,
        false,
    ),
    ProfileEvidence::new(
        "KRAKEN-API-KEY-INFO",
        "https://docs.kraken.com/api/docs/rest-api/get-api-key-info",
        "2026-08-09",
        None,
        false,
    ),
    ProfileEvidence::new(
        "KRAKEN-SPOT-REST-AUTH",
        "https://docs.kraken.com/api/docs/guides/spot-rest-auth/",
        "2026-08-09",
        None,
        false,
    ),
    ProfileEvidence::new(
        "KRAKEN-WEBSOCKET-TOKEN",
        "https://docs.kraken.com/api/docs/rest-api/get-websockets-token/",
        "2026-08-09",
        None,
        false,
    ),
    ProfileEvidence::new(
        "KRAKEN-GLOBAL-TERMS",
        "https://www.kraken.com/legal/global-terms",
        "2026-08-09",
        None,
        true,
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
        SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE,
        "https://github.com/Sawmonabo/market-squawk/blob/1b7231087780845e2a8358f8cb63a4525f6b38a3/docs/architecture/market-data-provider-architecture.md",
        "2026-08-11",
        Some(SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST),
        false,
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
        build(alpaca_basic()?)?,
        build(nasdaq_reference()?)?,
        build(schwab_market_data()?)?,
        build(yahoo_enrichment()?)?,
        build(iex_hist()?)?,
        build(occ_reference()?)?,
        build(cboe_reference()?)?,
        build(bea()?)?,
        build(census()?)?,
        build(eia()?)?,
        build(federal_reserve_board()?)?,
        build(tiingo()?)?,
        build(kraken()?)?,
        build(kraken_l3()?)?,
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
    if spec.id == FRED_PROFILE {
        return build_current_fred(spec);
    }
    let credentialed = spec.setup == ProfileActivationMode::ManualSecretImport;
    let prior_credential_kind = initial_credential_kind(spec.id, credentialed);
    let legacy_capability = build_capability_with_rights_state(
        &spec,
        ProviderCapabilityRevision::new(1)?,
        prior_credential_kind,
        RatePolicyDescriptor::try_new(
            SourceIdentifier::try_from(spec.rate_policy)?,
            LEGACY_REPORT_DIGEST,
            true,
        )?,
        spec.rights_state,
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
            spec.probe.transport() != ProbeTransport::Local
                && spec.id != FEDERAL_RESERVE_BOARD_PROFILE
                && spec.id != SCHWAB_MARKET_DATA_PROFILE,
        )?,
        spec.rights_state,
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
                SourceIdentifier::try_from(spec.rate_policy)?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                spec.probe.transport() != ProbeTransport::Local,
            )?,
        )?;
        let revision_four = build_capability(
            &spec,
            ProviderCapabilityRevision::new(4)?,
            prior_credential_kind,
            revision_three.rate_policy().clone(),
        )?;
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(5)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(3)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                treasury_budget()?,
                true,
            )?,
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
    } else if spec.id == TREASURY_FISCAL_PROFILE {
        let revision_three = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(spec.rate_policy)?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                true,
            )?,
        )?;
        let revision_four = build_capability(
            &spec,
            ProviderCapabilityRevision::new(4)?,
            prior_credential_kind,
            revision_three.rate_policy().clone(),
        )?;
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(5)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(3)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                treasury_budget()?,
                true,
            )?,
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
    } else if spec.id == FEDERAL_RESERVE_BOARD_PROFILE {
        let revision_three = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(spec.rate_policy)?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                false,
            )?,
        )?;
        let current = build_capability(
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
        (
            vec![legacy_capability, revision_two, revision_three],
            current,
        )
    } else if matches!(spec.id, SEC_EDGAR_PROFILE_ID | BLS_PUBLIC_V1_PROFILE) {
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
    } else if spec.id == SCHWAB_MARKET_DATA_PROFILE {
        // Preserve revision three's local-placeholder policy exactly. Revision four binds the
        // network User Preference bootstrap and the complete twenty-attempt entitlement doctor to
        // one conservative application/account budget without presenting it as a Schwab limit.
        let revision_three = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(spec.rate_policy)?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, false)?,
                false,
            )?,
        )?;
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(4)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(3)?,
                SourceIdentifier::try_from("schwab.trader-api-market-data.entitlement-doctor.v1")?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                true,
            )?,
        )?;
        (
            vec![legacy_capability, revision_two, revision_three],
            current,
        )
    } else if spec.id == ALPACA_BASIC_PROFILE {
        // Preserve the exact revision-three provider-fact budget. Revision four separates that
        // 200/min historical fact from Market Squawk's 150/min application ceiling and requires
        // the closed Paper/IEX doctor rather than treating one quote as complete readiness.
        let revision_three = build_capability(
            &spec,
            ProviderCapabilityRevision::new(3)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(spec.rate_policy)?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(2)?,
                SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, false)?,
                true,
            )?,
        )?;
        let current = build_capability(
            &spec,
            ProviderCapabilityRevision::new(4)?,
            prior_credential_kind,
            RatePolicyDescriptor::try_new_enforced(
                SourceIdentifier::try_from(current_rate_policy(&spec))?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                true,
                ProviderCapabilityRevision::new(3)?,
                SourceIdentifier::try_from("alpaca.basic-market-data.paper-iex-doctor.v1")?,
                PROVIDER_RELEASE_REPORT_DIGEST,
                built_in_budget(&spec, true)?,
                true,
            )?,
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
    finish_profile(spec, historical_capabilities, capability)
}

fn build_current_fred(
    spec: BuiltInSpec,
) -> Result<ProviderOnboardingProfile, ProviderProfileError> {
    let revision = ProviderCapabilityRevision::new(1)?;
    let capability = build_capability(
        &spec,
        revision,
        CredentialKind::ApiKey,
        RatePolicyDescriptor::try_new_enforced(
            SourceIdentifier::try_from(spec.rate_policy)?,
            PROVIDER_RELEASE_REPORT_DIGEST,
            true,
            revision,
            SourceIdentifier::try_from(format!("{}.onboarding-probe", spec.id))?,
            PROVIDER_RELEASE_REPORT_DIGEST,
            fred_budget(
                BackoffPolicy::try_new(nonzero_u64(SECOND_NANOS)?, nonzero_u64(MINUTE_NANOS)?, 0)?,
                "fred.onboarding-api-key",
            )?,
            true,
        )?,
    )?;
    finish_profile(spec, Vec::new(), capability)
}

fn finish_profile(
    spec: BuiltInSpec,
    historical_capabilities: Vec<ProviderCapability>,
    capability: ProviderCapability,
) -> Result<ProviderOnboardingProfile, ProviderProfileError> {
    let credentialed = spec.setup == ProfileActivationMode::ManualSecretImport;
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
                || (spec.id == FEDERAL_RESERVE_BOARD_PROFILE && revision.get() >= 4)
                || (spec.id == ALPACA_BASIC_PROFILE && revision.get() >= 4)
                || (spec.id == SCHWAB_MARKET_DATA_PROFILE && revision.get() >= 4)
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
            "bls.v2-registered"
                | "coinbase.exchange-direct-market-data"
                | ALPACA_BASIC_PROFILE
                | KRAKEN_L3_PROFILE
        ) {
            LifecycleSupport::new(true, false, true)
        } else {
            LifecycleSupport::new(false, false, false)
        },
        evidence: capability_evidence(spec, revision)?,
        refresh_trigger: SourceIdentifier::try_from(
            if spec.id == SEC_EDGAR_PROFILE_ID && revision.get() >= 4 {
                SEC_PUBLIC_API_AUTHORITY_SOURCE
            } else if spec.id == BLS_PUBLIC_V1_PROFILE && revision.get() >= 4 {
                BLS_PUBLIC_V1_AUTHORITY_SOURCE
            } else if spec.id == TREASURY_DAILY_RATES_PROFILE && revision.get() >= 4 {
                "TREASURY-XML-AUTHORITY-2026-07-26"
            } else if spec.id == FRED_PROFILE {
                SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE
            } else {
                spec.refresh_trigger
            },
        )?,
    })?)
}

fn initial_credential_kind(profile_id: &str, credentialed: bool) -> CredentialKind {
    if !credentialed {
        CredentialKind::None
    } else if matches!(
        profile_id,
        ALPACA_BASIC_PROFILE | SCHWAB_MARKET_DATA_PROFILE | KRAKEN_L3_PROFILE
    ) {
        CredentialKind::ApiKeyPair
    } else {
        CredentialKind::ApiKey
    }
}

fn capability_evidence(
    spec: &BuiltInSpec,
    revision: ProviderCapabilityRevision,
) -> Result<Vec<EvidenceBinding>, ProviderProfileError> {
    let (report_source, report_digest) = if (revision.get() >= 3
        && has_provider_release_revision(spec.id))
        || spec.id == FRED_PROFILE
    {
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
    if is_selected_architecture_profile(spec.id) && (revision.get() >= 3 || spec.id == FRED_PROFILE)
    {
        evidence.push(EvidenceBinding::new(
            SourceIdentifier::try_from(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE)?,
            SELECTED_MARKET_DATA_ARCHITECTURE_DIGEST,
        ));
    }
    if spec.id == TREASURY_DAILY_RATES_PROFILE && revision.get() >= 4 {
        evidence.push(EvidenceBinding::new(
            SourceIdentifier::try_from(TREASURY_DAILY_RATES_AUTHORITY_SOURCE)?,
            TREASURY_DAILY_RATES_AUTHORITY_DIGEST,
        ));
    }
    if spec.id == SEC_EDGAR_PROFILE_ID && revision.get() >= 4 {
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
    Ok(evidence)
}

fn has_provider_release_revision(profile_id: &str) -> bool {
    matches!(
        profile_id,
        SEC_EDGAR_PROFILE_ID
            | "bls.v1-unregistered"
            | "bls.v2-registered"
            | "treasury.daily-rates-xml"
            | "treasury.fiscal-data"
    ) || is_selected_architecture_profile(profile_id)
}

fn is_selected_architecture_profile(profile_id: &str) -> bool {
    matches!(
        profile_id,
        COINBASE_DIRECT_PROFILE
            | ALPACA_BASIC_PROFILE
            | NASDAQ_REFERENCE_PROFILE
            | SCHWAB_MARKET_DATA_PROFILE
            | YAHOO_ENRICHMENT_PROFILE
            | IEX_HIST_PROFILE
            | OCC_REFERENCE_PROFILE
            | CBOE_REFERENCE_PROFILE
            | BEA_PROFILE
            | CENSUS_PROFILE
            | EIA_PROFILE
            | FRED_PROFILE
            | FEDERAL_RESERVE_BOARD_PROFILE
            | TIINGO_PROFILE
            | "kraken.spot-public-market-data"
            | KRAKEN_L3_PROFILE
    )
}

fn current_rate_policy(spec: &BuiltInSpec) -> &'static str {
    if spec.id == TREASURY_DAILY_RATES_PROFILE {
        "treasury.daily-rates-xml.rate-policy.v2"
    } else if spec.id == TREASURY_FISCAL_PROFILE {
        "treasury.fiscal-data.rate-policy.v2"
    } else if spec.id == ALPACA_BASIC_PROFILE {
        "alpaca.basic-market-data.account-rate-policy.v2"
    } else if spec.id == SCHWAB_MARKET_DATA_PROFILE {
        "schwab.trader-api-market-data.application-account-rate-policy.v1"
    } else if spec.id == FEDERAL_RESERVE_BOARD_PROFILE {
        "federal-reserve-board.data-download-program.rate-policy.v1"
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
        SEC_EDGAR_PROFILE_ID => SEC_EDGAR_AUTHORITY
            .budget_policy()
            .map_err(|_| ProviderProfileError::InvalidProfile),
        "bls.v1-unregistered" => bls_budget(None, 25, backoff),
        "bls.v2-registered" => bls_budget(Some("bls.registered-onboarding"), 500, backoff),
        FRED_PROFILE => fred_budget(backoff, "fred.onboarding-api-key"),
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
        ALPACA_BASIC_PROFILE => simple_budget(
            "alpaca-market-data",
            Some("alpaca.basic.account-template"),
            if current_revision { 150 } else { 200 },
            MINUTE_NANOS,
            1,
            backoff,
        ),
        // Nasdaq Trader publishes no numeric automated-download ceiling for these two files.
        // This matches the existing reference service's conservative shared application budget.
        NASDAQ_REFERENCE_PROFILE => simple_budget(
            "nasdaq-trader-symbol-directory",
            None,
            8,
            MINUTE_NANOS,
            1,
            backoff,
        ),
        SCHWAB_MARKET_DATA_PROFILE => {
            // Revision three retains its one-per-minute local-placeholder budget. The current
            // twenty-per-fifteen-minute ceiling is Market Squawk application policy, not a
            // provider-published Schwab limit. Single flight plus 429/Retry-After feedback keeps
            // the complete bounded doctor inside one shared account scope.
            simple_budget(
                "schwab-trader-api",
                Some("schwab.trader-api.account-template"),
                if current_revision {
                    SCHWAB_MARKET_DATA_DOCTOR_ATTEMPTS
                } else {
                    1
                },
                if current_revision {
                    SCHWAB_MARKET_DATA_DOCTOR_WINDOW_NANOS
                } else {
                    MINUTE_NANOS
                },
                1,
                backoff,
            )
        }
        // These remaining refresh-required profiles use local non-network probes. Their
        // one-per-minute placeholder budgets are not recurring provider capacity and confer no
        // network authority.
        YAHOO_ENRICHMENT_PROFILE => simple_budget(
            "yahoo-finance-experimental",
            None,
            1,
            MINUTE_NANOS,
            1,
            backoff,
        ),
        IEX_HIST_PROFILE => simple_budget("iex-hist", None, 1, MINUTE_NANOS, 1, backoff),
        OCC_REFERENCE_PROFILE => simple_budget("occ-reference", None, 1, MINUTE_NANOS, 1, backoff),
        CBOE_REFERENCE_PROFILE => {
            simple_budget("cboe-reference", None, 1, MINUTE_NANOS, 1, backoff)
        }
        BEA_PROFILE => simple_budget(
            "us-bea",
            Some("bea.api-user-template"),
            60,
            MINUTE_NANOS,
            1,
            backoff,
        ),
        CENSUS_PROFILE => census_budget(backoff),
        EIA_PROFILE => simple_budget(
            "us-eia",
            Some("eia.api-key-template"),
            1,
            SECOND_NANOS,
            1,
            backoff,
        ),
        // No numeric Board ceiling is published. This single-flight one-per-minute bound is a
        // Market Squawk application policy shared by the doctor and later H.15 retrieval.
        FEDERAL_RESERVE_BOARD_PROFILE => {
            simple_budget("federal-reserve-board", None, 1, MINUTE_NANOS, 1, backoff)
        }
        TIINGO_PROFILE => tiingo_budget(backoff),
        "kraken.spot-public-market-data" => {
            simple_budget("kraken", None, 1, MINUTE_NANOS, 1, backoff)
        }
        KRAKEN_L3_PROFILE => simple_budget(
            "kraken",
            Some("kraken.level3.account-template"),
            1,
            SECOND_NANOS,
            1,
            backoff,
        ),
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

fn fred_budget(
    backoff: BackoffPolicy,
    account: &str,
) -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    // One account-scoped lane governs the combined v1/v2 surface.
    let windows = [ProviderBudgetWindow::try_new(
        NonZeroU32::new(1).ok_or(ProviderProfileError::InvalidProfile)?,
        nonzero_u64(SECOND_NANOS)?,
        BudgetWindowSemantics::Sliding,
    )?];
    Ok(ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(
            SourceIdentifier::try_from("fred")?,
            SourceIdentifier::try_from(account)?,
        ),
        &windows,
        NonZeroU16::new(1).ok_or(ProviderProfileError::InvalidProfile)?,
        backoff,
    )?)
}

fn treasury_budget() -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    let windows = [ProviderBudgetWindow::try_new(
        NonZeroU32::new(1).ok_or(ProviderProfileError::InvalidProfile)?,
        nonzero_u64(SECOND_NANOS)?,
        BudgetWindowSemantics::Sliding,
    )?];
    Ok(ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::new(SourceIdentifier::try_from("us-treasury")?),
        &windows,
        NonZeroU16::new(1).ok_or(ProviderProfileError::InvalidProfile)?,
        BackoffPolicy::try_new(nonzero_u64(SECOND_NANOS)?, nonzero_u64(MINUTE_NANOS)?, 0)?,
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

fn census_budget(backoff: BackoffPolicy) -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(1).ok_or(ProviderProfileError::InvalidProfile)?,
            nonzero_u64(SECOND_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(400).ok_or(ProviderProfileError::InvalidProfile)?,
            nonzero_u64(DAY_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )?,
    ];
    Ok(ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(
            SourceIdentifier::try_from("us-census")?,
            SourceIdentifier::try_from("census.api-key-template")?,
        ),
        &windows,
        NonZeroU16::new(1).ok_or(ProviderProfileError::InvalidProfile)?,
        backoff,
    )?)
}

fn tiingo_budget(backoff: BackoffPolicy) -> Result<ProviderBudgetPolicy, ProviderProfileError> {
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(40).ok_or(ProviderProfileError::InvalidProfile)?,
            nonzero_u64(HOUR_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(800).ok_or(ProviderProfileError::InvalidProfile)?,
            nonzero_u64(DAY_NANOS)?,
            BudgetWindowSemantics::Sliding,
        )?,
    ];
    Ok(ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(
            SourceIdentifier::try_from("tiingo")?,
            SourceIdentifier::try_from("tiingo.starter-account-template")?,
        ),
        &windows,
        NonZeroU16::new(1).ok_or(ProviderProfileError::InvalidProfile)?,
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
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: PRIVATE_CRYPTO_RESEARCH_DUTIES,
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "create a replacement View-only Exchange key and import the complete envelope as a higher generation",
        revocation: "delete the exact Exchange key remotely, then delete the exact local generation",
        recovery: COMMON_RECOVERY,
        evidence: COINBASE_DIRECT_EVIDENCE,
        rate_policy: "coinbase.exchange-direct-market-data.rate-policy.v1",
        refresh_trigger: "CB-EXCHANGE-DIRECT",
        handoff_instruction: "Create a Coinbase Exchange API key with View permission only, then import one version-1 envelope containing api_key, passphrase, and signing_secret.",
    })
}

fn alpaca_basic() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: ALPACA_BASIC_PROFILE,
        display_name: "Alpaca Paper Only / Basic market data",
        official_entry: "https://app.alpaca.markets/signup",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("alpaca.market-data.read"),
        permissions: &["market-data.read"],
        coverage: "One Alpaca Paper Only key generation in the paper realm: Basic real-time US equities and ETFs from IEX only with an official 30-symbol WebSocket ceiling at DirectUnverified quality; code-owned quote, 50-symbol snapshot sentinel, WebSocket control acknowledgement, raw daily history, and exact IEX/UTC calendar reconciliation require runtime doctor proof; indicative options, fixed income, and corporate actions remain unprobed by this capability; IEX stock history retains the Basic latest-15-minute restriction and provider-published 200 historical-calls/minute fact; top-of-book only and never consolidated SIP, NBBO, OPRA, Level II/III, account, position, order, trading, or execution coverage",
        quality: DataQuality::DirectUnverified,
        probe: VerificationProbe::network_exact_public_query(
            ProbeTransport::HttpGet,
            "https://data.alpaca.markets/v2/stocks/AAPL/quotes/latest",
            "https://data.alpaca.markets/v2/stocks/AAPL/quotes/latest?feed=iex",
            &[("feed", "iex")],
        )?,
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "retain the exact IEX-only or indicative-options provider and feed label on every raw, canonical, and derived observation",
            "admit persistence and model use only inside owner-local personal research datasets",
            "retain the provider-published Basic facts of 200 historical calls per minute, 30 equity stream symbols, and 200 option quote subscriptions without treating response headers as permission to expand",
            "enforce one shared Paper account application ceiling of 150 REST requests per minute, target at most 120 recurring requests per minute, and lower admission from runtime headers, 429, Retry-After, partial returns, latency, and pressure",
            "treat the fixed AAPL latest-quote probe as legacy portal bootstrap evidence only; current revision-four runtime verification requires the closed Paper/IEX quote, 50-symbol snapshot, WebSocket acknowledgement, raw-history, and calendar receipt",
            "use this credential only with code-owned data.alpaca.markets routes, the IEX market-data WebSocket, and GET https://paper-api.alpaca.markets/v3/calendar/IEX with bounded start, end, and timezone=UTC query coordinates; every other paper-api origin route and all account, position, order, trading, or execution endpoints remain forbidden",
            "keep source-data export and redistribution closed",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "create a replacement Paper Trading API key pair and import one complete version-1 envelope as a higher generation",
        revocation: "delete the exact Alpaca Paper key remotely, then delete the exact local generation",
        recovery: COMMON_RECOVERY,
        evidence: ALPACA_BASIC_EVIDENCE,
        rate_policy: "alpaca.basic-market-data.account-rate-policy.v1",
        refresh_trigger: "ALPACA-BASIC-MARKET-DATA",
        handoff_instruction: "Create a free email-only Alpaca Paper Only account, generate its Paper key pair, then import one version-1 envelope containing key_id, secret_key, and trading_api_environment set exactly to paper. Market Squawk grants only read-only Basic market-data authority; live-realm credentials and every account, position, order, and trading surface are rejected.",
    })
}

fn nasdaq_reference() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: NASDAQ_REFERENCE_PROFILE,
        display_name: "Nasdaq Trader current symbol-directory reference",
        official_entry: "https://www.nasdaqtrader.com/",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NoCredentialFeeNotEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RightsLimited,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Current Nasdaq-listed and other-exchange-listed US equity and ETF reference identity from exactly nasdaqlisted.txt and otherlisted.txt: symbols, security names, listing venues, market categories, financial/test/ETF flags, round lots, and provider file timestamps; process-local reference only, never a quote, trade, book, current price, trading-status, historical-lifecycle, or execution source",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
            None,
        )?,
        rights: RIGHTS_LIMITED,
        duties: &[
            "bind this profile to the existing NasdaqReferenceUniverseService and its exact two code-owned directory endpoints",
            "retain provider file timestamps and exact Nasdaq versus other-exchange listing provenance",
            "keep normalized directory rows process-local and reacquire them after restart",
            "never promote reference identity to quote, trade, market-status, book-depth, or execution evidence",
        ],
        persistence_evidence_source_id: None,
        rotation: "not applicable: this surface has no credential",
        revocation: "disable the reference profile locally; there is no provider credential to revoke",
        recovery: COMMON_RECOVERY,
        evidence: NASDAQ_REFERENCE_EVIDENCE,
        rate_policy: "nasdaq-trader-symbol-directory-reference.rate-policy.v1",
        refresh_trigger: "NASDAQ-SYMBOL-DIRECTORY",
        handoff_instruction: "No account or key is required; continue with the bounded code-owned Nasdaq-listed directory probe. Activation reuses the existing NasdaqReferenceUniverseService, which fetches and validates both exact directory files.",
    })
}

fn schwab_market_data() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: SCHWAB_MARKET_DATA_PROFILE,
        display_name: "Charles Schwab Trader API read-only market data",
        official_entry: "https://developer.schwab.com/products/trader-api--individual",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::NotSeparatelyEstablished,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("schwab.market-data.read"),
        permissions: &["market-data.read", "streamer-bootstrap.read"],
        coverage: "Optional owner-enabled target for Schwab Trader API market-data REST quotes, price history, option and expiration chains, movers, market hours, instruments/reference data, and one Streamer connection carrying selected level-one, named-book, chart, and screener services; source semantics remain provider/service-specific and never imply SIP, NBBO, OPRA, consolidated depth, account access, or execution; the provider-native read-only REST/Streamer core, protected OAuth lifecycle, exact User Preference bootstrap, and bounded twenty-attempt entitlement doctor authority are present, while complete activation-to-publication binding, PIT typed reads, product composition, and restart/release proof remain incomplete",
        quality: DataQuality::DirectUnverified,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            SCHWAB_USER_PREFERENCE_PROBE_URL,
            None,
        )?,
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "import only the application key and secret pair; authorization codes, access tokens, and refresh tokens may enter only the application-owned protected OAuth/token authority",
            "use the exact code-owned https://127.0.0.1:8182 callback and never accept an operator-supplied provider endpoint",
            "allowlist only market-data routes plus trader/v1/userPreference fields required for Streamer bootstrap; never admit account, position, order, or trading authority",
            "retain Schwab endpoint or Streamer service, provider symbol, named venue/book, account realm, event and receive clocks, sequence, reconnect, and delay or indicative fields",
            "admit at most one Streamer connection and one provider attempt in flight across the application/account scope",
            "enforce the Market Squawk ceiling of twenty provider attempts per fifteen minutes for the exact User Preference plus seven REST plus twelve Streamer doctor probes; this is not a provider-published Schwab limit",
            "refresh shared capacity on HTTP 429, honor valid server Retry-After feedback, and lower admission from refusals, partial returns, latency, bytes, acknowledgements, and queue pressure",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "rotate the provider application secret or seven-day refresh-token generation atomically in application-owned protected provider state",
        revocation: "revoke the Schwab application or token remotely, then delete the exact local secret and token generations",
        recovery: REFRESH_RECOVERY,
        evidence: SCHWAB_MARKET_DATA_EVIDENCE,
        rate_policy: "schwab.trader-api-market-data.pending-rate-policy.v1",
        refresh_trigger: "SCHWAB-TRADER-API-MARKET-DATA",
        handoff_instruction: "Import the configured Schwab application key and secret pair, complete provider-controlled OAuth authorization, then run the code-owned User Preference bootstrap and bounded entitlement doctor. The profile remains unavailable until complete activation-to-publication binding, PIT reads, product composition, and restart/release proof are delivered.",
    })
}

fn yahoo_enrichment() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: YAHOO_ENRICHMENT_PROFILE,
        display_name: "Yahoo Finance experimental explicit-demand enrichment",
        official_entry: "https://ranaroussi.github.io/yfinance/reference/index.html",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NoCredentialFeeNotEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Experimental no-key target for explicit-demand quote components, index and fund facts, price history, corporate actions, option expirations/chains, and search hints through one pinned and hashed yfinance-lineage request, cookie/crumb HTTP, adaptive admission, coalescing, schema pinning, raw sealing, canonical publication, and exact PIT restart authority; never a scheduled broad-market lane, authoritative tick source, sole decision input, or SIP/NBBO/OPRA substitute",
        quality: DataQuality::Aggregated,
        probe: VerificationProbe::local(
            "Yahoo's selected no-key explicit-demand application composition is installed: governed onboarding activation, one durable adaptive HTTP lane, exact provider Retry-After handling, raw sealing, family-typed canonical publication, and exact PIT restart verification are code owned",
        ),
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "permit only a user-visible explicit-demand operation; never schedule recurring broad-market collection",
            "pin and hash the exact client release and retain effective arguments, upstream attempts, cache, repair, fallback, and response provenance",
            "serialize one provider lane, coalesce identical work, prefer a bounded fresh cache, and begin with zero automatic transient retries",
            "treat every returned field independently and never upgrade it to SIP, NBBO, OPRA, consolidated, or authoritative semantics",
            "no numeric provider rate, daily quota, watchlist maximum, batch ceiling, or streaming ceiling is admitted until dated runtime evidence is frozen",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "not applicable: this selected surface has no credential",
        revocation: "disable the experimental source locally and remove its bounded cache and pending jobs",
        recovery: REFRESH_RECOVERY,
        evidence: YAHOO_ENRICHMENT_EVIDENCE,
        rate_policy: "yahoo-finance.experimental-enrichment.pending-rate-policy.v1",
        refresh_trigger: "YAHOO-FINANCE-EXPERIMENTAL",
        handoff_instruction: "No account or key is required. Continue with the installed explicit-demand Yahoo enrichment activation; every request remains bounded, supplemental, adaptively admitted, and unavailable while exact currentness or provider cooldown authority blocks it.",
    })
}

fn iex_hist() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: IEX_HIST_PROFILE,
        display_name: "IEX HIST selected feed files",
        official_entry: "https://iextrading.com/trading/market-data/index.html",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NoCredentialFeeNotEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Explicit feed-and-date cold-research target for T+1 IEX HIST TOPS top-of-book/last-sale/status/auction messages, DEEP displayed price-level depth, and DEEP+ displayed order-level depth within the provider-described recent 12-month window; always IEX venue-specific, never live, consolidated, or a complete market-wide book; provider-native catalog selection, bounded cold-job download/materialization, versioned PCAP decode, planning, and receipt core are present, while application doctor/activation, canonical publication, PIT reads, product composition, and restart/release proof remain absent",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::local(
            "IEX HIST catalog, bounded cold transport/materialization, versioned decoder, planning, and receipt core are installed, but application activation, canonical publication, PIT read, and product proof are not; activation remains refresh_required",
        ),
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "admit only an explicit feed and trade date after catalog descriptor, compressed bytes, expanded-byte ceiling, disk reserve, and exact feed-spec version are frozen",
            "retain venue IEX, feed and transport versions, file date, message type, source clock, sequence, local availability, decoder version, and raw digest",
            "quarantine gaps, corrupt packets, unsupported versions, duplicates, resets, out-of-order messages, and clock anomalies",
            "never automatically mirror the archive or infer stable retention, numeric request capacity, checksums, or replay guarantees",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "not applicable: this selected surface has no credential",
        revocation: "disable new IEX HIST jobs locally; retained immutable research evidence remains governed by its manifests",
        recovery: REFRESH_RECOVERY,
        evidence: IEX_HIST_EVIDENCE,
        rate_policy: "iex.hist-feed-files.pending-rate-policy.v1",
        refresh_trigger: "IEX-HIST-FEED-FILES",
        handoff_instruction: "No key is required. The bounded catalog, cold transport, materialization, and versioned decode core is present; the profile remains unavailable until application doctor/activation, canonical publication, PIT selection, product composition, and restart/release proof are implemented.",
    })
}

fn occ_reference() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: OCC_REFERENCE_PROFILE,
        display_name: "OCC listed-options and contract-event reference",
        official_entry: "https://www.theocc.com/market-data/market-data-reports/series-and-trading-data/directory-of-listed-products",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NoCredentialFeeNotEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RefreshRequired,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "No-key target for OCC Directory of Listed Products option root/product/series discovery and complete Information Memo plus attachment evidence for adjustments, symbol or expiration changes, settlement, and deliverables; reference and operative-event evidence only, never live quotes, trades, Greeks, depth, current tradability, or execution; exact selected/daily DLP and memo request plans, bounded injected-HTTP transport contracts, decoders, complete-document requirements, and publication-catalog conflict handling are present, while an application executor/doctor, activation, durable canonical publication, PIT typed read, Options composition, and restart/release proof remain absent",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::local(
            "OCC request plans, bounded transport contracts, DLP/memo decoders, and publication-catalog core are installed, but the application executor/doctor, activation, durable canonical publisher, PIT read, and Options proof are not; activation remains refresh_required",
        ),
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "freeze the exact supported machine batch request, record layout, control records, completion signal, effective-time rules, and byte and row bounds before acquisition",
            "retain each directory response, record layout, memo, and attachment as separate exact evidence with posting, effective, first-observed, and publication clocks",
            "never interpret a memo title alone or treat a format-valid OCC/OSI symbol as proof of listing or activity",
            "do not invent a numeric provider limit; one shared bounded reference queue may only be admitted after the doctor proves a current route",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "not applicable: this selected surface has no credential",
        revocation: "disable future OCC reference jobs locally",
        recovery: REFRESH_RECOVERY,
        evidence: OCC_REFERENCE_EVIDENCE,
        rate_policy: "occ.options-reference.pending-rate-policy.v1",
        refresh_trigger: "OCC-OPTIONS-REFERENCE",
        handoff_instruction: "No key is required. The selected OCC request, transport-contract, decoder, and publication-catalog core is present; the profile remains unavailable until an application executor/doctor, activation, content-addressed canonical publication, PIT read, Options composition, and restart/release proof are implemented.",
    })
}

fn cboe_reference() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: CBOE_REFERENCE_PROFILE,
        display_name: "Cboe venue-specific options reference",
        official_entry: "https://www.cboe.com/markets/us/options/market-statistics/reference-data",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NoCredentialFeeNotEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RefreshRequired,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "No-key initial target for the four separate C1, BZX Options, C2 Options, and EDGX Options All Series files plus frozen Cboe Symbol ID/OSI symbology mappings; venue-specific option identity only, never a consolidated chain, OPRA quote, trade, Greek, depth, current tradability, or execution source; exact four-file request plans, schema freezes, redirect/byte-bounded injected-HTTP transport contracts, decoders, and publication-catalog conflict handling are present, while an application executor/doctor, activation, durable canonical publication, PIT typed read, Options composition, and restart/release proof remain absent",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::local(
            "Cboe four-file request plans, schema freezes, bounded transport contracts, decoders, and publication-catalog core are installed, but the application executor/doctor, activation, durable canonical publisher, PIT read, and Options proof are not; activation remains refresh_required",
        ),
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "treat each venue and reference family as a distinct exact object and preserve redirect, bytes, digest, schema, row count, and local availability",
            "retain Cboe Symbol ID, OSI and other source-native aliases without silently collapsing cross-venue evidence",
            "initial implementation may admit only the four All Series files; every other family needs its own frozen schema and bounds",
            "do not invent a numeric provider limit; fetch each admitted publication once through one shared bounded reference queue",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "not applicable: this selected surface has no credential",
        revocation: "disable future Cboe reference jobs locally",
        recovery: REFRESH_RECOVERY,
        evidence: CBOE_REFERENCE_EVIDENCE,
        rate_policy: "cboe.options-reference.pending-rate-policy.v1",
        refresh_trigger: "CBOE-OPTIONS-REFERENCE",
        handoff_instruction: "No key is required. The four All Series request, schema, bounded transport-contract, decoder, and publication-catalog core is present; the profile remains unavailable until an application executor/doctor, activation, durable canonical publication, PIT read, Options composition, and restart/release proof are implemented.",
    })
}

fn bea() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: BEA_PROFILE,
        display_name: "Bureau of Economic Analysis API",
        official_entry: "https://apps.bea.gov/api/signup/",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::NotSeparatelyEstablished,
        account: Requirement::RequiredProviderControlled,
        // Organization/email are collected by BEA when the UserID is issued; the runtime API
        // authenticates with the UserID alone and the credential bundle intentionally stores no
        // duplicate registration-form metadata.
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("bea.data.read"),
        permissions: &["data.read"],
        coverage: "Credentialed metadata-driven BEA national, regional, industry, personal-income, and international accounts through GetDatasetList, parameter discovery, and exact GetData dataset coordinates; protected UserID authentication, bounded HTTPS, metadata and data parsing, correction evidence, exact dataset contracts, and raw-capture lineage are available under shared 60-request, 60-MB, and 10-error per-minute application budgets",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::local(
            "BEA protected credential handling, metadata discovery, exact dataset request authority, bounded HTTPS, parsing, correction evidence, and raw-capture lineage are available for activation",
        ),
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "import the 36-character UserID as one protected API-key value and redact it from every URL, log, trace, error, receipt, and diagnostic",
            "discover datasets, parameters, and values through BEA metadata and address response dimensions by name rather than assuming one common table schema",
            "enforce one shared 60-request, 60-MB, and 10-error per-minute application ledger and honor HTTP 429 Retry-After",
            "retain exact dataset dimensions, units, multipliers, notes, correction evidence, response identity, and point-in-time availability",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "create a replacement BEA UserID and import it as a higher protected generation",
        revocation: "remove the exact local credential generation and disable the provider; use provider support for any remote account action",
        recovery: COMMON_RECOVERY,
        evidence: BEA_EVIDENCE,
        rate_policy: "bea.api-data.rate-policy.v1",
        refresh_trigger: "BEA-API-DATA",
        handoff_instruction: "Import the configured 36-character BEA UserID through the protected secret boundary, choose the exact dataset and dimensions, and verify availability before activation.",
    })
}

fn census() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: CENSUS_PROFILE,
        display_name: "U.S. Census Data API",
        official_entry: "https://api.census.gov/data/key_signup.html",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::NotSeparatelyEstablished,
        account: Requirement::RequiredProviderControlled,
        // Census collects contact details during key issuance. The Data API request contract uses
        // only the issued key, so provider onboarding does not require those details again.
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RefreshRequired,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("census.data.read"),
        permissions: &["data.read"],
        coverage: "Credentialed cold-research target for exact Census dataset-vintage-variable-geography coordinates spanning demographic, household, business, trade, and geographic statistical evidence; current provider request, daily, variable, row, and pagination maxima remain unverified, with selected application safety limits of one request per second and 400 requests per day; provider-native query grammar, discovery/response contracts, bounded HTTPS, and raw-capture source core are present, while an application redacted doctor, activation, durable canonical publication, PIT typed read, product composition, and restart/release proof remain absent",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::local(
            "Census query, discovery, response, bounded HTTPS, dataset-contract, and raw-capture core is installed, but the application redacted doctor, activation, durable canonical publisher, PIT read, and product proof are not; activation remains refresh_required",
        ),
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "import the API key as one protected value and redact the complete secret-bearing query from every URL, log, trace, error, receipt, and diagnostic",
            "freeze current discovery and query grammar for each admitted dataset; never treat one popular-family list as a permanent catalog",
            "enforce one shared one-request-per-second and 400-request-per-day application ledger without presenting either value as a provider limit",
            "retain dataset and vintage, variables, group metadata, geography/FIPS, annotations, response header, raw digest, revisions, and point-in-time availability",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "request and import a replacement Census API key as a higher protected generation",
        revocation: "remove the exact local credential generation and disable the provider; use provider support for any remote key action",
        recovery: REFRESH_RECOVERY,
        evidence: CENSUS_EVIDENCE,
        rate_policy: "census.data-api.pending-rate-policy.v1",
        refresh_trigger: "CENSUS-DATA-API",
        handoff_instruction: "Import the configured Census API key. The provider-native query, discovery, response, transport, and capture core is present; the profile remains unavailable until an application redacted doctor, activation, durable canonical macro/reference publication, PIT read, product composition, and restart/release proof are implemented.",
    })
}

fn eia() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: EIA_PROFILE,
        display_name: "U.S. Energy Information Administration API v2",
        official_entry: "https://www.eia.gov/opendata/register.php",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::NotSeparatelyEstablished,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::RefreshRequired,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("eia.data.read"),
        permissions: &["data.read"],
        coverage: "Credentialed metadata-driven API v2 target for petroleum, natural-gas, electricity, inventory, production, consumption, and price observations; JSON pages have an official 5,000-row maximum and XML pages 300, while no numeric provider request rate is established and the application ceiling is one shared request per second; provider-native route metadata, request construction, bounded pagination, exact transport/capture, revision planning, and canonical-mapping core are present, while an application redacted doctor, activation, durable checkpoints/publication, PIT typed read, product composition, and restart/release proof remain absent",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::local(
            "EIA route-metadata, request, bounded pagination, transport/capture, revision, and canonical-mapping core is installed, but the application redacted doctor, activation, durable checkpoints/publisher, PIT read, and product proof are not; activation remains refresh_required",
        ),
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "import the API key as one protected value and redact the complete secret-bearing query from every URL, log, trace, error, receipt, and diagnostic",
            "discover and freeze each route through metadata and retain route, dimensions, units, frequency, explicit sort, bounds, API version, and secret-free request echo",
            "enforce one shared application ceiling of one request per second and may only lower it without new reviewed evidence",
            "page JSON at no more than 5,000 rows and require bounded offset coverage through response total before publication",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "request and import a replacement EIA API key as a higher protected generation",
        revocation: "remove the exact local credential generation and disable the provider; use provider support for any remote key action",
        recovery: REFRESH_RECOVERY,
        evidence: EIA_EVIDENCE,
        rate_policy: "eia.api-v2.pending-rate-policy.v1",
        refresh_trigger: "EIA-API-V2",
        handoff_instruction: "Import the configured EIA API key. The provider-native metadata, request, transport/capture, pagination, revision, and mapping core is present; the profile remains unavailable until an application redacted doctor, activation, durable checkpoints/canonical publication, PIT read, product composition, and restart/release proof are implemented.",
    })
}

fn federal_reserve_board() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: FEDERAL_RESERVE_BOARD_PROFILE,
        display_name: "Federal Reserve Board Data Download Program",
        official_entry: "https://www.federalreserve.gov/datadownload/",
        setup: ProfileActivationMode::NoCredential,
        zero_fee: ZeroFeeStatus::NoCredentialFeeNotEstablished,
        account: Requirement::NotRequired,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "No-key Federal Reserve Board Data Download Program current-definition statistical-release surface, admitted initially only for the exact 11-series H.15 Treasury constant-maturity family; the unchanged bounded doctor requests exactly ten recent dates per series, while the active production contract is the distinct live-verified rolling 100-date dashboard response with exactly 1,100 observations; the full-history Download contract remains a distinct research identity but is unavailable through the indivisible one-batch source/publication path until partitioned resumable extraction exists; DDP is not a vintage-history authority, has no established numeric request ceiling, and is limited by application policy to one shared request per minute with one request in flight; provider-native authority-governed HTTPS, strict parsing, canonical mapping, application activation/source construction, rich raw-capture handoff, analytical-dataset registration, and lifecycle restore code are present; scripted and exact-head real-network installed journeys prove rolling capture, durable publication, typed dashboard reads, stable same-root restart evidence, and unchanged post-restart raw/publication objects, while native Desktop/package and release acceptance remain incomplete",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network_exact_public_query(
            ProbeTransport::HttpGet,
            FEDERAL_RESERVE_BOARD_H15_PROBE_BASE_URL,
            FEDERAL_RESERVE_BOARD_H15_PROBE_URL,
            &[
                ("filetype", "csv"),
                ("label", "include"),
                ("lastobs", "10"),
                ("layout", "seriescolumn"),
                ("rel", "H15"),
                ("series", "bf17364827e38702b42a58cf8eaa3f78"),
                ("type", "package"),
            ],
        )?,
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "freeze release, series, frequency, bounds, format, exact generated automation URL, and matching structure/schema digests for every admitted package",
            "activate the exact rolling 100-date H.15 dashboard contract only; reject the full-history package from the one-batch source until a bounded partitioned resumable extraction contract exists",
            "enforce one shared release-driven application queue at no more than one request per minute without presenting it as a provider limit",
            "retain scheduled release, publication, route availability, correction or repost, local receipt, schema, and local revision as separate evidence",
            "never claim DDP supplies pre-revision or complete real-time vintage history",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "not applicable: this selected surface has no credential",
        revocation: "disable future Board DDP jobs locally",
        recovery: COMMON_RECOVERY,
        evidence: FEDERAL_RESERVE_BOARD_EVIDENCE,
        rate_policy: "federal-reserve-board.data-download-program.pending-rate-policy.v1",
        refresh_trigger: "FEDERAL-RESERVE-BOARD-DDP",
        handoff_instruction: "No key is required. Continue with the unchanged exact ten-date H.15 doctor. After a successful probe, activate only the code-owned rolling 100-date dashboard profile; the distinct full-history package remains unavailable until partitioned resumable extraction is implemented. Scripted and exact-head real-network installed rolling publication, typed dashboard read, and same-root restart proofs pass; native Desktop/package and release acceptance remain required.",
    })
}

fn tiingo() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: TIINGO_PROFILE,
        display_name: "Tiingo Starter EOD and mutual-fund NAV",
        official_entry: "https://www.tiingo.com/documentation/general",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("tiingo.eod.read"),
        permissions: &["daily.read"],
        coverage: "Optional credentialed target for supported mutual-fund daily NAV plus curated raw and adjusted equity/ETF EOD, dividends, and split factors; official Starter limits are 50 requests/hour, 1,000/day, 500 unique symbols/month, and 1 GB/month, with application budgets of 40/hour, 800/day, 400 symbols/month, and 800 MB/month; the installed application owns redacted secret-store activation, one shared provider-rate authority, persistent four-dimensional quota/history state, bounded provider-native HTTP/capture, strict decoding, distinct FundNav and EOD canonical publication, and exact PIT restart verification",
        quality: DataQuality::Aggregated,
        probe: VerificationProbe::local(
            "Tiingo's selected token-backed NAV/EOD application composition is installed: redacted secret-store activation, durable shared quota/history authority, bounded metadata/latest capture, distinct canonical families, exact-generation publication admission, and PIT restart verification are code owned",
        ),
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "import the token as one protected value and redact it from URLs, headers, logs, traces, errors, receipts, and diagnostics",
            "require per-ticker metadata and non-null coverage dates before treating a symbol as supported",
            "enforce persistent conjunctive budgets of 40 requests/hour, 800/day, 400 unique symbols/month, and 800 MB/month through the shared request authority and the durable symbol/byte ledger",
            "retain raw and adjusted EOD, dividend cash, split factor, and mutual-fund NAV as separate source-authored evidence with provisional/correction clocks",
            "never fabricate intraday mutual-fund prices or interpret four equal NAV fields as intraday OHLC trades",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "create or retrieve a replacement Tiingo token and import it as a higher protected generation",
        revocation: "revoke or replace the provider token, then delete the exact local generation",
        recovery: REFRESH_RECOVERY,
        evidence: TIINGO_EVIDENCE,
        rate_policy: "tiingo.starter-eod-nav.pending-rate-policy.v1",
        refresh_trigger: "TIINGO-STARTER-EOD-NAV",
        handoff_instruction: "Import the configured Tiingo API token as one protected generation. The installed application activates only bounded daily FundNav or EOD operations and reports unavailable while shared request capacity, provider Retry-After, monthly symbol/byte quota, schema, or exact-generation authority blocks dispatch.",
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
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: None,
        permissions: &[],
        coverage: "Kraken Spot WebSocket v2 venue-qualified public instruments, price-level books, and trades for the exact configured pairs; independent book and trade channels, DirectUnverified, crypto-only, and never consolidated market, account, order, or execution coverage",
        quality: DataQuality::DirectUnverified,
        probe: VerificationProbe::network(
            ProbeTransport::HttpGet,
            "https://api.kraken.com/0/public/SystemStatus",
            None,
        )?,
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: PRIVATE_CRYPTO_RESEARCH_DUTIES,
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "not applicable: this surface has no credential",
        revocation: "not applicable: this surface has no credential",
        recovery: COMMON_RECOVERY,
        evidence: KRAKEN_EVIDENCE,
        rate_policy: "kraken.spot-public-market-data.rate-policy.v1",
        refresh_trigger: "KR-PUBLIC",
        handoff_instruction: "No account or key is requested; continue with the bounded public probe.",
    })
}

fn kraken_l3() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: KRAKEN_L3_PROFILE,
        display_name: "Kraken Spot authenticated order-level market data",
        official_entry: "https://support.kraken.com/articles/360000919966-how-to-create-an-api-key",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("kraken.websocket-token.create"),
        permissions: &["create-ws-token"],
        coverage: "One user-authorized Kraken Spot account on WebSocket v2 level3: individual visible orders for up to 200 configured crypto pairs per connection at OrderLevel depth and DirectUnverified quality; CRC32 checksum over the top ten price levels; no provider sequence and no execution authority",
        quality: DataQuality::DirectUnverified,
        probe: VerificationProbe::network(
            ProbeTransport::HttpPostJson,
            "https://api.kraken.com/0/private/GetApiKeyInfo",
            Some(r#"{"nonce":"runtime-generated-monotonic"}"#),
        )?,
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: &[
            "admit only a key whose exact permission set is create-ws-token",
            "retain OrderLevel depth separately from DirectUnverified data quality",
            "treat the provider sequence as unsupported and validate every supplied checksum",
            "enforce the documented depth-weighted subscription counter through the account runtime",
            "use the credential only for API-key inspection and WebSocket-token creation and never for trading",
            "preserve exact provider, venue, pair, channel, credential generation, connection generation, and raw-payload provenance",
            "restrict persisted source data and every transformed or model-derived artifact to owner-local personal research use",
            "prohibit sale, export, redistribution, third-party serving, and commercial exploitation of source data or derived datasets",
        ],
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "create a replacement Kraken key with only WebSocket interface permission and import one complete version-1 envelope as a higher generation",
        revocation: "delete the exact Kraken key remotely, then delete the exact local generation",
        recovery: COMMON_RECOVERY,
        evidence: KRAKEN_L3_EVIDENCE,
        rate_policy: "kraken.spot-authenticated-level3.account-rate-policy.v1",
        refresh_trigger: "KRAKEN-AUTHENTICATED-L3",
        handoff_instruction: "Create a Kraken Spot API key with WebSocket interface permission only and no funds, trading, ledger, export, or withdrawal permissions. Import one version-1 envelope containing api_key and api_secret.",
    })
}

fn sec() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: SEC_EDGAR_PROFILE_ID,
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
        coverage: "Public SEC-owned EDGAR submissions, filing documents, XBRL, bulk submissions and company facts, and quarterly N-PORT/N-CEN archives",
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
        rate_policy: SEC_EDGAR_AUTHORITY.rate_policy_id(),
        refresh_trigger: "SEC",
        handoff_instruction: "Provide a truthful non-secret organization and monitored administrative email, then continue with the bounded public probe.",
    })
}

fn fred() -> Result<BuiltInSpec, ProviderProfileError> {
    Ok(BuiltInSpec {
        id: FRED_PROFILE,
        display_name: "FRED and ALFRED macro data",
        official_entry: "https://fred.stlouisfed.org/docs/api/api_key.html",
        setup: ProfileActivationMode::ManualSecretImport,
        zero_fee: ZeroFeeStatus::Confirmed,
        account: Requirement::RequiredProviderControlled,
        contact: Requirement::NotRequired,
        release: ProfileReleaseState::Available,
        rights_state: RightsAdmissionState::AdmittedScoped,
        authority: Some("fred.series.read"),
        permissions: &["series.read"],
        coverage: "Bounded FRED and ALFRED series observations and vintages for exact configured datasets and owner-local personal research",
        quality: DataQuality::OfficialDelayed,
        probe: VerificationProbe::network_secret_query(
            ProbeTransport::HttpGet,
            "https://api.stlouisfed.org/fred/series",
            &[("series_id", "UNRATE"), ("file_type", "json")],
            "api_key",
            32,
        )?,
        rights: RIGHTS_LOCAL_PERSONAL_RESEARCH,
        duties: FRED_PERSONAL_RESEARCH_DUTIES,
        persistence_evidence_source_id: Some(SELECTED_MARKET_DATA_ARCHITECTURE_SOURCE),
        rotation: "create a replacement provider key and import it as a higher generation",
        revocation: "delete the exact provider key remotely, then delete the exact local generation",
        recovery: FRED_RECOVERY,
        evidence: FRED_EVIDENCE,
        rate_policy: "fred-alfred.api-v1-v2.rate-policy.v1",
        refresh_trigger: "FRED",
        handoff_instruction: "Create a zero-fee API key, import it through the protected secret boundary, and select the exact dataset, series, and vintage interval to acquire.",
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
        // BLS collects organization/email in its registration engine, but API v2 calls require
        // only the issued registration key. Do not expand the exact credential-bundle schema with
        // registration-form metadata that the runtime does not consume.
        contact: Requirement::NotRequired,
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
