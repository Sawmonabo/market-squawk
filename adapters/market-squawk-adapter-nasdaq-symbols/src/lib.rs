//! Official Nasdaq Trader equity, ETF, bond, and option reference-directory ingestion.
//!
//! This crate preserves current listing-reference fields and exact source-file lineage. It does
//! not provide quotes, trades, market depth, trading status, or execution-quality evidence.

mod archive;
mod client;
mod model;
mod parser;
mod source;

pub use archive::{
    BONDS_LIST_URL, MAX_BONDS_RECORDS, MAX_BONDS_SOURCE_BYTES, MAX_OPTIONS_RECORDS,
    MAX_OPTIONS_SOURCE_BYTES, MAX_REFERENCE_INDEX_BYTES, MAX_REFERENCE_PAGE_RECORDS,
    NasdaqBondReferenceRecord, NasdaqHttpResponseEvidence, NasdaqIdentityDisposition,
    NasdaqOptionClosingType, NasdaqOptionReferenceRecord, NasdaqProviderDecimal,
    NasdaqRawObjectStore, NasdaqReferenceCompleteness, NasdaqReferenceCurrentnessDisposition,
    NasdaqReferenceDoctorReport, NasdaqReferenceError, NasdaqReferenceGenerationEvidence,
    NasdaqReferenceIdentityCandidate, NasdaqReferenceLifecycleDisposition, NasdaqReferencePage,
    NasdaqReferencePageCursor, NasdaqReferencePageRequest, NasdaqReferenceProvenance,
    NasdaqReferenceQuery, NasdaqReferenceQueryDisposition, NasdaqReferenceQueryResult,
    NasdaqReferenceRecord, NasdaqReferenceTradabilityDisposition,
    NasdaqReferenceValidityDisposition, NasdaqSealedRawObject, NasdaqValidatedObject, OPTIONS_URL,
};
pub use model::{
    NasdaqDirectoryKind, NasdaqDirectoryPresence, NasdaqFileCreationTime, NasdaqFinancialStatus,
    NasdaqListingRecord, NasdaqMarketCategory, NasdaqModelError, NasdaqOtherExchange,
    NasdaqProviderFields,
};
pub use parser::{MAX_DIRECTORY_RECORDS, MAX_SOURCE_BYTES, NasdaqParseError};
pub use source::{
    NASDAQ_APPLICATION_BUDGET_WINDOW_NANOS, NASDAQ_APPLICATION_MAX_CONCURRENT_REQUESTS,
    NASDAQ_APPLICATION_MIN_BACKOFF_MAXIMUM_NANOS, NASDAQ_APPLICATION_REQUESTS_PER_MINUTE,
    NASDAQ_LISTED_URL, NASDAQ_REFERENCE_MIN_TOTAL_TIMEOUT_NANOS, NASDAQ_SYMBOL_DIRECTORY_DATASET,
    NASDAQ_SYMBOL_DIRECTORY_PROVIDER, NASDAQ_SYMBOL_DIRECTORY_VENUES, NasdaqDirectoryHealth,
    NasdaqLiveReferenceDoctorResult, NasdaqReferenceActivation, NasdaqReferenceIngestError,
    NasdaqReferenceRetryEvidence, NasdaqSymbolDirectoryConfig, NasdaqSymbolDirectoryDiscovery,
    NasdaqSymbolDirectorySource, NasdaqSymbolDirectorySourceError, OTHER_LISTED_URL,
    nasdaq_reference_endpoint_policy,
};
