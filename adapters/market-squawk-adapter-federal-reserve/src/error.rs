//! Closed adapter failures.

use thiserror::Error;

/// A bounded Federal Reserve Board file-contract, parser, or publication failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum BoardAdapterError {
    /// A configured contract is empty, inconsistent, or outside its code-owned bounds.
    #[error("invalid Federal Reserve Board dataset contract")]
    InvalidContract,
    /// The acquisition URL is not an admitted official Board HTTPS route.
    #[error("Federal Reserve Board request URL is outside the official allowlist")]
    RequestUrlRejected,
    /// The supplied bytes do not match the contract's selected transport format.
    #[error("Federal Reserve Board file format does not match its contract")]
    FormatMismatch,
    /// Input or expanded data crossed an explicit byte bound.
    #[error("Federal Reserve Board input exceeds its byte budget")]
    ByteLimitExceeded,
    /// A cardinality or nesting bound was crossed.
    #[error("Federal Reserve Board input exceeds a structural limit")]
    StructuralLimitExceeded,
    /// Allocation arithmetic overflowed or a bounded allocation failed.
    #[error("Federal Reserve Board parser could not allocate within its bound")]
    AllocationFailed,
    /// A CSV parser failure occurred.
    #[error("invalid Federal Reserve Board CSV: {0}")]
    InvalidCsv(String),
    /// A CSV metadata/header contract drifted.
    #[error("Federal Reserve Board CSV header or schema drifted")]
    CsvSchemaDrift,
    /// A ZIP container is unsafe, inconsistent, or does not match its closed entry contract.
    #[error("unsafe or unexpected Federal Reserve Board ZIP archive")]
    UnsafeArchive,
    /// A ZIP member crossed the configured decompression-ratio bound.
    #[error("Federal Reserve Board ZIP member exceeds its compression-ratio budget")]
    CompressionRatioExceeded,
    /// An expected structural artifact is absent or has a different digest.
    #[error("Federal Reserve Board SDMX structural artifact identity mismatch")]
    StructuralArtifactMismatch,
    /// XML is not well formed.
    #[error("invalid Federal Reserve Board SDMX XML: {0}")]
    InvalidXml(String),
    /// XML namespace, header, version, element, or attribute semantics drifted.
    #[error("Federal Reserve Board SDMX schema or header drifted")]
    SdmxSchemaDrift,
    /// A series differs from the selected exact series contract.
    #[error("Federal Reserve Board series does not match the selected contract")]
    SeriesMismatch,
    /// A series or observation identity was repeated.
    #[error("Federal Reserve Board file contains a duplicate identity")]
    DuplicateIdentity,
    /// A provider value is neither an exact decimal nor an admitted missing marker.
    #[error("invalid Federal Reserve Board observation value")]
    InvalidValue,
    /// A provider period does not match the selected frequency or is not valid.
    #[error("invalid Federal Reserve Board observation period")]
    InvalidPeriod,
    /// Publication or revision chronology is inconsistent.
    #[error("invalid Federal Reserve Board publication chronology")]
    InvalidChronology,
    /// Publication event semantics do not match the observed change.
    #[error("Federal Reserve Board publication event does not match the dataset change")]
    InvalidRevisionEvidence,
    /// A predecessor receipt does not bind the predecessor dataset.
    #[error("Federal Reserve Board predecessor publication binding mismatch")]
    PredecessorMismatch,
    /// Checked count or revision arithmetic overflowed.
    #[error("Federal Reserve Board publication count overflow")]
    CountOverflow,
}
