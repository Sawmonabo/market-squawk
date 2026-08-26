//! Streamed SEC quarterly archive capture, inspection, recovery, and typed row projection.

mod archive;
mod model;
mod native_query;
mod query;

pub use archive::{
    SecBulkParseLimits, SecBulkRowSink, SecBulkScanReport, SecBulkTypedArchiveScan,
    inspect_bulk_archive, recover_bulk_archive, scan_bulk_archive, scan_bulk_archive_typed,
};
pub use model::{
    SEC_BULK_CATALOG_SNAPSHOT_DATE, SEC_NCEN_SCHEMA_EFFECTIVE_DATE, SEC_NCEN_SCHEMA_VERSION,
    SEC_NPORT_SCHEMA_EFFECTIVE_DATE, SEC_NPORT_SCHEMA_VERSION, SecAuthoritativeIdentifierNamespace,
    SecBulkActivationEvidence, SecBulkCandidatePublicationPermit, SecBulkCapture,
    SecBulkCatalogSnapshot, SecBulkColumnContract, SecBulkCoverage, SecBulkDeclaredTableContract,
    SecBulkDoctorReport, SecBulkDoctorState, SecBulkFamily, SecBulkJoinCoordinate,
    SecBulkJoinDomain, SecBulkKeyField, SecBulkLayoutManifest, SecBulkMediaKind, SecBulkNativeRow,
    SecBulkNativeRowMembership, SecBulkNotRepresentedReason, SecBulkNumericAttribute,
    SecBulkProjectionDisposition, SecBulkProviderProjection, SecBulkRelatedRowsState,
    SecBulkRelatedTableRows, SecBulkRepresentationState, SecBulkSchemaIdentity, SecBulkSelection,
    SecBulkTableKind, SecBulkTablePresence, SecBulkTableReceipt, SecBulkTransportEvidence,
    SecBulkTypedField, SecBulkTypedValue, SecFilingChronology, SecFundHoldingCandidate,
    SecFundHoldingCandidatesQuery, SecFundIdentityResolution, SecGovernedIdentityReceipt,
    SecHoldingInstrumentResolution, SecHoldingResolutionState, SecNcenEtfRow,
    SecNcenFundMetadataCandidate, SecNcenFundMetadataQuery, SecNcenFundRow, SecNcenRegistrantRow,
    SecNcenSecurityExchangeRow, SecNcenSubmissionRow, SecNportFundRow, SecNportHoldingRow,
    SecNportHoldingSupplementSet, SecNportIdentifierRow, SecNportRegistrantRow,
    SecNportSubmissionRow, SecQuarter,
};
pub use native_query::{
    SecBulkNativeGenerationReceipt, SecBulkNativeJoinFilter, SecBulkNativePublicationSession,
    SecBulkNativePublishedGeneration, SecBulkNativeQueryCursor, SecBulkNativeQueryPage,
    query_native_rows, query_native_rows_by_joins, query_nport_holding_supplements,
    recover_native_generation, recover_native_generation_from_receipt,
};
pub use query::{
    SecBulkCandidateGenerationReceipt, SecBulkPublicationSession, SecBulkPublishedGeneration,
    SecBulkPublishedRecord, SecBulkQueryCompleteness, SecBulkQueryCursor, SecBulkQueryLimits,
    SecBulkQueryPage, query_fund_holding_candidates, query_ncen_fund_metadata,
    recover_bulk_candidate_generation_from_receipt, recover_fund_holding_candidate_generation,
    recover_ncen_generation,
};

use thiserror::Error;

/// Hardened SEC quarterly-bulk contract failure.
#[derive(Debug, Error)]
pub enum SecBulkError {
    /// Quarter components are structurally invalid.
    #[error("SEC bulk quarter is invalid")]
    InvalidQuarter,
    /// The exact family/quarter is outside the frozen official catalogue snapshot.
    #[error("SEC bulk quarter is not published in the admitted catalogue snapshot")]
    QuarterNotPublished,
    /// Accepted technical-specification identity is invalid.
    #[error("SEC bulk schema identity is invalid")]
    InvalidSchemaIdentity,
    /// Streamed capture identity or length is invalid.
    #[error("SEC bulk capture is invalid")]
    InvalidCapture,
    /// Archive layout, metadata, or exact member closure is invalid.
    #[error("SEC bulk layout is invalid")]
    InvalidLayout,
    /// The compressed archive exceeds its configured bound.
    #[error("SEC bulk compressed archive exceeds its bound")]
    ArchiveTooLarge,
    /// ZIP structure, member path, method, encryption state, or overlap is unsafe.
    #[error("SEC bulk archive is unsafe")]
    UnsafeArchive,
    /// ZIP member count exceeds its configured bound.
    #[error("SEC bulk archive entry count exceeds its bound")]
    EntryLimitExceeded,
    /// A decoded ZIP member exceeds its configured bound.
    #[error("SEC bulk archive member exceeds its decoded-byte bound")]
    EntryByteLimitExceeded,
    /// Total decoded ZIP bytes exceed their configured bound.
    #[error("SEC bulk archive exceeds its aggregate decoded-byte bound")]
    ExpandedByteLimitExceeded,
    /// A ZIP member exceeds the configured compression ratio.
    #[error("SEC bulk archive member exceeds its compression-ratio bound")]
    CompressionRatioExceeded,
    /// W3C tabular metadata is malformed or does not bind the exact archive layout.
    #[error("SEC bulk W3C metadata is invalid")]
    InvalidMetadata,
    /// A required typed table is missing from exact metadata/member closure.
    #[error("SEC bulk archive is missing a required table")]
    MissingRequiredTable,
    /// The TSV header differs from its exact metadata order.
    #[error("SEC bulk TSV header mismatches W3C metadata")]
    HeaderMismatch,
    /// A scan-time TSV header differs from the sealed table receipt.
    #[error("SEC bulk TSV header mismatches the sealed contract for {0}")]
    TableHeaderMismatch(market_squawk_domain::SourceIdentifier),
    /// A TSV row is malformed or contains invalid UTF-8 or a typed field value.
    #[error("SEC bulk TSV row is invalid")]
    InvalidTsv,
    /// A TSV row, field, column, or row count exceeds configured bounds.
    #[error("SEC bulk TSV content exceeds configured bounds")]
    TsvLimitExceeded,
    /// Declared primary keys are duplicated or an exact cross-table key has no producer.
    #[error("SEC bulk primary-key or cross-table relational integrity failed")]
    RelationalIntegrity,
    /// Archive recovery does not reproduce the expected immutable layout manifest.
    #[error("SEC bulk recovery evidence mismatches its manifest")]
    RecoveryMismatch,
    /// A governed fund/security identity bridge is missing or invalid.
    #[error("SEC bulk identity remains unresolved")]
    UnresolvedIdentity,
    /// The immutable generation has no row for the exact provider-native coordinate.
    #[error("SEC bulk provider-native row is unavailable")]
    NativeRowUnavailable,
    /// The immutable generation has multiple rows for a coordinate that requires one exact row.
    #[error("SEC bulk provider-native row is ambiguous")]
    NativeRowAmbiguous,
    /// Provider-native and canonical identity/filing coordinates disagree.
    #[error("SEC bulk canonical mapping is inconsistent")]
    InvalidCanonicalMapping,
    /// Filing, provider-release, and local-observation clocks are inconsistent.
    #[error("SEC bulk filing chronology is invalid")]
    InvalidChronology,
    /// Doctor evidence does not authorize activation.
    #[error("SEC bulk activation evidence is not ready")]
    ActivationNotReady,
    /// Publication receipt or immutable generation state is incomplete.
    #[error("SEC bulk generation is not ready for publication or query")]
    PublicationNotReady,
    /// A scan, materialized result, or publication count exceeded its explicit bound.
    #[error("SEC bulk query or publication exceeds its bound")]
    QueryLimitExceeded,
    /// Aggregate confined scratch space exceeded its explicit finite validation bound.
    #[error("SEC bulk external-validation scratch exceeds its bound")]
    ScratchLimitExceeded,
    /// A bounded allocation failed.
    #[error("SEC bulk bounded allocation failed")]
    AllocationFailed,
    /// The operation was cooperatively cancelled.
    #[error("SEC bulk operation was cancelled")]
    Cancelled,
    /// The caller's absolute acquisition/inspection deadline elapsed.
    #[error("SEC bulk operation exceeded its deadline")]
    DeadlineExceeded,
    /// Exact raw evidence persistence/reopen failed.
    #[error(transparent)]
    RawEvidence(#[from] crate::RawEvidenceError),
    /// A provider locator or source identifier was invalid.
    #[error(transparent)]
    Client(#[from] crate::SecClientError),
    /// A source/canonical identifier was invalid.
    #[error(transparent)]
    Identity(#[from] market_squawk_domain::IdentityError),
}

impl From<serde_json::Error> for SecBulkError {
    fn from(_value: serde_json::Error) -> Self {
        Self::InvalidMetadata
    }
}

impl From<std::io::Error> for SecBulkError {
    fn from(value: std::io::Error) -> Self {
        Self::RawEvidence(crate::RawEvidenceError::Io(value))
    }
}
