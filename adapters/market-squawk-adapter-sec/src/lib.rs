//! Bounded SEC EDGAR submissions, filing, and XBRL extraction.

mod bulk;
mod client;
mod composite;
mod evidence_store;
mod extraction;
mod json;
mod normalize;
mod policy;
mod product;
mod representation_registry;
mod xbrl;

pub use bulk::{
    SEC_BULK_CATALOG_SNAPSHOT_DATE, SEC_NCEN_SCHEMA_EFFECTIVE_DATE, SEC_NCEN_SCHEMA_VERSION,
    SEC_NPORT_SCHEMA_EFFECTIVE_DATE, SEC_NPORT_SCHEMA_VERSION, SecAuthoritativeIdentifierNamespace,
    SecBulkActivationEvidence, SecBulkCandidateGenerationReceipt,
    SecBulkCandidatePublicationPermit, SecBulkCapture, SecBulkCatalogSnapshot,
    SecBulkColumnContract, SecBulkCoverage, SecBulkDeclaredTableContract, SecBulkDoctorReport,
    SecBulkDoctorState, SecBulkError, SecBulkFamily, SecBulkJoinCoordinate, SecBulkJoinDomain,
    SecBulkKeyField, SecBulkLayoutManifest, SecBulkLogicalPublicationHandoff, SecBulkLogicalRow,
    SecBulkLogicalRowLineage, SecBulkMediaKind, SecBulkNativeGenerationReceipt,
    SecBulkNativeJoinFilter, SecBulkNativePublicationSession, SecBulkNativePublishedGeneration,
    SecBulkNativeQueryCursor, SecBulkNativeQueryPage, SecBulkNativeRow, SecBulkNativeRowMembership,
    SecBulkNotRepresentedReason, SecBulkNumericAttribute, SecBulkParseLimits,
    SecBulkPendingLogicalRowSink, SecBulkProjectionDisposition, SecBulkProviderProjection,
    SecBulkPublicationSession, SecBulkPublishedGeneration, SecBulkPublishedRecord,
    SecBulkQueryCompleteness, SecBulkQueryCursor, SecBulkQueryLimits, SecBulkQueryPage,
    SecBulkRelatedRowsState, SecBulkRelatedTableRows, SecBulkRepresentationState, SecBulkRowSink,
    SecBulkScanReport, SecBulkSchemaIdentity, SecBulkSelection, SecBulkStagedLogicalPublication,
    SecBulkTableKind, SecBulkTablePresence, SecBulkTableReceipt, SecBulkTransportEvidence,
    SecBulkTypedArchiveScan, SecBulkTypedField, SecBulkTypedValue, SecFilingChronology,
    SecFundHoldingCandidate, SecFundHoldingCandidatesQuery, SecFundIdentityAuthority,
    SecFundIdentityResolution, SecFundPartitionAdmissions, SecFundPendingLogicalRows,
    SecFundPublicationScope, SecGovernedIdentityReceipt, SecHoldingInstrumentResolution,
    SecHoldingResolutionState, SecNcenEtfRow, SecNcenFundMetadataCandidate,
    SecNcenFundMetadataQuery, SecNcenFundRow, SecNcenRegistrantRow, SecNcenSecurityExchangeRow,
    SecNcenSubmissionRow, SecNportFundRow, SecNportHoldingRow,
    SecNportHoldingSupplementCompleteness, SecNportHoldingSupplementEvidence,
    SecNportHoldingSupplementSet, SecNportHoldingSupplementState, SecNportHoldingSupplementTable,
    SecNportHoldingSupplementTopology, SecNportIdentifierRow, SecNportRegistrantRow,
    SecNportSubmissionRow, SecPendingBulkLogicalPublication, SecPreparedFundCanonicalPartition,
    SecPreparedFundLogicalPublication, SecQuarter, inspect_bulk_archive,
    query_fund_holding_candidates, query_native_rows, query_native_rows_by_joins,
    query_ncen_fund_metadata, query_nport_holding_supplements, recover_bulk_archive,
    recover_bulk_candidate_generation_from_receipt, recover_fund_holding_candidate_generation,
    recover_native_generation, recover_native_generation_from_receipt, recover_ncen_generation,
    scan_bulk_archive, scan_bulk_archive_typed,
};
pub use client::{
    FilingTaxonomySharedRateBudgets, RetrievedCompanyFacts, RetrievedSecBytes,
    RetrievedSubmissions, RetrievedXbrlDocument, SecClientError, SecContact, SecEdgarSource,
    SecExtractionHealth, SecExtractionHealthState, SecObjectLocator,
};
pub use composite::SecCompositeBounds;
pub use evidence_store::{RawEvidenceError, RawEvidenceStore};
pub use extraction::{SecDiscoveryResult, SecExtractionResult, SecFilingXbrlCaptureHandoff};
pub use json::{
    CompanyFactOccurrence, CompanyFactPeriod, CompanyFactsDocument, SecFiling, SecFormerName,
    SecParserError, SecParserLimits, SecSubmissionCompanyMetadata, SecTickerExchangePair,
    SubmissionsArchive, SubmissionsDocument, reconcile_submissions,
    reconcile_submissions_with_cancellation,
};
pub use normalize::{
    SecNormalizationError, normalize_company_facts, normalize_company_facts_with_cancellation,
    normalize_filings, normalize_filings_with_cancellation,
};
pub use policy::{
    SEC_APPLICATION_MAX_CONCURRENT_REQUESTS, SEC_APPLICATION_REQUESTS_PER_SECOND,
    SEC_OFFICIAL_REQUEST_CEILING_PER_SECOND, SEC_PROVIDER_RATE_SCOPE,
    sec_application_budget_policy,
};
pub use product::{
    SEC_COMPANY_FACTS_DATASET_PREFIX, SEC_SUBMISSIONS_DATASET_PREFIX, SecResearchDataset,
    SecResearchDatasetKind, SecResearchSelection,
};
pub use representation_registry::{
    SecHttpValidators, SecRepresentation, SecRepresentationError, SecRepresentationLimits,
    SecRepresentationRegistry,
};
pub use xbrl::{
    ParsedXbrlDocument, SecXbrlError, XbrlDocumentContext, XbrlDocumentParser,
    XbrlNonnumericOccurrence, XbrlNumericFact,
};
