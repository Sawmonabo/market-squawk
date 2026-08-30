// Rust #159105: this macOS-only test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range.
#![cfg_attr(test, allow(linker_messages))]

//! Durable local catalog, source-rights admission, and recovery metadata.
//!
//! This crate is a control- and research-plane boundary. It is never queried from the live
//! event-to-action path.

mod analytical_backup;
mod analytical_read;
mod arrow_convert;
mod authority_transition;
mod blocking_supervisor;
mod catalog;
mod catalog_capabilities;
mod corporate_actions;
mod dataset_builder;
mod fund_holdings;
mod ingest;
mod manifest;
mod market_event;
mod migrations;
mod option_market;
mod parquet_store;
mod pit;
mod provider_event_selection;
mod provider_rate;
mod publication_coordinator;
mod python_dataset;
mod query;
mod research_use;
mod rights;
mod schema;
mod universe;

pub use analytical_backup::{
    AnalyticalBackupBundleReceipt, AnalyticalBackupError, AnalyticalBackupLimits,
    AnalyticalBackupLocation, AnalyticalBackupReceiptError, AnalyticalBackupService,
    AnalyticalRestoreMode, AnalyticalRestoreTarget, VerifiedAnalyticalBackup,
};
pub use analytical_read::{
    AnalyticalFeatureDataset, AnalyticalFeatureDatasetPage, AnalyticalFeatureDatasetSelection,
    AnalyticalFundNavOutput, AnalyticalFundNavReadLimit, AnalyticalFundNavReadRequest,
    AnalyticalGeneration, AnalyticalGenerationPage, AnalyticalMacroLatestKnownOutput,
    AnalyticalMacroLatestKnownRequest, AnalyticalMacroProviderPeriodLatestKnownOutput,
    AnalyticalMacroProviderPeriodLatestKnownRequest, AnalyticalMacroSeriesAllowlist,
    AnalyticalMacroSourceQualifiedSeries, AnalyticalMarketBarOutput, AnalyticalMarketBarReadLimit,
    AnalyticalMarketBarReadRequest, AnalyticalObservationOutput, AnalyticalObservationReadRequest,
    AnalyticalObservationTemplate, AnalyticalReadCapability, AnalyticalReadError,
    AnalyticalReadLimit, CompleteMarketBarHistoryOutput, CompleteMarketBarHistoryReadReceipt,
    ForecastDatasetEvidence, ForecastDatasetEvidenceFence, ForecastDatasetReadLimits,
    ForecastFeatureRow, ForecastFeatureValue, FundNavDateRange, MarketBarEffectiveRange,
    ObservationKnowledgeRange, OutcomeMarketBarRequest, OutcomeMarketBarSelectedReceipt,
    OutcomeMarketBarSelection, OutcomeMarketBarSeries, OutcomeMarketBarUnavailableReason,
};
pub use arrow_convert::{
    ArrowConversionError, DatasetArrowBatch, DatasetSchemaError, DatasetSchemaRef,
    DatasetSchemaRegistry, FeatureLabelBatchBindings, ResearchArrowBatch,
};
pub use authority_transition::evidence::CatalogContentEvidenceDigest;
pub use authority_transition::{
    ArtifactInventoryDigest, AuthorityEventDigest, AuthorityEvidenceDigest, AuthorityGeneration,
    CatalogEndpointIdentity, StableArtifactRootIdentity,
};
pub use catalog::{
    ArtifactRecord, AuditEvent, BackupReceipt, Catalog, CatalogAuthority, CatalogConfig,
    CatalogDiagnosticSnapshot, CatalogError, CatalogHealth, CatalogLimit, CatalogResultLimits,
    CompanyIdentityMatchKind, CompanyIdentityMatchReason, CompanyIdentitySearchMatch,
    CompanyIdentitySearchPage, CompanySecurityIdentityCatalogError,
    CompanySecurityIdentityDisposition, CompanySecurityIdentityExclusion,
    CompanySecurityIdentityExclusionReason, CompanySecurityIdentityQuery,
    CompanySecurityIdentityReadCapability, CompanySecurityIdentityRecord,
    CompanySecurityIdentitySelection, CompanySecurityIdentitySelectionReceipt,
    CompanySecurityLinkPublicationCapability, CompanySecurityLinkPublicationDisposition,
    CompanySecurityLinkPublicationReceipt, CompanySecuritySelectionReceiptEntry,
    ContractCompletion, DatasetManifestRecord, FairValueCatalogAuditEvent, FairValueCatalogCommit,
    FairValueCatalogLink, FairValueCatalogOperation, FairValueCatalogPosition,
    FairValueCatalogRecord, FairValueCatalogSnapshot, FairValueCatalogSnapshotLimits,
    FairValueCommitDisposition, FairValueLinkRelation, FairValueOperationKind, FairValueRecordKind,
    IngestReservation, IngestRunRecord, IngestRunState, InstrumentSearchMatch,
    InstrumentSearchPage, ListingReferenceDirectoryPresence, ListingReferenceError,
    ListingReferenceExchangeCode, ListingReferenceFileEvidence, ListingReferenceFileKind,
    ListingReferenceFinancialStatus, ListingReferenceGenerationInput,
    ListingReferenceGenerationReceipt, ListingReferenceGenerationSelection,
    ListingReferenceMarketCategory, ListingReferenceMatchKind, ListingReferenceMembershipCursor,
    ListingReferenceMembershipPage, ListingReferenceMembershipPageState,
    ListingReferenceMembershipSelectionReceipt, ListingReferencePublicationCapability,
    ListingReferencePublicationDisposition, ListingReferencePublicationReceipt,
    ListingReferenceReadCapability, ListingReferenceRecord, ListingReferenceRecordInput,
    ListingReferenceRightsState, ListingReferenceSearchMatch, ListingReferenceSearchPage,
    ListingReferenceSourceFileInput, MAX_COMPANY_SECURITY_SELECTION_ROWS,
    MAX_LISTING_REFERENCE_MEMBERSHIP_PAGE_ROWS, MAX_LISTING_REFERENCE_RECORDS,
    MAX_LISTING_REFERENCE_SEARCH_ROWS, MAX_MARKET_DATA_INSTRUMENT_POPULATION_ROWS,
    MAX_MARKET_DATA_INSTRUMENT_SEARCH_ROWS, MAX_MARKET_DATA_INSTRUMENT_SYNC_ROWS,
    MarketDataInstrumentCatalogError, MarketDataInstrumentMatchKind,
    MarketDataInstrumentPopulationDisposition, MarketDataInstrumentPopulationExclusion,
    MarketDataInstrumentPopulationExclusionReason, MarketDataInstrumentPopulationQuery,
    MarketDataInstrumentPopulationSelection, MarketDataInstrumentReadCapability,
    MarketDataInstrumentRecord, MarketDataInstrumentSearchMatch, MarketDataInstrumentSearchPage,
    MarketDataInstrumentSynchronization, MarketDataInstrumentSynchronizationCapability,
    MarketDataInstrumentSynchronizationReceipt, OnboardingAppendOutcome, OnboardingReservation,
    OnboardingReservationRequest, PinnedInstrumentDefinitions, ProviderOnboardingDiagnostic,
    PublishedIngest, QueryArtifactReservation, QueryArtifactReservationInput, QueryArtifactResult,
    ReferenceBundle, ResumedIngest, ResumedProviderOnboarding, SourceCursor,
    StoredObservedRevision,
};
pub use catalog::{
    PersistedProviderCaptureBindingEvidence, PersistedProviderCaptureBindingRow,
    PersistedProviderCapturePhysicalClaim, PersistedProviderNativeLineageSchema,
};
pub use catalog::{
    PersistedProviderEventBindingEvidence, PersistedProviderEventBindingRow,
    PersistedProviderEventNativeLineage, PersistedProviderPublicationEvidence,
    PersistedProviderResponseMarketEventBindingEvidence,
    PersistedProviderResponseMarketEventBindingRow,
};
pub use catalog::{
    PersistedProviderOptionMarketBindingEvidence, PersistedProviderOptionMarketBindingRow,
    PersistedProviderOptionMarketNativeLineage,
};
pub use catalog_capabilities::{
    CompanyIdentityReadCapability, FairValueCatalogCapability, InstrumentCatalogCapability,
    InstrumentDefinitionReadCapability, OnboardingCatalogCapability,
};
pub use corporate_actions::{
    AdjustmentConflict, AdjustmentRatio, AdjustmentStep, CorporateActionAdjustment,
    CorporateActionError, CorporateActionExclusion, CorporateActionExclusionReason,
    CorporateActionLimits, CorporateActionPlan, CorporateActionPolicy, CorporateActionRecord,
    MAX_CORPORATE_ACTION_RETAINED_BYTES, MAX_CORPORATE_ACTIONS,
};
pub use dataset_builder::{
    ChronologicalSplitPolicy, ComponentAdjustmentEvidence, ComponentKind, ComponentScope,
    ComponentSelector, ComponentValue, CorporateActionSensitivity, DatasetBuildError,
    DatasetBuildInputs, DatasetBuildLimits, DatasetBuildPolicy, DatasetBuildPrecommitAuthority,
    DatasetBuildRequest, DatasetBuilder, DatasetBuilderService, DatasetExample,
    DatasetOutputAuthorization, DatasetResearchUsePreflightReceipt, DatasetSplit,
    DatasetSplitCounts, FEATURE_DATASET_PRODUCTION_RECEIPT_SCHEMA, FEATURE_LABEL_PROBABILITY_UNIT,
    FEATURE_LABEL_RETURN_UNIT, FeatureDatasetProductContract, FeatureDatasetProductionComposition,
    FeatureDatasetProductionError, FeatureDatasetProductionProofV1,
    FeatureDatasetProductionPublication, FeatureDatasetProductionPublicationDisposition,
    FeatureDatasetProductionPublisher, FeatureDatasetProductionReceiptV1,
    FeatureLabelComponentInput, FeatureLabelComponentSpec, FeatureLabelDataset,
    FeatureLabelMeasurement, FeatureLabelMeasurementBinding, FeatureLabelPythonExport,
    MAX_FEATURE_DATASET_PRODUCTION_RECEIPT_BYTES, MAX_FEATURE_LABEL_EXPORT_BYTES,
    MissingValuePolicy,
};
pub use fund_holdings::{
    FundHoldingsArrowBatch, FundLatestUnavailableReason, FundPointInTimeOutcome,
    FundPointInTimeRequest, FundPointInTimeRevisionMode, FundPointInTimeSelection,
    MAX_FUND_HOLDINGS_BATCH_RECORDS, MAX_FUND_HOLDINGS_RETAINED_BYTES,
};
pub use ingest::{
    AnalyticalDataService, CommittedDataset, CompactionRequest,
    GenerationOwnedProviderCaptureEvidence, IngestError, IngestPrecommitAuthority,
    ListingReferenceAdmissionCapability, PendingProviderMacroPlanPublication,
    PinnedArtifactQueryRequest, ProviderMacroPlanChunkInput, ProviderMacroPlanPublicationInput,
    ProviderMacroPlanPublicationReceipt, ProviderMacroPlanRestartSelector,
    ProviderMacroPlanSemantics, ProviderMarketEventPublicationKind,
    ProviderMarketEventPublicationSelector, ProviderOptionMarketPublicationSelector,
    ProviderPublicationInput, QueryArtifactPublication, ResearchIngestService,
    extraction_batch_digest, extraction_provider_payload_digest,
    provider_market_event_publication_digest, provider_option_market_publication_digest,
};
pub use manifest::{
    AnalyticalManifestCatalog, CanonicalMarketBarHistoryRequest, CompleteMarketBarHistoryRequest,
    CompleteMarketBarHistorySelection, DatasetBuildSpecDigest, DatasetId, DatasetManifestRef,
    DerivedGenerationParents, GenerationKind, GenerationParent, GenerationParentRelation,
    MAX_DERIVED_GENERATION_PARENTS, MAX_RETAINED_FEATURE_DATASET_PRODUCTION_ADMISSIONS,
    MAX_RETAINED_FEATURE_DATASET_PRODUCTION_PAYLOAD_BYTES, ManifestCatalogError, ManifestObject,
    ManifestPlan, ManifestPlanError, MarketBarHistoryPublicationReceipt,
    MarketHistorySelectionPolicy, PinnedDataset, PinnedManifestObject, Sha256Digest,
};
#[cfg(feature = "release-evidence")]
pub use manifest::{
    ReleaseEvidenceStorageError, ReleaseEvidenceStorageResult, run_release_evidence_storage,
};
pub use market_event::ProviderMarketEventArrowBatch;
pub use option_market::{
    OptionMarketPointInTimeRequest, OptionMarketPointInTimeSelection,
    ProviderOptionMarketArrowBatch,
};
pub use parquet_store::{
    ObjectStoreConfig, OrphanRecoveryReport, ParquetObjectStore, ParquetStoreError, PublishedObject,
};
pub use pit::{
    MAX_POINT_IN_TIME_CANDIDATES, MAX_POINT_IN_TIME_CONFLICTS, MAX_POINT_IN_TIME_FAMILIES,
    MAX_POINT_IN_TIME_RESULT_ROWS, MAX_POINT_IN_TIME_RETAINED_BYTES, ObservationFamilyKey,
    POINT_IN_TIME_IDENTITY_SCHEMA_VERSION, PointInTimeCandidate, PointInTimeConflict,
    PointInTimeConflictCounts, PointInTimeConflictReport, PointInTimeError, PointInTimeExclusion,
    PointInTimeExclusionCounts, PointInTimeExclusionReason, PointInTimeExclusionReasons,
    PointInTimeLimits, PointInTimePolicy, PointInTimeRecord, PointInTimeRequest,
    PointInTimeRevisionCounts, PointInTimeRevisionMode, PointInTimeRevisionState,
    PointInTimeSelection, PointInTimeService,
};
pub use provider_event_selection::{
    MAX_PROVIDER_MARKET_EVENT_POINT_IN_TIME_CANDIDATES, ProviderMarketEventComponentKind,
    ProviderMarketEventEffectiveTimeBasis, ProviderMarketEventExactPublication,
    ProviderMarketEventExclusionCounts, ProviderMarketEventPointInTimeRequest,
    ProviderMarketEventPointInTimeSelection, ProviderMarketEventSelectedCandidate,
    ProviderMarketEventSelectionCompleteness, ProviderMarketEventSelectionCoordinate,
    ProviderMarketEventSelectionError, ProviderMarketEventSourceSelection,
};
pub(crate) use provider_event_selection::{
    ProviderMarketEventCatalogCandidate, ProviderMarketEventCatalogPlan,
};
pub use provider_rate::SqliteProviderRateStore;
pub use python_dataset::{
    PythonDatasetCatalogError, PythonDatasetIdentity, PythonDatasetRow, PythonDatasetSelection,
    PythonDatasetSelectionRevalidation, PythonDatasetValue, PythonDatasetVerificationLimits,
    verify_python_dataset,
};
pub use query::{
    PinnedFeatureMonetaryValue, PinnedMonetaryValue, PinnedQueryOutput, QueryError, QueryLimits,
    QueryRequest, QueryResult, ResearchQueryEngine, ResearchQueryService,
};
pub use research_use::{
    AuthorizedResearchUse, DerivedOutputObjectInput, DerivedPublicationDigest,
    DerivedPublicationInput, DerivedPublicationObject, DerivedRetentionOperation,
    MAX_DERIVED_PUBLICATION_OBJECTS, MAX_RESEARCH_USE_EDGES, MAX_RESEARCH_USE_GRAPH_NODES,
    MAX_RESEARCH_USE_PERMIT_LIFETIME_SECS, MAX_RESEARCH_USE_RETAINED_BYTES, MAX_RESEARCH_USE_ROOTS,
    MAX_RESEARCH_USE_SOURCES, MAX_RESEARCH_USE_TRAVERSAL_DEADLINE_SECS, PublishedDerivedGeneration,
    RegisteredResearchUseGrant, ResearchUse, ResearchUseAuthorityEvidence, ResearchUseCatalogError,
    ResearchUseDecisionDigest, ResearchUseDecisionInput, ResearchUseDecisionOutcome,
    ResearchUseDenialReason, ResearchUseError, ResearchUseGeneration, ResearchUseGrantInput,
    ResearchUseGraph, ResearchUseGraphDigest, ResearchUseGraphEdge, ResearchUseLimits,
    ResearchUsePermit, ResearchUseRequest, ResearchUseRevocationInput, ResearchUseRevocationReason,
    ResearchUseRevocationReceipt, ResearchUseSet, ResearchUseSourceInput,
};
pub use rights::{
    ImportedUserInputBasis, ImportedUserInputEvidence, IngestIdentity, RegisteredRightsGrant,
    ReviewedTermsBasis, RightsBasis, RightsDecisionInput, RightsError, SourceOperation,
    UserOwnedLocalBasis,
};
pub use universe::{
    ContractRollEvidence, DerivativeBoundary, DerivativeCivilDate, DerivativeLifecycle,
    DerivativeLifecycleEvidence, DerivativeSelectionDecision, DerivativeUniverseSnapshot,
    MAX_UNIVERSE_CANDIDATES, MAX_UNIVERSE_RETAINED_BYTES, UniverseConflictCounts,
    UniverseConflictEvidence, UniverseError, UniverseExclusion, UniverseExclusionCounts,
    UniverseExclusionReason, UniverseId, UniverseLimits, UniverseMembership, UniverseSnapshot,
};
