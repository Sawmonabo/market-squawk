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
mod ingest;
mod manifest;
mod migrations;
mod parquet_store;
mod pit;
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
    AnalyticalGeneration, AnalyticalGenerationPage, AnalyticalObservationOutput,
    AnalyticalObservationReadRequest, AnalyticalObservationTemplate, AnalyticalReadCapability,
    AnalyticalReadError, AnalyticalReadLimit, ForecastDatasetEvidence,
    ForecastDatasetEvidenceFence, ForecastDatasetReadLimits, ForecastFeatureRow,
    ForecastFeatureValue, ObservationKnowledgeRange,
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
    CompanyIdentitySearchPage, ContractCompletion, DatasetManifestRecord,
    FairValueCatalogAuditEvent, FairValueCatalogCommit, FairValueCatalogLink,
    FairValueCatalogOperation, FairValueCatalogPosition, FairValueCatalogRecord,
    FairValueCatalogSnapshot, FairValueCatalogSnapshotLimits, FairValueCommitDisposition,
    FairValueLinkRelation, FairValueOperationKind, FairValueRecordKind, IngestReservation,
    IngestRunRecord, IngestRunState, InstrumentSearchMatch, InstrumentSearchPage,
    OnboardingAppendOutcome, OnboardingReservation, OnboardingReservationRequest,
    PinnedInstrumentDefinitions, ProviderOnboardingDiagnostic, PublishedIngest,
    QueryArtifactReservation, QueryArtifactReservationInput, QueryArtifactResult, ReferenceBundle,
    ResumedIngest, ResumedProviderOnboarding, SourceCursor, StoredObservedRevision,
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
    DatasetOutputAuthorization, DatasetSplit, DatasetSplitCounts, FeatureLabelComponentInput,
    FeatureLabelComponentSpec, FeatureLabelDataset, FeatureLabelPythonExport,
    MAX_FEATURE_LABEL_EXPORT_BYTES, MissingValuePolicy, PythonDatasetAdmission,
};
pub use ingest::{
    AnalyticalDataService, CommittedDataset, CompactionRequest, IngestError,
    IngestPrecommitAuthority, PinnedArtifactQueryRequest, QueryArtifactPublication,
    ResearchIngestService, extraction_batch_digest, extraction_provider_payload_digest,
};
pub use manifest::{
    AnalyticalManifestCatalog, DatasetBuildSpecDigest, DatasetId, DatasetManifestRef,
    DerivedGenerationParents, GenerationKind, GenerationParent, GenerationParentRelation,
    MAX_DERIVED_GENERATION_PARENTS, MAX_RETAINED_PYTHON_DATASET_ADMISSIONS,
    MAX_RETAINED_PYTHON_DATASET_DESCRIPTOR_BYTES, ManifestCatalogError, ManifestObject,
    ManifestPlan, ManifestPlanError, PinnedDataset, PinnedManifestObject, Sha256Digest,
};
#[cfg(feature = "release-evidence")]
pub use manifest::{
    ReleaseEvidenceStorageError, ReleaseEvidenceStorageResult, run_release_evidence_storage,
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
    IngestIdentity, RegisteredRightsGrant, ReviewedTermsBasis, RightsBasis, RightsDecisionInput,
    RightsError, SourceOperation, UserOwnedLocalBasis,
};
pub use universe::{
    ContractRollEvidence, DerivativeBoundary, DerivativeCivilDate, DerivativeLifecycle,
    DerivativeLifecycleEvidence, DerivativeSelectionDecision, DerivativeUniverseSnapshot,
    MAX_UNIVERSE_CANDIDATES, MAX_UNIVERSE_RETAINED_BYTES, UniverseConflictCounts,
    UniverseConflictEvidence, UniverseError, UniverseExclusion, UniverseExclusionCounts,
    UniverseExclusionReason, UniverseId, UniverseLimits, UniverseMembership, UniverseSnapshot,
};
