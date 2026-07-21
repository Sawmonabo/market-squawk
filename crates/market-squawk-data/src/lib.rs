// Rust #159105: this macOS-only test-link diagnostic is caused by the measured
// `__eh_frame` exceeding arm64 compact-unwind's 24-bit offset range.
#![cfg_attr(test, allow(linker_messages))]

//! Durable local catalog, source-rights admission, and recovery metadata.
//!
//! This crate is a control- and research-plane boundary. It is never queried from the live
//! event-to-action path.

mod analytical_backup;
mod arrow_convert;
mod authority_transition;
mod blocking_supervisor;
mod catalog;
mod corporate_actions;
mod ingest;
mod manifest;
mod migrations;
mod parquet_store;
mod pit;
mod publication_coordinator;
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
    CatalogError, CatalogHealth, CatalogLimit, CatalogResultLimits, ContractCompletion,
    DatasetManifestRecord, IngestReservation, IngestRunRecord, IngestRunState, PublishedIngest,
    QueryArtifactReservation, QueryArtifactReservationInput, QueryArtifactResult, ReferenceBundle,
    ResumedIngest, SourceCursor, StoredObservedRevision,
};
pub use corporate_actions::{
    AdjustmentConflict, AdjustmentRatio, AdjustmentStep, CorporateActionAdjustment,
    CorporateActionError, CorporateActionExclusion, CorporateActionExclusionReason,
    CorporateActionLimits, CorporateActionPlan, CorporateActionPolicy, CorporateActionRecord,
    MAX_CORPORATE_ACTION_RETAINED_BYTES, MAX_CORPORATE_ACTIONS,
};
pub use ingest::{
    AnalyticalDataService, CommittedDataset, CompactionRequest, IngestError,
    QueryArtifactPublication, ResearchIngestService, extraction_batch_digest,
};
pub use manifest::{
    AnalyticalManifestCatalog, DatasetBuildSpecDigest, DatasetId, DatasetManifestRef,
    DerivedGenerationParents, GenerationKind, GenerationParent, GenerationParentRelation,
    MAX_DERIVED_GENERATION_PARENTS, ManifestCatalogError, ManifestObject, ManifestPlan,
    ManifestPlanError, PinnedDataset, PinnedManifestObject, Sha256Digest,
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
pub use query::{
    QueryError, QueryLimits, QueryRequest, QueryResult, ResearchQueryEngine, ResearchQueryService,
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
    MAX_UNIVERSE_CANDIDATES, MAX_UNIVERSE_RETAINED_BYTES, UniverseConflictCounts,
    UniverseConflictEvidence, UniverseError, UniverseExclusion, UniverseExclusionCounts,
    UniverseExclusionReason, UniverseId, UniverseLimits, UniverseMembership, UniverseSnapshot,
};
