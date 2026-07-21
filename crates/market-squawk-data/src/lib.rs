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
    ResumedIngest, SourceCursor,
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
pub use rights::{
    IngestIdentity, RegisteredRightsGrant, RightsDecisionInput, RightsError, SourceOperation,
};
pub use universe::{
    MAX_UNIVERSE_CANDIDATES, MAX_UNIVERSE_RETAINED_BYTES, UniverseConflictCounts,
    UniverseConflictEvidence, UniverseError, UniverseExclusion, UniverseExclusionCounts,
    UniverseExclusionReason, UniverseId, UniverseLimits, UniverseMembership, UniverseSnapshot,
};
