//! Durable local catalog, source-rights admission, and recovery metadata.
//!
//! This crate is a control- and research-plane boundary. It is never queried from the live
//! event-to-action path.

mod analytical_backup;
mod arrow_convert;
mod authority_transition;
mod blocking_supervisor;
mod catalog;
mod ingest;
mod manifest;
mod migrations;
mod parquet_store;
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
pub use ingest::{
    AnalyticalDataService, CommittedDataset, CompactionRequest, IngestError,
    QueryArtifactPublication, ResearchIngestService, extraction_batch_digest,
};
pub use manifest::{
    AnalyticalManifestCatalog, DatasetId, DatasetManifestRef, GenerationKind, ManifestCatalogError,
    ManifestObject, ManifestPlan, ManifestPlanError, PinnedDataset, PinnedManifestObject,
    Sha256Digest,
};
pub use parquet_store::{
    ObjectStoreConfig, OrphanRecoveryReport, ParquetObjectStore, ParquetStoreError, PublishedObject,
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
