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
mod ingest;
mod manifest;
mod migrations;
mod parquet_store;
mod publication_coordinator;
mod query;
mod rights;
mod schema;

pub use analytical_backup::{
    AnalyticalBackupBundleReceipt, AnalyticalBackupError, AnalyticalBackupLimits,
    AnalyticalBackupLocation, AnalyticalBackupReceiptError, AnalyticalBackupService,
    AnalyticalRestoreMode, AnalyticalRestoreTarget, VerifiedAnalyticalBackup,
};
pub use arrow_convert::{ArrowConversionError, ResearchArrowBatch};
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
