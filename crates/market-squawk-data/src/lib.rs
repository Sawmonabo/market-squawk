//! Durable local catalog, source-rights admission, and recovery metadata.
//!
//! This crate is a control- and research-plane boundary. It is never queried from the live
//! event-to-action path.

mod arrow_convert;
mod catalog;
mod ingest;
mod manifest;
mod migrations;
mod parquet_store;
mod publication_coordinator;
mod query;
mod rights;
mod schema;

pub use arrow_convert::{ArrowConversionError, ResearchArrowBatch};
pub use catalog::{
    ArtifactRecord, AuditEvent, BackupReceipt, Catalog, CatalogAuthority, CatalogConfig,
    CatalogError, CatalogHealth, CatalogLimit, CatalogResultLimits, ContractCompletion,
    DatasetManifestRecord, IngestReservation, IngestRunRecord, IngestRunState, PublishedIngest,
    QueryArtifactPublisher, QueryArtifactReservation, QueryArtifactReservationInput,
    QueryArtifactResult, ReferenceBundle, ResumedIngest, SourceCursor,
};
pub use ingest::{
    AnalyticalDataService, CommittedDataset, CompactionRequest, IngestError, ResearchIngestService,
    extraction_batch_digest,
};
pub use manifest::{
    AnalyticalManifestCatalog, DatasetId, DatasetManifestRef, GenerationKind, ManifestCatalogError,
    ManifestObject, ManifestPlan, ManifestPlanError, PinnedDataset, PinnedManifestObject,
    Sha256Digest,
};
pub use parquet_store::{
    ObjectStoreConfig, OrphanRecoveryReport, ParquetObjectStore, ParquetStoreError, PublishedObject,
};
pub use publication_coordinator::PublicationLease;
pub use query::{
    QueryError, QueryLimits, QueryRequest, QueryResult, ResearchQueryEngine, ResearchQueryService,
};
pub use rights::{
    IngestIdentity, RegisteredRightsGrant, RightsDecisionInput, RightsError, SourceOperation,
};
