//! Durable local catalog, source-rights admission, and recovery metadata.
//!
//! This crate is a control- and research-plane boundary. It is never queried from the live
//! event-to-action path.

mod catalog;
mod migrations;
mod rights;

pub use catalog::{
    ArtifactRecord, AuditEvent, BackupReceipt, Catalog, CatalogAuthority, CatalogConfig,
    CatalogError, CatalogHealth, CatalogLimit, CatalogResultLimits, ContractCompletion,
    DatasetManifestRecord, IngestReservation, IngestRunRecord, IngestRunState, PublishedIngest,
    ReferenceBundle, ResumedIngest, SourceCursor,
};
pub use rights::{
    IngestIdentity, RegisteredRightsGrant, RightsDecisionInput, RightsError, SourceOperation,
};
