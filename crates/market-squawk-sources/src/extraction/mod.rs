//! Bounded discovery and research extraction contracts.

mod batch;
mod contracts;

use futures_util::future::BoxFuture;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::{SourceError, SourceMetadataProvider};

pub use batch::ExtractionBatch;
pub use contracts::{
    AvailabilityEvidence, DiscoveryBatch, DiscoveryRequest, DiscoveryRequestId, ExtractionError,
    ExtractionRecord, ExtractionRequest, ExtractionRequestId, MAX_DISCOVERY_OBJECTS,
    MAX_EXTRACTION_BATCH_BYTES, MAX_EXTRACTION_RECORD_BYTES, MAX_EXTRACTION_RECORDS,
    MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, SourceObject,
};

/// Object-safe research extraction contract with one boxed future per request.
pub trait ExtractionSource: SourceMetadataProvider {
    /// Discovers a bounded set of versioned source objects.
    fn discover(
        &self,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>>;

    /// Extracts one source object into a bounded normalized batch.
    fn extract(
        &self,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>>;
}

/// Adapter-facing extraction failure preserving transport and contract classes.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ExtractionSourceError {
    /// Source transport/lifecycle failure.
    #[error("source extraction transport failed: {0}")]
    Source(#[from] SourceError),
    /// Bounded extraction contract failure.
    #[error("source extraction contract failed: {0}")]
    Contract(#[from] ExtractionError),
    /// Request deadline elapsed.
    #[error("source extraction deadline elapsed")]
    DeadlineExceeded,
    /// Cancellation was requested.
    #[error("source extraction was cancelled")]
    Cancelled,
}
