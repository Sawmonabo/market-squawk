//! Authority-free current source-runtime contracts.

use std::{fmt, time::Instant};

use async_trait::async_trait;
use market_squawk_domain::{
    CaptureIntegrityState, ConnectionGeneration, CoverageScope, CoverageStatus, DataQuality,
    InstrumentId, SourceId, SourceIdentifier, StreamIntegrityState, Timestamp,
};
use market_squawk_live::{SourceRuntimeEvidenceSnapshot, StreamSnapshot};
use market_squawk_sources::{
    ConnectionLiveness, MarketFreshness, SourceTimestampFreshness, TransportFreshness,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

const MAX_RUNTIME_SNAPSHOTS: usize = 4_096;

/// Bounded read request supplied to an authority-free runtime view.
#[derive(Clone)]
pub struct SourceRuntimeRequest {
    source_filters: Box<[SourceIdentifier]>,
    maximum_items: usize,
    cancellation: CancellationToken,
    deadline: Instant,
}

impl SourceRuntimeRequest {
    pub(super) fn new(
        source_filters: Box<[SourceIdentifier]>,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Self {
        Self {
            source_filters,
            maximum_items: MAX_RUNTIME_SNAPSHOTS,
            cancellation,
            deadline,
        }
    }

    /// Requested profile-surface or live-source identities. An empty slice means all sources.
    pub fn source_filters(&self) -> &[SourceIdentifier] {
        &self.source_filters
    }

    /// Hard maximum number of complete runtime records the producer may return.
    pub const fn maximum_items(&self) -> usize {
        self.maximum_items
    }

    /// Caller cancellation propagated without granting application lifecycle authority.
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Absolute caller deadline that a runtime view may narrow but never extend.
    pub const fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl fmt::Debug for SourceRuntimeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SourceRuntimeRequest")
            .field("source_filter_count", &self.source_filters.len())
            .field("maximum_items", &self.maximum_items)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// One authority-free runtime record reconstructed from source-health and qualification evidence.
#[derive(Clone, Debug)]
pub struct SourceRuntimeSnapshot {
    pub(super) surface_id: SourceIdentifier,
    pub(super) source_id: SourceId,
    pub(super) instrument_id: InstrumentId,
    pub(super) connection_generation: ConnectionGeneration,
    pub(super) session_id: SourceIdentifier,
    pub(super) health_epoch: u64,
    pub(super) state_revision: u64,
    pub(super) assessment_id: SourceIdentifier,
    pub(super) binding_digest: [u8; 32],
    pub(super) connection: ConnectionLiveness,
    pub(super) transport_freshness: TransportFreshness,
    pub(super) market_freshness: MarketFreshness,
    pub(super) source_freshness: SourceTimestampFreshness,
    pub(super) stream_integrity: StreamIntegrityState,
    pub(super) capture_integrity: CaptureIntegrityState,
    pub(super) coverage_scope: CoverageScope,
    pub(super) coverage_status: CoverageStatus,
    pub(super) quality: DataQuality,
    pub(super) observed_at: Timestamp,
    pub(super) qualification_evaluated_at: Timestamp,
    pub(super) qualification_valid_until: Timestamp,
}

impl SourceRuntimeSnapshot {
    pub(crate) fn try_from_live_evidence(
        surface_id: SourceIdentifier,
        stream: &StreamSnapshot,
        evidence: &SourceRuntimeEvidenceSnapshot,
    ) -> Result<Self, SourceRuntimeSnapshotError> {
        if !evidence.matches_stream(stream) {
            return Err(SourceRuntimeSnapshotError::EvidenceMismatch);
        }
        Ok(Self {
            surface_id,
            source_id: stream.source().clone(),
            instrument_id: stream.instrument(),
            connection_generation: stream.connection_generation(),
            session_id: evidence.session_id().clone(),
            health_epoch: evidence.health_epoch(),
            state_revision: evidence.state_revision(),
            assessment_id: evidence.assessment_id().as_source_identifier().clone(),
            binding_digest: evidence.binding_digest(),
            connection: evidence.connection(),
            transport_freshness: evidence.transport_freshness(),
            market_freshness: evidence.market_freshness(),
            source_freshness: evidence.source_freshness(),
            stream_integrity: evidence.stream_integrity(),
            capture_integrity: evidence.capture_integrity(),
            coverage_scope: evidence.coverage_scope().clone(),
            coverage_status: evidence.coverage_status(),
            quality: evidence.quality(),
            observed_at: evidence.health_observed_at(),
            qualification_evaluated_at: evidence.qualification_evaluated_at(),
            qualification_valid_until: evidence.qualification_valid_until(),
        })
    }

    /// Code-owned provider surface to which composition bound this runtime.
    pub const fn surface_id(&self) -> &SourceIdentifier {
        &self.surface_id
    }

    /// Exact live source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) fn sort_key(&self) -> (&str, &str, &str, InstrumentId, &str, &str) {
        (
            self.surface_id.as_str(),
            self.source_id.as_str(),
            self.coverage_scope.venue_id().as_str(),
            self.instrument_id,
            self.coverage_scope
                .provider_product()
                .as_source_identifier()
                .as_str(),
            self.coverage_scope
                .provider_channel()
                .as_source_identifier()
                .as_str(),
        )
    }
}

/// Invalid authority-free runtime snapshot construction.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SourceRuntimeSnapshotError {
    /// Health and qualification evidence do not bind the same exact source runtime.
    #[error("source runtime evidence does not share one exact binding")]
    EvidenceMismatch,
}

/// Complete bounded runtime view. It never carries execution, source-session, or secret authority.
#[derive(Clone, Debug)]
pub struct SourceRuntimeSnapshotBatch {
    records: Box<[SourceRuntimeSnapshot]>,
}

impl SourceRuntimeSnapshotBatch {
    /// Retains one complete bounded runtime view.
    ///
    /// # Errors
    ///
    /// Returns [`SourceRuntimeViewError::ResourceExhausted`] above the code-owned source ceiling.
    pub fn try_new(records: Vec<SourceRuntimeSnapshot>) -> Result<Self, SourceRuntimeViewError> {
        if records.len() > MAX_RUNTIME_SNAPSHOTS {
            return Err(SourceRuntimeViewError::ResourceExhausted);
        }
        Ok(Self {
            records: records.into_boxed_slice(),
        })
    }

    /// Complete records returned by the current runtime.
    pub fn records(&self) -> &[SourceRuntimeSnapshot] {
        &self.records
    }

    pub(super) fn into_records(self) -> Vec<SourceRuntimeSnapshot> {
        self.records.into_vec()
    }
}

/// Stable runtime-view failure without provider payload or authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum SourceRuntimeViewError {
    /// Caller cancellation won the runtime-view race.
    #[error("source runtime view was cancelled")]
    Cancelled,
    /// Caller deadline elapsed.
    #[error("source runtime view deadline elapsed")]
    DeadlineExceeded,
    /// The complete runtime view exceeded its hard bound.
    #[error("source runtime view exceeded its resource bound")]
    ResourceExhausted,
    /// The current runtime view is temporarily unavailable.
    #[error("source runtime view is unavailable")]
    Unavailable,
    /// The producer returned contradictory authority-free facts.
    #[error("source runtime view is invalid")]
    InvalidSnapshot,
}

/// Least-authority read seam implemented by the application-owned live runtime.
#[async_trait]
pub trait SourceRuntimeView: Send + Sync + 'static {
    /// Returns a complete bounded current view without transferring runtime-control authority.
    async fn current(
        &self,
        request: SourceRuntimeRequest,
    ) -> Result<SourceRuntimeSnapshotBatch, SourceRuntimeViewError>;
}
