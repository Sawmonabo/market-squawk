//! Authority-free Source-domain projection of the paper runtime's live state.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_domain::SourceIdentifier;
use market_squawk_live::{ShardLifecycleSnapshot, SnapshotCompleteness, SnapshotReadError};
use market_squawk_services::ServiceError;

use super::{PaperController, PaperProvider, PaperState, bounded_lock};
use crate::{
    ProductionSourceProvider,
    application::source::{
        SourceRuntimeRequest, SourceRuntimeSnapshot, SourceRuntimeSnapshotBatch, SourceRuntimeView,
        SourceRuntimeViewError,
    },
};

const COINBASE_SURFACE_ID: &str = "coinbase.public-market-data";
const COINBASE_DIRECT_SURFACE_ID: &str = "coinbase.exchange-direct-market-data";
const KRAKEN_SURFACE_ID: &str = "kraken.spot-public-market-data";

/// Read-only view sharing the paper controller's sole live-runtime owner.
#[derive(Debug)]
pub(super) struct PaperSourceRuntimeView {
    controller: Arc<PaperController>,
}

impl PaperSourceRuntimeView {
    pub(super) const fn new(controller: Arc<PaperController>) -> Self {
        Self { controller }
    }
}

#[async_trait]
impl SourceRuntimeView for PaperSourceRuntimeView {
    async fn current(
        &self,
        request: SourceRuntimeRequest,
    ) -> Result<SourceRuntimeSnapshotBatch, SourceRuntimeViewError> {
        ensure_request_live(&request)?;
        let active = {
            let state = bounded_lock(
                &self.controller.state,
                request.deadline(),
                request.cancellation(),
            )
            .await
            .map_err(map_lock_error)?;
            match &*state {
                PaperState::Stopped { .. } => None,
                PaperState::Starting { .. } | PaperState::Stopping => {
                    return Err(SourceRuntimeViewError::Unavailable);
                }
                PaperState::Running {
                    provider,
                    runtime,
                    exports,
                    cancellation,
                    ..
                } => {
                    if cancellation.is_cancelled()
                        || !runtime.source_is_healthy()
                        || !exports.is_healthy()
                    {
                        return Err(SourceRuntimeViewError::Unavailable);
                    }
                    Some((*provider, runtime.snapshots()))
                }
            }
        };
        let Some((provider, reader)) = active else {
            return SourceRuntimeSnapshotBatch::try_new(Vec::new());
        };

        ensure_request_live(&request)?;
        let lease = reader.try_load_all().map_err(map_snapshot_error)?;
        let surface_id = surface_id(provider)?;
        let mut records = Vec::new();
        for shard in lease.snapshots() {
            ensure_request_live(&request)?;
            if shard.route_dimension().completeness() != SnapshotCompleteness::Complete
                || !matches!(
                    shard.lifecycle(),
                    ShardLifecycleSnapshot::Ready | ShardLifecycleSnapshot::Degraded
                )
            {
                return Err(SourceRuntimeViewError::Unavailable);
            }
            for route in shard.routes() {
                if route.stream_dimension().completeness() != SnapshotCompleteness::Complete {
                    return Err(SourceRuntimeViewError::Unavailable);
                }
                for stream in route.streams() {
                    let evidence = stream
                        .runtime_evidence()
                        .ok_or(SourceRuntimeViewError::Unavailable)?;
                    if !evidence.matches_stream(stream) {
                        return Err(SourceRuntimeViewError::InvalidSnapshot);
                    }
                    if records.len() == request.maximum_items() {
                        return Err(SourceRuntimeViewError::ResourceExhausted);
                    }
                    records
                        .try_reserve(1)
                        .map_err(|_error| SourceRuntimeViewError::ResourceExhausted)?;
                    records.push(SourceRuntimeSnapshot::try_from_live_evidence(
                        surface_id.clone(),
                        stream,
                        evidence,
                    )?);
                }
            }
        }
        if records.is_empty() {
            return Err(SourceRuntimeViewError::Unavailable);
        }

        records.sort_unstable_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        if !request.source_filters().is_empty() {
            records.retain(|record| matches_filter(record, request.source_filters()));
        }
        ensure_request_live(&request)?;
        SourceRuntimeSnapshotBatch::try_new(records)
    }
}

fn matches_filter(record: &SourceRuntimeSnapshot, filters: &[SourceIdentifier]) -> bool {
    filters.iter().any(|filter| {
        filter == record.surface_id() || filter.as_str() == record.source_id().as_str()
    })
}

fn surface_id(provider: PaperProvider) -> Result<SourceIdentifier, SourceRuntimeViewError> {
    SourceIdentifier::try_from(match provider {
        PaperProvider::Public(ProductionSourceProvider::Coinbase) => COINBASE_SURFACE_ID,
        PaperProvider::Public(ProductionSourceProvider::Kraken) => KRAKEN_SURFACE_ID,
        PaperProvider::CoinbaseDirect { .. } => COINBASE_DIRECT_SURFACE_ID,
    })
    .map_err(|_error| SourceRuntimeViewError::InvalidSnapshot)
}

fn ensure_request_live(request: &SourceRuntimeRequest) -> Result<(), SourceRuntimeViewError> {
    if request.cancellation().is_cancelled() {
        Err(SourceRuntimeViewError::Cancelled)
    } else if Instant::now() >= request.deadline() {
        Err(SourceRuntimeViewError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

const fn map_lock_error(error: ServiceError) -> SourceRuntimeViewError {
    match error {
        ServiceError::Cancelled => SourceRuntimeViewError::Cancelled,
        ServiceError::DeadlineExceeded => SourceRuntimeViewError::DeadlineExceeded,
        ServiceError::ResourceExhausted => SourceRuntimeViewError::ResourceExhausted,
        ServiceError::InvalidResult => SourceRuntimeViewError::InvalidSnapshot,
        ServiceError::InvalidRequest
        | ServiceError::NotFound
        | ServiceError::Unauthorized
        | ServiceError::Unavailable
        | ServiceError::Internal => SourceRuntimeViewError::Unavailable,
    }
}

const fn map_snapshot_error(error: SnapshotReadError) -> SourceRuntimeViewError {
    match error {
        SnapshotReadError::ReaderLimitReached | SnapshotReadError::CapacityOverflow => {
            SourceRuntimeViewError::ResourceExhausted
        }
        SnapshotReadError::UnknownShard | SnapshotReadError::Closed => {
            SourceRuntimeViewError::Unavailable
        }
    }
}

impl From<crate::application::source::SourceRuntimeSnapshotError> for SourceRuntimeViewError {
    fn from(_error: crate::application::source::SourceRuntimeSnapshotError) -> Self {
        Self::InvalidSnapshot
    }
}
