//! Authority-free Source-domain projection of every active market runtime.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_domain::SourceIdentifier;
use market_squawk_live::{ShardLifecycleSnapshot, SnapshotCompleteness};

use super::super::market_runtime::MarketRuntimeRegistry;
use crate::application::source::{
    SourceRuntimeRequest, SourceRuntimeSnapshot, SourceRuntimeSnapshotBatch, SourceRuntimeView,
    SourceRuntimeViewError,
};

/// Read-only view over the bounded multi-provider market registry.
#[derive(Debug)]
pub(super) struct MarketSourceRuntimeView {
    registry: Arc<MarketRuntimeRegistry>,
}

impl MarketSourceRuntimeView {
    pub(super) const fn new(registry: Arc<MarketRuntimeRegistry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl SourceRuntimeView for MarketSourceRuntimeView {
    async fn current(
        &self,
        request: SourceRuntimeRequest,
    ) -> Result<SourceRuntimeSnapshotBatch, SourceRuntimeViewError> {
        ensure_request_live(&request)?;
        let snapshots = self
            .registry
            .snapshots(request.deadline(), request.cancellation())
            .await
            .map_err(map_service_error)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(request.maximum_items())
            .map_err(|_error| SourceRuntimeViewError::ResourceExhausted)?;
        for source in snapshots.sources() {
            for shard in source.lease().snapshots() {
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
                        if !request.source_filters().is_empty()
                            && !matches_filter(
                                source.surface_id(),
                                stream.source(),
                                request.source_filters(),
                            )
                        {
                            continue;
                        }
                        if records.len() == request.maximum_items() {
                            return Err(SourceRuntimeViewError::ResourceExhausted);
                        }
                        records
                            .try_reserve(1)
                            .map_err(|_error| SourceRuntimeViewError::ResourceExhausted)?;
                        records.push(SourceRuntimeSnapshot::try_from_live_evidence(
                            source.surface_id().clone(),
                            stream,
                            evidence,
                        )?);
                    }
                }
            }
        }
        records.sort_unstable_by(|left, right| left.sort_key().cmp(&right.sort_key()));
        ensure_request_live(&request)?;
        SourceRuntimeSnapshotBatch::try_new(records)
    }
}

fn matches_filter(
    surface_id: &SourceIdentifier,
    source_id: &market_squawk_domain::SourceId,
    filters: &[SourceIdentifier],
) -> bool {
    filters
        .iter()
        .any(|filter| filter == surface_id || filter.as_str() == source_id.as_str())
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

const fn map_service_error(error: market_squawk_services::ServiceError) -> SourceRuntimeViewError {
    match error {
        market_squawk_services::ServiceError::Cancelled => SourceRuntimeViewError::Cancelled,
        market_squawk_services::ServiceError::DeadlineExceeded => {
            SourceRuntimeViewError::DeadlineExceeded
        }
        market_squawk_services::ServiceError::ResourceExhausted => {
            SourceRuntimeViewError::ResourceExhausted
        }
        market_squawk_services::ServiceError::InvalidResult => {
            SourceRuntimeViewError::InvalidSnapshot
        }
        market_squawk_services::ServiceError::InvalidRequest
        | market_squawk_services::ServiceError::NotFound
        | market_squawk_services::ServiceError::Unauthorized
        | market_squawk_services::ServiceError::Unavailable
        | market_squawk_services::ServiceError::Internal => SourceRuntimeViewError::Unavailable,
    }
}

impl From<crate::application::source::SourceRuntimeSnapshotError> for SourceRuntimeViewError {
    fn from(_error: crate::application::source::SourceRuntimeSnapshotError) -> Self {
        Self::InvalidSnapshot
    }
}
