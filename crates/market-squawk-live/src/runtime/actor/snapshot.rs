//! Authority-free conversion from bounded processor seeds to immutable public DTOs.

use crate::processor::ProcessorSnapshotSeed;
use crate::snapshot::SnapshotBuildError;
use crate::{
    BookLevelSnapshot, GenerationPhase, RouteSnapshot, SnapshotDimension, StatusSnapshot,
    StreamPhaseSnapshot, StreamSnapshot,
};

pub(super) fn route_from_seed(
    route: crate::ShardKey,
    seed: ProcessorSnapshotSeed,
) -> Result<RouteSnapshot, SnapshotBuildError> {
    let stream_dimension = SnapshotDimension::from_counts(
        seed.total_streams,
        seed.output_stream_count,
        seed.requested_stream_limit,
    )?;
    let status_dimension = SnapshotDimension::from_counts(
        seed.total_statuses,
        seed.output_status_count,
        seed.requested_status_limit,
    )?;
    let mut streams = Vec::new();
    streams
        .try_reserve_exact(seed.streams.len())
        .map_err(|_| SnapshotBuildError::RetainedSizeOverflow)?;
    for stream in seed.streams {
        let bid_dimension = SnapshotDimension::from_counts(
            stream.total_bid_levels,
            stream.output_bid_levels,
            stream.requested_depth,
        )?;
        let ask_dimension = SnapshotDimension::from_counts(
            stream.total_ask_levels,
            stream.output_ask_levels,
            stream.requested_depth,
        )?;
        let bids = stream
            .bids
            .into_vec()
            .into_iter()
            .map(|level| BookLevelSnapshot::new(level.price(), level.quantity()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let asks = stream
            .asks
            .into_vec()
            .into_iter()
            .map(|level| BookLevelSnapshot::new(level.price(), level.quantity()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        streams.push(StreamSnapshot {
            source: stream.key.source_id().clone(),
            venue: stream.key.venue().clone(),
            instrument: stream.key.instrument(),
            provider_product: stream.key.provider_product().clone(),
            provider_channel: stream.key.provider_channel().clone(),
            connection_generation: stream.generation,
            phase: phase_snapshot(stream.phase, stream.generation_current),
            state_revision: stream.revision,
            snapshot_origin_revision: stream.snapshot_origin_revision,
            health_epoch: stream.health_epoch,
            source_valid_until: stream
                .source_valid_until
                .ok_or(SnapshotBuildError::IncompleteStreamProvenance)?,
            source_timestamp: stream.source_timestamp,
            received_at: stream
                .received_at
                .ok_or(SnapshotBuildError::IncompleteStreamProvenance)?,
            evaluated_at: stream
                .evaluated_at
                .ok_or(SnapshotBuildError::IncompleteStreamProvenance)?,
            configured_depth: u32::try_from(stream.configured_depth)
                .map_err(|_| SnapshotBuildError::CountOverflow)?,
            state_bid_depth: stream.total_bid_levels,
            state_ask_depth: stream.total_ask_levels,
            bids,
            asks,
            bid_dimension,
            ask_dimension,
        });
    }
    let statuses = seed
        .statuses
        .into_vec()
        .into_iter()
        .map(|status| StatusSnapshot {
            source: status.source_id,
            venue: status.venue,
            instrument: status.instrument,
            connection_generation: status.generation,
            trading_status: status.status,
            status_revision: status.revision,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(RouteSnapshot {
        route,
        streams: streams.into_boxed_slice(),
        statuses,
        stream_dimension,
        status_dimension,
    })
}

const fn phase_snapshot(phase: GenerationPhase, current: bool) -> StreamPhaseSnapshot {
    if !current {
        return StreamPhaseSnapshot::Quarantined;
    }
    match phase {
        GenerationPhase::Disconnected => StreamPhaseSnapshot::Disconnected,
        GenerationPhase::AwaitingSnapshot => StreamPhaseSnapshot::AwaitingSnapshot,
        GenerationPhase::Synchronizing => StreamPhaseSnapshot::Synchronizing,
        GenerationPhase::Healthy => StreamPhaseSnapshot::Healthy,
        GenerationPhase::Quarantined => StreamPhaseSnapshot::Quarantined,
    }
}
