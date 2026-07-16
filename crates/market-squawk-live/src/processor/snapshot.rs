//! Explicitly bounded direct-final diagnostic construction for Task 8 publication.

use market_squawk_domain::InstrumentId;
use market_squawk_sources::CurrentStreamKey;

use super::status::StatusBook;
use super::{LiveApplyError, StreamState};
use crate::snapshot::SnapshotBuildError;
use crate::{
    BookLevelSnapshot, GenerationPhase, RouteSnapshot, SnapshotDimension, StatusSnapshot,
    StreamPhaseSnapshot, StreamSnapshot,
};

const MAX_SNAPSHOT_STREAMS: usize = 64;
const MAX_SNAPSHOT_LEVELS_PER_SIDE: usize = 10_000;
const MAX_SNAPSHOT_RETAINED_BYTES: usize = 64 * 1024 * 1024;

/// Caller-selected hard output limits. All dimensions are nonzero and locally bounded.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessorSnapshotLimits {
    max_streams: usize,
    max_statuses: usize,
    max_levels_per_side: usize,
    max_retained_bytes: usize,
}

impl ProcessorSnapshotLimits {
    pub(crate) fn try_new(
        max_streams: usize,
        max_statuses: usize,
        max_levels_per_side: usize,
        max_retained_bytes: usize,
    ) -> Result<Self, LiveApplyError> {
        if max_streams == 0
            || max_streams > MAX_SNAPSHOT_STREAMS
            || max_statuses == 0
            || max_statuses > MAX_SNAPSHOT_STREAMS
            || max_levels_per_side == 0
            || max_levels_per_side > MAX_SNAPSHOT_LEVELS_PER_SIDE
            || max_retained_bytes < std::mem::size_of::<ProcessorSnapshotSeed>()
            || max_retained_bytes > MAX_SNAPSHOT_RETAINED_BYTES
        {
            return Err(LiveApplyError::InvalidSnapshotLimits);
        }
        Ok(Self {
            max_streams,
            max_statuses,
            max_levels_per_side,
            max_retained_bytes,
        })
    }
}

/// One bounded route publication already stored in its final public DTO representation.
///
/// The wrapper carries retained-byte accounting until the actor adds the route identity. Moving it
/// into [`RouteSnapshot`] reuses every stream, status, and book-level allocation.
#[derive(Debug)]
pub(crate) struct ProcessorSnapshotSeed {
    pub(crate) instrument: InstrumentId,
    pub(crate) configured_depth: usize,
    pub(crate) requested_stream_limit: usize,
    pub(crate) requested_status_limit: usize,
    pub(crate) requested_levels_per_side: usize,
    pub(crate) output_stream_count: usize,
    pub(crate) output_status_count: usize,
    pub(crate) total_streams: usize,
    pub(crate) total_statuses: usize,
    pub(crate) streams_complete: bool,
    pub(crate) statuses_complete: bool,
    pub(crate) retained_bytes: usize,
    pub(crate) stream_dimension: SnapshotDimension,
    pub(crate) status_dimension: SnapshotDimension,
    pub(crate) streams: Box<[StreamSnapshot]>,
    pub(crate) statuses: Box<[StatusSnapshot]>,
}

pub(crate) type StreamSnapshotSeed = StreamSnapshot;
pub(crate) type StatusSnapshotSeed = StatusSnapshot;

impl ProcessorSnapshotSeed {
    pub(crate) fn into_route(self, route: crate::ShardKey) -> RouteSnapshot {
        RouteSnapshot {
            route,
            streams: self.streams,
            statuses: self.statuses,
            stream_dimension: self.stream_dimension,
            status_dimension: self.status_dimension,
        }
    }
}

pub(super) fn build_snapshot_seed(
    instrument: InstrumentId,
    configured_depth: usize,
    streams: &std::collections::HashMap<CurrentStreamKey, StreamState>,
    statuses: &StatusBook,
    limits: ProcessorSnapshotLimits,
) -> Result<ProcessorSnapshotSeed, LiveApplyError> {
    let mut retained_bytes = std::mem::size_of::<ProcessorSnapshotSeed>();
    let mut ordered_streams = streams.iter().collect::<Vec<_>>();
    ordered_streams.sort_by(|(left, _), (right, _)| compare_stream_keys(left, right));
    let mut stream_snapshots = Vec::new();
    stream_snapshots
        .try_reserve(limits.max_streams.min(streams.len()))
        .map_err(|_| LiveApplyError::Allocation)?;
    for (key, state) in ordered_streams.into_iter().take(limits.max_streams) {
        let total_bids = state.book().bid_level_count();
        let total_asks = state.book().ask_level_count();
        let mut bid_count = total_bids.min(limits.max_levels_per_side);
        let mut ask_count = total_asks.min(limits.max_levels_per_side);
        let base_charge = stream_base_charge(key)?;
        let Some(available) = limits
            .max_retained_bytes
            .checked_sub(retained_bytes)
            .and_then(|value| value.checked_sub(base_charge))
        else {
            break;
        };
        let level_size = std::mem::size_of::<BookLevelSnapshot>();
        let level_budget = available / level_size.max(1);
        if bid_count.saturating_add(ask_count) > level_budget {
            ask_count = ask_count.min(level_budget.saturating_sub(bid_count));
            bid_count = bid_count.min(level_budget.saturating_sub(ask_count));
        }
        let level_charge = bid_count
            .checked_add(ask_count)
            .and_then(|count| count.checked_mul(level_size))
            .ok_or(LiveApplyError::SnapshotRetainedSizeOverflow)?;
        retained_bytes = retained_bytes
            .checked_add(base_charge)
            .and_then(|value| value.checked_add(level_charge))
            .ok_or(LiveApplyError::SnapshotRetainedSizeOverflow)?;
        let bids = state
            .book()
            .bid_levels_limited(bid_count)?
            .into_iter()
            .map(|level| BookLevelSnapshot::new(level.price(), level.quantity()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let asks = state
            .book()
            .ask_levels_limited(ask_count)?
            .into_iter()
            .map(|level| BookLevelSnapshot::new(level.price(), level.quantity()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let (trading_status, trading_status_revision) = statuses
            .status_for_stream(key)
            .map_or((None, None), |(status, revision)| {
                (Some(status), Some(revision))
            });
        let generation_current = state.generation_lease().validate().is_ok();
        stream_snapshots.push(StreamSnapshot {
            source: key.source_id().clone(),
            venue: key.venue().clone(),
            instrument: key.instrument(),
            provider_product: key.provider_product().clone(),
            provider_channel: key.provider_channel().clone(),
            connection_generation: state.connection_generation(),
            phase: phase_snapshot(state.phase(), generation_current),
            state_revision: state.revision(),
            last_sequence: state.sequence().last_sequence(),
            snapshot_origin_revision: state.snapshot_origin().map(|origin| origin.state_revision),
            snapshot_initialized: state.snapshot_origin().is_some(),
            generation_current,
            health_epoch: state.health_epoch(),
            source_valid_until: state
                .source_valid_until()
                .ok_or(SnapshotBuildError::IncompleteStreamProvenance)?,
            source_timestamp: state.source_timestamp(),
            received_at: state
                .received_at()
                .ok_or(SnapshotBuildError::IncompleteStreamProvenance)?,
            evaluated_at: state
                .evaluated_at()
                .ok_or(SnapshotBuildError::IncompleteStreamProvenance)?,
            trading_status,
            trading_status_revision,
            configured_depth: u32::try_from(configured_depth)
                .map_err(|_| SnapshotBuildError::CountOverflow)?,
            state_bid_depth: total_bids,
            state_ask_depth: total_asks,
            bids,
            asks,
            bid_dimension: SnapshotDimension::from_counts(
                total_bids,
                bid_count,
                limits.max_levels_per_side,
            )?,
            ask_dimension: SnapshotDimension::from_counts(
                total_asks,
                ask_count,
                limits.max_levels_per_side,
            )?,
        });
    }

    let mut ordered_statuses = statuses.iter().collect::<Vec<_>>();
    ordered_statuses.sort_by(|(left, ..), (right, ..)| {
        left.source_id()
            .as_str()
            .cmp(right.source_id().as_str())
            .then_with(|| left.venue().as_str().cmp(right.venue().as_str()))
            .then_with(|| left.instrument().cmp(&right.instrument()))
    });
    let total_statuses = ordered_statuses.len();
    let mut status_snapshots = Vec::new();
    status_snapshots
        .try_reserve(limits.max_statuses.min(total_statuses))
        .map_err(|_| LiveApplyError::Allocation)?;
    for (key, generation, status, revision) in
        ordered_statuses.into_iter().take(limits.max_statuses)
    {
        let charge = status_charge(key)?;
        if retained_bytes
            .checked_add(charge)
            .is_none_or(|value| value > limits.max_retained_bytes)
        {
            break;
        }
        retained_bytes = retained_bytes
            .checked_add(charge)
            .ok_or(LiveApplyError::SnapshotRetainedSizeOverflow)?;
        status_snapshots.push(StatusSnapshot {
            source: key.source_id().clone(),
            venue: key.venue().clone(),
            instrument: key.instrument(),
            connection_generation: generation,
            trading_status: status,
            status_revision: revision,
        });
    }
    let output_streams = stream_snapshots.len();
    let output_statuses = status_snapshots.len();
    Ok(ProcessorSnapshotSeed {
        instrument,
        configured_depth,
        requested_stream_limit: limits.max_streams,
        requested_status_limit: limits.max_statuses,
        requested_levels_per_side: limits.max_levels_per_side,
        output_stream_count: output_streams,
        output_status_count: output_statuses,
        total_streams: streams.len(),
        total_statuses,
        streams_complete: output_streams == streams.len(),
        statuses_complete: output_statuses == total_statuses,
        retained_bytes,
        stream_dimension: SnapshotDimension::from_counts(
            streams.len(),
            output_streams,
            limits.max_streams,
        )?,
        status_dimension: SnapshotDimension::from_counts(
            total_statuses,
            output_statuses,
            limits.max_statuses,
        )?,
        streams: stream_snapshots.into_boxed_slice(),
        statuses: status_snapshots.into_boxed_slice(),
    })
}

fn stream_base_charge(_key: &CurrentStreamKey) -> Result<usize, LiveApplyError> {
    std::mem::size_of::<StreamSnapshot>()
        .checked_add(market_squawk_domain::SourceId::MAX_LENGTH)
        .and_then(|value| value.checked_add(market_squawk_domain::VenueId::MAX_LENGTH))
        .and_then(|value| value.checked_add(market_squawk_domain::SourceIdentifier::MAX_LENGTH))
        .and_then(|value| value.checked_add(market_squawk_domain::SourceIdentifier::MAX_LENGTH))
        .ok_or(LiveApplyError::SnapshotRetainedSizeOverflow)
}

fn status_charge(_key: &super::status::StatusKey) -> Result<usize, LiveApplyError> {
    std::mem::size_of::<StatusSnapshot>()
        .checked_add(market_squawk_domain::SourceId::MAX_LENGTH)
        .and_then(|value| value.checked_add(market_squawk_domain::VenueId::MAX_LENGTH))
        .ok_or(LiveApplyError::SnapshotRetainedSizeOverflow)
}

fn compare_stream_keys(left: &CurrentStreamKey, right: &CurrentStreamKey) -> std::cmp::Ordering {
    left.source_id()
        .as_str()
        .cmp(right.source_id().as_str())
        .then_with(|| left.venue().as_str().cmp(right.venue().as_str()))
        .then_with(|| left.instrument().cmp(&right.instrument()))
        .then_with(|| {
            left.provider_product()
                .as_source_identifier()
                .as_str()
                .cmp(right.provider_product().as_source_identifier().as_str())
        })
        .then_with(|| {
            left.provider_channel()
                .as_source_identifier()
                .as_str()
                .cmp(right.provider_channel().as_source_identifier().as_str())
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

#[cfg(test)]
#[path = "snapshot/tests.rs"]
mod tests;
