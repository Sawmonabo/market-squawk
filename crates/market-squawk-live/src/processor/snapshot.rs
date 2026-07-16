//! Explicitly bounded immutable diagnostic seed for Task 8 snapshot publication.

use market_squawk_domain::{
    BookLevel, ConnectionGeneration, InstrumentId, SequenceNumber, SourceId, Timestamp,
    TradingStatus, VenueId,
};
use market_squawk_sources::CurrentStreamKey;

use super::status::StatusBook;
use super::{LiveApplyError, StreamState};
use crate::GenerationPhase;

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

/// Complete bounded diagnostic state for one instrument owner.
#[derive(Debug)]
pub(crate) struct ProcessorSnapshotSeed {
    pub(crate) instrument: InstrumentId,
    pub(crate) configured_depth: usize,
    pub(crate) requested_stream_limit: usize,
    pub(crate) requested_status_limit: usize,
    pub(crate) requested_levels_per_side: usize,
    pub(crate) output_stream_count: usize,
    pub(crate) output_status_count: usize,
    pub(crate) retained_bytes: usize,
    pub(crate) total_streams: usize,
    pub(crate) total_statuses: usize,
    pub(crate) streams_complete: bool,
    pub(crate) statuses_complete: bool,
    pub(crate) streams: Box<[StreamSnapshotSeed]>,
    pub(crate) statuses: Box<[StatusSnapshotSeed]>,
}

/// One source/product/channel image, including quarantined and incomplete states.
#[derive(Debug)]
pub(crate) struct StreamSnapshotSeed {
    pub(crate) key: CurrentStreamKey,
    pub(crate) generation: ConnectionGeneration,
    pub(crate) phase: GenerationPhase,
    pub(crate) revision: u64,
    pub(crate) last_sequence: Option<SequenceNumber>,
    pub(crate) configured_depth: usize,
    pub(crate) requested_depth: usize,
    pub(crate) total_bid_levels: usize,
    pub(crate) total_ask_levels: usize,
    pub(crate) output_bid_levels: usize,
    pub(crate) output_ask_levels: usize,
    pub(crate) bids_complete: bool,
    pub(crate) asks_complete: bool,
    pub(crate) bids: Box<[BookLevel]>,
    pub(crate) asks: Box<[BookLevel]>,
    pub(crate) snapshot_initialized: bool,
    pub(crate) snapshot_origin_revision: Option<u64>,
    pub(crate) generation_current: bool,
    pub(crate) health_epoch: u64,
    pub(crate) source_valid_until: Option<Timestamp>,
    pub(crate) source_timestamp: Option<Timestamp>,
    pub(crate) received_at: Option<Timestamp>,
    pub(crate) evaluated_at: Option<Timestamp>,
    pub(crate) trading_status: Option<TradingStatus>,
    /// Monotonic allocation version of the shared status authority.
    ///
    /// This is intentionally distinct from the allocation-local revision lease used to validate
    /// an execution capability.
    pub(crate) trading_status_revision: Option<u64>,
}

/// Cross-channel source/venue/instrument status image.
#[derive(Debug)]
pub(crate) struct StatusSnapshotSeed {
    pub(crate) source_id: SourceId,
    pub(crate) venue: VenueId,
    pub(crate) instrument: InstrumentId,
    pub(crate) generation: ConnectionGeneration,
    pub(crate) status: TradingStatus,
    /// Monotonic allocation version, suitable for ordering diagnostic status publications.
    pub(crate) revision: u64,
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
    let mut stream_seeds = Vec::new();
    stream_seeds
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
        let level_size = std::mem::size_of::<BookLevel>();
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
        let (trading_status, trading_status_revision) = statuses
            .status_for_stream(key)
            .map_or((None, None), |(status, revision)| {
                (Some(status), Some(revision))
            });
        stream_seeds.push(StreamSnapshotSeed {
            key: key.clone(),
            generation: state.connection_generation(),
            phase: state.phase(),
            revision: state.revision(),
            last_sequence: state.sequence().last_sequence(),
            configured_depth,
            requested_depth: limits.max_levels_per_side,
            total_bid_levels: total_bids,
            total_ask_levels: total_asks,
            output_bid_levels: bid_count,
            output_ask_levels: ask_count,
            bids_complete: bid_count == total_bids,
            asks_complete: ask_count == total_asks,
            bids: state
                .book()
                .bid_levels_limited(bid_count)?
                .into_boxed_slice(),
            asks: state
                .book()
                .ask_levels_limited(ask_count)?
                .into_boxed_slice(),
            snapshot_initialized: state.snapshot_origin().is_some(),
            snapshot_origin_revision: state.snapshot_origin().map(|origin| origin.state_revision),
            generation_current: state.generation_lease().validate().is_ok(),
            health_epoch: state.health_epoch(),
            source_valid_until: state.source_valid_until(),
            source_timestamp: state.source_timestamp(),
            received_at: state.received_at(),
            evaluated_at: state.evaluated_at(),
            trading_status,
            trading_status_revision,
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
    let mut status_seeds = Vec::new();
    status_seeds
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
        status_seeds.push(StatusSnapshotSeed {
            source_id: key.source_id().clone(),
            venue: key.venue().clone(),
            instrument: key.instrument(),
            generation,
            status,
            revision,
        });
    }
    Ok(ProcessorSnapshotSeed {
        instrument,
        configured_depth,
        requested_stream_limit: limits.max_streams,
        requested_status_limit: limits.max_statuses,
        requested_levels_per_side: limits.max_levels_per_side,
        output_stream_count: stream_seeds.len(),
        output_status_count: status_seeds.len(),
        retained_bytes,
        total_streams: streams.len(),
        total_statuses,
        streams_complete: stream_seeds.len() == streams.len(),
        statuses_complete: status_seeds.len() == total_statuses,
        streams: stream_seeds.into_boxed_slice(),
        statuses: status_seeds.into_boxed_slice(),
    })
}

fn stream_base_charge(_key: &CurrentStreamKey) -> Result<usize, LiveApplyError> {
    std::mem::size_of::<StreamSnapshotSeed>()
        .checked_add(market_squawk_domain::SourceId::MAX_LENGTH)
        .and_then(|value| value.checked_add(market_squawk_domain::VenueId::MAX_LENGTH))
        .and_then(|value| value.checked_add(market_squawk_domain::SourceIdentifier::MAX_LENGTH))
        .and_then(|value| value.checked_add(market_squawk_domain::SourceIdentifier::MAX_LENGTH))
        .ok_or(LiveApplyError::SnapshotRetainedSizeOverflow)
}

fn status_charge(_key: &super::status::StatusKey) -> Result<usize, LiveApplyError> {
    std::mem::size_of::<StatusSnapshotSeed>()
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

#[cfg(test)]
#[path = "snapshot/tests.rs"]
mod tests;
