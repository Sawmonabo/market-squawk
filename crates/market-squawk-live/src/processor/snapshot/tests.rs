use std::collections::HashMap;
use std::error::Error;
use std::str::FromStr;

use crate::{BookLevelSnapshot, SnapshotCompleteness, StreamPhaseSnapshot};
use market_squawk_domain::{InstrumentId, QuantityLots, SequenceNumber, TradingStatus};

use super::{
    MAX_SNAPSHOT_LEVELS_PER_SIDE, MAX_SNAPSHOT_RETAINED_BYTES, MAX_SNAPSHOT_STREAMS,
    ProcessorSnapshotLimits, ProcessorSnapshotSeed, StatusBook, build_snapshot_seed, status_charge,
    stream_base_charge,
};

#[path = "tests/fixture.rs"]
mod fixture;

use fixture::{CONFIGURED_DEPTH, PopulatedState, TestResult, populated_state};

const INSTRUMENT: &str = "4c74ab95-53b9-42ad-9b66-0ed403b88fed";

#[test]
fn limits_reject_zero_over_hard_bounds_and_retained_bytes_below_base() {
    let base = std::mem::size_of::<ProcessorSnapshotSeed>();
    for limits in [
        (0, 1, 1, base),
        (1, 0, 1, base),
        (1, 1, 0, base),
        (MAX_SNAPSHOT_STREAMS + 1, 1, 1, base),
        (1, MAX_SNAPSHOT_STREAMS + 1, 1, base),
        (1, 1, MAX_SNAPSHOT_LEVELS_PER_SIDE + 1, base),
        (1, 1, 1, base - 1),
        (1, 1, 1, MAX_SNAPSHOT_RETAINED_BYTES + 1),
        (usize::MAX, usize::MAX, usize::MAX, usize::MAX),
    ] {
        assert!(
            ProcessorSnapshotLimits::try_new(limits.0, limits.1, limits.2, limits.3).is_err(),
            "invalid limits unexpectedly accepted: {limits:?}"
        );
    }
}

#[test]
fn exact_base_retained_bytes_accepts_empty_state_and_reports_exact_counts() -> TestResult {
    let base = std::mem::size_of::<ProcessorSnapshotSeed>();
    let limits = ProcessorSnapshotLimits::try_new(1, 1, 1, base)?;
    let instrument = InstrumentId::from_str(INSTRUMENT)?;
    let streams = HashMap::new();
    let statuses = StatusBook::try_new(1)?;

    let seed = build_snapshot_seed(instrument, CONFIGURED_DEPTH, &streams, &statuses, limits)?;

    assert_eq!(seed.instrument, instrument);
    assert_eq!(seed.configured_depth, CONFIGURED_DEPTH);
    assert_eq!(seed.requested_stream_limit, 1);
    assert_eq!(seed.requested_status_limit, 1);
    assert_eq!(seed.requested_levels_per_side, 1);
    assert_eq!(seed.retained_bytes, base);
    assert_eq!(seed.total_streams, 0);
    assert_eq!(seed.total_statuses, 0);
    assert_eq!(seed.output_stream_count, 0);
    assert_eq!(seed.output_status_count, 0);
    assert!(seed.streams_complete);
    assert!(seed.statuses_complete);
    assert!(seed.streams.is_empty());
    assert!(seed.statuses.is_empty());
    Ok(())
}

#[test]
fn deterministic_sort_and_count_caps_report_requested_available_and_returned_state() -> TestResult {
    let state = populated_state()?;
    let limits = ProcessorSnapshotLimits::try_new(2, 1, 2, MAX_SNAPSHOT_RETAINED_BYTES)?;

    let seed = snapshot(&state, limits)?;

    assert_eq!(seed.requested_stream_limit, 2);
    assert_eq!(seed.requested_status_limit, 1);
    assert_eq!(seed.requested_levels_per_side, 2);
    assert_eq!(seed.total_streams, 3);
    assert_eq!(seed.total_statuses, 3);
    assert_eq!(seed.output_stream_count, 2);
    assert_eq!(seed.output_status_count, 1);
    assert!(!seed.streams_complete);
    assert!(!seed.statuses_complete);
    assert_eq!(stream_sources(&seed), ["source-a", "source-m"]);
    assert_eq!(status_sources(&seed), ["source-a"]);
    for stream in &seed.streams {
        assert_eq!(stream.configured_depth, u32::try_from(CONFIGURED_DEPTH)?);
        assert_eq!(stream.bid_dimension.configured_limit(), 2);
        assert_eq!(stream.state_bid_depth, 3);
        assert_eq!(stream.state_ask_depth, 2);
        assert_eq!(stream.bid_dimension.returned(), 2);
        assert_eq!(stream.ask_dimension.returned(), 2);
        assert_eq!(
            stream.bid_dimension.completeness(),
            SnapshotCompleteness::Truncated
        );
        assert_eq!(
            stream.ask_dimension.completeness(),
            SnapshotCompleteness::Complete
        );
        assert_eq!(stream.bids.len(), 2);
        assert_eq!(stream.asks.len(), 2);
    }
    Ok(())
}

#[test]
fn complete_seed_retains_depth_book_status_and_all_provenance_dimensions() -> TestResult {
    let state = populated_state()?;
    let limits =
        ProcessorSnapshotLimits::try_new(3, 3, CONFIGURED_DEPTH, MAX_SNAPSHOT_RETAINED_BYTES)?;

    let seed = snapshot(&state, limits)?;

    assert_eq!(seed.output_stream_count, 3);
    assert_eq!(seed.output_status_count, 3);
    assert!(seed.streams_complete);
    assert!(seed.statuses_complete);
    assert_eq!(stream_sources(&seed), ["source-a", "source-m", "source-z"]);
    assert_eq!(status_sources(&seed), ["source-a", "source-m", "source-z"]);
    let stream = seed
        .streams
        .iter()
        .find(|candidate| candidate.source.as_str() == "source-a")
        .ok_or("sorted seed lost source-a")?;
    assert_eq!(stream.connection_generation.get(), 1);
    assert_eq!(stream.phase, StreamPhaseSnapshot::Healthy);
    assert_eq!(stream.state_revision, 1);
    assert_eq!(stream.last_sequence, Some(SequenceNumber::new(10)));
    assert_eq!(stream.configured_depth, u32::try_from(CONFIGURED_DEPTH)?);
    assert_eq!(
        stream.bid_dimension.configured_limit(),
        u32::try_from(CONFIGURED_DEPTH)?
    );
    assert_eq!(stream.state_bid_depth, 3);
    assert_eq!(stream.state_ask_depth, 2);
    assert_eq!(stream.bid_dimension.returned(), 3);
    assert_eq!(stream.ask_dimension.returned(), 2);
    assert_eq!(
        stream.bid_dimension.completeness(),
        SnapshotCompleteness::Complete
    );
    assert_eq!(
        stream.ask_dimension.completeness(),
        SnapshotCompleteness::Complete
    );
    assert_eq!(prices(&stream.bids), [10_000, 9_900, 9_800]);
    assert_eq!(prices(&stream.asks), [10_100, 10_200]);
    let one_lot = QuantityLots::new(1)?;
    assert!(
        stream
            .bids
            .iter()
            .chain(stream.asks.iter())
            .all(|level| level.quantity() == one_lot)
    );
    assert!(stream.snapshot_initialized);
    assert_eq!(stream.snapshot_origin_revision, Some(1));
    assert!(stream.generation_current);
    assert_eq!(stream.health_epoch, 1);
    assert_eq!(stream.source_valid_until, state.source_valid_until);
    assert_eq!(stream.source_timestamp, Some(state.source_timestamp));
    assert_eq!(stream.received_at, state.received_at);
    assert_eq!(stream.evaluated_at, state.evaluated_at);
    assert_ne!(stream.source_timestamp, Some(stream.received_at));
    assert_ne!(stream.received_at, stream.evaluated_at);
    assert_eq!(stream.trading_status, Some(TradingStatus::Halted));
    assert_eq!(stream.trading_status_revision, Some(1));
    let status = seed
        .statuses
        .iter()
        .find(|candidate| candidate.source.as_str() == "source-a")
        .ok_or("sorted status seed lost source-a")?;
    assert_eq!(status.venue.as_str(), "coinbase");
    assert_eq!(status.instrument, state.instrument);
    assert_eq!(status.connection_generation.get(), 1);
    assert_eq!(status.trading_status, TradingStatus::Halted);
    assert_eq!(status.status_revision, 1);

    let expected = exact_retained_bytes(&state)?;
    assert_eq!(seed.retained_bytes, expected);
    assert!(seed.retained_bytes <= MAX_SNAPSHOT_RETAINED_BYTES);
    Ok(())
}

#[test]
fn byte_bound_truncates_deterministically_without_exceeding_exact_limit() -> TestResult {
    let state = populated_state()?;
    let first_key = state
        .streams
        .keys()
        .find(|key| key.source_id().as_str() == "source-a")
        .ok_or("fixture lost source-a stream")?;
    let byte_limit = std::mem::size_of::<ProcessorSnapshotSeed>()
        .checked_add(stream_base_charge(first_key)?)
        .and_then(|value| value.checked_add(std::mem::size_of::<BookLevelSnapshot>()))
        .ok_or("test byte limit overflow")?;
    let limits = ProcessorSnapshotLimits::try_new(1, 3, CONFIGURED_DEPTH, byte_limit)?;

    let seed = snapshot(&state, limits)?;

    assert_eq!(seed.retained_bytes, byte_limit);
    assert!(seed.retained_bytes <= byte_limit);
    assert_eq!(seed.total_streams, 3);
    assert_eq!(seed.output_stream_count, 1);
    assert_eq!(seed.total_statuses, 3);
    assert_eq!(seed.output_status_count, 0);
    assert!(!seed.streams_complete);
    assert!(!seed.statuses_complete);
    let stream = seed.streams.first().ok_or("byte-bound seed lost stream")?;
    assert_eq!(stream.source.as_str(), "source-a");
    assert_eq!(
        stream.bid_dimension.configured_limit(),
        u32::try_from(CONFIGURED_DEPTH)?
    );
    assert_eq!(stream.state_bid_depth, 3);
    assert_eq!(stream.state_ask_depth, 2);
    assert_eq!(stream.bid_dimension.returned(), 1);
    assert_eq!(stream.ask_dimension.returned(), 0);
    assert_eq!(
        stream.bid_dimension.completeness(),
        SnapshotCompleteness::Truncated
    );
    assert_eq!(
        stream.ask_dimension.completeness(),
        SnapshotCompleteness::Unavailable
    );
    assert_eq!(prices(&stream.bids), [10_000]);
    assert!(stream.asks.is_empty());
    Ok(())
}

#[test]
fn route_finalization_reuses_stream_status_and_book_allocations() -> TestResult {
    let state = populated_state()?;
    let limits =
        ProcessorSnapshotLimits::try_new(3, 3, CONFIGURED_DEPTH, MAX_SNAPSHOT_RETAINED_BYTES)?;
    let seed = snapshot(&state, limits)?;
    let streams = seed.streams.as_ptr();
    let statuses = seed.statuses.as_ptr();
    let bids = seed
        .streams
        .first()
        .ok_or("missing direct-final stream")?
        .bids
        .as_ptr();
    let route = seed.into_route(crate::ShardKey::new(
        market_squawk_domain::VenueId::try_from("coinbase")?,
        state.instrument,
    ));

    assert_eq!(route.streams.as_ptr(), streams);
    assert_eq!(route.statuses.as_ptr(), statuses);
    assert_eq!(
        route
            .streams
            .first()
            .ok_or("missing finalized stream")?
            .bids
            .as_ptr(),
        bids
    );
    Ok(())
}

fn snapshot(
    state: &PopulatedState,
    limits: ProcessorSnapshotLimits,
) -> Result<ProcessorSnapshotSeed, crate::processor::error::LiveApplyError> {
    build_snapshot_seed(
        state.instrument,
        CONFIGURED_DEPTH,
        &state.streams,
        &state.statuses,
        limits,
    )
}

fn stream_sources(seed: &ProcessorSnapshotSeed) -> Vec<&str> {
    seed.streams
        .iter()
        .map(|stream| stream.source.as_str())
        .collect()
}

fn status_sources(seed: &ProcessorSnapshotSeed) -> Vec<&str> {
    seed.statuses
        .iter()
        .map(|status| status.source.as_str())
        .collect()
}

fn prices(levels: &[BookLevelSnapshot]) -> Vec<i64> {
    levels.iter().map(|level| level.price().get()).collect()
}

fn exact_retained_bytes(state: &PopulatedState) -> TestResult<usize> {
    let base = std::mem::size_of::<ProcessorSnapshotSeed>();
    let stream_bytes =
        state
            .streams
            .keys()
            .try_fold(0_usize, |total, key| -> TestResult<usize> {
                let levels = state
                    .streams
                    .get(key)
                    .ok_or("stream disappeared during accounting")?
                    .book()
                    .bid_level_count()
                    .checked_add(
                        state
                            .streams
                            .get(key)
                            .ok_or("stream disappeared during accounting")?
                            .book()
                            .ask_level_count(),
                    )
                    .and_then(|count| count.checked_mul(std::mem::size_of::<BookLevelSnapshot>()))
                    .ok_or("level accounting overflow")?;
                total
                    .checked_add(stream_base_charge(key)?)
                    .and_then(|value| value.checked_add(levels))
                    .ok_or_else(|| Box::<dyn Error>::from("stream accounting overflow"))
            })?;
    let status_bytes =
        state
            .statuses
            .iter()
            .try_fold(0_usize, |total, (key, ..)| -> TestResult<usize> {
                total
                    .checked_add(status_charge(key)?)
                    .ok_or_else(|| Box::<dyn Error>::from("status accounting overflow"))
            })?;
    base.checked_add(stream_bytes)
        .and_then(|value| value.checked_add(status_bytes))
        .ok_or_else(|| "snapshot accounting overflow".into())
}
