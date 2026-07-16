use std::str::FromStr;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, Currency, Denomination, InstrumentDefinition, InstrumentDefinitionInput,
    InstrumentId, LotSize, TickSize, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use rust_decimal::Decimal;

use super::{
    ACTOR_FIXED_BYTES, BOOK_LEVEL_BYTES, CHANNEL_COMMAND_SLOT_BYTES, CONTROL_SLOT_BYTES,
    HEALTH_EVENT_BYTES, NONCE_SLOT_BYTES, ROUTE_FIXED_BYTES, SNAPSHOT_NOTIFICATION_BYTES,
    SOURCE_STREAM_BYTES, add, estimate_peak_bytes, multiply,
};
use crate::runtime::{
    LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigError,
    LiveRuntimeConfigInput,
};
use crate::{DepthLimit, ShardKey, ShardRoutingVersion, SnapshotLimits};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const INSTRUMENT_ONE: &str = "018f0000-0000-7000-8000-000000000001";
const INSTRUMENT_TWO: &str = "018f0000-0000-7000-8000-000000000002";
const VENUE: &str = "coinbase";

fn instrument(value: &str) -> TestResult<InstrumentId> {
    Ok(InstrumentId::from_str(value)?)
}

fn definition(instrument_id: &str) -> TestResult<InstrumentDefinition> {
    Ok(InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument(instrument_id)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 2))?,
        venue_mappings: vec![VenueMapping::new(
            VenueId::try_from(VENUE)?,
            VenueSymbol::try_from("BTC-USD")?,
        )],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?)
}

fn route(instrument_id: &str, depth: usize, nonce_capacity: usize) -> TestResult<LiveRouteConfig> {
    Ok(LiveRouteConfig::try_new(LiveRouteConfigInput {
        route: ShardKey::new(VenueId::try_from(VENUE)?, instrument(instrument_id)?),
        definition: definition(instrument_id)?,
        depth: DepthLimit::new(depth)?,
        nonce_capacity,
        nonce_reclaim_budget: 1,
        maximum_capability_lifetime: Duration::from_secs(1),
    })?)
}

fn input() -> TestResult<LiveRuntimeConfigInput> {
    Ok(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: 2,
        mailbox_count_per_shard: 4,
        mailbox_bytes_per_shard: 1_024,
        maximum_message_bytes: 512,
        maximum_routes_per_shard: 8,
        maximum_sources_per_route: 2,
        registration_control_capacity: 2,
        registration_deadline: Duration::from_secs(1),
        health_event_capacity: 4,
        snapshot_event_budget: 8,
        snapshot_interval: Duration::from_millis(10),
        snapshot_limits: SnapshotLimits::try_new(8, 2, 2, 4, 4_096)?,
        maximum_retained_snapshot_readers: 2,
        shutdown_deadline: Duration::from_secs(1),
        maximum_runtime_bytes: u64::MAX,
    })
}

fn estimate(input: LiveRuntimeConfigInput, routes: &[LiveRouteConfig]) -> TestResult<u64> {
    let config = LiveRuntimeConfig::try_new(input)?;
    Ok(estimate_peak_bytes(&config, routes)?.get())
}

#[test]
fn checked_arithmetic_accepts_exact_maximum_and_rejects_overflow() -> TestResult {
    assert_eq!(add(u64::MAX - 1, 1)?, u64::MAX);
    assert_eq!(multiply(u64::MAX, 1)?, u64::MAX);
    assert_eq!(multiply(0, u64::MAX)?, 0);
    assert!(matches!(
        add(u64::MAX, 1),
        Err(LiveRuntimeConfigError::CapacityOverflow)
    ));
    assert!(matches!(
        multiply(u64::MAX, 2),
        Err(LiveRuntimeConfigError::CapacityOverflow)
    ));
    Ok(())
}

#[test]
fn route_state_nonce_source_and_book_terms_have_exact_deltas() -> TestResult {
    let base_route = route(INSTRUMENT_ONE, 4, 8)?;
    let base = estimate(input()?, std::slice::from_ref(&base_route))?;

    let second_route = route(INSTRUMENT_TWO, 4, 8)?;
    let with_second = estimate(input()?, &[base_route.clone(), second_route])?;
    let expected_route =
        ROUTE_FIXED_BYTES + 8 * NONCE_SLOT_BYTES + 2 * SOURCE_STREAM_BYTES + 8 * BOOK_LEVEL_BYTES;
    assert_eq!(with_second - base, expected_route);

    let larger_nonce = estimate(input()?, &[route(INSTRUMENT_ONE, 4, 9)?])?;
    assert_eq!(larger_nonce - base, NONCE_SLOT_BYTES);

    let deeper_book = estimate(input()?, &[route(INSTRUMENT_ONE, 5, 8)?])?;
    assert_eq!(deeper_book - base, 2 * BOOK_LEVEL_BYTES);

    let mut more_sources = input()?;
    more_sources.maximum_sources_per_route = 3;
    let more_sources = estimate(more_sources, &[base_route])?;
    assert_eq!(more_sources - base, SOURCE_STREAM_BYTES);
    Ok(())
}

#[test]
fn one_more_shard_charges_mailbox_candidate_control_snapshot_and_actor() -> TestResult {
    let route = route(INSTRUMENT_ONE, 4, 8)?;
    let base = estimate(input()?, std::slice::from_ref(&route))?;
    let mut three_shards = input()?;
    three_shards.shard_count = 3;
    let three_shards = estimate(three_shards, &[route])?;

    let expected_delta = 1_024
        + 4 * CHANNEL_COMMAND_SLOT_BYTES
        + 2 * 512
        + 2 * CONTROL_SLOT_BYTES
        + 2 * 4_096
        + ACTOR_FIXED_BYTES
        + SNAPSHOT_NOTIFICATION_BYTES;
    assert_eq!(three_shards - base, expected_delta);
    Ok(())
}

#[test]
fn mailbox_and_processing_dimensions_charge_independent_exact_deltas() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 4, 8)?];
    let base = estimate(input()?, &routes)?;

    let mut one_more_count = input()?;
    one_more_count.mailbox_count_per_shard += 1;
    assert_eq!(
        estimate(one_more_count, &routes)? - base,
        2 * CHANNEL_COMMAND_SLOT_BYTES
    );

    let mut one_more_byte = input()?;
    one_more_byte.mailbox_bytes_per_shard += 1;
    assert_eq!(estimate(one_more_byte, &routes)? - base, 2);

    let mut one_more_candidate_byte = input()?;
    one_more_candidate_byte.maximum_message_bytes += 1;
    assert_eq!(estimate(one_more_candidate_byte, &routes)? - base, 4);

    let mut one_more_control = input()?;
    one_more_control.registration_control_capacity += 1;
    assert_eq!(
        estimate(one_more_control, &routes)? - base,
        2 * CONTROL_SLOT_BYTES
    );
    Ok(())
}

#[test]
fn snapshot_reader_and_health_terms_are_bounded_at_the_documented_scope() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 4, 8)?];
    let base = estimate(input()?, &routes)?;

    let mut one_more_reader = input()?;
    one_more_reader.maximum_retained_snapshot_readers += 1;
    assert_eq!(estimate(one_more_reader, &routes)? - base, 4_096);

    let mut one_more_snapshot_byte = input()?;
    one_more_snapshot_byte.snapshot_limits = SnapshotLimits::try_new(8, 2, 2, 4, 4_097)?;
    // Two bytes per shard (construction + publication), plus one per runtime-wide reader.
    assert_eq!(estimate(one_more_snapshot_byte, &routes)? - base, 6);

    let mut one_more_health_event = input()?;
    one_more_health_event.health_event_capacity += 1;
    assert_eq!(
        estimate(one_more_health_event, &routes)? - base,
        HEALTH_EVENT_BYTES
    );
    Ok(())
}

#[test]
fn memory_ceiling_is_inclusive_and_reports_the_exact_rejected_estimate() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 4, 8)?];
    let estimated = estimate(input()?, &routes)?;

    let mut exact = input()?;
    exact.maximum_runtime_bytes = estimated;
    assert_eq!(estimate(exact, &routes)?, estimated);

    let mut below = input()?;
    below.maximum_runtime_bytes = estimated - 1;
    let below = LiveRuntimeConfig::try_new(below)?;
    assert!(matches!(
        estimate_peak_bytes(&below, &routes),
        Err(LiveRuntimeConfigError::PeakMemoryExceedsCeiling {
            estimated: rejected,
            ceiling,
        }) if rejected == estimated && ceiling == estimated - 1
    ));
    Ok(())
}

#[test]
fn estimate_is_route_order_independent_and_rejects_invalid_partitions() -> TestResult {
    let first = route(INSTRUMENT_ONE, 4, 8)?;
    let second = route(INSTRUMENT_TWO, 5, 9)?;
    let forward = estimate(input()?, &[first.clone(), second.clone()])?;
    let reverse = estimate(input()?, &[second, first.clone()])?;
    assert_eq!(forward, reverse);

    let config = LiveRuntimeConfig::try_new(input()?)?;
    assert!(matches!(
        estimate_peak_bytes(&config, &[first.clone(), first]),
        Err(LiveRuntimeConfigError::DuplicateRoute)
    ));

    let mut overfull = input()?;
    overfull.shard_count = 1;
    overfull.maximum_routes_per_shard = 1;
    let overfull = LiveRuntimeConfig::try_new(overfull)?;
    assert!(matches!(
        estimate_peak_bytes(
            &overfull,
            &[route(INSTRUMENT_ONE, 4, 8)?, route(INSTRUMENT_TWO, 4, 8)?,],
        ),
        Err(LiveRuntimeConfigError::TooManyRoutesForShard {
            shard: 0,
            count: 2,
            maximum: 1,
        })
    ));
    Ok(())
}
