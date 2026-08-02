use std::str::FromStr;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, Currency, Denomination, InstrumentDefinition, InstrumentDefinitionInput,
    InstrumentId, LotSize, TickSize, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use rust_decimal::Decimal;

use super::{
    ACTOR_FIXED_BYTES, CHANNEL_COMMAND_SLOT_BYTES, CONTROL_SLOT_BYTES,
    CROSS_VENUE_COMMAND_SLOT_BYTES, CROSS_VENUE_INSTRUMENT_SLOT_BYTES,
    CROSS_VENUE_VENUE_SLOT_BYTES, FEATURE_SET_SLOT_BYTES, HEALTH_EVENT_BYTES, NONCE_SLOT_BYTES,
    ROUTE_FIXED_BYTES, SNAPSHOT_NOTIFICATION_BYTES, SNAPSHOT_ROUTE_SORT_SCRATCH_BYTES,
    SNAPSHOT_STATUS_SORT_SCRATCH_BYTES, SNAPSHOT_STREAM_SORT_SCRATCH_BYTES, SOURCE_ADMISSION_BYTES,
    add, all_shard_book_processing_bytes, book_processing_peak, estimate_peak_bytes, multiply,
    persistent_stream_bytes, route_feature_owner_bytes, snapshot_publication_reader_peak,
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
        definition_revision: market_squawk_domain::InstrumentDefinitionRevision::try_from(1_u64)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        quote_currency: Currency::try_from("USD")?,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 2))?,
        contract_multiplier: Decimal::ONE,
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
        maximum_streams_per_route: 2,
        maximum_feature_window_observations_per_route: 8,
        maximum_feature_window_bytes_per_route: 1_048_576,
        maximum_feature_sets_per_route: 2,
        cross_venue_command_count: 4,
        cross_venue_command_bytes: 65_536,
        maximum_cross_venue_instruments: 2,
        maximum_venues_per_cross_venue_instrument: 2,
        maximum_feature_snapshot_bytes: 4_096,
        maximum_action_hook_bytes_per_route: 4_096,
        registration_control_capacity: 2,
        registration_deadline: Duration::from_secs(1),
        health_event_capacity: 4,
        snapshot_event_trigger: 8,
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
fn route_state_nonce_source_stream_and_dual_book_terms_have_exact_deltas() -> TestResult {
    let base_route = route(INSTRUMENT_ONE, 4, 8)?;
    let base = estimate(input()?, std::slice::from_ref(&base_route))?;

    let second_route = route(INSTRUMENT_TWO, 4, 8)?;
    let both_routes = [base_route.clone(), second_route];
    let with_second = estimate(input()?, &both_routes)?;
    let config = LiveRuntimeConfig::try_new(input()?)?;
    let processing_delta = all_shard_book_processing_bytes(&config, &both_routes)?
        - all_shard_book_processing_bytes(&config, std::slice::from_ref(&base_route))?;
    let expected_route = ROUTE_FIXED_BYTES
        + 8 * NONCE_SLOT_BYTES
        + 2 * SOURCE_ADMISSION_BYTES
        + 2 * persistent_stream_bytes(4)?
        + route_feature_owner_bytes(&config)?
        + snapshot_publication_reader_peak(4_096, 2, 2)?.publication_count
            * u64::from(config.maximum_feature_snapshot_bytes().get())
        + SNAPSHOT_ROUTE_SORT_SCRATCH_BYTES
        + processing_delta;
    assert_eq!(with_second - base, expected_route);

    let larger_nonce = estimate(input()?, &[route(INSTRUMENT_ONE, 4, 9)?])?;
    assert_eq!(larger_nonce - base, NONCE_SLOT_BYTES);

    let deeper_book = estimate(input()?, &[route(INSTRUMENT_ONE, 5, 8)?])?;
    let shallow_processing = book_processing_peak(512, 4)?;
    let deeper_processing = book_processing_peak(512, 5)?;
    let processing_delta = (deeper_processing.additional_bytes
        - deeper_processing.shard_scratch_bytes)
        - (shallow_processing.additional_bytes - shallow_processing.shard_scratch_bytes);
    assert_eq!(
        deeper_book - base,
        2 * (persistent_stream_bytes(5)? - persistent_stream_bytes(4)?) + processing_delta
    );

    let mut more_streams = input()?;
    more_streams.maximum_streams_per_route = 3;
    let more_streams_input = more_streams.clone();
    let more_streams = estimate(more_streams, std::slice::from_ref(&base_route))?;
    let per_stream_sort_scratch =
        SNAPSHOT_STREAM_SORT_SCRATCH_BYTES + SNAPSHOT_STATUS_SORT_SCRATCH_BYTES;
    assert_eq!(
        more_streams - base,
        persistent_stream_bytes(4)? + 2 * per_stream_sort_scratch
    );

    let mut more_sources = more_streams_input;
    more_sources.maximum_sources_per_route = 3;
    let more_sources = estimate(more_sources, &[base_route])?;
    assert_eq!(more_sources - more_streams, SOURCE_ADMISSION_BYTES);
    Ok(())
}

#[test]
fn one_route_charges_every_persistent_book_for_one_or_sixty_four_streams() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 10, 8)?];
    let mut one = input()?;
    one.maximum_sources_per_route = 1;
    one.maximum_streams_per_route = 1;
    let one = estimate(one, &routes)?;

    let mut sixty_four = input()?;
    sixty_four.maximum_sources_per_route = 1;
    sixty_four.maximum_streams_per_route = 64;
    let sixty_four = estimate(sixty_four, &routes)?;
    let per_stream_sort_scratch =
        SNAPSHOT_STREAM_SORT_SCRATCH_BYTES + SNAPSHOT_STATUS_SORT_SCRATCH_BYTES;
    assert_eq!(
        sixty_four - one,
        63 * (persistent_stream_bytes(10)? + 2 * per_stream_sort_scratch)
    );
    Ok(())
}

#[test]
fn expanded_stream_accounting_rejects_the_former_single_book_ceiling() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 10, 8)?];
    let mut one = input()?;
    one.maximum_sources_per_route = 1;
    one.maximum_streams_per_route = 1;
    let former_ceiling = estimate(one, &routes)?;

    let mut expanded = input()?;
    expanded.maximum_sources_per_route = 1;
    expanded.maximum_streams_per_route = 64;
    expanded.maximum_runtime_bytes = former_ceiling;
    let expanded = LiveRuntimeConfig::try_new(expanded)?;
    let per_stream_sort_scratch =
        SNAPSHOT_STREAM_SORT_SCRATCH_BYTES + SNAPSHOT_STATUS_SORT_SCRATCH_BYTES;
    let expected =
        former_ceiling + 63 * (persistent_stream_bytes(10)? + 2 * per_stream_sort_scratch);
    assert!(matches!(
        estimate_peak_bytes(&expanded, &routes),
        Err(LiveRuntimeConfigError::PeakMemoryExceedsCeiling {
            estimated,
            ceiling,
        }) if estimated == expected && ceiling == former_ceiling
    ));
    Ok(())
}

#[test]
fn one_more_shard_charges_mailbox_candidate_control_snapshot_and_actor() -> TestResult {
    let route = route(INSTRUMENT_ONE, 4, 8)?;
    let mut base_input = input()?;
    base_input.maximum_retained_snapshot_readers = 3;
    let mut three_shards = base_input.clone();
    let base = estimate(base_input, std::slice::from_ref(&route))?;
    three_shards.shard_count = 3;
    let three_shards = estimate(three_shards, &[route])?;

    let processing = book_processing_peak(512, 4)?;
    let base_snapshot = snapshot_publication_reader_peak(4_096, 2, 3)?;
    let expanded_snapshot = snapshot_publication_reader_peak(4_096, 3, 3)?;
    let expected_delta = 1_024
        + 4 * CHANNEL_COMMAND_SLOT_BYTES
        + processing.shard_scratch_bytes
        + 2 * CONTROL_SLOT_BYTES
        + (expanded_snapshot.additional_bytes - base_snapshot.additional_bytes)
        + (expanded_snapshot.publication_count - base_snapshot.publication_count)
            * u64::from(configured_feature_snapshot_bytes()?)
        + 2 * (SNAPSHOT_STREAM_SORT_SCRATCH_BYTES + SNAPSHOT_STATUS_SORT_SCRATCH_BYTES)
        + ACTOR_FIXED_BYTES
        + SNAPSHOT_NOTIFICATION_BYTES;
    assert_eq!(three_shards - base, expected_delta);
    Ok(())
}

fn configured_feature_snapshot_bytes() -> TestResult<u32> {
    Ok(LiveRuntimeConfig::try_new(input()?)?
        .maximum_feature_snapshot_bytes()
        .get())
}

#[test]
fn feature_owner_terms_have_exact_incremental_charges() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 4, 8)?];
    let base_input = input()?;
    let base = estimate(base_input.clone(), &routes)?;

    let mut window_byte = base_input.clone();
    window_byte.maximum_feature_window_bytes_per_route += 1;
    assert_eq!(estimate(window_byte, &routes)? - base, 1);

    let mut feature_set = base_input.clone();
    feature_set.maximum_feature_sets_per_route += 1;
    assert_eq!(
        estimate(feature_set, &routes)? - base,
        FEATURE_SET_SLOT_BYTES
    );

    let mut hook_byte = base_input.clone();
    hook_byte.maximum_action_hook_bytes_per_route += 1;
    assert_eq!(estimate(hook_byte, &routes)? - base, 1);

    let mut command_count = base_input.clone();
    command_count.cross_venue_command_count += 1;
    assert_eq!(
        estimate(command_count, &routes)? - base,
        CROSS_VENUE_COMMAND_SLOT_BYTES
    );

    let mut command_byte = base_input.clone();
    command_byte.cross_venue_command_bytes += 1;
    assert_eq!(estimate(command_byte, &routes)? - base, 1);

    let mut instrument = base_input.clone();
    instrument.maximum_cross_venue_instruments += 1;
    assert_eq!(
        estimate(instrument, &routes)? - base,
        CROSS_VENUE_INSTRUMENT_SLOT_BYTES + 2 * CROSS_VENUE_VENUE_SLOT_BYTES
    );

    let mut venue = base_input.clone();
    venue.maximum_venues_per_cross_venue_instrument += 1;
    assert_eq!(
        estimate(venue, &routes)? - base,
        2 * CROSS_VENUE_VENUE_SLOT_BYTES
    );

    let publication_count = snapshot_publication_reader_peak(4_096, 2, 2)?.publication_count;
    let mut snapshot_byte = base_input;
    snapshot_byte.maximum_feature_snapshot_bytes += 1;
    assert_eq!(estimate(snapshot_byte, &routes)? - base, publication_count);
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

    let processing = book_processing_peak(512, 4)?;
    assert_eq!(processing.maximum_message_bytes, 512);
    assert!(processing.maximum_book_items > 0);
    assert!(processing.snapshot_additional_bytes > 0);
    assert!(processing.delta_additional_bytes > 2 * 512);
    assert_eq!(
        processing.additional_bytes,
        processing
            .snapshot_additional_bytes
            .max(processing.delta_additional_bytes)
    );

    let mut one_more_message_byte = input()?;
    one_more_message_byte.maximum_message_bytes += 1;
    let expanded_input = one_more_message_byte.clone();
    let expanded = estimate(one_more_message_byte, &routes)?;
    let expanded_processing = book_processing_peak(513, 4)?;
    let current_config = LiveRuntimeConfig::try_new(input()?)?;
    let expanded_config = LiveRuntimeConfig::try_new(expanded_input)?;
    assert_eq!(
        expanded - base,
        all_shard_book_processing_bytes(&expanded_config, &routes)?
            - all_shard_book_processing_bytes(&current_config, &routes)?
    );
    assert!(expanded_processing.maximum_book_items >= processing.maximum_book_items);

    let mut one_more_control = input()?;
    one_more_control.registration_control_capacity += 1;
    assert_eq!(
        estimate(one_more_control, &routes)? - base,
        2 * CONTROL_SLOT_BYTES
    );
    Ok(())
}

#[test]
fn former_wire_multiplier_ceiling_rejects_the_structural_delta_peak() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 4, 8)?];
    let complete = estimate(input()?, &routes)?;
    let config = LiveRuntimeConfig::try_new(input()?)?;
    let structural = all_shard_book_processing_bytes(&config, &routes)?;
    let former_processing = 2 * u64::from(config.shard_count().get()) * 512;
    assert!(structural > former_processing);

    let former_estimate = complete
        .checked_sub(structural)
        .and_then(|value| value.checked_add(former_processing))
        .ok_or("former estimate overflow")?;
    let mut undercharged = input()?;
    undercharged.maximum_runtime_bytes = former_estimate;
    let undercharged = LiveRuntimeConfig::try_new(undercharged)?;
    assert!(matches!(
        estimate_peak_bytes(&undercharged, &routes),
        Err(LiveRuntimeConfigError::PeakMemoryExceedsCeiling {
            estimated,
            ceiling,
        }) if estimated == complete && ceiling == former_estimate
    ));
    Ok(())
}

#[test]
fn structural_processing_peak_accepts_exact_ceiling_and_rejects_one_byte_under() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 10_000, 8)?];
    let mut feature_complete = input()?;
    feature_complete.maximum_feature_window_bytes_per_route = 2 * 1_048_576;
    let estimated = estimate(feature_complete.clone(), &routes)?;

    let mut exact = feature_complete.clone();
    exact.maximum_runtime_bytes = estimated;
    assert_eq!(estimate(exact, &routes)?, estimated);

    let mut below = feature_complete;
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
fn snapshot_reader_and_health_terms_are_bounded_at_the_documented_scope() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 4, 8)?];
    let base = estimate(input()?, &routes)?;

    let mut one_more_reader = input()?;
    one_more_reader.maximum_retained_snapshot_readers += 1;
    let base_snapshot = snapshot_publication_reader_peak(4_096, 2, 2)?;
    let expanded_snapshot = snapshot_publication_reader_peak(4_096, 2, 3)?;
    assert_eq!(
        estimate(one_more_reader, &routes)? - base,
        expanded_snapshot.additional_bytes - base_snapshot.additional_bytes
            + u64::from(configured_feature_snapshot_bytes()?)
    );

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
fn snapshot_publication_generations_and_aggregate_metadata_have_exact_boundaries() -> TestResult {
    let peak = snapshot_publication_reader_peak(4_096, 2, 4)?;
    assert_eq!(peak.publication_count, 8);
    assert!(peak.publication_bytes > 4_096);
    assert!(peak.reader_metadata_bytes > 0);
    assert_eq!(
        peak.additional_bytes,
        peak.publication_count * peak.publication_bytes + peak.reader_metadata_bytes
    );

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
fn single_shard_multiple_generation_ceiling_is_inclusive_and_one_under_rejects() -> TestResult {
    let routes = [route(INSTRUMENT_ONE, 4, 8)?];
    let mut single = input()?;
    single.shard_count = 1;
    single.maximum_retained_snapshot_readers = 3;
    let estimated = estimate(single.clone(), &routes)?;

    single.maximum_runtime_bytes = estimated;
    assert_eq!(estimate(single.clone(), &routes)?, estimated);
    single.maximum_runtime_bytes = estimated - 1;
    let below = LiveRuntimeConfig::try_new(single)?;
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
fn all_shard_processing_and_worst_reader_generations_coexist_below_the_runtime_ceiling()
-> TestResult {
    let routes = [route(INSTRUMENT_ONE, 4, 8)?, route(INSTRUMENT_TWO, 4, 8)?];
    let mut configured = input()?;
    configured.maximum_retained_snapshot_readers = 4;
    let config = LiveRuntimeConfig::try_new(configured)?;
    let processing = all_shard_book_processing_bytes(&config, &routes)?;
    let per_shard = book_processing_peak(512, 4)?;
    assert_eq!(processing, 2 * per_shard.additional_bytes);
    let snapshots = snapshot_publication_reader_peak(4_096, 2, 4)?;
    let total = estimate_peak_bytes(&config, &routes)?.get();
    assert!(total >= processing + snapshots.additional_bytes);
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
