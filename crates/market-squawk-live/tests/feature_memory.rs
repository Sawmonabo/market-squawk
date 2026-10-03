use std::time::Duration;

use market_squawk_live::{
    LiveRuntimeConfig, LiveRuntimeConfigError, LiveRuntimeConfigInput, ShardRoutingVersion,
    SnapshotLimits,
};

use crate::current_source;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn input(maximum_runtime_bytes: u64) -> TestResult<LiveRuntimeConfigInput> {
    Ok(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: 1,
        mailbox_count_per_shard: 4,
        mailbox_bytes_per_shard: 1_048_576,
        maximum_message_bytes: 262_144,
        maximum_routes_per_shard: 2,
        maximum_sources_per_route: 2,
        maximum_streams_per_route: 2,
        maximum_feature_window_observations_per_route: 8,
        maximum_feature_window_bytes_per_route: 64 * 1024,
        maximum_feature_sets_per_route: 2,
        cross_venue_command_count: 4,
        cross_venue_command_bytes: 4 * 1024,
        maximum_cross_venue_instruments: 2,
        maximum_venues_per_cross_venue_instrument: 2,
        maximum_feature_snapshot_bytes: 64 * 1024,
        maximum_action_hook_bytes_per_route: 4 * 1024,
        registration_control_capacity: 2,
        registration_deadline: Duration::from_secs(1),
        health_event_capacity: 8,
        snapshot_event_trigger: 8,
        snapshot_interval: Duration::from_millis(10),
        snapshot_limits: SnapshotLimits::try_new(2, 2, 2, 8, 1_048_576)?,
        maximum_retained_snapshot_readers: 1,
        shutdown_deadline: Duration::from_secs(1),
        maximum_runtime_bytes,
    })
}

#[test]
fn complete_feature_budget_accepts_equality_and_rejects_one_byte_below() -> TestResult {
    let route = current_source::route_config(current_source::INSTRUMENT_ONE)?;
    let provisional = LiveRuntimeConfig::try_new(input(u64::MAX)?)?;
    let exact = provisional.estimated_peak_bytes(std::slice::from_ref(&route))?;

    let accepted = LiveRuntimeConfig::try_new(input(exact.get())?)?;
    assert_eq!(
        accepted.estimated_peak_bytes(std::slice::from_ref(&route))?,
        exact
    );

    let below = LiveRuntimeConfig::try_new(input(exact.get() - 1)?)?;
    assert!(matches!(
        below.estimated_peak_bytes(&[route]),
        Err(LiveRuntimeConfigError::PeakMemoryExceedsCeiling {
            estimated,
            ceiling,
        }) if estimated == exact.get() && ceiling == exact.get() - 1
    ));
    Ok(())
}

#[test]
fn required_feature_capacities_are_nonzero_and_action_hooks_are_optional() -> TestResult {
    let mut zero_window = input(u64::MAX)?;
    zero_window.maximum_feature_window_observations_per_route = 0;
    assert!(LiveRuntimeConfig::try_new(zero_window).is_err());

    let mut zero_hook = input(u64::MAX)?;
    zero_hook.maximum_action_hook_bytes_per_route = 0;
    assert_eq!(
        LiveRuntimeConfig::try_new(zero_hook)?.maximum_action_hook_bytes_per_route(),
        0
    );

    let mut one_venue = input(u64::MAX)?;
    one_venue.maximum_venues_per_cross_venue_instrument = 1;
    assert!(LiveRuntimeConfig::try_new(one_venue).is_err());
    Ok(())
}
