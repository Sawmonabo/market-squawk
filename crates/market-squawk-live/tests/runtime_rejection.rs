use std::time::Duration;

use market_squawk_analytics::{FeatureValidity, RequiredLiveFeature};
use market_squawk_live::{
    CommittedResearchMarketBatchOutcome, LiveRuntime, LiveRuntimeConfig, LiveRuntimeConfigInput,
    LiveRuntimeExportPlan, LiveRuntimeHealthKind, RouteCommittedResearchMarketExport,
    ShardShutdownStatus, StreamPhaseSnapshot,
};
use tokio_util::sync::CancellationToken;

use crate::current_source;

use current_source::{
    INSTRUMENT_ONE, INSTRUMENT_TWO, SourceHarness, TestResult, route, route_config, runtime_config,
};

fn rejection_runtime_config(
    maximum_streams_per_route: usize,
    maximum_feature_sets_per_route: usize,
    snapshot_event_trigger: usize,
) -> TestResult<LiveRuntimeConfig> {
    let base = runtime_config(8, 8 * 1024 * 1024, 4 * 1024 * 1024)?;
    Ok(LiveRuntimeConfig::try_new(LiveRuntimeConfigInput {
        routing_version: base.routing_version(),
        shard_count: base.shard_count().get(),
        mailbox_count_per_shard: base.mailbox_count_per_shard().get(),
        mailbox_bytes_per_shard: base.mailbox_bytes_per_shard().get(),
        maximum_message_bytes: base.maximum_message_bytes().get(),
        maximum_routes_per_shard: base.maximum_routes_per_shard().get(),
        maximum_sources_per_route: base
            .maximum_sources_per_route()
            .get()
            .min(maximum_streams_per_route),
        maximum_streams_per_route,
        maximum_feature_window_observations_per_route: base
            .maximum_feature_window_observations_per_route()
            .get(),
        maximum_feature_window_bytes_per_route: base.maximum_feature_window_bytes_per_route().get(),
        maximum_feature_sets_per_route,
        cross_venue_command_count: base.cross_venue_command_count().get(),
        cross_venue_command_bytes: base.cross_venue_command_bytes().get(),
        maximum_cross_venue_instruments: base.maximum_cross_venue_instruments().get(),
        maximum_venues_per_cross_venue_instrument: base
            .maximum_venues_per_cross_venue_instrument()
            .get(),
        maximum_feature_snapshot_bytes: base.maximum_feature_snapshot_bytes().get(),
        maximum_action_hook_bytes_per_route: base.maximum_action_hook_bytes_per_route(),
        registration_control_capacity: base.registration_control_capacity().get(),
        registration_deadline: base.registration_deadline(),
        health_event_capacity: base.health_event_capacity().get(),
        snapshot_event_trigger,
        snapshot_interval: Duration::from_secs(60),
        snapshot_limits: base.snapshot_limits(),
        maximum_retained_snapshot_readers: base.maximum_retained_snapshot_readers().get(),
        shutdown_deadline: base.shutdown_deadline(),
        maximum_runtime_bytes: base.maximum_runtime_bytes().get(),
    })?)
}

async fn bind(
    runtime: &LiveRuntime,
    source: &SourceHarness,
    instrument: &str,
) -> TestResult<market_squawk_live::BoundShardIngress> {
    Ok(runtime
        .ingress()
        .bind_generation(
            route(instrument)?,
            source.current_lease()?,
            CancellationToken::new(),
        )
        .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_first_observation_is_quarantined_without_killing_other_routes_or_shutdown()
-> TestResult {
    let (export, mut terminal_batches) =
        RouteCommittedResearchMarketExport::try_new(route(INSTRUMENT_ONE)?, 1, 4 * 1024 * 1024)?;
    let mut runtime = LiveRuntime::start_with_exports(
        rejection_runtime_config(4, 4, 1)?,
        vec![route_config(INSTRUMENT_ONE)?, route_config(INSTRUMENT_TWO)?],
        LiveRuntimeExportPlan::new(Vec::new(), vec![export]),
    )
    .await?;
    let mut rejected_source = SourceHarness::try_new_with_quality(
        "rejected-source",
        1,
        INSTRUMENT_ONE,
        market_squawk_domain::DataQuality::DirectUnverified,
    )?;
    let mut healthy_source = SourceHarness::try_new("healthy-source", 1, INSTRUMENT_TWO)?;
    let rejected_ingress = bind(&runtime, &rejected_source, INSTRUMENT_ONE).await?;
    let healthy_ingress = bind(&runtime, &healthy_source, INSTRUMENT_TWO).await?;
    let (_, rejected) = rejected_source.batch_with_price("inexact-price", 1, "100.001")?;
    let (_, healthy) = healthy_source.batch("healthy-trade", 1)?;

    healthy_ingress.try_publish(healthy)?;

    let healthy_key = route(INSTRUMENT_TWO)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.snapshots().try_load_all()?;
            let healthy = snapshot
                .snapshots()
                .flat_map(|shard| shard.routes())
                .find(|candidate| candidate.route() == &healthy_key)
                .and_then(|route| route.streams().first())
                .is_some_and(|stream| {
                    stream.phase() == StreamPhaseSnapshot::Healthy && stream.generation_current()
                });
            if healthy {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            drop(snapshot);
            tokio::task::yield_now().await;
        }
    })
    .await??;

    let original_evidence = rejected.observations()[0].evidence().clone();
    rejected_ingress.try_publish(rejected)?;
    let terminal = tokio::time::timeout(Duration::from_secs(1), terminal_batches.recv())
        .await?
        .ok_or("rejected research batch did not emit terminal coverage")?;
    let (coordinates, outcome) = terminal.into_parts();
    assert_eq!(coordinates.evidence(), &original_evidence);
    assert_eq!(coordinates.row_count(), 1);
    assert_eq!(coordinates.wire_ordinals(), &[0]);
    assert!(matches!(
        outcome,
        CommittedResearchMarketBatchOutcome::Rejected
    ));
    assert!(terminal_batches.try_recv().is_err());

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            while let Some(event) = runtime.try_next_health() {
                if event.kind() == LiveRuntimeHealthKind::ProcessingRejected {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;

    let rejected_key = route(INSTRUMENT_ONE)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.snapshots().try_load_all()?;
            let routes = snapshot
                .snapshots()
                .flat_map(|shard| shard.routes())
                .collect::<Vec<_>>();
            let rejected_route = routes
                .iter()
                .find(|candidate| candidate.route() == &rejected_key);
            let healthy_route = routes
                .iter()
                .find(|candidate| candidate.route() == &healthy_key);
            if let (Some(rejected_route), Some(healthy_route)) = (rejected_route, healthy_route)
                && rejected_route.streams().len() == 1
                && healthy_route.streams().len() == 1
                && rejected_route.streams()[0].phase() == StreamPhaseSnapshot::Quarantined
                && !rejected_route.streams()[0].generation_current()
                && healthy_route.streams()[0].phase() == StreamPhaseSnapshot::Healthy
                && healthy_route.streams()[0].generation_current()
            {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            drop(snapshot);
            tokio::task::yield_now().await;
        }
    })
    .await??;

    let shutdown = runtime.shutdown().await;
    assert!(shutdown.is_complete(), "shutdown outcomes: {shutdown:?}");
    assert!(
        shutdown
            .outcomes()
            .iter()
            .all(|outcome| outcome.status() == ShardShutdownStatus::Complete)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn committed_feature_capacity_failure_is_visible_and_actor_recovers() -> TestResult {
    let mut runtime = LiveRuntime::start(
        rejection_runtime_config(2, 1, 1)?,
        vec![route_config(INSTRUMENT_ONE)?],
    )
    .await?;
    let mut source_a = SourceHarness::try_new("feature-a", 1, INSTRUMENT_ONE)?;
    let mut source_b = SourceHarness::try_new("feature-b", 1, INSTRUMENT_ONE)?;
    let ingress_a = bind(&runtime, &source_a, INSTRUMENT_ONE).await?;
    let ingress_b = bind(&runtime, &source_b, INSTRUMENT_ONE).await?;

    let (_, first) = source_a.batch("feature-a-1", 1)?;
    ingress_a.try_publish(first)?;
    wait_for_feature_validity(&runtime, "feature-a", FeatureValidity::WarmingUp, 1).await?;

    let (_, committed_without_slot) = source_b.batch("feature-b-1", 1)?;
    ingress_b.try_publish(committed_without_slot)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            while let Some(event) = runtime.try_next_health() {
                if event.kind() == LiveRuntimeHealthKind::FeatureUnavailable {
                    return;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    wait_for_feature_validity(&runtime, "feature-a", FeatureValidity::Overflow, 1).await?;

    let (_, recovery) = source_a.batch("feature-a-2", 2)?;
    ingress_a.try_publish(recovery)?;
    wait_for_feature_validity(&runtime, "feature-a", FeatureValidity::WarmingUp, 2).await?;

    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

async fn wait_for_feature_validity(
    runtime: &LiveRuntime,
    source: &str,
    validity: FeatureValidity,
    sequence: u64,
) -> TestResult {
    let expected_route = route(INSTRUMENT_ONE)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshots = runtime.snapshots().try_load_all()?;
            let route = snapshots
                .snapshots()
                .flat_map(|snapshot| snapshot.routes())
                .find(|route| route.route() == &expected_route);
            if let Some(route) = route {
                let stream_committed = route.streams().iter().any(|stream| {
                    stream.source().as_str() == source
                        && stream.last_sequence()
                            == Some(market_squawk_domain::SequenceNumber::new(sequence))
                });
                let feature_matches = route.features().sets().iter().any(|set| {
                    set.source().as_str() == source
                        && set
                            .feature(RequiredLiveFeature::RollingVwap)
                            .is_some_and(|feature| feature.validity() == validity)
                });
                if stream_committed && feature_matches {
                    return Ok::<_, Box<dyn std::error::Error>>(());
                }
            }
            drop(snapshots);
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn snapshot_event_trigger_skips_intermediate_prefix_of_successful_batch() -> TestResult {
    let (export, mut terminal_batches) =
        RouteCommittedResearchMarketExport::try_new(route(INSTRUMENT_ONE)?, 1, 4 * 1024 * 1024)?;
    let runtime = LiveRuntime::start_with_exports(
        rejection_runtime_config(4, 4, 2)?,
        vec![route_config(INSTRUMENT_ONE)?],
        LiveRuntimeExportPlan::new(Vec::new(), vec![export]),
    )
    .await?;
    let mut source = SourceHarness::try_new_with_quality(
        "batched-source",
        1,
        INSTRUMENT_ONE,
        market_squawk_domain::DataQuality::DirectUnverified,
    )?;
    let ingress = bind(&runtime, &source, INSTRUMENT_ONE).await?;
    let (_, batch) = source.batch_many(&[
        ("trade-1", 1, "100.00"),
        ("trade-2", 2, "100.01"),
        ("trade-3", 3, "100.02"),
        ("trade-4", 4, "100.03"),
        ("trade-5", 5, "100.04"),
        ("trade-6", 6, "100.05"),
        ("trade-7", 7, "100.06"),
    ])?;
    let original_evidence = batch.observations()[0].evidence().clone();
    ingress.try_publish(batch)?;
    // One channel slot must hold the whole seven-row result; there is no concurrent row drain.
    let terminal = tokio::time::timeout(Duration::from_secs(1), terminal_batches.recv())
        .await?
        .ok_or("complete research batch was not exported")?;
    let (coordinates, outcome) = terminal.into_parts();
    assert_eq!(coordinates.evidence(), &original_evidence);
    assert_eq!(coordinates.row_count(), 7);
    assert_eq!(coordinates.wire_ordinals(), &[0, 1, 2, 3, 4, 5, 6]);
    let CommittedResearchMarketBatchOutcome::Committed(rows) = outcome else {
        return Err("valid research batch was rejected".into());
    };
    assert_eq!(rows.len(), 7);
    for (ordinal, row) in rows.iter().enumerate() {
        assert_eq!(row.observation().wire_ordinal(), ordinal);
        assert_eq!(row.observation().row_count(), 7);
        assert_eq!(
            row.observation().source_coordinate().evidence(),
            &original_evidence
        );
    }
    assert!(terminal_batches.try_recv().is_err());
    drop(rows);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshots = runtime.snapshots().try_load_all()?;
            let published = snapshots
                .snapshots()
                .flat_map(|snapshot| snapshot.routes())
                .flat_map(|route| route.streams())
                .find(|stream| stream.source().as_str() == "batched-source");
            if let Some(stream) = published
                && stream.last_sequence() == Some(market_squawk_domain::SequenceNumber::new(7))
            {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;

    let shutdown = runtime.shutdown().await;
    assert!(shutdown.is_complete(), "shutdown outcomes: {shutdown:?}");
    Ok(())
}
