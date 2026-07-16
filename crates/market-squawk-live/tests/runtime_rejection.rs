use std::time::Duration;

pub(crate) use market_squawk_live::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, ShardKey, ShardRoutingVersion,
    SnapshotLimits,
};
use market_squawk_live::{
    LiveRuntime, LiveRuntimeConfig, LiveRuntimeConfigInput, LiveRuntimeHealthKind,
    ShardShutdownStatus, StreamPhaseSnapshot,
};
use tokio_util::sync::CancellationToken;

// This integration test intentionally consumes only the rejection subset of the shared harness.
#[allow(dead_code)]
#[path = "support/current_source.rs"]
mod current_source;

use current_source::{
    INSTRUMENT_ONE, INSTRUMENT_TWO, SourceHarness, TestResult, route, route_config, runtime_config,
};

fn rejection_runtime_config() -> TestResult<LiveRuntimeConfig> {
    let base = runtime_config(8, 8 * 1024 * 1024, 4 * 1024 * 1024)?;
    Ok(LiveRuntimeConfig::try_new(LiveRuntimeConfigInput {
        routing_version: base.routing_version(),
        shard_count: base.shard_count().get(),
        mailbox_count_per_shard: base.mailbox_count_per_shard().get(),
        mailbox_bytes_per_shard: base.mailbox_bytes_per_shard().get(),
        maximum_message_bytes: base.maximum_message_bytes().get(),
        maximum_routes_per_shard: base.maximum_routes_per_shard().get(),
        maximum_sources_per_route: base.maximum_sources_per_route().get(),
        registration_control_capacity: base.registration_control_capacity().get(),
        registration_deadline: base.registration_deadline(),
        health_event_capacity: base.health_event_capacity().get(),
        snapshot_event_budget: 1,
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
    let mut runtime = LiveRuntime::start(
        rejection_runtime_config()?,
        vec![route_config(INSTRUMENT_ONE)?, route_config(INSTRUMENT_TWO)?],
    )
    .await?;
    let mut rejected_source = SourceHarness::try_new("rejected-source", 1, INSTRUMENT_ONE)?;
    let mut healthy_source = SourceHarness::try_new("healthy-source", 1, INSTRUMENT_TWO)?;
    let rejected_ingress = bind(&runtime, &rejected_source, INSTRUMENT_ONE).await?;
    let healthy_ingress = bind(&runtime, &healthy_source, INSTRUMENT_TWO).await?;
    let (_, rejected) = rejected_source.batch_with_price("inexact-price", 1, "100.001")?;
    let (_, healthy) = healthy_source.batch("healthy-trade", 1)?;

    rejected_ingress.try_publish(rejected)?;
    healthy_ingress.try_publish(healthy)?;

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
    let healthy_key = route(INSTRUMENT_TWO)?;
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
                && healthy_route.streams()[0].phase() == StreamPhaseSnapshot::Healthy
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
