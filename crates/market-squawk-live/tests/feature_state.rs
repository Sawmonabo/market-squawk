use std::time::Duration;

use market_squawk_analytics::{FeatureValidity, RequiredLiveFeature};
pub(crate) use market_squawk_live::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigInput,
    ShardKey, ShardRoutingVersion, SnapshotLimits,
};
use market_squawk_live::{
    LiveFeatureSnapshot, LiveRuntime, SnapshotCompleteness, StreamPhaseSnapshot,
};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "support/current_source.rs"]
mod current_source;

use current_source::{
    INSTRUMENT_ONE, SourceHarness, TestResult, route, route_config, runtime_config,
    runtime_config_with_feature_snapshot_bytes,
};

#[tokio::test(flavor = "current_thread")]
async fn committed_trade_features_warm_then_reset_on_generation_replacement() -> TestResult {
    let runtime = LiveRuntime::start(
        runtime_config(8, 8 * 1024 * 1024, 4 * 1024 * 1024)?,
        vec![route_config(INSTRUMENT_ONE)?],
    )
    .await?;
    let mut source = SourceHarness::try_new("feature-source", 1, INSTRUMENT_ONE)?;
    let ingress = runtime
        .ingress()
        .bind_generation(
            route(INSTRUMENT_ONE)?,
            source.current_lease()?,
            CancellationToken::new(),
        )
        .await?;
    let (_, batch) = source.batch_many(&[
        ("trade-1", 1, "100.00"),
        ("trade-2", 2, "101.00"),
        ("trade-3", 3, "102.00"),
    ])?;
    ingress.try_publish(batch)?;

    wait_for_feature(&runtime, ConnectionExpectation::GenerationOneReady).await?;

    let mut source = source.rollover(2)?;
    let ingress = runtime
        .ingress()
        .bind_generation(
            route(INSTRUMENT_ONE)?,
            source.current_lease()?,
            CancellationToken::new(),
        )
        .await?;
    let (_, batch) = source.batch("trade-4", 1)?;
    ingress.try_publish(batch)?;
    wait_for_feature(&runtime, ConnectionExpectation::GenerationTwoWarming).await?;

    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn feature_snapshot_byte_limit_truncates_without_hiding_committed_market_state() -> TestResult
{
    let feature_base = u32::try_from(std::mem::size_of::<LiveFeatureSnapshot>())?;
    let runtime = LiveRuntime::start(
        runtime_config_with_feature_snapshot_bytes(
            8,
            8 * 1024 * 1024,
            4 * 1024 * 1024,
            feature_base,
        )?,
        vec![route_config(INSTRUMENT_ONE)?],
    )
    .await?;
    let mut source = SourceHarness::try_new("bounded-feature-source", 1, INSTRUMENT_ONE)?;
    let ingress = runtime
        .ingress()
        .bind_generation(
            route(INSTRUMENT_ONE)?,
            source.current_lease()?,
            CancellationToken::new(),
        )
        .await?;
    let (_, batch) = source.batch("bounded-feature", 1)?;
    ingress.try_publish(batch)?;

    let expected_route = route(INSTRUMENT_ONE)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshots = runtime.snapshots().try_load_all()?;
            let published = snapshots
                .snapshots()
                .flat_map(|snapshot| snapshot.routes())
                .find(|candidate| candidate.route() == &expected_route);
            if let Some(published) = published
                && published.streams().first().is_some_and(|stream| {
                    stream.last_sequence() == Some(market_squawk_domain::SequenceNumber::new(1))
                })
                && published.features().sets().is_empty()
                && published.features().set_dimension().completeness()
                    == SnapshotCompleteness::Unavailable
            {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            drop(snapshots);
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

#[derive(Clone, Copy)]
enum ConnectionExpectation {
    GenerationOneReady,
    GenerationTwoWarming,
}

async fn wait_for_feature(runtime: &LiveRuntime, expectation: ConnectionExpectation) -> TestResult {
    let key = route(INSTRUMENT_ONE)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshots = runtime.snapshots().try_load_all()?;
            let route = snapshots
                .snapshots()
                .flat_map(|snapshot| snapshot.routes())
                .find(|candidate| candidate.route() == &key);
            if let Some(route) = route
                && route
                    .streams()
                    .first()
                    .is_some_and(|stream| stream.phase() == StreamPhaseSnapshot::Healthy)
                && let Some(set) = route.features().sets().first()
            {
                let vwap = set
                    .feature(RequiredLiveFeature::RollingVwap)
                    .ok_or("rolling VWAP missing from complete feature set")?;
                let matches = match expectation {
                    ConnectionExpectation::GenerationOneReady => {
                        set.connection_generation().get() == 1
                            && vwap.validity() == FeatureValidity::Ready
                            && vwap.scalar().is_some()
                    }
                    ConnectionExpectation::GenerationTwoWarming => {
                        set.connection_generation().get() == 2
                            && vwap.validity() == FeatureValidity::WarmingUp
                            && vwap.scalar().is_none()
                    }
                };
                if matches {
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
