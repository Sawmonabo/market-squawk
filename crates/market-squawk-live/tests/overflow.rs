use market_squawk_live::{BoundShardIngress, LiveIngressError, LiveRuntime, LiveRuntimeHealthKind};
use tokio_util::sync::CancellationToken;

use crate::current_source;

use crate::current_source::{
    INSTRUMENT_ONE, INSTRUMENT_TWO, SourceHarness, TestResult, route_config, runtime_config,
};

async fn start(maximum_message_bytes: u32) -> TestResult<LiveRuntime> {
    Ok(LiveRuntime::start(
        runtime_config(4, 8 * 1024 * 1024, maximum_message_bytes)?,
        vec![route_config(INSTRUMENT_ONE)?],
    )
    .await?)
}

async fn bind(runtime: &LiveRuntime, source: &SourceHarness) -> TestResult<BoundShardIngress> {
    Ok(runtime
        .ingress()
        .bind_generation(
            current_source::route(INSTRUMENT_ONE)?,
            source.current_lease()?,
            CancellationToken::new(),
        )
        .await?)
}

#[tokio::test(flavor = "current_thread")]
async fn oversized_publish_invalidates_before_error_is_observable() -> TestResult {
    let mut runtime = start(1).await?;
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let ingress = bind(&runtime, &source).await?;
    let (_, oversized) = source.batch("oversized", 1)?;

    assert!(matches!(
        ingress.try_publish(oversized),
        Err(LiveIngressError::MessageTooLarge {
            retained,
            maximum: 1,
        }) if retained > 1
    ));
    let (_, after_failure) = source.batch("after-overweight", 2)?;
    assert_eq!(
        ingress.try_publish(after_failure),
        Err(LiveIngressError::GenerationNotCurrent)
    );
    let mut saw_rejection = false;
    while let Some(event) = runtime.try_next_health() {
        saw_rejection |= event.kind() == LiveRuntimeHealthKind::IngressRejected;
    }
    assert!(saw_rejection);
    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn wrong_route_publish_invalidates_the_exact_bound_generation() -> TestResult {
    let runtime = start(8 * 1024 * 1024).await?;
    let mut primary = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let ingress = bind(&runtime, &primary).await?;
    let mut wrong_route = SourceHarness::try_new("source-b", 1, INSTRUMENT_TWO)?;
    let (_, wrong) = wrong_route.batch("wrong-route", 1)?;

    assert_eq!(
        ingress.try_publish(wrong),
        Err(LiveIngressError::WrongRoute)
    );
    let (_, after_failure) = primary.batch("after-wrong-route", 1)?;
    assert_eq!(
        ingress.try_publish(after_failure),
        Err(LiveIngressError::GenerationNotCurrent)
    );
    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn registry_transplant_publish_invalidates_the_bound_generation() -> TestResult {
    let runtime = start(8 * 1024 * 1024).await?;
    let mut primary = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let ingress = bind(&runtime, &primary).await?;
    let mut transplanted = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let (_, transplant) = transplanted.batch("transplant", 1)?;

    assert_eq!(
        ingress.try_publish(transplant),
        Err(LiveIngressError::SourceLeaseTransplant)
    );
    let (_, after_failure) = primary.batch("after-transplant", 1)?;
    assert_eq!(
        ingress.try_publish(after_failure),
        Err(LiveIngressError::GenerationNotCurrent)
    );
    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_closes_previously_bound_ingress_before_return() -> TestResult {
    let runtime = start(8 * 1024 * 1024).await?;
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let ingress = bind(&runtime, &source).await?;
    let (_, queued_after_shutdown) = source.batch("closed", 1)?;

    assert!(runtime.shutdown().await.is_complete());
    assert_eq!(
        ingress.try_publish(queued_after_shutdown),
        Err(LiveIngressError::RuntimeClosed)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn health_refresh_rebind_accepts_fresh_batch_on_same_route() -> TestResult {
    let runtime = start(8 * 1024 * 1024).await?;
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let original = bind(&runtime, &source).await?;
    source.refresh_health()?;
    let refreshed = bind(&runtime, &source).await?;
    let (_, fresh_batch) = source.batch("refreshed", 1)?;

    assert_eq!(original.route(), refreshed.route());
    assert_eq!(original.shard(), refreshed.shard());
    refreshed.try_publish(fresh_batch)?;
    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn rollover_revokes_old_ingress_without_revoking_successor() -> TestResult {
    let runtime = start(8 * 1024 * 1024).await?;
    let mut source = SourceHarness::try_new("source-a", 1, INSTRUMENT_ONE)?;
    let old_ingress = bind(&runtime, &source).await?;
    let (_, old_batch) = source.batch("old-generation", 1)?;

    let mut source = source.rollover(2)?;
    let successor = bind(&runtime, &source).await?;
    assert_eq!(
        old_ingress.try_publish(old_batch),
        Err(LiveIngressError::GenerationNotCurrent)
    );
    let (_, successor_batch) = source.batch("successor", 1)?;
    successor.try_publish(successor_batch)?;
    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}
