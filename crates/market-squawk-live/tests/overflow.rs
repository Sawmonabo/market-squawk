use std::time::Duration;

use market_squawk_live::{
    BoundShardIngress, LiveIngressBindError, LiveIngressError, LiveRuntime, LiveRuntimeHealthKind,
    RegistrationFailure,
};
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
    let route = original.route().clone();
    let shard = original.shard();
    let (_, initial) = source.batch("initial", 1)?;
    original.try_publish(initial)?;
    wait_for_generation_current(&runtime, true).await?;

    original.revoke_generation().await?;
    let snapshots = runtime.snapshots().try_load_all()?;
    let revoked = snapshots
        .snapshots()
        .flat_map(|snapshot| snapshot.routes())
        .find(|candidate| candidate.route() == &route)
        .and_then(|candidate| candidate.streams().first())
        .ok_or("revocation publication omitted the configured source stream")?;
    assert!(!revoked.generation_current());
    drop(snapshots);

    assert_eq!(
        bind(&runtime, &source).await.err(),
        Some(LiveIngressBindError::Registration(
            RegistrationFailure::NotCurrent
        ))
    );

    source.refresh_health()?;
    let refreshed = bind(&runtime, &source).await?;
    let (_, fresh_batch) = source.batch("refreshed", 2)?;

    assert_eq!(&route, refreshed.route());
    assert_eq!(shard, refreshed.shard());
    refreshed.try_publish(fresh_batch)?;
    wait_for_generation_current(&runtime, true).await?;
    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

async fn wait_for_generation_current(runtime: &LiveRuntime, expected: bool) -> TestResult {
    let route = current_source::route(INSTRUMENT_ONE)?;
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshots = runtime.snapshots().try_load_all()?;
            let current = snapshots
                .snapshots()
                .flat_map(|snapshot| snapshot.routes())
                .find(|candidate| candidate.route() == &route)
                .is_some_and(|candidate| {
                    candidate
                        .streams()
                        .iter()
                        .any(|stream| stream.generation_current() == expected)
                });
            if current {
                return TestResult::Ok(());
            }
            drop(snapshots);
            tokio::task::yield_now().await;
        }
    })
    .await;
    match result {
        Ok(result) => result?,
        Err(_) => {
            let snapshots = runtime.snapshots().try_load_all()?;
            return Err(format!(
                "timed out waiting for generation_current={expected}: {snapshots:#?}"
            )
            .into());
        }
    }
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
