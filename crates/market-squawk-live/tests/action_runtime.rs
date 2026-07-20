#![allow(
    clippy::panic,
    reason = "invalid fixed fixtures and failed assertions must terminate this test binary"
)]

use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use market_squawk_analytics::RequiredLiveFeature;
use market_squawk_live::{
    ActionAuthorityIssueLimit, ActionHookDisposition, CommittedActionContext, CurrentAuthorityGate,
    CurrentAuthorityGateError, LiveActionHook, LiveActionHookError, LiveRuntime, RouteActionHook,
};
pub(crate) use market_squawk_live::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigInput,
    ShardKey, ShardRoutingVersion, SnapshotLimits,
};
use tokio_util::sync::CancellationToken;

#[allow(dead_code)]
#[path = "support/current_source.rs"]
mod current_source;

use current_source::{
    INSTRUMENT_ONE, SourceHarness, TestResult, route, route_config, runtime_config,
};

#[derive(Debug)]
struct ConsumingHook {
    calls: Arc<AtomicUsize>,
}

impl LiveActionHook for ConsumingHook {
    fn on_committed(
        &mut self,
        _context: CommittedActionContext<'_>,
        authority: &mut CurrentAuthorityGate<'_>,
    ) -> ActionHookDisposition {
        let capability = authority
            .issue()
            .unwrap_or_else(|error| panic!("actor-owned issue must succeed: {error}"));
        authority
            .consume(capability)
            .unwrap_or_else(|error| panic!("actor-owned consume must succeed: {error}"));
        assert!(matches!(
            authority.issue(),
            Err(CurrentAuthorityGateError::IssueLimitExceeded)
        ));
        self.calls.fetch_add(1, Ordering::Relaxed);
        ActionHookDisposition::Dispatched
    }

    fn retained_bytes(&self) -> Result<usize, LiveActionHookError> {
        Ok(size_of::<Self>())
    }

    fn maximum_authority_issues(&self) -> ActionAuthorityIssueLimit {
        ActionAuthorityIssueLimit::MIN
    }
}

#[tokio::test(flavor = "current_thread")]
async fn action_enabled_runtime_owns_and_invokes_the_exact_bounded_route_hook() -> TestResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let hook = RouteActionHook::try_new(
        route(INSTRUMENT_ONE)?,
        Box::new(ConsumingHook {
            calls: Arc::clone(&calls),
        }),
        vec![RequiredLiveFeature::RollingVwap],
    )?;
    assert_eq!(Arc::strong_count(&calls), 2);

    let runtime = LiveRuntime::start_with_action_hooks(
        runtime_config(8, 8 * 1024 * 1024, 4 * 1024 * 1024)?,
        vec![route_config(INSTRUMENT_ONE)?],
        vec![hook],
    )
    .await?;
    let mut source = SourceHarness::try_new("action-source", 1, INSTRUMENT_ONE)?;
    let ingress = runtime
        .ingress()
        .bind_generation(
            route(INSTRUMENT_ONE)?,
            source.current_lease()?,
            CancellationToken::new(),
        )
        .await?;

    for (identifier, sequence) in [("trade-1", 1), ("trade-2", 2)] {
        let (_, batch) = source.batch(identifier, sequence)?;
        ingress.try_publish(batch)?;
    }
    wait_for_sequence(&runtime, 2).await?;
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    let (_, ready) = source.batch("trade-3", 3)?;
    ingress.try_publish(ready)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(calls.load(Ordering::Relaxed), 1);

    assert!(runtime.shutdown().await.is_complete());
    assert_eq!(Arc::strong_count(&calls), 1);
    Ok(())
}

async fn wait_for_sequence(runtime: &LiveRuntime, expected: u64) -> TestResult {
    let expected_route = route(INSTRUMENT_ONE)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshots = runtime.snapshots().try_load_all()?;
            let observed = snapshots
                .snapshots()
                .flat_map(|snapshot| snapshot.routes())
                .find(|candidate| candidate.route() == &expected_route)
                .and_then(|route| route.streams().first())
                .and_then(|stream| stream.last_sequence());
            if observed == Some(market_squawk_domain::SequenceNumber::new(expected)) {
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            drop(snapshots);
            tokio::task::yield_now().await;
        }
    })
    .await??;
    Ok(())
}
