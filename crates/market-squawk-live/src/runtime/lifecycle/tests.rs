#[path = "tests/fixture.rs"]
mod fixture;

use std::collections::HashMap;
use std::future::pending;
use std::num::NonZeroU64;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::{
    LiveRuntime, LiveRuntimeStartError, ShardShutdownStatus, await_readiness,
    cleanup_failed_startup,
};
use crate::authority::{RuntimeLeaseOwner, ShardLeaseOwner};
use crate::runtime::actor::{ActorCompletion, ActorError, ActorStartFailure, ShardActorInput, run};
use crate::snapshot::create_snapshot_plane;
use crate::{ShardId, ShardLifecycleSnapshot, ShardRoutingVersion, SnapshotReadError};

use fixture::{DropSignal, TestResult, config, route, runtime_shell};

#[tokio::test]
async fn complete_startup_publishes_every_ready_shard_before_runtime_escape() -> TestResult {
    let config = config(2, 4, Duration::from_secs(1))?;
    let mut runtime = LiveRuntime::start(config, vec![route()?]).await?;
    let incarnation = runtime.incarnation();

    let lease = runtime.snapshots().try_load_all()?;
    let snapshots = lease.snapshots().collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 2);
    for (index, snapshot) in snapshots.iter().enumerate() {
        assert_eq!(snapshot.shard_id().index(), u16::try_from(index)?);
        assert_eq!(snapshot.runtime_incarnation(), incarnation);
        assert_eq!(snapshot.snapshot_revision().get(), 2);
        assert_eq!(snapshot.lifecycle(), ShardLifecycleSnapshot::Ready);
    }
    drop(lease);

    let first = runtime
        .try_next_snapshot_notification()
        .ok_or("missing first ready notification")?;
    let second = runtime
        .try_next_snapshot_notification()
        .ok_or("missing second ready notification")?;
    assert_ne!(first, second);
    assert!(runtime.try_next_snapshot_notification().is_none());

    let shutdown = runtime.shutdown().await;
    assert!(shutdown.is_complete());
    assert_eq!(shutdown.outcomes().len(), 2);
    assert!(
        shutdown
            .outcomes()
            .iter()
            .all(|outcome| outcome.status() == ShardShutdownStatus::Complete)
    );
    Ok(())
}

#[tokio::test]
async fn exact_aggregate_reader_budget_supports_one_complete_runtime_lease() -> TestResult {
    let config = config(2, 2, Duration::from_secs(1))?;
    let runtime = LiveRuntime::start(config, vec![route()?]).await?;
    let aggregate = runtime.snapshots().try_load_all()?;
    assert_eq!(aggregate.snapshots().count(), 2);
    assert!(matches!(
        runtime.snapshots().try_load(ShardId::new(0, 2)?),
        Err(SnapshotReadError::ReaderLimitReached)
    ));
    assert!(matches!(
        runtime.snapshots().try_load_all(),
        Err(SnapshotReadError::ReaderLimitReached)
    ));
    drop(aggregate);
    let single = runtime.snapshots().try_load(ShardId::new(0, 2)?)?;
    drop(single);
    assert_eq!(runtime.snapshots().try_load_all()?.snapshots().count(), 2);
    assert!(runtime.shutdown().await.is_complete());
    Ok(())
}

#[tokio::test]
async fn readiness_failure_is_typed_by_exact_shard_and_never_waits_for_order() -> TestResult {
    let first = ShardId::new(0, 2)?;
    let second = ShardId::new(1, 2)?;
    let (first_sender, first_receiver) = oneshot::channel();
    let (second_sender, second_receiver) = oneshot::channel();
    first_sender
        .send(Ok(()))
        .map_err(|_| "first readiness receiver dropped")?;
    second_sender
        .send(Err(ActorStartFailure::Initialization))
        .map_err(|_| "second readiness receiver dropped")?;

    assert!(matches!(
        await_readiness(
            vec![(second, second_receiver), (first, first_receiver)],
            Duration::from_secs(1),
        )
        .await,
        Err(LiveRuntimeStartError::ActorInitialization { shard }) if shard == second
    ));
    Ok(())
}

#[tokio::test]
async fn partial_startup_cleanup_invalidates_closes_aborts_and_awaits_every_task() -> TestResult {
    let mut owner = RuntimeLeaseOwner::new(70);
    let lease = owner.lease();
    let cancellation = CancellationToken::new();
    let mut actors = JoinSet::new();
    let (entered_sender, entered_receiver) = oneshot::channel();
    let (dropped_sender, dropped_receiver) = oneshot::channel();
    let shard = ShardId::new(0, 1)?;
    actors.spawn(async move {
        let _drop_signal = DropSignal::new(dropped_sender);
        let _ = entered_sender.send(());
        pending::<()>().await;
        ActorCompletion {
            shard,
            result: Ok(()),
        }
    });
    entered_receiver.await?;

    let config = config(1, 1, Duration::from_secs(1))?;
    let initial = super::initial_snapshots(
        &config,
        NonZeroU64::new(70).ok_or("zero incarnation")?,
        &[Vec::new()],
    )?;
    let snapshots = create_snapshot_plane(initial, 1)?.reader;

    cleanup_failed_startup(&mut owner, &cancellation, &mut actors, &snapshots).await;

    assert!(lease.validate().is_err());
    assert!(cancellation.is_cancelled());
    assert!(actors.is_empty());
    dropped_receiver.await?;
    assert_eq!(
        snapshots.try_load(ShardId::new(0, 1)?).err(),
        Some(SnapshotReadError::Closed)
    );
    Ok(())
}

#[tokio::test]
async fn graceful_shutdown_invalidates_before_controlled_drain_then_joins_without_leak()
-> TestResult {
    let config = config(1, 1, Duration::from_secs(1))?;
    let owner = RuntimeLeaseOwner::new(80);
    let cancellation = CancellationToken::new();
    let child = cancellation.child_token();
    let mut actors = JoinSet::new();
    let mut task_shards = HashMap::new();
    let shard = ShardId::new(0, 1)?;
    let (drain_sender, drain_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = oneshot::channel();
    let (dropped_sender, dropped_receiver) = oneshot::channel();
    let handle = actors.spawn(async move {
        let _drop_signal = DropSignal::new(dropped_sender);
        child.cancelled().await;
        let _ = drain_sender.send(());
        let _ = release_receiver.await;
        ActorCompletion {
            shard,
            result: Ok(()),
        }
    });
    task_shards.insert(handle.id(), shard);
    let harness = runtime_shell(config, 80, owner, cancellation, actors, task_shards)?;
    let runtime_lease = harness.runtime_lease.clone();
    let reader = harness.reader.clone();
    let shutdown = harness.runtime.shutdown();
    tokio::pin!(shutdown);

    tokio::select! {
        biased;
        result = &mut shutdown => return Err(format!("shutdown escaped before drain barrier: {result:?}").into()),
        result = drain_receiver => result?,
    }
    assert!(runtime_lease.validate().is_err());
    release_sender
        .send(())
        .map_err(|_| "controlled actor exited before drain release")?;
    let outcome = shutdown.await;

    assert!(outcome.is_complete());
    assert_eq!(outcome.outcomes().len(), 1);
    assert_eq!(
        outcome.outcomes()[0].status(),
        ShardShutdownStatus::Complete
    );
    dropped_receiver.await?;
    assert_eq!(
        reader.try_load(ShardId::new(0, 1)?).err(),
        Some(SnapshotReadError::Closed)
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_deadline_aborts_and_awaits_a_parked_actor_without_detaching_it() -> TestResult {
    let config = config(1, 1, Duration::from_nanos(1))?;
    let owner = RuntimeLeaseOwner::new(90);
    let cancellation = CancellationToken::new();
    let mut actors = JoinSet::new();
    let mut task_shards = HashMap::new();
    let shard = ShardId::new(0, 1)?;
    let (entered_sender, entered_receiver) = oneshot::channel();
    let (dropped_sender, dropped_receiver) = oneshot::channel();
    let handle = actors.spawn(async move {
        let _drop_signal = DropSignal::new(dropped_sender);
        let _ = entered_sender.send(());
        pending::<()>().await;
        ActorCompletion {
            shard,
            result: Ok(()),
        }
    });
    task_shards.insert(handle.id(), shard);
    entered_receiver.await?;
    let harness = runtime_shell(config, 90, owner, cancellation, actors, task_shards)?;

    let shutdown = harness.runtime.shutdown().await;

    assert!(shutdown.deadline_elapsed());
    assert!(!shutdown.is_complete());
    assert_eq!(shutdown.outcomes().len(), 1);
    assert_eq!(
        shutdown.outcomes()[0].status(),
        ShardShutdownStatus::DeadlineAborted
    );
    dropped_receiver.await?;
    Ok(())
}

#[tokio::test]
async fn drop_fallback_revokes_and_aborts_owned_tasks_without_leaving_a_task_alive() -> TestResult {
    let config = config(1, 1, Duration::from_secs(1))?;
    let owner = RuntimeLeaseOwner::new(100);
    let cancellation = CancellationToken::new();
    let mut actors = JoinSet::new();
    let mut task_shards = HashMap::new();
    let shard = ShardId::new(0, 1)?;
    let (entered_sender, entered_receiver) = oneshot::channel();
    let (dropped_sender, dropped_receiver) = oneshot::channel();
    let handle = actors.spawn(async move {
        let _drop_signal = DropSignal::new(dropped_sender);
        let _ = entered_sender.send(());
        pending::<()>().await;
        ActorCompletion {
            shard,
            result: Ok(()),
        }
    });
    task_shards.insert(handle.id(), shard);
    entered_receiver.await?;
    let harness = runtime_shell(config, 100, owner, cancellation, actors, task_shards)?;
    let runtime_lease = harness.runtime_lease;
    let reader = harness.reader;

    drop(harness.runtime);

    assert!(runtime_lease.validate().is_err());
    dropped_receiver.await?;
    assert_eq!(
        reader.try_load(shard).err(),
        Some(SnapshotReadError::Closed)
    );
    Ok(())
}

#[tokio::test]
async fn actor_exit_invalidates_shared_runtime_before_completion_is_observed() -> TestResult {
    let config = config(1, 1, Duration::from_secs(1))?;
    let mut runtime_owner = RuntimeLeaseOwner::new(110);
    let runtime_lease = runtime_owner.lease();
    let shard_owner = ShardLeaseOwner::new(1);
    let initial = super::initial_snapshots(
        &config,
        NonZeroU64::new(110).ok_or("zero incarnation")?,
        &[Vec::new()],
    )?;
    let plane = create_snapshot_plane(initial, 1)?;
    let publisher = plane.publishers.into_vec().remove(0);
    let (health, _health_receiver) = mpsc::channel(4);
    let (ready, ready_receiver) = oneshot::channel();
    let (startup_release, startup_wait) = oneshot::channel();
    let (cross_venue, _worker) = crate::cross_venue::create_cross_venue_plane(
        &[],
        config.feature_capacity(),
        CancellationToken::new(),
    )?;
    let input = ShardActorInput {
        shard: ShardId::new(0, 1)?,
        routing_version: ShardRoutingVersion::V1,
        runtime_incarnation: NonZeroU64::new(110).ok_or("zero incarnation")?,
        runtime: runtime_lease.clone(),
        shard_owner,
        routes: Vec::new(),
        action_hooks: Vec::new(),
        maximum_action_hook_bytes_per_route: config.maximum_action_hook_bytes_per_route().get(),
        maximum_sources_per_route: 1,
        maximum_streams_per_route: 1,
        feature_capacity: config.feature_capacity(),
        cross_venue,
        maximum_book_items_per_message: crate::provider_book::maximum_book_items_for_message(
            config.maximum_message_bytes().get(),
        ),
        mailbox: mpsc::channel(1).1,
        registrations: mpsc::channel(1).1,
        snapshot_limits: config.snapshot_limits(),
        snapshot_interval: config.snapshot_interval(),
        snapshot_event_trigger: config.snapshot_event_trigger().get(),
        publisher,
        cancellation: CancellationToken::new(),
        health,
        ready: Some(ready),
        startup_release: startup_wait,
    };
    let actor = tokio::spawn(run(input));
    ready_receiver.await??;

    drop(startup_release);
    let completion = actor.await?;

    assert!(matches!(
        completion.result,
        Err(ActorError::StartupReleaseDropped)
    ));
    assert!(runtime_lease.validate().is_err());
    runtime_owner.invalidate();
    assert_eq!(
        plane
            .reader
            .try_load(ShardId::new(0, 1)?)?
            .snapshot()
            .lifecycle(),
        ShardLifecycleSnapshot::Ready
    );
    Ok(())
}

#[tokio::test]
async fn same_route_replacement_closes_old_readers_and_starts_a_fresh_incarnation() -> TestResult {
    let config = config(1, 4, Duration::from_secs(1))?;
    let route = route()?;
    let runtime = LiveRuntime::start(config.clone(), vec![route.clone()]).await?;
    let former_incarnation = runtime.incarnation();
    let old_reader = runtime.snapshots();

    let replacement = runtime.replace(config, vec![route.clone()]).await?;

    assert_ne!(replacement.incarnation(), former_incarnation);
    assert_eq!(
        old_reader.try_load(ShardId::new(0, 1)?).err(),
        Some(SnapshotReadError::Closed)
    );
    let latest = replacement.snapshots().try_load(ShardId::new(0, 1)?)?;
    assert_eq!(
        latest.snapshot().runtime_incarnation(),
        replacement.incarnation()
    );
    assert_eq!(latest.snapshot().routes().len(), 1);
    assert_eq!(latest.snapshot().routes()[0].route(), route.route());
    drop(latest);
    assert!(replacement.shutdown().await.is_complete());
    Ok(())
}
