//! Complete-startup supervision, runtime replacement, and bounded shutdown.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::{Id, JoinSet};
use tokio_util::sync::CancellationToken;

use super::actor::{ActorCompletion, ActorStartFailure, ShardActorInput, run};
use super::admission::{
    LiveRuntimeHealthEvent, LiveRuntimeIngress, RouteIngressChannels, ShardCommand,
};
use super::{LiveRouteConfig, LiveRuntimeConfig, LiveRuntimeConfigError, system_timestamp};
use crate::authority::{RuntimeLeaseOwner, ShardLeaseOwner};
use crate::cross_venue::create_cross_venue_plane;
use crate::snapshot::{SnapshotPlaneBundle, create_snapshot_plane};
use crate::{
    LiveSnapshotReader, ShardId, ShardLifecycleSnapshot, ShardRouter, ShardSnapshot,
    SnapshotDimension, SnapshotReadError,
};

static NEXT_RUNTIME_INCARNATION: AtomicU64 = AtomicU64::new(1);

/// Complete owner of one exact runtime incarnation and every actor task it spawned.
#[derive(Debug)]
pub struct LiveRuntime {
    config: LiveRuntimeConfig,
    incarnation: NonZeroU64,
    estimated_peak_bytes: NonZeroU64,
    runtime_owner: Option<RuntimeLeaseOwner>,
    ingress: LiveRuntimeIngress,
    snapshots: LiveSnapshotReader,
    snapshot_notifications: Box<[mpsc::Receiver<()>]>,
    notification_cursor: usize,
    health: mpsc::Receiver<LiveRuntimeHealthEvent>,
    cancellation: CancellationToken,
    actors: Option<JoinSet<ActorCompletion>>,
    cross_venue_task: Option<tokio::task::JoinHandle<()>>,
    task_shards: HashMap<Id, ShardId>,
}

impl LiveRuntime {
    /// Allocates, spawns, and completely readies every configured shard before returning ingress.
    pub async fn start(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
    ) -> Result<Self, LiveRuntimeStartError> {
        config.validate_routes(&routes)?;
        let estimated_peak_bytes = config.estimated_peak_bytes(&routes)?;
        let incarnation = next_incarnation()?;
        let mut runtime_owner = RuntimeLeaseOwner::new(incarnation.get());
        let runtime = runtime_owner.lease();
        let cancellation = CancellationToken::new();
        let (cross_venue, cross_venue_worker) = create_cross_venue_plane(
            &routes,
            config.feature_capacity(),
            cancellation.child_token(),
        )
        .map_err(|_| LiveRuntimeStartError::CrossVenueInitialization)?;
        let router = ShardRouter::v1(config.shard_count().get())?;
        let shard_count = usize::from(config.shard_count().get());
        let mut partitions = (0..shard_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<LiveRouteConfig>>>();
        for route in routes {
            let shard = router.route(route.route());
            partitions
                .get_mut(usize::from(shard.index()))
                .ok_or(LiveRuntimeStartError::RoutePartitionInvariant)?
                .push(route);
        }
        for partition in &mut partitions {
            partition.sort_by(|left, right| {
                left.route()
                    .venue()
                    .as_str()
                    .cmp(right.route().venue().as_str())
                    .then_with(|| left.route().instrument().cmp(&right.route().instrument()))
            });
        }

        let initial = initial_snapshots(&config, incarnation, &partitions)?;
        let SnapshotPlaneBundle {
            publishers,
            reader: snapshots,
            notifications: snapshot_notifications,
        } = create_snapshot_plane(initial, config.maximum_retained_snapshot_readers().get())?;
        let mut publishers = publishers.into_vec().into_iter();
        if publishers.len() != shard_count {
            return Err(LiveRuntimeStartError::SnapshotPlaneInvariant);
        }
        let mailbox_byte_permits = usize::try_from(config.mailbox_bytes_per_shard().get())
            .map_err(|_| LiveRuntimeStartError::ShardCount)?;
        let mut shard_ids = Vec::new();
        shard_ids
            .try_reserve_exact(shard_count)
            .map_err(|_| LiveRuntimeStartError::Allocation)?;
        for index in 0..shard_count {
            shard_ids.push(ShardId::new(
                u16::try_from(index).map_err(|_| LiveRuntimeStartError::ShardCount)?,
                config.shard_count().get(),
            )?);
        }
        let (health_sender, health) = mpsc::channel(config.health_event_capacity().get());
        let mut actors = JoinSet::new();
        let mut task_shards = HashMap::new();
        task_shards
            .try_reserve(shard_count)
            .map_err(|_| LiveRuntimeStartError::Allocation)?;
        let mut ready_receivers = Vec::new();
        let mut startup_releases = Vec::new();
        ready_receivers
            .try_reserve_exact(shard_count)
            .map_err(|_| LiveRuntimeStartError::Allocation)?;
        startup_releases
            .try_reserve_exact(shard_count)
            .map_err(|_| LiveRuntimeStartError::Allocation)?;
        let route_total = partitions
            .iter()
            .try_fold(0_usize, |total, routes| total.checked_add(routes.len()))
            .ok_or(LiveRuntimeStartError::Allocation)?;
        let mut ingress_routes = HashMap::new();
        ingress_routes
            .try_reserve(route_total)
            .map_err(|_| LiveRuntimeStartError::Allocation)?;

        for (shard, shard_routes) in shard_ids.into_iter().zip(partitions) {
            let shard_index = shard.index();
            let shard_owner = ShardLeaseOwner::new(u64::from(shard_index) + 1);
            let shard_liveness = shard_owner.lease();
            let byte_budget = Arc::new(Semaphore::new(mailbox_byte_permits));
            let (mailbox_sender, mailbox) =
                mpsc::channel::<ShardCommand>(config.mailbox_count_per_shard().get());
            let (registration_sender, registrations) =
                mpsc::channel(config.registration_control_capacity().get());
            for route in &shard_routes {
                let channels = RouteIngressChannels {
                    shard,
                    runtime: runtime.clone(),
                    shard_liveness: shard_liveness.clone(),
                    mailbox: mailbox_sender.clone(),
                    byte_budget: Arc::clone(&byte_budget),
                    registration: registration_sender.clone(),
                    registration_deadline: config.registration_deadline(),
                    maximum_message_bytes: config.maximum_message_bytes().get(),
                    health: health_sender.clone(),
                };
                if ingress_routes
                    .insert(route.route().clone(), channels)
                    .is_some()
                {
                    cleanup_failed_startup(
                        &mut runtime_owner,
                        &cancellation,
                        &mut actors,
                        &snapshots,
                    )
                    .await;
                    return Err(LiveRuntimeStartError::RoutePartitionInvariant);
                }
            }
            let Some(publisher) = publishers.next() else {
                cleanup_failed_startup(&mut runtime_owner, &cancellation, &mut actors, &snapshots)
                    .await;
                return Err(LiveRuntimeStartError::SnapshotPlaneInvariant);
            };
            let (ready, ready_receiver) = oneshot::channel();
            let (startup_release, startup_wait) = oneshot::channel();
            ready_receivers.push((shard, ready_receiver));
            startup_releases.push((shard, startup_release));
            let input = ShardActorInput {
                shard,
                routing_version: config.routing_version(),
                runtime_incarnation: incarnation,
                runtime: runtime.clone(),
                shard_owner,
                routes: shard_routes,
                maximum_sources_per_route: config.maximum_sources_per_route().get(),
                maximum_streams_per_route: config.maximum_streams_per_route().get(),
                feature_capacity: config.feature_capacity(),
                cross_venue: cross_venue.clone(),
                maximum_book_items_per_message:
                    crate::provider_book::maximum_book_items_for_message(
                        config.maximum_message_bytes().get(),
                    ),
                mailbox,
                registrations,
                snapshot_limits: config.snapshot_limits(),
                snapshot_interval: config.snapshot_interval(),
                snapshot_event_trigger: config.snapshot_event_trigger().get(),
                publisher,
                cancellation: cancellation.child_token(),
                health: health_sender.clone(),
                ready: Some(ready),
                startup_release: startup_wait,
            };
            let handle = actors.spawn(run(input));
            if task_shards.insert(handle.id(), shard).is_some() {
                cleanup_failed_startup(&mut runtime_owner, &cancellation, &mut actors, &snapshots)
                    .await;
                return Err(LiveRuntimeStartError::TaskIdentityCollision);
            }
        }
        if publishers.next().is_some() {
            cleanup_failed_startup(&mut runtime_owner, &cancellation, &mut actors, &snapshots)
                .await;
            return Err(LiveRuntimeStartError::SnapshotPlaneInvariant);
        }
        drop(health_sender);
        let readiness = await_readiness(ready_receivers, config.shutdown_deadline()).await;
        if let Err(error) = readiness {
            cleanup_failed_startup(&mut runtime_owner, &cancellation, &mut actors, &snapshots)
                .await;
            return Err(error);
        }
        if runtime.validate().is_err() {
            cleanup_failed_startup(&mut runtime_owner, &cancellation, &mut actors, &snapshots)
                .await;
            return Err(LiveRuntimeStartError::RuntimeInvalidBeforeReady);
        }
        if actors.try_join_next().is_some() {
            cleanup_failed_startup(&mut runtime_owner, &cancellation, &mut actors, &snapshots)
                .await;
            return Err(LiveRuntimeStartError::ActorExitedAfterReady);
        }
        let mut cross_venue_task = cross_venue_worker.map(|worker| tokio::spawn(worker.run()));
        for (_shard, release) in startup_releases {
            if release.send(()).is_err() {
                if let Some(task) = cross_venue_task.as_mut() {
                    task.abort();
                    let _ = task.await;
                }
                cleanup_failed_startup(&mut runtime_owner, &cancellation, &mut actors, &snapshots)
                    .await;
                return Err(LiveRuntimeStartError::ActorExitedAfterReady);
            }
        }
        tokio::task::yield_now().await;
        if runtime.validate().is_err()
            || actors.try_join_next().is_some()
            || cross_venue_task
                .as_ref()
                .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            if let Some(task) = cross_venue_task.as_mut() {
                task.abort();
                let _ = task.await;
            }
            cleanup_failed_startup(&mut runtime_owner, &cancellation, &mut actors, &snapshots)
                .await;
            return Err(LiveRuntimeStartError::ActorExitedAfterReady);
        }
        Ok(Self {
            config,
            incarnation,
            estimated_peak_bytes,
            runtime_owner: Some(runtime_owner),
            ingress: LiveRuntimeIngress {
                routes: Arc::new(ingress_routes),
                runtime,
            },
            snapshots,
            snapshot_notifications,
            notification_cursor: 0,
            health,
            cancellation,
            actors: Some(actors),
            cross_venue_task,
            task_shards,
        })
    }

    /// Returns a cloneable bind-only ingress after complete startup readiness.
    pub fn ingress(&self) -> LiveRuntimeIngress {
        self.ingress.clone()
    }

    /// Returns bounded read-only snapshot access; publication cells remain private.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.snapshots.clone()
    }

    /// Takes one best-effort health mirror without waiting.
    pub fn try_next_health(&mut self) -> Option<LiveRuntimeHealthEvent> {
        self.health.try_recv().ok()
    }

    /// Takes one coalesced shard snapshot-change hint without waiting.
    ///
    /// Hints are fair-scanned by shard and may coalesce multiple publications. Callers always load
    /// the current immutable value from [`Self::snapshots`] rather than treating a hint as data.
    pub fn try_next_snapshot_notification(&mut self) -> Option<ShardId> {
        let count = self.snapshot_notifications.len();
        for offset in 0..count {
            let index = (self.notification_cursor + offset) % count;
            if self.snapshot_notifications[index].try_recv().is_ok() {
                self.notification_cursor = (index + 1) % count;
                return ShardId::new(u16::try_from(index).ok()?, self.config.shard_count().get())
                    .ok();
            }
        }
        None
    }

    /// Returns the nonzero process-local incarnation carried by every shard snapshot.
    pub const fn incarnation(&self) -> NonZeroU64 {
        self.incarnation
    }

    /// Returns the checked conservative peak model accepted at startup.
    pub const fn estimated_peak_bytes(&self) -> NonZeroU64 {
        self.estimated_peak_bytes
    }

    /// Invalidates and completely joins this incarnation before starting a clean replacement.
    pub async fn replace(
        self,
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
    ) -> Result<Self, LiveRuntimeReplaceError> {
        let shutdown = self.shutdown().await;
        if !shutdown.is_complete() {
            return Err(LiveRuntimeReplaceError::Shutdown(shutdown));
        }
        Self::start(config, routes)
            .await
            .map_err(LiveRuntimeReplaceError::Start)
    }

    /// Release-invalidates ingress, drains or aborts-and-awaits every actor, and returns outcomes.
    pub async fn shutdown(mut self) -> LiveRuntimeShutdown {
        if let Some(owner) = self.runtime_owner.as_mut() {
            owner.invalidate();
        }
        for channels in self.ingress.routes.values() {
            channels.byte_budget.close();
        }
        self.cancellation.cancel();
        let mut actors = self.actors.take().unwrap_or_default();
        let mut cross_venue_task = self.cross_venue_task.take();
        let deadline = self.config.shutdown_deadline();
        let mut outcomes = HashMap::new();
        let mut cross_venue_join_error = false;
        let joined = tokio::time::timeout(deadline, async {
            while let Some(result) = actors.join_next_with_id().await {
                record_join_result(result, &self.task_shards, &mut outcomes);
            }
            if let Some(task) = cross_venue_task.as_mut()
                && (&mut *task).await.is_err()
            {
                cross_venue_join_error = true;
            }
        })
        .await;
        let deadline_elapsed = joined.is_err();
        if deadline_elapsed {
            actors.abort_all();
            if let Some(task) = cross_venue_task.as_mut() {
                task.abort();
            }
            while let Some(result) = actors.join_next_with_id().await {
                record_deadline_result(result, &self.task_shards, &mut outcomes);
            }
            if let Some(task) = cross_venue_task.as_mut() {
                let _ = (&mut *task).await;
            }
        }
        if cross_venue_join_error {
            for shard in self.task_shards.values().copied() {
                outcomes.insert(shard, ShardShutdownStatus::ActorError);
            }
        }
        for shard in self.task_shards.values().copied() {
            outcomes.entry(shard).or_insert(if deadline_elapsed {
                ShardShutdownStatus::DeadlineAborted
            } else {
                ShardShutdownStatus::JoinError
            });
        }
        self.snapshots.plane.close();
        let mut outcomes = outcomes
            .into_iter()
            .map(|(shard, status)| ShardShutdownOutcome { shard, status })
            .collect::<Vec<_>>();
        outcomes.sort_by_key(|outcome| outcome.shard);
        LiveRuntimeShutdown {
            incarnation: self.incarnation,
            deadline_elapsed,
            outcomes: outcomes.into_boxed_slice(),
        }
    }
}

impl Drop for LiveRuntime {
    fn drop(&mut self) {
        if let Some(owner) = self.runtime_owner.as_mut() {
            owner.invalidate();
        }
        self.cancellation.cancel();
        self.snapshots.plane.close();
        if let Some(actors) = self.actors.as_mut() {
            actors.abort_all();
        }
        if let Some(task) = self.cross_venue_task.as_mut() {
            task.abort();
        }
    }
}

fn initial_snapshots(
    config: &LiveRuntimeConfig,
    incarnation: NonZeroU64,
    partitions: &[Vec<LiveRouteConfig>],
) -> Result<Vec<ShardSnapshot>, LiveRuntimeStartError> {
    let now = system_timestamp().map_err(|_| LiveRuntimeStartError::ClockRange)?;
    let mut snapshots = Vec::new();
    snapshots
        .try_reserve_exact(partitions.len())
        .map_err(|_| LiveRuntimeStartError::Allocation)?;
    for (index, routes) in partitions.iter().enumerate() {
        let shard = ShardId::new(
            u16::try_from(index).map_err(|_| LiveRuntimeStartError::ShardCount)?,
            config.shard_count().get(),
        )?;
        snapshots.push(ShardSnapshot {
            routing_version: config.routing_version(),
            shard_count: config.shard_count(),
            runtime_incarnation: incarnation,
            shard_id: shard,
            snapshot_revision: NonZeroU64::MIN,
            health_revision: 0,
            lifecycle: ShardLifecycleSnapshot::Starting,
            evaluated_at: now,
            published_at: now,
            routes: Box::new([]),
            route_dimension: SnapshotDimension::from_counts(
                routes.len(),
                0,
                config.snapshot_limits().maximum_routes().get(),
            )?,
            retained_bytes: u64::try_from(std::mem::size_of::<ShardSnapshot>())
                .map_err(|_| LiveRuntimeStartError::Allocation)?,
        });
    }
    Ok(snapshots)
}

async fn await_readiness(
    receivers: Vec<(ShardId, oneshot::Receiver<Result<(), ActorStartFailure>>)>,
    deadline: std::time::Duration,
) -> Result<(), LiveRuntimeStartError> {
    let mut pending = FuturesUnordered::new();
    for (shard, receiver) in receivers {
        pending.push(async move { (shard, receiver.await) });
    }
    tokio::time::timeout(deadline, async {
        while let Some((shard, result)) = pending.next().await {
            result
                .map_err(|_| LiveRuntimeStartError::ActorExitedBeforeReady { shard })?
                .map_err(|_| LiveRuntimeStartError::ActorInitialization { shard })?;
        }
        Ok(())
    })
    .await
    .map_err(|_| LiveRuntimeStartError::ReadinessDeadline)?
}

async fn cleanup_failed_startup(
    runtime_owner: &mut RuntimeLeaseOwner,
    cancellation: &CancellationToken,
    actors: &mut JoinSet<ActorCompletion>,
    snapshots: &LiveSnapshotReader,
) {
    runtime_owner.invalidate();
    cancellation.cancel();
    actors.abort_all();
    while actors.join_next().await.is_some() {}
    snapshots.plane.close();
}

fn record_join_result(
    result: Result<(Id, ActorCompletion), tokio::task::JoinError>,
    task_shards: &HashMap<Id, ShardId>,
    outcomes: &mut HashMap<ShardId, ShardShutdownStatus>,
) {
    match result {
        Ok((id, completion)) => {
            let expected = task_shards.get(&id).copied();
            let status = if expected == Some(completion.shard) && completion.result.is_ok() {
                ShardShutdownStatus::Complete
            } else {
                ShardShutdownStatus::ActorError
            };
            outcomes.insert(expected.unwrap_or(completion.shard), status);
        }
        Err(error) => {
            if let Some(shard) = task_shards.get(&error.id()).copied() {
                outcomes.insert(shard, ShardShutdownStatus::JoinError);
            }
        }
    }
}

fn record_deadline_result(
    result: Result<(Id, ActorCompletion), tokio::task::JoinError>,
    task_shards: &HashMap<Id, ShardId>,
    outcomes: &mut HashMap<ShardId, ShardShutdownStatus>,
) {
    let id = match &result {
        Ok((id, _)) => *id,
        Err(error) => error.id(),
    };
    if let Some(shard) = task_shards.get(&id).copied() {
        outcomes.insert(shard, ShardShutdownStatus::DeadlineAborted);
    }
}

fn next_incarnation() -> Result<NonZeroU64, LiveRuntimeStartError> {
    let value = NEXT_RUNTIME_INCARNATION
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current.checked_add(1)
        })
        .map_err(|_| LiveRuntimeStartError::IncarnationExhausted)?;
    NonZeroU64::new(value).ok_or(LiveRuntimeStartError::IncarnationExhausted)
}

/// Per-shard bounded-shutdown disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShardShutdownStatus {
    Complete,
    ActorError,
    JoinError,
    DeadlineAborted,
}

/// One configured shard's observed shutdown result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardShutdownOutcome {
    shard: ShardId,
    status: ShardShutdownStatus,
}

impl ShardShutdownOutcome {
    pub const fn shard(&self) -> ShardId {
        self.shard
    }
    pub const fn status(&self) -> ShardShutdownStatus {
        self.status
    }
}

/// Complete bounded shutdown report for one consumed runtime incarnation.
#[derive(Debug)]
pub struct LiveRuntimeShutdown {
    incarnation: NonZeroU64,
    deadline_elapsed: bool,
    outcomes: Box<[ShardShutdownOutcome]>,
}

impl LiveRuntimeShutdown {
    pub const fn incarnation(&self) -> NonZeroU64 {
        self.incarnation
    }
    pub const fn deadline_elapsed(&self) -> bool {
        self.deadline_elapsed
    }
    pub fn outcomes(&self) -> &[ShardShutdownOutcome] {
        &self.outcomes
    }
    pub fn is_complete(&self) -> bool {
        !self.deadline_elapsed
            && self
                .outcomes
                .iter()
                .all(|outcome| outcome.status == ShardShutdownStatus::Complete)
    }
}

/// Complete-startup failure after fail-closed cleanup of every spawned task.
#[derive(Debug, Error)]
pub enum LiveRuntimeStartError {
    #[error(transparent)]
    Config(#[from] LiveRuntimeConfigError),
    #[error(transparent)]
    Routing(#[from] crate::ShardRoutingError),
    #[error(transparent)]
    Snapshot(#[from] SnapshotReadError),
    #[error("initial bounded snapshot construction failed")]
    SnapshotConstruction,
    #[error("runtime incarnation identity exhausted")]
    IncarnationExhausted,
    #[error("runtime bounded allocation failed")]
    Allocation,
    #[error("configured shard count cannot be represented")]
    ShardCount,
    #[error("deterministic route partition violated configured ownership")]
    RoutePartitionInvariant,
    #[error("snapshot publication plane did not match configured shards")]
    SnapshotPlaneInvariant,
    #[error("bounded cross-venue plane could not initialize")]
    CrossVenueInitialization,
    #[error("actor task identity collision")]
    TaskIdentityCollision,
    #[error("shard {shard} exited before startup readiness")]
    ActorExitedBeforeReady { shard: ShardId },
    #[error("shard {shard} failed bounded startup initialization")]
    ActorInitialization { shard: ShardId },
    #[error("complete runtime readiness exceeded its bounded deadline")]
    ReadinessDeadline,
    #[error("shared runtime authority was invalid before complete readiness")]
    RuntimeInvalidBeforeReady,
    #[error("an actor exited after reporting ready but before runtime release")]
    ActorExitedAfterReady,
    #[error("trusted system clock is outside the supported range")]
    ClockRange,
}

/// Runtime replacement failure after consuming and invalidating the former incarnation.
#[derive(Debug, Error)]
pub enum LiveRuntimeReplaceError {
    #[error("former runtime did not shut down completely")]
    Shutdown(LiveRuntimeShutdown),
    #[error(transparent)]
    Start(LiveRuntimeStartError),
}

impl From<crate::snapshot::SnapshotBuildError> for LiveRuntimeStartError {
    fn from(_: crate::snapshot::SnapshotBuildError) -> Self {
        Self::SnapshotConstruction
    }
}

#[cfg(test)]
#[path = "lifecycle/tests.rs"]
mod tests;
