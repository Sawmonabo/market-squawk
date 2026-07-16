//! Single-writer shard actor ownership and event/action linearization.

use std::collections::HashMap;
use std::num::NonZeroU64;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[path = "actor/snapshot.rs"]
mod snapshot;

use super::admission::{
    LiveRuntimeHealthEvent, LiveRuntimeHealthKind, RegistrationCommand, RegistrationFailure,
    ShardCommand,
};
use super::{LiveRouteConfig, system_timestamp};
use crate::authority::{RuntimeLease, ShardLeaseOwner};
use crate::processor::{
    GenerationAuthorityRegistry, GenerationRegistryExitHandle, InstrumentLiveProcessor,
    LiveApplyError, ProcessorLivenessBinding, ProcessorSnapshotLimits, ProcessorSnapshotSeed,
};
use crate::snapshot::{SnapshotBuildError, SnapshotPublisher};
use crate::{
    RouteSnapshot, ShardId, ShardLifecycleSnapshot, ShardRoutingVersion, ShardSnapshot,
    SnapshotDimension, SnapshotLimits,
};

use snapshot::route_from_seed;

#[derive(Debug)]
struct RouteOwner {
    processor: InstrumentLiveProcessor<crate::authority::SystemTrustedClock>,
    generations: GenerationAuthorityRegistry,
}

/// All already-allocated actor inputs; construction performs no late route discovery.
#[derive(Debug)]
pub(crate) struct ShardActorInput {
    pub(crate) shard: ShardId,
    pub(crate) routing_version: ShardRoutingVersion,
    pub(crate) runtime_incarnation: NonZeroU64,
    pub(crate) runtime: RuntimeLease,
    pub(crate) shard_owner: ShardLeaseOwner,
    pub(crate) routes: Vec<LiveRouteConfig>,
    pub(crate) maximum_sources_per_route: usize,
    pub(crate) mailbox: mpsc::Receiver<ShardCommand>,
    pub(crate) registrations: mpsc::Receiver<RegistrationCommand>,
    pub(crate) snapshot_limits: SnapshotLimits,
    pub(crate) snapshot_interval: std::time::Duration,
    pub(crate) snapshot_event_budget: usize,
    pub(crate) publisher: SnapshotPublisher,
    pub(crate) cancellation: CancellationToken,
    pub(crate) health: mpsc::Sender<LiveRuntimeHealthEvent>,
    pub(crate) ready: Option<oneshot::Sender<Result<(), ActorStartFailure>>>,
    pub(crate) startup_release: oneshot::Receiver<()>,
}

/// Actor task completion returned to the runtime supervisor.
#[derive(Debug)]
pub(crate) struct ActorCompletion {
    pub(crate) shard: ShardId,
    pub(crate) result: Result<(), ActorError>,
}

/// Runs one shard until cancellation or channel closure; no mutable state escapes this future.
pub(crate) async fn run(input: ShardActorInput) -> ActorCompletion {
    let shard = input.shard;
    let mut input = input;
    let ready = input.ready.take();
    let result = ShardActor::try_new(input);
    let result = match result {
        Ok(mut actor) => {
            if let Err(error) = actor.prepare_ready() {
                if let Some(ready) = ready {
                    let _ = ready.send(Err(ActorStartFailure::Initialization));
                }
                return ActorCompletion {
                    shard,
                    result: Err(error),
                };
            }
            let Some(ready) = ready else {
                return ActorCompletion {
                    shard,
                    result: Err(ActorError::StartupReceiverDropped),
                };
            };
            if ready.send(Ok(())).is_err() {
                Err(ActorError::StartupReceiverDropped)
            } else if (&mut actor.startup_release).await.is_err() {
                Err(ActorError::StartupReleaseDropped)
            } else {
                actor.run_loop().await
            }
        }
        Err(error) => {
            if let Some(ready) = ready {
                let _ = ready.send(Err(ActorStartFailure::Initialization));
            }
            Err(error)
        }
    };
    ActorCompletion { shard, result }
}

#[derive(Debug)]
struct ActorExitGuard {
    shard_owner: ShardLeaseOwner,
    runtime: RuntimeLease,
    generation_handles: Vec<GenerationRegistryExitHandle>,
}

impl Drop for ActorExitGuard {
    fn drop(&mut self) {
        self.invalidate();
    }
}

impl ActorExitGuard {
    fn validate(&self) -> Result<(), ActorError> {
        self.shard_owner
            .lease()
            .validate()
            .map_err(|_| ActorError::ShardClosed)?;
        self.runtime
            .validate()
            .map_err(|_| ActorError::RuntimeClosed)
    }

    fn invalidate(&mut self) {
        self.shard_owner.invalidate();
        self.runtime.invalidate();
        for handle in &self.generation_handles {
            handle.invalidate();
        }
    }
}

#[derive(Debug)]
struct ShardActor {
    shard: ShardId,
    routing_version: ShardRoutingVersion,
    runtime_incarnation: NonZeroU64,
    routes: HashMap<crate::ShardKey, RouteOwner>,
    mailbox: mpsc::Receiver<ShardCommand>,
    registrations: mpsc::Receiver<RegistrationCommand>,
    snapshot_limits: SnapshotLimits,
    snapshot_interval: std::time::Duration,
    snapshot_event_budget: usize,
    publisher: SnapshotPublisher,
    cancellation: CancellationToken,
    health: mpsc::Sender<LiveRuntimeHealthEvent>,
    snapshot_revision: NonZeroU64,
    health_revision: u64,
    events_since_snapshot: usize,
    dirty: bool,
    terminal_health_emitted: bool,
    observed_notification_drops: u64,
    startup_release: oneshot::Receiver<()>,
    _guard: ActorExitGuard,
}

impl Drop for ShardActor {
    fn drop(&mut self) {
        self._guard.invalidate();
        for owner in self.routes.values_mut() {
            owner.generations.invalidate_all();
            owner.processor.invalidate_for_exit();
        }
        self.emit_terminal_health();
    }
}

#[derive(Debug)]
enum ActorLoopEvent {
    Cancelled,
    Registration(Option<RegistrationCommand>),
    Market(Option<ShardCommand>),
    SnapshotTick,
}

impl ShardActor {
    fn try_new(input: ShardActorInput) -> Result<Self, ActorError> {
        let shard_lease = input.shard_owner.lease();
        let liveness = ProcessorLivenessBinding::new(shard_lease, input.runtime.clone());
        let mut routes = HashMap::new();
        routes
            .try_reserve(input.routes.len())
            .map_err(|_| ActorError::Allocation)?;
        let mut handles = Vec::new();
        handles
            .try_reserve_exact(input.routes.len())
            .map_err(|_| ActorError::Allocation)?;
        for route in input.routes {
            let generations =
                GenerationAuthorityRegistry::try_new(input.maximum_sources_per_route)?;
            handles.push(generations.exit_handle());
            let processor = InstrumentLiveProcessor::new_system(
                route.definition().clone(),
                route.depth(),
                route.nonce_capacity().get(),
                route.nonce_reclaim_budget().get(),
                route.maximum_capability_lifetime(),
                liveness.clone(),
            )?;
            if routes
                .insert(
                    route.route().clone(),
                    RouteOwner {
                        processor,
                        generations,
                    },
                )
                .is_some()
            {
                return Err(ActorError::DuplicateRoute);
            }
        }
        Ok(Self {
            shard: input.shard,
            routing_version: input.routing_version,
            runtime_incarnation: input.runtime_incarnation,
            routes,
            mailbox: input.mailbox,
            registrations: input.registrations,
            snapshot_limits: input.snapshot_limits,
            snapshot_interval: input.snapshot_interval,
            snapshot_event_budget: input.snapshot_event_budget,
            publisher: input.publisher,
            cancellation: input.cancellation,
            health: input.health,
            snapshot_revision: NonZeroU64::MIN,
            health_revision: 0,
            events_since_snapshot: 0,
            dirty: true,
            terminal_health_emitted: false,
            observed_notification_drops: 0,
            startup_release: input.startup_release,
            _guard: ActorExitGuard {
                shard_owner: input.shard_owner,
                runtime: input.runtime,
                generation_handles: handles,
            },
        })
    }

    async fn run_loop(mut self) -> Result<(), ActorError> {
        let mut interval = tokio::time::interval(self.snapshot_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate first tick because readiness already published the complete state.
        interval.tick().await;
        let mut mailbox_open = true;
        let mut registrations_open = true;
        let mut prefer_registration = true;
        loop {
            if !mailbox_open && !registrations_open {
                break;
            }
            match self
                .next_event(
                    prefer_registration,
                    registrations_open,
                    mailbox_open,
                    &mut interval,
                )
                .await
            {
                ActorLoopEvent::Cancelled => break,
                ActorLoopEvent::Registration(command) => match command {
                    Some(command) => {
                        self.register(command);
                        prefer_registration = false;
                    }
                    None => registrations_open = false,
                },
                ActorLoopEvent::Market(command) => match command {
                    Some(command) => {
                        self.process(command)?;
                        prefer_registration = true;
                    }
                    None => mailbox_open = false,
                },
                ActorLoopEvent::SnapshotTick => {
                    if self.dirty {
                        self.publish_snapshot(ShardLifecycleSnapshot::Ready)?;
                    }
                }
            }
        }
        self.mailbox.close();
        self.registrations.close();
        while let Ok(command) = self.mailbox.try_recv() {
            command.admission.invalidate_on_admission_failure();
        }
        self._guard.invalidate();
        for owner in self.routes.values_mut() {
            owner.generations.invalidate_all();
            owner.processor.invalidate_for_exit();
        }
        self.publish_snapshot(ShardLifecycleSnapshot::Stopped)?;
        self.emit_terminal_health();
        Ok(())
    }

    async fn next_event(
        &mut self,
        prefer_registration: bool,
        registrations_open: bool,
        mailbox_open: bool,
        interval: &mut tokio::time::Interval,
    ) -> ActorLoopEvent {
        if prefer_registration {
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => ActorLoopEvent::Cancelled,
                _ = interval.tick() => ActorLoopEvent::SnapshotTick,
                command = self.registrations.recv(), if registrations_open => {
                    ActorLoopEvent::Registration(command)
                }
                command = self.mailbox.recv(), if mailbox_open => ActorLoopEvent::Market(command),
            }
        } else {
            tokio::select! {
                biased;
                () = self.cancellation.cancelled() => ActorLoopEvent::Cancelled,
                _ = interval.tick() => ActorLoopEvent::SnapshotTick,
                command = self.mailbox.recv(), if mailbox_open => ActorLoopEvent::Market(command),
                command = self.registrations.recv(), if registrations_open => {
                    ActorLoopEvent::Registration(command)
                }
            }
        }
    }

    fn prepare_ready(&mut self) -> Result<(), ActorError> {
        self.publish_snapshot(ShardLifecycleSnapshot::Ready)?;
        self.emit_health(LiveRuntimeHealthKind::ShardReady, None);
        Ok(())
    }

    fn register(&mut self, command: RegistrationCommand) {
        let result = self.register_inner(&command);
        if result.is_err() {
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(
                LiveRuntimeHealthKind::GenerationRejected,
                Some(command.route.clone()),
            );
        }
        if let Err(Ok(admission)) = command.response.send(result) {
            admission.invalidate_on_admission_failure();
        }
    }

    fn register_inner(
        &mut self,
        command: &RegistrationCommand,
    ) -> Result<crate::processor::GenerationAdmission, RegistrationFailure> {
        let now = system_timestamp().map_err(|_| RegistrationFailure::NotCurrent)?;
        let owner = self
            .routes
            .get_mut(&command.route)
            .ok_or(RegistrationFailure::UnknownRoute)?;
        owner
            .generations
            .bind_current(&command.source, now)
            .map_err(|error| match error {
                LiveApplyError::GenerationCapacityExhausted => RegistrationFailure::Capacity,
                _ => RegistrationFailure::NotCurrent,
            })
    }

    fn process(&mut self, command: ShardCommand) -> Result<(), ActorError> {
        let admission = command.admission.clone();
        match self.process_inner(command) {
            Ok(()) => Ok(()),
            Err(error) => {
                admission.invalidate_on_admission_failure();
                self.health_revision = self.health_revision.saturating_add(1);
                self.emit_health(LiveRuntimeHealthKind::ProcessingRejected, None);
                if error.is_fatal() { Err(error) } else { Ok(()) }
            }
        }
    }

    fn process_inner(&mut self, command: ShardCommand) -> Result<(), ActorError> {
        self._guard.validate()?;
        let now = system_timestamp().map_err(|_| ActorError::ClockRange)?;
        command
            .admission
            .validate_at(now)
            .map_err(|_| ActorError::GenerationNotCurrent)?;
        let key = crate::ShardKey::new(
            command.batch.key().venue().clone(),
            command.batch.key().instrument(),
        );
        let admission = command.admission.clone();
        let _retained_bytes = command.retained_bytes;
        let mut publish_after_batch = false;
        {
            let Some(owner) = self.routes.get_mut(&key) else {
                admission.invalidate_on_admission_failure();
                return Err(ActorError::UnknownRoute);
            };
            let mut cursor = match owner
                .processor
                .accept_batch(command.batch, &command.admission)
            {
                Ok(cursor) => cursor,
                Err(error) => {
                    admission.invalidate_on_admission_failure();
                    return Err(error.into());
                }
            };
            loop {
                let applied = match owner.processor.apply_next(&mut cursor) {
                    Ok(Some(applied)) => applied,
                    Ok(None) => break,
                    Err(error) => {
                        admission.invalidate_on_admission_failure();
                        return Err(error.into());
                    }
                };
                if let Some(authority) = applied.authority.as_ref() {
                    owner.processor.validate_applied_current(authority)?;
                    // Task 9's bounded feature hook linearizes here.
                    owner.processor.validate_applied_current(authority)?;
                    // Task 10 returns NoStrategy in Task 8, so no capability is minted.
                }
                self.events_since_snapshot = self.events_since_snapshot.saturating_add(1);
                self.dirty = true;
                publish_after_batch |= self.events_since_snapshot >= self.snapshot_event_budget;
            }
        }
        if publish_after_batch {
            self.publish_snapshot(ShardLifecycleSnapshot::Ready)?;
        }
        Ok(())
    }

    fn publish_snapshot(&mut self, lifecycle: ShardLifecycleSnapshot) -> Result<(), ActorError> {
        let next = self
            .snapshot_revision
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or(SnapshotBuildError::RevisionExhausted)?;
        let evaluated_at = system_timestamp().map_err(|_| SnapshotBuildError::ClockRange)?;
        let mut route_keys = self.routes.keys().cloned().collect::<Vec<_>>();
        route_keys.sort_by(|left, right| {
            left.venue()
                .as_str()
                .cmp(right.venue().as_str())
                .then_with(|| left.instrument().cmp(&right.instrument()))
        });
        let available_routes = route_keys.len();
        let route_limit = self.snapshot_limits.maximum_routes().get();
        let mut routes = Vec::new();
        routes
            .try_reserve(route_limit.min(available_routes))
            .map_err(|_| ActorError::Allocation)?;
        let mut retained_bytes = std::mem::size_of::<ShardSnapshot>();
        for key in route_keys.into_iter().take(route_limit) {
            let remaining = usize::try_from(self.snapshot_limits.maximum_retained_bytes().get())
                .map_err(|_| SnapshotBuildError::RetainedSizeOverflow)?
                .checked_sub(retained_bytes)
                .ok_or(SnapshotBuildError::RetainedSizeOverflow)?;
            let minimum = std::mem::size_of::<ProcessorSnapshotSeed>();
            if remaining < minimum {
                break;
            }
            let owner = self.routes.get(&key).ok_or(ActorError::UnknownRoute)?;
            let seed = owner
                .processor
                .snapshot_seed(ProcessorSnapshotLimits::try_new(
                    self.snapshot_limits.maximum_streams_per_route().get(),
                    self.snapshot_limits.maximum_statuses_per_route().get(),
                    self.snapshot_limits.maximum_levels_per_side().get() as usize,
                    remaining,
                )?)?;
            let candidate_retained_bytes = retained_bytes
                .checked_add(seed.retained_bytes)
                .and_then(|value| value.checked_add(std::mem::size_of::<RouteSnapshot>()))
                .and_then(|value| value.checked_add(key.venue().as_str().len()))
                .ok_or(SnapshotBuildError::RetainedSizeOverflow)?;
            if candidate_retained_bytes
                > self.snapshot_limits.maximum_retained_bytes().get() as usize
            {
                break;
            }
            retained_bytes = candidate_retained_bytes;
            routes.push(route_from_seed(key, seed)?);
        }
        let route_dimension =
            SnapshotDimension::from_counts(available_routes, routes.len(), route_limit)?;
        let published_at = system_timestamp().map_err(|_| SnapshotBuildError::ClockRange)?;
        let snapshot = ShardSnapshot {
            routing_version: self.routing_version,
            shard_count: self.shard.count(),
            runtime_incarnation: self.runtime_incarnation,
            shard_id: self.shard,
            snapshot_revision: next,
            health_revision: self.health_revision,
            lifecycle,
            evaluated_at,
            published_at,
            routes: routes.into_boxed_slice(),
            route_dimension,
            retained_bytes: u64::try_from(retained_bytes)
                .map_err(|_| SnapshotBuildError::RetainedSizeOverflow)?,
        };
        self.publisher.publish(snapshot)?;
        let notification_drops = self.publisher.dropped_notifications();
        if notification_drops > self.observed_notification_drops {
            self.observed_notification_drops = notification_drops;
            self.health_revision = self.health_revision.saturating_add(1);
            self.emit_health(LiveRuntimeHealthKind::SnapshotNotificationDropped, None);
        }
        self.snapshot_revision = next;
        self.events_since_snapshot = 0;
        self.dirty = false;
        Ok(())
    }

    fn emit_health(&self, kind: LiveRuntimeHealthKind, route: Option<crate::ShardKey>) {
        let Ok(observed_at) = system_timestamp() else {
            return;
        };
        let _ = self.health.try_send(LiveRuntimeHealthEvent::new(
            kind,
            self.shard,
            route,
            observed_at,
        ));
    }

    fn emit_terminal_health(&mut self) {
        if !self.terminal_health_emitted {
            self.terminal_health_emitted = true;
            self.emit_health(LiveRuntimeHealthKind::ShardExited, None);
        }
    }
}

/// Startup handshake failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum ActorStartFailure {
    #[error("actor could not initialize its bounded route state")]
    Initialization,
}

/// Actor-owned processing or snapshot failure.
#[derive(Debug, Error)]
pub(crate) enum ActorError {
    #[error("actor bounded allocation failed")]
    Allocation,
    #[error("actor route table contained a duplicate route")]
    DuplicateRoute,
    #[error("actor received a command for an unknown route")]
    UnknownRoute,
    #[error("actor received a command whose generation is no longer current")]
    GenerationNotCurrent,
    #[error("runtime incarnation is closed")]
    RuntimeClosed,
    #[error("shard liveness is closed")]
    ShardClosed,
    #[error("runtime startup receiver dropped before readiness")]
    StartupReceiverDropped,
    #[error("runtime startup release dropped after shard readiness")]
    StartupReleaseDropped,
    #[error("trusted system clock is outside the supported range")]
    ClockRange,
    #[error(transparent)]
    Apply(#[from] LiveApplyError),
    #[error(transparent)]
    Authority(#[from] crate::AuthorityError),
    #[error(transparent)]
    Snapshot(#[from] SnapshotBuildError),
    #[error(transparent)]
    Publish(#[from] crate::snapshot::SnapshotPublishError),
}

impl ActorError {
    fn is_fatal(&self) -> bool {
        match self {
            Self::Allocation
            | Self::DuplicateRoute
            | Self::UnknownRoute
            | Self::RuntimeClosed
            | Self::ShardClosed
            | Self::ClockRange
            | Self::Snapshot(_)
            | Self::Publish(_)
            | Self::StartupReceiverDropped
            | Self::StartupReleaseDropped => true,
            Self::Apply(error) => error.is_fatal_to_actor(),
            Self::Authority(error) => matches!(
                error,
                crate::AuthorityError::RevisionExhausted
                    | crate::AuthorityError::NonceRegistryInitialization
                    | crate::AuthorityError::NonceInvariant
                    | crate::AuthorityError::ClockRange
            ),
            Self::GenerationNotCurrent => false,
        }
    }
}
