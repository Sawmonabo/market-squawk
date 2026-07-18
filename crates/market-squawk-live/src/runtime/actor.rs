//! Single-writer shard actor ownership and event/action linearization.

use std::collections::HashMap;
use std::num::NonZeroU64;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

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
use crate::provider_book::BookProcessingScratch;
use crate::snapshot::{SnapshotBuildError, SnapshotPublisher};
use crate::{
    RouteSnapshot, ShardId, ShardLifecycleSnapshot, ShardRoutingVersion, ShardSnapshot,
    SnapshotDimension, SnapshotLimits,
};

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
    pub(crate) maximum_streams_per_route: usize,
    pub(crate) maximum_book_items_per_message: usize,
    pub(crate) mailbox: mpsc::Receiver<ShardCommand>,
    pub(crate) registrations: mpsc::Receiver<RegistrationCommand>,
    pub(crate) snapshot_limits: SnapshotLimits,
    pub(crate) snapshot_interval: std::time::Duration,
    pub(crate) snapshot_event_trigger: usize,
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
    book_scratch: BookProcessingScratch,
    mailbox: mpsc::Receiver<ShardCommand>,
    registrations: mpsc::Receiver<RegistrationCommand>,
    snapshot_limits: SnapshotLimits,
    snapshot_interval: std::time::Duration,
    snapshot_event_trigger: usize,
    publisher: SnapshotPublisher,
    cancellation: CancellationToken,
    health: mpsc::Sender<LiveRuntimeHealthEvent>,
    snapshot_revision: NonZeroU64,
    health_revision: u64,
    events_since_snapshot: usize,
    dirty: bool,
    fair_turn: FairTurn,
    snapshot_pending: bool,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FairTurn {
    Registration,
    Market,
    Snapshot,
}

impl FairTurn {
    const fn next(self) -> Self {
        match self {
            Self::Registration => Self::Market,
            Self::Market => Self::Snapshot,
            Self::Snapshot => Self::Registration,
        }
    }
}

#[derive(Debug)]
enum FairEvent<R, M> {
    Cancelled,
    Registration(Option<R>),
    Market(Option<M>),
    SnapshotDue,
    SnapshotPublish,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotSchedule {
    Due,
    Publish,
}

struct FairSources<'a, R, M> {
    cancellation: &'a CancellationToken,
    registrations: &'a mut mpsc::Receiver<R>,
    mailbox: &'a mut mpsc::Receiver<M>,
    registrations_open: bool,
    mailbox_open: bool,
    interval: &'a mut tokio::time::Interval,
}

async fn select_fair_event<R, M>(
    turn: FairTurn,
    snapshot_pending: bool,
    sources: FairSources<'_, R, M>,
) -> FairEvent<R, M> {
    async fn snapshot_event(
        snapshot_pending: bool,
        interval: &mut tokio::time::Interval,
    ) -> SnapshotSchedule {
        if snapshot_pending {
            SnapshotSchedule::Publish
        } else {
            interval.tick().await;
            SnapshotSchedule::Due
        }
    }

    match turn {
        FairTurn::Registration => {
            tokio::select! {
                biased;
                () = sources.cancellation.cancelled() => FairEvent::Cancelled,
                command = sources.registrations.recv(), if sources.registrations_open => {
                    FairEvent::Registration(command)
                }
                command = sources.mailbox.recv(), if sources.mailbox_open => {
                    FairEvent::Market(command)
                },
                event = snapshot_event(snapshot_pending, sources.interval) => match event {
                    SnapshotSchedule::Due => FairEvent::SnapshotDue,
                    SnapshotSchedule::Publish => FairEvent::SnapshotPublish,
                },
            }
        }
        FairTurn::Market => {
            tokio::select! {
                biased;
                () = sources.cancellation.cancelled() => FairEvent::Cancelled,
                command = sources.mailbox.recv(), if sources.mailbox_open => {
                    FairEvent::Market(command)
                },
                event = snapshot_event(snapshot_pending, sources.interval) => match event {
                    SnapshotSchedule::Due => FairEvent::SnapshotDue,
                    SnapshotSchedule::Publish => FairEvent::SnapshotPublish,
                },
                command = sources.registrations.recv(), if sources.registrations_open => {
                    FairEvent::Registration(command)
                }
            }
        }
        FairTurn::Snapshot => {
            tokio::select! {
                biased;
                () = sources.cancellation.cancelled() => FairEvent::Cancelled,
                event = snapshot_event(snapshot_pending, sources.interval) => match event {
                    SnapshotSchedule::Due => FairEvent::SnapshotDue,
                    SnapshotSchedule::Publish => FairEvent::SnapshotPublish,
                },
                command = sources.registrations.recv(), if sources.registrations_open => {
                    FairEvent::Registration(command)
                }
                command = sources.mailbox.recv(), if sources.mailbox_open => {
                    FairEvent::Market(command)
                },
            }
        }
    }
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
                input.maximum_streams_per_route,
                input.maximum_sources_per_route,
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
        let book_scratch = BookProcessingScratch::try_new(input.maximum_book_items_per_message)
            .map_err(|_| ActorError::Allocation)?;
        Ok(Self {
            shard: input.shard,
            routing_version: input.routing_version,
            runtime_incarnation: input.runtime_incarnation,
            routes,
            book_scratch,
            mailbox: input.mailbox,
            registrations: input.registrations,
            snapshot_limits: input.snapshot_limits,
            snapshot_interval: input.snapshot_interval,
            snapshot_event_trigger: input.snapshot_event_trigger,
            publisher: input.publisher,
            cancellation: input.cancellation,
            health: input.health,
            snapshot_revision: NonZeroU64::MIN,
            health_revision: 0,
            events_since_snapshot: 0,
            dirty: true,
            fair_turn: FairTurn::Registration,
            snapshot_pending: false,
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
        loop {
            if !mailbox_open && !registrations_open {
                break;
            }
            let event = select_fair_event(
                self.fair_turn,
                self.snapshot_pending,
                FairSources {
                    cancellation: &self.cancellation,
                    registrations: &mut self.registrations,
                    mailbox: &mut self.mailbox,
                    registrations_open,
                    mailbox_open,
                    interval: &mut interval,
                },
            )
            .await;
            if !matches!(event, FairEvent::Cancelled) {
                self.fair_turn = self.fair_turn.next();
            }
            match event {
                FairEvent::Cancelled => break,
                FairEvent::Registration(command) => match command {
                    Some(command) => {
                        self.register(command);
                    }
                    None => registrations_open = false,
                },
                FairEvent::Market(command) => match command {
                    Some(command) => {
                        self.process(command)?;
                    }
                    None => mailbox_open = false,
                },
                FairEvent::SnapshotDue => self.snapshot_pending = true,
                FairEvent::SnapshotPublish => {
                    self.snapshot_pending = false;
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
                let applied = match owner
                    .processor
                    .apply_next(&mut cursor, &mut self.book_scratch)
                {
                    Ok(Some(applied)) => applied,
                    Ok(None) => break,
                    Err(error) => {
                        admission.invalidate_on_admission_failure();
                        return Err(error.into());
                    }
                };
                if let Some(authority) = applied.authority.as_ref() {
                    owner.processor.validate_applied_current(authority)?;
                    // The bounded feature hook linearizes here.
                    owner.processor.validate_applied_current(authority)?;
                    // NoStrategy produces no order intent, so no capability is minted.
                }
                self.events_since_snapshot = self.events_since_snapshot.saturating_add(1);
                self.dirty = true;
                publish_after_batch |= self.events_since_snapshot >= self.snapshot_event_trigger;
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
            routes.push(seed.into_route(key));
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

#[cfg(test)]
mod fairness_tests {
    use super::{FairEvent, FairSources, FairTurn, select_fair_event};
    use tokio::sync::mpsc;
    use tokio::time::{Duration, advance};
    use tokio_util::sync::CancellationToken;

    #[tokio::test(start_paused = true)]
    async fn perpetually_ready_snapshot_work_services_both_queues_within_one_rotation() {
        let (registrations, mut registration_rx) = mpsc::channel(1);
        let (market, mut market_rx) = mpsc::channel(1);
        assert!(registrations.send(11_u8).await.is_ok());
        assert!(market.send(22_u8).await.is_ok());
        let cancellation = CancellationToken::new();
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        interval.tick().await;
        advance(Duration::from_secs(1)).await;

        let mut turn = FairTurn::Snapshot;
        let mut saw_registration = false;
        let mut saw_market = false;
        let mut unexpected = None;
        for _ in 0..3 {
            let event = select_fair_event(
                turn,
                true,
                FairSources {
                    cancellation: &cancellation,
                    registrations: &mut registration_rx,
                    mailbox: &mut market_rx,
                    registrations_open: true,
                    mailbox_open: true,
                    interval: &mut interval,
                },
            )
            .await;
            turn = turn.next();
            match event {
                FairEvent::SnapshotPublish => {}
                FairEvent::Registration(Some(11)) => saw_registration = true,
                FairEvent::Market(Some(22)) => saw_market = true,
                other => unexpected = Some(format!("{other:?}")),
            }
        }
        assert!(unexpected.is_none(), "unexpected event: {unexpected:?}");
        assert!(saw_registration);
        assert!(saw_market);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wins_when_snapshot_and_both_queues_are_ready() {
        let (registrations, mut registration_rx) = mpsc::channel(1);
        let (market, mut market_rx) = mpsc::channel(1);
        assert!(registrations.send(1_u8).await.is_ok());
        assert!(market.send(2_u8).await.is_ok());
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut interval = tokio::time::interval(Duration::from_millis(1));
        interval.tick().await;
        advance(Duration::from_secs(1)).await;

        assert!(matches!(
            select_fair_event(
                FairTurn::Snapshot,
                true,
                FairSources {
                    cancellation: &cancellation,
                    registrations: &mut registration_rx,
                    mailbox: &mut market_rx,
                    registrations_open: true,
                    mailbox_open: true,
                    interval: &mut interval,
                },
            )
            .await,
            FairEvent::Cancelled
        ));
    }
}
