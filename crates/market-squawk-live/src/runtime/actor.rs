//! Single-writer shard actor ownership and event/action linearization.

use std::collections::HashMap;
use std::num::NonZeroU64;

use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::admission::{
    LiveRuntimeHealthEvent, LiveRuntimeHealthKind, RegistrationCommand, ShardCommand,
};
use super::{LiveFeatureCapacity, LiveRouteConfig, system_timestamp};
use crate::authority::{RuntimeLease, ShardLeaseOwner};
use crate::cross_venue::{
    CrossVenuePlaneHandle, CrossVenueRoutePublisher, CrossVenueRuntimeReader,
};
use crate::features::RouteFeatureState;
use crate::processor::{
    GenerationAuthorityRegistry, GenerationRegistryExitHandle, InstrumentLiveProcessor,
    LiveApplyError, ProcessorLivenessBinding,
};
use crate::provider_book::BookProcessingScratch;
use crate::snapshot::{SnapshotBuildError, SnapshotPublisher};
use crate::{ShardId, ShardLifecycleSnapshot, ShardRoutingVersion, SnapshotLimits};

#[path = "actor/processing.rs"]
mod processing;
#[path = "actor/scheduling.rs"]
mod scheduling;
#[path = "actor/snapshot_publication.rs"]
mod snapshot_publication;

use scheduling::{FairEvent, FairSources, FairTurn, select_fair_event};

#[derive(Debug)]
struct RouteOwner {
    processor: InstrumentLiveProcessor<crate::authority::SystemTrustedClock>,
    generations: GenerationAuthorityRegistry,
    features: RouteFeatureState,
    action_hook: Option<crate::RouteActionHook>,
    qualified_market_export: Option<crate::RouteQualifiedMarketExport>,
    cross_venue_publisher: Option<CrossVenueRoutePublisher>,
    cross_venue_reader: Option<CrossVenueRuntimeReader>,
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
    pub(crate) action_hooks: Vec<crate::RouteActionHook>,
    pub(crate) qualified_market_exports: Vec<crate::RouteQualifiedMarketExport>,
    pub(crate) maximum_action_hook_bytes_per_route: usize,
    pub(crate) maximum_sources_per_route: usize,
    pub(crate) maximum_streams_per_route: usize,
    pub(crate) feature_capacity: LiveFeatureCapacity,
    pub(crate) cross_venue: CrossVenuePlaneHandle,
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
    maximum_feature_snapshot_bytes: usize,
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
        let mut action_hooks = HashMap::new();
        action_hooks
            .try_reserve(input.action_hooks.len())
            .map_err(|_| ActorError::Allocation)?;
        for hook in input.action_hooks {
            hook.validate_retained_bytes(input.maximum_action_hook_bytes_per_route)?;
            let route = hook.route().clone();
            if action_hooks.insert(route, hook).is_some() {
                return Err(ActorError::DuplicateActionHook);
            }
        }
        let mut qualified_market_exports = HashMap::new();
        qualified_market_exports
            .try_reserve(input.qualified_market_exports.len())
            .map_err(|_| ActorError::Allocation)?;
        for exporter in input.qualified_market_exports {
            let route = exporter.route().clone();
            if qualified_market_exports.insert(route, exporter).is_some() {
                return Err(ActorError::DuplicateQualifiedMarketExport);
            }
        }
        for route in input.routes {
            let cross_venue = input.cross_venue.route(route.route());
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
            let features = RouteFeatureState::try_new(input.feature_capacity, route.depth())?;
            let route_key = route.route().clone();
            let action_hook = action_hooks.remove(&route_key);
            let qualified_market_export = qualified_market_exports.remove(&route_key);
            if routes
                .insert(
                    route_key.clone(),
                    RouteOwner {
                        processor,
                        generations,
                        features,
                        action_hook,
                        qualified_market_export,
                        cross_venue_publisher: cross_venue
                            .as_ref()
                            .map(|(publisher, _)| publisher.clone()),
                        cross_venue_reader: cross_venue.map(|(_, reader)| reader),
                    },
                )
                .is_some()
            {
                return Err(ActorError::DuplicateRoute);
            }
        }
        if !action_hooks.is_empty() {
            return Err(ActorError::UnknownActionHook);
        }
        if !qualified_market_exports.is_empty() {
            return Err(ActorError::UnknownQualifiedMarketExport);
        }
        let book_scratch = BookProcessingScratch::try_new(input.maximum_book_items_per_message)
            .map_err(|_| ActorError::Allocation)?;
        let maximum_feature_snapshot_bytes =
            usize::try_from(input.feature_capacity.maximum_feature_snapshot_bytes.get())
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
            maximum_feature_snapshot_bytes,
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
            if let Ok(observed_at) = system_timestamp() {
                owner.features.invalidate_all(
                    crate::FeatureInvalidationReason::SourceReplacement,
                    observed_at,
                )?;
            }
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
    #[error("actor received duplicate action hook ownership")]
    DuplicateActionHook,
    #[error("actor received action hook ownership for an unknown route")]
    UnknownActionHook,
    #[error("actor received duplicate qualified-market export ownership")]
    DuplicateQualifiedMarketExport,
    #[error("actor received qualified-market export ownership for an unknown route")]
    UnknownQualifiedMarketExport,
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
    #[error(transparent)]
    Feature(#[from] crate::RouteFeatureError),
    #[error(transparent)]
    ActionHook(#[from] crate::RouteActionHookError),
}

impl ActorError {
    fn is_fatal(&self) -> bool {
        match self {
            Self::Allocation
            | Self::DuplicateRoute
            | Self::DuplicateActionHook
            | Self::UnknownActionHook
            | Self::DuplicateQualifiedMarketExport
            | Self::UnknownQualifiedMarketExport
            | Self::UnknownRoute
            | Self::RuntimeClosed
            | Self::ShardClosed
            | Self::ClockRange
            | Self::Feature(_)
            | Self::ActionHook(_)
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
