//! Complete-startup supervision, runtime replacement, and bounded shutdown.

use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures_util::stream::{FuturesUnordered, StreamExt};
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::{Id, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::actor::{ActorCompletion, ActorStartFailure, ShardActorInput, run};
use super::admission::{
    ActionControlChannels, ActionHookControlFailure, ActionHookInstallCommand,
    ActionHookRemoveCommand, ActorControlCommand, LiveRuntimeHealthEvent, LiveRuntimeIngress,
    RouteIngressChannels, ShardCommand,
};
use super::{LiveRouteConfig, LiveRuntimeConfig, LiveRuntimeConfigError, system_timestamp};
use crate::authority::{RuntimeLeaseOwner, ShardLeaseOwner};
use crate::cross_venue::create_cross_venue_plane;
use crate::snapshot::{SnapshotPlaneBundle, create_snapshot_plane};
use crate::{
    ActionHookActivationLease, LiveActionControlError, LiveActionControlRejection,
    LiveActionHookGeneration, LiveActionHookReapReceipt, LiveSnapshotReader,
    PreparedLiveActionHookGroup, RouteActionHook, RouteActionHookError, RouteQualifiedMarketExport,
    ShardId, ShardKey, ShardLifecycleSnapshot, ShardRouter, ShardSnapshot, SnapshotDimension,
    SnapshotReadError,
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
    action_controls: Box<[ActionControlChannels]>,
    dynamic_action_group: Option<DynamicActionGroupRecord>,
    startup_action_hooks: bool,
    next_action_hook_generation: u64,
    cancellation: CancellationToken,
    actors: Option<JoinSet<ActorCompletion>>,
    cross_venue_task: Option<tokio::task::JoinHandle<()>>,
    task_shards: HashMap<Id, ShardId>,
}

impl LiveRuntime {
    /// Starts an explicitly market-data-only runtime with no action hooks.
    ///
    /// This compatibility constructor cannot invoke strategies or issue execution capabilities.
    pub async fn start(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
    ) -> Result<Self, LiveRuntimeStartError> {
        Self::start_inner(config, routes, None, Vec::new()).await
    }

    /// Starts a runtime only after every configured route transfers one exact action hook.
    ///
    /// # Errors
    ///
    /// Fails before actor release when a hook is missing, duplicated, unknown, transplanted, or
    /// exceeds the already-reserved route footprint.
    pub async fn start_with_action_hooks(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        action_hooks: Vec<RouteActionHook>,
    ) -> Result<Self, LiveRuntimeStartError> {
        Self::start_inner(config, routes, Some(action_hooks), Vec::new()).await
    }

    /// Starts a market-data runtime with opt-in bounded post-decision observation exports.
    pub async fn start_with_qualified_market_exports(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
    ) -> Result<Self, LiveRuntimeStartError> {
        Self::start_inner(config, routes, None, qualified_market_exports).await
    }

    /// Starts an action-enabled runtime with independently bounded post-decision exports.
    pub async fn start_with_action_hooks_and_qualified_market_exports(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        action_hooks: Vec<RouteActionHook>,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
    ) -> Result<Self, LiveRuntimeStartError> {
        Self::start_inner(config, routes, Some(action_hooks), qualified_market_exports).await
    }

    async fn start_inner(
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        action_hooks: Option<Vec<RouteActionHook>>,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
    ) -> Result<Self, LiveRuntimeStartError> {
        config.validate_routes(&routes)?;
        let startup_action_hooks = action_hooks.is_some();
        let mut action_hooks = validate_action_hooks(&config, &routes, action_hooks)?;
        let mut qualified_market_exports =
            validate_qualified_market_exports(&routes, qualified_market_exports)?;
        let export_bytes =
            qualified_market_exports
                .values()
                .try_fold(0_u64, |total, exporter| {
                    total
                        .checked_add(
                            u64::try_from(exporter.reserved_bytes().get())
                                .map_err(|_| LiveRuntimeStartError::Allocation)?,
                        )
                        .ok_or(LiveRuntimeStartError::Allocation)
                })?;
        let estimated_peak_bytes = config
            .estimated_peak_bytes(&routes)?
            .get()
            .checked_add(export_bytes)
            .and_then(NonZeroU64::new)
            .ok_or(LiveRuntimeStartError::Allocation)?;
        if estimated_peak_bytes > config.maximum_runtime_bytes() {
            return Err(LiveRuntimeStartError::QualifiedMarketExportMemoryExceeded);
        }
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
        let mut action_hook_byte_budgets = Vec::new();
        action_hook_byte_budgets
            .try_reserve_exact(partitions.len())
            .map_err(|_| LiveRuntimeStartError::Allocation)?;
        for shard_routes in &partitions {
            let permits = config
                .maximum_action_hook_bytes_per_route()
                .checked_mul(shard_routes.len())
                .ok_or(LiveRuntimeStartError::Allocation)?;
            if permits > Semaphore::MAX_PERMITS {
                return Err(LiveRuntimeStartError::Allocation);
            }
            action_hook_byte_budgets.push(permits);
        }
        let mut action_hook_partitions = (0..shard_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<RouteActionHook>>>();
        if let Some(validated_hooks) = action_hooks.as_mut() {
            for (shard_routes, shard_hooks) in partitions.iter().zip(&mut action_hook_partitions) {
                shard_hooks
                    .try_reserve_exact(shard_routes.len())
                    .map_err(|_| LiveRuntimeStartError::Allocation)?;
                for route in shard_routes {
                    let hook = validated_hooks.remove(route.route()).ok_or_else(|| {
                        LiveRuntimeStartError::MissingActionHook {
                            route: route.route().clone(),
                        }
                    })?;
                    shard_hooks.push(hook);
                }
            }
            if !validated_hooks.is_empty() {
                return Err(LiveRuntimeStartError::ActionHookPartitionInvariant);
            }
        }
        let mut qualified_market_export_partitions = (0..shard_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<RouteQualifiedMarketExport>>>();
        for (shard_routes, shard_exports) in partitions
            .iter()
            .zip(&mut qualified_market_export_partitions)
        {
            for route in shard_routes {
                if let Some(exporter) = qualified_market_exports.remove(route.route()) {
                    shard_exports.push(exporter);
                }
            }
        }
        if !qualified_market_exports.is_empty() {
            return Err(LiveRuntimeStartError::QualifiedMarketExportPartitionInvariant);
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
        let mut action_controls = Vec::new();
        action_controls
            .try_reserve_exact(shard_count)
            .map_err(|_| LiveRuntimeStartError::Allocation)?;

        for (
            (((shard, shard_routes), shard_action_hooks), shard_qualified_market_exports),
            action_hook_byte_permits,
        ) in shard_ids
            .into_iter()
            .zip(partitions)
            .zip(action_hook_partitions)
            .zip(qualified_market_export_partitions)
            .zip(action_hook_byte_budgets)
        {
            let shard_index = shard.index();
            let shard_owner = ShardLeaseOwner::new(u64::from(shard_index) + 1);
            let shard_liveness = shard_owner.lease();
            let byte_budget = Arc::new(Semaphore::new(mailbox_byte_permits));
            let (mailbox_sender, mailbox) =
                mpsc::channel::<ShardCommand>(config.mailbox_count_per_shard().get());
            let (control_sender, controls) =
                mpsc::channel(config.registration_control_capacity().get());
            let action_hook_byte_budget = Arc::new(Semaphore::new(action_hook_byte_permits));
            for route in &shard_routes {
                let channels = RouteIngressChannels {
                    shard,
                    runtime: runtime.clone(),
                    shard_liveness: shard_liveness.clone(),
                    mailbox: mailbox_sender.clone(),
                    byte_budget: Arc::clone(&byte_budget),
                    control: control_sender.clone(),
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
            action_controls.push(ActionControlChannels {
                shard,
                runtime: runtime.clone(),
                shard_liveness: shard_liveness.clone(),
                control: control_sender,
                byte_budget: action_hook_byte_budget,
            });
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
                action_hooks: shard_action_hooks,
                qualified_market_exports: shard_qualified_market_exports,
                maximum_action_hook_bytes_per_route: config.maximum_action_hook_bytes_per_route(),
                maximum_sources_per_route: config.maximum_sources_per_route().get(),
                maximum_streams_per_route: config.maximum_streams_per_route().get(),
                feature_capacity: config.feature_capacity(),
                cross_venue: cross_venue.clone(),
                maximum_book_items_per_message:
                    crate::provider_book::maximum_book_items_for_message(
                        config.maximum_message_bytes().get(),
                    ),
                mailbox,
                controls,
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
            action_controls: action_controls.into_boxed_slice(),
            dynamic_action_group: None,
            startup_action_hooks,
            next_action_hook_generation: 1,
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

    /// Installs one complete route-hook group into this already-running incarnation while disabled.
    ///
    /// Every affected shard acknowledges actor ownership before the returned non-cloneable token
    /// can activate the group. Installation never reconnects a feed or exports execution
    /// authority. If installation fails after a partial cross-shard transfer, the shared gate
    /// remains disabled and bounded rollback is attempted before this method returns.
    ///
    /// # Errors
    ///
    /// Rejects an empty, duplicate, unknown, oversized, stale, or competing group; cancellation;
    /// bounded control-plane timeout or closure; actor rejection; and incomplete rollback.
    pub async fn prepare_action_hooks(
        &mut self,
        mut hooks: Vec<RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<PreparedLiveActionHookGroup, LiveActionControlError> {
        if self.startup_action_hooks {
            return Err(LiveActionControlError::StartupHooksInstalled);
        }
        if self.dynamic_action_group.is_some() {
            return Err(LiveActionControlError::GroupAlreadyPrepared);
        }
        if hooks.is_empty() {
            return Err(LiveActionControlError::EmptyGroup);
        }
        self.ingress
            .runtime
            .validate()
            .map_err(|_| LiveActionControlError::RuntimeClosed)?;
        hooks.sort_unstable_by(|left, right| {
            left.route()
                .venue()
                .as_str()
                .cmp(right.route().venue().as_str())
                .then_with(|| left.route().instrument().cmp(&right.route().instrument()))
        });
        for hook in &hooks {
            if !self.ingress.routes.contains_key(hook.route()) {
                return Err(LiveActionControlError::UnknownRoute {
                    route: hook.route().clone(),
                });
            }
            hook.validate_retained_bytes(self.config.maximum_action_hook_bytes_per_route())
                .map_err(|error| LiveActionControlError::InvalidHook {
                    route: hook.route().clone(),
                    error,
                })?;
        }
        if let Some(duplicate) = hooks
            .windows(2)
            .find(|pair| pair[0].route() == pair[1].route())
        {
            return Err(LiveActionControlError::DuplicateRoute {
                route: duplicate[0].route().clone(),
            });
        }

        let generation = self.next_action_hook_generation()?;
        let (activation, prepared) = ActionHookActivationLease::prepare(
            self.incarnation,
            generation,
            self.ingress.runtime.clone(),
        );
        let router = ShardRouter::v1(self.config.shard_count().get())?;
        let shard_count = usize::from(self.config.shard_count().get());
        let mut partition_counts = Vec::new();
        partition_counts
            .try_reserve_exact(shard_count)
            .map_err(|_| LiveActionControlError::Allocation)?;
        partition_counts.resize(shard_count, 0_usize);
        for hook in &hooks {
            let index = usize::from(router.route(hook.route()).index());
            let count = partition_counts
                .get_mut(index)
                .ok_or(LiveActionControlError::ShardInvariant)?;
            *count = count
                .checked_add(1)
                .ok_or(LiveActionControlError::RetainedSizeOverflow)?;
        }
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(shard_count)
            .map_err(|_| LiveActionControlError::Allocation)?;
        for count in &partition_counts {
            let mut partition = Vec::new();
            partition
                .try_reserve_exact(*count)
                .map_err(|_| LiveActionControlError::Allocation)?;
            partitions.push(partition);
        }
        for hook in hooks {
            let shard = router.route(hook.route());
            partitions
                .get_mut(usize::from(shard.index()))
                .ok_or(LiveActionControlError::ShardInvariant)?
                .push(hook);
        }
        let mut shard_counts = Vec::new();
        shard_counts
            .try_reserve_exact(partitions.iter().filter(|hooks| !hooks.is_empty()).count())
            .map_err(|_| LiveActionControlError::Allocation)?;
        for (index, hooks) in partitions.iter().enumerate() {
            if !hooks.is_empty() {
                shard_counts.push(DynamicActionShard {
                    shard: ShardId::new(
                        u16::try_from(index).map_err(|_| LiveActionControlError::ShardInvariant)?,
                        self.config.shard_count().get(),
                    )?,
                    expected_hooks: hooks.len(),
                });
            }
        }
        self.dynamic_action_group = Some(DynamicActionGroupRecord {
            activation: activation.clone(),
            generation,
            shards: shard_counts,
            install_complete: false,
            removal_started: false,
        });

        let install = self
            .install_dynamic_action_hooks(partitions, activation, generation, &cancellation)
            .await;
        if let Err(error) = install {
            let rollback_cancellation = CancellationToken::new();
            let rollback = self.reap_action_hooks_inner(&rollback_cancellation).await;
            return if rollback.is_ok() {
                Err(error)
            } else {
                Err(LiveActionControlError::RollbackIncomplete { generation })
            };
        }
        let record = self
            .dynamic_action_group
            .as_mut()
            .ok_or(LiveActionControlError::GroupStateLost)?;
        record.install_complete = true;
        Ok(prepared)
    }

    /// Removes and drops the one current disabled dynamic group after bounded actor acknowledgments.
    ///
    /// This operation is retryable after cancellation, timeout, or a lost acknowledgement. The
    /// gate must have been disabled synchronously first; active hooks are never removed underneath
    /// an executing application owner.
    pub async fn reap_action_hooks(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<LiveActionHookReapReceipt, LiveActionControlError> {
        self.reap_action_hooks_inner(&cancellation).await
    }

    async fn install_dynamic_action_hooks(
        &mut self,
        mut partitions: Vec<Vec<RouteActionHook>>,
        activation: ActionHookActivationLease,
        generation: LiveActionHookGeneration,
        cancellation: &CancellationToken,
    ) -> Result<(), LiveActionControlError> {
        let deadline = control_deadline(self.config.registration_deadline())?;
        let mut responses = Vec::new();
        responses
            .try_reserve_exact(partitions.iter().filter(|hooks| !hooks.is_empty()).count())
            .map_err(|_| LiveActionControlError::Allocation)?;
        for (index, hooks) in partitions.iter_mut().enumerate() {
            if hooks.is_empty() {
                continue;
            }
            let channels = self
                .action_controls
                .get(index)
                .ok_or(LiveActionControlError::ShardInvariant)?;
            channels
                .runtime
                .validate()
                .map_err(|_| LiveActionControlError::RuntimeClosed)?;
            channels.shard_liveness.validate().map_err(|_| {
                LiveActionControlError::ShardClosed {
                    shard: channels.shard,
                }
            })?;
            let mut byte_permits = Vec::new();
            byte_permits
                .try_reserve_exact(hooks.len())
                .map_err(|_| LiveActionControlError::Allocation)?;
            let mut retained_bytes = Vec::new();
            retained_bytes
                .try_reserve_exact(hooks.len())
                .map_err(|_| LiveActionControlError::Allocation)?;
            for hook in hooks.iter() {
                retained_bytes.push(
                    u32::try_from(hook.declared_retained_bytes())
                        .map_err(|_| LiveActionControlError::RetainedSizeOverflow)?,
                );
            }
            for retained in retained_bytes {
                let permit = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => return Err(LiveActionControlError::Cancelled),
                    result = Arc::clone(&channels.byte_budget).acquire_many_owned(retained) => {
                        result.map_err(|_| LiveActionControlError::ControlClosed)?
                    }
                    () = tokio::time::sleep_until(deadline) => {
                        return Err(LiveActionControlError::DeadlineExceeded);
                    }
                };
                byte_permits.push(permit);
            }
            let sender = channels.control.clone();
            let permit = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(LiveActionControlError::Cancelled),
                result = sender.reserve_owned() => {
                    result.map_err(|_| LiveActionControlError::ControlClosed)?
                }
                () = tokio::time::sleep_until(deadline) => {
                    return Err(LiveActionControlError::DeadlineExceeded);
                }
            };
            let expected_hooks = hooks.len();
            let (response, receiver) = oneshot::channel();
            permit.send(ActorControlCommand::InstallActionHooks(
                ActionHookInstallCommand {
                    runtime_incarnation: self.incarnation,
                    generation,
                    activation: activation.clone(),
                    hooks: std::mem::take(hooks),
                    response,
                    _byte_permits: byte_permits,
                },
            ));
            responses.push((channels.shard, expected_hooks, receiver));
        }
        await_action_control_responses(responses, deadline, cancellation, false).await?;
        Ok(())
    }

    async fn reap_action_hooks_inner(
        &mut self,
        cancellation: &CancellationToken,
    ) -> Result<LiveActionHookReapReceipt, LiveActionControlError> {
        let record = self
            .dynamic_action_group
            .as_mut()
            .ok_or(LiveActionControlError::NoPreparedGroup)?;
        record
            .activation
            .validate_disabled(self.incarnation, record.generation)?;
        let allow_absent = record.removal_started || !record.install_complete;
        record.removal_started = true;
        let activation = record.activation.clone();
        let generation = record.generation;
        let mut shards = Vec::new();
        shards
            .try_reserve_exact(record.shards.len())
            .map_err(|_| LiveActionControlError::Allocation)?;
        shards.extend_from_slice(&record.shards);
        let deadline = control_deadline(self.config.registration_deadline())?;
        let mut responses = Vec::new();
        responses
            .try_reserve_exact(shards.len())
            .map_err(|_| LiveActionControlError::Allocation)?;
        for shard in &shards {
            let channels = self
                .action_controls
                .get(usize::from(shard.shard.index()))
                .ok_or(LiveActionControlError::ShardInvariant)?;
            channels
                .runtime
                .validate()
                .map_err(|_| LiveActionControlError::RuntimeClosed)?;
            channels.shard_liveness.validate().map_err(|_| {
                LiveActionControlError::ShardClosed {
                    shard: channels.shard,
                }
            })?;
            let sender = channels.control.clone();
            let permit = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(LiveActionControlError::Cancelled),
                result = sender.reserve_owned() => {
                    result.map_err(|_| LiveActionControlError::ControlClosed)?
                }
                () = tokio::time::sleep_until(deadline) => {
                    return Err(LiveActionControlError::DeadlineExceeded);
                }
            };
            let (response, receiver) = oneshot::channel();
            permit.send(ActorControlCommand::RemoveActionHooks(
                ActionHookRemoveCommand {
                    runtime_incarnation: self.incarnation,
                    generation,
                    activation: activation.clone(),
                    expected_hooks: shard.expected_hooks,
                    response,
                },
            ));
            responses.push((channels.shard, shard.expected_hooks, receiver));
        }
        let removed =
            await_action_control_responses(responses, deadline, cancellation, allow_absent).await?;
        activation.retire()?;
        self.dynamic_action_group = None;
        Ok(LiveActionHookReapReceipt::new(
            self.incarnation,
            generation,
            removed,
        ))
    }

    fn next_action_hook_generation(
        &mut self,
    ) -> Result<LiveActionHookGeneration, LiveActionControlError> {
        if self.next_action_hook_generation == 0 || self.next_action_hook_generation == u64::MAX {
            return Err(LiveActionControlError::GenerationExhausted);
        }
        let generation = NonZeroU64::new(self.next_action_hook_generation)
            .map(LiveActionHookGeneration::new)
            .ok_or(LiveActionControlError::GenerationExhausted)?;
        self.next_action_hook_generation = self
            .next_action_hook_generation
            .checked_add(1)
            .ok_or(LiveActionControlError::GenerationExhausted)?;
        Ok(generation)
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

    /// Replaces this incarnation with a fully action-enabled runtime and exact route hooks.
    pub async fn replace_with_action_hooks(
        self,
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        action_hooks: Vec<RouteActionHook>,
    ) -> Result<Self, LiveRuntimeReplaceError> {
        let shutdown = self.shutdown().await;
        if !shutdown.is_complete() {
            return Err(LiveRuntimeReplaceError::Shutdown(shutdown));
        }
        Self::start_with_action_hooks(config, routes, action_hooks)
            .await
            .map_err(LiveRuntimeReplaceError::Start)
    }

    /// Replaces this incarnation with bounded post-decision observation exports.
    pub async fn replace_with_qualified_market_exports(
        self,
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
    ) -> Result<Self, LiveRuntimeReplaceError> {
        let shutdown = self.shutdown().await;
        if !shutdown.is_complete() {
            return Err(LiveRuntimeReplaceError::Shutdown(shutdown));
        }
        Self::start_with_qualified_market_exports(config, routes, qualified_market_exports)
            .await
            .map_err(LiveRuntimeReplaceError::Start)
    }

    /// Replaces this incarnation with action hooks and bounded post-decision exports.
    pub async fn replace_with_action_hooks_and_qualified_market_exports(
        self,
        config: LiveRuntimeConfig,
        routes: Vec<LiveRouteConfig>,
        action_hooks: Vec<RouteActionHook>,
        qualified_market_exports: Vec<RouteQualifiedMarketExport>,
    ) -> Result<Self, LiveRuntimeReplaceError> {
        let shutdown = self.shutdown().await;
        if !shutdown.is_complete() {
            return Err(LiveRuntimeReplaceError::Shutdown(shutdown));
        }
        Self::start_with_action_hooks_and_qualified_market_exports(
            config,
            routes,
            action_hooks,
            qualified_market_exports,
        )
        .await
        .map_err(LiveRuntimeReplaceError::Start)
    }

    /// Release-invalidates ingress, drains or aborts-and-awaits every actor, and returns outcomes.
    pub async fn shutdown(mut self) -> LiveRuntimeShutdown {
        if let Some(group) = self.dynamic_action_group.as_ref() {
            group.activation.disable();
        }
        if let Some(owner) = self.runtime_owner.as_mut() {
            owner.invalidate();
        }
        for channels in self.ingress.routes.values() {
            channels.byte_budget.close();
        }
        for channels in &self.action_controls {
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
        if let Some(group) = self.dynamic_action_group.as_ref() {
            group.activation.disable();
        }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DynamicActionShard {
    shard: ShardId,
    expected_hooks: usize,
}

#[derive(Debug)]
struct DynamicActionGroupRecord {
    activation: ActionHookActivationLease,
    generation: LiveActionHookGeneration,
    shards: Vec<DynamicActionShard>,
    install_complete: bool,
    removal_started: bool,
}

type ActionControlResponse = (
    ShardId,
    usize,
    oneshot::Receiver<Result<usize, ActionHookControlFailure>>,
);

async fn await_action_control_responses(
    responses: Vec<ActionControlResponse>,
    deadline: Instant,
    cancellation: &CancellationToken,
    allow_absent: bool,
) -> Result<usize, LiveActionControlError> {
    let mut acknowledged = 0_usize;
    for (shard, expected, response) in responses {
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(LiveActionControlError::Cancelled),
            response = response => response.map_err(|_| LiveActionControlError::ControlClosed)?,
            () = tokio::time::sleep_until(deadline) => {
                return Err(LiveActionControlError::DeadlineExceeded);
            }
        };
        let observed = response.map_err(|error| LiveActionControlError::ActorRejected {
            shard,
            reason: action_control_rejection(&error),
        })?;
        if observed != expected && !(allow_absent && observed == 0) {
            return Err(LiveActionControlError::AcknowledgementMismatch {
                shard,
                expected,
                observed,
            });
        }
        acknowledged = acknowledged
            .checked_add(observed)
            .ok_or(LiveActionControlError::RetainedSizeOverflow)?;
    }
    Ok(acknowledged)
}

fn control_deadline(duration: std::time::Duration) -> Result<Instant, LiveActionControlError> {
    Instant::now()
        .checked_add(duration)
        .ok_or(LiveActionControlError::DeadlineRange)
}

fn action_control_rejection(error: &ActionHookControlFailure) -> LiveActionControlRejection {
    match error {
        ActionHookControlFailure::RuntimeMismatch => LiveActionControlRejection::RuntimeMismatch,
        ActionHookControlFailure::InvalidActivation => {
            LiveActionControlRejection::InvalidActivation
        }
        ActionHookControlFailure::EmptyGroup => LiveActionControlRejection::EmptyGroup,
        ActionHookControlFailure::DuplicateRoute => LiveActionControlRejection::DuplicateRoute,
        ActionHookControlFailure::UnknownRoute => LiveActionControlRejection::UnknownRoute,
        ActionHookControlFailure::HookAlreadyInstalled => {
            LiveActionControlRejection::HookAlreadyInstalled
        }
        ActionHookControlFailure::PartialGroup => LiveActionControlRejection::PartialGroup,
        ActionHookControlFailure::InvalidHook(_) => LiveActionControlRejection::InvalidHook,
    }
}

fn validate_action_hooks(
    config: &LiveRuntimeConfig,
    routes: &[LiveRouteConfig],
    action_hooks: Option<Vec<RouteActionHook>>,
) -> Result<Option<HashMap<ShardKey, RouteActionHook>>, LiveRuntimeStartError> {
    let Some(action_hooks) = action_hooks else {
        return Ok(None);
    };
    let mut known_routes = std::collections::HashSet::new();
    known_routes
        .try_reserve(routes.len())
        .map_err(|_| LiveRuntimeStartError::Allocation)?;
    for route in routes {
        known_routes.insert(route.route().clone());
    }
    let mut validated = HashMap::new();
    validated
        .try_reserve(action_hooks.len())
        .map_err(|_| LiveRuntimeStartError::Allocation)?;
    for hook in action_hooks {
        let route = hook.route().clone();
        if !known_routes.contains(&route) {
            return Err(LiveRuntimeStartError::UnknownActionHook { route });
        }
        hook.validate_retained_bytes(config.maximum_action_hook_bytes_per_route())
            .map_err(|error| LiveRuntimeStartError::InvalidActionHook {
                route: route.clone(),
                error,
            })?;
        if validated.insert(route.clone(), hook).is_some() {
            return Err(LiveRuntimeStartError::DuplicateActionHook { route });
        }
    }
    for route in routes {
        if !validated.contains_key(route.route()) {
            return Err(LiveRuntimeStartError::MissingActionHook {
                route: route.route().clone(),
            });
        }
    }
    Ok(Some(validated))
}

fn validate_qualified_market_exports(
    routes: &[LiveRouteConfig],
    exporters: Vec<RouteQualifiedMarketExport>,
) -> Result<HashMap<ShardKey, RouteQualifiedMarketExport>, LiveRuntimeStartError> {
    let mut known_routes = std::collections::HashSet::new();
    known_routes
        .try_reserve(routes.len())
        .map_err(|_| LiveRuntimeStartError::Allocation)?;
    for route in routes {
        known_routes.insert(route.route().clone());
    }
    let mut validated = HashMap::new();
    validated
        .try_reserve(exporters.len())
        .map_err(|_| LiveRuntimeStartError::Allocation)?;
    for exporter in exporters {
        let route = exporter.route().clone();
        if !known_routes.contains(&route) {
            return Err(LiveRuntimeStartError::UnknownQualifiedMarketExport { route });
        }
        if validated.insert(route.clone(), exporter).is_some() {
            return Err(LiveRuntimeStartError::DuplicateQualifiedMarketExport { route });
        }
    }
    Ok(validated)
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
    #[error("action-enabled runtime is missing a hook for route {route:?}")]
    MissingActionHook { route: ShardKey },
    #[error("action-enabled runtime received duplicate hooks for route {route:?}")]
    DuplicateActionHook { route: ShardKey },
    #[error("action-enabled runtime received a hook for unknown route {route:?}")]
    UnknownActionHook { route: ShardKey },
    #[error("route action hook {route:?} failed validation")]
    InvalidActionHook {
        route: ShardKey,
        #[source]
        error: RouteActionHookError,
    },
    #[error("validated action hooks did not preserve deterministic shard partitioning")]
    ActionHookPartitionInvariant,
    #[error("qualified-market export was configured more than once for route {route:?}")]
    DuplicateQualifiedMarketExport { route: ShardKey },
    #[error("qualified-market export was configured for unknown route {route:?}")]
    UnknownQualifiedMarketExport { route: ShardKey },
    #[error("qualified-market exports did not preserve deterministic shard partitioning")]
    QualifiedMarketExportPartitionInvariant,
    #[error("qualified-market export reservation exceeds the configured runtime memory ceiling")]
    QualifiedMarketExportMemoryExceeded,
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
