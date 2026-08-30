//! Account-scoped Coinbase Direct runtime supervision.
//!
//! Startup completes every credential, metadata, registry, capture, route, and product allocation
//! before a single product task may open a provider connection. Each product task is the sole
//! mutable owner of its registry and book generation; the account owner retains cross-process
//! exclusion, onboarding currentness, cancellation, and coordinated cleanup.

mod evidence;
mod output;
mod product;

use std::mem::size_of;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use market_squawk_adapter_coinbase::{CoinbaseDirectHmacSigner, CoinbaseDirectSigningError};
use market_squawk_live::{LiveRuntimeConfig, LiveSnapshotReader, RouteActionHook, ShardKey};
use market_squawk_platform::{
    CaptureProcessInfrastructureLimits, DestinationFenceRegistryInitializationError,
    initialize_capture_process_infrastructure,
};
use market_squawk_sources::SourceMetadata;
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    ProviderOnboardingError,
    application::{CryptoPublicationRendezvousLimits, MarketEventDurableRead},
    live_runtime::LiveRuntimeCompositionError,
    live_source::order_level::OrderLevelDirectory,
    provider_activation::{
        CoinbaseDirectAccountActivation, CoinbaseMarketPublicationPackage,
        ProviderAdapterActivationError,
    },
};

pub(crate) use evidence::try_build_product_metadata_set;
use evidence::try_build_product_spec;
pub use output::CoinbaseDirectOutputFailure;
pub use product::CoinbaseDirectProductRuntimeError;
use product::{ProductReady, ProductRuntimeSpec, run_product};

use super::{
    composition::{
        CRYPTO_PUBLICATION_CHANNEL_CAPACITY, CRYPTO_PUBLICATION_RETAINED_FRAMES,
        CoinbasePublicationSupervisor, CoinbasePublicationSupervisorError,
        ProductionLiveRuntimeOwner, SupervisorDropCancellation, committed_research_exports,
    },
    route_actor::RouteBufferLimits,
    sink::CoinbaseCapturedPublicationIngress,
};

const ONBOARDING_CURRENTNESS_INTERVAL: Duration = Duration::from_secs(1);
type ProductTaskOutput = (usize, Result<(), CoinbaseDirectProductRuntimeError>);
type ProductJoinOutcome = Option<Result<ProductTaskOutput, JoinError>>;

/// Complete account owner for authenticated Coinbase Direct market data.
#[derive(Debug)]
pub struct CoinbaseDirectLiveRuntime {
    supervisor_cancellation: SupervisorDropCancellation,
    publication_cancellation: SupervisorDropCancellation,
    supervisor_live: Arc<AtomicBool>,
    publication: Vec<CoinbasePublicationSupervisor>,
    live: ProductionLiveRuntimeOwner,
    durable_reads: Arc<[MarketEventDurableRead]>,
    metadata: Arc<[SourceMetadata]>,
    routes: Arc<[ShardKey]>,
    supervisor: tokio::task::JoinHandle<Result<(), CoinbaseDirectSupervisorError>>,
    shutdown_deadline: Duration,
}

impl CoinbaseDirectLiveRuntime {
    /// Reports whether the account supervisor still owns a live Direct product set.
    pub(crate) fn is_healthy(&self) -> bool {
        self.supervisor_live.load(Ordering::Acquire)
            && !self.publication.is_empty()
            && self.publication.iter().all(|owner| owner.is_healthy())
    }

    /// Returns authority-free immutable snapshot access.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.live.snapshots()
    }

    /// Returns every exact source-metadata record retained by this account runtime.
    pub(crate) fn metadata(&self) -> Arc<[SourceMetadata]> {
        Arc::clone(&self.metadata)
    }

    /// Returns the exact route topology retained by this account runtime.
    pub(crate) fn routes(&self) -> Arc<[ShardKey]> {
        Arc::clone(&self.routes)
    }

    /// Returns the exact provider-neutral durable market reads owned by this runtime.
    pub(crate) fn durable_reads(&self) -> Arc<[MarketEventDurableRead]> {
        Arc::clone(&self.durable_reads)
    }

    /// Installs one complete disabled action-hook group without reconnecting the account source.
    pub async fn prepare_action_hooks(
        &mut self,
        hooks: Vec<RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<market_squawk_live::PreparedLiveActionHookGroup, CoinbaseDirectSupervisorError>
    {
        self.live
            .prepare_action_hooks(hooks, cancellation)
            .await
            .map_err(Into::into)
    }

    /// Removes the exact disabled dynamic action-hook group from the running actors.
    pub async fn reap_action_hooks(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<market_squawk_live::LiveActionHookReapReceipt, CoinbaseDirectSupervisorError> {
        self.live
            .reap_action_hooks(cancellation)
            .await
            .map_err(Into::into)
    }

    /// Cancels every product, reaps capture and route workers, then releases credential authority.
    ///
    /// # Errors
    ///
    /// Reports supervisor, timeout, task, live-runtime, or combined shutdown failures after
    /// attempting every owned cleanup boundary.
    pub async fn shutdown(mut self) -> Result<(), CoinbaseDirectSupervisorError> {
        self.supervisor_cancellation.cancel();
        let supervisor =
            match tokio::time::timeout(self.shutdown_deadline, &mut self.supervisor).await {
                Ok(Ok(Ok(()))) => None,
                Ok(Ok(Err(error))) => Some(error),
                Ok(Err(error)) => Some(CoinbaseDirectSupervisorError::SupervisorTask(error)),
                Err(_elapsed) => {
                    self.supervisor.abort();
                    let _aborted = self.supervisor.await;
                    Some(CoinbaseDirectSupervisorError::SupervisorShutdownDeadline)
                }
            };
        self.publication_cancellation.cancel();
        let publication_deadline = Instant::now()
            .checked_add(self.shutdown_deadline)
            .unwrap_or_else(Instant::now);
        let publication = shutdown_publication_supervisors(
            std::mem::take(&mut self.publication),
            publication_deadline,
        )
        .await
        .err();
        let supervisor = match (supervisor, publication) {
            (None, None) => None,
            (Some(error), None) => Some(error),
            (None, Some(error)) => Some(error.into()),
            (Some(primary), Some(cleanup)) => {
                Some(CoinbaseDirectSupervisorError::PublicationAndCleanup {
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                })
            }
        };
        let live = self.live.shutdown().await;
        match (supervisor, live) {
            (None, Ok(_shutdown)) => Ok(()),
            (Some(error), Ok(_shutdown)) => Err(error),
            (None, Err(error)) => Err(error.into()),
            (Some(supervisor), Err(live)) => Err(CoinbaseDirectSupervisorError::ShutdownFailures {
                supervisor: Box::new(supervisor),
                live,
            }),
        }
    }
}

impl CoinbaseDirectAccountActivation {
    /// Starts Direct products on the bounded live runtime after atomic account preflight.
    ///
    /// # Errors
    ///
    /// Returns without network access when any product, credential, queue, capture, registry, or
    /// live-route prerequisite cannot be admitted.
    pub async fn start_live(
        self,
        runtime_config: LiveRuntimeConfig,
        cancellation: CancellationToken,
    ) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
        start_account(self, runtime_config, None, None, cancellation).await
    }

    /// Starts Direct products with one shared generation-owned order-level read directory.
    pub(crate) async fn start_live_with_order_level(
        self,
        runtime_config: LiveRuntimeConfig,
        order_level: OrderLevelDirectory,
        cancellation: CancellationToken,
    ) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
        start_account(self, runtime_config, None, Some(order_level), cancellation).await
    }

    /// Starts Direct products only after exact execution action hooks are installed per route.
    ///
    /// This is the source-side composition seam used by paper execution. It preserves the same
    /// centralized strategy, current-authority, risk, dispatcher, and execution chain as public
    /// sources.
    ///
    /// # Errors
    ///
    /// Returns without provider network access when hook or product preflight fails.
    pub async fn start_live_with_action_hooks(
        self,
        runtime_config: LiveRuntimeConfig,
        action_hooks: Vec<RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
        start_account(self, runtime_config, Some(action_hooks), None, cancellation).await
    }
}

async fn start_account(
    mut activation: CoinbaseDirectAccountActivation,
    runtime_config: LiveRuntimeConfig,
    action_hooks: Option<Vec<RouteActionHook>>,
    order_level: Option<OrderLevelDirectory>,
    cancellation: CancellationToken,
) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
    activation.require_current().await?;
    let product_count = activation.product_count();
    let admission = activation.runtime_admission();
    validate_startup_queue(product_count, admission)?;

    let products = activation.take_products();
    let mut specs = Vec::new();
    specs
        .try_reserve_exact(product_count)
        .map_err(|_error| CoinbaseDirectSupervisorError::AllocationFailed)?;
    for (slot, product) in products.into_iter().enumerate() {
        if let Some(product) = product {
            specs.push(try_build_product_spec(
                slot,
                activation.lease(),
                product,
                order_level.is_some(),
            )?);
        }
    }
    if specs.len() != product_count {
        return Err(CoinbaseDirectSupervisorError::ActivationTopology);
    }
    let mut metadata = Vec::new();
    metadata
        .try_reserve_exact(specs.len())
        .map_err(|_error| CoinbaseDirectSupervisorError::AllocationFailed)?;
    metadata.extend(specs.iter().map(|spec| spec.metadata().clone()));
    let metadata: Arc<[SourceMetadata]> = metadata.into();
    let mut retained_routes = Vec::new();
    retained_routes
        .try_reserve_exact(specs.len())
        .map_err(|_error| CoinbaseDirectSupervisorError::AllocationFailed)?;
    retained_routes.extend(specs.iter().map(|spec| spec.route().route().clone()));
    let retained_routes: Arc<[ShardKey]> = retained_routes.into();
    let routes = specs
        .iter()
        .map(|spec| spec.route().clone())
        .collect::<Vec<_>>();

    let (publication_packages, publication_cancellation) = activation.take_market_publication()?;
    let publication_start_guard = SupervisorDropCancellation::new(publication_cancellation.clone());
    if publication_packages.len() != specs.len() {
        publication_cancellation.cancel();
        return Err(CoinbaseDirectSupervisorError::ActivationTopology);
    }
    let mut durable_reads = Vec::new();
    durable_reads
        .try_reserve_exact(publication_packages.len())
        .map_err(|_error| CoinbaseDirectSupervisorError::AllocationFailed)?;
    durable_reads.extend(
        publication_packages
            .iter()
            .map(|package| package.durable_read().clone()),
    );
    let durable_reads: Arc<[MarketEventDurableRead]> = durable_reads.into();

    let publication_capacity = std::num::NonZeroUsize::new(CRYPTO_PUBLICATION_CHANNEL_CAPACITY)
        .ok_or(CoinbaseDirectSupervisorError::PublicationBounds)?;
    let maximum_message_bytes = usize::try_from(runtime_config.maximum_message_bytes().get())
        .map_err(|_| CoinbaseDirectSupervisorError::PublicationBounds)?;
    let publication_retained_bytes = maximum_message_bytes
        .checked_mul(CRYPTO_PUBLICATION_RETAINED_FRAMES)
        .and_then(std::num::NonZeroUsize::new)
        .ok_or(CoinbaseDirectSupervisorError::PublicationBounds)?;
    let (committed_exports, committed_receivers) =
        committed_research_exports(&routes, publication_capacity, publication_retained_bytes)
            .map_err(|_| CoinbaseDirectSupervisorError::PublicationBounds)?;
    let publication_limits = CryptoPublicationRendezvousLimits::new(
        publication_capacity,
        publication_retained_bytes,
        activation.app_config().source_shutdown(),
    );

    let secret = activation
        .onboarding()
        .read_secret_for_activation_request(activation.lease(), cancellation.clone())
        .await?;
    let signer = Arc::new(CoinbaseDirectHmacSigner::try_from_secret_envelope(
        secret.expose_secret(),
    )?);
    drop(secret);
    activation.require_current().await?;

    let app_config = activation.app_config().clone();
    let capture_process =
        initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
            app_config.capture_destination_registry_memory_ceiling_bytes(),
        ))?;
    let route_buffer_limits = RouteBufferLimits::new(
        runtime_config.mailbox_count_per_shard(),
        runtime_config.maximum_message_bytes(),
    );
    let mut live = ProductionLiveRuntimeOwner::start_with_research_exports(
        runtime_config,
        routes,
        Vec::new(),
        committed_exports,
    )
    .await?;
    if let Some(action_hooks) = action_hooks
        && let Err(startup) = live
            .prepare_action_hooks(action_hooks, cancellation.child_token())
            .await
    {
        publication_cancellation.cancel();
        return rollback_live_start(startup.into(), live).await;
    }
    let started = start_on_live_runtime(
        activation,
        specs,
        metadata,
        retained_routes,
        signer,
        app_config,
        admission,
        capture_process,
        route_buffer_limits,
        live,
        publication_packages,
        committed_receivers,
        publication_capacity,
        publication_limits,
        publication_cancellation,
        durable_reads,
        order_level,
        cancellation,
    )
    .await;
    if started.is_ok() {
        publication_start_guard.disarm();
    }
    started
}

#[allow(
    clippy::too_many_arguments,
    reason = "account startup retains every independently owned runtime capability"
)]
async fn start_on_live_runtime(
    activation: CoinbaseDirectAccountActivation,
    specs: Vec<ProductRuntimeSpec>,
    metadata: Arc<[SourceMetadata]>,
    routes: Arc<[ShardKey]>,
    signer: Arc<CoinbaseDirectHmacSigner>,
    app_config: market_squawk_platform::AppConfig,
    admission: crate::provider_activation::CoinbaseDirectRuntimeAdmission,
    capture_process: market_squawk_platform::CaptureProcessInfrastructure,
    route_buffer_limits: RouteBufferLimits,
    live: ProductionLiveRuntimeOwner,
    publication_packages: Vec<CoinbaseMarketPublicationPackage>,
    committed_receivers: Vec<market_squawk_live::CommittedResearchMarketObservationReceiver>,
    publication_capacity: std::num::NonZeroUsize,
    publication_limits: CryptoPublicationRendezvousLimits,
    publication_cancellation: CancellationToken,
    durable_reads: Arc<[MarketEventDurableRead]>,
    order_level: Option<OrderLevelDirectory>,
    cancellation: CancellationToken,
) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
    let live_ingress = live.production_ingress();
    let shutdown_deadline = app_config.source_shutdown();
    let publication_start = start_publication_supervisors(
        &specs,
        publication_packages,
        committed_receivers,
        publication_capacity,
        publication_limits,
        publication_cancellation.clone(),
        shutdown_deadline,
    )
    .await;
    let (publication, publication_ingresses) = match publication_start {
        Ok(started) => started,
        Err(startup) => {
            publication_cancellation.cancel();
            return rollback_live_start(startup, live).await;
        }
    };
    let (startup_sender, startup_receiver) = oneshot::channel();
    let supervisor_cancellation = cancellation.clone();
    let terminal_cancellation = cancellation.clone();
    let supervisor_live = Arc::new(AtomicBool::new(true));
    let task_supervisor_live = Arc::clone(&supervisor_live);
    let mut supervisor = tokio::spawn(async move {
        let _liveness = SupervisorLiveness::new(task_supervisor_live);
        let outcome = run_account(
            activation,
            specs,
            signer,
            app_config,
            admission,
            capture_process,
            route_buffer_limits,
            live_ingress,
            publication_ingresses,
            order_level,
            supervisor_cancellation,
            startup_sender,
        )
        .await;
        terminal_cancellation.cancel();
        outcome
    });
    tokio::select! {
        startup = startup_receiver => match startup {
            Ok(()) => require_healthy_start(CoinbaseDirectLiveRuntime {
                    supervisor_cancellation: SupervisorDropCancellation::new(cancellation),
                    publication_cancellation: SupervisorDropCancellation::new(
                        publication_cancellation,
                    ),
                    supervisor_live,
                    publication,
                    live,
                    durable_reads,
                    metadata,
                    routes,
                    supervisor,
                    shutdown_deadline,
                }).await,
            Err(_closed) => {
                let startup = map_supervisor_outcome(supervisor.await);
                rollback_direct_start(
                    startup,
                    publication,
                    publication_cancellation,
                    live,
                    shutdown_deadline,
                ).await
            }
        },
        outcome = &mut supervisor => {
            let startup = map_supervisor_outcome(outcome);
            rollback_direct_start(
                startup,
                publication,
                publication_cancellation,
                live,
                shutdown_deadline,
            ).await
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "each product retains one exact package, captured ingress, and committed export"
)]
async fn start_publication_supervisors(
    specs: &[ProductRuntimeSpec],
    packages: Vec<CoinbaseMarketPublicationPackage>,
    committed_receivers: Vec<market_squawk_live::CommittedResearchMarketObservationReceiver>,
    maximum_inflight: std::num::NonZeroUsize,
    limits: CryptoPublicationRendezvousLimits,
    cancellation: CancellationToken,
    shutdown_timeout: Duration,
) -> Result<
    (
        Vec<CoinbasePublicationSupervisor>,
        Vec<CoinbaseCapturedPublicationIngress>,
    ),
    CoinbaseDirectSupervisorError,
> {
    if cancellation.is_cancelled()
        || specs.is_empty()
        || specs.len() != packages.len()
        || specs.len() != committed_receivers.len()
        || specs.iter().zip(&packages).any(|(spec, package)| {
            package
                .durable_read()
                .point_in_time_selector()
                .source_surface()
                != spec.metadata().source_id()
        })
    {
        return Err(CoinbaseDirectSupervisorError::ActivationTopology);
    }
    let mut supervisors = Vec::new();
    let mut ingresses = Vec::new();
    supervisors
        .try_reserve_exact(specs.len())
        .map_err(|_error| CoinbaseDirectSupervisorError::AllocationFailed)?;
    ingresses
        .try_reserve_exact(specs.len())
        .map_err(|_error| CoinbaseDirectSupervisorError::AllocationFailed)?;
    for (package, committed) in packages.into_iter().zip(committed_receivers) {
        let (ingress, receiver) = CoinbaseCapturedPublicationIngress::try_channel(maximum_inflight);
        match CoinbasePublicationSupervisor::start(
            package,
            receiver,
            vec![committed],
            maximum_inflight,
            limits,
            cancellation.clone(),
        ) {
            Ok(supervisor) => {
                supervisors.push(supervisor);
                ingresses.push(ingress);
            }
            Err(startup) => {
                cancellation.cancel();
                let deadline = Instant::now()
                    .checked_add(shutdown_timeout)
                    .unwrap_or_else(Instant::now);
                let cleanup = shutdown_publication_supervisors(supervisors, deadline)
                    .await
                    .err();
                return Err(match cleanup {
                    None => startup.into(),
                    Some(cleanup) => CoinbaseDirectSupervisorError::PublicationAndCleanup {
                        primary: Box::new(startup.into()),
                        cleanup: Box::new(cleanup),
                    },
                });
            }
        }
    }
    Ok((supervisors, ingresses))
}

async fn require_healthy_start(
    runtime: CoinbaseDirectLiveRuntime,
) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
    if runtime.is_healthy() {
        return Ok(runtime);
    }
    let startup = CoinbaseDirectSupervisorError::PublicationExitedBeforeStartup;
    match runtime.shutdown().await {
        Ok(()) => Err(startup),
        Err(cleanup) => Err(CoinbaseDirectSupervisorError::StartupAndCleanup {
            startup: Box::new(startup),
            cleanup: Box::new(cleanup),
        }),
    }
}

async fn shutdown_publication_supervisors(
    supervisors: Vec<CoinbasePublicationSupervisor>,
    deadline: Instant,
) -> Result<(), CoinbasePublicationSupervisorError> {
    let mut first_error = None;
    for supervisor in supervisors {
        if let Err(error) = supervisor.shutdown(deadline).await
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

struct SupervisorLiveness {
    live: Arc<AtomicBool>,
}

impl SupervisorLiveness {
    const fn new(live: Arc<AtomicBool>) -> Self {
        Self { live }
    }
}

impl Drop for SupervisorLiveness {
    fn drop(&mut self) {
        self.live.store(false, Ordering::Release);
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the account supervisor is the explicit owner of every product capability"
)]
async fn run_account(
    activation: CoinbaseDirectAccountActivation,
    specs: Vec<ProductRuntimeSpec>,
    signer: Arc<CoinbaseDirectHmacSigner>,
    app_config: market_squawk_platform::AppConfig,
    admission: crate::provider_activation::CoinbaseDirectRuntimeAdmission,
    capture_process: market_squawk_platform::CaptureProcessInfrastructure,
    route_buffer_limits: RouteBufferLimits,
    live_ingress: market_squawk_live::LiveRuntimeIngress,
    publication_ingresses: Vec<CoinbaseCapturedPublicationIngress>,
    order_level: Option<OrderLevelDirectory>,
    cancellation: CancellationToken,
    startup: oneshot::Sender<()>,
) -> Result<(), CoinbaseDirectSupervisorError> {
    let product_count = specs.len();
    let bootstrap_capacity = usize::from(
        activation
            .lease()
            .provider_budget_policy()
            .ok_or(CoinbaseDirectSupervisorError::ActivationTopology)?
            .max_concurrent(),
    );
    if bootstrap_capacity == 0 {
        return Err(CoinbaseDirectSupervisorError::ActivationTopology);
    }
    let bootstrap_slots = Arc::new(Semaphore::new(bootstrap_capacity));
    let (ready_sender, mut ready_receiver) =
        mpsc::channel(admission.supervisor_queue_records().get());
    let (start_sender, start_receiver) = watch::channel(false);
    if publication_ingresses.len() != product_count {
        return Err(CoinbaseDirectSupervisorError::ActivationTopology);
    }
    let mut publication_ingresses = publication_ingresses.into_iter();
    let mut products = JoinSet::new();
    for spec in specs {
        let slot = spec.slot();
        let task_publication = publication_ingresses
            .next()
            .ok_or(CoinbaseDirectSupervisorError::ActivationTopology)?;
        let task_config = app_config.clone();
        let provider_rate = activation.provider_rate().clone();
        let account_subject = activation.account_subject().clone();
        let task_signer = Arc::clone(&signer);
        let task_ready = ready_sender.clone();
        let task_start = start_receiver.clone();
        let task_cancellation = cancellation.child_token();
        let task_ingress = live_ingress.clone();
        let task_order_level = order_level.clone();
        let task_bootstrap_slots = Arc::clone(&bootstrap_slots);
        products.spawn(async move {
            (
                slot,
                run_product(
                    spec,
                    task_config,
                    provider_rate,
                    account_subject,
                    admission,
                    capture_process,
                    task_ingress,
                    task_publication,
                    task_order_level,
                    route_buffer_limits,
                    task_signer,
                    task_ready,
                    task_start,
                    task_bootstrap_slots,
                    task_cancellation,
                )
                .await,
            )
        });
    }
    if publication_ingresses.next().is_some() {
        return stop_products(
            products,
            cancellation,
            Some(CoinbaseDirectSupervisorError::ActivationTopology),
        )
        .await;
    }
    drop(ready_sender);
    drop(start_receiver);
    drop(signer);

    let mut observed = [false; crate::provider_activation::COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS];
    let mut ready_count = 0;
    while ready_count < product_count {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return stop_products(products, cancellation, None).await;
            }
            outcome = products.join_next() => {
                let primary = product_outcome(outcome);
                return stop_products(products, cancellation, Some(primary)).await;
            }
            ready = ready_receiver.recv() => {
                let Some(ready) = ready else {
                    return stop_products(
                        products,
                        cancellation,
                        Some(CoinbaseDirectSupervisorError::StartupQueueClosed),
                    ).await;
                };
                let Some(slot) = observed.get_mut(ready.slot) else {
                    return stop_products(
                        products,
                        cancellation,
                        Some(CoinbaseDirectSupervisorError::ActivationTopology),
                    ).await;
                };
                if *slot {
                    return stop_products(
                        products,
                        cancellation,
                        Some(CoinbaseDirectSupervisorError::DuplicateReady),
                    ).await;
                }
                *slot = true;
                ready_count += 1;
            }
        }
    }
    if let Err(error) = activation.require_current().await {
        return stop_products(
            products,
            cancellation,
            Some(CoinbaseDirectSupervisorError::Onboarding(error)),
        )
        .await;
    }
    if start_sender.send(true).is_err() {
        return stop_products(
            products,
            cancellation,
            Some(CoinbaseDirectSupervisorError::StartupBarrierClosed),
        )
        .await;
    }
    if startup.send(()).is_err() {
        return stop_products(
            products,
            cancellation,
            Some(CoinbaseDirectSupervisorError::StartupObserverDropped),
        )
        .await;
    }

    let mut currentness = tokio::time::interval(ONBOARDING_CURRENTNESS_INTERVAL);
    currentness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return stop_products(products, cancellation, None).await;
            }
            outcome = products.join_next() => {
                let primary = product_outcome(outcome);
                return stop_products(products, cancellation, Some(primary)).await;
            }
            _ = currentness.tick() => {
                if let Err(error) = activation.require_current().await {
                    return stop_products(
                        products,
                        cancellation,
                        Some(CoinbaseDirectSupervisorError::Onboarding(error)),
                    ).await;
                }
            }
        }
    }
}

fn product_outcome(outcome: ProductJoinOutcome) -> CoinbaseDirectSupervisorError {
    match outcome {
        Some(Ok((slot, Ok(())))) => CoinbaseDirectSupervisorError::ProductExited { slot },
        Some(Ok((slot, Err(source)))) => CoinbaseDirectSupervisorError::Product { slot, source },
        Some(Err(error)) => CoinbaseDirectSupervisorError::ProductTask(error),
        None => CoinbaseDirectSupervisorError::AllProductsExited,
    }
}

async fn stop_products(
    mut products: JoinSet<ProductTaskOutput>,
    cancellation: CancellationToken,
    primary: Option<CoinbaseDirectSupervisorError>,
) -> Result<(), CoinbaseDirectSupervisorError> {
    cancellation.cancel();
    let mut cleanup = None;
    while let Some(outcome) = products.join_next().await {
        match outcome {
            Ok((_slot, Ok(()))) => {}
            Ok((slot, Err(source))) if cleanup.is_none() => {
                cleanup = Some(CoinbaseDirectSupervisorError::Product { slot, source });
            }
            Ok((_slot, Err(_source))) => {}
            Err(error) if cleanup.is_none() => {
                cleanup = Some(CoinbaseDirectSupervisorError::ProductTask(error));
            }
            Err(_error) => {}
        }
    }
    match (primary, cleanup) {
        (None, None) => Ok(()),
        (Some(primary), None) => Err(primary),
        (None, Some(cleanup)) => Err(cleanup),
        (Some(primary), Some(cleanup)) => Err(CoinbaseDirectSupervisorError::ProductAndCleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn validate_startup_queue(
    product_count: usize,
    admission: crate::provider_activation::CoinbaseDirectRuntimeAdmission,
) -> Result<(), CoinbaseDirectSupervisorError> {
    let required_bytes = product_count
        .checked_mul(size_of::<ProductReady>())
        .ok_or(CoinbaseDirectSupervisorError::StartupQueueBounds)?;
    if admission.supervisor_queue_records().get() < product_count
        || admission.supervisor_queue_bytes().get() < required_bytes
    {
        return Err(CoinbaseDirectSupervisorError::StartupQueueBounds);
    }
    Ok(())
}

fn map_supervisor_outcome(
    outcome: Result<Result<(), CoinbaseDirectSupervisorError>, JoinError>,
) -> CoinbaseDirectSupervisorError {
    match outcome {
        Ok(Ok(())) => CoinbaseDirectSupervisorError::SupervisorExitedBeforeStartup,
        Ok(Err(error)) => error,
        Err(error) => CoinbaseDirectSupervisorError::SupervisorTask(error),
    }
}

async fn rollback_live_start(
    startup: CoinbaseDirectSupervisorError,
    live: ProductionLiveRuntimeOwner,
) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
    match live.shutdown().await {
        Ok(_shutdown) => Err(startup),
        Err(rollback) => Err(CoinbaseDirectSupervisorError::StartupRollback {
            startup: Box::new(startup),
            rollback,
        }),
    }
}

async fn rollback_direct_start(
    startup: CoinbaseDirectSupervisorError,
    publication: Vec<CoinbasePublicationSupervisor>,
    publication_cancellation: CancellationToken,
    live: ProductionLiveRuntimeOwner,
    shutdown_timeout: Duration,
) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
    publication_cancellation.cancel();
    let deadline = Instant::now()
        .checked_add(shutdown_timeout)
        .unwrap_or_else(Instant::now);
    let startup = match shutdown_publication_supervisors(publication, deadline)
        .await
        .err()
    {
        None => startup,
        Some(cleanup) => CoinbaseDirectSupervisorError::PublicationAndCleanup {
            primary: Box::new(startup),
            cleanup: Box::new(cleanup),
        },
    };
    rollback_live_start(startup, live).await
}

/// Coinbase Direct account construction, supervision, or coordinated shutdown failure.
#[derive(Debug, Error)]
pub enum CoinbaseDirectSupervisorError {
    /// Product or startup storage could not be reserved.
    #[error("Coinbase Direct account allocation failed")]
    AllocationFailed,
    /// The activated fixed product slots did not match the admitted product count.
    #[error("Coinbase Direct activation topology is inconsistent")]
    ActivationTopology,
    /// The configured startup queue cannot retain one ready signal per product.
    #[error("Coinbase Direct startup queue bounds are insufficient")]
    StartupQueueBounds,
    /// The shared committed-publication queues cannot satisfy their fixed retained-memory bound.
    #[error("Coinbase Direct publication bounds are invalid")]
    PublicationBounds,
    /// The startup queue closed before every product completed preflight.
    #[error("Coinbase Direct startup queue closed")]
    StartupQueueClosed,
    /// One product attempted to satisfy the startup barrier twice.
    #[error("Coinbase Direct product sent a duplicate ready signal")]
    DuplicateReady,
    /// Every product task exited unexpectedly.
    #[error("all Coinbase Direct product tasks exited")]
    AllProductsExited,
    /// A product exited without account cancellation.
    #[error("Coinbase Direct product slot {slot} exited unexpectedly")]
    ProductExited {
        /// Fixed activation slot.
        slot: usize,
    },
    /// One product failed terminally.
    #[error("Coinbase Direct product slot {slot} failed: {source}")]
    Product {
        /// Fixed activation slot.
        slot: usize,
        /// Exact product failure.
        source: CoinbaseDirectProductRuntimeError,
    },
    /// A primary account/product failure and bounded cleanup both failed.
    #[error("Coinbase Direct account failure and product cleanup both failed")]
    ProductAndCleanup {
        /// Primary failure.
        primary: Box<Self>,
        /// Cleanup failure.
        cleanup: Box<Self>,
    },
    /// Account/product shutdown and the durable publication supervisor both failed.
    #[error("Coinbase Direct runtime and publication cleanup both failed")]
    PublicationAndCleanup {
        /// Primary account or startup failure.
        primary: Box<Self>,
        /// Durable publication cleanup failure.
        cleanup: Box<CoinbasePublicationSupervisorError>,
    },
    /// No product retained the account start receiver.
    #[error("Coinbase Direct account startup barrier closed")]
    StartupBarrierClosed,
    /// The runtime startup observer was dropped.
    #[error("Coinbase Direct startup observer was dropped")]
    StartupObserverDropped,
    /// The supervisor exited before publishing the startup barrier.
    #[error("Coinbase Direct supervisor exited before startup")]
    SupervisorExitedBeforeStartup,
    /// A durable publication worker exited before the runtime became externally reachable.
    #[error("Coinbase Direct publication exited before startup")]
    PublicationExitedBeforeStartup,
    /// Startup rejection and coordinated cleanup both failed.
    #[error("Coinbase Direct startup rejection and cleanup both failed")]
    StartupAndCleanup {
        /// Exact startup rejection.
        startup: Box<Self>,
        /// Exact cleanup failure.
        cleanup: Box<Self>,
    },
    /// Supervisor shutdown exceeded the configured deadline.
    #[error("Coinbase Direct supervisor shutdown deadline elapsed")]
    SupervisorShutdownDeadline,
    /// A product task panicked or was aborted.
    #[error("Coinbase Direct product task failed")]
    ProductTask(JoinError),
    /// The account supervisor task panicked or was aborted.
    #[error("Coinbase Direct account supervisor task failed")]
    SupervisorTask(JoinError),
    /// Startup failed and the already-started live runtime also failed to roll back.
    #[error("Coinbase Direct startup and live-runtime rollback both failed")]
    StartupRollback {
        /// Startup failure.
        startup: Box<Self>,
        /// Live-runtime rollback failure.
        rollback: LiveRuntimeCompositionError,
    },
    /// Supervisor and live-runtime shutdown both failed.
    #[error("Coinbase Direct supervisor and live-runtime shutdown both failed")]
    ShutdownFailures {
        /// Supervisor failure.
        supervisor: Box<Self>,
        /// Live-runtime shutdown failure.
        live: LiveRuntimeCompositionError,
    },
    /// Provider onboarding authority is no longer current.
    #[error(transparent)]
    Onboarding(#[from] ProviderOnboardingError),
    /// The exact registered durable publication generation could not be acquired.
    #[error(transparent)]
    PublicationActivation(#[from] ProviderAdapterActivationError),
    /// The durable raw/committed publication supervisor failed.
    #[error(transparent)]
    Publication(#[from] CoinbasePublicationSupervisorError),
    /// Credential envelope or signing-capability construction failed.
    #[error(transparent)]
    Signing(#[from] CoinbaseDirectSigningError),
    /// Capture-process infrastructure could not initialize.
    #[error(transparent)]
    CaptureInfrastructure(#[from] DestinationFenceRegistryInitializationError),
    /// Live-runtime construction or shutdown failed.
    #[error(transparent)]
    Live(#[from] LiveRuntimeCompositionError),
    /// One product runtime failed during construction.
    #[error(transparent)]
    ProductConstruction(#[from] CoinbaseDirectProductRuntimeError),
}
