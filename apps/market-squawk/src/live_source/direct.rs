//! Account-scoped Coinbase Direct runtime supervision.
//!
//! Startup completes every credential, metadata, registry, capture, route, and product allocation
//! before a single product task may open a provider connection. Each product task is the sole
//! mutable owner of its registry and book generation; the account owner retains cross-process
//! exclusion, onboarding currentness, cancellation, and coordinated cleanup.

pub(super) mod canonical;
mod evidence;
mod output;
mod product;
pub(super) mod publication_actor;

use std::mem::size_of;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use market_squawk_adapter_coinbase::{CoinbaseDirectHmacSigner, CoinbaseDirectSigningError};
use market_squawk_data::RightsBasis;
use market_squawk_live::{LiveRuntimeConfig, LiveSnapshotReader, RouteActionHook, ShardKey};
use market_squawk_platform::{
    CaptureProcessInfrastructureLimits, DestinationFenceRegistryInitializationError,
    initialize_capture_process_infrastructure,
};
use market_squawk_sources::{DataUseOperation, SourceMetadata};
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    ProviderActivationLease, ProviderOnboardingError, ResearchService,
    application::ResearchRightsAuthority,
    live_runtime::{LiveRuntimeComposition, LiveRuntimeCompositionError},
    live_source::order_level::OrderLevelDirectory,
    provider_activation::CoinbaseDirectAccountActivation,
};

use evidence::try_build_product_spec;
pub use output::CoinbaseDirectOutputFailure;
pub use product::CoinbaseDirectProductRuntimeError;
use product::{ProductReady, ProductRuntimeSpec, run_product};
use publication_actor::{
    CoinbaseDirectPublicationActorRunError, ProductionCoinbaseDirectPublicationHandler,
    coinbase_direct_publication_actor_channel,
};

use super::{composition::SupervisorDropCancellation, route_actor::RouteBufferLimits};

const ONBOARDING_CURRENTNESS_INTERVAL: Duration = Duration::from_secs(1);
type ProductTaskOutput = (usize, Result<(), CoinbaseDirectProductRuntimeError>);
type ProductJoinOutcome = Option<Result<ProductTaskOutput, JoinError>>;

/// Complete account owner for authenticated Coinbase Direct market data.
#[derive(Debug)]
pub struct CoinbaseDirectLiveRuntime {
    supervisor_cancellation: SupervisorDropCancellation,
    supervisor_live: Arc<AtomicBool>,
    live: LiveRuntimeComposition,
    metadata: Arc<[SourceMetadata]>,
    routes: Arc<[ShardKey]>,
    supervisor: tokio::task::JoinHandle<Result<(), CoinbaseDirectSupervisorError>>,
    shutdown_deadline: Duration,
}

impl CoinbaseDirectLiveRuntime {
    /// Reports whether the account supervisor still owns a live Direct product set.
    pub(crate) fn is_healthy(&self) -> bool {
        self.supervisor_live.load(Ordering::Acquire)
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
        research: Arc<ResearchService>,
        cancellation: CancellationToken,
    ) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
        start_account(self, runtime_config, research, None, None, cancellation).await
    }

    /// Starts Direct products with one shared generation-owned order-level read directory.
    pub(crate) async fn start_live_with_order_level(
        self,
        runtime_config: LiveRuntimeConfig,
        research: Arc<ResearchService>,
        order_level: OrderLevelDirectory,
        cancellation: CancellationToken,
    ) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
        start_account(
            self,
            runtime_config,
            research,
            None,
            Some(order_level),
            cancellation,
        )
        .await
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
        research: Arc<ResearchService>,
        action_hooks: Vec<RouteActionHook>,
        cancellation: CancellationToken,
    ) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
        start_account(
            self,
            runtime_config,
            research,
            Some(action_hooks),
            None,
            cancellation,
        )
        .await
    }
}

async fn start_account(
    mut activation: CoinbaseDirectAccountActivation,
    runtime_config: LiveRuntimeConfig,
    research: Arc<ResearchService>,
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
    let routes = specs.iter().map(|spec| spec.route().clone()).collect();

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
    let live = match action_hooks {
        Some(action_hooks) => {
            LiveRuntimeComposition::start_with_action_hooks(runtime_config, routes, action_hooks)
                .await?
        }
        None => LiveRuntimeComposition::start(runtime_config, routes).await?,
    };
    start_on_live_runtime(
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
        research,
        order_level,
        cancellation,
    )
    .await
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
    live: LiveRuntimeComposition,
    research: Arc<ResearchService>,
    order_level: Option<OrderLevelDirectory>,
    cancellation: CancellationToken,
) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
    let live_ingress = live.production_ingress();
    let shutdown_deadline = app_config.source_shutdown();
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
            research,
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
            Ok(()) => Ok(CoinbaseDirectLiveRuntime {
                supervisor_cancellation: SupervisorDropCancellation::new(cancellation),
                supervisor_live,
                live,
                metadata,
                routes,
                supervisor,
                shutdown_deadline,
            }),
            Err(_closed) => {
                let startup = map_supervisor_outcome(supervisor.await);
                rollback_live_start(startup, live).await
            }
        },
        outcome = &mut supervisor => {
            let startup = map_supervisor_outcome(outcome);
            rollback_live_start(startup, live).await
        }
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
    research: Arc<ResearchService>,
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
    let mut products = JoinSet::new();
    for spec in specs {
        let slot = spec.slot();
        let publication_limits = spec.publication_actor_limits()?;
        let publication_key = spec.publication_key();
        let publication_rights =
            direct_research_rights(activation.lease(), spec.metadata().source_id())?;
        let publication_handler = ProductionCoinbaseDirectPublicationHandler::try_new(
            Arc::clone(&research),
            spec.config().clone(),
            activation.lease().authority_effective_at(),
            publication_rights,
        )
        .map_err(|_error| CoinbaseDirectSupervisorError::PublicationConstruction)?;
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
        let (publication_ingress, publication_actor) = coinbase_direct_publication_actor_channel(
            publication_key,
            publication_limits,
            task_cancellation.clone(),
        );
        products.spawn(async move {
            let runtime = tokio::runtime::Handle::current();
            let product_cancellation = task_cancellation.clone();
            let mut product = tokio::task::spawn_blocking(move || {
                runtime.block_on(run_product(
                    spec,
                    task_config,
                    provider_rate,
                    account_subject,
                    admission,
                    capture_process,
                    task_ingress,
                    publication_ingress,
                    task_order_level,
                    route_buffer_limits,
                    task_signer,
                    task_ready,
                    task_start,
                    task_bootstrap_slots,
                    product_cancellation,
                ))
            });
            let mut publisher = tokio::spawn(publication_actor.run(publication_handler));
            let result = tokio::select! {
                biased;
                product = &mut product => {
                    let product = product
                        .unwrap_or_else(|error| Err(CoinbaseDirectProductRuntimeError::ProductWorkerTask(error)));
                    task_cancellation.cancel();
                    let publication = map_publication_actor_outcome(publisher.await);
                    merge_product_publication_outcomes(product, publication)
                }
                publication = &mut publisher => {
                    let publication = map_publication_actor_outcome(publication);
                    task_cancellation.cancel();
                    let product = product
                        .await
                        .unwrap_or_else(|error| Err(CoinbaseDirectProductRuntimeError::ProductWorkerTask(error)));
                    merge_publication_product_outcomes(publication, product)
                }
            };
            (slot, result)
        });
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

fn map_publication_actor_outcome(
    outcome: Result<Result<(), CoinbaseDirectPublicationActorRunError>, JoinError>,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_error)) => Err(CoinbaseDirectProductRuntimeError::PublicationActor),
        Err(error) => Err(CoinbaseDirectProductRuntimeError::PublicationActorTask(
            error,
        )),
    }
}

fn merge_product_publication_outcomes(
    product: Result<(), CoinbaseDirectProductRuntimeError>,
    publication: Result<(), CoinbaseDirectProductRuntimeError>,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    match (product, publication) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(source), Ok(())) => Err(source),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(source), Err(cleanup)) => Err(
            CoinbaseDirectProductRuntimeError::ProductPublicationCleanup {
                source: Box::new(source),
                cleanup: Box::new(cleanup),
            },
        ),
    }
}

fn merge_publication_product_outcomes(
    publication: Result<(), CoinbaseDirectProductRuntimeError>,
    product: Result<(), CoinbaseDirectProductRuntimeError>,
) -> Result<(), CoinbaseDirectProductRuntimeError> {
    match publication {
        Err(source) => match product {
            Ok(()) => Err(source),
            Err(cleanup) => Err(
                CoinbaseDirectProductRuntimeError::ProductPublicationCleanup {
                    source: Box::new(source),
                    cleanup: Box::new(cleanup),
                },
            ),
        },
        Ok(()) => product,
    }
}

fn direct_research_rights(
    lease: &ProviderActivationLease,
    source_id: &market_squawk_domain::SourceId,
) -> Result<ResearchRightsAuthority, CoinbaseDirectSupervisorError> {
    if !lease.admits(DataUseOperation::Persist) {
        return Err(CoinbaseDirectSupervisorError::PublicationConstruction);
    }
    let evidence = lease
        .persistence_evidence()
        .filter(|evidence| !evidence.refresh_required())
        .ok_or(CoinbaseDirectSupervisorError::PublicationConstruction)?;
    let terms_digest = evidence
        .content_digest()
        .ok_or(CoinbaseDirectSupervisorError::PublicationConstruction)?;
    let basis = RightsBasis::reviewed_terms(evidence.official_url(), terms_digest)
        .map_err(|_error| CoinbaseDirectSupervisorError::PublicationConstruction)?;
    ResearchRightsAuthority::try_new(
        source_id.clone(),
        basis,
        lease.rights_decision_digest(),
        lease.verification_expires_at(),
    )
    .map_err(|_error| CoinbaseDirectSupervisorError::PublicationConstruction)
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
    live: LiveRuntimeComposition,
) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
    match live.shutdown().await {
        Ok(_shutdown) => Err(startup),
        Err(rollback) => Err(CoinbaseDirectSupervisorError::StartupRollback {
            startup: Box::new(startup),
            rollback,
        }),
    }
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
    /// No product retained the account start receiver.
    #[error("Coinbase Direct account startup barrier closed")]
    StartupBarrierClosed,
    /// The runtime startup observer was dropped.
    #[error("Coinbase Direct startup observer was dropped")]
    StartupObserverDropped,
    /// The supervisor exited before publishing the startup barrier.
    #[error("Coinbase Direct supervisor exited before startup")]
    SupervisorExitedBeforeStartup,
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
    /// Durable Direct publication authority could not be constructed before network release.
    #[error("Coinbase Direct publication authority construction failed")]
    PublicationConstruction,
}
