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
use std::sync::Arc;
use std::time::Duration;

use market_squawk_adapter_coinbase::{CoinbaseDirectHmacSigner, CoinbaseDirectSigningError};
use market_squawk_live::{LiveRuntimeConfig, LiveSnapshotReader, RouteActionHook};
use market_squawk_platform::{
    CaptureProcessInfrastructureLimits, DestinationFenceRegistryInitializationError,
    initialize_capture_process_infrastructure,
};
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    ProviderOnboardingError,
    live_runtime::{LiveRuntimeComposition, LiveRuntimeCompositionError},
    provider_activation::CoinbaseDirectAccountActivation,
};

use evidence::try_build_product_spec;
pub use output::CoinbaseDirectOutputFailure;
pub use product::CoinbaseDirectProductRuntimeError;
use product::{ProductReady, ProductRuntimeSpec, run_product};

use super::{composition::SupervisorDropCancellation, route_actor::RouteBufferLimits};

const ONBOARDING_CURRENTNESS_INTERVAL: Duration = Duration::from_secs(1);
type ProductTaskOutput = (usize, Result<(), CoinbaseDirectProductRuntimeError>);
type ProductJoinOutcome = Option<Result<ProductTaskOutput, JoinError>>;

/// Complete account owner for authenticated Coinbase Direct market data.
#[derive(Debug)]
pub struct CoinbaseDirectLiveRuntime {
    supervisor_cancellation: SupervisorDropCancellation,
    live: LiveRuntimeComposition,
    supervisor: tokio::task::JoinHandle<Result<(), CoinbaseDirectSupervisorError>>,
    shutdown_deadline: Duration,
}

impl CoinbaseDirectLiveRuntime {
    /// Returns authority-free immutable snapshot access.
    pub fn snapshots(&self) -> LiveSnapshotReader {
        self.live.snapshots()
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
        cancellation: CancellationToken,
    ) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
        start_account(self, runtime_config, None, cancellation).await
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
        start_account(self, runtime_config, Some(action_hooks), cancellation).await
    }
}

async fn start_account(
    mut activation: CoinbaseDirectAccountActivation,
    runtime_config: LiveRuntimeConfig,
    action_hooks: Option<Vec<RouteActionHook>>,
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
            specs.push(try_build_product_spec(slot, activation.lease(), product)?);
        }
    }
    if specs.len() != product_count {
        return Err(CoinbaseDirectSupervisorError::ActivationTopology);
    }
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
        signer,
        app_config,
        admission,
        capture_process,
        route_buffer_limits,
        live,
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
    signer: Arc<CoinbaseDirectHmacSigner>,
    app_config: market_squawk_platform::AppConfig,
    admission: crate::provider_activation::CoinbaseDirectRuntimeAdmission,
    capture_process: market_squawk_platform::CaptureProcessInfrastructure,
    route_buffer_limits: RouteBufferLimits,
    live: LiveRuntimeComposition,
    cancellation: CancellationToken,
) -> Result<CoinbaseDirectLiveRuntime, CoinbaseDirectSupervisorError> {
    let live_ingress = live.production_ingress();
    let shutdown_deadline = app_config.source_shutdown();
    let (startup_sender, startup_receiver) = oneshot::channel();
    let supervisor_cancellation = cancellation.clone();
    let mut supervisor = tokio::spawn(async move {
        run_account(
            activation,
            specs,
            signer,
            app_config,
            admission,
            capture_process,
            route_buffer_limits,
            live_ingress,
            supervisor_cancellation,
            startup_sender,
        )
        .await
    });
    tokio::select! {
        startup = startup_receiver => match startup {
            Ok(()) => Ok(CoinbaseDirectLiveRuntime {
                supervisor_cancellation: SupervisorDropCancellation::new(cancellation),
                live,
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
        let task_config = app_config.clone();
        let provider_rate = activation.provider_rate().clone();
        let account_subject = activation.account_subject().clone();
        let task_signer = Arc::clone(&signer);
        let task_ready = ready_sender.clone();
        let task_start = start_receiver.clone();
        let task_cancellation = cancellation.child_token();
        let task_ingress = live_ingress.clone();
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
}
