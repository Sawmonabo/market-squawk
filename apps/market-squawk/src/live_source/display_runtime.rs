//! Display-only production source composition for authenticated U.S. market data.

use std::sync::Arc;
use crate::application::{AlpacaPublicationRuntime,AlpacaPublicationRuntimeInput};
use super::super::sink::{AlpacaCapturedPublicationIngress,ProductionCapturedPublicationIngress};

use market_squawk_adapter_alpaca::{
    AlpacaCredentials, AlpacaIexLiveConfig, AlpacaOptionsLiveConfig,
};
use market_squawk_domain::InstrumentId;
use market_squawk_platform::{
    AppConfig, CaptureProcessInfrastructureLimits, DestinationFenceRegistryInitializationError,
    LocalPaths, PathError, initialize_capture_process_infrastructure,
};
use market_squawk_sources::{ProviderRateAuthority, SourceMetadata};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    DisplayMarketActorLimits, DisplayMarketDirectory, DisplayMarketDirectoryError,
    DisplayMarketReadAdmission, DisplayMarketRouteIdentity,
};
use crate::live_source::{
    ProductionCatalogSelection,
    provider::{ProductionProviderError, ProductionSourceProfile},
    supervisor::{
        ProductionSourceSupervisor, ProductionSupervisorError, ProductionSupervisorRunOutcome,
    },
};

/// Owned display-only source runtime with exact-generation cleanup and no execution authority.
#[derive(Debug)]
pub(crate) struct ProductionDisplaySourceRuntime {
    // Drop cancellation is declared first so the supervisor cannot outlive its owner uncancelled.
    supervisor_cancellation: DisplaySupervisorCancellation,
    supervisor: tokio::task::JoinHandle<ProductionSupervisorRunOutcome>,
    publication: AlpacaPublicationRuntime,
    supervisor_result: Option<Result<(), ProductionDisplaySourceRuntimeError>>,
    shutdown_result: Option<Result<(), ProductionDisplaySourceRuntimeError>>,
}

/// Startup cause stays separate from the original supervisor's cleanup result.
#[derive(Debug)]
pub(crate) struct ProductionDisplaySourceStartFailure {
    pub(crate) cause: ProductionDisplaySourceRuntimeError,
    pub(crate) cleanup: Result<(), ProductionDisplaySourceRuntimeError>,
}

impl ProductionDisplaySourceStartFailure {
    fn before_owner(error: impl Into<ProductionDisplaySourceRuntimeError>) -> Self {
        Self {
            cause: error.into(),
            cleanup: Ok(()),
        }
    }
}

impl ProductionDisplaySourceRuntime {
    /// Starts one Alpaca Basic IEX display source against an app-owned shared directory and budget.
    #[allow(
        clippy::too_many_arguments,
        reason = "source configuration, credentials, shared authorities, bounds, and cancellation stay explicit"
    )]
    pub(crate) async fn start_alpaca_iex_with_rate_authority(
        app_config: AppConfig,
        directory: DisplayMarketDirectory,
        source: AlpacaIexLiveConfig,
        credentials: Arc<AlpacaCredentials>,
        publication: AlpacaPublicationRuntimeInput,
        actor_limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        provider_rate: ProviderRateAuthority,
        catalog: ProductionCatalogSelection,
        deadline: std::time::Instant,
        caller_cancellation: &CancellationToken,
        cancellation: CancellationToken,
    ) -> Result<Self, ProductionDisplaySourceStartFailure> {
        let routes = display_routes(
            source.metadata(),
            source.mappings().iter().map(|mapping| mapping.instrument()),
            DisplayTopology::PartialVenue,
        )
        .map_err(ProductionDisplaySourceStartFailure::before_owner)?;
        let profile = ProductionSourceProfile::alpaca_iex(source, credentials, publication.references())
            .map_err(ProductionDisplaySourceStartFailure::before_owner)?;
        Self::start(
            app_config,
            directory,
            profile,
            publication,
            routes,
            actor_limits,
            read_admission,
            provider_rate,
            catalog,
            deadline,
            caller_cancellation,
            cancellation,
        )
        .await
    }

    /// Starts one Alpaca Basic indicative-options display source under its separate quality ceiling.
    #[allow(
        clippy::too_many_arguments,
        reason = "source configuration, credentials, shared authorities, bounds, and cancellation stay explicit"
    )]
    pub(crate) async fn start_alpaca_options_with_rate_authority(
        app_config: AppConfig,
        directory: DisplayMarketDirectory,
        source: AlpacaOptionsLiveConfig,
        credentials: Arc<AlpacaCredentials>,
        publication: AlpacaPublicationRuntimeInput,
        actor_limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        provider_rate: ProviderRateAuthority,
        catalog: ProductionCatalogSelection,
        deadline: std::time::Instant,
        caller_cancellation: &CancellationToken,
        cancellation: CancellationToken,
    ) -> Result<Self, ProductionDisplaySourceStartFailure> {
        let routes = display_routes(
            source.metadata(),
            source.mappings().iter().map(|mapping| mapping.instrument()),
            DisplayTopology::SingleVenue,
        )
        .map_err(ProductionDisplaySourceStartFailure::before_owner)?;
        let profile = ProductionSourceProfile::alpaca_options(source, credentials, publication.references())
            .map_err(ProductionDisplaySourceStartFailure::before_owner)?;
        Self::start(
            app_config,
            directory,
            profile,
            publication,
            routes,
            actor_limits,
            read_admission,
            provider_rate,
            catalog,
            deadline,
            caller_cancellation,
            cancellation,
        )
        .await
    }

    async fn start(
        app_config: AppConfig,
        directory: DisplayMarketDirectory,
        profile: ProductionSourceProfile,
        publication: AlpacaPublicationRuntimeInput,
        routes: Vec<DisplayMarketRouteIdentity>,
        actor_limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        provider_rate: ProviderRateAuthority,
        catalog: ProductionCatalogSelection,
        deadline: std::time::Instant,
        caller_cancellation: &CancellationToken,
        cancellation: CancellationToken,
    ) -> Result<Self, ProductionDisplaySourceStartFailure> {
        let paths = LocalPaths::prepare(app_config.data_dir())
            .map_err(ProductionDisplaySourceStartFailure::before_owner)?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                app_config.capture_destination_registry_memory_ceiling_bytes(),
            ))
            .map_err(ProductionDisplaySourceStartFailure::before_owner)?;
        let (publication_ingress, publication_receiver) = AlpacaCapturedPublicationIngress::try_channel(
            app_config.capture_queue_capacity(), app_config.capture_memory_ceiling_bytes().get(),
        ).map_err(|_| ProductionDisplaySourceStartFailure::before_owner(ProductionDisplaySourceRuntimeError::Allocation))?;
        let supervisor = ProductionSourceSupervisor::try_new_display_with_provider_rate(
            &app_config,
            profile,
            paths,
            capture_process,
            directory.clone(),
            routes,
            actor_limits,
            read_admission,
            provider_rate,
            &catalog,
            deadline,
            caller_cancellation,
        )
        .map_err(|(cause, cleanup)| ProductionDisplaySourceStartFailure {
            cause: ProductionDisplaySourceRuntimeError::Supervisor(cause),
            cleanup: cleanup.map_err(ProductionDisplaySourceRuntimeError::Supervisor),
        })?;
        let supervisor = supervisor.with_publication(ProductionCapturedPublicationIngress::Alpaca(publication_ingress));
        let mut publication = AlpacaPublicationRuntime::start(
            publication, publication_receiver, app_config.source_shutdown().max(app_config.capture_shutdown()), cancellation.clone(),
        );
        let (startup_sender, startup_receiver) = oneshot::channel();
        let supervisor_cancellation = cancellation.clone();
        let mut supervisor_task = tokio::spawn(async move {
            supervisor
                .run_with_cleanup(supervisor_cancellation, startup_sender)
                .await
        });
        let failure = tokio::select! {
            startup = startup_receiver => match startup {
                Ok(()) => return Ok(Self {
                    supervisor_cancellation: DisplaySupervisorCancellation::new(cancellation),
                    supervisor: supervisor_task, publication, supervisor_result: None,
                    shutdown_result: None,
                }),
                Err(_closed) => map_startup_outcome(supervisor_task.await),
            },
            outcome = &mut supervisor_task => map_startup_outcome(outcome),
        };
        publication.begin_shutdown();
        let cleanup = publication.finish_retained_shutdown().await;
        Err(ProductionDisplaySourceStartFailure {
            cause: failure.cause,
            cleanup: failure.cleanup.and(cleanup.map_err(|_| ProductionDisplaySourceRuntimeError::Publication)),
        })
    }

    pub(crate) fn begin_shutdown(&self) {
        self.publication.begin_shutdown();
        self.supervisor_cancellation.cancel();
    }

    /// Reports whether this child supervisor still owns a source generation.
    pub(crate) fn is_healthy(&self) -> bool {
        !self.supervisor_cancellation.token.is_cancelled() && !self.supervisor.is_finished() && self.publication.is_healthy()
    }

    /// Retains the exact child and joined outcome when a shutdown waiter is interrupted.
    pub(crate) async fn finish_shutdown_before(
        &mut self,
        deadline: std::time::Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), market_squawk_services::ServiceError> {
        use market_squawk_services::ServiceError;
        self.publication.begin_shutdown();
        self.supervisor_cancellation.cancel();
        if let Some(result) = &self.shutdown_result {
            return result
                .as_ref()
                .map(|()| ())
                .map_err(|_| ServiceError::Unavailable);
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ServiceError::Cancelled),
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                Err(ServiceError::DeadlineExceeded)
            }
            result = self.finish_retained_shutdown() => result,
        }
    }

    /// Joins under retained startup custody; ordinary waiters use the bounded finish method.
    pub(crate) async fn finish_retained_shutdown(
        &mut self,
    ) -> Result<(), market_squawk_services::ServiceError> {
        use market_squawk_services::ServiceError;
        self.publication.begin_shutdown();
        self.supervisor_cancellation.cancel();
        if let Some(result) = &self.shutdown_result {
            return result
                .as_ref()
                .map(|()| ())
                .map_err(|_| ServiceError::Unavailable);
        }
        if self.supervisor_result.is_none() {
            let outcome = (&mut self.supervisor).await;
            self.supervisor_result = Some(display_cleanup_outcome(outcome));
        }
        let publication_result = self.publication.finish_retained_shutdown().await;
        let result = self.supervisor_result.take().ok_or(ProductionDisplaySourceRuntimeError::Publication)
            .and_then(|result| result)
            .and(publication_result.map_err(|_| ProductionDisplaySourceRuntimeError::Publication));
        let status = match &result {
            Ok(()) => Ok(()),
            Err(error) => {
                tracing::error!(%error, "retained account child cleanup failed");
                Err(ServiceError::Unavailable)
            }
        };
        // No await separates joining the child from retaining its terminal outcome.
        self.shutdown_result = Some(result);
        status
    }


}

fn display_routes(
    metadata: &SourceMetadata,
    instruments: impl ExactSizeIterator<Item = InstrumentId>,
    expected: DisplayTopology,
) -> Result<Vec<DisplayMarketRouteIdentity>, ProductionDisplaySourceRuntimeError> {
    let topology = metadata.coverage().topology();
    let [venue] = topology.venues() else {
        return Err(ProductionDisplaySourceRuntimeError::InvalidCoverageTopology);
    };
    let valid_topology = match expected {
        DisplayTopology::PartialVenue => topology.is_partial(),
        DisplayTopology::SingleVenue => topology.is_single_venue(),
    };
    if !valid_topology || instruments.len() == 0 {
        return Err(ProductionDisplaySourceRuntimeError::InvalidCoverageTopology);
    }
    let mut routes = Vec::new();
    routes
        .try_reserve_exact(instruments.len())
        .map_err(|_error| ProductionDisplaySourceRuntimeError::Allocation)?;
    for instrument in instruments {
        let route = DisplayMarketRouteIdentity::try_new(venue, instrument)?;
        if routes.contains(&route) {
            return Err(ProductionDisplaySourceRuntimeError::DuplicateRoute);
        }
        routes.push(route);
    }
    Ok(routes)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DisplayTopology {
    PartialVenue,
    SingleVenue,
}

fn map_startup_outcome(
    outcome: Result<ProductionSupervisorRunOutcome, tokio::task::JoinError>,
) -> ProductionDisplaySourceStartFailure {
    match outcome {
        Ok(outcome) => ProductionDisplaySourceStartFailure {
            cause: outcome.run.err().map_or(
                ProductionDisplaySourceRuntimeError::SupervisorExitedBeforeStartup,
                ProductionDisplaySourceRuntimeError::Supervisor,
            ),
            cleanup: outcome
                .cleanup
                .map_err(ProductionDisplaySourceRuntimeError::Supervisor),
        },
        Err(error) => ProductionDisplaySourceStartFailure {
            cause: ProductionDisplaySourceRuntimeError::SupervisorExitedBeforeStartup,
            cleanup: Err(ProductionDisplaySourceRuntimeError::SupervisorTask(error)),
        },
    }
}

fn display_cleanup_outcome(
    outcome: Result<ProductionSupervisorRunOutcome, tokio::task::JoinError>,
) -> Result<(), ProductionDisplaySourceRuntimeError> {
    match outcome {
        Ok(outcome) => {
            if let Err(error) = outcome.run {
                tracing::warn!(%error, "display source ended before original cleanup");
            }
            outcome
                .cleanup
                .map_err(ProductionDisplaySourceRuntimeError::Supervisor)
        }
        Err(error) => Err(ProductionDisplaySourceRuntimeError::SupervisorTask(error)),
    }
}

#[derive(Debug)]
struct DisplaySupervisorCancellation {
    token: CancellationToken,
}

impl DisplaySupervisorCancellation {
    const fn new(token: CancellationToken) -> Self {
        Self { token }
    }

    fn cancel(&self) {
        self.token.cancel();
    }
}

impl Drop for DisplaySupervisorCancellation {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// Display-only source startup or bounded shutdown failure.
#[derive(Debug, Error)]
pub(crate) enum ProductionDisplaySourceRuntimeError {
    #[error("Alpaca durable publication worker failed")]
    Publication,
    #[error("display source route allocation failed")]
    Allocation,
    #[error("display source metadata has an incompatible coverage topology")]
    InvalidCoverageTopology,
    #[error("display source contains a duplicate venue/instrument route")]
    DuplicateRoute,
    #[error("display source supervisor exited before first qualified data readiness")]
    SupervisorExitedBeforeStartup,
    #[error(transparent)]
    DisplayDirectory(#[from] DisplayMarketDirectoryError),
    #[error(transparent)]
    Paths(#[from] PathError),
    #[error(transparent)]
    CaptureInfrastructure(#[from] DestinationFenceRegistryInitializationError),
    #[error(transparent)]
    Provider(#[from] ProductionProviderError),
    #[error(transparent)]
    Supervisor(#[from] ProductionSupervisorError),
    #[error("display source supervisor task failed: {0}")]
    SupervisorTask(#[from] tokio::task::JoinError),
}
