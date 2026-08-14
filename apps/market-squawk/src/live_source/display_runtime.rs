//! Display-only production source composition for authenticated U.S. market data.

use std::{sync::Arc, time::Duration};

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
    provider::{ProductionProviderError, ProductionSourceProfile},
    supervisor::{ProductionSourceSupervisor, ProductionSupervisorError},
};

/// Owned display-only source runtime with exact-generation cleanup and no execution authority.
#[derive(Debug)]
pub(crate) struct ProductionDisplaySourceRuntime {
    // Drop cancellation is declared first so the supervisor cannot outlive its owner uncancelled.
    supervisor_cancellation: DisplaySupervisorCancellation,
    supervisor: tokio::task::JoinHandle<Result<(), ProductionSupervisorError>>,
    source_shutdown: Duration,
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
        actor_limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        provider_rate: ProviderRateAuthority,
        cancellation: CancellationToken,
    ) -> Result<Self, ProductionDisplaySourceRuntimeError> {
        let routes = display_routes(
            source.metadata(),
            source.mappings().iter().map(|mapping| mapping.instrument()),
            DisplayTopology::PartialVenue,
        )?;
        let profile = ProductionSourceProfile::alpaca_iex(source, credentials)?;
        Self::start(
            app_config,
            directory,
            profile,
            routes,
            actor_limits,
            read_admission,
            provider_rate,
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
        actor_limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        provider_rate: ProviderRateAuthority,
        cancellation: CancellationToken,
    ) -> Result<Self, ProductionDisplaySourceRuntimeError> {
        let routes = display_routes(
            source.metadata(),
            source.mappings().iter().map(|mapping| mapping.instrument()),
            DisplayTopology::SingleVenue,
        )?;
        let profile = ProductionSourceProfile::alpaca_options(source, credentials)?;
        Self::start(
            app_config,
            directory,
            profile,
            routes,
            actor_limits,
            read_admission,
            provider_rate,
            cancellation,
        )
        .await
    }

    async fn start(
        app_config: AppConfig,
        directory: DisplayMarketDirectory,
        profile: ProductionSourceProfile,
        routes: Vec<DisplayMarketRouteIdentity>,
        actor_limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        provider_rate: ProviderRateAuthority,
        cancellation: CancellationToken,
    ) -> Result<Self, ProductionDisplaySourceRuntimeError> {
        let paths = LocalPaths::prepare(app_config.data_dir())?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                app_config.capture_destination_registry_memory_ceiling_bytes(),
            ))?;
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
        )?;
        let source_shutdown = app_config.source_shutdown();
        let (startup_sender, startup_receiver) = oneshot::channel();
        let supervisor_cancellation = cancellation.clone();
        let mut supervisor_task = tokio::spawn(async move {
            supervisor
                .run(supervisor_cancellation, startup_sender)
                .await
        });
        tokio::select! {
            startup = startup_receiver => match startup {
                Ok(()) => Ok(Self {
                    supervisor_cancellation: DisplaySupervisorCancellation::new(cancellation),
                    supervisor: supervisor_task,
                    source_shutdown,
                }),
                Err(_closed) => Err(map_startup_outcome(supervisor_task.await)),
            },
            outcome = &mut supervisor_task => Err(map_startup_outcome(outcome)),
        }
    }

    /// Reports whether this child supervisor still owns a source generation.
    pub(crate) fn is_healthy(&self) -> bool {
        !self.supervisor_cancellation.token.is_cancelled() && !self.supervisor.is_finished()
    }

    /// Cancels, unregisters, and reaps this source without shutting down the shared directory.
    pub(crate) async fn shutdown(mut self) -> Result<(), ProductionDisplaySourceRuntimeError> {
        self.supervisor_cancellation.cancel();
        match tokio::time::timeout(self.source_shutdown, &mut self.supervisor).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(ProductionDisplaySourceRuntimeError::Supervisor(error)),
            Ok(Err(error)) => Err(ProductionDisplaySourceRuntimeError::SupervisorTask(error)),
            Err(_elapsed) => {
                self.supervisor.abort();
                let _aborted = self.supervisor.await;
                Err(ProductionDisplaySourceRuntimeError::SupervisorShutdownDeadline)
            }
        }
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
    outcome: Result<Result<(), ProductionSupervisorError>, tokio::task::JoinError>,
) -> ProductionDisplaySourceRuntimeError {
    match outcome {
        Ok(Ok(())) => ProductionDisplaySourceRuntimeError::SupervisorExitedBeforeStartup,
        Ok(Err(error)) => ProductionDisplaySourceRuntimeError::Supervisor(error),
        Err(error) => ProductionDisplaySourceRuntimeError::SupervisorTask(error),
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
    #[error("display source route allocation failed")]
    Allocation,
    #[error("display source metadata has an incompatible coverage topology")]
    InvalidCoverageTopology,
    #[error("display source contains a duplicate venue/instrument route")]
    DuplicateRoute,
    #[error("display source supervisor exited before first qualified data readiness")]
    SupervisorExitedBeforeStartup,
    #[error("display source supervisor exceeded its shutdown deadline")]
    SupervisorShutdownDeadline,
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
