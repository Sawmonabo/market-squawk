//! Display-only production source composition for authenticated U.S. market data.

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
use std::num::{NonZeroU32, NonZeroUsize};
use std::{sync::Arc, time::Duration};

use market_squawk_adapter_alpaca::{
    AlpacaCredentials, AlpacaIexLiveConfig, AlpacaOptionsLiveConfig,
};
#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
use market_squawk_adapter_alpaca::{
    AlpacaInstalledFixtureIexConfig, AlpacaScriptedTransportFactory,
};
use market_squawk_adapter_tradier::{
    TradierAccountMarketData, TradierSourceConfig, TradierSubscriptionAuthority,
};
use market_squawk_domain::InstrumentId;
#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
use market_squawk_domain::{ConnectionGeneration, Timestamp};
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
#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
use crate::live_source::provider::InstalledFixtureSourceProfile;
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

    /// Starts one consolidated Tradier display stream with its shared mutable subscription owner.
    #[allow(
        clippy::too_many_arguments,
        reason = "source configuration, account, shared authorities, bounds, and cancellation stay explicit"
    )]
    pub(crate) async fn start_tradier_with_rate_authority(
        app_config: AppConfig,
        directory: DisplayMarketDirectory,
        source: TradierSourceConfig,
        account: Arc<TradierAccountMarketData>,
        subscriptions: TradierSubscriptionAuthority,
        actor_limits: DisplayMarketActorLimits,
        read_admission: DisplayMarketReadAdmission,
        provider_rate: ProviderRateAuthority,
        cancellation: CancellationToken,
    ) -> Result<Self, ProductionDisplaySourceRuntimeError> {
        let routes = display_routes(
            source.metadata(),
            source.mappings().iter().map(|mapping| mapping.instrument()),
            DisplayTopology::Consolidated,
        )?;
        let profile = ProductionSourceProfile::tradier_streaming(source, account, subscriptions)?;
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

/// Owned local installed-fixture display runtime over a private one-route directory.
///
/// The directory is never shared with production resolution, enumeration, ranking, or quota
/// accounting. Reads stay closed until the application registry atomically publishes the exact
/// fixture descriptor and admits this runtime generation.
#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
#[derive(Debug)]
pub(crate) struct InstalledFixtureDisplaySourceRuntime {
    cancellation: DisplaySupervisorCancellation,
    terminal: CancellationToken,
    supervisor: tokio::task::JoinHandle<Result<(), ProductionSupervisorError>>,
    directory: DisplayMarketDirectory,
    read_admission: DisplayMarketReadAdmission,
    key: super::DisplayMarketKey,
    exclusive_expires_at: Timestamp,
    source_shutdown: Duration,
}

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
impl InstalledFixtureDisplaySourceRuntime {
    pub(crate) async fn start(
        app_config: AppConfig,
        config: AlpacaInstalledFixtureIexConfig,
        transport: AlpacaScriptedTransportFactory,
        lifecycle_cancellation: CancellationToken,
        operation_cancellation: &CancellationToken,
        operation_deadline: std::time::Instant,
    ) -> Result<Self, InstalledFixtureDisplayRuntimeError> {
        let metadata = config.metadata();
        let topology = metadata.coverage().topology();
        let [venue] = topology.venues() else {
            return Err(InstalledFixtureDisplayRuntimeError::ContractMismatch);
        };
        let [instrument] = metadata.coverage().instruments().instruments() else {
            return Err(InstalledFixtureDisplayRuntimeError::ContractMismatch);
        };
        if !topology.is_partial() || *instrument != config.aapl_route() {
            return Err(InstalledFixtureDisplayRuntimeError::ContractMismatch);
        }
        let route = DisplayMarketRouteIdentity::try_new(venue, config.aapl_route())?;
        let profile = InstalledFixtureSourceProfile::try_new(config.clone(), transport)?;
        let directory = DisplayMarketDirectory::try_new(
            NonZeroUsize::MIN,
            lifecycle_cancellation.child_token(),
        )?;
        let read_admission = DisplayMarketReadAdmission::closed();
        let paths = LocalPaths::prepare(app_config.data_dir())?;
        let capture_process =
            initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
                app_config.capture_destination_registry_memory_ceiling_bytes(),
            ))?;
        let supervisor = ProductionSourceSupervisor::try_new_installed_fixture_display(
            &app_config,
            profile,
            paths,
            capture_process,
            directory.clone(),
            route,
            installed_fixture_actor_limits()?,
            read_admission.clone(),
        )?;
        let source_shutdown = app_config.source_shutdown();
        let exclusive_expires_at = config.exclusive_expires_at();
        let source_id = metadata.source_id().clone();
        let venue = venue.clone();
        let route_id = config.aapl_route();
        let (startup_sender, startup_receiver) = oneshot::channel();
        let supervisor_cancellation = lifecycle_cancellation.clone();
        let terminal = CancellationToken::new();
        let supervisor_terminal = terminal.clone();
        let mut supervisor_task = tokio::spawn(async move {
            let _terminal_on_exit = supervisor_terminal.drop_guard();
            supervisor
                .run_installed_fixture(supervisor_cancellation, startup_sender)
                .await
        });
        let mut supervisor_joined = false;
        let startup = tokio::select! {
            biased;
            () = operation_cancellation.cancelled() => {
                Err(InstalledFixtureDisplayRuntimeError::Cancelled)
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(operation_deadline)) => {
                Err(InstalledFixtureDisplayRuntimeError::Deadline)
            }
            startup = startup_receiver => match startup {
                Ok(readiness) => Ok(readiness),
                Err(_closed) => {
                    let outcome = (&mut supervisor_task).await;
                    supervisor_joined = true;
                    Err(map_installed_fixture_startup(outcome))
                }
            },
            outcome = &mut supervisor_task => {
                supervisor_joined = true;
                Err(map_installed_fixture_startup(outcome))
            }
        };
        let readiness = match startup {
            Ok(readiness) => readiness,
            Err(error) => {
                lifecycle_cancellation.cancel();
                let cleanup = cleanup_installed_fixture_startup(
                    &mut supervisor_task,
                    supervisor_joined,
                    &directory,
                    source_shutdown,
                )
                .await;
                if let Err(cleanup_error) = cleanup {
                    return Err(cleanup_error);
                }
                return Err(error);
            }
        };
        let key = match super::DisplayMarketKey::try_new(
            &source_id,
            &venue,
            route_id,
            readiness.generation(),
        ) {
            Ok(key) => key,
            Err(error) => {
                lifecycle_cancellation.cancel();
                cleanup_installed_fixture_startup(
                    &mut supervisor_task,
                    false,
                    &directory,
                    source_shutdown,
                )
                .await?;
                return Err(error.into());
            }
        };
        Ok(Self {
            cancellation: DisplaySupervisorCancellation::new(lifecycle_cancellation),
            terminal,
            supervisor: supervisor_task,
            directory,
            read_admission,
            key,
            exclusive_expires_at,
            source_shutdown,
        })
    }

    pub(crate) fn is_healthy(&self) -> bool {
        !self.cancellation.token.is_cancelled() && !self.supervisor.is_finished()
    }

    pub(crate) fn is_published_healthy(&self) -> bool {
        self.read_admission.is_admitted() && self.is_healthy()
    }

    pub(crate) const fn generation(&self) -> ConnectionGeneration {
        self.key.generation()
    }

    pub(crate) const fn key(&self) -> &super::DisplayMarketKey {
        &self.key
    }

    /// Notifies the registry when the sole no-retry fixture supervisor finishes for any reason.
    pub(crate) fn terminal_notification(&self) -> CancellationToken {
        self.terminal.clone()
    }

    pub(crate) fn admit_reads(&self) -> bool {
        self.read_admission.admit()
    }

    pub(crate) fn revoke_reads(&self) {
        self.read_admission.revoke();
    }

    pub(crate) fn read_authority(&self) -> InstalledFixtureDisplayReadAuthority {
        InstalledFixtureDisplayReadAuthority {
            directory: self.directory.clone(),
            key: self.key.clone(),
            exclusive_expires_at: self.exclusive_expires_at,
        }
    }

    pub(crate) fn begin_shutdown(&self) {
        self.revoke_reads();
        self.cancellation.cancel();
    }

    pub(crate) async fn shutdown(mut self) -> Result<(), InstalledFixtureDisplayRuntimeError> {
        self.begin_shutdown();
        let supervisor_result =
            match tokio::time::timeout(self.source_shutdown, &mut self.supervisor).await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(error))) => Err(InstalledFixtureDisplayRuntimeError::Supervisor(error)),
                Ok(Err(error)) => Err(InstalledFixtureDisplayRuntimeError::SupervisorTask(error)),
                Err(_elapsed) => {
                    self.supervisor.abort();
                    let _aborted = self.supervisor.await;
                    Err(InstalledFixtureDisplayRuntimeError::SupervisorShutdownDeadline)
                }
            };
        let cleanup = CancellationToken::new();
        let deadline = std::time::Instant::now()
            .checked_add(self.source_shutdown)
            .ok_or(InstalledFixtureDisplayRuntimeError::ShutdownDeadlineRange)?;
        let directory_result = self
            .directory
            .shutdown(&cleanup, deadline)
            .await
            .map_err(InstalledFixtureDisplayRuntimeError::DisplayDirectory)
            .and_then(|report| {
                if report.is_complete() {
                    Ok(())
                } else {
                    Err(InstalledFixtureDisplayRuntimeError::IncompleteDirectoryShutdown)
                }
            });
        match (supervisor_result, directory_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(_supervisor), Err(_directory)) => {
                Err(InstalledFixtureDisplayRuntimeError::IncompleteShutdown)
            }
        }
    }
}

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
async fn cleanup_installed_fixture_startup(
    supervisor: &mut tokio::task::JoinHandle<Result<(), ProductionSupervisorError>>,
    supervisor_joined: bool,
    directory: &DisplayMarketDirectory,
    shutdown_budget: Duration,
) -> Result<(), InstalledFixtureDisplayRuntimeError> {
    let supervisor_result = if supervisor_joined {
        Ok(())
    } else if supervisor.is_finished() {
        match supervisor.await {
            Ok(Ok(())) | Ok(Err(_)) => Ok(()),
            Err(error) => Err(InstalledFixtureDisplayRuntimeError::SupervisorTask(error)),
        }
    } else {
        match tokio::time::timeout(shutdown_budget, &mut *supervisor).await {
            Ok(Ok(Ok(()))) | Ok(Ok(Err(_))) => Ok(()),
            Ok(Err(error)) => Err(InstalledFixtureDisplayRuntimeError::SupervisorTask(error)),
            Err(_elapsed) => {
                supervisor.abort();
                let _aborted = supervisor.await;
                Err(InstalledFixtureDisplayRuntimeError::SupervisorShutdownDeadline)
            }
        }
    };
    let cleanup = CancellationToken::new();
    let deadline = std::time::Instant::now()
        .checked_add(shutdown_budget)
        .ok_or(InstalledFixtureDisplayRuntimeError::ShutdownDeadlineRange)?;
    let directory_result = directory
        .shutdown(&cleanup, deadline)
        .await
        .map_err(InstalledFixtureDisplayRuntimeError::DisplayDirectory)
        .and_then(|report| {
            report
                .is_complete()
                .then_some(())
                .ok_or(InstalledFixtureDisplayRuntimeError::IncompleteDirectoryShutdown)
        });
    match (supervisor_result, directory_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(_supervisor), Err(_directory)) => {
            Err(InstalledFixtureDisplayRuntimeError::IncompleteShutdown)
        }
    }
}

/// Cloneable read-only handle for the exact private fixture display generation.
#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
#[derive(Clone, Debug)]
pub(crate) struct InstalledFixtureDisplayReadAuthority {
    directory: DisplayMarketDirectory,
    key: super::DisplayMarketKey,
    exclusive_expires_at: Timestamp,
}

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
impl InstalledFixtureDisplayReadAuthority {
    pub(crate) const fn key(&self) -> &super::DisplayMarketKey {
        &self.key
    }

    pub(crate) async fn snapshot(
        &self,
        at: Timestamp,
        cancellation: &CancellationToken,
        deadline: std::time::Instant,
    ) -> Result<super::DisplayMarketSnapshotLease, InstalledFixtureDisplayRuntimeError> {
        let now = system_timestamp()?;
        if now >= self.exclusive_expires_at {
            return Err(InstalledFixtureDisplayRuntimeError::Expired);
        }
        if at >= self.exclusive_expires_at {
            return Err(InstalledFixtureDisplayRuntimeError::Expired);
        }
        let mut snapshots = self
            .directory
            .snapshots_for_instrument(
                self.key.instrument_id(),
                NonZeroUsize::MIN,
                at,
                cancellation,
                deadline,
            )
            .await?;
        let snapshot = snapshots
            .pop()
            .ok_or(InstalledFixtureDisplayRuntimeError::Unavailable)?;
        if system_timestamp()? >= self.exclusive_expires_at {
            return Err(InstalledFixtureDisplayRuntimeError::Expired);
        }
        if !snapshots.is_empty() || snapshot.key() != &self.key {
            return Err(InstalledFixtureDisplayRuntimeError::ContractMismatch);
        }
        Ok(snapshot)
    }
}

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
fn installed_fixture_actor_limits()
-> Result<DisplayMarketActorLimits, InstalledFixtureDisplayRuntimeError> {
    DisplayMarketActorLimits::try_new(
        NonZeroUsize::new(64).ok_or(InstalledFixtureDisplayRuntimeError::StaticPolicy)?,
        NonZeroU32::new(512 * 1024).ok_or(InstalledFixtureDisplayRuntimeError::StaticPolicy)?,
        NonZeroU32::new(128 * 1024).ok_or(InstalledFixtureDisplayRuntimeError::StaticPolicy)?,
        NonZeroUsize::new(8).ok_or(InstalledFixtureDisplayRuntimeError::StaticPolicy)?,
        NonZeroU32::new(512 * 1024).ok_or(InstalledFixtureDisplayRuntimeError::StaticPolicy)?,
        NonZeroU32::new(64 * 1024).ok_or(InstalledFixtureDisplayRuntimeError::StaticPolicy)?,
    )
    .map_err(|_error| InstalledFixtureDisplayRuntimeError::StaticPolicy)
}

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
fn map_installed_fixture_startup(
    outcome: Result<Result<(), ProductionSupervisorError>, tokio::task::JoinError>,
) -> InstalledFixtureDisplayRuntimeError {
    match outcome {
        Ok(Ok(())) => InstalledFixtureDisplayRuntimeError::SupervisorExitedBeforeStartup,
        Ok(Err(error)) => InstalledFixtureDisplayRuntimeError::Supervisor(error),
        Err(error) => InstalledFixtureDisplayRuntimeError::SupervisorTask(error),
    }
}

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
fn system_timestamp() -> Result<Timestamp, InstalledFixtureDisplayRuntimeError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_error| InstalledFixtureDisplayRuntimeError::TrustedTime)?;
    let nanos = i64::try_from(elapsed.as_nanos())
        .map_err(|_error| InstalledFixtureDisplayRuntimeError::TrustedTime)?;
    Ok(Timestamp::from_unix_nanos(nanos))
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
        DisplayTopology::Consolidated => topology.is_consolidated(),
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
    Consolidated,
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

/// Local installed-fixture display startup, read, or bounded cleanup failure.
#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
#[derive(Debug, Error)]
pub(crate) enum InstalledFixtureDisplayRuntimeError {
    #[error("installed fixture contract does not match its one-route display profile")]
    ContractMismatch,
    #[error("installed fixture static display policy is invalid")]
    StaticPolicy,
    #[error("installed fixture display observation is not available")]
    Unavailable,
    #[error("installed fixture display authority has expired")]
    Expired,
    #[error("installed fixture display startup was cancelled")]
    Cancelled,
    #[error("installed fixture display startup exceeded its operation deadline")]
    Deadline,
    #[error("installed fixture trusted wall time is unavailable")]
    TrustedTime,
    #[error("installed fixture display supervisor exited before decoded-quote readiness")]
    SupervisorExitedBeforeStartup,
    #[error("installed fixture display supervisor exceeded its shutdown deadline")]
    SupervisorShutdownDeadline,
    #[error("installed fixture display shutdown deadline cannot be represented")]
    ShutdownDeadlineRange,
    #[error("installed fixture private display directory did not shut down completely")]
    IncompleteDirectoryShutdown,
    #[error("installed fixture supervisor and private display directory both failed shutdown")]
    IncompleteShutdown,
    #[error(transparent)]
    DisplayDirectory(#[from] DisplayMarketDirectoryError),
    #[error(transparent)]
    DisplayRead(#[from] super::DisplayMarketReadError),
    #[error(transparent)]
    Paths(#[from] PathError),
    #[error(transparent)]
    CaptureInfrastructure(#[from] DestinationFenceRegistryInitializationError),
    #[error(transparent)]
    Provider(#[from] ProductionProviderError),
    #[error(transparent)]
    Supervisor(#[from] ProductionSupervisorError),
    #[error("installed fixture display supervisor task failed: {0}")]
    SupervisorTask(#[from] tokio::task::JoinError),
}
