//! Durable, supervisor-owned lifecycle for the sealed Coinbase production source.

use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use market_squawk_domain::{ConnectionGeneration, IdentityError, SourceIdentifier};
use market_squawk_live::{LiveIngressBindError, LiveRuntimeIngress, ShardKey};
use market_squawk_platform::{
    AppConfig, CaptureChannelError, CaptureChannelLimits, CaptureGenerationError,
    CaptureProcessInfrastructure, CaptureWriterPolicy, CaptureWriterPolicyError,
    LocalAuthorityStateStore, LocalAuthorityStateStoreError, LocalPaths,
    ProcessCaptureShutdownDisposition, ProcessCaptureShutdownPolicy,
    ProcessCaptureShutdownPolicyError, ProcessCaptureWriterSpawnError, ProcessJournalCaptureConfig,
    ProcessJournalCaptureConfigError, RawCaptureControl, raw_capture_channel,
    spawn_process_journal_capture_writer,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, BudgetUnavailableReason, CaptureGenerationCapabilities,
    ProviderBackoffAuthority, ProviderBackoffDecision, ProviderBackoffError, ProviderRateAuthority,
    RegisteredSource, RegistryError, SessionId, SourceError,
};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
use super::provider::InstalledFixtureSourceProfile;
use super::{
    composition::{ProductionCoinbaseProfileError, system_timestamp},
    display_market::{
        DisplayMarketActorLimits, DisplayMarketActorShutdown, DisplayMarketDirectory,
        DisplayMarketIngress, DisplayMarketKey, DisplayMarketMonitorError,
        DisplayMarketRouteIdentity, DisplayMarketSupervisorMonitor, DisplayMarketTerminalFailure,
    },
    provider::{ProductionLiveSource, ProductionProviderError, ProductionSourceProfile},
    route_actor::{RouteActorWorker, RouteBufferLimits, spawn_route_activation},
    sink::{
        ProductionDisplayMarketSinkInput, ProductionRawMarketSink, ProductionRawMarketSinkInput,
        ProductionSinkConstructionError, ProductionSinkFailure,
    },
    subscription_state::{
        GenerationIdentity, SubscriptionConstructionError, SubscriptionLimits,
        SubscriptionStateMachine,
    },
};

const CAPTURE_FLUSH_RECORDS: usize = 256;
// A freshly linked helper can incur first-execution operating-system verification before it can
// complete the authenticated readiness handshake. Startup and shutdown are different policies:
// keeping this bounded deadline independent prevents the five-second shutdown budget from
// incorrectly quarantining a healthy source after a rebuild.
const CAPTURE_HELPER_STARTUP_DEADLINE: Duration = Duration::from_secs(30);
const BACKOFF_JITTER_SAMPLE_BASIS_POINTS: u16 = 1_000;

/// One completed exact-generation source run after all generation-owned resources were reaped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProductionGenerationOutcome {
    generation: ConnectionGeneration,
    source_error: Option<SourceError>,
    startup_required: bool,
    startup_ready: bool,
}

impl ProductionGenerationOutcome {
    #[cfg(test)]
    pub(super) const fn generation(self) -> ConnectionGeneration {
        self.generation
    }

    pub(super) const fn source_error(self) -> Option<SourceError> {
        self.source_error
    }

    const fn failed_before_startup_readiness(self) -> bool {
        self.startup_required && !self.startup_ready
    }
}

/// Sole owner of durable source authority and exact-generation lifecycle transitions.
#[derive(Debug)]
pub(super) struct ProductionSourceSupervisor {
    config: AppConfig,
    profile: SupervisorSourceProfile,
    registry: Option<AuthoritativeSourceRegistry>,
    registered: RegisteredSource,
    refusal: SupervisorRefusalPolicy,
    paths: LocalPaths,
    capture_process: CaptureProcessInfrastructure,
    output: ProductionSupervisorOutput,
}

#[derive(Debug)]
enum SupervisorSourceProfile {
    Production(ProductionSourceProfile),
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    InstalledFixture(InstalledFixtureSourceProfile),
}

impl SupervisorSourceProfile {
    fn metadata(&self) -> &market_squawk_sources::SourceMetadata {
        match self {
            Self::Production(profile) => profile.metadata(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => profile.metadata(),
        }
    }

    const fn source_key(&self) -> &'static str {
        match self {
            Self::Production(profile) => profile.source_key(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => profile.source_key(),
        }
    }

    fn authority_path(&self, paths: &LocalPaths) -> std::path::PathBuf {
        let base = paths.root().join("authority").join(self.source_key());
        match self {
            Self::Production(_) => base,
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => base.join(
                profile
                    .metadata()
                    .revision()
                    .as_source_identifier()
                    .as_str(),
            ),
        }
    }

    fn subscription_product_snapshot(&self) -> Result<Vec<String>, ProductionProviderError> {
        match self {
            Self::Production(profile) => profile.subscription_product_snapshot(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => profile.subscription_product_snapshot(),
        }
    }

    const fn subscription_ack_timeout(&self) -> Duration {
        match self {
            Self::Production(profile) => profile.subscription_ack_timeout(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => profile.subscription_ack_timeout(),
        }
    }

    const fn control_message_capacity(&self) -> usize {
        match self {
            Self::Production(profile) => profile.control_message_capacity(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => profile.control_message_capacity(),
        }
    }

    const fn control_byte_capacity(&self) -> usize {
        match self {
            Self::Production(profile) => profile.control_byte_capacity(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => profile.control_byte_capacity(),
        }
    }

    const fn pre_acknowledgement_data_message_capacity(&self) -> usize {
        match self {
            Self::Production(profile) => profile.pre_acknowledgement_data_message_capacity(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(_) => 0,
        }
    }

    const fn pre_acknowledgement_data_byte_capacity(&self) -> usize {
        match self {
            Self::Production(profile) => profile.pre_acknowledgement_data_byte_capacity(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(_) => 0,
        }
    }

    const fn subscription_acknowledgement_policy(
        &self,
    ) -> super::subscription_state::SubscriptionAcknowledgementPolicy {
        match self {
            Self::Production(profile) => profile.subscription_acknowledgement_policy(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(_) => {
                super::subscription_state::SubscriptionAcknowledgementPolicy::ExplicitProviderFrame
            }
        }
    }

    fn decoder(&self) -> Result<super::provider::ProductionMarketDecoder, ProductionProviderError> {
        match self {
            Self::Production(profile) => profile.decoder(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => profile.decoder(),
        }
    }

    fn try_source(
        &self,
        generation: market_squawk_sources::LiveSourceGeneration,
    ) -> Result<ProductionLiveSource, ProductionProviderError> {
        match self {
            Self::Production(profile) => profile.try_source(generation),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(profile) => profile.try_source(generation),
        }
    }

    const fn supports_display_output(&self) -> bool {
        match self {
            Self::Production(profile) => profile.supports_display_output(),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            Self::InstalledFixture(_) => true,
        }
    }
}

#[derive(Debug)]
enum SupervisorRefusalPolicy {
    Provider(ProviderBackoffAuthority),
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    InstalledFixtureNoRetry,
}

/// Decoder-validated exact-AAPL subscription and first-quote readiness for one fixture generation.
#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct InstalledFixtureSourceReadiness {
    generation: ConnectionGeneration,
}

#[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
impl InstalledFixtureSourceReadiness {
    pub(super) const fn generation(self) -> ConnectionGeneration {
        self.generation
    }
}

#[derive(Debug)]
enum ProductionSupervisorOutput {
    Live {
        ingress: LiveRuntimeIngress,
        routes: Vec<ShardKey>,
        buffer_limits: RouteBufferLimits,
    },
    Display {
        directory: DisplayMarketDirectory,
        routes: Vec<DisplayMarketRouteIdentity>,
        actor_limits: DisplayMarketActorLimits,
        read_admission: super::display_market::DisplayMarketReadAdmission,
    },
}

#[derive(Debug)]
enum PreparedGenerationOutput {
    Live {
        ingress: LiveRuntimeIngress,
        route_publishers: Vec<super::route_actor::RouteActivationPublisher>,
    },
    Display {
        display_ingresses: Vec<DisplayMarketIngress>,
    },
}

impl ProductionSourceSupervisor {
    #[cfg(test)]
    pub(super) fn try_new(
        config: &AppConfig,
        profile: ProductionSourceProfile,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        live_ingress: LiveRuntimeIngress,
        routes: Vec<ShardKey>,
        route_buffer_limits: RouteBufferLimits,
    ) -> Result<Self, ProductionSupervisorError> {
        let provider_rate =
            crate::provider_rate::open_provider_rate_authority(paths.control_root()?.root())?;
        Self::try_new_with_provider_rate(
            config,
            profile,
            paths,
            capture_process,
            live_ingress,
            routes,
            route_buffer_limits,
            provider_rate,
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "independent live-plane dependencies stay explicit at supervisor composition"
    )]
    pub(super) fn try_new_with_provider_rate(
        config: &AppConfig,
        profile: ProductionSourceProfile,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        live_ingress: LiveRuntimeIngress,
        routes: Vec<ShardKey>,
        route_buffer_limits: RouteBufferLimits,
        provider_rate: ProviderRateAuthority,
    ) -> Result<Self, ProductionSupervisorError> {
        let output = ProductionSupervisorOutput::Live {
            ingress: live_ingress,
            routes,
            buffer_limits: route_buffer_limits,
        };
        Self::try_new_with_output(
            config,
            SupervisorSourceProfile::Production(profile),
            paths,
            capture_process,
            output,
            Some(provider_rate),
        )
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "display source composition keeps directory, bounds, and durable authority explicit"
    )]
    pub(super) fn try_new_display_with_provider_rate(
        config: &AppConfig,
        profile: ProductionSourceProfile,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        directory: DisplayMarketDirectory,
        routes: Vec<DisplayMarketRouteIdentity>,
        actor_limits: DisplayMarketActorLimits,
        read_admission: super::display_market::DisplayMarketReadAdmission,
        provider_rate: ProviderRateAuthority,
    ) -> Result<Self, ProductionSupervisorError> {
        if !profile.supports_display_output() {
            return Err(ProductionSupervisorError::UnsupportedDisplayProvider);
        }
        if routes.is_empty() {
            return Err(ProductionSupervisorError::MissingDisplayRoutes);
        }
        for (index, route) in routes.iter().enumerate() {
            if routes[index.saturating_add(1)..].contains(route) {
                return Err(ProductionSupervisorError::DuplicateDisplayRoute);
            }
        }
        let output = ProductionSupervisorOutput::Display {
            directory,
            routes,
            actor_limits,
            read_admission,
        };
        Self::try_new_with_output(
            config,
            SupervisorSourceProfile::Production(profile),
            paths,
            capture_process,
            output,
            Some(provider_rate),
        )
    }

    /// Constructs the single local installed-fixture generation without provider budget or
    /// provider backoff authority.
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    #[allow(
        clippy::too_many_arguments,
        reason = "the local fixture keeps every bounded capture/display dependency explicit"
    )]
    pub(super) fn try_new_installed_fixture_display(
        config: &AppConfig,
        profile: InstalledFixtureSourceProfile,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        directory: DisplayMarketDirectory,
        route: DisplayMarketRouteIdentity,
        actor_limits: DisplayMarketActorLimits,
        read_admission: super::display_market::DisplayMarketReadAdmission,
    ) -> Result<Self, ProductionSupervisorError> {
        let output = ProductionSupervisorOutput::Display {
            directory,
            routes: vec![route],
            actor_limits,
            read_admission,
        };
        Self::try_new_with_output(
            config,
            SupervisorSourceProfile::InstalledFixture(profile),
            paths,
            capture_process,
            output,
            None,
        )
    }

    fn try_new_with_output(
        config: &AppConfig,
        profile: SupervisorSourceProfile,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        output: ProductionSupervisorOutput,
        provider_rate: Option<ProviderRateAuthority>,
    ) -> Result<Self, ProductionSupervisorError> {
        let registered_at = system_timestamp()?;
        let authority_store = LocalAuthorityStateStore::try_open(profile.authority_path(&paths))?;
        let mut registry = match provider_rate {
            Some(provider_rate) => AuthoritativeSourceRegistry::try_new_durable_with_provider_rate(
                authority_store,
                provider_rate,
            )?,
            None => AuthoritativeSourceRegistry::try_new_durable(authority_store)?,
        };
        let registered = match registry
            .register_or_resume_exact(profile.metadata().clone(), registered_at)
        {
            Ok(registered) => registered,
            Err(source) => {
                return match registry.shutdown() {
                    Ok(()) => Err(ProductionSupervisorError::Registry(source)),
                    Err(cleanup) => {
                        Err(ProductionSupervisorError::RegistryStartupCleanup { source, cleanup })
                    }
                };
            }
        };
        let refusal = match &profile {
            SupervisorSourceProfile::Production(_) => {
                SupervisorRefusalPolicy::Provider(registry.provider_backoff_authority(&registered)?)
            }
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            SupervisorSourceProfile::InstalledFixture(_) => {
                SupervisorRefusalPolicy::InstalledFixtureNoRetry
            }
        };
        Ok(Self {
            config: config.clone(),
            profile,
            registry: Some(registry),
            registered,
            refusal,
            paths,
            capture_process,
            output,
        })
    }

    async fn run_one_generation(
        &mut self,
        cancellation: CancellationToken,
        startup: &mut Option<oneshot::Sender<()>>,
        generation_startup: &mut Option<oneshot::Sender<ConnectionGeneration>>,
    ) -> Result<ProductionGenerationOutcome, ProductionSupervisorError> {
        let at = system_timestamp()?;
        let session_id = SessionId::new(SourceIdentifier::try_from(format!(
            "{}-{}",
            self.profile.source_key(),
            uuid::Uuid::new_v4()
        ))?);
        let output_route_count = match &self.output {
            ProductionSupervisorOutput::Live { routes, .. } => routes.len(),
            ProductionSupervisorOutput::Display { routes, .. } => routes.len(),
        };
        let mut route_workers = Vec::new();
        let mut display_monitors = Vec::new();
        route_workers
            .try_reserve_exact(output_route_count)
            .map_err(|_error| ProductionSupervisorError::AllocationFailed)?;
        display_monitors
            .try_reserve_exact(output_route_count)
            .map_err(|_error| ProductionSupervisorError::AllocationFailed)?;
        let registry = self
            .registry
            .as_mut()
            .ok_or(ProductionSupervisorError::AlreadyShutdown)?;
        let session = registry.begin_next_session(&self.registered, session_id, at)?;
        let generation = session.generation();
        if let Some(observer) = generation_startup.take() {
            observer
                .send(generation)
                .map_err(|_value| ProductionSupervisorError::InstalledFixtureObserverDropped)?;
        }
        let startup_required = startup.is_some();
        let route_cancellation = cancellation.child_token();
        let mut capture_control = None;
        let mut writer_handle = None;

        let source_result: Result<(Option<SourceError>, bool), ProductionSupervisorError> = async {
            let capabilities = registry.take_capture_generation_capabilities(&session)?;
            let health_reporter = registry.take_current_health_reporter(&session)?;
            let (publisher, control, writer) = raw_capture_channel(
                &self.capture_process,
                CaptureChannelLimits::new(
                    self.config.capture_queue_capacity(),
                    self.config.capture_memory_ceiling_bytes(),
                ),
                capabilities,
            )?;
            let flush_records = NonZeroUsize::new(CAPTURE_FLUSH_RECORDS)
                .ok_or(ProductionSupervisorError::InvalidStaticPolicy)?;
            let policy =
                CaptureWriterPolicy::try_new(flush_records, self.config.capture_flush_interval())?;
            let process_config = ProcessJournalCaptureConfig::try_new(
                self.paths.root(),
                self.profile.source_key(),
                CAPTURE_HELPER_STARTUP_DEADLINE,
            )?;
            let handle = spawn_process_journal_capture_writer(writer, process_config, policy)?;
            capture_control = Some(control);
            writer_handle = Some(handle);
            activate_owned_capture(&mut capture_control, &writer_handle)?;
            let source_generation = registry.take_live_source_generation(&session)?;

            let prepared_output = match &self.output {
                ProductionSupervisorOutput::Live {
                    ingress,
                    routes,
                    buffer_limits,
                } => {
                    let mut route_publishers = Vec::new();
                    route_publishers
                        .try_reserve_exact(routes.len())
                        .map_err(|_error| ProductionSupervisorError::AllocationFailed)?;
                    for route in routes {
                        let dormant = ingress.reserve_route(route.clone())?;
                        let (publisher, worker) = spawn_route_activation(
                            dormant,
                            *buffer_limits,
                            route_cancellation.clone(),
                        );
                        route_publishers.push(publisher);
                        route_workers.push(worker);
                    }
                    PreparedGenerationOutput::Live {
                        ingress: ingress.clone(),
                        route_publishers,
                    }
                }
                ProductionSupervisorOutput::Display {
                    directory,
                    routes,
                    actor_limits,
                    read_admission,
                } => {
                    let registration_deadline = Instant::now()
                        .checked_add(self.config.source_shutdown())
                        .ok_or(ProductionSupervisorError::DisplayDeadlineRange)?;
                    let mut display_ingresses = Vec::new();
                    display_ingresses
                        .try_reserve_exact(routes.len())
                        .map_err(|_error| ProductionSupervisorError::AllocationFailed)?;
                    for route in routes {
                        let key = DisplayMarketKey::try_new(
                            self.profile.metadata().source_id(),
                            route.venue_id(),
                            route.instrument_id(),
                            generation,
                        )
                        .map_err(|error| {
                            tracing::error!(%error, "display-market route key is invalid");
                            ProductionSupervisorError::DisplayDirectory
                        })?;
                        let registration = directory
                            .register(
                                key,
                                *actor_limits,
                                read_admission.clone(),
                                &cancellation,
                                registration_deadline,
                            )
                            .await
                            .map_err(|error| {
                                tracing::error!(%error, "display-market registration failed");
                                ProductionSupervisorError::DisplayDirectory
                            })?;
                        let (ingress, monitor) = registration.into_parts();
                        display_ingresses.push(ingress);
                        display_monitors.push(monitor);
                    }
                    PreparedGenerationOutput::Display { display_ingresses }
                }
            };

            let subscription_products = self.profile.subscription_product_snapshot()?;
            let subscription = SubscriptionStateMachine::try_new_with_policy(
                GenerationIdentity::from_session(&session),
                subscription_products.iter().map(String::as_str),
                self.profile.subscription_ack_timeout(),
                Instant::now(),
                SubscriptionLimits::try_new(
                    self.profile.control_message_capacity(),
                    self.profile.control_byte_capacity(),
                    self.profile.pre_acknowledgement_data_message_capacity(),
                    self.profile.pre_acknowledgement_data_byte_capacity(),
                )?,
                self.profile.subscription_acknowledgement_policy(),
            )?;
            tracing::debug!(
                source = self.profile.source_key(),
                generation = session.generation().get(),
                subscription_state_peak_bytes = subscription.estimated_peak_bytes().get(),
                "prepared bounded production subscription state"
            );
            let mut source = self.profile.try_source(source_generation)?;
            let decoder = self.profile.decoder()?;
            let mut sink = match prepared_output {
                PreparedGenerationOutput::Live {
                    ingress,
                    route_publishers,
                } => {
                    let input = ProductionRawMarketSinkInput {
                        capture: publisher,
                        registry,
                        session: &session,
                        health_reporter,
                        decoder,
                        subscription,
                        live_ingress: ingress,
                        routes: route_publishers,
                    };
                    match startup.take() {
                        Some(readiness) => ProductionRawMarketSink::try_new_with_startup_readiness(
                            input, readiness,
                        )?,
                        None => ProductionRawMarketSink::try_new(input)?,
                    }
                }
                PreparedGenerationOutput::Display { display_ingresses } => {
                    let input = ProductionDisplayMarketSinkInput {
                        capture: publisher,
                        registry,
                        session: &session,
                        health_reporter,
                        decoder,
                        subscription,
                        display_ingresses,
                        ingress_timeout: self.config.source_shutdown(),
                    };
                    match startup.take() {
                        Some(readiness) => {
                            ProductionRawMarketSink::try_new_display_with_startup_readiness(
                                input, readiness,
                            )?
                        }
                        None => ProductionRawMarketSink::try_new_display(input)?,
                    }
                }
            };
            let result = if display_monitors.is_empty() {
                source.run(&mut sink, cancellation.clone()).await
            } else {
                run_display_source(
                    &mut source,
                    &mut sink,
                    cancellation.clone(),
                    &mut display_monitors,
                    self.config.source_shutdown(),
                )
                .await?
            };
            let terminal = sink.terminal_failure();
            let startup_ready = sink.startup_ready();
            if let Some(failure) = terminal {
                tracing::warn!(
                    source = self.profile.source_key(),
                    generation = generation.get(),
                    failure = %failure,
                    "production source generation stopped after a sink failure"
                );
            }
            drop(sink);
            let source_error = match (result, terminal) {
                (Err(_error), Some(failure)) if failure.requires_generation_resynchronization() => {
                    Ok(Some(SourceError::GenerationResynchronizationRequired))
                }
                (Err(_error), Some(failure)) => Err(ProductionSupervisorError::Sink(failure)),
                (Err(error), None) => Ok(Some(error)),
                (Ok(()), Some(failure)) if failure.requires_generation_resynchronization() => {
                    Ok(Some(SourceError::GenerationResynchronizationRequired))
                }
                (Ok(()), Some(failure)) => Err(ProductionSupervisorError::Sink(failure)),
                (Ok(()), None) => Ok(None),
            }?;
            Ok((source_error, startup_ready))
        }
        .await;

        route_cancellation.cancel();
        let mut cleanup_error = None;
        for worker in route_workers {
            let route_result = route_worker_cleanup_error(worker).await;
            if cleanup_error.is_none() {
                cleanup_error = route_result;
            }
        }
        if let ProductionSupervisorOutput::Display { directory, .. } = &self.output {
            let display_result = unregister_display_generation(
                directory,
                &display_monitors,
                self.config.source_shutdown(),
            )
            .await;
            if cleanup_error.is_none() {
                cleanup_error = display_result;
            }
        }
        if let Err(error) = registry.end_session(&session, at)
            && cleanup_error.is_none()
        {
            cleanup_error = Some(ProductionSupervisorError::Registry(error));
        }
        if let Some(mut control) = capture_control {
            control.invalidate_current();
            drop(control);
        }
        if let Some(handle) = writer_handle {
            let shutdown_policy = ProcessCaptureShutdownPolicy::try_new(
                self.config.capture_shutdown(),
                self.config.capture_shutdown(),
            )?;
            let shutdown = handle.shutdown(shutdown_policy).await;
            let clean = shutdown.disposition() == ProcessCaptureShutdownDisposition::Complete
                && shutdown.helper_reaped()
                && shutdown.worker_termination().is_some_and(|termination| {
                    !termination.shutdown_deadline_elapsed()
                        && !termination.outcome().is_incomplete()
                });
            if !clean && cleanup_error.is_none() {
                cleanup_error = Some(ProductionSupervisorError::IncompleteCaptureShutdown(
                    shutdown,
                ));
            }
        }
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        let (source_error, startup_ready) = source_result?;
        Ok(ProductionGenerationOutcome {
            generation,
            source_error,
            startup_required,
            startup_ready,
        })
    }

    pub(super) async fn run(
        mut self,
        cancellation: CancellationToken,
        startup: oneshot::Sender<()>,
    ) -> Result<(), ProductionSupervisorError> {
        #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
        if !matches!(self.refusal, SupervisorRefusalPolicy::Provider(_)) {
            return Err(ProductionSupervisorError::InstalledFixtureEntryPointRequired);
        }
        let mut startup = Some(startup);
        let run = self.run_loop(&cancellation, &mut startup).await;
        let shutdown = self.shutdown();
        match (run, shutdown) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(source), Ok(())) => Err(source),
            (Ok(()), Err(shutdown)) => Err(shutdown),
            (Err(source), Err(shutdown)) => Err(ProductionSupervisorError::RunShutdown {
                source: Box::new(source),
                shutdown: Box::new(shutdown),
            }),
        }
    }

    /// Runs exactly one local installed-fixture generation and never enters provider backoff.
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    pub(super) async fn run_installed_fixture(
        mut self,
        cancellation: CancellationToken,
        startup: oneshot::Sender<InstalledFixtureSourceReadiness>,
    ) -> Result<(), ProductionSupervisorError> {
        if !matches!(
            self.refusal,
            SupervisorRefusalPolicy::InstalledFixtureNoRetry
        ) {
            return Err(ProductionSupervisorError::InstalledFixtureEntryPointRequired);
        }
        let generation_result = {
            let generation_cancellation = cancellation.child_token();
            let (data_ready_sender, data_ready_receiver) = oneshot::channel();
            let (generation_sender, generation_receiver) = oneshot::channel();
            let mut data_ready_sender = Some(data_ready_sender);
            let mut generation_sender = Some(generation_sender);
            let generation_run = self.run_one_generation(
                generation_cancellation.clone(),
                &mut data_ready_sender,
                &mut generation_sender,
            );
            tokio::pin!(generation_run);
            let readiness = async move {
                let generation = generation_receiver.await.map_err(|_closed| {
                    ProductionSupervisorError::InstalledFixtureObserverDropped
                })?;
                data_ready_receiver.await.map_err(|_closed| {
                    ProductionSupervisorError::InstalledFixtureObserverDropped
                })?;
                Ok::<_, ProductionSupervisorError>(InstalledFixtureSourceReadiness { generation })
            };
            tokio::pin!(readiness);

            let (completed, observer_failure) = tokio::select! {
                biased;
                ready = &mut readiness => match ready {
                    Ok(ready) => {
                        if startup.send(ready).is_ok() {
                            (None, None)
                        } else {
                            (
                                None,
                                Some(ProductionSupervisorError::InstalledFixtureObserverDropped),
                            )
                        }
                    }
                    Err(error) => (None, Some(error)),
                },
                completed = generation_run.as_mut() => (Some(completed), None),
            };
            match (completed, observer_failure) {
                (Some(result), None) => result,
                (None, Some(error)) => {
                    generation_cancellation.cancel();
                    let _cleanup = generation_run.as_mut().await;
                    Err(error)
                }
                (None, None) => generation_run.as_mut().await,
                (Some(_), Some(_)) => {
                    Err(ProductionSupervisorError::InstalledFixtureObserverDropped)
                }
            }
        };
        let run = generation_result.and_then(|outcome| {
            if outcome.failed_before_startup_readiness() {
                return match outcome.source_error() {
                    Some(SourceError::Cancelled) if cancellation.is_cancelled() => Ok(()),
                    Some(source) => Err(ProductionSupervisorError::SourceFailedBeforeReadiness(
                        source,
                    )),
                    None => Err(ProductionSupervisorError::SourceCompletedBeforeReadiness),
                };
            }
            match outcome.source_error() {
                Some(SourceError::Cancelled) if cancellation.is_cancelled() => Ok(()),
                Some(SourceError::SessionNotCurrent) => Ok(()),
                Some(source) => Err(ProductionSupervisorError::InstalledFixtureRefused(source)),
                None => Err(ProductionSupervisorError::InstalledFixtureCompleted),
            }
        });
        let shutdown = self.shutdown();
        match (run, shutdown) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(source), Ok(())) => Err(source),
            (Ok(()), Err(shutdown)) => Err(shutdown),
            (Err(source), Err(shutdown)) => Err(ProductionSupervisorError::RunShutdown {
                source: Box::new(source),
                shutdown: Box::new(shutdown),
            }),
        }
    }

    async fn run_loop(
        &mut self,
        cancellation: &CancellationToken,
        startup: &mut Option<oneshot::Sender<()>>,
    ) -> Result<(), ProductionSupervisorError> {
        loop {
            if cancellation.is_cancelled() {
                return Ok(());
            }
            let outcome = self
                .run_one_generation(cancellation.child_token(), startup, &mut None)
                .await?;
            if outcome.failed_before_startup_readiness() {
                return match outcome.source_error() {
                    Some(SourceError::Cancelled) if cancellation.is_cancelled() => Ok(()),
                    Some(source) => Err(ProductionSupervisorError::SourceFailedBeforeReadiness(
                        source,
                    )),
                    None => Err(ProductionSupervisorError::SourceCompletedBeforeReadiness),
                };
            }
            let Some(error) = outcome.source_error() else {
                self.wait_after_refusal(cancellation).await?;
                continue;
            };
            match error {
                SourceError::Cancelled if cancellation.is_cancelled() => return Ok(()),
                SourceError::BudgetWaitUntil { deadline } => {
                    self.wait_until(cancellation, deadline).await?;
                }
                SourceError::BudgetUnavailable { reason } => {
                    return Err(ProductionSupervisorError::BudgetUnavailable(reason));
                }
                SourceError::Network
                | SourceError::ConnectionIdle
                | SourceError::GenerationResynchronizationRequired
                | SourceError::ProviderUnavailable => {
                    self.wait_after_refusal(cancellation).await?;
                }
                SourceError::InvalidProtocolState
                | SourceError::FrameTooLarge { .. }
                | SourceError::Unauthorized
                | SourceError::Sink(_)
                | SourceError::Cancelled
                | SourceError::FrameIdentityExhausted
                | SourceError::SessionNotCurrent
                | SourceError::CaptureNotHealthy
                | SourceError::GenerationAuthorityMismatch
                | SourceError::TrustedTimeUnavailable
                | SourceError::TrustedTimeDiscontinuity => {
                    return Err(ProductionSupervisorError::TerminalSource(error));
                }
            }
        }
    }

    async fn wait_after_refusal(
        &self,
        cancellation: &CancellationToken,
    ) -> Result<(), ProductionSupervisorError> {
        let decision = self
            .provider_backoff()?
            .apply_refusal(BACKOFF_JITTER_SAMPLE_BASIS_POINTS)?;
        match decision {
            ProviderBackoffDecision::WaitUntil(deadline) => {
                self.wait_until(cancellation, deadline).await
            }
            ProviderBackoffDecision::Unavailable(reason) => {
                Err(ProductionSupervisorError::BudgetUnavailable(reason))
            }
        }
    }

    async fn wait_until(
        &self,
        cancellation: &CancellationToken,
        deadline: market_squawk_sources::MonotonicInstant,
    ) -> Result<(), ProductionSupervisorError> {
        let wait = self.provider_backoff()?.remaining_wait(deadline)?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(()),
            () = tokio::time::sleep(wait) => Ok(()),
        }
    }

    #[cfg(test)]
    pub(super) async fn run_one_generation_for_test(
        &mut self,
        cancellation: CancellationToken,
    ) -> Result<ProductionGenerationOutcome, ProductionSupervisorError> {
        self.run_one_generation(cancellation, &mut None, &mut None)
            .await
    }

    fn provider_backoff(&self) -> Result<&ProviderBackoffAuthority, ProductionSupervisorError> {
        match &self.refusal {
            SupervisorRefusalPolicy::Provider(backoff) => Ok(backoff),
            #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
            SupervisorRefusalPolicy::InstalledFixtureNoRetry => {
                Err(ProductionSupervisorError::InstalledFixtureRetryForbidden)
            }
        }
    }

    pub(super) fn shutdown(mut self) -> Result<(), ProductionSupervisorError> {
        let registry = self
            .registry
            .take()
            .ok_or(ProductionSupervisorError::AlreadyShutdown)?;
        registry.shutdown()?;
        Ok(())
    }
}

async fn run_display_source(
    source: &mut ProductionLiveSource,
    sink: &mut ProductionRawMarketSink<'_>,
    cancellation: CancellationToken,
    monitors: &mut [DisplayMarketSupervisorMonitor],
    shutdown_timeout: Duration,
) -> Result<Result<(), SourceError>, ProductionSupervisorError> {
    enum Outcome {
        Source(Result<(), SourceError>),
        Terminal(DisplayMarketTerminalFailure),
        Cancelled(Result<(), SourceError>),
        MonitorClosed(DisplayMarketMonitorError),
    }

    let outcome = {
        let source_run = source.run(sink, cancellation.clone());
        tokio::pin!(source_run);
        tokio::select! {
            biased;
            result = &mut source_run => Outcome::Source(result),
            monitor = wait_for_display_terminal(monitors, &cancellation) => {
                cancellation.cancel();
                let stopped = tokio::time::timeout(shutdown_timeout, &mut source_run)
                    .await
                    .map_err(|_elapsed| {
                        ProductionSupervisorError::DisplaySourceShutdownDeadline
                    })?;
                match monitor {
                    Ok(failure) => Outcome::Terminal(failure),
                    Err(DisplayMarketMonitorError::Cancelled) => Outcome::Cancelled(stopped),
                    Err(error) => Outcome::MonitorClosed(error),
                }
            }
        }
    };
    match outcome {
        Outcome::Source(result) | Outcome::Cancelled(result) => Ok(result),
        Outcome::Terminal(failure) => {
            sink.record_display_terminal_failure(failure);
            Ok(Ok(()))
        }
        Outcome::MonitorClosed(error) => {
            tracing::error!(%error, "display-market terminal monitor failed");
            Err(ProductionSupervisorError::DisplayMonitor)
        }
    }
}

async fn wait_for_display_terminal(
    monitors: &mut [DisplayMarketSupervisorMonitor],
    cancellation: &CancellationToken,
) -> Result<DisplayMarketTerminalFailure, DisplayMarketMonitorError> {
    let waits = FuturesUnordered::new();
    for monitor in monitors {
        waits.push(monitor.wait_until_terminal(cancellation));
    }
    let mut waits = waits;
    waits
        .next()
        .await
        .ok_or(DisplayMarketMonitorError::WorkerClosed)?
}

async fn unregister_display_generation(
    directory: &DisplayMarketDirectory,
    monitors: &[DisplayMarketSupervisorMonitor],
    shutdown_timeout: Duration,
) -> Option<ProductionSupervisorError> {
    let Some(deadline) = Instant::now().checked_add(shutdown_timeout) else {
        return Some(ProductionSupervisorError::DisplayDeadlineRange);
    };
    let cleanup_cancellation = CancellationToken::new();
    let mut first_error = None;
    for monitor in monitors.iter().rev() {
        let result = directory
            .unregister(monitor.key(), &cleanup_cancellation, deadline)
            .await;
        let error = match result {
            Ok(DisplayMarketActorShutdown::Graceful) => None,
            Ok(disposition) => {
                tracing::error!(?disposition, "display-market actor shutdown was incomplete");
                Some(ProductionSupervisorError::IncompleteDisplayShutdown)
            }
            Err(error) => {
                tracing::error!(%error, "display-market actor unregister failed");
                Some(ProductionSupervisorError::DisplayDirectory)
            }
        };
        if first_error.is_none() {
            first_error = error;
        }
    }
    first_error
}

pub(super) async fn route_worker_cleanup_error(
    worker: RouteActorWorker,
) -> Option<ProductionSupervisorError> {
    match worker.await {
        Ok(Ok(())) => None,
        Ok(Err(failure)) => Some(ProductionSupervisorError::Sink(
            ProductionSinkFailure::RouteActivation(failure),
        )),
        Err(error) => Some(ProductionSupervisorError::RouteWorker(error)),
    }
}

pub(super) fn activate_owned_capture<W>(
    control: &mut Option<RawCaptureControl<CaptureGenerationCapabilities>>,
    writer: &Option<W>,
) -> Result<(), ProductionSupervisorError> {
    if writer.is_none() {
        return Err(ProductionSupervisorError::MissingCaptureWriterOwnership);
    }
    control
        .as_mut()
        .ok_or(ProductionSupervisorError::MissingCaptureControlOwnership)?
        .activate_initial()?;
    Ok(())
}

/// Production source startup, generation, or bounded cleanup failure.
#[derive(Debug, Error)]
pub enum ProductionSupervisorError {
    #[error("production source supervisor is already shut down")]
    AlreadyShutdown,
    #[error("production source supervisor bounded allocation failed")]
    AllocationFailed,
    #[error("production source supervisor static policy is invalid")]
    InvalidStaticPolicy,
    #[error("production display mode is unavailable for this provider")]
    UnsupportedDisplayProvider,
    #[error("production display mode requires at least one mapped instrument")]
    MissingDisplayRoutes,
    #[error("production display mode contains a duplicate mapped instrument route")]
    DuplicateDisplayRoute,
    #[error("production display lifecycle deadline cannot be represented")]
    DisplayDeadlineRange,
    #[error("production display source did not stop within its bounded cancellation deadline")]
    DisplaySourceShutdownDeadline,
    #[error("display-market terminal monitor failed")]
    DisplayMonitor,
    #[error("display-market directory operation failed")]
    DisplayDirectory,
    #[error("display-market exact-generation actor did not stop cleanly")]
    IncompleteDisplayShutdown,
    #[error("capture activation began without cleanup-owned control")]
    MissingCaptureControlOwnership,
    #[error("capture activation began without cleanup-owned writer")]
    MissingCaptureWriterOwnership,
    #[error("capture writer did not complete bounded shutdown: {0:?}")]
    IncompleteCaptureShutdown(market_squawk_platform::ProcessCaptureShutdownOutcome),
    #[error("production source failed before subscription and first-data readiness: {0}")]
    SourceFailedBeforeReadiness(SourceError),
    #[error("production source completed before subscription and first-data readiness")]
    SourceCompletedBeforeReadiness,
    #[error("production source generation failed terminally: {0}")]
    TerminalSource(SourceError),
    #[error("production provider budget is unavailable: {0:?}")]
    BudgetUnavailable(BudgetUnavailableReason),
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    #[error("installed fixture must use its closed one-generation supervisor entry point")]
    InstalledFixtureEntryPointRequired,
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    #[error("installed fixture retry was refused by the local no-budget policy")]
    InstalledFixtureRetryForbidden,
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    #[error("installed fixture source failed under the local no-retry policy: {0}")]
    InstalledFixtureRefused(SourceError),
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    #[error("installed fixture source completed before cancellation or exclusive expiry")]
    InstalledFixtureCompleted,
    #[cfg(all(feature = "alpaca-installed-fixture", debug_assertions))]
    #[error("installed fixture startup observer was dropped")]
    InstalledFixtureObserverDropped,
    #[error(transparent)]
    ProviderBackoff(#[from] ProviderBackoffError),
    #[error(transparent)]
    AuthorityStore(#[from] LocalAuthorityStateStoreError),
    #[error(transparent)]
    Paths(#[from] market_squawk_platform::PathError),
    #[error(transparent)]
    ProviderRate(#[from] market_squawk_sources::ProviderRateStoreError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error("source registration failed and clean registry rollback also failed")]
    RegistryStartupCleanup {
        source: RegistryError,
        cleanup: RegistryError,
    },
    #[error("source supervisor run and clean registry shutdown both failed")]
    RunShutdown {
        source: Box<ProductionSupervisorError>,
        shutdown: Box<ProductionSupervisorError>,
    },
    #[error(transparent)]
    Profile(#[from] ProductionCoinbaseProfileError),
    #[error(transparent)]
    Provider(#[from] ProductionProviderError),
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error(transparent)]
    CaptureChannel(#[from] CaptureChannelError),
    #[error(transparent)]
    CaptureGeneration(#[from] CaptureGenerationError),
    #[error(transparent)]
    CaptureWriterPolicy(#[from] CaptureWriterPolicyError),
    #[error(transparent)]
    ProcessCaptureConfig(#[from] ProcessJournalCaptureConfigError),
    #[error(transparent)]
    ProcessCaptureShutdownPolicy(#[from] ProcessCaptureShutdownPolicyError),
    #[error(transparent)]
    ProcessCaptureSpawn(#[from] ProcessCaptureWriterSpawnError),
    #[error(transparent)]
    RouteBind(#[from] LiveIngressBindError),
    #[error(transparent)]
    RouteWorker(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Subscription(#[from] SubscriptionConstructionError),
    #[error(transparent)]
    SinkConstruction(#[from] ProductionSinkConstructionError),
    #[error("production sink failed closed: {0}")]
    Sink(ProductionSinkFailure),
}
