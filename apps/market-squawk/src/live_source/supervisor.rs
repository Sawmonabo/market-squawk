//! Durable, supervisor-owned lifecycle for the sealed Coinbase production source.

use std::{
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
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
    AuthoritativeSourceRegistry, AuthorizationSubjectResolver, BudgetUnavailableReason,
    CaptureGenerationCapabilities, ProviderBackoffAuthority, ProviderBackoffDecision,
    ProviderBackoffError, ProviderNativeIdentityRequest, ProviderRateAuthority, RegisteredSource,
    RegistryError, SessionId, SourceError,
};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    composition::{ProductionCoinbaseProfileError, system_timestamp},
    display_market::{
        DisplayMarketActorLimits, DisplayMarketActorShutdown, DisplayMarketDirectory,
        DisplayMarketIngress, DisplayMarketKey, DisplayMarketMonitorError,
        DisplayMarketRouteIdentity, DisplayMarketSupervisorMonitor, DisplayMarketTerminalFailure,
    },
    instruments::ProductionCatalogSelection,
    provider::{ProductionLiveSource, ProductionProviderError, ProductionSourceProfile},
    route_actor::{RouteActorWorker, RouteBufferLimits, spawn_route_activation},
    sink::{
        ProductionCapturedPublicationIngress, ProductionDisplayMarketSinkInput,
        ProductionRawMarketSink, ProductionRawMarketSinkInput, ProductionSinkConstructionError,
        ProductionSinkFailure,
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
const CATALOG_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);
const CATALOG_SELECTION_RETRY_DELAY: Duration = Duration::from_millis(5);

/// One completed exact-generation source run after all generation-owned resources were reaped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProductionGenerationOutcome {
    generation: ConnectionGeneration,
    source_error: Option<SourceError>,
    startup_required: bool,
    startup_ready: bool,
}

impl ProductionGenerationOutcome {
    pub(super) const fn source_error(self) -> Option<SourceError> {
        self.source_error
    }

    const fn failed_before_startup_readiness(self) -> bool {
        self.startup_required && !self.startup_ready
    }
}

/// Both results come from the original supervisor after its physical cleanup joins.
#[derive(Debug)]
pub(super) struct ProductionSupervisorRunOutcome {
    pub(super) run: Result<(), ProductionSupervisorError>,
    pub(super) cleanup: Result<(), ProductionSupervisorError>,
}

/// Sole owner of durable source authority and exact-generation lifecycle transitions.
#[derive(Debug)]
pub(super) struct ProductionSourceSupervisor {
    config: AppConfig,
    profile: ProductionSourceProfile,
    publication: ProductionCapturedPublicationIngress,
    graceful_shutdown: CancellationToken,
    registry: Option<AuthoritativeSourceRegistry>,
    catalog: Option<ProductionCatalogSelection>,
    startup_catalog_admission: Option<(Instant, CancellationToken)>,
    registered: RegisteredSource,
    backoff: ProviderBackoffAuthority,
    paths: LocalPaths,
    capture_process: CaptureProcessInfrastructure,
    output: ProductionSupervisorOutput,
    cleanup_failure: Option<ProductionSupervisorError>,
    completion: Option<Arc<super::composition::PublicSourceCompletion>>,
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
            profile,
            paths,
            capture_process,
            output,
            provider_rate,
            None,
        )
    }

    /// Installs the actual catalog reader and retains startup admission for async selection of
    /// every native route before a live session or capture/network work can begin.
    #[allow(
        clippy::too_many_arguments,
        reason = "catalog selection, live ingress, rate, capture, and route bounds are separate authorities"
    )]
    pub(super) fn try_new_with_provider_rate_and_catalog(
        config: &AppConfig,
        profile: ProductionSourceProfile,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        live_ingress: LiveRuntimeIngress,
        routes: Vec<ShardKey>,
        route_buffer_limits: RouteBufferLimits,
        provider_rate: ProviderRateAuthority,
        catalog: &ProductionCatalogSelection,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ProductionSupervisorError> {
        let output = ProductionSupervisorOutput::Live {
            ingress: live_ingress,
            routes,
            buffer_limits: route_buffer_limits,
        };
        Self::try_new_with_output(
            config,
            profile,
            paths,
            capture_process,
            output,
            provider_rate,
            Some((catalog, deadline, cancellation)),
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
        catalog: &ProductionCatalogSelection,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<
        Self,
        (
            ProductionSupervisorError,
            Result<(), ProductionSupervisorError>,
        ),
    > {
        if !profile.supports_display_output() {
            return Err((
                ProductionSupervisorError::UnsupportedDisplayProvider,
                Ok(()),
            ));
        }
        if routes.is_empty() {
            return Err((ProductionSupervisorError::MissingDisplayRoutes, Ok(())));
        }
        for (index, route) in routes.iter().enumerate() {
            if routes[index.saturating_add(1)..].contains(route) {
                return Err((ProductionSupervisorError::DuplicateDisplayRoute, Ok(())));
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
            profile,
            paths,
            capture_process,
            output,
            provider_rate,
            Some((catalog, deadline, cancellation)),
        )
        .map_err(|error| match error {
            ProductionSupervisorError::RegistryStartupCleanup { source, cleanup } => {
                (*source, Err(ProductionSupervisorError::Registry(cleanup)))
            }
            // Every remaining constructor branch precedes ownership or explicitly shut down its registry.
            cause => (cause, Ok(())),
        })
    }

    fn try_new_with_output(
        config: &AppConfig,
        profile: ProductionSourceProfile,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        output: ProductionSupervisorOutput,
        provider_rate: ProviderRateAuthority,
        catalog: Option<(&ProductionCatalogSelection, Instant, &CancellationToken)>,
    ) -> Result<Self, ProductionSupervisorError> {
        let registered_at = system_timestamp()?;
        let authority_path = paths.root().join("authority").join(profile.source_key());
        let authority_store = LocalAuthorityStateStore::try_open(authority_path)?;
        let authorization_subject_resolver: Arc<dyn AuthorizationSubjectResolver> =
            Arc::new(provider_rate.clone());
        let mut registry = AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            authority_store,
            authorization_subject_resolver,
            provider_rate,
        )?;
        if let Some((selection, _, _)) = catalog {
            registry = registry.with_provider_identity_authority(Arc::new(selection.reader()))?;
        }
        let registered =
            match registry.register_or_resume_exact(profile.metadata().clone(), registered_at) {
                Ok(registered) => registered,
                Err(source) => {
                    return match registry.shutdown() {
                        Ok(()) => Err(ProductionSupervisorError::from_registry_selection(source)),
                        Err(cleanup) => Err(ProductionSupervisorError::RegistryStartupCleanup {
                            source: Box::new(ProductionSupervisorError::from_registry_selection(
                                source,
                            )),
                            cleanup,
                        }),
                    };
                }
            };
        let backoff = match registry.provider_backoff_authority(&registered) {
            Ok(backoff) => backoff,
            Err(source) => {
                return match registry.shutdown() {
                    Ok(()) => Err(ProductionSupervisorError::ProviderBackoff(source)),
                    Err(cleanup) => Err(ProductionSupervisorError::RegistryStartupCleanup {
                        source: Box::new(ProductionSupervisorError::ProviderBackoff(source)),
                        cleanup,
                    }),
                };
            }
        };
        Ok(Self {
            config: config.clone(),
            profile,
            publication: ProductionCapturedPublicationIngress::none(),
            graceful_shutdown: CancellationToken::new(),
            registry: Some(registry),
            catalog: catalog.map(|(selection, _, _)| selection.clone()),
            startup_catalog_admission: catalog
                .map(|(_, deadline, cancellation)| (deadline, cancellation.clone())),
            registered,
            backoff,
            paths,
            capture_process,
            output,
            cleanup_failure: None,
            completion: None,
        })
    }

    pub(super) fn with_graceful_shutdown(mut self, graceful: CancellationToken) -> Self {
        self.graceful_shutdown = graceful;
        self
    }

    pub(super) fn with_completion(
        mut self,
        completion: Option<Arc<super::composition::PublicSourceCompletion>>,
    ) -> Self {
        self.completion = completion;
        self
    }

    pub(super) fn with_publication(
        mut self,
        publication: ProductionCapturedPublicationIngress,
    ) -> Self {
        self.publication = publication;
        self
    }

    async fn run_one_generation(
        &mut self,
        cancellation: CancellationToken,
        startup: &mut Option<oneshot::Sender<()>>,
    ) -> Result<ProductionGenerationOutcome, ProductionSupervisorError> {
        let shutdown_policy = ProcessCaptureShutdownPolicy::try_new(
            self.config.capture_shutdown(),
            self.config.capture_shutdown(),
        )?;
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
        if let Some(selection) = &self.catalog {
            let selection_deadline = Instant::now()
                .checked_add(CATALOG_SELECTION_TIMEOUT)
                .ok_or(ProductionSupervisorError::InvalidStaticPolicy)?;
            let startup_admission = self.startup_catalog_admission.take();
            let selection_deadline = startup_admission
                .as_ref()
                .map_or(selection_deadline, |(deadline, _)| {
                    selection_deadline.min(*deadline)
                });
            let startup_cancellation = startup_admission
                .as_ref()
                .map(|(_, cancellation)| cancellation);
            let mut requests = selection.requests().to_vec();
            let selection_started = Instant::now();
            let mut attempts = 0_usize;
            let result = retry_catalog_selection(
                selection_deadline,
                &cancellation,
                startup_cancellation,
                || {
                    attempts += 1;
                    select_catalog_routes(
                        registry,
                        &self.registered,
                        &mut requests,
                        selection_deadline,
                        &cancellation,
                    )
                },
            )
            .await;
            tracing::info!(
                source = self.profile.source_key(),
                routes = requests.len(),
                startup = startup_admission.is_some(),
                budget_ms = selection_deadline.saturating_duration_since(selection_started).as_millis(),
                elapsed_ms = selection_started.elapsed().as_millis(),
                attempts,
                error = ?result.as_ref().err(),
                "production catalog route selection completed"
            );
            result?;
        }
        let at = system_timestamp()?;
        let session = registry.begin_next_session(&self.registered, session_id, at)?;
        let generation = session.generation();
        let startup_required = startup.is_some();
        let route_cancellation = cancellation.child_token();
        // An aborted supervisor must still stop every route actor. Graceful network shutdown
        // only cancels its separate child below, leaving accepted route work alive until drained.
        let _route_drop_cancellation = route_cancellation.clone().drop_guard();
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
            let (mut source, decoder) = self.profile.try_generation(source_generation)?;
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
                    let mut sink = ProductionRawMarketSink::try_new_with_publication(
                        input,
                        self.publication.clone(),
                    )?;
                    if let Some(readiness) = startup.take() {
                        sink.install_startup_readiness(readiness)?;
                    }
                    sink
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
                        startup_readiness_policy: self.profile.startup_readiness_policy(),
                    };
                    let mut sink = ProductionRawMarketSink::try_new_display_with_publication(
                        input,
                        self.publication.clone(),
                    )?;
                    if let Some(readiness) = startup.take() {
                        sink.install_startup_readiness(readiness)?;
                    }
                    sink
                }
            };
            let network_cancellation = cancellation.child_token();
            let drain_deadline = Arc::new(OnceLock::new());
            sink.install_stream_shutdown(
                self.graceful_shutdown.clone(),
                cancellation.clone(),
                Arc::clone(&drain_deadline),
            );
            let result = if display_monitors.is_empty() {
                let source_run = source.run(&mut sink, network_cancellation.clone());
                tokio::pin!(source_run);
                tokio::select! {
                    biased;
                    result = &mut source_run => result,
                    () = self.graceful_shutdown.cancelled() => {
                        let deadline = Instant::now().checked_add(self.config.source_shutdown())
                            .ok_or(ProductionSupervisorError::InvalidStaticPolicy)?;
                        drain_deadline.set(deadline)
                            .map_err(|_| ProductionSupervisorError::InvalidStaticPolicy)?;
                        network_cancellation.cancel();
                        // The adapter's cancellation hook drains while its active budget and
                        // decoder owner still exist. Never drop this original future to stop it.
                        source_run.await
                    }
                }
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
                (
                    _,
                    Some(ProductionSinkFailure::Registry(
                        RegistryError::ProviderIdentitySelectionStale,
                    )),
                ) => Err(ProductionSupervisorError::CatalogSelectionStale),
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
        let mut cleanup_error = if matches!(
            source_result,
            Err(ProductionSupervisorError::DisplaySourceShutdownDeadline)
        ) {
            Some(ProductionSupervisorError::SourceCleanupIncomplete)
        } else {
            None
        };
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
            self.cleanup_failure = Some(error);
            return match source_result {
                Err(cause) => Err(cause),
                Ok((Some(source), ready)) if startup_required && !ready => Err(
                    ProductionSupervisorError::SourceFailedBeforeReadiness(source),
                ),
                Ok((Some(source), _)) => Err(ProductionSupervisorError::TerminalSource(source)),
                Ok((None, _)) => Err(ProductionSupervisorError::SourceCleanupIncomplete),
            };
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
        self,
        cancellation: CancellationToken,
        startup: oneshot::Sender<()>,
    ) -> Result<(), ProductionSupervisorError> {
        let completion = self.completion.clone();
        let outcome = self.run_with_cleanup(cancellation, startup).await;
        if let Some(completion) = completion {
            completion.finish();
        }
        match (outcome.run, outcome.cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(source), Err(shutdown)) => Err(ProductionSupervisorError::RunShutdown {
                source: Box::new(source),
                shutdown: Box::new(shutdown),
            }),
        }
    }

    pub(super) async fn run_with_cleanup(
        mut self,
        cancellation: CancellationToken,
        startup: oneshot::Sender<()>,
    ) -> ProductionSupervisorRunOutcome {
        let mut startup = Some(startup);
        let run = self.run_loop(&cancellation, &mut startup).await;
        let generation_cleanup = self.cleanup_failure.take();
        let shutdown = self.shutdown();
        let cleanup = match (generation_cleanup, shutdown) {
            (None, result) => result,
            (Some(error), Ok(())) => Err(error),
            (Some(source), Err(shutdown)) => Err(ProductionSupervisorError::RunShutdown {
                source: Box::new(source),
                shutdown: Box::new(shutdown),
            }),
        };
        if let Err(error) = &run {
            trace_supervisor_failure("run", error);
        }
        if let Err(error) = &cleanup {
            trace_supervisor_failure("cleanup", error);
        }
        ProductionSupervisorRunOutcome { run, cleanup }
    }

    async fn run_loop(
        &mut self,
        cancellation: &CancellationToken,
        startup: &mut Option<oneshot::Sender<()>>,
    ) -> Result<(), ProductionSupervisorError> {
        loop {
            if cancellation.is_cancelled() || self.graceful_shutdown.is_cancelled() {
                return Ok(());
            }
            let outcome = self
                .run_one_generation(cancellation.child_token(), startup)
                .await?;
            if outcome.failed_before_startup_readiness() {
                return match outcome.source_error() {
                    Some(SourceError::Cancelled)
                        if cancellation.is_cancelled() || self.graceful_shutdown.is_cancelled() =>
                    {
                        Ok(())
                    }
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
                SourceError::Cancelled
                    if cancellation.is_cancelled() || self.graceful_shutdown.is_cancelled() =>
                {
                    return Ok(());
                }
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
            .provider_backoff()
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
        let wait = self.provider_backoff().remaining_wait(deadline)?;
        tokio::select! {
            biased;
            () = cancellation.cancelled() => Ok(()),
            () = tokio::time::sleep(wait) => Ok(()),
        }
    }

    const fn provider_backoff(&self) -> &ProviderBackoffAuthority {
        &self.backoff
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

/// The installed source supervisors run on Tokio's multi-thread runtime. Keep their non-cloneable
/// registry custody borrowed in place while bounded catalog I/O yields the worker to peer tasks.
pub(super) fn select_catalog_routes(
    registry: &mut AuthoritativeSourceRegistry,
    registered: &RegisteredSource,
    requests: &mut [ProviderNativeIdentityRequest],
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), ProductionSupervisorError> {
    tokio::task::block_in_place(|| {
        let at = system_timestamp()?;
        for request in requests.iter_mut() {
            request.knowledge_at = at;
            request.effective_at = at;
        }
        registry
            .record_provider_identities(registered, requests, deadline, cancellation)
            .map_err(ProductionSupervisorError::from_registry_selection)
    })
}

/// Waits only for transient catalog contention; each attempt installs one complete route set.
/// No registry/catalog guard or partial mapping survives the asynchronous wait.
pub(super) async fn retry_catalog_selection(
    deadline: Instant,
    cancellation: &CancellationToken,
    startup_cancellation: Option<&CancellationToken>,
    mut select: impl FnMut() -> Result<(), ProductionSupervisorError>,
) -> Result<(), ProductionSupervisorError> {
    loop {
        if cancellation.is_cancelled()
            || startup_cancellation.is_some_and(CancellationToken::is_cancelled)
        {
            return Err(RegistryError::ProviderIdentitySelectionCancelled.into());
        }
        if Instant::now() >= deadline {
            return Err(RegistryError::ProviderIdentitySelectionDeadlineExceeded.into());
        }
        match select() {
            Err(ProductionSupervisorError::Registry(
                RegistryError::ProviderIdentityAuthorityBusy,
            )) => {}
            Ok(()) => {
                if cancellation.is_cancelled()
                    || startup_cancellation.is_some_and(CancellationToken::is_cancelled)
                {
                    return Err(RegistryError::ProviderIdentitySelectionCancelled.into());
                }
                if Instant::now() >= deadline {
                    return Err(RegistryError::ProviderIdentitySelectionDeadlineExceeded.into());
                }
                return Ok(());
            }
            Err(error) => return Err(error),
        }
        let retry_at = Instant::now()
            .checked_add(CATALOG_SELECTION_RETRY_DELAY)
            .unwrap_or(deadline)
            .min(deadline);
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Err(RegistryError::ProviderIdentitySelectionCancelled.into()),
            () = async {
                match startup_cancellation {
                    Some(cancellation) => cancellation.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            } => return Err(RegistryError::ProviderIdentitySelectionCancelled.into()),
            () = tokio::time::sleep_until(retry_at.into()) => {}
        }
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
                let stopped = match tokio::time::timeout(shutdown_timeout, &mut source_run).await {
                    Ok(stopped) => stopped,
                    Err(_elapsed) => {
                        // Retain and finish the original source future; the expired cleanup
                        // deadline remains an incomplete outcome even after it eventually exits.
                        let _stopped = source_run.await;
                        return Err(ProductionSupervisorError::DisplaySourceShutdownDeadline);
                    }
                };
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
    #[error("source generation cleanup did not complete successfully")]
    SourceCleanupIncomplete,
    #[error("production source supervisor bounded allocation failed")]
    AllocationFailed,
    #[error("production source supervisor static policy is invalid")]
    InvalidStaticPolicy,
    #[error("production display mode is unavailable for this provider")]
    UnsupportedDisplayProvider,
    #[error("production display mode requires at least one mapped instrument")]
    MissingDisplayRoutes,
    #[error("accepted catalog native routes are unavailable for this source")]
    MissingCatalogSelection,
    #[error("the accepted native catalog selection changed during this source generation")]
    CatalogSelectionStale,
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
        source: Box<ProductionSupervisorError>,
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

impl ProductionSupervisorError {
    fn from_registry_selection(error: RegistryError) -> Self {
        match error {
            RegistryError::ProviderIdentitySelectionStale => Self::CatalogSelectionStale,
            other => Self::Registry(other),
        }
    }
}

fn trace_supervisor_failure(stage: &'static str, error: &ProductionSupervisorError) {
    let category = match error {
        ProductionSupervisorError::AlreadyShutdown => "AlreadyShutdown",
        ProductionSupervisorError::SourceCleanupIncomplete => "SourceCleanupIncomplete",
        ProductionSupervisorError::AllocationFailed => "AllocationFailed",
        ProductionSupervisorError::InvalidStaticPolicy => "InvalidStaticPolicy",
        ProductionSupervisorError::UnsupportedDisplayProvider => "UnsupportedDisplayProvider",
        ProductionSupervisorError::MissingDisplayRoutes => "MissingDisplayRoutes",
        ProductionSupervisorError::MissingCatalogSelection => "MissingCatalogSelection",
        ProductionSupervisorError::CatalogSelectionStale => "CatalogSelectionStale",
        ProductionSupervisorError::DuplicateDisplayRoute => "DuplicateDisplayRoute",
        ProductionSupervisorError::DisplayDeadlineRange => "DisplayDeadlineRange",
        ProductionSupervisorError::DisplaySourceShutdownDeadline => "DisplaySourceShutdownDeadline",
        ProductionSupervisorError::DisplayMonitor => "DisplayMonitor",
        ProductionSupervisorError::DisplayDirectory => "DisplayDirectory",
        ProductionSupervisorError::IncompleteDisplayShutdown => "IncompleteDisplayShutdown",
        ProductionSupervisorError::MissingCaptureControlOwnership => {
            "MissingCaptureControlOwnership"
        }
        ProductionSupervisorError::MissingCaptureWriterOwnership => "MissingCaptureWriterOwnership",
        ProductionSupervisorError::IncompleteCaptureShutdown(..) => "IncompleteCaptureShutdown",
        ProductionSupervisorError::SourceFailedBeforeReadiness(..) => "SourceFailedBeforeReadiness",
        ProductionSupervisorError::SourceCompletedBeforeReadiness => {
            "SourceCompletedBeforeReadiness"
        }
        ProductionSupervisorError::TerminalSource(..) => "TerminalSource",
        ProductionSupervisorError::BudgetUnavailable(..) => "BudgetUnavailable",
        ProductionSupervisorError::ProviderBackoff(..) => "ProviderBackoff",
        ProductionSupervisorError::AuthorityStore(..) => "AuthorityStore",
        ProductionSupervisorError::Paths(..) => "Paths",
        ProductionSupervisorError::ProviderRate(..) => "ProviderRate",
        ProductionSupervisorError::Registry(..) => "Registry",
        ProductionSupervisorError::RegistryStartupCleanup { .. } => "RegistryStartupCleanup",
        ProductionSupervisorError::RunShutdown { .. } => "RunShutdown",
        ProductionSupervisorError::Profile(..) => "Profile",
        ProductionSupervisorError::Provider(..) => "Provider",
        ProductionSupervisorError::Identity(..) => "Identity",
        ProductionSupervisorError::CaptureChannel(..) => "CaptureChannel",
        ProductionSupervisorError::CaptureGeneration(..) => "CaptureGeneration",
        ProductionSupervisorError::CaptureWriterPolicy(..) => "CaptureWriterPolicy",
        ProductionSupervisorError::ProcessCaptureConfig(..) => "ProcessCaptureConfig",
        ProductionSupervisorError::ProcessCaptureShutdownPolicy(..) => {
            "ProcessCaptureShutdownPolicy"
        }
        ProductionSupervisorError::ProcessCaptureSpawn(..) => "ProcessCaptureSpawn",
        ProductionSupervisorError::RouteBind(..) => "RouteBind",
        ProductionSupervisorError::RouteWorker(..) => "RouteWorker",
        ProductionSupervisorError::Subscription(..) => "Subscription",
        ProductionSupervisorError::SinkConstruction(..) => "SinkConstruction",
        ProductionSupervisorError::Sink(..) => "Sink",
    };
    tracing::warn!(
        stage,
        category,
        "production source supervisor boundary failed"
    );
    match error {
        ProductionSupervisorError::Registry(error) => {
            tracing::warn!(stage, error = ?error, "production source registry boundary failed");
        }
        ProductionSupervisorError::IncompleteCaptureShutdown(outcome) => {
            tracing::warn!(stage, disposition = ?outcome.disposition(), helper_reaped = outcome.helper_reaped(),
                worker_present = outcome.worker_termination().is_some(),
                worker_deadline_elapsed = outcome.worker_termination().is_some_and(|worker| worker.shutdown_deadline_elapsed()),
                worker_incomplete = outcome.worker_termination().is_some_and(|worker| worker.outcome().is_incomplete()),
                "production capture shutdown incomplete");
        }
        ProductionSupervisorError::RunShutdown { source, shutdown } => {
            trace_supervisor_failure("nested_run", source);
            trace_supervisor_failure("nested_shutdown", shutdown);
        }
        ProductionSupervisorError::RegistryStartupCleanup { source, cleanup } => {
            trace_supervisor_failure("registry_startup", source);
            tracing::warn!(error = ?cleanup, "production registry startup cleanup failed");
        }
        _ => {}
    }
}
