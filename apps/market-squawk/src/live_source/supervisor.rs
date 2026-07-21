//! Durable, supervisor-owned lifecycle for the sealed Coinbase production source.

use std::{num::NonZeroUsize, time::Instant};

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
    AuthoritativeSourceRegistry, BudgetDecision, BudgetUnavailableReason,
    CaptureGenerationCapabilities, RegisteredSource, RegistryError, SessionId, SourceError,
};
use thiserror::Error;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    composition::{ProductionCoinbaseProfileError, system_timestamp},
    provider::{ProductionProviderError, ProductionSourceProfile},
    route_actor::{RouteActorWorker, RouteBufferLimits, spawn_route_activation},
    sink::{
        ProductionRawMarketSink, ProductionRawMarketSinkInput, ProductionSinkConstructionError,
        ProductionSinkFailure,
    },
    subscription_state::{
        GenerationIdentity, SubscriptionConstructionError, SubscriptionLimits,
        SubscriptionStateMachine,
    },
};

const CAPTURE_FLUSH_RECORDS: usize = 256;
const BACKOFF_JITTER_SAMPLE_BASIS_POINTS: u16 = 1_000;

/// One completed exact-generation source run after all generation-owned resources were reaped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProductionGenerationOutcome {
    generation: ConnectionGeneration,
    source_error: Option<SourceError>,
}

impl ProductionGenerationOutcome {
    #[cfg(test)]
    pub(super) const fn generation(self) -> ConnectionGeneration {
        self.generation
    }

    pub(super) const fn source_error(self) -> Option<SourceError> {
        self.source_error
    }
}

/// Sole owner of durable source authority and exact-generation lifecycle transitions.
#[derive(Debug)]
pub(super) struct ProductionSourceSupervisor {
    config: AppConfig,
    profile: ProductionSourceProfile,
    registry: Option<AuthoritativeSourceRegistry>,
    registered: RegisteredSource,
    paths: LocalPaths,
    capture_process: CaptureProcessInfrastructure,
    live_ingress: LiveRuntimeIngress,
    routes: Vec<ShardKey>,
    route_buffer_limits: RouteBufferLimits,
}

impl ProductionSourceSupervisor {
    pub(super) fn try_new(
        config: &AppConfig,
        profile: ProductionSourceProfile,
        paths: LocalPaths,
        capture_process: CaptureProcessInfrastructure,
        live_ingress: LiveRuntimeIngress,
        routes: Vec<ShardKey>,
        route_buffer_limits: RouteBufferLimits,
    ) -> Result<Self, ProductionSupervisorError> {
        let registered_at = system_timestamp()?;
        let authority_store = LocalAuthorityStateStore::try_open(
            paths.root().join("authority").join(profile.source_key()),
        )?;
        let mut registry = AuthoritativeSourceRegistry::try_new_durable(authority_store)?;
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
        Ok(Self {
            config: config.clone(),
            profile,
            registry: Some(registry),
            registered,
            paths,
            capture_process,
            live_ingress,
            routes,
            route_buffer_limits,
        })
    }

    async fn run_one_generation(
        &mut self,
        cancellation: CancellationToken,
        startup: &mut Option<oneshot::Sender<()>>,
    ) -> Result<ProductionGenerationOutcome, ProductionSupervisorError> {
        let at = system_timestamp()?;
        let session_id = SessionId::new(SourceIdentifier::try_from(format!(
            "{}-{}",
            self.profile.source_key(),
            uuid::Uuid::new_v4()
        ))?);
        let mut route_workers = Vec::new();
        route_workers
            .try_reserve_exact(self.routes.len())
            .map_err(|_error| ProductionSupervisorError::AllocationFailed)?;
        let registry = self
            .registry
            .as_mut()
            .ok_or(ProductionSupervisorError::AlreadyShutdown)?;
        let session = registry.begin_next_session(&self.registered, session_id, at)?;
        let generation = session.generation();
        let route_cancellation = cancellation.child_token();
        let mut capture_control = None;
        let mut writer_handle = None;

        let source_result = async {
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
                self.config.capture_shutdown(),
            )?;
            let handle = spawn_process_journal_capture_writer(writer, process_config, policy)?;
            capture_control = Some(control);
            writer_handle = Some(handle);
            activate_owned_capture(&mut capture_control, &writer_handle)?;
            let source_generation = registry.take_live_source_generation(&session)?;

            let mut route_publishers = Vec::new();
            route_publishers
                .try_reserve_exact(self.routes.len())
                .map_err(|_error| ProductionSupervisorError::AllocationFailed)?;
            for route in &self.routes {
                let dormant = self.live_ingress.reserve_route(route.clone())?;
                let (publisher, worker) = spawn_route_activation(
                    dormant,
                    self.route_buffer_limits,
                    route_cancellation.clone(),
                );
                route_publishers.push(publisher);
                route_workers.push(worker);
            }

            let subscription = SubscriptionStateMachine::try_new(
                GenerationIdentity::from_session(&session),
                self.profile
                    .subscription_products()
                    .iter()
                    .map(String::as_str),
                self.profile.subscription_ack_timeout(),
                Instant::now(),
                SubscriptionLimits::try_new(
                    self.profile.control_message_capacity(),
                    self.profile.control_byte_capacity(),
                )?,
            )?;
            tracing::debug!(
                source = self.profile.source_key(),
                generation = session.generation().get(),
                subscription_state_peak_bytes = subscription.estimated_peak_bytes().get(),
                "prepared bounded production subscription state"
            );
            let mut source = self
                .profile
                .try_source(source_generation)
                .map_err(ProductionSupervisorError::TerminalSource)?;
            let decoder = self.profile.decoder()?;
            let mut sink = ProductionRawMarketSink::try_new(ProductionRawMarketSinkInput {
                capture: publisher,
                registry,
                session: &session,
                health_reporter,
                decoder,
                subscription,
                live_ingress: self.live_ingress.clone(),
                routes: route_publishers,
            })?;
            if let Some(sender) = startup.take() {
                sender
                    .send(())
                    .map_err(|_value| ProductionSupervisorError::StartupObserverDropped)?;
            }
            let result = source.run(&mut sink, cancellation).await;
            let terminal = sink.terminal_failure();
            drop(sink);
            match (result, terminal) {
                (Err(_error), Some(failure)) => Err(ProductionSupervisorError::Sink(failure)),
                (Err(error), None) => Ok(Some(error)),
                (Ok(()), Some(failure)) => Err(ProductionSupervisorError::Sink(failure)),
                (Ok(()), None) => Ok(None),
            }
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
                cleanup_error = Some(ProductionSupervisorError::IncompleteCaptureShutdown);
            }
        }
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        Ok(ProductionGenerationOutcome {
            generation,
            source_error: source_result?,
        })
    }

    pub(super) async fn run(
        mut self,
        cancellation: CancellationToken,
        startup: oneshot::Sender<()>,
    ) -> Result<(), ProductionSupervisorError> {
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
                .run_one_generation(cancellation.child_token(), startup)
                .await?;
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
                | SourceError::ProviderUnavailable => {
                    self.wait_after_refusal(cancellation).await?;
                }
                SourceError::FrameTooLarge { .. }
                | SourceError::InvalidProtocolState
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
        let budget = self
            .registered
            .budget()
            .ok_or(ProductionSupervisorError::MissingProviderBudget)?;
        let decision = budget.apply_refusal(BACKOFF_JITTER_SAMPLE_BASIS_POINTS);
        match decision {
            BudgetDecision::WaitUntil(deadline) => self.wait_until(cancellation, deadline).await,
            BudgetDecision::Unavailable(reason) => {
                Err(ProductionSupervisorError::BudgetUnavailable(reason))
            }
            BudgetDecision::Ready(permit) => {
                drop(permit);
                Err(ProductionSupervisorError::UnexpectedBudgetReady)
            }
        }
    }

    async fn wait_until(
        &self,
        cancellation: &CancellationToken,
        deadline: market_squawk_sources::MonotonicInstant,
    ) -> Result<(), ProductionSupervisorError> {
        let budget = self
            .registered
            .budget()
            .ok_or(ProductionSupervisorError::MissingProviderBudget)?;
        let wait = budget
            .remaining_wait(deadline)
            .map_err(ProductionSupervisorError::BudgetUnavailable)?;
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
        self.run_one_generation(cancellation, &mut None).await
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
    #[error("capture activation began without cleanup-owned control")]
    MissingCaptureControlOwnership,
    #[error("capture activation began without cleanup-owned writer")]
    MissingCaptureWriterOwnership,
    #[error("capture writer did not complete bounded shutdown")]
    IncompleteCaptureShutdown,
    #[error("production source startup observer was dropped")]
    StartupObserverDropped,
    #[error("production source has no registry-coordinated provider budget")]
    MissingProviderBudget,
    #[error("provider budget returned ready after a refusal or closed generation")]
    UnexpectedBudgetReady,
    #[error("production source generation failed terminally: {0}")]
    TerminalSource(SourceError),
    #[error("production provider budget is unavailable: {0:?}")]
    BudgetUnavailable(BudgetUnavailableReason),
    #[error(transparent)]
    AuthorityStore(#[from] LocalAuthorityStateStoreError),
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
