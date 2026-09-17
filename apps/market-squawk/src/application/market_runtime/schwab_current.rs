//! Installed construction of one exact Schwab current-quote generation.

use std::{
    num::NonZeroUsize,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_live::ShardKey;
use market_squawk_platform::{
    AppConfig, CaptureChannelLimits, CaptureProcessInfrastructure, CaptureWriterPolicy,
    LocalAuthorityStateStore, LocalPaths, RollingMemoryCaptureSink, raw_capture_channel,
    spawn_capture_writer,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationSubjectResolver, ProviderRateAuthority, SessionId,
    SourceMetadata,
};
use tokio_util::sync::CancellationToken;

use crate::{
    application::MarketEventDurableRead,
    live_source::{
        SchwabRestQuoteCurrentSessionInput,
        display_market::{
            DisplayMarketActorLimits, DisplayMarketDirectory, DisplayMarketKey,
            DisplayMarketReadAdmission, DisplayMarketSupervisorMonitor,
        },
    },
    provider_activation::{PreparedSchwabMarketRuntimeStart, ProviderAccountRuntimeCurrentness},
};

use super::{display::DisplaySourceDescriptor, schwab_sink::SchwabRestQuoteCurrentRuntime};

/// Installation-wide key for the sole active Schwab account surface. The owning runtime registry
/// validates that no second Schwab surface exists before this authority can be opened.
pub(crate) const SCHWAB_CURRENT_LIVE_AUTHORITY_KEY: &str = "schwab-rest-quotes-current";
const CAPTURE_FLUSH_RECORDS: usize = 256;

pub(super) struct StartedSchwabCurrentRuntime {
    pub(super) runtime: SchwabRestQuoteCurrentRuntime,
    pub(super) currentness: ProviderAccountRuntimeCurrentness,
    pub(super) descriptor: Arc<DisplaySourceDescriptor>,
    pub(super) metadata: Arc<[SourceMetadata]>,
    pub(super) routes: Arc<[ShardKey]>,
    pub(super) durable_read: MarketEventDurableRead,
    pub(super) display_monitor: tokio::task::JoinHandle<()>,
}

#[allow(
    clippy::too_many_arguments,
    reason = "every prepared, registry, capture, display, and lifecycle authority remains explicit"
)]
pub(super) async fn start_schwab_current_runtime(
    prepared: PreparedSchwabMarketRuntimeStart,
    app_config: AppConfig,
    provider_rate: ProviderRateAuthority,
    capture_process: CaptureProcessInfrastructure,
    display_directory: DisplayMarketDirectory,
    actor_limits: DisplayMarketActorLimits,
    read_admission: DisplayMarketReadAdmission,
    lifecycle: CancellationToken,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<StartedSchwabCurrentRuntime, ServiceError> {
    ensure_before(deadline, cancellation, &lifecycle)?;
    let currentness = prepared.currentness();
    let metadata = Arc::<[SourceMetadata]>::from([prepared.metadata().clone()]);
    let durable_read = prepared.durable_read().clone();
    let session_identifier =
        SourceIdentifier::try_from(prepared.generation().session_id().to_string())
            .map_err(|_| ServiceError::InvalidResult)?;
    let venue = prepared.venue_id().clone();

    let mut display_bindings = Vec::new();
    display_bindings
        .try_reserve_exact(prepared.display_bindings().len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    display_bindings.extend(prepared.display_bindings().iter().cloned());
    let descriptor = DisplaySourceDescriptor::try_new(
        super::configuration::AccountMarketSurface::SchwabMarketData.surface_id(),
        prepared.metadata().clone(),
        display_bindings.into_boxed_slice(),
    )?;
    let mut routes = Vec::new();
    routes
        .try_reserve_exact(prepared.bindings().len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for (binding, _approval) in prepared.bindings() {
        routes.push(ShardKey::new(venue.clone(), binding.instrument_id()));
    }
    let routes: Arc<[ShardKey]> = routes.into();
    let terminal_read_admission = read_admission.clone();

    let (current, monitors) = prepare_current_session(
        &app_config,
        provider_rate,
        capture_process,
        display_directory,
        actor_limits,
        read_admission,
        prepared.metadata(),
        &session_identifier,
        &routes,
        lifecycle.clone(),
        deadline,
        cancellation,
    )
    .await?;
    let display_monitor =
        supervise_display_routes(monitors, terminal_read_admission, lifecycle.clone());
    let input = prepared.into_runtime_input(current, lifecycle.clone());
    let runtime = match SchwabRestQuoteCurrentRuntime::start(input, deadline).await {
        Ok(runtime) => runtime,
        Err(error) => {
            lifecycle.cancel();
            tracing::warn!(%error, "Schwab current-market runtime start failed");
            let _ = display_monitor.await;
            return Err(request_state_error(deadline, cancellation));
        }
    };
    Ok(StartedSchwabCurrentRuntime {
        runtime,
        currentness,
        descriptor,
        metadata,
        routes,
        durable_read,
        display_monitor,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "every current registry, capture, display, and lifecycle authority remains explicit"
)]
async fn prepare_current_session(
    app_config: &AppConfig,
    provider_rate: ProviderRateAuthority,
    capture_process: CaptureProcessInfrastructure,
    display_directory: DisplayMarketDirectory,
    actor_limits: DisplayMarketActorLimits,
    read_admission: DisplayMarketReadAdmission,
    metadata: &SourceMetadata,
    session_identifier: &SourceIdentifier,
    routes: &[ShardKey],
    lifecycle: CancellationToken,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<
    (
        SchwabRestQuoteCurrentSessionInput,
        Vec<DisplayMarketSupervisorMonitor>,
    ),
    ServiceError,
> {
    ensure_before(deadline, cancellation, &lifecycle)?;
    if routes.is_empty() {
        return Err(ServiceError::InvalidRequest);
    }
    let mut display_ingresses = Vec::new();
    display_ingresses
        .try_reserve_exact(routes.len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    let mut monitors = Vec::new();
    monitors
        .try_reserve_exact(routes.len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    let queue_capacity = app_config.capture_queue_capacity();
    let capture_sink = RollingMemoryCaptureSink::try_new(
        queue_capacity,
        app_config.capture_memory_ceiling_bytes(),
    )
    .map_err(|error| {
        tracing::error!(%error, "Schwab current capture sink construction failed");
        ServiceError::ResourceExhausted
    })?;
    let flush_records = NonZeroUsize::new(queue_capacity.get().min(CAPTURE_FLUSH_RECORDS))
        .ok_or(ServiceError::Internal)?;
    let capture_policy =
        CaptureWriterPolicy::try_new(flush_records, app_config.capture_flush_interval()).map_err(
            |error| {
                tracing::error!(%error, "Schwab current capture policy is invalid");
                ServiceError::Internal
            },
        )?;
    let registered_at = system_timestamp()?;
    let started_at = system_timestamp()?;

    let paths = LocalPaths::prepare(app_config.data_dir()).map_err(|error| {
        tracing::error!(%error, "Schwab current authority path is unavailable");
        ServiceError::Unavailable
    })?;
    let store = LocalAuthorityStateStore::try_open(
        paths
            .root()
            .join("authority")
            .join(SCHWAB_CURRENT_LIVE_AUTHORITY_KEY),
    )
    .map_err(|error| {
        tracing::error!(%error, "Schwab current authority store is unavailable");
        ServiceError::Unavailable
    })?;
    let resolver: Arc<dyn AuthorizationSubjectResolver> = Arc::new(provider_rate.clone());
    let mut registry =
        AuthoritativeSourceRegistry::try_new_durable_with_authorization_subject_resolver_and_provider_rate(
            store,
            resolver,
            provider_rate,
        )
        .map_err(|error| {
            tracing::error!(%error, "Schwab current source registry is unavailable");
            ServiceError::Unavailable
        })?;
    let registered = match registry.register_or_resume_exact(metadata.clone(), registered_at) {
        Ok(registered) => registered,
        Err(error) => {
            tracing::error!(%error, "Schwab current source registration failed");
            let _ = registry.shutdown();
            return Err(ServiceError::Unavailable);
        }
    };
    let session = match registry.begin_next_session(
        &registered,
        SessionId::new(session_identifier.clone()),
        started_at,
    ) {
        Ok(session) => session,
        Err(error) => {
            tracing::error!(%error, "Schwab current source session failed");
            let _ = registry.shutdown();
            return Err(ServiceError::Unavailable);
        }
    };
    drop(registered);

    let capabilities = match registry.take_capture_generation_capabilities(&session) {
        Ok(capabilities) => capabilities,
        Err(error) => {
            tracing::error!(%error, "Schwab current capture generation is unavailable");
            close_unstarted_registry(registry, session);
            return Err(ServiceError::Unavailable);
        }
    };
    let health_reporter = match registry.take_current_health_reporter(&session) {
        Ok(reporter) => reporter,
        Err(error) => {
            tracing::error!(%error, "Schwab current health authority is unavailable");
            close_unstarted_registry(registry, session);
            return Err(ServiceError::Unavailable);
        }
    };
    let raw_frames = match registry.take_raw_frame_factory(&session) {
        Ok(factory) => factory,
        Err(error) => {
            tracing::error!(%error, "Schwab current raw-frame authority is unavailable");
            close_unstarted_registry(registry, session);
            return Err(ServiceError::Unavailable);
        }
    };
    let (capture, capture_control, capture_writer) = match raw_capture_channel(
        &capture_process,
        CaptureChannelLimits::new(queue_capacity, app_config.capture_memory_ceiling_bytes()),
        capabilities,
    ) {
        Ok(channel) => channel,
        Err(error) => {
            tracing::error!(%error, "Schwab current capture channel construction failed");
            close_unstarted_registry(registry, session);
            return Err(ServiceError::ResourceExhausted);
        }
    };
    let capture_writer = match spawn_capture_writer(capture_writer, capture_sink, capture_policy) {
        Ok(writer) => writer,
        Err(error) => {
            tracing::error!(%error, "Schwab current capture writer start failed");
            close_unstarted_registry(registry, session);
            return Err(ServiceError::Unavailable);
        }
    };
    let mut current = SchwabRestQuoteCurrentSessionInput::new(
        registry,
        session,
        raw_frames,
        capture,
        capture_control,
        capture_writer,
        health_reporter,
        display_directory,
        display_ingresses,
        app_config.source_shutdown(),
        app_config.source_shutdown(),
        app_config.capture_shutdown(),
    );
    if let Err(error) = current.activate_capture_initial() {
        tracing::error!(?error, "Schwab current capture activation failed");
        let _ = current.shutdown().await;
        return Err(ServiceError::Unavailable);
    }
    for route in routes {
        let key = match DisplayMarketKey::try_new(
            metadata.source_id(),
            route.venue(),
            route.instrument(),
            current.connection_generation(),
        ) {
            Ok(key) => key,
            Err(error) => {
                tracing::error!(%error, "Schwab current display key is invalid");
                let _ = current.shutdown().await;
                return Err(ServiceError::InvalidResult);
            }
        };
        match current
            .register_display_route(
                key,
                actor_limits,
                read_admission.clone(),
                cancellation,
                deadline,
            )
            .await
        {
            Ok(monitor) => monitors.push(monitor),
            Err(error) => {
                tracing::error!(?error, "Schwab current display route registration failed");
                let _ = current.shutdown().await;
                return Err(request_state_error(deadline, cancellation));
            }
        }
    }
    Ok((current, monitors))
}

fn close_unstarted_registry(
    mut registry: AuthoritativeSourceRegistry,
    session: market_squawk_sources::CurrentSourceSession,
) {
    let _ = registry.end_session(&session, session.started_at());
    let _ = registry.shutdown();
}

fn supervise_display_routes(
    monitors: Vec<DisplayMarketSupervisorMonitor>,
    read_admission: DisplayMarketReadAdmission,
    lifecycle: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut workers = tokio::task::JoinSet::new();
        for mut monitor in monitors {
            let cancellation = lifecycle.clone();
            workers.spawn(async move { monitor.wait_until_terminal(&cancellation).await });
        }
        tokio::select! {
            biased;
            () = lifecycle.cancelled() => {
                read_admission.revoke();
            }
            result = workers.join_next() => {
                if result.is_some() {
                    read_admission.revoke();
                    lifecycle.cancel();
                }
            }
        }
        workers.abort_all();
        while workers.join_next().await.is_some() {}
    })
}

fn ensure_before(
    deadline: Instant,
    cancellation: &CancellationToken,
    lifecycle: &CancellationToken,
) -> Result<(), ServiceError> {
    if cancellation.is_cancelled() || lifecycle.is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn request_state_error(deadline: Instant, cancellation: &CancellationToken) -> ServiceError {
    if cancellation.is_cancelled() {
        ServiceError::Cancelled
    } else if Instant::now() >= deadline {
        ServiceError::DeadlineExceeded
    } else {
        ServiceError::Unavailable
    }
}

fn system_timestamp() -> Result<Timestamp, ServiceError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ServiceError::Unavailable)?;
    let nanos = u128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(u128::from(elapsed.subsec_nanos())))
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(ServiceError::Unavailable)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
