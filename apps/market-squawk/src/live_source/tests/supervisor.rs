use std::{num::NonZeroUsize, time::Duration};

use market_squawk_domain::ConnectionGeneration;
use market_squawk_live::LiveRuntime;
use market_squawk_platform::{
    CaptureChannelLimits, CaptureProcessInfrastructureLimits, CaptureWriterPolicy, LocalPaths,
    MemoryCaptureSink, initialize_capture_process_infrastructure, raw_capture_channel,
    spawn_capture_writer,
};
use market_squawk_sources::{AuthoritativeSourceRegistry, SessionId, SourceError};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

use super::super::{
    composition::{ProductionCoinbaseProfile, SupervisorDropCancellation, system_timestamp},
    route_actor::RouteBufferLimits,
    supervisor::{ProductionSourceSupervisor, activate_owned_capture},
};
use super::budget_free_metadata;
use super::sink::{app_config, runtime_config};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn dropping_runtime_owner_cancels_and_reaps_a_blocked_provider_task() -> TestResult {
    let cancellation = CancellationToken::new();
    let provider_cancellation = cancellation.clone();
    let owner = SupervisorDropCancellation::new(cancellation.clone());
    let (admitted, admission_observed) = tokio::sync::oneshot::channel();
    let provider = tokio::spawn(async move {
        admitted
            .send(())
            .map_err(|_value| "admission observer dropped")?;
        provider_cancellation.cancelled().await;
        Result::<(), &'static str>::Ok(())
    });
    admission_observed.await?;

    drop(owner);

    tokio::time::timeout(Duration::from_secs(1), provider).await???;
    assert!(cancellation.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn clean_restart_resumes_exact_metadata_and_advances_registry_generation() -> TestResult {
    let root = TempDir::new()?;
    let first = run_cancelled_generation(root.path()).await?;
    let second = run_cancelled_generation(root.path()).await?;

    assert_eq!(first, ConnectionGeneration::new(1)?);
    assert_eq!(second, ConnectionGeneration::new(2)?);
    Ok(())
}

#[tokio::test]
async fn activation_failure_keeps_capture_control_and_writer_under_cleanup_ownership() -> TestResult
{
    let config = app_config()?;
    let source = config
        .coinbase()
        .ok_or("Coinbase production configuration missing")?;
    let profile = ProductionCoinbaseProfile::try_from(source)?;
    let now = system_timestamp()?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(budget_free_metadata(profile.metadata())?, now)?;
    let session = registry.begin_next_session(
        &registered,
        SessionId::new(market_squawk_domain::SourceIdentifier::try_from(
            "capture-activation-owner",
        )?),
        now,
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let process =
        initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
            config.capture_destination_registry_memory_ceiling_bytes(),
        ))?;
    let (_publisher, control, writer) = raw_capture_channel(
        &process,
        CaptureChannelLimits::new(
            config.capture_queue_capacity(),
            config.capture_memory_ceiling_bytes(),
        ),
        capabilities,
    )?;
    let writer = spawn_capture_writer(
        writer,
        MemoryCaptureSink::try_new(
            NonZeroUsize::new(64).ok_or("record bound must be nonzero")?,
            NonZeroUsize::new(16 * 1024 * 1024).ok_or("memory bound must be nonzero")?,
        )?,
        CaptureWriterPolicy::default(),
    )?;
    let mut owned_control = Some(control);
    let mut owned_writer = Some(writer);
    owned_control
        .as_mut()
        .ok_or("capture control missing")?
        .invalidate_current();

    assert!(activate_owned_capture(&mut owned_control, &owned_writer).is_err());
    assert!(owned_control.is_some());
    assert!(owned_writer.is_some());

    drop(owned_control.take());
    let writer = owned_writer.take().ok_or("capture writer missing")?;
    let mut pending = writer.shutdown(config.capture_shutdown());
    let _status = pending.wait_until_deadline().await;
    if !pending.is_worker_terminated() {
        pending.wait_until_terminated().await;
    }
    let _termination = pending.try_reap()?;
    registry.end_session(&session, system_timestamp()?)?;
    Ok(())
}

async fn run_cancelled_generation(root: &std::path::Path) -> TestResult<ConnectionGeneration> {
    let mut config = app_config()?;
    let mut overrides: market_squawk_platform::ConfigOverrides = config.clone().into();
    overrides.data_dir = Some(root.to_path_buf());
    config = market_squawk_platform::AppConfig::try_from(overrides)?;
    let source = config
        .coinbase()
        .ok_or("Coinbase production configuration missing")?;
    let profile = ProductionCoinbaseProfile::try_from(source)?;
    let runtime_config = runtime_config()?;
    let route_buffer_limits = RouteBufferLimits::new(
        runtime_config.mailbox_count_per_shard(),
        runtime_config.maximum_message_bytes(),
    );
    let routes = route_configs(source)?;
    let route_keys = routes.iter().map(|route| route.route().clone()).collect();
    let live = LiveRuntime::start(runtime_config, routes).await?;
    let ingress = live.ingress();
    let process =
        initialize_capture_process_infrastructure(CaptureProcessInfrastructureLimits::new(
            config.capture_destination_registry_memory_ceiling_bytes(),
        ))?;
    let paths = LocalPaths::prepare(config.data_dir())?;
    let mut supervisor = ProductionSourceSupervisor::try_new(
        &config,
        profile,
        paths,
        process,
        ingress,
        route_keys,
        route_buffer_limits,
    )?;
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let outcome = supervisor.run_one_generation_for_test(cancellation).await?;
    assert_eq!(outcome.source_error(), Some(SourceError::Cancelled));
    let generation = outcome.generation();
    supervisor.shutdown()?;
    assert!(live.shutdown().await.is_complete());
    Ok(generation)
}

fn route_configs(
    source: &market_squawk_platform::CoinbaseSourceConfig,
) -> TestResult<Vec<market_squawk_live::LiveRouteConfig>> {
    source
        .instruments()
        .iter()
        .map(|mapping| {
            Ok(market_squawk_live::LiveRouteConfig::try_new(
                market_squawk_live::LiveRouteConfigInput {
                    route: market_squawk_live::ShardKey::new(
                        market_squawk_domain::VenueId::try_from("coinbase-exchange")?,
                        mapping.definition().instrument_id(),
                    ),
                    definition: mapping.definition().clone(),
                    depth: market_squawk_live::DepthLimit::new(32)?,
                    nonce_capacity: 32,
                    nonce_reclaim_budget: 4,
                    maximum_capability_lifetime: std::time::Duration::from_secs(1),
                },
            )?)
        })
        .collect()
}
