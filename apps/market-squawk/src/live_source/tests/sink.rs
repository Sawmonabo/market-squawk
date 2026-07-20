use std::{
    collections::BTreeMap,
    ffi::OsString,
    num::{NonZeroU32, NonZeroUsize},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use market_squawk_domain::{DataQuality, SourceIdentifier, Timestamp, VenueId};
use market_squawk_live::{
    DepthLimit, LiveIngressError, LiveRouteConfig, LiveRouteConfigInput, LiveRuntime,
    LiveRuntimeConfig, LiveRuntimeConfigInput, LiveSnapshotReader, ShardKey, ShardRoutingVersion,
    SnapshotLimits,
};
use market_squawk_platform::{
    AppConfig, CaptureChannelLimits, CaptureProcessInfrastructureLimits, CaptureWriterPolicy,
    ConfigOverrides, ConfigSources, MemoryCaptureSink, initialize_capture_process_infrastructure,
    raw_capture_channel, spawn_capture_writer,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, RawMarketSink, RegistryError, SessionId, SinkError,
    TransportFrameKind,
};
use tokio_util::sync::CancellationToken;

use super::super::{
    composition::ProductionCoinbaseProfile,
    route_actor::{RouteBufferLimits, spawn_route_activation},
    sink::{
        ProductionRawMarketSink, ProductionRawMarketSinkInput, ProductionSinkFailure,
        RouteActivationFailure,
    },
    subscription_state::{
        GenerationIdentity, SubscriptionFailure, SubscriptionLimits, SubscriptionStateMachine,
    },
    supervisor::{ProductionSupervisorError, route_worker_cleanup_error},
};
use super::budget_free_metadata;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn capture_receipt_precedes_fail_closed_pre_acknowledgement_data() -> TestResult {
    let app_config = app_config()?;
    let source_config = app_config
        .coinbase()
        .ok_or("Coinbase configuration missing")?;
    let now = system_timestamp()?;
    let profile = ProductionCoinbaseProfile::try_from_at(source_config, now)?;
    let definition = source_config
        .instruments()
        .first()
        .ok_or("Coinbase instrument mapping missing")?
        .definition()
        .clone();
    let route = ShardKey::new(
        VenueId::try_from("coinbase-exchange")?,
        definition.instrument_id(),
    );
    let runtime = LiveRuntime::start(
        runtime_config()?,
        vec![LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: route.clone(),
            definition,
            depth: DepthLimit::new(32)?,
            nonce_capacity: 32,
            nonce_reclaim_budget: 4,
            maximum_capability_lifetime: Duration::from_secs(1),
        })?],
    )
    .await?;
    let live_ingress = runtime.ingress();
    let dormant = live_ingress.reserve_route(route)?;
    let cancellation = CancellationToken::new();
    let (route_activation, route_worker) =
        spawn_route_activation(dormant, route_buffer_limits()?, cancellation.clone());

    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(budget_free_metadata(profile.metadata())?, now)?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("coinbase-sink-session")?),
        market_squawk_domain::ConnectionGeneration::new(1)?,
        now,
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let mut frame_factory = registry.take_raw_frame_factory(&session)?;
    let health_reporter = registry.take_current_health_reporter(&session)?;
    let process = initialize_capture_process_infrastructure(
        CaptureProcessInfrastructureLimits::new(nonzero(1024 * 1024)?),
    )?;
    let (publisher, mut capture_control, writer) = raw_capture_channel(
        &process,
        CaptureChannelLimits::new(nonzero(8)?, nonzero(64 * 1024 * 1024)?),
        capabilities,
    )?;
    let writer_handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::try_new(nonzero(64)?, nonzero(16 * 1024 * 1024)?)?,
        CaptureWriterPolicy::default(),
    )?;
    capture_control.activate_initial()?;
    let controls = source_config.control_limits();
    let subscription = SubscriptionStateMachine::try_new(
        GenerationIdentity::from_session(&session),
        source_config
            .instruments()
            .iter()
            .map(market_squawk_platform::CoinbaseInstrumentMapping::product),
        source_config.subscription_ack_timeout(),
        Instant::now(),
        SubscriptionLimits::try_new(
            controls.message_capacity().get(),
            controls.byte_capacity().get(),
        )?,
    )?;
    assert!(matches!(
        registry.validate_current_authority(&session),
        Err(RegistryError::HealthNotQualified)
    ));
    let mut sink = ProductionRawMarketSink::try_new(ProductionRawMarketSinkInput {
        capture: publisher,
        registry: &mut registry,
        session: &session,
        health_reporter,
        decoder: profile.decoder().clone(),
        subscription,
        live_ingress,
        routes: vec![route_activation],
    })?;

    let snapshot = frame_factory.try_frame(
        TransportFrameKind::Text,
        Bytes::from_static(include_bytes!(
            "../../../../../adapters/market-squawk-adapter-coinbase/fixtures/snapshot.json"
        )),
    )?;
    assert_eq!(
        sink.try_publish(snapshot),
        Err(SinkError::CaptureIncomplete)
    );
    assert_eq!(
        sink.terminal_failure(),
        Some(ProductionSinkFailure::Subscription(
            SubscriptionFailure::DataBeforeAcknowledgement
        ))
    );
    drop(sink);

    cancellation.cancel();
    route_worker.await??;
    registry.end_session(&session, system_timestamp()?)?;
    drop(capture_control);
    let mut capture_shutdown = writer_handle.shutdown(Duration::from_secs(1));
    let _status = capture_shutdown.wait_until_deadline().await;
    let _termination = capture_shutdown.try_reap()?;
    let runtime_shutdown = runtime.shutdown().await;
    assert!(runtime_shutdown.is_complete());
    Ok(())
}

#[tokio::test]
async fn acknowledged_frames_reach_the_immutable_live_book_without_execution_quality() -> TestResult
{
    let app_config = app_config()?;
    let source_config = app_config
        .coinbase()
        .ok_or("Coinbase configuration missing")?;
    let now = system_timestamp()?;
    let profile = ProductionCoinbaseProfile::try_from_at(source_config, now)?;
    assert_eq!(
        profile.metadata().quality_ceiling(),
        DataQuality::DirectUnverified
    );
    let definition = source_config
        .instruments()
        .first()
        .ok_or("Coinbase instrument mapping missing")?
        .definition()
        .clone();
    let route = ShardKey::new(
        VenueId::try_from("coinbase-exchange")?,
        definition.instrument_id(),
    );
    let runtime = LiveRuntime::start(
        runtime_config()?,
        vec![LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: route.clone(),
            definition,
            depth: DepthLimit::new(32)?,
            nonce_capacity: 32,
            nonce_reclaim_budget: 4,
            maximum_capability_lifetime: Duration::from_secs(1),
        })?],
    )
    .await?;
    let snapshots = runtime.snapshots();
    let live_ingress = runtime.ingress();
    let dormant = live_ingress.reserve_route(route.clone())?;
    let cancellation = CancellationToken::new();
    let (route_activation, route_worker) =
        spawn_route_activation(dormant, route_buffer_limits()?, cancellation.clone());

    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let registered = registry.register(budget_free_metadata(profile.metadata())?, now)?;
    let session = registry.begin_session(
        &registered,
        SessionId::new(SourceIdentifier::try_from("coinbase-happy-session")?),
        market_squawk_domain::ConnectionGeneration::new(1)?,
        now,
    )?;
    let capabilities = registry.take_capture_generation_capabilities(&session)?;
    let mut frame_factory = registry.take_raw_frame_factory(&session)?;
    let health_reporter = registry.take_current_health_reporter(&session)?;
    let process = initialize_capture_process_infrastructure(
        CaptureProcessInfrastructureLimits::new(nonzero(1024 * 1024)?),
    )?;
    let (publisher, mut capture_control, writer) = raw_capture_channel(
        &process,
        CaptureChannelLimits::new(nonzero(8)?, nonzero(64 * 1024 * 1024)?),
        capabilities,
    )?;
    let writer_handle = spawn_capture_writer(
        writer,
        MemoryCaptureSink::try_new(nonzero(64)?, nonzero(16 * 1024 * 1024)?)?,
        CaptureWriterPolicy::default(),
    )?;
    capture_control.activate_initial()?;
    let controls = source_config.control_limits();
    let subscription = SubscriptionStateMachine::try_new(
        GenerationIdentity::from_session(&session),
        source_config
            .instruments()
            .iter()
            .map(market_squawk_platform::CoinbaseInstrumentMapping::product),
        source_config.subscription_ack_timeout(),
        Instant::now(),
        SubscriptionLimits::try_new(
            controls.message_capacity().get(),
            controls.byte_capacity().get(),
        )?,
    )?;
    assert!(matches!(
        registry.validate_current_authority(&session),
        Err(RegistryError::HealthNotQualified)
    ));
    let mut sink = ProductionRawMarketSink::try_new(ProductionRawMarketSinkInput {
        capture: publisher,
        registry: &mut registry,
        session: &session,
        health_reporter,
        decoder: profile.decoder().clone(),
        subscription,
        live_ingress,
        routes: vec![route_activation],
    })?;

    publish_fixture(&mut sink, &mut frame_factory, "subscriptions.json")?;
    publish_fixture(&mut sink, &mut frame_factory, "snapshot.json")?;
    publish_fixture(&mut sink, &mut frame_factory, "l2update.json")?;
    publish_fixture(&mut sink, &mut frame_factory, "match.json")?;
    let updated = wait_for_book(&snapshots, &route, 10010, 0).await?;
    publish_fixture(&mut sink, &mut frame_factory, "heartbeat.json")?;
    assert_eq!(current_book_revision(&snapshots, &route)?, updated);
    assert_eq!(sink.terminal_failure(), None);

    assert!(runtime.shutdown().await.is_complete());
    publish_fixture(&mut sink, &mut frame_factory, "match.json")?;
    let route_cleanup = route_worker_cleanup_error(route_worker)
        .await
        .ok_or("last queued frame must preserve its actor failure through cleanup")?;
    let ProductionSupervisorError::Sink(ProductionSinkFailure::RouteActivation(route_failure)) =
        route_cleanup
    else {
        return Err("supervisor cleanup lost the exact route actor failure".into());
    };
    assert_eq!(
        route_failure,
        RouteActivationFailure::Ingress(LiveIngressError::RuntimeClosed)
    );
    let heartbeat = fixture_frame(&mut frame_factory, "heartbeat.json")?;
    assert_eq!(
        sink.try_publish(heartbeat),
        Err(SinkError::CaptureIncomplete)
    );
    assert_eq!(
        sink.terminal_failure(),
        Some(ProductionSinkFailure::RouteActivation(route_failure))
    );
    cancellation.cancel();
    drop(sink);

    registry.end_session(&session, system_timestamp()?)?;
    capture_control.invalidate_current();
    drop(capture_control);
    let mut capture_shutdown = writer_handle.shutdown(Duration::from_secs(1));
    let _status = capture_shutdown.wait_until_deadline().await;
    let termination = capture_shutdown
        .try_reap()?
        .ok_or("capture writer termination missing")?;
    assert!(!termination.outcome().is_incomplete());
    assert_eq!(termination.outcome().records_written(), 7);
    Ok(())
}

fn publish_fixture(
    sink: &mut ProductionRawMarketSink<'_>,
    frame_factory: &mut market_squawk_sources::RawFrameFactory,
    fixture: &str,
) -> TestResult {
    let frame = fixture_frame(frame_factory, fixture)?;
    sink.try_publish(frame)?;
    Ok(())
}

fn fixture_frame(
    frame_factory: &mut market_squawk_sources::RawFrameFactory,
    fixture: &str,
) -> TestResult<market_squawk_sources::RawMarketFrame> {
    let payload = std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../adapters/market-squawk-adapter-coinbase/fixtures")
            .join(fixture),
    )?;
    Ok(frame_factory.try_frame(TransportFrameKind::Text, Bytes::from(payload))?)
}

async fn wait_for_book(
    snapshots: &LiveSnapshotReader,
    route: &ShardKey,
    expected_bid: i64,
    expected_ask: i64,
) -> TestResult<u64> {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some((revision, bid, ask)) = current_book(snapshots, route)?
                && bid == expected_bid
                && ask == expected_ask
            {
                return TestResult::Ok(revision);
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

fn current_book_revision(snapshots: &LiveSnapshotReader, route: &ShardKey) -> TestResult<u64> {
    current_book(snapshots, route)?
        .map(|(revision, _bid, _ask)| revision)
        .ok_or_else(|| "live book snapshot missing".into())
}

fn current_book(
    snapshots: &LiveSnapshotReader,
    route: &ShardKey,
) -> TestResult<Option<(u64, i64, i64)>> {
    let lease = snapshots.try_load_all()?;
    Ok(lease.snapshots().find_map(|shard| {
        shard.routes().iter().find_map(|candidate| {
            if candidate.route() != route {
                return None;
            }
            candidate.streams().first().map(|stream| {
                (
                    shard.snapshot_revision().get(),
                    stream.bids().first().map_or(0, |level| level.price().get()),
                    stream.asks().first().map_or(0, |level| level.price().get()),
                )
            })
        })
    }))
}

pub(super) fn app_config() -> TestResult<AppConfig> {
    let json = r#"{
      "endpoint":"wss://ws-feed.exchange.coinbase.com",
      "event_classes":["book_snapshot","book_delta","trade"],
      "depth":"price_level",
      "freshness_ms":5000,
      "max_frame_bytes":1048576,
      "subscription_ack_timeout_ms":5000,
      "control_message_capacity":64,
      "control_byte_capacity":65536,
      "authorization":{
        "mode":"public_interface",
        "provider":"coinbase-exchange",
        "basis":"user-reviewed-coinbase-public-interface",
        "evidence_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "evidence_reference":"https://docs.cdp.coinbase.com/exchange/websocket-feed/overview",
        "evidence_version":"reviewed-2026-07-20",
        "effective_from_unix_nanos":1700000000000000000,
        "effective_until_unix_nanos":1900000000000000000
      },
      "instruments":[{
        "product":"BTC-USD",
        "instrument_id":"4c74ab95-53b9-42ad-9b66-0ed403b88fed",
        "definition_revision":1,
        "asset_class":"crypto",
        "primary_asset":"b9f6d14f-9140-4ca3-a412-9bd59b3b5e67",
        "quote_currency":"USD",
        "tick_size":"0.01",
        "lot_size":"0.00000001",
        "contract_multiplier":"1",
        "venue":"coinbase-exchange",
        "trading_status":"active"
      }]
    }"#;
    let environment = BTreeMap::from([(
        OsString::from("MARKET_SQUAWK_COINBASE_JSON"),
        OsString::from(json),
    )]);
    Ok(AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides::default(),
    ))?)
}

pub(super) fn runtime_config() -> TestResult<LiveRuntimeConfig> {
    Ok(LiveRuntimeConfig::try_new(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: 1,
        mailbox_count_per_shard: 16,
        mailbox_bytes_per_shard: 1024 * 1024,
        maximum_message_bytes: 1024 * 1024,
        maximum_routes_per_shard: 4,
        maximum_sources_per_route: 4,
        maximum_streams_per_route: 4,
        maximum_feature_window_observations_per_route: 8,
        maximum_feature_window_bytes_per_route: 1024 * 1024,
        maximum_feature_sets_per_route: 4,
        cross_venue_command_count: 8,
        cross_venue_command_bytes: 64 * 1024,
        maximum_cross_venue_instruments: 8,
        maximum_venues_per_cross_venue_instrument: 2,
        maximum_feature_snapshot_bytes: 64 * 1024,
        maximum_action_hook_bytes_per_route: 64 * 1024,
        registration_control_capacity: 8,
        registration_deadline: Duration::from_secs(1),
        health_event_capacity: 16,
        snapshot_event_trigger: 1,
        snapshot_interval: Duration::from_secs(1),
        snapshot_limits: SnapshotLimits::try_new(4, 4, 4, 32, 256 * 1024)?,
        maximum_retained_snapshot_readers: 4,
        shutdown_deadline: Duration::from_secs(1),
        maximum_runtime_bytes: 64 * 1024 * 1024,
    })?)
}

fn nonzero(value: usize) -> TestResult<NonZeroUsize> {
    NonZeroUsize::new(value).ok_or_else(|| "test bound must be nonzero".into())
}

fn route_buffer_limits() -> TestResult<RouteBufferLimits> {
    Ok(RouteBufferLimits::new(
        nonzero(16)?,
        NonZeroU32::new(1024 * 1024).ok_or("test byte bound must be nonzero")?,
    ))
}

fn system_timestamp() -> TestResult<Timestamp> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let nanos = i64::try_from(elapsed.as_nanos())?;
    Ok(Timestamp::from_unix_nanos(nanos))
}
