use std::{str::FromStr, time::Duration};

use market_squawk::{LiveRuntimeComposition, LiveRuntimeCompositionError};
use market_squawk_domain::{
    AssetClass, Currency, Denomination, InstrumentDefinition, InstrumentDefinitionInput,
    InstrumentId, LotSize, TickSize, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_live::{
    ActionAuthorityIssueLimit, ActionHookDisposition, CommittedActionContext, CurrentAuthorityGate,
    DepthLimit, LiveActionHook, LiveActionHookError, LiveRouteConfig, LiveRouteConfigInput,
    LiveRuntimeConfig, LiveRuntimeConfigError, LiveRuntimeConfigInput, LiveRuntimeHealthKind,
    LiveRuntimeStartError, RouteActionHook, ShardId, ShardKey, ShardLifecycleSnapshot,
    ShardRoutingVersion, ShardShutdownStatus, SnapshotLimits, SnapshotReadError,
};
use rust_decimal::Decimal;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const INSTRUMENT: &str = "018f0000-0000-7000-8000-000000000001";

#[derive(Debug)]
struct NoAction;

impl LiveActionHook for NoAction {
    fn on_committed(
        &mut self,
        _context: CommittedActionContext<'_>,
        _authority: &mut CurrentAuthorityGate<'_>,
    ) -> ActionHookDisposition {
        ActionHookDisposition::NoAction
    }

    fn retained_bytes(&self) -> Result<usize, LiveActionHookError> {
        Ok(std::mem::size_of::<Self>())
    }

    fn maximum_authority_issues(&self) -> ActionAuthorityIssueLimit {
        ActionAuthorityIssueLimit::MIN
    }
}

fn instrument_id() -> TestResult<InstrumentId> {
    Ok(InstrumentId::from_str(INSTRUMENT)?)
}

fn route_config() -> TestResult<LiveRouteConfig> {
    let venue = VenueId::try_from("coinbase")?;
    let instrument = instrument_id()?;
    let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument,
        definition_revision: market_squawk_domain::InstrumentDefinitionRevision::try_from(1_u64)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        quote_currency: Currency::try_from("USD")?,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 4))?,
        contract_multiplier: Decimal::ONE,
        venue_mappings: vec![VenueMapping::new(
            venue.clone(),
            VenueSymbol::try_from("BTC-USD")?,
        )],
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?;
    Ok(LiveRouteConfig::try_new(LiveRouteConfigInput {
        route: ShardKey::new(venue, instrument),
        definition,
        depth: DepthLimit::new(32)?,
        nonce_capacity: 32,
        nonce_reclaim_budget: 4,
        maximum_capability_lifetime: Duration::from_secs(1),
    })?)
}

fn runtime_config() -> TestResult<LiveRuntimeConfig> {
    Ok(LiveRuntimeConfig::try_new(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: 2,
        mailbox_count_per_shard: 16,
        mailbox_bytes_per_shard: 256 * 1024,
        maximum_message_bytes: 64 * 1024,
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
        snapshot_event_trigger: 32,
        snapshot_interval: Duration::from_secs(1),
        snapshot_limits: SnapshotLimits::try_new(4, 4, 4, 32, 256 * 1024)?,
        maximum_retained_snapshot_readers: 4,
        shutdown_deadline: Duration::from_secs(1),
        maximum_runtime_bytes: 64 * 1024 * 1024,
    })?)
}

#[tokio::test]
async fn startup_exposes_every_ready_shard_and_shutdown_joins_the_incarnation() -> TestResult {
    let config = runtime_config()?;
    let route = route_config()?;
    let expected_peak = config.estimated_peak_bytes(std::slice::from_ref(&route))?;
    let mut composition = LiveRuntimeComposition::start(config, vec![route]).await?;
    let incarnation = composition.incarnation();

    assert_eq!(composition.estimated_peak_bytes(), expected_peak);
    let lease = composition.snapshots().try_load_all()?;
    let snapshots = lease.snapshots().collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 2);
    assert!(snapshots.iter().all(|snapshot| {
        snapshot.runtime_incarnation() == incarnation
            && snapshot.routing_version() == ShardRoutingVersion::V1
            && snapshot.lifecycle() == ShardLifecycleSnapshot::Ready
            && snapshot.snapshot_revision().get() == 2
    }));
    assert_eq!(
        snapshots
            .iter()
            .map(|snapshot| snapshot.route_dimension().available())
            .sum::<u32>(),
        1
    );
    drop(lease);

    let mut notified = [
        composition
            .try_next_snapshot_notification()
            .ok_or("missing first startup notification")?,
        composition
            .try_next_snapshot_notification()
            .ok_or("missing second startup notification")?,
    ];
    notified.sort();
    assert_eq!(notified, [ShardId::new(0, 2)?, ShardId::new(1, 2)?]);
    assert!(composition.try_next_snapshot_notification().is_none());

    let mut ready = Vec::new();
    while let Some(event) = composition.try_next_health() {
        if event.kind() == LiveRuntimeHealthKind::ShardReady {
            ready.push(event.shard());
        }
    }
    ready.sort();
    assert_eq!(ready, [ShardId::new(0, 2)?, ShardId::new(1, 2)?]);

    let shutdown = composition.shutdown().await?;
    assert!(shutdown.is_complete());
    assert_eq!(shutdown.incarnation(), incarnation);
    assert!(!shutdown.deadline_elapsed());
    assert_eq!(shutdown.outcomes().len(), 2);
    assert!(
        shutdown
            .outcomes()
            .iter()
            .all(|outcome| outcome.status() == ShardShutdownStatus::Complete)
    );
    Ok(())
}

#[tokio::test]
async fn application_composition_can_install_one_owned_action_hook_per_route() -> TestResult {
    let route = route_config()?;
    let hook = RouteActionHook::try_new(route.route().clone(), Box::new(NoAction), Vec::new())?;

    let composition =
        LiveRuntimeComposition::start_with_action_hooks(runtime_config()?, vec![route], vec![hook])
            .await?;

    assert!(composition.shutdown().await?.is_complete());
    Ok(())
}

#[tokio::test]
async fn replacement_closes_old_snapshot_access_before_exposing_fresh_state() -> TestResult {
    let config = runtime_config()?;
    let routes = vec![route_config()?];
    let first = LiveRuntimeComposition::start(config.clone(), routes.clone()).await?;
    let first_incarnation = first.incarnation();
    let old_reader = first.snapshots();
    let old_lease = old_reader.try_load_all()?;

    let replacement = first.replace(config, routes).await?;
    assert_ne!(replacement.incarnation(), first_incarnation);
    assert_eq!(
        old_reader.try_load_all().err(),
        Some(SnapshotReadError::Closed)
    );
    assert!(
        old_lease
            .snapshots()
            .all(|snapshot| snapshot.runtime_incarnation() == first_incarnation)
    );

    let fresh_lease = replacement.snapshots().try_load_all()?;
    assert!(fresh_lease.snapshots().all(|snapshot| {
        snapshot.runtime_incarnation() == replacement.incarnation()
            && snapshot.lifecycle() == ShardLifecycleSnapshot::Ready
            && snapshot.snapshot_revision().get() == 2
    }));
    drop(fresh_lease);

    let shutdown = replacement.shutdown().await?;
    assert!(shutdown.is_complete());
    Ok(())
}

#[tokio::test]
async fn composition_preserves_typed_fail_closed_startup_errors() -> TestResult {
    let route = route_config()?;
    let result = LiveRuntimeComposition::start(runtime_config()?, vec![route.clone(), route]).await;

    assert!(matches!(
        result,
        Err(LiveRuntimeCompositionError::Start(
            LiveRuntimeStartError::Config(LiveRuntimeConfigError::DuplicateRoute)
        ))
    ));
    Ok(())
}
