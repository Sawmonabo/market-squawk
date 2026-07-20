use std::collections::HashMap;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use market_squawk_domain::{
    AssetClass, Currency, Denomination, InstrumentDefinition, InstrumentDefinitionInput,
    InstrumentId, LotSize, TickSize, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use rust_decimal::Decimal;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::super::{LiveRuntime, initial_snapshots};
use crate::authority::{RuntimeLease, RuntimeLeaseOwner};
use crate::runtime::actor::ActorCompletion;
use crate::runtime::admission::LiveRuntimeIngress;
use crate::snapshot::create_snapshot_plane;
use crate::{
    DepthLimit, LiveRouteConfig, LiveRouteConfigInput, LiveRuntimeConfig, LiveRuntimeConfigInput,
    LiveSnapshotReader, ShardId, ShardKey, ShardRoutingVersion, SnapshotLimits,
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const INSTRUMENT: &str = "018f0000-0000-7000-8000-000000000001";
const VENUE: &str = "coinbase";

pub(super) fn config(
    shard_count: u16,
    maximum_readers: u32,
    shutdown_deadline: Duration,
) -> TestResult<LiveRuntimeConfig> {
    Ok(LiveRuntimeConfig::try_new(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count,
        mailbox_count_per_shard: 4,
        mailbox_bytes_per_shard: 16 * 1024,
        maximum_message_bytes: 8 * 1024,
        maximum_routes_per_shard: 4,
        maximum_sources_per_route: 2,
        maximum_streams_per_route: 4,
        registration_control_capacity: 2,
        registration_deadline: Duration::from_secs(1),
        health_event_capacity: 16,
        snapshot_event_trigger: 4,
        snapshot_interval: Duration::from_secs(60),
        snapshot_limits: SnapshotLimits::try_new(4, 4, 4, 4, 16 * 1024)?,
        maximum_retained_snapshot_readers: maximum_readers,
        shutdown_deadline,
        maximum_runtime_bytes: u64::MAX,
    })?)
}

pub(super) fn route() -> TestResult<LiveRouteConfig> {
    let instrument_id = InstrumentId::from_str(INSTRUMENT)?;
    let venue = VenueId::try_from(VENUE)?;
    let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id,
        definition_revision: market_squawk_domain::InstrumentDefinitionRevision::try_from(1_u64)?,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        quote_currency: Currency::try_from("USD")?,
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::new(1, 2))?,
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
        route: ShardKey::new(venue, instrument_id),
        definition,
        depth: DepthLimit::new(4)?,
        nonce_capacity: 4,
        nonce_reclaim_budget: 1,
        maximum_capability_lifetime: Duration::from_secs(1),
    })?)
}

#[derive(Debug)]
pub(super) struct DropSignal(Option<oneshot::Sender<()>>);

impl DropSignal {
    pub(super) const fn new(sender: oneshot::Sender<()>) -> Self {
        Self(Some(sender))
    }
}

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

pub(super) struct RuntimeHarness {
    pub(super) runtime: LiveRuntime,
    pub(super) runtime_lease: RuntimeLease,
    pub(super) reader: LiveSnapshotReader,
}

pub(super) fn runtime_shell(
    config: LiveRuntimeConfig,
    incarnation: u64,
    runtime_owner: RuntimeLeaseOwner,
    cancellation: CancellationToken,
    actors: JoinSet<ActorCompletion>,
    task_shards: HashMap<tokio::task::Id, ShardId>,
) -> TestResult<RuntimeHarness> {
    let incarnation = NonZeroU64::new(incarnation).ok_or("zero incarnation")?;
    let runtime_lease = runtime_owner.lease();
    let partitions = (0..config.shard_count().get())
        .map(|_| Vec::new())
        .collect::<Vec<Vec<LiveRouteConfig>>>();
    let initial = initial_snapshots(&config, incarnation, &partitions)?;
    let bundle = create_snapshot_plane(initial, config.maximum_retained_snapshot_readers().get())?;
    let reader = bundle.reader.clone();
    let (_health_sender, health) = mpsc::channel(config.health_event_capacity().get());
    let runtime = LiveRuntime {
        estimated_peak_bytes: config.estimated_peak_bytes(&[])?,
        config,
        incarnation,
        runtime_owner: Some(runtime_owner),
        ingress: LiveRuntimeIngress {
            routes: Arc::new(HashMap::new()),
            runtime: runtime_lease.clone(),
        },
        snapshots: bundle.reader,
        snapshot_notifications: bundle.notifications,
        notification_cursor: 0,
        health,
        cancellation,
        actors: Some(actors),
        task_shards,
    };
    Ok(RuntimeHarness {
        runtime,
        runtime_lease,
        reader,
    })
}
