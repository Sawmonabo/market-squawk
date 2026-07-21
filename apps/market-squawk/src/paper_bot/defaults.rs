//! Conservative local CLI policy for the sealed Coinbase paper-bot service.

use std::{
    collections::BTreeSet,
    mem::size_of,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    str::FromStr,
    time::Duration,
};

use anyhow::{Result, anyhow, bail};
use market_squawk_adapter_paper::{
    FeeSchedule, PaperAccountBootstrap, PaperExecutionConfig, PaperExecutionConfigInput,
    PaperExposureValuation, PaperVenueSession, PaperVenueSessionCalendar,
};
use market_squawk_domain::{
    AccountId, BasisPoints, Currency, MarketEvent, Money, RuleVersion, SourceIdentifier, Timestamp,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap, BoundedOrderIntents,
    ExecutionAuditConfig, ExecutionDispatcherConfig, RiskLimits, RiskLimitsInput,
    RiskPolicyIdentity, RiskServiceConfig, Strategy, StrategyContext, StrategyError,
};
use market_squawk_live::{
    ActionAuthorityIssueLimit, DepthLimit, LiveRouteConfig, LiveRouteConfigInput,
    LiveRuntimeConfig, LiveRuntimeConfigInput, ShardKey, ShardRoutingVersion, SnapshotLimits,
};
use rust_decimal::Decimal;

use super::{
    ProductionPaperBotComposition, ProductionPaperBotExecutionConfig, ProductionPaperBotRoute,
};
use crate::{AppConfig, ProductionLiveSourceComposition};

const LOCAL_PAPER_ACCOUNT_ID: &str = "c8cadf63-d1ce-4c37-837c-8f9f71f9525e";

/// Builds the controlled local CLI service using explicit virtual cash and fee assumptions.
///
/// Coinbase's sealed production profile remains `DirectUnverified`, so this composition installs
/// the full execution graph but cannot issue live authority or mutate paper state. The no-intent
/// strategy is a second fail-closed barrier; enabling an order-producing strategy remains an
/// explicit application configuration change.
pub fn local_coinbase_paper_bot(
    config: AppConfig,
    initial_cash: Decimal,
    fee_basis_points: u32,
) -> Result<ProductionPaperBotComposition> {
    if initial_cash <= Decimal::ZERO {
        bail!("paper initial cash must be positive");
    }
    if fee_basis_points > 10_000 {
        bail!("paper fee basis points must not exceed 10000");
    }
    let source_config = config
        .coinbase()
        .ok_or_else(|| anyhow!("production Coinbase configuration is required"))?;
    let first = source_config
        .instruments()
        .first()
        .ok_or_else(|| anyhow!("production Coinbase instrument set is empty"))?;
    let currency = first.definition().quote_currency();
    let venue = first
        .definition()
        .venue_mappings()
        .first()
        .ok_or_else(|| anyhow!("production Coinbase instrument has no venue mapping"))?
        .venue_id()
        .clone();
    if source_config.instruments().iter().any(|mapping| {
        mapping.definition().quote_currency() != currency
            || mapping
                .definition()
                .venue_mappings()
                .first()
                .is_none_or(|mapping| mapping.venue_id() != &venue)
    }) {
        bail!("one local paper run requires a single reporting currency and venue");
    }

    let mut routes = Vec::new();
    routes.try_reserve_exact(source_config.instruments().len())?;
    for mapping in source_config.instruments() {
        routes.push(LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: ShardKey::new(venue.clone(), mapping.definition().instrument_id()),
            definition: mapping.definition().clone(),
            depth: DepthLimit::new(32)?,
            nonce_capacity: 64,
            nonce_reclaim_budget: 8,
            maximum_capability_lifetime: Duration::from_secs(1),
        })?);
    }
    let runtime_config = live_runtime_config(routes.len())?;
    let account_id = AccountId::from_str(LOCAL_PAPER_ACCOUNT_ID)?;
    let cash = Money::new(initial_cash, currency);
    let zero = Money::new(Decimal::ZERO, currency);
    let account = AccountBootstrap {
        account_id,
        revision: NonZeroU64::MIN,
        eligible: true,
        cash,
        capital: cash,
        peak_capital: cash,
        gross_exposure: zero,
        realized_pnl: zero,
        realized_loss: zero,
        positions: Vec::new(),
        position_cost_basis: Vec::new(),
        idempotency: AccountIdempotencyBootstrap::empty(),
    };
    let paper_account = PaperAccountBootstrap {
        account_id,
        revision: NonZeroU64::MIN,
        eligible: true,
        cash: vec![cash],
        capital: cash,
        peak_capital: cash,
        gross_exposure: zero,
        realized_pnl: zero,
        realized_loss: zero,
        positions: Vec::new(),
        position_cost_basis: Vec::new(),
    };
    let risk_limits = RiskLimits::try_new(RiskLimitsInput {
        currency,
        eligible_instruments: routes
            .iter()
            .map(|route| route.route().instrument())
            .collect::<BTreeSet<_>>(),
        maximum_position_lots: 1_000_000,
        maximum_order_notional: cash,
        maximum_gross_exposure: cash,
        maximum_leverage: BasisPoints::new(10_000),
        minimum_capital: Money::new(Decimal::ONE, currency),
        maximum_loss: cash,
        maximum_drawdown: cash,
        maximum_fee: BasisPoints::new(i32::try_from(fee_basis_points)?),
        maximum_price_deviation: BasisPoints::new(1_000),
        maximum_slippage: BasisPoints::new(1_000),
        maximum_orders_per_window: nonzero_u32(16)?,
        order_rate_window_nanos: 60_000_000_000,
        reservation_ttl_nanos: 5_000_000_000,
        allow_short: false,
        kill_switch: false,
    })?;
    let risk_policy = RiskPolicyIdentity::new(
        &SourceIdentifier::try_from("local-coinbase-paper-risk")?,
        RuleVersion::new(1)?,
    );
    let paper = paper_config(currency, venue, fee_basis_points)?;
    let execution = ProductionPaperBotExecutionConfig {
        account_coordinator: AccountCoordinatorConfig {
            partition_count: NonZeroUsize::MIN,
            max_accounts_per_partition: NonZeroUsize::MIN,
            max_reservations_per_account: nonzero_usize(4_096)?,
            max_positions_per_account: nonzero_usize(routes.len())?,
            max_idempotency_keys_per_account: nonzero_usize(4_096)?,
            maximum_intent_lifetime_nanos: nonzero_u64(86_400_000_000_000)?,
            max_rate_events_per_account: nonzero_usize(1_024)?,
        },
        accounts: vec![account],
        risk_limits,
        risk_service: RiskServiceConfig {
            policy: risk_policy,
            policy_valid_until: Timestamp::from_unix_nanos(i64::MAX),
            maximum_approval_lifetime: Duration::from_secs(5),
        },
        execution_audit: ExecutionAuditConfig {
            maximum_records: nonzero_usize(4_096)?,
            maximum_bytes: nonzero_u32(16 * 1024 * 1024)?,
        },
        dispatcher: ExecutionDispatcherConfig {
            maximum_queued_commands: nonzero_usize(1_024)?,
            maximum_queued_bytes: nonzero_u32(64 * 1024 * 1024)?,
            maximum_registry_entries: nonzero_usize(4_096)?,
            maximum_pending_reconciliation_bytes: nonzero_u32(16 * 1024 * 1024)?,
            operation_deadline: Duration::from_secs(2),
            shutdown_deadline: Duration::from_secs(5),
        },
        paper,
        paper_accounts: vec![paper_account],
        paper_control_timeout: Duration::from_secs(5),
    };
    let strategies = routes
        .iter()
        .map(|route| {
            ProductionPaperBotRoute::new(
                route.route().clone(),
                Box::new(NoIntentStrategy),
                Vec::new(),
                ActionAuthorityIssueLimit::MIN,
            )
        })
        .collect();
    let source = ProductionLiveSourceComposition::try_new(config, routes)?;
    Ok(ProductionPaperBotComposition::try_new(
        source,
        runtime_config,
        execution,
        strategies,
    )?)
}

fn live_runtime_config(route_count: usize) -> Result<LiveRuntimeConfig> {
    Ok(LiveRuntimeConfig::try_new(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: NonZeroU16::MIN.get(),
        mailbox_count_per_shard: 4_096,
        mailbox_bytes_per_shard: 64 * 1024 * 1024,
        maximum_message_bytes: 1024 * 1024,
        maximum_routes_per_shard: route_count,
        maximum_sources_per_route: 2,
        maximum_streams_per_route: 8,
        maximum_feature_window_observations_per_route: 4_096,
        maximum_feature_window_bytes_per_route: 16 * 1024 * 1024,
        maximum_feature_sets_per_route: 4,
        cross_venue_command_count: 1_024,
        cross_venue_command_bytes: 16 * 1024 * 1024,
        maximum_cross_venue_instruments: route_count,
        maximum_venues_per_cross_venue_instrument: 2,
        maximum_feature_snapshot_bytes: 4 * 1024 * 1024,
        maximum_action_hook_bytes_per_route: 1024 * 1024,
        registration_control_capacity: 128,
        registration_deadline: Duration::from_secs(5),
        health_event_capacity: 4_096,
        snapshot_event_trigger: 1_000,
        snapshot_interval: Duration::from_secs(1),
        snapshot_limits: SnapshotLimits::try_new(
            route_count,
            route_count,
            route_count,
            32,
            64 * 1024 * 1024,
        )?,
        maximum_retained_snapshot_readers: 16,
        shutdown_deadline: Duration::from_secs(5),
        maximum_runtime_bytes: 512 * 1024 * 1024,
    })?)
}

fn paper_config(
    currency: Currency,
    venue: market_squawk_domain::VenueId,
    fee_basis_points: u32,
) -> Result<PaperExecutionConfig> {
    let calendar = PaperVenueSessionCalendar::try_new(
        SourceIdentifier::try_from("coinbase-continuous-calendar")?,
        RuleVersion::new(1)?,
        venue,
        "UTC",
        vec![PaperVenueSession::try_new(
            SourceIdentifier::try_from("coinbase-continuous-session")?,
            Timestamp::from_unix_nanos(i64::MIN),
            Timestamp::from_unix_nanos(i64::MAX),
        )?],
    )?;
    let fees = FeeSchedule::try_new(
        fee_basis_points,
        fee_basis_points,
        Money::new(Decimal::ZERO, currency),
        None,
        8,
    )?;
    Ok(PaperExecutionConfig::try_new(PaperExecutionConfigInput {
        configuration_version: NonZeroU64::MIN,
        deterministic_seed: [0x5a; 32],
        command_capacity: nonzero_usize(1_024)?,
        command_maximum_bytes: nonzero_u32(64 * 1024 * 1024)?,
        market_capacity: nonzero_usize(4_096)?,
        market_maximum_bytes: nonzero_u32(64 * 1024 * 1024)?,
        audit_capacity: nonzero_usize(16_384)?,
        audit_maximum_bytes: nonzero_u32(16 * 1024 * 1024)?,
        maximum_orders: nonzero_usize(4_096)?,
        maximum_fills: nonzero_usize(16_384)?,
        maximum_idempotency_keys: nonzero_usize(4_096)?,
        maximum_archived_orders: nonzero_usize(4_096)?,
        minimum_latency_nanos: 5_000_000,
        maximum_latency_nanos: 25_000_000,
        cancel_latency_nanos: 5_000_000,
        day_session_calendar: calendar,
        maximum_participation_basis_points: 1_000,
        impact_basis_points_per_level: 10,
        reporting_currency: currency,
        ledger_maximum_accounts: NonZeroUsize::MIN,
        ledger_maximum_balances: NonZeroUsize::MIN,
        ledger_maximum_positions: nonzero_usize(4_096)?,
        allow_short: false,
        exposure_valuation: PaperExposureValuation::OpenCost,
        abort_join_deadline: Duration::from_secs(5),
        fee_schedule: fees,
    })?)
}

#[derive(Debug)]
struct NoIntentStrategy;

impl Strategy for NoIntentStrategy {
    fn on_market_event(
        &mut self,
        _context: &StrategyContext<'_>,
        _event: &MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        Ok(BoundedOrderIntents::new())
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        Ok(size_of::<Self>())
    }
}

fn nonzero_usize(value: usize) -> Result<NonZeroUsize> {
    NonZeroUsize::new(value).ok_or_else(|| anyhow!("bounded count must be positive"))
}

fn nonzero_u32(value: u32) -> Result<NonZeroU32> {
    NonZeroU32::new(value).ok_or_else(|| anyhow!("bounded byte/count value must be positive"))
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64> {
    NonZeroU64::new(value).ok_or_else(|| anyhow!("bounded duration value must be positive"))
}
