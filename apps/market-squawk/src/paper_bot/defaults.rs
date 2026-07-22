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
    FeeSchedule, PaperAccountBootstrap, PaperCheckpointRepository, PaperExecutionConfig,
    PaperExecutionConfigInput, PaperExposureValuation, PaperVenueSession,
    PaperVenueSessionCalendar,
};
#[cfg(test)]
use market_squawk_data::{DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest};
#[cfg(test)]
use market_squawk_domain::RevisionNumber;
use market_squawk_domain::{
    AccountId, BasisPoints, Currency, InstrumentDefinition, MarketEvent, Money, RuleVersion,
    SourceIdentifier, Timestamp,
};
#[cfg(test)]
use market_squawk_execution::portfolio_execution_state;
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap, BoundedOrderIntents,
    ExecutionAuditConfig, ExecutionDispatcherConfig, ExecutionLiveActionHook,
    PortfolioReadCapability, PortfolioReadLimits, RiskLimits, RiskLimitsInput, RiskPolicyIdentity,
    RiskServiceConfig, Strategy, StrategyContext, StrategyError,
};
use market_squawk_live::{
    ActionAuthorityIssueLimit, DepthLimit, LiveRouteConfig, LiveRouteConfigInput,
    LiveRuntimeConfig, LiveRuntimeConfigInput, RouteActionHook, ShardKey, ShardRoutingVersion,
    SnapshotLimits,
};
use market_squawk_platform::LocalPaths;
#[cfg(test)]
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, PortfolioLedger, PortfolioLimitInput,
    PortfolioLimits, PortfolioService, PortfolioServiceLimitInput, PortfolioServiceLimits,
    RevisionEvidence, TransactionRevision, ValuationSet,
};
use rust_decimal::Decimal;
#[cfg(test)]
use sha2::{Digest, Sha256};

use super::{
    ProductionPaperBotComposition, ProductionPaperBotExecutionConfig, ProductionPaperBotRoute,
};
use crate::{AppConfig, ProductionLiveSourceComposition, ProductionSourceProvider};

const LOCAL_PAPER_ACCOUNT_ID: &str = "c8cadf63-d1ce-4c37-837c-8f9f71f9525e";
const COINBASE_RETAINED_DEPTH: usize = 32;
const MAILBOX_COMMANDS_PER_ROUTE: usize = 4_096;
const MAILBOX_BYTES_PER_ROUTE: u32 = 16 * 1024 * 1024;
const FEATURE_WINDOW_OBSERVATIONS_PER_ROUTE: usize = 4_096;
const FEATURE_WINDOW_BYTES_PER_ROUTE: usize = 4 * 1024 * 1024;
const FEATURE_SETS_PER_ROUTE: usize = 4;
const CROSS_VENUE_COMMANDS_PER_ROUTE: usize = 256;
const CROSS_VENUE_BYTES_PER_ROUTE: u32 = 1024 * 1024;
const FEATURE_SNAPSHOT_BYTES_PER_ROUTE: u32 = 1024 * 1024;
const SNAPSHOT_BYTES_PER_ROUTE: u32 = 1024 * 1024;
const HEALTH_EVENTS_PER_ROUTE: usize = 1_024;
const REGISTRATION_COMMANDS_PER_ROUTE: usize = 128;
const RETAINED_SNAPSHOT_READERS_PER_SHARD: u32 = 4;
const LOCAL_LIVE_RUNTIME_MEMORY_CEILING_BYTES: u64 = 512 * 1024 * 1024;
const LOCAL_PAPER_CHECKPOINT_MAXIMUM_BYTES: usize = 64 * 1024 * 1024;
const LOCAL_PAPER_MATCHING_WORK_QUANTUM: usize = 256;

/// Builds the controlled local CLI service using explicit virtual cash and fee assumptions.
///
/// The currently selectable sealed public-source profiles remain `DirectUnverified`, so this
/// composition installs the full execution graph but cannot issue live authority or mutate paper
/// state. The no-intent strategy is a second fail-closed barrier; enabling an order-producing
/// strategy remains an explicit application configuration change.
pub fn local_paper_bot(
    config: AppConfig,
    provider: ProductionSourceProvider,
    initial_cash: Decimal,
    fee_basis_points: u32,
) -> Result<ProductionPaperBotComposition> {
    let source = configured_source(&config, provider)?;
    build_local_paper_bot(
        config,
        provider,
        source,
        initial_cash,
        fee_basis_points,
        |_route| Ok(Box::new(NoIntentStrategy)),
    )
}

/// Backward-compatible Coinbase selection for existing application callers.
pub fn local_coinbase_paper_bot(
    config: AppConfig,
    initial_cash: Decimal,
    fee_basis_points: u32,
) -> Result<ProductionPaperBotComposition> {
    local_paper_bot(
        config,
        ProductionSourceProvider::Coinbase,
        initial_cash,
        fee_basis_points,
    )
}

#[derive(Debug)]
struct ConfiguredPaperSource {
    definitions: Vec<InstrumentDefinition>,
    retained_depth: usize,
    maximum_message_bytes: u32,
    freshness_nanos: u64,
}

fn configured_source(
    config: &AppConfig,
    provider: ProductionSourceProvider,
) -> Result<ConfiguredPaperSource> {
    match provider {
        ProductionSourceProvider::Coinbase => {
            let source = config
                .coinbase()
                .ok_or_else(|| anyhow!("production Coinbase configuration is required"))?;
            Ok(ConfiguredPaperSource {
                definitions: source
                    .instruments()
                    .iter()
                    .map(|mapping| mapping.definition().clone())
                    .collect(),
                retained_depth: COINBASE_RETAINED_DEPTH,
                maximum_message_bytes: u32::try_from(source.max_frame_bytes().get())?,
                freshness_nanos: u64::try_from(source.freshness().as_nanos())?,
            })
        }
        ProductionSourceProvider::Kraken => {
            let source = config
                .kraken()
                .ok_or_else(|| anyhow!("production Kraken configuration is required"))?;
            Ok(ConfiguredPaperSource {
                definitions: vec![source.definition().clone()],
                retained_depth: source.depth(),
                maximum_message_bytes: u32::try_from(source.max_frame_bytes().get())?,
                freshness_nanos: u64::try_from(source.freshness().as_nanos())?,
            })
        }
    }
}

fn build_local_paper_bot<F>(
    config: AppConfig,
    provider: ProductionSourceProvider,
    source_profile: ConfiguredPaperSource,
    initial_cash: Decimal,
    fee_basis_points: u32,
    mut strategy_for_route: F,
) -> Result<ProductionPaperBotComposition>
where
    F: FnMut(&LiveRouteConfig) -> Result<Box<dyn Strategy>>,
{
    let ConfiguredPaperSource {
        definitions,
        retained_depth,
        maximum_message_bytes,
        freshness_nanos,
    } = source_profile;
    if initial_cash <= Decimal::ZERO {
        bail!("paper initial cash must be positive");
    }
    if fee_basis_points > 10_000 {
        bail!("paper fee basis points must not exceed 10000");
    }
    let first = definitions
        .first()
        .ok_or_else(|| anyhow!("production source instrument set is empty"))?;
    let currency = first.quote_currency();
    let venue = first
        .venue_mappings()
        .first()
        .ok_or_else(|| anyhow!("production source instrument has no venue mapping"))?
        .venue_id()
        .clone();
    if definitions.iter().any(|definition| {
        definition.quote_currency() != currency
            || definition
                .venue_mappings()
                .first()
                .is_none_or(|mapping| mapping.venue_id() != &venue)
    }) {
        bail!("one local paper run requires a single reporting currency and venue");
    }

    let mut routes = Vec::new();
    routes.try_reserve_exact(definitions.len())?;
    for definition in definitions {
        routes.push(LiveRouteConfig::try_new(LiveRouteConfigInput {
            route: ShardKey::new(venue.clone(), definition.instrument_id()),
            definition,
            depth: DepthLimit::new(retained_depth)?,
            nonce_capacity: 64,
            nonce_reclaim_budget: 8,
            maximum_capability_lifetime: Duration::from_secs(1),
        })?);
    }
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
    let portfolio_results = nonzero_usize(routes.len().max(1))?;
    let portfolio_retained_bytes = nonzero_usize(4 * 1024 * 1024)?;
    // This no-intent profile has no published portfolio dataset. Fail closed instead of
    // manufacturing provenance; an order-producing composition must inject real revision state.
    let portfolio = PortfolioReadCapability::unavailable(PortfolioReadLimits::new(
        portfolio_results,
        portfolio_retained_bytes,
    ))?;
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
    let paper = paper_config(currency, venue, fee_basis_points, freshness_nanos)?;
    let paths = LocalPaths::prepare(config.data_dir())?;
    let paper_checkpoint_repository = PaperCheckpointRepository::try_new(
        paths.artifacts()?.clone(),
        paper.clone(),
        nonzero_usize(LOCAL_PAPER_CHECKPOINT_MAXIMUM_BYTES)?,
    )?;
    let dispatcher = ExecutionDispatcherConfig {
        maximum_queued_commands: nonzero_usize(256)?,
        maximum_queued_bytes: nonzero_u32(16 * 1024 * 1024)?,
        maximum_registry_entries: nonzero_usize(1_024)?,
        maximum_pending_reconciliation_bytes: nonzero_u32(4 * 1024 * 1024)?,
        operation_deadline: Duration::from_secs(2),
        shutdown_deadline: Duration::from_secs(5),
    };
    let market_sink_retained_bytes = paper.market_ingress_retained_bytes()?;
    let mut strategies = Vec::new();
    strategies.try_reserve_exact(routes.len())?;
    let mut maximum_action_hook_bytes_per_route = 0_usize;
    for route in &routes {
        let strategy = strategy_for_route(route)?;
        let hook_retained_bytes = ExecutionLiveActionHook::retained_bytes_for_composition(
            strategy.as_ref(),
            &risk_limits,
            dispatcher,
            market_sink_retained_bytes,
        )?;
        let route_retained_bytes =
            RouteActionHook::retained_bytes_for_composition(route.route(), 0, hook_retained_bytes)?;
        maximum_action_hook_bytes_per_route =
            maximum_action_hook_bytes_per_route.max(route_retained_bytes);
        strategies.push(ProductionPaperBotRoute::new(
            route.route().clone(),
            strategy,
            Vec::new(),
            ActionAuthorityIssueLimit::MIN,
        ));
    }
    let (runtime_config, runtime_peak_bytes) = live_runtime_config(
        &routes,
        maximum_message_bytes,
        maximum_action_hook_bytes_per_route,
    )?;
    let risk_policy = bound_risk_policy(
        provider,
        maximum_action_hook_bytes_per_route,
        runtime_peak_bytes.get(),
    )?;
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
        portfolio,
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
        dispatcher,
        paper,
        paper_checkpoint_repository,
        audit_directory: paths.control_root()?.try_clone_directory()?,
        paper_accounts: vec![paper_account],
        paper_control_timeout: Duration::from_secs(5),
    };
    let source = ProductionLiveSourceComposition::try_for_provider(config, routes, provider)?;
    Ok(ProductionPaperBotComposition::try_new(
        source,
        runtime_config,
        execution,
        strategies,
    )?)
}

#[cfg(test)]
fn local_paper_portfolio_capability(
    account_id: AccountId,
    cash: Money,
    maximum_instruments: usize,
) -> Result<PortfolioReadCapability> {
    let maximum_instruments = maximum_instruments.max(1);
    let limits = PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: maximum_instruments,
        max_lots: maximum_instruments,
        max_transactions: 1,
        max_factors: 1,
        max_scenarios: 1,
        max_history: 2,
        max_results: maximum_instruments,
        max_retained_bytes: 4 * 1024 * 1024,
    })?;
    let source = SourceIdentifier::try_from("local-paper-account-bootstrap")?;
    let as_of = Timestamp::from_unix_nanos(0);
    let point_in_time_content = Sha256Digest::new(local_paper_portfolio_digest(
        b"market-squawk/local-paper-portfolio-pit-content/v1\0",
        account_id,
        cash,
    ));
    let point_in_time_audit = Sha256Digest::new(local_paper_portfolio_digest(
        b"market-squawk/local-paper-portfolio-pit-audit/v1\0",
        account_id,
        cash,
    ));
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("local-paper-account-bootstrap")?,
        1,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new(local_paper_portfolio_digest(
            b"market-squawk/local-paper-portfolio-manifest/v1\0",
            account_id,
            cash,
        )),
    )?;
    let mut ledger = PortfolioLedger::try_new(account_id, cash.currency(), limits)?;
    let revision = ledger.try_apply(
        vec![LedgerEntry::try_new(
            account_id,
            TransactionRevision::try_new(
                SourceIdentifier::try_from("local-paper-initial-cash")?,
                RevisionNumber::new(1)?,
                None,
            )?,
            as_of,
            source.clone(),
            LedgerEntryKind::CashFlow(CashFlow::try_new(CashFlowKind::Deposit, cash, None)?),
        )?],
        None,
        ValuationSet::try_new(
            cash.currency(),
            as_of,
            dataset.clone(),
            point_in_time_content,
            Vec::new(),
            Vec::new(),
            limits,
        )?,
        RevisionEvidence::try_new(
            as_of,
            dataset,
            point_in_time_content,
            point_in_time_audit,
            vec![source],
            Vec::new(),
            None,
        )?,
    )?;
    let retained_bytes = nonzero_usize(4 * 1024 * 1024)?;
    let service = PortfolioService::try_new(
        vec![revision],
        Vec::new(),
        PortfolioServiceLimits::try_new(PortfolioServiceLimitInput {
            max_accounts: NonZeroUsize::MIN,
            max_history_per_account: nonzero_usize(2)?,
            max_results: nonzero_usize(maximum_instruments)?,
            max_retained_bytes: retained_bytes,
        })?,
    )?;
    Ok(portfolio_execution_state(
        service,
        PortfolioReadLimits::new(nonzero_usize(maximum_instruments)?, retained_bytes),
    )
    .1)
}

#[cfg(test)]
fn local_paper_portfolio_digest(domain: &[u8], account_id: AccountId, cash: Money) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(account_id.as_uuid().as_bytes());
    digest.update([0]);
    digest.update(cash.amount().normalize().to_string().as_bytes());
    digest.update([0]);
    digest.update(cash.currency().as_str().as_bytes());
    digest.finalize().into()
}

#[cfg(test)]
pub(crate) fn local_paper_portfolio_capability_for_test(
    account_id: AccountId,
    cash: Money,
    maximum_instruments: usize,
) -> Result<PortfolioReadCapability> {
    local_paper_portfolio_capability(account_id, cash, maximum_instruments)
}

#[cfg(test)]
pub(crate) fn local_kraken_paper_bot_with_strategy_for_test(
    config: AppConfig,
    initial_cash: Decimal,
    fee_basis_points: u32,
    strategy: Box<dyn Strategy>,
) -> Result<ProductionPaperBotComposition> {
    let provider = ProductionSourceProvider::Kraken;
    let source = configured_source(&config, provider)?;
    let mut strategy = Some(strategy);
    build_local_paper_bot(
        config,
        provider,
        source,
        initial_cash,
        fee_basis_points,
        |_route| {
            strategy
                .take()
                .ok_or_else(|| anyhow!("Kraken test profile unexpectedly contains multiple routes"))
        },
    )
}

fn bound_risk_policy(
    provider: ProductionSourceProvider,
    maximum_action_hook_bytes_per_route: usize,
    runtime_peak_bytes: u64,
) -> Result<RiskPolicyIdentity> {
    let base = match provider {
        ProductionSourceProvider::Coinbase => "local-coinbase-paper-risk",
        ProductionSourceProvider::Kraken => "local-kraken-paper-risk",
    };
    let identity = SourceIdentifier::try_from(format!(
        "{base}-hook-{maximum_action_hook_bytes_per_route}-peak-{runtime_peak_bytes}"
    ))?;
    Ok(RiskPolicyIdentity::new(&identity, RuleVersion::new(1)?))
}

fn live_runtime_config(
    routes: &[LiveRouteConfig],
    maximum_message_bytes: u32,
    maximum_action_hook_bytes_per_route: usize,
) -> Result<(LiveRuntimeConfig, NonZeroU64)> {
    let route_count = routes.len();
    let maximum_depth = routes
        .iter()
        .map(|route| route.depth().get())
        .max()
        .ok_or_else(|| anyhow!("local live runtime requires at least one route"))?;
    let config = LiveRuntimeConfig::try_new(LiveRuntimeConfigInput {
        routing_version: ShardRoutingVersion::V1,
        shard_count: NonZeroU16::MIN.get(),
        mailbox_count_per_shard: scaled_usize(MAILBOX_COMMANDS_PER_ROUTE, route_count)?,
        mailbox_bytes_per_shard: scaled_u32(MAILBOX_BYTES_PER_ROUTE, route_count)?,
        maximum_message_bytes,
        maximum_routes_per_shard: route_count,
        maximum_sources_per_route: 2,
        maximum_streams_per_route: 8,
        maximum_feature_window_observations_per_route: FEATURE_WINDOW_OBSERVATIONS_PER_ROUTE,
        maximum_feature_window_bytes_per_route: FEATURE_WINDOW_BYTES_PER_ROUTE,
        maximum_feature_sets_per_route: FEATURE_SETS_PER_ROUTE,
        cross_venue_command_count: scaled_usize(CROSS_VENUE_COMMANDS_PER_ROUTE, route_count)?,
        cross_venue_command_bytes: scaled_u32(CROSS_VENUE_BYTES_PER_ROUTE, route_count)?,
        maximum_cross_venue_instruments: route_count,
        maximum_venues_per_cross_venue_instrument: 2,
        maximum_feature_snapshot_bytes: FEATURE_SNAPSHOT_BYTES_PER_ROUTE,
        maximum_action_hook_bytes_per_route,
        registration_control_capacity: scaled_usize(REGISTRATION_COMMANDS_PER_ROUTE, route_count)?,
        registration_deadline: Duration::from_secs(5),
        health_event_capacity: scaled_usize(HEALTH_EVENTS_PER_ROUTE, route_count)?,
        snapshot_event_trigger: 1_000,
        snapshot_interval: Duration::from_secs(1),
        snapshot_limits: SnapshotLimits::try_new(
            route_count,
            route_count,
            route_count,
            u32::try_from(maximum_depth)?,
            scaled_u32(SNAPSHOT_BYTES_PER_ROUTE, route_count)?,
        )?,
        maximum_retained_snapshot_readers: RETAINED_SNAPSHOT_READERS_PER_SHARD,
        shutdown_deadline: Duration::from_secs(5),
        maximum_runtime_bytes: LOCAL_LIVE_RUNTIME_MEMORY_CEILING_BYTES,
    })?;
    let peak = config.estimated_peak_bytes(routes)?;
    Ok((config, peak))
}

fn scaled_usize(per_route: usize, route_count: usize) -> Result<usize> {
    per_route
        .checked_mul(route_count)
        .ok_or_else(|| anyhow!("local live runtime count capacity overflowed"))
}

fn scaled_u32(per_route: u32, route_count: usize) -> Result<u32> {
    per_route
        .checked_mul(u32::try_from(route_count)?)
        .ok_or_else(|| anyhow!("local live runtime byte capacity overflowed"))
}

fn paper_config(
    currency: Currency,
    venue: market_squawk_domain::VenueId,
    fee_basis_points: u32,
    maximum_mark_age_nanos: u64,
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
        command_capacity: nonzero_usize(256)?,
        command_maximum_bytes: nonzero_u32(16 * 1024 * 1024)?,
        market_capacity: nonzero_usize(2_048)?,
        market_maximum_bytes: nonzero_u32(16 * 1024 * 1024)?,
        audit_capacity: nonzero_usize(16_384)?,
        audit_maximum_bytes: nonzero_u32(16 * 1024 * 1024)?,
        maximum_orders: nonzero_usize(4_096)?,
        maximum_fills: nonzero_usize(16_384)?,
        maximum_idempotency_keys: nonzero_usize(4_096)?,
        maximum_archived_orders: nonzero_usize(4_096)?,
        matching_work_quantum: nonzero_usize(LOCAL_PAPER_MATCHING_WORK_QUANTUM)?,
        minimum_latency_nanos: 5_000_000,
        maximum_latency_nanos: 25_000_000,
        cancel_latency_nanos: 5_000_000,
        maximum_mark_age_nanos,
        day_session_calendar: calendar,
        maximum_participation_basis_points: 1_000,
        impact_basis_points_per_level: 10,
        reporting_currency: currency,
        ledger_maximum_accounts: NonZeroUsize::MIN,
        ledger_maximum_balances: NonZeroUsize::MIN,
        ledger_maximum_positions: nonzero_usize(4_096)?,
        allow_short: false,
        exposure_valuation: PaperExposureValuation::ExecutableExit,
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
