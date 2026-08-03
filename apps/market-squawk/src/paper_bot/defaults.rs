//! Conservative local CLI policy for the sealed Coinbase paper-bot service.

use std::{
    collections::BTreeSet,
    num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize},
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Result, anyhow, bail};
use market_squawk_adapter_coinbase::{CoinbaseDirectLimits, CoinbaseTransportLimits};
use market_squawk_adapter_paper::{
    FeeSchedule, PaperAccountBootstrap, PaperCheckpointRepository, PaperExecutionConfig,
    PaperExecutionConfigInput, PaperExposureValuation, PaperVenueSession,
    PaperVenueSessionCalendar,
};
use market_squawk_analytics::{ExactFeatureRatio, RequiredLiveFeature};
use market_squawk_data::{DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest};
use market_squawk_domain::RevisionNumber;
use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, InstrumentDefinition, Money, OrderId,
    OrderReasonCode, PriceTicks, ProviderProduct, RuleVersion, SourceIdentifier, StrategyId,
    Timestamp,
};
use market_squawk_execution::portfolio_execution_state;
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap,
    BookImbalancePaperStrategy, BookImbalancePaperStrategyConfig,
    BookImbalancePaperStrategyConfigInput, ExecutionAuditConfig, ExecutionDispatcherConfig,
    ExecutionLiveActionHook, MAX_PAPER_FEE_BASIS_POINTS, ManualPaperDraftIngress,
    ManualPaperStrategy, PortfolioReadCapability, PortfolioReadLimits, RiskLimits, RiskLimitsInput,
    RiskPolicyIdentity, RiskServiceConfig, Strategy,
};
use market_squawk_live::{
    ActionAuthorityIssueLimit, DepthLimit, DirectBookLimits, LiveRouteConfig, LiveRouteConfigInput,
    LiveRuntimeConfig, LiveRuntimeConfigInput, RouteActionHook, ShardKey, ShardRoutingVersion,
    SnapshotLimits,
};
use market_squawk_platform::LocalPaths;
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, PortfolioLedger, PortfolioLimitInput,
    PortfolioLimits, PortfolioService, PortfolioServiceLimitInput, PortfolioServiceLimits,
    RevisionEvidence, TransactionRevision, ValuationSet,
};
use market_squawk_sources::{FreshnessPolicy, ProviderRateAuthority};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ProductionPaperBotComposition, ProductionPaperBotExecutionConfig, ProductionPaperBotRoute,
};
use crate::provider_rate::open_provider_rate_authority;
use crate::{
    AppConfig, CoinbaseDirectAccountActivation, CoinbaseDirectAdapterActivation,
    CoinbaseDirectProductActivation, ProductionLiveSourceComposition, ProductionSourceProvider,
    ProviderActivationOutcome, ProviderAdapterActivation, ProviderAdapterActivationRequest,
};

const LOCAL_PAPER_ACCOUNT_ID: &str = "c8cadf63-d1ce-4c37-837c-8f9f71f9525e";
const LOCAL_PAPER_STRATEGY_ID: &str = "454b500a-22ce-4a6d-a174-7320c724f78f";
const LOCAL_PAPER_REASON_CODE: &str = "paper.manual.target";
const BOOK_IMBALANCE_PAPER_REASON_CODE: &str = "paper.book-imbalance.buy";
const LOCAL_PAPER_MAXIMUM_SPREAD_TICKS: i64 = 5;
const LOCAL_PAPER_MINIMUM_IMBALANCE_NUMERATOR: i128 = 1;
const LOCAL_PAPER_MINIMUM_IMBALANCE_DENOMINATOR: u128 = 5;
const BOOK_IMBALANCE_PAPER_REQUIRED_FEATURES: [RequiredLiveFeature; 2] = [
    RequiredLiveFeature::Spread,
    RequiredLiveFeature::BookImbalance,
];
const COINBASE_RETAINED_DEPTH: usize = 32;
const COINBASE_DIRECT_MAXIMUM_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024;
const COINBASE_DIRECT_MAXIMUM_SNAPSHOT_SEGMENTS: usize = 16;
const COINBASE_DIRECT_PRODUCT_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const COINBASE_DIRECT_MAXIMUM_ORDERS: usize = 100_000;
const COINBASE_DIRECT_MAXIMUM_PRICE_LEVELS: usize = 50_000;
const COINBASE_DIRECT_MAXIMUM_QUEUE_EVENTS: usize = 16_384;
const COINBASE_DIRECT_MAXIMUM_QUEUE_BYTES: usize = 64 * 1024 * 1024;
const COINBASE_DIRECT_MAXIMUM_RUNTIME_BYTES: u64 = 6 * 1024 * 1024 * 1024;
const COINBASE_DIRECT_SUPERVISOR_QUEUE_RECORDS: usize =
    crate::COINBASE_DIRECT_MAXIMUM_SUBSCRIPTIONS;
const COINBASE_DIRECT_SUPERVISOR_QUEUE_BYTES: usize = 64 * 1024;
const COINBASE_DIRECT_MAXIMUM_CLOCK_SKEW_NANOS: u64 = 1_000_000_000;
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
/// Initial cash is admitted as an explicit evidence-bound paper sandbox portfolio. That immutable
/// revision is the same capability consumed by central risk; no empty or bypass portfolio
/// authority is installed.
pub fn local_paper_bot(
    config: AppConfig,
    provider: ProductionSourceProvider,
    initial_cash: Decimal,
    fee_basis_points: u32,
) -> Result<ProductionPaperBotComposition> {
    let paths = LocalPaths::prepare(config.data_dir())?;
    let provider_rate = open_provider_rate_authority(paths.control_root()?.root())?;
    local_paper_bot_with_provider_rate(
        config,
        provider,
        initial_cash,
        fee_basis_points,
        provider_rate,
    )
}

pub(crate) fn local_paper_bot_with_provider_rate(
    config: AppConfig,
    provider: ProductionSourceProvider,
    initial_cash: Decimal,
    fee_basis_points: u32,
    provider_rate: ProviderRateAuthority,
) -> Result<ProductionPaperBotComposition> {
    local_paper_bot_with_provider_rate_and_strategy_mode(
        config,
        provider,
        initial_cash,
        fee_basis_points,
        provider_rate,
        PaperStrategyMode::Manual,
    )
}

/// Builds one public paper source with an explicitly selected, closed strategy mode.
pub(crate) fn local_paper_bot_with_provider_rate_and_strategy_mode(
    config: AppConfig,
    provider: ProductionSourceProvider,
    initial_cash: Decimal,
    fee_basis_points: u32,
    provider_rate: ProviderRateAuthority,
    strategy_mode: PaperStrategyMode,
) -> Result<ProductionPaperBotComposition> {
    let source = configured_source(&config, provider)?;
    build_local_paper_bot(
        config,
        PaperBotBuildSource::Production {
            provider,
            provider_rate,
        },
        source,
        initial_cash,
        fee_basis_points,
        0,
        move |route| strategy_mode.for_route(route),
    )
}

/// Builds one activated Coinbase Direct paper source with an explicitly selected closed strategy.
pub(crate) async fn local_coinbase_direct_paper_bot_with_activation_and_strategy_mode(
    config: AppConfig,
    provider_session_id: Uuid,
    initial_cash: Decimal,
    fee_basis_points: u32,
    provider_activation: &ProviderAdapterActivation,
    cancellation: CancellationToken,
    strategy_mode: PaperStrategyMode,
) -> Result<ProductionPaperBotComposition> {
    let source = configured_source(&config, ProductionSourceProvider::Coinbase)?;
    let request = coinbase_direct_activation_request(&config, &source)?;
    let activation = provider_activation
        .activate_ready_profile(provider_session_id, request, cancellation)
        .await?;
    let ProviderActivationOutcome::CoinbaseDirect(activation) = activation else {
        bail!("provider session did not activate the Coinbase Direct surface");
    };
    build_local_paper_bot(
        config,
        PaperBotBuildSource::CoinbaseDirect(activation),
        source,
        initial_cash,
        fee_basis_points,
        0,
        move |route| strategy_mode.for_route(route),
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
    routes: Vec<LiveRouteConfig>,
    maximum_message_bytes: u32,
    freshness_nanos: u64,
}

/// Closed production paper-strategy selection.
///
/// Manual operation is the default and only exposes a route-bound draft ingress. Automated
/// operation is retained for explicit operator configuration; it never creates a manual ingress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PaperStrategyMode {
    Manual,
    BookImbalance,
}

impl PaperStrategyMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::BookImbalance => "book_imbalance",
        }
    }

    fn for_route(self, route: &LiveRouteConfig) -> Result<PaperRouteStrategy> {
        match self {
            Self::Manual => PaperRouteStrategy::manual(route),
            Self::BookImbalance => {
                book_imbalance_paper_strategy(route).map(PaperRouteStrategy::automated)
            }
        }
    }
}

/// One strategy transferred into its route with an optional paired manual-draft sender.
///
/// The sender stays inside production composition and is never exposed by a strategy factory.
pub(super) struct PaperRouteStrategy {
    strategy: Box<dyn Strategy>,
    required_features: Vec<RequiredLiveFeature>,
    manual_draft_ingress: Option<ManualPaperDraftIngress>,
}

impl PaperRouteStrategy {
    pub(super) fn automated(strategy: Box<dyn Strategy>) -> Self {
        Self {
            strategy,
            required_features: BOOK_IMBALANCE_PAPER_REQUIRED_FEATURES.to_vec(),
            manual_draft_ingress: None,
        }
    }

    fn manual(route: &LiveRouteConfig) -> Result<Self> {
        let (ingress, strategy) = ManualPaperStrategy::try_new(route.route().clone())?;
        Ok(Self {
            strategy: Box::new(strategy),
            required_features: Vec::new(),
            manual_draft_ingress: Some(ingress),
        })
    }
}

enum PaperBotBuildSource {
    Production {
        provider: ProductionSourceProvider,
        provider_rate: ProviderRateAuthority,
    },
    CoinbaseDirect(Box<CoinbaseDirectAccountActivation>),
    #[cfg(feature = "release-evidence")]
    ReleaseBenchmark,
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
            configured_paper_source(
                source
                    .instruments()
                    .iter()
                    .map(|mapping| mapping.definition().clone())
                    .collect(),
                COINBASE_RETAINED_DEPTH,
                u32::try_from(source.max_frame_bytes().get())?,
                u64::try_from(source.freshness().as_nanos())?,
            )
        }
        ProductionSourceProvider::Kraken => {
            let source = config
                .kraken()
                .ok_or_else(|| anyhow!("production Kraken configuration is required"))?;
            configured_paper_source(
                vec![source.definition().clone()],
                source.depth(),
                u32::try_from(source.max_frame_bytes().get())?,
                u64::try_from(source.freshness().as_nanos())?,
            )
        }
    }
}

fn configured_paper_source(
    definitions: Vec<InstrumentDefinition>,
    retained_depth: usize,
    maximum_message_bytes: u32,
    freshness_nanos: u64,
) -> Result<ConfiguredPaperSource> {
    let first = definitions
        .first()
        .ok_or_else(|| anyhow!("production source instrument set is empty"))?;
    let venue = first
        .venue_mappings()
        .first()
        .ok_or_else(|| anyhow!("production source instrument has no venue mapping"))?
        .venue_id()
        .clone();
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
    Ok(ConfiguredPaperSource {
        routes,
        maximum_message_bytes,
        freshness_nanos,
    })
}

fn coinbase_direct_activation_request(
    config: &AppConfig,
    source: &ConfiguredPaperSource,
) -> Result<ProviderAdapterActivationRequest> {
    let coinbase = config
        .coinbase()
        .ok_or_else(|| anyhow!("production Coinbase configuration is required"))?;
    if coinbase.instruments().len() != source.routes.len() {
        bail!("Coinbase Direct route topology differs from configured products");
    }
    let freshness = FreshnessPolicy::try_new(
        source.freshness_nanos,
        source.freshness_nanos,
        source.freshness_nanos,
        source.freshness_nanos,
        COINBASE_DIRECT_MAXIMUM_CLOCK_SKEW_NANOS,
    )?;
    let transport = CoinbaseTransportLimits::try_new(
        coinbase.max_frame_bytes().get(),
        coinbase.subscription_ack_timeout(),
        coinbase.subscription_ack_timeout(),
    )?;
    let limits = CoinbaseDirectLimits::try_new(
        transport,
        COINBASE_DIRECT_MAXIMUM_SNAPSHOT_BYTES,
        COINBASE_DIRECT_MAXIMUM_SNAPSHOT_SEGMENTS,
        COINBASE_DIRECT_PRODUCT_REFRESH_INTERVAL,
        DirectBookLimits::try_new(
            COINBASE_DIRECT_MAXIMUM_ORDERS,
            COINBASE_DIRECT_MAXIMUM_PRICE_LEVELS,
            COINBASE_DIRECT_MAXIMUM_QUEUE_EVENTS,
            COINBASE_DIRECT_MAXIMUM_QUEUE_BYTES,
            COINBASE_RETAINED_DEPTH,
        )?,
    )?;
    let mut products = Vec::new();
    products.try_reserve_exact(source.routes.len())?;
    for (mapping, route) in coinbase.instruments().iter().zip(&source.routes) {
        products.push(CoinbaseDirectProductActivation::try_new(
            ProviderProduct::new(SourceIdentifier::try_from(mapping.product())?),
            route.clone(),
            freshness,
            limits,
        )?);
    }
    Ok(ProviderAdapterActivationRequest::CoinbaseDirect(
        CoinbaseDirectAdapterActivation::try_new(
            products,
            nonzero_u64(COINBASE_DIRECT_MAXIMUM_RUNTIME_BYTES)?,
            config.capture_queue_capacity(),
            config.capture_memory_ceiling_bytes(),
            nonzero_usize(COINBASE_DIRECT_SUPERVISOR_QUEUE_RECORDS)?,
            nonzero_usize(COINBASE_DIRECT_SUPERVISOR_QUEUE_BYTES)?,
        )?,
    ))
}

fn build_local_paper_bot<F>(
    config: AppConfig,
    build_source: PaperBotBuildSource,
    source_profile: ConfiguredPaperSource,
    initial_cash: Decimal,
    fee_basis_points: u32,
    action_hook_overhead_bytes: usize,
    mut strategy_for_route: F,
) -> Result<ProductionPaperBotComposition>
where
    F: FnMut(&LiveRouteConfig) -> Result<PaperRouteStrategy>,
{
    let ConfiguredPaperSource {
        routes,
        maximum_message_bytes,
        freshness_nanos,
    } = source_profile;
    if initial_cash <= Decimal::ZERO {
        bail!("paper initial cash must be positive");
    }
    if u64::from(fee_basis_points) > MAX_PAPER_FEE_BASIS_POINTS {
        bail!("paper fee basis points must not exceed {MAX_PAPER_FEE_BASIS_POINTS}");
    }
    let first = routes
        .first()
        .ok_or_else(|| anyhow!("production source route set is empty"))?;
    let currency = first.definition().quote_currency();
    let venue = first.route().venue().clone();
    if routes.iter().any(|route| {
        route.definition().quote_currency() != currency || route.route().venue() != &venue
    }) {
        bail!("one local paper run requires a single reporting currency and venue");
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
    let portfolio =
        paper_sandbox_portfolio_capability(account_id, cash, routes.len(), current_timestamp()?)?;
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
        let PaperRouteStrategy {
            strategy,
            required_features,
            manual_draft_ingress,
        } = strategy_for_route(route)?;
        let hook_retained_bytes = ExecutionLiveActionHook::retained_bytes_for_composition(
            strategy.as_ref(),
            &risk_limits,
            dispatcher,
            market_sink_retained_bytes,
        )?
        .checked_add(action_hook_overhead_bytes)
        .ok_or_else(|| anyhow!("paper action-hook retained bytes overflowed"))?;
        let route_retained_bytes = RouteActionHook::retained_bytes_for_composition(
            route.route(),
            required_features.len(),
            hook_retained_bytes,
        )?;
        maximum_action_hook_bytes_per_route =
            maximum_action_hook_bytes_per_route.max(route_retained_bytes);
        let strategy = ProductionPaperBotRoute::new(
            route.route().clone(),
            strategy,
            required_features,
            ActionAuthorityIssueLimit::MIN,
        );
        let strategy = match manual_draft_ingress {
            Some(ingress) => strategy.with_manual_draft_ingress(ingress),
            None => strategy,
        };
        strategies.push(strategy);
    }
    let (runtime_config, runtime_peak_bytes) = live_runtime_config(
        &routes,
        maximum_message_bytes,
        maximum_action_hook_bytes_per_route,
    )?;
    let risk_policy = bound_risk_policy(
        &build_source,
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
    match build_source {
        PaperBotBuildSource::Production {
            provider,
            provider_rate,
        } => {
            let source = ProductionLiveSourceComposition::try_for_provider_with_rate_authority(
                config,
                routes,
                provider,
                provider_rate,
            )?;
            Ok(ProductionPaperBotComposition::try_new(
                source,
                runtime_config,
                execution,
                strategies,
            )?)
        }
        PaperBotBuildSource::CoinbaseDirect(activation) => {
            Ok(ProductionPaperBotComposition::try_new_coinbase_direct(
                *activation,
                runtime_config,
                execution,
                strategies,
            )?)
        }
        #[cfg(feature = "release-evidence")]
        PaperBotBuildSource::ReleaseBenchmark => {
            Ok(ProductionPaperBotComposition::try_new_release_benchmark(
                routes,
                runtime_config,
                execution,
                strategies,
            )?)
        }
    }
}

#[cfg(feature = "release-evidence")]
pub(super) fn release_benchmark_paper_bot<F>(
    config: AppConfig,
    definition: InstrumentDefinition,
    action_hook_overhead_bytes: usize,
    strategy_for_route: F,
) -> Result<ProductionPaperBotComposition>
where
    F: FnMut(&LiveRouteConfig) -> Result<PaperRouteStrategy>,
{
    build_local_paper_bot(
        config,
        PaperBotBuildSource::ReleaseBenchmark,
        configured_paper_source(vec![definition], 10, 16 * 1024 * 1024, 60_000_000_000)?,
        Decimal::new(1_000_000, 0),
        0,
        action_hook_overhead_bytes,
        strategy_for_route,
    )
}

fn paper_sandbox_portfolio_capability(
    account_id: AccountId,
    cash: Money,
    maximum_instruments: usize,
    admitted_at: Timestamp,
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
    let source = SourceIdentifier::try_from("paper-sandbox-user-authorized-initial-cash")?;
    let point_in_time_content = Sha256Digest::new(paper_sandbox_portfolio_digest(
        b"market-squawk/paper-sandbox-portfolio-pit-content/v1\0",
        account_id,
        cash,
        admitted_at,
    ));
    let point_in_time_audit = Sha256Digest::new(paper_sandbox_portfolio_digest(
        b"market-squawk/paper-sandbox-portfolio-pit-audit/v1\0",
        account_id,
        cash,
        admitted_at,
    ));
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("paper-sandbox-initial-capital")?,
        1,
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new(paper_sandbox_portfolio_digest(
            b"market-squawk/paper-sandbox-portfolio-manifest/v1\0",
            account_id,
            cash,
            admitted_at,
        )),
    )?;
    let mut ledger = PortfolioLedger::try_new(account_id, cash.currency(), limits)?;
    let revision = ledger.try_apply(
        vec![LedgerEntry::try_new(
            account_id,
            TransactionRevision::try_new(
                SourceIdentifier::try_from("paper-sandbox-initial-cash-deposit")?,
                RevisionNumber::new(1)?,
                None,
            )?,
            admitted_at,
            source.clone(),
            LedgerEntryKind::CashFlow(CashFlow::try_new(CashFlowKind::Deposit, cash, None)?),
        )?],
        None,
        ValuationSet::try_new(
            cash.currency(),
            admitted_at,
            dataset.clone(),
            point_in_time_content,
            Vec::new(),
            Vec::new(),
            limits,
        )?,
        RevisionEvidence::try_new(
            admitted_at,
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
        PortfolioReadLimits::new(
            nonzero_usize(maximum_instruments)?,
            retained_bytes,
            nonzero_usize(4_096)?,
        ),
    )?
    .1)
}

fn paper_sandbox_portfolio_digest(
    domain: &[u8],
    account_id: AccountId,
    cash: Money,
    admitted_at: Timestamp,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(account_id.as_uuid().as_bytes());
    digest.update([0]);
    digest.update(cash.amount().normalize().to_string().as_bytes());
    digest.update([0]);
    digest.update(cash.currency().as_str().as_bytes());
    digest.update([0]);
    digest.update(admitted_at.unix_nanos().to_be_bytes());
    digest.finalize().into()
}

fn current_timestamp() -> Result<Timestamp> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(Timestamp::from_unix_nanos(i64::try_from(
        elapsed.as_nanos(),
    )?))
}

#[cfg(test)]
pub(crate) fn local_paper_portfolio_capability_for_test(
    account_id: AccountId,
    cash: Money,
    maximum_instruments: usize,
) -> Result<PortfolioReadCapability> {
    paper_sandbox_portfolio_capability(
        account_id,
        cash,
        maximum_instruments,
        Timestamp::from_unix_nanos(0),
    )
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
    let paths = LocalPaths::prepare(config.data_dir())?;
    let provider_rate = open_provider_rate_authority(paths.control_root()?.root())?;
    let mut strategy = Some(strategy);
    build_local_paper_bot(
        config,
        PaperBotBuildSource::Production {
            provider,
            provider_rate,
        },
        source,
        initial_cash,
        fee_basis_points,
        0,
        |_route| {
            strategy
                .take()
                .map(PaperRouteStrategy::automated)
                .ok_or_else(|| anyhow!("Kraken test profile unexpectedly contains multiple routes"))
        },
    )
}

fn bound_risk_policy(
    build_source: &PaperBotBuildSource,
    maximum_action_hook_bytes_per_route: usize,
    runtime_peak_bytes: u64,
) -> Result<RiskPolicyIdentity> {
    let base = match build_source {
        PaperBotBuildSource::Production {
            provider: ProductionSourceProvider::Coinbase,
            ..
        } => "local-coinbase-paper-risk",
        PaperBotBuildSource::Production {
            provider: ProductionSourceProvider::Kraken,
            ..
        } => "local-kraken-paper-risk",
        PaperBotBuildSource::CoinbaseDirect(_) => "local-coinbase-direct-paper-risk",
        #[cfg(feature = "release-evidence")]
        PaperBotBuildSource::ReleaseBenchmark => "release-benchmark-paper-risk",
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

pub(super) fn book_imbalance_paper_strategy(route: &LiveRouteConfig) -> Result<Box<dyn Strategy>> {
    let order_uuid = Uuid::new_v4();
    let config =
        BookImbalancePaperStrategyConfig::try_new(BookImbalancePaperStrategyConfigInput {
            route: route.route().clone(),
            account_id: AccountId::from_str(LOCAL_PAPER_ACCOUNT_ID)?,
            order_id: OrderId::try_from(order_uuid)?,
            client_order_id: ClientOrderId::try_from(format!("paper-book-imbalance-{order_uuid}"))?,
            strategy_id: StrategyId::from_str(LOCAL_PAPER_STRATEGY_ID)?,
            reason_code: OrderReasonCode::try_from(BOOK_IMBALANCE_PAPER_REASON_CODE)?,
            maximum_spread: PriceTicks::new(LOCAL_PAPER_MAXIMUM_SPREAD_TICKS),
            minimum_book_imbalance: ExactFeatureRatio::try_new(
                LOCAL_PAPER_MINIMUM_IMBALANCE_NUMERATOR,
                LOCAL_PAPER_MINIMUM_IMBALANCE_DENOMINATOR,
            )?,
        })?;
    Ok(Box::new(BookImbalancePaperStrategy::try_new(config)?))
}

pub(crate) fn manual_paper_account_id() -> Result<AccountId> {
    AccountId::from_str(LOCAL_PAPER_ACCOUNT_ID).map_err(Into::into)
}

pub(crate) fn manual_paper_strategy_id() -> Result<StrategyId> {
    StrategyId::from_str(LOCAL_PAPER_STRATEGY_ID).map_err(Into::into)
}

pub(crate) fn manual_paper_reason_code() -> Result<OrderReasonCode> {
    OrderReasonCode::try_from(LOCAL_PAPER_REASON_CODE).map_err(Into::into)
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
