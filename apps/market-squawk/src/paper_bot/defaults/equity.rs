//! Explicit whole-share simulation policy over genuine admitted IEX routes.
use super::super::VirtualEquityRoute;
use super::*;

pub(crate) fn local_equity_paper_bot(
    config: AppConfig,
    routes: Vec<Arc<VirtualEquityRoute>>,
    initial_cash: Decimal,
    fee_basis_points: u32,
    mode: PaperStrategyMode,
) -> Result<ProductionPaperBotComposition> {
    if routes.is_empty()
        || routes.len() > 32
        || mode != PaperStrategyMode::Manual
        || initial_cash <= Decimal::ZERO
        || u64::from(fee_basis_points) > MAX_PAPER_FEE_BASIS_POINTS
    {
        bail!("virtual equity policy unavailable");
    }
    let first = &routes[0];
    let currency = first.terms().quote_currency();
    if routes.iter().any(|r| {
        r.key.venue().as_str() != "iex"
            || r.terms().quote_currency() != currency
            || r.evidence.session().reference() != first.evidence.session().reference()
            || r.evidence.session().date() != first.evidence.session().date()
    }) {
        bail!("virtual equity route scope differs");
    }
    let session = first.evidence.session();
    let source_digest = session
        .evidence_digest()
        .bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let calendar = PaperVenueSessionCalendar::try_new(
        SourceIdentifier::try_from(format!("paper-iex-{source_digest}"))?,
        RuleVersion::new(1)?,
        first.key.venue().clone(),
        "America/New_York",
        vec![PaperVenueSession::try_new(
            SourceIdentifier::try_from(format!(
                "iex-{source_digest}-{}",
                session.opens_at().unix_nanos(),
            ))?,
            session.opens_at(),
            session.closes_at_exclusive(),
        )?],
    )?;
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
    let (portfolio_publication, portfolio) =
        paper_sandbox_portfolio_publication(account_id, cash, routes.len(), current_timestamp()?)?;
    let risk_limits = local_risk_limits_for_instruments(
        routes.iter().map(|r| r.key.instrument()).collect(),
        cash,
        fee_basis_points,
    )?;
    let requested_paper =
        paper_config_with_calendar(currency, fee_basis_points, 5_000_000_000, calendar)?;
    let paths = LocalPaths::prepare(config.data_dir())?;
    let repository = PaperCheckpointRepository::open_stopped(
        paths.artifacts()?.clone(),
        nonzero_usize(LOCAL_PAPER_CHECKPOINT_MAXIMUM_BYTES)?,
    )?.ok_or_else(|| anyhow!("create and confirm the virtual account before starting a session"))?;
    if repository.original_account_cash(account_id)? != cash {
        bail!("selected paper cash differs from the original virtual account");
    }
    let paper_checkpoint_repository = repository.bind_execution_policy(requested_paper)?;
    let paper = paper_checkpoint_repository.original_config().clone();
    let dispatcher = local_dispatcher_config()?;
    // One fixed bounded hook per installed route, checked against the actual retained graph at start.
    let mut maximum_action_hook_bytes_per_route = 0usize;
    let identity = SourceIdentifier::try_from("local-iex-virtual-whole-share-paper-risk-v1")?;
    let risk_policy = RiskPolicyIdentity::new(&identity, RuleVersion::new(1)?);
    let mut strategies = Vec::new();
    strategies.try_reserve_exact(routes.len())?;
    for route in &routes {
        let (ingress, strategy) = ManualPaperStrategy::try_new(route.key.clone())?;
        let bound = ExecutionLiveActionHook::retained_bytes_for_composition(
            &strategy,
            &risk_limits,
            dispatcher,
            paper.market_ingress_retained_bytes()?,
        )?
        .checked_add(std::mem::size_of::<
            market_squawk_execution::virtual_paper::ExecutionVirtualPaperHook,
        >())
        .and_then(|bytes| bytes.checked_add(route.key.venue().retained_bytes()))
        .ok_or_else(|| anyhow!("virtual hook retained size overflow"))?;
        maximum_action_hook_bytes_per_route = maximum_action_hook_bytes_per_route.max(bound);
        strategies.push(
            ProductionPaperBotRoute::new(
                route.key.clone(),
                Box::new(strategy),
                Vec::new(),
                ActionAuthorityIssueLimit::MIN,
            )
            .with_manual_draft_ingress(ingress),
        );
    }
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
        portfolio_publication: Some(portfolio_publication),
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
    Ok(ProductionPaperBotComposition::try_new_virtual_equity(
        routes,
        execution,
        strategies,
        maximum_action_hook_bytes_per_route,
    )?)
}
