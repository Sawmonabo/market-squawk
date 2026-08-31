use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    ffi::OsString,
    mem::size_of,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::{SecondsFormat, Utc};
use futures_util::{SinkExt, StreamExt};
use market_squawk_domain::{
    AccountId, AggressorSide, BasisPoints, ChecksumCapability, ClientOrderId, ConnectionGeneration,
    Currency, DataQuality, InstrumentExecutionTerms, InstrumentId, Money, OrderId, OrderReasonCode,
    OrderSide, OrderType, PriceTicks, QuantityLots, RuleVersion, SourceId, SourceIdentifier,
    StrategyId, TimeInForce, Timestamp, TradingStatus,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap,
    AccountRiskCoordinator, AccountRiskViolation, BoundedOrderIntents, ExecutionAuditConfig,
    ExecutionAuditWriter, MarketRiskInput, OrderIntent, OrderIntentInput, PreAuthorityRiskOutcome,
    RiskLimits, RiskLimitsInput, RiskPolicyIdentity, RiskRejectionCode, RiskService,
    RiskServiceConfig, Strategy, StrategyContext, StrategyError,
};
use market_squawk_live::StreamPhaseSnapshot;
use market_squawk_platform::{
    AppConfig, ConfigOverrides, ConfigSources, JournalReader, LocalPaths,
};
use rust_decimal::Decimal;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::paper_bot::{
    local_kraken_paper_bot_with_strategy_for_test, local_paper_portfolio_capability_for_test,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const PAPER_ACCOUNT_ID: &str = "c8cadf63-d1ce-4c37-837c-8f9f71f9525e";
const INSTRUMENT_ID: &str = "4c74ab95-53b9-42ad-9b66-0ed403b88fed";
const UPDATE_BEFORE_SNAPSHOT: &str = r#"{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":"45283.5","qty":"0"}],"asks":[],"checksum":1,"timestamp":"2023-10-04T07:48:26Z"}]}"#;
const FIRST_GENERATION_READY: &[u8] = b"kraken-generation-one-ready";
const LOCAL_SUBSCRIPTION_BOUND: Duration = Duration::from_secs(45);
const LOCAL_SNAPSHOT_BOUND: Duration = Duration::from_secs(45);
static KRAKEN_VERTICAL_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// This integration test preserves the two independent safety layers:
///
/// 1. the real source/capture/live pipeline admits Kraken only as `DirectUnverified`, so it never
///    creates `AppliedObservationAuthority` or invokes the installed action graph; and
/// 2. a separate authority-free risk probe built from that running session's exact immutable
///    metadata and snapshot evidence independently rejects `SourceQuality`.
///
/// The risk probe is evidence for defense in depth. It is deliberately not represented as a
/// production hook invocation and cannot create an approval or reach dispatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_kraken_reaches_live_state_but_both_execution_safety_layers_reject_it() -> TestResult
{
    let _budget_guard = KRAKEN_VERTICAL_TEST_LOCK.lock().await;
    let temporary = tempfile::tempdir()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}/", listener.local_addr()?);
    let frames = current_kraken_frames()?;
    let expected_book_acknowledgement = frames.book_acknowledgement.clone();
    let expected_book_snapshot = frames.book_snapshot.clone();
    let expected_trade_acknowledgement = frames.trade_acknowledgement.clone();
    let expected_trade_snapshot = frames.trade_snapshot.clone();
    let (resync_trigger, resync_requested) = oneshot::channel();
    let (resync_sent, resync_observed) = oneshot::channel();
    let (recovery_trigger, recovery_requested) = oneshot::channel();
    let server = tokio::spawn(serve_resynchronizing_kraken_sessions(
        listener,
        frames,
        resync_requested,
        resync_sent,
        recovery_requested,
    ));

    let config = kraken_config(temporary.path())?;
    let source_config = config.kraken().ok_or("Kraken source profile missing")?;
    let profiles =
        super::super::kraken::ProductionKrakenProfileSet::try_from_config(source_config)?;
    let book_coordinates = profiles
        .book()
        .publication_config()
        .native_coordinates()
        .clone();
    let trade_coordinates = profiles
        .trades()
        .publication_config()
        .native_coordinates()
        .clone();
    assert_eq!(
        book_coordinates.provider_identity_key(),
        trade_coordinates.provider_identity_key()
    );
    assert_eq!(
        book_coordinates.provider_identity_revision(),
        trade_coordinates.provider_identity_revision()
    );
    assert_eq!(
        book_coordinates.provider_identity_digest(),
        trade_coordinates.provider_identity_digest()
    );
    assert_eq!(
        book_coordinates
            .provider_identity_key()
            .source_id()
            .as_str(),
        "kraken"
    );
    assert_eq!(
        book_coordinates
            .provider_identity_key()
            .provider_instrument_id()
            .as_str(),
        "BTC/USD"
    );
    assert_eq!(book_coordinates.venue().as_str(), "kraken");
    assert_eq!(book_coordinates.venue_symbol().as_str(), "BTC/USD");
    let book_metadata = profiles.book().metadata().clone();
    let trade_metadata = profiles.trades().metadata().clone();
    assert_eq!(
        book_metadata.capabilities().checksum(),
        ChecksumCapability::Provided
    );
    assert_eq!(
        trade_metadata.capabilities().checksum(),
        ChecksumCapability::Unsupported
    );
    assert_eq!(
        book_metadata
            .coverage()
            .live()
            .ok_or("Kraken book coverage missing")?
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        "book-v2"
    );
    assert_eq!(
        trade_metadata
            .coverage()
            .live()
            .ok_or("Kraken trade coverage missing")?
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        "trade-v2"
    );
    let definition = source_config.definition().clone();
    let invocations = Arc::new(AtomicUsize::new(0));
    let composition = local_kraken_paper_bot_with_strategy_for_test(
        config,
        Decimal::new(100_000, 0),
        100,
        Box::new(InvocationProbeStrategy {
            invocations: Arc::clone(&invocations),
        }),
    )?;
    let calendar = composition.day_session_calendar_for_test();
    assert_eq!(
        calendar.calendar_id().as_str(),
        "kraken-continuous-calendar"
    );
    assert_eq!(calendar.venue_id().as_str(), "kraken");
    assert_eq!(calendar.time_zone(), "UTC");
    assert_eq!(calendar.sessions().len(), 1);
    assert_eq!(
        calendar.sessions()[0].session_id().as_str(),
        "kraken-continuous-session"
    );
    let composition = composition.with_local_kraken_endpoint_for_test(&endpoint)?;
    let cancellation = CancellationToken::new();
    let runtime = composition.start(cancellation.clone()).await?;

    let initial = wait_for_kraken_snapshot(
        runtime.snapshots(),
        book_metadata.source_id(),
        trade_metadata.source_id(),
    )
    .await?;
    assert_eq!(initial.connection_generation, ConnectionGeneration::new(1)?);
    assert_eq!(initial.trade_generation, ConnectionGeneration::new(1)?);
    assert!(runtime.source_is_healthy());

    resync_trigger
        .send(())
        .map_err(|_| "Kraken resynchronization trigger receiver closed")?;
    tokio::time::timeout(LOCAL_SUBSCRIPTION_BOUND, resync_observed)
        .await?
        .map_err(|_| "Kraken resynchronization evidence sender closed")?;
    wait_for_source_health(&runtime, false).await?;
    recovery_trigger
        .send(())
        .map_err(|_| "Kraken recovery trigger receiver closed")?;
    let observed = wait_for_kraken_snapshot(
        runtime.snapshots(),
        book_metadata.source_id(),
        trade_metadata.source_id(),
    )
    .await?;
    wait_for_source_health(&runtime, true).await?;
    assert!(runtime.source_is_healthy());
    assert_eq!(
        book_metadata.quality_ceiling(),
        DataQuality::DirectUnverified
    );
    assert_eq!(
        trade_metadata.quality_ceiling(),
        DataQuality::DirectUnverified
    );
    assert_eq!(observed.source, *book_metadata.source_id());
    assert_eq!(observed.trade_source, *trade_metadata.source_id());
    assert_eq!(observed.instrument, definition.instrument_id());
    assert_eq!(
        observed.connection_generation,
        ConnectionGeneration::new(2)?
    );
    assert!(observed.generation_current);
    assert_eq!(observed.phase, StreamPhaseSnapshot::Healthy);
    assert!(observed.snapshot_initialized);
    assert_eq!(observed.trading_status, Some(TradingStatus::Active));
    assert_eq!(observed.trade_generation, ConnectionGeneration::new(1)?);
    assert!(observed.trade_generation_current);
    assert_eq!(observed.trade_phase, StreamPhaseSnapshot::Healthy);
    assert_eq!(observed.trade_id.as_str(), "1001");
    assert_eq!(observed.trade_aggressor, AggressorSide::Buy);
    assert_eq!(invocations.load(Ordering::SeqCst), 0);

    let rejection = defense_in_depth_risk_probe(&definition.execution_terms(), observed)?;
    assert!(
        matches!(
            rejection.as_ref(),
            [
                RiskRejectionCode::SourceQuality,
                RiskRejectionCode::Account(AccountRiskViolation::UnsupportedSettlement),
            ] | [
                RiskRejectionCode::SourceQuality,
                RiskRejectionCode::SourceStale,
                RiskRejectionCode::Account(AccountRiskViolation::UnsupportedSettlement),
            ]
        ),
        "{rejection:#?}"
    );
    assert_eq!(invocations.load(Ordering::SeqCst), 0);

    cancellation.cancel();
    let shutdown = tokio::time::timeout(Duration::from_secs(30), runtime.shutdown()).await?;
    assert!(shutdown.is_complete(), "{shutdown:#?}");
    let persisted_checkpoint = *shutdown
        .checkpoint()
        .as_ref()
        .map_err(|error| error.to_string())?;
    let recovery_digest = *shutdown
        .recovery_content()
        .as_ref()
        .map_err(|error| error.to_string())?;
    let paper = shutdown
        .paper()
        .as_ref()
        .map_err(|error| error.to_string())?;
    assert_eq!(persisted_checkpoint.generation().get(), 2);
    assert_eq!(persisted_checkpoint.sequence(), paper.sequence());
    assert_eq!(persisted_checkpoint.recovery_digest(), recovery_digest);
    assert!(paper.orders().is_empty());
    assert!(paper.fills().is_empty());
    assert!(paper.positions().is_empty());
    assert_eq!(paper.cash().len(), 1);
    assert_eq!(
        paper.cash()[0].balance(),
        Money::new(Decimal::new(100_000, 0), Currency::try_from("USD")?)
    );
    let audit = shutdown
        .audit()
        .evidence()
        .ok_or("audit drain incomplete")?;
    assert_eq!(audit.execution_records(), 0);
    assert_eq!(audit.paper_records(), 0);

    server.await??;
    let paths = LocalPaths::prepare(temporary.path())?;
    let records = JournalReader::open(paths.journal_write_file("kraken-public-book-v2")?)?
        .read_all_bounded(5, 4 * 1024 * 1024)?;
    assert_eq!(records.len(), 5);
    assert_eq!(
        records[0].payload(),
        expected_book_acknowledgement.as_bytes()
    );
    assert_eq!(records[1].payload(), expected_book_snapshot.as_bytes());
    assert_eq!(records[2].payload(), UPDATE_BEFORE_SNAPSHOT.as_bytes());
    assert_eq!(
        records[3].payload(),
        expected_book_acknowledgement.as_bytes()
    );
    assert_eq!(records[4].payload(), expected_book_snapshot.as_bytes());
    let trade_records = JournalReader::open(paths.journal_write_file("kraken-public-trades-v2")?)?
        .read_all_bounded(2, 4 * 1024 * 1024)?;
    assert_eq!(trade_records.len(), 2);
    assert_eq!(
        trade_records[0].payload(),
        expected_trade_acknowledgement.as_bytes()
    );
    assert_eq!(
        trade_records[1].payload(),
        expected_trade_snapshot.as_bytes()
    );

    let restart_listener = TcpListener::bind("127.0.0.1:0").await?;
    let restart_endpoint = format!("ws://{}/", restart_listener.local_addr()?);
    let restart_server = tokio::spawn(serve_one_kraken_session(
        restart_listener,
        current_kraken_frames()?,
    ));
    let restart_composition = local_kraken_paper_bot_with_strategy_for_test(
        kraken_config(temporary.path())?,
        Decimal::new(100_000, 0),
        100,
        Box::new(InvocationProbeStrategy {
            invocations: Arc::new(AtomicUsize::new(0)),
        }),
    )?
    .with_local_kraken_endpoint_for_test(&restart_endpoint)?;
    let restart_cancellation = CancellationToken::new();
    let restarted = restart_composition
        .start(restart_cancellation.clone())
        .await?;
    let restart_observed = wait_for_kraken_snapshot(
        restarted.snapshots(),
        book_metadata.source_id(),
        trade_metadata.source_id(),
    )
    .await?;
    assert_eq!(
        restart_observed.connection_generation,
        ConnectionGeneration::new(3)?
    );
    assert_eq!(
        restart_observed.trade_generation,
        ConnectionGeneration::new(2)?
    );
    assert!(restarted.financial_reconciliation_current());
    restart_cancellation.cancel();
    let restart_shutdown =
        tokio::time::timeout(Duration::from_secs(30), restarted.shutdown()).await?;
    assert!(restart_shutdown.is_complete(), "{restart_shutdown:#?}");
    assert_eq!(
        restart_shutdown
            .audit()
            .evidence()
            .ok_or("restart audit drain incomplete")?
            .paper_records(),
        1
    );
    restart_server.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn silent_peer_is_replaced_at_the_ack_deadline_before_transport_idle() -> TestResult {
    // The fixture's transport-idle limit is 30 seconds; this leaves scheduling headroom while
    // still proving that acknowledgement expiry, rather than transport idleness, rotates it.
    const ROTATION_BOUND: Duration = Duration::from_secs(10);

    let _budget_guard = KRAKEN_VERTICAL_TEST_LOCK.lock().await;
    let temporary = tempfile::tempdir()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("ws://{}/", listener.local_addr()?);
    let server = tokio::spawn(observe_silent_generation_rotation(
        listener,
        current_kraken_frames()?,
    ));
    let config = kraken_config_with_ack_timeout(temporary.path(), 100)?;
    let invocations = Arc::new(AtomicUsize::new(0));
    let composition = local_kraken_paper_bot_with_strategy_for_test(
        config,
        Decimal::new(100_000, 0),
        100,
        Box::new(InvocationProbeStrategy {
            invocations: Arc::clone(&invocations),
        }),
    )?
    .with_local_kraken_endpoint_for_test(&endpoint)?;
    let cancellation = CancellationToken::new();
    let runtime =
        match tokio::time::timeout(ROTATION_BOUND, composition.start(cancellation.clone())).await {
            Ok(result) => result?,
            Err(_elapsed) => {
                cancellation.cancel();
                server.abort();
                let _aborted = server.await;
                return Err(
                    "silent Kraken generation outlived its acknowledgement deadline".into(),
                );
            }
        };
    assert!(runtime.source_is_healthy());
    cancellation.cancel();
    let shutdown = tokio::time::timeout(Duration::from_secs(30), runtime.shutdown()).await?;
    assert!(shutdown.is_complete(), "{shutdown:#?}");
    assert_eq!(invocations.load(Ordering::SeqCst), 0);
    server.await??;
    Ok(())
}

#[derive(Debug)]
struct InvocationProbeStrategy {
    invocations: Arc<AtomicUsize>,
}

impl Strategy for InvocationProbeStrategy {
    fn on_market_event(
        &mut self,
        _context: &StrategyContext<'_>,
        _event: &market_squawk_domain::MarketEvent,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        self.invocations.fetch_add(1, Ordering::SeqCst);
        Ok(BoundedOrderIntents::new())
    }

    fn retained_bytes(&self) -> Result<usize, StrategyError> {
        Ok(size_of::<Self>())
    }
}

#[derive(Clone, Debug)]
struct ObservedKrakenSession {
    source: SourceId,
    trade_source: SourceId,
    instrument: InstrumentId,
    connection_generation: ConnectionGeneration,
    generation_current: bool,
    phase: StreamPhaseSnapshot,
    snapshot_initialized: bool,
    trading_status: Option<TradingStatus>,
    observed_at: Timestamp,
    valid_until: Timestamp,
    best_ask: PriceTicks,
    trade_generation: ConnectionGeneration,
    trade_generation_current: bool,
    trade_phase: StreamPhaseSnapshot,
    trade_id: SourceIdentifier,
    trade_aggressor: AggressorSide,
}

async fn wait_for_kraken_snapshot(
    snapshots: market_squawk_live::LiveSnapshotReader,
    book_source: &SourceId,
    trade_source: &SourceId,
) -> TestResult<ObservedKrakenSession> {
    // The vertical's acknowledgement and freshness windows are deliberately noncompeting here;
    // the separate silent-peer case below owns acknowledgement-expiry behavior.
    tokio::time::timeout(LOCAL_SNAPSHOT_BOUND, async {
        loop {
            if let Ok(lease) = snapshots.try_load_all() {
                for shard in lease.snapshots() {
                    for route in shard.routes() {
                        let mut book = None;
                        let mut trade = None;
                        for stream in route.streams() {
                            if stream.source() == book_source
                                && stream.phase() == StreamPhaseSnapshot::Healthy
                                && stream.generation_current()
                                && stream.snapshot_initialized()
                            {
                                let best_ask = stream
                                    .asks()
                                    .first()
                                    .ok_or("Kraken snapshot contains no ask")?
                                    .price();
                                book = Some((
                                    stream.source().clone(),
                                    stream.instrument(),
                                    stream.connection_generation(),
                                    stream.generation_current(),
                                    stream.phase(),
                                    stream.snapshot_initialized(),
                                    stream.trading_status(),
                                    stream.received_at(),
                                    stream.source_valid_until(),
                                    best_ask,
                                ));
                            } else if stream.source() == trade_source
                                && stream.phase() == StreamPhaseSnapshot::Healthy
                                && stream.generation_current()
                                && let Some(last_trade) = stream.last_trade()
                            {
                                trade = Some((
                                    stream.source().clone(),
                                    stream.connection_generation(),
                                    stream.generation_current(),
                                    stream.phase(),
                                    last_trade.stable_trade_id().clone(),
                                    last_trade.aggressor_side(),
                                ));
                            }
                        }
                        if let (Some(book), Some(trade)) = (book, trade) {
                            return TestResult::Ok(ObservedKrakenSession {
                                source: book.0,
                                trade_source: trade.0,
                                instrument: book.1,
                                connection_generation: book.2,
                                generation_current: book.3,
                                phase: book.4,
                                snapshot_initialized: book.5,
                                trading_status: book.6,
                                observed_at: book.7,
                                valid_until: book.8,
                                best_ask: book.9,
                                trade_generation: trade.1,
                                trade_generation_current: trade.2,
                                trade_phase: trade.3,
                                trade_id: trade.4,
                                trade_aggressor: trade.5,
                            });
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

async fn wait_for_source_health(
    runtime: &crate::paper_bot::ProductionPaperBotRuntime,
    expected: bool,
) -> TestResult {
    tokio::time::timeout(LOCAL_SNAPSHOT_BOUND, async {
        loop {
            if runtime.source_is_healthy() == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .map_err(|_elapsed| format!("Kraken composite source health did not become {expected}").into())
}

fn defense_in_depth_risk_probe(
    terms: &InstrumentExecutionTerms,
    observed: ObservedKrakenSession,
) -> TestResult<Box<[RiskRejectionCode]>> {
    let currency = terms.quote_currency();
    let account_id = AccountId::from_str(PAPER_ACCOUNT_ID)?;
    let capital = Money::new(Decimal::new(100_000, 0), currency);
    let zero = Money::new(Decimal::ZERO, currency);
    let accounts = Arc::new(AccountRiskCoordinator::try_new(
        AccountCoordinatorConfig::default(),
        [AccountBootstrap {
            account_id,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: capital,
            capital,
            peak_capital: capital,
            gross_exposure: zero,
            realized_pnl: zero,
            realized_loss: zero,
            positions: Vec::new(),
            position_cost_basis: Vec::new(),
            idempotency: AccountIdempotencyBootstrap::empty(),
        }],
    )?);
    let limits = RiskLimits::try_new(RiskLimitsInput {
        currency,
        eligible_instruments: BTreeSet::from([terms.instrument_id()]),
        maximum_position_lots: 1_000_000,
        maximum_order_notional: capital,
        maximum_gross_exposure: capital,
        maximum_leverage: BasisPoints::new(10_000),
        minimum_capital: Money::new(Decimal::ONE, currency),
        maximum_loss: capital,
        maximum_drawdown: capital,
        maximum_fee: BasisPoints::new(100),
        maximum_price_deviation: BasisPoints::new(1_000),
        maximum_slippage: BasisPoints::new(1_000),
        maximum_orders_per_window: NonZeroU32::MIN,
        order_rate_window_nanos: 60_000_000_000,
        reservation_ttl_nanos: 5_000_000_000,
        allow_short: false,
        kill_switch: false,
    })?;
    let (audit, _audit_reader) = ExecutionAuditWriter::try_new(ExecutionAuditConfig {
        maximum_records: NonZeroUsize::MIN,
        maximum_bytes: NonZeroU32::new(64 * 1024).ok_or("zero audit bound")?,
    })?;
    let risk = RiskService::try_new(
        accounts,
        local_paper_portfolio_capability_for_test(account_id, capital, 1)?,
        limits,
        audit,
        RiskServiceConfig {
            policy: RiskPolicyIdentity::new(
                &SourceIdentifier::try_from("kraken-defense-in-depth-risk")?,
                RuleVersion::new(1)?,
            ),
            policy_valid_until: Timestamp::from_unix_nanos(i64::MAX),
            maximum_approval_lifetime: Duration::from_secs(1),
        },
    )?;
    let expires_at = observed.observed_at.checked_add_nanos(20_000_000_000)?;
    let intent = OrderIntent::try_new(OrderIntentInput {
        order_id: OrderId::from_str("20000000-0000-0000-0000-000000000021")?,
        client_order_id: ClientOrderId::try_from("kraken-app-risk-probe")?,
        strategy_id: StrategyId::from_str("30000000-0000-0000-0000-000000000021")?,
        model_id: None,
        account_id,
        execution_terms: *terms,
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        quantity: QuantityLots::new(1)?,
        limit_price: None,
        stop_price: None,
        time_in_force: TimeInForce::ImmediateOrCancel,
        signal_at: observed.observed_at,
        expires_at,
        reason_codes: vec![OrderReasonCode::try_from("kraken.defense-in-depth")?],
        maximum_slippage: BasisPoints::new(100),
        required_quality: DataQuality::DirectVerified,
    })?;
    let market = MarketRiskInput::try_new(
        *terms,
        DataQuality::DirectUnverified,
        observed.generation_current && observed.phase == StreamPhaseSnapshot::Healthy,
        observed.trading_status == Some(TradingStatus::Active),
        observed.observed_at,
        observed.valid_until,
        observed.best_ask,
        observed.best_ask,
    )?;
    match risk.evaluate_pre_authority(&intent, &market) {
        PreAuthorityRiskOutcome::Rejected(rejection) => Ok(rejection.reasons().into()),
        PreAuthorityRiskOutcome::Reserved(_reservation) => {
            Err("DirectUnverified Kraken evidence unexpectedly reserved account capacity".into())
        }
    }
}

async fn serve_resynchronizing_kraken_sessions(
    listener: TcpListener,
    frames: KrakenTestFrames,
    resync_requested: oneshot::Receiver<()>,
    resync_sent: oneshot::Sender<()>,
    recovery_requested: oneshot::Receiver<()>,
) -> TestResult {
    let mut workers = tokio::task::JoinSet::new();
    let mut book_seen = false;
    let mut trades_seen = false;
    let mut resync_requested = Some(resync_requested);
    let mut resync_sent = Some(resync_sent);
    while !book_seen || !trades_seen {
        let (socket, channel) = accept_kraken_subscription(&listener).await?;
        match channel {
            KrakenTestChannel::Book if !book_seen => {
                book_seen = true;
                let _worker = workers.spawn(serve_book_until_resynchronization(
                    socket,
                    frames.book_acknowledgement.clone(),
                    frames.book_snapshot.clone(),
                    resync_requested
                        .take()
                        .ok_or("Kraken resynchronization trigger was already consumed")?,
                    resync_sent
                        .take()
                        .ok_or("Kraken resynchronization sender was already consumed")?,
                ));
            }
            KrakenTestChannel::Trades if !trades_seen => {
                trades_seen = true;
                let _worker = workers.spawn(serve_kraken_frames_until_close(
                    socket,
                    vec![
                        frames.trade_acknowledgement.clone(),
                        frames.trade_snapshot.clone(),
                    ],
                ));
            }
            _ => return Err("Kraken source opened an unexpected duplicate channel".into()),
        }
    }
    let (replacement, channel) = accept_kraken_subscription(&listener).await?;
    if channel != KrakenTestChannel::Book {
        return Err("Kraken source rotated the healthy trade channel during book resync".into());
    }
    let _replacement = workers.spawn(serve_kraken_frames_after_barrier(
        replacement,
        recovery_requested,
        vec![frames.book_acknowledgement, frames.book_snapshot],
    ));
    while let Some(result) = workers.join_next().await {
        result??;
    }
    Ok(())
}

async fn serve_book_until_resynchronization(
    mut socket: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    acknowledgement: String,
    snapshot: String,
    mut resync_requested: oneshot::Receiver<()>,
    resync_sent: oneshot::Sender<()>,
) -> TestResult {
    socket.send(Message::Text(acknowledgement.into())).await?;
    socket.send(Message::Text(snapshot.into())).await?;
    socket
        .send(Message::Ping(FIRST_GENERATION_READY.into()))
        .await?;
    tokio::time::timeout(LOCAL_SUBSCRIPTION_BOUND, async {
        let mut snapshot_observed = false;
        loop {
            tokio::select! {
                biased;
                requested = &mut resync_requested, if snapshot_observed => {
                    requested.map_err(|_| "Kraken resynchronization trigger sender closed")?;
                    return TestResult::Ok(());
                }
                message = socket.next() => match message {
                    Some(Ok(Message::Pong(payload)))
                        if payload.as_ref() == FIRST_GENERATION_READY =>
                    {
                        snapshot_observed = true;
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        socket.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        return Err(
                            "Kraken source closed before the controlled resynchronization".into(),
                        );
                    }
                    Some(Ok(_)) => {}
                }
            }
        }
    })
    .await??;
    socket
        .send(Message::Text(UPDATE_BEFORE_SNAPSHOT.into()))
        .await?;
    resync_sent
        .send(())
        .map_err(|_| "Kraken resynchronization evidence receiver closed")?;
    while let Some(message) = socket.next().await {
        match message {
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(Message::Ping(payload)) => socket.send(Message::Pong(payload)).await?,
            Ok(_) => {}
        }
    }
    Ok(())
}

async fn serve_kraken_frames_after_barrier(
    mut socket: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    mut barrier: oneshot::Receiver<()>,
    frames: Vec<String>,
) -> TestResult {
    tokio::time::timeout(LOCAL_SUBSCRIPTION_BOUND, async {
        loop {
            tokio::select! {
                biased;
                released = &mut barrier => {
                    released.map_err(|_| "Kraken recovery trigger sender closed")?;
                    return TestResult::Ok(());
                }
                message = socket.next() => match message {
                    Some(Ok(Message::Ping(payload))) => {
                        socket.send(Message::Pong(payload)).await?;
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                        return Err("Kraken replacement closed before recovery release".into());
                    }
                    Some(Ok(_)) => {}
                }
            }
        }
    })
    .await??;
    serve_kraken_frames_until_close(socket, frames).await
}

async fn serve_one_kraken_session(listener: TcpListener, frames: KrakenTestFrames) -> TestResult {
    let mut workers = tokio::task::JoinSet::new();
    let mut book_seen = false;
    let mut trades_seen = false;
    while !book_seen || !trades_seen {
        let (socket, channel) = accept_kraken_subscription(&listener).await?;
        let channel_frames = match channel {
            KrakenTestChannel::Book if !book_seen => {
                book_seen = true;
                vec![
                    frames.book_acknowledgement.clone(),
                    frames.book_snapshot.clone(),
                ]
            }
            KrakenTestChannel::Trades if !trades_seen => {
                trades_seen = true;
                vec![
                    frames.trade_acknowledgement.clone(),
                    frames.trade_snapshot.clone(),
                ]
            }
            _ => return Err("Kraken source opened an unexpected duplicate channel".into()),
        };
        let _worker = workers.spawn(serve_kraken_frames_until_close(socket, channel_frames));
    }
    while let Some(result) = workers.join_next().await {
        result??;
    }
    Ok(())
}

async fn serve_kraken_frames_until_close(
    mut socket: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    frames: Vec<String>,
) -> TestResult {
    for frame in frames {
        socket.send(Message::Text(frame.into())).await?;
    }
    while let Some(message) = socket.next().await {
        match message {
            Ok(Message::Close(_)) | Err(_) => return Ok(()),
            Ok(Message::Ping(payload)) => socket.send(Message::Pong(payload)).await?,
            Ok(_) => {}
        }
    }
    Ok(())
}

async fn observe_silent_generation_rotation(
    listener: TcpListener,
    frames: KrakenTestFrames,
) -> TestResult {
    let mut workers = tokio::task::JoinSet::new();
    let mut book_connections = 0_usize;
    let mut trades_seen = false;
    while book_connections < 2 || !trades_seen {
        let (socket, channel) = accept_kraken_subscription(&listener).await?;
        match channel {
            KrakenTestChannel::Book if book_connections == 0 => {
                book_connections += 1;
                let _worker = workers.spawn(serve_kraken_frames_until_close(socket, Vec::new()));
            }
            KrakenTestChannel::Book if book_connections == 1 => {
                book_connections += 1;
                let _worker = workers.spawn(serve_kraken_frames_until_close(
                    socket,
                    vec![
                        frames.book_acknowledgement.clone(),
                        frames.book_snapshot.clone(),
                    ],
                ));
            }
            KrakenTestChannel::Trades if !trades_seen => {
                trades_seen = true;
                let _worker = workers.spawn(serve_kraken_frames_until_close(
                    socket,
                    vec![
                        frames.trade_acknowledgement.clone(),
                        frames.trade_snapshot.clone(),
                    ],
                ));
            }
            _ => return Err("Kraken source opened an unexpected duplicate channel".into()),
        }
    }
    while let Some(result) = workers.join_next().await {
        result??;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KrakenTestChannel {
    Book,
    Trades,
}

async fn accept_kraken_subscription(
    listener: &TcpListener,
) -> TestResult<(
    tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    KrakenTestChannel,
)> {
    let (stream, _) = listener.accept().await?;
    let mut socket = tokio_tungstenite::accept_async(stream).await?;
    let Some(Ok(Message::Text(subscription))) =
        tokio::time::timeout(LOCAL_SUBSCRIPTION_BOUND, socket.next()).await?
    else {
        return Err("Kraken source did not send a text subscription".into());
    };
    let request: serde_json::Value = serde_json::from_str(&subscription)?;
    let params = request["params"]
        .as_object()
        .ok_or("Kraken subscription params are not an object")?;
    let channel = match params.get("channel").and_then(serde_json::Value::as_str) {
        Some("book") if params.get("depth") == Some(&serde_json::json!(10)) => {
            KrakenTestChannel::Book
        }
        Some("trade") if !params.contains_key("depth") => KrakenTestChannel::Trades,
        _ => return Err("Kraken source sent an unsupported channel subscription".into()),
    };
    if request["method"] != "subscribe"
        || request["req_id"] != serde_json::json!(1)
        || params.get("snapshot") != Some(&serde_json::Value::Bool(true))
        || params.get("symbol") != Some(&serde_json::json!(["BTC/USD"]))
    {
        return Err("Kraken source sent a mismatched production subscription".into());
    }
    Ok((socket, channel))
}

#[derive(Clone, Debug)]
struct KrakenTestFrames {
    book_acknowledgement: String,
    book_snapshot: String,
    trade_acknowledgement: String,
    trade_snapshot: String,
}

fn current_kraken_frames() -> TestResult<KrakenTestFrames> {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true);
    let book_acknowledgement = serde_json::json!({
        "method": "subscribe",
        "result": {
            "channel": "book",
            "depth": 10,
            "snapshot": true,
            "symbol": "BTC/USD",
            "warnings": []
        },
        "success": true,
        "time_in": now.clone(),
        "time_out": now.clone(),
        "req_id": 1
    })
    .to_string();
    let mut snapshot: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../adapters/market-squawk-adapter-kraken/fixtures/official_book_checksum.json"
    ))?;
    snapshot["data"][0]["timestamp"] = serde_json::Value::String(now.clone());
    let trade_acknowledgement = serde_json::json!({
        "method": "subscribe",
        "result": {
            "channel": "trade",
            "snapshot": true,
            "symbol": "BTC/USD",
            "warnings": []
        },
        "success": true,
        "time_in": now.clone(),
        "time_out": now.clone(),
        "req_id": 1
    })
    .to_string();
    let trade_snapshot = serde_json::json!({
        "channel": "trade",
        "type": "snapshot",
        "data": [{
            "symbol": "BTC/USD",
            "side": "buy",
            "price": "45283.5",
            "qty": "0.001",
            "ord_type": "market",
            "trade_id": 1001,
            "timestamp": now
        }]
    })
    .to_string();
    Ok(KrakenTestFrames {
        book_acknowledgement,
        book_snapshot: snapshot.to_string(),
        trade_acknowledgement,
        trade_snapshot,
    })
}

fn kraken_config(data_dir: &std::path::Path) -> TestResult<AppConfig> {
    // Acknowledgement expiry is covered independently below. This vertical keeps that deadline
    // noncompeting so a saturated cross-platform test runner cannot select the wrong scenario.
    kraken_config_with_ack_timeout(data_dir, 60_000)
}

fn kraken_config_with_ack_timeout(
    data_dir: &std::path::Path,
    acknowledgement_timeout_ms: u64,
) -> TestResult<AppConfig> {
    let json = format!(
        r#"{{
          "endpoint":"wss://ws.kraken.com/v2",
          "channels":["book","trade"],
          "depth":10,
          "freshness_ms":120000,
          "max_frame_bytes":1048576,
          "subscription_ack_timeout_ms":{acknowledgement_timeout_ms},
          "control_message_capacity":64,
          "control_byte_capacity":65536,
          "authorization":{{
            "mode":"public_interface",
            "provider":"kraken",
            "basis":"user-reviewed-kraken-public-interface",
            "evidence_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "evidence_reference":"https://github.com/Sawmonabo/market-squawk/blob/main/docs/research/2026-07-16-kraken-websocket-v2-checksum.md",
            "evidence_version":"reviewed-2026-08-14",
            "effective_from_unix_nanos":1700000000000000000,
            "effective_until_unix_nanos":1900000000000000000
          }},
          "instrument":{{
            "symbol":"BTC/USD",
            "instrument_id":"{INSTRUMENT_ID}",
            "definition_revision":1,
            "asset_class":"crypto",
            "primary_asset":"b9f6d14f-9140-4ca3-a412-9bd59b3b5e67",
            "quote_currency":"USD",
            "tick_size":"0.1",
            "lot_size":"0.00000001",
            "contract_multiplier":"1",
            "venue":"kraken",
            "trading_status":"active"
          }}
        }}"#
    );
    let environment = BTreeMap::from([(
        OsString::from("MARKET_SQUAWK_KRAKEN_JSON"),
        OsString::from(json),
    )]);
    Ok(AppConfig::load(ConfigSources::new(
        None,
        &environment,
        ConfigOverrides {
            data_dir: Some(data_dir.to_path_buf()),
            ..ConfigOverrides::default()
        },
    ))?)
}
