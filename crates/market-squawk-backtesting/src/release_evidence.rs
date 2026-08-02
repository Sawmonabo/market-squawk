//! Bounded release-evidence backtest over production point-in-time and accounting kernels.

use market_squawk_data::{DatasetId, DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest};
use market_squawk_domain::{
    AccountId, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize, Money,
    OrderReasonCode, OrderSide, OrderType, PriceTicks, QuantityLots, SourceIdentifier, TickSize,
    TimeInForce, Timestamp,
};
use market_squawk_execution::{BoundedOrderIntents, OrderIntent, OrderIntentInput, StrategyError};
use market_squawk_portfolio::{PortfolioLimitInput, PortfolioLimits};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::dataset::{BacktestDatasetInput, BacktestObservationInput};
use crate::{
    AccountingReconciliation, BacktestDataset, BacktestEngine, BacktestError, BacktestLimits,
    BacktestLimitsInput, BacktestObservation, BacktestRequest, BacktestStrategy,
    HistoricalUniverseStatus, PortfolioSeed, RESEARCH_EXECUTION_POLICY_VERSION,
    ResearchExecutionAssumptions, ResearchExecutionAssumptionsInput, ResearchLiquidityPriority,
};

/// Deterministic result proving research execution, partial fills, and independent accounting.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseEvidenceBacktestResult {
    dataset_identity_sha256: [u8; 32],
    object_graph_sha256: [u8; 32],
    result_sha256: [u8; 32],
    fill_count: usize,
    filled_lots: i64,
    partial_fill_count: usize,
    execution_policy_version: u32,
    fee_basis_points: i32,
    slippage_basis_points: i32,
    latency_nanos: u64,
    partial_fills_enabled: bool,
    fee_amount: String,
    fee_currency: String,
    accounting_reconciliation: String,
}

/// Release-evidence fixture or deterministic backtest failure.
#[derive(Debug, Error)]
pub enum ReleaseEvidenceBacktestError {
    /// The immutable production fixture could not be admitted.
    #[error("release-evidence backtest fixture is invalid")]
    InvalidFixture,
    /// The production backtest or portfolio reconciliation failed.
    #[error("release-evidence backtest execution failed: {0}")]
    Backtest(#[from] BacktestError),
}

/// Runs one bounded production backtest with realistic research execution assumptions.
///
/// # Errors
///
/// Returns a typed error if the immutable fixture cannot be admitted, the strategy/fill path
/// fails, or independent portfolio reconciliation does not agree exactly.
pub fn run_release_evidence_backtest()
-> Result<ReleaseEvidenceBacktestResult, ReleaseEvidenceBacktestError> {
    let account_id = parse_account("00000000-0000-0000-0000-000000000030")?;
    let terms = execution_terms()?;
    let dataset = dataset(terms)?;
    let dataset_identity = dataset.identity();
    let object_graph = dataset.object_graph_digest();
    let request = BacktestRequest::try_new(
        dataset,
        assumptions()?,
        PortfolioSeed::try_new(
            account_id,
            Money::new(Decimal::new(100_000, 2), currency("USD")?),
            portfolio_limits()?,
        )?,
        None,
        vec![source_identifier("release-feature-labels")?],
        7,
        BacktestLimits::try_new(BacktestLimitsInput {
            max_observations: 100,
            max_pending_intents: 16,
            max_fills: 16,
            max_retained_bytes: 1_000_000,
        })?,
    )?;
    let mut strategy = BuyOnce {
        account_id,
        emitted: false,
    };
    let result = BacktestEngine::run(&request, &mut strategy, &CancellationToken::new())?;
    let filled_lots = result.fills().iter().try_fold(0_i64, |total, fill| {
        total.checked_add(fill.quantity().get())
    });
    let partial_fill_count = result.fills().iter().filter(|fill| fill.partial()).count();
    let fees = result
        .fills()
        .iter()
        .map(|fill| fill.fee().amount())
        .sum::<Decimal>();
    if result.fills().len() != 2
        || filled_lots != Some(4)
        || partial_fill_count != 2
        || result.accounting_reconciliation() != AccountingReconciliation::Independent
    {
        return Err(ReleaseEvidenceBacktestError::InvalidFixture);
    }
    Ok(ReleaseEvidenceBacktestResult {
        dataset_identity_sha256: dataset_identity.bytes(),
        object_graph_sha256: object_graph.bytes(),
        result_sha256: result.result_digest().bytes(),
        fill_count: result.fills().len(),
        filled_lots: filled_lots.ok_or(ReleaseEvidenceBacktestError::InvalidFixture)?,
        partial_fill_count,
        execution_policy_version: RESEARCH_EXECUTION_POLICY_VERSION,
        fee_basis_points: 10,
        slippage_basis_points: 5,
        latency_nanos: 1,
        partial_fills_enabled: true,
        fee_amount: fees.normalize().to_string(),
        fee_currency: "USD".to_owned(),
        accounting_reconciliation: "independent".to_owned(),
    })
}

#[derive(Debug)]
struct BuyOnce {
    account_id: AccountId,
    emitted: bool,
}

impl BacktestStrategy for BuyOnce {
    fn on_observation(
        &mut self,
        context: &crate::BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError> {
        let mut output = BoundedOrderIntents::new();
        if self.emitted {
            return Ok(output);
        }
        output.try_push(
            OrderIntent::try_new(OrderIntentInput {
                order_id: "00000000-0000-0000-0000-000000000040"
                    .parse()
                    .map_err(|_| StrategyError::Evaluation)?,
                client_order_id: ClientOrderId::try_from("release-backtest-buy")
                    .map_err(|_| StrategyError::Evaluation)?,
                strategy_id: "00000000-0000-0000-0000-000000000041"
                    .parse()
                    .map_err(|_| StrategyError::Evaluation)?,
                model_id: None,
                account_id: self.account_id,
                execution_terms: context.execution_terms(),
                side: OrderSide::Buy,
                order_type: OrderType::Market,
                quantity: QuantityLots::new(4).map_err(|_| StrategyError::Evaluation)?,
                limit_price: None,
                stop_price: None,
                time_in_force: TimeInForce::Day,
                signal_at: context.decision_at(),
                expires_at: context
                    .decision_at()
                    .checked_add_nanos(100)
                    .map_err(|_| StrategyError::Evaluation)?,
                reason_codes: vec![
                    OrderReasonCode::try_from("research-signal")
                        .map_err(|_| StrategyError::Evaluation)?,
                ],
                maximum_slippage: BasisPoints::new(100),
                required_quality: DataQuality::DirectVerified,
            })
            .map_err(|_| StrategyError::Evaluation)?,
        )?;
        self.emitted = true;
        Ok(output)
    }
}

fn dataset(
    terms: InstrumentExecutionTerms,
) -> Result<BacktestDataset, ReleaseEvidenceBacktestError> {
    let manifest = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("release-backtest-features").map_err(|_| invalid())?,
        1,
        DatasetSchemaRegistry::local()
            .canonical_feature_labels()
            .map_err(|_| invalid())?,
        Sha256Digest::new([1; 32]),
    )
    .map_err(|_| invalid())?;
    let observations = [(10, 100, 10), (20, 110, 2), (30, 120, 10)]
        .into_iter()
        .map(|(at, price, depth)| observation(terms, at, price, depth))
        .collect::<Result<Vec<_>, _>>()?;
    BacktestDataset::try_new(BacktestDatasetInput {
        manifest,
        object_graph_digest: Sha256Digest::new([2; 32]),
        point_in_time_content: Sha256Digest::new([3; 32]),
        point_in_time_audit: Sha256Digest::new([4; 32]),
        instrument_definition_content: Sha256Digest::new([5; 32]),
        instrument_definition_audit: Sha256Digest::new([6; 32]),
        observations,
    })
    .map_err(ReleaseEvidenceBacktestError::from)
}

fn observation(
    terms: InstrumentExecutionTerms,
    at: i64,
    price: i64,
    depth: i64,
) -> Result<BacktestObservation, ReleaseEvidenceBacktestError> {
    BacktestObservation::try_new(BacktestObservationInput {
        execution_terms: terms,
        event_at: Timestamp::from_unix_nanos(at - 2),
        available_at: Timestamp::from_unix_nanos(at - 1),
        decision_at: Timestamp::from_unix_nanos(at),
        stale_at: Timestamp::from_unix_nanos(at + 5),
        mid_price: Some(PriceTicks::new(price)),
        spread_basis_points: BasisPoints::new(20),
        executable_depth: QuantityLots::new(depth).map_err(|_| invalid())?,
        universe: HistoricalUniverseStatus::Eligible,
        features: Vec::new(),
        lineage_digest: Sha256Digest::new([u8::try_from(at).map_err(|_| invalid())?; 32]),
    })
    .map_err(ReleaseEvidenceBacktestError::from)
}

fn execution_terms() -> Result<InstrumentExecutionTerms, ReleaseEvidenceBacktestError> {
    InstrumentExecutionTerms::try_new(
        parse_instrument("00000000-0000-0000-0000-000000000020")?,
        InstrumentDefinitionRevision::try_from(1).map_err(|_| invalid())?,
        TickSize::try_from_decimal(Decimal::ONE).map_err(|_| invalid())?,
        LotSize::try_from_decimal(Decimal::ONE).map_err(|_| invalid())?,
        currency("USD")?,
        Denomination::Currency(currency("USD")?),
        Decimal::ONE,
    )
    .map_err(|_| invalid())
}

fn assumptions() -> Result<ResearchExecutionAssumptions, ReleaseEvidenceBacktestError> {
    ResearchExecutionAssumptions::try_new(ResearchExecutionAssumptionsInput {
        version: RESEARCH_EXECUTION_POLICY_VERSION,
        fee_basis_points: BasisPoints::new(10),
        slippage_basis_points: BasisPoints::new(5),
        maximum_random_slippage_basis_points: BasisPoints::new(0),
        maximum_participation_basis_points: BasisPoints::new(10_000),
        liquidity_priority: ResearchLiquidityPriority::SignalTimeThenOrderId,
        latency_nanos: 1,
        allow_partial_fills: true,
        fee_decimal_scale: 4,
    })
    .map_err(|_| invalid())
}

fn portfolio_limits() -> Result<PortfolioLimits, ReleaseEvidenceBacktestError> {
    PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: 16,
        max_lots: 64,
        max_transactions: 64,
        max_factors: 16,
        max_scenarios: 16,
        max_history: 16,
        max_results: 128,
        max_retained_bytes: 1_000_000,
    })
    .map_err(|_| invalid())
}

fn parse_account(value: &str) -> Result<AccountId, ReleaseEvidenceBacktestError> {
    value.parse().map_err(|_| invalid())
}

fn parse_instrument(value: &str) -> Result<InstrumentId, ReleaseEvidenceBacktestError> {
    value.parse().map_err(|_| invalid())
}

fn currency(value: &str) -> Result<Currency, ReleaseEvidenceBacktestError> {
    Currency::try_from(value).map_err(|_| invalid())
}

fn source_identifier(value: &str) -> Result<SourceIdentifier, ReleaseEvidenceBacktestError> {
    SourceIdentifier::try_from(value).map_err(|_| invalid())
}

const fn invalid() -> ReleaseEvidenceBacktestError {
    ReleaseEvidenceBacktestError::InvalidFixture
}
