//! Manifest-pinned point-in-time orchestration and portfolio reconciliation.

use market_squawk_data::{CorporateActionPlan, Sha256Digest};
use std::collections::BTreeMap;

use market_squawk_domain::{
    AccountId, InstrumentExecutionTerms, InstrumentId, Money, QuantityLots, SourceIdentifier,
    Timestamp,
};
use market_squawk_execution::{OrderIntent, StrategyError};
use market_squawk_portfolio::{PortfolioError, PortfolioLimits, PortfolioRevision};
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::clock::{EventTimeClock, EventTimeClockError};
use crate::dataset::{
    BacktestDataset, BacktestLimits, BacktestObservation, HistoricalUniverseStatus,
    ResearchFeatureValue,
};
use crate::fills::{
    ResearchExecutionAssumptions, ResearchFill, ResearchFillError, ResearchFillSimulator,
};
use crate::strategy::BacktestStrategy;

mod accounting;

use accounting::{ShadowPortfolio, reconcile};

/// Exact initial account state supplied to Task 16 reconciliation.
#[derive(Clone, Copy, Debug)]
pub struct PortfolioSeed {
    pub(crate) account_id: AccountId,
    pub(crate) initial_cash: Money,
    pub(crate) limits: PortfolioLimits,
}

impl PortfolioSeed {
    /// Constructs positive initial capital in the portfolio base currency.
    pub fn try_new(
        account_id: AccountId,
        initial_cash: Money,
        limits: PortfolioLimits,
    ) -> Result<Self, BacktestError> {
        if initial_cash.amount() <= Decimal::ZERO {
            return Err(BacktestError::InvalidPortfolioSeed);
        }
        Ok(Self {
            account_id,
            initial_cash,
            limits,
        })
    }
}

/// Complete immutable research run request.
#[derive(Clone, Debug)]
pub struct BacktestRequest {
    pub(crate) dataset: BacktestDataset,
    pub(crate) assumptions: ResearchExecutionAssumptions,
    pub(crate) portfolio: PortfolioSeed,
    pub(crate) corporate_actions: Option<CorporateActionPlan>,
    pub(crate) sources: Box<[SourceIdentifier]>,
    pub(crate) seed: u64,
    pub(crate) limits: BacktestLimits,
}

impl BacktestRequest {
    /// Validates data, accounting, action, source, and resource bindings.
    #[allow(
        clippy::too_many_arguments,
        reason = "run data, fill, portfolio, action, source, seed, and resource contracts stay explicit"
    )]
    pub fn try_new(
        dataset: BacktestDataset,
        assumptions: ResearchExecutionAssumptions,
        portfolio: PortfolioSeed,
        corporate_actions: Option<CorporateActionPlan>,
        mut sources: Vec<SourceIdentifier>,
        seed: u64,
        limits: BacktestLimits,
    ) -> Result<Self, BacktestError> {
        sources.sort_unstable();
        if sources.is_empty()
            || sources.windows(2).any(|pair| pair[0] == pair[1])
            || dataset.observations.len() > limits.max_observations
            || dataset.retained_bytes > limits.max_retained_bytes
            || corporate_actions.as_ref().is_some_and(|plan| {
                plan.knowledge_cutoff()
                    > dataset
                        .observations
                        .last()
                        .map_or(Timestamp::from_unix_nanos(i64::MIN), |value| {
                            value.decision_at
                        })
            })
        {
            return Err(BacktestError::InvalidRequest);
        }
        Ok(Self {
            dataset,
            assumptions,
            portfolio,
            corporate_actions,
            sources: sources.into_boxed_slice(),
            seed,
            limits,
        })
    }

    /// Returns the complete admitted point-in-time dataset identity.
    #[must_use]
    pub const fn dataset_identity(&self) -> Sha256Digest {
        self.dataset.identity
    }

    /// Returns the exact catalog-resolved generation and object-graph identity.
    #[must_use]
    pub const fn object_graph_digest(&self) -> Sha256Digest {
        self.dataset.object_graph_digest()
    }

    /// Returns the exact research execution-assumption identity.
    #[must_use]
    pub const fn assumption_digest(&self) -> Sha256Digest {
        self.assumptions.digest()
    }

    /// Returns the deterministic pseudo-random seed.
    #[must_use]
    pub const fn seed(&self) -> u64 {
        self.seed
    }
}

/// Borrowed current point-in-time state exposed to a research strategy.
#[derive(Debug)]
pub struct BacktestContext<'observation> {
    observation: &'observation BacktestObservation,
    cash: Money,
    position: Decimal,
}

impl BacktestContext<'_> {
    /// Returns the cutoff at which every exposed value was available.
    #[must_use]
    pub const fn decision_at(&self) -> Timestamp {
        self.observation.decision_at
    }

    /// Returns immutable instrument execution terms for typed intent construction.
    #[must_use]
    pub const fn execution_terms(&self) -> InstrumentExecutionTerms {
        self.observation.execution_terms
    }

    /// Returns current exact research cash before this observation's new intents.
    #[must_use]
    pub const fn cash(&self) -> Money {
        self.cash
    }

    /// Returns current signed instrument units.
    #[must_use]
    pub const fn position(&self) -> Decimal {
        self.position
    }

    /// Returns one finite current-cutoff feature by stable name.
    #[must_use]
    pub fn feature(&self, name: &SourceIdentifier) -> Option<f64> {
        self.observation
            .features
            .binary_search_by(|candidate| candidate.name.cmp(name))
            .ok()
            .and_then(|index| self.observation.features.get(index))
            .map(ResearchFeatureValue::value)
    }

    pub(crate) fn versioned_feature(&self, name: &str, version: u32) -> Option<f64> {
        self.observation
            .features
            .iter()
            .find(|feature| feature.name().as_str() == name && feature.version().get() == version)
            .map(ResearchFeatureValue::value)
    }
}

/// Detailed bounded run result retained before controlled artifact publication.
#[derive(Clone, Debug)]
pub struct BacktestRun {
    fills: Box<[ResearchFill]>,
    portfolio: PortfolioRevision,
    no_action_count: usize,
    accounting_reconciliation: AccountingReconciliation,
    performance: BacktestPerformanceStatistics,
    result_digest: Sha256Digest,
}

impl BacktestRun {
    /// Returns deterministic research fills.
    #[must_use]
    pub fn fills(&self) -> &[ResearchFill] {
        &self.fills
    }

    /// Returns Task 16's immutable reconciled account revision.
    #[must_use]
    pub const fn portfolio(&self) -> &PortfolioRevision {
        &self.portfolio
    }

    /// Returns audited model/strategy no-action outputs.
    #[must_use]
    pub const fn no_action_count(&self) -> usize {
        self.no_action_count
    }

    /// Returns the explicit accounting-verification mode.
    #[must_use]
    pub const fn accounting_reconciliation(&self) -> AccountingReconciliation {
        self.accounting_reconciliation
    }

    /// Returns the complete deterministic result identity.
    #[must_use]
    pub const fn result_digest(&self) -> Sha256Digest {
        self.result_digest
    }

    pub(crate) const fn performance(&self) -> BacktestPerformanceStatistics {
        self.performance
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BacktestPerformanceStatistics {
    pub(crate) sharpe: f64,
    pub(crate) observations: usize,
    pub(crate) skewness: f64,
    pub(crate) excess_kurtosis: f64,
}

impl BacktestPerformanceStatistics {
    fn from_equity_marks(marks: &[Decimal]) -> Result<Self, BacktestError> {
        let returns = marks
            .windows(2)
            .map(|window| {
                let opening = window[0];
                if opening.is_zero() {
                    return Err(BacktestError::PerformanceMetrics);
                }
                window[1]
                    .checked_sub(opening)
                    .and_then(|change| change.checked_div(opening))
                    .and_then(|value| rust_decimal::prelude::ToPrimitive::to_f64(&value))
                    .filter(|value| value.is_finite())
                    .ok_or(BacktestError::PerformanceMetrics)
            })
            .collect::<Result<Vec<_>, _>>()?;
        if returns.len() < 3 {
            return Ok(Self {
                sharpe: 0.0,
                observations: returns.len(),
                skewness: 0.0,
                excess_kurtosis: 0.0,
            });
        }
        let count = returns.len() as f64;
        let mean = returns.iter().sum::<f64>() / count;
        let squared = returns
            .iter()
            .map(|value| {
                let deviation = value - mean;
                deviation * deviation
            })
            .sum::<f64>();
        let sample_variance = squared / (count - 1.0);
        let population_variance = squared / count;
        let standard_deviation = sample_variance.sqrt();
        let sharpe = if standard_deviation > 0.0 {
            mean / standard_deviation
        } else {
            0.0
        };
        let (skewness, excess_kurtosis) = if population_variance > 0.0 {
            let population_standard_deviation = population_variance.sqrt();
            let third = returns
                .iter()
                .map(|value| ((value - mean) / population_standard_deviation).powi(3))
                .sum::<f64>()
                / count;
            let fourth = returns
                .iter()
                .map(|value| ((value - mean) / population_standard_deviation).powi(4))
                .sum::<f64>()
                / count;
            (third, fourth - 3.0)
        } else {
            (0.0, 0.0)
        };
        if [sharpe, skewness, excess_kurtosis]
            .into_iter()
            .all(f64::is_finite)
        {
            Ok(Self {
                sharpe,
                observations: returns.len(),
                skewness,
                excess_kurtosis,
            })
        } else {
            Err(BacktestError::PerformanceMetrics)
        }
    }
}

/// Explicit strength of the accounting verification performed for one run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountingReconciliation {
    /// Independent fill shadow cash/positions/fees agreed exactly with Task 16.
    Independent,
}

/// Deterministic research engine; it owns no live adapter, broker, or journal capability.
#[derive(Debug)]
pub struct BacktestEngine;

impl BacktestEngine {
    /// Streams the exact PIT input in event-time order and reconciles all fills through Task 16.
    pub fn run(
        request: &BacktestRequest,
        strategy: &mut dyn BacktestStrategy,
        cancellation: &CancellationToken,
    ) -> Result<BacktestRun, BacktestError> {
        let mut clock = EventTimeClock::default();
        let mut simulator = ResearchFillSimulator::new(request.assumptions, request.seed);
        let mut pending = Vec::<OrderIntent>::new();
        let mut fills = Vec::<ResearchFill>::new();
        let mut no_action_count = 0_usize;
        let mut latest_prices = BTreeMap::<InstrumentId, (Money, Timestamp)>::new();
        let mut equity_marks = vec![request.portfolio.initial_cash.amount()];

        for observation in &request.dataset.observations {
            if cancellation.is_cancelled() {
                return Err(BacktestError::Cancelled);
            }
            clock.advance(observation.decision_at)?;
            let mut shadow = ShadowPortfolio::replay(request, &fills, observation.decision_at)?;
            if observation.stale_at >= observation.decision_at
                && let Some(mid_price) = observation.mid_price
            {
                let terms = observation.execution_terms;
                let amount = mid_price
                    .checked_to_decimal(terms.price_tick())?
                    .checked_mul(terms.contract_multiplier())
                    .ok_or(BacktestError::AccountingMismatch)?;
                latest_prices.insert(
                    observation.instrument_id(),
                    (
                        Money::new(amount, terms.quote_currency()),
                        observation.stale_at,
                    ),
                );
            }
            pending.retain(|intent| intent.expires_at() >= observation.decision_at);
            if observation.universe == HistoricalUniverseStatus::Delisted {
                pending.retain(|intent| {
                    intent.execution_terms().instrument_id() != observation.instrument_id()
                });
            }
            if observation.universe == HistoricalUniverseStatus::Eligible
                && observation.stale_at >= observation.decision_at
                && let Some(mid_price) = observation.mid_price
            {
                match request.assumptions.liquidity_priority() {
                    crate::ResearchLiquidityPriority::SignalTimeThenOrderId => {
                        pending
                            .sort_unstable_by_key(|intent| (intent.signal_at(), intent.order_id()));
                    }
                }
                let capacity = simulator.observation_capacity(observation.executable_depth)?;
                let mut remaining_capacity = capacity;
                let mut index = 0_usize;
                while index < pending.len() {
                    if pending[index].execution_terms().instrument_id()
                        != observation.instrument_id()
                        || !clock.is_execution_eligible(
                            pending[index].signal_at(),
                            request.assumptions.latency_nanos(),
                        )?
                    {
                        index += 1;
                        continue;
                    }
                    let outcome = simulator.simulate(
                        &pending[index],
                        observation.decision_at,
                        mid_price,
                        observation.spread_basis_points,
                        remaining_capacity,
                    )?;
                    let Some(fill) = outcome else {
                        index += 1;
                        continue;
                    };
                    shadow.apply(&fill, pending[index].execution_terms())?;
                    let remaining_lots = remaining_capacity
                        .get()
                        .checked_sub(fill.quantity().get())
                        .ok_or(BacktestError::AccountingMismatch)?;
                    remaining_capacity = QuantityLots::new(remaining_lots)?;
                    if fills.len() >= request.limits.max_fills {
                        return Err(BacktestError::LimitExceeded);
                    }
                    fills.push(fill);
                    pending.remove(index);
                }
                let context = BacktestContext {
                    observation,
                    cash: shadow.cash,
                    position: shadow.position(observation.instrument_id()),
                };
                let output = strategy.on_observation(&context)?;
                if output.no_action().is_some() {
                    no_action_count = no_action_count
                        .checked_add(1)
                        .ok_or(BacktestError::LimitExceeded)?;
                }
                for intent in output {
                    if intent.account_id() != request.portfolio.account_id
                        || intent.execution_terms() != observation.execution_terms
                        || intent.signal_at() != observation.decision_at
                    {
                        return Err(BacktestError::InvalidIntent);
                    }
                    if pending.len() >= request.limits.max_pending_intents {
                        return Err(BacktestError::LimitExceeded);
                    }
                    pending.push(intent);
                }
            }
            if let Some(equity) = shadow.marked_equity(&latest_prices, observation.decision_at)? {
                equity_marks.push(equity.amount());
            }
        }
        let portfolio = reconcile(request, &fills)?;
        let final_at = request
            .dataset
            .observations
            .last()
            .ok_or(BacktestError::InvalidDataset)?
            .decision_at;
        let shadow = ShadowPortfolio::replay(request, &fills, final_at)?;
        let accounting_reconciliation = if shadow.matches_revision(&portfolio) {
            AccountingReconciliation::Independent
        } else {
            return Err(BacktestError::AccountingMismatch);
        };
        let performance = BacktestPerformanceStatistics::from_equity_marks(&equity_marks)?;
        let result_digest = result_digest(request, &fills, &portfolio, no_action_count);
        Ok(BacktestRun {
            fills: fills.into_boxed_slice(),
            portfolio,
            no_action_count,
            accounting_reconciliation,
            performance,
            result_digest,
        })
    }
}

fn result_digest(
    request: &BacktestRequest,
    fills: &[ResearchFill],
    portfolio: &PortfolioRevision,
    no_action_count: usize,
) -> Sha256Digest {
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/backtest-result/v1");
    hash.update(request.dataset.identity.bytes());
    hash.update(request.assumptions.digest().bytes());
    hash.update(request.seed.to_be_bytes());
    for fill in fills {
        hash.update(fill.intent_digest().as_bytes());
        hash.update(fill.instrument_id().as_uuid().into_bytes());
        hash.update(fill.executed_at().unix_nanos().to_be_bytes());
        hash.update(fill.quantity().get().to_be_bytes());
        hash.update(fill.price().get().to_be_bytes());
        hash.update(fill.fee().amount().mantissa().to_be_bytes());
        hash.update(fill.fee().amount().scale().to_be_bytes());
    }
    hash.update(portfolio.token().bytes());
    hash.update(
        u64::try_from(no_action_count)
            .map_or(u64::MAX, |value| value)
            .to_be_bytes(),
    );
    Sha256Digest::new(hash.finalize().into())
}

/// Point-in-time, strategy, fill, portfolio, or resource failure.
#[derive(Debug, Error)]
pub enum BacktestError {
    #[error("backtest observation is invalid")]
    InvalidObservation,
    #[error("backtest dataset identity or ordering is invalid")]
    InvalidDataset,
    #[error("backtest input must be an inline bounded pinned-query batch receipt")]
    PinnedInputRequiresInlineBatches,
    #[error("backtest request bindings are invalid")]
    InvalidRequest,
    #[error("backtest limits are invalid")]
    InvalidLimits,
    #[error("backtest initial portfolio is invalid")]
    InvalidPortfolioSeed,
    #[error("strategy emitted an intent inconsistent with current PIT state")]
    InvalidIntent,
    #[error("backtest resource limit exceeded")]
    LimitExceeded,
    #[error("backtest was cancelled")]
    Cancelled,
    #[error("research portfolio constraint rejected a fill")]
    PortfolioConstraint,
    #[error("Task 16 and research shadow accounting disagreed")]
    AccountingMismatch,
    #[error("open inventory lacks a fresh final valuation")]
    MissingFinalPrice,
    #[error("backtest equity path cannot produce finite performance diagnostics")]
    PerformanceMetrics,
    #[error("event-time clock failed: {0}")]
    Clock(#[from] EventTimeClockError),
    #[error("research fill failed: {0}")]
    Fill(#[from] ResearchFillError),
    #[error("strategy failed: {0}")]
    Strategy(#[from] StrategyError),
    #[error("portfolio accounting failed: {0}")]
    Portfolio(#[from] PortfolioError),
    #[error("dataset schema failed: {0}")]
    DatasetSchema(#[from] market_squawk_data::DatasetSchemaError),
    #[error("financial arithmetic failed: {0}")]
    Financial(#[from] market_squawk_domain::FinancialError),
    #[error("price arithmetic failed: {0}")]
    Price(#[from] market_squawk_domain::PriceError),
    #[error("quantity arithmetic failed: {0}")]
    Quantity(#[from] market_squawk_domain::QuantityError),
    #[error("time arithmetic failed: {0}")]
    Time(#[from] market_squawk_domain::TimeError),
    #[error("source identity failed: {0}")]
    SourceIdentity(#[from] market_squawk_domain::IdentityError),
    #[error("revision evidence failed: {0}")]
    Provenance(#[from] market_squawk_domain::ProvenanceError),
}
