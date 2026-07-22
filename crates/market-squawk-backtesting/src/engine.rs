//! Manifest-pinned point-in-time orchestration and portfolio reconciliation.

use market_squawk_data::{CorporateActionPlan, Sha256Digest};
use market_squawk_domain::{
    AccountId, InstrumentExecutionTerms, Money, SourceIdentifier, Timestamp,
};
use market_squawk_execution::{BoundedOrderIntents, OrderIntent, StrategyError};
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

/// Research strategy contract sharing execution's bounded, validated order-intent output.
pub trait BacktestStrategy: Send + std::fmt::Debug {
    /// Evaluates only the current immutable point-in-time observation.
    fn on_observation(
        &mut self,
        context: &BacktestContext<'_>,
    ) -> Result<BoundedOrderIntents, StrategyError>;
}

/// Detailed bounded run result retained before controlled artifact publication.
#[derive(Clone, Debug)]
pub struct BacktestRun {
    fills: Box<[ResearchFill]>,
    portfolio: PortfolioRevision,
    no_action_count: usize,
    accounting_reconciliation: AccountingReconciliation,
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
}

/// Explicit strength of the accounting verification performed for one run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountingReconciliation {
    /// Independent fill shadow cash/positions/fees agreed exactly with Task 16.
    Independent,
    /// Task 16 applied a typed corporate-action plan that the fill-only shadow cannot reproduce.
    Task16AuthoritativeCorporateActions,
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
        let mut shadow = ShadowPortfolio::new(request.portfolio.initial_cash);
        let mut no_action_count = 0_usize;

        for observation in &request.dataset.observations {
            if cancellation.is_cancelled() {
                return Err(BacktestError::Cancelled);
            }
            clock.advance(observation.decision_at)?;
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
                        observation.executable_depth,
                    )?;
                    let Some(fill) = outcome else {
                        index += 1;
                        continue;
                    };
                    shadow.apply(&fill, pending[index].execution_terms())?;
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
        }
        let portfolio = reconcile(request, &fills)?;
        let accounting_reconciliation = if request.corporate_actions.is_some() {
            AccountingReconciliation::Task16AuthoritativeCorporateActions
        } else if shadow.matches_revision(&portfolio) {
            AccountingReconciliation::Independent
        } else {
            return Err(BacktestError::AccountingMismatch);
        };
        let result_digest = result_digest(request, &fills, &portfolio, no_action_count);
        Ok(BacktestRun {
            fills: fills.into_boxed_slice(),
            portfolio,
            no_action_count,
            accounting_reconciliation,
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
