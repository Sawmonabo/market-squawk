//! Canonical persisted policy inputs for governed backtests.

use std::{str::FromStr as _, time::Duration};

use market_squawk_backtesting::{
    BacktestLimits, BacktestLimitsInput, PortfolioSeed, ResearchExecutionAssumptions,
    ResearchExecutionAssumptionsInput, ResearchLiquidityPriority, TrialParameter,
    TrialSearchDimension,
};
use market_squawk_domain::{AccountId, BasisPoints, Currency, Money, SourceIdentifier};
use market_squawk_portfolio::{PortfolioLimitInput, PortfolioLimits};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::BacktestExperimentPlan;

use super::RecipeError;

const MAX_EXPERIMENT_PARAMETERS: usize = 1_024;
const MAX_SEARCH_DIMENSIONS: usize = 1_024;
const MAX_EXPERIMENT_TRIALS: usize = 1_000_000;

/// Caller-selected bounded analytical query limits retained by the immutable recipe.
#[derive(Clone, Copy, Debug)]
pub struct GovernedBacktestQueryLimitsInput {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_memory_bytes: u64,
    pub max_partitions: usize,
    pub max_ast_nodes: usize,
    pub max_plan_nodes: usize,
    pub deadline: Duration,
}

/// Exact initial account state retained by the immutable recipe.
#[derive(Clone, Copy, Debug)]
pub struct GovernedBacktestPortfolioSeedInput {
    pub account_id: AccountId,
    pub initial_cash: Money,
    pub limits: PortfolioLimitInput,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct QueryLimitsWire {
    max_rows: u64,
    max_bytes: u64,
    max_memory_bytes: u64,
    max_partitions: usize,
    max_ast_nodes: usize,
    max_plan_nodes: usize,
    deadline_nanos: u64,
}

impl QueryLimitsWire {
    pub(super) fn try_from_input(
        input: GovernedBacktestQueryLimitsInput,
    ) -> Result<Self, RecipeError> {
        let deadline_nanos =
            u64::try_from(input.deadline.as_nanos()).map_err(|_| RecipeError::Invalid)?;
        let wire = Self {
            max_rows: input.max_rows,
            max_bytes: input.max_bytes,
            max_memory_bytes: input.max_memory_bytes,
            max_partitions: input.max_partitions,
            max_ast_nodes: input.max_ast_nodes,
            max_plan_nodes: input.max_plan_nodes,
            deadline_nanos,
        };
        wire.build()?;
        Ok(wire)
    }

    pub(super) fn build(self) -> Result<market_squawk_data::QueryLimits, RecipeError> {
        market_squawk_data::QueryLimits::try_new(
            self.max_rows,
            self.max_bytes,
            self.max_memory_bytes,
            self.max_partitions,
            self.max_ast_nodes,
            self.max_plan_nodes,
            Duration::from_nanos(self.deadline_nanos),
        )
        .map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn into_input(self) -> Result<GovernedBacktestQueryLimitsInput, RecipeError> {
        self.build()?;
        Ok(GovernedBacktestQueryLimitsInput {
            max_rows: self.max_rows,
            max_bytes: self.max_bytes,
            max_memory_bytes: self.max_memory_bytes,
            max_partitions: self.max_partitions,
            max_ast_nodes: self.max_ast_nodes,
            max_plan_nodes: self.max_plan_nodes,
            deadline: Duration::from_nanos(self.deadline_nanos),
        })
    }

    pub(super) const fn max_bytes(self) -> u64 {
        self.max_bytes
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ExecutionAssumptionsWire {
    version: u32,
    fee_basis_points: i32,
    slippage_basis_points: i32,
    maximum_random_slippage_basis_points: i32,
    maximum_participation_basis_points: i32,
    liquidity_priority: LiquidityPriorityWire,
    latency_nanos: i64,
    allow_partial_fills: bool,
    fee_decimal_scale: u32,
}

impl ExecutionAssumptionsWire {
    pub(super) fn try_from_input(
        input: ResearchExecutionAssumptionsInput,
    ) -> Result<Self, RecipeError> {
        let wire = Self {
            version: input.version,
            fee_basis_points: input.fee_basis_points.get(),
            slippage_basis_points: input.slippage_basis_points.get(),
            maximum_random_slippage_basis_points: input.maximum_random_slippage_basis_points.get(),
            maximum_participation_basis_points: input.maximum_participation_basis_points.get(),
            liquidity_priority: input.liquidity_priority.into(),
            latency_nanos: input.latency_nanos,
            allow_partial_fills: input.allow_partial_fills,
            fee_decimal_scale: input.fee_decimal_scale,
        };
        wire.build()?;
        Ok(wire)
    }

    pub(super) fn build(self) -> Result<ResearchExecutionAssumptions, RecipeError> {
        ResearchExecutionAssumptions::try_new(ResearchExecutionAssumptionsInput {
            version: self.version,
            fee_basis_points: BasisPoints::new(self.fee_basis_points),
            slippage_basis_points: BasisPoints::new(self.slippage_basis_points),
            maximum_random_slippage_basis_points: BasisPoints::new(
                self.maximum_random_slippage_basis_points,
            ),
            maximum_participation_basis_points: BasisPoints::new(
                self.maximum_participation_basis_points,
            ),
            liquidity_priority: self.liquidity_priority.into(),
            latency_nanos: self.latency_nanos,
            allow_partial_fills: self.allow_partial_fills,
            fee_decimal_scale: self.fee_decimal_scale,
        })
        .map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn into_input(self) -> Result<ResearchExecutionAssumptionsInput, RecipeError> {
        self.build()?;
        Ok(ResearchExecutionAssumptionsInput {
            version: self.version,
            fee_basis_points: BasisPoints::new(self.fee_basis_points),
            slippage_basis_points: BasisPoints::new(self.slippage_basis_points),
            maximum_random_slippage_basis_points: BasisPoints::new(
                self.maximum_random_slippage_basis_points,
            ),
            maximum_participation_basis_points: BasisPoints::new(
                self.maximum_participation_basis_points,
            ),
            liquidity_priority: self.liquidity_priority.into(),
            latency_nanos: self.latency_nanos,
            allow_partial_fills: self.allow_partial_fills,
            fee_decimal_scale: self.fee_decimal_scale,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum LiquidityPriorityWire {
    SignalTimeThenOrderId,
}

impl From<ResearchLiquidityPriority> for LiquidityPriorityWire {
    fn from(value: ResearchLiquidityPriority) -> Self {
        match value {
            ResearchLiquidityPriority::SignalTimeThenOrderId => Self::SignalTimeThenOrderId,
        }
    }
}

impl From<LiquidityPriorityWire> for ResearchLiquidityPriority {
    fn from(value: LiquidityPriorityWire) -> Self {
        match value {
            LiquidityPriorityWire::SignalTimeThenOrderId => Self::SignalTimeThenOrderId,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PortfolioSeedWire {
    account_id: AccountId,
    initial_cash_amount: String,
    initial_cash_currency: String,
    limits: PortfolioLimitsWire,
}

impl PortfolioSeedWire {
    pub(super) fn try_from_input(
        input: GovernedBacktestPortfolioSeedInput,
    ) -> Result<Self, RecipeError> {
        let normalized = Money::new(input.initial_cash.amount(), input.initial_cash.currency());
        let wire = Self {
            account_id: input.account_id,
            initial_cash_amount: normalized.amount().to_string(),
            initial_cash_currency: normalized.currency().as_str().to_owned(),
            limits: PortfolioLimitsWire::from(input.limits),
        };
        wire.build()?;
        Ok(wire)
    }

    pub(super) fn build(&self) -> Result<PortfolioSeed, RecipeError> {
        let amount =
            Decimal::from_str(&self.initial_cash_amount).map_err(|_| RecipeError::Invalid)?;
        if amount.normalize().to_string() != self.initial_cash_amount {
            return Err(RecipeError::Invalid);
        }
        let currency = Currency::try_from(self.initial_cash_currency.as_str())
            .map_err(|_| RecipeError::Invalid)?;
        if currency.as_str() != self.initial_cash_currency {
            return Err(RecipeError::Invalid);
        }
        let limits =
            PortfolioLimits::try_new(self.limits.into()).map_err(|_| RecipeError::Invalid)?;
        PortfolioSeed::try_new(self.account_id, Money::new(amount, currency), limits)
            .map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn into_input(self) -> Result<GovernedBacktestPortfolioSeedInput, RecipeError> {
        self.build()?;
        let amount =
            Decimal::from_str(&self.initial_cash_amount).map_err(|_| RecipeError::Invalid)?;
        let currency = Currency::try_from(self.initial_cash_currency.as_str())
            .map_err(|_| RecipeError::Invalid)?;
        Ok(GovernedBacktestPortfolioSeedInput {
            account_id: self.account_id,
            initial_cash: Money::new(amount, currency),
            limits: self.limits.into(),
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PortfolioLimitsWire {
    max_accounts: usize,
    max_instruments: usize,
    max_lots: usize,
    max_transactions: usize,
    max_factors: usize,
    max_scenarios: usize,
    max_history: usize,
    max_results: usize,
    max_retained_bytes: usize,
}

impl From<PortfolioLimitInput> for PortfolioLimitsWire {
    fn from(value: PortfolioLimitInput) -> Self {
        Self {
            max_accounts: value.max_accounts,
            max_instruments: value.max_instruments,
            max_lots: value.max_lots,
            max_transactions: value.max_transactions,
            max_factors: value.max_factors,
            max_scenarios: value.max_scenarios,
            max_history: value.max_history,
            max_results: value.max_results,
            max_retained_bytes: value.max_retained_bytes,
        }
    }
}

impl From<PortfolioLimitsWire> for PortfolioLimitInput {
    fn from(value: PortfolioLimitsWire) -> Self {
        Self {
            max_accounts: value.max_accounts,
            max_instruments: value.max_instruments,
            max_lots: value.max_lots,
            max_transactions: value.max_transactions,
            max_factors: value.max_factors,
            max_scenarios: value.max_scenarios,
            max_history: value.max_history,
            max_results: value.max_results,
            max_retained_bytes: value.max_retained_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BacktestLimitsWire {
    max_observations: usize,
    max_pending_intents: usize,
    max_fills: usize,
    max_retained_bytes: usize,
}

impl BacktestLimitsWire {
    pub(super) fn try_from_input(input: BacktestLimitsInput) -> Result<Self, RecipeError> {
        let wire = Self {
            max_observations: input.max_observations,
            max_pending_intents: input.max_pending_intents,
            max_fills: input.max_fills,
            max_retained_bytes: input.max_retained_bytes,
        };
        wire.build()?;
        Ok(wire)
    }

    pub(super) fn build(self) -> Result<BacktestLimits, RecipeError> {
        BacktestLimits::try_new(BacktestLimitsInput {
            max_observations: self.max_observations,
            max_pending_intents: self.max_pending_intents,
            max_fills: self.max_fills,
            max_retained_bytes: self.max_retained_bytes,
        })
        .map_err(|_| RecipeError::Invalid)
    }

    pub(super) fn into_input(self) -> Result<BacktestLimitsInput, RecipeError> {
        self.build()?;
        Ok(BacktestLimitsInput {
            max_observations: self.max_observations,
            max_pending_intents: self.max_pending_intents,
            max_fills: self.max_fills,
            max_retained_bytes: self.max_retained_bytes,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ExperimentWire {
    parameters: Vec<ParameterWire>,
    search_space: Vec<SearchDimensionWire>,
    selection_criterion: SourceIdentifier,
}

impl ExperimentWire {
    pub(super) fn try_from_plan(plan: BacktestExperimentPlan) -> Result<Self, RecipeError> {
        let mut parameters = plan
            .parameters
            .into_iter()
            .map(ParameterWire::from)
            .collect::<Vec<_>>();
        let mut search_space = plan
            .search_space
            .into_iter()
            .map(SearchDimensionWire::from)
            .collect::<Vec<_>>();
        parameters.sort_unstable();
        search_space.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let wire = Self {
            parameters,
            search_space,
            selection_criterion: plan.selection_criterion,
        };
        wire.validate()?;
        Ok(wire)
    }

    pub(super) fn build(&self) -> Result<BacktestExperimentPlan, RecipeError> {
        self.validate()?;
        let search_space = self
            .search_space
            .iter()
            .map(SearchDimensionWire::build)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BacktestExperimentPlan {
            parameters: self
                .parameters
                .iter()
                .map(|parameter| {
                    TrialParameter::new(parameter.name.clone(), parameter.value.clone())
                })
                .collect(),
            search_space,
            selection_criterion: self.selection_criterion.clone(),
        })
    }

    /// True only when selected parameters may differ while the exact search and selection design
    /// remains shared by every independently materialized cohort member.
    pub(super) fn same_design(&self, other: &Self) -> bool {
        self.search_space == other.search_space
            && self.selection_criterion == other.selection_criterion
    }

    fn validate(&self) -> Result<(), RecipeError> {
        if self.parameters.len() > MAX_EXPERIMENT_PARAMETERS
            || self.search_space.len() > MAX_SEARCH_DIMENSIONS
            || self.parameters.len() != self.search_space.len()
            || self
                .parameters
                .windows(2)
                .any(|pair| pair[0].name >= pair[1].name)
            || self
                .search_space
                .windows(2)
                .any(|pair| pair[0].name >= pair[1].name)
        {
            return Err(RecipeError::Invalid);
        }
        let trial_count = self
            .search_space
            .iter()
            .try_fold(1_usize, |count, dimension| {
                if dimension.candidates.is_empty()
                    || dimension
                        .candidates
                        .windows(2)
                        .any(|pair| pair[0] >= pair[1])
                {
                    return Err(RecipeError::Invalid);
                }
                count
                    .checked_mul(dimension.candidates.len())
                    .filter(|value| *value <= MAX_EXPERIMENT_TRIALS)
                    .ok_or(RecipeError::Invalid)
            })?;
        if trial_count > MAX_EXPERIMENT_TRIALS {
            return Err(RecipeError::Invalid);
        }
        for parameter in &self.parameters {
            let dimension = self
                .search_space
                .binary_search_by(|candidate| candidate.name.cmp(&parameter.name))
                .ok()
                .and_then(|index| self.search_space.get(index))
                .ok_or(RecipeError::Invalid)?;
            if dimension
                .candidates
                .binary_search(&parameter.value)
                .is_err()
            {
                return Err(RecipeError::Invalid);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ParameterWire {
    pub(super) name: SourceIdentifier,
    pub(super) value: SourceIdentifier,
}

impl From<TrialParameter> for ParameterWire {
    fn from(value: TrialParameter) -> Self {
        Self {
            name: value.name().clone(),
            value: value.value().clone(),
        }
    }
}

impl ParameterWire {
    pub(super) fn into_trial_parameter(self) -> TrialParameter {
        TrialParameter::new(self.name, self.value)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SearchDimensionWire {
    name: SourceIdentifier,
    candidates: Vec<SourceIdentifier>,
}

impl From<TrialSearchDimension> for SearchDimensionWire {
    fn from(value: TrialSearchDimension) -> Self {
        Self {
            name: value.name().clone(),
            candidates: value.candidates().to_vec(),
        }
    }
}

impl SearchDimensionWire {
    fn build(&self) -> Result<TrialSearchDimension, RecipeError> {
        TrialSearchDimension::try_new(self.name.clone(), self.candidates.clone())
            .map_err(|_| RecipeError::Invalid)
    }
}
