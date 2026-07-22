//! Task 12-backed tracking-error, tail-risk, scenario, and stress reports.

use std::collections::BTreeSet;

use market_squawk_analytics::{
    ExactDecimalScale, ExactRate, MonetaryBasis, MonetaryValue, PortfolioAllocation, Quantile,
    ReturnSeries, ScenarioShock, ShockComposition, StatisticalDispersion, StatisticalInput,
    StatisticalResult, discrete_expected_shortfall, historical_var, scenario_impact,
    tracking_error,
};
use market_squawk_domain::{Money, SourceIdentifier};
use rust_decimal::Decimal;

use crate::exposure::instrument_dimension;
use crate::{PortfolioError, PortfolioLimits, PortfolioRevision, PortfolioRevisionId};

/// One named set of exact Task 12 shocks and its composition rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioDefinition {
    id: SourceIdentifier,
    composition: ShockComposition,
    shocks: Vec<ScenarioShock>,
}

impl ScenarioDefinition {
    /// Constructs a nonempty scenario with duplicate dimensions permitted for explicit composition.
    ///
    /// # Errors
    ///
    /// Rejects an empty shock set.
    pub fn try_new(
        id: SourceIdentifier,
        composition: ShockComposition,
        shocks: Vec<ScenarioShock>,
    ) -> Result<Self, PortfolioError> {
        if shocks.is_empty() {
            return Err(PortfolioError::InvalidPolicy);
        }
        Ok(Self {
            id,
            composition,
            shocks,
        })
    }

    /// Returns stable scenario identity.
    pub const fn id(&self) -> &SourceIdentifier {
        &self.id
    }

    /// Returns shock composition policy.
    pub const fn composition(&self) -> ShockComposition {
        self.composition
    }

    /// Returns exact Task 12 shocks.
    pub fn shocks(&self) -> &[ScenarioShock] {
        &self.shocks
    }
}

/// One exact named scenario impact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScenarioResult {
    id: SourceIdentifier,
    impact: Money,
}

impl ScenarioResult {
    /// Returns stable scenario identity.
    pub const fn id(&self) -> &SourceIdentifier {
        &self.id
    }

    /// Returns exact portfolio value impact.
    pub const fn impact(&self) -> Money {
        self.impact
    }
}

/// Complete immutable-revision risk report.
#[derive(Clone, Debug, PartialEq)]
pub struct PortfolioRiskReport {
    revision_id: PortfolioRevisionId,
    confidence: Quantile,
    tracking_error: StatisticalDispersion,
    value_at_risk: StatisticalResult,
    expected_shortfall: StatisticalResult,
    scenarios: Vec<ScenarioResult>,
}

impl PortfolioRiskReport {
    /// Runs Task 12 pure tracking-error, VaR, ES, scenario, and stress kernels.
    ///
    /// # Errors
    ///
    /// Rejects duplicate/excessive scenarios, invalid loss units, or kernel failures.
    #[allow(
        clippy::too_many_arguments,
        reason = "risk inputs and immutable revision binding remain explicit"
    )]
    pub fn try_calculate(
        revision: &PortfolioRevision,
        returns: &ReturnSeries,
        benchmark: &ReturnSeries,
        losses: &[StatisticalInput],
        confidence: Quantile,
        scenarios: &[ScenarioDefinition],
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if scenarios.len() > limits.max_scenarios || scenarios.len() > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "risk scenarios",
                observed: scenarios.len(),
                limit: limits.max_scenarios.min(limits.max_results),
            });
        }
        let unique = scenarios
            .iter()
            .map(|scenario| &scenario.id)
            .collect::<BTreeSet<_>>();
        if unique.len() != scenarios.len() {
            return Err(PortfolioError::InvalidDimension);
        }
        let allocations = revision
            .positions()
            .iter()
            .map(|position| {
                PortfolioAllocation::try_new(
                    &instrument_dimension(position.instrument_id()),
                    MonetaryValue::new(position.market_value(), MonetaryBasis::Total),
                    ExactRate::try_new(Decimal::ZERO, ExactDecimalScale::Unit)
                        .map_err(|_| PortfolioError::Analytics)?,
                )
                .map_err(|_| PortfolioError::Analytics)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let scenario_results = scenarios
            .iter()
            .map(|scenario| {
                let impact = scenario_impact(&allocations, &scenario.shocks, scenario.composition)
                    .map_err(|_| PortfolioError::Analytics)?;
                Ok(ScenarioResult {
                    id: scenario.id.clone(),
                    impact: impact.total().money(),
                })
            })
            .collect::<Result<Vec<_>, PortfolioError>>()?;
        Ok(Self {
            revision_id: revision.id(),
            confidence,
            tracking_error: tracking_error(returns, benchmark)
                .map_err(|_| PortfolioError::Analytics)?,
            value_at_risk: historical_var(losses, confidence)
                .map_err(|_| PortfolioError::Analytics)?,
            expected_shortfall: discrete_expected_shortfall(losses, confidence)
                .map_err(|_| PortfolioError::Analytics)?,
            scenarios: scenario_results,
        })
    }

    /// Returns bound immutable revision identity.
    pub const fn revision_id(&self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns tail confidence policy.
    pub const fn confidence(&self) -> Quantile {
        self.confidence
    }

    /// Returns annualized active-return tracking error.
    pub const fn tracking_error(&self) -> StatisticalDispersion {
        self.tracking_error
    }

    /// Returns discrete historical Value at Risk.
    pub const fn value_at_risk(&self) -> StatisticalResult {
        self.value_at_risk
    }

    /// Returns coherent discrete expected shortfall.
    pub const fn expected_shortfall(&self) -> StatisticalResult {
        self.expected_shortfall
    }

    /// Returns scenario and stress impacts in caller order.
    pub fn scenarios(&self) -> &[ScenarioResult] {
        &self.scenarios
    }
}
