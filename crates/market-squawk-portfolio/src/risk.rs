//! Task 12-backed tracking-error, tail-risk, scenario, and stress reports.

use market_squawk_analytics::{
    ExactDecimalScale, ExactRate, MAX_ANALYTICS_IDENTIFIER_BYTES, MonetaryBasis, MonetaryValue,
    PortfolioAllocation, Quantile, ReturnSeries, ScenarioShock, ShockComposition,
    StatisticalDispersion, StatisticalInput, StatisticalResult, discrete_expected_shortfall,
    historical_var, scenario_impact, tracking_error,
};
use market_squawk_data::Sha256Digest;
use market_squawk_domain::{Money, SourceIdentifier};
use rust_decimal::Decimal;

use crate::exposure::try_instrument_dimension;
use crate::{
    PortfolioAnalyticsEvidence, PortfolioError, PortfolioLimits, PortfolioRevision,
    PortfolioRevisionId, admit_retained_bytes, checked_usize_add, checked_usize_mul,
};

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
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        if shocks.is_empty() {
            return Err(PortfolioError::InvalidPolicy);
        }
        if shocks.len() > limits.max_factors {
            return Err(PortfolioError::LimitExceeded {
                resource: "scenario shocks",
                observed: shocks.len(),
                limit: limits.max_factors,
            });
        }
        let retained_bytes = [
            std::mem::size_of::<Self>(),
            id.retained_bytes(),
            checked_usize_mul(shocks.capacity(), std::mem::size_of::<ScenarioShock>())?,
            checked_usize_mul(shocks.len(), MAX_ANALYTICS_IDENTIFIER_BYTES)?,
        ]
        .into_iter()
        .try_fold(0_usize, checked_usize_add)?;
        admit_retained_bytes(retained_bytes, limits)?;
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
    analytics_evidence_digest: Sha256Digest,
    confidence: Quantile,
    tracking_error: StatisticalDispersion,
    value_at_risk: StatisticalResult,
    expected_shortfall: StatisticalResult,
    scenarios: Vec<ScenarioResult>,
    retained_bytes: usize,
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
        analytics_evidence: &PortfolioAnalyticsEvidence,
        returns: &ReturnSeries,
        benchmark: &ReturnSeries,
        losses: &[StatisticalInput],
        confidence: Quantile,
        scenarios: &[ScenarioDefinition],
        limits: PortfolioLimits,
    ) -> Result<Self, PortfolioError> {
        let report_through = revision.evidence().as_of();
        analytics_evidence.validate_report(revision, report_through, report_through)?;
        let histories = [
            returns.observations(),
            benchmark.observations(),
            losses.len(),
        ];
        if let Some(observed) = histories
            .into_iter()
            .find(|observed| *observed > limits.max_history)
        {
            return Err(PortfolioError::LimitExceeded {
                resource: "risk history",
                observed,
                limit: limits.max_history,
            });
        }
        if scenarios.len() > limits.max_scenarios || scenarios.len() > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "risk scenarios",
                observed: scenarios.len(),
                limit: limits.max_scenarios.min(limits.max_results),
            });
        }
        let total_shocks = scenarios.iter().try_fold(0_usize, |total, scenario| {
            if scenario.shocks.len() > limits.max_factors {
                return Err(PortfolioError::LimitExceeded {
                    resource: "scenario shocks",
                    observed: scenario.shocks.len(),
                    limit: limits.max_factors,
                });
            }
            checked_usize_add(total, scenario.shocks.len())
        })?;
        if total_shocks > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "scenario shocks",
                observed: total_shocks,
                limit: limits.max_results,
            });
        }
        let allocation_rows = revision.positions().len();
        if allocation_rows > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "risk allocation rows",
                observed: allocation_rows,
                limit: limits.max_results,
            });
        }
        let scenario_work = checked_usize_mul(allocation_rows, total_shocks)?;
        if scenario_work > limits.max_results {
            return Err(PortfolioError::LimitExceeded {
                resource: "scenario work",
                observed: scenario_work,
                limit: limits.max_results,
            });
        }
        let retained_preflight = [
            std::mem::size_of::<Self>(),
            checked_usize_mul(scenarios.len(), std::mem::size_of::<ScenarioResult>())?,
            checked_usize_mul(scenarios.len(), SourceIdentifier::MAX_LENGTH)?,
        ]
        .into_iter()
        .try_fold(0_usize, checked_usize_add)?;
        admit_retained_bytes(retained_preflight, limits)?;
        let mut unique = Vec::new();
        unique
            .try_reserve_exact(scenarios.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        unique.extend(scenarios.iter().map(|scenario| &scenario.id));
        unique.sort_unstable();
        if unique.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(PortfolioError::InvalidDimension);
        }
        let mut allocations = Vec::new();
        allocations
            .try_reserve_exact(allocation_rows)
            .map_err(|_| PortfolioError::AllocationFailed)?;
        for position in revision.positions() {
            let dimension = try_instrument_dimension(position.instrument_id())?;
            allocations.push(
                PortfolioAllocation::try_new(
                    &dimension,
                    MonetaryValue::new(position.market_value(), MonetaryBasis::Total),
                    ExactRate::try_new(Decimal::ZERO, ExactDecimalScale::Unit)
                        .map_err(|_| PortfolioError::Analytics)?,
                )
                .map_err(|_| PortfolioError::Analytics)?,
            );
        }
        let mut scenario_results = Vec::new();
        scenario_results
            .try_reserve_exact(scenarios.len())
            .map_err(|_| PortfolioError::AllocationFailed)?;
        for scenario in scenarios {
            let impact = scenario_impact(&allocations, &scenario.shocks, scenario.composition)
                .map_err(|_| PortfolioError::Analytics)?;
            scenario_results.push(ScenarioResult {
                id: scenario.id.clone(),
                impact: impact.total().money(),
            });
        }
        let retained_bytes = scenario_results.iter().try_fold(
            checked_usize_add(
                std::mem::size_of::<Self>(),
                checked_usize_mul(
                    scenario_results.capacity(),
                    std::mem::size_of::<ScenarioResult>(),
                )?,
            )?,
            |retained, result| checked_usize_add(retained, result.id.retained_bytes()),
        )?;
        admit_retained_bytes(retained_bytes, limits)?;
        Ok(Self {
            revision_id: revision.id(),
            analytics_evidence_digest: analytics_evidence.semantic_digest(),
            confidence,
            tracking_error: tracking_error(returns, benchmark)
                .map_err(|_| PortfolioError::Analytics)?,
            value_at_risk: historical_var(losses, confidence)
                .map_err(|_| PortfolioError::Analytics)?,
            expected_shortfall: discrete_expected_shortfall(losses, confidence)
                .map_err(|_| PortfolioError::Analytics)?,
            scenarios: scenario_results,
            retained_bytes,
        })
    }

    /// Returns bound immutable revision identity.
    pub const fn revision_id(&self) -> PortfolioRevisionId {
        self.revision_id
    }

    /// Returns the exact point-in-time analytics authority digest.
    pub const fn analytics_evidence_digest(&self) -> Sha256Digest {
        self.analytics_evidence_digest
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

    /// Returns exact Rust-visible bytes retained by this report.
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}
