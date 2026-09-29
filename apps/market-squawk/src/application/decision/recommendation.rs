//! Explicit composition from recommendation backtests into proposal evidence.

#![allow(
    dead_code,
    reason = "the workflow producer calls this narrow adapter at the next composition seam"
)]

mod equity_premium;

pub(crate) use equity_premium::derive_default_financial_model_macro_assumptions;

use std::num::{NonZeroU32, NonZeroU64};

use market_squawk_backtesting::{
    RECOMMENDATION_TARGET_HORIZON_NANOS_V1, RecommendationAggregateEvidenceV1,
};
use market_squawk_decisions::{
    ChronologicalOutOfSampleEvidence, CostAdjustedBacktestEvidence, DecisionContentDigest,
    FinancialModelEvidence, FinancialModelMacroAssumptions, ForecastCalibrationSummary,
    ForecastPriceRanges, MacroRateMaturity, MacroRateReferenceEvidence, PriceForecastEvidence,
    ProposalEvidenceWindow, ProposalForecastVintageId, RecommendationPolicy,
    RecommendationStudyQualification, TargetPriceCases, TargetPriceRange,
};
use market_squawk_domain::{
    BasisPoints, Currency, DigestAlgorithm, EvidenceDigest, Money, Timestamp,
};
use market_squawk_modeling::{ForecastCentralStatistic, ForecastValue};
use market_squawk_valuation::{
    AutomaticValuationAssumption, AutomaticValuationAssumptionKind, AutomaticValuationMethodReceipt,
};
use rust_decimal::{Decimal, RoundingStrategy};

use crate::application::{
    analysis::GovernedRecommendationBacktestEvidenceV1,
    model::forecast::{ExactHorizonPriceForecastProjection, SelectedPriceIntervals},
    research::MacroInvestmentContext,
};

/// Why an exact saved monetary forecast cannot enter the shared decision evidence envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecommendationForecastAdapterError {
    InvalidTime,
    InvalidMonetaryValue,
    InvalidCalibration,
    InvalidIdentity,
}

/// Pure projection of actual price mean and calibrated interval endpoints. Both live and study
/// callers perform their own source admission before using this shared monetary mapping.
pub(crate) fn monetary_forecast_cases(
    currency: Currency,
    central: ForecastValue,
    intervals: SelectedPriceIntervals,
) -> Result<(TargetPriceCases, ForecastPriceRanges, Money), RecommendationForecastAdapterError> {
    let monetary = |value: ForecastValue| {
        Decimal::try_from_i128_with_scale(value.mantissa(), u32::from(value.scale()))
            .map(|decimal| Money::new(decimal, currency))
            .map_err(|_| RecommendationForecastAdapterError::InvalidMonetaryValue)
    };
    let ranges = ForecastPriceRanges::try_new(
        TargetPriceRange::try_new(
            monetary(intervals.interval_95().lower())?,
            monetary(intervals.interval_80().lower())?,
        )
        .map_err(|_| RecommendationForecastAdapterError::InvalidMonetaryValue)?,
        TargetPriceRange::try_new(
            monetary(intervals.interval_50().lower())?,
            monetary(intervals.interval_50().upper())?,
        )
        .map_err(|_| RecommendationForecastAdapterError::InvalidMonetaryValue)?,
        TargetPriceRange::try_new(
            monetary(intervals.interval_80().upper())?,
            monetary(intervals.interval_95().upper())?,
        )
        .map_err(|_| RecommendationForecastAdapterError::InvalidMonetaryValue)?,
    )
    .map_err(|_| RecommendationForecastAdapterError::InvalidMonetaryValue)?;
    let mean = monetary(central)?;
    let cases = TargetPriceCases::try_new(
        monetary(intervals.interval_80().lower())?,
        mean,
        monetary(intervals.interval_80().upper())?,
    )
    .map_err(|_| RecommendationForecastAdapterError::InvalidMonetaryValue)?;
    Ok((cases, ranges, mean))
}

/// Preserves an actual conditional mean and calibrated outcome zones from a revalidated vintage.
/// The downside/upside cases are the real 80% interval endpoints, not invented expectations or
/// scenario probabilities. The exact monetary derivation commits any causal return-to-price map.
pub(crate) fn adapt_price_forecast_evidence(
    projection: ExactHorizonPriceForecastProjection<'_>,
    policy: &RecommendationPolicy,
    source_cutoff: Timestamp,
    admitted_at: Timestamp,
) -> Result<PriceForecastEvidence, RecommendationForecastAdapterError> {
    let identity = |bytes| {
        DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
            .map_err(|_| RecommendationForecastAdapterError::InvalidIdentity)
    };
    if admitted_at < source_cutoff
        || projection.source_knowledge_cutoff() > source_cutoff
        || projection.selected_as_of() > admitted_at
        || projection.expires_at() <= admitted_at
        || i64::try_from(projection.terminal_horizon_nanos().get()).ok()
            != Some(policy.horizon_nanos())
        || projection
            .observed_through()
            .checked_add_nanos(policy.horizon_nanos())
            .ok()
            != Some(projection.terminal_at())
    {
        return Err(RecommendationForecastAdapterError::InvalidTime);
    }
    let available_at = projection.available_at().max(projection.created_at());
    if available_at > admitted_at {
        return Err(RecommendationForecastAdapterError::InvalidTime);
    }
    let (cases, ranges, mean) = monetary_forecast_cases(
        projection.currency(),
        projection.terminal_mean(),
        projection.intervals(),
    )?;
    let bands = projection.calibration().bands();
    let summarize = |index: usize| -> Result<(u32, u32, u32), RecommendationForecastAdapterError> {
        let band = bands
            .get(index)
            .ok_or(RecommendationForecastAdapterError::InvalidCalibration)?;
        let realized = projection
            .coverage_evaluation()
            .realized()
            .get(index)
            .ok_or(RecommendationForecastAdapterError::InvalidCalibration)?;
        let nominal = u32::from(band.coverage().basis_points()) * 100;
        let total = u128::from(realized.total().get());
        let numerator = u128::from(realized.covered()) * 1_000_000;
        // Quantize toward the adverse direction so a fractional-ppm miss never passes the ceiling.
        let realized = if numerator > u128::from(nominal) * total {
            numerator.div_ceil(total)
        } else {
            numerator / total
        };
        Ok((
            nominal,
            u32::try_from(realized)
                .map_err(|_| RecommendationForecastAdapterError::InvalidCalibration)?,
            u32::try_from(total)
                .map_err(|_| RecommendationForecastAdapterError::InvalidCalibration)?,
        ))
    };
    let mut chosen = summarize(1)?;
    let mut count = chosen.2;
    let maximum_error = policy.parameters().maximum_forecast_calibration_error_ppm;
    for index in [0, 2] {
        let band = summarize(index)?;
        count = count.min(band.2);
        let error = band.0.abs_diff(band.1);
        if error > maximum_error && error > chosen.0.abs_diff(chosen.1) {
            chosen = band;
        }
    }
    let calibration = ForecastCalibrationSummary::try_new(
        chosen.0,
        chosen.1,
        NonZeroU32::new(count).ok_or(RecommendationForecastAdapterError::InvalidCalibration)?,
    )
    .map_err(|_| RecommendationForecastAdapterError::InvalidCalibration)?;
    let monetary_identity = identity(projection.price_derivation_identity().bytes())?;
    let window = ProposalEvidenceWindow::try_from_derived(
        projection.observed_through(),
        projection.source_knowledge_cutoff(),
        available_at,
        projection.expires_at(),
        DecisionContentDigest::try_new(projection.selection_receipt_digest())
            .map_err(|_| RecommendationForecastAdapterError::InvalidIdentity)?,
    )
    .map_err(|_| RecommendationForecastAdapterError::InvalidTime)?;
    PriceForecastEvidence::try_new(
        projection.instrument_id(),
        cases,
        ranges,
        projection.terminal_at(),
        Some(ForecastCentralStatistic::ModelEstimatedConditionalMean),
        Some(mean),
        Some(projection.terminal_at()),
        Some(monetary_identity),
        ProposalForecastVintageId::try_from_bytes(projection.vintage_id().bytes())
            .map_err(|_| RecommendationForecastAdapterError::InvalidIdentity)?,
        monetary_identity,
        identity(projection.calibration_identity().bytes())?,
        identity(projection.calibration().residuals_hash().bytes())?,
        calibration,
        window,
    )
    .map_err(|_| RecommendationForecastAdapterError::InvalidMonetaryValue)
}

/// Why complete recommendation-backtest evidence could not be admitted into a proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecommendationBacktestAdapterError {
    IncompleteAggregate,
    InvalidCount,
    UnrepresentableBasisPoints,
    InvalidIdentity,
    InvalidProposalEvidence,
}

/// Exact historical-test and chronological OOS projections from one governed study.
pub(crate) struct RecommendationBacktestProposalEvidence {
    pub(crate) historical_test: CostAdjustedBacktestEvidence,
    pub(crate) out_of_sample: ChronologicalOutOfSampleEvidence,
}

/// Why a model receipt and exact Macro assumption context could not be joined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecommendationFinancialModelAdapterError {
    MacroContextMismatch,
    InvalidIdentity,
    InvalidProposalEvidence,
}

/// Projects one method-specific model receipt with its exact contemporaneous Macro context.
///
/// Government yields remain assumption context. Individual methods must evidence any economic
/// assumption they actually use; this seam never relabels the context as a model, fair value,
/// recommendation confidence, or execution authority.
#[allow(
    clippy::too_many_arguments,
    reason = "model, scenario, sensitivity, Macro, horizon, and timing authorities remain explicit"
)]
pub(crate) fn adapt_financial_model_evidence(
    receipt: &AutomaticValuationMethodReceipt,
    macro_context: Option<&MacroInvestmentContext>,
    macro_assumptions: Option<FinancialModelMacroAssumptions>,
    scenarios: TargetPriceCases,
    scenario_identity: DecisionContentDigest,
    sensitivity_range: TargetPriceRange,
    sensitivity_identity: DecisionContentDigest,
    horizon_at: Timestamp,
    window: ProposalEvidenceWindow,
) -> Result<FinancialModelEvidence, RecommendationFinancialModelAdapterError> {
    match (macro_context, macro_assumptions.as_ref()) {
        (Some(context), Some(binding)) => {
            let reference = binding.reference();
            let selected = match reference.maturity() {
                MacroRateMaturity::TenYear => context.valuation_rates().ten_year_reference(),
                MacroRateMaturity::ThirtyYear => context.valuation_rates().thirty_year_reference(),
            };
            let maximum_age = NonZeroU64::new(30 * 86_400 * 1_000_000_000)
                .ok_or(RecommendationFinancialModelAdapterError::InvalidProposalEvidence)?;
            let maximum_expiry = selected
                .expires_at(maximum_age)
                .map_err(|_| RecommendationFinancialModelAdapterError::MacroContextMismatch)?;
            if context.parent_manifests().is_empty()
                || context.knowledge_cutoff() > receipt.measurement_at()
                || reference.context_identity() != context.evidence_digest()
                || reference.evidence_identity() != selected.evidence_digest()
                || reference.annual_yield_percent() != selected.annual_yield_percent()
                || reference.available_at() != selected.available_at()
                || reference.knowledge_cutoff() != context.knowledge_cutoff()
                || reference.effective_date_cutoff() != context.effective_date_cutoff()
                || reference.expires_at() > maximum_expiry
            {
                return Err(RecommendationFinancialModelAdapterError::MacroContextMismatch);
            }
        }
        (None, None) => {}
        _ => return Err(RecommendationFinancialModelAdapterError::MacroContextMismatch),
    }
    FinancialModelEvidence::try_from_automatic_valuation_receipt(
        receipt,
        scenarios,
        scenario_identity,
        sensitivity_range,
        sensitivity_identity,
        macro_assumptions,
        horizon_at,
        window,
    )
    .map_err(|_| RecommendationFinancialModelAdapterError::InvalidProposalEvidence)
}

/// Derives an annual model-rate assumption before the valuation calculation consumes it.
/// The producer must use annual cash-flow/residual-income periods and separately evidenced premium.
pub(crate) fn derive_financial_model_macro_assumptions(
    context: &MacroInvestmentContext,
    maturity: MacroRateMaturity,
    premium: AutomaticValuationAssumption,
    rate_kind: AutomaticValuationAssumptionKind,
    identifier: &str,
    maximum_age_nanos: NonZeroU64,
) -> Result<FinancialModelMacroAssumptions, RecommendationFinancialModelAdapterError> {
    if context.parent_manifests().is_empty() {
        return Err(RecommendationFinancialModelAdapterError::MacroContextMismatch);
    }
    let selected = match maturity {
        MacroRateMaturity::TenYear => context.valuation_rates().ten_year_reference(),
        MacroRateMaturity::ThirtyYear => context.valuation_rates().thirty_year_reference(),
    };
    let reference = MacroRateReferenceEvidence::try_new(
        maturity,
        selected.annual_yield_percent(),
        context.evidence_digest(),
        selected.evidence_digest(),
        context.knowledge_cutoff(),
        context.effective_date_cutoff(),
        selected.available_at(),
        selected
            .expires_at(maximum_age_nanos)
            .map_err(|_| RecommendationFinancialModelAdapterError::MacroContextMismatch)?,
    )
    .map_err(|_| RecommendationFinancialModelAdapterError::MacroContextMismatch)?;
    FinancialModelMacroAssumptions::try_new(reference, premium, rate_kind, identifier)
        .map_err(|_| RecommendationFinancialModelAdapterError::InvalidProposalEvidence)
}

/// Adapts the one strict 365-day recommendation backtest into proposal evidence.
///
/// The mapping retains the qualified dataset, preauthorized signal cohort, execution-cost policy,
/// fold stability, actual snapshot/publication timing, and aggregate identities. Exact source metrics
/// remain in that bound study. The integer-basis-point comparison projects return downward and
/// drawdown upward, so quantization cannot turn a failing financial threshold into a passing one.
/// Partial aggregates and values outside the integer contract remain unavailable.
pub(crate) fn adapt_recommendation_backtest_v1(
    evidence: &GovernedRecommendationBacktestEvidenceV1,
) -> Result<RecommendationBacktestProposalEvidence, RecommendationBacktestAdapterError> {
    let aggregate = match evidence.aggregate() {
        RecommendationAggregateEvidenceV1::Available(value) => value,
        RecommendationAggregateEvidenceV1::Unavailable(_) => {
            return Err(RecommendationBacktestAdapterError::IncompleteAggregate);
        }
    };
    let policy = evidence.policy();
    let execution = policy.execution_assumptions();
    let publication = evidence.publication();
    let qualification =
        RecommendationStudyQualification::try_new(evidence.basis(), evidence.limitations())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidProposalEvidence)?;
    let observations = NonZeroU32::new(
        u32::try_from(aggregate.observation_count())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?,
    )
    .ok_or(RecommendationBacktestAdapterError::InvalidCount)?;
    let trials = NonZeroU32::new(
        u32::try_from(aggregate.trial_count())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?,
    )
    .ok_or(RecommendationBacktestAdapterError::InvalidCount)?;
    let window = ProposalEvidenceWindow::try_from_derived(
        publication.simulation_cutoff(),
        evidence.snapshot_as_of(),
        publication.available_at(),
        publication.expires_at(),
        content(publication.digest().bytes())?,
    )
    .map_err(|_| RecommendationBacktestAdapterError::InvalidProposalEvidence)?;
    let historical_test = CostAdjustedBacktestEvidence::try_new(
        policy.subject_instrument_id(),
        policy.reporting_currency(),
        qualification,
        RECOMMENDATION_TARGET_HORIZON_NANOS_V1,
        conservative_basis_points(
            aggregate.cost_adjusted_total_return(),
            RoundingStrategy::ToNegativeInfinity,
        )?,
        conservative_basis_points(
            aggregate.worst_maximum_drawdown(),
            RoundingStrategy::ToPositiveInfinity,
        )?,
        execution.fee_basis_points(),
        execution.slippage_basis_points(),
        execution.maximum_random_slippage_basis_points(),
        observations,
        trials,
        aggregate.positive_fold_stability_ppm(),
        publication.simulation_cutoff(),
        content(evidence.dataset_identity().bytes())?,
        content(evidence.signal_plan_digest().bytes())?,
        content(aggregate.digest().bytes())?,
        content(evidence.digest().bytes())?,
        content(evidence.preauthorized_signal_plan_digest().bytes())?,
        content(policy.execution_assumption_digest().bytes())?,
        window,
    )
    .map_err(|_| RecommendationBacktestAdapterError::InvalidProposalEvidence)?;
    let study = evidence.study();
    let first_fold = study
        .folds()
        .first()
        .ok_or(RecommendationBacktestAdapterError::IncompleteAggregate)?;
    let last_fold = study
        .folds()
        .last()
        .ok_or(RecommendationBacktestAdapterError::IncompleteAggregate)?;
    let total_signals = NonZeroU32::new(
        u32::try_from(study.results().len())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?,
    )
    .ok_or(RecommendationBacktestAdapterError::InvalidCount)?;
    let fold_count = NonZeroU32::new(
        u32::try_from(study.folds().len())
            .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?,
    )
    .ok_or(RecommendationBacktestAdapterError::InvalidCount)?;
    let completion_coverage_ppm = u32::try_from(
        u64::from(observations.get())
            .checked_mul(1_000_000)
            .ok_or(RecommendationBacktestAdapterError::InvalidCount)?
            / u64::from(total_signals.get()),
    )
    .map_err(|_| RecommendationBacktestAdapterError::InvalidCount)?;
    let out_of_sample = ChronologicalOutOfSampleEvidence::try_new(
        policy.subject_instrument_id(),
        policy.reporting_currency(),
        qualification,
        RECOMMENDATION_TARGET_HORIZON_NANOS_V1,
        first_fold.starts_at(),
        last_fold.ends_at(),
        publication.simulation_cutoff(),
        observations,
        total_signals,
        fold_count,
        completion_coverage_ppm,
        content(evidence.dataset_identity().bytes())?,
        content(evidence.signal_plan_digest().bytes())?,
        content(aggregate.digest().bytes())?,
        content(evidence.digest().bytes())?,
        window,
    )
    .map_err(|_| RecommendationBacktestAdapterError::InvalidProposalEvidence)?;
    Ok(RecommendationBacktestProposalEvidence {
        historical_test,
        out_of_sample,
    })
}

fn conservative_basis_points(
    value: Decimal,
    direction: RoundingStrategy,
) -> Result<BasisPoints, RecommendationBacktestAdapterError> {
    let scaled = value
        .checked_mul(Decimal::from(10_000_u32))
        .ok_or(RecommendationBacktestAdapterError::UnrepresentableBasisPoints)?
        .round_dp_with_strategy(0, direction)
        .normalize();
    let integer = i32::try_from(scaled.mantissa())
        .map_err(|_| RecommendationBacktestAdapterError::UnrepresentableBasisPoints)?;
    Ok(BasisPoints::new(integer))
}

fn content(bytes: [u8; 32]) -> Result<DecisionContentDigest, RecommendationBacktestAdapterError> {
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
        .map_err(|_| RecommendationBacktestAdapterError::InvalidIdentity)
}
