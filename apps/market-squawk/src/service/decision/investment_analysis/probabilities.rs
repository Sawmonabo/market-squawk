//! Pure display of each original saved event forecast; no probability inference or current lookup.

use market_squawk_data::ProbabilityEventTarget;
use market_squawk_decisions::{
    InvestmentAnalysisEvidence, ProbabilityEventEvidence, ProbabilityUnavailableReason,
};
use market_squawk_modeling::ForecastTargetMeaning;
use market_squawk_services::ServiceError;
use rust_decimal::Decimal;
use serde_json::{Value, json};

pub(super) fn value(evidence: &InvestmentAnalysisEvidence) -> Result<Value, ServiceError> {
    let group = evidence.probabilities();
    Ok(json!({
        "priceHigher": event(group.map(|v| v.price_higher()))?,
        "benchmarkOutperformance": event(group.map(|v| v.benchmark_outperformance()))?,
        "profitAfterCosts": event(group.map(|v| v.profit_after_costs()))?,
    }))
}

fn event(value: Option<&ProbabilityEventEvidence>) -> Result<Value, ServiceError> {
    let target = value.and_then(ProbabilityEventEvidence::target);
    let assumptions = assumptions(target);
    let benchmark = match target {
        Some(ForecastTargetMeaning::FixedHorizonEvent {
            event:
                ProbabilityEventTarget::BenchmarkOutperformance {
                    benchmark_instrument_id,
                    benchmark_definition,
                },
            ..
        }) => json!({
            "instrumentId": benchmark_instrument_id.to_string(),
            "definitionAlgorithm": match benchmark_definition.algorithm() { market_squawk_domain::DigestAlgorithm::Sha256 => "sha256", market_squawk_domain::DigestAlgorithm::Blake3 => "blake3" },
            "definitionDigest": benchmark_definition.bytes().iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        }),
        _ => Value::Null,
    };
    match value {
        Some(ProbabilityEventEvidence::Ready(value)) => {
            let record = value.record();
            let probability = Decimal::try_from_i128_with_scale(
                record.probability.mantissa(),
                u32::from(record.probability.scale()),
            )
            .map_err(|_| ServiceError::InvalidResult)?;
            let calibration = &record.calibration;
            Ok(json!({
                "state": "available",
                "probabilityPercent": super::percentage_from_decimal_ratio(probability)?,
                "observedAt": super::super::product_timestamp(record.observed_at),
                "endsAt": super::super::product_timestamp(record.target_at),
                "expiresAt": super::super::product_timestamp(record.window.expires_at()),
                "assumptions": assumptions,
                "benchmark": benchmark,
                "calibration": {
                    "evaluatedFrom": super::super::product_timestamp(calibration.evaluation_window.start().ok_or(ServiceError::InvalidResult)?),
                    "evaluatedThrough": super::super::product_timestamp(calibration.evaluation_window.end().ok_or(ServiceError::InvalidResult)?),
                    "completedOutcomes": calibration.evaluation_window.observations().get(),
                    "brierScore": calibration.brier_score(),
                    "logLoss": calibration.log_loss(),
                },
            }))
        }
        _ => {
            let summary = match value {
                Some(ProbabilityEventEvidence::Unavailable { reason, .. }) => unavailable(*reason),
                _ => "No independent event forecast was saved with this analysis.",
            };
            Ok(
                json!({"state": "unavailable", "summary": summary, "assumptions": assumptions, "benchmark": benchmark}),
            )
        }
    }
}

fn assumptions(target: Option<ForecastTargetMeaning>) -> Vec<String> {
    let Some(ForecastTargetMeaning::FixedHorizonEvent { event, .. }) = target else {
        return Vec::new();
    };
    match event {
        ProbabilityEventTarget::PriceHigher => vec![
            "Split-adjusted ending price exceeds the starting price; cash dividends are excluded.".into(),
        ],
        ProbabilityEventTarget::BenchmarkOutperformance { .. } => vec![
            "The investment's split-adjusted price return exceeds the selected benchmark over the same dates; cash dividends are excluded.".into(),
        ],
        ProbabilityEventTarget::ProfitAfterCosts { policy } => {
            let mut values = vec![
                "A long entry and exit produce positive total wealth after modeled trading costs, including supported distributions and entitlements.".into(),
                format!("Fee per fill: {}%. Slippage: {}%. Maximum random slippage: {}%.",
                    super::percentage_from_basis_points(policy.fee_basis_points),
                    super::percentage_from_basis_points(policy.slippage_basis_points),
                    super::percentage_from_basis_points(policy.maximum_random_slippage_basis_points)),
                format!("{} lots, reported in {}. Maximum participation: {}%. Partial fills {}.", policy.quantity_lots,
                    policy.reporting_currency.as_str(), super::percentage_from_basis_points(policy.maximum_participation_basis_points),
                    if policy.allow_partial_fills { "allowed" } else { "not allowed" }),
                format!("Execution delay: {} seconds. Maximum entry delay: {} seconds; maximum exit delay: {} seconds.",
                    seconds(policy.latency_nanos), seconds(policy.maximum_entry_lag_nanos), seconds(policy.maximum_exit_lag_nanos)),
            ];
            values.push(match policy.daily_bar_assumed_spread_basis_points {
                Some(spread) => format!("Completed daily-bar simulation with an assumed spread of {}%.", super::percentage_from_basis_points(spread)),
                None => "Simulation uses observed quotes and depth.".into(),
            });
            values
        }
    }
}

fn seconds(nanos: i64) -> String {
    Decimal::from_i128_with_scale(i128::from(nanos), 9)
        .normalize()
        .to_string()
}

fn unavailable(reason: ProbabilityUnavailableReason) -> &'static str {
    match reason {
        ProbabilityUnavailableReason::ForecastEvidenceUnavailable => {
            "An independent forecast for this event was not available."
        }
        ProbabilityUnavailableReason::SourceEvidenceUnavailable => {
            "The required source observations were unavailable."
        }
        ProbabilityUnavailableReason::BenchmarkEvidenceUnavailable => {
            "The selected benchmark's matching evidence was unavailable."
        }
        ProbabilityUnavailableReason::CostEvidenceUnavailable => {
            "The required trading-cost evidence was unavailable."
        }
        ProbabilityUnavailableReason::CalibrationUnavailable => {
            "Held-out probability calibration was unavailable."
        }
        ProbabilityUnavailableReason::HorizonMismatch => {
            "The forecast used a different investment horizon."
        }
        ProbabilityUnavailableReason::OriginMismatch => {
            "The forecast used a different starting observation."
        }
        ProbabilityUnavailableReason::ForecastExpired => {
            "The forecast had expired when this analysis was saved."
        }
        ProbabilityUnavailableReason::ForecastFailed => {
            "The independent event forecast did not complete successfully."
        }
    }
}
