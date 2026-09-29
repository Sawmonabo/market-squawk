//! Product-language controls over the existing finite financial component constructors.

use market_squawk_services::ServiceError;
use serde_json::{Value, json};

use super::{AnalyticalProfileConfiguration, AnalyticalProfileError, model_choices, resolve};
use crate::application::model::forecast_preparation::ForecastPreparationCatalog;

/// Bounded discovery; model choices are borrowed from the sole forecast authority per request.
pub(crate) fn catalog(
    models: Option<&ForecastPreparationCatalog>,
    benchmark_choices: &[crate::application::research::BenchmarkComparisonChoice],
) -> Result<Value, AnalyticalProfileError> {
    let default = resolve(None, models)?;
    let configuration = &default.resolution().configuration;
    let cost = default.execution_assumptions();
    let available_models = models
        .map(|models| model_choices(models, default.horizon()))
        .unwrap_or_default();
    let mut components = Vec::with_capacity(10);
    for receipt in &default.resolution().components {
        let family =
            serde_json::to_value(receipt.family).map_err(|_| AnalyticalProfileError::Encoding)?;
        let (description, choices, editable_fields) = controls(
            family.as_str().ok_or(AnalyticalProfileError::Encoding)?,
            configuration,
        )?;
        components.push(json!({
            "family": receipt.family,
            "label": receipt.label,
            "description": description,
            "choices": choices,
            "editableFields": editable_fields,
        }));
    }
    Ok(json!({
        "benchmarkChoices": benchmark_choices,
        "defaultConfiguration": configuration,
        "defaultResolution": default.resolution(),
        "components": components,
        "models": available_models,
        "nextCursor": models.and_then(ForecastPreparationCatalog::next_cursor),
        "historicalCosts": {
            "feesBps": cost.fee_basis_points(),
            "slippageBps": cost.slippage_basis_points(),
            "maximumRandomSlippageBps": cost.maximum_random_slippage_basis_points(),
            "maximumParticipationBps": cost.maximum_participation_basis_points(),
            "latencyNanos": cost.latency_nanos().to_string(),
            "allowPartialFills": cost.allow_partial_fills(),
            "feeDecimalScale": cost.fee_decimal_scale(),
            "appliesToEntryAndExit": true,
        },
        "analysisOnly": true,
    }))
}

type Controls = (&'static str, Vec<Value>, &'static [&'static str]);

fn controls(
    family: &str,
    configuration: &AnalyticalProfileConfiguration,
) -> Result<Controls, AnalyticalProfileError> {
    let fixed = |id: Value, label: &str| vec![json!({"id": id, "label": label})];
    Ok(match family {
        "supported_investment_policy" => (
            "Choose listed companies alone or include exchange-traded funds.",
            vec![
                json!({"id":"listed_equities_and_etfs_v1", "label":"Listed companies and exchange-traded funds"}),
                json!({"id":"listed_equities_v1", "label":"Listed companies"}),
            ],
            &[],
        ),
        "historical_dataset_policy" => (
            "Keep data as known at the time separate from simulations using today's historical data. Later revisions and any assumptions remain visible in the results.",
            fixed(
                json!(configuration.historical_dataset_policy),
                "Historical data with stated limits",
            ),
            &["allow_retrospective_studies"],
        ),
        "required_feature_set" => (
            "Combine price returns, economic conditions, interest rates, and causal price-pattern evidence.",
            fixed(
                json!(configuration.required_feature_set),
                "Price, economic, and pattern evidence",
            ),
            &[],
        ),
        "model_bundle_policy" => (
            "Use an admitted calibrated one-year mean forecast. Automatic selection prefers the newest compatible training history; it makes no performance guarantee.",
            fixed(
                json!("best_admitted_calibrated_mean_v1"),
                "Automatic calibrated forecast",
            ),
            &[],
        ),
        "training_calibration_policy" => (
            "Keep training separate from chronological evaluation and require observed forecast coverage before recommendations qualify.",
            fixed(
                json!(configuration.training_calibration_policy),
                "Chronological training and rolling forecast evaluation",
            ),
            &[
                "minimum_forecast_outcomes",
                "minimum_nominal_forecast_coverage_ppm",
                "maximum_nominal_forecast_coverage_ppm",
                "minimum_realized_forecast_coverage_ppm",
                "maximum_forecast_calibration_error_ppm",
            ],
        ),
        "forecast_horizon_policy" => (
            "Compare forecasts, historical outcomes, and recommendations at the same exact one-year horizon.",
            fixed(
                json!(configuration.forecast_horizon_policy),
                "365 elapsed days",
            ),
            &[],
        ),
        "valuation_policy" => (
            "Evaluate cash flows, comparable companies, residual income, and probability-weighted forecasts only when their respective financial evidence qualifies.",
            fixed(
                json!(configuration.valuation_policy),
                "All supported automatic financial methods",
            ),
            &["valuation_max_age_nanos", "financial_model_max_age_nanos"],
        ),
        "backtest_cost_policy" => (
            "Use the same conservative research cost assumptions for historical entry and exit and a modeled trading-cost allowance for current sizing. Current sizing uses supplied bid/ask depth and preserves the cash reserve; it does not quote broker fees or future exit costs.",
            fixed(
                json!(configuration.backtest_cost_policy),
                "Modeled trading-cost allowance",
            ),
            &[
                "minimum_backtest_observations",
                "minimum_backtest_trials",
                "minimum_backtest_stability_ppm",
                "minimum_oos_completion_coverage_ppm",
                "minimum_cost_adjusted_return",
                "maximum_backtest_drawdown",
            ],
        ),
        "recommendation_policy" => (
            "Combine independent forecast and valuation evidence under explicit action thresholds, ordered price ranges, and measured evidence reliability.",
            fixed(
                json!("recommendation_v1"),
                "Evidence-based investment rules",
            ),
            &[
                "bullish_threshold",
                "bearish_threshold",
                "forecast_base_weight_bps",
                "valuation_weight_bps",
                "confidence_weights_ppm",
                "price_range_weights_bps",
                "price_scale",
                "rounding_policy",
            ],
        ),
        "risk_freshness_abstention_policy" => (
            "Require current evidence. Portfolio scenarios use 252 to 1,260 complete daily returns and estimate one-session losses at 95% confidence. Missing or conflicting evidence prevents action.",
            fixed(
                json!(configuration.risk_freshness_abstention_policy),
                "Current evidence and explicit abstention",
            ),
            &[
                "proposal_lifetime_nanos",
                "market_max_age_nanos",
                "forecast_max_age_nanos",
                "backtest_max_age_nanos",
                "out_of_sample_max_age_nanos",
                "harmonic_pattern_max_age_nanos",
                "liquidity_max_age_nanos",
                "portfolio_risk_max_age_nanos",
                "portfolio_risk_minimum_daily_returns",
                "portfolio_risk_maximum_daily_returns",
                "maximum_liquidity_spread",
                "minimum_liquidity_capacity_ppm",
                "minimum_portfolio_risk_capacity_ppm",
                "minimum_confidence_ppm",
            ],
        ),
        _ => return Err(AnalyticalProfileError::Encoding),
    })
}

impl From<AnalyticalProfileError> for ServiceError {
    fn from(error: AnalyticalProfileError) -> Self {
        match error {
            AnalyticalProfileError::InvalidPolicy => Self::InvalidRequest,
            AnalyticalProfileError::ModelUnavailable => Self::Unavailable,
            AnalyticalProfileError::IdentityMismatch => Self::InvalidRequest,
            AnalyticalProfileError::Encoding => Self::InvalidResult,
        }
    }
}
