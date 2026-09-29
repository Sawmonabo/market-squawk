//! Shared input/output shapes for finite financial settings and their semantic commitments.

use serde_json::{Value, json};

use super::{
    boolean, bounded_array, bounded_integer_range, bounded_text, bounded_unsigned,
    bounded_unsigned_range, closed_complete, constant, constant_bool, enumeration, fixed_array,
    nullable, one_of, positive_integer_text, sha256, unsigned_integer_text, uuid,
};

pub(crate) fn configuration() -> Value {
    closed_complete(vec![
        (
            "supportedInvestmentPolicy",
            enumeration(&["listed_equities_and_etfs_v1", "listed_equities_v1"]),
        ),
        (
            "historicalDatasetPolicy",
            constant("split_adjusted_qualified_history_v1"),
        ),
        (
            "requiredFeatureSet",
            constant("price_return_macro_and_patterns_v1"),
        ),
        ("modelBundlePolicy", model_policy()),
        (
            "trainingCalibrationPolicy",
            constant("chronological_rolling_origin_v1"),
        ),
        ("forecastHorizonPolicy", constant("elapsed_365_days_v1")),
        (
            "valuationPolicy",
            constant("all_admitted_automatic_methods_v1"),
        ),
        ("backtestCostPolicy", constant("conservative_round_trip_v1")),
        (
            "recommendationPolicyParameters",
            recommendation_parameters(),
        ),
        (
            "riskFreshnessAbstentionPolicy",
            constant("mandatory_evidence_v1"),
        ),
        (
            "portfolioRiskMinimumDailyReturns",
            bounded_unsigned_range(252, 1260),
        ),
        (
            "portfolioRiskMaximumDailyReturns",
            bounded_unsigned_range(252, 1260),
        ),
    ])
}

pub(crate) fn resolution() -> Value {
    closed_complete(vec![
        ("configuration", configuration()),
        ("configurationDigest", sha256()),
        (
            "components",
            fixed_array(
                closed_complete(vec![
                    ("family", family()),
                    ("identity", bounded_text(128)),
                    ("version", constant("1")),
                    ("digest", sha256()),
                    ("label", bounded_text(128)),
                ]),
                10,
            ),
        ),
        ("recommendationPolicyDigest", sha256()),
    ])
}

pub(super) fn benchmark_choices() -> Value {
    bounded_array(
        closed_complete(vec![
            ("instrumentId", uuid()),
            ("displayName", bounded_text(512)),
            ("symbol", bounded_text(32)),
            ("comparisonDescription", bounded_text(521)),
            ("isDefault", boolean()),
        ]),
        3,
    )
}

pub(super) fn catalog() -> Value {
    closed_complete(vec![
        ("nextCursor", nullable(bounded_text(512))),
        ("benchmarkChoices", benchmark_choices()),
        ("defaultConfiguration", configuration()),
        ("defaultResolution", resolution()),
        (
            "components",
            fixed_array(
                closed_complete(vec![
                    ("family", family()),
                    ("label", bounded_text(128)),
                    ("description", bounded_text(1024)),
                    (
                        "choices",
                        bounded_array(
                            closed_complete(vec![
                                ("id", bounded_text(128)),
                                ("label", bounded_text(256)),
                            ]),
                            2,
                        ),
                    ),
                    ("editableFields", bounded_array(bounded_text(128), 32)),
                ]),
                10,
            ),
        ),
        (
            "models",
            bounded_array(
                closed_complete(vec![
                    ("id", uuid()),
                    ("label", bounded_text(256)),
                    ("selection", model_policy()),
                    (
                        "identity",
                        closed_complete(vec![
                            ("modelToken", uuid()),
                            ("metadataSha256", sha256()),
                            ("artifactSha256", sha256()),
                            ("datasetExportSha256", sha256()),
                            ("datasetPolicySha256", sha256()),
                            (
                                "featureCount",
                                bounded_unsigned_range(1, u64::from(u32::MAX)),
                            ),
                        ]),
                    ),
                ]),
                100,
            ),
        ),
        (
            "historicalCosts",
            closed_complete(vec![
                ("feesBps", bounded_unsigned(u64::from(u32::MAX))),
                ("slippageBps", bounded_unsigned(u64::from(u32::MAX))),
                (
                    "maximumRandomSlippageBps",
                    bounded_unsigned(u64::from(u32::MAX)),
                ),
                ("maximumParticipationBps", bounded_unsigned(10000)),
                ("latencyNanos", {
                    let mut schema = unsigned_integer_text();
                    schema["maxLength"] = json!(20);
                    schema
                }),
                ("allowPartialFills", boolean()),
                ("feeDecimalScale", bounded_unsigned(28)),
                ("appliesToEntryAndExit", constant_bool(true)),
            ]),
        ),
        ("analysisOnly", constant_bool(true)),
    ])
}

fn model_policy() -> Value {
    one_of(vec![
        closed_complete(vec![("kind", constant("best_admitted_calibrated_mean_v1"))]),
        closed_complete(vec![("kind", constant("exact")), ("modelToken", uuid())]),
    ])
}

fn family() -> Value {
    enumeration(&[
        "supported_investment_policy",
        "historical_dataset_policy",
        "required_feature_set",
        "model_bundle_policy",
        "training_calibration_policy",
        "forecast_horizon_policy",
        "valuation_policy",
        "backtest_cost_policy",
        "recommendation_policy",
        "risk_freshness_abstention_policy",
    ])
}

fn recommendation_parameters() -> Value {
    let mut fields = Vec::with_capacity(34);
    fields.push(("allow_retrospective_studies", boolean()));
    let mut duration = positive_integer_text();
    duration["maxLength"] = json!(19);
    for name in [
        "proposal_lifetime_nanos",
        "market_max_age_nanos",
        "forecast_max_age_nanos",
        "valuation_max_age_nanos",
        "financial_model_max_age_nanos",
        "backtest_max_age_nanos",
        "out_of_sample_max_age_nanos",
        "harmonic_pattern_max_age_nanos",
        "liquidity_max_age_nanos",
        "portfolio_risk_max_age_nanos",
    ] {
        fields.push((name, duration.clone()));
    }
    for name in [
        "bullish_threshold",
        "bearish_threshold",
        "minimum_cost_adjusted_return",
        "maximum_backtest_drawdown",
        "maximum_liquidity_spread",
    ] {
        fields.push((
            name,
            bounded_integer_range(i64::from(i32::MIN), i64::from(i32::MAX)),
        ));
    }
    for name in [
        "minimum_forecast_outcomes",
        "minimum_backtest_observations",
        "minimum_backtest_trials",
    ] {
        fields.push((name, bounded_unsigned_range(1, u64::from(u32::MAX))));
    }
    for name in [
        "minimum_nominal_forecast_coverage_ppm",
        "maximum_nominal_forecast_coverage_ppm",
        "minimum_realized_forecast_coverage_ppm",
        "maximum_forecast_calibration_error_ppm",
        "minimum_backtest_stability_ppm",
        "minimum_oos_completion_coverage_ppm",
        "minimum_liquidity_capacity_ppm",
        "minimum_portfolio_risk_capacity_ppm",
        "minimum_confidence_ppm",
    ] {
        fields.push((name, bounded_unsigned(1_000_000)));
    }
    fields.extend([
        ("forecast_base_weight_bps", bounded_unsigned(10_000)),
        ("valuation_weight_bps", bounded_unsigned(10_000)),
        (
            "confidence_weights_ppm",
            fixed_array(bounded_unsigned(1_000_000), 6),
        ),
        (
            "price_range_weights_bps",
            fixed_array(bounded_unsigned(10_000), 9),
        ),
        ("price_scale", bounded_unsigned(28)),
        (
            "rounding_policy",
            enumeration(&[
                "nearest_even",
                "away_from_zero",
                "toward_zero",
                "floor",
                "ceiling",
            ]),
        ),
    ]);
    closed_complete(fields)
}
