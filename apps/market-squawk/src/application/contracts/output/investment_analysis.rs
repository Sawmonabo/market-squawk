//! Provider-neutral saved investment analyses and comparable realized outcome history.

use market_squawk_decisions::RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED;
use serde_json::{Value, json};

use super::{
    bounded_array, bounded_nonempty_array, bounded_text, bounded_unsigned, bounded_unsigned_range,
    canonical_decimal_text, canonical_market_timestamp, closed, closed_complete, constant,
    constant_bool, constant_unsigned, enumeration, fixed_array,
    investment_analysis_currency as currency, investment_analysis_money as money,
    investment_analysis_sha256 as sha256, nullable, one_of, unsigned_integer_text,
};

pub(super) fn result() -> Value {
    closed_complete(vec![
        ("actionToken", action_token()),
        ("investment", display()),
        ("portfolioLabel", bounded_text(128)),
        ("currency", currency()),
        ("recommendation", recommendation()),
        ("horizon", horizon()),
        ("priceSummary", price_summary()),
        ("chart", json!({"type":"null"})),
        ("chartAvailable", json!({"type":"boolean"})),
        ("probabilities", saved_probabilities()),
        ("reasons", bounded_nonempty_array(product_text(), 32)),
        ("risks", bounded_array(product_text(), 32)),
        ("assumptions", bounded_array(product_text(), 32)),
        ("invalidators", bounded_array(product_text(), 32)),
        ("evidenceSummary", evidence_summary()),
        ("analyticalEvidence", analytical_evidence()),
        ("liquidity", liquidity()),
        ("portfolioContext", portfolio_context()),
        ("virtualPaperEligibility", virtual_paper_eligibility()),
        ("outcomeProjection", nullable(outcome_projection())),
        ("sizing", sizing()),
        ("expectedReturn", expected_return()),
        ("realizedOutcome", nullable(realized_outcome_current())),
        ("trackRecordActionToken", nullable(action_token())),
    ])
}

/// Exact saved generation acknowledgment, shared by fresh publication and immutable retry.
pub(super) fn generated_result() -> Value {
    closed_complete(vec![
        ("actionToken", action_token()),
        ("analysisId", sha256()),
        ("explanationDigest", sha256()),
        ("publishedAtUnixNanos", timestamp_nanos_text()),
        ("valuationMethodSetIdentity", nullable(sha256())),
        ("outcomeProjection", nullable(outcome_projection())),
        ("sizing", sizing()),
        ("expectedReturn", expected_return()),
    ])
}

fn action_token() -> Value {
    json!({
        "type": "string",
        "format": "uuid",
        "not": {
            "type": "string",
            "const": "00000000-0000-0000-0000-000000000000",
        },
    })
}

fn display() -> Value {
    closed_complete(vec![
        ("symbol", nullable(bounded_text(64))),
        ("name", nullable(product_text())),
    ])
}

fn recommendation() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("action")),
            ("action", action()),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("abstain")),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", product_text()),
        ]),
    ])
}

fn horizon() -> Value {
    closed_complete(vec![
        ("informationCurrentThrough", canonical_market_timestamp()),
        ("endsAt", canonical_market_timestamp()),
        ("expiresAt", canonical_market_timestamp()),
    ])
}

fn price_summary() -> Value {
    closed_complete(vec![
        ("current", nullable(money())),
        ("fairValue", nullable(money())),
        ("valuationMethods", nullable(valuation_method_set())),
        ("scenarios", nullable(scenario_ranges())),
        ("actionRanges", nullable(action_ranges())),
    ])
}

fn timestamp_nanos_text() -> Value {
    json!({
        "type": "string", "minLength": 1, "maxLength": 20,
        "pattern": "^(?:0|-?[1-9][0-9]*)$",
    })
}

fn valuation_method_set() -> Value {
    closed_complete(vec![
        ("sourceCutoffUnixNanos", timestamp_nanos_text()),
        ("marketCutoffUnixNanos", timestamp_nanos_text()),
        ("completedAtUnixNanos", timestamp_nanos_text()),
        (
            "methods",
            json!({
                "type": "array", "minItems": 4, "maxItems": 4,
                "prefixItems": [
                    valuation_method("discounted_cash_flow"),
                    valuation_method("comparable_companies"),
                    valuation_method("residual_income"),
                    valuation_method("forecast_distribution"),
                ],
                "items": false,
            }),
        ),
    ])
}

fn valuation_method(method: &'static str) -> Value {
    one_of(vec![
        closed_complete(vec![
            ("method", constant(method)),
            ("status", constant("unavailable")),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("method", constant(method)),
            ("status", constant("calculated")),
            (
                "basis",
                enumeration(&[
                    "per_instrument_unit",
                    "total_common_equity",
                    "reporting_entity_total",
                    "position_total",
                ]),
            ),
            ("lower", signed_money()),
            ("central", signed_money()),
            ("upper", signed_money()),
            (
                "recommendationUse",
                enumeration(&[
                    "selected",
                    "not_per_instrument_unit",
                    "share_unit_basis_unproven",
                    "another_method_selected",
                    "admission_unavailable",
                ]),
            ),
            (
                "terminalGrowth",
                if method == "discounted_cash_flow" {
                    closed_complete(vec![
                        ("uncapped", canonical_decimal_text()),
                        ("riskFreeCap", canonical_decimal_text()),
                        ("applied", canonical_decimal_text()),
                    ])
                } else {
                    json!({"type": "null"})
                },
            ),
            (
                "residualTerminal",
                if method == "residual_income" {
                    closed_complete(vec![
                        ("condition", product_text()),
                        (
                            "explicitPeriods",
                            bounded_unsigned_range(1, u64::from(u32::MAX)),
                        ),
                        ("continuingValueSensitivity", canonical_decimal_text()),
                    ])
                } else {
                    json!({"type": "null"})
                },
            ),
        ]),
    ])
}

fn scenario_ranges() -> Value {
    closed_complete(vec![
        ("endsAt", canonical_market_timestamp()),
        ("downside", price_range()),
        ("base", price_range()),
        ("upside", price_range()),
    ])
}

fn action_ranges() -> Value {
    closed_complete(vec![
        ("entry", price_range()),
        ("add", price_range()),
        ("trim", price_range()),
        ("exit", price_range()),
    ])
}

fn price_range() -> Value {
    closed_complete(vec![("lower", money()), ("upper", money())])
}

fn evidence_summary() -> Value {
    closed_complete(vec![
        ("coverage", coverage()),
        ("calibration", calibration()),
        ("outOfSample", out_of_sample()),
        ("historicalTest", nullable(historical_test())),
        ("costs", cost_summary()),
        ("uncertainty", uncertainty()),
    ])
}

fn coverage() -> Value {
    closed_complete(vec![
        ("availableCount", bounded_unsigned(10)),
        ("possibleCount", constant_unsigned(10)),
        ("items", coverage_items()),
        ("summary", product_text()),
    ])
}

fn coverage_items() -> Value {
    let kinds = [
        "current_market",
        "broader_research",
        "price_pattern",
        "forecast",
        "financial_model",
        "valuation",
        "historical_test",
        "out_of_sample",
        "liquidity",
        "portfolio_risk",
    ];
    let items = kinds
        .into_iter()
        .map(|kind| {
            closed_complete(vec![
                ("kind", constant(kind)),
                ("state", enumeration(&["available", "unavailable"])),
            ])
        })
        .collect::<Vec<_>>();
    json!({
        "type": "array",
        "minItems": 10,
        "maxItems": 10,
        "prefixItems": items,
        "items": false,
    })
}

fn calibration() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("nominalCoveragePercent", percentage()),
            ("realizedCoveragePercent", percentage()),
            (
                "completedOutcomes",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", product_text()),
        ]),
    ])
}

fn historical_test() -> Value {
    closed_complete(vec![
        ("netReturnPercent", canonical_decimal_text()),
        ("maximumDrawdownPercent", percentage()),
        (
            "observations",
            bounded_unsigned_range(1, u64::from(u32::MAX)),
        ),
        ("trials", bounded_unsigned_range(1, u64::from(u32::MAX))),
        ("stabilityPercent", percentage()),
        ("evaluatedThrough", canonical_market_timestamp()),
        ("studyQualification", study_qualification()),
        ("summary", product_text()),
    ])
}

fn cost_summary() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("modeled")),
            ("feePercent", percentage()),
            ("slippagePercent", percentage()),
            ("maximumRandomSlippagePercent", percentage()),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", product_text()),
        ]),
    ])
}

fn uncertainty() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("evidenceReliabilityPercent", percentage()),
            ("reason", json!({"type": "null"})),
            (
                "applicablePolicyWeightPpm",
                bounded_unsigned_range(1, 1_000_000),
            ),
            ("components", fixed_array(uncertainty_component(), 6)),
            ("studyQualification", study_qualification()),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("evidenceReliabilityPercent", json!({"type": "null"})),
            (
                "reason",
                enumeration(&[
                    "buy_add_capacity_unavailable",
                    "trim_sell_capacity_unavailable",
                    "action_side_not_established",
                    "no_applicable_policy_weight",
                ]),
            ),
            ("applicablePolicyWeightPpm", bounded_unsigned(1_000_000)),
            ("components", fixed_array(uncertainty_component(), 6)),
            ("studyQualification", study_qualification()),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", product_text()),
        ]),
    ])
}

fn uncertainty_component() -> Value {
    one_of(vec![
        closed_complete(vec![
            (
                "kind",
                enumeration(&[
                    "forecast_calibration",
                    "valuation_agreement",
                    "backtest_stability",
                    "market_integrity",
                    "liquidity_capacity",
                    "portfolio_risk_capacity",
                ]),
            ),
            ("state", constant("available")),
            ("reliabilityPercent", percentage()),
            ("configuredWeightPpm", bounded_unsigned(1_000_000)),
            ("reason", json!({"type": "null"})),
        ]),
        closed_complete(vec![
            ("kind", constant("liquidity_capacity")),
            ("state", constant("unavailable")),
            ("reliabilityPercent", json!({"type": "null"})),
            ("configuredWeightPpm", bounded_unsigned(1_000_000)),
            (
                "reason",
                enumeration(&[
                    "buy_add_capacity_unavailable",
                    "trim_sell_capacity_unavailable",
                    "action_side_not_established",
                ]),
            ),
        ]),
        closed_complete(vec![
            ("kind", constant("liquidity_capacity")),
            ("state", constant("not_applicable")),
            ("reliabilityPercent", json!({"type": "null"})),
            ("configuredWeightPpm", bounded_unsigned(1_000_000)),
            ("reason", json!({"type": "null"})),
        ]),
    ])
}

fn outcome_projection() -> Value {
    closed_complete(vec![
        ("startingPrice", money()),
        ("endsAt", canonical_market_timestamp()),
        (
            "positionScale",
            nullable(closed_complete(vec![
                ("quantityLots", unsigned_integer_text()),
                ("summary", product_text()),
            ])),
        ),
        ("downside", price_change_range()),
        ("base", price_change_range()),
        ("upside", price_change_range()),
        ("entryDistance", zone_distance()),
        ("addDistance", zone_distance()),
        ("trimDistance", zone_distance()),
        ("exitDistance", zone_distance()),
        ("expectedReturn", expected_return()),
        ("expectedGrossPricePnl", expected_gross_price_pnl()),
        ("netPnl", unavailable_summary()),
        ("benchmarkReturn", unavailable_summary()),
        ("afterTaxPnl", unavailable_summary()),
        ("limitations", bounded_nonempty_array(product_text(), 8)),
    ])
}

fn price_change_range() -> Value {
    closed(
        vec![
            ("priceRange", price_range()),
            ("absolutePriceChange", signed_money_range()),
            ("exactPriceReturnRatio", exact_ratio_range()),
            ("grossPricePnl", gross_price_pnl()),
            (
                "priceChangePercent",
                closed_complete(vec![
                    ("lower", canonical_decimal_text()),
                    ("upper", canonical_decimal_text()),
                ]),
            ),
        ],
        &[
            "priceRange",
            "absolutePriceChange",
            "exactPriceReturnRatio",
            "grossPricePnl",
        ],
    )
}

fn zone_distance() -> Value {
    closed_complete(vec![
        ("priceRange", price_range()),
        ("absolutePriceChange", signed_money_range()),
        ("exactPriceReturnRatio", exact_ratio_range()),
    ])
}

fn exact_ratio() -> Value {
    closed_complete(vec![
        ("numerator", signed_money()),
        ("denominator", money()),
    ])
}

fn exact_ratio_range() -> Value {
    closed_complete(vec![("lower", exact_ratio()), ("upper", exact_ratio())])
}

fn sizing() -> Value {
    one_of(vec![
        sizing_projection(),
        closed_complete(vec![
            ("state", constant("unavailable")),
            (
                "reason",
                enumeration(&[
                    "no_generated_proposal",
                    "price_not_on_execution_tick",
                    "exact_portfolio_lots_unavailable",
                ]),
            ),
            ("summary", product_text()),
        ]),
    ])
}

fn sizing_projection() -> Value {
    closed_complete(vec![
        ("state", constant("evaluated")),
        ("evaluatedAt", canonical_market_timestamp()),
        ("currentLots", unsigned_integer_text()),
        ("markedEquity", money()),
        ("settlementAvailableCash", nullable(signed_money())),
        ("perLotNotional", money()),
        ("perLotDownsideLoss", nonnegative_money()),
        ("constraintCaps", fixed_array(sizing_cap(), 6)),
        ("hardFeasibleLots", feasible_lots()),
        ("preferredFeasibleLots", feasible_lots()),
        ("hardFeasibleTargetNotional", feasible_notional()),
        ("preferredFeasibleTargetNotional", feasible_notional()),
        ("hardBindingCaps", bounded_array(sizing_kind(), 5)),
        ("preferredBindingCaps", bounded_array(sizing_kind(), 6)),
        (
            "preferredWeightRounding",
            closed_complete(vec![
                ("lowerRoundUpExcess", nonnegative_money()),
                ("upperRoundDownRemainder", nonnegative_money()),
            ]),
        ),
        ("summary", product_text()),
    ])
}

fn sizing_kind() -> Value {
    enumeration(&[
        "cash_reserve",
        "downside_loss",
        "liquidity",
        "portfolio_risk",
        "forward_cost",
        "preferred_weight",
    ])
}

fn sizing_cap() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", sizing_kind()),
            ("state", constant("available")),
            ("lower", unsigned_integer_text()),
            ("upper", unsigned_integer_text()),
        ]),
        closed_complete(vec![
            ("kind", sizing_kind()),
            ("state", constant("unavailable")),
            ("summary", product_text()),
        ]),
    ])
}

fn nonnegative_money() -> Value {
    closed_complete(vec![
        ("amount", nonnegative_decimal()),
        ("currency", currency()),
    ])
}

fn feasible_notional() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("available")),
            ("lower", nonnegative_money()),
            ("upper", nonnegative_money()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("reasons", bounded_nonempty_array(product_text(), 8)),
        ]),
    ])
}

fn feasible_lots() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("available")),
            ("lower", unsigned_integer_text()),
            ("upper", unsigned_integer_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("reasons", bounded_nonempty_array(product_text(), 8)),
        ]),
    ])
}

fn realized_outcome_current() -> Value {
    closed_complete(vec![
        ("evaluatedAt", canonical_market_timestamp()),
        ("result", realized_outcome_result()),
    ])
}

fn realized_outcome_result() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("pending")),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("completed")),
            ("metric", constant("gross_instrument_price_return")),
            ("startMark", money()),
            ("endpointPrice", money()),
            ("grossPriceReturnPercent", canonical_decimal_text()),
            ("observedAt", canonical_market_timestamp()),
            ("availableAt", canonical_market_timestamp()),
            ("limitations", bounded_nonempty_array(product_text(), 8)),
        ]),
    ])
}

pub(super) fn track_record() -> Value {
    closed_complete(vec![
        ("actionToken", action_token()),
        ("evaluatedAt", canonical_market_timestamp()),
        (
            "unavailableAnalysisCount",
            bounded_unsigned(u64::from(u32::MAX)),
        ),
        (
            "minimumCompletedSamples",
            constant_unsigned(u64::from(RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED)),
        ),
        ("minimumCoveragePercent", constant("80")),
        ("groups", track_record_groups()),
        ("forecastCalibrationIncluded", constant_bool(false)),
        ("executionResultsIncluded", constant_bool(false)),
        ("summary", product_text()),
    ])
}

fn track_record_groups() -> Value {
    json!({
        "type": "array",
        "minItems": 6,
        "maxItems": 6,
        "prefixItems": [
            track_record_group("buy"),
            track_record_group("add"),
            track_record_group("hold"),
            track_record_group("trim"),
            track_record_group("sell"),
            track_record_group("abstain"),
        ],
        "items": false,
    })
}

fn track_record_group(action: &'static str) -> Value {
    closed_complete(vec![
        ("action", constant(action)),
        ("recommendationCount", bounded_unsigned(u64::from(u32::MAX))),
        ("dueCount", bounded_unsigned(u64::from(u32::MAX))),
        ("completedCount", bounded_unsigned(u64::from(u32::MAX))),
        ("pendingCount", bounded_unsigned(u64::from(u32::MAX))),
        ("unavailableCount", bounded_unsigned(u64::from(u32::MAX))),
        ("coveragePercent", percentage()),
        ("performance", track_record_performance()),
    ])
}

fn track_record_performance() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", product_text()),
            (
                "required",
                constant_unsigned(u64::from(RECOMMENDATION_TRACK_RECORD_MINIMUM_COMPLETED)),
            ),
            ("actual", bounded_unsigned(u64::from(u32::MAX))),
        ]),
        closed_complete(vec![
            ("kind", constant("unavailable")),
            ("summary", product_text()),
            ("requiredPercent", constant("80")),
            ("actualPercent", percentage()),
        ]),
        closed_complete(vec![
            ("kind", constant("available")),
            ("meanGrossPriceReturnPercent", canonical_decimal_text()),
            ("positiveOutcomes", bounded_unsigned(u64::from(u32::MAX))),
            ("unchangedOutcomes", bounded_unsigned(u64::from(u32::MAX))),
            ("negativeOutcomes", bounded_unsigned(u64::from(u32::MAX))),
            ("summary", product_text()),
        ]),
    ])
}

pub(super) fn page() -> Value {
    closed_complete(vec![
        ("completeness", enumeration(&["complete", "truncated"])),
        ("returnedCount", bounded_unsigned(1_000)),
        ("availableCount", bounded_unsigned(4_096)),
        ("nextAfterActionToken", nullable(action_token())),
        ("analyses", bounded_array(locator(), 1_000)),
    ])
}

fn locator() -> Value {
    closed_complete(vec![
        ("actionToken", action_token()),
        ("investment", display()),
        ("portfolioLabel", bounded_text(128)),
        ("currency", currency()),
        ("horizon", horizon()),
        ("recommendation", recommendation()),
    ])
}

fn action() -> Value {
    enumeration(&["buy", "add", "hold", "trim", "sell"])
}

fn product_text() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": 2_048,
        "pattern": "\\S",
    })
}

fn percentage() -> Value {
    json!({
        "type": "string",
        "pattern": "^(?:(?:0|[1-9]|[1-9][0-9])(?:\\.[0-9]*[1-9])?|100)$",
    })
}

fn unavailable_summary() -> Value {
    closed_complete(vec![
        ("state", constant("unavailable")),
        ("summary", product_text()),
    ])
}

fn study_qualification() -> Value {
    closed_complete(vec![
        (
            "basis",
            enumeration(&["historical_as_known", "retrospective_frozen_snapshot"]),
        ),
        ("limitations", bounded_array(product_text(), 4)),
        ("summary", product_text()),
    ])
}

fn out_of_sample() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            (
                "completedObservations",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            (
                "totalSignals",
                bounded_unsigned_range(1, u64::from(u32::MAX)),
            ),
            ("folds", bounded_unsigned_range(1, u64::from(u32::MAX))),
            ("completionCoveragePercent", percentage()),
            ("evaluatedFrom", canonical_market_timestamp()),
            ("evaluatedThrough", canonical_market_timestamp()),
            ("studyQualification", study_qualification()),
            ("summary", product_text()),
        ]),
        unavailable_summary(),
    ])
}

fn analytical_evidence() -> Value {
    let mut fields = [
        "currentMarket",
        "broaderResearch",
        "forecast",
        "financialModel",
        "valuation",
        "historicalTest",
        "outOfSample",
        "liquidity",
        "portfolioRisk",
    ]
    .into_iter()
    .map(|kind| {
        (
            kind,
            closed_complete(vec![
                ("state", enumeration(&["available", "unavailable"])),
                ("summary", product_text()),
            ]),
        )
    })
    .collect::<Vec<_>>();
    fields.push(("pricePattern", price_pattern_evidence()));
    fields.push((
        "combination",
        closed_complete(vec![
            ("state", enumeration(&["multi_evidence", "insufficient"])),
            ("summary", product_text()),
        ]),
    ));
    closed_complete(fields)
}

fn price_pattern_evidence() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            (
                "outcome",
                enumeration(&[
                    "pattern_detected",
                    "no_matching_pattern",
                    "pattern_expired",
                    "pattern_invalidated",
                ]),
            ),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            (
                "outcome",
                enumeration(&[
                    "insufficient_bars",
                    "insufficient_turning_points",
                    "history_unavailable",
                    "adjustment_unavailable",
                    "trading_activity_unavailable",
                    "price_precision_unavailable",
                    "assessment_unavailable",
                    "not_evaluated",
                ]),
            ),
            ("summary", product_text()),
        ]),
    ])
}

fn liquidity() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("quotedSpreadPercent", nonnegative_decimal()),
            ("buyAddCapacityPercent", nullable(percentage())),
            ("trimSellCapacityPercent", nullable(percentage())),
            ("summary", product_text()),
        ]),
        unavailable_summary(),
    ])
}

fn portfolio_context() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("portfolioLabel", bounded_text(128)),
            (
                "positionState",
                enumeration(&["no_position", "current_position"]),
            ),
            ("riskCapacityPercent", percentage()),
            ("summary", product_text()),
        ]),
        unavailable_summary(),
    ])
}

fn virtual_paper_eligibility() -> Value {
    closed_complete(vec![
        ("state", constant("not_eligible")),
        ("executionAuthority", constant("none")),
        ("requiresExplicitPaperApproval", constant_bool(true)),
        ("requiresFreshRiskCheck", constant_bool(true)),
        ("summary", product_text()),
    ])
}

fn nonnegative_decimal() -> Value {
    json!({
        "type": "string",
        "pattern": "^(?:0|[1-9][0-9]*)(?:\\.[0-9]*[1-9])?$",
    })
}

fn signed_money() -> Value {
    closed_complete(vec![
        ("amount", canonical_decimal_text()),
        ("currency", currency()),
    ])
}

fn signed_money_range() -> Value {
    closed_complete(vec![("lower", signed_money()), ("upper", signed_money())])
}

fn gross_price_pnl() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("range", signed_money_range()),
            ("summary", product_text()),
        ]),
        unavailable_summary(),
    ])
}

pub(super) fn expected_return() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("metric", constant("expected_gross_price_return")),
            (
                "basis",
                constant("admitted_conditional_mean_terminal_price"),
            ),
            (
                "grossPriceReturnPercent",
                nullable(canonical_decimal_text()),
            ),
            ("exactRatio", exact_ratio()),
            ("summary", product_text()),
        ]),
        unavailable_summary(),
    ])
}

fn expected_gross_price_pnl() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("amount", signed_money()),
            ("summary", product_text()),
        ]),
        unavailable_summary(),
    ])
}

/// Saved native chart values; no frontend recomputation or cross-basis stitching is authorized.
pub(super) fn chart() -> Value {
    let positive = super::investment_analysis_positive_decimal();
    let range = || {
        closed_complete(vec![
            ("lower", positive.clone()),
            ("upper", positive.clone()),
        ])
    };
    let coordinate = one_of(vec![
        closed_complete(vec![
            ("kind", constant("timestamp")),
            ("timeUnixNanos", timestamp_nanos_text()),
        ]),
        closed_complete(vec![
            ("kind", constant("session_date")),
            (
                "date",
                json!({"type":"string","pattern":"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"}),
            ),
            ("sessionCloseUnixNanos", timestamp_nanos_text()),
        ]),
    ]);
    let history_point = one_of(vec![
        closed_complete(vec![
            ("originalOrdinal", unsigned_integer_text()),
            ("breakBefore", fixed_array(json!({"type":"boolean"}), 1)),
            ("coordinate", coordinate.clone()),
            ("availableAtUnixNanos", timestamp_nanos_text()),
            ("value", positive.clone()),
            ("quality", chart_quality()),
        ]),
        closed_complete(vec![
            ("originalOrdinal", unsigned_integer_text()),
            ("breakBefore", fixed_array(json!({"type":"boolean"}), 1)),
            ("coordinate", coordinate.clone()),
            ("availableAtUnixNanos", json!({"type":"null"})),
            ("value", json!({"type":"null"})),
            ("quality", json!({"type":"null"})),
        ]),
    ]);
    let history = one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("summary", product_text()),
            ("basis", constant("split_adjusted_price")),
            ("display", chart_display()),
            ("points", bounded_array(history_point, 4096)),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", product_text()),
            ("basis", constant("split_adjusted_price")),
            ("points", bounded_array(json!({"type":"null"}), 0)),
        ]),
    ]);
    let forecast_point = closed_complete(vec![
        ("timeUnixNanos", timestamp_nanos_text()),
        ("central", positive.clone()),
        ("interval50", nullable(range())),
        ("interval80", nullable(range())),
        ("interval95", nullable(range())),
    ]);
    let forecast_origin = one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("basis", constant("split_adjusted_price")),
            ("coordinate", coordinate),
            ("value", positive.clone()),
            ("summary", product_text()),
            ("quality", chart_quality()),
        ]),
        unavailable_summary(),
    ]);
    let forecast = one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("summary", product_text()),
            ("basis", constant("saved_price_projection")),
            ("observedThroughUnixNanos", timestamp_nanos_text()),
            ("origin", forecast_origin.clone()),
            ("points", bounded_array(forecast_point, 1)),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("summary", product_text()),
            ("basis", constant("saved_price_projection")),
            ("observedThroughUnixNanos", json!({"type":"null"})),
            ("origin", unavailable_summary()),
            ("points", bounded_array(json!({"type":"null"}), 0)),
        ]),
    ]);
    closed_complete(vec![
        ("informationCurrentThroughUnixNanos", timestamp_nanos_text()),
        ("basisExplanation", product_text()),
        ("history", history),
        ("forecast", forecast),
        ("benchmark", benchmark_chart()),
        ("actionRanges", chart_action_ranges()),
        (
            "viewport",
            closed_complete(vec![
                ("startUnixNanos", nullable(timestamp_nanos_text())),
                ("endUnixNanos", nullable(timestamp_nanos_text())),
                ("pointLimit", bounded_unsigned_range(8, 4096)),
                (
                    "layer",
                    enumeration(&[
                        "all",
                        "history",
                        "forecast",
                        "benchmark",
                        "price_pattern",
                        "action_ranges",
                    ]),
                ),
                ("fullStartUnixNanos", nullable(timestamp_nanos_text())),
                ("fullEndUnixNanos", nullable(timestamp_nanos_text())),
            ]),
        ),
        (
            "pricePattern",
            closed_complete(vec![
                (
                    "status",
                    enumeration(&[
                        "unavailable",
                        "confirmed",
                        "insufficient_bars",
                        "insufficient_pivots",
                        "no_matching_pattern",
                        "expired",
                        "invalidated",
                    ]),
                ),
                ("summary", product_text()),
                ("basis", constant("split_adjusted_price")),
                (
                    "kind",
                    nullable(enumeration(&[
                        "ab_cd",
                        "gartley",
                        "bat",
                        "butterfly",
                        "crab",
                        "deep_crab",
                        "cypher",
                        "shark",
                    ])),
                ),
                ("direction", nullable(enumeration(&["bullish", "bearish"]))),
                (
                    "pivots",
                    bounded_array(
                        closed_complete(vec![
                            ("name", enumeration(&["X", "A", "B", "C", "D"])),
                            ("kind", enumeration(&["high", "low"])),
                            ("observedAtUnixNanos", timestamp_nanos_text()),
                            ("availableAtUnixNanos", timestamp_nanos_text()),
                            ("confirmedAtUnixNanos", timestamp_nanos_text()),
                            ("value", positive.clone()),
                        ]),
                        5,
                    ),
                ),
                (
                    "ratios",
                    bounded_array(
                        closed_complete(vec![
                            (
                                "name",
                                enumeration(&[
                                    "AB/XA", "BC/AB", "CD/BC", "CD/AB", "AD/XA", "XC/XA", "CD/XC",
                                ]),
                            ),
                            ("numerator", unsigned_integer_text()),
                            ("denominator", unsigned_integer_text()),
                        ]),
                        7,
                    ),
                ),
                ("reversalZone", nullable(range())),
                ("invalidation", nullable(positive.clone())),
                ("targets", bounded_array(positive.clone(), 3)),
                ("expiresAtUnixNanos", nullable(timestamp_nanos_text())),
                (
                    "observationCutoffUnixNanos",
                    nullable(timestamp_nanos_text()),
                ),
                (
                    "confirmationCutoffUnixNanos",
                    nullable(timestamp_nanos_text()),
                ),
                ("interpretation", bounded_array(product_text(), 8)),
            ]),
        ),
    ])
}

fn chart_quality() -> Value {
    enumeration(&[
        "direct_verified",
        "direct_unverified",
        "official_delayed",
        "aggregated",
        "indicative",
        "modeled",
        "estimated",
        "stale",
        "quarantined",
    ])
}

pub(super) fn chart_display() -> Value {
    closed_complete(vec![
        ("method", constant("first_last_min_max")),
        ("originalPointCount", unsigned_integer_text()),
        ("visibleOriginalPointCount", unsigned_integer_text()),
        ("returnedPointCount", bounded_unsigned(4096)),
        ("firstTimeUnixNanos", nullable(timestamp_nanos_text())),
        ("lastTimeUnixNanos", nullable(timestamp_nanos_text())),
        ("projectionDigest", sha256()),
        ("reduced", json!({"type":"boolean"})),
    ])
}

fn chart_action_ranges() -> Value {
    let common = || {
        vec![
            ("basis", constant("split_adjusted_price")),
            ("summary", product_text()),
            ("informationCurrentThroughUnixNanos", timestamp_nanos_text()),
            ("admittedAtUnixNanos", timestamp_nanos_text()),
            ("expiresAtUnixNanos", timestamp_nanos_text()),
        ]
    };
    let mut available = common();
    available.extend([
        ("state", constant("available")),
        (
            "ranges",
            fixed_array(
                closed_complete(vec![
                    ("kind", enumeration(&["entry", "add", "trim", "exit"])),
                    ("label", product_text()),
                    ("lower", super::investment_analysis_positive_decimal()),
                    ("upper", super::investment_analysis_positive_decimal()),
                    ("startAtUnixNanos", timestamp_nanos_text()),
                    ("endAtUnixNanos", timestamp_nanos_text()),
                    ("summary", product_text()),
                ]),
                4,
            ),
        ),
    ]);
    let mut unavailable = common();
    unavailable.extend([
        ("state", constant("unavailable")),
        (
            "reason",
            enumeration(&[
                "no_supported_action_ranges",
                "share_conversion_unavailable",
                "original_history_unavailable",
                "expired_at_admission",
                "range_conversion_unavailable",
                "not_requested",
            ]),
        ),
        ("ranges", bounded_array(json!({"type":"null"}), 0)),
    ]);
    one_of(vec![
        closed_complete(available),
        closed_complete(unavailable),
    ])
}

/// Original, backend-indexed split-price comparisons, including missing saved sources.
fn benchmark_chart() -> Value {
    let members = |count: usize| {
        let items = ["subject", "selected", "accompanying"]
            .into_iter()
            .take(count)
            .map(|role| {
                closed_complete(vec![
                    ("role", constant(role)),
                    ("instrumentId", action_token()),
                    ("label", bounded_text(128)),
                ])
            })
            .collect::<Vec<_>>();
        json!({"type":"array", "minItems":count, "maxItems":count,
            "prefixItems":items, "items":false})
    };
    let coordinate = || {
        closed_complete(vec![
            (
                "date",
                json!({"type":"string", "pattern":"^[0-9]{4}-[0-9]{2}-[0-9]{2}$"}),
            ),
            ("sessionCloseUnixNanos", timestamp_nanos_text()),
        ])
    };
    let observation = closed_complete(vec![
        ("close", super::investment_analysis_positive_decimal()),
        ("priceIndex", super::investment_analysis_positive_decimal()),
        ("availableAtUnixNanos", timestamp_nanos_text()),
        (
            "providerCompletedAtUnixNanos",
            nullable(timestamp_nanos_text()),
        ),
        (
            "quality",
            enumeration(&[
                "direct_verified",
                "direct_unverified",
                "official_delayed",
                "aggregated",
                "indicative",
                "modeled",
                "estimated",
                "stale",
                "quarantined",
            ]),
        ),
    ]);
    let available = |count| {
        closed_complete(vec![
            ("state", constant("available")),
            ("basis", constant("split_adjusted_price_index")),
            ("members", members(count)),
            ("summary", product_text()),
            ("baseline", coordinate()),
            ("display", chart_display()),
            (
                "points",
                bounded_array(
                    closed_complete(vec![
                        ("originalOrdinal", unsigned_integer_text()),
                        ("breakBefore", fixed_array(json!({"type":"boolean"}), count)),
                        ("coordinate", coordinate()),
                        (
                            "observations",
                            fixed_array(nullable(observation.clone()), count),
                        ),
                    ]),
                    4096,
                ),
            ),
        ])
    };
    one_of(vec![
        available(2),
        available(3),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("basis", constant("split_adjusted_price_index")),
            ("members", bounded_array(json!({"type":"null"}), 0)),
            ("reason", constant("not_requested")),
            ("summary", product_text()),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("basis", constant("split_adjusted_price_index")),
            ("members", one_of(vec![members(1), members(2), members(3)])),
            (
                "reason",
                enumeration(&[
                    "selection_unavailable",
                    "missing_subject",
                    "missing_selected_comparison",
                    "no_common_observation",
                    "storage_unavailable",
                    "integrity_unproven",
                ]),
            ),
            ("summary", product_text()),
        ]),
    ])
}

fn saved_probabilities() -> Value {
    closed_complete(vec![
        ("priceHigher", saved_probability()),
        ("benchmarkOutperformance", saved_probability()),
        ("profitAfterCosts", saved_probability()),
    ])
}
fn saved_probability() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("state", constant("available")),
            ("benchmark", saved_probability_benchmark()),
            ("probabilityPercent", percentage()),
            ("observedAt", canonical_market_timestamp()),
            ("endsAt", canonical_market_timestamp()),
            ("expiresAt", canonical_market_timestamp()),
            ("assumptions", bounded_array(product_text(), 5)),
            (
                "calibration",
                closed_complete(vec![
                    ("evaluatedFrom", canonical_market_timestamp()),
                    ("evaluatedThrough", canonical_market_timestamp()),
                    (
                        "completedOutcomes",
                        bounded_unsigned_range(1, u64::from(u32::MAX)),
                    ),
                    (
                        "brierScore",
                        json!({"type":"number","minimum":0,"maximum":1}),
                    ),
                    ("logLoss", json!({"type":"number","minimum":0})),
                ]),
            ),
        ]),
        closed_complete(vec![
            ("state", constant("unavailable")),
            ("benchmark", saved_probability_benchmark()),
            ("summary", product_text()),
            ("assumptions", bounded_array(product_text(), 5)),
        ]),
    ])
}

fn saved_probability_benchmark() -> Value {
    nullable(closed_complete(vec![
        ("instrumentId", action_token()),
        ("definitionAlgorithm", enumeration(&["sha256", "blake3"])),
        ("definitionDigest", sha256()),
    ]))
}

#[cfg(test)]
mod tests {
    use market_squawk_services::{
        JsonStructureLimits, ServiceLimits, ToolDescriptor, ToolResultMetadata, TypedToolResult,
    };
    use serde_json::{Map, Value, json};

    #[test]
    fn saved_benchmark_chart_accepts_retained_series_and_unavailability()
    -> Result<(), Box<dyn std::error::Error>> {
        let operation = super::super::super::OPERATION_SPECS
            .iter()
            .find(|operation| operation.name == "Decision.GetInvestmentChart")
            .ok_or("missing investment analysis operation")?;
        let original = super::super::super::descriptor_for(*operation)?;
        // Exercise the advertised nested contract through the actual publication validator.
        let descriptor = ToolDescriptor::try_new_with_output(
            original.name(),
            "1",
            "Saved benchmark chart contract",
            Value::Object(original.input_schema().clone()),
            super::chart()["properties"]["benchmark"].clone(),
            original.contract(),
            original.effects(),
            |_: &Map<String, Value>| Ok(()),
        )?;
        let limits = ServiceLimits::try_new(
            1024 * 1024,
            1024,
            1024 * 1024,
            1024,
            JsonStructureLimits::try_new(32, 64 * 1024, 10_000, 2_000)?,
        )?;
        let validate = |value| -> Result<(), Box<dyn std::error::Error>> {
            TypedToolResult::try_new(
                value,
                1,
                ToolResultMetadata::complete_not_applicable(),
                limits,
            )?
            .validate_for(&descriptor)?;
            Ok(())
        };
        let members = json!([
            {"role":"subject", "instrumentId":"11111111-1111-4111-8111-111111111111", "label":"Investment"},
            {"role":"selected", "instrumentId":"22222222-2222-4222-8222-222222222222", "label":"Selected comparison"}
        ]);
        let unavailable = json!({"state":"unavailable", "basis":"split_adjusted_price_index",
            "members":members, "reason":"missing_selected_comparison", "summary":"No saved comparison prices."});
        validate(unavailable)?;
        let coordinate =
            json!({"date":"2026-09-22", "sessionCloseUnixNanos":"1790107200000000000"});
        let observation = json!({"close":"101.25", "priceIndex":"100",
            "availableAtUnixNanos":"1790107201000000000", "providerCompletedAtUnixNanos":null,
            "quality":"aggregated"});
        let available = json!({"state":"available", "basis":"split_adjusted_price_index",
            "members":members, "summary":"Saved split-adjusted comparison.", "baseline":coordinate,
            "display":{"method":"first_last_min_max","originalPointCount":"2","visibleOriginalPointCount":"2",
                "returnedPointCount":2,"firstTimeUnixNanos":"1790107200000000000","lastTimeUnixNanos":"1790193600000000000",
                "projectionDigest":"1111111111111111111111111111111111111111111111111111111111111111","reduced":false},
            "points":[{"coordinate":coordinate, "observations":[observation, observation],"originalOrdinal":"0","breakBefore":[false,false]},
                {"coordinate":{"date":"2026-09-23", "sessionCloseUnixNanos":"1790193600000000000"},
                    "observations":[observation, null],"originalOrdinal":"1","breakBefore":[false,true]}]});
        validate(available.clone())?;
        let mut missing = available.clone();
        missing["points"][0]["observations"][0]
            .as_object_mut()
            .ok_or("missing observation")?
            .remove("availableAtUnixNanos");
        assert!(validate(missing).is_err());
        let mut mismatched = available;
        mismatched["points"][0]["observations"]
            .as_array_mut()
            .ok_or("missing observations")?
            .pop();
        assert!(validate(mismatched).is_err());
        Ok(())
    }
}
