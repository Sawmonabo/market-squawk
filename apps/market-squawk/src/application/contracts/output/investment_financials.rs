//! Shared financial-page wire schema; financial calculations stay in the research projection.
use super::*;

pub(super) fn page() -> Value {
    one_of(
        [
            ("facts", fact()),
            ("statements", statement()),
            ("ratios", ratio()),
            ("filings", filing()),
        ]
        .into_iter()
        .map(|(section, item)| {
            closed_complete(vec![
                ("selectionToken", opaque_product_token()),
                ("section", constant(section)),
                ("knowledgeAt", nullable(timestamp())),
                (
                    "effectiveOn",
                    nullable(json!({"type":"string", "format":"date"})),
                ),
                ("revisionPolicy", constant("allKnown")),
                (
                    "state",
                    enumeration(&[
                        "reported",
                        "preparation_required",
                        "missing",
                        "conflict",
                        "unavailable",
                        "expired",
                    ]),
                ),
                (
                    "families",
                    array(closed_complete(vec![
                        (
                            "family",
                            enumeration(&["company_facts", "filing_details", "filings"]),
                        ),
                        (
                            "state",
                            enumeration(&[
                                "reported",
                                "preparation_required",
                                "missing",
                                "conflict",
                                "unavailable",
                            ]),
                        ),
                        (
                            "reason",
                            nullable(enumeration(&[
                                "identity_missing",
                                "identity_ambiguous",
                                "identity_stale",
                                "identity_revoked",
                                "revision_conflict",
                                "no_records",
                                "evidence_unavailable",
                                "rights_unavailable",
                                "preparation_required",
                            ])),
                        ),
                    ])),
                ),
                ("items", array(item)),
                ("currentCursor", nullable(text())),
                ("nextCursor", nullable(text())),
                ("readToken", nullable(uuid())),
                ("omittedItems", unsigned()),
                (
                    "limitations",
                    array(enumeration(&[
                        "item_exceeds_response_limit",
                        "some_reported_facts_not_supported",
                        "read_expired",
                    ])),
                ),
            ])
        })
        .collect(),
    )
}

fn revision() -> Value {
    enumeration(&["current", "superseded", "incomparable_history"])
}

fn time() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("precision", constant("timestamp")),
            ("value", integer()),
        ]),
        closed_complete(vec![
            ("precision", constant("calendar_date")),
            ("value", calendar_date()),
        ]),
    ])
}

fn period() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("kind", constant("instant")),
            ("instant", calendar_date()),
        ]),
        closed_complete(vec![
            ("kind", constant("duration")),
            ("start", calendar_date()),
            ("end", calendar_date()),
        ]),
    ])
}

fn fiscal_context() -> Value {
    closed_complete(vec![
        (
            "fiscalYear",
            nullable(bounded_unsigned(u64::from(u16::MAX))),
        ),
        (
            "fiscalPeriod",
            enumeration(&[
                "fiscal_year",
                "calendar_year",
                "first_quarter",
                "second_quarter",
                "third_quarter",
                "fourth_quarter",
                "unavailable",
            ]),
        ),
        (
            "cadence",
            enumeration(&["annual", "quarterly", "other", "unavailable"]),
        ),
    ])
}

fn reporting_context(fact: bool) -> Value {
    let mut fields = vec![
        (
            "dimensionality",
            enumeration(&["unavailable", "no_dimensions"]),
        ),
        (
            "consolidation",
            enumeration(&[
                "reported_consolidated",
                "reported_non_consolidated",
                "unavailable",
            ]),
        ),
        (
            "amendment",
            enumeration(&["original", "amendment", "unavailable"]),
        ),
        (
            "restatement",
            enumeration(&["reported_restated", "reported_not_restated", "unavailable"]),
        ),
    ];
    if fact {
        fields.push(("occurrence", bounded_unsigned(u64::from(u32::MAX))));
    }
    closed_complete(fields)
}

fn reporting_fields(fact: bool) -> Vec<(&'static str, Value)> {
    vec![
        ("period", period()),
        ("fiscalContext", fiscal_context()),
        ("reportingContext", reporting_context(fact)),
        ("scope", enumeration(&["company_wide", "filing_detail"])),
        ("filedOn", nullable(calendar_date())),
        ("effective", time()),
        ("knownAt", integer()),
    ]
}

fn fact() -> Value {
    let mut fields = reporting_fields(true);
    fields.extend([
        ("revision", revision()),
        ("metric", metric()),
        ("displayName", text()),
        ("value", canonical_decimal_text()),
        (
            "unit",
            one_of(vec![
                closed_complete(vec![
                    ("kind", constant("currency")),
                    ("currency", investment_analysis_currency()),
                ]),
                closed_complete(vec![
                    ("kind", constant("currency_per_share")),
                    ("currency", investment_analysis_currency()),
                ]),
                closed_complete(vec![("kind", constant("shares"))]),
            ]),
        ),
    ]);
    closed_complete(fields)
}

fn metric() -> Value {
    enumeration(&[
        "cash_and_cash_equivalents",
        "accounts_receivable_net_current",
        "inventory_net",
        "current_assets",
        "total_assets",
        "current_liabilities",
        "total_liabilities",
        "current_long_term_debt",
        "noncurrent_long_term_debt",
        "shareholders_equity",
        "total_equity_including_noncontrolling_interests",
        "revenue",
        "net_sales",
        "customer_revenue_excluding_assessed_tax",
        "cost_of_revenue",
        "gross_profit",
        "operating_expenses",
        "operating_income",
        "net_income",
        "common_net_income",
        "preferred_dividends_and_adjustments",
        "profit_or_loss_including_noncontrolling_interests",
        "basic_earnings_per_share",
        "diluted_earnings_per_share",
        "operating_cash_flow",
        "investing_cash_flow",
        "financing_cash_flow",
        "property_plant_and_equipment_purchases",
        "long_term_borrowing_proceeds",
        "long_term_debt_repayments",
        "preferred_dividends_paid",
        "preferred_stock_issued_value",
        "entity_common_shares_outstanding",
        "common_stock_shares_outstanding",
        "weighted_average_basic_shares",
        "weighted_average_diluted_shares",
    ])
}

fn statement() -> Value {
    closed_complete(vec![
        (
            "statement",
            enumeration(&[
                "financial_position",
                "operations",
                "cash_flows",
                "share_data",
            ]),
        ),
        ("envelope", closed_complete(reporting_fields(false))),
        ("items", array(fact())),
    ])
}

fn ratio() -> Value {
    closed_complete(vec![
        (
            "metric",
            enumeration(&[
                "current_ratio",
                "gross_margin",
                "operating_margin",
                "net_margin",
            ]),
        ),
        ("displayName", text()),
        (
            "state",
            enumeration(&[
                "reported",
                "missing_input",
                "conflicting_input",
                "incompatible_units",
                "zero_denominator",
                "unavailable",
            ]),
        ),
        ("value", nullable(canonical_decimal_text())),
        ("unit", constant("ratio")),
        (
            "envelope",
            nullable(closed_complete(reporting_fields(false))),
        ),
        (
            "inputs",
            array(closed_complete(vec![
                ("role", enumeration(&["numerator", "denominator"])),
                ("fact", fact()),
            ])),
        ),
    ])
}

fn filing() -> Value {
    closed_complete(vec![
        ("revision", revision()),
        ("form", text()),
        ("effective", time()),
        ("published", nullable(time())),
        ("knownAt", integer()),
    ])
}
