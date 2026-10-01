//! Exact saved market inputs shared by the default and advanced investment workflows.

use serde_json::Value;

use super::{
    boolean, bounded_array, bounded_text, canonical_decimal_text as decimal, closed_complete,
    constant, currency_code, enumeration, integer_text, investment_analysis_sha256 as exact_digest,
    nullable, one_of, positive_integer_text, sha256, uuid,
};

pub(crate) fn reference() -> Value {
    closed_complete(vec![
        ("instrumentId", uuid()),
        ("sourceCutoffUnixNanos", integer_text()),
        ("maximumMarkAgeNanos", positive_integer_text()),
        ("evidenceDigest", sha256()),
        ("sourceSelectionDigest", sha256()),
        ("publicationSelectionDigest", sha256()),
        ("definitionSelectionDigest", sha256()),
        ("priceAuthorityDigest", sha256()),
        ("rightsInputDigest", sha256()),
        ("sourceScopeDigests", nullable(bounded_array(sha256(), 256))),
    ])
}

pub(super) fn preparation_result() -> Value {
    let scope = || enumeration(&["investment_analysis", "current_market"]);
    let reference = || {
        nullable(closed_complete(vec![
            ("originContentDigest", exact_digest()),
            ("captureBindingDigest", exact_digest()),
        ]))
    };
    let sources = || {
        bounded_array(
            closed_complete(vec![
                (
                    "source",
                    enumeration(&[
                        "current_session",
                        "government_history",
                        "benchmark_history",
                        "selected_history",
                        "source_actions",
                        "equity_premium",
                        "option_context",
                        "current_share_actions",
                        "fundamental_share_actions",
                    ]),
                ),
                ("status", enumeration(&["available", "unavailable"])),
                ("evidenceDigest", nullable(sha256())),
                ("failure", nullable(bounded_text(1024))),
                ("startedAtUnixNanos", integer_text()),
                ("completedAtUnixNanos", integer_text()),
            ]),
            9,
        )
    };
    let mut schema=one_of(vec![
        closed_complete(vec![
            ("status", constant("prepared")),
            ("scope", scope()),
            ("financialConfigurationDigest", sha256()),
            ("instrumentId", uuid()),
            ("preparedAtUnixNanos", integer_text()),
            ("reference", reference()),
            ("sourceActionReference", nullable(crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference::json_schema())),
            ("fundamentalShareSources", nullable(bounded_text(crate::application::fair_value::MAX_FUNDAMENTAL_SHARE_SOURCE_BYTES))),
            ("sources", sources()),
        ]),
        closed_complete(vec![
            ("status", constant("unavailable")),
            ("scope", scope()),
            ("financialConfigurationDigest", sha256()),
            ("instrumentId", nullable(uuid())),
            ("preparedAtUnixNanos", nullable(integer_text())),
            ("reference", reference()),
            ("sourceActionReference", nullable(crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference::json_schema())),
            ("fundamentalShareSources", nullable(bounded_text(crate::application::fair_value::MAX_FUNDAMENTAL_SHARE_SOURCE_BYTES))),
            ("sources", sources()),
            (
                "reason",
                enumeration(&[
                    "selection_changed",
                    "identity_unavailable",
                    "unsupported_investment",
                    "source_evidence_unavailable",
                    "evidence_changed",
                ]),
            ),
        ]),
    ]);
    // Optional only on the actual source-assessed Find unavailable branch. Ordinary and ready
    // callers preserve their existing exact response; no client supplies this result field.
    schema["oneOf"][1]["properties"]["findMemberUnavailable"] =
        super::find_results::member_unavailable();
    schema
}

pub(super) fn result() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("status", constant("available")),
            ("sourceCutoffUnixNanos", integer_text()),
            ("financialConfigurationDigest", sha256()),
            ("instrumentId", uuid()),
            ("instrument", identity()),
            ("reference", reference()),
            (
                "authorization",
                closed_complete(vec![
                    ("admittedAtUnixNanos", integer_text()),
                    ("expiresAtUnixNanos", integer_text()),
                    ("decisionDigest", sha256()),
                    ("selectionAuditDigest", sha256()),
                ]),
            ),
            ("mark", mark()),
            ("liquidity", liquidity()),
        ]),
        closed_complete(vec![
            ("status", constant("unavailable")),
            ("sourceCutoffUnixNanos", integer_text()),
            ("financialConfigurationDigest", sha256()),
            ("instrumentId", nullable(uuid())),
            ("instrument", nullable(identity())),
            (
                "reason",
                enumeration(&[
                    "selection_changed",
                    "identity_unavailable",
                    "unsupported_investment",
                    "market_evidence_unavailable",
                    "evidence_changed",
                ]),
            ),
        ]),
    ])
}

fn identity() -> Value {
    closed_complete(vec![
        ("instrumentId", uuid()),
        ("symbol", bounded_text(256)),
        ("name", bounded_text(1024)),
        (
            "assetClass",
            enumeration(&[
                "equity",
                "fixed_income",
                "option",
                "future",
                "foreign_exchange",
                "crypto",
                "commodity",
                "fund",
                "index",
                "cash",
            ]),
        ),
        ("currency", currency_code()),
        ("exchangeTradedFund", boolean()),
    ])
}

fn mark() -> Value {
    closed_complete(vec![
        ("value", decimal()),
        ("currency", currency_code()),
        ("basis", enumeration(&["last_trade", "bid_ask_midpoint"])),
        ("observedAtUnixNanos", integer_text()),
        ("availableAtUnixNanos", integer_text()),
        ("freshUntilUnixNanos", integer_text()),
    ])
}

fn depth_side() -> Value {
    closed_complete(vec![("price", decimal()), ("quantity", decimal())])
}

fn liquidity() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("status", constant("available")),
            ("depth", constant("top_of_book")),
            ("currency", currency_code()),
            ("contractMultiplier", decimal()),
            ("bid", depth_side()),
            ("ask", depth_side()),
            ("observedAtUnixNanos", integer_text()),
            ("availableAtUnixNanos", integer_text()),
            ("freshUntilUnixNanos", integer_text()),
        ]),
        closed_complete(vec![
            ("status", constant("unavailable")),
            (
                "reason",
                enumeration(&[
                    "source_does_not_supply_depth",
                    "no_positive_displayed_size",
                    "stale_depth",
                ]),
            ),
        ]),
    ])
}
