//! Closed reconstruction schema owned beside the sole active source-plan wire.

use super::SourceAppliedCorporateActionPlanReference;
use serde_json::{Value, json};

impl SourceAppliedCorporateActionPlanReference {
    /// Describes inert V1 reconstruction values. Descriptors must also run this type's strict
    /// Deserialize admission; neither schema acceptance nor decoding grants source authority.
    pub fn json_schema() -> Value {
        object([
            ("version", json!({"type":"integer","const":1})),
            ("source_origin_content", digest()),
            ("source_binding", digest()),
            ("source_snapshot", digest()),
            ("calendar", calendar()),
            (
                "requested_instruments",
                json!({"type":"array","minItems":1,"maxItems":32,"items":uuid()}),
            ),
            ("interval", pair(date())),
            ("knowledge_cutoff", timestamp()),
            ("valuation_cutoff", timestamp()),
            ("evaluated_at", timestamp()),
            (
                "adjustment",
                json!({"type":"integer","minimum":0,"maximum":2}),
            ),
            (
                "policy_version",
                json!({"type":"integer","minimum":1,"maximum":u32::MAX}),
            ),
            (
                "payment_policy",
                json!({"type":"string","enum":["retain_receivable","end_of_reported_payable_session_v1"]}),
            ),
            ("content_hash", digest()),
            ("audit_hash", digest()),
            (
                "ordinary",
                json!({"type":"array","maxItems":32,"items":object([
                    ("history",crate::application::research::ingest::TiingoCompletedEodHistoryReference::json_schema()),
                    ("calendar",calendar()),
                    ("timestamped",nullable(anchor_history("raw"))),
                ])}),
            ),
            ("ordinary_coverage_digest", nullable(digest())),
            ("current_ordinary", nullable(object([
                ("id", json!({"type":"string","minLength":1,"maxLength":160,"pattern":"^[A-Za-z0-9][A-Za-z0-9_-]*$"})),
                ("sha256", json!({"type":"string","minLength":64,"maxLength":64,"pattern":"^[0-9a-f]{64}$"})),
                ("bytes", json!({"type":"integer","minimum":1,"maximum":super::current_ordinary::MAX_CURRENT_RECIPE_BYTES})),
            ]))),
            ("current_ordinary_digest", nullable(digest())),
            (
                "anchor",
                nullable(object([
                    ("raw", anchor_history("raw")),
                    ("split", anchor_history("split")),
                    ("raw_page_received_at", pages()),
                    ("split_page_received_at", pages()),
                ])),
            ),
        ])
    }
}

fn anchor_history(adjustment: &str) -> Value {
    object([
        ("version", json!({"type":"integer","const":1})),
        ("instrument", uuid()),
        ("requested", pair(timestamp())),
        ("provider_instrument", identifier()),
        ("venue", identifier()),
        ("feed", identifier()),
        ("interval", identifier()),
        ("adjustment", json!({"type":"string","const":adjustment})),
        (
            "timestamp_basis",
            json!({"type":"string","enum":["period_start","period_end"]}),
        ),
        (
            "session_kind",
            json!({"type":"string","enum":["regular","extended","continuous","provider_defined"]}),
        ),
        ("session_ruleset", identifier()),
        ("cutoff", timestamp()),
        ("origin", digest()),
        ("binding", digest()),
        ("publication", digest()),
        ("read", digest()),
    ])
}
fn object<const N: usize>(fields: [(&str, Value); N]) -> Value {
    let required: Vec<_> = fields.iter().map(|(name, _)| *name).collect();
    let properties: serde_json::Map<_, _> = fields
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect();
    json!({"type":"object","additionalProperties":false,"required":required,"properties":properties})
}
fn digest() -> Value {
    object([
        ("algorithm", json!({"type":"string","const":"sha256"})),
        (
            "bytes",
            json!({"type":"array","minItems":32,"maxItems":32,"items":{"type":"integer","minimum":0,"maximum":255}}),
        ),
    ])
}
fn calendar() -> Value {
    let hex = || json!({"type":"string","minLength":64,"maxLength":64,"pattern":"^[0-9a-f]{64}$"});
    object([
        ("originContentDigest", hex()),
        ("captureBindingDigest", hex()),
    ])
}
fn date() -> Value {
    object([
        (
            "year",
            json!({"type":"integer","minimum":1,"maximum":u16::MAX}),
        ),
        ("month", json!({"type":"integer","minimum":1,"maximum":12})),
        ("day", json!({"type":"integer","minimum":1,"maximum":31})),
    ])
}
fn timestamp() -> Value {
    json!({"type":"integer","minimum":i64::MIN,"maximum":i64::MAX})
}
fn identifier() -> Value {
    json!({"type":"string","minLength":1,"maxLength":1024})
}
fn uuid() -> Value {
    json!({"type":"string","format":"uuid"})
}
fn nullable(value: Value) -> Value {
    json!({"oneOf":[{"type":"null"},value]})
}
fn pair(value: Value) -> Value {
    json!({"type":"array","minItems":2,"maxItems":2,"items":value})
}
fn pages() -> Value {
    json!({"type":"array","minItems":1,"maxItems":market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES,"items":timestamp()})
}
