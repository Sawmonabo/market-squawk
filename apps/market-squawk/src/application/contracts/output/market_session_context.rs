//! Shared exact session-reference and returned-entry schemas.
use super::*;

pub(in crate::application::contracts) fn reference() -> Value {
    closed_complete(vec![
        (
            "request",
            closed_complete(vec![
                (
                    "product",
                    enumeration(super::super::MARKET_SESSION_PRODUCTS),
                ),
                ("date", super::super::calendar_date_argument_schema()),
            ]),
        ),
        (
            "originContentSha256",
            super::super::nonzero_sha256_argument_schema(),
        ),
        (
            "captureBindingSha256",
            super::super::nonzero_sha256_argument_schema(),
        ),
    ])
}

pub(super) fn result() -> Value {
    let maximum = market_squawk_domain::MAX_MARKET_CALENDAR_INTERVALS;
    let window = closed_complete(vec![
        (
            "role",
            enumeration(&["core", "pre", "post", "intermission", "source_defined"]),
        ),
        ("ordinal", bounded_unsigned((maximum - 1) as u64)),
        ("startUnixNanos", integer_text()),
        ("endUnixNanos", integer_text()),
        (
            "startUtcOffsetSeconds",
            bounded_integer_range(-86_340, 86_340),
        ),
        (
            "endUtcOffsetSeconds",
            bounded_integer_range(-86_340, 86_340),
        ),
    ]);
    let entry = closed_complete(vec![
        ("entry", bounded_unsigned_range(1, 64)),
        (
            "status",
            enumeration(&[
                "scheduled_sessions",
                "explicitly_open",
                "explicitly_closed",
                "unknown",
            ]),
        ),
        (
            "sessionPresence",
            enumeration(&["reported", "missing", "source_null", "unknown"]),
        ),
        ("windows", bounded_array(window, maximum)),
    ]);
    closed_complete(vec![
        ("reference", reference()),
        (
            "product",
            enumeration(super::super::MARKET_SESSION_PRODUCTS),
        ),
        ("date", super::super::calendar_date_argument_schema()),
        ("coverage", constant("returned_entries_only")),
        ("entries", bounded_nonempty_array(entry, 64)),
    ])
}
