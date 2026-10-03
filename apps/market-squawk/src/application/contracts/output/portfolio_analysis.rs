//! Saved portfolio inputs and their financial result, shared across product transports.

use serde_json::Value;

use super::{
    bounded_text, canonical_decimal_text as decimal, closed_complete, constant, constant_bool,
    enumeration, integer_text, investment_analysis_sha256 as exact_digest, money, nullable, sha256,
    unsigned, uuid,
};

pub(crate) fn reference() -> Value {
    closed_complete(vec![
        ("prerequisites", prerequisite_reference()),
        (
            "calendar",
            nullable(closed_complete(vec![
                ("originContentDigest", exact_digest()),
                ("captureBindingDigest", exact_digest()),
            ])),
        ),
    ])
}

fn prerequisite_reference() -> Value {
    let mut fields = vec![
        ("candidateInstrumentId", uuid()),
        ("sourceCutoffUnixNanos", integer_text()),
        ("accountId", uuid()),
        ("portfolioRevision", sha256()),
        ("minimumHistoricalReturnObservations", unsigned()),
        ("maximumHistoricalReturnObservations", unsigned()),
        ("calculatedAtUnixNanos", integer_text()),
        ("status", enumeration(&["evaluated", "unavailable"])),
    ];
    for name in [
        "setupAuthorityDigest",
        "configurationDigest",
        "profileDigest",
        "catalogDigest",
        "prerequisitePolicyDigest",
        "evidenceDigest",
        "portfolioSnapshotDigest",
        "marketSetDigest",
    ] {
        fields.push((name, sha256()));
    }
    closed_complete(fields)
}

pub(super) fn result() -> Value {
    closed_complete(vec![
        (
            "status",
            enumeration(&["setup_required", "unavailable", "evaluated"]),
        ),
        ("summary", bounded_text(1024)),
        ("instrumentId", uuid()),
        ("accountId", nullable(uuid())),
        ("portfolioAsOfUnixNanos", nullable(integer_text())),
        ("sourceCutoffUnixNanos", integer_text()),
        ("calculatedAtUnixNanos", nullable(integer_text())),
        ("financialConfigurationDigest", sha256()),
        ("reference", nullable(reference())),
        (
            "markedPortfolio",
            nullable(closed_complete(vec![
                ("equity", money()),
                ("cash", money()),
                ("receivables", money()),
                ("holdingCount", unsigned()),
                ("candidateQuantity", nullable(decimal())),
                ("candidateValue", nullable(money())),
            ])),
        ),
        (
            "historicalRisk",
            nullable(closed_complete(vec![
                ("basis", bounded_text(128)),
                ("interpretation", bounded_text(1024)),
                ("confidenceBasisPoints", unsigned()),
                ("valueAtRiskReturn", decimal()),
                ("expectedShortfallReturn", decimal()),
                ("remainingDownsideBudgetPpm", unsigned()),
                ("observations", unsigned()),
                ("sourceObservations", unsigned()),
                ("holdings", unsigned()),
                ("sampleStartUnixNanos", nullable(integer_text())),
                ("sampleEndUnixNanos", nullable(integer_text())),
                (
                    "currentSessionCoverage",
                    enumeration(&["complete", "unavailable", "not_applicable"]),
                ),
                ("cashAssumption", bounded_text(1024)),
            ])),
        ),
        ("analysisOnly", constant_bool(true)),
        ("executionAuthority", constant("none")),
    ])
}
