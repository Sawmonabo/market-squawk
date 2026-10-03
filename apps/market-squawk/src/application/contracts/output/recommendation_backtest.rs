//! Financial study reports with explicit population, cost and comparison denominators.

use serde_json::Value;

use super::{
    boolean, bounded_array, bounded_text, canonical_decimal_text as decimal, closed_complete,
    constant, currency_code, enumeration, integer_text, nullable, one_of, positive_integer_text,
    sha256, unsigned, unsigned_integer_text, uuid,
};

pub(super) fn result() -> Value {
    let benchmark = closed_complete(vec![("instrumentId", uuid()), ("approvalDigest", sha256())]);
    let mut fields = vec![
        ("requestDigest", sha256()),
        ("evidenceDigest", sha256()),
        (
            "studyBasis",
            enumeration(&["historical_as_known", "retrospective_frozen_snapshot"]),
        ),
        (
            "studyLimitations",
            bounded_array(
                enumeration(&[
                    "historical_revision_coverage_unproven",
                    "later_vintage_inputs",
                    "present_day_fixed_cohort",
                    "simulated_availability",
                ]),
                4,
            ),
        ),
        ("snapshotAsOfUnixNanos", integer_text()),
        ("sourceSnapshotDigest", sha256()),
        ("targetHorizonNanos", positive_integer_text()),
        ("population", population()),
        (
            "folds",
            bounded_array(
                closed_complete(vec![
                    ("foldId", bounded_text(256)),
                    ("startsAtUnixNanos", integer_text()),
                    ("endsAtUnixNanos", integer_text()),
                    ("population", population()),
                ]),
                1024,
            ),
        ),
        (
            "aggregate",
            one_of(vec![
                closed_complete(vec![
                    ("status", constant("available")),
                    ("observationCount", unsigned()),
                    ("independentFoldCount", unsigned()),
                    ("meanCostAdjustedReturn", decimal()),
                    ("worstMaximumDrawdown", decimal()),
                    ("positiveFoldCount", unsigned()),
                    ("positiveFoldStability", decimal()),
                    ("benchmark", benchmark_result()),
                    ("accompanyingBenchmark", benchmark_result()),
                ]),
                closed_complete(vec![
                    ("status", constant("unavailable")),
                    (
                        "reason",
                        enumeration(&[
                            "truncated-signal-population",
                            "incomplete-declared-entry",
                            "missing-completed-observation-in-fold",
                        ]),
                    ),
                ]),
            ]),
        ),
        (
            "methodology",
            closed_complete(vec![
                ("policyDigest", sha256()),
                ("subjectInstrumentId", uuid()),
                ("reportingCurrency", currency_code()),
                ("decisionLagNanos", nullable(unsigned_integer_text())),
                (
                    "executionPriceRounding",
                    constant("adverse-tick-rounding-after-costs"),
                ),
                ("priceBasis", constant("raw-with-corporate-action-ledger")),
                (
                    "targetTiming",
                    constant("financial-origin-plus-365-elapsed-days"),
                ),
                (
                    "distributionTreatment",
                    constant("cash-entitlement-without-reinvestment"),
                ),
                ("distributionTiming", bounded_text(1024)),
                (
                    "executionBasis",
                    enumeration(&["observed-quote-depth", "completed-daily-bar"]),
                ),
                ("assumedFullSpreadBasisPoints", nullable(unsigned())),
                (
                    "fillTiming",
                    enumeration(&[
                        "next-eligible-observation",
                        "next-eligible-completed-bar-close",
                    ]),
                ),
                (
                    "participationBasis",
                    enumeration(&["observed-executable-depth", "completed-bar-traded-volume"]),
                ),
                ("executionLimitations", bounded_array(bounded_text(1024), 4)),
                ("rawPriceEvidenceDigest", sha256()),
                ("corporateActionContentDigest", sha256()),
                ("corporateActionAuditDigest", sha256()),
                ("corporateActionCoverageStartsAtUnixNanos", integer_text()),
                ("primaryBenchmark", benchmark.clone()),
                ("accompanyingBenchmark", benchmark),
            ]),
        ),
        (
            "executionAssumptions",
            closed_complete(vec![
                ("feeBasisPointsPerLeg", unsigned()),
                ("slippageBasisPointsPerLeg", unsigned()),
                ("maximumRandomSlippageBasisPointsPerLeg", unsigned()),
                ("maximumParticipationBasisPoints", unsigned()),
                ("latencyNanos", positive_integer_text()),
                ("allowPartialFills", boolean()),
                ("digest", sha256()),
            ]),
        ),
    ];
    for name in [
        "simulationCutoffUnixNanos",
        "evaluatedAtUnixNanos",
        "publishedAtUnixNanos",
        "availableAtUnixNanos",
        "expiresAtUnixNanos",
    ] {
        fields.push((name, integer_text()));
    }
    closed_complete(fields)
}

fn population() -> Value {
    closed_complete(
        [
            "totalSignals",
            "completedSubjectAndBenchmark",
            "noAction",
            "unavailable",
            "censoredTargetAfterCutoff",
            "censoredOutsideFold",
            "entryUnfilled",
            "exitUnfilled",
            "benchmarkUnavailable",
            "completedSubject",
            "accompanyingBenchmarkCompleted",
            "accompanyingBenchmarkUnavailable",
        ]
        .into_iter()
        .map(|name| (name, unsigned()))
        .collect(),
    )
}

fn benchmark_result() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("status", constant("available")),
            ("meanCostAdjustedReturn", decimal()),
            ("meanExcessReturn", decimal()),
        ]),
        closed_complete(vec![("status", constant("unavailable"))]),
    ])
}
