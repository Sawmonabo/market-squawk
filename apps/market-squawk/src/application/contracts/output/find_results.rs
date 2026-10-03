//! Exact final Find output; financial display schema is shared with the saved analysis.
use super::*;

pub(super) fn result() -> Value {
    let mut result=closed_complete(vec![
        ("preparationId", uuid()),
        ("preparationSha256", lowercase_sha256()),
        ("resultSha256", lowercase_sha256()),
        (
            "ordering",
            enumeration(&[
                "estimated_gain_descending",
                "unavailable_incomparable_horizons",
            ]),
        ),
        ("screenRunId", nullable(bounded_text(256))),
        ("coverageReference", current_find::reference()),
        ("coverage", current_find::coverage()),
        (
            "results",
            bounded_array(
                closed_complete(vec![
                    ("candidateId", bounded_text(256)),
                    ("actionToken", nullable(uuid())),
                    ("analysisId", nullable(lowercase_sha256())),
                    ("screenRank", bounded_unsigned_range(1, 32)),
                    ("rank", bounded_unsigned_range(1, 32)),
                    (
                        "analysisState",
                        enumeration(&["generated", "no_action", "unavailable"]),
                    ),
                    ("expectedReturn", investment_analysis::expected_return()),
                ]),
                32,
            ),
        ),
        (
            "benchmarks",
            closed_complete(vec![
                ("primary", constant("SPY")),
                ("alongside", constant("VTI")),
            ]),
        ),
    ]);
    // Existing real-analysis rows retain their original byte-level projection. The new field
    // appears only for a source-assessed member and is validated against custody by its owner.
    result["properties"]["results"]["items"]["properties"]["memberUnavailable"]=member_unavailable();
    result
}

pub(in crate::application::contracts) fn member_context() -> Value {
    closed_complete(vec![
        ("preparationId", uuid()),
        ("preparationSha256", lowercase_sha256()),
        ("candidateId", bounded_text(256)),
        ("screenRunId", bounded_text(256)),
    ])
}
pub(super) fn member_unavailable() -> Value {
    closed_complete(vec![
        ("member", member_context()),
        ("assessmentSha256", lowercase_sha256()),
        (
            "reason",
            enumeration(&["identity_unavailable", "source_evidence_unavailable"]),
        ),
        (
            "sourceFailures",
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
                        ]),
                    ),
                    ("failure", bounded_text(1024)),
                ]),
                6,
            ),
        ),
    ])
}
pub(in crate::application::contracts) fn analysis_references() -> Value {
    let mut analyzed=closed_complete(vec![("candidateId",bounded_text(256)),("actionToken",uuid())]);
    analyzed["properties"]["unavailableAssessmentSha256"]=serde_json::json!({"type":"null"});
    bounded_array(one_of(vec![analyzed,
        closed_complete(vec![("candidateId",bounded_text(256)),("actionToken",serde_json::json!({"type":"null"})),("unavailableAssessmentSha256",lowercase_sha256())]),
    ]),32)
}
