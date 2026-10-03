//! Closed financial-screen preparation, original coverage, and saved ranking projections.

use super::*;

pub(super) fn reference() -> Value {
    closed_complete(vec![
        ("preparationId", uuid()),
        ("preparationSha256", lowercase_sha256()),
    ])
}

pub(super) fn coverage() -> Value {
    closed_complete(vec![
        ("scope", constant("admitted_canonical_catalog")),
        ("complete", boolean()),
        ("canonicalPopulationCount", bounded_unsigned(65_536)),
        ("populationCount", bounded_unsigned(65_536)),
        ("excludedCount", bounded_unsigned(65_536)),
        ("unavailableCount", bounded_unsigned(65_536)),
        ("preparedPartitionCount", bounded_unsigned(65_536)),
        ("partitionCount", bounded_unsigned(65_536)),
    ])
}

pub(super) fn preparation() -> Value {
    closed_complete(vec![
        ("preparationId", uuid()),
        ("preparationSha256", lowercase_sha256()),
        ("sourceCutoffUnixNanos", integer_text()),
        ("forecastCohort", nullable(forecast_cohort())),
        ("partitionCount", bounded_unsigned(65_536)),
        ("populationCount", bounded_unsigned(65_536)),
        (
            "maximumCandidates",
            json!({"type":"integer","enum":[8,16,32]}),
        ),
        ("coverage", coverage()),
        ("coverageReference", reference()),
        ("availablePartitionCount", bounded_unsigned(65_536)),
    ])
}

pub(super) fn partition() -> Value {
    closed_complete(vec![
        ("preparationId", uuid()),
        ("preparationSha256", lowercase_sha256()),
        ("ordinal", bounded_unsigned(65_535)),
        ("buildRequired", boolean()),
        ("memberCount", bounded_unsigned_range(1, 128)),
        ("unavailableCount", bounded_unsigned(128)),
    ])
}

pub(super) fn result() -> Value {
    closed_complete(vec![
        ("job", job_receipt()),
        ("preparationId", uuid()),
        ("preparationSha256", lowercase_sha256()),
        ("sourceCutoffUnixNanos", integer_text()),
        ("forecastCohort", nullable(forecast_cohort())),
        ("screenRunId", bounded_text(256)),
        (
            "candidates",
            bounded_array(
                closed_complete(vec![
                    ("instrumentId", uuid()),
                    ("candidateId", bounded_text(256)),
                    ("screenRunId", bounded_text(256)),
                    ("evidenceDigest", lowercase_sha256()),
                    ("selectionToken", bounded_text(512)),
                    ("rank", bounded_unsigned_range(1, 32)),
                    ("currentFeatureInput", forecast_current_feature_input()),
                ]),
                32,
            ),
        ),
        ("coverage", coverage()),
        ("coverageReference", reference()),
        ("rankingSha256", lowercase_sha256()),
    ])
}

pub(super) fn coverage_page() -> Value {
    closed_complete(vec![
        ("preparationId", uuid()),
        ("preparationSha256", lowercase_sha256()),
        ("ordinal", nullable(bounded_unsigned(65_535))),
        ("offset", bounded_unsigned(65_536)),
        ("total", bounded_unsigned(65_536)),
        (
            "rows",
            bounded_array(
                closed_complete(vec![
                    ("instrumentId", uuid()),
                    (
                        "reason",
                        enumeration(&[
                            "no_effective_canonical_definition",
                            "outside_profile_asset_scope",
                            "missing_official_listing",
                            "ambiguous_official_listing",
                            "source_history_unavailable",
                            "required_feature_unavailable",
                            "source_rights_unavailable",
                            "freshness_unavailable",
                            "calendar_unavailable",
                        ]),
                    ),
                ]),
                128,
            ),
        ),
    ])
}

pub(in crate::application::contracts) fn dataset_job() -> Value {
    closed_complete(vec![("jobId", uuid()), ("generation", positive_integer())])
}

/// Inert original-calendar coordinates; only the original reader can issue authority from them.
pub(in crate::application::contracts) fn forecast_cohort() -> Value {
    closed_complete(vec![
        (
            "calendar",
            closed_complete(vec![
                ("originContentDigest", lowercase_sha256()),
                ("captureBindingDigest", lowercase_sha256()),
            ]),
        ),
        ("sessionDate", calendar_date()),
        ("regularOpensAtUnixNanos", integer_text()),
        ("regularClosesAtUnixNanos", integer_text()),
        ("aggregationStartsAtUnixNanos", integer_text()),
        ("aggregationEndsAtUnixNanos", integer_text()),
        ("aggregationProviderTimestampUnixNanos", integer_text()),
        (
            "aggregationSemanticsSha256",
            fixed_array(bounded_unsigned(255), 32),
        ),
        (
            "calendarSessionEvidenceSha256",
            fixed_array(bounded_unsigned(255), 32),
        ),
        ("knowledgeCutoffUnixNanos", integer_text()),
        ("horizonNanos", integer_text()),
    ])
}

pub(in crate::application::contracts) fn completion_reference() -> Value {
    closed_complete(vec![("ordinal", bounded_unsigned(65_535)), ("completionSha256", lowercase_sha256())])
}
pub(super) fn completion() -> Value {
    closed_complete(vec![
        ("preparationId", uuid()), ("preparationSha256", lowercase_sha256()),
        ("completion", completion_reference()), ("datasetJob", nullable(dataset_job())),
        ("datasetContentSha256", nullable(lowercase_sha256())),
    ])
}
