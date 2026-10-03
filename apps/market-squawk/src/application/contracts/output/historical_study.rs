//! Exact historical source-plan and immutable fiscal-page wires. References confer no authority.
use super::*;
use crate::application::{
    HISTORICAL_FISCAL_MAXIMUM_ORIGINS, HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES,
    HISTORICAL_FISCAL_MAXIMUM_PAGES, HISTORICAL_FISCAL_PAGE_SIZE,
};

fn bytes32() -> Value {
    fixed_array(bounded_unsigned(255), 32)
}
fn source_digest() -> Value {
    closed_complete(vec![
        ("algorithm", constant("sha256")),
        ("bytes", bytes32()),
    ])
}
fn source_time() -> Value {
    json!({"type":"integer","minimum":i64::MIN,"maximum":i64::MAX})
}
fn benchmark() -> Value {
    closed_complete(vec![
        ("instrument_id", uuid()),
        ("revision_digest", source_digest()),
        (
            "revision_sequence",
            bounded_unsigned_range(1, u32::MAX.into()),
        ),
        ("published_at", source_time()),
    ])
}
fn benchmarks() -> Value {
    closed_complete(vec![
        ("version", constant_unsigned(1)),
        ("knowledge_at", source_time()),
        ("effective_at", source_time()),
        ("selected_at", source_time()),
        ("primary", benchmark()),
        ("accompanying", benchmark()),
        ("population_receipt_digest", source_digest()),
        ("selection_digest", bytes32()),
    ])
}
pub(in crate::application::contracts) fn plan_reference() -> Value {
    closed_complete(vec![
        ("version", constant_unsigned(1)), ("subjectInstrumentId", uuid()),
        ("sourceCutoffUnixNanos", integer_text()),
        ("financialProfile", analytical_profile_resolution()), ("benchmarks", benchmarks()),
        ("sourceActionReference", crate::application::research::corporate_actions::SourceAppliedCorporateActionPlanReference::json_schema()),
        ("sources", fixed_array(bytes32(), 3)), ("planDigest", bytes32()),
    ])
}
pub(in crate::application::contracts) fn job_reference() -> Value {
    closed_complete(vec![
        ("jobId", uuid()),
        ("generation", bounded_unsigned_range(1, u64::MAX)),
    ])
}
pub(in crate::application::contracts) fn price_example() -> Value {
    // The source owner admits ASCII identifiers through 256 bytes, independently of page size.
    bounded_text(256)
}
pub(in crate::application::contracts) fn target_id() -> Value {
    json!({"type":"string","enum":crate::application::fiscal_projection_targets()
        .into_iter().map(|target| target.target_id).collect::<Vec<_>>()})
}
pub(in crate::application::contracts) fn fiscal_origin() -> Value {
    closed_complete(vec![
        ("studyInputJob", job_reference()),
        ("priceExampleId", price_example()),
        ("targetId", target_id()),
    ])
}
pub(in crate::application::contracts) fn completed_fold() -> Value {
    closed_complete(vec![
        ("jobId", uuid()),
        ("generation", bounded_unsigned_range(1, u64::MAX)),
        ("datasetJob", job_reference()),
    ])
}
pub(in crate::application::contracts) fn completed_target() -> Value {
    closed_complete(vec![
        ("priceExampleId", price_example()),
        ("targetId", target_id()),
        ("trainingDatasetJob", job_reference()),
        ("inputDatasetJob", job_reference()),
        ("trainingJob", job_reference()),
    ])
}
fn dataset_reference() -> Value {
    closed_complete(vec![
        ("dataset_id", bounded_text(256)),
        ("build_spec", bytes32()),
        ("manifest_version", bounded_unsigned_range(1, u64::MAX)),
        ("manifest_hash", bytes32()),
    ])
}
fn binding() -> Value {
    closed_complete(vec![
        ("planIdentity", bytes32()),
        ("profileIdentity", bytes32()),
        ("subject", uuid()),
        ("sourceCutoff", source_time()),
        ("evaluationStart", source_time()),
        ("evaluationEnd", source_time()),
        ("priceInputs", dataset_reference()),
        ("studyInputJob", job_reference()),
        (
            "totalOrigins",
            bounded_unsigned_range(1, HISTORICAL_FISCAL_MAXIMUM_ORIGINS as u64),
        ),
        ("epochSetDigest", bytes32()),
    ])
}
pub(in crate::application::contracts) fn page_ordinal() -> Value {
    bounded_unsigned((HISTORICAL_FISCAL_MAXIMUM_PAGES - 1) as u64)
}
fn page() -> Value {
    closed_complete(vec![
        ("binding", binding()),
        ("pageOrdinal", page_ordinal()),
        (
            "totalOrigins",
            bounded_unsigned_range(1, HISTORICAL_FISCAL_MAXIMUM_ORIGINS as u64),
        ),
        (
            "totalPages",
            bounded_unsigned_range(1, HISTORICAL_FISCAL_MAXIMUM_PAGES as u64),
        ),
        ("epochSetDigest", bytes32()),
        (
            "origins",
            bounded_nonempty_array(
                closed_complete(vec![
                    ("priceExampleId", price_example()),
                    ("epochIdentity", bytes32()),
                    ("economicOriginUnixNanos", integer_text()),
                ]),
                HISTORICAL_FISCAL_PAGE_SIZE,
            ),
        ),
    ])
}
pub(in crate::application::contracts) fn page_reference() -> Value {
    closed_complete(vec![
        ("bindingDigest", bytes32()),
        ("pageOrdinal", page_ordinal()),
        (
            "originCount",
            bounded_unsigned_range(1, HISTORICAL_FISCAL_PAGE_SIZE as u64),
        ),
        (
            "artifact",
            closed_complete(vec![
                ("artifact_id", bounded_text(160)),
                ("sha256", lowercase_sha256()),
                (
                    "byte_count",
                    bounded_unsigned_range(1, HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES as u64),
                ),
            ]),
        ),
    ])
}
fn fiscal() -> Value {
    one_of(vec![
        json!({"type":"null"}),
        closed_complete(vec![
            ("page", page()),
            ("targets", fixed_array(fiscal_projection_target(), 9)),
        ]),
        closed_complete(vec![
            ("page", page()),
            ("targets", fixed_array(fiscal_projection_target(), 9)),
            ("fiscalPage", page_reference()),
        ]),
        closed_complete(vec![
            ("priceExampleId", price_example()),
            (
                "targets",
                fixed_array(
                    closed_complete(vec![
                        ("target", fiscal_projection_target()),
                        ("availability", enumeration(&["ready", "unavailable"])),
                    ]),
                    9,
                ),
            ),
        ]),
    ])
}
pub(in crate::application::contracts) fn result() -> Value {
    one_of(vec![
        closed_complete(vec![
            ("status", constant("available")),
            ("plan", plan_reference()),
            ("fiscal", fiscal()),
            (
                "folds",
                fixed_array(
                    closed_complete(vec![
                        ("foldIndex", bounded_unsigned(2)),
                        ("startsAtUnixNanos", integer_text()),
                        ("endsAtUnixNanos", integer_text()),
                    ]),
                    3,
                ),
            ),
        ]),
        closed_complete(vec![
            ("status", constant("unavailable")),
            (
                "reasons",
                fixed_array(constant("insufficient_source_qualified_history"), 1),
            ),
        ]),
    ])
}
pub(in crate::application::contracts) fn completed_page() -> Value {
    closed_complete(vec![
        ("status", constant("completed")),
        ("page", page()),
        ("fiscalPage", page_reference()),
    ])
}
