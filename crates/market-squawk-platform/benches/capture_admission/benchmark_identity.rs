//! Shared benchmark target identities and evidence authority labels.

pub(crate) const EVIDENCE_TARGET: &str = "capture_admission_evidence";
pub(crate) const EVIDENCE_CARGO_TARGET: &str = "capture-admission-evidence";
pub(crate) const CRITERION_TARGET: &str = "capture_admission_criterion";
pub(crate) const FIXED_QUOTA_EVIDENCE_MODE: &str = "diagnostic_fixed_quota";
pub(crate) const CRITERION_EVIDENCE_MODE: &str = "exploratory_zero_authority";

pub(crate) fn verify_distinct_authority_labels() -> Result<(), &'static str> {
    if EVIDENCE_TARGET == CRITERION_TARGET
        || EVIDENCE_CARGO_TARGET == EVIDENCE_TARGET
        || EVIDENCE_CARGO_TARGET == CRITERION_TARGET
        || FIXED_QUOTA_EVIDENCE_MODE == CRITERION_EVIDENCE_MODE
        || !CRITERION_EVIDENCE_MODE.contains("zero_authority")
    {
        return Err("benchmark target authority identities are not distinct");
    }
    Ok(())
}
