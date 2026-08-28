//! Bounded HTTP `Retry-After` parsing at the shared provider-budget boundary.

use super::{BudgetDecision, SharedProviderBudget};

/// Applies one bounded HTTP `Retry-After` field to the shared provider budget.
///
/// Positive decimal seconds and standard HTTP dates retain provider-supplied timing. A missing,
/// malformed, zero, non-ASCII, or representation-overflowing field falls back to the budget's
/// capped refusal policy. A valid instruction that exceeds policy remains a fail-closed budget
/// decision; it is never weakened to fallback backoff.
pub fn apply_http_retry_after(
    budget: &SharedProviderBudget,
    field: Option<&[u8]>,
    fallback_jitter_sample_basis_points: u16,
) -> BudgetDecision {
    match crate::ProviderRateRetryAfterDisposition::parse_http(field).retry_after() {
        Some(retry_after) => budget.apply_retry_after(retry_after),
        None => budget.apply_refusal(fallback_jitter_sample_basis_points),
    }
}
