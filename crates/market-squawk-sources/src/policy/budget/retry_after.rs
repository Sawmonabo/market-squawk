//! Bounded HTTP `Retry-After` parsing at the shared provider-budget boundary.

use std::num::NonZeroU64;
use std::time::{SystemTime, UNIX_EPOCH};

use market_squawk_domain::Timestamp;

use super::{BudgetDecision, RetryAfter, SharedProviderBudget};

const MAX_RETRY_AFTER_FIELD_BYTES: usize = 128;
const NANOS_PER_SECOND: u64 = 1_000_000_000;

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
    match field.and_then(parse_retry_after) {
        Some(retry_after) => budget.apply_retry_after(retry_after),
        None => budget.apply_refusal(fallback_jitter_sample_basis_points),
    }
}

fn parse_retry_after(field: &[u8]) -> Option<RetryAfter> {
    if field.is_empty() || field.len() > MAX_RETRY_AFTER_FIELD_BYTES || !field.is_ascii() {
        return None;
    }
    let field = std::str::from_utf8(field).ok()?;
    if field.bytes().all(|byte| byte.is_ascii_digit()) {
        return field
            .parse::<u64>()
            .ok()
            .and_then(|seconds| seconds.checked_mul(NANOS_PER_SECOND))
            .and_then(NonZeroU64::new)
            .map(RetryAfter::Delay);
    }
    httpdate::parse_http_date(field)
        .ok()
        .and_then(system_time_to_timestamp)
        .map(RetryAfter::AtWallClock)
}

fn system_time_to_timestamp(value: SystemTime) -> Option<Timestamp> {
    let unix_nanos = match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_to_nanos(duration).and_then(|nanos| i64::try_from(nanos).ok())?,
        Err(error) => {
            let magnitude = duration_to_nanos(error.duration())?;
            i128::try_from(magnitude)
                .ok()
                .and_then(i128::checked_neg)
                .and_then(|nanos| i64::try_from(nanos).ok())?
        }
    };
    Some(Timestamp::from_unix_nanos(unix_nanos))
}

fn duration_to_nanos(duration: std::time::Duration) -> Option<u128> {
    u128::from(duration.as_secs())
        .checked_mul(u128::from(NANOS_PER_SECOND))
        .and_then(|nanos| nanos.checked_add(u128::from(duration.subsec_nanos())))
}
