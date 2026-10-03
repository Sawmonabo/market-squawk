//! Application-owned SEC aggregate request policy.
//!
//! The SEC's published fair-access ceiling and Market Squawk's deliberately lower operating
//! policy are different facts. Keeping both values here prevents onboarding and restored runtime
//! activation from drifting to independent, more aggressive budgets.

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_domain::SourceIdentifier;
use market_squawk_sources::{BackoffPolicy, BudgetScope, ProviderBudgetPolicy};

use crate::SecClientError;

/// SEC-published aggregate automated-access ceiling.
pub const SEC_OFFICIAL_REQUEST_CEILING_PER_SECOND: u32 = 10;

/// Market Squawk's app-wide SEC request target.
pub const SEC_APPLICATION_REQUESTS_PER_SECOND: u32 = 2;

/// Market Squawk serializes SEC sends through one application queue.
pub const SEC_APPLICATION_MAX_CONCURRENT_REQUESTS: u16 = 1;

/// Stable collision scope shared by every SEC surface in this application.
pub const SEC_PROVIDER_RATE_SCOPE: &str = "us-sec-edgar";

const SECOND_NANOS: u64 = 1_000_000_000;
const MINUTE_NANOS: u64 = 60 * SECOND_NANOS;

/// Builds the sole code-owned application budget for public SEC requests.
///
/// This is intentionally below the SEC's published ceiling. Provider metadata, onboarding, fresh
/// activation, and restored activation should all call this function instead of repeating numeric
/// values. A provider response may still require a longer `Retry-After` through the shared rate
/// authority.
pub fn sec_application_budget_policy() -> Result<ProviderBudgetPolicy, SecClientError> {
    let scope = BudgetScope::new(SourceIdentifier::try_from(SEC_PROVIDER_RATE_SCOPE)?);
    let backoff = BackoffPolicy::try_new(
        NonZeroU64::new(SECOND_NANOS).ok_or(SecClientError::UnsafeBudgetPolicy)?,
        NonZeroU64::new(MINUTE_NANOS).ok_or(SecClientError::UnsafeBudgetPolicy)?,
        0,
    )?;
    ProviderBudgetPolicy::try_new(
        scope,
        NonZeroU32::new(SEC_APPLICATION_REQUESTS_PER_SECOND)
            .ok_or(SecClientError::UnsafeBudgetPolicy)?,
        NonZeroU64::new(SECOND_NANOS).ok_or(SecClientError::UnsafeBudgetPolicy)?,
        NonZeroU16::new(SEC_APPLICATION_MAX_CONCURRENT_REQUESTS)
            .ok_or(SecClientError::UnsafeBudgetPolicy)?,
        backoff,
    )
    .map_err(Into::into)
}
