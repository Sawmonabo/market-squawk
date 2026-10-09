//! Application-owned SEC aggregate request policy.
//!
//! The SEC's published fair-access ceiling and Market Squawk's deliberately lower operating
//! policy are different facts. The shared SEC authority constructs the application budget used
//! by onboarding and fresh/restored runtime activation.

use market_squawk_sources::{ProviderBudgetPolicy, SEC_EDGAR_AUTHORITY};

use crate::SecClientError;

/// SEC-published aggregate automated-access ceiling.
pub const SEC_OFFICIAL_REQUEST_CEILING_PER_SECOND: u32 = 10;

/// Stable collision scope shared by every SEC surface in this application.
pub const SEC_PROVIDER_RATE_SCOPE: &str = SEC_EDGAR_AUTHORITY.rate_scope();

/// Returns the shared code-owned application budget for public SEC requests.
///
/// This is intentionally below the SEC's published ceiling. Provider metadata, onboarding, fresh
/// activation, and restored activation use the same SEC authority descriptor. A provider response
/// may still require a longer `Retry-After` through the shared rate authority.
pub fn sec_application_budget_policy() -> Result<ProviderBudgetPolicy, SecClientError> {
    SEC_EDGAR_AUTHORITY
        .budget_policy()
        .map_err(|_| SecClientError::UnsafeBudgetPolicy)
}
