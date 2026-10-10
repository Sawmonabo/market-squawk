//! SEC source declaration and official request ceiling.
//!
//! The SEC's published fair-access ceiling and Market Squawk's deliberately lower operating
//! policy are different facts. The source declaration stays stable across scheduling changes;
//! the shared provider-rate authority owns current operational concurrency.

use market_squawk_sources::{ProviderBudgetPolicy, SEC_EDGAR_AUTHORITY};

use crate::SecClientError;

/// SEC-published aggregate automated-access ceiling.
pub const SEC_OFFICIAL_REQUEST_CEILING_PER_SECOND: u32 = 10;

/// Stable collision scope shared by every SEC surface in this application.
pub const SEC_PROVIDER_RATE_SCOPE: &str = SEC_EDGAR_AUTHORITY.rate_scope();

/// Returns the stable budget declaration used to validate SEC source metadata.
///
/// This is intentionally below the SEC's published ceiling. Provider metadata, onboarding, fresh
/// activation, and restored activation retain the same SEC source descriptor. Runtime request
/// concurrency is resolved separately by the shared provider-rate authority. A provider response
/// may still require a longer `Retry-After` through the shared rate authority.
pub fn sec_application_budget_policy() -> Result<ProviderBudgetPolicy, SecClientError> {
    SEC_EDGAR_AUTHORITY
        .budget_policy()
        .map_err(|_| SecClientError::UnsafeBudgetPolicy)
}
