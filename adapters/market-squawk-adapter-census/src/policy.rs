use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_domain::SourceIdentifier;
use market_squawk_sources::{
    BackoffPolicy, BudgetScope, BudgetWindowSemantics, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderRateDeclaration,
};
use thiserror::Error;

use crate::{CENSUS_APPLICATION_REQUESTS_PER_DAY, CENSUS_APPLICATION_REQUESTS_PER_SECOND};

const CENSUS_SECOND_NANOS: u64 = 1_000_000_000;
const CENSUS_DAY_NANOS: u64 = 86_400_000_000_000;
const CENSUS_MAXIMUM_BACKOFF_NANOS: u64 = 60_000_000_000;

/// Builds the exact product-wide Census request allocation for one stable credential subject.
///
/// Both windows are conservative Market Squawk policy because Census does not publish a current
/// numeric keyed-request limit. App composition must register this declaration with the durable
/// `ProviderRateAuthority`, and use the same stable root authorization subject for the Census
/// source, doctor, and jobs. This adapter never creates a private quota pool.
pub fn census_provider_rate_declaration(
    authorization_subject: &SourceIdentifier,
) -> Result<ProviderRateDeclaration, CensusRateDeclarationError> {
    let provider = SourceIdentifier::try_from("us-census")
        .map_err(|_| CensusRateDeclarationError::InvalidRateDeclaration)?;
    let windows = [
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(CENSUS_APPLICATION_REQUESTS_PER_SECOND)
                .ok_or(CensusRateDeclarationError::InvalidRateDeclaration)?,
            NonZeroU64::new(CENSUS_SECOND_NANOS)
                .ok_or(CensusRateDeclarationError::InvalidRateDeclaration)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| CensusRateDeclarationError::InvalidRateDeclaration)?,
        ProviderBudgetWindow::try_new(
            NonZeroU32::new(CENSUS_APPLICATION_REQUESTS_PER_DAY)
                .ok_or(CensusRateDeclarationError::InvalidRateDeclaration)?,
            NonZeroU64::new(CENSUS_DAY_NANOS)
                .ok_or(CensusRateDeclarationError::InvalidRateDeclaration)?,
            BudgetWindowSemantics::Sliding,
        )
        .map_err(|_| CensusRateDeclarationError::InvalidRateDeclaration)?,
    ];
    let policy = ProviderBudgetPolicy::try_new_conjunctive(
        BudgetScope::with_authorization_account(provider, authorization_subject.clone()),
        &windows,
        NonZeroU16::new(1).ok_or(CensusRateDeclarationError::InvalidRateDeclaration)?,
        BackoffPolicy::try_new(
            NonZeroU64::new(CENSUS_SECOND_NANOS)
                .ok_or(CensusRateDeclarationError::InvalidRateDeclaration)?,
            NonZeroU64::new(CENSUS_MAXIMUM_BACKOFF_NANOS)
                .ok_or(CensusRateDeclarationError::InvalidRateDeclaration)?,
            0,
        )
        .map_err(|_| CensusRateDeclarationError::InvalidRateDeclaration)?,
    )
    .map_err(|_| CensusRateDeclarationError::InvalidRateDeclaration)?;
    ProviderRateDeclaration::try_for_authorization_subject(policy, authorization_subject)
        .map_err(|_| CensusRateDeclarationError::InvalidRateDeclaration)
}

/// Census shared provider-rate declaration failure.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CensusRateDeclarationError {
    /// The conservative shared provider-rate declaration could not be represented.
    #[error("invalid Census provider-rate declaration")]
    InvalidRateDeclaration,
}
