//! Pure, bounded source selection for the unified Markets read authority.
//!
//! This module owns no provider connection, cache, subscription, or persistence authority. It
//! consumes already-observed source candidates and returns a deterministic, source-preserving
//! selection receipt for the application read model.

mod candidate;
mod digest;
mod investment;
pub(crate) mod product;
mod receipt;
mod requirements;
mod resolver;

pub(crate) use candidate::{
    BudgetAvailability, CandidateAdmissionState, CandidateCapabilities, CandidateHealth,
    CandidateIdentity, CandidateIntegrity, CandidateTimestamps, HealthState, IntegrityState,
    ProviderBudgetSnapshot, RightsAdmission, RightsState, SourceCandidate,
};
pub use investment::MarketInvestmentReadReference;
pub(crate) use investment::{
    LiveMarketInvestmentSource, MarketFeatureEvidence, MarketFeatureUnavailableReason,
    MarketInvestmentMarkBasis, MarketInvestmentObservation, MarketInvestmentRead,
    MarketInvestmentReadCapability, MarketInvestmentReadError, MarketInvestmentReadReceipt,
    MarketInvestmentUnavailableReason, NativeReferenceUse, SelectedMarketInvestmentSource,
    map_market_event_read_error, read_market_investment_observation, selected_generation_matches,
    validate_native_reference, validate_retained_native_reference,
};
pub(crate) use receipt::{
    AdmittedDowngrade, DowngradeDimension, MarketSelectionError, MarketSelectionReceipt,
    SelectedMarketSource, SelectionClass,
};
pub(crate) use requirements::{
    DowngradePolicy, FreshnessBasis, FreshnessRequirement, MarketCoverage, MarketOperation,
    MarketOperationSet, MarketSelectionPolicy, MarketSelectionRequest, ObservationTiming,
    RequestPriority,
};
pub(crate) use resolver::select_market_source;

#[cfg(test)]
mod tests;
