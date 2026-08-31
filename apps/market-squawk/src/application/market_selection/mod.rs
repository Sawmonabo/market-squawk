//! Pure, bounded source selection for the unified Markets read authority.
//!
//! This module owns no provider connection, cache, subscription, or persistence authority. It
//! consumes already-observed source candidates and returns a deterministic, source-preserving
//! selection receipt for the application read model.

mod candidate;
mod digest;
mod investment;
mod receipt;
mod requirements;
mod resolver;

pub(crate) use candidate::{
    BudgetAvailability, CandidateAdmissionState, CandidateCapabilities, CandidateHealth,
    CandidateIdentity, CandidateIntegrity, CandidateTimestamps, HealthState, IntegrityState,
    ProviderBudgetSnapshot, RightsAdmission, RightsState, SourceCandidate,
};
pub(crate) use investment::{
    LiveMarketInvestmentSource, MarketFeatureEvidence, MarketFeatureUnavailableReason,
    MarketInvestmentMarkBasis, MarketInvestmentObservation, MarketInvestmentRead,
    MarketInvestmentReadError, MarketInvestmentUnavailableReason, SelectedMarketInvestmentSource,
    read_market_investment_observation, selected_generation_matches,
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
