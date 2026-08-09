//! Pure, bounded source selection for the unified Markets read authority.
//!
//! This module owns no provider connection, cache, subscription, or persistence authority. It
//! consumes already-observed source candidates and returns a deterministic, source-preserving
//! selection receipt for the application read model.

#![allow(
    dead_code,
    unused_imports,
    reason = "the Wave M1 selection contract is consumed by the immediately following read-model integration"
)]

mod candidate;
mod receipt;
mod requirements;
mod resolver;

pub(crate) use candidate::{
    BudgetAvailability, CandidateAdmissionState, CandidateCapabilities, CandidateHealth,
    CandidateIdentity, CandidateIntegrity, CandidateTimestamps, HealthState, IntegrityState,
    ProviderBudgetSnapshot, RightsAdmission, RightsState, SourceCandidate,
};
pub(crate) use receipt::{
    AdmittedDowngrade, CandidateRejectionReason, DowngradeDimension, EligibleCandidate,
    MarketSelectionError, MarketSelectionReceipt, RejectedCandidate, SelectedMarketSource,
    SelectionClass,
};
pub(crate) use requirements::{
    DowngradePolicy, FreshnessBasis, FreshnessRequirement, MarketCoverage, MarketOperation,
    MarketOperationSet, MarketSelectionPolicy, MarketSelectionRequest, ObservationTiming,
    RequestPriority,
};
pub(crate) use resolver::select_market_source;

#[cfg(test)]
mod tests;
