use std::cmp::Ordering;

use market_squawk_domain::{
    AssetClass, ConnectionGeneration, DataQuality, ExecutionEligibility, InstrumentId, MarketDepth,
    ProviderChannel, ProviderProduct, SourceId, SourceIdentifier, Timestamp, VenueId,
};

use super::{
    MarketCoverage, MarketOperationSet, MarketSelectionError, ObservationTiming, RequestPriority,
};

/// Exact provider/source/instrument identity of one candidate observation.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CandidateIdentity {
    provider: SourceIdentifier,
    product: ProviderProduct,
    feed: ProviderChannel,
    source_id: SourceId,
    venue_id: Option<VenueId>,
    instrument_id: InstrumentId,
    observation_id: SourceIdentifier,
}

impl CandidateIdentity {
    pub(crate) const fn new(
        provider: SourceIdentifier,
        product: ProviderProduct,
        feed: ProviderChannel,
        source_id: SourceId,
        venue_id: Option<VenueId>,
        instrument_id: InstrumentId,
        observation_id: SourceIdentifier,
    ) -> Self {
        Self {
            provider,
            product,
            feed,
            source_id,
            venue_id,
            instrument_id,
            observation_id,
        }
    }

    pub(crate) const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    pub(crate) const fn product(&self) -> &ProviderProduct {
        &self.product
    }

    pub(crate) const fn feed(&self) -> &ProviderChannel {
        &self.feed
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn venue_id(&self) -> Option<&VenueId> {
        self.venue_id.as_ref()
    }

    pub(crate) const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    pub(crate) const fn observation_id(&self) -> &SourceIdentifier {
        &self.observation_id
    }

    pub(super) fn stable_cmp(&self, other: &Self) -> Ordering {
        self.provider
            .as_str()
            .cmp(other.provider.as_str())
            .then_with(|| {
                self.product
                    .as_source_identifier()
                    .as_str()
                    .cmp(other.product.as_source_identifier().as_str())
            })
            .then_with(|| {
                self.feed
                    .as_source_identifier()
                    .as_str()
                    .cmp(other.feed.as_source_identifier().as_str())
            })
            .then_with(|| self.source_id.cmp(&other.source_id))
            .then_with(|| self.venue_id.cmp(&other.venue_id))
            .then_with(|| self.instrument_id.cmp(&other.instrument_id))
            .then_with(|| self.observation_id.cmp(&other.observation_id))
    }
}

/// Complete timestamps used to preserve source provenance and evaluate selection cutoffs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CandidateTimestamps {
    effective_at: Timestamp,
    source_timestamp: Option<Timestamp>,
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
}

impl CandidateTimestamps {
    pub(crate) fn try_new(
        effective_at: Timestamp,
        source_timestamp: Option<Timestamp>,
        received_at: Timestamp,
        available_at: Timestamp,
        ingested_at: Timestamp,
    ) -> Result<Self, MarketSelectionError> {
        if received_at > available_at || available_at > ingested_at {
            return Err(MarketSelectionError::InvalidTimestampOrder);
        }
        Ok(Self {
            effective_at,
            source_timestamp,
            received_at,
            available_at,
            ingested_at,
        })
    }

    pub(crate) const fn effective_at(self) -> Timestamp {
        self.effective_at
    }

    pub(crate) const fn source_timestamp(self) -> Option<Timestamp> {
        self.source_timestamp
    }

    pub(crate) const fn received_at(self) -> Timestamp {
        self.received_at
    }

    pub(crate) const fn available_at(self) -> Timestamp {
        self.available_at
    }

    pub(crate) const fn ingested_at(self) -> Timestamp {
        self.ingested_at
    }
}

/// Provider runtime health retained independently of data quality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HealthState {
    Healthy,
    Degraded,
    Unavailable,
    Quarantined,
}

/// Timestamped source-health snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CandidateHealth {
    state: HealthState,
    observed_at: Timestamp,
}

impl CandidateHealth {
    pub(crate) const fn new(state: HealthState, observed_at: Timestamp) -> Self {
        Self { state, observed_at }
    }

    pub(crate) const fn state(self) -> HealthState {
        self.state
    }

    pub(crate) const fn observed_at(self) -> Timestamp {
        self.observed_at
    }
}

/// Provider-budget availability at the exact observed instant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BudgetAvailability {
    /// No upstream request is made because this candidate is already retained locally.
    NotRequired,
    Open,
    InteractiveOnly,
    Exhausted,
    Unknown,
}

/// Pure budget snapshot; it cannot reserve, consume, or replenish provider capacity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProviderBudgetSnapshot {
    availability: BudgetAvailability,
    remaining_units: Option<u64>,
    reset_at: Option<Timestamp>,
    observed_at: Timestamp,
}

impl ProviderBudgetSnapshot {
    pub(crate) fn try_new(
        availability: BudgetAvailability,
        remaining_units: Option<u64>,
        reset_at: Option<Timestamp>,
        observed_at: Timestamp,
    ) -> Result<Self, MarketSelectionError> {
        let invalid = match availability {
            BudgetAvailability::NotRequired => remaining_units.is_some() || reset_at.is_some(),
            BudgetAvailability::Open | BudgetAvailability::InteractiveOnly => {
                remaining_units == Some(0)
            }
            BudgetAvailability::Exhausted => {
                remaining_units.is_some_and(|remaining| remaining != 0)
            }
            BudgetAvailability::Unknown => remaining_units.is_some(),
        };
        if invalid {
            return Err(MarketSelectionError::InvalidBudgetSnapshot);
        }
        Ok(Self {
            availability,
            remaining_units,
            reset_at,
            observed_at,
        })
    }

    pub(crate) const fn availability(self) -> BudgetAvailability {
        self.availability
    }

    pub(crate) const fn remaining_units(self) -> Option<u64> {
        self.remaining_units
    }

    pub(crate) const fn reset_at(self) -> Option<Timestamp> {
        self.reset_at
    }

    pub(crate) const fn observed_at(self) -> Timestamp {
        self.observed_at
    }

    pub(super) const fn admits(self, priority: RequestPriority) -> bool {
        match self.availability {
            BudgetAvailability::NotRequired | BudgetAvailability::Open => true,
            BudgetAvailability::InteractiveOnly => {
                matches!(priority, RequestPriority::Interactive)
            }
            BudgetAvailability::Exhausted | BudgetAvailability::Unknown => false,
        }
    }

    pub(super) const fn preference(self) -> u8 {
        match self.availability {
            BudgetAvailability::NotRequired => 3,
            BudgetAvailability::Open => 2,
            BudgetAvailability::InteractiveOnly => 1,
            BudgetAvailability::Exhausted | BudgetAvailability::Unknown => 0,
        }
    }
}

/// State of the exact rights decision carried by a source candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RightsState {
    Admitted,
    Unknown,
    Denied,
}

/// Exact rights decision and validity interval for a provider surface.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RightsAdmission {
    decision_id: SourceIdentifier,
    state: RightsState,
    permitted_operations: MarketOperationSet,
    decided_at: Timestamp,
    effective_from: Option<Timestamp>,
    effective_until: Option<Timestamp>,
}

impl RightsAdmission {
    pub(crate) fn try_admitted(
        decision_id: SourceIdentifier,
        permitted_operations: MarketOperationSet,
        decided_at: Timestamp,
        effective_from: Timestamp,
        effective_until: Option<Timestamp>,
    ) -> Result<Self, MarketSelectionError> {
        if effective_until.is_some_and(|until| until < effective_from) {
            return Err(MarketSelectionError::InvalidRightsInterval);
        }
        Ok(Self {
            decision_id,
            state: RightsState::Admitted,
            permitted_operations,
            decided_at,
            effective_from: Some(effective_from),
            effective_until,
        })
    }

    pub(crate) fn unavailable(
        decision_id: SourceIdentifier,
        state: RightsState,
        observed_at: Timestamp,
    ) -> Result<Self, MarketSelectionError> {
        if state == RightsState::Admitted {
            return Err(MarketSelectionError::InvalidRightsState);
        }
        Ok(Self {
            decision_id,
            state,
            permitted_operations: MarketOperationSet::empty(),
            decided_at: observed_at,
            effective_from: None,
            effective_until: None,
        })
    }

    pub(crate) const fn decision_id(&self) -> &SourceIdentifier {
        &self.decision_id
    }

    pub(crate) const fn state(&self) -> RightsState {
        self.state
    }

    pub(crate) const fn permitted_operations(&self) -> MarketOperationSet {
        self.permitted_operations
    }

    pub(crate) const fn decided_at(&self) -> Timestamp {
        self.decided_at
    }

    pub(crate) const fn effective_from(&self) -> Option<Timestamp> {
        self.effective_from
    }

    pub(crate) const fn effective_until(&self) -> Option<Timestamp> {
        self.effective_until
    }
}

/// Integrity state retained independently of quality, coverage, and fair-value hierarchy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IntegrityState {
    Verified,
    Unverified,
    NotApplicable,
    Failed,
    Quarantined,
}

/// Exact integrity generation and assessment time of one candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CandidateIntegrity {
    state: IntegrityState,
    generation: Option<ConnectionGeneration>,
    assessed_at: Timestamp,
}

impl CandidateIntegrity {
    pub(crate) const fn new(
        state: IntegrityState,
        generation: Option<ConnectionGeneration>,
        assessed_at: Timestamp,
    ) -> Self {
        Self {
            state,
            generation,
            assessed_at,
        }
    }

    pub(crate) const fn state(self) -> IntegrityState {
        self.state
    }

    pub(crate) const fn generation(self) -> Option<ConnectionGeneration> {
        self.generation
    }

    pub(crate) const fn assessed_at(self) -> Timestamp {
        self.assessed_at
    }
}

/// Source capabilities and classifications that remain attached to the selected observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CandidateCapabilities {
    asset_class: AssetClass,
    operations: MarketOperationSet,
    timing: ObservationTiming,
    depth: Option<MarketDepth>,
    quality: DataQuality,
    coverage: MarketCoverage,
}

impl CandidateCapabilities {
    pub(crate) fn try_new(
        asset_class: AssetClass,
        operations: MarketOperationSet,
        timing: ObservationTiming,
        depth: Option<MarketDepth>,
        quality: DataQuality,
        coverage: MarketCoverage,
    ) -> Result<Self, MarketSelectionError> {
        if asset_class == AssetClass::Index && depth.is_some() {
            return Err(MarketSelectionError::IndexBookDepth);
        }
        if matches!(
            coverage,
            MarketCoverage::Benchmark | MarketCoverage::Reference
        ) && depth.is_some()
        {
            return Err(MarketSelectionError::CoverageBookDepth);
        }
        Ok(Self {
            asset_class,
            operations,
            timing,
            depth,
            quality,
            coverage,
        })
    }

    pub(crate) const fn asset_class(self) -> AssetClass {
        self.asset_class
    }

    pub(crate) const fn operations(self) -> MarketOperationSet {
        self.operations
    }

    pub(crate) const fn timing(self) -> ObservationTiming {
        self.timing
    }

    pub(crate) const fn depth(self) -> Option<MarketDepth> {
        self.depth
    }

    pub(crate) const fn quality(self) -> DataQuality {
        self.quality
    }

    pub(crate) const fn coverage(self) -> MarketCoverage {
        self.coverage
    }
}

/// Runtime admission facts retained by one source without granting authority to the resolver.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CandidateAdmissionState {
    health: CandidateHealth,
    budget: ProviderBudgetSnapshot,
    rights: RightsAdmission,
    integrity: CandidateIntegrity,
    execution_eligibility: ExecutionEligibility,
}

impl CandidateAdmissionState {
    pub(crate) const fn new(
        health: CandidateHealth,
        budget: ProviderBudgetSnapshot,
        rights: RightsAdmission,
        integrity: CandidateIntegrity,
        execution_eligibility: ExecutionEligibility,
    ) -> Self {
        Self {
            health,
            budget,
            rights,
            integrity,
            execution_eligibility,
        }
    }

    pub(crate) const fn health(&self) -> CandidateHealth {
        self.health
    }

    pub(crate) const fn budget(&self) -> ProviderBudgetSnapshot {
        self.budget
    }

    pub(crate) const fn rights(&self) -> &RightsAdmission {
        &self.rights
    }

    pub(crate) const fn integrity(&self) -> CandidateIntegrity {
        self.integrity
    }

    pub(crate) const fn execution_eligibility(&self) -> ExecutionEligibility {
        self.execution_eligibility
    }
}

/// One immutable, source-preserving candidate evaluated by the pure resolver.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SourceCandidate {
    identity: CandidateIdentity,
    capabilities: CandidateCapabilities,
    timestamps: CandidateTimestamps,
    admission: CandidateAdmissionState,
}

impl SourceCandidate {
    pub(crate) fn try_new(
        identity: CandidateIdentity,
        capabilities: CandidateCapabilities,
        timestamps: CandidateTimestamps,
        admission: CandidateAdmissionState,
    ) -> Result<Self, MarketSelectionError> {
        if capabilities.coverage == MarketCoverage::SingleVenue && identity.venue_id.is_none() {
            return Err(MarketSelectionError::MissingVenue);
        }
        let integrity = admission.integrity;
        if capabilities.quality == DataQuality::DirectVerified
            && (integrity.state != IntegrityState::Verified || integrity.generation.is_none())
        {
            return Err(MarketSelectionError::UnverifiedDirectQuality);
        }
        if admission.execution_eligibility == ExecutionEligibility::Eligible
            && (capabilities.quality != DataQuality::DirectVerified
                || integrity.state != IntegrityState::Verified
                || integrity.generation.is_none())
        {
            return Err(MarketSelectionError::InvalidExecutionEligibility);
        }
        Ok(Self {
            identity,
            capabilities,
            timestamps,
            admission,
        })
    }

    pub(crate) const fn identity(&self) -> &CandidateIdentity {
        &self.identity
    }

    pub(crate) const fn capabilities(&self) -> CandidateCapabilities {
        self.capabilities
    }

    pub(crate) const fn timestamps(&self) -> CandidateTimestamps {
        self.timestamps
    }

    pub(crate) const fn admission(&self) -> &CandidateAdmissionState {
        &self.admission
    }
}
