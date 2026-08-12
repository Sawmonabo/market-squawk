//! Canonical identity for one complete market-source selection receipt.

use market_squawk_domain::{
    AssetClass, DataQuality, DigestAlgorithm, EvidenceDigest, ExecutionEligibility, MarketDepth,
};
use sha2::{Digest as _, Sha256};

use super::receipt::{CandidateRejectionReason, EligibleCandidate, RejectedCandidate};
use super::{
    AdmittedDowngrade, BudgetAvailability, CandidateIdentity, DowngradeDimension, FreshnessBasis,
    HealthState, IntegrityState, MarketCoverage, MarketOperation, MarketOperationSet,
    MarketSelectionError, MarketSelectionRequest, ObservationTiming, RequestPriority, RightsState,
    SourceCandidate,
};

const RECEIPT_DIGEST_DOMAIN: &[u8] = b"market-squawk/market-selection-receipt/v1";
const ALL_OPERATIONS: [MarketOperation; 10] = [
    MarketOperation::ReferenceLookup,
    MarketOperation::SnapshotDisplay,
    MarketOperation::StreamDisplay,
    MarketOperation::HistoricalRead,
    MarketOperation::ResearchAnalysis,
    MarketOperation::PortfolioMark,
    MarketOperation::ModelInput,
    MarketOperation::Backtest,
    MarketOperation::PaperDecision,
    MarketOperation::AutomatedExecution,
];
const ALL_TIMINGS: [ObservationTiming; 5] = [
    ObservationTiming::RealTime,
    ObservationTiming::Delayed,
    ObservationTiming::EndOfDay,
    ObservationTiming::Historical,
    ObservationTiming::Stored,
];
const ALL_DEPTHS: [Option<MarketDepth>; 4] = [
    None,
    Some(MarketDepth::TopOfBook),
    Some(MarketDepth::PriceLevel),
    Some(MarketDepth::OrderLevel),
];
const ALL_QUALITIES: [DataQuality; 9] = [
    DataQuality::DirectVerified,
    DataQuality::DirectUnverified,
    DataQuality::OfficialDelayed,
    DataQuality::Aggregated,
    DataQuality::Indicative,
    DataQuality::Modeled,
    DataQuality::Estimated,
    DataQuality::Stale,
    DataQuality::Quarantined,
];
const ALL_COVERAGES: [MarketCoverage; 6] = [
    MarketCoverage::Consolidated,
    MarketCoverage::MultiVenuePartial,
    MarketCoverage::SingleVenue,
    MarketCoverage::Benchmark,
    MarketCoverage::Reference,
    MarketCoverage::UserOwned,
];

pub(super) fn selection_receipt_digest(
    policy_revision: u32,
    policy_digest: EvidenceDigest,
    policy_candidate_limit: usize,
    request: &MarketSelectionRequest,
    eligible: &[EligibleCandidate],
    rejected: &[RejectedCandidate],
    selected_at: market_squawk_domain::Timestamp,
) -> Result<EvidenceDigest, MarketSelectionError> {
    let mut digest = Sha256::new();
    hash_bytes(&mut digest, RECEIPT_DIGEST_DOMAIN)?;
    hash_u32(&mut digest, policy_revision)?;
    hash_evidence_digest(&mut digest, policy_digest)?;
    hash_count(&mut digest, policy_candidate_limit)?;
    hash_request(&mut digest, request)?;
    hash_timestamp(&mut digest, selected_at)?;

    hash_count(&mut digest, eligible.len())?;
    for candidate in eligible {
        hash_eligible(&mut digest, candidate)?;
    }
    hash_count(&mut digest, rejected.len())?;
    for candidate in rejected {
        hash_rejected(&mut digest, candidate)?;
    }

    match eligible.first() {
        Some(selected) => {
            hash_tag(&mut digest, 1)?;
            hash_identity(&mut digest, selected.candidate().identity())?;
        }
        None => hash_tag(&mut digest, 0)?,
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

fn hash_request(
    digest: &mut Sha256,
    request: &MarketSelectionRequest,
) -> Result<(), MarketSelectionError> {
    hash_tag(digest, asset_class_tag(request.asset_class()))?;
    hash_tag(digest, operation_tag(request.operation()))?;
    hash_tag(digest, timing_tag(request.timing()))?;
    hash_tag(digest, depth_tag(request.minimum_depth()))?;
    hash_tag(digest, quality_tag(request.minimum_quality()))?;
    hash_tag(digest, coverage_tag(request.coverage()))?;
    let freshness = request.freshness();
    hash_timestamp(digest, freshness.as_of())?;
    hash_tag(digest, freshness_basis_tag(freshness.basis()))?;
    hash_u64(digest, freshness.maximum_age_nanos())?;
    hash_tag(digest, priority_tag(request.priority()))?;

    let downgrade = request.downgrade();
    for timing in ALL_TIMINGS {
        hash_bool(digest, downgrade.allows_timing(timing))?;
    }
    for depth in ALL_DEPTHS {
        hash_bool(digest, downgrade.allows_depth(depth))?;
    }
    for quality in ALL_QUALITIES {
        hash_bool(digest, downgrade.allows_quality(quality))?;
    }
    for coverage in ALL_COVERAGES {
        hash_bool(digest, downgrade.allows_coverage(coverage))?;
    }
    hash_optional_u64(digest, downgrade.maximum_age_nanos())
}

fn hash_eligible(
    digest: &mut Sha256,
    eligible: &EligibleCandidate,
) -> Result<(), MarketSelectionError> {
    hash_candidate(digest, eligible.candidate())?;
    hash_u64(digest, eligible.freshness_age_nanos())?;
    hash_optional_downgrade(digest, eligible.downgrade())
}

fn hash_rejected(
    digest: &mut Sha256,
    rejected: &RejectedCandidate,
) -> Result<(), MarketSelectionError> {
    hash_candidate(digest, rejected.candidate())?;
    hash_count(digest, rejected.reasons().len())?;
    for reason in rejected.reasons() {
        hash_rejection_reason(digest, *reason)?;
    }
    Ok(())
}

fn hash_candidate(
    digest: &mut Sha256,
    candidate: &SourceCandidate,
) -> Result<(), MarketSelectionError> {
    hash_identity(digest, candidate.identity())?;
    let capabilities = candidate.capabilities();
    hash_tag(digest, asset_class_tag(capabilities.asset_class()))?;
    hash_operation_set(digest, capabilities.operations())?;
    hash_tag(digest, timing_tag(capabilities.timing()))?;
    hash_tag(digest, depth_tag(capabilities.depth()))?;
    hash_tag(digest, quality_tag(capabilities.quality()))?;
    hash_tag(digest, coverage_tag(capabilities.coverage()))?;

    let timestamps = candidate.timestamps();
    hash_timestamp(digest, timestamps.effective_at())?;
    hash_optional_timestamp(digest, timestamps.source_timestamp())?;
    hash_timestamp(digest, timestamps.received_at())?;
    hash_timestamp(digest, timestamps.available_at())?;
    hash_timestamp(digest, timestamps.ingested_at())?;

    let admission = candidate.admission();
    let health = admission.health();
    hash_tag(digest, health_tag(health.state()))?;
    hash_timestamp(digest, health.observed_at())?;
    let budget = admission.budget();
    hash_tag(digest, budget_tag(budget.availability()))?;
    hash_optional_u64(digest, budget.remaining_units())?;
    hash_optional_timestamp(digest, budget.reset_at())?;
    hash_timestamp(digest, budget.observed_at())?;
    let rights = admission.rights();
    hash_text(digest, rights.decision_id().as_str())?;
    hash_tag(digest, rights_tag(rights.state()))?;
    hash_operation_set(digest, rights.permitted_operations())?;
    hash_timestamp(digest, rights.decided_at())?;
    hash_optional_timestamp(digest, rights.effective_from())?;
    hash_optional_timestamp(digest, rights.effective_until())?;
    let integrity = admission.integrity();
    hash_tag(digest, integrity_tag(integrity.state()))?;
    hash_optional_u64(digest, integrity.generation().map(|value| value.get()))?;
    hash_timestamp(digest, integrity.assessed_at())?;
    hash_tag(
        digest,
        execution_eligibility_tag(admission.execution_eligibility()),
    )
}

fn hash_identity(
    digest: &mut Sha256,
    identity: &CandidateIdentity,
) -> Result<(), MarketSelectionError> {
    hash_text(digest, identity.provider().as_str())?;
    hash_text(digest, identity.product().as_source_identifier().as_str())?;
    hash_text(digest, identity.feed().as_source_identifier().as_str())?;
    hash_text(digest, identity.source_id().as_str())?;
    match identity.venue_id() {
        Some(venue) => {
            hash_tag(digest, 1)?;
            hash_text(digest, venue.as_str())?;
        }
        None => hash_tag(digest, 0)?,
    }
    hash_bytes(digest, identity.instrument_id().as_uuid().as_bytes())?;
    hash_text(digest, identity.observation_id().as_str())
}

fn hash_optional_downgrade(
    digest: &mut Sha256,
    downgrade: Option<&AdmittedDowngrade>,
) -> Result<(), MarketSelectionError> {
    let Some(downgrade) = downgrade else {
        return hash_tag(digest, 0);
    };
    hash_tag(digest, 1)?;
    hash_count(digest, downgrade.dimensions().len())?;
    for dimension in downgrade.dimensions() {
        hash_downgrade_dimension(digest, *dimension)?;
    }
    Ok(())
}

fn hash_downgrade_dimension(
    digest: &mut Sha256,
    dimension: DowngradeDimension,
) -> Result<(), MarketSelectionError> {
    match dimension {
        DowngradeDimension::Timing { required, selected } => {
            hash_tag(digest, 1)?;
            hash_tag(digest, timing_tag(required))?;
            hash_tag(digest, timing_tag(selected))
        }
        DowngradeDimension::Depth { minimum, selected } => {
            hash_tag(digest, 2)?;
            hash_tag(digest, depth_tag(Some(minimum)))?;
            hash_tag(digest, depth_tag(selected))
        }
        DowngradeDimension::Quality { minimum, selected } => {
            hash_tag(digest, 3)?;
            hash_tag(digest, quality_tag(minimum))?;
            hash_tag(digest, quality_tag(selected))
        }
        DowngradeDimension::Coverage { required, selected } => {
            hash_tag(digest, 4)?;
            hash_tag(digest, coverage_tag(required))?;
            hash_tag(digest, coverage_tag(selected))
        }
        DowngradeDimension::Freshness {
            maximum_age_nanos,
            selected_age_nanos,
        } => {
            hash_tag(digest, 5)?;
            hash_u64(digest, maximum_age_nanos)?;
            hash_u64(digest, selected_age_nanos)
        }
    }
}

fn hash_rejection_reason(
    digest: &mut Sha256,
    reason: CandidateRejectionReason,
) -> Result<(), MarketSelectionError> {
    match reason {
        CandidateRejectionReason::AssetMismatch { required, actual } => {
            hash_tag(digest, 1)?;
            hash_tag(digest, asset_class_tag(required))?;
            hash_tag(digest, asset_class_tag(actual))
        }
        CandidateRejectionReason::OperationUnsupported { operation } => {
            hash_tag(digest, 2)?;
            hash_tag(digest, operation_tag(operation))
        }
        CandidateRejectionReason::TimingUnavailable { required, actual } => {
            hash_tag(digest, 3)?;
            hash_tag(digest, timing_tag(required))?;
            hash_tag(digest, timing_tag(actual))
        }
        CandidateRejectionReason::DepthUnavailable { minimum, actual } => {
            hash_tag(digest, 4)?;
            hash_tag(digest, depth_tag(Some(minimum)))?;
            hash_tag(digest, depth_tag(actual))
        }
        CandidateRejectionReason::QualityBelowMinimum { minimum, actual } => {
            hash_tag(digest, 5)?;
            hash_tag(digest, quality_tag(minimum))?;
            hash_tag(digest, quality_tag(actual))
        }
        CandidateRejectionReason::CoverageUnavailable { required, actual } => {
            hash_tag(digest, 6)?;
            hash_tag(digest, coverage_tag(required))?;
            hash_tag(digest, coverage_tag(actual))
        }
        CandidateRejectionReason::FreshnessAnchorMissing => hash_tag(digest, 7),
        CandidateRejectionReason::FreshnessAnchorAfterSelection => hash_tag(digest, 8),
        CandidateRejectionReason::FreshnessExceeded {
            maximum_age_nanos,
            actual_age_nanos,
        } => {
            hash_tag(digest, 9)?;
            hash_u64(digest, maximum_age_nanos)?;
            hash_u64(digest, actual_age_nanos)
        }
        CandidateRejectionReason::EffectiveAfterSelection => hash_tag(digest, 10),
        CandidateRejectionReason::AvailableAfterSelection => hash_tag(digest, 11),
        CandidateRejectionReason::IngestedAfterSelection => hash_tag(digest, 12),
        CandidateRejectionReason::HealthObservedAfterSelection => hash_tag(digest, 13),
        CandidateRejectionReason::HealthUnavailable { state } => {
            hash_tag(digest, 14)?;
            hash_tag(digest, health_tag(state))
        }
        CandidateRejectionReason::ExecutionRequiresHealthySource => hash_tag(digest, 15),
        CandidateRejectionReason::BudgetObservedAfterSelection => hash_tag(digest, 16),
        CandidateRejectionReason::BudgetUnavailable { state } => {
            hash_tag(digest, 17)?;
            hash_tag(digest, budget_tag(state))
        }
        CandidateRejectionReason::BudgetPriorityDenied => hash_tag(digest, 18),
        CandidateRejectionReason::RightsUnavailable { state } => {
            hash_tag(digest, 19)?;
            hash_tag(digest, rights_tag(state))
        }
        CandidateRejectionReason::RightsObservedAfterSelection => hash_tag(digest, 20),
        CandidateRejectionReason::RightsOperationDenied => hash_tag(digest, 21),
        CandidateRejectionReason::RightsNotEffective => hash_tag(digest, 22),
        CandidateRejectionReason::RightsExpired => hash_tag(digest, 23),
        CandidateRejectionReason::IntegrityObservedAfterSelection => hash_tag(digest, 24),
        CandidateRejectionReason::IntegrityUnavailable { state } => {
            hash_tag(digest, 25)?;
            hash_tag(digest, integrity_tag(state))
        }
        CandidateRejectionReason::QuarantinedQuality => hash_tag(digest, 26),
        CandidateRejectionReason::ExecutionRequiresDirectVerified { actual } => {
            hash_tag(digest, 27)?;
            hash_tag(digest, quality_tag(actual))
        }
        CandidateRejectionReason::ExecutionIneligible { actual } => {
            hash_tag(digest, 28)?;
            hash_tag(digest, execution_eligibility_tag(actual))
        }
    }
}

fn hash_operation_set(
    digest: &mut Sha256,
    operations: MarketOperationSet,
) -> Result<(), MarketSelectionError> {
    for operation in ALL_OPERATIONS {
        hash_bool(digest, operations.contains(operation))?;
    }
    Ok(())
}

fn hash_evidence_digest(
    digest: &mut Sha256,
    evidence: EvidenceDigest,
) -> Result<(), MarketSelectionError> {
    hash_tag(
        digest,
        match evidence.algorithm() {
            DigestAlgorithm::Sha256 => 1,
            DigestAlgorithm::Blake3 => 2,
        },
    )?;
    hash_bytes(digest, &evidence.bytes())
}

fn hash_optional_timestamp(
    digest: &mut Sha256,
    value: Option<market_squawk_domain::Timestamp>,
) -> Result<(), MarketSelectionError> {
    match value {
        Some(value) => {
            hash_tag(digest, 1)?;
            hash_timestamp(digest, value)
        }
        None => hash_tag(digest, 0),
    }
}

fn hash_optional_u64(digest: &mut Sha256, value: Option<u64>) -> Result<(), MarketSelectionError> {
    match value {
        Some(value) => {
            hash_tag(digest, 1)?;
            hash_u64(digest, value)
        }
        None => hash_tag(digest, 0),
    }
}

fn hash_timestamp(
    digest: &mut Sha256,
    value: market_squawk_domain::Timestamp,
) -> Result<(), MarketSelectionError> {
    hash_bytes(digest, &value.unix_nanos().to_be_bytes())
}

fn hash_u64(digest: &mut Sha256, value: u64) -> Result<(), MarketSelectionError> {
    hash_bytes(digest, &value.to_be_bytes())
}

fn hash_u32(digest: &mut Sha256, value: u32) -> Result<(), MarketSelectionError> {
    hash_bytes(digest, &value.to_be_bytes())
}

fn hash_count(digest: &mut Sha256, value: usize) -> Result<(), MarketSelectionError> {
    let value = u64::try_from(value).map_err(|_error| MarketSelectionError::ReceiptEncoding)?;
    hash_u64(digest, value)
}

fn hash_bool(digest: &mut Sha256, value: bool) -> Result<(), MarketSelectionError> {
    hash_tag(digest, u8::from(value))
}

fn hash_tag(digest: &mut Sha256, value: u8) -> Result<(), MarketSelectionError> {
    hash_bytes(digest, &[value])
}

fn hash_text(digest: &mut Sha256, value: &str) -> Result<(), MarketSelectionError> {
    hash_bytes(digest, value.as_bytes())
}

fn hash_bytes(digest: &mut Sha256, value: &[u8]) -> Result<(), MarketSelectionError> {
    let length =
        u64::try_from(value.len()).map_err(|_error| MarketSelectionError::ReceiptEncoding)?;
    digest.update(length.to_be_bytes());
    digest.update(value);
    Ok(())
}

const fn asset_class_tag(value: AssetClass) -> u8 {
    match value {
        AssetClass::Equity => 1,
        AssetClass::FixedIncome => 2,
        AssetClass::Option => 3,
        AssetClass::Future => 4,
        AssetClass::ForeignExchange => 5,
        AssetClass::Crypto => 6,
        AssetClass::Commodity => 7,
        AssetClass::Fund => 8,
        AssetClass::Index => 9,
        AssetClass::Cash => 10,
    }
}

const fn operation_tag(value: MarketOperation) -> u8 {
    match value {
        MarketOperation::ReferenceLookup => 1,
        MarketOperation::SnapshotDisplay => 2,
        MarketOperation::StreamDisplay => 3,
        MarketOperation::HistoricalRead => 4,
        MarketOperation::ResearchAnalysis => 5,
        MarketOperation::PortfolioMark => 6,
        MarketOperation::ModelInput => 7,
        MarketOperation::Backtest => 8,
        MarketOperation::PaperDecision => 9,
        MarketOperation::AutomatedExecution => 10,
    }
}

const fn timing_tag(value: ObservationTiming) -> u8 {
    match value {
        ObservationTiming::RealTime => 1,
        ObservationTiming::Delayed => 2,
        ObservationTiming::EndOfDay => 3,
        ObservationTiming::Historical => 4,
        ObservationTiming::Stored => 5,
    }
}

const fn depth_tag(value: Option<MarketDepth>) -> u8 {
    match value {
        None => 0,
        Some(MarketDepth::TopOfBook) => 1,
        Some(MarketDepth::PriceLevel) => 2,
        Some(MarketDepth::OrderLevel) => 3,
    }
}

const fn quality_tag(value: DataQuality) -> u8 {
    match value {
        DataQuality::DirectVerified => 1,
        DataQuality::DirectUnverified => 2,
        DataQuality::OfficialDelayed => 3,
        DataQuality::Aggregated => 4,
        DataQuality::Indicative => 5,
        DataQuality::Modeled => 6,
        DataQuality::Estimated => 7,
        DataQuality::Stale => 8,
        DataQuality::Quarantined => 9,
    }
}

const fn coverage_tag(value: MarketCoverage) -> u8 {
    match value {
        MarketCoverage::Consolidated => 1,
        MarketCoverage::MultiVenuePartial => 2,
        MarketCoverage::SingleVenue => 3,
        MarketCoverage::Benchmark => 4,
        MarketCoverage::Reference => 5,
        MarketCoverage::UserOwned => 6,
    }
}

const fn freshness_basis_tag(value: FreshnessBasis) -> u8 {
    match value {
        FreshnessBasis::Source => 1,
        FreshnessBasis::Effective => 2,
        FreshnessBasis::Received => 3,
        FreshnessBasis::Available => 4,
        FreshnessBasis::Ingested => 5,
    }
}

const fn priority_tag(value: RequestPriority) -> u8 {
    match value {
        RequestPriority::Interactive => 1,
        RequestPriority::Foreground => 2,
        RequestPriority::Background => 3,
    }
}

const fn health_tag(value: HealthState) -> u8 {
    match value {
        HealthState::Healthy => 1,
        HealthState::Degraded => 2,
        HealthState::Unavailable => 3,
        HealthState::Quarantined => 4,
    }
}

const fn budget_tag(value: BudgetAvailability) -> u8 {
    match value {
        BudgetAvailability::NotRequired => 1,
        BudgetAvailability::Open => 2,
        BudgetAvailability::InteractiveOnly => 3,
        BudgetAvailability::Exhausted => 4,
        BudgetAvailability::Unknown => 5,
    }
}

const fn rights_tag(value: RightsState) -> u8 {
    match value {
        RightsState::Admitted => 1,
        RightsState::Unknown => 2,
        RightsState::Denied => 3,
    }
}

const fn integrity_tag(value: IntegrityState) -> u8 {
    match value {
        IntegrityState::Verified => 1,
        IntegrityState::Unverified => 2,
        IntegrityState::NotApplicable => 3,
        IntegrityState::Failed => 4,
        IntegrityState::Quarantined => 5,
    }
}

const fn execution_eligibility_tag(value: ExecutionEligibility) -> u8 {
    match value {
        ExecutionEligibility::Eligible => 1,
        ExecutionEligibility::Ineligible => 2,
    }
}
