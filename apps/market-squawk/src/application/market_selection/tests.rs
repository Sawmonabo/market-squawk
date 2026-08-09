use market_squawk_domain::{
    AssetClass, ConnectionGeneration, DataQuality, ExecutionEligibility, InstrumentId, MarketDepth,
    ProviderChannel, ProviderProduct, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use uuid::Uuid;

use super::*;

fn source_identifier(value: &str) -> Result<SourceIdentifier, Box<dyn std::error::Error>> {
    Ok(SourceIdentifier::try_from(value)?)
}

fn candidate(
    provider: &str,
    observation: &str,
    timing: ObservationTiming,
    quality: DataQuality,
    coverage: MarketCoverage,
    execution_eligibility: ExecutionEligibility,
) -> Result<SourceCandidate, Box<dyn std::error::Error>> {
    let operations = MarketOperationSet::try_new(&[
        MarketOperation::SnapshotDisplay,
        MarketOperation::AutomatedExecution,
    ])?;
    let identity = CandidateIdentity::new(
        source_identifier(provider)?,
        ProviderProduct::new(source_identifier("product")?),
        ProviderChannel::new(source_identifier("feed")?),
        SourceId::try_from(provider)?,
        Some(VenueId::try_from("venue")?),
        InstrumentId::try_from(Uuid::from_u128(1))?,
        source_identifier(observation)?,
    );
    let capabilities = CandidateCapabilities::try_new(
        AssetClass::Equity,
        operations,
        timing,
        Some(MarketDepth::TopOfBook),
        quality,
        coverage,
    )?;
    let timestamps = CandidateTimestamps::try_new(
        Timestamp::from_unix_nanos(90),
        Some(Timestamp::from_unix_nanos(90)),
        Timestamp::from_unix_nanos(91),
        Timestamp::from_unix_nanos(92),
        Timestamp::from_unix_nanos(93),
    )?;
    let rights = RightsAdmission::try_admitted(
        source_identifier("rights")?,
        operations,
        Timestamp::from_unix_nanos(1),
        Timestamp::from_unix_nanos(1),
        None,
    )?;
    let integrity = CandidateIntegrity::new(
        if quality == DataQuality::DirectVerified {
            IntegrityState::Verified
        } else {
            IntegrityState::Unverified
        },
        Some(ConnectionGeneration::new(1)?),
        Timestamp::from_unix_nanos(94),
    );
    let admission = CandidateAdmissionState::new(
        CandidateHealth::new(HealthState::Healthy, Timestamp::from_unix_nanos(94)),
        ProviderBudgetSnapshot::try_new(
            BudgetAvailability::Open,
            Some(10),
            None,
            Timestamp::from_unix_nanos(94),
        )?,
        rights,
        integrity,
        execution_eligibility,
    );
    Ok(SourceCandidate::try_new(
        identity,
        capabilities,
        timestamps,
        admission,
    )?)
}

fn display_request(
    downgrade: DowngradePolicy,
) -> Result<MarketSelectionRequest, Box<dyn std::error::Error>> {
    Ok(MarketSelectionRequest::try_new(
        AssetClass::Equity,
        MarketOperation::SnapshotDisplay,
        ObservationTiming::RealTime,
        Some(MarketDepth::TopOfBook),
        DataQuality::DirectVerified,
        MarketCoverage::Consolidated,
        FreshnessRequirement::try_new(Timestamp::from_unix_nanos(100), FreshnessBasis::Source, 20)?,
        RequestPriority::Interactive,
        downgrade,
    )?)
}

#[test]
fn selection_is_deterministic_downgrade_is_explicit_and_execution_fails_closed()
-> Result<(), Box<dyn std::error::Error>> {
    let policy = MarketSelectionPolicy::v1(8)?;
    let alpha = candidate(
        "alpha",
        "observation-alpha",
        ObservationTiming::RealTime,
        DataQuality::DirectVerified,
        MarketCoverage::Consolidated,
        ExecutionEligibility::Eligible,
    )?;
    let beta = candidate(
        "beta",
        "observation-beta",
        ObservationTiming::RealTime,
        DataQuality::DirectVerified,
        MarketCoverage::Consolidated,
        ExecutionEligibility::Eligible,
    )?;
    let selected = select_market_source(
        policy,
        display_request(DowngradePolicy::deny())?,
        vec![beta, alpha],
    )?;
    assert_eq!(
        selected
            .selected()
            .ok_or("missing deterministic selection")?
            .candidate()
            .identity()
            .provider()
            .as_str(),
        "alpha"
    );
    assert_eq!(selected.eligible().len(), 2);
    assert!(selected.rejected().is_empty());

    let downgrade_policy = DowngradePolicy::try_new(
        &[ObservationTiming::Delayed],
        &[],
        &[DataQuality::DirectUnverified],
        &[MarketCoverage::SingleVenue],
        None,
    )?;
    let fallback = candidate(
        "fallback",
        "observation-fallback",
        ObservationTiming::Delayed,
        DataQuality::DirectUnverified,
        MarketCoverage::SingleVenue,
        ExecutionEligibility::Ineligible,
    )?;
    let downgraded =
        select_market_source(policy, display_request(downgrade_policy)?, vec![fallback])?;
    let selected_fallback = downgraded.selected().ok_or("missing admitted downgrade")?;
    assert_eq!(selected_fallback.class(), SelectionClass::AdmittedDowngrade);
    assert_eq!(
        selected_fallback
            .downgrade()
            .ok_or("missing downgrade receipt")?
            .dimensions()
            .len(),
        3
    );
    assert_eq!(
        selected_fallback.candidate().capabilities().quality(),
        DataQuality::DirectUnverified
    );
    assert_eq!(
        selected_fallback.candidate().capabilities().coverage(),
        MarketCoverage::SingleVenue
    );
    assert_eq!(
        selected_fallback
            .candidate()
            .admission()
            .execution_eligibility(),
        ExecutionEligibility::Ineligible
    );

    let unsafe_execution_downgrade =
        DowngradePolicy::try_new(&[], &[], &[DataQuality::DirectUnverified], &[], None)?;
    let unsafe_execution = MarketSelectionRequest::try_new(
        AssetClass::Equity,
        MarketOperation::AutomatedExecution,
        ObservationTiming::RealTime,
        Some(MarketDepth::TopOfBook),
        DataQuality::DirectVerified,
        MarketCoverage::Consolidated,
        FreshnessRequirement::try_new(Timestamp::from_unix_nanos(100), FreshnessBasis::Source, 20)?,
        RequestPriority::Foreground,
        unsafe_execution_downgrade,
    );
    assert_eq!(
        unsafe_execution,
        Err(MarketSelectionError::UnsafeExecutionRequest)
    );

    let execution_request = MarketSelectionRequest::try_new(
        AssetClass::Equity,
        MarketOperation::AutomatedExecution,
        ObservationTiming::RealTime,
        Some(MarketDepth::TopOfBook),
        DataQuality::DirectVerified,
        MarketCoverage::Consolidated,
        FreshnessRequirement::try_new(Timestamp::from_unix_nanos(100), FreshnessBasis::Source, 20)?,
        RequestPriority::Foreground,
        DowngradePolicy::deny(),
    )?;
    let execution_denied = candidate(
        "execution-denied",
        "observation-execution-denied",
        ObservationTiming::RealTime,
        DataQuality::DirectVerified,
        MarketCoverage::Consolidated,
        ExecutionEligibility::Ineligible,
    )?;
    let denied = select_market_source(policy, execution_request, vec![execution_denied])?;
    assert!(denied.selected().is_none());
    assert!(denied.rejected()[0].reasons().contains(
        &CandidateRejectionReason::ExecutionIneligible {
            actual: ExecutionEligibility::Ineligible,
        }
    ));
    Ok(())
}
