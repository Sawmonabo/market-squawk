use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AuthorizationBasis, BookStateBinding, ConnectionGeneration, CoverageConsolidation,
    CoverageDelay, CoverageScope, CoverageStatus, DataQuality, EvidenceDigest,
    ExecutionEligibility, InstrumentId, LiveEventClass, LiveEvidenceBinding, LiveProvenance,
    MarketDepth, MetadataRevision, PayloadReference, ProviderChannel, ProviderProduct,
    RecordedLiveProvenanceInput, SourceCoverageRecord, SourceId, SourceIdentifier, Timestamp,
    VenueId,
};

fn instrument() -> Result<InstrumentId, Box<dyn Error>> {
    InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb").map_err(Into::into)
}

fn binding() -> Result<LiveEvidenceBinding, Box<dyn Error>> {
    Ok(LiveEvidenceBinding::new(
        SourceId::try_from("coinbase-direct")?,
        SourceIdentifier::try_from("session-7")?,
        MetadataRevision::new(SourceIdentifier::try_from("coinbase-advanced-trade-v3")?),
        AuthorizationBasis::new(SourceIdentifier::try_from("user-authorized-account")?),
        VenueId::try_from("COINBASE")?,
        instrument()?,
        ConnectionGeneration::new(7)?,
        ProviderProduct::new(SourceIdentifier::try_from("BTC-USD")?),
        ProviderChannel::new(SourceIdentifier::try_from("level2")?),
        LiveEventClass::BookDelta,
        SourceIdentifier::try_from("update-42")?,
        EvidenceDigest::new([1; 32]),
        EvidenceDigest::new([3; 32]),
        Some(BookStateBinding::new(
            MarketDepth::PriceLevel,
            SourceIdentifier::try_from("book-state-42")?,
            EvidenceDigest::new([3; 32]),
        )),
    )?)
}

#[test]
fn coverage_is_scoped_and_bound_to_effective_provider_metadata() -> Result<(), Box<dyn Error>> {
    let binding = binding()?;
    let coverage = SourceCoverageRecord::new(
        binding.clone(),
        CoverageScope::new(
            VenueId::try_from("COINBASE")?,
            ProviderProduct::new(SourceIdentifier::try_from("BTC-USD")?),
            LiveEventClass::BookDelta,
            Some(MarketDepth::PriceLevel),
            CoverageDelay::RealTime,
            CoverageConsolidation::SingleVenue,
            Timestamp::from_unix_nanos(900),
            Some(Timestamp::from_unix_nanos(2_000)),
            MetadataRevision::new(SourceIdentifier::try_from("coinbase-advanced-trade-v3")?),
        )?,
        CoverageStatus::Sufficient,
    )?;

    assert_eq!(coverage.binding(), &binding);
    assert_eq!(coverage.scope().event_class(), LiveEventClass::BookDelta);
    Ok(())
}

#[test]
fn recorded_direct_verified_is_archival_and_always_ineligible() -> Result<(), Box<dyn Error>> {
    let binding = binding()?;
    let recorded = LiveProvenance::recorded(RecordedLiveProvenanceInput::new(
        binding,
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_010),
        DataQuality::DirectVerified,
        CoverageStatus::Sufficient,
        PayloadReference::SourceReference(SourceIdentifier::try_from("capture:7:42")?),
        SourceIdentifier::try_from("assessment:7:42")?,
    ))?;

    assert_eq!(recorded.recorded_quality(), DataQuality::DirectVerified);
    assert_eq!(
        recorded.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(recorded.requires_requalification());

    let wire = serde_json::to_string(&recorded)?;
    let restored: LiveProvenance = serde_json::from_str(&wire)?;
    assert_eq!(restored.recorded_quality(), DataQuality::DirectVerified);
    assert_eq!(
        restored.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert!(restored.requires_requalification());
    Ok(())
}
