use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AvailabilityEvidence, CorporateActionEvent, CorporateActionKind, CorporateActionObservation,
    CoverageStatus, DataQuality, DecodedLiveProvenanceInput, InstrumentId, LiveEventClass,
    LiveProvenance, MarketEventError, PayloadReference, ResearchContext, ResearchError,
    ResearchProvenance, ResearchTime, RevisionNumber, SourceId, SourceIdentifier, Timestamp,
    VenueId, VenueSymbol,
};

fn live_provenance(venue: &'static str) -> Result<LiveProvenance, Box<dyn Error>> {
    let binding = support::live::binding(&support::live::BindingSpec {
        venue,
        event_class: LiveEventClass::CorporateAction,
        ..support::live::BindingSpec::default()
    })?;
    LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding,
        Some(Timestamp::from_unix_nanos(100)),
        Timestamp::from_unix_nanos(110),
        Timestamp::from_unix_nanos(120),
        DataQuality::DirectUnverified,
        CoverageStatus::Sufficient,
        PayloadReference::SourceReference(SourceIdentifier::try_from("record:1")?),
    ))
    .map_err(Into::into)
}

fn research_provenance(venue: Option<&str>) -> Result<ResearchProvenance, Box<dyn Error>> {
    ResearchProvenance::new(
        SourceId::try_from("reference-feed")?,
        Some(InstrumentId::from_str(
            "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
        )?),
        venue.map(VenueId::try_from).transpose()?,
        SourceIdentifier::try_from("symbol-change-1")?,
        Some(Timestamp::from_unix_nanos(100)),
        Timestamp::from_unix_nanos(110),
        Timestamp::from_unix_nanos(120),
        DataQuality::OfficialDelayed,
        PayloadReference::SourceReference(SourceIdentifier::try_from("record:1")?),
        AvailabilityEvidence::evidenced(
            Timestamp::from_unix_nanos(110),
            SourceIdentifier::try_from("source-publication-record")?,
        ),
    )
    .map_err(Into::into)
}

fn research_context(venue: Option<&str>) -> Result<ResearchContext, Box<dyn Error>> {
    ResearchContext::new(
        research_provenance(venue)?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(90),
            Some(Timestamp::from_unix_nanos(100)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )
    .map_err(Into::into)
}

fn action(venue: &str) -> Result<CorporateActionKind, Box<dyn Error>> {
    Ok(CorporateActionKind::SymbolChange {
        venue_id: VenueId::try_from(venue)?,
        previous: VenueSymbol::try_from("OLD")?,
        current: VenueSymbol::try_from("NEW")?,
    })
}

#[test]
fn symbol_change_requires_matching_venue_in_live_and_research() -> Result<(), Box<dyn Error>> {
    assert!(
        CorporateActionEvent::new(
            live_provenance("XNYS")?,
            Timestamp::from_unix_nanos(500),
            action("XNYS")?,
        )
        .is_ok()
    );
    assert!(
        CorporateActionObservation::new(research_context(Some("XNYS"))?, action("XNYS")?,).is_ok()
    );

    assert!(matches!(
        CorporateActionEvent::new(
            live_provenance("XNYS")?,
            Timestamp::from_unix_nanos(500),
            action("XNAS")?,
        ),
        Err(MarketEventError::CorporateActionVenueMismatch)
    ));
    assert!(matches!(
        CorporateActionObservation::new(research_context(Some("XNYS"))?, action("XNAS")?),
        Err(ResearchError::CorporateActionVenueMismatch)
    ));

    assert!(matches!(
        CorporateActionObservation::new(research_context(None)?, action("XNYS")?),
        Err(ResearchError::MissingVenue)
    ));
    Ok(())
}
mod support;
