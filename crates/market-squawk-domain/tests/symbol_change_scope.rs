use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    CorporateActionEvent, CorporateActionKind, CorporateActionObservation, DataQuality,
    InstrumentId, MarketEventError, PayloadReference, Provenance, ResearchContext, ResearchError,
    ResearchTime, RevisionNumber, SchemaVersion, SourceId, SourceIdentifier, Timestamp, VenueId,
    VenueSymbol,
};

fn provenance(venue: Option<&str>) -> Result<Provenance, Box<dyn Error>> {
    Provenance::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("reference-feed")?,
        Some(InstrumentId::from_str(
            "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
        )?),
        venue.map(VenueId::try_from).transpose()?,
        SourceIdentifier::try_from("symbol-change-1")?,
        Some(Timestamp::from_unix_nanos(100)),
        Timestamp::from_unix_nanos(110),
        Timestamp::from_unix_nanos(110),
        Timestamp::from_unix_nanos(120),
        DataQuality::OfficialDelayed,
        PayloadReference::SourceReference(SourceIdentifier::try_from("record:1")?),
    )
    .map_err(Into::into)
}

fn research_context(venue: Option<&str>) -> Result<ResearchContext, Box<dyn Error>> {
    ResearchContext::new(
        provenance(venue)?,
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
            provenance(Some("XNYS"))?,
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
            provenance(Some("XNYS"))?,
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
        CorporateActionEvent::new(
            provenance(None)?,
            Timestamp::from_unix_nanos(500),
            action("XNYS")?,
        ),
        Err(MarketEventError::MissingVenue)
    ));
    assert!(matches!(
        CorporateActionObservation::new(research_context(None)?, action("XNYS")?),
        Err(ResearchError::MissingVenue)
    ));
    Ok(())
}
