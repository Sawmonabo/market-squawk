use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DataQuality, InstrumentId, PayloadReference,
    ProvenanceError, ResearchContext, ResearchProvenance, ResearchProvenanceInput,
    ResearchTemporalCoordinate, ResearchTemporalPrecision, ResearchTime, RevisionNumber,
    SchemaVersion, SourceId, SourceIdentifier, Timestamp, VenueId,
};

fn valid_provenance(
    availability: AvailabilityEvidence,
) -> Result<ResearchProvenance, Box<dyn Error>> {
    ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: SourceId::try_from("sec-edgar")?,
        instrument_id: Some(InstrumentId::from_str(
            "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
        )?),
        venue_id: Some(VenueId::try_from("XNYS")?),
        source_identifier: SourceIdentifier::try_from("0000320193-26-000001")?,
        source_timestamp: None,
        received_at: Timestamp::from_unix_nanos(200),
        ingested_at: Timestamp::from_unix_nanos(300),
        quality: DataQuality::OfficialDelayed,
        payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
            "edgar/data/320193/filing.json",
        )?),
        availability,
    })
    .map_err(Into::into)
}

#[test]
fn research_provenance_retains_unknown_source_and_availability_times() -> Result<(), Box<dyn Error>>
{
    let provenance = valid_provenance(AvailabilityEvidence::unknown())?;

    assert_eq!(provenance.schema_version(), SchemaVersion::CURRENT);
    assert_eq!(provenance.source_timestamp(), None);
    assert_eq!(provenance.availability().reported_at(), None);
    assert_eq!(provenance.received_at(), Timestamp::from_unix_nanos(200));
    assert_eq!(provenance.ingested_at(), Timestamp::from_unix_nanos(300));
    Ok(())
}

#[test]
fn research_provenance_rejects_receive_after_ingestion() -> Result<(), Box<dyn Error>> {
    let result = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: SourceId::try_from("fred")?,
        instrument_id: None,
        venue_id: None,
        source_identifier: SourceIdentifier::try_from("GDP")?,
        source_timestamp: None,
        received_at: Timestamp::from_unix_nanos(400),
        ingested_at: Timestamp::from_unix_nanos(300),
        quality: DataQuality::OfficialDelayed,
        payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
            "GDP:2026-Q1",
        )?),
        availability: AvailabilityEvidence::unknown(),
    });

    assert!(matches!(
        result,
        Err(ProvenanceError::ReceivedAfterIngested)
    ));
    Ok(())
}

#[test]
fn research_provenance_rejects_reported_availability_after_ingestion() -> Result<(), Box<dyn Error>>
{
    let result = ResearchProvenance::try_new(ResearchProvenanceInput {
        source_id: SourceId::try_from("sec-edgar")?,
        instrument_id: Some(InstrumentId::from_str(
            "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
        )?),
        venue_id: Some(VenueId::try_from("XNYS")?),
        source_identifier: SourceIdentifier::try_from("0000320193-26-000001")?,
        source_timestamp: None,
        received_at: Timestamp::from_unix_nanos(200),
        ingested_at: Timestamp::from_unix_nanos(300),
        quality: DataQuality::OfficialDelayed,
        payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
            "filing.json",
        )?),
        availability: AvailabilityEvidence::local_first_observed(Timestamp::from_unix_nanos(400)),
    });

    assert!(matches!(
        result,
        Err(ProvenanceError::AvailabilityAfterIngested)
    ));
    Ok(())
}

#[test]
fn deserialization_cannot_bypass_research_ordering() -> Result<(), Box<dyn Error>> {
    let mut value = serde_json::to_value(valid_provenance(AvailabilityEvidence::unknown())?)?;
    value["received_at"] = serde_json::json!(400);

    assert!(serde_json::from_value::<ResearchProvenance>(value).is_err());
    Ok(())
}

#[test]
fn research_context_preserves_point_in_time_fields() -> Result<(), Box<dyn Error>> {
    let time = ResearchTime::new(
        Timestamp::from_unix_nanos(50),
        Some(Timestamp::from_unix_nanos(90)),
        RevisionNumber::new(2)?,
        Some(Timestamp::from_unix_nanos(250)),
    )?;
    let context = ResearchContext::new(
        valid_provenance(AvailabilityEvidence::evidenced(
            Timestamp::from_unix_nanos(100),
            SourceIdentifier::try_from("sec-acceptance")?,
        ))?,
        time,
    )?;

    assert_eq!(
        context.time().effective(),
        ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(50))
    );
    assert_eq!(
        context.time().published(),
        Some(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(90)
        ))
    );
    assert_eq!(context.time().revision().get(), 2);
    assert_eq!(
        context.time().superseded(),
        Some(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(250)
        ))
    );
    Ok(())
}

#[test]
fn calendar_date_roundtrip_never_manufactures_an_intraday_instant() -> Result<(), Box<dyn Error>> {
    assert_eq!(CalendarDate::new(1970, 1, 1)?.days_since_unix_epoch(), 0);
    let effective = ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2026, 7, 1)?);
    let published = ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2026, 7, 15)?);
    let time = ResearchTime::try_new_with_coordinates(
        effective,
        Some(published),
        RevisionNumber::new(1)?,
        None,
    )?;

    let encoded = serde_json::to_vec(&time)?;
    let restored: ResearchTime = serde_json::from_slice(&encoded)?;
    assert_eq!(restored.effective(), effective);
    assert_eq!(restored.published(), Some(published));
    assert_eq!(
        effective.precision(),
        ResearchTemporalPrecision::CalendarDate
    );
    assert_eq!(effective.exact_timestamp(), None);
    assert_eq!(
        effective.calendar_date_value(),
        Some(CalendarDate::new(2026, 7, 1)?)
    );
    let mut future = serde_json::to_value(time)?;
    future["effective"]["schema_version"] = serde_json::json!(2);
    assert!(serde_json::from_value::<ResearchTime>(future).is_err());
    Ok(())
}

#[test]
fn research_context_rejects_availability_before_publication() -> Result<(), Box<dyn Error>> {
    let time = ResearchTime::new(
        Timestamp::from_unix_nanos(50),
        Some(Timestamp::from_unix_nanos(110)),
        RevisionNumber::new(1)?,
        None,
    )?;
    let provenance = valid_provenance(AvailabilityEvidence::evidenced(
        Timestamp::from_unix_nanos(100),
        SourceIdentifier::try_from("release-calendar")?,
    ))?;

    assert!(matches!(
        ResearchContext::new(provenance, time),
        Err(ProvenanceError::AvailabilityBeforePublished)
    ));
    Ok(())
}

#[test]
fn revision_zero_is_invalid() {
    assert!(matches!(
        RevisionNumber::new(0),
        Err(ProvenanceError::ZeroRevision)
    ));
}
