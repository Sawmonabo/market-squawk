use std::error::Error;
use std::num::NonZeroU16;
use std::str::FromStr;

use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, DataQuality, InstrumentId, PayloadReference,
    ProvenanceError, ResearchContext, ResearchPeriod, ResearchProvenance, ResearchProvenanceInput,
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
        &ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(50))
    );
    assert_eq!(
        context
            .time()
            .published()
            .and_then(ResearchTemporalCoordinate::exact_timestamp),
        Some(Timestamp::from_unix_nanos(90))
    );
    assert_eq!(context.time().revision().get(), 2);
    assert_eq!(
        context
            .time()
            .superseded()
            .and_then(ResearchTemporalCoordinate::exact_timestamp),
        Some(Timestamp::from_unix_nanos(250))
    );
    Ok(())
}

#[test]
fn calendar_date_roundtrip_never_manufactures_an_intraday_instant() -> Result<(), Box<dyn Error>> {
    assert_eq!(CalendarDate::new(1970, 1, 1)?.days_since_unix_epoch(), 0);
    let effective = ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2026, 7, 1)?);
    let published = ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2026, 7, 15)?);
    let time = ResearchTime::try_new_with_coordinates(
        effective.clone(),
        Some(published.clone()),
        RevisionNumber::new(1)?,
        None,
    )?;

    let encoded = serde_json::to_vec(&time)?;
    let restored: ResearchTime = serde_json::from_slice(&encoded)?;
    assert_eq!(restored.effective(), &effective);
    assert_eq!(restored.published(), Some(&published));
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
    future["effective"]["schema_version"] = serde_json::json!(3);
    assert!(serde_json::from_value::<ResearchTime>(future).is_err());
    Ok(())
}

#[test]
fn legacy_exact_research_time_decodes_without_precision_loss() -> Result<(), Box<dyn Error>> {
    let legacy = serde_json::json!({
        "effective_at": 50,
        "published_at": 75,
        "revision": 2,
        "superseded_at": 100
    });

    let restored: ResearchTime = serde_json::from_value(legacy)?;

    assert_eq!(
        restored.effective().exact_timestamp(),
        Some(Timestamp::from_unix_nanos(50))
    );
    assert_eq!(
        restored
            .published()
            .and_then(ResearchTemporalCoordinate::exact_timestamp),
        Some(Timestamp::from_unix_nanos(75))
    );
    assert_eq!(restored.revision().get(), 2);
    assert_eq!(
        restored
            .superseded()
            .and_then(ResearchTemporalCoordinate::exact_timestamp),
        Some(Timestamp::from_unix_nanos(100))
    );
    Ok(())
}

#[test]
fn research_time_rejects_incomparable_supersession_precision() -> Result<(), Box<dyn Error>> {
    let result = ResearchTime::try_new_with_coordinates(
        ResearchTemporalCoordinate::exact(Timestamp::from_unix_nanos(0)),
        Some(ResearchTemporalCoordinate::calendar_date(
            CalendarDate::new(2026, 7, 2)?,
        )),
        RevisionNumber::new(1)?,
        Some(ResearchTemporalCoordinate::exact(
            Timestamp::from_unix_nanos(100),
        )),
    );

    assert!(matches!(
        result,
        Err(ProvenanceError::SupersededNotAfterPublished)
    ));
    Ok(())
}

#[test]
fn research_time_keeps_effective_and_revision_axes_independent() -> Result<(), Box<dyn Error>> {
    let effective = ResearchTemporalCoordinate::source_period(ResearchPeriod::try_new(
        SourceIdentifier::try_from("bls-monthly")?,
        2026,
        NonZeroU16::try_from(12_u16)?,
        SourceIdentifier::try_from("M12")?,
    )?);
    let candidates = [
        ResearchTemporalCoordinate::source_period(ResearchPeriod::try_new(
            SourceIdentifier::try_from("bls-monthly")?,
            2025,
            NonZeroU16::try_from(1_u16)?,
            SourceIdentifier::try_from("M01")?,
        )?),
        ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2027, 1, 1)?),
    ];

    for superseded in candidates {
        let time = ResearchTime::try_new_with_coordinates(
            effective.clone(),
            None,
            RevisionNumber::new(1)?,
            Some(superseded.clone()),
        )?;
        assert_eq!(time.superseded(), Some(&superseded));
    }
    Ok(())
}

#[test]
fn provider_period_roundtrip_never_manufactures_a_calendar_day() -> Result<(), Box<dyn Error>> {
    let period = ResearchPeriod::try_new(
        SourceIdentifier::try_from("bls-monthly")?,
        2026,
        NonZeroU16::try_from(13_u16)?,
        SourceIdentifier::try_from("M13")?,
    )?;
    let effective = ResearchTemporalCoordinate::source_period(period.clone());
    let time = ResearchTime::try_new_with_coordinates(
        effective.clone(),
        None,
        RevisionNumber::new(1)?,
        None,
    )?;

    let restored: ResearchTime = serde_json::from_slice(&serde_json::to_vec(&time)?)?;
    assert_eq!(restored.effective(), &effective);
    assert_eq!(restored.effective().exact_timestamp(), None);
    assert_eq!(restored.effective().calendar_date_value(), None);
    assert_eq!(restored.effective().source_period_value(), Some(&period));
    assert_eq!(period.scheme().as_str(), "bls-monthly");
    assert_eq!(period.year(), 2026);
    assert_eq!(period.ordinal().get(), 13);
    assert_eq!(period.code().as_str(), "M13");
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
