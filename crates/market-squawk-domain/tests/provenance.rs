use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    DataQuality, InstrumentId, PayloadReference, Provenance, ProvenanceError, ResearchContext,
    ResearchTime, RevisionNumber, SchemaVersion, SourceId, SourceIdentifier, Timestamp, VenueId,
};

fn valid_provenance() -> Result<Provenance, Box<dyn Error>> {
    Provenance::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("sec-edgar")?,
        Some(InstrumentId::from_str(
            "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
        )?),
        Some(VenueId::try_from("XNYS")?),
        SourceIdentifier::try_from("0000320193-26-000001")?,
        None,
        Timestamp::from_unix_nanos(200),
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(300),
        DataQuality::OfficialDelayed,
        PayloadReference::SourceReference(SourceIdentifier::try_from(
            "edgar/data/320193/filing.json",
        )?),
    )
    .map_err(Into::into)
}

#[test]
fn provenance_retains_absent_source_timestamp_without_invention() -> Result<(), Box<dyn Error>> {
    let provenance = valid_provenance()?;

    assert_eq!(provenance.source_timestamp(), None);
    assert_eq!(provenance.received_at(), Timestamp::from_unix_nanos(200));
    assert_eq!(provenance.available_at(), Timestamp::from_unix_nanos(100));
    assert_eq!(provenance.ingested_at(), Timestamp::from_unix_nanos(300));
    Ok(())
}

#[test]
fn provenance_rejects_local_receive_after_ingestion() -> Result<(), Box<dyn Error>> {
    let result = Provenance::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("fred")?,
        None,
        None,
        SourceIdentifier::try_from("GDP")?,
        None,
        Timestamp::from_unix_nanos(400),
        Timestamp::from_unix_nanos(100),
        Timestamp::from_unix_nanos(300),
        DataQuality::OfficialDelayed,
        PayloadReference::SourceReference(SourceIdentifier::try_from("GDP:2026-Q1")?),
    );

    assert!(matches!(
        result,
        Err(ProvenanceError::ReceivedAfterIngested)
    ));
    Ok(())
}

#[test]
fn provenance_rejects_availability_after_ingestion() -> Result<(), Box<dyn Error>> {
    let result = Provenance::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("fred")?,
        None,
        None,
        SourceIdentifier::try_from("GDP")?,
        None,
        Timestamp::from_unix_nanos(200),
        Timestamp::from_unix_nanos(400),
        Timestamp::from_unix_nanos(300),
        DataQuality::OfficialDelayed,
        PayloadReference::SourceReference(SourceIdentifier::try_from("GDP:2026-Q1")?),
    );

    assert!(matches!(
        result,
        Err(ProvenanceError::AvailableAfterIngested)
    ));
    Ok(())
}

#[test]
fn deserialization_cannot_bypass_provenance_ordering() -> Result<(), Box<dyn Error>> {
    let mut value = serde_json::to_value(valid_provenance()?)?;
    value["received_at"] = serde_json::json!(400);

    assert!(serde_json::from_value::<Provenance>(value).is_err());
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
    let context = ResearchContext::new(valid_provenance()?, time)?;

    assert_eq!(
        context.time().effective_at(),
        Timestamp::from_unix_nanos(50)
    );
    assert_eq!(
        context.time().published_at(),
        Some(Timestamp::from_unix_nanos(90))
    );
    assert_eq!(context.time().revision().get(), 2);
    assert_eq!(
        context.time().superseded_at(),
        Some(Timestamp::from_unix_nanos(250))
    );
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

    assert!(matches!(
        ResearchContext::new(valid_provenance()?, time),
        Err(ProvenanceError::AvailableBeforePublished)
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
