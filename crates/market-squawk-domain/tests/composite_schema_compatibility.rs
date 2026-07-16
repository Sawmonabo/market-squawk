use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AggressorSide, AvailabilityEvidence, DataQuality, InstrumentId, LiveProvenance,
    MacroObservation, MarketEvent, PayloadReference, PriceTicks, QuantityLots, ResearchContext,
    ResearchObservation, ResearchProvenance, ResearchTime, RevisionNumber, SourceCoverageEvidence,
    SourceId, SourceIdentifier, Timestamp, TradeEvent, VenueId,
};
use rust_decimal::Decimal;

fn instrument() -> Result<InstrumentId, Box<dyn Error>> {
    InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb").map_err(Into::into)
}

fn payload_reference(name: &str) -> Result<PayloadReference, Box<dyn Error>> {
    Ok(PayloadReference::SourceReference(
        SourceIdentifier::try_from(name)?,
    ))
}

fn market_event() -> Result<MarketEvent, Box<dyn Error>> {
    let provenance = LiveProvenance::decoded(
        SourceId::try_from("direct-feed")?,
        Some(instrument()?),
        Some(VenueId::try_from("XNYS")?),
        SourceIdentifier::try_from("trade-42")?,
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        DataQuality::DirectUnverified,
        market_squawk_domain::ConnectionGeneration::new(7)?,
        SourceCoverageEvidence::Explicit,
        payload_reference("capture:7:42")?,
    )?;
    Ok(MarketEvent::Trade(TradeEvent::new(
        provenance,
        PriceTicks::new(100),
        QuantityLots::new(2)?,
        AggressorSide::Buy,
    )?))
}

fn research_observation() -> Result<ResearchObservation, Box<dyn Error>> {
    let provenance = ResearchProvenance::new(
        SourceId::try_from("fred")?,
        None,
        None,
        SourceIdentifier::try_from("GDP:2026-Q1")?,
        None,
        Timestamp::from_unix_nanos(200),
        Timestamp::from_unix_nanos(300),
        DataQuality::OfficialDelayed,
        payload_reference("fred:GDP:2026-Q1")?,
        AvailabilityEvidence::evidenced(
            Timestamp::from_unix_nanos(100),
            SourceIdentifier::try_from("fred-release-calendar")?,
        ),
    )?;
    let context = ResearchContext::new(
        provenance,
        ResearchTime::new(
            Timestamp::from_unix_nanos(50),
            Some(Timestamp::from_unix_nanos(90)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )?;
    Ok(ResearchObservation::Macro(MacroObservation::new(
        context,
        SourceIdentifier::try_from("GDP")?,
        Decimal::new(25_000, 0),
        SourceIdentifier::try_from("billions_usd")?,
    )))
}

#[test]
fn future_market_event_schema_is_rejected_by_composite_deserialization()
-> Result<(), Box<dyn Error>> {
    let mut value = serde_json::to_value(market_event()?)?;
    value["payload"]["provenance"]["schema_version"] = serde_json::json!(2);

    let error = serde_json::from_value::<MarketEvent>(value).err();
    assert!(error.is_some());
    assert!(
        error
            .map(|error| error.to_string().contains("newer than supported"))
            .unwrap_or(false)
    );
    Ok(())
}

#[test]
fn future_research_schema_is_rejected_by_composite_deserialization() -> Result<(), Box<dyn Error>> {
    let mut value = serde_json::to_value(research_observation()?)?;
    value["payload"]["context"]["provenance"]["schema_version"] = serde_json::json!(2);

    let error = serde_json::from_value::<ResearchObservation>(value).err();
    assert!(error.is_some());
    assert!(
        error
            .map(|error| error.to_string().contains("newer than supported"))
            .unwrap_or(false)
    );
    Ok(())
}

#[test]
fn unknown_v1_market_and_research_payload_fields_are_rejected() -> Result<(), Box<dyn Error>> {
    let mut market = serde_json::to_value(market_event()?)?;
    market["payload"]["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<MarketEvent>(market).is_err());

    let mut research = serde_json::to_value(research_observation()?)?;
    research["payload"]["unexpected"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ResearchObservation>(research).is_err());
    Ok(())
}
