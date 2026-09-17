use crate::support;

use std::error::Error;

use market_squawk_domain::{
    AggressorSide, CoverageStatus, DataQuality, DecodedLiveProvenanceInput, LiveEventClass,
    LiveProvenance, MarketEvent, PayloadReference, PriceTicks, ProvenanceError, QuantityLots,
    RecordedLiveProvenanceInput, SourceIdentifier, Timestamp, TradeEvent,
};
use support::live::{BindingSpec, binding};

fn payload_reference() -> Result<PayloadReference, Box<dyn Error>> {
    Ok(PayloadReference::SourceReference(
        SourceIdentifier::try_from("capture:7:42")?,
    ))
}

fn decoded_at(
    received_at: Timestamp,
    available_at: Timestamp,
    ingested_at: Timestamp,
) -> Result<LiveProvenance, ProvenanceError> {
    LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding(&BindingSpec {
            event_class: LiveEventClass::Trade,
            ..BindingSpec::default()
        })
        .map_err(|_| ProvenanceError::PayloadDigestMismatch)?,
        Some(Timestamp::from_unix_nanos(995)),
        received_at,
        available_at,
        ingested_at,
        DataQuality::DirectUnverified,
        CoverageStatus::Sufficient,
        payload_reference().map_err(|_| ProvenanceError::PayloadDigestMismatch)?,
    ))
}

#[test]
fn live_availability_is_explicit_and_round_trips() -> Result<(), Box<dyn Error>> {
    let provenance = decoded_at(
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        Timestamp::from_unix_nanos(1_002),
    )?;
    assert_eq!(provenance.available_at(), Timestamp::from_unix_nanos(1_001));

    let restored: LiveProvenance = serde_json::from_str(&serde_json::to_string(&provenance)?)?;
    assert_eq!(restored, provenance);
    Ok(())
}

#[test]
fn live_availability_rejects_every_invalid_local_time_permutation() {
    assert_eq!(
        decoded_at(
            Timestamp::from_unix_nanos(1_001),
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_002),
        ),
        Err(ProvenanceError::AvailabilityBeforeReceived)
    );
    assert_eq!(
        decoded_at(
            Timestamp::from_unix_nanos(1_000),
            Timestamp::from_unix_nanos(1_002),
            Timestamp::from_unix_nanos(1_001),
        ),
        Err(ProvenanceError::AvailabilityAfterIngested)
    );
}

#[test]
fn recorded_and_composite_live_events_preserve_availability() -> Result<(), Box<dyn Error>> {
    let binding = binding(&BindingSpec {
        event_class: LiveEventClass::Trade,
        ..BindingSpec::default()
    })?;
    let provenance = LiveProvenance::recorded(RecordedLiveProvenanceInput::new(
        binding,
        Some(Timestamp::from_unix_nanos(995)),
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        Timestamp::from_unix_nanos(1_002),
        DataQuality::DirectVerified,
        CoverageStatus::Sufficient,
        payload_reference()?,
        SourceIdentifier::try_from("assessment:7:42")?,
    ))?;
    let event = MarketEvent::Trade(TradeEvent::new(
        provenance,
        PriceTicks::new(100),
        QuantityLots::new(2)?,
        AggressorSide::Buy,
        None,
    )?);
    let restored: MarketEvent = serde_json::from_str(&serde_json::to_string(&event)?)?;
    let MarketEvent::Trade(trade) = restored else {
        return Err("expected trade event".into());
    };
    assert_eq!(
        trade.provenance().available_at(),
        Timestamp::from_unix_nanos(1_001)
    );
    Ok(())
}

#[test]
fn live_availability_cannot_be_omitted_or_aliased_on_the_wire() -> Result<(), Box<dyn Error>> {
    let provenance = decoded_at(
        Timestamp::from_unix_nanos(1_000),
        Timestamp::from_unix_nanos(1_001),
        Timestamp::from_unix_nanos(1_002),
    )?;
    let value = serde_json::to_value(provenance)?;
    let mut missing = value.clone();
    missing
        .as_object_mut()
        .ok_or("expected object")?
        .remove("available_at");
    assert!(serde_json::from_value::<LiveProvenance>(missing).is_err());

    let mut before_receive = value.clone();
    before_receive["available_at"] = serde_json::json!(999);
    assert!(serde_json::from_value::<LiveProvenance>(before_receive).is_err());

    let mut after_ingest = value;
    after_ingest["available_at"] = serde_json::json!(1_003);
    assert!(serde_json::from_value::<LiveProvenance>(after_ingest).is_err());
    Ok(())
}
