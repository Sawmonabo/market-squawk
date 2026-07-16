use market_squawk_domain::{
    AssetClass, Currency, Denomination, EffectiveInterval, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentError, InstrumentId, LotSize, PayloadHash,
    PayloadHashAlgorithm, PayloadReference, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderInstrumentId, SourceId, SourceIdentifier, TickSize, Timestamp, TradingStatus,
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn instrument() -> Result<InstrumentId, Box<dyn std::error::Error>> {
    Ok(InstrumentId::try_from(Uuid::parse_str(
        "936da01f-9abd-4d9d-80c7-02af85c822a8",
    )?)?)
}

fn record(
    revision: &str,
    observed_at: i64,
    digest: u8,
    validity: EffectiveInterval,
) -> Result<ProviderIdentityRecord, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: instrument()?,
        source_id: SourceId::try_from("vendor-alpha")?,
        provider_instrument_id: ProviderInstrumentId::try_from("12345")?,
        source_reference: PayloadReference::ContentHash(PayloadHash::new(
            PayloadHashAlgorithm::Sha256,
            [digest; 32],
        )),
        source_timestamp: Some(Timestamp::from_unix_nanos(observed_at - 1)),
        observed_at: Timestamp::from_unix_nanos(observed_at),
        metadata_revision: SourceIdentifier::try_from(revision)?,
        validity,
    }))
}

fn definition(
    provider_identities: Vec<ProviderIdentityRecord>,
) -> Result<InstrumentDefinition, InstrumentError> {
    InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument().map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        asset_class: AssetClass::Equity,
        primary_denomination: Denomination::Currency(
            Currency::try_from("USD").map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        ),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))
            .map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        lot_size: LotSize::try_from_decimal(Decimal::ONE)
            .map_err(|_| InstrumentError::InvalidEffectiveInterval)?,
        venue_mappings: Vec::new(),
        provider_identities,
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })
}

#[test]
fn provider_identity_has_immutable_mapping_evidence_and_checked_wire()
-> Result<(), Box<dyn std::error::Error>> {
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    let record = record("revision-7", 100, 7, validity)?;
    assert_eq!(
        record.source_timestamp(),
        Some(Timestamp::from_unix_nanos(99))
    );
    assert_eq!(record.observed_at(), Timestamp::from_unix_nanos(100));
    assert_eq!(record.metadata_revision().as_str(), "revision-7");
    assert!(matches!(
        record.source_reference(),
        PayloadReference::ContentHash(_)
    ));

    let wire = serde_json::to_value(&record)?;
    assert_eq!(
        serde_json::from_value::<ProviderIdentityRecord>(wire.clone())?,
        record
    );
    let mut unknown = wire;
    unknown["mutable_note"] = serde_json::json!("not evidence");
    assert!(serde_json::from_value::<ProviderIdentityRecord>(unknown).is_err());
    Ok(())
}

#[test]
fn repeated_and_revised_evidence_is_retained_but_exact_duplicates_are_rejected()
-> Result<(), Box<dyn std::error::Error>> {
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    let first = record("revision-7", 100, 7, validity)?;
    let repeated = record("revision-7", 200, 8, validity)?;
    let revised = record("revision-8", 300, 9, validity)?;

    let accepted = definition(vec![first.clone(), repeated, revised])?;
    assert_eq!(accepted.provider_identities().len(), 3);
    assert_eq!(
        definition(vec![first.clone(), first]),
        Err(InstrumentError::DuplicateProviderIdentityEvidence)
    );
    Ok(())
}

#[test]
fn one_metadata_revision_cannot_make_conflicting_interval_claims()
-> Result<(), Box<dyn std::error::Error>> {
    let open = EffectiveInterval::new(Timestamp::from_unix_nanos(10), None)?;
    let closed = EffectiveInterval::new(
        Timestamp::from_unix_nanos(10),
        Some(Timestamp::from_unix_nanos(20)),
    )?;
    let first = record("revision-7", 100, 7, open)?;
    let conflicting = record("revision-7", 200, 8, closed)?;

    assert_eq!(
        definition(vec![first, conflicting]),
        Err(InstrumentError::ConflictingProviderIdentityInterval)
    );

    let revised = record("revision-8", 300, 9, closed)?;
    assert!(definition(vec![record("revision-7", 100, 7, open)?, revised]).is_ok());
    Ok(())
}
