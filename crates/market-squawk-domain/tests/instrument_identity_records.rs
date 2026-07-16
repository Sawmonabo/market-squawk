use market_squawk_domain::{
    AssetClass, AssignmentVerification, ContractRollMapping, Currency, Denomination,
    EffectiveInterval, ExternalIdentifier, ExternalIdentifierRecord, IdentifierEntitlement,
    IdentifierRightsPolicyReference, IdentifierSyntaxVerification, InstrumentDefinition,
    InstrumentError, InstrumentId, Isin, LifecycleTransition, LifecycleTransitionKind, LotSize,
    PayloadHash, PayloadHashAlgorithm, PayloadReference, ProviderIdentityRecord,
    ProviderInstrumentId, SourceId, SourceIdentifier, SymbolIdentityRecord, TickSize, Timestamp,
    TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn instrument(value: &str) -> Result<InstrumentId, Box<dyn std::error::Error>> {
    Ok(InstrumentId::try_from(Uuid::parse_str(value)?)?)
}

fn rights() -> Result<IdentifierRightsPolicyReference, Box<dyn std::error::Error>> {
    Ok(IdentifierRightsPolicyReference::new(
        SourceIdentifier::try_from("policy:identifier-restricted-v1")?,
        IdentifierEntitlement::UnknownOrRestricted,
        SourceIdentifier::try_from("https://www.iso.org/standard/78502.html")?,
    ))
}

fn identifier_record() -> Result<ExternalIdentifierRecord, Box<dyn std::error::Error>> {
    Ok(ExternalIdentifierRecord::new(
        ExternalIdentifier::Isin(Isin::try_from("US0378331005")?),
        AssignmentVerification::VerifiedAssigned,
        SourceId::try_from("anna-reference")?,
        PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Sha256, [3_u8; 32])),
        Some(Timestamp::from_unix_nanos(90)),
        Timestamp::from_unix_nanos(100),
        EffectiveInterval::new(Timestamp::from_unix_nanos(80), None)?,
        rights()?,
    ))
}

#[test]
fn external_identifier_record_retains_verification_provenance_time_and_rights()
-> Result<(), Box<dyn std::error::Error>> {
    let record = identifier_record()?;
    assert_eq!(record.identifier().to_string(), "US0378331005");
    assert_eq!(
        record.syntax_verification(),
        IdentifierSyntaxVerification::ChecksumValidated
    );
    assert_eq!(
        record.assignment_verification(),
        AssignmentVerification::VerifiedAssigned
    );
    assert_eq!(record.source_id().as_str(), "anna-reference");
    assert!(matches!(
        record.source_reference(),
        PayloadReference::ContentHash(_)
    ));
    assert_eq!(
        record.source_timestamp(),
        Some(Timestamp::from_unix_nanos(90))
    );
    assert_eq!(record.observed_at(), Timestamp::from_unix_nanos(100));
    assert_eq!(
        record.validity().starts_at(),
        Timestamp::from_unix_nanos(80)
    );
    assert_eq!(record.rights_policy(), &rights()?);
    assert_eq!(
        record.rights_policy().policy_id().as_str(),
        "policy:identifier-restricted-v1"
    );
    assert_eq!(
        record.rights_policy().entitlement(),
        IdentifierEntitlement::UnknownOrRestricted
    );
    assert_eq!(
        record.rights_policy().terms_reference().as_str(),
        "https://www.iso.org/standard/78502.html"
    );
    assert_eq!(record.to_string(), "US0378331005");
    Ok(())
}

#[test]
fn identifier_record_wire_derives_syntax_and_rejects_tampering()
-> Result<(), Box<dyn std::error::Error>> {
    let record = identifier_record()?;
    let encoded = serde_json::to_value(&record)?;
    assert!(encoded.get("syntax_verification").is_none());
    assert_eq!(
        serde_json::from_value::<ExternalIdentifierRecord>(encoded)?,
        record
    );

    let mut tampered = serde_json::to_value(&record)?;
    tampered["syntax_verification"] = serde_json::json!("syntax_validated");
    assert!(serde_json::from_value::<ExternalIdentifierRecord>(tampered).is_err());
    Ok(())
}

#[test]
fn definition_uses_typed_denomination_and_rejects_duplicate_identifiers()
-> Result<(), Box<dyn std::error::Error>> {
    let id = instrument("936da01f-9abd-4d9d-80c7-02af85c822a8")?;
    let settlement_asset = instrument("7d9e9f3e-b62d-4fce-a85f-fad3ca549c97")?;
    let record = identifier_record()?;
    let definition = InstrumentDefinition::try_new(
        id,
        AssetClass::Crypto,
        Denomination::Asset(settlement_asset),
        TickSize::try_from_decimal(Decimal::new(1, 2))?,
        LotSize::try_from_decimal(Decimal::ONE)?,
        Vec::new(),
        vec![record.clone()],
        TradingStatus::Active,
    )?;
    assert_eq!(
        definition.primary_denomination(),
        Denomination::Asset(settlement_asset)
    );
    assert_eq!(definition.identifiers(), std::slice::from_ref(&record));
    assert_eq!(
        InstrumentDefinition::try_new(
            id,
            AssetClass::Equity,
            Denomination::Currency(Currency::try_from("USD")?),
            TickSize::try_from_decimal(Decimal::new(1, 2))?,
            LotSize::try_from_decimal(Decimal::ONE)?,
            Vec::new(),
            vec![record.clone(), record],
            TradingStatus::Active,
        ),
        Err(InstrumentError::DuplicateExternalIdentifier)
    );
    Ok(())
}

#[test]
fn identity_records_and_mappings_expose_complete_borrowed_state()
-> Result<(), Box<dyn std::error::Error>> {
    let id = instrument("936da01f-9abd-4d9d-80c7-02af85c822a8")?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?;
    let mapping = VenueMapping::new(
        VenueId::try_from("XNAS")?,
        VenueSymbol::try_from("AAPL")?,
        Some(ProviderInstrumentId::try_from("AAPL.O")?),
    );
    assert_eq!(mapping.venue_id().as_str(), "XNAS");
    assert_eq!(mapping.venue_symbol().as_str(), "AAPL");
    assert_eq!(
        mapping
            .provider_instrument_id()
            .map(ProviderInstrumentId::as_str),
        Some("AAPL.O")
    );
    assert_eq!(mapping.to_string(), "XNAS:AAPL");

    let symbol = SymbolIdentityRecord::new(
        id,
        VenueId::try_from("XNAS")?,
        VenueSymbol::try_from("AAPL")?,
        validity,
    );
    assert_eq!(symbol.instrument_id(), id);
    assert_eq!(symbol.venue_id().as_str(), "XNAS");
    assert_eq!(symbol.venue_symbol().as_str(), "AAPL");
    assert_eq!(symbol.validity(), validity);

    let provider = ProviderIdentityRecord::new(
        id,
        SourceId::try_from("nasdaq-reference")?,
        ProviderInstrumentId::try_from("AAPL.O")?,
        validity,
    );
    assert_eq!(provider.instrument_id(), id);
    assert_eq!(provider.source_id().as_str(), "nasdaq-reference");
    assert_eq!(provider.provider_instrument_id().as_str(), "AAPL.O");
    assert_eq!(provider.validity(), validity);

    let successor = instrument("7d9e9f3e-b62d-4fce-a85f-fad3ca549c97")?;
    let effective_at = Timestamp::from_unix_nanos(2);
    let transition = LifecycleTransition::new(
        id,
        effective_at,
        LifecycleTransitionKind::Merger { successor },
    )?;
    assert_eq!(transition.instrument_id(), id);
    assert_eq!(transition.effective_at(), effective_at);
    assert_eq!(
        transition.kind(),
        LifecycleTransitionKind::Merger { successor }
    );
    assert!(transition.to_string().contains("merger"));

    let roll = ContractRollMapping::new(id, successor, effective_at)?;
    assert_eq!(roll.from_instrument_id(), id);
    assert_eq!(roll.to_instrument_id(), successor);
    assert_eq!(roll.effective_at(), effective_at);
    assert!(roll.to_string().contains("->"));
    Ok(())
}
