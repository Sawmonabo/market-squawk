use market_squawk_domain::{
    AssetClass, AssignmentVerification, ContractRollMapping, Currency, Denomination,
    EffectiveInterval, EvidenceDigest, ExternalIdentifier, ExternalIdentifierRecord,
    ExternalIdentifierRecordInput, IdentifierEntitlement, IdentifierRightsPolicyReference,
    IdentifierSyntaxVerification, InstrumentDefinition, InstrumentDefinitionInput, InstrumentError,
    InstrumentId, Isin, LifecycleTransition, LifecycleTransitionKind, LotSize, MetadataRevision,
    PayloadHash, PayloadHashAlgorithm, PayloadReference, ProviderIdentityEvidence,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderInstrumentId, SourceId,
    SourceIdentifier, SymbolIdentityRecord, TickSize, Timestamp, TradingStatus, VenueId,
    VenueMapping, VenueSymbol,
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
        ExternalIdentifierRecordInput {
            identifier: ExternalIdentifier::Isin(Isin::try_from("US0378331005")?),
            assignment_verification: AssignmentVerification::VerifiedAssigned,
            source_id: SourceId::try_from("anna-reference")?,
            source_reference: PayloadReference::ContentHash(PayloadHash::new(
                PayloadHashAlgorithm::Sha256,
                [3_u8; 32],
            )),
            source_timestamp: Some(Timestamp::from_unix_nanos(90)),
            observed_at: Timestamp::from_unix_nanos(100),
            validity: EffectiveInterval::new(Timestamp::from_unix_nanos(80), None)?,
            rights_policy: rights()?,
        },
    ))
}

fn provider_identity(
    instrument_id: InstrumentId,
    source_id: &str,
    provider_instrument_id: &str,
    validity: EffectiveInterval,
    evidence_byte: u8,
) -> Result<ProviderIdentityRecord, Box<dyn std::error::Error>> {
    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id,
        source_id: SourceId::try_from(source_id)?,
        provider_instrument_id: ProviderInstrumentId::try_from(provider_instrument_id)?,
        evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
            PayloadHashAlgorithm::Sha256,
            [evidence_byte; 32],
        )),
        source_timestamp: Some(Timestamp::from_unix_nanos(90)),
        observed_at: Timestamp::from_unix_nanos(100),
        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(format!(
            "revision-{evidence_byte}",
        ))?),
        validity,
        supersedes: None,
    }))
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
    let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: id,
        asset_class: AssetClass::Crypto,
        primary_denomination: Denomination::Asset(settlement_asset),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
        venue_mappings: Vec::new(),
        provider_identities: Vec::new(),
        identifiers: vec![record.clone()],
        trading_status: TradingStatus::Active,
    })?;
    assert_eq!(
        definition.primary_denomination(),
        Denomination::Asset(settlement_asset)
    );
    assert_eq!(definition.identifiers(), std::slice::from_ref(&record));
    assert_eq!(
        InstrumentDefinition::try_new(InstrumentDefinitionInput {
            instrument_id: id,
            asset_class: AssetClass::Equity,
            primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
            tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
            lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
            venue_mappings: Vec::new(),
            provider_identities: Vec::new(),
            identifiers: vec![record.clone(), record],
            trading_status: TradingStatus::Active,
        }),
        Err(InstrumentError::DuplicateExternalIdentifier)
    );
    Ok(())
}

#[test]
fn identity_records_and_mappings_expose_complete_borrowed_state()
-> Result<(), Box<dyn std::error::Error>> {
    let id = instrument("936da01f-9abd-4d9d-80c7-02af85c822a8")?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?;
    let mapping = VenueMapping::new(VenueId::try_from("XNAS")?, VenueSymbol::try_from("AAPL")?);
    assert_eq!(mapping.venue_id().as_str(), "XNAS");
    assert_eq!(mapping.venue_symbol().as_str(), "AAPL");
    assert_eq!(mapping.to_string(), "XNAS:AAPL");
    assert!(
        serde_json::to_value(&mapping)?
            .get("provider_instrument_id")
            .is_none()
    );
    assert!(
        serde_json::from_value::<VenueMapping>(serde_json::json!({
            "venue_id": "XNAS",
            "venue_symbol": "AAPL",
            "provider_instrument_id": "AAPL.O"
        }))
        .is_err()
    );

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

    let provider = provider_identity(id, "nasdaq-reference", "AAPL.O", validity, 4)?;
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

#[test]
fn provider_identity_text_is_qualified_by_source_in_instrument_definition()
-> Result<(), Box<dyn std::error::Error>> {
    let id = instrument("936da01f-9abd-4d9d-80c7-02af85c822a8")?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?;
    let first = provider_identity(id, "vendor-alpha", "12345", validity, 5)?;
    let second = provider_identity(id, "vendor-beta", "12345", validity, 6)?;
    let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: id,
        asset_class: AssetClass::Equity,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
        venue_mappings: vec![VenueMapping::new(
            VenueId::try_from("XNAS")?,
            VenueSymbol::try_from("ACME")?,
        )],
        provider_identities: vec![first.clone(), second.clone()],
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?;

    assert_eq!(definition.provider_identities(), &[first.clone(), second]);
    assert_ne!(
        definition.provider_identities()[0].source_id(),
        definition.provider_identities()[1].source_id()
    );
    assert_eq!(
        definition.provider_identities()[0]
            .provider_instrument_id()
            .as_str(),
        definition.provider_identities()[1]
            .provider_instrument_id()
            .as_str()
    );

    let mut ambiguous_wire = serde_json::to_value(&definition)?;
    ambiguous_wire["provider_instrument_id"] = serde_json::json!("12345");
    assert!(serde_json::from_value::<InstrumentDefinition>(ambiguous_wire).is_err());

    let definition_input = || -> Result<InstrumentDefinitionInput, Box<dyn std::error::Error>> {
        Ok(InstrumentDefinitionInput {
            instrument_id: id,
            asset_class: AssetClass::Equity,
            primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
            tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
            lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
            venue_mappings: Vec::new(),
            provider_identities: Vec::new(),
            identifiers: Vec::new(),
            trading_status: TradingStatus::Active,
        })
    };
    let mut duplicate = definition_input()?;
    duplicate.provider_identities = vec![first.clone(), first];
    let coalesced = InstrumentDefinition::try_new(duplicate)?;
    assert_eq!(coalesced.provider_identities().len(), 1);
    assert_eq!(
        coalesced.provider_identities()[0]
            .observation_timestamps()
            .len(),
        1
    );

    let other_id = instrument("7d9e9f3e-b62d-4fce-a85f-fad3ca549c97")?;
    let mut mismatched = definition_input()?;
    mismatched.provider_identities = vec![provider_identity(
        other_id,
        "vendor-alpha",
        "12345",
        validity,
        7,
    )?];
    assert_eq!(
        InstrumentDefinition::try_new(mismatched),
        Err(InstrumentError::ProviderIdentityInstrumentMismatch {
            definition: id,
            record: other_id,
        })
    );
    Ok(())
}
