use market_squawk_domain::{
    CalendarDate, ContractMonth, FuturesContractIdentity, FuturesContractIdentityInput, FuturesLeg,
    FuturesLegSide, FuturesLifecycleDateFields, FuturesLifecycleDates, FuturesLifecycleDatesInput,
    FuturesSecurityType, IdentifierError, PayloadHash, PayloadHashAlgorithm, PayloadReference,
    ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp, VenueId, VenueSymbol,
};

fn reference() -> PayloadReference {
    PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Sha256, [7_u8; 32]))
}

fn lifecycle() -> Result<FuturesLifecycleDates, Box<dyn std::error::Error>> {
    Ok(FuturesLifecycleDates::try_new(
        FuturesLifecycleDatesInput {
            source_id: SourceId::try_from("cme-reference")?,
            source_reference: reference(),
            observed_at: Timestamp::from_unix_nanos(1_000),
            dates: FuturesLifecycleDateFields {
                maturity_date: Some(CalendarDate::new(2026, 3, 20)?),
                expiration_date: Some(CalendarDate::new(2026, 3, 20)?),
                last_trade_date: Some(CalendarDate::new(2026, 3, 20)?),
                first_notice_date: Some(CalendarDate::new(2026, 2, 24)?),
                last_notice_date: Some(CalendarDate::new(2026, 2, 27)?),
                first_delivery_date: Some(CalendarDate::new(2026, 3, 18)?),
                last_delivery_date: Some(CalendarDate::new(2026, 3, 23)?),
            },
        },
    )?)
}

#[test]
fn lifecycle_dates_retain_evidence_and_enforce_ordering() -> Result<(), Box<dyn std::error::Error>>
{
    let dates = lifecycle()?;
    assert_eq!(dates.source_id().as_str(), "cme-reference");
    assert_eq!(dates.source_reference(), &reference());
    assert_eq!(dates.observed_at(), Timestamp::from_unix_nanos(1_000));
    assert_eq!(dates.maturity_date(), Some(CalendarDate::new(2026, 3, 20)?));
    assert_eq!(
        dates.expiration_date(),
        Some(CalendarDate::new(2026, 3, 20)?)
    );
    assert_eq!(
        dates.last_trade_date(),
        Some(CalendarDate::new(2026, 3, 20)?)
    );
    assert_eq!(
        dates.first_notice_date(),
        Some(CalendarDate::new(2026, 2, 24)?)
    );
    assert_eq!(
        dates.last_notice_date(),
        Some(CalendarDate::new(2026, 2, 27)?)
    );
    assert_eq!(
        dates.first_delivery_date(),
        Some(CalendarDate::new(2026, 3, 18)?)
    );
    assert_eq!(
        dates.last_delivery_date(),
        Some(CalendarDate::new(2026, 3, 23)?)
    );

    assert_eq!(
        FuturesLifecycleDates::try_new(FuturesLifecycleDatesInput {
            source_id: SourceId::try_from("cme-reference")?,
            source_reference: reference(),
            observed_at: Timestamp::from_unix_nanos(1_000),
            dates: FuturesLifecycleDateFields::default(),
        }),
        Err(IdentifierError::MissingLifecycleDate)
    );
    assert_eq!(
        FuturesLifecycleDates::try_new(FuturesLifecycleDatesInput {
            source_id: SourceId::try_from("cme-reference")?,
            source_reference: reference(),
            observed_at: Timestamp::from_unix_nanos(1_000),
            dates: FuturesLifecycleDateFields {
                first_notice_date: Some(CalendarDate::new(2026, 3, 2)?),
                last_notice_date: Some(CalendarDate::new(2026, 3, 1)?),
                ..FuturesLifecycleDateFields::default()
            },
        }),
        Err(IdentifierError::InvalidLifecycleOrdering)
    );
    Ok(())
}

#[test]
fn multileg_futures_are_ordered_structured_and_nonzero() -> Result<(), Box<dyn std::error::Error>> {
    let first = FuturesLeg::try_new(
        1,
        ProviderInstrumentId::try_from("ESH6")?,
        SourceIdentifier::try_from("8")?,
        Some(ContractMonth::new(2026, 3)?),
        FuturesLegSide::Buy,
        1,
    )?;
    let second = FuturesLeg::try_new(
        2,
        ProviderInstrumentId::try_from("ESM6")?,
        SourceIdentifier::try_from("8")?,
        Some(ContractMonth::new(2026, 6)?),
        FuturesLegSide::Sell,
        2,
    )?;
    assert_eq!(first.position(), 1);
    assert_eq!(first.security_id().as_str(), "ESH6");
    assert_eq!(first.security_id_source().as_str(), "8");
    assert_eq!(first.contract_month(), Some(ContractMonth::new(2026, 3)?));
    assert_eq!(first.side(), FuturesLegSide::Buy);
    assert_eq!(first.ratio(), 1);
    assert_eq!(
        FuturesLeg::try_new(
            1,
            ProviderInstrumentId::try_from("ESH6")?,
            SourceIdentifier::try_from("8")?,
            None,
            FuturesLegSide::Buy,
            0,
        ),
        Err(IdentifierError::ZeroLegRatio)
    );

    let spread = FuturesContractIdentity::try_new(FuturesContractIdentityInput {
        venue_id: VenueId::try_from("XCME")?,
        security_id: ProviderInstrumentId::try_from("calendar-spread")?,
        security_id_source: SourceIdentifier::try_from("8")?,
        product_code: ProviderInstrumentId::try_from("ES")?,
        native_symbol: VenueSymbol::try_from("ESH6-ESM6")?,
        security_type: FuturesSecurityType::SpreadOrMultileg,
        contract_month: None,
        lifecycle: lifecycle()?,
        legs: vec![first.clone(), second.clone()],
    })?;
    assert_eq!(spread.venue_id().as_str(), "XCME");
    assert_eq!(spread.security_id().as_str(), "calendar-spread");
    assert_eq!(spread.security_id_source().as_str(), "8");
    assert_eq!(spread.product_code().as_str(), "ES");
    assert_eq!(spread.native_symbol().as_str(), "ESH6-ESM6");
    assert_eq!(
        spread.security_type(),
        FuturesSecurityType::SpreadOrMultileg
    );
    assert_eq!(spread.contract_month(), None);
    assert_eq!(
        spread.lifecycle().maturity_date(),
        Some(CalendarDate::new(2026, 3, 20)?)
    );
    assert_eq!(
        spread.legs()[0].contract_month(),
        Some(ContractMonth::new(2026, 3)?)
    );
    assert_eq!(
        spread.legs()[1].contract_month(),
        Some(ContractMonth::new(2026, 6)?)
    );
    assert_eq!(spread.lifecycle(), &lifecycle()?);
    assert_eq!(spread.legs(), &[first.clone(), second.clone()]);
    assert_eq!(spread.to_string(), "XCME:calendar-spread");

    assert_eq!(
        FuturesContractIdentity::try_new(FuturesContractIdentityInput {
            venue_id: VenueId::try_from("XCME")?,
            security_id: ProviderInstrumentId::try_from("calendar-spread")?,
            security_id_source: SourceIdentifier::try_from("8")?,
            product_code: ProviderInstrumentId::try_from("ES")?,
            native_symbol: VenueSymbol::try_from("ESH6-ESM6")?,
            security_type: FuturesSecurityType::SpreadOrMultileg,
            contract_month: None,
            lifecycle: lifecycle()?,
            legs: vec![second, first],
        }),
        Err(IdentifierError::InvalidLegOrdering)
    );
    Ok(())
}

#[test]
fn outright_full_date_does_not_invent_contract_month_and_wire_preserves_absence()
-> Result<(), Box<dyn std::error::Error>> {
    let contract = FuturesContractIdentity::try_new(FuturesContractIdentityInput {
        venue_id: VenueId::try_from("XNYM")?,
        security_id: ProviderInstrumentId::try_from("daily-2026-03-20")?,
        security_id_source: SourceIdentifier::try_from("8")?,
        product_code: ProviderInstrumentId::try_from("CL")?,
        native_symbol: VenueSymbol::try_from("CL-20260320")?,
        security_type: FuturesSecurityType::Daily,
        contract_month: None,
        lifecycle: lifecycle()?,
        legs: Vec::new(),
    })?;

    assert_eq!(contract.contract_month(), None);
    assert_eq!(
        contract.lifecycle().maturity_date(),
        Some(CalendarDate::new(2026, 3, 20)?)
    );

    let encoded = serde_json::to_value(&contract)?;
    assert!(encoded.get("contract_month").is_none());
    let decoded: FuturesContractIdentity = serde_json::from_value(encoded)?;
    assert_eq!(decoded.contract_month(), None);
    assert_eq!(decoded.lifecycle(), contract.lifecycle());
    assert!(decoded.legs().is_empty());

    let mut unknown_top_level = serde_json::to_value(&contract)?;
    unknown_top_level["derived_contract_month"] = serde_json::json!({
        "year": 2026,
        "month": 3
    });
    assert!(serde_json::from_value::<FuturesContractIdentity>(unknown_top_level).is_err());
    Ok(())
}
