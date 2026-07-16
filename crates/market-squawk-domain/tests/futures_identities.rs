use market_squawk_domain::{
    CalendarDate, FuturesContractIdentity, FuturesContractIdentityInput, FuturesLeg,
    FuturesLegInput, FuturesLegSide, FuturesLifecycleDateFields, FuturesLifecycleDates,
    FuturesSecurityType, IdentifierError, MaturityMonthYear, PayloadHash, PayloadHashAlgorithm,
    PayloadReference, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp, VenueId,
    VenueSymbol,
};

fn reference() -> PayloadReference {
    PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Sha256, [7; 32]))
}

fn lifecycle() -> Result<FuturesLifecycleDates, Box<dyn std::error::Error>> {
    Ok(FuturesLifecycleDates::try_new(
        FuturesLifecycleDateFields {
            maturity_date: Some(CalendarDate::new(2026, 3, 20)?),
            expiration_date: Some(CalendarDate::new(2026, 3, 20)?),
            last_trade_date: Some(CalendarDate::new(2026, 3, 20)?),
            first_notice_date: Some(CalendarDate::new(2026, 2, 24)?),
            last_notice_date: Some(CalendarDate::new(2026, 2, 27)?),
            first_delivery_date: Some(CalendarDate::new(2026, 3, 18)?),
            last_delivery_date: Some(CalendarDate::new(2026, 3, 23)?),
        },
    )?)
}

fn identity_input(
    security_type: FuturesSecurityType,
    maturity_month_year: Option<MaturityMonthYear>,
    lifecycle: FuturesLifecycleDates,
    legs: Vec<FuturesLeg>,
) -> Result<FuturesContractIdentityInput, Box<dyn std::error::Error>> {
    Ok(FuturesContractIdentityInput {
        source_id: SourceId::try_from("cme-reference")?,
        source_reference: reference(),
        source_timestamp: Some(Timestamp::from_unix_nanos(900)),
        observed_at: Timestamp::from_unix_nanos(1_000),
        metadata_revision: SourceIdentifier::try_from("security-definition:42")?,
        venue_id: VenueId::try_from("XCME")?,
        security_id: ProviderInstrumentId::try_from("calendar-spread")?,
        security_id_source: SourceIdentifier::try_from("8")?,
        product_code: ProviderInstrumentId::try_from("ES")?,
        native_symbol: VenueSymbol::try_from("ESH6-ESM6")?,
        security_type,
        maturity_month_year,
        lifecycle,
        legs,
    })
}

fn leg(
    position: u16,
    security_id: &str,
    maturity_month_year: MaturityMonthYear,
    side: FuturesLegSide,
    ratio: u32,
) -> Result<FuturesLeg, Box<dyn std::error::Error>> {
    Ok(FuturesLeg::try_new(FuturesLegInput {
        position,
        security_id: ProviderInstrumentId::try_from(security_id)?,
        security_id_source: SourceIdentifier::try_from("8")?,
        maturity_month_year: Some(maturity_month_year),
        maturity_date: None,
        side,
        ratio,
    })?)
}

#[test]
fn lifecycle_dates_allow_absence_and_enforce_ordering() -> Result<(), Box<dyn std::error::Error>> {
    let dates = lifecycle()?;
    assert!(!dates.is_empty());
    assert_eq!(dates.maturity_date(), Some(CalendarDate::new(2026, 3, 20)?));
    assert_eq!(
        dates.last_notice_date(),
        Some(CalendarDate::new(2026, 2, 27)?)
    );
    assert!(FuturesLifecycleDates::default().is_empty());
    assert_eq!(
        FuturesLifecycleDates::try_new(FuturesLifecycleDateFields {
            first_notice_date: Some(CalendarDate::new(2026, 3, 2)?),
            last_notice_date: Some(CalendarDate::new(2026, 3, 1)?),
            ..FuturesLifecycleDateFields::default()
        }),
        Err(IdentifierError::InvalidLifecycleOrdering)
    );
    Ok(())
}

#[test]
fn multileg_futures_are_ordered_structured_and_nonzero() -> Result<(), Box<dyn std::error::Error>> {
    let march = MaturityMonthYear::month(2026, 3)?;
    let june = MaturityMonthYear::month(2026, 6)?;
    let first = leg(1, "ESH6", march, FuturesLegSide::Buy, 1)?;
    let second = leg(2, "ESM6", june, FuturesLegSide::Sell, 2)?;
    assert_eq!(first.position(), 1);
    assert_eq!(first.security_id().as_str(), "ESH6");
    assert_eq!(first.maturity_month_year(), Some(march));
    assert_eq!(first.side(), FuturesLegSide::Buy);
    assert_eq!(first.ratio(), 1);
    assert_eq!(
        FuturesLeg::try_new(FuturesLegInput {
            position: 1,
            security_id: ProviderInstrumentId::try_from("ESH6")?,
            security_id_source: SourceIdentifier::try_from("8")?,
            maturity_month_year: None,
            maturity_date: None,
            side: FuturesLegSide::Buy,
            ratio: 0,
        }),
        Err(IdentifierError::ZeroLegRatio)
    );

    let spread = FuturesContractIdentity::try_new(identity_input(
        FuturesSecurityType::SpreadOrMultileg,
        None,
        lifecycle()?,
        vec![first.clone(), second.clone()],
    )?)?;
    assert_eq!(spread.venue_id().as_str(), "XCME");
    assert_eq!(
        spread.security_type(),
        FuturesSecurityType::SpreadOrMultileg
    );
    assert_eq!(spread.maturity_month_year(), None);
    assert_eq!(spread.legs()[0].maturity_month_year(), Some(march));
    assert_eq!(spread.legs()[1].maturity_month_year(), Some(june));
    assert_eq!(spread.legs(), &[first.clone(), second.clone()]);
    assert_eq!(spread.to_string(), "XCME:calendar-spread");

    assert_eq!(
        FuturesContractIdentity::try_new(identity_input(
            FuturesSecurityType::SpreadOrMultileg,
            None,
            lifecycle()?,
            vec![second, first],
        )?),
        Err(IdentifierError::InvalidLegOrdering)
    );
    Ok(())
}

#[test]
fn maturity_date_does_not_invent_tag_200_and_wire_preserves_absence()
-> Result<(), Box<dyn std::error::Error>> {
    let mut input = identity_input(FuturesSecurityType::Daily, None, lifecycle()?, Vec::new())?;
    input.venue_id = VenueId::try_from("XNYM")?;
    input.security_id = ProviderInstrumentId::try_from("daily-2026-03-20")?;
    input.product_code = ProviderInstrumentId::try_from("CL")?;
    input.native_symbol = VenueSymbol::try_from("CL-20260320")?;
    let contract = FuturesContractIdentity::try_new(input)?;

    assert_eq!(contract.maturity_month_year(), None);
    assert_eq!(
        contract.lifecycle().maturity_date(),
        Some(CalendarDate::new(2026, 3, 20)?)
    );
    let encoded = serde_json::to_value(&contract)?;
    assert!(encoded.get("maturity_month_year").is_none());
    assert_eq!(
        serde_json::from_value::<FuturesContractIdentity>(encoded)?,
        contract
    );
    Ok(())
}
