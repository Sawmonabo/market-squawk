use market_squawk_domain::{
    CalendarDate, ContractMonth, FuturesContractIdentity, FuturesLeg, FuturesLegSide,
    FuturesLifecycleDates, FuturesSecurityType, IdentifierError, PayloadHash, PayloadHashAlgorithm,
    PayloadReference, ProviderInstrumentId, SourceId, SourceIdentifier, Timestamp, VenueId,
    VenueSymbol,
};

fn reference() -> PayloadReference {
    PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Sha256, [7_u8; 32]))
}

fn lifecycle() -> Result<FuturesLifecycleDates, Box<dyn std::error::Error>> {
    Ok(FuturesLifecycleDates::try_new(
        SourceId::try_from("cme-reference")?,
        reference(),
        Timestamp::from_unix_nanos(1_000),
        Some(CalendarDate::new(2026, 3, 20)?),
        Some(CalendarDate::new(2026, 3, 20)?),
        Some(CalendarDate::new(2026, 3, 20)?),
        Some(CalendarDate::new(2026, 2, 24)?),
        Some(CalendarDate::new(2026, 2, 27)?),
        Some(CalendarDate::new(2026, 3, 18)?),
        Some(CalendarDate::new(2026, 3, 23)?),
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
        FuturesLifecycleDates::try_new(
            SourceId::try_from("cme-reference")?,
            reference(),
            Timestamp::from_unix_nanos(1_000),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        Err(IdentifierError::MissingLifecycleDate)
    );
    assert_eq!(
        FuturesLifecycleDates::try_new(
            SourceId::try_from("cme-reference")?,
            reference(),
            Timestamp::from_unix_nanos(1_000),
            None,
            None,
            None,
            Some(CalendarDate::new(2026, 3, 2)?),
            Some(CalendarDate::new(2026, 3, 1)?),
            None,
            None,
        ),
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

    let spread = FuturesContractIdentity::try_new(
        VenueId::try_from("XCME")?,
        ProviderInstrumentId::try_from("calendar-spread")?,
        SourceIdentifier::try_from("8")?,
        ProviderInstrumentId::try_from("ES")?,
        VenueSymbol::try_from("ESH6-ESM6")?,
        FuturesSecurityType::SpreadOrMultileg,
        ContractMonth::new(2026, 3)?,
        lifecycle()?,
        vec![first.clone(), second.clone()],
    )?;
    assert_eq!(spread.venue_id().as_str(), "XCME");
    assert_eq!(spread.security_id().as_str(), "calendar-spread");
    assert_eq!(spread.security_id_source().as_str(), "8");
    assert_eq!(spread.product_code().as_str(), "ES");
    assert_eq!(spread.native_symbol().as_str(), "ESH6-ESM6");
    assert_eq!(
        spread.security_type(),
        FuturesSecurityType::SpreadOrMultileg
    );
    assert_eq!(spread.contract_month(), ContractMonth::new(2026, 3)?);
    assert_eq!(spread.lifecycle(), &lifecycle()?);
    assert_eq!(spread.legs(), &[first.clone(), second.clone()]);
    assert_eq!(spread.to_string(), "XCME:calendar-spread");

    assert_eq!(
        FuturesContractIdentity::try_new(
            VenueId::try_from("XCME")?,
            ProviderInstrumentId::try_from("calendar-spread")?,
            SourceIdentifier::try_from("8")?,
            ProviderInstrumentId::try_from("ES")?,
            VenueSymbol::try_from("ESH6-ESM6")?,
            FuturesSecurityType::SpreadOrMultileg,
            ContractMonth::new(2026, 3)?,
            lifecycle()?,
            vec![second, first],
        ),
        Err(IdentifierError::InvalidLegOrdering)
    );
    Ok(())
}
