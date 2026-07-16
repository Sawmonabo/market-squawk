use std::str::FromStr;

use market_squawk_domain::{
    FuturesContractIdentity, FuturesContractIdentityInput, FuturesLeg, FuturesLegInput,
    FuturesLegSide, FuturesLifecycleDates, FuturesSecurityType, MaturityMonthYear, PayloadHash,
    PayloadHashAlgorithm, PayloadReference, ProviderInstrumentId, SourceId, SourceIdentifier,
    Timestamp, VenueId, VenueSymbol,
};

fn evidence() -> PayloadReference {
    PayloadReference::ContentHash(PayloadHash::new(PayloadHashAlgorithm::Sha256, [17; 32]))
}

fn leg(
    position: u16,
    maturity_month_year: MaturityMonthYear,
    maturity_date: Option<market_squawk_domain::CalendarDate>,
    side: FuturesLegSide,
) -> Result<FuturesLeg, Box<dyn std::error::Error>> {
    Ok(FuturesLeg::try_new(FuturesLegInput {
        position,
        security_id: ProviderInstrumentId::try_from("ES-WEEKLY")?,
        security_id_source: SourceIdentifier::try_from("8")?,
        maturity_month_year: Some(maturity_month_year),
        maturity_date,
        side,
        ratio: 1,
    })?)
}

fn identity_input(
    security_type: FuturesSecurityType,
    maturity_month_year: Option<MaturityMonthYear>,
    legs: Vec<FuturesLeg>,
) -> Result<FuturesContractIdentityInput, Box<dyn std::error::Error>> {
    Ok(FuturesContractIdentityInput {
        source_id: SourceId::try_from("cme-security-definition")?,
        source_reference: evidence(),
        source_timestamp: Some(Timestamp::from_unix_nanos(900)),
        observed_at: Timestamp::from_unix_nanos(1_000),
        metadata_revision: SourceIdentifier::try_from("cme-sd:2026-03-01:42")?,
        venue_id: VenueId::try_from("XCME")?,
        security_id: ProviderInstrumentId::try_from("ES-WEEKLY-SPREAD")?,
        security_id_source: SourceIdentifier::try_from("8")?,
        product_code: ProviderInstrumentId::try_from("ES")?,
        native_symbol: VenueSymbol::try_from("ES-W1-W2")?,
        security_type,
        maturity_month_year,
        lifecycle: FuturesLifecycleDates::default(),
        legs,
    })
}

#[test]
fn fix_latest_month_year_forms_are_distinct_and_exact() -> Result<(), Box<dyn std::error::Error>> {
    let fixtures = [
        ("202603", MaturityMonthYear::month(2026, 3)?),
        ("20260320", MaturityMonthYear::day(2026, 3, 20)?),
        ("202603w1", MaturityMonthYear::week(2026, 3, 1)?),
        ("202603w2", MaturityMonthYear::week(2026, 3, 2)?),
    ];

    for (wire, expected) in fixtures {
        let parsed = MaturityMonthYear::from_str(wire)?;
        assert_eq!(parsed, expected);
        assert_eq!(parsed.to_string(), wire);
        assert_eq!(serde_json::to_string(&parsed)?, format!("\"{wire}\""));
        assert_eq!(
            serde_json::from_str::<MaturityMonthYear>(&format!("\"{wire}\""))?,
            parsed
        );
    }

    assert_ne!(fixtures[0].1, fixtures[1].1);
    assert_ne!(fixtures[2].1, fixtures[3].1);
    for invalid in [
        "2026-03",
        "202600",
        "202613",
        "20260300",
        "20260332",
        "202603w0",
        "202603w6",
        "202603W1",
        "202603w10",
    ] {
        assert!(MaturityMonthYear::from_str(invalid).is_err(), "{invalid}");
    }
    Ok(())
}

#[test]
fn tag_200_only_outright_keeps_identity_evidence_without_inventing_dates()
-> Result<(), Box<dyn std::error::Error>> {
    let input = identity_input(
        FuturesSecurityType::Future,
        Some(MaturityMonthYear::from_str("202603")?),
        Vec::new(),
    )?;
    let identity = FuturesContractIdentity::try_new(input)?;

    assert_eq!(
        identity.maturity_month_year(),
        Some(MaturityMonthYear::from_str("202603")?)
    );
    assert!(identity.lifecycle().is_empty());
    assert_eq!(identity.source_id().as_str(), "cme-security-definition");
    assert_eq!(identity.source_reference(), &evidence());
    assert_eq!(
        identity.source_timestamp(),
        Some(Timestamp::from_unix_nanos(900))
    );
    assert_eq!(identity.observed_at(), Timestamp::from_unix_nanos(1_000));
    assert_eq!(
        identity.metadata_revision().as_str(),
        "cme-sd:2026-03-01:42"
    );

    let wire = serde_json::to_value(&identity)?;
    assert!(wire.get("lifecycle").is_none());
    assert_eq!(
        serde_json::from_value::<FuturesContractIdentity>(wire)?,
        identity
    );
    Ok(())
}

#[test]
fn tag_610_week_legs_remain_distinct_and_tag_611_is_separate()
-> Result<(), Box<dyn std::error::Error>> {
    let first = leg(
        1,
        MaturityMonthYear::from_str("202603w1")?,
        Some(market_squawk_domain::CalendarDate::new(2026, 3, 6)?),
        FuturesLegSide::Buy,
    )?;
    let second = leg(
        2,
        MaturityMonthYear::from_str("202603w2")?,
        Some(market_squawk_domain::CalendarDate::new(2026, 3, 13)?),
        FuturesLegSide::Sell,
    )?;
    let identity = FuturesContractIdentity::try_new(identity_input(
        FuturesSecurityType::SpreadOrMultileg,
        None,
        vec![first, second],
    )?)?;

    assert_eq!(
        identity.legs()[0].maturity_month_year(),
        Some(MaturityMonthYear::from_str("202603w1")?)
    );
    assert_eq!(
        identity.legs()[1].maturity_month_year(),
        Some(MaturityMonthYear::from_str("202603w2")?)
    );
    assert_eq!(
        identity.legs()[0].maturity_date(),
        Some(market_squawk_domain::CalendarDate::new(2026, 3, 6)?)
    );
    assert_eq!(identity.source_reference(), &evidence());
    assert_eq!(
        serde_json::from_value::<FuturesContractIdentity>(serde_json::to_value(&identity)?)?,
        identity
    );
    Ok(())
}

#[test]
fn futures_wire_rejects_unknown_and_tampered_maturity_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let identity = FuturesContractIdentity::try_new(identity_input(
        FuturesSecurityType::Future,
        Some(MaturityMonthYear::from_str("20260320")?),
        Vec::new(),
    )?)?;
    let mut unknown = serde_json::to_value(&identity)?;
    unknown["derived_contract_month"] = serde_json::json!("202603");
    assert!(serde_json::from_value::<FuturesContractIdentity>(unknown).is_err());

    let mut invalid = serde_json::to_value(&identity)?;
    invalid["maturity_month_year"] = serde_json::json!("202603w6");
    assert!(serde_json::from_value::<FuturesContractIdentity>(invalid).is_err());
    Ok(())
}
