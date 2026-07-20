use std::error::Error;

use market_squawk_domain::{
    AccountId, ApprovalId, AssetClass, ClientOrderId, Currency, Denomination, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentDefinitionRevision, InstrumentExecutionTerms,
    InstrumentId, LotSize, OrderId, OrderReasonCode, OrderSide, OrderType, TickSize, TimeInForce,
    TradingStatus,
};
use rust_decimal::Decimal;
use uuid::Uuid;

#[test]
fn execution_uuid_identities_reject_nil_and_round_trip() -> Result<(), Box<dyn Error>> {
    assert!(AccountId::try_from(Uuid::nil()).is_err());
    assert!(OrderId::try_from(Uuid::nil()).is_err());
    assert!(ApprovalId::try_from(Uuid::nil()).is_err());

    let value = AccountId::try_from(Uuid::from_u128(1))?;
    let wire = serde_json::to_vec(&value)?;
    assert_eq!(serde_json::from_slice::<AccountId>(&wire)?, value);
    Ok(())
}

#[test]
fn client_and_reason_identifiers_are_bounded_and_strict() {
    assert!(ClientOrderId::try_from("").is_err());
    assert!(ClientOrderId::try_from("client-order-001").is_ok());
    assert!(OrderReasonCode::try_from("paper.momentum.v1").is_ok());
    assert!(OrderReasonCode::try_from("reason with spaces").is_err());
}

#[test]
fn execution_terms_bind_revision_precision_currency_and_multiplier() -> Result<(), Box<dyn Error>> {
    let instrument = InstrumentId::try_from(Uuid::from_u128(3))?;
    let usd = Currency::try_from("USD")?;
    let revision = InstrumentDefinitionRevision::try_from(7_u64)?;
    let terms = InstrumentExecutionTerms::try_new(
        instrument,
        revision,
        TickSize::try_from_decimal(Decimal::new(1, 2))?,
        LotSize::try_from_decimal(Decimal::new(1, 4))?,
        usd,
        Denomination::Currency(usd),
        Decimal::ONE,
    )?;

    assert_eq!(terms.instrument_id(), instrument);
    assert_eq!(terms.definition_revision(), revision);
    assert_eq!(terms.quote_currency(), usd);
    assert_eq!(terms.settlement_currency(), Some(usd));
    assert_eq!(terms.contract_multiplier(), Decimal::ONE);
    assert!(
        InstrumentExecutionTerms::try_new(
            instrument,
            revision,
            TickSize::try_from_decimal(Decimal::ONE)?,
            LotSize::try_from_decimal(Decimal::ONE)?,
            usd,
            Denomination::Currency(usd),
            Decimal::ZERO,
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn order_enums_use_closed_strict_wire_values() -> Result<(), Box<dyn Error>> {
    assert_eq!(serde_json::to_string(&OrderSide::Buy)?, "\"buy\"");
    assert_eq!(
        serde_json::to_string(&OrderType::StopLimit)?,
        "\"stop_limit\""
    );
    assert_eq!(
        serde_json::to_string(&TimeInForce::ImmediateOrCancel)?,
        "\"immediate_or_cancel\""
    );
    assert!(serde_json::from_str::<OrderType>("\"trailing_stop\"").is_err());
    Ok(())
}

#[test]
fn instrument_definition_owns_the_exact_execution_terms() -> Result<(), Box<dyn Error>> {
    let instrument = InstrumentId::try_from(Uuid::from_u128(4))?;
    let usd = Currency::try_from("USD")?;
    let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: instrument,
        definition_revision: InstrumentDefinitionRevision::try_from(9_u64)?,
        asset_class: AssetClass::Equity,
        primary_denomination: Denomination::Currency(usd),
        quote_currency: usd,
        tick_size: TickSize::try_from_decimal(Decimal::new(5, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
        contract_multiplier: Decimal::ONE,
        venue_mappings: Vec::new(),
        provider_identities: Vec::new(),
        identifiers: Vec::new(),
        trading_status: TradingStatus::Active,
    })?;

    assert_eq!(definition.definition_revision().get(), 9);
    assert_eq!(definition.execution_terms().instrument_id(), instrument);
    assert_eq!(
        definition.price_tick(),
        TickSize::try_from_decimal(Decimal::new(5, 2))?
    );
    assert_eq!(definition.quote_currency(), usd);
    assert_eq!(definition.settlement_currency(), Some(usd));
    assert_eq!(definition.contract_multiplier(), Decimal::ONE);
    Ok(())
}
