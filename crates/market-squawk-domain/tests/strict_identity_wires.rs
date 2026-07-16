use market_squawk_domain::{
    CalendarDate, EffectiveInterval, InstrumentId, LifecycleTransition, LifecycleTransitionKind,
    SymbolIdentityRecord, Timestamp, VenueId, VenueSymbol,
};
use uuid::Uuid;

fn instrument(value: u128) -> Result<InstrumentId, Box<dyn std::error::Error>> {
    Ok(InstrumentId::try_from(Uuid::from_u128(value))?)
}

#[test]
fn calendar_and_effective_interval_wires_reject_unknown_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let mut date = serde_json::to_value(CalendarDate::new(2026, 7, 16)?)?;
    date["timezone"] = serde_json::json!("UTC");
    assert!(serde_json::from_value::<CalendarDate>(date).is_err());

    let mut interval =
        serde_json::to_value(EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?)?;
    interval["inclusive_end"] = serde_json::json!(true);
    assert!(serde_json::from_value::<EffectiveInterval>(interval).is_err());
    Ok(())
}

#[test]
fn initial_identity_record_wires_reject_unassigned_fields() -> Result<(), Box<dyn std::error::Error>>
{
    let symbol = SymbolIdentityRecord::new(
        instrument(1)?,
        VenueId::try_from("XNYS")?,
        VenueSymbol::try_from("ABC")?,
        EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
    );
    let mut symbol_wire = serde_json::to_value(symbol)?;
    symbol_wire["global_symbol"] = serde_json::json!("ABC");
    assert!(serde_json::from_value::<SymbolIdentityRecord>(symbol_wire).is_err());

    let transition = LifecycleTransition::new(
        instrument(1)?,
        Timestamp::from_unix_nanos(2),
        LifecycleTransitionKind::Delisting,
    )?;
    let mut transition_wire = serde_json::to_value(transition)?;
    transition_wire["inferred_successor"] = serde_json::json!(instrument(2)?.to_string());
    assert!(serde_json::from_value::<LifecycleTransition>(transition_wire).is_err());
    Ok(())
}
