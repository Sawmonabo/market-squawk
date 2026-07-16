use std::error::Error;
use std::str::FromStr;

use market_squawk_domain::{
    AggressorSide, AlternativeDataObservation, AuctionEvent, AuctionPhase, BookDeltaEvent,
    BookLevel, BookSnapshotEvent, CorporateActionEvent, CorporateActionKind,
    CorporateActionObservation, DataQuality, FilingObservation, FundamentalObservation,
    HaltTransition, InstrumentId, InstrumentStatusEvent, MacroObservation, MarketDepth,
    MarketEvent, MarketEventError, MarketSide, PayloadReference, PositionObservation, PositionSide,
    PriceTicks, Provenance, QuantityLots, QuoteEvent, ResearchContext, ResearchError,
    ResearchObservation, ResearchTime, RevisionNumber, SchemaVersion, SourceId, SourceIdentifier,
    Timestamp, TradeEvent, TradingHaltEvent, TradingStatus, TransactionObservation, VenueId,
};
use rust_decimal::Decimal;

fn live_provenance(instrument: bool) -> Result<Provenance, Box<dyn Error>> {
    Provenance::new(
        SchemaVersion::CURRENT,
        SourceId::try_from("direct-feed")?,
        if instrument {
            Some(InstrumentId::from_str(
                "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
            )?)
        } else {
            None
        },
        Some(VenueId::try_from("XNYS")?),
        SourceIdentifier::try_from("trade-7")?,
        Some(Timestamp::from_unix_nanos(100)),
        Timestamp::from_unix_nanos(110),
        Timestamp::from_unix_nanos(110),
        Timestamp::from_unix_nanos(120),
        DataQuality::DirectVerified,
        PayloadReference::SourceReference(SourceIdentifier::try_from("capture:7")?),
    )
    .map_err(Into::into)
}

fn research_context(instrument: bool) -> Result<ResearchContext, Box<dyn Error>> {
    ResearchContext::new(
        live_provenance(instrument)?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(90),
            Some(Timestamp::from_unix_nanos(100)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )
    .map_err(Into::into)
}

#[test]
fn trade_event_requires_positive_quantity_and_live_identity() -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        TradeEvent::new(
            live_provenance(true)?,
            PriceTicks::new(10_000),
            QuantityLots::new(0)?,
            AggressorSide::Buy,
        ),
        Err(MarketEventError::ZeroQuantity)
    ));
    assert!(matches!(
        TradeEvent::new(
            live_provenance(false)?,
            PriceTicks::new(10_000),
            QuantityLots::new(1)?,
            AggressorSide::Buy,
        ),
        Err(MarketEventError::MissingInstrument)
    ));
    Ok(())
}

#[test]
fn quote_rejects_a_crossed_market() -> Result<(), Box<dyn Error>> {
    let bid = BookLevel::new(PriceTicks::new(101), QuantityLots::new(5)?)?;
    let ask = BookLevel::new(PriceTicks::new(100), QuantityLots::new(5)?)?;

    assert!(matches!(
        QuoteEvent::new(live_provenance(true)?, Some(bid), Some(ask)),
        Err(MarketEventError::CrossedMarket)
    ));
    Ok(())
}

#[test]
fn snapshot_requires_canonical_side_ordering() -> Result<(), Box<dyn Error>> {
    let bids = vec![
        BookLevel::new(PriceTicks::new(99), QuantityLots::new(1)?)?,
        BookLevel::new(PriceTicks::new(100), QuantityLots::new(1)?)?,
    ];
    let result = BookSnapshotEvent::new(
        live_provenance(true)?,
        MarketDepth::PriceLevel,
        bids,
        Vec::new(),
        None,
    );

    assert!(matches!(
        result,
        Err(MarketEventError::InvalidBookOrdering {
            side: MarketSide::Bid
        })
    ));
    Ok(())
}

#[test]
fn delta_cannot_be_an_empty_marker_payload() -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        BookDeltaEvent::new(
            live_provenance(true)?,
            MarketDepth::PriceLevel,
            Vec::new(),
            None,
        ),
        Err(MarketEventError::EmptyBookDelta)
    ));
    Ok(())
}

#[test]
fn canonical_market_family_is_serializable() -> Result<(), Box<dyn Error>> {
    let event = MarketEvent::Trade(TradeEvent::new(
        live_provenance(true)?,
        PriceTicks::new(10_000),
        QuantityLots::new(3)?,
        AggressorSide::Sell,
    )?);

    let wire = serde_json::to_string(&event)?;
    let decoded: MarketEvent = serde_json::from_str(&wire)?;
    assert_eq!(decoded, event);
    Ok(())
}

#[test]
fn market_payload_fields_are_available_through_typed_views() -> Result<(), Box<dyn Error>> {
    let provenance = live_provenance(true)?;
    let trade = TradeEvent::new(
        provenance.clone(),
        PriceTicks::new(100),
        QuantityLots::new(2)?,
        AggressorSide::Buy,
    )?;
    let quote = QuoteEvent::new(
        provenance.clone(),
        Some(BookLevel::new(PriceTicks::new(99), QuantityLots::new(1)?)?),
        None,
    )?;
    let snapshot = BookSnapshotEvent::new(
        provenance.clone(),
        MarketDepth::PriceLevel,
        Vec::new(),
        Vec::new(),
        None,
    )?;
    let delta = BookDeltaEvent::new(
        provenance.clone(),
        MarketDepth::PriceLevel,
        vec![market_squawk_domain::BookChange::new(
            MarketSide::Bid,
            PriceTicks::new(99),
            QuantityLots::new(0)?,
        )],
        None,
    )?;
    let auction = AuctionEvent::new(
        provenance.clone(),
        AuctionPhase::Opening,
        Some(PriceTicks::new(100)),
        QuantityLots::new(4)?,
    )?;
    let halt = TradingHaltEvent::new(
        provenance.clone(),
        HaltTransition::Halted,
        SourceIdentifier::try_from("LUDP")?,
    )?;
    let status = InstrumentStatusEvent::new(provenance.clone(), TradingStatus::Halted)?;
    let action = CorporateActionEvent::new(
        provenance,
        Timestamp::from_unix_nanos(500),
        CorporateActionKind::Delisting,
    )?;

    assert_eq!(trade.aggressor_side(), AggressorSide::Buy);
    assert_eq!(quote.provenance().quality(), DataQuality::DirectVerified);
    assert_eq!(snapshot.depth(), MarketDepth::PriceLevel);
    assert_eq!(snapshot.sequence(), None);
    assert_eq!(delta.depth(), MarketDepth::PriceLevel);
    assert_eq!(delta.sequence(), None);
    assert_eq!(auction.phase(), AuctionPhase::Opening);
    assert_eq!(auction.indicative_price(), Some(PriceTicks::new(100)));
    assert_eq!(auction.paired_quantity(), QuantityLots::new(4)?);
    assert_eq!(halt.transition(), HaltTransition::Halted);
    assert_eq!(halt.reason().as_str(), "LUDP");
    assert_eq!(status.status(), TradingStatus::Halted);
    assert_eq!(action.effective_at(), Timestamp::from_unix_nanos(500));
    assert!(matches!(action.action(), CorporateActionKind::Delisting));
    Ok(())
}

#[test]
fn canonical_research_family_has_non_marker_payloads() -> Result<(), Box<dyn Error>> {
    let filing = ResearchObservation::Filing(FilingObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("10-K")?,
        SourceIdentifier::try_from("0000320193-26-000001")?,
    )?);
    let fundamental = ResearchObservation::Fundamental(FundamentalObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("Revenue")?,
        Decimal::new(1_234, 0),
        SourceIdentifier::try_from("USD")?,
    )?);
    let macro_observation = ResearchObservation::Macro(MacroObservation::new(
        research_context(false)?,
        SourceIdentifier::try_from("GDP")?,
        Decimal::new(25_000, 0),
        SourceIdentifier::try_from("billions_usd")?,
    ));
    let position = ResearchObservation::PortfolioPosition(PositionObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("brokerage-1")?,
        PositionSide::Short,
        QuantityLots::new(5)?,
    )?);
    let transaction = ResearchObservation::Transaction(TransactionObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("brokerage-1")?,
        SourceIdentifier::try_from("buy")?,
        SourceIdentifier::try_from("broker-record-7")?,
    ));
    let corporate_action = ResearchObservation::CorporateAction(CorporateActionObservation::new(
        research_context(true)?,
        CorporateActionKind::Delisting,
    )?);
    let alternative = ResearchObservation::AlternativeData(AlternativeDataObservation::new(
        research_context(false)?,
        SourceIdentifier::try_from("foot-traffic")?,
        SourceIdentifier::try_from("weekly_index")?,
        Decimal::new(1_035, 3),
        None,
    ));

    for observation in [
        filing,
        fundamental,
        macro_observation,
        position,
        transaction,
        corporate_action,
        alternative,
    ] {
        let wire = serde_json::to_string(&observation)?;
        let decoded: ResearchObservation = serde_json::from_str(&wire)?;
        assert_eq!(decoded, observation);
    }
    Ok(())
}

#[test]
fn research_instrument_payloads_reject_missing_identity() -> Result<(), Box<dyn Error>> {
    assert!(matches!(
        FilingObservation::new(
            research_context(false)?,
            SourceIdentifier::try_from("10-K")?,
            SourceIdentifier::try_from("0000320193-26-000001")?,
        ),
        Err(ResearchError::MissingInstrument)
    ));
    assert!(matches!(
        PositionObservation::new(
            research_context(true)?,
            SourceIdentifier::try_from("brokerage-1")?,
            PositionSide::Long,
            QuantityLots::new(0)?,
        ),
        Err(ResearchError::ZeroPosition)
    ));
    Ok(())
}

#[test]
fn research_payload_fields_are_available_through_typed_views() -> Result<(), Box<dyn Error>> {
    let filing = FilingObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("10-Q")?,
        SourceIdentifier::try_from("accession-1")?,
    )?;
    let fundamental = FundamentalObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("Assets")?,
        Decimal::new(42, 0),
        SourceIdentifier::try_from("USD")?,
    )?;
    let macro_observation = MacroObservation::new(
        research_context(false)?,
        SourceIdentifier::try_from("CPI")?,
        Decimal::new(321, 1),
        SourceIdentifier::try_from("index")?,
    );
    let position = PositionObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("account-1")?,
        PositionSide::Long,
        QuantityLots::new(9)?,
    )?;
    let transaction = TransactionObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("account-1")?,
        SourceIdentifier::try_from("sell")?,
        SourceIdentifier::try_from("transaction-1")?,
    );
    let corporate_action =
        CorporateActionObservation::new(research_context(true)?, CorporateActionKind::Delisting)?;
    let alternative = AlternativeDataObservation::new(
        research_context(false)?,
        SourceIdentifier::try_from("satellite")?,
        SourceIdentifier::try_from("capacity")?,
        Decimal::new(78, 1),
        Some(SourceIdentifier::try_from("percent")?),
    );

    assert_eq!(filing.form_type().as_str(), "10-Q");
    assert_eq!(filing.accession().as_str(), "accession-1");
    assert_eq!(fundamental.concept().as_str(), "Assets");
    assert_eq!(fundamental.unit().as_str(), "USD");
    assert_eq!(macro_observation.series().as_str(), "CPI");
    assert_eq!(macro_observation.value(), Decimal::new(321, 1));
    assert_eq!(position.account_id().as_str(), "account-1");
    assert_eq!(transaction.transaction_type().as_str(), "sell");
    assert_eq!(transaction.source_record_id().as_str(), "transaction-1");
    assert!(matches!(
        corporate_action.action(),
        CorporateActionKind::Delisting
    ));
    assert_eq!(alternative.dataset().as_str(), "satellite");
    assert_eq!(alternative.field().as_str(), "capacity");
    assert_eq!(
        alternative.unit().map(SourceIdentifier::as_str),
        Some("percent")
    );
    Ok(())
}
