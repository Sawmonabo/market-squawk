use std::error::Error;
use std::num::NonZeroU32;
use std::str::FromStr;

use market_squawk_domain::{
    AggressorSide, AlternativeDataObservation, AuctionEvent, AuctionPhase, AvailabilityEvidence,
    BookDeltaEvent, BookLevel, BookSnapshotEvent, CalendarDate, CorporateActionEvent,
    CorporateActionKind, CorporateActionObservation, CoverageStatus, Currency, DataQuality,
    DecodedLiveProvenanceInput, DigestAlgorithm, EvidenceDigest, FilingObservation,
    FundamentalAmendmentStatus, FundamentalCadence, FundamentalConsolidation,
    FundamentalDimensionContext, FundamentalFactContext, FundamentalFactContextInput,
    FundamentalObservation, FundamentalPeriod, FundamentalRestatementStatus,
    FundamentalRevisionOrder, HaltTransition, InstrumentId, InstrumentStatusEvent, LiveEventClass,
    LiveProvenance, MacroMissingValue, MacroObservation, MarketDepth, MarketEvent,
    MarketEventError, MarketSide, MergerConsideration, Money, NormalizedPortfolioLotMethod,
    NormalizedPortfolioTransactionClass, NormalizedPortfolioTransactionError,
    NormalizedPortfolioTransactionEvidence, NormalizedPortfolioTransactionEvidenceInput,
    PayloadReference, PositionObservation, PositionSide, PriceTicks, QuantityLots, QuoteEvent,
    ResearchContext, ResearchError, ResearchObservation, ResearchProvenance,
    ResearchProvenanceInput, ResearchTemporalCoordinate, ResearchTime, RevisionNumber,
    SchemaVersion, SourceId, SourceIdentifier, Timestamp, TradeEvent, TradeTakerOrderType,
    TradingHaltEvent, TradingStatus, TransactionObservation,
};
use rust_decimal::Decimal;

fn live_provenance(event_class: LiveEventClass) -> Result<LiveProvenance, Box<dyn Error>> {
    let binding = support::live::binding(&support::live::BindingSpec {
        event_class,
        ..support::live::BindingSpec::default()
    })?;
    LiveProvenance::decoded(DecodedLiveProvenanceInput::new(
        binding,
        Some(Timestamp::from_unix_nanos(100)),
        Timestamp::from_unix_nanos(110),
        Timestamp::from_unix_nanos(115),
        Timestamp::from_unix_nanos(120),
        DataQuality::DirectUnverified,
        CoverageStatus::Sufficient,
        PayloadReference::SourceReference(SourceIdentifier::try_from("capture:7")?),
    ))
    .map_err(Into::into)
}

fn research_context(instrument: bool) -> Result<ResearchContext, Box<dyn Error>> {
    ResearchContext::new(
        ResearchProvenance::try_new(ResearchProvenanceInput {
            source_id: SourceId::try_from("historical-file")?,
            instrument_id: if instrument {
                Some(InstrumentId::from_str(
                    "0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb",
                )?)
            } else {
                None
            },
            venue_id: None,
            source_identifier: SourceIdentifier::try_from("record-7")?,
            source_timestamp: None,
            received_at: Timestamp::from_unix_nanos(110),
            ingested_at: Timestamp::from_unix_nanos(120),
            quality: DataQuality::OfficialDelayed,
            payload_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
                "fixture:7",
            )?),
            availability: AvailabilityEvidence::evidenced(
                Timestamp::from_unix_nanos(100),
                SourceIdentifier::try_from("release-calendar")?,
            ),
        })?,
        ResearchTime::new(
            Timestamp::from_unix_nanos(90),
            Some(Timestamp::from_unix_nanos(100)),
            RevisionNumber::new(1)?,
            None,
        )?,
    )
    .map_err(Into::into)
}

fn fundamental_fixture() -> Result<(ResearchContext, FundamentalFactContext), Box<dyn Error>> {
    let start = CalendarDate::new(2025, 1, 1)?;
    let end = CalendarDate::new(2025, 12, 31)?;
    let revision = RevisionNumber::new(1)?;
    let context = ResearchContext::new(
        research_context(true)?.provenance().clone(),
        ResearchTime::try_new_with_coordinates(
            ResearchTemporalCoordinate::calendar_date(end),
            None,
            revision,
            None,
        )?,
    )?;
    let fact_context = FundamentalFactContext::try_new(FundamentalFactContextInput {
        schema_version: SchemaVersion::CURRENT,
        period: FundamentalPeriod::duration(start, end)?,
        unit: SourceIdentifier::try_from("USD")?,
        accession: SourceIdentifier::try_from("record-7")?,
        filing_form: None,
        amendment_status: FundamentalAmendmentStatus::Unavailable,
        filed_on: None,
        frame: None,
        fiscal_year: None,
        fiscal_period: None,
        cadence: FundamentalCadence::Unavailable,
        xbrl_context_id: None,
        dimensions: FundamentalDimensionContext::unavailable(),
        consolidation: FundamentalConsolidation::Unavailable,
        revision_order: FundamentalRevisionOrder::new(
            revision,
            SourceIdentifier::try_from("historical-file-order-v1")?,
        ),
        restatement_status: FundamentalRestatementStatus::Unavailable,
    })?;
    Ok((context, fact_context))
}

#[test]
fn trade_event_requires_positive_quantity_and_matching_live_identity() -> Result<(), Box<dyn Error>>
{
    assert!(matches!(
        TradeEvent::new(
            live_provenance(LiveEventClass::Trade)?,
            PriceTicks::new(10_000),
            QuantityLots::new(0)?,
            AggressorSide::Buy,
            None,
        ),
        Err(MarketEventError::ZeroQuantity)
    ));
    assert!(matches!(
        TradeEvent::new(
            live_provenance(LiveEventClass::Quote)?,
            PriceTicks::new(10_000),
            QuantityLots::new(1)?,
            AggressorSide::Buy,
            None,
        ),
        Err(MarketEventError::ProvenanceEventClassMismatch)
    ));
    Ok(())
}

#[test]
fn quote_rejects_a_crossed_market() -> Result<(), Box<dyn Error>> {
    let bid = BookLevel::new(PriceTicks::new(101), QuantityLots::new(5)?)?;
    let ask = BookLevel::new(PriceTicks::new(100), QuantityLots::new(5)?)?;

    assert!(matches!(
        QuoteEvent::new(
            live_provenance(LiveEventClass::Quote)?,
            Some(bid),
            Some(ask)
        ),
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
        live_provenance(LiveEventClass::BookSnapshot)?,
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
            live_provenance(LiveEventClass::BookDelta)?,
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
        live_provenance(LiveEventClass::Trade)?,
        PriceTicks::new(10_000),
        QuantityLots::new(3)?,
        AggressorSide::Sell,
        Some(TradeTakerOrderType::Market),
    )?);

    let wire = serde_json::to_string(&event)?;
    let decoded: MarketEvent = serde_json::from_str(&wire)?;
    assert_eq!(decoded, event);
    Ok(())
}

#[test]
fn market_payload_fields_are_available_through_typed_views() -> Result<(), Box<dyn Error>> {
    let trade = TradeEvent::new(
        live_provenance(LiveEventClass::Trade)?,
        PriceTicks::new(100),
        QuantityLots::new(2)?,
        AggressorSide::Buy,
        Some(TradeTakerOrderType::Limit),
    )?;
    let quote = QuoteEvent::new(
        live_provenance(LiveEventClass::Quote)?,
        Some(BookLevel::new(PriceTicks::new(99), QuantityLots::new(1)?)?),
        None,
    )?;
    let snapshot = BookSnapshotEvent::new(
        live_provenance(LiveEventClass::BookSnapshot)?,
        MarketDepth::PriceLevel,
        Vec::new(),
        Vec::new(),
        None,
    )?;
    let delta = BookDeltaEvent::new(
        live_provenance(LiveEventClass::BookDelta)?,
        MarketDepth::PriceLevel,
        vec![market_squawk_domain::BookChange::new(
            MarketSide::Bid,
            PriceTicks::new(99),
            QuantityLots::new(0)?,
        )],
        None,
    )?;
    let auction = AuctionEvent::new(
        live_provenance(LiveEventClass::Auction)?,
        AuctionPhase::Opening,
        Some(PriceTicks::new(100)),
        QuantityLots::new(4)?,
    )?;
    let halt = TradingHaltEvent::new(
        live_provenance(LiveEventClass::TradingHalt)?,
        HaltTransition::Halted,
        SourceIdentifier::try_from("LUDP")?,
    )?;
    let status = InstrumentStatusEvent::new(
        live_provenance(LiveEventClass::InstrumentStatus)?,
        TradingStatus::Halted,
    )?;
    let action = CorporateActionEvent::new(
        live_provenance(LiveEventClass::CorporateAction)?,
        Timestamp::from_unix_nanos(500),
        CorporateActionKind::Delisting,
    )?;

    assert_eq!(trade.aggressor_side(), AggressorSide::Buy);
    assert_eq!(trade.taker_order_type(), Some(TradeTakerOrderType::Limit));
    assert_eq!(quote.provenance().quality(), DataQuality::DirectUnverified);
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
    let (fundamental_context, fact_context) = fundamental_fixture()?;
    let fundamental = ResearchObservation::Fundamental(FundamentalObservation::new(
        fundamental_context,
        SourceIdentifier::try_from("Revenue")?,
        Decimal::new(1_234, 0),
        fact_context,
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
fn normalized_portfolio_transaction_evidence_binds_raw_lineage_and_economic_scalars()
-> Result<(), Box<dyn Error>> {
    let account_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse()?;
    let instrument_id = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?;
    let payload_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, [42; 32]);
    let evidence = NormalizedPortfolioTransactionEvidence::try_new(
        NormalizedPortfolioTransactionEvidenceInput {
            source_id: SourceId::try_from("portfolio-file")?,
            logical_record_id: SourceIdentifier::try_from("record-7")?,
            source_revision: SourceIdentifier::try_from("statement-2")?,
            supersedes_source_revision: Some(SourceIdentifier::try_from("statement-1")?),
            revision: RevisionNumber::new(2)?,
            raw_source_reference: SourceIdentifier::try_from("portfolio-raw-7")?,
            raw_payload_digest: payload_digest,
            broker_transaction_id: SourceIdentifier::try_from("broker-transaction-7")?,
            account_id,
            instrument_id: Some(instrument_id),
            classification: NormalizedPortfolioTransactionClass::Trade,
            amount: Money::new(Decimal::from(-50), Currency::try_from("USD")?),
            quantity: Some(Decimal::new(-5, 1)),
            occurred_at: Timestamp::from_unix_nanos(99),
            lot_method: Some(NormalizedPortfolioLotMethod::Fifo),
        },
    )?;

    assert_eq!(evidence.source_id().as_str(), "portfolio-file");
    assert_eq!(evidence.logical_record_id().as_str(), "record-7");
    assert_eq!(evidence.source_revision().as_str(), "statement-2");
    assert_eq!(
        evidence
            .supersedes_source_revision()
            .ok_or("supersession absent")?
            .as_str(),
        "statement-1"
    );
    assert_eq!(evidence.revision(), RevisionNumber::new(2)?);
    assert_eq!(evidence.raw_payload_digest(), payload_digest);
    assert_eq!(evidence.account_id(), account_id);
    assert_eq!(evidence.instrument_id(), Some(instrument_id));
    assert_eq!(evidence.amount().amount(), Decimal::from(-50));
    assert_eq!(evidence.quantity(), Some(Decimal::new(-5, 1)));
    assert_eq!(
        evidence.lot_method(),
        Some(NormalizedPortfolioLotMethod::Fifo)
    );

    let invalid = NormalizedPortfolioTransactionEvidence::try_new(
        NormalizedPortfolioTransactionEvidenceInput {
            source_id: SourceId::try_from("portfolio-file")?,
            logical_record_id: SourceIdentifier::try_from("record-8")?,
            source_revision: SourceIdentifier::try_from("statement-1")?,
            supersedes_source_revision: None,
            revision: RevisionNumber::new(1)?,
            raw_source_reference: SourceIdentifier::try_from("portfolio-raw-8")?,
            raw_payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, [43; 32]),
            broker_transaction_id: SourceIdentifier::try_from("broker-transaction-8")?,
            account_id,
            instrument_id: None,
            classification: NormalizedPortfolioTransactionClass::CashTransfer,
            amount: Money::new(Decimal::ONE, Currency::try_from("USD")?),
            quantity: Some(Decimal::ONE),
            occurred_at: Timestamp::from_unix_nanos(100),
            lot_method: None,
        },
    );
    assert_eq!(
        invalid,
        Err(NormalizedPortfolioTransactionError::InvalidFieldCombination)
    );
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
fn corporate_action_economic_terms_are_exact_and_validated() -> Result<(), Box<dyn Error>> {
    let subject = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cb")?;
    let related = InstrumentId::from_str("0187f5f1-6fc2-7fa2-bf05-2ce5354c55cc")?;
    let one = NonZeroU32::try_from(1_u32)?;
    let two = NonZeroU32::try_from(2_u32)?;
    let three = NonZeroU32::try_from(3_u32)?;
    let usd = Currency::try_from("USD")?;
    let cash = Money::new(Decimal::new(125, 2), usd);

    let valid_actions = [
        CorporateActionKind::Split {
            numerator: two,
            denominator: one,
        },
        CorporateActionKind::CashDividend { amount: cash },
        CorporateActionKind::Spinoff {
            distributed_instrument: related,
            numerator: one,
            denominator: three,
        },
        CorporateActionKind::ReturnOfCapital { amount: cash },
        CorporateActionKind::Merger {
            successor: related,
            consideration: MergerConsideration::Stock {
                numerator: two,
                denominator: three,
            },
        },
        CorporateActionKind::Merger {
            successor: related,
            consideration: MergerConsideration::Cash { amount: cash },
        },
        CorporateActionKind::Merger {
            successor: related,
            consideration: MergerConsideration::Mixed {
                numerator: one,
                denominator: two,
                cash,
            },
        },
    ];

    for action in valid_actions {
        let live = CorporateActionEvent::new(
            live_provenance(LiveEventClass::CorporateAction)?,
            Timestamp::from_unix_nanos(500),
            action.clone(),
        )?;
        assert_eq!(
            serde_json::from_value::<CorporateActionEvent>(serde_json::to_value(&live)?)?,
            live
        );

        let research = CorporateActionObservation::new(research_context(true)?, action)?;
        assert_eq!(
            serde_json::from_value::<CorporateActionObservation>(serde_json::to_value(&research)?)?,
            research
        );
    }

    let legacy_merger: CorporateActionKind = serde_json::from_value(serde_json::json!({
        "kind": "merger",
        "successor": related,
    }))?;
    assert_eq!(
        legacy_merger,
        CorporateActionKind::Merger {
            successor: related,
            consideration: MergerConsideration::Unspecified,
        }
    );

    assert!(matches!(
        CorporateActionEvent::new(
            live_provenance(LiveEventClass::CorporateAction)?,
            Timestamp::from_unix_nanos(500),
            CorporateActionKind::Merger {
                successor: subject,
                consideration: MergerConsideration::Unspecified,
            },
        ),
        Err(MarketEventError::SelfMerger)
    ));
    assert!(matches!(
        CorporateActionObservation::new(
            research_context(true)?,
            CorporateActionKind::Spinoff {
                distributed_instrument: subject,
                numerator: one,
                denominator: two,
            },
        ),
        Err(ResearchError::SelfSpinoff)
    ));

    for invalid in [
        CorporateActionKind::CashDividend {
            amount: Money::new(Decimal::ZERO, usd),
        },
        CorporateActionKind::ReturnOfCapital {
            amount: Money::new(Decimal::NEGATIVE_ONE, usd),
        },
        CorporateActionKind::Merger {
            successor: related,
            consideration: MergerConsideration::Cash {
                amount: Money::new(Decimal::ZERO, usd),
            },
        },
        CorporateActionKind::Merger {
            successor: related,
            consideration: MergerConsideration::Mixed {
                numerator: one,
                denominator: two,
                cash: Money::new(Decimal::NEGATIVE_ONE, usd),
            },
        },
    ] {
        assert!(matches!(
            CorporateActionEvent::new(
                live_provenance(LiveEventClass::CorporateAction)?,
                Timestamp::from_unix_nanos(500),
                invalid.clone(),
            ),
            Err(MarketEventError::NonPositiveCorporateActionAmount)
        ));
        assert!(matches!(
            CorporateActionObservation::new(research_context(true)?, invalid),
            Err(ResearchError::NonPositiveCorporateActionAmount)
        ));
    }

    let valid_live = CorporateActionEvent::new(
        live_provenance(LiveEventClass::CorporateAction)?,
        Timestamp::from_unix_nanos(500),
        CorporateActionKind::CashDividend { amount: cash },
    )?;
    let mut invalid_live = serde_json::to_value(valid_live)?;
    invalid_live["action"]["amount"] = serde_json::to_value(Money::new(Decimal::ZERO, usd))?;
    assert!(serde_json::from_value::<CorporateActionEvent>(invalid_live).is_err());

    let valid_research = CorporateActionObservation::new(
        research_context(true)?,
        CorporateActionKind::ReturnOfCapital { amount: cash },
    )?;
    let mut invalid_research = serde_json::to_value(valid_research)?;
    invalid_research["action"]["amount"] =
        serde_json::to_value(Money::new(Decimal::NEGATIVE_ONE, usd))?;
    assert!(serde_json::from_value::<CorporateActionObservation>(invalid_research).is_err());

    Ok(())
}

#[test]
fn research_payload_fields_are_available_through_typed_views() -> Result<(), Box<dyn Error>> {
    let filing = FilingObservation::new(
        research_context(true)?,
        SourceIdentifier::try_from("10-Q")?,
        SourceIdentifier::try_from("accession-1")?,
    )?;
    let (fundamental_context, fact_context) = fundamental_fixture()?;
    let fundamental = FundamentalObservation::new(
        fundamental_context,
        SourceIdentifier::try_from("Assets")?,
        Decimal::new(42, 0),
        fact_context,
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
    assert_eq!(
        macro_observation.value().observed_value(),
        Some(Decimal::new(321, 1))
    );
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

#[test]
fn macro_missing_values_preserve_provider_marker_and_reason() -> Result<(), Box<dyn Error>> {
    let missing = MacroMissingValue::new(
        SourceIdentifier::try_from("-")?,
        Some(SourceIdentifier::try_from("not-reported")?),
    );
    let observation = MacroObservation::missing(
        research_context(false)?,
        SourceIdentifier::try_from("CPI")?,
        missing.clone(),
        SourceIdentifier::try_from("index")?,
    );
    let wire = serde_json::to_value(&observation)?;

    assert!(wire.get("value").is_none());
    assert_eq!(wire["missing"]["marker"], "-");
    assert_eq!(wire["missing"]["reason"], "not-reported");
    assert_eq!(
        serde_json::from_value::<MacroObservation>(wire)?,
        observation
    );
    assert_eq!(observation.value().missing_value(), Some(&missing));

    let observed = MacroObservation::new(
        research_context(false)?,
        SourceIdentifier::try_from("CPI")?,
        Decimal::new(321, 1),
        SourceIdentifier::try_from("index")?,
    );
    let observed_wire = serde_json::to_value(&observed)?;
    assert!(observed_wire.get("missing").is_none());
    assert_eq!(
        serde_json::from_value::<MacroObservation>(observed_wire.clone())?,
        observed
    );
    let context = observed_wire["context"].clone();
    for invalid in [
        serde_json::json!({
            "context": context.clone(),
            "series": "CPI",
            "value": null,
            "missing": {"marker": "-"},
            "unit": "index"
        }),
        serde_json::json!({
            "context": context.clone(),
            "series": "CPI",
            "value": 32.1,
            "missing": null,
            "unit": "index"
        }),
        serde_json::json!({
            "context": context.clone(),
            "series": "CPI",
            "value": 32.1,
            "missing": {"marker": "-"},
            "unit": "index"
        }),
        serde_json::json!({
            "context": context,
            "series": "CPI",
            "unit": "index"
        }),
    ] {
        assert!(serde_json::from_value::<MacroObservation>(invalid).is_err());
    }
    Ok(())
}
use crate::support;
