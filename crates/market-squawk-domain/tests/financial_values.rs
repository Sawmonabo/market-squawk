use std::str::FromStr;

use market_squawk_domain::{
    AssetClass, AssignmentVerification, BasisPoints, CalendarDate, ChainAddress, ChainAddressRole,
    ChainId, ConnectionGeneration, ContractRollMapping, CryptoPair, CryptoProductType, Currency,
    Cusip, Denomination, EffectiveInterval, EvidenceDigest, EvmChainId, ExternalIdentifier,
    ExternalIdentifierRecord, ExternalIdentifierRecordInput, Figi, FinancialError,
    FuturesContractIdentity, FuturesContractIdentityInput, FuturesLifecycleDateFields,
    FuturesLifecycleDates, FuturesSecurityType, IdentifierEntitlement, IdentifierError,
    IdentifierRightsPolicyReference, IdentityError, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentError, InstrumentId, Isin, LifecycleTransition,
    LifecycleTransitionKind, LotSize, MaturityMonthYear, MetadataRevision, Money,
    OccOptionIdentity, OptionKind, PayloadHashAlgorithm, PayloadReference, PriceError, PriceTicks,
    ProviderIdentityEvidence, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderInstrumentId, QuantityError, QuantityLots, RoundingPolicy, Sedol, SequenceNumber,
    SolanaChainId, SourceId, SourceIdentifier, SymbolIdentityRecord, TickSize, Ticker, TimeError,
    Timestamp, TradingStatus, VenueId, VenueMapping, VenueSymbol,
};
use rust_decimal::Decimal;
use uuid::Uuid;

fn provider_evidence(byte: u8) -> ProviderIdentityEvidence {
    ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
        PayloadHashAlgorithm::Sha256,
        [byte; 32],
    ))
}

#[test]
fn instrument_id_round_trips_uuid_text_and_serde() -> Result<(), Box<dyn std::error::Error>> {
    let uuid = Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8")?;
    let id = InstrumentId::try_from(uuid)?;

    assert_eq!(id.as_uuid(), uuid);
    assert_eq!(InstrumentId::from_str(&id.to_string())?, id);
    let encoded = serde_json::to_string(&id)?;
    assert_eq!(serde_json::from_str::<InstrumentId>(&encoded)?, id);
    Ok(())
}

#[test]
fn instrument_id_rejects_nil_uuid() {
    assert_eq!(
        InstrumentId::try_from(Uuid::nil()),
        Err(IdentityError::NilUuid)
    );
}

#[test]
fn venue_id_is_nonempty_bounded_and_validated_during_deserialization()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(VenueId::try_from(""), Err(IdentityError::Empty));
    assert_eq!(VenueId::try_from("  "), Err(IdentityError::Empty));

    let boundary = "v".repeat(VenueId::MAX_LENGTH);
    assert_eq!(VenueId::try_from(boundary.as_str())?.as_str(), boundary);

    let oversized = "v".repeat(VenueId::MAX_LENGTH + 1);
    assert_eq!(
        VenueId::try_from(oversized.as_str()),
        Err(IdentityError::TooLong {
            max: VenueId::MAX_LENGTH,
        })
    );
    assert!(serde_json::from_str::<VenueId>("\"\"").is_err());
    Ok(())
}

#[test]
fn source_side_identifiers_are_bounded_and_borrowable() -> Result<(), Box<dyn std::error::Error>> {
    let source = SourceId::try_from("coinbase")?;
    let provider_instrument = ProviderInstrumentId::try_from("BTC-USD")?;
    let source_identifier = SourceIdentifier::try_from("channel:ticker:BTC-USD")?;

    assert_eq!(source.as_str(), "coinbase");
    assert_eq!(provider_instrument.as_str(), "BTC-USD");
    assert_eq!(source_identifier.as_str(), "channel:ticker:BTC-USD");
    assert_eq!(source.to_string(), "coinbase");

    let oversized = "x".repeat(SourceIdentifier::MAX_LENGTH + 1);
    assert_eq!(
        SourceIdentifier::try_from(oversized),
        Err(IdentityError::TooLong {
            max: SourceIdentifier::MAX_LENGTH,
        })
    );
    Ok(())
}

#[test]
fn sequence_and_connection_counters_are_checked() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(SequenceNumber::new(0).get(), 0);
    assert_eq!(
        SequenceNumber::new(u64::MAX).checked_next(),
        Err(IdentityError::CounterOverflow)
    );

    assert_eq!(
        ConnectionGeneration::new(0),
        Err(IdentityError::ZeroGeneration)
    );
    let first = ConnectionGeneration::new(1)?;
    assert_eq!(first.get(), 1);
    assert_eq!(first.checked_next()?.get(), 2);
    assert_eq!(
        ConnectionGeneration::new(u64::MAX)?.checked_next(),
        Err(IdentityError::CounterOverflow)
    );
    assert!(serde_json::from_str::<ConnectionGeneration>("0").is_err());
    Ok(())
}

#[test]
fn tick_and_lot_sizes_must_be_positive_and_are_normalized() -> Result<(), Box<dyn std::error::Error>>
{
    assert_eq!(
        TickSize::try_from_decimal(Decimal::ZERO),
        Err(FinancialError::NonPositiveIncrement)
    );
    assert_eq!(
        LotSize::try_from_decimal(Decimal::new(-1, 2)),
        Err(FinancialError::NonPositiveIncrement)
    );

    let tick = TickSize::try_from_decimal(Decimal::new(500, 4))?;
    let lot = LotSize::try_from_decimal(Decimal::new(1_000, 3))?;
    assert_eq!(tick.as_decimal(), Decimal::new(5, 2));
    assert_eq!(lot.as_decimal(), Decimal::ONE);
    Ok(())
}

#[test]
fn price_rejects_fractional_tick() -> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::new(5, 2))?;
    let result = PriceTicks::try_from_decimal(Decimal::new(102, 2), tick);
    assert_eq!(result, Err(PriceError::InexactTick));
    Ok(())
}

#[test]
fn exact_price_and_quantity_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::new(5, 2))?;
    let price = PriceTicks::try_from_decimal(Decimal::new(510, 2), tick)?;
    assert_eq!(price, PriceTicks::new(102));
    assert_eq!(price.checked_to_decimal(tick)?, Decimal::new(510, 2));

    let lot = LotSize::try_from_decimal(Decimal::new(25, 1))?;
    let quantity = QuantityLots::try_from_decimal(Decimal::new(10, 0), lot)?;
    assert_eq!(quantity.get(), 4);
    assert_eq!(quantity.checked_to_decimal(lot)?, Decimal::new(10, 0));
    Ok(())
}

#[test]
fn quantity_rejects_negative_and_fractional_lots() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(QuantityLots::new(-1), Err(QuantityError::NegativeQuantity));
    let lot = LotSize::try_from_decimal(Decimal::new(25, 1))?;
    assert_eq!(
        QuantityLots::try_from_decimal(Decimal::new(11, 0), lot),
        Err(QuantityError::InexactLot)
    );
    Ok(())
}

#[test]
fn quantity_rounding_is_explicit_and_cannot_create_negative_quantity()
-> Result<(), Box<dyn std::error::Error>> {
    let lot = LotSize::try_from_decimal(Decimal::new(2, 0))?;
    assert_eq!(
        QuantityLots::from_decimal_rounded(Decimal::new(5, 0), lot, RoundingPolicy::NearestEven,)?,
        QuantityLots::new(2)?
    );
    assert_eq!(
        QuantityLots::from_decimal_rounded(Decimal::new(-1, 1), lot, RoundingPolicy::TowardZero,),
        Err(QuantityError::NegativeQuantity)
    );
    Ok(())
}

#[test]
fn rounding_requires_an_explicit_policy() -> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::new(5, 2))?;
    let value = Decimal::new(1025, 3);

    assert_eq!(
        PriceTicks::try_from_decimal(value, tick),
        Err(PriceError::InexactTick)
    );
    assert_eq!(
        PriceTicks::from_decimal_rounded(value, tick, RoundingPolicy::NearestEven)?,
        PriceTicks::new(20)
    );
    assert_eq!(
        PriceTicks::from_decimal_rounded(value, tick, RoundingPolicy::AwayFromZero)?,
        PriceTicks::new(21)
    );
    Ok(())
}

#[test]
fn scaled_integer_arithmetic_is_checked() -> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(
        PriceTicks::new(i64::MAX).checked_add(PriceTicks::new(1)),
        Err(PriceError::Overflow)
    );
    assert_eq!(
        PriceTicks::new(i64::MIN).checked_sub(PriceTicks::new(1)),
        Err(PriceError::Overflow)
    );

    let maximum = QuantityLots::new(i64::MAX)?;
    assert_eq!(
        maximum.checked_add(QuantityLots::new(1)?),
        Err(QuantityError::Overflow)
    );
    assert_eq!(
        QuantityLots::new(0)?.checked_sub(QuantityLots::new(1)?),
        Err(QuantityError::NegativeQuantity)
    );
    Ok(())
}

#[test]
fn currency_is_normalized_and_money_requires_matching_currency()
-> Result<(), Box<dyn std::error::Error>> {
    let usd = Currency::try_from("usd")?;
    let eur = Currency::try_from("EUR")?;
    assert_eq!(usd.as_str(), "USD");
    assert_eq!(
        Currency::try_from("US"),
        Err(FinancialError::InvalidCurrency)
    );

    let left = Money::new(Decimal::new(100, 2), usd);
    let right = Money::new(Decimal::new(250, 2), usd);
    assert_eq!(left.checked_add(right)?.amount(), Decimal::new(350, 2));
    assert_eq!(right.checked_sub(left)?.amount(), Decimal::new(150, 2));
    assert_eq!(
        left.checked_add(Money::new(Decimal::ONE, eur)),
        Err(FinancialError::CurrencyMismatch {
            left: usd,
            right: eur,
        })
    );
    assert_eq!(
        Money::new(Decimal::MAX, usd).checked_add(Money::new(Decimal::ONE, usd)),
        Err(FinancialError::Overflow)
    );
    assert_eq!(
        Money::new(Decimal::MAX, usd)
            .checked_add(Money::new(Decimal::try_new(1, Decimal::MAX_SCALE)?, usd)),
        Err(FinancialError::Overflow)
    );
    Ok(())
}

#[test]
fn money_wire_is_exact_and_rejects_nested_unknown_fields() -> Result<(), Box<dyn std::error::Error>>
{
    let money = Money::new(Decimal::new(12_345, 2), Currency::try_from("USD")?);
    let canonical = serde_json::to_value(money)?;
    assert_eq!(serde_json::from_value::<Money>(canonical.clone())?, money);

    let mut unexpected = canonical;
    unexpected["unrecognized_nested_field"] = serde_json::json!(true);
    assert!(serde_json::from_value::<Money>(unexpected).is_err());
    Ok(())
}

#[test]
fn basis_points_are_exact_and_checked() -> Result<(), Box<dyn std::error::Error>> {
    let one_percent = BasisPoints::new(100);
    assert_eq!(one_percent.get(), 100);
    assert_eq!(one_percent.as_decimal_rate(), Decimal::new(1, 2));
    assert_eq!(
        one_percent.checked_sub(BasisPoints::new(25))?,
        BasisPoints::new(75)
    );
    assert_eq!(
        BasisPoints::new(i32::MAX).checked_add(BasisPoints::new(1)),
        Err(FinancialError::Overflow)
    );
    Ok(())
}

#[test]
fn checked_price_times_quantity_produces_currency_aware_money()
-> Result<(), Box<dyn std::error::Error>> {
    let price = PriceTicks::new(102);
    let quantity = QuantityLots::new(2)?;
    let tick = TickSize::try_from_decimal(Decimal::new(5, 2))?;
    let lot = LotSize::try_from_decimal(Decimal::new(100, 0))?;
    let usd = Currency::try_from("USD")?;

    let notional = price.checked_mul_quantity(quantity, tick, lot, usd)?;
    assert_eq!(notional.amount(), Decimal::new(1_020, 0));
    assert_eq!(notional.currency(), usd);
    Ok(())
}

#[test]
fn ticker_and_venue_symbol_use_bounded_source_safe_syntax() -> Result<(), Box<dyn std::error::Error>>
{
    let ticker = Ticker::try_from("BRK.B")?;
    let venue_symbol = VenueSymbol::try_from("BTC-USD")?;
    assert_eq!(ticker.as_str(), "BRK.B");
    assert_eq!(venue_symbol.as_str(), "BTC-USD");
    assert_eq!(
        Ticker::try_from("AAPL US"),
        Err(IdentifierError::InvalidCharacter)
    );
    assert_eq!(VenueSymbol::try_from(""), Err(IdentifierError::Empty));
    Ok(())
}

#[test]
fn cusip_validates_its_type_specific_check_digit() -> Result<(), Box<dyn std::error::Error>> {
    // CGS describes the 9-character layout and Modulus 10 Double-Add-Double check:
    // https://www.cusip.com/identifiers.html?section=CUSIP
    let cusip = Cusip::try_from("023135106")?;
    assert_eq!(cusip.as_str(), "023135106");
    assert_eq!(
        Cusip::try_from("023135107"),
        Err(IdentifierError::InvalidChecksum)
    );
    assert_eq!(
        Cusip::try_from("*23135@06"),
        Err(IdentifierError::InvalidCharacter)
    );
    Ok(())
}

#[test]
fn isin_validates_iso_6166_structure_and_check_digit() -> Result<(), Box<dyn std::error::Error>> {
    // ISO TC 68 describes the 12-character structure and modulus-10 algorithm:
    // https://committee.iso.org/sites/tc68/home/articles/content-left-area/articles/what-is-isin.html
    let isin = Isin::try_from("us0378331005")?;
    assert_eq!(isin.as_str(), "US0378331005");
    assert_eq!(
        Isin::try_from("US0378331004"),
        Err(IdentifierError::InvalidChecksum)
    );
    Ok(())
}

#[test]
fn sedol_accepts_legacy_numeric_and_current_consonant_formats()
-> Result<(), Box<dyn std::error::Error>> {
    // LSEG SEDOL Masterfile Service & Technical Guide v8.8 defines both issuance formats and
    // weights [1,3,1,7,3,9,1].
    assert_eq!(Sedol::try_from("0263494")?.as_str(), "0263494");
    assert_eq!(Sedol::try_from("B123456")?.as_str(), "B123456");
    assert_eq!(
        Sedol::try_from("A123455"),
        Err(IdentifierError::InvalidCharacter)
    );
    assert_eq!(
        Sedol::try_from("B123455"),
        Err(IdentifierError::InvalidChecksum)
    );
    Ok(())
}

#[test]
fn figi_validates_x9_145_grammar_reserved_prefixes_and_check_digit()
-> Result<(), Box<dyn std::error::Error>> {
    // ANSI X9.145-2021 publishes the grammar, character-position weights, and this vector:
    // https://x9.org/wp-content/uploads/2021/08/ANSI-X9.145-2021-Financial-Instrument-Global-Identifier-FIGI.pdf
    assert_eq!(Figi::try_from("BBG000BLNQ16")?.as_str(), "BBG000BLNQ16");
    assert_eq!(
        Figi::try_from("BBG000BLNQ15"),
        Err(IdentifierError::InvalidChecksum)
    );
    assert_eq!(
        Figi::try_from("BSG000BLNQ12"),
        Err(IdentifierError::ReservedPrefix)
    );
    Ok(())
}

#[test]
fn occ_option_identity_preserves_fixed_width_and_parses_fields()
-> Result<(), Box<dyn std::error::Error>> {
    // CAT Industry Member Technical Specification v4.1.0 r15 documents the fixed 21-character
    // representation and the example used here.
    let option = OccOptionIdentity::try_from("4SPXW 230818P04418350")?;
    assert_eq!(option.as_str(), "4SPXW 230818P04418350");
    assert_eq!(option.root(), "4SPXW");
    assert_eq!(option.expiration_yy(), 23);
    assert_eq!(option.expiration_month(), 8);
    assert_eq!(option.expiration_day(), 18);
    assert_eq!(option.kind(), OptionKind::Put);
    assert_eq!(option.strike_thousandths(), 4_418_350);
    assert_eq!(
        OccOptionIdentity::try_from("4SPXW 230230P04418350"),
        Err(IdentifierError::InvalidDate)
    );
    assert_eq!(
        OccOptionIdentity::try_from("4SPXW 230818X04418350"),
        Err(IdentifierError::InvalidOptionKind)
    );
    Ok(())
}

#[test]
fn futures_identity_uses_venue_fields_instead_of_parsing_a_universal_symbol()
-> Result<(), Box<dyn std::error::Error>> {
    // CFTC/CME reference contracts keep exchange security IDs, sources, native symbols, and
    // expiry fields separate; month-letter parsing is venue metadata behavior.
    assert_eq!(
        MaturityMonthYear::month(2026, 0),
        Err(IdentifierError::InvalidDate)
    );
    let expiry = MaturityMonthYear::month(2026, 3)?;
    let lifecycle = FuturesLifecycleDates::try_new(FuturesLifecycleDateFields {
        maturity_date: Some(CalendarDate::new(2026, 3, 20)?),
        ..FuturesLifecycleDateFields::default()
    })?;
    let source_reference =
        PayloadReference::SourceReference(SourceIdentifier::try_from("security-definition:1")?);
    let contract = FuturesContractIdentity::try_new(FuturesContractIdentityInput {
        source_id: SourceId::try_from("cme-reference")?,
        source_reference,
        source_timestamp: None,
        observed_at: Timestamp::from_unix_nanos(1),
        metadata_revision: SourceIdentifier::try_from("security-definition-revision:1")?,
        venue_id: VenueId::try_from("XCME")?,
        security_id: ProviderInstrumentId::try_from("123456")?,
        security_id_source: SourceIdentifier::try_from("8")?,
        product_code: ProviderInstrumentId::try_from("ES")?,
        native_symbol: VenueSymbol::try_from("ESH6")?,
        security_type: FuturesSecurityType::Future,
        maturity_month_year: Some(expiry),
        lifecycle,
        legs: Vec::new(),
    })?;
    assert_eq!(contract.native_symbol().as_str(), "ESH6");
    assert_eq!(contract.maturity_month_year(), Some(expiry));
    Ok(())
}

#[test]
fn crypto_pair_preserves_venue_direction_and_raw_product_id()
-> Result<(), Box<dyn std::error::Error>> {
    let venue = VenueId::try_from("coinbase")?;
    let raw = ProviderInstrumentId::try_from("BTC-USD")?;
    let base = ProviderInstrumentId::try_from("BTC")?;
    let quote = ProviderInstrumentId::try_from("USD")?;
    let pair = CryptoPair::new(venue, raw, base.clone(), quote, CryptoProductType::Spot)?;
    assert_eq!(pair.base_asset_id(), &base);
    assert_eq!(pair.raw_product_id().as_str(), "BTC-USD");
    assert_eq!(pair.to_string(), "BTC-USD");
    assert_eq!(
        CryptoPair::new(
            VenueId::try_from("coinbase")?,
            ProviderInstrumentId::try_from("BTC-BTC")?,
            base.clone(),
            base,
            CryptoProductType::Spot,
        ),
        Err(IdentifierError::IdenticalPairAssets)
    );
    Ok(())
}

#[test]
fn chain_addresses_are_chain_qualified_and_protocol_specific()
-> Result<(), Box<dyn std::error::Error>> {
    // CAIP-2 chain IDs are case-sensitive. EIP-55 defines EVM checksum case; Solana RPC renders
    // 32-byte public keys as case-sensitive base58. Syntax does not establish on-chain existence.
    let ethereum = EvmChainId::try_from("eip155:1")?;
    let evm = ChainAddress::try_evm(
        ethereum.clone(),
        "0X52908400098527886E0F7030069857D2E4169EE7",
        ChainAddressRole::TokenContract,
    )?;
    assert_eq!(evm.chain_id(), ethereum.chain_id());
    assert_eq!(
        evm.canonical(),
        "0x52908400098527886e0f7030069857d2e4169ee7"
    );
    assert_eq!(evm.decoded_bytes().len(), 20);
    assert_eq!(
        ChainId::try_from("EIP155:1"),
        Err(IdentifierError::InvalidChainId)
    );
    assert_eq!(
        ChainAddress::try_evm(
            ethereum,
            "0x52908400098527886e0F7030069857D2E4169EE7",
            ChainAddressRole::Account,
        ),
        Err(IdentifierError::InvalidAddressChecksum)
    );

    let solana = ChainAddress::try_solana(
        SolanaChainId::mainnet(),
        "11111111111111111111111111111111",
        ChainAddressRole::Mint,
    )?;
    assert_eq!(solana.canonical(), "11111111111111111111111111111111");
    assert_eq!(solana.decoded_bytes().len(), 32);
    Ok(())
}

#[test]
fn timestamp_and_effective_intervals_are_checked_and_open_ended()
-> Result<(), Box<dyn std::error::Error>> {
    let start = Timestamp::from_unix_nanos(100);
    let end = Timestamp::from_unix_nanos(200);
    assert_eq!(start.unix_nanos(), 100);
    assert_eq!(start.checked_add_nanos(50)?.unix_nanos(), 150);
    assert_eq!(
        Timestamp::from_unix_nanos(i64::MAX).checked_add_nanos(1),
        Err(TimeError::Overflow)
    );
    assert_eq!(
        EffectiveInterval::new(start, Some(start)),
        Err(InstrumentError::InvalidEffectiveInterval)
    );
    let open = EffectiveInterval::new(start, None)?;
    assert_eq!(open.starts_at(), start);
    assert_eq!(open.ends_at(), None);
    assert_eq!(
        EffectiveInterval::new(start, Some(end))?.ends_at(),
        Some(end)
    );
    Ok(())
}

#[test]
fn identity_lifecycle_records_keep_effective_time_and_stable_instrument_ids()
-> Result<(), Box<dyn std::error::Error>> {
    let current = InstrumentId::try_from(Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8")?)?;
    let successor =
        InstrumentId::try_from(Uuid::parse_str("7d9e9f3e-b62d-4fce-a85f-fad3ca549c97")?)?;
    let effective_at = Timestamp::from_unix_nanos(500);
    let interval = EffectiveInterval::new(effective_at, None)?;
    let symbol = SymbolIdentityRecord::new(
        current,
        VenueId::try_from("XNAS")?,
        VenueSymbol::try_from("ACME")?,
        interval,
    );
    let provider = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: current,
        source_id: SourceId::try_from("nasdaq-reference")?,
        provider_instrument_id: ProviderInstrumentId::try_from("ACME.O")?,
        evidence: provider_evidence(11),
        source_timestamp: None,
        observed_at: effective_at,
        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
            "nasdaq-reference-revision:1",
        )?),
        validity: interval,
        supersedes: None,
    });
    assert_eq!(symbol.validity().ends_at(), None);
    assert_eq!(provider.instrument_id(), current);

    let merger = LifecycleTransition::new(
        current,
        effective_at,
        LifecycleTransitionKind::Merger { successor },
    )?;
    assert_eq!(merger.effective_at(), effective_at);
    assert_eq!(
        LifecycleTransition::new(
            current,
            effective_at,
            LifecycleTransitionKind::Merger { successor: current },
        ),
        Err(InstrumentError::SelfTransition)
    );
    assert_eq!(
        ContractRollMapping::new(current, current, effective_at),
        Err(InstrumentError::SelfTransition)
    );
    assert_eq!(
        ContractRollMapping::new(current, successor, effective_at)?.to_instrument_id(),
        successor
    );
    Ok(())
}

#[test]
fn instrument_definition_owns_precision_mappings_identifiers_and_status()
-> Result<(), Box<dyn std::error::Error>> {
    let id = InstrumentId::try_from(Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8")?)?;
    let venue = VenueId::try_from("XNAS")?;
    let mapping = VenueMapping::new(venue.clone(), VenueSymbol::try_from("AAPL")?);
    let provider_identity = ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: id,
        source_id: SourceId::try_from("user-reference")?,
        provider_instrument_id: ProviderInstrumentId::try_from("AAPL.O")?,
        evidence: provider_evidence(12),
        source_timestamp: None,
        observed_at: Timestamp::from_unix_nanos(1),
        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
            "user-reference-revision:1",
        )?),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
        supersedes: None,
    });
    let identifier = ExternalIdentifier::Isin(Isin::try_from("US0378331005")?);
    let identifier_record = ExternalIdentifierRecord::new(ExternalIdentifierRecordInput {
        identifier,
        assignment_verification: AssignmentVerification::Unverified,
        source_id: SourceId::try_from("user-reference")?,
        source_reference: PayloadReference::SourceReference(SourceIdentifier::try_from(
            "instrument-row:1",
        )?),
        source_timestamp: None,
        observed_at: Timestamp::from_unix_nanos(1),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
        rights_policy: IdentifierRightsPolicyReference::new(
            SourceIdentifier::try_from("policy:user-local-v1")?,
            IdentifierEntitlement::UserOwned,
            SourceIdentifier::try_from("user-provided")?,
        ),
    });
    let definition = InstrumentDefinition::try_new(InstrumentDefinitionInput {
        instrument_id: id,
        asset_class: AssetClass::Equity,
        primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
        tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
        lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
        venue_mappings: vec![mapping.clone()],
        provider_identities: vec![provider_identity.clone()],
        identifiers: vec![identifier_record.clone()],
        trading_status: TradingStatus::Active,
    })?;
    assert_eq!(definition.instrument_id(), id);
    assert_eq!(definition.asset_class(), AssetClass::Equity);
    assert_eq!(
        definition.primary_denomination(),
        Denomination::Currency(Currency::try_from("USD")?)
    );
    assert_eq!(definition.tick_size().as_decimal(), Decimal::new(1, 2));
    assert_eq!(definition.lot_size().as_decimal(), Decimal::ONE);
    assert_eq!(definition.venue_mappings(), std::slice::from_ref(&mapping));
    assert_eq!(
        definition.provider_identities(),
        std::slice::from_ref(&provider_identity)
    );
    assert_eq!(definition.identifiers(), &[identifier_record]);
    assert_eq!(definition.trading_status(), TradingStatus::Active);

    assert_eq!(
        InstrumentDefinition::try_new(InstrumentDefinitionInput {
            instrument_id: id,
            asset_class: AssetClass::Equity,
            primary_denomination: Denomination::Currency(Currency::try_from("USD")?),
            tick_size: TickSize::try_from_decimal(Decimal::new(1, 2))?,
            lot_size: LotSize::try_from_decimal(Decimal::ONE)?,
            venue_mappings: vec![mapping.clone(), mapping],
            provider_identities: Vec::new(),
            identifiers: Vec::new(),
            trading_status: TradingStatus::Active,
        }),
        Err(InstrumentError::DuplicateVenueMapping { venue })
    );
    Ok(())
}

#[test]
fn validated_display_and_deserialization_do_not_bypass_invariants()
-> Result<(), Box<dyn std::error::Error>> {
    assert_eq!(SequenceNumber::new(42).to_string(), "42");
    assert_eq!(ConnectionGeneration::new(7)?.to_string(), "7");
    assert_eq!(MaturityMonthYear::month(2026, 3)?.to_string(), "202603");
    assert!(serde_json::from_str::<MaturityMonthYear>(r#"{"year":2026,"month":3}"#).is_err());

    let address = ChainAddress::try_solana(
        SolanaChainId::mainnet(),
        "11111111111111111111111111111111",
        ChainAddressRole::Account,
    )?;
    assert_eq!(address.to_string(), address.canonical());
    Ok(())
}

#[test]
fn notional_rejects_decimal_scale_rounding() -> Result<(), Box<dyn std::error::Error>> {
    let tick = TickSize::try_from_decimal(Decimal::try_new(1, 20)?)?;
    let lot = LotSize::try_from_decimal(Decimal::try_new(1, 20)?)?;
    let result = PriceTicks::new(1).checked_mul_quantity(
        QuantityLots::new(1)?,
        tick,
        lot,
        Currency::try_from("USD")?,
    );
    assert_eq!(result, Err(FinancialError::Overflow));
    Ok(())
}
