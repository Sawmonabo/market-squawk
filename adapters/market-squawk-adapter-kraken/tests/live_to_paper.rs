use market_squawk_adapter_kraken::{
    KRAKEN_BOOK_SEQUENCE_RULE, KRAKEN_QUALIFICATION_POLICY_DIGEST,
    KRAKEN_QUALIFICATION_POLICY_VERSION, KrakenConfig, KrakenDecodeOutcome, KrakenDecoder,
    KrakenDepth, KrakenMetadataInput, KrakenQualificationPolicy,
};
use market_squawk_adapter_paper::{
    FeeSchedule, PaperAccountBootstrap, PaperExposureValuation, PaperLedger, PaperLedgerConfig,
};
use market_squawk_domain::{
    AccountId, AuthorizationBasis, BasisPoints, ClientOrderId, Currency, DataQuality, Denomination,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, ExecutionEligibility,
    InstrumentDefinitionRevision, InstrumentExecutionTerms, InstrumentId, LotSize,
    MetadataRevision, Money, OrderId, OrderReasonCode, OrderSide, OrderType, PriceTicks,
    ProviderIdentityEvidence, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderIdentityRegistry, ProviderInstrumentId, QuantityLots, RevisionBoundPayloadEvidence,
    RuleVersion, SourceId, SourceIdentifier, StrategyId, TickSize, TimeInForce, Timestamp,
};
use market_squawk_execution::{
    AccountBootstrap, AccountCoordinatorConfig, AccountIdempotencyBootstrap,
    AccountRiskCoordinator, ExecutionAuditConfig, ExecutionAuditWriter, MarketRiskInput,
    OrderIntent, OrderIntentInput, PortfolioReadCapability, PortfolioReadError,
    PortfolioReadLimits, PreAuthorityRiskOutcome, RiskLimits, RiskLimitsInput, RiskPolicyIdentity,
    RiskRejectionCode, RiskService, RiskServiceConfig,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, ChecksumValidationProfile,
    DecodeError, FreshnessPolicy, ProviderBudgetPolicy, SourceProtocolProfile,
};
use rust_decimal::Decimal;
use std::collections::BTreeSet;
use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn no_book_sequence_means_no_automated_action() {
    let policy = KrakenQualificationPolicy::current();
    assert_eq!(policy.quality_ceiling(), DataQuality::DirectUnverified);
    assert_eq!(
        policy.execution_eligibility(),
        ExecutionEligibility::Ineligible
    );
    assert_eq!(policy.version(), KRAKEN_QUALIFICATION_POLICY_VERSION);
    assert_eq!(policy.digest(), KRAKEN_QUALIFICATION_POLICY_DIGEST);
    assert!(KRAKEN_BOOK_SEQUENCE_RULE.contains("unsupported"));
}

#[test]
fn metadata_binds_the_reviewed_ceiling_and_contains_no_fabricated_sequence()
-> Result<(), Box<dyn Error>> {
    let metadata = metadata_input(false)?.try_build()?;
    let trade_metadata = metadata_input(true)?.try_build()?;

    assert_eq!(metadata.quality_ceiling(), DataQuality::DirectUnverified);
    assert_eq!(
        trade_metadata.quality_ceiling(),
        DataQuality::DirectUnverified
    );
    let SourceProtocolProfile::Live(book_protocol) = metadata.protocol_profile() else {
        return Err("book metadata is not live".into());
    };
    let SourceProtocolProfile::Live(trade_protocol) = trade_metadata.protocol_profile() else {
        return Err("trade metadata is not live".into());
    };
    assert!(matches!(
        book_protocol.checksum(),
        ChecksumValidationProfile::Provided { .. }
    ));
    assert!(matches!(
        trade_protocol.checksum(),
        ChecksumValidationProfile::Unsupported { .. }
    ));
    let json = serde_json::to_string(&metadata)?;
    assert!(json.contains("unsupported"));
    assert!(!json.contains("sequence_number"));
    assert!(json.contains(KRAKEN_QUALIFICATION_POLICY_DIGEST));

    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let book_config = KrakenConfig::try_new(
        metadata,
        "BTC/USD",
        instrument,
        KrakenDepth::Ten,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;
    let trade_config = KrakenConfig::try_trades(
        trade_metadata,
        "BTC/USD",
        instrument,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;

    let provider_identity = kraken_provider_identity(instrument)?;
    let identities = ProviderIdentityRegistry::try_from_records(vec![provider_identity.clone()])?;
    let book_coordinates = book_config.try_native_coordinates(&provider_identity, &identities)?;
    let trade_coordinates = trade_config.try_native_coordinates(&provider_identity, &identities)?;
    assert_eq!(
        book_coordinates
            .provider_identity_key()
            .provider_instrument_id()
            .as_str(),
        "XBTUSD"
    );
    assert_eq!(book_coordinates.venue_symbol().as_str(), "BTC/USD");
    assert_eq!(
        book_coordinates.provider_identity_revision(),
        provider_identity.metadata_revision()
    );
    assert_eq!(
        book_coordinates.provider_identity_digest(),
        provider_identity.evidence().content_digest()
    );
    assert_eq!(
        book_coordinates
            .provider_product()
            .as_source_identifier()
            .as_str(),
        "kraken-spot"
    );
    assert_eq!(
        book_coordinates
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        "book-v2"
    );
    assert_eq!(
        trade_coordinates
            .provider_channel()
            .as_source_identifier()
            .as_str(),
        "trade-v2"
    );
    assert!(matches!(
        KrakenDecoder::try_trades_with_coordinates(book_coordinates.clone()),
        Err(DecodeError::InvalidProviderEvidence)
    ));

    let mut decoder =
        KrakenDecoder::try_new_with_coordinates(book_coordinates.clone(), KrakenDepth::Ten)?;
    let KrakenDecodeOutcome::Market(observations) =
        decoder.decode_payload(include_bytes!("../fixtures/official_book_checksum.json"))?
    else {
        return Err("official Kraken book snapshot decoded as control traffic".into());
    };
    assert_eq!(decoder.native_coordinates(), Some(&book_coordinates));
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].instrument(), instrument);
    assert_eq!(observations[0].venue(), book_coordinates.venue());
    let _trade_decoder = KrakenDecoder::try_trades_with_coordinates(trade_coordinates)?;

    Ok(())
}

#[test]
fn kraken_quality_is_rejected_before_paper_state_mutation() -> Result<(), Box<dyn Error>> {
    let metadata = metadata_input(false)?.try_build()?;
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let account = AccountId::from_str("50000000-0000-0000-0000-000000000006")?;
    let usd = Currency::try_from("USD")?;
    let terms = InstrumentExecutionTerms::try_new(
        instrument,
        InstrumentDefinitionRevision::try_from(1)?,
        TickSize::try_from_decimal(Decimal::new(1, 2))?,
        LotSize::try_from_decimal(Decimal::new(1, 8))?,
        usd,
        Denomination::Currency(usd),
        Decimal::ONE,
    )?;
    let capital = Money::new(Decimal::new(1_000_000, 0), usd);
    let zero = Money::new(Decimal::ZERO, usd);
    let bootstrap = PaperAccountBootstrap {
        account_id: account,
        revision: NonZeroU64::MIN,
        eligible: true,
        cash: vec![capital],
        capital,
        peak_capital: capital,
        gross_exposure: zero,
        realized_pnl: zero,
        realized_loss: zero,
        positions: vec![(instrument, 0)],
        position_cost_basis: vec![(instrument, zero)],
    };
    let mut paper = PaperLedger::try_new(
        PaperLedgerConfig {
            allow_short: false,
            exposure_valuation: PaperExposureValuation::ExecutableExit,
            maximum_accounts: 1,
            maximum_balances: 1,
            maximum_positions: 1,
            maximum_reservations: 1,
            fee_schedule: FeeSchedule::try_new(0, 0, zero, None, 2)?,
        },
        [bootstrap],
    )?;
    let settled_before = paper.cash(account, usd)?;
    let available_before = paper.available_cash(account, usd)?;
    let position_before = paper.position_lots(account, instrument)?;
    let risk_before = paper.account_risk(account)?;

    let coordinator = Arc::new(AccountRiskCoordinator::try_new(
        AccountCoordinatorConfig::default(),
        [AccountBootstrap {
            account_id: account,
            revision: NonZeroU64::MIN,
            eligible: true,
            cash: capital,
            capital,
            peak_capital: capital,
            gross_exposure: zero,
            realized_pnl: zero,
            realized_loss: zero,
            positions: vec![(instrument, 0)],
            position_cost_basis: vec![(instrument, zero)],
            idempotency: AccountIdempotencyBootstrap::empty(),
        }],
    )?);
    let limits = RiskLimits::try_new(RiskLimitsInput {
        currency: usd,
        eligible_instruments: BTreeSet::from([instrument]),
        maximum_position_lots: 1_000,
        maximum_order_notional: capital,
        maximum_gross_exposure: capital,
        maximum_leverage: BasisPoints::new(100_000),
        minimum_capital: Money::new(Decimal::ONE, usd),
        maximum_loss: capital,
        maximum_drawdown: capital,
        maximum_fee: BasisPoints::new(100),
        maximum_price_deviation: BasisPoints::new(1_000),
        maximum_slippage: BasisPoints::new(1_000),
        maximum_orders_per_window: NonZeroU32::MIN,
        order_rate_window_nanos: 60_000_000_000,
        reservation_ttl_nanos: 5_000_000_000,
        allow_short: false,
        kill_switch: false,
    })?;
    let (audit, _audit_reader) = ExecutionAuditWriter::try_new(ExecutionAuditConfig {
        maximum_records: NonZeroUsize::new(4).ok_or("zero audit record bound")?,
        maximum_bytes: NonZeroU32::new(64 * 1024).ok_or("zero audit byte bound")?,
    })?;
    let risk = RiskService::try_new(
        coordinator,
        PortfolioReadCapability::unavailable(PortfolioReadLimits::default())?,
        limits,
        audit,
        RiskServiceConfig {
            policy: RiskPolicyIdentity::new(
                &SourceIdentifier::try_from("risk/kraken-v1")?,
                RuleVersion::new(1)?,
            ),
            policy_valid_until: Timestamp::from_unix_nanos(i64::MAX),
            maximum_approval_lifetime: Duration::from_secs(1),
        },
    )?;
    let observed_at = current_timestamp()?;
    let expires_at = observed_at.checked_add_nanos(30_000_000_000)?;
    let intent = OrderIntent::try_new(OrderIntentInput {
        order_id: OrderId::from_str("20000000-0000-0000-0000-000000000006")?,
        client_order_id: ClientOrderId::try_from("kraken-risk-ceiling")?,
        strategy_id: StrategyId::from_str("30000000-0000-0000-0000-000000000006")?,
        model_id: None,
        account_id: account,
        execution_terms: terms,
        side: OrderSide::Buy,
        order_type: OrderType::Market,
        quantity: QuantityLots::new(1)?,
        limit_price: None,
        stop_price: None,
        time_in_force: TimeInForce::ImmediateOrCancel,
        signal_at: observed_at,
        expires_at,
        reason_codes: vec![OrderReasonCode::try_from("kraken.quality-ceiling")?],
        maximum_slippage: BasisPoints::new(100),
        required_quality: DataQuality::DirectVerified,
    })?;
    let market = MarketRiskInput::try_new(
        terms,
        metadata.quality_ceiling(),
        true,
        true,
        observed_at,
        expires_at,
        PriceTicks::new(10_000),
        PriceTicks::new(10_000),
    )?;

    let rejection = match risk.evaluate_pre_authority(&intent, &market) {
        PreAuthorityRiskOutcome::Rejected(rejection) => Some(rejection),
        PreAuthorityRiskOutcome::Reserved(_reservation) => {
            paper.reserve(
                intent.order_id(),
                intent.account_id(),
                intent.execution_terms(),
                intent.side(),
                intent.quantity(),
                market.estimated_execution_price(),
            )?;
            None
        }
    };

    assert_eq!(paper.cash(account, usd)?, settled_before);
    assert_eq!(paper.available_cash(account, usd)?, available_before);
    assert_eq!(paper.position_lots(account, instrument)?, position_before);
    assert_eq!(paper.account_risk(account)?, risk_before);
    let rejection = rejection.ok_or("Kraken quality unexpectedly passed canonical risk")?;
    assert_eq!(
        rejection.reasons(),
        &[
            RiskRejectionCode::SourceQuality,
            RiskRejectionCode::Portfolio(PortfolioReadError::MissingAccount),
        ]
    );
    Ok(())
}

fn current_timestamp() -> Result<Timestamp, Box<dyn Error>> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let nanos = i128::from(elapsed.as_secs())
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(elapsed.subsec_nanos())))
        .ok_or("system timestamp overflow")?;
    Ok(Timestamp::from_unix_nanos(i64::try_from(nanos)?))
}

fn kraken_provider_identity(
    instrument: InstrumentId,
) -> Result<ProviderIdentityRecord, Box<dyn Error>> {
    Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
        instrument_id: instrument,
        source_id: SourceId::try_from("kraken")?,
        provider_instrument_id: ProviderInstrumentId::try_from("XBTUSD")?,
        evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [4; 32],
        )),
        source_timestamp: None,
        observed_at: Timestamp::from_unix_nanos(1),
        metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(
            "kraken-instrument-identity-v1",
        )?),
        validity: EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
        supersedes: None,
    }))
}

fn metadata_input(trades: bool) -> Result<KrakenMetadataInput, Box<dyn Error>> {
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let exact = |byte| {
        ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            [byte; 32],
        ))
    };
    let provider = SourceIdentifier::try_from("kraken")?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider),
        NonZeroU32::new(20).ok_or("zero request budget")?,
        NonZeroU64::new(1_000_000_000).ok_or("zero budget window")?,
        NonZeroU16::new(3).ok_or("zero concurrency")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(100_000_000).ok_or("zero initial backoff")?,
            NonZeroU64::new(30_000_000_000).ok_or("zero maximum backoff")?,
            1_000,
        )?,
    )?;
    let source_id = if trades {
        "kraken-public-trades-v2"
    } else {
        "kraken-public-book-v2"
    };
    let input = if trades {
        KrakenMetadataInput::new_trades(
            SourceId::try_from(source_id)?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("kraken-trade-policy-v1")?),
                exact(1),
            ),
            AuthorizationGrant::new(
                AuthorizationMode::PublicInterface,
                AuthorizationBasis::new(SourceIdentifier::try_from("kraken-terms-reviewed")?),
                exact(2),
                effective,
            ),
            exact(3),
            effective,
            InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
            FreshnessPolicy::try_new(
                5_000_000_000,
                1_000_000_000,
                2_000_000_000,
                1_000_000_000,
                100_000_000,
            )?,
            budget,
        )
    } else {
        KrakenMetadataInput::new(
            SourceId::try_from(source_id)?,
            RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("kraken-policy-v1")?),
                exact(1),
            ),
            AuthorizationGrant::new(
                AuthorizationMode::PublicInterface,
                AuthorizationBasis::new(SourceIdentifier::try_from("kraken-terms-reviewed")?),
                exact(2),
                effective,
            ),
            exact(3),
            effective,
            InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?,
            FreshnessPolicy::try_new(
                5_000_000_000,
                1_000_000_000,
                2_000_000_000,
                1_000_000_000,
                100_000_000,
            )?,
            budget,
        )
    };
    Ok(input)
}
