use market_squawk_adapter_kraken::{
    KRAKEN_BOOK_SEQUENCE_RULE, KRAKEN_QUALIFICATION_POLICY_DIGEST,
    KRAKEN_QUALIFICATION_POLICY_VERSION, KrakenConfig, KrakenDecodeOutcome, KrakenDecoder,
    KrakenDecoderState, KrakenDepth, KrakenMetadataInput, KrakenQualificationPolicy,
};
use market_squawk_domain::{
    AuthorizationBasis, DataQuality, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, ExecutionEligibility, InstrumentId, MetadataRevision,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    AuthoritativeSourceRegistry, AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope,
    ChecksumValidationProfile, FreshnessPolicy, ProviderBudgetPolicy, ProviderChecksumEvidence,
    SourceProtocolProfile,
};
use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64, NonZeroUsize};
use std::str::FromStr;

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

    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?;
    let book_registration = registry.register(metadata.clone(), Timestamp::from_unix_nanos(1))?;
    let book_budget = book_registration
        .budget()
        .cloned()
        .ok_or("book registration has no budget")?;
    let trade_registration =
        registry.register(trade_metadata.clone(), Timestamp::from_unix_nanos(1))?;
    let trade_budget = trade_registration
        .budget()
        .cloned()
        .ok_or("trade registration has no budget")?;
    let instrument = InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let _book_config = KrakenConfig::try_new(
        metadata,
        book_budget,
        "BTC/USD",
        instrument,
        KrakenDepth::Ten,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;
    let _trade_config = KrakenConfig::try_trades(
        trade_metadata,
        trade_budget,
        "BTC/USD",
        instrument,
        NonZeroUsize::new(1 << 20).ok_or("zero frame bound")?,
    )?;

    let mut decoder = KrakenDecoder::try_trades("BTC/USD", instrument)?;
    let trade = br#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":"45283.50000","qty":"0.01000000","ord_type":"market","trade_id":123,"timestamp":"2023-10-04T07:48:26Z"}]}"#;
    let KrakenDecodeOutcome::Market(observations) = decoder.decode_payload(trade)? else {
        return Err("trade decoded as control traffic".into());
    };
    assert!(matches!(
        observations[0].checksum(),
        ProviderChecksumEvidence::Unsupported { .. }
    ));
    assert_eq!(decoder.state(), KrakenDecoderState::Healthy);
    Ok(())
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
