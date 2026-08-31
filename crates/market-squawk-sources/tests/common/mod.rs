#![allow(
    dead_code,
    reason = "shared integration fixtures are compiled independently for each test binary"
)]

use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use market_squawk_domain::{
    AssetClass, AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality,
    DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentId, IntegrityRule, LiveEventClass, MarketDataInstrumentDefinition,
    MarketDataInstrumentDefinitionInput, MetadataRevision, ProviderChannel,
    ProviderIdentityEvidence, ProviderIdentityKey, ProviderIdentityRecord,
    ProviderIdentityRecordInput, ProviderInstrumentId, ProviderProduct,
    RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion, SequenceCapability,
    SnapshotApplicability, SourceId, SourceIdentifier, Timestamp, VenueId, VenueMapping,
    VenueSymbol,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetScope, CoverageTopology,
    EndpointPolicy, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderBudgetPolicy, ProviderNativeInstrumentAttestation,
    ProviderNativeInstrumentAttestationInput, ProviderNumericPolicy, SemanticInterpretationProfile,
    SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

pub(crate) fn now_timestamp() -> TestResult<Timestamp> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(Timestamp::from_unix_nanos(i64::try_from(nanos)?))
}

pub(crate) fn next_timestamp_after(previous: Timestamp) -> TestResult<Timestamp> {
    for _ in 0..10_000 {
        let candidate = now_timestamp()?;
        if candidate > previous {
            return Ok(candidate);
        }
        std::hint::spin_loop();
    }
    Err("system clock did not advance for test fixture".into())
}

pub(crate) fn source_identifier(value: &str) -> TestResult<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value)?)
}

pub(crate) fn exact_evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

pub(crate) fn instrument_attestation(
    source: &str,
    instrument: InstrumentId,
    selected_at: Timestamp,
) -> TestResult<ProviderNativeInstrumentAttestation> {
    let source_id = SourceId::try_from(source)?;
    let provider_instrument_id = ProviderInstrumentId::try_from("native-instrument-1")?;
    let venue_mapping = VenueMapping::new(
        VenueId::try_from("coinbase")?,
        VenueSymbol::try_from("BTC-USD")?,
    );
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let definition =
        MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
            instrument_id: instrument,
            reference_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(source_identifier("test-reference-v1")?),
                exact_evidence(41),
            ),
            effective_interval: effective,
            asset_class: AssetClass::Crypto,
            display_name: None,
            quote_currency: market_squawk_domain::Currency::try_from("USD")?,
            quote_currency_evidence: exact_evidence(42),
            venue_mappings: vec![venue_mapping.clone()],
            provider_identities: vec![ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id: instrument,
                source_id: source_id.clone(),
                provider_instrument_id: provider_instrument_id.clone(),
                evidence: ProviderIdentityEvidence::from_content_digest(
                    exact_evidence(43).content_digest(),
                ),
                source_timestamp: None,
                observed_at: Timestamp::from_unix_nanos(0),
                metadata_revision: MetadataRevision::new(source_identifier(
                    "test-provider-identity-v1",
                )?),
                validity: effective,
                supersedes: None,
            })],
            identifiers: Vec::new(),
        })?;
    Ok(ProviderNativeInstrumentAttestation::try_select(
        ProviderNativeInstrumentAttestationInput {
            definition: &definition,
            definition_revision_digest: exact_evidence(44).content_digest(),
            definition_published_at: Timestamp::from_unix_nanos(0),
            provider_key: ProviderIdentityKey::new(source_id, provider_instrument_id),
            venue_mapping,
            selected_at,
        },
    )?)
}

pub(crate) fn direct_metadata(
    source: &str,
    revision: &str,
    starts_at: i64,
    ends_at: Option<i64>,
) -> TestResult<SourceMetadata> {
    direct_metadata_with_instruments(
        source,
        revision,
        starts_at,
        ends_at,
        vec![market_squawk_domain::InstrumentId::from_str(
            "4c74ab95-53b9-42ad-9b66-0ed403b88fed",
        )?],
    )
}

pub(crate) fn direct_metadata_with_instruments(
    source: &str,
    revision: &str,
    starts_at: i64,
    ends_at: Option<i64>,
    instruments: Vec<market_squawk_domain::InstrumentId>,
) -> TestResult<SourceMetadata> {
    let source_id = SourceId::try_from(source)?;
    let revision = MetadataRevision::new(source_identifier(revision)?);
    let revision_evidence =
        RevisionBoundPayloadEvidence::new(revision, exact_evidence(source.as_bytes()[0]));
    let effective = EffectiveInterval::new(
        Timestamp::from_unix_nanos(starts_at),
        ends_at.map(Timestamp::from_unix_nanos),
    )?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(source_identifier("public-interface-terms-v1")?),
        exact_evidence(2),
        effective,
    );
    let rule = IntegrityRule::new(
        source_identifier("trade-no-snapshot-v1")?,
        RuleVersion::new(1)?,
    );
    let live_rule = LiveCoverageRule::try_new(
        LiveEventClass::Trade,
        None,
        SnapshotApplicability::NotApplicable {
            metadata_rule: rule,
        },
    )?;
    let live = LiveCoverageDeclaration::try_new(
        ProviderProduct::new(source_identifier("direct-product")?),
        ProviderChannel::new(source_identifier("trades")?),
        vec![live_rule],
    )?;
    let coverage = SourceCoverage::try_instrument(
        exact_evidence(3),
        effective,
        vec![AssetClass::Crypto],
        CoverageTopology::single_venue(VenueId::try_from("coinbase")?),
        InstrumentCoverage::enumerated(instruments)?,
        Some(live),
        CoverageDelay::RealTime,
        DeliveryEvidence::DirectVenue,
    )?;
    let provider = source_identifier("coinbase")?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::new(provider.clone()),
        NonZeroU32::try_from(10_u32)?,
        NonZeroU64::try_from(60_000_000_000_u64)?,
        NonZeroU16::try_from(1_u16)?,
        BackoffPolicy::try_new(
            NonZeroU64::try_from(1_000_000_u64)?,
            NonZeroU64::try_from(60_000_000_000_u64)?,
            1_000,
        )?,
    )?;
    let input = SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        revision_evidence,
        SourceClass::Exchange,
        provider,
        authorization,
        coverage,
        DataQuality::DirectVerified,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_new([
            "wss://advanced-trade-ws.coinbase.com",
        ])?),
        FreshnessPolicy::try_new(
            5_000_000_000,
            1_000_000_000,
            2_000_000_000,
            1_000_000_000,
            100_000_000,
        )?,
        Some(budget),
        SourceCapabilities::new(
            true,
            false,
            SequenceCapability::Provided,
            ChecksumCapability::Unsupported,
            HistoricalCapability::None,
            true,
        ),
        SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
            IntegrityRule::new(source_identifier("coinbase-decoder")?, RuleVersion::new(1)?),
            SemanticInterpretationProfile::new(
                IntegrityRule::new(
                    source_identifier("coinbase-aggressor")?,
                    RuleVersion::new(1)?,
                ),
                IntegrityRule::new(source_identifier("coinbase-auction")?, RuleVersion::new(1)?),
                IntegrityRule::new(
                    source_identifier("coinbase-trading-status")?,
                    RuleVersion::new(1)?,
                ),
                IntegrityRule::new(
                    source_identifier("coinbase-corporate-action")?,
                    RuleVersion::new(1)?,
                ),
            ),
            IntegrityRule::new(
                source_identifier("coinbase-timestamp")?,
                RuleVersion::new(1)?,
            ),
            SequenceValidationProfile::Provided {
                rule: IntegrityRule::new(
                    source_identifier("coinbase-sequence")?,
                    RuleVersion::new(1)?,
                ),
                progression: market_squawk_domain::SequenceValidationRule::Consecutive,
            },
            market_squawk_sources::ChecksumValidationProfile::Unsupported {
                rule: IntegrityRule::new(
                    source_identifier("coinbase-no-checksum")?,
                    RuleVersion::new(1)?,
                ),
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        ))),
    );
    Ok(SourceMetadata::try_new(input)?)
}
