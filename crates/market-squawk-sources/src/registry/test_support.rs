use std::error::Error;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::str::FromStr;

use market_squawk_domain::{
    AssetClass, AuthorizationBasis, CaptureIntegrityState, ChecksumCapability, CoverageDelay,
    DataQuality, DeliveryEvidence, DigestAlgorithm, EffectiveInterval, EvidenceDigest,
    ExactPayloadEvidence, IntegrityRule, LiveEventClass, MetadataRevision, ProviderChannel,
    ProviderProduct, RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion, SequenceCapability,
    SnapshotApplicability, SourceId, SourceIdentifier, StreamIntegrityState, Timestamp, VenueId,
};

use super::CurrentSourceSession;
use crate::{
    AuthorizationGrant, AuthorizationHealth, AuthorizationMode, BackoffPolicy, BudgetHealth,
    BudgetScope, ChecksumValidationProfile, ConnectionLiveness, CoverageHealth, CoverageTopology,
    EndpointPolicy, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderBudgetPolicy, ProviderNumericPolicy, SemanticInterpretationProfile,
    SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage,
    SourceHealthSnapshot, SourceMetadata, SourceMetadataInput, SourceProtocolProfile,
};

pub(super) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

pub(super) fn source_identifier(value: &str) -> TestResult<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value)?)
}

pub(super) fn exact_evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}

pub(super) fn direct_metadata(source: &str, revision: &str) -> TestResult<SourceMetadata> {
    direct_metadata_with_provider_and_limit(source, revision, source, 10)
}

pub(super) fn direct_metadata_with_quality(
    source: &str,
    revision: &str,
    quality_ceiling: DataQuality,
) -> TestResult<SourceMetadata> {
    direct_metadata_with_provider_limit_and_quality(source, revision, source, 10, quality_ceiling)
}

pub(super) fn direct_metadata_with_provider_and_limit(
    source: &str,
    revision: &str,
    provider: &str,
    requests_per_window: u32,
) -> TestResult<SourceMetadata> {
    direct_metadata_with_provider_limit_and_quality(
        source,
        revision,
        provider,
        requests_per_window,
        DataQuality::DirectVerified,
    )
}

fn direct_metadata_with_provider_limit_and_quality(
    source: &str,
    revision: &str,
    provider: &str,
    requests_per_window: u32,
    quality_ceiling: DataQuality,
) -> TestResult<SourceMetadata> {
    let endpoint = format!("wss://{provider}.source.test/feed");
    let source_id = SourceId::try_from(source)?;
    let revision = MetadataRevision::new(source_identifier(revision)?);
    let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::PublicInterface,
        AuthorizationBasis::new(source_identifier("public-interface-terms-v1")?),
        exact_evidence(2),
        effective,
    );
    let live = LiveCoverageDeclaration::try_new(
        ProviderProduct::new(source_identifier("direct-product")?),
        ProviderChannel::new(source_identifier("trades")?),
        vec![LiveCoverageRule::try_new(
            LiveEventClass::Trade,
            None,
            SnapshotApplicability::NotApplicable {
                metadata_rule: rule("trade-no-snapshot-v1")?,
            },
        )?],
    )?;
    let instrument =
        market_squawk_domain::InstrumentId::from_str("4c74ab95-53b9-42ad-9b66-0ed403b88fed")?;
    let coverage = SourceCoverage::try_instrument(
        exact_evidence(3),
        effective,
        vec![AssetClass::Crypto],
        CoverageTopology::single_venue(VenueId::try_from("coinbase")?),
        InstrumentCoverage::enumerated(vec![instrument])?,
        Some(live),
        CoverageDelay::RealTime,
        DeliveryEvidence::DirectVenue,
    )?;
    let provider = source_identifier(provider)?;
    let budget = ProviderBudgetPolicy::try_new(
        BudgetScope::for_authorization(provider.clone(), &authorization)?,
        NonZeroU32::new(requests_per_window).ok_or("test request budget must be nonzero")?,
        NonZeroU64::new(60_000_000_000).ok_or("test budget window must be nonzero")?,
        NonZeroU16::new(1).ok_or("test concurrency budget must be nonzero")?,
        BackoffPolicy::try_new(
            NonZeroU64::new(1_000_000).ok_or("test backoff must be nonzero")?,
            NonZeroU64::new(60_000_000_000).ok_or("test backoff cap must be nonzero")?,
            1_000,
        )?,
    )?;
    let input = SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        source_id,
        RevisionBoundPayloadEvidence::new(revision, exact_evidence(source.as_bytes()[0])),
        SourceClass::Exchange,
        provider,
        authorization,
        coverage,
        quality_ceiling,
        NetworkAccessPolicy::Allowlisted(EndpointPolicy::try_new([endpoint])?),
        freshness_policy()?,
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
            rule("coinbase-decoder")?,
            SemanticInterpretationProfile::new(
                rule("coinbase-aggressor")?,
                rule("coinbase-auction")?,
                rule("coinbase-trading-status")?,
                rule("coinbase-corporate-action")?,
            ),
            rule("coinbase-timestamp")?,
            SequenceValidationProfile::Provided {
                rule: rule("coinbase-sequence")?,
                progression: market_squawk_domain::SequenceValidationRule::Consecutive,
            },
            ChecksumValidationProfile::Unsupported {
                rule: rule("coinbase-no-checksum")?,
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        ))),
    );
    Ok(SourceMetadata::try_new(input)?)
}

pub(super) fn healthy_snapshot(
    session: &CurrentSourceSession,
    observed_at: Timestamp,
    valid_until: Timestamp,
) -> TestResult<SourceHealthSnapshot> {
    Ok(SourceHealthSnapshot::try_new(
        session,
        observed_at,
        ConnectionLiveness::Live {
            last_activity_at: observed_at,
        },
        Some(observed_at),
        Some(observed_at),
        Some(observed_at),
        freshness_policy()?,
        StreamIntegrityState::Healthy,
        CaptureIntegrityState::Healthy,
        AuthorizationHealth::Valid {
            evidence: exact_evidence(31),
            valid_until,
        },
        CoverageHealth::Sufficient {
            evidence: exact_evidence(32),
            provider_product: ProviderProduct::new(source_identifier("direct-product")?),
            provider_channel: ProviderChannel::new(source_identifier("trades")?),
            valid_until,
        },
        BudgetHealth::Available,
        None,
        Vec::new(),
    )?)
}

pub(super) fn freshness_policy() -> TestResult<FreshnessPolicy> {
    Ok(FreshnessPolicy::try_new(
        5_000_000_000,
        1_000_000_000,
        2_000_000_000,
        1_000_000_000,
        100_000_000,
    )?)
}

fn rule(value: &str) -> TestResult<IntegrityRule> {
    Ok(IntegrityRule::new(
        source_identifier(value)?,
        RuleVersion::new(1)?,
    ))
}
