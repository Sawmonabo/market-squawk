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
    IntegrityRule, LiveEventClass, MetadataRevision, ProviderChannel, ProviderProduct,
    RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion, SequenceCapability,
    SnapshotApplicability, SourceId, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, BackoffPolicy, BudgetDecision, BudgetDispatchDecision,
    BudgetReservationDecision, BudgetScope, CoverageTopology,
    EndpointPolicy, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderBudgetPolicy, ProviderNumericPolicy, SemanticInterpretationProfile,
    SequenceValidationProfile, SharedProviderBudget, SourceCapabilities, SourceClass, SourceCoverage,
    SourceMetadata,
    SourceMetadataInput, SourceProtocolProfile,
};

pub(crate) type TestResult<T = ()> = Result<T, Box<dyn Error>>;

use market_squawk_sources as sources;
mod provider_identity;

/// Deterministic selection for registry/processor tests, not real catalog evidence.
/// Real catalog selection, replacement, and restart are exercised in data/adapter tests.
/// Only the explicitly installed native routes can pass this test composition seam.
pub(crate) fn register_fixture_source(
    metadata: SourceMetadata,
    routes: &[(market_squawk_domain::InstrumentId, &str)],
    registered_at: Timestamp,
) -> TestResult<(
    market_squawk_sources::AuthoritativeSourceRegistry,
    market_squawk_sources::RegisteredSource,
)> {
    use market_squawk_sources::AuthoritativeSourceRegistry;
    let (authority, requests) =
        provider_identity::fixture_identity_authority(routes, now_timestamp()?)?;
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?
        .with_provider_identity_authority(authority)?;
    let registered = registry.register(metadata, registered_at)?;
    registry.record_provider_identities(
        &registered,
        &requests,
        std::time::Instant::now() + std::time::Duration::from_secs(2),
        &tokio_util::sync::CancellationToken::new(),
    )?;
    Ok((registry, registered))
}

pub(crate) fn acquire_budget(budget: &SharedProviderBudget) -> BudgetDecision {
    match budget.try_reserve_request() {
        BudgetReservationDecision::Ready(reservation) => match reservation.commit_dispatch() {
            BudgetDispatchDecision::Ready(permit) => BudgetDecision::Ready(permit),
            BudgetDispatchDecision::WaitUntil(deadline) => BudgetDecision::WaitUntil(deadline),
            BudgetDispatchDecision::Unavailable(reason) => BudgetDecision::Unavailable(reason),
        },
        BudgetReservationDecision::WaitUntil(deadline) => BudgetDecision::WaitUntil(deadline),
        BudgetReservationDecision::Unavailable(reason) => BudgetDecision::Unavailable(reason),
    }
}

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
