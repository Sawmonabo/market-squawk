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
    use market_squawk_sources::{
        AuthoritativeSourceRegistry, ProviderIdentitySelectionEvidence,
        ProviderNativeIdentityRequest,
    };
    let selected_at = now_timestamp()?;
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let mut selections = Vec::new();
    for (instrument, symbol) in routes {
        selections.push(std::sync::Arc::new(FixtureIdentitySelection(
            ProviderIdentitySelectionEvidence {
                native: ProviderNativeIdentityRequest {
                    namespace: SourceId::try_from("coinbase-advanced-trade")?,
                    provider_instrument_id: market_squawk_domain::ProviderInstrumentId::try_from(
                        *symbol,
                    )?,
                    instrument: *instrument,
                    venue: VenueId::try_from("coinbase")?,
                    venue_symbol: market_squawk_domain::VenueSymbol::try_from(*symbol)?,
                    knowledge_at: selected_at,
                    effective_at: selected_at,
                },
                definition_digest: exact_evidence(31).content_digest(),
                definition_sequence: 1,
                reference_revision: MetadataRevision::new(source_identifier(
                    "fixture-reference-v1",
                )?),
                reference_payload_digest: exact_evidence(32).content_digest(),
                definition_published_at: selected_at,
                definition_validity: validity,
                provider_revision: MetadataRevision::new(source_identifier("fixture-provider-v1")?),
                provider_payload_digest: exact_evidence(33).content_digest(),
                provider_validity: validity,
                resolution_digest: exact_evidence(34).content_digest(),
                selection_digest: exact_evidence(35).content_digest(),
            },
        )));
    }
    let requests = selections
        .iter()
        .map(|selected| selected.0.native.clone())
        .collect::<Vec<_>>();
    let mut registry = AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?
        .with_provider_identity_authority(std::sync::Arc::new(FixtureIdentityCatalog(
            selections,
        )))?;
    let registered = registry.register(metadata, registered_at)?;
    registry.record_provider_identities(
        &registered,
        &requests,
        std::time::Instant::now() + std::time::Duration::from_secs(2),
        &tokio_util::sync::CancellationToken::new(),
    )?;
    Ok((registry, registered))
}

#[derive(Debug)]
struct FixtureIdentityCatalog(Vec<std::sync::Arc<FixtureIdentitySelection>>);

impl market_squawk_sources::CatalogProviderIdentityAuthority for FixtureIdentityCatalog {
    fn select_current(
        &self,
        request: &market_squawk_sources::ProviderNativeIdentityRequest,
        deadline: std::time::Instant,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<
        std::sync::Arc<dyn market_squawk_sources::CurrentCatalogProviderIdentity>,
        market_squawk_sources::RegistryError,
    > {
        use market_squawk_sources::RegistryError;
        if cancellation.is_cancelled() {
            return Err(RegistryError::ProviderIdentitySelectionCancelled);
        }
        if std::time::Instant::now() >= deadline {
            return Err(RegistryError::ProviderIdentitySelectionDeadlineExceeded);
        }
        let mut matches = self
            .0
            .iter()
            .filter(|selected| selected.0.native == *request);
        let selected = matches.next().ok_or(RegistryError::LiveScopeNotCovered)?;
        if matches.next().is_some() {
            return Err(RegistryError::LiveScopeNotCovered);
        }
        Ok(selected.clone())
    }
}

#[derive(Debug)]
struct FixtureIdentitySelection(market_squawk_sources::ProviderIdentitySelectionEvidence);

impl market_squawk_sources::CurrentCatalogProviderIdentity for FixtureIdentitySelection {
    fn evidence(&self) -> &market_squawk_sources::ProviderIdentitySelectionEvidence {
        &self.0
    }

    fn validate_at(&self, at: Timestamp) -> Result<(), market_squawk_sources::RegistryError> {
        if at < self.0.definition_published_at
            || [self.0.definition_validity, self.0.provider_validity]
                .into_iter()
                .any(|validity| {
                    at < validity.starts_at() || validity.ends_at().is_some_and(|end| at >= end)
                })
        {
            return Err(market_squawk_sources::RegistryError::StaleHandle);
        }
        Ok(())
    }

    fn retained_bytes(&self) -> Result<usize, market_squawk_sources::RegistryError> {
        std::mem::size_of::<Self>()
            .checked_add(
                self.0
                    .dynamic_retained_bytes()
                    .ok_or(market_squawk_sources::RegistryError::RetainedSizeOverflow)?,
            )
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or(market_squawk_sources::RegistryError::RetainedSizeOverflow)
    }
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
