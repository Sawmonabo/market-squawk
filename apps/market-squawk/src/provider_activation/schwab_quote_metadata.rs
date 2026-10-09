//! Exact account-scoped REST quotes and requested Streamer family declarations.

use market_squawk_domain::{
    AuthorizationBasis, ChecksumCapability, CoverageDelay, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, IntegrityRule,
    LiveEventClass, MetadataRevision, ProviderChannel, ProviderProduct,
    RevisionBoundPayloadEvidence, RuleVersion, SchemaVersion, SequenceCapability,
    SnapshotApplicability, SourceId, SourceIdentifier, VenueId,
};
use market_squawk_services::ServiceError;
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, ChecksumValidationProfile, CoverageTopology,
    EndpointPolicy, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderNumericPolicy, SemanticInterpretationProfile, SequenceValidationProfile,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataInput,
    SourceProtocolProfile,
};
use sha2::{Digest as _, Sha256};

use super::{
    ProviderAdapterActivation, SchwabMarketDataAccountActivation, SchwabQuoteReferenceBinding,
};
use crate::application::{ResearchProviderRuntimeGeneration, ResearchRightsAuthority};

pub(crate) const STREAMER_PROFILE: &str = "schwab.trader-api-market-data.streamer";
pub(crate) const STREAMER_SOURCE: &str = "schwab-streamer-market-data";
pub(crate) const STREAMER_DATASET: &str = "schwab.streamer.market-data";

struct QuoteSourceContract<'a> {
    source: &'a str,
    profile: &'a str,
    dataset: &'a str,
    channels: Vec<LiveCoverageDeclaration>,
    endpoint: &'a str,
    delay: CoverageDelay,
}

const SOURCE: &str = "schwab-trader-api";
const DATASET: &str = "schwab.quotes";
const PROFILE: &str = "schwab.trader-api-market-data";
const LOCAL_FRESHNESS_NANOS: u64 = 120_000_000_000;

impl ProviderAdapterActivation {
    /// Registers the exact finite quote scope only after real reference records exist.
    pub(crate) async fn register_schwab_quote_generation(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        instruments: &[SchwabQuoteReferenceBinding],
    ) -> Result<ResearchProviderRuntimeGeneration, ServiceError> {
        activation
            .require_runtime_current()
            .await
            .map_err(|_| ServiceError::Unauthorized)?;
        let contract = QuoteSourceContract {
            source: SOURCE,
            profile: PROFILE,
            dataset: DATASET,
            channels: vec![quote_channel("schwab-rest", "schwab-rest-quotes")?],
            endpoint: "https://api.schwabapi.com/marketdata/v1/quotes",
            delay: CoverageDelay::Unknown,
        };
        self.register_schwab_quote_source_generation(activation, instruments, contract)
            .await
    }

    pub(crate) async fn register_schwab_streamer_generation(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        instruments: &[SchwabQuoteReferenceBinding],
        bootstrap: &market_squawk_adapter_schwab::StreamerBootstrap,
    ) -> Result<ResearchProviderRuntimeGeneration, ServiceError> {
        activation
            .require_runtime_current()
            .await
            .map_err(|_| ServiceError::Unauthorized)?;
        // Desired read-only scope is registration, never proof of family availability.
        // Same-service sealed ACK/data later supplies the actual publication qualification.
        let selections = schwab_streamer_selections(instruments.iter())?;
        let mut channels = Vec::new();
        channels
            .try_reserve_exact(selections.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        for (service, keys) in selections {
            channels.push(streamer_channel(service, &keys)?);
        }
        let contract = QuoteSourceContract {
            source: STREAMER_SOURCE,
            profile: STREAMER_PROFILE,
            dataset: STREAMER_DATASET,
            channels,
            endpoint: bootstrap.socket_url(),
            delay: CoverageDelay::Unknown,
        };
        self.register_schwab_quote_source_generation(activation, instruments, contract)
            .await
    }

    async fn register_schwab_quote_source_generation(
        &self,
        activation: &SchwabMarketDataAccountActivation,
        instruments: &[SchwabQuoteReferenceBinding],
        contract: QuoteSourceContract<'_>,
    ) -> Result<ResearchProviderRuntimeGeneration, ServiceError> {
        let metadata = metadata(activation, instruments, &contract)?;
        let lease = activation.lease();
        let dataset =
            SourceIdentifier::try_from(contract.dataset).map_err(|_| ServiceError::Internal)?;
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/schwab-quote-rights/v1\0");
        hash.update(lease.rights_decision_digest().bytes());
        hash.update(metadata.source_id().as_str().as_bytes());
        hash.update(dataset.as_str().as_bytes());
        let rights = ResearchRightsAuthority::try_new_scoped(
            metadata.source_id().clone(),
            super::provider_research_rights_basis(lease).map_err(|_| ServiceError::Unauthorized)?,
            lease.rights_decision_digest(),
            EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
            lease.verification_expires_at(),
            vec![dataset],
            super::lease_research_operations(lease),
        )
        .map_err(|_| ServiceError::Unauthorized)?;
        let generation = ResearchProviderRuntimeGeneration::try_new(
            SourceIdentifier::try_from(contract.profile).map_err(|_| ServiceError::Internal)?,
            lease.session_id(),
            lease.capability_revision(),
            lease.capability_digest(),
            lease.generation(),
            lease.secret_reference().cloned(),
            lease.authority_effective_at(),
            metadata,
            rights.clone(),
        )
        .map_err(|_| ServiceError::Unauthorized)?;
        let guard = activation
            .runtime_currentness()
            .try_acquire_publication_authority()
            .map_err(|_| ServiceError::Unauthorized)?;
        guard
            .require_current()
            .map_err(|_| ServiceError::Unauthorized)?;
        self.research_mutation
            .register_provider_publication_generation(generation.clone(), rights)
            .map_err(|_| ServiceError::Unavailable)?;
        Ok(generation)
    }
}

fn metadata(
    activation: &SchwabMarketDataAccountActivation,
    instruments: &[SchwabQuoteReferenceBinding],
    contract: &QuoteSourceContract<'_>,
) -> Result<SourceMetadata, ServiceError> {
    let lease = activation.lease();
    if instruments.is_empty() || instruments.len() > 50 {
        return Err(ServiceError::InvalidRequest);
    }
    let effective = EffectiveInterval::new(
        lease.authority_effective_at(),
        lease.verification_expires_at(),
    )
    .map_err(|_| ServiceError::Unauthorized)?;
    let delay = contract.delay;
    let source_age = match delay {
        CoverageDelay::RealTime | CoverageDelay::Unknown => LOCAL_FRESHNESS_NANOS,
        CoverageDelay::Delayed(delay) => delay
            .checked_add(LOCAL_FRESHNESS_NANOS)
            .ok_or(ServiceError::InvalidResult)?,
        CoverageDelay::NotApplicable => return Err(ServiceError::Unavailable),
    };
    let mut ordered: Vec<_> = instruments.iter().collect();
    ordered.sort_by_key(|binding| binding.instrument_id());
    if ordered
        .windows(2)
        .any(|pair| pair[0].instrument_id() == pair[1].instrument_id())
    {
        return Err(ServiceError::InvalidRequest);
    }
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/schwab-quotes-source/v1\0");
    hash.update(include_bytes!("schwab_quote_metadata.rs"));
    for value in [
        contract.source,
        contract.profile,
        contract.dataset,
        contract.endpoint,
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    let channel_bytes =
        serde_json::to_vec(&contract.channels).map_err(|_| ServiceError::Internal)?;
    hash.update((channel_bytes.len() as u64).to_be_bytes());
    hash.update(channel_bytes);
    hash.update(lease.capability_digest().bytes());
    hash.update(lease.public_configuration_digest().bytes());
    hash.update(lease.rights_decision_digest().bytes());
    hash.update(activation.account_binding().verification_evidence().bytes());
    for binding in &ordered {
        hash.update(binding.instrument_id().as_uuid().as_bytes());
        hash.update(binding.canonical_record().revision_digest().bytes());
    }
    let evidence = ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ));
    // Keep the genuine classes in the already stable instrument declaration order.
    let mut classes = Vec::new();
    for binding in &ordered {
        let class = binding.definition().asset_class();
        if !classes.contains(&class) {
            classes.push(class);
        }
    }
    let authorization = AuthorizationGrant::new(
        AuthorizationMode::UserAuthorized,
        AuthorizationBasis::new(activation.account_binding().subject().clone()),
        ExactPayloadEvidence::from_content_digest(
            activation.account_binding().verification_evidence(),
        ),
        effective,
    );
    let source = SourceMetadata::try_new(SourceMetadataInput::new(
        SchemaVersion::CURRENT,
        SourceId::try_from(contract.source).map_err(|_| ServiceError::Internal)?,
        RevisionBoundPayloadEvidence::new(
            MetadataRevision::new(identifier(&format!(
                "schwab-quotes-{}",
                evidence
                    .content_digest()
                    .bytes()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            ))?),
            evidence.clone(),
        ),
        SourceClass::Broker,
        identifier("schwab-trader-api")?,
        authorization,
        SourceCoverage::try_instrument_channels(
            evidence,
            effective,
            classes,
            CoverageTopology::single_venue(
                VenueId::try_from("schwab").map_err(|_| ServiceError::Internal)?,
            ),
            InstrumentCoverage::enumerated(
                ordered
                    .iter()
                    .map(|binding| binding.instrument_id())
                    .collect(),
            )
            .map_err(|_| ServiceError::InvalidResult)?,
            contract.channels.clone(),
            delay,
            DeliveryEvidence::AuthorizedBroker,
        )
        .map_err(|_| ServiceError::InvalidResult)?,
        DataQuality::DirectUnverified,
        NetworkAccessPolicy::Allowlisted(
            EndpointPolicy::try_new([contract.endpoint]).map_err(|_| ServiceError::Internal)?,
        ),
        // These are application age ceilings, not claims of provider delivery guarantees.
        FreshnessPolicy::try_new(
            source_age,
            LOCAL_FRESHNESS_NANOS,
            LOCAL_FRESHNESS_NANOS,
            source_age,
            1_000_000_000,
        )
        .map_err(|_| ServiceError::InvalidResult)?,
        Some(
            activation
                .provider_rate_declaration()
                .map_err(|_| ServiceError::Unauthorized)?
                .policy()
                .clone(),
        ),
        SourceCapabilities::new(
            true,
            true,
            SequenceCapability::Unsupported,
            ChecksumCapability::Unsupported,
            HistoricalCapability::None,
            true,
        ),
        SourceProtocolProfile::Live(Box::new(LiveProtocolProfile::new(
            rule("schwab-quotes-exact-decimal")?,
            SemanticInterpretationProfile::new(
                rule("schwab-quote-no-aggressor")?,
                rule("schwab-quote-no-auction")?,
                rule("schwab-quote-no-status")?,
                rule("schwab-quote-no-corporate-action")?,
            ),
            rule("schwab-native-quote-time-milliseconds")?,
            SequenceValidationProfile::Unsupported {
                rule: rule("schwab-quote-no-sequence")?,
            },
            ChecksumValidationProfile::Unsupported {
                rule: rule("schwab-quote-no-checksum")?,
            },
            true,
            ProviderNumericPolicy::ExactDecimalLexeme,
        ))),
    ))
    .map_err(|_| ServiceError::InvalidResult)?;
    if !activation.account_binding().validates_metadata(&source) {
        return Err(ServiceError::Unauthorized);
    }
    Ok(source)
}
fn identifier(value: &str) -> Result<SourceIdentifier, ServiceError> {
    SourceIdentifier::try_from(value).map_err(|_| ServiceError::Internal)
}
fn rule(value: &str) -> Result<IntegrityRule, ServiceError> {
    Ok(IntegrityRule::new(
        identifier(value)?,
        RuleVersion::new(1).map_err(|_| ServiceError::Internal)?,
    ))
}

fn quote_channel(product: &str, channel: &str) -> Result<LiveCoverageDeclaration, ServiceError> {
    LiveCoverageDeclaration::try_new(
        ProviderProduct::new(identifier(product)?),
        ProviderChannel::new(identifier(channel)?),
        vec![
            LiveCoverageRule::try_new(
                LiveEventClass::Quote,
                None,
                SnapshotApplicability::NotApplicable {
                    metadata_rule: rule("schwab-quote-no-snapshot")?,
                },
            )
            .map_err(|_| ServiceError::InvalidResult)?,
        ],
    )
    .map_err(|_| ServiceError::InvalidResult)
}

/// One code-owned desired selection supplies both registered coverage and native requests.
/// Cohort keys are source scope and never receive a fabricated canonical instrument.
pub(crate) fn schwab_streamer_selections<'a>(
    bindings: impl IntoIterator<Item = &'a SchwabQuoteReferenceBinding>,
) -> Result<
    std::collections::BTreeMap<
        market_squawk_adapter_schwab::MarketDataService,
        Vec<market_squawk_adapter_schwab::ProviderIdentifier>,
    >,
    ServiceError,
> {
    use market_squawk_adapter_schwab::{MarketDataService as S, ProviderIdentifier};
    use market_squawk_domain::AssetClass;
    let mut selected: std::collections::BTreeMap<S, Vec<ProviderIdentifier>> =
        std::collections::BTreeMap::new();
    for binding in bindings {
        let services: &[S] = match binding.definition().asset_class() {
            AssetClass::Equity | AssetClass::Fund => &[
                S::LevelOneEquities,
                S::NyseBook,
                S::NasdaqBook,
                S::ChartEquity,
            ],
            AssetClass::Option => &[S::LevelOneOptions, S::OptionsBook],
            AssetClass::Future => &[S::LevelOneFutures, S::ChartFutures],
            AssetClass::ForeignExchange => &[S::LevelOneForex],
            _ => &[],
        };
        for service in services {
            let keys = selected.entry(*service).or_default();
            if keys.len() >= 50 {
                return Err(ServiceError::InvalidRequest);
            }
            let key = ProviderIdentifier::try_new(binding.provider_symbol().to_owned())
                .map_err(|_| ServiceError::InvalidResult)?;
            if keys.contains(&key) {
                return Err(ServiceError::InvalidRequest);
            }
            keys.push(key);
        }
    }
    for (service, key) in [
        (S::ScreenerEquity, "EQUITY_ALL_VOLUME_0"),
        (S::ScreenerOption, "OPTION_ALL_VOLUME_0"),
    ] {
        selected.insert(
            service,
            vec![
                ProviderIdentifier::try_new(key.to_owned())
                    .map_err(|_| ServiceError::InvalidResult)?,
            ],
        );
    }
    Ok(selected)
}

fn streamer_channel(
    service: market_squawk_adapter_schwab::MarketDataService,
    keys: &[market_squawk_adapter_schwab::ProviderIdentifier],
) -> Result<LiveCoverageDeclaration, ServiceError> {
    use market_squawk_adapter_schwab::MarketDataService as S;
    use market_squawk_domain::MarketDepth;
    let (suffix, class) = match service {
        S::LevelOneEquities => ("level-one-equities", LiveEventClass::Quote),
        S::LevelOneOptions => ("level-one-options", LiveEventClass::Quote),
        S::LevelOneFutures => ("level-one-futures", LiveEventClass::Quote),
        S::LevelOneFuturesOptions => ("level-one-futures-options", LiveEventClass::Quote),
        S::LevelOneForex => ("level-one-forex", LiveEventClass::Quote),
        S::NyseBook => ("nyse-book", LiveEventClass::BookSnapshot),
        S::NasdaqBook => ("nasdaq-book", LiveEventClass::BookSnapshot),
        S::OptionsBook => ("options-book", LiveEventClass::BookSnapshot),
        S::ChartEquity => ("chart-equity", LiveEventClass::Chart),
        S::ChartFutures => ("chart-futures", LiveEventClass::Chart),
        S::ScreenerEquity => ("screener-equity", LiveEventClass::Screener),
        S::ScreenerOption => ("screener-option", LiveEventClass::Screener),
    };
    if class == LiveEventClass::Quote {
        return quote_channel("schwab-streamer", &format!("schwab-streamer-{suffix}"));
    }
    let snapshot = SnapshotApplicability::NotApplicable {
        metadata_rule: rule("schwab-observation-no-snapshot")?,
    };
    let rule = if class == LiveEventClass::Screener {
        LiveCoverageRule::try_source_cohorts(
            keys.iter()
                .map(|key| identifier(key.as_str()))
                .collect::<Result<Vec<_>, _>>()?,
            snapshot,
        )
    } else if class == LiveEventClass::BookSnapshot {
        LiveCoverageRule::try_new(
            class,
            Some(MarketDepth::PriceLevel),
            SnapshotApplicability::Required,
        )
    } else {
        LiveCoverageRule::try_new(class, None, snapshot)
    }
    .map_err(|_| ServiceError::InvalidResult)?;
    LiveCoverageDeclaration::try_new(
        ProviderProduct::new(identifier("schwab-streamer")?),
        ProviderChannel::new(identifier(&format!("schwab-streamer-{suffix}"))?),
        vec![rule],
    )
    .map_err(|_| ServiceError::InvalidResult)
}
