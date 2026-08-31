mod kraken_vertical;
mod pipeline;
mod sink;
mod subscription;
mod supervisor;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use market_squawk_data::{
    CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits,
    MarketDataInstrumentReadCapability, MarketDataInstrumentRecord,
    MarketDataInstrumentSynchronization, MarketDataInstrumentSynchronizationCapability,
};
use market_squawk_domain::{
    AssetClass, Currency, DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence,
    MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionInput, MetadataRevision,
    ProviderIdentityEvidence, ProviderIdentityRecord, ProviderIdentityRecordInput,
    ProviderInstrumentId, ProviderProduct, RevisionBoundPayloadEvidence, SourceIdentifier,
    Timestamp,
};
use market_squawk_platform::{CoinbaseSourceConfig, LocalPaths};
use market_squawk_sources::{
    AuthorizationGrant, AuthorizationMode, NetworkAccessPolicy, SourceClass, SourceMetadata,
    SourceMetadataInput,
};
use tokio_util::sync::CancellationToken;

type SharedTestResult<T> = Result<T, Box<dyn std::error::Error>>;

#[test]
fn display_market_read_admission_cannot_reopen_after_revocation() {
    let admission = super::display_market::DisplayMarketReadAdmission::closed();
    let revoker = admission.clone();
    assert!(!admission.is_admitted());
    assert!(admission.admit());
    assert!(admission.is_admitted());
    revoker.revoke();
    assert!(!admission.is_admitted());
    assert!(!admission.admit());
    assert!(!admission.is_admitted());
}

fn budget_free_metadata(metadata: &SourceMetadata) -> SharedTestResult<SourceMetadata> {
    Ok(SourceMetadata::try_new(SourceMetadataInput::new(
        metadata.schema_version(),
        metadata.source_id().clone(),
        metadata.revision_evidence().clone(),
        SourceClass::LocalFile,
        metadata.provider().clone(),
        AuthorizationGrant::new(
            AuthorizationMode::UserOwnedLocal,
            metadata.authorization().basis().clone(),
            metadata.authorization().evidence().clone(),
            metadata.authorization().effective_interval(),
        ),
        metadata.coverage().clone(),
        metadata.quality_ceiling(),
        NetworkAccessPolicy::Denied,
        metadata.freshness_policy(),
        None,
        metadata.capabilities(),
        metadata.protocol_profile().clone(),
    ))?)
}

fn coinbase_market_data_record(
    source: &CoinbaseSourceConfig,
) -> SharedTestResult<MarketDataInstrumentRecord> {
    let configured = source
        .instruments()
        .first()
        .ok_or("Coinbase test instrument missing")?;
    let execution = configured.definition();
    let product = ProviderProduct::new(SourceIdentifier::try_from(configured.product())?);
    let public_source = super::instruments::public_source_id()?;
    let direct_source = super::instruments::direct_source_id(&product)?;
    let provider_instrument = ProviderInstrumentId::try_from(configured.product())?;
    let digest = |byte| EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]);
    let effective = source.authorization().effective_interval();
    let observed_at = effective.starts_at();
    let venue_mapping = execution
        .venue_mappings()
        .first()
        .cloned()
        .ok_or("Coinbase test venue mapping missing")?;
    let provider_identity =
        |source_id, revision: &str, byte| -> SharedTestResult<ProviderIdentityRecord> {
            Ok(ProviderIdentityRecord::new(ProviderIdentityRecordInput {
                instrument_id: execution.instrument_id(),
                source_id,
                provider_instrument_id: provider_instrument.clone(),
                evidence: ProviderIdentityEvidence::from_content_digest(digest(byte)),
                source_timestamp: None,
                observed_at,
                metadata_revision: MetadataRevision::new(SourceIdentifier::try_from(revision)?),
                validity: effective,
                supersedes: None,
            }))
        };
    let definition =
        MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
            instrument_id: execution.instrument_id(),
            reference_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(SourceIdentifier::try_from("coinbase-test-reference-v1")?),
                ExactPayloadEvidence::from_content_digest(digest(51)),
            ),
            effective_interval: effective,
            asset_class: AssetClass::Crypto,
            display_name: None,
            quote_currency: Currency::try_from(execution.quote_currency().as_str())?,
            quote_currency_evidence: ExactPayloadEvidence::from_content_digest(digest(52)),
            venue_mappings: vec![venue_mapping],
            provider_identities: vec![
                provider_identity(public_source, "coinbase-test-public-identity-v1", 53)?,
                provider_identity(direct_source, "coinbase-test-direct-identity-v1", 54)?,
            ],
            identifiers: Vec::new(),
        })?;

    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("coinbase-market-data"))?;
    let config = CatalogConfig::try_new(
        paths.catalog()?.clone(),
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let authority = Arc::new(Mutex::new(CatalogAuthority::open(config)?));
    let writer = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority));
    let reader = MarketDataInstrumentReadCapability::new(authority);
    let cancellation = CancellationToken::new();
    let deadline = Instant::now() + Duration::from_secs(2);
    writer.synchronize(
        MarketDataInstrumentSynchronization::try_new(vec![definition], 1)?,
        deadline,
        &cancellation,
    )?;
    reader
        .latest(execution.instrument_id(), deadline, &cancellation)?
        .ok_or_else(|| "Coinbase test market-data record missing".into())
}
