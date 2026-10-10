//! Real catalog-selected Coinbase identity for adapter tests.

use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use market_squawk_data::{
    CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits,
    MarketDataInstrumentReadCapability, MarketDataInstrumentSynchronization,
    MarketDataInstrumentSynchronizationCapability, MarketDataProviderIdentityQuery,
};
use market_squawk_domain::{
    AssetClass, Currency, DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence,
    InstrumentId, MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionInput,
    MetadataRevision, ProviderIdentityEvidence, ProviderIdentityRecord,
    ProviderIdentityRecordInput, ProviderInstrumentId, RevisionBoundPayloadEvidence, SourceId,
    SourceIdentifier, Timestamp, VenueId, VenueMapping, VenueSymbol,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    AuthoritativeSourceRegistry, ProviderIdentitySelectionEvidence, ProviderNativeIdentityRequest,
    RegisteredSource, SourceMetadata,
};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

type TestResult<T> = Result<T, Box<dyn Error>>;

pub(crate) struct CatalogFixture {
    pub(crate) selected: ProviderIdentitySelectionEvidence,
    reader: MarketDataInstrumentReadCapability,
    _directory: TempDir,
}

impl CatalogFixture {
    pub(crate) fn new(instrument: InstrumentId, namespace: SourceId) -> TestResult<Self> {
        let directory = TempDir::new()?;
        let paths = LocalPaths::prepare(directory.path().join("catalog"))?;
        let catalog = CatalogConfig::try_new(
            paths.catalog()?.clone(),
            Duration::from_millis(750),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
        )?;
        let authority = Arc::new(Mutex::new(CatalogAuthority::open(catalog)?));
        let writer = MarketDataInstrumentSynchronizationCapability::new(Arc::clone(&authority));
        let reader = MarketDataInstrumentReadCapability::new(
            authority,
            Instant::now() + Duration::from_secs(5),
            &CancellationToken::new(),
        )?;
        let effective = EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?;
        let observed_at = system_timestamp()?;
        let native_id = ProviderInstrumentId::try_from("BTC-USD")?;
        let venue = VenueId::try_from("coinbase-exchange")?;
        let symbol = VenueSymbol::try_from("BTC-USD")?;
        let definition =
            MarketDataInstrumentDefinition::try_new(MarketDataInstrumentDefinitionInput {
                instrument_id: instrument,
                reference_evidence: RevisionBoundPayloadEvidence::new(
                    MetadataRevision::new(identifier("coinbase-catalog-reference-test-v1")?),
                    evidence(5),
                ),
                effective_interval: effective,
                asset_class: AssetClass::Crypto,
                display_name: None,
                quote_currency: Currency::try_from("USD")?,
                quote_currency_evidence: evidence(6),
                venue_mappings: vec![VenueMapping::new(venue.clone(), symbol.clone())],
                provider_identities: vec![ProviderIdentityRecord::new(
                    ProviderIdentityRecordInput {
                        instrument_id: instrument,
                        source_id: namespace.clone(),
                        provider_instrument_id: native_id.clone(),
                        evidence: ProviderIdentityEvidence::from_content_digest(
                            evidence(7).content_digest(),
                        ),
                        source_timestamp: None,
                        observed_at,
                        metadata_revision: MetadataRevision::new(identifier(
                            "coinbase-product-test-v1",
                        )?),
                        validity: effective,
                        supersedes: None,
                    },
                )],
                identifiers: Vec::new(),
            })?;
        let cancellation = CancellationToken::new();
        let deadline = || Instant::now() + Duration::from_secs(2);
        writer.synchronize(
            MarketDataInstrumentSynchronization::try_new(vec![definition], 1)?,
            deadline(),
            &cancellation,
        )?;
        let selected_at = system_timestamp()?;
        let request = ProviderNativeIdentityRequest {
            namespace: namespace.clone(),
            provider_instrument_id: native_id.clone(),
            instrument,
            venue,
            venue_symbol: symbol,
            knowledge_at: selected_at,
            effective_at: selected_at,
        };
        let selection = reader
            .select_provider_identity_as_of(
                MarketDataProviderIdentityQuery::try_new(
                    namespace,
                    native_id,
                    selected_at,
                    selected_at,
                )?,
                deadline(),
                &cancellation,
            )?
            .ok_or("catalog did not select Coinbase product")?;
        let selected = reader.selected_provider_identity_evidence(
            &selection,
            &request,
            deadline(),
            &cancellation,
        )?;
        Ok(Self {
            selected,
            reader,
            _directory: directory,
        })
    }

    pub(crate) fn selected_registry(
        &self,
        metadata: &SourceMetadata,
    ) -> TestResult<(AuthoritativeSourceRegistry, RegisteredSource)> {
        self.register_selected(
            AuthoritativeSourceRegistry::try_new_ephemeral_for_diagnostics()?,
            metadata,
        )
    }

    pub(crate) fn register_selected(
        &self,
        registry: AuthoritativeSourceRegistry,
        metadata: &SourceMetadata,
    ) -> TestResult<(AuthoritativeSourceRegistry, RegisteredSource)> {
        let mut registry =
            registry.with_provider_identity_authority(Arc::new(self.reader.clone()))?;
        let registered = registry.register(metadata.clone(), Timestamp::from_unix_nanos(1))?;
        registry.record_provider_identities(
            &registered,
            &[self.selected.native.clone()],
            Instant::now() + Duration::from_secs(2),
            &CancellationToken::new(),
        )?;
        Ok((registry, registered))
    }
}

fn system_timestamp() -> TestResult<Timestamp> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(Timestamp::from_unix_nanos(i64::try_from(nanos)?))
}

fn identifier(value: &str) -> TestResult<SourceIdentifier> {
    Ok(SourceIdentifier::try_from(value)?)
}

fn evidence(byte: u8) -> ExactPayloadEvidence {
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        [byte; 32],
    ))
}
