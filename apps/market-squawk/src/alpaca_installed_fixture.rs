//! Closed installed fixture for one synthetic AAPL-shaped IEX presentation route.
//!
//! This module exists only in non-release debug compositions. Its identity is intentionally
//! separate from canonical instrument, provider, research, investment, portfolio, valuation,
//! recommendation, backtest, and execution authority. The fixture exercises the real raw-frame
//! and Alpaca decoder path, but it is not market data and never establishes facts about Apple,
//! Nasdaq, OpenFIGI, Alpaca entitlement, a brokerage account, or a tradable security.

use std::sync::Arc;

use market_squawk_adapter_alpaca::{
    ALPACA_INSTALLED_FIXTURE_IEX_SOURCE_ID, AlpacaError, AlpacaInstalledFixtureIexConfig,
    AlpacaScriptedTransportFactory, AlpacaScriptedTransportTranscript,
};
use market_squawk_domain::{
    AssetClass, ChecksumCapability, CoverageDelay, Currency, DataQuality, DeliveryEvidence,
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
    LiveEventClass, MetadataRevision, RevisionBoundPayloadEvidence, SequenceCapability, SourceId,
    SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    AuthorizationMode, HistoricalCapability, NetworkAccessPolicy, SourceClass,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

const FIXTURE_WIRE_SYMBOL: &str = "AAPL";
const FIXTURE_VENUE_TOKEN: &str = "iex";
const FIXTURE_PRICE_CURRENCY: &str = "USD";
const FIXTURE_DISPLAY_NAME: &str = "AAPL scripted fixture";
const FIXTURE_PROVIDER: &str = "market-squawk-installed-fixture";
const FIXTURE_PRODUCT: &str = "market-squawk-installed-aapl-iex-fixture-v1";
const FIXTURE_CHANNEL: &str = "local-alpaca-iex-json-compatible-script-v1";
const FIXTURE_DEFINITION_REVISION_PREFIX: &str = "alpaca-installed-fixture-aapl-definition-v1";
const FIXTURE_DEFINITION_DIGEST_DOMAIN: &[u8] =
    b"market-squawk/installed-fixture-instrument-definition/v1\0";

/// Application-level installed fixture construction failure.
#[derive(Debug, Error)]
pub enum AlpacaInstalledFixtureError {
    /// The sealed adapter configuration could not be constructed.
    #[error(transparent)]
    Adapter(#[from] AlpacaError),
    /// Adapter metadata no longer matches the reviewed installed-fixture contract.
    #[error("installed Alpaca fixture metadata is not the exact closed fixture contract")]
    ContractMismatch,
    /// A code-owned fixture identity or evidence value could not be represented.
    #[error("installed Alpaca fixture definition could not be represented")]
    Definition,
}

/// Noncanonical route coordinate retained only by the fixture display composition.
///
/// It deliberately has no parser, serialization, generic constructor, or `From`/`Into`
/// conversion. The sole extraction method is crate-private and exists only to join the real
/// decoder output to the fixture-only display directory.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct InstalledFixtureInstrumentRouteId(InstrumentId);

impl InstalledFixtureInstrumentRouteId {
    const fn from_config(config: &AlpacaInstalledFixtureIexConfig) -> Self {
        Self(config.aapl_route())
    }

    pub(crate) const fn runtime_instrument_id(self) -> InstrumentId {
        self.0
    }
}

/// Exact noncanonical definition for one fixture presentation row.
///
/// This is not, cannot be converted into, and must never be persisted as a
/// `MarketDataInstrumentDefinition` or executable `InstrumentDefinition`. `USD` is only the unit
/// of the locally generated numeric fixture payload; it is not reference-master currency evidence
/// for Apple or any listed security.
#[derive(Clone, Debug)]
pub(crate) struct InstalledFixtureInstrumentDefinition {
    route_id: InstalledFixtureInstrumentRouteId,
    source_id: SourceId,
    wire_symbol: SourceIdentifier,
    venue_token: VenueId,
    asset_class: AssetClass,
    fixture_price_currency: Currency,
    display_name: &'static str,
    definition_evidence: RevisionBoundPayloadEvidence,
    source_metadata_revision: MetadataRevision,
    source_metadata_digest: EvidenceDigest,
    effective_interval: EffectiveInterval,
}

impl InstalledFixtureInstrumentDefinition {
    fn try_from_alpaca_aapl_iex(
        config: &AlpacaInstalledFixtureIexConfig,
    ) -> Result<Arc<Self>, AlpacaInstalledFixtureError> {
        validate_config(config)?;
        let route_id = InstalledFixtureInstrumentRouteId::from_config(config);
        let source_id = config.metadata().source_id().clone();
        let wire_symbol = SourceIdentifier::try_from(FIXTURE_WIRE_SYMBOL)
            .map_err(|_| AlpacaInstalledFixtureError::Definition)?;
        let venue_token = VenueId::try_from(FIXTURE_VENUE_TOKEN)
            .map_err(|_| AlpacaInstalledFixtureError::Definition)?;
        let fixture_price_currency = Currency::try_from(FIXTURE_PRICE_CURRENCY)
            .map_err(|_| AlpacaInstalledFixtureError::Definition)?;
        let effective_interval = config.metadata().coverage().effective_interval();
        let source_metadata_revision = config.metadata().revision().clone();
        let source_metadata_digest = config
            .metadata()
            .revision_evidence()
            .payload_evidence()
            .content_digest();
        let definition_digest = definition_digest(
            route_id,
            &source_id,
            &wire_symbol,
            &venue_token,
            fixture_price_currency,
            effective_interval,
            &source_metadata_revision,
            source_metadata_digest,
        );
        let definition_revision = SourceIdentifier::try_from(format!(
            "{FIXTURE_DEFINITION_REVISION_PREFIX}.{}",
            encode_lower_hex(definition_digest.bytes())
        ))
        .map_err(|_| AlpacaInstalledFixtureError::Definition)?;
        Ok(Arc::new(Self {
            route_id,
            source_id,
            wire_symbol,
            venue_token,
            asset_class: AssetClass::Equity,
            fixture_price_currency,
            display_name: FIXTURE_DISPLAY_NAME,
            definition_evidence: RevisionBoundPayloadEvidence::new(
                MetadataRevision::new(definition_revision),
                ExactPayloadEvidence::from_content_digest(definition_digest),
            ),
            source_metadata_revision,
            source_metadata_digest,
            effective_interval,
        }))
    }

    pub(crate) const fn route_id(&self) -> InstalledFixtureInstrumentRouteId {
        self.route_id
    }

    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    pub(crate) const fn wire_symbol(&self) -> &SourceIdentifier {
        &self.wire_symbol
    }

    pub(crate) const fn venue_token(&self) -> &VenueId {
        &self.venue_token
    }

    pub(crate) const fn asset_class(&self) -> AssetClass {
        self.asset_class
    }

    pub(crate) const fn fixture_price_currency(&self) -> Currency {
        self.fixture_price_currency
    }

    pub(crate) const fn display_name(&self) -> &'static str {
        self.display_name
    }

    pub(crate) const fn definition_evidence(&self) -> &RevisionBoundPayloadEvidence {
        &self.definition_evidence
    }

    pub(crate) const fn source_metadata_revision(&self) -> &MetadataRevision {
        &self.source_metadata_revision
    }

    pub(crate) const fn source_metadata_digest(&self) -> EvidenceDigest {
        self.source_metadata_digest
    }

    pub(crate) const fn effective_interval(&self) -> EffectiveInterval {
        self.effective_interval
    }

    pub(crate) fn matches_config(&self, config: &AlpacaInstalledFixtureIexConfig) -> bool {
        validate_config(config).is_ok()
            && self.route_id.runtime_instrument_id() == config.aapl_route()
            && self.source_id == *config.metadata().source_id()
            && self.source_metadata_revision == *config.metadata().revision()
            && self.source_metadata_digest
                == config
                    .metadata()
                    .revision_evidence()
                    .payload_evidence()
                    .content_digest()
            && self.effective_interval == config.metadata().coverage().effective_interval()
            && self.effective_interval.ends_at() == Some(config.exclusive_expires_at())
    }
}

/// One restart-reusable owner of the exact config, definition, and scripted transport.
///
/// Cloning the bundle preserves the same finite fixture contract. It does not create a runtime
/// generation, open a read gate, or grant any provider, research, investment, or execution
/// authority. Those remain separate, non-public application composition decisions.
#[derive(Clone, Debug)]
pub struct AlpacaInstalledFixtureBundle {
    inner: Arc<AlpacaInstalledFixtureBundleInner>,
}

#[derive(Debug)]
struct AlpacaInstalledFixtureBundleInner {
    config: AlpacaInstalledFixtureIexConfig,
    definition: Arc<InstalledFixtureInstrumentDefinition>,
    transport: AlpacaScriptedTransportFactory,
}

impl AlpacaInstalledFixtureBundle {
    /// Constructs the sole code-owned AAPL-shaped fixture contract at `starts_at`.
    ///
    /// # Errors
    ///
    /// Fails when the finite interval cannot be represented or any adapter/source fact differs
    /// from the reviewed fixture-only contract.
    pub fn try_new(starts_at: Timestamp) -> Result<Self, AlpacaInstalledFixtureError> {
        let config = AlpacaInstalledFixtureIexConfig::try_new(starts_at)?;
        let definition = InstalledFixtureInstrumentDefinition::try_from_alpaca_aapl_iex(&config)?;
        Ok(Self {
            inner: Arc::new(AlpacaInstalledFixtureBundleInner {
                config,
                definition,
                transport: AlpacaScriptedTransportFactory::new(),
            }),
        })
    }

    /// Returns diagnostic transport history only.
    ///
    /// A transcript is not runtime, provider, entitlement, definition, restart, or acceptance
    /// authority. Installed E2E acceptance must instead use the definition- and generation-bound
    /// application presentation receipt.
    pub fn transport_transcript(
        &self,
    ) -> Result<AlpacaScriptedTransportTranscript, AlpacaInstalledFixtureError> {
        self.inner
            .transport
            .transcript()
            .map_err(AlpacaInstalledFixtureError::Adapter)
    }

    pub(crate) fn config(&self) -> AlpacaInstalledFixtureIexConfig {
        self.inner.config.clone()
    }

    pub(crate) fn definition(&self) -> Arc<InstalledFixtureInstrumentDefinition> {
        Arc::clone(&self.inner.definition)
    }

    pub(crate) fn transport_factory(&self) -> AlpacaScriptedTransportFactory {
        self.inner.transport.clone()
    }
}

fn validate_config(
    config: &AlpacaInstalledFixtureIexConfig,
) -> Result<(), AlpacaInstalledFixtureError> {
    let metadata = config.metadata();
    let coverage = metadata.coverage();
    let effective = coverage.effective_interval();
    let authorization = metadata.authorization();
    let capabilities = metadata.capabilities();
    let revision_digest = metadata
        .revision_evidence()
        .payload_evidence()
        .content_digest();
    let [asset_class] = coverage.asset_classes() else {
        return Err(AlpacaInstalledFixtureError::ContractMismatch);
    };
    let [venue] = coverage.topology().venues() else {
        return Err(AlpacaInstalledFixtureError::ContractMismatch);
    };
    let [instrument] = coverage.instruments().instruments() else {
        return Err(AlpacaInstalledFixtureError::ContractMismatch);
    };
    let Some(live) = coverage.live() else {
        return Err(AlpacaInstalledFixtureError::ContractMismatch);
    };
    let [rule] = live.rules() else {
        return Err(AlpacaInstalledFixtureError::ContractMismatch);
    };
    let exact = metadata.source_id().as_str() == ALPACA_INSTALLED_FIXTURE_IEX_SOURCE_ID
        && metadata.provider().as_str() == FIXTURE_PROVIDER
        && metadata.source_class() == SourceClass::LocalFile
        && authorization.mode() == AuthorizationMode::UserOwnedLocal
        && authorization.effective_interval() == effective
        && matches!(metadata.network_policy(), NetworkAccessPolicy::Denied)
        && metadata.budget_policy().is_none()
        && metadata.quality_ceiling() == DataQuality::DirectUnverified
        && *asset_class == AssetClass::Equity
        && coverage.topology().is_partial()
        && venue.as_str() == FIXTURE_VENUE_TOKEN
        && *instrument == config.aapl_route()
        && coverage.delay() == CoverageDelay::RealTime
        && coverage.delivery() == DeliveryEvidence::Unknown
        && live.provider_product().as_source_identifier().as_str() == FIXTURE_PRODUCT
        && live.provider_channel().as_source_identifier().as_str() == FIXTURE_CHANNEL
        && rule.event_class() == LiveEventClass::Quote
        && rule.depth().is_none()
        && capabilities.live()
        && !capabilities.extraction()
        && capabilities.sequence() == SequenceCapability::Unsupported
        && capabilities.checksum() == ChecksumCapability::Unsupported
        && capabilities.historical() == HistoricalCapability::None
        && capabilities.source_timestamps()
        && effective.ends_at() == Some(config.exclusive_expires_at())
        && revision_digest.algorithm() == DigestAlgorithm::Sha256
        && revision_digest.bytes() != [0; 32];
    if !exact {
        return Err(AlpacaInstalledFixtureError::ContractMismatch);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "the definition digest closes every retained coordinate"
)]
fn definition_digest(
    route_id: InstalledFixtureInstrumentRouteId,
    source_id: &SourceId,
    wire_symbol: &SourceIdentifier,
    venue_token: &VenueId,
    fixture_price_currency: Currency,
    effective_interval: EffectiveInterval,
    source_metadata_revision: &MetadataRevision,
    source_metadata_digest: EvidenceDigest,
) -> EvidenceDigest {
    let mut digest = Sha256::new();
    digest.update(FIXTURE_DEFINITION_DIGEST_DOMAIN);
    digest.update(route_id.runtime_instrument_id().as_uuid().as_bytes());
    update_text(&mut digest, source_id.as_str());
    update_text(&mut digest, wire_symbol.as_str());
    update_text(&mut digest, venue_token.as_str());
    digest.update([1]); // Closed `AssetClass::Equity` discriminant.
    update_text(&mut digest, fixture_price_currency.as_str());
    digest.update(effective_interval.starts_at().unix_nanos().to_be_bytes());
    match effective_interval.ends_at() {
        Some(ends_at) => {
            digest.update([1]);
            digest.update(ends_at.unix_nanos().to_be_bytes());
        }
        None => digest.update([0]),
    }
    update_text(
        &mut digest,
        source_metadata_revision.as_source_identifier().as_str(),
    );
    digest.update([match source_metadata_digest.algorithm() {
        DigestAlgorithm::Sha256 => 1,
        DigestAlgorithm::Blake3 => 2,
    }]);
    digest.update(source_metadata_digest.bytes());
    EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into())
}

fn update_text(digest: &mut Sha256, value: &str) {
    let length = u64::try_from(value.len()).unwrap_or(u64::MAX);
    digest.update(length.to_be_bytes());
    digest.update(value.as_bytes());
}

fn encode_lower_hex(bytes: [u8; 32]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}
