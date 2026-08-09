//! Closed production-provider dispatch without caller-replaceable connectors.

use std::time::Duration;

use futures_util::future::BoxFuture;
use market_squawk_adapter_coinbase::{CoinbaseExchangeDecoder, CoinbaseExchangeSource};
use market_squawk_adapter_kraken::{KrakenMarketDecoder, KrakenSource};
use market_squawk_sources::{
    DecodeInternalError, DecodeOutcome, LiveMarketSource, LiveSourceGeneration, MarketDecoder,
    RawMarketSink, SourceError, SourceMetadata, SourceMetadataProvider, ValidatedRawMarketFrame,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    composition::ProductionCoinbaseProfile,
    kraken::{ProductionKrakenProfile, ProductionKrakenProfileError},
};

/// Closed provider set selectable by the production application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProductionSourceProvider {
    /// Coinbase Exchange WebSocket feed.
    Coinbase,
    /// Kraken Spot WebSocket v2 book feed.
    Kraken,
}

/// Sealed connector profile plus bounded subscription-control policy.
#[derive(Debug)]
pub(super) struct ProductionSourceProfile {
    connector: ProductionConnectorProfile,
    subscription_products: Box<[String]>,
    subscription_ack_timeout: Duration,
    control_message_capacity: usize,
    control_byte_capacity: usize,
    pre_acknowledgement_data_message_capacity: usize,
    pre_acknowledgement_data_byte_capacity: usize,
}

impl ProductionSourceProfile {
    pub(super) fn coinbase(
        profile: ProductionCoinbaseProfile,
        config: &market_squawk_platform::CoinbaseSourceConfig,
        pre_acknowledgement_data_message_capacity: usize,
        pre_acknowledgement_data_byte_capacity: usize,
    ) -> Result<Self, ProductionProviderError> {
        let mut products = Vec::new();
        products
            .try_reserve_exact(config.instruments().len())
            .map_err(|_error| ProductionProviderError::AllocationFailed)?;
        products.extend(
            config
                .instruments()
                .iter()
                .map(|mapping| mapping.product().to_owned()),
        );
        let controls = config.control_limits();
        Ok(Self {
            connector: ProductionConnectorProfile::Coinbase(Box::new(profile)),
            subscription_products: products.into_boxed_slice(),
            subscription_ack_timeout: config.subscription_ack_timeout(),
            control_message_capacity: controls.message_capacity().get(),
            control_byte_capacity: controls.byte_capacity().get(),
            pre_acknowledgement_data_message_capacity,
            pre_acknowledgement_data_byte_capacity,
        })
    }

    pub(super) fn kraken(
        profile: ProductionKrakenProfile,
        config: &market_squawk_platform::KrakenSourceConfig,
    ) -> Self {
        let controls = config.control_limits();
        Self {
            connector: ProductionConnectorProfile::Kraken(Box::new(profile)),
            subscription_products: vec![config.symbol().to_owned()].into_boxed_slice(),
            subscription_ack_timeout: config.subscription_ack_timeout(),
            control_message_capacity: controls.message_capacity().get(),
            control_byte_capacity: controls.byte_capacity().get(),
            pre_acknowledgement_data_message_capacity: 0,
            pre_acknowledgement_data_byte_capacity: 0,
        }
    }

    pub(super) fn metadata(&self) -> &SourceMetadata {
        self.connector.metadata()
    }

    pub(super) fn endpoint(&self) -> &str {
        self.connector.endpoint()
    }

    pub(super) const fn source_key(&self) -> &'static str {
        self.connector.source_key()
    }

    pub(super) fn subscription_products(&self) -> &[String] {
        &self.subscription_products
    }

    pub(super) const fn subscription_ack_timeout(&self) -> Duration {
        self.subscription_ack_timeout
    }

    pub(super) const fn control_message_capacity(&self) -> usize {
        self.control_message_capacity
    }

    pub(super) const fn control_byte_capacity(&self) -> usize {
        self.control_byte_capacity
    }

    pub(super) const fn pre_acknowledgement_data_message_capacity(&self) -> usize {
        self.pre_acknowledgement_data_message_capacity
    }

    pub(super) const fn pre_acknowledgement_data_byte_capacity(&self) -> usize {
        self.pre_acknowledgement_data_byte_capacity
    }

    pub(super) fn decoder(&self) -> Result<ProductionMarketDecoder, ProductionProviderError> {
        self.connector.decoder()
    }

    pub(super) fn try_source(
        &self,
        generation: LiveSourceGeneration,
    ) -> Result<ProductionLiveSource, SourceError> {
        self.connector.try_source(generation)
    }

    #[cfg(all(test, debug_assertions))]
    pub(super) fn with_local_kraken_endpoint_for_test(
        self,
        endpoint: &str,
    ) -> Result<Self, ProductionProviderError> {
        let Self {
            connector,
            subscription_products,
            subscription_ack_timeout,
            control_message_capacity,
            control_byte_capacity,
            pre_acknowledgement_data_message_capacity,
            pre_acknowledgement_data_byte_capacity,
        } = self;
        let ProductionConnectorProfile::Kraken(profile) = connector else {
            return Err(ProductionProviderError::TestConnectorMismatch);
        };
        Ok(Self {
            connector: ProductionConnectorProfile::Kraken(Box::new(
                (*profile).with_local_endpoint_for_test(endpoint)?,
            )),
            subscription_products,
            subscription_ack_timeout,
            control_message_capacity,
            control_byte_capacity,
            pre_acknowledgement_data_message_capacity,
            pre_acknowledgement_data_byte_capacity,
        })
    }
}

#[derive(Debug)]
enum ProductionConnectorProfile {
    Coinbase(Box<ProductionCoinbaseProfile>),
    Kraken(Box<ProductionKrakenProfile>),
}

impl ProductionConnectorProfile {
    fn metadata(&self) -> &SourceMetadata {
        match self {
            Self::Coinbase(profile) => profile.metadata(),
            Self::Kraken(profile) => profile.metadata(),
        }
    }

    fn endpoint(&self) -> &str {
        match self {
            Self::Coinbase(profile) => profile.endpoint(),
            Self::Kraken(profile) => profile.endpoint(),
        }
    }

    const fn source_key(&self) -> &'static str {
        match self {
            Self::Coinbase(_) => "coinbase-exchange-public",
            Self::Kraken(_) => "kraken-public-book-v2",
        }
    }

    fn decoder(&self) -> Result<ProductionMarketDecoder, ProductionProviderError> {
        match self {
            Self::Coinbase(profile) => {
                Ok(ProductionMarketDecoder::Coinbase(profile.decoder().clone()))
            }
            Self::Kraken(profile) => Ok(ProductionMarketDecoder::Kraken(profile.decoder()?)),
        }
    }

    fn try_source(
        &self,
        generation: LiveSourceGeneration,
    ) -> Result<ProductionLiveSource, SourceError> {
        match self {
            Self::Coinbase(profile) => profile
                .try_source(generation)
                .map(ProductionLiveSource::Coinbase),
            Self::Kraken(profile) => profile
                .try_source(generation)
                .map(ProductionLiveSource::Kraken),
        }
    }
}

/// Closed decoder dispatch retained by one capture-first sink generation.
#[derive(Debug)]
pub(super) enum ProductionMarketDecoder {
    Coinbase(CoinbaseExchangeDecoder),
    Kraken(KrakenMarketDecoder),
}

impl SourceMetadataProvider for ProductionMarketDecoder {
    fn metadata(&self) -> &SourceMetadata {
        match self {
            Self::Coinbase(decoder) => decoder.metadata(),
            Self::Kraken(decoder) => decoder.metadata(),
        }
    }
}

impl MarketDecoder for ProductionMarketDecoder {
    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<DecodeOutcome, DecodeInternalError> {
        match self {
            Self::Coinbase(decoder) => decoder.decode(frame),
            Self::Kraken(decoder) => decoder.decode(frame),
        }
    }
}

/// Closed live-source dispatch retaining source-owned generation authority.
#[derive(Debug)]
pub(super) enum ProductionLiveSource {
    Coinbase(CoinbaseExchangeSource),
    Kraken(KrakenSource),
}

impl ProductionLiveSource {
    pub(super) fn run<'a>(
        &'a mut self,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>> {
        match self {
            Self::Coinbase(source) => source.run(sink, cancellation),
            Self::Kraken(source) => source.run(sink, cancellation),
        }
    }
}

/// Production provider composition failure.
#[derive(Debug, Error)]
pub enum ProductionProviderError {
    #[error("production provider bounded allocation failed")]
    AllocationFailed,
    #[error(transparent)]
    KrakenProfile(#[from] ProductionKrakenProfileError),
    #[error("production Kraken decoder could not be constructed")]
    KrakenDecoder(#[from] market_squawk_sources::DecodeError),
    #[cfg(all(test, debug_assertions))]
    #[error("the deterministic connector override requires a Kraken profile")]
    TestConnectorMismatch,
}
