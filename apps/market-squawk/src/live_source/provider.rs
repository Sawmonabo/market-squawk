//! Closed production-provider dispatch without caller-replaceable connectors.

use std::{sync::Arc, time::Duration};

use futures_util::future::BoxFuture;
use market_squawk_adapter_alpaca::{
    AlpacaCredentials, AlpacaIexDecoder, AlpacaIexLiveConfig, AlpacaIexLiveSource,
    AlpacaOptionsDecoder, AlpacaOptionsLiveConfig, AlpacaOptionsLiveSource,
};
use market_squawk_adapter_coinbase::{CoinbaseExchangeDecoder, CoinbaseExchangeSource};
use market_squawk_adapter_kraken::{KrakenMarketDecoder, KrakenSource};
use market_squawk_adapter_tradier::{
    TRADIER_WEBSOCKET_ENDPOINT, TradierAccessSurface, TradierAccountMarketData,
    TradierAccountMarketDataError, TradierConfigError, TradierLogicalProfile, TradierMarketDecoder,
    TradierSourceConfig, TradierStreamingSource, TradierSubscriptionAuthority,
    TradierSubscriptionError,
};
use market_squawk_sources::{
    DecodeInternalError, DecodeOutcome, LiveMarketSource, LiveSourceGeneration, MarketDecoder,
    RawMarketSink, SourceError, SourceMetadata, SourceMetadataProvider, ValidatedRawMarketFrame,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::{
    composition::ProductionCoinbaseProfile,
    kraken::{ProductionKrakenProfile, ProductionKrakenProfileError},
    subscription_state::SubscriptionAcknowledgementPolicy,
};

const AUTHENTICATED_CONTROL_MESSAGE_CAPACITY: usize = 64;
const AUTHENTICATED_CONTROL_BYTE_CAPACITY: usize = 64 * 1024;

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
    subscription_acknowledgement_policy: SubscriptionAcknowledgementPolicy,
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
            subscription_acknowledgement_policy:
                SubscriptionAcknowledgementPolicy::ExplicitProviderFrame,
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
            subscription_acknowledgement_policy:
                SubscriptionAcknowledgementPolicy::ExplicitProviderFrame,
        }
    }

    pub(super) fn alpaca_iex(
        config: AlpacaIexLiveConfig,
        credentials: Arc<AlpacaCredentials>,
    ) -> Result<Self, ProductionProviderError> {
        let mut products = Vec::new();
        products
            .try_reserve_exact(config.mappings().len())
            .map_err(|_error| ProductionProviderError::AllocationFailed)?;
        products.extend(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.symbol().to_owned()),
        );
        Ok(Self {
            subscription_ack_timeout: config.transport_limits().io_timeout(),
            connector: ProductionConnectorProfile::AlpacaIex {
                config: Box::new(config),
                credentials,
            },
            subscription_products: products.into_boxed_slice(),
            control_message_capacity: AUTHENTICATED_CONTROL_MESSAGE_CAPACITY,
            control_byte_capacity: AUTHENTICATED_CONTROL_BYTE_CAPACITY,
            pre_acknowledgement_data_message_capacity: 0,
            pre_acknowledgement_data_byte_capacity: 0,
            subscription_acknowledgement_policy:
                SubscriptionAcknowledgementPolicy::ExplicitProviderFrame,
        })
    }

    pub(super) fn alpaca_options(
        config: AlpacaOptionsLiveConfig,
        credentials: Arc<AlpacaCredentials>,
    ) -> Result<Self, ProductionProviderError> {
        let mut products = Vec::new();
        products
            .try_reserve_exact(config.mappings().len())
            .map_err(|_error| ProductionProviderError::AllocationFailed)?;
        products.extend(
            config
                .mappings()
                .iter()
                .map(|mapping| mapping.symbol().to_owned()),
        );
        Ok(Self {
            subscription_ack_timeout: config.transport_limits().io_timeout(),
            connector: ProductionConnectorProfile::AlpacaOptions {
                config: Box::new(config),
                credentials,
            },
            subscription_products: products.into_boxed_slice(),
            control_message_capacity: AUTHENTICATED_CONTROL_MESSAGE_CAPACITY,
            control_byte_capacity: AUTHENTICATED_CONTROL_BYTE_CAPACITY,
            pre_acknowledgement_data_message_capacity: 0,
            pre_acknowledgement_data_byte_capacity: 0,
            subscription_acknowledgement_policy:
                SubscriptionAcknowledgementPolicy::ExplicitProviderFrame,
        })
    }

    pub(super) fn tradier_streaming(
        config: TradierSourceConfig,
        account: Arc<TradierAccountMarketData>,
        subscriptions: TradierSubscriptionAuthority,
    ) -> Result<Self, ProductionProviderError> {
        if config.profile() != TradierLogicalProfile::ConsolidatedSecurities
            || config.access_surface() != TradierAccessSurface::Streaming
        {
            return Err(ProductionProviderError::TradierProfileMismatch);
        }
        let selected = subscriptions.current_symbols()?;
        if selected.iter().any(|symbol| {
            !config
                .mappings()
                .iter()
                .any(|mapping| mapping.symbol() == symbol)
        }) {
            return Err(ProductionProviderError::TradierProfileMismatch);
        }
        let products = owned_product_names(selected)?;
        Ok(Self {
            subscription_ack_timeout: config.transport_limits().io_timeout(),
            connector: ProductionConnectorProfile::TradierStreaming {
                config: Box::new(config),
                account,
                subscriptions,
            },
            subscription_products: products.into_boxed_slice(),
            control_message_capacity: AUTHENTICATED_CONTROL_MESSAGE_CAPACITY,
            control_byte_capacity: AUTHENTICATED_CONTROL_BYTE_CAPACITY,
            pre_acknowledgement_data_message_capacity: 0,
            pre_acknowledgement_data_byte_capacity: 0,
            subscription_acknowledgement_policy:
                SubscriptionAcknowledgementPolicy::FirstValidatedData,
        })
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

    pub(super) fn subscription_product_snapshot(
        &self,
    ) -> Result<Vec<String>, ProductionProviderError> {
        if let Some(subscriptions) = self.connector.tradier_subscription_authority() {
            return owned_product_names(subscriptions.current_symbols()?);
        }
        let mut products = Vec::new();
        products
            .try_reserve_exact(self.subscription_products.len())
            .map_err(|_error| ProductionProviderError::AllocationFailed)?;
        for product in &self.subscription_products {
            let mut cloned = String::new();
            cloned
                .try_reserve_exact(product.len())
                .map_err(|_error| ProductionProviderError::AllocationFailed)?;
            cloned.push_str(product);
            products.push(cloned);
        }
        Ok(products)
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

    pub(super) const fn subscription_acknowledgement_policy(
        &self,
    ) -> SubscriptionAcknowledgementPolicy {
        self.subscription_acknowledgement_policy
    }

    pub(super) fn decoder(&self) -> Result<ProductionMarketDecoder, ProductionProviderError> {
        self.connector.decoder()
    }

    pub(super) fn try_source(
        &self,
        generation: LiveSourceGeneration,
    ) -> Result<ProductionLiveSource, ProductionProviderError> {
        self.connector.try_source(generation)
    }

    pub(super) fn tradier_subscription_authority(&self) -> Option<TradierSubscriptionAuthority> {
        self.connector.tradier_subscription_authority()
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
            subscription_acknowledgement_policy,
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
            subscription_acknowledgement_policy,
        })
    }
}

fn owned_product_names(
    selected: Vec<market_squawk_domain::SourceIdentifier>,
) -> Result<Vec<String>, ProductionProviderError> {
    let mut products = Vec::new();
    products
        .try_reserve_exact(selected.len())
        .map_err(|_error| ProductionProviderError::AllocationFailed)?;
    for symbol in selected {
        let mut product = String::new();
        product
            .try_reserve_exact(symbol.as_str().len())
            .map_err(|_error| ProductionProviderError::AllocationFailed)?;
        product.push_str(symbol.as_str());
        products.push(product);
    }
    Ok(products)
}

#[derive(Debug)]
enum ProductionConnectorProfile {
    Coinbase(Box<ProductionCoinbaseProfile>),
    Kraken(Box<ProductionKrakenProfile>),
    AlpacaIex {
        config: Box<AlpacaIexLiveConfig>,
        credentials: Arc<AlpacaCredentials>,
    },
    AlpacaOptions {
        config: Box<AlpacaOptionsLiveConfig>,
        credentials: Arc<AlpacaCredentials>,
    },
    TradierStreaming {
        config: Box<TradierSourceConfig>,
        account: Arc<TradierAccountMarketData>,
        subscriptions: TradierSubscriptionAuthority,
    },
}

impl ProductionConnectorProfile {
    fn metadata(&self) -> &SourceMetadata {
        match self {
            Self::Coinbase(profile) => profile.metadata(),
            Self::Kraken(profile) => profile.metadata(),
            Self::AlpacaIex { config, .. } => config.metadata(),
            Self::AlpacaOptions { config, .. } => config.metadata(),
            Self::TradierStreaming { config, .. } => config.metadata(),
        }
    }

    fn endpoint(&self) -> &str {
        match self {
            Self::Coinbase(profile) => profile.endpoint(),
            Self::Kraken(profile) => profile.endpoint(),
            Self::AlpacaIex { config, .. } => config.endpoint(),
            Self::AlpacaOptions { config, .. } => config.endpoint(),
            Self::TradierStreaming { .. } => TRADIER_WEBSOCKET_ENDPOINT,
        }
    }

    const fn source_key(&self) -> &'static str {
        match self {
            Self::Coinbase(_) => "coinbase-exchange-public",
            Self::Kraken(_) => "kraken-public-book-v2",
            Self::AlpacaIex { .. } => "alpaca-basic-iex-live",
            Self::AlpacaOptions { .. } => "alpaca-basic-indicative-options-live",
            Self::TradierStreaming { .. } => "tradier-consolidated-streaming",
        }
    }

    fn decoder(&self) -> Result<ProductionMarketDecoder, ProductionProviderError> {
        match self {
            Self::Coinbase(profile) => {
                Ok(ProductionMarketDecoder::Coinbase(profile.decoder().clone()))
            }
            Self::Kraken(profile) => Ok(ProductionMarketDecoder::Kraken(profile.decoder()?)),
            Self::AlpacaIex { config, .. } => Ok(ProductionMarketDecoder::AlpacaIex(
                AlpacaIexDecoder::try_new(config)?,
            )),
            Self::AlpacaOptions { config, .. } => Ok(ProductionMarketDecoder::AlpacaOptions(
                AlpacaOptionsDecoder::try_new(config)?,
            )),
            Self::TradierStreaming { config, .. } => Ok(ProductionMarketDecoder::Tradier(
                TradierMarketDecoder::try_new(config)?,
            )),
        }
    }

    fn try_source(
        &self,
        generation: LiveSourceGeneration,
    ) -> Result<ProductionLiveSource, ProductionProviderError> {
        match self {
            Self::Coinbase(profile) => Ok(ProductionLiveSource::Coinbase(
                profile.try_source(generation)?,
            )),
            Self::Kraken(profile) => Ok(ProductionLiveSource::Kraken(
                profile.try_source(generation)?,
            )),
            Self::AlpacaIex {
                config,
                credentials,
            } => AlpacaIexLiveSource::try_new(
                config.as_ref().clone(),
                generation,
                Arc::clone(credentials),
            )
            .map(ProductionLiveSource::AlpacaIex)
            .map_err(Into::into),
            Self::AlpacaOptions {
                config,
                credentials,
            } => AlpacaOptionsLiveSource::try_new(
                config.as_ref().clone(),
                generation,
                Arc::clone(credentials),
            )
            .map(ProductionLiveSource::AlpacaOptions)
            .map_err(Into::into),
            Self::TradierStreaming {
                config,
                account,
                subscriptions,
            } => account
                .streaming_source_with_authority(config.as_ref().clone(), generation, subscriptions)
                .map(ProductionLiveSource::Tradier)
                .map_err(Into::into),
        }
    }

    fn tradier_subscription_authority(&self) -> Option<TradierSubscriptionAuthority> {
        match self {
            Self::TradierStreaming { subscriptions, .. } => Some(subscriptions.clone()),
            Self::Coinbase(_)
            | Self::Kraken(_)
            | Self::AlpacaIex { .. }
            | Self::AlpacaOptions { .. } => None,
        }
    }
}

/// Closed decoder dispatch retained by one capture-first sink generation.
#[derive(Debug)]
pub(super) enum ProductionMarketDecoder {
    Coinbase(CoinbaseExchangeDecoder),
    Kraken(KrakenMarketDecoder),
    AlpacaIex(AlpacaIexDecoder),
    AlpacaOptions(AlpacaOptionsDecoder),
    Tradier(TradierMarketDecoder),
}

impl SourceMetadataProvider for ProductionMarketDecoder {
    fn metadata(&self) -> &SourceMetadata {
        match self {
            Self::Coinbase(decoder) => decoder.metadata(),
            Self::Kraken(decoder) => decoder.metadata(),
            Self::AlpacaIex(decoder) => decoder.metadata(),
            Self::AlpacaOptions(decoder) => decoder.metadata(),
            Self::Tradier(decoder) => decoder.metadata(),
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
            Self::AlpacaIex(decoder) => decoder.decode(frame),
            Self::AlpacaOptions(decoder) => decoder.decode(frame),
            Self::Tradier(decoder) => decoder.decode(frame),
        }
    }
}

/// Closed live-source dispatch retaining source-owned generation authority.
#[derive(Debug)]
pub(super) enum ProductionLiveSource {
    Coinbase(CoinbaseExchangeSource),
    Kraken(KrakenSource),
    AlpacaIex(AlpacaIexLiveSource),
    AlpacaOptions(AlpacaOptionsLiveSource),
    Tradier(TradierStreamingSource),
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
            Self::AlpacaIex(source) => source.run(sink, cancellation),
            Self::AlpacaOptions(source) => source.run(sink, cancellation),
            Self::Tradier(source) => source.run(sink, cancellation),
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
    #[error(transparent)]
    Alpaca(#[from] market_squawk_adapter_alpaca::AlpacaError),
    #[error(transparent)]
    TradierAccount(#[from] TradierAccountMarketDataError),
    #[error(transparent)]
    TradierConfig(#[from] TradierConfigError),
    #[error(transparent)]
    TradierSubscription(#[from] TradierSubscriptionError),
    #[error("Tradier production profile is not the consolidated streaming surface")]
    TradierProfileMismatch,
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error("production Kraken decoder could not be constructed")]
    KrakenDecoder(#[from] market_squawk_sources::DecodeError),
    #[cfg(all(test, debug_assertions))]
    #[error("the deterministic connector override requires a Kraken profile")]
    TestConnectorMismatch,
}
