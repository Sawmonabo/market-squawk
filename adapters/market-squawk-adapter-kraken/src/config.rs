//! Checked adapter configuration.

use std::num::NonZeroUsize;

use market_squawk_domain::{
    DataQuality, InstrumentId, LiveEventClass, MarketDepth, SequenceCapability,
};
use market_squawk_sources::{
    ChecksumValidationProfile, InstrumentCoverageMembership, MAX_RAW_FRAME_BYTES,
    NetworkAccessPolicy, ResolvedChecksumValidator, SharedProviderBudget, SourceClass,
    SourceMetadata, SourceMetadataProvider,
};
use thiserror::Error;
use url::Url;

const KRAKEN_ENDPOINT: &str = "wss://ws.kraken.com/v2";
const MAX_SYMBOL_BYTES: usize = 64;

/// Kraken-supported retained book depths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenDepth {
    /// Ten price levels per side.
    Ten,
    /// Twenty-five price levels per side.
    TwentyFive,
    /// One hundred price levels per side.
    OneHundred,
    /// Five hundred price levels per side.
    FiveHundred,
    /// One thousand price levels per side.
    OneThousand,
}

impl KrakenDepth {
    /// Returns the provider depth value.
    pub const fn get(self) -> usize {
        match self {
            Self::Ten => 10,
            Self::TwentyFive => 25,
            Self::OneHundred => 100,
            Self::FiveHundred => 500,
            Self::OneThousand => 1_000,
        }
    }
}

/// Independently registered Kraken channel and its integrity capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenChannel {
    /// Price-level book with the selected retained depth and CRC32 validation.
    Book(KrakenDepth),
    /// Trade stream; Kraken supplies trade IDs but no book-style checksum.
    Trades,
}

/// Immutable configuration for one Kraken symbol and one connection generation.
#[derive(Clone, Debug)]
pub struct KrakenConfig {
    metadata: SourceMetadata,
    budget: SharedProviderBudget,
    endpoint: Url,
    symbol: String,
    instrument: InstrumentId,
    channel: KrakenChannel,
    max_message_bytes: NonZeroUsize,
}

impl KrakenConfig {
    /// Constructs a configuration bound to authoritative metadata and a shared provider budget.
    ///
    /// # Errors
    ///
    /// Rejects metadata that overstates Kraken's capabilities, an unapproved endpoint, a malformed
    /// symbol, an unsupported checksum profile, or a message bound outside the global capture
    /// ceiling.
    pub fn try_new(
        metadata: SourceMetadata,
        budget: SharedProviderBudget,
        symbol: impl Into<String>,
        instrument: InstrumentId,
        depth: KrakenDepth,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        Self::try_for_channel(
            metadata,
            budget,
            symbol,
            instrument,
            KrakenChannel::Book(depth),
            max_message_bytes,
        )
    }

    /// Constructs a trade-channel configuration with checksum-unsupported metadata.
    pub fn try_trades(
        metadata: SourceMetadata,
        budget: SharedProviderBudget,
        symbol: impl Into<String>,
        instrument: InstrumentId,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        Self::try_for_channel(
            metadata,
            budget,
            symbol,
            instrument,
            KrakenChannel::Trades,
            max_message_bytes,
        )
    }

    fn try_for_channel(
        metadata: SourceMetadata,
        budget: SharedProviderBudget,
        symbol: impl Into<String>,
        instrument: InstrumentId,
        channel: KrakenChannel,
        max_message_bytes: NonZeroUsize,
    ) -> Result<Self, KrakenConfigError> {
        let symbol = symbol.into();
        if symbol.is_empty()
            || symbol.len() > MAX_SYMBOL_BYTES
            || !symbol.is_ascii()
            || symbol.chars().any(char::is_whitespace)
        {
            return Err(KrakenConfigError::InvalidSymbol);
        }
        if metadata.source_class() != SourceClass::Exchange
            || metadata.provider().as_str() != "kraken"
            || metadata.quality_ceiling() != DataQuality::DirectUnverified
            || metadata.capabilities().sequence() != SequenceCapability::Unsupported
            || !metadata.capabilities().source_timestamps()
            || metadata.coverage().instruments().membership(instrument)
                != InstrumentCoverageMembership::Enumerated
        {
            return Err(KrakenConfigError::InvalidMetadata);
        }
        let NetworkAccessPolicy::Allowlisted(endpoint_policy) = metadata.network_policy() else {
            return Err(KrakenConfigError::InvalidMetadata);
        };
        let endpoint = Url::parse(KRAKEN_ENDPOINT).map_err(|_| KrakenConfigError::Endpoint)?;
        endpoint_policy
            .authorize(KRAKEN_ENDPOINT)
            .map_err(|_| KrakenConfigError::Endpoint)?;
        let market_squawk_sources::SourceProtocolProfile::Live(live) = metadata.protocol_profile()
        else {
            return Err(KrakenConfigError::InvalidMetadata);
        };
        let coverage = metadata
            .coverage()
            .live()
            .ok_or(KrakenConfigError::InvalidMetadata)?;
        match channel {
            KrakenChannel::Book(depth) => {
                if coverage
                    .rule_for(LiveEventClass::BookSnapshot, Some(MarketDepth::PriceLevel))
                    .is_none()
                    || coverage
                        .rule_for(LiveEventClass::BookDelta, Some(MarketDepth::PriceLevel))
                        .is_none()
                {
                    return Err(KrakenConfigError::InvalidMetadata);
                }
                ResolvedChecksumValidator::resolve(live.checksum(), depth.get())
                    .map_err(|_| KrakenConfigError::InvalidMetadata)?;
            }
            KrakenChannel::Trades => {
                if coverage.rule_for(LiveEventClass::Trade, None).is_none()
                    || !matches!(
                        live.checksum(),
                        ChecksumValidationProfile::Unsupported { .. }
                    )
                {
                    return Err(KrakenConfigError::InvalidMetadata);
                }
            }
        }
        if max_message_bytes.get() > MAX_RAW_FRAME_BYTES {
            return Err(KrakenConfigError::MessageBound);
        }
        Ok(Self {
            metadata,
            budget,
            endpoint,
            symbol,
            instrument,
            channel,
            max_message_bytes,
        })
    }

    /// Returns the exact allowlisted endpoint.
    pub const fn endpoint(&self) -> &Url {
        &self.endpoint
    }

    /// Returns the provider symbol handled by this source instance.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Returns the internal instrument identity.
    pub const fn instrument(&self) -> InstrumentId {
        self.instrument
    }

    /// Returns the independently registered provider channel.
    pub const fn channel(&self) -> KrakenChannel {
        self.channel
    }

    /// Returns the maximum exact frame size.
    pub const fn max_message_bytes(&self) -> usize {
        self.max_message_bytes.get()
    }

    pub(crate) const fn budget(&self) -> &SharedProviderBudget {
        &self.budget
    }
}

impl SourceMetadataProvider for KrakenConfig {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

/// Kraken configuration error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum KrakenConfigError {
    /// Metadata is inconsistent with the reviewed Kraken policy.
    #[error("Kraken source metadata is inconsistent with adapter capabilities")]
    InvalidMetadata,
    /// The configured symbol is invalid or unbounded.
    #[error("Kraken symbol is invalid")]
    InvalidSymbol,
    /// The endpoint is not the exact approved production authority.
    #[error("Kraken endpoint is not allowlisted")]
    Endpoint,
    /// The per-message bound exceeds global capture limits.
    #[error("Kraken message bound is invalid")]
    MessageBound,
}
