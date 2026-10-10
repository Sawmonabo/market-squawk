//! Exact Spot WebSocket v2 `instrument` snapshot reference, separate from book/trade frames.

use serde::Deserialize;
use thiserror::Error;

/// Maximum original v2 instrument snapshot bytes retained for one admission.
pub const MAX_KRAKEN_INSTRUMENT_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;

/// One exact active pair from the complete v2 instrument snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenSpotPairReference {
    symbol: String,
    base: String,
    quote: String,
}

impl KrakenSpotPairReference {
    /// Requires an exact active pair in an original provider snapshot.
    pub fn from_snapshot(
        original_frame: &[u8],
        requested_symbol: &str,
    ) -> Result<Self, KrakenSpotPairReferenceError> {
        if original_frame.is_empty() || original_frame.len() > MAX_KRAKEN_INSTRUMENT_SNAPSHOT_BYTES
        {
            return Err(KrakenSpotPairReferenceError::InvalidSize);
        }
        let wire: InstrumentWire = serde_json::from_slice(original_frame)
            .map_err(|_| KrakenSpotPairReferenceError::InvalidSnapshot)?;
        if wire.channel != "instrument" || wire.kind != "snapshot" {
            return Err(KrakenSpotPairReferenceError::InvalidSnapshot);
        }
        let mut matching = wire
            .data
            .pairs
            .into_iter()
            .filter(|pair| pair.symbol == requested_symbol);
        let pair = matching
            .next()
            .ok_or(KrakenSpotPairReferenceError::PairUnavailable)?;
        if matching.next().is_some()
            || pair.status != "online"
            || pair.base.is_empty()
            || pair.quote.is_empty()
            || pair.symbol.len() > 64
            || pair.base.len() > 16
            || pair.quote.len() > 16
        {
            return Err(KrakenSpotPairReferenceError::PairUnavailable);
        }
        Ok(Self {
            symbol: pair.symbol,
            base: pair.base,
            quote: pair.quote,
        })
    }

    /// Exact WebSocket v2 pair symbol, used as both provider ID and venue symbol.
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
    /// Provider-reported base asset ID.
    pub fn base(&self) -> &str {
        &self.base
    }
    /// Provider-reported quote asset ID.
    pub fn quote(&self) -> &str {
        &self.quote
    }
}

#[derive(Deserialize)]
struct InstrumentWire {
    channel: String,
    #[serde(rename = "type")]
    kind: String,
    data: InstrumentData,
}
#[derive(Deserialize)]
struct InstrumentData {
    pairs: Vec<PairWire>,
}
#[derive(Deserialize)]
struct PairWire {
    symbol: String,
    base: String,
    quote: String,
    status: String,
}

/// Public reference is unavailable or no longer agrees with the requested active spot pair.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum KrakenSpotPairReferenceError {
    #[error("Kraken instrument snapshot exceeds its bound or is empty")]
    InvalidSize,
    #[error("Kraken instrument snapshot is malformed or not a snapshot")]
    InvalidSnapshot,
    #[error("Kraken active spot pair is missing, duplicated, or unavailable")]
    PairUnavailable,
}
