//! Kraken Spot WebSocket v2 market-data adapter.

mod config;
mod decoder;
mod messages;
mod qualification;
mod session;

pub use config::{KrakenChannel, KrakenConfig, KrakenConfigError, KrakenDepth};
pub use decoder::{
    KrakenControl, KrakenDecodeOutcome, KrakenDecoder, KrakenDecoderState, KrakenMarketDecoder,
    KrakenSubscription,
};
pub use qualification::{
    KRAKEN_BOOK_SEQUENCE_RULE, KRAKEN_QUALIFICATION_POLICY_DIGEST,
    KRAKEN_QUALIFICATION_POLICY_VERSION, KRAKEN_TRADE_SEQUENCE_RULE, KrakenMetadataError,
    KrakenMetadataInput, KrakenQualificationPolicy,
};
pub use session::{KrakenHealth, KrakenSource};
