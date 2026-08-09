//! Kraken Spot WebSocket v2 market-data adapter.

mod config;
mod decoder;
mod level3;
mod messages;
mod qualification;
mod session;

pub use config::{KrakenChannel, KrakenConfig, KrakenConfigError, KrakenDepth};
pub use decoder::{
    KrakenControl, KrakenDecodeOutcome, KrakenDecoder, KrakenDecoderState, KrakenMarketDecoder,
    KrakenSubscription,
};
pub use level3::{
    KRAKEN_L3_CHECKSUM_CANONICALIZATION_ID, KRAKEN_L3_CHECKSUM_SCOPE_ID,
    KRAKEN_L3_GET_TOKEN_ENDPOINT, KRAKEN_L3_QUALIFICATION_POLICY_DIGEST,
    KRAKEN_L3_QUALIFICATION_POLICY_VERSION, KRAKEN_L3_WEBSOCKET_ENDPOINT, KrakenL3BatchKind,
    KrakenL3BookBatch, KrakenL3ClientTier, KrakenL3Config, KrakenL3ConfigError, KrakenL3Control,
    KrakenL3DecodeError, KrakenL3DecodeOutcome, KrakenL3Decoder, KrakenL3DecoderState,
    KrakenL3Depth, KrakenL3MetadataError, KrakenL3MetadataInput, KrakenL3Order, KrakenL3OrderEvent,
    KrakenL3OrderEventKind, KrakenL3ProductMapping, KrakenL3ScaleError, KrakenL3ScaledOrder,
    KrakenL3ScaledOrderEvent, KrakenL3SecretPayload, KrakenL3WebSocketToken,
};
pub use qualification::{
    KRAKEN_BOOK_SEQUENCE_RULE, KRAKEN_QUALIFICATION_POLICY_DIGEST,
    KRAKEN_QUALIFICATION_POLICY_VERSION, KRAKEN_TRADE_SEQUENCE_RULE, KrakenMetadataError,
    KrakenMetadataInput, KrakenQualificationPolicy,
};
pub use session::{KrakenHealth, KrakenSource};
