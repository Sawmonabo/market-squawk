//! Authenticated Kraken Spot WebSocket v2 order-level market data.

mod config;
mod decoder;
mod messages;
mod normalized;

pub use config::{
    KRAKEN_L3_CHECKSUM_CANONICALIZATION_ID, KRAKEN_L3_CHECKSUM_SCOPE_ID,
    KRAKEN_L3_GET_TOKEN_ENDPOINT, KRAKEN_L3_QUALIFICATION_POLICY_DIGEST,
    KRAKEN_L3_QUALIFICATION_POLICY_VERSION, KRAKEN_L3_WEBSOCKET_ENDPOINT, KrakenL3ClientTier,
    KrakenL3Config, KrakenL3ConfigError, KrakenL3CredentialAuthority, KrakenL3Depth,
    KrakenL3MetadataError, KrakenL3MetadataInput, KrakenL3ProductMapping, KrakenL3SecretPayload,
    KrakenL3SubscriptionRequestEvidence, KrakenL3TokenCapability,
};
pub use decoder::{
    KrakenL3BatchKind, KrakenL3BookBatch, KrakenL3Control, KrakenL3DecodeError, KrakenL3Decoder,
    KrakenL3DecoderState, KrakenL3Order, KrakenL3OrderEvent, KrakenL3OrderEventKind,
};
pub use normalized::{KrakenL3ScaleError, KrakenL3ScaledOrder, KrakenL3ScaledOrderEvent};
