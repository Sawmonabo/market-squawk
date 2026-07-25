//! Bounded Coinbase Exchange WebSocket v1 market-data adapter.
//!
//! This adapter is pinned to the public Coinbase Exchange Market Data endpoint and an immutable
//! `DirectUnverified` ceiling. It emits provider-normalized evidence; canonical tick/lot
//! conversion, book qualification, capture admission, and execution eligibility remain owned by
//! their respective platform services.

mod config;
mod decoder;
mod direct;
mod source;

pub use config::{
    COINBASE_EXCHANGE_ENDPOINT, CoinbaseChannel, CoinbaseConfigError, CoinbaseExchangeConfig,
    CoinbaseProductMapping, CoinbaseTransportLimits,
};
pub use decoder::CoinbaseExchangeDecoder;
pub use direct::{
    COINBASE_DIRECT_WEBSOCKET_ENDPOINT, CoinbaseDirectActivation, CoinbaseDirectAuthentication,
    CoinbaseDirectCaptureError, CoinbaseDirectConfig, CoinbaseDirectDecodeError,
    CoinbaseDirectDecodeOutcome, CoinbaseDirectDecoder, CoinbaseDirectLimits,
    CoinbaseDirectNonBookEvent, CoinbaseDirectNonBookKind, CoinbaseDirectProductError,
    CoinbaseDirectProductEvidence, CoinbaseDirectReceivedLifecycle,
    CoinbaseDirectSigningCapability, CoinbaseDirectSigningError, CoinbaseDirectSigningRequest,
    CoinbaseDirectSnapshotDecoder, CoinbaseDirectSnapshotError, CoinbaseDirectStopType,
    CoinbaseDirectTpslTriggeredLifecycle, CoinbaseSignedSubscription,
};
pub use source::CoinbaseExchangeSource;
