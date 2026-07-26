//! Bounded Coinbase Exchange public and authenticated Direct market-data adapters.
//!
//! The public profile remains pinned to the Exchange Market Data endpoint and a
//! `DirectUnverified` ceiling. The authenticated profile combines `ws-direct` `full` with exact
//! REST product and level-3 snapshot capture. Both emit provider evidence only; current
//! qualification, canonical events, order composition, and execution eligibility remain owned by
//! their respective platform services.

mod config;
mod decoder;
mod direct;
mod direct_transport;
mod source;

pub use config::{
    COINBASE_EXCHANGE_ENDPOINT, CoinbaseChannel, CoinbaseConfigError, CoinbaseExchangeConfig,
    CoinbaseProductMapping, CoinbaseTransportLimits,
};
pub use decoder::CoinbaseExchangeDecoder;
pub use direct::{
    COINBASE_DIRECT_VERIFY_ENDPOINT, COINBASE_DIRECT_WEBSOCKET_ENDPOINT, CoinbaseDirectActivation,
    CoinbaseDirectAuthentication, CoinbaseDirectCaptureError, CoinbaseDirectConfig,
    CoinbaseDirectDecodeError, CoinbaseDirectDecodeOutcome, CoinbaseDirectDecoder,
    CoinbaseDirectHmacSigner, CoinbaseDirectLimits, CoinbaseDirectNonBookEvent,
    CoinbaseDirectNonBookKind, CoinbaseDirectProductError, CoinbaseDirectProductEvidence,
    CoinbaseDirectReceivedLifecycle, CoinbaseDirectSigningCapability, CoinbaseDirectSigningError,
    CoinbaseDirectSigningRequest, CoinbaseDirectSnapshotDecoder, CoinbaseDirectSnapshotError,
    CoinbaseDirectStopType, CoinbaseDirectTpslTriggeredLifecycle, CoinbaseSignedSubscription,
};
pub use direct_transport::{
    CoinbaseDirectBookUpdate, CoinbaseDirectOutput, CoinbaseDirectPublicationError,
    CoinbaseDirectSession, CoinbaseDirectSessionError,
};
pub use source::CoinbaseExchangeSource;
