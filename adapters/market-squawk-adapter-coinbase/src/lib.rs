//! Bounded Coinbase public Advanced Trade and authenticated Direct market-data adapters.
//!
//! The public profile is pinned to the Advanced Trade Market Data endpoint and a
//! `DirectUnverified` ceiling. The authenticated profile combines `ws-direct` `full` with exact
//! REST product and level-3 snapshot capture. Both emit provider evidence only; current
//! qualification, canonical events, order composition, and execution eligibility remain owned by
//! their respective platform services.

mod config;
mod decoder;
mod direct;
mod direct_transport;
mod market_handoff;
mod publication;
mod source;

pub use config::{
    COINBASE_ADVANCED_TRADE_MARKET_DATA_ENDPOINT, CoinbaseChannel, CoinbaseConfigError,
    CoinbaseExchangeConfig, CoinbaseProductMapping, CoinbaseTransportLimits,
};
pub use decoder::CoinbaseExchangeDecoder;
pub use direct::{
    COINBASE_DIRECT_VERIFY_ENDPOINT, COINBASE_DIRECT_WEBSOCKET_ENDPOINT, CoinbaseDirectActivation,
    CoinbaseDirectAuthentication, CoinbaseDirectCaptureError, CoinbaseDirectConfig,
    CoinbaseDirectDecodeError, CoinbaseDirectDecodeOutcome, CoinbaseDirectDecoder,
    CoinbaseDirectHmacSigner, CoinbaseDirectLimits, CoinbaseDirectNonBookEvent,
    CoinbaseDirectNonBookKind, CoinbaseDirectProductError, CoinbaseDirectProductEvidence,
    CoinbaseDirectReceivedLifecycle, CoinbaseDirectSequencedEvent, CoinbaseDirectSigningCapability,
    CoinbaseDirectSigningError, CoinbaseDirectSigningRequest, CoinbaseDirectSnapshotDecoder,
    CoinbaseDirectSnapshotError, CoinbaseDirectStopType, CoinbaseDirectTpslTriggeredLifecycle,
    CoinbaseSignedSubscription,
};
pub use direct_transport::{
    CoinbaseDirectOrderLevelPayload, CoinbaseDirectOrderLevelPublicationError,
    CoinbaseDirectOrderLevelUpdate, CoinbaseDirectOutput, CoinbaseDirectOutputAdmission,
    CoinbaseDirectPublicationError, CoinbaseDirectPublicationKind, CoinbaseDirectSession,
    CoinbaseDirectSessionError,
};
pub use market_handoff::{
    CoinbaseDirectInitialMarketLineage, CoinbaseDirectReplayFrame, CoinbaseDirectTradeEvidence,
    CoinbaseMarketChannel, CoinbaseMarketContinuity, CoinbaseMarketDecodeOutcome,
    CoinbaseMarketFeed, CoinbaseMarketHandoff, CoinbaseMarketHandoffError,
    CoinbaseMarketHandoffEvidence, CoinbaseMarketRawLineage,
};
pub use publication::{
    CoinbaseDirectSnapshotSealMaterial, CoinbaseDirectSnapshotSegmentEvidence,
    CoinbaseEventMicrobatchSealMaterial, CoinbaseMarketNonPublicationReason,
    CoinbaseMarketOmission, CoinbaseMarketOmissionReason, CoinbaseMarketPhysicalCaptureIdentity,
    CoinbaseMarketPublicationContext, CoinbaseMarketPublicationError,
    CoinbaseMarketQualificationOutcome, CoinbaseMarketRawSealFrame, CoinbaseMarketSealMaterial,
    CoinbaseMarketSealRejoin, CoinbaseMarketSealedTokens, CoinbaseQualifiedDirectReplayRow,
    CoinbaseQualifiedMarketPublication, CoinbaseQualifiedPublicRow,
    CoinbaseSealedMarketPublication, CoinbaseSealedRawMarketPublication,
};
pub use source::CoinbaseExchangeSource;
