//! Authenticated, bounded Tradier Brokerage market-data transport and normalization.
//!
//! Tradier is represented by two distinct logical surfaces under one user-authorized account:
//! consolidated US equity/ETF/options data and provider-derived index values. The former has an
//! [`market_squawk_domain::DataQuality::Aggregated`] ceiling; the latter has a
//! [`market_squawk_domain::DataQuality::Modeled`] ceiling. Neither surface can mint
//! `DirectVerified` evidence or submit an order.
//!
//! The account owner enforces one WebSocket session at a time, shares one provider-budget
//! allocation across its logical surfaces, and exposes bounded REST bootstrap separately from the
//! live source. Reconnection and generation rollover remain central-supervisor responsibilities.

mod config;
mod credentials;
mod decoder;
mod rate_limit;
mod rest;
mod source;

#[cfg(test)]
mod tests;

pub use config::{
    TRADIER_MARKET_SESSION_ENDPOINT, TRADIER_OPTIONS_CHAIN_ENDPOINT, TRADIER_QUOTES_ENDPOINT,
    TRADIER_WEBSOCKET_ENDPOINT, TradierAccessSurface, TradierConfigError, TradierInstrumentKind,
    TradierLogicalProfile, TradierSourceConfig, TradierSymbolMapping, TradierTransportLimits,
};
pub use credentials::{TradierAccessToken, TradierCredentialError};
pub use decoder::TradierMarketDecoder;
pub use rate_limit::{TradierRateLimitError, TradierRateLimitEvidence};
pub use rest::{
    TradierDerivedIndexBatch, TradierDerivedIndexObservation, TradierOptionChain,
    TradierOptionContract, TradierOptionGreeks, TradierOptionSide, TradierQuoteBatch,
    TradierQuoteRequest, TradierQuoteSide, TradierQuoteSnapshot, TradierRestError,
    TradierRestEvidence, TradierSnapshotClient,
};
pub use source::{
    TradierAccountMarketData, TradierAccountMarketDataError, TradierStreamingSource,
    TradierSubscriptionController, TradierSubscriptionError,
};
