//! Shared invariant-preserving Market Squawk domain contracts.

mod financial;
mod identifiers;
mod identity;
mod instrument;
mod time;
mod version;

pub use financial::{
    BasisPoints, Currency, FinancialError, LotSize, Money, PriceError, PriceTicks, QuantityError,
    QuantityLots, RoundingPolicy, TickSize,
};
pub use identifiers::{
    ChainAddress, ChainAddressRole, ChainAddressRule, ChainId, ContractMonth, CryptoPair,
    CryptoProductType, Cusip, Figi, FuturesContractIdentity, FuturesSecurityType, IdentifierError,
    Isin, OccOptionIdentity, OptionKind, Sedol, Ticker, VenueSymbol,
};
pub use identity::{
    ConnectionGeneration, IdentityError, InstrumentId, ProviderInstrumentId, SequenceNumber,
    SourceId, SourceIdentifier, VenueId,
};
pub use instrument::{
    AssetClass, ContractRollMapping, EffectiveInterval, ExternalIdentifier, InstrumentDefinition,
    InstrumentError, LifecycleTransition, LifecycleTransitionKind, ProviderIdentityRecord,
    SymbolIdentityRecord, TradingStatus, VenueMapping,
};
pub use time::{TimeError, Timestamp};
pub use version::{SchemaVersion, SchemaVersionError};
