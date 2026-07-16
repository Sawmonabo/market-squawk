//! Shared invariant-preserving Market Squawk domain contracts.

mod classification;
mod financial;
mod identifiers;
mod identity;
mod instrument;
mod market;
mod provenance;
mod research;
mod time;
mod version;

pub use classification::{
    BookIntegrity, CaptureIntegrityState, ChecksumIntegrity, ClassificationError, DataQuality,
    DeliveryEvidence, EligibilityFailure, EligibilityFailures, EventTimingEvidence,
    ExecutionEligibility, FairValueHierarchy, FreshnessEvidence, FreshnessState, MarketDepth,
    PrecisionIntegrity, QualificationEvidence, QualificationEvidenceInput, SequenceIntegrity,
    SnapshotConsistency, SourceAuthorization, SourceCoverageEvidence, StreamIntegrityState,
    TimestampIntegrity,
};
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
pub use market::{
    AggressorSide, AuctionEvent, AuctionPhase, BookChange, BookDeltaEvent, BookLevel,
    BookSnapshotEvent, CorporateActionEvent, CorporateActionKind, HaltTransition,
    InstrumentStatusEvent, MarketEvent, MarketEventError, MarketSide, QuoteEvent, TradeEvent,
    TradingHaltEvent,
};
pub use provenance::{
    PayloadHash, PayloadHashAlgorithm, PayloadReference, Provenance, ProvenanceError,
    ResearchContext, ResearchTime, RevisionNumber,
};
pub use research::{
    AlternativeDataObservation, CorporateActionObservation, FilingObservation,
    FundamentalObservation, MacroObservation, PositionObservation, PositionSide, ResearchError,
    ResearchObservation, TransactionObservation,
};
pub use time::{TimeError, Timestamp};
pub use version::{SchemaVersion, SchemaVersionError};
