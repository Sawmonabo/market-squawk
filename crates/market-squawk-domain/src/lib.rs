//! Shared invariant-preserving Market Squawk domain contracts.

mod classification;
mod denomination;
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
    BookIntegrity, CaptureIntegrityState, ChecksumCapability, ChecksumEvidence, ChecksumIntegrity,
    ChecksumScope, ChecksumValue, ClassificationError, DataQuality, DeliveryEvidence,
    EligibilityFailure, EligibilityFailures, ExecutionEligibility, FairValueHierarchy,
    FreshnessState, IntegrityCapabilities, IntegrityEvidenceError, IntegrityRule,
    LiveTimingAssessment, LiveTimingPolicy, MarketDepth, MarketEventTiming, PrecisionIntegrity,
    QualificationComponent, QualificationError, QualificationEvidence, QualificationEvidenceId,
    QualificationEvidenceInput, RuleVersion, SequenceCapability, SequenceEvidence,
    SequenceIntegrity, SequenceValidationRule, SnapshotConsistency, SnapshotEvidence,
    SourceAuthorization, SourceCoverageEvidence, StreamIntegrityState, TimestampIntegrity,
};
pub use denomination::Denomination;
pub use financial::{
    BasisPoints, Currency, FinancialError, LotSize, Money, PriceError, PriceTicks, QuantityError,
    QuantityLots, RoundingPolicy, TickSize,
};
pub use identifiers::{
    BitcoinAddressType, BitcoinNetwork, ChainAddress, ChainAddressRole, ChainAddressRule, ChainId,
    ContractMonth, CryptoPair, CryptoProductType, Cusip, Figi, FuturesContractIdentity, FuturesLeg,
    FuturesLegSide, FuturesLifecycleDates, FuturesSecurityType, IdentifierError, Isin,
    OccOptionIdentity, OptionKind, Sedol, Ticker, VenueSymbol,
};
pub use identity::{
    ConnectionGeneration, IdentityError, InstrumentId, ProviderInstrumentId, SequenceNumber,
    SourceId, SourceIdentifier, VenueId,
};
pub use instrument::{
    AssetClass, AssignmentVerification, ContractRollMapping, EffectiveInterval, ExternalIdentifier,
    ExternalIdentifierRecord, IdentifierEntitlement, IdentifierRightsPolicyReference,
    IdentifierSyntaxVerification, InstrumentDefinition, InstrumentError, LifecycleTransition,
    LifecycleTransitionKind, ProviderIdentityRecord, SymbolIdentityRecord, TradingStatus,
    VenueMapping,
};
pub use market::{
    AggressorSide, AuctionEvent, AuctionPhase, BookChange, BookDeltaEvent, BookLevel,
    BookSnapshotEvent, CorporateActionEvent, CorporateActionKind, HaltTransition,
    InstrumentStatusEvent, MarketEvent, MarketEventError, MarketSide, QuoteEvent, TradeEvent,
    TradingHaltEvent,
};
pub use provenance::{
    AvailabilityEvidence, LiveProvenance, LiveVerificationState, PayloadHash, PayloadHashAlgorithm,
    PayloadReference, ProvenanceError, ResearchContext, ResearchProvenance, ResearchTime,
    RevisionNumber,
};
pub use research::{
    AlternativeDataObservation, CorporateActionObservation, FilingObservation,
    FundamentalObservation, MacroObservation, PositionObservation, PositionSide, ResearchError,
    ResearchObservation, TransactionObservation,
};
pub use time::{CalendarDate, TimeError, Timestamp};
pub use version::{SchemaVersion, SchemaVersionError};
