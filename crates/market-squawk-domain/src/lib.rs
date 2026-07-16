//! Shared invariant-preserving Market Squawk domain contracts.

mod classification;
mod denomination;
mod digest;
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
    AssessmentStatus, AssessmentValidity, AuthorizationBasis, BindingError, BookIntegrity,
    BookStateBinding, BoundAssessment, CanonicalStateDigest, CanonicalizationRule,
    CaptureIntegrityState, ChecksumCapability, ChecksumEvidence, ChecksumIntegrity, ChecksumScope,
    ChecksumTarget, ChecksumValue, ClassificationError, CoverageConsolidation, CoverageDelay,
    CoverageDimension, CoverageError, CoverageScope, CoverageStatus, DataQuality, DeliveryEvidence,
    EligibilityFailure, EligibilityFailures, EvidenceDigest, ExecutionEligibility,
    FairValueHierarchy, FreshnessState, InitializedSnapshot, IntegrityAssessmentSet,
    IntegrityCapabilities, IntegrityEvidenceError, IntegrityRule, LiveEventClass,
    LiveEvidenceBinding, LiveTimingAssessment, LiveTimingPolicy, MarketAssessmentSet, MarketDepth,
    MarketEventTiming, MetadataRevision, PayloadChecksumScope, PrecisionIntegrity, ProviderChannel,
    ProviderProduct, QualificationAssessment, QualificationAssessmentId,
    QualificationAssessmentInput, QualificationComponent, QualificationError, RuleVersion,
    SequenceCapability, SequenceEvidence, SequenceIntegrity, SequenceValidationRule,
    SnapshotApplicability, SnapshotConsistency, SnapshotEvidence, SnapshotState,
    SourceAuthorization, SourceCoverageRecord, SourcePolicyAssessment, StreamIntegrityState,
    TimestampIntegrity,
};
pub use denomination::Denomination;
pub use digest::DigestAlgorithm;
pub use digest::DigestAlgorithm as PayloadHashAlgorithm;
pub use financial::{
    BasisPoints, Currency, FinancialError, LotSize, Money, PriceError, PriceTicks, QuantityError,
    QuantityLots, RoundingPolicy, TickSize,
};
pub use identifiers::{
    BitcoinAddressType, BitcoinNetwork, ChainAddress, ChainAddressRole, ChainAddressRule, ChainId,
    CryptoPair, CryptoProductType, Cusip, EvmChainId, Figi, FuturesContractIdentity,
    FuturesContractIdentityInput, FuturesLeg, FuturesLegInput, FuturesLegSide,
    FuturesLifecycleDateFields, FuturesLifecycleDates, FuturesSecurityType, IdentifierError, Isin,
    MaturityMonthYear, OccOptionIdentity, OptionKind, Sedol, SolanaChainId, SolanaNetwork, Ticker,
    VenueSymbol,
};
pub use identity::{
    ConnectionGeneration, IdentityError, InstrumentId, ProviderInstrumentId, SequenceNumber,
    SourceId, SourceIdentifier, VenueId,
};
pub use instrument::{
    AssetClass, AssignmentVerification, ContractRollMapping, EffectiveInterval, ExternalIdentifier,
    ExternalIdentifierRecord, ExternalIdentifierRecordInput, IdentifierEntitlement,
    IdentifierRightsPolicyReference, IdentifierSyntaxVerification, InstrumentDefinition,
    InstrumentDefinitionInput, InstrumentError, LifecycleTransition, LifecycleTransitionKind,
    ProviderIdentityConflict, ProviderIdentityConflictReason, ProviderIdentityEvidence,
    ProviderIdentityKey, ProviderIdentityLocator, ProviderIdentityRecord,
    ProviderIdentityRecordInput, ProviderIdentitySupersession, SymbolIdentityRecord, TradingStatus,
    VenueMapping,
};
pub use market::{
    AggressorSide, AuctionEvent, AuctionPhase, BookChange, BookDeltaEvent, BookLevel,
    BookSnapshotEvent, CorporateActionEvent, CorporateActionKind, HaltTransition,
    InstrumentStatusEvent, MarketEvent, MarketEventError, MarketSide, QuoteEvent, TradeEvent,
    TradingHaltEvent,
};
pub use provenance::{
    AvailabilityEvidence, DecodedLiveProvenanceInput, LiveProvenance, LiveRecordState, PayloadHash,
    PayloadReference, ProvenanceError, RecordedLiveProvenanceInput, ResearchContext,
    ResearchProvenance, ResearchProvenanceInput, ResearchTime, RevisionNumber,
};
pub use research::{
    AlternativeDataObservation, CorporateActionObservation, FilingObservation,
    FundamentalObservation, MacroObservation, PositionObservation, PositionSide, ResearchError,
    ResearchObservation, TransactionObservation,
};
pub use time::{CalendarDate, TimeError, Timestamp};
pub use version::{SchemaVersion, SchemaVersionError};
