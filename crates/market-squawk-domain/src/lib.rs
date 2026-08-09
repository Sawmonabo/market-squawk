//! Shared invariant-preserving Market Squawk domain contracts.

mod capture;
mod classification;
mod company_identity;
mod denomination;
mod digest;
mod evidence;
mod financial;
mod identifiers;
mod identity;
mod instrument;
mod market;
mod order;
mod provenance;
mod research;
mod retained;
mod time;
mod version;

pub use capture::{
    CaptureAdmission, CaptureAuthorityBundle, CaptureAuthorityError, CaptureAuthorityIdentity,
    CaptureDegradation, CaptureFrameFootprint, CaptureInitializer, CapturePayload,
    CapturePayloadError, CaptureResidentGenerationLease, CaptureResidentToken,
    CaptureRetainedComponent, CaptureRetainedReceipt, CaptureRetainedSizeError,
    MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES, MAX_LIVE_CAPTURE_PAYLOAD_BYTES, RawCaptureFrameView,
};
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
pub use company_identity::{
    CompanyIdentityError, CompanyIdentityObservation, CompanyIdentityObservationInput,
    CompanyIdentitySurface, FormerCompanyName, MAX_COMPANY_FORMER_NAMES,
    MAX_COMPANY_SECURITY_ASSOCIATIONS, ProviderReportedSecurityAssociation,
};
pub use denomination::Denomination;
pub use digest::DigestAlgorithm;
pub use digest::DigestAlgorithm as PayloadHashAlgorithm;
pub use evidence::{
    ExactPayloadEvidence, RevisionBoundPayloadEvidence, VersionPinnedSourceLocator,
};
pub use financial::{
    BasisPoints, Currency, FinancialError, LotSize, Money, PriceError, PriceTicks, QuantityError,
    QuantityLots, RoundingPolicy, TickSize,
};
pub use identifiers::{
    AccountId, ApprovalId, BitcoinAddressType, BitcoinNetwork, ChainAddress, ChainAddressRole,
    ChainAddressRule, ChainId, ClientOrderId, CryptoPair, CryptoProductType, Cusip, EvmChainId,
    ExecutionIdentityError, Figi, FuturesContractIdentity, FuturesContractIdentityInput,
    FuturesLeg, FuturesLegInput, FuturesLegSide, FuturesLifecycleDateFields, FuturesLifecycleDates,
    FuturesSecurityType, IdentifierError, Isin, MaturityMonthYear, ModelId, OccOptionIdentity,
    OptionKind, OrderId, Sedol, SolanaChainId, SolanaNetwork, StrategyId, Ticker, VenueSymbol,
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
    MAX_MARKET_DATA_DISPLAY_NAME_BYTES, MAX_MARKET_DATA_EXTERNAL_IDENTIFIERS,
    MAX_MARKET_DATA_PROVIDER_IDENTITIES, MAX_MARKET_DATA_VENUE_MAPPINGS, MarketDataDisplayName,
    MarketDataInstrumentDefinition, MarketDataInstrumentDefinitionError,
    MarketDataInstrumentDefinitionInput, ProviderIdentityCollection, ProviderIdentityConflict,
    ProviderIdentityConflictReason, ProviderIdentityEvidence, ProviderIdentityIngestOutcome,
    ProviderIdentityKey, ProviderIdentityLocator, ProviderIdentityRecord,
    ProviderIdentityRecordInput, ProviderIdentityRegistry, ProviderIdentitySupersession,
    SymbolIdentityRecord, TradingStatus, VenueMapping,
};
pub use market::{
    AggressorSide, AuctionEvent, AuctionPhase, BookChange, BookDeltaEvent, BookLevel,
    BookSnapshotEvent, CorporateActionEvent, CorporateActionKind, HaltTransition,
    InstrumentStatusEvent, MarketEvent, MarketEventError, MarketSide, MergerConsideration,
    QuoteEvent, TradeEvent, TradingHaltEvent,
};
pub use order::{
    InstrumentDefinitionRevision, InstrumentExecutionTerms, OrderContractError, OrderReasonCode,
    OrderSide, OrderType, TimeInForce,
};
pub use provenance::{
    AvailabilityEvidence, DecodedLiveProvenanceInput, LiveProvenance, LiveRecordState, PayloadHash,
    PayloadReference, ProvenanceError, RecordedLiveProvenanceInput, ResearchContext,
    ResearchPeriod, ResearchProvenance, ResearchProvenanceInput, ResearchTemporalCoordinate,
    ResearchTemporalPrecision, ResearchTime, RevisionNumber,
};
pub use research::{
    AlternativeDataObservation, CorporateActionObservation, FilingObservation,
    FundamentalObservation, MAX_XBRL_DIMENSIONS, MAX_XBRL_GRAPH_EVENTS, MAX_XBRL_RELATIONSHIP_REFS,
    MAX_XBRL_RELATIONSHIPS, MAX_XBRL_UNIT_MEASURES, MacroMissingValue, MacroObservation,
    MacroValue, NormalizedPortfolioLotMethod, NormalizedPortfolioTransactionClass,
    NormalizedPortfolioTransactionError, NormalizedPortfolioTransactionEvidence,
    NormalizedPortfolioTransactionEvidenceInput, PositionObservation, PositionSide, ResearchError,
    ResearchObservation, TransactionObservation, UniverseMembershipObservation,
    XBRL_FACT_EVIDENCE_SCHEMA_VERSION, XbrlAccuracy, XbrlAccuracyValue, XbrlContextGraph,
    XbrlDimensionEvidence, XbrlDimensionLocation, XbrlDimensionMember, XbrlDuplicateClass,
    XbrlDuplicateEvidence, XbrlEntity, XbrlEvidenceError, XbrlFactEvidence, XbrlFactEvidenceInput,
    XbrlOccurrenceRelationships, XbrlPeriod, XbrlQualifiedName, XbrlRelationshipEvidence, XbrlSign,
    XbrlTaxonomySet, XbrlTaxonomyStatus, XbrlText, XbrlTypedMemberValidation, XbrlUnitExpression,
    XbrlXmlEvent,
};
pub use retained::{
    RetainedLayoutError, checked_arc_bytes_allocation_bytes, checked_arc_str_allocation_bytes,
    checked_arc_value_allocation_bytes,
};
pub use time::{CalendarDate, TimeError, Timestamp};
pub use version::{SchemaVersion, SchemaVersionError};
