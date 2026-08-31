//! Shared invariant-preserving Market Squawk domain contracts.

mod capture;
mod classification;
mod company_identity;
mod company_security;
mod denomination;
mod digest;
mod evidence;
mod financial;
mod identifiers;
mod identity;
mod instrument;
mod macro_feature;
mod market;
mod option_market;
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
pub use company_security::{
    CommonEquitySuitability, CompanySecurityIdentityError, CompanySecurityIdentityLink,
    CompanySecurityIdentityLinkInput, CompanySecurityKind, CompanySecurityLinkTransition,
    CompanySecurityRelationshipKind, CompanySecurityResolutionBasis,
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
pub use macro_feature::{
    FeatureDatasetMacroComponentDescriptor, feature_dataset_macro_components_v1,
};
pub use market::{
    AggressorSide, AuctionEvent, AuctionPhase, BookChange, BookDeltaEvent, BookLevel,
    BookSnapshotEvent, CorporateActionEvent, CorporateActionKind, HaltTransition,
    InstrumentStatusEvent, MarketEvent, MarketEventError, MarketSide, MergerConsideration,
    QuoteEvent, TradeEvent, TradeTakerOrderType, TradingHaltEvent,
};
pub use option_market::{
    MAX_OPTION_TRADE_CONDITIONS, OptionComponent, OptionComponentState, OptionContractTerms,
    OptionContractTermsInput, OptionExerciseStyle, OptionExpirationClass,
    OptionExpirationObservation, OptionExpirationObservationInput, OptionMarketError,
    OptionSettlementKind, OptionSnapshotObservation, OptionSnapshotObservationInput,
    OptionUnderlyingObservation,
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
    AlternativeDataObservation, BarTimeSemantics, BarTimestampBasis, CorporateActionObservation,
    FUND_HOLDING_SUPPLEMENT_TABLE_COUNT, FUND_HOLDINGS_SCHEMA_NAME, FUND_HOLDINGS_SCHEMA_VERSION,
    FilingObservation, FundAmendmentState, FundConflictState, FundCurrencyAmount, FundEtfMechanics,
    FundEvidenceRecord, FundExchangeAssociation, FundFilingChronology, FundFilingIdentity,
    FundHoldingAssociations, FundHoldingQuantity, FundHoldingSecurityIdentity,
    FundHoldingSupplementEvidence, FundHoldingUnit, FundHoldingsError, FundLineageRowRange,
    FundMissingState, FundNavCompleteness, FundNavCorrectionState, FundNavDisposition,
    FundNavEntitlementEvidence, FundNavFinality, FundNavLineage, FundNavMissingState,
    FundNavNativeSchema, FundNavObservation, FundNavObservationInput, FundNavRevisionEvidence,
    FundNavValuationBasis, FundNavValue, FundPortfolioHoldingAttributes,
    FundPortfolioHoldingEvidence, FundReleaseCoverage, FundReportAttributes, FundReportEvidence,
    FundReportedDecimal, FundReportedValue, FundRevisionEvidence, FundRevisionLink,
    FundRevisionStatus, FundSecurityIdentifier, FundShareClassAttributes, FundShareClassEvidence,
    FundShareClassIdentity, FundSourceFamily, FundSourceLineage, FundSourceRowEvidence,
    FundSourceTable, FundSourceText, FundSupplementDisposition, FundamentalAmendmentStatus,
    FundamentalCadence, FundamentalConsolidation, FundamentalContextError,
    FundamentalDimensionContext, FundamentalFactContext, FundamentalFactContextInput,
    FundamentalObservation, FundamentalPeriod, FundamentalRestatementStatus,
    FundamentalRevisionOrder, MAX_FUND_COMPETING_ACCESSIONS, MAX_FUND_EXCHANGE_ASSOCIATIONS,
    MAX_FUND_SOURCE_ROWS, MAX_XBRL_DIMENSIONS, MAX_XBRL_GRAPH_EVENTS, MAX_XBRL_RELATIONSHIP_REFS,
    MAX_XBRL_RELATIONSHIPS, MAX_XBRL_UNIT_MEASURES, MacroMissingValue, MacroObservation,
    MacroValue, MarketBarAdjustment, MarketBarObservation, MarketBarSessionEvidence,
    MarketBarSessionKind, NormalizedPortfolioLotMethod, NormalizedPortfolioTransactionClass,
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
