//! Fail-closed source metadata, runtime contracts, and lawful provider access policy.
//!
//! The types in this crate deliberately keep source registration authority separate from
//! serializable metadata and health evidence. Live adapters and extraction adapters share source
//! identity contracts, but they do not share one oversized adapter trait.

mod bounded;
mod capture;
mod decoder;
mod extraction;
mod health;
mod live;
mod metadata;
mod policy;
mod registry;

pub use capture::{
    CaptureAdmissionError, CaptureAdmissionIssuer, CaptureAdmissionReceipt,
    CaptureDegradationCapability, CaptureGenerationCapabilities, CaptureGenerationHealth,
    CaptureGenerationLease, CaptureInitializationControl,
};
pub use decoder::{
    DecodeError, DecodedProviderBatch, DecoderEvidence, MAX_DECODED_BOOK_ITEMS, MAX_DECODED_EVENTS,
    MarketDecoder, ProviderAggressorEvidence, ProviderBookChange, ProviderBookDeltaPayload,
    ProviderBookLevel, ProviderBookSide, ProviderBookSnapshotPayload, ProviderChecksumEvidence,
    ProviderDecimalLexeme, ProviderNormalizedObservation, ProviderObservationPayload,
    ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
    ProviderStatusEvidence, ProviderTimestampEvidence,
};
pub use extraction::{
    AvailabilityEvidence, DiscoveryBatch, DiscoveryRequest, DiscoveryRequestId, ExtractionBatch,
    ExtractionError, ExtractionRecord, ExtractionRequest, ExtractionRequestId, ExtractionSource,
    ExtractionSourceError, MAX_DISCOVERY_OBJECTS, MAX_EXTRACTION_BATCH_BYTES,
    MAX_EXTRACTION_RECORD_BYTES, MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
    SourceObject,
};
pub use health::{
    AuthorizationHealth, BudgetHealth, ConnectionLiveness, CoverageHealth, HealthErrorClass,
    MarketFreshness, SourceHealthError, SourceHealthSnapshot, SourceTimestampFreshness,
    TransportFreshness,
};
pub use live::{
    FrameId, FrameSessionBinding, LiveMarketSource, MAX_RAW_FRAME_BYTES, RawMarketFrame,
    RawMarketSink, SessionId, SinkError, SourceError, SourceMetadataProvider, TransportFrameKind,
    ValidatedRawMarketFrame,
};
pub use metadata::{
    AuthorizationGrant, AuthorizationMode, ChecksumAlgorithm, ChecksumBookScope,
    ChecksumValidationProfile, CoverageDomain, CoverageTopology, FreshnessPolicy,
    HistoricalCapability, InstrumentCoverage, InstrumentCoverageMembership,
    LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile, NetworkAccessPolicy,
    ProviderNumericPolicy, SemanticInterpretationProfile, SequenceValidationProfile,
    SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata, SourceMetadataError,
    SourceMetadataInput, SourceProtocolProfile,
};
pub use policy::{
    ApiEndpointRule, AuthorizedRequest, BackoffPolicy, BudgetDecision, BudgetPermit,
    BudgetPoolError, BudgetScope, BudgetUnavailableReason, EndpointDenialReason, EndpointPolicy,
    HttpClientProfile, HttpRequestBounds, MonotonicInstant, NetworkPolicyError, PathScope,
    ProviderBudgetPolicy, QueryParameterRule, QuerySensitivity, RedirectAuthorization, RetryAfter,
    SharedProviderBudget,
};
pub use registry::{
    AuthoritativeSourceRegistry, CurrentBatchIter, CurrentBatchKey, CurrentCoveragePolicy,
    CurrentDecodedProviderBatch, CurrentDecodedProviderBatches, CurrentFrameEvidence,
    CurrentHealthReporter, CurrentHealthUpdate, CurrentLivePolicy, CurrentObservationIter,
    CurrentProviderObservation, CurrentSourceAuthorityLease, CurrentSourceSession,
    CurrentStreamKey, InstrumentUniverseAttestation, RawFrameFactory, RegisteredSource,
    RegistryAuthorityState, RegistryError, ValidatedCurrentSourceAuthority, ValidatedLiveScope,
    ValidatedSourceSession,
};
