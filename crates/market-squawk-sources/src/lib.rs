//! Fail-closed source metadata, runtime contracts, and lawful provider access policy.
//!
//! The types in this crate deliberately keep source registration authority separate from
//! serializable metadata and health evidence. Live adapters and extraction adapters share source
//! identity contracts, but they do not share one oversized adapter trait.

mod authority_time;
mod bounded;
mod capture;
mod checksum;
mod decoder;
mod direct_order_book;
mod extraction;
mod health;
mod http_capture;
mod live;
mod metadata;
mod normalization;
mod onboarding;
mod order;
mod policy;
mod registry;
mod tls;

/// Conservative charge for the allocation metadata every `Arc<T>` retains independently of `T`.
///
/// Rust does not expose the standard library's control-block layout. Two atomic `usize` counters
/// plus worst-case alignment padding is a stable, allocator-metadata-independent upper charge for
/// the language-visible control block. Callers add `size_of::<T>()` and `T`'s owned allocations.
pub(crate) const fn conservative_arc_control_block_charge<T>() -> usize {
    let value_alignment = std::mem::align_of::<T>();
    let counter_alignment = std::mem::align_of::<usize>();
    let allocation_alignment = if value_alignment > counter_alignment {
        value_alignment
    } else {
        counter_alignment
    };
    (2 * std::mem::size_of::<usize>()) + (allocation_alignment - 1)
}

pub use capture::{
    CaptureAdmissionError, CaptureAdmissionIssuer, CaptureAdmissionReceipt,
    CaptureDegradationCapability, CaptureGenerationCapabilities, CaptureGenerationHealth,
    CaptureGenerationLease, CaptureInitializationControl,
};
pub use checksum::{
    ChecksumValidationError, ExactChecksumLevel, KRAKEN_V2_CANONICALIZATION_ID, KRAKEN_V2_SCOPE_ID,
    ResolvedChecksumValidator, kraken_v2_crc32,
};
pub use decoder::{
    ControlFrameKind, DecodeError, DecodeInternalError, DecodeOutcome, DecodedControlFrame,
    DecodedIgnoredFrame, DecodedProviderBatch, DecodedQuarantineAction, DecodedRecoveryAction,
    DecoderEvidence, IgnoredFrameReason, MAX_DECODED_BOOK_ITEMS, MAX_DECODED_EVENTS, MarketDecoder,
    ProviderAggressorEvidence, ProviderBookChange, ProviderBookDeltaPayload, ProviderBookLevel,
    ProviderBookSide, ProviderBookSnapshotPayload, ProviderChecksumEvidence, ProviderDecimalLexeme,
    ProviderNormalizedObservation, ProviderObservationPayload, ProviderPrice, ProviderQuantity,
    ProviderSequenceEvidence, ProviderSnapshotEvidence, ProviderStatusEvidence,
    ProviderTimestampEvidence, QuarantineReason, ResynchronizationReason,
};
pub use direct_order_book::{
    DirectBookLimits, DirectOrderBook, DirectOrderBookError, DirectPublishedBook,
    DirectPublishedLevel, DirectSyncPhase,
};
pub use extraction::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, CanonicalObservationFamily,
    CanonicalObservationPayload, CompleteMarketBarHistoryV1, DiscoveryBatch, DiscoveryRequest,
    DiscoveryRequestId, ExtractionAuthorityError, ExtractionBatch, ExtractionBatchAccumulator,
    ExtractionContentIdentity, ExtractionError, ExtractionRecord, ExtractionRedirectPermit,
    ExtractionRequest, ExtractionRequestId, ExtractionRequestPermit, ExtractionRevisionEvidence,
    ExtractionRevisionPlan, ExtractionSource, ExtractionSourceError, InFlightExtractionRequest,
    MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMP_BYTES, MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS,
    MAX_DISCOVERY_OBJECTS, MAX_EXTRACTION_BATCH_BYTES, MAX_EXTRACTION_RECORD_BYTES,
    MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES,
    MAX_OBSERVED_REVISION_BATCH_BYTES, MAX_OBSERVED_REVISION_BATCH_RECORDS,
    MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES, MAX_OBSERVED_VERSION_EVIDENCE_BYTES,
    MAX_PROVIDER_CAPTURE_BYTES, MAX_PROVIDER_CAPTURE_PAGE_BYTES, MAX_PROVIDER_CAPTURE_PAGES,
    ObservedProviderOrder, ObservedRevisionAssignments, ObservedRevisionAuthority,
    ObservedRevisionBatch, ObservedRevisionError, ObservedRevisionRecord, ObservedSemanticPayload,
    ObservedVersionEvidence, ObservedVersionKind, PitV1CanonicalEncoder, PitV1EncodingControl,
    PitV1EncodingError, ProviderCaptureError, ProviderCaptureMaterial,
    ProviderCaptureMaterialSealError, ProviderCapturePageReceipt,
    ProviderCaptureRequestGraphComponent, ProviderCaptureSemanticBinding,
    ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition, SealedProviderCaptureSetReceipt,
    SourceObject, SourceObjectCaptureIdentity, payload_matches_exact_evidence,
};
pub use health::{
    AuthorizationHealth, BudgetHealth, ConnectionLiveness, CoverageHealth, HealthErrorClass,
    MarketFreshness, SourceHealthError, SourceHealthSnapshot, SourceTimestampFreshness,
    TransportFreshness,
};
pub use http_capture::{
    HttpCaptureMethod, HttpResponseSegmentReceipt, SegmentedHttpCaptureError,
    SegmentedHttpResponseBuilder, SegmentedHttpResponseCapture, SegmentedHttpResponseReader,
    SegmentedHttpResponseReceipt,
};
pub use live::{
    FrameId, FrameSessionBinding, LiveMarketSource, MAX_RAW_FRAME_BYTES, RawMarketFrame,
    RawMarketSink, SessionId, SinkError, SourceError, SourceMetadataProvider, TransportFrameKind,
    ValidatedRawMarketFrame,
};
pub use metadata::{
    AuthorizationGrant, AuthorizationMode, AuthorizationSubjectResolutionError,
    AuthorizationSubjectResolver, ChecksumAlgorithm, ChecksumBookScope, ChecksumValidationProfile,
    CoverageDomain, CoverageTopology, FreshnessPolicy, HistoricalCapability, InstrumentCoverage,
    InstrumentCoverageMembership, LiveCoverageDeclaration, LiveCoverageRule, LiveProtocolProfile,
    NetworkAccessPolicy, ProviderNumericPolicy, SemanticInterpretationProfile,
    SequenceValidationProfile, SourceCapabilities, SourceClass, SourceCoverage, SourceMetadata,
    SourceMetadataError, SourceMetadataInput, SourceProtocolProfile,
};
pub use normalization::{
    NormalizationError, normalize_delta_quantity, normalize_positive_quantity, normalize_price,
};
pub use onboarding::{
    ALPACA_BASIC_MARKET_DATA_SURFACE_ID, ALPACA_PAPER_IEX_DOCTOR_RECEIPT_SCHEMA,
    AlpacaDoctorAdditionalCapability, AlpacaDoctorBatchObservation,
    AlpacaDoctorCalendarObservation, AlpacaDoctorCapabilityEvidence, AlpacaDoctorCredentialRealm,
    AlpacaDoctorHistoricalObservation, AlpacaDoctorHistoricalPageEvidence,
    AlpacaDoctorHttpEvidence, AlpacaDoctorProbeEvidence, AlpacaDoctorQuoteObservation,
    AlpacaDoctorRateEvidence, AlpacaDoctorStreamObservation, AlpacaPaperIexDoctorReceiptInput,
    AlpacaPaperIexDoctorReceiptV1, AlpacaRateLimitField, AlpacaRetryAfterEvidence,
    AuthorityBindings, AuthoritySet, AuthorityVerification, AuthorityVerificationInput,
    CapabilityRegistrationOutcome, CredentialGenerationState, CredentialKind, DataUseOperation,
    DataUseRight, EvidenceBinding, FRED_ALFRED_API_SURFACE_ID, HumanBoundary, LifecycleSupport,
    LocalDeletionOutcome, MAX_ALPACA_PAPER_IEX_DOCTOR_RECEIPT_BYTES,
    MAX_PROVIDER_PUBLIC_CONFIGURATION_BYTES, MAX_PROVIDER_PUBLIC_CONFIGURATION_FIELDS,
    OnboardingEvent, OnboardingEventKind, OnboardingLifecycle, OnboardingState,
    OnboardingStateError, OperationAdmission, ProbeTransport, ProfileActivationMode,
    ProfileEvidence, ProfileReleaseState, ProviderCapability, ProviderCapabilityError,
    ProviderCapabilityInput, ProviderCapabilityRegistry, ProviderCapabilityRevision,
    ProviderOnboardingProfile, ProviderProfileError, ProviderProfileRegistry,
    ProviderPublicConfiguration, PublicConfigurationError, RatePolicyDescriptor,
    RemoteRevocationOutcome, Requirement, RightsAdmissionState, RuntimeCapabilityDisposition,
    RuntimeCapabilityObservation, RuntimeProviderCapability, RuntimeVerificationContext,
    RuntimeVerificationDigestV1, RuntimeVerificationEvidence, RuntimeVerificationEvidenceError,
    SecretStoreClearOutcome, SetupMode, TREASURY_DAILY_RATES_PROBE_YEAR, VerificationProbe,
    ZeroFeeStatus, built_in_provider_profiles,
};
pub use order::{
    ProviderCursorOnlyReason, ProviderOrderChangeReason, ProviderOrderEvent,
    ProviderOrderEventError, ProviderOrderEventKind, ProviderOrderRecord,
};
pub use policy::{
    ApiEndpointRule, AuthorizedRequest, BackoffPolicy, BudgetDecision, BudgetDispatchDecision,
    BudgetPermit, BudgetPermitLease, BudgetPoolError, BudgetReservation, BudgetReservationDecision,
    BudgetScope, BudgetUnavailableReason, BudgetWindowSemantics, EndpointDenialReason,
    EndpointPolicy, HttpClientProfile, HttpRequestBounds, MonotonicInstant, NetworkPolicyError,
    PathScope, PreparedProviderRateRegistrationBatch, ProviderBudgetPolicy, ProviderBudgetWindow,
    ProviderRateAuthority, ProviderRateCollisionIdentity, ProviderRateCollisionKind,
    ProviderRateDeclaration, ProviderRateDispatchDecision, ProviderRateGroupId,
    ProviderRatePermitId, ProviderRateRegistration, ProviderRateReservationDecision,
    ProviderRateReservationId, ProviderRateRunId, ProviderRateStore, ProviderRateStoreError,
    QueryParameterRule, QuerySensitivity, RedirectAuthorization, RetryAfter, SharedProviderBudget,
    apply_http_retry_after,
};
pub use registry::{
    ActiveLiveSourceGeneration, AuthoritativeSourceRegistry, CapturedDecodedProviderBatch,
    CurrentBatchIter, CurrentBatchKey, CurrentCoveragePolicy, CurrentDecodedProviderBatch,
    CurrentDecodedProviderBatches, CurrentFrameEvidence, CurrentHealthRecording,
    CurrentHealthReporter, CurrentHealthUnqualification, CurrentHealthUpdate, CurrentLivePolicy,
    CurrentObservationIter, CurrentProviderObservation, CurrentSourceAuthorityLease,
    CurrentSourceSession, CurrentStreamKey, ExtractionAuthority, FrameSessionLease,
    InstrumentUniverseAttestation, LiveSourceGeneration, ProviderBackoffAuthority,
    ProviderBackoffDecision, ProviderBackoffError, RESEARCH_SOURCE_AUTHORITY_DIRECTORY,
    RawFrameFactory, RegisteredSource, RegistryAuthorityState, RegistryError,
    SessionControlDisposition, SessionIgnoredDisposition, SessionQuarantineDisposition,
    SessionRecoveryDisposition, ValidatedCurrentSourceAuthority, ValidatedLiveScope,
    ValidatedSessionDecodeOutcome, ValidatedSourceSession,
};
pub use tls::{TlsProviderCapability, TlsProviderError, install_ring_tls_provider};

#[cfg(test)]
mod allocation_charge_tests {
    use std::sync::atomic::AtomicU8;

    use super::conservative_arc_control_block_charge;

    #[repr(align(64))]
    struct OverAligned(u8);

    #[test]
    fn arc_control_charge_covers_small_atomic_and_over_aligned_layouts() {
        let counter_bytes = 2 * std::mem::size_of::<usize>();
        let counter_padding = std::mem::align_of::<usize>() - 1;
        assert_eq!(
            conservative_arc_control_block_charge::<u8>(),
            counter_bytes + counter_padding
        );
        assert!(
            conservative_arc_control_block_charge::<AtomicU8>() >= counter_bytes + counter_padding
        );
        assert_eq!(
            conservative_arc_control_block_charge::<OverAligned>(),
            counter_bytes + std::mem::align_of::<OverAligned>() - 1
        );
        assert_eq!(std::mem::size_of::<OverAligned>(), 64);
        let OverAligned(value) = OverAligned(7);
        assert_eq!(value, 7);
    }
}
