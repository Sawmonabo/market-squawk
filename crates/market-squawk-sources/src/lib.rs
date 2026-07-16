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
