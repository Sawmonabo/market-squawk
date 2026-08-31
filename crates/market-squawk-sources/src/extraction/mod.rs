//! Bounded discovery and research extraction contracts.

mod batch;
mod capture;
mod contracts;
mod logical_publication;
mod native_lineage;
mod option_market;
mod revisions;

use std::num::NonZeroU64;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use market_squawk_domain::{EvidenceDigest, Timestamp};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::registry::ExtractionAuthority;
use crate::{
    AuthorizedRequest, BudgetDispatchDecision, BudgetPermit, BudgetReservation,
    BudgetUnavailableReason, HttpClientProfile, HttpRequestBounds, MonotonicInstant,
    NetworkPolicyError, RedirectAuthorization, SourceError, SourceMetadataProvider,
};

pub use batch::{ExtractionBatch, ExtractionBatchAccumulator, ExtractionContentIdentity};
pub use capture::{
    CompleteMarketBarHistoryV1, MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMP_BYTES,
    MAX_COMPLETE_MARKET_BAR_HISTORY_TIMESTAMPS, MAX_PROVIDER_CAPTURE_BYTES,
    MAX_PROVIDER_CAPTURE_PAGE_BYTES, MAX_PROVIDER_CAPTURE_PAGES,
    MAX_PROVIDER_EVENT_MICROBATCH_BYTES, MAX_PROVIDER_EVENT_MICROBATCH_FRAMES,
    MAX_PROVIDER_MARKET_EVENT_BATCH_BYTES, MAX_PROVIDER_MARKET_EVENT_BATCH_EVENTS,
    PROVIDER_MARKET_EVENT_SCHEMA_VERSION, ProviderCaptureBindingDigest,
    ProviderCaptureBindingLayout, ProviderCaptureComponentToken, ProviderCaptureComponentTokenSet,
    ProviderCaptureError, ProviderCaptureMaterial, ProviderCaptureMaterialSealError,
    ProviderCapturePageReceipt, ProviderCapturePhysicalClaimEvidenceRef,
    ProviderCaptureRequestGraphComponent, ProviderCaptureRowFrame, ProviderCaptureRowFrameEvidence,
    ProviderCaptureScope, ProviderCaptureSealExpectation, ProviderCaptureSealRequest,
    ProviderCaptureSemanticBinding, ProviderCaptureSetReceipt, ProviderCaptureTerminalDisposition,
    ProviderCompositeResponseEventBindingDigest, ProviderCompositeResponseEventRowCoordinate,
    ProviderEventMicrobatchBindingDigest, ProviderEventMicrobatchFrameReceipt,
    ProviderEventMicrobatchMaterial, ProviderEventMicrobatchReceipt,
    ProviderEventMicrobatchRowFrame, ProviderEventMicrobatchRowFrameEvidence,
    ProviderEventMicrobatchSealExpectation, ProviderEventMicrobatchToken, ProviderMarketEventBatch,
    ProviderMarketEventContentIdentity, ProviderMarketEventNativeLineageBatch,
    ProviderMarketEventNativeLineageRowEvidenceRef, ProviderOrderedCaptureSegments,
    ProviderPublicationBindingDigest, ProviderPublicationBindingKind,
    ProviderResponseMarketEventBindingDigest, ProviderResponseMarketEventRowFrameEvidence,
    ProviderWholeCaptureToken, RejoinedProviderCapture, SealedProviderCaptureBinding,
    SealedProviderCaptureMaterial, SealedProviderCaptureSetReceipt,
    SealedProviderCompositeResponseEventBinding, SealedProviderEventMicrobatchBinding,
    SealedProviderEventMicrobatchReceipt, SealedProviderPublicationBinding,
    SealedProviderResponseMarketEventBinding, SourceObjectCaptureIdentity,
    verify_provider_market_event_native_lineage_batch_evidence,
};
pub use contracts::{
    AvailabilityEvidence, CURRENT_RESEARCH_RECORD_SCHEMA, DiscoveryBatch, DiscoveryRequest,
    DiscoveryRequestId, ExtractionError, ExtractionRecord, ExtractionRequest, ExtractionRequestId,
    MAX_DISCOVERY_OBJECTS, MAX_EXTRACTION_BATCH_BYTES, MAX_EXTRACTION_RECORD_BYTES,
    MAX_EXTRACTION_RECORDS, MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, SourceObject,
    payload_matches_exact_evidence,
};
pub use logical_publication::{
    CanonicalPartitionExpectation, LOGICAL_PARTITION_FRAME_HEADER_BYTES, LogicalItemRange,
    LogicalObjectRole, LogicalPartitionFamily, LogicalPartitionSetAdmission,
    LogicalPartitionSetCheckpoint, MAX_PROVIDER_CANONICAL_PARTITIONS,
    MAX_PROVIDER_LOGICAL_CATALOG_BYTES, MAX_PROVIDER_LOGICAL_OBJECTS,
    MAX_PROVIDER_LOGICAL_PARTITIONS, PendingLogicalPartitionSet, ProviderLogicalPublicationError,
    ProviderLogicalTerminalInput, ProviderLogicalTerminalReceipt, SealedLogicalObjectInput,
    SealedLogicalPartitionClaim, SealedLogicalPartitionInput, SealedLogicalPartitionSet,
    SealedProviderLogicalPublicationBinding, StagedLogicalItemCoordinate,
};
pub use native_lineage::{
    MAX_PROVIDER_NATIVE_LINEAGE_BATCH_BYTES, MAX_PROVIDER_NATIVE_LINEAGE_ROW_BYTES,
    MAX_PROVIDER_NATIVE_LINEAGE_SIDECAR_BYTES, PROVIDER_NATIVE_LINEAGE_SCHEMA_VERSION,
    ProviderNativeLineageBatch, ProviderNativeLineageBatchBuilder,
    ProviderNativeLineageBatchSidecar, ProviderNativeLineageBatchSidecarEvidenceRef,
    ProviderNativeLineageError, ProviderNativeLineageImplementation, ProviderNativeLineageRow,
    ProviderNativeLineageRowEvidenceRef, ProviderNativeLineageSchema,
    verify_provider_native_lineage_batch_evidence,
};
pub use option_market::{
    MAX_OPTION_REQUEST_CONTRACTS, MAX_PROVIDER_OPTION_MARKET_BATCH_BYTES,
    MAX_PROVIDER_OPTION_MARKET_BATCH_ROWS, MAX_PROVIDER_OPTION_MARKET_ROW_BYTES,
    OptionExpirationRange, OptionMarketBatchDisposition, OptionMarketBatchKind,
    OptionMarketCompleteness, OptionMarketCompletenessInput, OptionMarketCursorState,
    OptionMarketRequestFilter, OptionMarketRequestScope, OptionMarketRequestScopeInput,
    OptionStrikeRange, PROVIDER_OPTION_MARKET_SCHEMA_VERSION, ProviderOptionMarketBatch,
    ProviderOptionMarketBindingDigest, ProviderOptionMarketContentIdentity,
    ProviderOptionMarketNativeLineageBatch, ProviderOptionMarketRowFrame,
    SealedProviderOptionMarketBinding,
};
pub use revisions::{
    CanonicalObservationFamily, CanonicalObservationPayload, ExtractionRevisionEvidence,
    ExtractionRevisionPlan, MAX_OBSERVED_REVISION_BATCH_BYTES, MAX_OBSERVED_REVISION_BATCH_RECORDS,
    MAX_OBSERVED_SEMANTIC_PAYLOAD_BYTES, MAX_OBSERVED_VERSION_EVIDENCE_BYTES,
    ObservedProviderOrder, ObservedRevisionAssignments, ObservedRevisionAuthority,
    ObservedRevisionBatch, ObservedRevisionError, ObservedRevisionRecord, ObservedSemanticPayload,
    ObservedVersionEvidence, ObservedVersionKind, PitV1CanonicalEncoder, PitV1EncodingControl,
    PitV1EncodingError,
};

/// Weighted provider-budget dimension settled after one dispatched response terminates.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRateWeightedDimension {
    /// Post-decompression response body bytes.
    ResponseBytes,
    /// Provider-originated response failures, with at most one unit per dispatched response.
    ProviderErrors,
}

/// One bounded weighted limit in a conjunctive provider-rate policy.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRateWeightedWindow {
    dimension: ProviderRateWeightedDimension,
    maximum_units: NonZeroU64,
    window_nanos: NonZeroU64,
    semantics: crate::BudgetWindowSemantics,
}

impl ProviderRateWeightedWindow {
    /// Constructs one checked weighted limit.
    ///
    /// # Errors
    ///
    /// Rejects a duration that cannot participate in durable wall-clock arithmetic.
    pub fn try_new(
        dimension: ProviderRateWeightedDimension,
        maximum_units: NonZeroU64,
        window_nanos: NonZeroU64,
        semantics: crate::BudgetWindowSemantics,
    ) -> Result<Self, crate::NetworkPolicyError> {
        if window_nanos.get() > i64::MAX as u64 {
            return Err(crate::NetworkPolicyError::InvalidBudgetPolicy);
        }
        Ok(Self {
            dimension,
            maximum_units,
            window_nanos,
            semantics,
        })
    }

    /// Returns the governed weighted dimension.
    pub const fn dimension(self) -> ProviderRateWeightedDimension {
        self.dimension
    }

    /// Returns the maximum units admitted within this window.
    pub const fn maximum_units(self) -> u64 {
        self.maximum_units.get()
    }

    /// Returns this window's duration in nanoseconds.
    pub const fn window_nanos(self) -> u64 {
        self.window_nanos.get()
    }

    /// Returns this window's reset semantics.
    pub const fn semantics(self) -> crate::BudgetWindowSemantics {
        self.semantics
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateWeightedWindowWire {
    dimension: ProviderRateWeightedDimension,
    maximum_units: NonZeroU64,
    window_nanos: NonZeroU64,
    semantics: crate::BudgetWindowSemantics,
}

impl<'de> Deserialize<'de> for ProviderRateWeightedWindow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderRateWeightedWindowWire::deserialize(deserializer)?;
        Self::try_new(
            wire.dimension,
            wire.maximum_units,
            wire.window_nanos,
            wire.semantics,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// Exact worst-case weighted capacity reserved for one transport dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRateDispatchClaim {
    maximum_response_bytes: Option<NonZeroU64>,
    provider_error_units: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateDispatchClaimWire {
    maximum_response_bytes: Option<NonZeroU64>,
    provider_error_units: u8,
}

impl<'de> Deserialize<'de> for ProviderRateDispatchClaim {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderRateDispatchClaimWire::deserialize(deserializer)?;
        Self::try_new(wire.maximum_response_bytes, wire.provider_error_units)
            .map_err(serde::de::Error::custom)
    }
}

impl ProviderRateDispatchClaim {
    /// Constructs an empty claim for a request-only provider policy.
    pub const fn request_only() -> Self {
        Self {
            maximum_response_bytes: None,
            provider_error_units: 0,
        }
    }

    /// Constructs the bounded claim required by the exact weighted policy.
    ///
    /// # Errors
    ///
    /// Rejects provider-error reservations above one unit per dispatched response.
    pub const fn try_new(
        maximum_response_bytes: Option<NonZeroU64>,
        provider_error_units: u8,
    ) -> Result<Self, ProviderRateContractError> {
        if provider_error_units > 1 {
            return Err(ProviderRateContractError::InvalidDispatchClaim);
        }
        Ok(Self {
            maximum_response_bytes,
            provider_error_units,
        })
    }

    /// Returns whether this claim carries no weighted reservation.
    pub const fn is_request_only(self) -> bool {
        self.maximum_response_bytes.is_none() && self.provider_error_units == 0
    }

    /// Returns the reserved response-byte ceiling, when governed.
    pub const fn maximum_response_bytes(self) -> Option<u64> {
        match self.maximum_response_bytes {
            Some(value) => Some(value.get()),
            None => None,
        }
    }

    /// Returns the reserved provider-error units.
    pub const fn provider_error_units(self) -> u8 {
        self.provider_error_units
    }
}

/// Closed terminal class for one dispatched provider response.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRateResponseClass {
    /// A complete response passed status, bounds, and payload validation.
    ValidatedSuccess,
    /// A complete response with exact bytes was abandoned solely by local control after receipt.
    KnownCompleteLocalAbort,
    /// A complete non-refusal HTTP response reported a provider-originated failure.
    HttpProviderError,
    /// A complete HTTP response refused service or imposed a rate limit.
    ProviderRefusal,
    /// A syntactically valid transport response contained a typed provider body error.
    ProviderBodyError,
    /// A complete response violated the provider protocol or payload contract.
    InvalidProviderResponse,
    /// Completion and exact received byte count are unknowable after transport failure or abort.
    AbandonedUnknown,
}

impl ProviderRateResponseClass {
    /// Returns the non-caller-controlled provider-error charge for this response.
    pub const fn provider_error_units(self) -> u8 {
        match self {
            Self::ValidatedSuccess | Self::KnownCompleteLocalAbort => 0,
            Self::HttpProviderError
            | Self::ProviderRefusal
            | Self::ProviderBodyError
            | Self::InvalidProviderResponse
            | Self::AbandonedUnknown => 1,
        }
    }
}

/// Parsed, bounded disposition of the provider's `Retry-After` field.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderRateRetryAfterDisposition {
    /// No field was present.
    Absent,
    /// A field was present but was malformed, unsupported, non-ASCII, zero, or oversized.
    MalformedOrUnsupported,
    /// A positive decimal-seconds field was parsed exactly.
    ValidRelativeDelay(NonZeroU64),
    /// A standard HTTP-date field was parsed exactly.
    ValidHttpDate(Timestamp),
}

impl ProviderRateRetryAfterDisposition {
    /// Parses a bounded HTTP `Retry-After` field without retaining raw header text.
    pub fn parse_http(field: Option<&[u8]>) -> Self {
        const MAX_FIELD_BYTES: usize = 128;
        const NANOS_PER_SECOND: u64 = 1_000_000_000;

        let Some(field) = field else {
            return Self::Absent;
        };
        if field.is_empty() || field.len() > MAX_FIELD_BYTES || !field.is_ascii() {
            return Self::MalformedOrUnsupported;
        }
        let Ok(field) = std::str::from_utf8(field) else {
            return Self::MalformedOrUnsupported;
        };
        if field.bytes().all(|byte| byte.is_ascii_digit()) {
            return field
                .parse::<u64>()
                .ok()
                .and_then(|seconds| seconds.checked_mul(NANOS_PER_SECOND))
                .and_then(NonZeroU64::new)
                .map_or(Self::MalformedOrUnsupported, Self::ValidRelativeDelay);
        }
        httpdate::parse_http_date(field)
            .ok()
            .and_then(system_time_to_timestamp)
            .map_or(Self::MalformedOrUnsupported, Self::ValidHttpDate)
    }

    pub(crate) const fn retry_after(self) -> Option<crate::RetryAfter> {
        match self {
            Self::ValidRelativeDelay(delay) => Some(crate::RetryAfter::Delay(delay)),
            Self::ValidHttpDate(deadline) => Some(crate::RetryAfter::AtWallClock(deadline)),
            Self::Absent | Self::MalformedOrUnsupported => None,
        }
    }

    const fn requires_fallback(self) -> bool {
        matches!(self, Self::Absent | Self::MalformedOrUnsupported)
    }
}

fn system_time_to_timestamp(value: SystemTime) -> Option<Timestamp> {
    const NANOS_PER_SECOND: u64 = 1_000_000_000;
    let unix_nanos = match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => u128::from(duration.as_secs())
            .checked_mul(u128::from(NANOS_PER_SECOND))
            .and_then(|nanos| nanos.checked_add(u128::from(duration.subsec_nanos())))
            .and_then(|nanos| i64::try_from(nanos).ok())?,
        Err(error) => u128::from(error.duration().as_secs())
            .checked_mul(u128::from(NANOS_PER_SECOND))
            .and_then(|nanos| nanos.checked_add(u128::from(error.duration().subsec_nanos())))
            .and_then(|nanos| i128::try_from(nanos).ok())
            .and_then(i128::checked_neg)
            .and_then(|nanos| i64::try_from(nanos).ok())?,
    };
    Some(Timestamp::from_unix_nanos(unix_nanos))
}

/// One consuming response-terminalization input bound to the dispatched permit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRateResponseSettlement {
    completed_response_bytes: u64,
    response_class: ProviderRateResponseClass,
    retry_after: ProviderRateRetryAfterDisposition,
    fallback_jitter_sample_basis_points: u16,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderRateResponseSettlementWire {
    completed_response_bytes: u64,
    response_class: ProviderRateResponseClass,
    retry_after: ProviderRateRetryAfterDisposition,
    fallback_jitter_sample_basis_points: u16,
}

impl<'de> Deserialize<'de> for ProviderRateResponseSettlement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ProviderRateResponseSettlementWire::deserialize(deserializer)?;
        Self::try_new(
            wire.completed_response_bytes,
            wire.response_class,
            wire.retry_after,
            wire.fallback_jitter_sample_basis_points,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl ProviderRateResponseSettlement {
    /// Constructs the conservative terminalization used when response completion is unknowable.
    pub const fn abandoned_unknown() -> Self {
        Self {
            completed_response_bytes: 0,
            response_class: ProviderRateResponseClass::AbandonedUnknown,
            retry_after: ProviderRateRetryAfterDisposition::Absent,
            fallback_jitter_sample_basis_points: 0,
        }
    }

    /// Constructs one checked terminal response classification.
    ///
    /// `completed_response_bytes` must be exact for every class except
    /// [`ProviderRateResponseClass::AbandonedUnknown`], for which the durable store replaces it
    /// with the dispatch reservation. Retry-After evidence is accepted for a provider refusal or
    /// retained protocol-invalid response. The fallback jitter sample is accepted only when a
    /// refusal did not contain a valid Retry-After instruction.
    ///
    /// # Errors
    ///
    /// Rejects contradictory response, Retry-After, or jitter evidence.
    pub const fn try_new(
        completed_response_bytes: u64,
        response_class: ProviderRateResponseClass,
        retry_after: ProviderRateRetryAfterDisposition,
        fallback_jitter_sample_basis_points: u16,
    ) -> Result<Self, ProviderRateContractError> {
        let retains_retry_after = matches!(
            response_class,
            ProviderRateResponseClass::ProviderRefusal
                | ProviderRateResponseClass::InvalidProviderResponse
        );
        if fallback_jitter_sample_basis_points > 10_000
            || (matches!(response_class, ProviderRateResponseClass::AbandonedUnknown)
                && completed_response_bytes != 0)
            || (!retains_retry_after
                && (!matches!(retry_after, ProviderRateRetryAfterDisposition::Absent)
                    || fallback_jitter_sample_basis_points != 0))
            || (matches!(response_class, ProviderRateResponseClass::ProviderRefusal)
                && !retry_after.requires_fallback()
                && fallback_jitter_sample_basis_points != 0)
            || (matches!(
                response_class,
                ProviderRateResponseClass::InvalidProviderResponse
            ) && fallback_jitter_sample_basis_points != 0)
        {
            return Err(ProviderRateContractError::InvalidResponseSettlement);
        }
        Ok(Self {
            completed_response_bytes,
            response_class,
            retry_after,
            fallback_jitter_sample_basis_points,
        })
    }

    /// Returns the exact completed bytes reported by the adapter.
    pub const fn completed_response_bytes(self) -> u64 {
        self.completed_response_bytes
    }

    /// Returns the closed terminal class.
    pub const fn response_class(self) -> ProviderRateResponseClass {
        self.response_class
    }

    /// Returns the derived zero-or-one provider-error charge.
    pub const fn provider_error_units(self) -> u8 {
        self.response_class.provider_error_units()
    }

    /// Returns the parsed Retry-After disposition.
    pub const fn retry_after(self) -> ProviderRateRetryAfterDisposition {
        self.retry_after
    }

    /// Returns the bounded fallback jitter sample.
    pub const fn fallback_jitter_sample_basis_points(self) -> u16 {
        self.fallback_jitter_sample_basis_points
    }
}

/// Shared provider-rate availability after atomic response terminalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderRateSettlementAvailability {
    /// No current rate deadline or administrative disablement remains.
    Available,
    /// Dispatch is unavailable until this inclusive wall-clock instant.
    WaitUntil(Timestamp),
    /// Dispatch requires an external state change.
    Unavailable(crate::BudgetUnavailableReason),
}

/// Durable receipt proving one exact dispatched permit was terminalized once.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderRateResponseSettlementReceipt {
    group_id: crate::ProviderRateGroupId,
    permit_id: crate::ProviderRatePermitId,
    settlement: ProviderRateResponseSettlement,
    charged_response_bytes: u64,
    availability: ProviderRateSettlementAvailability,
    consecutive_refusals: u32,
    state_version: NonZeroU64,
    state_digest: EvidenceDigest,
}

impl ProviderRateResponseSettlementReceipt {
    /// Constructs the exact receipt returned by a durable provider-rate store.
    ///
    /// # Errors
    ///
    /// Rejects an exact-response receipt whose charged byte count differs from the adapter's
    /// completed byte evidence. Unknown/abandoned completion is permitted to retain the larger
    /// dispatch reservation chosen by the store.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        group_id: crate::ProviderRateGroupId,
        permit_id: crate::ProviderRatePermitId,
        settlement: ProviderRateResponseSettlement,
        charged_response_bytes: u64,
        availability: ProviderRateSettlementAvailability,
        consecutive_refusals: u32,
        state_version: NonZeroU64,
        state_digest: EvidenceDigest,
    ) -> Result<Self, ProviderRateContractError> {
        if settlement.response_class() != ProviderRateResponseClass::AbandonedUnknown
            && settlement.completed_response_bytes() != charged_response_bytes
        {
            return Err(ProviderRateContractError::InvalidSettlementReceipt);
        }
        Ok(Self {
            group_id,
            permit_id,
            settlement,
            charged_response_bytes,
            availability,
            consecutive_refusals,
            state_version,
            state_digest,
        })
    }

    /// Returns the exact aggregate group.
    pub const fn group_id(self) -> crate::ProviderRateGroupId {
        self.group_id
    }

    /// Returns the consumed exact dispatched permit.
    pub const fn permit_id(self) -> crate::ProviderRatePermitId {
        self.permit_id
    }

    /// Returns the terminal response evidence.
    pub const fn settlement(self) -> ProviderRateResponseSettlement {
        self.settlement
    }

    /// Returns the response bytes charged to weighted windows.
    pub const fn charged_response_bytes(self) -> u64 {
        self.charged_response_bytes
    }

    /// Returns the aggregate availability after settlement.
    pub const fn availability(self) -> ProviderRateSettlementAvailability {
        self.availability
    }

    /// Returns the exact post-settlement refusal count.
    pub const fn consecutive_refusals(self) -> u32 {
        self.consecutive_refusals
    }

    /// Returns the exact post-settlement durable state version.
    pub const fn state_version(self) -> u64 {
        self.state_version.get()
    }

    /// Returns the exact post-settlement durable state digest.
    pub const fn state_digest(self) -> EvidenceDigest {
        self.state_digest
    }
}

/// Structural failure in a provider-rate dispatch or response-terminalization contract.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ProviderRateContractError {
    /// A dispatch attempted to reserve an invalid weighted claim.
    #[error("provider-rate dispatch claim is invalid")]
    InvalidDispatchClaim,
    /// A response terminalization contained contradictory evidence.
    #[error("provider-rate response settlement is invalid")]
    InvalidResponseSettlement,
    /// A durable response receipt did not match its terminal evidence.
    #[error("provider-rate response settlement receipt is invalid")]
    InvalidSettlementReceipt,
}

/// Object-safe research extraction contract with one boxed future per request.
pub trait ExtractionSource: SourceMetadataProvider + Sync {
    /// Discovers a bounded set of versioned source objects.
    ///
    /// Every provider HTTP request, including each pagination request, must acquire its own
    /// [`ExtractionRequestPermit`] from `authority`. The permit must be consumed immediately
    /// before sending the exact request target, and the resulting in-flight permit must be held
    /// until that response has been fully validated or discarded.
    fn discover(
        &self,
        authority: ExtractionAuthority,
        request: DiscoveryRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<DiscoveryBatch, ExtractionSourceError>>;

    /// Extracts one source object into a bounded normalized batch.
    ///
    /// Every provider HTTP request, including each pagination request, must acquire its own
    /// [`ExtractionRequestPermit`] from `authority`. The permit must be consumed immediately
    /// before sending the exact request target, and the resulting in-flight permit must be held
    /// until that response has been fully validated or discarded.
    fn extract(
        &self,
        authority: ExtractionAuthority,
        request: ExtractionRequest,
        cancellation: CancellationToken,
    ) -> BoxFuture<'_, Result<ExtractionBatch, ExtractionSourceError>>;
}

/// Failure to admit or retain one extraction operation under current registry authority.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ExtractionAuthorityError {
    /// Metadata was replaced, revoked, or its authoritative registry was dropped.
    #[error("extraction authority is no longer current")]
    NotCurrent,
    /// Authorization or coverage is no longer effective at sealed registry time.
    #[error("extraction authority is outside its effective interval")]
    NotEffective,
    /// The sealed registry clock could not provide a reading.
    #[error("extraction authority trusted time is unavailable")]
    TrustedTimeUnavailable,
    /// Registry authority-time continuity is permanently invalid.
    #[error("extraction authority trusted time is discontinuous")]
    TrustedTimeDiscontinuous,
    /// Local-only source metadata denies network access.
    #[error("extraction authority denies network access")]
    NetworkDenied,
    /// The exact target or response violated the metadata-bound network policy.
    #[error("extraction network policy rejected the operation: {0}")]
    NetworkPolicy(#[from] NetworkPolicyError),
    /// A one-use request admission was presented for a different exact target.
    #[error("extraction request admission does not match the exact authorized target")]
    RequestTargetMismatch,
    /// Remote source metadata did not retain a registry-coordinated provider budget.
    #[error("extraction provider budget is not configured")]
    BudgetNotConfigured,
    /// Shared request capacity is unavailable until the inclusive monotonic deadline.
    #[error("extraction provider budget is cooling down")]
    BudgetWaitUntil {
        /// Process-local inclusive retry deadline.
        deadline: MonotonicInstant,
    },
    /// Shared provider-budget state is terminally unavailable.
    #[error("extraction provider budget is unavailable: {reason:?}")]
    BudgetUnavailable {
        /// Exact fail-closed budget reason.
        reason: BudgetUnavailableReason,
    },
}

/// Non-clone request admission retaining current authority and one in-flight budget reservation.
pub struct ExtractionRequestPermit {
    authority: ExtractionAuthority,
    authorization: AuthorizedRequest,
    budget: BudgetReservation,
    redirects_followed: u8,
}

impl std::fmt::Debug for ExtractionRequestPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExtractionRequestPermit")
            .field("authority", &self.authority)
            .field(
                "contains_sensitive_query",
                &self.authorization.contains_sensitive_query(),
            )
            .finish_non_exhaustive()
    }
}

impl ExtractionRequestPermit {
    pub(crate) const fn new(
        authority: ExtractionAuthority,
        authorization: AuthorizedRequest,
        budget: BudgetReservation,
    ) -> Self {
        Self {
            authority,
            authorization,
            budget,
            redirects_followed: 0,
        }
    }

    /// Returns redacted sensitivity metadata for the authorized exact target.
    pub const fn authorization(&self) -> AuthorizedRequest {
        self.authorization
    }

    /// Revalidates currentness and effective time during paged or streamed response handling.
    pub fn validate_current(&self) -> Result<(), ExtractionAuthorityError> {
        self.authority.validate_current()
    }

    /// Returns hardened HTTP client construction requirements bound to the registered metadata.
    pub fn client_profile(&self) -> Result<HttpClientProfile, ExtractionAuthorityError> {
        self.validate_current()?;
        Ok(self.endpoint_policy()?.client_profile())
    }

    /// Returns request deadlines, redirect limits, and response-size bounds.
    pub fn request_bounds(&self) -> Result<HttpRequestBounds, ExtractionAuthorityError> {
        self.validate_current()?;
        Ok(self.endpoint_policy()?.request_bounds())
    }

    /// Consumes this one-use admission for the exact final target immediately before HTTP send.
    pub fn authorize_send(
        self,
        target: &str,
    ) -> Result<InFlightExtractionRequest, ExtractionAuthorityError> {
        self.validate_current()?;
        if !self.authorization.matches_exact_target(target) {
            return Err(ExtractionAuthorityError::RequestTargetMismatch);
        }
        let maximum_response_bytes = NonZeroU64::new(self.request_bounds()?.max_response_bytes())
            .ok_or(ExtractionAuthorityError::BudgetUnavailable {
            reason: BudgetUnavailableReason::StateCorrupt,
        })?;
        let budget = match self
            .budget
            .commit_dispatch_with_response_bound(maximum_response_bytes)
        {
            BudgetDispatchDecision::Ready(permit) => permit,
            BudgetDispatchDecision::WaitUntil(deadline) => {
                return Err(ExtractionAuthorityError::BudgetWaitUntil { deadline });
            }
            BudgetDispatchDecision::Unavailable(reason) => {
                return Err(ExtractionAuthorityError::BudgetUnavailable { reason });
            }
        };
        self.authority.validate_current()?;
        Ok(InFlightExtractionRequest {
            authority: self.authority,
            authorization: self.authorization,
            budget,
            redirects_followed: self.redirects_followed,
        })
    }

    fn endpoint_policy(&self) -> Result<&crate::EndpointPolicy, ExtractionAuthorityError> {
        match self.authority.metadata().network_policy() {
            crate::NetworkAccessPolicy::Allowlisted(policy) => Ok(policy),
            crate::NetworkAccessPolicy::Denied => Err(ExtractionAuthorityError::NetworkDenied),
        }
    }

    /// Cancels before send, releasing concurrency without consuming a request window.
    pub fn release(self) {
        self.budget.release();
    }
}

/// One exact, already-authorized provider request whose in-flight slot spans response handling.
pub struct InFlightExtractionRequest {
    authority: ExtractionAuthority,
    authorization: AuthorizedRequest,
    budget: BudgetPermit,
    redirects_followed: u8,
}

impl std::fmt::Debug for InFlightExtractionRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InFlightExtractionRequest")
            .field("authority", &self.authority)
            .field(
                "contains_sensitive_query",
                &self.authorization.contains_sensitive_query(),
            )
            .finish_non_exhaustive()
    }
}

impl InFlightExtractionRequest {
    /// Revalidates currentness during streamed response handling.
    pub fn validate_current(&self) -> Result<(), ExtractionAuthorityError> {
        self.authority.validate_current()
    }

    /// Enforces the registered response-size ceiling before further buffering.
    pub fn validate_response_size(&self, size: u64) -> Result<(), ExtractionAuthorityError> {
        self.validate_current()?;
        let policy = match self.authority.metadata().network_policy() {
            crate::NetworkAccessPolicy::Allowlisted(policy) => policy,
            crate::NetworkAccessPolicy::Denied => {
                return Err(ExtractionAuthorityError::NetworkDenied);
            }
        };
        policy.validate_response_size(size)?;
        self.validate_current()
    }

    /// Atomically terminalizes this exact weighted provider response.
    ///
    /// Complete response classes are checked against the registered response-size ceiling before
    /// the exact permit is consumed. Unknown/abandoned completion deliberately bypasses that
    /// assertion because the durable store charges the pessimistic dispatch reservation instead.
    /// The returned receipt binds the permit, terminal evidence, charged units, and exact durable
    /// state revision.
    ///
    /// # Errors
    ///
    /// Fails when extraction authority is stale, the complete response exceeds policy, the
    /// request was not dispatched with weighted shared authority, or terminalization cannot be
    /// persisted exactly once.
    pub fn settle_response(
        self,
        settlement: ProviderRateResponseSettlement,
    ) -> Result<ProviderRateResponseSettlementReceipt, ExtractionAuthorityError> {
        self.validate_current()?;
        if settlement.response_class() != ProviderRateResponseClass::AbandonedUnknown {
            self.validate_response_size(settlement.completed_response_bytes())?;
        }
        let authority = self.authority.clone();
        let receipt = self
            .budget
            .settle_response(settlement)
            .map_err(|reason| ExtractionAuthorityError::BudgetUnavailable { reason })?;
        authority.validate_current()?;
        Ok(receipt)
    }

    /// Applies one provider HTTP `Retry-After` response to this request's shared allocation.
    ///
    /// Missing or malformed fields use the existing capped refusal backoff. Valid fields retain
    /// their provider-supplied deadline, and instructions beyond configured policy fail closed.
    /// The operation consumes the completed in-flight response so one response cannot apply the
    /// refusal more than once, and releases its concurrency slot on return. No provider-budget
    /// admission capability is exposed to the adapter.
    ///
    /// # Errors
    ///
    /// Fails when this request's extraction authority is stale, the coordinated budget is absent,
    /// persistence or budget state is unavailable, or the refusal terminally violates policy.
    pub fn apply_retry_after_header(
        self,
        field: Option<&[u8]>,
        fallback_jitter_sample_basis_points: u16,
    ) -> Result<MonotonicInstant, ExtractionAuthorityError> {
        self.validate_current()?;
        match self
            .authority
            .apply_retry_after_header(field, fallback_jitter_sample_basis_points)?
        {
            crate::BudgetDecision::WaitUntil(deadline) => Ok(deadline),
            crate::BudgetDecision::Unavailable(reason) => {
                Err(ExtractionAuthorityError::BudgetUnavailable { reason })
            }
            crate::BudgetDecision::Ready(permit) => {
                permit.release();
                Err(ExtractionAuthorityError::BudgetUnavailable {
                    reason: BudgetUnavailableReason::StateCorrupt,
                })
            }
        }
    }

    /// Records one completely handled successful provider response on the shared allocation.
    ///
    /// Callers must invoke this only after the response status, bounds, and payload have all been
    /// validated. The operation consumes the in-flight request so one response cannot reset
    /// refusal escalation more than once, and releases its concurrency slot on return. A success
    /// does not erase a provider-directed cooldown established by another in-flight response.
    ///
    /// # Errors
    ///
    /// Fails when this request's extraction authority is stale, the coordinated budget is absent,
    /// or shared provider-budget state cannot durably record the successful response.
    pub fn record_success(self) -> Result<(), ExtractionAuthorityError> {
        self.authority.record_success()
    }

    /// Completes one redirect response and admits the next exact request hop.
    ///
    /// The current in-flight slot is released before the next hop reserves a distinct shared
    /// provider-budget request. The returned permit retains the same registry authority, exact
    /// target binding, bounded redirect count, and sensitive-header forwarding decision.
    ///
    /// # Errors
    ///
    /// Rejects a mismatched previous target, a denied or cross-origin target, a redirect beyond
    /// the configured chain limit, stale authority, or unavailable budget for the next hop.
    pub fn authorize_redirect_from(
        self,
        previous: &str,
        target: &str,
        carried_sensitive_headers: bool,
    ) -> Result<ExtractionRedirectPermit, ExtractionAuthorityError> {
        self.validate_current()?;
        if !self.authorization.matches_exact_target(previous) {
            return Err(ExtractionAuthorityError::RequestTargetMismatch);
        }
        let policy = match self.authority.metadata().network_policy() {
            crate::NetworkAccessPolicy::Allowlisted(policy) => policy,
            crate::NetworkAccessPolicy::Denied => {
                return Err(ExtractionAuthorityError::NetworkDenied);
            }
        };
        let redirect_count = self.redirects_followed.saturating_add(1);
        let max_redirects = policy.request_bounds().max_redirects();
        if redirect_count > max_redirects {
            return Err(ExtractionAuthorityError::NetworkPolicy(
                NetworkPolicyError::TooManyRedirects {
                    actual: usize::from(redirect_count),
                    max: max_redirects,
                },
            ));
        }
        let redirect =
            policy.authorize_redirect_from(previous, target, carried_sensitive_headers)?;
        let authority = self.authority.clone();
        self.budget.release();
        let mut request = authority.try_network_request(target)?;
        request.redirects_followed = redirect_count;
        Ok(ExtractionRedirectPermit { request, redirect })
    }

    /// Explicitly abandons response handling and releases the in-flight slot.
    ///
    /// Weighted provider-rate authority conservatively charges the pending maximum byte/error
    /// claim. Use [`Self::settle_response`] whenever complete response evidence is available.
    pub fn release(self) {
        self.budget.release();
    }
}

/// Admitted next hop in a bounded redirect chain with its sensitive-header decision.
pub struct ExtractionRedirectPermit {
    request: ExtractionRequestPermit,
    redirect: RedirectAuthorization,
}

impl std::fmt::Debug for ExtractionRedirectPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExtractionRedirectPermit")
            .field("request", &self.request)
            .field("redirect", &self.redirect)
            .finish_non_exhaustive()
    }
}

impl ExtractionRedirectPermit {
    /// Returns the policy decision for forwarding sensitive headers to this exact next hop.
    pub const fn redirect_authorization(&self) -> RedirectAuthorization {
        self.redirect
    }

    /// Consumes this one-use redirect admission immediately before sending the exact target.
    pub fn authorize_send(
        self,
        target: &str,
    ) -> Result<InFlightExtractionRequest, ExtractionAuthorityError> {
        self.request.authorize_send(target)
    }

    /// Cancels the redirect before send without consuming a request window.
    pub fn release(self) {
        self.request.release();
    }
}

/// Adapter-facing extraction failure preserving transport and contract classes.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ExtractionSourceError {
    /// Source transport/lifecycle failure.
    #[error("source extraction transport failed: {0}")]
    Source(#[from] SourceError),
    /// Bounded extraction contract failure.
    #[error("source extraction contract failed: {0}")]
    Contract(#[from] ExtractionError),
    /// Registry-minted extraction authority rejected or expired.
    #[error("source extraction authority failed: {0}")]
    Authority(#[from] ExtractionAuthorityError),
    /// Request deadline elapsed.
    #[error("source extraction deadline elapsed")]
    DeadlineExceeded,
    /// Cancellation was requested.
    #[error("source extraction was cancelled")]
    Cancelled,
}
