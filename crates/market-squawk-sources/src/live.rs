//! Object-safe live source and bounded raw-frame contracts.

use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    CaptureFrameFootprint, CapturePayload, CaptureRetainedComponent, CaptureRetainedSizeError,
    ConnectionGeneration, MAX_LIVE_CAPTURE_PAYLOAD_BYTES, MetadataRevision, SourceId,
    SourceIdentifier, Timestamp, checked_arc_value_allocation_bytes,
};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::authority_time::TrustedReceiptObservation;
use crate::bounded::BoundedBytes;
use crate::{RawFrameFactory, SourceMetadata};

/// Maximum exact wire payload retained in one live frame.
pub const MAX_RAW_FRAME_BYTES: usize = MAX_LIVE_CAPTURE_PAYLOAD_BYTES;

/// Bounded source-defined connection-session identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SessionId(SourceIdentifier);

impl SessionId {
    /// Constructs a typed session identity.
    pub const fn new(value: SourceIdentifier) -> Self {
        Self(value)
    }

    /// Returns the bounded source identifier.
    pub const fn as_source_identifier(&self) -> &SourceIdentifier {
        &self.0
    }
}

/// Transport-level frame type; provider semantics are decoded later.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportFrameKind {
    /// UTF-8-oriented text transport frame retained as exact bytes.
    Text,
    /// Opaque binary transport frame.
    Binary,
}

/// Nonzero per-generation raw-frame ordinal.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct FrameId(NonZeroU64);

impl FrameId {
    pub(crate) const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    /// Returns the nonzero generation-local ordinal.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the typed nonzero ordinal for dependency-neutral capture composition.
    pub const fn as_nonzero(self) -> NonZeroU64 {
        self.0
    }
}

#[derive(Debug, Eq, PartialEq)]
struct FrameSessionIdentity {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    session_id: SessionId,
    connection_generation: ConnectionGeneration,
}

/// Immutable O(1)-clone binding shared by every frame in one source connection generation.
///
/// This value is data identity, not registry authority. Only [`crate::CurrentSourceSession`] and
/// [`crate::ValidatedSourceSession`] establish current registry state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrameSessionBinding(Arc<FrameSessionIdentity>);

impl FrameSessionBinding {
    pub(crate) fn new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        session_id: SessionId,
        connection_generation: ConnectionGeneration,
    ) -> Self {
        Self(Arc::new(FrameSessionIdentity {
            source_id,
            metadata_revision,
            session_id,
            connection_generation,
        }))
    }

    /// Returns the source identity.
    pub fn source_id(&self) -> &SourceId {
        &self.0.source_id
    }

    /// Returns the metadata revision.
    pub fn metadata_revision(&self) -> &MetadataRevision {
        &self.0.metadata_revision
    }

    /// Returns the source-defined session identity.
    pub fn session_id(&self) -> &SessionId {
        &self.0.session_id
    }

    /// Returns the connection generation.
    pub fn connection_generation(&self) -> ConnectionGeneration {
        self.0.connection_generation
    }

    /// Returns whether two bindings share the same immutable allocation.
    pub fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Returns the exact allocation reachable through this shared identity once.
    ///
    /// Cloned bindings share the same allocation and must be deduplicated by their owning object
    /// graph before this charge is added.
    pub(crate) fn checked_shared_allocation_bytes(
        &self,
    ) -> Result<usize, CaptureRetainedSizeError> {
        let dynamic = self
            .0
            .source_id
            .retained_bytes()
            .checked_add(
                self.0
                    .metadata_revision
                    .as_source_identifier()
                    .retained_bytes(),
            )
            .and_then(|bytes| {
                bytes.checked_add(self.0.session_id.as_source_identifier().retained_bytes())
            })
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::SessionBinding,
            })?;
        checked_arc_value_allocation_bytes::<FrameSessionIdentity>(dynamic).map_err(|_| {
            CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::SessionBinding,
            }
        })
    }

    pub(crate) fn shared_allocation_charge(&self) -> Option<usize> {
        self.checked_shared_allocation_bytes().ok()
    }
}

/// Exact bounded live payload captured before provider decoding.
///
/// Provider sequence and exchange timestamps are intentionally absent: they are decoded evidence,
/// not raw transport identity. Cloning a frame shares immutable payload storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawMarketFrame {
    binding: FrameSessionBinding,
    frame_id: FrameId,
    receipt: RawFrameReceipt,
    transport: TransportFrameKind,
    payload: CapturePayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum RawFrameReceipt {
    Trusted(TrustedReceiptObservation),
    Untrusted(Timestamp),
}

impl RawFrameReceipt {
    const fn received_at(&self) -> Timestamp {
        match self {
            Self::Trusted(receipt) => receipt.received_at(),
            Self::Untrusted(received_at) => *received_at,
        }
    }

    const fn trusted(&self) -> Option<&TrustedReceiptObservation> {
        match self {
            Self::Trusted(receipt) => Some(receipt),
            Self::Untrusted(_) => None,
        }
    }
}

/// Opaque process-local proof that a raw frame belongs to a current registry session lease.
#[derive(Debug)]
pub struct ValidatedRawMarketFrame<'a> {
    frame: &'a RawMarketFrame,
    receipt: &'a TrustedReceiptObservation,
}

impl<'a> ValidatedRawMarketFrame<'a> {
    pub(crate) const fn new(
        frame: &'a RawMarketFrame,
        receipt: &'a TrustedReceiptObservation,
    ) -> Self {
        Self { frame, receipt }
    }

    /// Returns the exact bounded frame after current-session validation.
    pub const fn frame(&self) -> &'a RawMarketFrame {
        self.frame
    }

    pub(crate) const fn trusted_receipt(&self) -> &'a TrustedReceiptObservation {
        self.receipt
    }
}

impl RawMarketFrame {
    pub(crate) fn try_from_parts(
        binding: FrameSessionBinding,
        frame_id: FrameId,
        receipt: TrustedReceiptObservation,
        transport: TransportFrameKind,
        payload: Bytes,
    ) -> Result<Self, SourceError> {
        let payload = CapturePayload::try_from_live(&payload).map_err(|_error| {
            SourceError::FrameTooLarge {
                max: MAX_RAW_FRAME_BYTES,
            }
        })?;
        Ok(Self {
            binding,
            frame_id,
            receipt: RawFrameReceipt::Trusted(receipt),
            transport,
            payload,
        })
    }

    /// Returns the source identity.
    pub fn source_id(&self) -> &SourceId {
        self.binding.source_id()
    }

    /// Returns the exact metadata revision used by the source session.
    pub fn metadata_revision(&self) -> &MetadataRevision {
        self.binding.metadata_revision()
    }

    /// Returns the source-defined session identity.
    pub fn session_id(&self) -> &SessionId {
        self.binding.session_id()
    }

    /// Returns the nonzero connection generation.
    pub fn connection_generation(&self) -> ConnectionGeneration {
        self.binding.connection_generation()
    }

    /// Returns the exact nonzero generation-local frame ordinal.
    pub const fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Returns the local receive timestamp.
    pub const fn received_at(&self) -> Timestamp {
        self.receipt.received_at()
    }

    /// Returns the transport frame kind.
    pub const fn transport(&self) -> TransportFrameKind {
        self.transport
    }

    /// Returns exact immutable payload bytes without a per-consumer copy.
    pub fn payload(&self) -> &[u8] {
        self.payload.as_bytes()
    }

    /// Returns normalized retained payload bytes; sliced oversized backing allocations are
    /// detached at construction.
    pub fn retained_payload_bytes(&self) -> usize {
        self.payload.as_bytes().len()
    }

    /// Returns the shared immutable session binding.
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    pub(crate) const fn trusted_receipt(&self) -> Option<&TrustedReceiptObservation> {
        self.receipt.trusted()
    }

    #[cfg(test)]
    pub(crate) fn strip_trusted_receipt_for_test(&mut self) {
        self.receipt = RawFrameReceipt::Untrusted(self.received_at());
    }
}

impl market_squawk_domain::RawCaptureFrameView for RawMarketFrame {
    fn source_id(&self) -> &SourceId {
        self.source_id()
    }

    fn metadata_revision(&self) -> &MetadataRevision {
        self.metadata_revision()
    }

    fn session_identifier(&self) -> &SourceIdentifier {
        self.session_id().as_source_identifier()
    }

    fn connection_generation(&self) -> ConnectionGeneration {
        self.connection_generation()
    }

    fn frame_ordinal(&self) -> NonZeroU64 {
        self.frame_id().as_nonzero()
    }

    fn received_at(&self) -> Timestamp {
        self.received_at()
    }

    fn payload(&self) -> &[u8] {
        self.payload()
    }

    fn capture_payload(&self) -> &CapturePayload {
        &self.payload
    }

    fn checked_retained_footprint(
        &self,
    ) -> Result<CaptureFrameFootprint, CaptureRetainedSizeError> {
        let continuity = self
            .trusted_receipt()
            .map(TrustedReceiptObservation::continuity)
            .map_or(Ok(0), |continuity| {
                continuity.checked_shared_allocation_bytes()
            })?;
        let resident = self
            .binding
            .checked_shared_allocation_bytes()?
            .checked_add(continuity)
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            })?;
        CaptureFrameFootprint::try_new(
            std::mem::size_of::<Self>(),
            resident,
            self.payload.checked_retained_allocation_bytes()?,
        )
    }
}

impl Serialize for RawMarketFrame {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        RawMarketFrameSerializeWire {
            source_id: self.source_id(),
            metadata_revision: self.metadata_revision(),
            session_id: self.session_id(),
            connection_generation: self.connection_generation(),
            frame_id: self.frame_id,
            received_at: self.received_at(),
            transport: self.transport,
            payload: &self.payload,
        }
        .serialize(serializer)
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct RawMarketFrameSerializeWire<'a> {
    source_id: &'a SourceId,
    metadata_revision: &'a MetadataRevision,
    session_id: &'a SessionId,
    connection_generation: ConnectionGeneration,
    frame_id: FrameId,
    received_at: Timestamp,
    transport: TransportFrameKind,
    payload: &'a CapturePayload,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMarketFrameWire {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    session_id: SessionId,
    connection_generation: ConnectionGeneration,
    frame_id: FrameId,
    received_at: Timestamp,
    transport: TransportFrameKind,
    payload: BoundedBytes<MAX_RAW_FRAME_BYTES>,
}

impl<'de> Deserialize<'de> for RawMarketFrame {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RawMarketFrameWire::deserialize(deserializer)?;
        let payload = CapturePayload::try_from_live(wire.payload.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            binding: FrameSessionBinding::new(
                wire.source_id,
                wire.metadata_revision,
                wire.session_id,
                wire.connection_generation,
            ),
            frame_id: wire.frame_id,
            receipt: RawFrameReceipt::Untrusted(wire.received_at),
            transport: wire.transport,
            payload,
        })
    }
}

/// Nonblocking bounded sink used by a live source reader before decoding.
pub trait RawMarketSink: Send {
    /// Attempts to publish one exact raw frame without waiting for capacity.
    ///
    /// # Errors
    ///
    /// Saturation, closure, and capture-integrity failure are explicit and must invalidate or
    /// degrade the affected stream according to supervision policy.
    fn try_publish(&mut self, frame: RawMarketFrame) -> Result<(), SinkError>;
}

/// Immutable source metadata access shared by distinct adapter contracts.
pub trait SourceMetadataProvider: Send + Sync {
    /// Returns this adapter's immutable configured source metadata.
    fn metadata(&self) -> &SourceMetadata;
}

/// Object-safe live source contract with one boxed future per connection session.
pub trait LiveMarketSource: SourceMetadataProvider {
    /// Runs the source until cancellation or a typed terminal error.
    fn run<'a>(
        &'a mut self,
        frames: &'a mut RawFrameFactory,
        sink: &'a mut dyn RawMarketSink,
        cancellation: CancellationToken,
    ) -> BoxFuture<'a, Result<(), SourceError>>;
}

/// Nonblocking raw sink failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SinkError {
    /// Bounded sink has no capacity; the frame was not accepted.
    #[error("raw market sink is saturated")]
    Saturated,
    /// Sink is closed; the frame was not accepted.
    #[error("raw market sink is closed")]
    Closed,
    /// Capture path is already known incomplete for this generation.
    #[error("raw capture integrity is incomplete")]
    CaptureIncomplete,
}

/// Live source lifecycle or bounded-input failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SourceError {
    /// Exact transport frame exceeded the global byte ceiling.
    #[error("raw frame exceeds maximum size {max}")]
    FrameTooLarge {
        /// Maximum accepted exact payload bytes.
        max: usize,
    },
    /// Source configuration or protocol state was invalid.
    #[error("source protocol state is invalid")]
    InvalidProtocolState,
    /// Provider authorization failed.
    #[error("source authorization failed")]
    Unauthorized,
    /// Allowlisted network operation failed.
    #[error("source network operation failed")]
    Network,
    /// Bounded sink rejected a frame.
    #[error("raw market sink rejected a frame: {0}")]
    Sink(#[from] SinkError),
    /// Caller requested cancellation.
    #[error("source run was cancelled")]
    Cancelled,
    /// Provider refused or throttled access; supervision must apply budget policy.
    #[error("provider access is temporarily unavailable")]
    ProviderUnavailable,
    /// Per-generation frame ordinal exhausted; the session fails closed.
    #[error("source frame identity space exhausted")]
    FrameIdentityExhausted,
    /// The generation ended, rolled over, or was revoked before frame construction.
    #[error("source session is no longer current")]
    SessionNotCurrent,
    /// The registry clock source could not provide one sealed paired observation.
    #[error("source-owned trusted receipt time is unavailable")]
    TrustedTimeUnavailable,
    /// The registry-wide paired wall/monotonic continuity latch is permanently terminal.
    #[error("source-owned trusted receipt time is discontinuous")]
    TrustedTimeDiscontinuity,
}

#[cfg(test)]
mod tests {
    use market_squawk_domain::{
        ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier,
        checked_arc_value_allocation_bytes,
    };

    use super::{FrameSessionBinding, FrameSessionIdentity, SessionId};

    fn retained_string(value: &str, capacity: usize) -> String {
        let mut retained = String::with_capacity(capacity);
        retained.push_str(value);
        retained
    }

    #[test]
    fn frame_session_binding_charge_uses_actual_identifier_capacities()
    -> Result<(), Box<dyn std::error::Error>> {
        let binding = FrameSessionBinding::new(
            SourceId::try_from(retained_string("s", 97))?,
            MetadataRevision::new(SourceIdentifier::try_from(retained_string("r", 113))?),
            SessionId::new(SourceIdentifier::try_from(retained_string("q", 131))?),
            ConnectionGeneration::new(1)?,
        );
        let dynamic = binding
            .source_id()
            .retained_bytes()
            .checked_add(
                binding
                    .metadata_revision()
                    .as_source_identifier()
                    .retained_bytes(),
            )
            .and_then(|bytes| {
                bytes.checked_add(binding.session_id().as_source_identifier().retained_bytes())
            })
            .ok_or("binding dynamic test total overflowed")?;
        let expected = checked_arc_value_allocation_bytes::<FrameSessionIdentity>(dynamic)?;

        assert_eq!(binding.checked_shared_allocation_bytes()?, expected);
        assert!(dynamic > 3);
        Ok(())
    }
}
