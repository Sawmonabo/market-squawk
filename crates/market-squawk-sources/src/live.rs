//! Object-safe live source and bounded raw-frame contracts.

use std::num::NonZeroU64;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use market_squawk_domain::{
    ConnectionGeneration, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::bounded::BoundedBytes;
use crate::{RawFrameFactory, SourceMetadata};

/// Maximum exact wire payload retained in one live frame.
pub const MAX_RAW_FRAME_BYTES: usize = 4 * 1024 * 1024;

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
    pub(crate) fn shared_allocation_charge(&self) -> Option<usize> {
        std::mem::size_of::<FrameSessionIdentity>()
            .checked_add(self.0.source_id.retained_bytes())
            .and_then(|bytes| {
                bytes.checked_add(
                    self.0
                        .metadata_revision
                        .as_source_identifier()
                        .retained_bytes(),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(self.0.session_id.as_source_identifier().retained_bytes())
            })
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
    received_at: Timestamp,
    transport: TransportFrameKind,
    payload: BoundedBytes<MAX_RAW_FRAME_BYTES>,
}

/// Opaque process-local proof that a raw frame belongs to a current registry session lease.
#[derive(Debug)]
pub struct ValidatedRawMarketFrame<'a> {
    frame: &'a RawMarketFrame,
}

impl<'a> ValidatedRawMarketFrame<'a> {
    pub(crate) const fn new(frame: &'a RawMarketFrame) -> Self {
        Self { frame }
    }

    /// Returns the exact bounded frame after current-session validation.
    pub const fn frame(&self) -> &'a RawMarketFrame {
        self.frame
    }
}

impl RawMarketFrame {
    pub(crate) fn try_from_parts(
        binding: FrameSessionBinding,
        frame_id: FrameId,
        received_at: Timestamp,
        transport: TransportFrameKind,
        payload: Bytes,
    ) -> Result<Self, SourceError> {
        let payload = BoundedBytes::try_from_bytes(payload)
            .map_err(|error| SourceError::FrameTooLarge { max: error.max })?;
        Ok(Self {
            binding,
            frame_id,
            received_at,
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
        self.received_at
    }

    /// Returns the transport frame kind.
    pub const fn transport(&self) -> TransportFrameKind {
        self.transport
    }

    /// Returns exact immutable payload bytes without a per-consumer copy.
    pub fn payload(&self) -> &Bytes {
        self.payload.as_bytes()
    }

    /// Returns normalized retained payload bytes; sliced oversized backing allocations are
    /// detached at construction.
    pub fn retained_payload_bytes(&self) -> usize {
        self.payload.retained_bytes()
    }

    /// Returns the shared immutable session binding.
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
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

    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .checked_add(std::mem::size_of::<FrameSessionIdentity>())
            .and_then(|bytes| bytes.checked_add(self.source_id().as_str().len()))
            .and_then(|bytes| {
                bytes.checked_add(
                    self.metadata_revision()
                        .as_source_identifier()
                        .as_str()
                        .len(),
                )
            })
            .and_then(|bytes| {
                bytes.checked_add(self.session_id().as_source_identifier().as_str().len())
            })
            .and_then(|bytes| bytes.checked_add(self.retained_payload_bytes()))
            .unwrap_or(usize::MAX)
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
            received_at: self.received_at,
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
    payload: &'a BoundedBytes<MAX_RAW_FRAME_BYTES>,
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
        Ok(Self {
            binding: FrameSessionBinding::new(
                wire.source_id,
                wire.metadata_revision,
                wire.session_id,
                wire.connection_generation,
            ),
            frame_id: wire.frame_id,
            received_at: wire.received_at,
            transport: wire.transport,
            payload: wire.payload,
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
}
