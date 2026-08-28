//! One-owner Schwab Streamer execution with bounded reconnect and exact payload microbatches.

use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt as _, StreamExt as _};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, SourceIdentifier};
use market_squawk_platform::RawCaptureRecord;
use market_squawk_sources::{
    ProviderCaptureSealRequest, ProviderEventMicrobatchMaterial,
    ProviderEventMicrobatchSealExpectation, ProviderEventMicrobatchToken,
    SealedProviderCaptureMaterial,
};
use sha2::{Digest as _, Sha256};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{
    Error as WebSocketError, Message, error::CapacityError, protocol::WebSocketConfig,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    ConnectionGeneration, ConnectionState, DesiredStateController, ParseBounds, ParsedNative,
    SchwabAdapterError, StreamerAdmission, StreamerBootstrap, StreamerFrame, StreamerResponseCode,
    StreamerSubscription, TransientStreamerRequest, parse_streamer_frame,
};

use super::{
    AccessTokenAdmission, AccessTokenGeneration, SchwabAccessTokenSource, SchwabCaptureCoordinates,
    SchwabTransportError, SchwabTransportTelemetry, StreamerTransportBounds, TransientAccessToken,
    hash_frame, hash_observation, unix_millis, unix_seconds,
};

/// Exact application payload kind delivered by the WebSocket implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RawStreamerFrameKind {
    Text,
    Binary,
    Ping,
    Pong,
}

impl RawStreamerFrameKind {
    const fn digest_tag(self) -> u8 {
        match self {
            Self::Text => 1,
            Self::Binary => 2,
            Self::Ping => 3,
            Self::Pong => 4,
        }
    }
}

/// One exact bounded WebSocket application payload and local observation receipt.
#[derive(Eq, PartialEq)]
pub struct RawStreamerFrame {
    generation: ConnectionGeneration,
    ordinal: NonZeroU64,
    kind: RawStreamerFrameKind,
    received_at_unix_millis: u64,
    payload_sha256: [u8; 32],
    payload: Bytes,
}

impl fmt::Debug for RawStreamerFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawStreamerFrame")
            .field("generation", &self.generation)
            .field("ordinal", &self.ordinal)
            .field("kind", &self.kind)
            .field("received_at_unix_millis", &self.received_at_unix_millis)
            .field("payload_bytes", &self.payload.len())
            .field("payload_sha256", &self.payload_sha256)
            .field("payload", &"[EXACT RAW FRAME REDACTED]")
            .finish()
    }
}

impl RawStreamerFrame {
    fn try_new(
        generation: ConnectionGeneration,
        ordinal: NonZeroU64,
        kind: RawStreamerFrameKind,
        payload: Bytes,
        maximum: usize,
    ) -> Result<Self, SchwabTransportError> {
        if payload.len() > maximum {
            return Err(SchwabTransportError::PayloadTooLarge);
        }
        Ok(Self {
            generation,
            ordinal,
            kind,
            received_at_unix_millis: unix_millis()?,
            payload_sha256: Sha256::digest(&payload).into(),
            payload,
        })
    }

    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub const fn ordinal(&self) -> NonZeroU64 {
        self.ordinal
    }

    pub const fn kind(&self) -> RawStreamerFrameKind {
        self.kind
    }

    pub const fn received_at_unix_millis(&self) -> u64 {
        self.received_at_unix_millis
    }

    pub const fn payload_sha256(&self) -> [u8; 32] {
        self.payload_sha256
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

/// Exact bounded application-microbatch receipt awaiting the shared event-material seam.
///
/// This is not an HTTP page set and expresses no provider-completeness terminal state. The future
/// consuming transition belongs to the common `ProviderEventMicrobatchMaterial` contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamerMicrobatchReceipt {
    generation: ConnectionGeneration,
    token_generation: AccessTokenGeneration,
    first_ordinal: NonZeroU64,
    last_ordinal: NonZeroU64,
    frame_count: u64,
    payload_bytes: u64,
    first_received_at_unix_millis: u64,
    last_received_at_unix_millis: u64,
    content_sha256: [u8; 32],
    observation_sha256: [u8; 32],
}

impl StreamerMicrobatchReceipt {
    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub const fn token_generation(&self) -> AccessTokenGeneration {
        self.token_generation
    }

    pub const fn first_ordinal(&self) -> NonZeroU64 {
        self.first_ordinal
    }

    pub const fn last_ordinal(&self) -> NonZeroU64 {
        self.last_ordinal
    }

    pub const fn frame_count(&self) -> u64 {
        self.frame_count
    }

    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub const fn first_received_at_unix_millis(&self) -> u64 {
        self.first_received_at_unix_millis
    }

    pub const fn last_received_at_unix_millis(&self) -> u64 {
        self.last_received_at_unix_millis
    }

    /// Digest of ordered exact frame kinds, lengths, and payload digests; excludes receive time.
    pub const fn content_sha256(&self) -> [u8; 32] {
        self.content_sha256
    }

    /// Digest binding generation, ordinals, local receive times, and payload digests.
    pub const fn observation_sha256(&self) -> [u8; 32] {
        self.observation_sha256
    }
}

/// Exact raw Streamer microbatch retained before canonical decoding.
#[derive(Eq, PartialEq)]
pub struct StreamerMicrobatch {
    receipt: StreamerMicrobatchReceipt,
    connection: SchwabStreamerConnectionEvidence,
    frames: Box<[RawStreamerFrame]>,
}

impl fmt::Debug for StreamerMicrobatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerMicrobatch")
            .field("receipt", &self.receipt)
            .field("frame_count", &self.frames.len())
            .field("frames", &"[EXACT RAW FRAMES REDACTED]")
            .finish()
    }
}

impl StreamerMicrobatch {
    pub const fn receipt(&self) -> &StreamerMicrobatchReceipt {
        &self.receipt
    }

    pub fn frames(&self) -> &[RawStreamerFrame] {
        &self.frames
    }

    /// Returns the exact application-minted connection evidence carried by every frame.
    pub const fn connection(&self) -> &SchwabStreamerConnectionEvidence {
        &self.connection
    }

    /// Consumes exact validated frames into the common event-microbatch physical seal boundary.
    pub fn into_pending_capture(
        self,
        event_ids: Vec<Uuid>,
        parse_bounds: ParseBounds,
    ) -> Result<(SchwabPendingStreamerCapture, ProviderCaptureSealRequest), SchwabTransportError>
    {
        validate_streamer_microbatch(&self)?;
        if event_ids.len() != self.frames.len()
            || event_ids.iter().any(Uuid::is_nil)
            || event_ids.iter().collect::<BTreeSet<_>>().len() != event_ids.len()
        {
            return Err(SchwabTransportError::CaptureMaterial);
        }
        let StreamerMicrobatch {
            receipt,
            connection,
            frames,
        } = self;
        let SchwabStreamerConnectionEvidence {
            coordinates,
            stream_identity,
            ..
        } = connection;
        let mut evidence = Vec::new();
        let mut records = Vec::new();
        let mut parsed_frames = Vec::new();
        evidence
            .try_reserve_exact(frames.len())
            .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
        records
            .try_reserve_exact(frames.len())
            .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
        parsed_frames
            .try_reserve_exact(frames.len())
            .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
        for (event_id, frame) in event_ids.into_iter().zip(frames.into_vec()) {
            if frame.kind != RawStreamerFrameKind::Text {
                return Err(SchwabTransportError::CaptureMaterial);
            }
            parsed_frames.push(
                parse_streamer_frame(&frame.payload, parse_bounds)
                    .map_err(|_| SchwabTransportError::Adapter)?,
            );
            let received_nanos = i64::try_from(frame.received_at_unix_millis)
                .ok()
                .and_then(|value| value.checked_mul(1_000_000))
                .ok_or(SchwabTransportError::CaptureMaterial)?;
            evidence.push(SchwabStreamerFrameSealEvidence {
                generation: frame.generation,
                transport_ordinal: frame.ordinal,
                kind: frame.kind,
                received_at_unix_millis: frame.received_at_unix_millis,
                payload_bytes: u64::try_from(frame.payload.len())
                    .map_err(|_| SchwabTransportError::Overflow)?,
                payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, frame.payload_sha256),
                event_id,
            });
            records.push(
                RawCaptureRecord::try_new_live(
                    event_id,
                    Arc::from(coordinates.source_id().as_str()),
                    coordinates.connection_id(),
                    None,
                    None,
                    chrono::DateTime::from_timestamp_nanos(received_nanos),
                    frame.payload,
                )
                .map_err(|_| SchwabTransportError::CaptureMaterial)?,
            );
        }
        let material = ProviderEventMicrobatchMaterial::try_new(
            coordinates.source_id().clone(),
            coordinates.metadata_revision().clone(),
            coordinates.dataset().clone(),
            stream_identity.clone(),
            records,
        )
        .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        let (expectation, seal_request) = material.into_sealing_parts();
        Ok((
            SchwabPendingStreamerCapture {
                expectation,
                coordinates,
                stream_identity,
                streamer_receipt: receipt,
                frames: evidence.into_boxed_slice(),
                parsed_frames: parsed_frames.into_boxed_slice(),
            },
            seal_request,
        ))
    }
}

/// Exact adapter transport-frame evidence aligned to one sealed common event frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchwabStreamerFrameSealEvidence {
    generation: ConnectionGeneration,
    transport_ordinal: NonZeroU64,
    kind: RawStreamerFrameKind,
    received_at_unix_millis: u64,
    payload_bytes: u64,
    payload_digest: EvidenceDigest,
    event_id: Uuid,
}

impl SchwabStreamerFrameSealEvidence {
    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    /// Returns the local connection-generation transport ordinal, not a provider sequence.
    pub const fn transport_ordinal(&self) -> NonZeroU64 {
        self.transport_ordinal
    }

    pub const fn kind(&self) -> RawStreamerFrameKind {
        self.kind
    }

    pub const fn received_at_unix_millis(&self) -> u64 {
        self.received_at_unix_millis
    }

    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }

    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }
}

/// Non-cloneable Schwab Streamer continuation awaiting its exact common physical seal.
pub struct SchwabPendingStreamerCapture {
    expectation: ProviderEventMicrobatchSealExpectation,
    coordinates: SchwabCaptureCoordinates,
    stream_identity: SourceIdentifier,
    streamer_receipt: StreamerMicrobatchReceipt,
    frames: Box<[SchwabStreamerFrameSealEvidence]>,
    parsed_frames: Box<[ParsedNative<StreamerFrame>]>,
}

impl fmt::Debug for SchwabPendingStreamerCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabPendingStreamerCapture")
            .field("coordinates", &self.coordinates)
            .field("stream_identity", &self.stream_identity)
            .field("streamer_receipt", &self.streamer_receipt)
            .field("frame_count", &self.frames.len())
            .field("parsed_frame_count", &self.parsed_frames.len())
            .field("raw_frames", &"AWAITING COMMON PHYSICAL SEAL")
            .finish()
    }
}

impl SchwabPendingStreamerCapture {
    /// Rejoins only the exact witness-matched physical result and validates every frame mapping.
    pub fn try_rejoin(
        self,
        sealed: SealedProviderCaptureMaterial,
    ) -> Result<SchwabSealedStreamerCapture, SchwabTransportError> {
        let token = self
            .expectation
            .try_rejoin(sealed)
            .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        let persisted = token.persisted_receipt();
        let capture = persisted.capture();
        if capture.source_id() != self.coordinates.source_id()
            || capture.metadata_revision() != self.coordinates.metadata_revision()
            || capture.dataset() != self.coordinates.dataset()
            || capture.stream_identity() != &self.stream_identity
            || capture.frames().len() != self.frames.len()
            || persisted.segment().frames().len() != self.frames.len()
            || self.parsed_frames.len() != self.frames.len()
        {
            return Err(SchwabTransportError::CaptureMaterial);
        }
        for ((common, physical), expected) in capture
            .frames()
            .iter()
            .zip(persisted.segment().frames())
            .zip(&self.frames)
        {
            let received_at = expected
                .received_at_unix_millis
                .checked_mul(1_000_000)
                .and_then(|value| i64::try_from(value).ok())
                .map(market_squawk_domain::Timestamp::from_unix_nanos)
                .ok_or(SchwabTransportError::CaptureMaterial)?;
            if common.event_id() != *expected.event_id.as_bytes()
                || common.connection_id() != *self.coordinates.connection_id().as_bytes()
                || common.source_sequence().is_some()
                || common.exchange_at().is_some()
                || common.received_at() != received_at
                || common.payload_bytes() != expected.payload_bytes
                || common.payload_digest() != expected.payload_digest
                || physical.provider_payload_bytes() != expected.payload_bytes
                || physical.provider_payload_digest() != expected.payload_digest
            {
                return Err(SchwabTransportError::CaptureMaterial);
            }
        }
        Ok(SchwabSealedStreamerCapture {
            token,
            coordinates: self.coordinates,
            stream_identity: self.stream_identity,
            streamer_receipt: self.streamer_receipt,
            frames: self.frames,
            parsed_frames: self.parsed_frames,
        })
    }
}

/// Opaque sealed Streamer frames awaiting typed event mapping and native-lineage publication.
pub struct SchwabSealedStreamerCapture {
    token: ProviderEventMicrobatchToken,
    coordinates: SchwabCaptureCoordinates,
    stream_identity: SourceIdentifier,
    streamer_receipt: StreamerMicrobatchReceipt,
    frames: Box<[SchwabStreamerFrameSealEvidence]>,
    parsed_frames: Box<[ParsedNative<StreamerFrame>]>,
}

pub(crate) struct SchwabSealedStreamerCaptureParts {
    pub(crate) token: ProviderEventMicrobatchToken,
    pub(crate) coordinates: SchwabCaptureCoordinates,
}

impl fmt::Debug for SchwabSealedStreamerCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabSealedStreamerCapture")
            .field("coordinates", &self.coordinates)
            .field("stream_identity", &self.stream_identity)
            .field("streamer_receipt", &self.streamer_receipt)
            .field("frame_count", &self.frames.len())
            .field("parsed_frame_count", &self.parsed_frames.len())
            .field("raw_frames", &"PHYSICALLY SEALED")
            .finish()
    }
}

impl SchwabSealedStreamerCapture {
    pub const fn coordinates(&self) -> &SchwabCaptureCoordinates {
        &self.coordinates
    }

    pub const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }

    pub const fn streamer_receipt(&self) -> &StreamerMicrobatchReceipt {
        &self.streamer_receipt
    }

    pub fn persisted_receipt(
        &self,
    ) -> &market_squawk_sources::SealedProviderEventMicrobatchReceipt {
        self.token.persisted_receipt()
    }

    pub fn frames(&self) -> &[SchwabStreamerFrameSealEvidence] {
        &self.frames
    }

    pub(crate) fn parsed_frames(&self) -> &[ParsedNative<StreamerFrame>] {
        &self.parsed_frames
    }

    pub(crate) fn into_parts(self) -> SchwabSealedStreamerCaptureParts {
        SchwabSealedStreamerCaptureParts {
            token: self.token,
            coordinates: self.coordinates,
        }
    }
}

fn validate_streamer_microbatch(
    microbatch: &StreamerMicrobatch,
) -> Result<(), SchwabTransportError> {
    let receipt = microbatch.receipt();
    let connection = microbatch.connection();
    let frames = microbatch.frames();
    let first = frames
        .first()
        .ok_or(SchwabTransportError::CaptureMaterial)?;
    let last = frames.last().ok_or(SchwabTransportError::CaptureMaterial)?;
    let mut content = Sha256::new();
    let mut observation = Sha256::new();
    let mut payload_bytes = 0_u64;
    let mut prior_ordinal = None;
    if receipt.generation != connection.generation {
        return Err(SchwabTransportError::CaptureMaterial);
    }
    for frame in frames {
        let observed_payload_sha256: [u8; 32] = Sha256::digest(&frame.payload).into();
        if frame.generation != receipt.generation
            || frame.payload_sha256 != observed_payload_sha256
            || prior_ordinal.is_some_and(|prior: u64| frame.ordinal.get() != prior + 1)
        {
            return Err(SchwabTransportError::CaptureMaterial);
        }
        payload_bytes = payload_bytes
            .checked_add(
                u64::try_from(frame.payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
            )
            .ok_or(SchwabTransportError::Overflow)?;
        hash_frame(&mut content, frame.kind.digest_tag(), &frame.payload)?;
        hash_observation(
            &mut observation,
            frame.generation,
            frame.ordinal,
            frame.received_at_unix_millis,
            frame.payload_sha256,
        );
        prior_ordinal = Some(frame.ordinal.get());
    }
    if receipt.first_ordinal != first.ordinal
        || receipt.last_ordinal != last.ordinal
        || receipt.frame_count
            != u64::try_from(frames.len()).map_err(|_| SchwabTransportError::Overflow)?
        || receipt.payload_bytes != payload_bytes
        || receipt.first_received_at_unix_millis != first.received_at_unix_millis
        || receipt.last_received_at_unix_millis != last.received_at_unix_millis
        || receipt.content_sha256 != <[u8; 32]>::from(content.finalize())
        || receipt.observation_sha256 != <[u8; 32]>::from(observation.finalize())
    {
        return Err(SchwabTransportError::CaptureMaterial);
    }
    Ok(())
}

/// Fail-closed nonblocking sink failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamerCaptureSinkError {
    Saturated,
    Closed,
    Integrity,
}

/// Application-owned nonblocking bridge to the pre-seal Streamer microbatch seam.
pub trait StreamerCaptureSink: Send {
    fn try_publish(
        &mut self,
        microbatch: StreamerMicrobatch,
    ) -> Result<(), StreamerCaptureSinkError>;
}

/// Provider frame delivered by an injectable connection boundary.
#[derive(Eq, PartialEq)]
pub enum InboundStreamerFrame {
    Text(Bytes),
    Binary(Bytes),
    Ping(Bytes),
    Pong(Bytes),
    Close,
}

impl fmt::Debug for InboundStreamerFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, payload_bytes) = match self {
            Self::Text(payload) => ("text", Some(payload.len())),
            Self::Binary(payload) => ("binary", Some(payload.len())),
            Self::Ping(payload) => ("ping", Some(payload.len())),
            Self::Pong(payload) => ("pong", Some(payload.len())),
            Self::Close => ("close", None),
        };
        formatter
            .debug_struct("InboundStreamerFrame")
            .field("kind", &kind)
            .field("payload_bytes", &payload_bytes)
            .field("payload", &"[RAW FRAME REDACTED]")
            .finish()
    }
}

/// One connected Streamer wire. Implementations must not retain outbound login bytes.
pub trait SchwabStreamerConnection: fmt::Debug + Send {
    fn send_text<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>>;

    fn send_pong<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>>;

    fn next<'a>(
        &'a mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<InboundStreamerFrame>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    >;

    fn close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>>;
}

/// Injectable Streamer connector used by the production WSS client and local mock proof.
pub trait SchwabStreamerConnector: fmt::Debug + Send + Sync {
    fn connect<'a>(
        &'a self,
        endpoint: &'a str,
        bounds: StreamerTransportBounds,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn SchwabStreamerConnection>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    >;
}

/// Production rustls WebSocket connector.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProductionSchwabStreamerConnector;

impl SchwabStreamerConnector for ProductionSchwabStreamerConnector {
    fn connect<'a>(
        &'a self,
        endpoint: &'a str,
        bounds: StreamerTransportBounds,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Box<dyn SchwabStreamerConnection>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            validate_wss_endpoint(endpoint)?;
            let config = WebSocketConfig::default()
                .read_buffer_size(bounds.max_frame_bytes().clamp(4 * 1024, 128 * 1024))
                .write_buffer_size(16 * 1024)
                .max_write_buffer_size(64 * 1024)
                .max_message_size(Some(bounds.max_frame_bytes()))
                .max_frame_size(Some(bounds.max_frame_bytes()));
            let (socket, response) = connect_async_with_config(endpoint, Some(config), true)
                .await
                .map_err(map_websocket_error)?;
            if response.status().as_u16() != 101 {
                return Err(SchwabTransportError::Protocol);
            }
            Ok(Box::new(TungsteniteSchwabConnection { socket })
                as Box<dyn SchwabStreamerConnection>)
        })
    }
}

struct TungsteniteSchwabConnection {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl fmt::Debug for TungsteniteSchwabConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TungsteniteSchwabConnection(..)")
    }
}

impl SchwabStreamerConnection for TungsteniteSchwabConnection {
    fn send_text<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            let text =
                String::from_utf8(payload.to_vec()).map_err(|_| SchwabTransportError::Protocol)?;
            self.socket
                .send(Message::Text(text.into()))
                .await
                .map_err(map_websocket_error)
        })
    }

    fn send_pong<'a>(
        &'a mut self,
        payload: Bytes,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move {
            self.socket
                .send(Message::Pong(payload))
                .await
                .map_err(map_websocket_error)
        })
    }

    fn next<'a>(
        &'a mut self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<Option<InboundStreamerFrame>, SchwabTransportError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            match self.socket.next().await {
                Some(Ok(Message::Text(value))) => Ok(Some(InboundStreamerFrame::Text(
                    Bytes::copy_from_slice(value.as_bytes()),
                ))),
                Some(Ok(Message::Binary(value))) => Ok(Some(InboundStreamerFrame::Binary(value))),
                Some(Ok(Message::Ping(value))) => Ok(Some(InboundStreamerFrame::Ping(value))),
                Some(Ok(Message::Pong(value))) => Ok(Some(InboundStreamerFrame::Pong(value))),
                Some(Ok(Message::Close(_))) | None => Ok(Some(InboundStreamerFrame::Close)),
                Some(Ok(Message::Frame(_))) => Err(SchwabTransportError::Protocol),
                Some(Err(error)) => Err(map_websocket_error(error)),
            }
        })
    }

    fn close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SchwabTransportError>> + Send + 'a>> {
        Box::pin(async move { self.socket.close(None).await.map_err(map_websocket_error) })
    }
}

/// Normal terminal exit for a run-until-cancelled Streamer owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamerRunExit {
    Cancelled,
}

/// One-use application-issued authority for exactly one Streamer connection generation.
///
/// The application owns durable monotonic generation allocation and the connection UUID. The
/// adapter consumes this control before connecting and carries only repeatable evidence into raw
/// microbatches. The control implements neither `Clone` nor serialization.
pub struct SchwabStreamerConnectionControl {
    generation: ConnectionGeneration,
    coordinates: SchwabCaptureCoordinates,
    stream_identity: SourceIdentifier,
}

impl SchwabStreamerConnectionControl {
    /// Binds one durable application generation to its exact raw-capture coordinates.
    pub fn new(
        generation: ConnectionGeneration,
        coordinates: SchwabCaptureCoordinates,
        stream_identity: SourceIdentifier,
    ) -> Self {
        Self {
            generation,
            coordinates,
            stream_identity,
        }
    }

    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub const fn coordinates(&self) -> &SchwabCaptureCoordinates {
        &self.coordinates
    }

    pub const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }

    fn into_evidence(self) -> SchwabStreamerConnectionEvidence {
        SchwabStreamerConnectionEvidence {
            generation: self.generation,
            coordinates: self.coordinates,
            stream_identity: self.stream_identity,
        }
    }
}

impl fmt::Debug for SchwabStreamerConnectionControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabStreamerConnectionControl")
            .field("generation", &self.generation)
            .field("coordinates", &self.coordinates)
            .field("stream_identity", &self.stream_identity)
            .finish()
    }
}

/// Repeatable secret-free evidence derived from one consumed connection control.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabStreamerConnectionEvidence {
    generation: ConnectionGeneration,
    coordinates: SchwabCaptureCoordinates,
    stream_identity: SourceIdentifier,
}

impl SchwabStreamerConnectionEvidence {
    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub const fn coordinates(&self) -> &SchwabCaptureCoordinates {
        &self.coordinates
    }

    pub const fn stream_identity(&self) -> &SourceIdentifier {
        &self.stream_identity
    }
}

/// Application-owned durable allocator for Streamer connection controls.
pub trait SchwabStreamerConnectionControlSource: fmt::Debug + Send + Sync {
    /// Mints the next restart-safe connection generation and exact capture coordinates.
    fn mint(
        &self,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<SchwabStreamerConnectionControl, SchwabTransportError>>
                + Send
                + '_,
        >,
    >;
}

/// Sole Streamer connection owner around the frozen desired-state controller.
pub struct SchwabStreamerExecutor {
    connector: Arc<dyn SchwabStreamerConnector>,
    token_source: Arc<dyn SchwabAccessTokenSource>,
    control_source: Arc<dyn SchwabStreamerConnectionControlSource>,
    controller: DesiredStateController,
    transport_bounds: StreamerTransportBounds,
    parse_bounds: ParseBounds,
    token_admission: AccessTokenAdmission,
    telemetry: SchwabTransportTelemetry,
    last_generation: Option<ConnectionGeneration>,
}

impl fmt::Debug for SchwabStreamerExecutor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabStreamerExecutor")
            .field("connector", &self.connector)
            .field("token_source", &"[PROTECTED AUTHORITY]")
            .field("control_source", &"[APPLICATION CONTROL AUTHORITY]")
            .field("controller", &self.controller)
            .field("transport_bounds", &self.transport_bounds)
            .field("parse_bounds", &self.parse_bounds)
            .field("token_admission", &self.token_admission)
            .field("telemetry", &self.telemetry)
            .field("last_generation", &self.last_generation)
            .finish()
    }
}

impl SchwabStreamerExecutor {
    /// Builds the production WSS executor.
    pub fn try_production(
        token_source: Arc<dyn SchwabAccessTokenSource>,
        control_source: Arc<dyn SchwabStreamerConnectionControlSource>,
        admission: StreamerAdmission,
        transport_bounds: StreamerTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<Self, SchwabTransportError> {
        Self::try_new(
            Arc::new(ProductionSchwabStreamerConnector),
            token_source,
            control_source,
            admission,
            transport_bounds,
            parse_bounds,
            token_admission,
            telemetry,
        )
    }

    /// Injects a connector while retaining the same single-owner lifecycle and bounds.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, authority, lifecycle, and resource inputs remain explicit"
    )]
    pub fn try_new(
        connector: Arc<dyn SchwabStreamerConnector>,
        token_source: Arc<dyn SchwabAccessTokenSource>,
        control_source: Arc<dyn SchwabStreamerConnectionControlSource>,
        admission: StreamerAdmission,
        transport_bounds: StreamerTransportBounds,
        parse_bounds: ParseBounds,
        token_admission: AccessTokenAdmission,
        telemetry: SchwabTransportTelemetry,
    ) -> Result<Self, SchwabTransportError> {
        if parse_bounds.max_response_bytes() > transport_bounds.max_frame_bytes() {
            return Err(SchwabTransportError::InvalidConfiguration);
        }
        Ok(Self {
            connector,
            token_source,
            control_source,
            controller: DesiredStateController::new(admission),
            transport_bounds,
            parse_bounds,
            token_admission,
            telemetry,
            last_generation: None,
        })
    }

    /// Returns the currently desired read-only services.
    pub fn desired(
        &self,
    ) -> &std::collections::BTreeMap<crate::MarketDataService, StreamerSubscription> {
        self.controller.desired()
    }

    /// Replaces one service's desired state while disconnected.
    pub fn replace_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabAdapterError> {
        if self.controller.state() != ConnectionState::Disconnected {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        if self.controller.replace_desired(subscription)?.is_some() {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        Ok(())
    }

    /// Adds keys/fields to one service's desired state while disconnected.
    pub fn add_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabAdapterError> {
        if self.controller.state() != ConnectionState::Disconnected {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        if self.controller.add_desired(subscription)?.is_some() {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        Ok(())
    }

    pub const fn telemetry(&self) -> &SchwabTransportTelemetry {
        &self.telemetry
    }

    /// Owns exactly one connection at a time, replaying desired state after bounded reconnects.
    pub async fn run(
        &mut self,
        bootstrap: &StreamerBootstrap,
        sink: &mut dyn StreamerCaptureSink,
        cancellation: CancellationToken,
    ) -> Result<StreamerRunExit, SchwabTransportError> {
        if self.controller.state() != ConnectionState::Disconnected {
            return Err(SchwabTransportError::Protocol);
        }
        validate_wss_endpoint(bootstrap.socket_url())?;
        let mut attempt = 0usize;
        loop {
            if cancellation.is_cancelled() {
                return Ok(StreamerRunExit::Cancelled);
            }
            if attempt > self.transport_bounds.max_reconnect_attempts() {
                return Err(SchwabTransportError::ReconnectExhausted);
            }
            if attempt > 0 {
                wait_reconnect(self.transport_bounds.reconnect_delay(), &cancellation).await?;
            }
            let reconnect = attempt > 0;
            self.telemetry.record_stream_connect_attempt(reconnect)?;
            let control = await_operation(
                self.control_source.mint(),
                self.transport_bounds.connect_timeout(),
                &cancellation,
            )
            .await?;
            let generation = control.generation();
            if self.last_generation.is_some_and(|last| generation <= last) {
                return Err(SchwabTransportError::Protocol);
            }
            self.last_generation = Some(generation);
            let token = match acquire_token(
                &*self.token_source,
                self.token_admission,
                self.transport_bounds.io_timeout(),
                &cancellation,
            )
            .await
            {
                Ok(token) => token,
                Err(SchwabTransportError::Cancelled) => return Ok(StreamerRunExit::Cancelled),
                Err(error) if retryable(error) => {
                    self.telemetry.record_stream_connect_failure()?;
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            self.controller.begin_connect(generation)?;
            let connection = await_operation(
                self.connector
                    .connect(bootstrap.socket_url(), self.transport_bounds),
                self.transport_bounds.connect_timeout(),
                &cancellation,
            )
            .await;
            let mut connection = match connection {
                Ok(connection) => connection,
                Err(SchwabTransportError::Cancelled) => {
                    self.controller.disconnected(generation)?;
                    return Ok(StreamerRunExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_connect_failure()?;
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                    continue;
                }
                Err(error) => {
                    self.controller.disconnected(generation)?;
                    return Err(error);
                }
            };
            self.controller.socket_connected(generation)?;
            let token_generation = token.generation();
            let refresh_deadline = token_refresh_deadline(&token, self.token_admission)?;
            let outcome = self
                .run_connection(
                    control,
                    token_generation,
                    refresh_deadline,
                    token,
                    bootstrap,
                    &mut *connection,
                    sink,
                    &cancellation,
                )
                .await;
            match outcome {
                Ok(ConnectionExit::Cancelled) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_clean_close()?;
                    return Ok(StreamerRunExit::Cancelled);
                }
                Ok(ConnectionExit::Retry) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_disconnect()?;
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                }
                Err(SchwabTransportError::Cancelled) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_clean_close()?;
                    return Ok(StreamerRunExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_disconnect()?;
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                }
                Err(error) => {
                    self.controller.disconnected(generation)?;
                    return Err(error);
                }
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one connection generation keeps authority and capture binding explicit"
    )]
    async fn run_connection(
        &mut self,
        control: SchwabStreamerConnectionControl,
        token_generation: AccessTokenGeneration,
        refresh_deadline: Instant,
        token: TransientAccessToken,
        bootstrap: &StreamerBootstrap,
        connection: &mut dyn SchwabStreamerConnection,
        sink: &mut dyn StreamerCaptureSink,
        cancellation: &CancellationToken,
    ) -> Result<ConnectionExit, SchwabTransportError> {
        let generation = control.generation();
        let mut batch = MicrobatchBuilder::new(control, token_generation, self.transport_bounds);
        let login = self
            .controller
            .login_request(bootstrap, token.expose_bearer())?;
        let login_id = login.request_id().get().to_string();
        send_request(
            connection,
            login,
            &self.telemetry,
            self.transport_bounds.io_timeout(),
            cancellation,
        )
        .await?;
        drop(token);
        let login_deadline = Instant::now()
            .checked_add(self.transport_bounds.io_timeout())
            .ok_or(SchwabTransportError::Overflow)?;
        loop {
            let incoming = match read_until(
                connection,
                login_deadline,
                cancellation,
                self.transport_bounds.io_timeout(),
            )
            .await
            {
                Ok(incoming) => incoming,
                Err(SchwabTransportError::Cancelled) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    close_with_deadline(connection, self.transport_bounds.io_timeout()).await;
                    return Ok(ConnectionExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    return Ok(ConnectionExit::Retry);
                }
                Err(error) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    return Err(error);
                }
            };
            match self
                .process_incoming(
                    generation,
                    connection,
                    incoming,
                    &mut batch,
                    sink,
                    cancellation,
                )
                .await?
            {
                ProcessedFrame::Parsed(frame) => {
                    if let Some(response) = frame.value().responses.iter().find(|response| {
                        response.service.as_ref() == "ADMIN"
                            && response.command.as_ref() == "LOGIN"
                            && response.request_id.as_ref() == login_id
                    }) {
                        if response.code != StreamerResponseCode::Success {
                            flush_batch(&mut batch, sink, &self.telemetry)?;
                            return Ok(ConnectionExit::Retry);
                        }
                        self.controller.login_accepted(generation)?;
                        self.telemetry.record_stream_connected()?;
                        break;
                    }
                }
                ProcessedFrame::Control => {}
                ProcessedFrame::Closed => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    return Ok(ConnectionExit::Retry);
                }
            }
        }

        let requests = self.controller.replay_desired()?;
        let mut pending = BTreeSet::new();
        for request in requests {
            pending.insert(request.request_id().get().to_string());
            send_request(
                connection,
                request,
                &self.telemetry,
                self.transport_bounds.io_timeout(),
                cancellation,
            )
            .await?;
        }
        let mut idle_deadline = Instant::now()
            .checked_add(self.transport_bounds.io_timeout())
            .ok_or(SchwabTransportError::Overflow)?;
        loop {
            let flush_deadline = batch.flush_deadline();
            let next_deadline = idle_deadline.min(refresh_deadline).min(flush_deadline);
            match read_or_deadline(connection, next_deadline, cancellation).await {
                Ok(incoming) => {
                    idle_deadline = Instant::now()
                        .checked_add(self.transport_bounds.io_timeout())
                        .ok_or(SchwabTransportError::Overflow)?;
                    match self
                        .process_incoming(
                            generation,
                            connection,
                            incoming,
                            &mut batch,
                            sink,
                            cancellation,
                        )
                        .await?
                    {
                        ProcessedFrame::Parsed(frame) => {
                            for response in &frame.value().responses {
                                if response.code != StreamerResponseCode::Success {
                                    flush_batch(&mut batch, sink, &self.telemetry)?;
                                    return Err(SchwabTransportError::Adapter);
                                }
                                if !pending.remove(response.request_id.as_ref()) {
                                    flush_batch(&mut batch, sink, &self.telemetry)?;
                                    return Err(SchwabTransportError::Protocol);
                                }
                            }
                        }
                        ProcessedFrame::Control => {}
                        ProcessedFrame::Closed => {
                            flush_batch(&mut batch, sink, &self.telemetry)?;
                            return Ok(ConnectionExit::Retry);
                        }
                    }
                }
                Err(SchwabTransportError::Deadline) => {
                    let now = Instant::now();
                    if now >= refresh_deadline || now >= idle_deadline {
                        flush_batch(&mut batch, sink, &self.telemetry)?;
                        close_with_deadline(connection, self.transport_bounds.io_timeout()).await;
                        return Ok(ConnectionExit::Retry);
                    }
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                }
                Err(SchwabTransportError::Cancelled) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    close_with_deadline(connection, self.transport_bounds.io_timeout()).await;
                    return Ok(ConnectionExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    flush_batch(&mut batch, sink, &self.telemetry)?;
                    return Ok(ConnectionExit::Retry);
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact frame capture and connection ownership remain explicit"
    )]
    async fn process_incoming(
        &self,
        generation: ConnectionGeneration,
        connection: &mut dyn SchwabStreamerConnection,
        incoming: InboundStreamerFrame,
        batch: &mut MicrobatchBuilder,
        sink: &mut dyn StreamerCaptureSink,
        cancellation: &CancellationToken,
    ) -> Result<ProcessedFrame, SchwabTransportError> {
        let payload = match incoming {
            InboundStreamerFrame::Text(payload) => payload,
            InboundStreamerFrame::Ping(payload) => {
                self.telemetry.record_stream_frame(
                    u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
                )?;
                await_operation(
                    connection.send_pong(payload),
                    self.transport_bounds.io_timeout(),
                    cancellation,
                )
                .await?;
                return Ok(ProcessedFrame::Control);
            }
            InboundStreamerFrame::Pong(payload) => {
                self.telemetry.record_stream_frame(
                    u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
                )?;
                return Ok(ProcessedFrame::Control);
            }
            InboundStreamerFrame::Binary(payload) => {
                self.telemetry.record_stream_frame(
                    u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
                )?;
                return Err(SchwabTransportError::Protocol);
            }
            InboundStreamerFrame::Close => return Ok(ProcessedFrame::Closed),
        };
        self.telemetry.record_stream_frame(
            u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
        )?;
        let parsed = match parse_streamer_frame(&payload, self.parse_bounds) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.telemetry.record_validation_failure()?;
                flush_batch(batch, sink, &self.telemetry)?;
                return Err(error.into());
            }
        };
        let events = parsed.value().data.iter().try_fold(0u64, |total, data| {
            let count =
                u64::try_from(data.content.len()).map_err(|_| SchwabTransportError::Overflow)?;
            total
                .checked_add(count)
                .ok_or(SchwabTransportError::Overflow)
        })?;
        let responses = u64::try_from(parsed.value().responses.len())
            .map_err(|_| SchwabTransportError::Overflow)?;
        let notifications = u64::try_from(parsed.value().notifications.len())
            .map_err(|_| SchwabTransportError::Overflow)?;
        self.telemetry
            .record_stream_semantics(events, responses, notifications)?;
        if !parsed.value().data.is_empty() || !parsed.value().notifications.is_empty() {
            append_frame(
                generation,
                RawStreamerFrameKind::Text,
                payload,
                batch,
                sink,
                &self.telemetry,
            )?;
        }
        Ok(ProcessedFrame::Parsed(parsed))
    }
}

enum ConnectionExit {
    Cancelled,
    Retry,
}

enum ProcessedFrame {
    Parsed(ParsedNative<StreamerFrame>),
    Control,
    Closed,
}

struct MicrobatchBuilder {
    connection: SchwabStreamerConnectionEvidence,
    token_generation: AccessTokenGeneration,
    bounds: StreamerTransportBounds,
    frames: Vec<RawStreamerFrame>,
    payload_bytes: usize,
    next_ordinal: NonZeroU64,
    opened_at: Instant,
}

impl MicrobatchBuilder {
    fn new(
        control: SchwabStreamerConnectionControl,
        token_generation: AccessTokenGeneration,
        bounds: StreamerTransportBounds,
    ) -> Self {
        Self {
            connection: control.into_evidence(),
            token_generation,
            bounds,
            frames: Vec::new(),
            payload_bytes: 0,
            next_ordinal: NonZeroU64::MIN,
            opened_at: Instant::now(),
        }
    }

    fn flush_deadline(&self) -> Instant {
        self.opened_at
            .checked_add(self.bounds.microbatch_flush_interval())
            .unwrap_or(self.opened_at)
    }

    fn would_exceed(&self, additional: usize) -> Result<bool, SchwabTransportError> {
        let bytes = self
            .payload_bytes
            .checked_add(additional)
            .ok_or(SchwabTransportError::Overflow)?;
        Ok(!self.frames.is_empty()
            && (self.frames.len() >= self.bounds.max_microbatch_frames()
                || bytes > self.bounds.max_microbatch_bytes()))
    }

    fn push(
        &mut self,
        kind: RawStreamerFrameKind,
        payload: Bytes,
    ) -> Result<(), SchwabTransportError> {
        if payload.len() > self.bounds.max_frame_bytes()
            || payload.len() > self.bounds.max_microbatch_bytes()
        {
            return Err(SchwabTransportError::PayloadTooLarge);
        }
        let frame = RawStreamerFrame::try_new(
            self.connection.generation,
            self.next_ordinal,
            kind,
            payload,
            self.bounds.max_frame_bytes(),
        )?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(frame.payload.len())
            .ok_or(SchwabTransportError::Overflow)?;
        self.next_ordinal = NonZeroU64::new(
            self.next_ordinal
                .get()
                .checked_add(1)
                .ok_or(SchwabTransportError::Overflow)?,
        )
        .ok_or(SchwabTransportError::Overflow)?;
        self.frames
            .try_reserve(1)
            .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
        self.frames.push(frame);
        Ok(())
    }

    fn finish(&mut self) -> Result<Option<StreamerMicrobatch>, SchwabTransportError> {
        if self.frames.is_empty() {
            self.opened_at = Instant::now();
            return Ok(None);
        }
        let frames = std::mem::take(&mut self.frames);
        let first = frames.first().ok_or(SchwabTransportError::Protocol)?;
        let last = frames.last().ok_or(SchwabTransportError::Protocol)?;
        let mut content = Sha256::new();
        let mut observation = Sha256::new();
        for frame in &frames {
            hash_frame(&mut content, frame.kind.digest_tag(), &frame.payload)?;
            hash_observation(
                &mut observation,
                frame.generation,
                frame.ordinal,
                frame.received_at_unix_millis,
                frame.payload_sha256,
            );
        }
        let frame_count =
            u64::try_from(frames.len()).map_err(|_| SchwabTransportError::Overflow)?;
        let payload_bytes =
            u64::try_from(self.payload_bytes).map_err(|_| SchwabTransportError::Overflow)?;
        let receipt = StreamerMicrobatchReceipt {
            generation: self.connection.generation,
            token_generation: self.token_generation,
            first_ordinal: first.ordinal,
            last_ordinal: last.ordinal,
            frame_count,
            payload_bytes,
            first_received_at_unix_millis: first.received_at_unix_millis,
            last_received_at_unix_millis: last.received_at_unix_millis,
            content_sha256: content.finalize().into(),
            observation_sha256: observation.finalize().into(),
        };
        self.payload_bytes = 0;
        self.opened_at = Instant::now();
        Ok(Some(StreamerMicrobatch {
            receipt,
            connection: self.connection.clone(),
            frames: frames.into_boxed_slice(),
        }))
    }
}

fn append_frame(
    _generation: ConnectionGeneration,
    kind: RawStreamerFrameKind,
    payload: Bytes,
    batch: &mut MicrobatchBuilder,
    sink: &mut dyn StreamerCaptureSink,
    telemetry: &SchwabTransportTelemetry,
) -> Result<(), SchwabTransportError> {
    if batch.would_exceed(payload.len())? {
        flush_batch(batch, sink, telemetry)?;
    }
    batch.push(kind, payload)
}

fn flush_batch(
    batch: &mut MicrobatchBuilder,
    sink: &mut dyn StreamerCaptureSink,
    telemetry: &SchwabTransportTelemetry,
) -> Result<(), SchwabTransportError> {
    let Some(microbatch) = batch.finish()? else {
        return Ok(());
    };
    let frames = microbatch.receipt().frame_count();
    let bytes = microbatch.receipt().payload_bytes();
    sink.try_publish(microbatch)
        .map_err(|_| SchwabTransportError::CaptureRejected)?;
    telemetry.record_stream_microbatch(frames, bytes)
}

async fn send_request(
    connection: &mut dyn SchwabStreamerConnection,
    request: TransientStreamerRequest,
    telemetry: &SchwabTransportTelemetry,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(), SchwabTransportError> {
    let bytes = Bytes::copy_from_slice(request.expose_body());
    let length = u64::try_from(bytes.len()).map_err(|_| SchwabTransportError::Overflow)?;
    await_operation(connection.send_text(bytes), timeout, cancellation).await?;
    telemetry.record_stream_request(length)
}

async fn acquire_token(
    source: &dyn SchwabAccessTokenSource,
    admission: AccessTokenAdmission,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<TransientAccessToken, SchwabTransportError> {
    let operation = async { source.acquire().await.map_err(SchwabTransportError::from) };
    let token = await_operation(operation, timeout, cancellation).await?;
    token.validate_at(unix_seconds()?, admission)?;
    Ok(token)
}

fn token_refresh_deadline(
    token: &TransientAccessToken,
    admission: AccessTokenAdmission,
) -> Result<Instant, SchwabTransportError> {
    let now = unix_seconds()?;
    let refresh_at = token
        .expires_at_unix_seconds()
        .checked_sub(admission.minimum_remaining_lifetime().as_secs())
        .ok_or(SchwabTransportError::TokenRefreshRequired)?;
    if refresh_at <= now {
        return Err(SchwabTransportError::TokenRefreshRequired);
    }
    Instant::now()
        .checked_add(Duration::from_secs(refresh_at - now))
        .ok_or(SchwabTransportError::Overflow)
}

async fn read_until(
    connection: &mut dyn SchwabStreamerConnection,
    hard_deadline: Instant,
    cancellation: &CancellationToken,
    maximum_wait: Duration,
) -> Result<InboundStreamerFrame, SchwabTransportError> {
    let remaining = hard_deadline.saturating_duration_since(Instant::now());
    await_operation(connection.next(), remaining.min(maximum_wait), cancellation)
        .await?
        .ok_or(SchwabTransportError::ResynchronizationRequired)
}

async fn read_or_deadline(
    connection: &mut dyn SchwabStreamerConnection,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<InboundStreamerFrame, SchwabTransportError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    await_operation(connection.next(), remaining, cancellation)
        .await?
        .ok_or(SchwabTransportError::ResynchronizationRequired)
}

async fn await_operation<T>(
    operation: impl Future<Output = Result<T, SchwabTransportError>>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<T, SchwabTransportError> {
    if timeout.is_zero() {
        return Err(SchwabTransportError::Deadline);
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SchwabTransportError::Cancelled),
        result = tokio::time::timeout(timeout, operation) => {
            result.map_err(|_| SchwabTransportError::Deadline)?
        }
    }
}

async fn wait_reconnect(
    delay: Duration,
    cancellation: &CancellationToken,
) -> Result<(), SchwabTransportError> {
    if delay.is_zero() {
        return if cancellation.is_cancelled() {
            Err(SchwabTransportError::Cancelled)
        } else {
            Ok(())
        };
    }
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(SchwabTransportError::Cancelled),
        () = tokio::time::sleep(delay) => Ok(()),
    }
}

async fn close_with_deadline(connection: &mut dyn SchwabStreamerConnection, timeout: Duration) {
    let _ignored = tokio::time::timeout(timeout, connection.close()).await;
}

fn retryable(error: SchwabTransportError) -> bool {
    matches!(
        error,
        SchwabTransportError::Network
            | SchwabTransportError::Deadline
            | SchwabTransportError::TokenRefreshRequired
            | SchwabTransportError::TokenAuthorityUnavailable
            | SchwabTransportError::ResynchronizationRequired
    )
}

fn validate_wss_endpoint(endpoint: &str) -> Result<(), SchwabTransportError> {
    let url = Url::parse(endpoint).map_err(|_| SchwabTransportError::Protocol)?;
    if url.scheme() != "wss"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(SchwabTransportError::Protocol);
    }
    Ok(())
}

fn map_websocket_error(error: WebSocketError) -> SchwabTransportError {
    match error {
        WebSocketError::Capacity(CapacityError::MessageTooLong { .. }) => {
            SchwabTransportError::PayloadTooLarge
        }
        WebSocketError::Http(response) if matches!(response.status().as_u16(), 401 | 403) => {
            SchwabTransportError::TokenRefreshRequired
        }
        WebSocketError::Http(_) => SchwabTransportError::Protocol,
        _ => SchwabTransportError::Network,
    }
}
