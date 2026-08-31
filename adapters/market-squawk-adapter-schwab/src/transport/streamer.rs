//! One-owner Schwab Streamer execution with bounded reconnect and exact payload microbatches.

use std::collections::{BTreeMap, BTreeSet};
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
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{
    Error as WebSocketError, Message, error::CapacityError, protocol::WebSocketConfig,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use crate::{
    CapacityObservation, CapacityUnit, ConnectionGeneration, ConnectionState,
    DesiredStateController, ParseBounds, ParsedNative, SchwabAdapterError, StreamerAdmission,
    StreamerBootstrap, StreamerCommand, StreamerFrame, StreamerResponseCode, StreamerSubscription,
    TransientStreamerRequest, parse_streamer_frame,
};

use super::{
    AccessTokenAdmission, AccessTokenGeneration, SchwabAccessTokenSource, SchwabCaptureCoordinates,
    SchwabTransportError, SchwabTransportTelemetry, StreamerTransportBounds, TransientAccessToken,
    duration_millis, hash_frame, hash_observation, unix_millis, unix_seconds,
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
    service_responses: Box<[PendingStreamerServiceResponseEvidence]>,
}

impl fmt::Debug for StreamerMicrobatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamerMicrobatch")
            .field("receipt", &self.receipt)
            .field("frame_count", &self.frames.len())
            .field("service_response_count", &self.service_responses.len())
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

    /// Consumes exact bounded frames into the common event-microbatch physical seal boundary.
    ///
    /// Provider-native decoding is an aligned, non-authoritative disposition. A malformed,
    /// unexpected, or binary frame remains physically sealable as raw-only evidence and cannot
    /// acquire typed publication authority.
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
            service_responses,
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
            if contains_account_activity(&frame.payload) {
                return Err(SchwabTransportError::CaptureMaterial);
            }
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
            parsed_frames.push(if frame.kind == RawStreamerFrameKind::Text {
                parse_streamer_frame(&frame.payload, parse_bounds).ok()
            } else {
                None
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
                service_responses,
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

#[derive(Debug, Eq, PartialEq)]
struct PendingStreamerServiceResponseEvidence {
    service: crate::MarketDataService,
    command: Box<str>,
    request_id: Box<str>,
    status_code: i64,
    provider_timestamp_millis: Option<u64>,
    round_trip_latency_ms: Option<u64>,
    request_payload_sha256: Option<EvidenceDigest>,
    request_payload_bytes: Option<u64>,
    frame_ordinal: NonZeroU64,
}

/// One provider-owned Streamer service acknowledgement or error bound to its sealed raw frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchwabStreamerServiceResponseEvidence {
    service: crate::MarketDataService,
    command: Box<str>,
    request_id: Box<str>,
    status_code: i64,
    provider_timestamp_millis: Option<u64>,
    round_trip_latency_ms: Option<u64>,
    request_payload_sha256: Option<EvidenceDigest>,
    request_payload_bytes: Option<u64>,
    generation: ConnectionGeneration,
    transport_ordinal: NonZeroU64,
    received_at_unix_millis: u64,
    payload_bytes: u64,
    payload_digest: EvidenceDigest,
    event_id: Uuid,
    sealed_capture_receipt_sha256: EvidenceDigest,
    observation_sha256: EvidenceDigest,
}

impl SchwabStreamerServiceResponseEvidence {
    pub const fn service(&self) -> crate::MarketDataService {
        self.service
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    /// Exact provider response code (`0` is success; other values are provider errors).
    pub const fn status_code(&self) -> i64 {
        self.status_code
    }

    pub const fn provider_timestamp_millis(&self) -> Option<u64> {
        self.provider_timestamp_millis
    }

    /// Monotonic local request-to-response measurement when the response matched an owned request.
    pub const fn round_trip_latency_ms(&self) -> Option<u64> {
        self.round_trip_latency_ms
    }

    /// SHA-256 of the exact owned outbound subscription request when request matching succeeded.
    pub const fn request_payload_sha256(&self) -> Option<EvidenceDigest> {
        self.request_payload_sha256
    }

    /// Exact encoded request bytes when this response matched an owned outbound command.
    pub const fn request_payload_bytes(&self) -> Option<u64> {
        self.request_payload_bytes
    }

    /// Response-scoped adaptive evidence for this exact sealed service acknowledgement.
    pub fn capacity_observation(&self) -> Result<CapacityObservation, SchwabTransportError> {
        let request_bytes = self
            .request_payload_bytes
            .ok_or(SchwabTransportError::Protocol)?;
        let latency_ms = self
            .round_trip_latency_ms
            .ok_or(SchwabTransportError::Protocol)?;
        let succeeded = self.status_code == 0;
        CapacityObservation::from_transport(
            CapacityUnit::Requests,
            1,
            u64::from(succeeded),
            u64::from(!succeeded),
            0,
            0,
            0,
            request_bytes,
            self.payload_bytes,
            latency_ms,
            0,
            false,
            false,
        )
        .validate()
        .map_err(|_| SchwabTransportError::Protocol)
    }

    pub const fn generation(&self) -> ConnectionGeneration {
        self.generation
    }

    pub const fn transport_ordinal(&self) -> NonZeroU64 {
        self.transport_ordinal
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

    pub const fn sealed_capture_receipt_sha256(&self) -> EvidenceDigest {
        self.sealed_capture_receipt_sha256
    }

    pub const fn observation_sha256(&self) -> EvidenceDigest {
        self.observation_sha256
    }
}

/// Non-cloneable Schwab Streamer continuation awaiting its exact common physical seal.
pub struct SchwabPendingStreamerCapture {
    expectation: ProviderEventMicrobatchSealExpectation,
    coordinates: SchwabCaptureCoordinates,
    stream_identity: SourceIdentifier,
    streamer_receipt: StreamerMicrobatchReceipt,
    frames: Box<[SchwabStreamerFrameSealEvidence]>,
    parsed_frames: Box<[Option<ParsedNative<StreamerFrame>>]>,
    service_responses: Box<[PendingStreamerServiceResponseEvidence]>,
}

impl fmt::Debug for SchwabPendingStreamerCapture {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SchwabPendingStreamerCapture")
            .field("coordinates", &self.coordinates)
            .field("stream_identity", &self.stream_identity)
            .field("streamer_receipt", &self.streamer_receipt)
            .field("frame_count", &self.frames.len())
            .field(
                "parsed_frame_count",
                &self.parsed_frames.iter().flatten().count(),
            )
            .field("service_response_count", &self.service_responses.len())
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
        let sealed_capture_receipt_sha256 = token.persisted_receipt().receipt_digest();
        let service_responses = bind_service_response_evidence(
            &self.service_responses,
            &self.frames,
            sealed_capture_receipt_sha256,
        )?;
        Ok(SchwabSealedStreamerCapture {
            token,
            coordinates: self.coordinates,
            stream_identity: self.stream_identity,
            streamer_receipt: self.streamer_receipt,
            frames: self.frames,
            parsed_frames: self.parsed_frames,
            service_responses,
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
    parsed_frames: Box<[Option<ParsedNative<StreamerFrame>>]>,
    service_responses: Box<[SchwabStreamerServiceResponseEvidence]>,
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
            .field(
                "parsed_frame_count",
                &self.parsed_frames.iter().flatten().count(),
            )
            .field("service_response_count", &self.service_responses.len())
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

    /// Exact selected-service acknowledgements/errors carried by these sealed frames.
    pub fn service_responses(&self) -> &[SchwabStreamerServiceResponseEvidence] {
        &self.service_responses
    }

    pub(crate) fn parsed_frames(&self) -> &[Option<ParsedNative<StreamerFrame>>] {
        &self.parsed_frames
    }

    pub(crate) fn into_parts(self) -> SchwabSealedStreamerCaptureParts {
        SchwabSealedStreamerCaptureParts {
            token: self.token,
            coordinates: self.coordinates,
        }
    }
}

fn bind_service_response_evidence(
    pending: &[PendingStreamerServiceResponseEvidence],
    frames: &[SchwabStreamerFrameSealEvidence],
    sealed_capture_receipt_sha256: EvidenceDigest,
) -> Result<Box<[SchwabStreamerServiceResponseEvidence]>, SchwabTransportError> {
    if sealed_capture_receipt_sha256.algorithm() != DigestAlgorithm::Sha256 {
        return Err(SchwabTransportError::CaptureMaterial);
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(pending.len())
        .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
    for response in pending {
        let frame_index = frames
            .binary_search_by_key(&response.frame_ordinal, |frame| frame.transport_ordinal)
            .map_err(|_| SchwabTransportError::CaptureMaterial)?;
        let frame = &frames[frame_index];
        let observation_sha256 =
            service_response_observation_sha256(response, frame, sealed_capture_receipt_sha256)?;
        output.push(SchwabStreamerServiceResponseEvidence {
            service: response.service,
            command: response.command.clone(),
            request_id: response.request_id.clone(),
            status_code: response.status_code,
            provider_timestamp_millis: response.provider_timestamp_millis,
            round_trip_latency_ms: response.round_trip_latency_ms,
            request_payload_sha256: response.request_payload_sha256,
            request_payload_bytes: response.request_payload_bytes,
            generation: frame.generation,
            transport_ordinal: frame.transport_ordinal,
            received_at_unix_millis: frame.received_at_unix_millis,
            payload_bytes: frame.payload_bytes,
            payload_digest: frame.payload_digest,
            event_id: frame.event_id,
            sealed_capture_receipt_sha256,
            observation_sha256,
        });
    }
    Ok(output.into_boxed_slice())
}

fn service_response_observation_sha256(
    response: &PendingStreamerServiceResponseEvidence,
    frame: &SchwabStreamerFrameSealEvidence,
    sealed_capture_receipt_sha256: EvidenceDigest,
) -> Result<EvidenceDigest, SchwabTransportError> {
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/schwab-streamer-service-response/v1");
    hash_bounded_text(&mut hasher, response.service.as_str())?;
    hash_bounded_text(&mut hasher, &response.command)?;
    hash_bounded_text(&mut hasher, &response.request_id)?;
    hasher.update(response.status_code.to_be_bytes());
    hash_optional_u64(&mut hasher, response.provider_timestamp_millis);
    hash_optional_u64(&mut hasher, response.round_trip_latency_ms);
    hash_optional_digest(&mut hasher, response.request_payload_sha256);
    hash_optional_u64(&mut hasher, response.request_payload_bytes);
    hasher.update(frame.generation.get().to_be_bytes());
    hasher.update(frame.transport_ordinal.get().to_be_bytes());
    hasher.update(frame.received_at_unix_millis.to_be_bytes());
    hasher.update(frame.payload_bytes.to_be_bytes());
    hasher.update(frame.payload_digest.bytes());
    hasher.update(frame.event_id.as_bytes());
    hasher.update(sealed_capture_receipt_sha256.bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

fn hash_bounded_text(hasher: &mut Sha256, value: &str) -> Result<(), SchwabTransportError> {
    hasher.update(
        u64::try_from(value.len())
            .map_err(|_| SchwabTransportError::Overflow)?
            .to_be_bytes(),
    );
    hasher.update(value.as_bytes());
    Ok(())
}

fn hash_optional_u64(hasher: &mut Sha256, value: Option<u64>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.to_be_bytes());
        }
        None => hasher.update([0]),
    }
}

fn hash_optional_digest(hasher: &mut Sha256, value: Option<EvidenceDigest>) {
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.bytes());
        }
        None => hasher.update([0]),
    }
}

const fn response_code_value(code: StreamerResponseCode) -> i64 {
    match code {
        StreamerResponseCode::Success => 0,
        StreamerResponseCode::SymbolLimit => 19,
        StreamerResponseCode::Other(value) => value,
    }
}

fn selected_service(value: &str) -> Option<crate::MarketDataService> {
    Some(match value {
        "LEVELONE_EQUITIES" => crate::MarketDataService::LevelOneEquities,
        "LEVELONE_OPTIONS" => crate::MarketDataService::LevelOneOptions,
        "LEVELONE_FUTURES" => crate::MarketDataService::LevelOneFutures,
        "LEVELONE_FUTURES_OPTIONS" => crate::MarketDataService::LevelOneFuturesOptions,
        "LEVELONE_FOREX" => crate::MarketDataService::LevelOneForex,
        "NYSE_BOOK" => crate::MarketDataService::NyseBook,
        "NASDAQ_BOOK" => crate::MarketDataService::NasdaqBook,
        "OPTIONS_BOOK" => crate::MarketDataService::OptionsBook,
        "CHART_EQUITY" => crate::MarketDataService::ChartEquity,
        "CHART_FUTURES" => crate::MarketDataService::ChartFutures,
        "SCREENER_EQUITY" => crate::MarketDataService::ScreenerEquity,
        "SCREENER_OPTION" => crate::MarketDataService::ScreenerOption,
        _ => return None,
    })
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

/// Seals the secret-bearing wire implementation boundary to this adapter crate.
pub(crate) trait SealedSchwabStreamerConnection {}

/// One connected Streamer wire. Only adapter-owned implementations can receive login bytes.
#[allow(
    private_bounds,
    reason = "the private supertrait prevents external secret-bearing wire implementations"
)]
pub trait SchwabStreamerConnection: SealedSchwabStreamerConnection + fmt::Debug + Send {
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

/// Seals connection construction to the production wire and adapter-local tests.
pub(crate) trait SealedSchwabStreamerConnector {}

/// Adapter-owned Streamer connector used by the production WSS client and local mock proof.
#[allow(
    private_bounds,
    reason = "the private supertrait prevents external interception of login handoffs"
)]
pub trait SchwabStreamerConnector:
    SealedSchwabStreamerConnector + fmt::Debug + Send + Sync
{
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

impl SealedSchwabStreamerConnector for ProductionSchwabStreamerConnector {}

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

impl SealedSchwabStreamerConnection for TungsteniteSchwabConnection {}

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
            self.socket
                .send(streamer_text_message(payload)?)
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

pub(crate) fn streamer_text_message(payload: Bytes) -> Result<Message, SchwabTransportError> {
    let text = tokio_tungstenite::tungstenite::Utf8Bytes::try_from(payload)
        .map_err(|_| SchwabTransportError::Protocol)?;
    Ok(Message::Text(text))
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
    desired_state_sender: Option<mpsc::Sender<(StreamerCommand, StreamerSubscription)>>,
    desired_state_receiver: Option<mpsc::Receiver<(StreamerCommand, StreamerSubscription)>>,
}

/// Bounded application input to the sole Streamer connection owner.
#[derive(Debug)]
pub struct SchwabStreamerDesiredStateSender {
    sender: mpsc::Sender<(StreamerCommand, StreamerSubscription)>,
}

/// Exact desired-state queue pressure retaining the command that was not admitted.
#[derive(Debug)]
pub enum SchwabStreamerDesiredStateSendError {
    Saturated(StreamerCommand, StreamerSubscription),
    Closed(StreamerCommand, StreamerSubscription),
}

impl SchwabStreamerDesiredStateSender {
    /// Attempts one nonblocking serialized update. Saturation is returned to the scheduler as
    /// measured queue pressure; the adapter never creates another socket or silently retries.
    pub fn try_send(
        &self,
        command: StreamerCommand,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabStreamerDesiredStateSendError> {
        match self.sender.try_send((command, subscription)) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full((command, subscription))) => Err(
                SchwabStreamerDesiredStateSendError::Saturated(command, subscription),
            ),
            Err(mpsc::error::TrySendError::Closed((command, subscription))) => Err(
                SchwabStreamerDesiredStateSendError::Closed(command, subscription),
            ),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    pub fn remaining_capacity(&self) -> usize {
        self.sender.capacity()
    }
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
            .field(
                "desired_state_channel_owned",
                &self.desired_state_sender.is_some(),
            )
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
        let (desired_state_sender, desired_state_receiver) =
            mpsc::channel(admission.max_services());
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
            desired_state_sender: Some(desired_state_sender),
            desired_state_receiver: Some(desired_state_receiver),
        })
    }

    /// Transfers the sole bounded runtime desired-state command sender to the application owner.
    ///
    /// `Subscribe` replaces one service, `Add` extends it, and `Unsubscribe` removes the supplied
    /// keys. Commands are serialized by this executor and never create another socket.
    pub fn take_desired_state_sender(&mut self) -> Option<SchwabStreamerDesiredStateSender> {
        self.desired_state_sender
            .take()
            .map(|sender| SchwabStreamerDesiredStateSender { sender })
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

    /// Removes keys from one service's desired state while disconnected.
    pub fn remove_desired(
        &mut self,
        subscription: StreamerSubscription,
    ) -> Result<(), SchwabAdapterError> {
        if self.controller.state() != ConnectionState::Disconnected {
            return Err(SchwabAdapterError::InvalidStreamerState);
        }
        if self.controller.remove_desired(subscription)?.is_some() {
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
        let mut consecutive_failures = 0usize;
        let mut reconnecting = false;
        loop {
            if cancellation.is_cancelled() {
                return Ok(StreamerRunExit::Cancelled);
            }
            if consecutive_failures > self.transport_bounds.max_reconnect_attempts() {
                return Err(SchwabTransportError::ReconnectExhausted);
            }
            if reconnecting {
                wait_reconnect(self.transport_bounds.reconnect_delay(), &cancellation).await?;
            }
            self.telemetry.record_stream_connect_attempt(reconnecting)?;
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
                    consecutive_failures = consecutive_failures
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                    reconnecting = true;
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
                    consecutive_failures = consecutive_failures
                        .checked_add(1)
                        .ok_or(SchwabTransportError::Overflow)?;
                    reconnecting = true;
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
            let authenticated = matches!(
                self.controller.state(),
                ConnectionState::Active(current) if current == generation
            );
            match outcome {
                Ok(ConnectionExit::Cancelled) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_clean_close()?;
                    return Ok(StreamerRunExit::Cancelled);
                }
                Ok(ConnectionExit::Retry) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_disconnect()?;
                    consecutive_failures = if authenticated {
                        0
                    } else {
                        consecutive_failures
                            .checked_add(1)
                            .ok_or(SchwabTransportError::Overflow)?
                    };
                    reconnecting = true;
                }
                Err(SchwabTransportError::Cancelled) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_clean_close()?;
                    return Ok(StreamerRunExit::Cancelled);
                }
                Err(error) if retryable(error) => {
                    self.controller.disconnected(generation)?;
                    self.telemetry.record_stream_disconnect()?;
                    consecutive_failures = if authenticated {
                        0
                    } else {
                        consecutive_failures
                            .checked_add(1)
                            .ok_or(SchwabTransportError::Overflow)?
                    };
                    reconnecting = true;
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
        let login_request = send_request(
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
                ProcessedFrame::Parsed {
                    frame,
                    captured_ordinal: _,
                } => {
                    if let Some(response) = frame.value().responses.iter().find(|response| {
                        response.service.as_ref() == "ADMIN"
                            && response.command.as_ref() == "LOGIN"
                            && response.request_id.as_ref() == login_request.request_id.as_ref()
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
        let mut pending = BTreeMap::new();
        for request in requests {
            let sent = send_request(
                connection,
                request,
                &self.telemetry,
                self.transport_bounds.io_timeout(),
                cancellation,
            )
            .await?;
            if sent.service.is_none() || pending.insert(sent.request_id.clone(), sent).is_some() {
                return Err(SchwabTransportError::Protocol);
            }
        }
        let mut idle_deadline = Instant::now()
            .checked_add(self.transport_bounds.io_timeout())
            .ok_or(SchwabTransportError::Overflow)?;
        loop {
            let flush_deadline = batch.flush_deadline();
            let mut next_deadline = idle_deadline.min(refresh_deadline).min(flush_deadline);
            for sent in pending.values() {
                let acknowledgement_deadline = sent
                    .dispatched_at
                    .checked_add(self.transport_bounds.io_timeout())
                    .ok_or(SchwabTransportError::Overflow)?;
                next_deadline = next_deadline.min(acknowledgement_deadline);
            }
            match read_active_connection_input(
                connection,
                self.desired_state_receiver.as_mut(),
                next_deadline,
                cancellation,
            )
            .await
            {
                Ok(ActiveConnectionInput::Incoming(incoming)) => {
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
                        ProcessedFrame::Parsed {
                            frame,
                            captured_ordinal,
                        } => {
                            let captured_ordinal =
                                captured_ordinal.ok_or(SchwabTransportError::Protocol)?;
                            let mut response_error = None;
                            for response in &frame.value().responses {
                                let sent = pending.remove(response.request_id.as_ref());
                                let latency = sent
                                    .as_ref()
                                    .map(|sent| duration_millis(sent.dispatched_at.elapsed()))
                                    .transpose()?;
                                let request_payload_sha256 =
                                    sent.as_ref().map(|sent| sent.request_payload_sha256);
                                let request_payload_bytes =
                                    sent.as_ref().map(|sent| sent.request_payload_bytes);
                                batch.record_service_response(
                                    captured_ordinal,
                                    response,
                                    latency,
                                    request_payload_sha256,
                                    request_payload_bytes,
                                )?;
                                let Some(sent) = sent else {
                                    response_error.get_or_insert(SchwabTransportError::Protocol);
                                    continue;
                                };
                                let Some(service) = sent.service else {
                                    response_error.get_or_insert(SchwabTransportError::Protocol);
                                    continue;
                                };
                                if response.service.as_ref() != service.as_str()
                                    || response.command.as_ref() != sent.command.as_ref()
                                {
                                    response_error.get_or_insert(SchwabTransportError::Protocol);
                                } else if response.code != StreamerResponseCode::Success {
                                    response_error.get_or_insert(SchwabTransportError::Adapter);
                                }
                            }
                            if let Some(error) = response_error {
                                flush_batch(&mut batch, sink, &self.telemetry)?;
                                return Err(error);
                            }
                        }
                        ProcessedFrame::Control => {}
                        ProcessedFrame::Closed => {
                            flush_batch(&mut batch, sink, &self.telemetry)?;
                            return Ok(ConnectionExit::Retry);
                        }
                    }
                }
                Ok(ActiveConnectionInput::DesiredState(command, subscription)) => {
                    let request = match command {
                        StreamerCommand::Subscribe => {
                            self.controller.replace_desired(subscription)?
                        }
                        StreamerCommand::Add => self.controller.add_desired(subscription)?,
                        StreamerCommand::Unsubscribe => {
                            self.controller.remove_desired(subscription)?
                        }
                    }
                    .ok_or(SchwabTransportError::Protocol)?;
                    let sent = send_request(
                        connection,
                        request,
                        &self.telemetry,
                        self.transport_bounds.io_timeout(),
                        cancellation,
                    )
                    .await?;
                    if sent.service.is_none()
                        || pending.insert(sent.request_id.clone(), sent).is_some()
                    {
                        return Err(SchwabTransportError::Protocol);
                    }
                }
                Ok(ActiveConnectionInput::DesiredStateChannelClosed) => {
                    self.desired_state_receiver = None;
                }
                Ok(ActiveConnectionInput::Deadline) => {
                    let now = Instant::now();
                    let acknowledgement_expired = pending.values().any(|sent| {
                        sent.dispatched_at
                            .checked_add(self.transport_bounds.io_timeout())
                            .is_none_or(|deadline| now >= deadline)
                    });
                    if now >= refresh_deadline || now >= idle_deadline || acknowledgement_expired {
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
                if contains_account_activity(&payload) {
                    self.telemetry.record_validation_failure()?;
                    flush_batch(batch, sink, &self.telemetry)?;
                    return Err(SchwabTransportError::Protocol);
                }
                append_frame(
                    generation,
                    RawStreamerFrameKind::Binary,
                    payload,
                    batch,
                    sink,
                    &self.telemetry,
                )?;
                self.telemetry.record_validation_failure()?;
                flush_batch(batch, sink, &self.telemetry)?;
                return Err(SchwabTransportError::Protocol);
            }
            InboundStreamerFrame::Close => return Ok(ProcessedFrame::Closed),
        };
        self.telemetry.record_stream_frame(
            u64::try_from(payload.len()).map_err(|_| SchwabTransportError::Overflow)?,
        )?;
        if contains_account_activity(&payload) {
            self.telemetry.record_validation_failure()?;
            flush_batch(batch, sink, &self.telemetry)?;
            return Err(SchwabTransportError::Protocol);
        }
        let parsed = match parse_streamer_frame(&payload, self.parse_bounds) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.telemetry.record_validation_failure()?;
                append_frame(
                    generation,
                    RawStreamerFrameKind::Text,
                    payload,
                    batch,
                    sink,
                    &self.telemetry,
                )?;
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
        if parsed.value().responses.is_empty()
            && !parsed.value().data.is_empty()
            && batch.has_service_responses()
        {
            flush_batch(batch, sink, &self.telemetry)?;
        }
        let captured_ordinal = if parsed
            .value()
            .responses
            .iter()
            .any(|response| response.service.as_ref() != "ADMIN")
            || !parsed.value().data.is_empty()
            || !parsed.value().notifications.is_empty()
        {
            Some(append_frame(
                generation,
                RawStreamerFrameKind::Text,
                payload,
                batch,
                sink,
                &self.telemetry,
            )?)
        } else {
            None
        };
        Ok(ProcessedFrame::Parsed {
            frame: parsed,
            captured_ordinal,
        })
    }
}

enum ConnectionExit {
    Cancelled,
    Retry,
}

enum ActiveConnectionInput {
    Incoming(InboundStreamerFrame),
    DesiredState(StreamerCommand, StreamerSubscription),
    DesiredStateChannelClosed,
    Deadline,
}

enum ProcessedFrame {
    Parsed {
        frame: ParsedNative<StreamerFrame>,
        captured_ordinal: Option<NonZeroU64>,
    },
    Control,
    Closed,
}

struct MicrobatchBuilder {
    connection: SchwabStreamerConnectionEvidence,
    token_generation: AccessTokenGeneration,
    bounds: StreamerTransportBounds,
    frames: Vec<RawStreamerFrame>,
    service_responses: Vec<PendingStreamerServiceResponseEvidence>,
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
            service_responses: Vec::new(),
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

    fn has_service_responses(&self) -> bool {
        !self.service_responses.is_empty()
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
    ) -> Result<NonZeroU64, SchwabTransportError> {
        if payload.len() > self.bounds.max_frame_bytes()
            || payload.len() > self.bounds.max_microbatch_bytes()
        {
            return Err(SchwabTransportError::PayloadTooLarge);
        }
        let ordinal = self.next_ordinal;
        let frame = RawStreamerFrame::try_new(
            self.connection.generation,
            ordinal,
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
        Ok(ordinal)
    }

    fn record_service_response(
        &mut self,
        frame_ordinal: NonZeroU64,
        response: &crate::StreamerResponse,
        round_trip_latency_ms: Option<u64>,
        request_payload_sha256: Option<EvidenceDigest>,
        request_payload_bytes: Option<u64>,
    ) -> Result<(), SchwabTransportError> {
        let Some(service) = selected_service(response.service.as_ref()) else {
            if response.service.as_ref() == "ADMIN" {
                return Ok(());
            }
            return Err(SchwabTransportError::Protocol);
        };
        if !self
            .frames
            .last()
            .is_some_and(|frame| frame.ordinal == frame_ordinal)
        {
            return Err(SchwabTransportError::Protocol);
        }
        self.service_responses
            .try_reserve(1)
            .map_err(|_| SchwabTransportError::PayloadTooLarge)?;
        self.service_responses
            .push(PendingStreamerServiceResponseEvidence {
                service,
                command: response.command.clone(),
                request_id: response.request_id.clone(),
                status_code: response_code_value(response.code),
                provider_timestamp_millis: response.timestamp_millis,
                round_trip_latency_ms,
                request_payload_sha256,
                request_payload_bytes,
                frame_ordinal,
            });
        Ok(())
    }

    fn finish(&mut self) -> Result<Option<StreamerMicrobatch>, SchwabTransportError> {
        if self.frames.is_empty() {
            if !self.service_responses.is_empty() {
                return Err(SchwabTransportError::Protocol);
            }
            self.opened_at = Instant::now();
            return Ok(None);
        }
        let frames = std::mem::take(&mut self.frames);
        let service_responses = std::mem::take(&mut self.service_responses);
        let first = frames.first().ok_or(SchwabTransportError::Protocol)?;
        let last = frames.last().ok_or(SchwabTransportError::Protocol)?;
        if service_responses.iter().any(|response| {
            response.frame_ordinal < first.ordinal || response.frame_ordinal > last.ordinal
        }) {
            return Err(SchwabTransportError::Protocol);
        }
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
            service_responses: service_responses.into_boxed_slice(),
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
) -> Result<NonZeroU64, SchwabTransportError> {
    if batch.would_exceed(payload.len())? {
        flush_batch(batch, sink, telemetry)?;
    }
    batch.push(kind, payload)
}

fn contains_account_activity(payload: &[u8]) -> bool {
    const FORBIDDEN_SERVICE: &[u8] = b"ACCOUNT_ACTIVITY";
    payload
        .windows(FORBIDDEN_SERVICE.len())
        .any(|window| window == FORBIDDEN_SERVICE)
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

struct SentStreamerRequest {
    service: Option<crate::MarketDataService>,
    command: Box<str>,
    request_id: Box<str>,
    request_payload_sha256: EvidenceDigest,
    request_payload_bytes: u64,
    dispatched_at: Instant,
}

async fn send_request(
    connection: &mut dyn SchwabStreamerConnection,
    request: TransientStreamerRequest,
    telemetry: &SchwabTransportTelemetry,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<SentStreamerRequest, SchwabTransportError> {
    let request_payload_sha256 = EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        Sha256::digest(request.expose_body()).into(),
    );
    let sent = SentStreamerRequest {
        service: request.service(),
        command: request.command().to_owned().into_boxed_str(),
        request_id: request.request_id().get().to_string().into_boxed_str(),
        request_payload_sha256,
        request_payload_bytes: u64::try_from(request.expose_body().len())
            .map_err(|_| SchwabTransportError::Overflow)?,
        dispatched_at: Instant::now(),
    };
    let bytes = request.into_shared_body();
    let length = u64::try_from(bytes.len()).map_err(|_| SchwabTransportError::Overflow)?;
    await_operation(connection.send_text(bytes), timeout, cancellation).await?;
    telemetry.record_stream_request(length)?;
    Ok(sent)
}

#[cfg(test)]
pub(crate) async fn send_streamer_request_for_test(
    connection: &mut dyn SchwabStreamerConnection,
    request: TransientStreamerRequest,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<(), SchwabTransportError> {
    send_request(
        connection,
        request,
        &SchwabTransportTelemetry::default(),
        timeout,
        cancellation,
    )
    .await
    .map(|_| ())
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

async fn read_active_connection_input(
    connection: &mut dyn SchwabStreamerConnection,
    desired_state: Option<&mut mpsc::Receiver<(StreamerCommand, StreamerSubscription)>>,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<ActiveConnectionInput, SchwabTransportError> {
    let deadline = tokio::time::Instant::from_std(deadline);
    if let Some(desired_state) = desired_state {
        tokio::select! {
            () = cancellation.cancelled() => Err(SchwabTransportError::Cancelled),
            incoming = connection.next() => incoming?
                .map(ActiveConnectionInput::Incoming)
                .ok_or(SchwabTransportError::ResynchronizationRequired),
            update = desired_state.recv() => Ok(match update {
                Some((command, subscription)) => {
                    ActiveConnectionInput::DesiredState(command, subscription)
                }
                None => ActiveConnectionInput::DesiredStateChannelClosed,
            }),
            () = tokio::time::sleep_until(deadline) => Ok(ActiveConnectionInput::Deadline),
        }
    } else {
        tokio::select! {
            () = cancellation.cancelled() => Err(SchwabTransportError::Cancelled),
            incoming = connection.next() => incoming?
                .map(ActiveConnectionInput::Incoming)
                .ok_or(SchwabTransportError::ResynchronizationRequired),
            () = tokio::time::sleep_until(deadline) => Ok(ActiveConnectionInput::Deadline),
        }
    }
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
