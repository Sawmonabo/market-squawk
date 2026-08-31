//! Sender-minted subscription write receipts sealed to one captured connection generation.

use std::sync::Arc;

use market_squawk_domain::{CapturePayload, ConnectionGeneration, MetadataRevision, SourceId};
use market_squawk_sources::SourceMetadataProvider;
use tokio_tungstenite::tungstenite::Message;

use crate::config::{KrakenChannel, KrakenConfig, KrakenConfigError, public_subscription_payload};
use crate::handoff::{
    KrakenInstrumentBinding, KrakenSubscriptionRequestEvidence, instrument_binding_from_coordinates,
};
use crate::level3::KrakenL3SecretPayload;
use crate::messages::PUBLIC_SUBSCRIPTION_REQUEST_ID;

/// A public request awaiting the adapter-owned established-session sender.
///
/// This value is deliberately non-cloneable and non-serializable. Its exact retained payload is
/// the same byte sequence moved into the WebSocket text message by the private session sender.
#[derive(Debug)]
pub(crate) struct KrakenPublicSubscriptionRequest {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    connection_generation: ConnectionGeneration,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    channel: KrakenChannel,
    wire: String,
    payload: CapturePayload,
}

impl KrakenConfig {
    /// Builds the exact public subscription request for one registry-issued connection generation.
    ///
    /// Constructing this value is not evidence that bytes were sent. Only the adapter-owned
    /// established-session sender can mint successful-write evidence.
    ///
    /// # Errors
    ///
    /// Rejects an impossible serialization or retained-payload invariant failure.
    pub(crate) fn try_subscription_request(
        &self,
        connection_generation: ConnectionGeneration,
    ) -> Result<KrakenPublicSubscriptionRequest, KrakenConfigError> {
        let wire = public_subscription_payload(self.symbol(), self.channel())
            .map_err(|_| KrakenConfigError::SubscriptionSerialization)?;
        let payload = CapturePayload::try_from_live(wire.as_bytes())
            .map_err(|_| KrakenConfigError::SubscriptionSerialization)?;
        let instrument_binding = instrument_binding_from_coordinates(self.native_coordinates())
            .map_err(|_| KrakenConfigError::SubscriptionSerialization)?;
        Ok(KrakenPublicSubscriptionRequest {
            source_id: self.metadata().source_id().clone(),
            metadata_revision: self.metadata().revision().clone(),
            connection_generation,
            instrument_binding,
            channel: self.channel(),
            wire,
            payload,
        })
    }
}

impl KrakenPublicSubscriptionRequest {
    pub(crate) fn into_pending_write(self) -> KrakenPendingSubscriptionWrite {
        let Self {
            source_id,
            metadata_revision,
            connection_generation,
            instrument_binding,
            channel,
            wire,
            payload,
        } = self;
        KrakenPendingSubscriptionWrite {
            message: Message::Text(wire.into()),
            source_id,
            metadata_revision,
            connection_generation,
            request: KrakenSubscriptionRequestEvidence::PublicExact {
                request_id: PUBLIC_SUBSCRIPTION_REQUEST_ID,
                payload,
                instrument_binding,
                channel,
            },
        }
    }
}

impl KrakenL3SecretPayload {
    /// Transfers the exact authenticated request allocation to the private established-session
    /// sender beside its secret-free contract.
    ///
    /// No extra adapter-owned string copy is created: the zeroizing payload allocation is moved
    /// into tungstenite's text message. Once transferred, tungstenite owns that allocation and the
    /// adapter cannot promise that third-party transport memory is sanitized.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed serialization invariant without minting write evidence.
    #[allow(
        dead_code,
        reason = "the selected authenticated L3 session foundation consumes this opaque payload"
    )]
    pub(crate) fn into_pending_write(
        self,
        connection_generation: ConnectionGeneration,
    ) -> Result<KrakenPendingSubscriptionWrite, crate::level3::KrakenL3ConfigError> {
        let (wire, request) = self.into_transport_parts()?;
        Ok(KrakenPendingSubscriptionWrite {
            message: Message::Text(wire.into()),
            source_id: request.source_id().clone(),
            metadata_revision: request.metadata_revision().clone(),
            connection_generation,
            request: KrakenSubscriptionRequestEvidence::AuthenticatedSecretBearing {
                request_evidence: std::sync::Arc::new(request),
            },
        })
    }
}

/// Exact outbound message and typed request contract held only by the established-session sender.
pub(crate) struct KrakenPendingSubscriptionWrite {
    message: Message,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    connection_generation: ConnectionGeneration,
    request: KrakenSubscriptionRequestEvidence,
}

impl KrakenPendingSubscriptionWrite {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Message,
        SourceId,
        MetadataRevision,
        ConnectionGeneration,
        KrakenSubscriptionRequestEvidence,
    ) {
        (
            self.message,
            self.source_id,
            self.metadata_revision,
            self.connection_generation,
            self.request,
        )
    }
}
