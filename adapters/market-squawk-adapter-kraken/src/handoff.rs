//! Consuming capture-bound handoff from Kraken decoders to the shared market-event mapper.

use std::sync::Arc;

use crate::config::KrakenChannel;
use crate::level3::{
    KrakenL3BatchKind, KrakenL3BookBatch, KrakenL3Control, KrakenL3DecodeError,
    KrakenL3SubscriptionRequestEvidence,
};
use market_squawk_domain::{
    CapturePayload, EvidenceDigest, InstrumentId, LiveEventClass, MarketDepth, ProviderChannel,
    ProviderProduct, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    ControlFrameKind, DecodeInternalError, DecodeOutcome, DecodedProviderBatch, DecoderEvidence,
    FrameId, FrameSessionBinding, IgnoredFrameReason, ProviderChecksumEvidence,
    ProviderSequenceEvidence, QuarantineReason, ResynchronizationReason, SourceMetadata,
    TransportFrameKind,
};

const PROVIDER: &str = "kraken";
const PRODUCT: &str = "kraken-spot";
const PUBLIC_BOOK_CHANNEL: &str = "book-v2";
const PUBLIC_TRADE_CHANNEL: &str = "trade-v2";
const AUTHENTICATED_LEVEL3_CHANNEL: &str = "level3-v2";
const MAX_PROVIDER_TEXT_BYTES: usize = 512;

/// Exact Kraken transport/feed surface represented by a handoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenFeed {
    /// Public Kraken Spot WebSocket v2 book or trade channel.
    PublicSpotWebSocketV2,
    /// User-authorized Kraken Spot WebSocket v2 `level3` channel.
    AuthenticatedSpotLevel3WebSocketV2,
}

/// Exact request evidence retained without persisting authenticated secret material.
#[derive(Debug, Eq, PartialEq)]
pub enum KrakenSubscriptionRequestEvidence {
    /// Exact public request bytes emitted by the adapter.
    PublicExact {
        /// Fixed public request identifier encoded on the wire.
        request_id: u64,
        /// Exact bounded outbound UTF-8 request.
        payload: CapturePayload,
        /// Exact provider-native symbol to externally resolved instrument mapping encoded by the
        /// sender beside the wire request.
        instrument_binding: Arc<KrakenInstrumentBinding>,
        /// Exact independently registered public subscription surface.
        channel: KrakenChannel,
    },
    /// Exact typed secret-free authenticated request contract created beside the emitted bytes.
    ///
    /// The actual request contains a short-lived token and is intentionally neither retained nor
    /// hashed into durable evidence. Exact inbound acknowledgement evidence is retained separately.
    AuthenticatedSecretBearing {
        /// Exact batch contract. Token material is deliberately absent.
        request_evidence: Arc<KrakenL3SubscriptionRequestEvidence>,
    },
}

/// Provider/product/channel binding shared by every result from one configured Kraken surface.
#[derive(Debug)]
pub struct KrakenConnectionBinding {
    provider: SourceIdentifier,
    venue: VenueId,
    provider_product: ProviderProduct,
    provider_channel: ProviderChannel,
    feed: KrakenFeed,
    depth: Option<MarketDepth>,
    subscription_request: Option<KrakenSubscriptionRequestEvidence>,
}

impl KrakenConnectionBinding {
    /// Returns the exact provider identity from immutable source metadata.
    pub const fn provider(&self) -> &SourceIdentifier {
        &self.provider
    }

    /// Returns the direct venue identity.
    pub const fn venue(&self) -> &VenueId {
        &self.venue
    }

    /// Returns the provider-native product identity.
    pub const fn provider_product(&self) -> &ProviderProduct {
        &self.provider_product
    }

    /// Returns the independently registered provider channel.
    pub const fn provider_channel(&self) -> &ProviderChannel {
        &self.provider_channel
    }

    /// Returns the exact public or authenticated feed surface.
    pub const fn feed(&self) -> KrakenFeed {
        self.feed
    }

    /// Returns event depth. Trades have no book depth.
    pub const fn depth(&self) -> Option<MarketDepth> {
        self.depth
    }

    /// Returns exact public request evidence or the secret-free authenticated contract.
    pub const fn subscription_request(&self) -> Option<&KrakenSubscriptionRequestEvidence> {
        self.subscription_request.as_ref()
    }
}

/// Provider-native symbol plus a caller-supplied canonical binding.
///
/// The adapter validates this relationship but never creates an [`InstrumentId`].
#[derive(Debug)]
pub struct KrakenInstrumentBinding {
    native_symbol: SourceIdentifier,
    externally_resolved_instrument: InstrumentId,
}

impl PartialEq for KrakenInstrumentBinding {
    fn eq(&self, other: &Self) -> bool {
        self.native_symbol == other.native_symbol
            && self.externally_resolved_instrument == other.externally_resolved_instrument
    }
}

impl Eq for KrakenInstrumentBinding {}

impl KrakenInstrumentBinding {
    /// Returns the exact Kraken product symbol.
    pub const fn native_symbol(&self) -> &SourceIdentifier {
        &self.native_symbol
    }

    /// Returns the instrument identity supplied by external reference authority.
    pub const fn externally_resolved_instrument(&self) -> InstrumentId {
        self.externally_resolved_instrument
    }
}

/// Truthful availability of a provider-native channel sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenSequenceAvailability {
    /// Kraken supplies no channel sequence for these admitted Spot surfaces.
    ProviderUnsupported,
}

/// Truthful checksum availability after provider-specific validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenChecksumAvailability {
    /// Provider checksum matched the complete candidate state.
    Validated(u32),
    /// The exact channel does not supply a checksum.
    Unsupported,
}

/// Snapshot or incremental-update relationship to prior state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenBookTransition {
    /// A complete initializing image.
    Snapshot,
    /// A message-atomic successor change.
    Update,
}

/// Provider continuity facts retained with one market-data handoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenMarketContinuity {
    /// Public price-level book state, checksum-validated without provider sequence.
    PriceLevelBook {
        /// Snapshot or update semantics.
        transition: KrakenBookTransition,
        /// Validated public book checksum.
        checksum: KrakenChecksumAvailability,
        /// Explicit provider sequence availability.
        sequence: KrakenSequenceAvailability,
    },
    /// Public trades. Trade identifiers are event identities, not channel sequence.
    Trades {
        /// Observations decoded from the exact frame.
        event_count: usize,
        /// Explicit checksum availability.
        checksum: KrakenChecksumAvailability,
        /// Explicit provider sequence availability.
        sequence: KrakenSequenceAvailability,
    },
    /// Authenticated individual-order book state.
    AuthenticatedLevel3 {
        /// Snapshot or update semantics.
        transition: KrakenBookTransition,
        /// Validated order-level checksum.
        checksum: KrakenChecksumAvailability,
        /// Connection-generation-local diagnostic ordinal, never provider sequence.
        local_generation_ordinal: u64,
        /// Explicit provider sequence availability.
        sequence: KrakenSequenceAvailability,
    },
}

/// Exact captured acknowledgement binding for an authenticated product subscription.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenSubscriptionAcknowledgementEvidence {
    request_id: Option<u64>,
    provider_request_received_at: Timestamp,
    provider_response_sent_at: Timestamp,
    binding: FrameSessionBinding,
    frame_id: FrameId,
    received_at: Timestamp,
    payload_digest: EvidenceDigest,
}

impl KrakenSubscriptionAcknowledgementEvidence {
    /// Returns the provider request identifier, when supplied.
    pub const fn request_id(&self) -> Option<u64> {
        self.request_id
    }

    /// Returns the provider clock at which Kraken accepted the subscription request.
    pub const fn provider_request_received_at(&self) -> Timestamp {
        self.provider_request_received_at
    }

    /// Returns the provider clock at which Kraken emitted the acknowledgement.
    pub const fn provider_response_sent_at(&self) -> Timestamp {
        self.provider_response_sent_at
    }

    /// Returns the exact source/session/generation binding of the acknowledgement frame.
    pub const fn binding(&self) -> &FrameSessionBinding {
        &self.binding
    }

    /// Returns the exact generation-local acknowledgement frame identity.
    pub const fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Returns the trusted local receipt time of the acknowledgement.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns SHA-256 over the exact captured acknowledgement bytes.
    pub const fn payload_digest(&self) -> EvidenceDigest {
        self.payload_digest
    }
}

/// Authenticated discontinuity that prevents a frame from becoming market state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenAuthenticatedDiscontinuity {
    /// Closed provider decoder failure.
    Decode {
        /// Exact decoder failure.
        error: KrakenL3DecodeError,
        /// Every configured product on the connection is quarantined.
        scope: KrakenDiscontinuityScope,
    },
    /// A market frame arrived through the captured boundary without an exact captured
    /// acknowledgement from the same source session and connection generation.
    MissingCapturedSubscriptionAcknowledgement {
        /// Every configured product on the connection is quarantined.
        scope: KrakenDiscontinuityScope,
    },
    /// Subscription authority or provider control retired this complete connection generation.
    GenerationRetired {
        /// Exact closed retirement reason.
        reason: KrakenGenerationRetirement,
    },
}

/// Explicit market-state scope invalidated by an authenticated discontinuity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenDiscontinuityScope {
    /// The authenticated decoder quarantines every configured product atomically.
    AllConfiguredProducts,
}

/// Bounded exact provider text retained from a typed control without classifying or rewriting it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KrakenProviderText(String);

impl KrakenProviderText {
    pub(crate) fn try_new(value: &str) -> Result<Self, DecodeInternalError> {
        if value.is_empty() || value.len() > MAX_PROVIDER_TEXT_BYTES {
            return Err(DecodeInternalError::InvariantViolation);
        }
        Ok(Self(value.to_owned()))
    }

    /// Returns the exact provider-supplied text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Terminal reason why a decoder allocation can never recover in place.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenGenerationRetirement {
    /// Sender evidence did not exactly match configured subscription authority.
    SubscriptionAuthorityRejected,
    /// A second acknowledgement attempted to reuse already established authority.
    DuplicateSubscriptionAcknowledgement,
    /// The provider explicitly refused the configured subscription.
    SubscriptionRefused,
    /// The provider explicitly reset the configured generation.
    ProviderReset,
    /// Control-plane syntax or state was inconsistent with this connection generation.
    ProtocolControlViolation,
    /// A later frame was presented to an allocation that was already retired.
    AlreadyRetired,
}

/// Lossless public Spot control. These values never refresh market data.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KrakenPublicControl {
    /// Connection liveness only.
    Heartbeat,
    /// Application pong with exact provider correlation and ordered clocks.
    Pong {
        /// Provider request identifier, when supplied.
        request_id: Option<u64>,
        /// Provider clock at request receipt.
        provider_request_received_at: Timestamp,
        /// Provider clock at response emission.
        provider_response_sent_at: Timestamp,
    },
    /// Exchange engine reported `online`.
    Online,
    /// Exact configured public subscription was acknowledged.
    Subscribed {
        /// Exact acknowledged public channel.
        channel: KrakenChannel,
        /// Provider request identifier.
        request_id: u64,
        /// Provider clock at request receipt.
        provider_request_received_at: Timestamp,
        /// Provider clock at acknowledgement emission.
        provider_response_sent_at: Timestamp,
    },
    /// Provider refused the exact configured request; this retires the generation.
    SubscriptionRefused {
        /// Provider request identifier, when supplied.
        request_id: Option<u64>,
        /// Provider clock at request receipt.
        provider_request_received_at: Timestamp,
        /// Provider clock at response emission.
        provider_response_sent_at: Timestamp,
        /// Bounded exact provider error text.
        error: KrakenProviderText,
    },
    /// Provider status explicitly reset this generation.
    ProviderReset {
        /// Exact bounded provider status value.
        system: KrakenProviderText,
    },
}

impl KrakenPublicControl {
    /// Returns a supplemental shared control classification without discarding typed evidence.
    pub const fn generic_kind(&self) -> ControlFrameKind {
        match self {
            Self::Heartbeat => ControlFrameKind::Heartbeat,
            Self::Pong { .. } => ControlFrameKind::Pong,
            Self::Online | Self::ProviderReset { .. } => ControlFrameKind::ProviderFlowControl,
            Self::Subscribed { .. } | Self::SubscriptionRefused { .. } => {
                ControlFrameKind::SubscriptionAcknowledgement
            }
        }
    }
}

/// Typed control or discontinuity classification. These outcomes never refresh market data.
#[derive(Debug, Eq, PartialEq)]
pub enum KrakenControlOrDiscontinuityKind {
    /// Public protocol control.
    PublicControl(KrakenPublicControl),
    /// Public connection generation is terminal and requires a new decoder and sender receipt.
    PublicGenerationRetired(KrakenGenerationRetirement),
    /// Documented public no-op or extension.
    PublicIgnored(IgnoredFrameReason),
    /// Public stream requires snapshot/reset recovery.
    PublicResynchronize(ResynchronizationReason),
    /// Public generation is unsafe and quarantined.
    PublicQuarantine(QuarantineReason),
    /// Authenticated L3 protocol control.
    AuthenticatedControl(KrakenL3Control),
    /// Authenticated L3 discontinuity.
    AuthenticatedDiscontinuity(KrakenAuthenticatedDiscontinuity),
}

#[derive(Debug)]
struct KrakenNativeFrame {
    payload: CapturePayload,
    transport: TransportFrameKind,
}

impl KrakenNativeFrame {
    fn payload(&self) -> &[u8] {
        self.payload.as_bytes()
    }
}

/// Public Spot market observations plus their exact captured frame.
#[derive(Debug)]
pub struct KrakenPublicMarketEventHandoff {
    native_frame: KrakenNativeFrame,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    subscription_acknowledgement: KrakenSubscriptionAcknowledgementEvidence,
    continuity: KrakenMarketContinuity,
    batch: DecodedProviderBatch,
}

impl KrakenPublicMarketEventHandoff {
    /// Returns exact captured provider bytes without reserialization.
    pub fn native_payload(&self) -> &[u8] {
        self.native_frame.payload()
    }

    /// Returns the exact transport kind.
    pub const fn transport(&self) -> TransportFrameKind {
        self.native_frame.transport
    }

    /// Returns exact source/revision/session/generation, receipt, frame, digest, and decoder rule.
    pub const fn evidence(&self) -> &DecoderEvidence {
        self.batch.evidence()
    }

    /// Returns SHA-256 over the exact retained native payload.
    pub const fn native_payload_digest(&self) -> EvidenceDigest {
        self.batch.evidence().payload_digest()
    }

    /// Returns provider, venue, product, channel, feed, depth, and request evidence.
    pub fn connection(&self) -> &KrakenConnectionBinding {
        &self.connection
    }

    /// Returns provider-native symbol and externally supplied instrument binding.
    pub fn instrument_binding(&self) -> &KrakenInstrumentBinding {
        &self.instrument_binding
    }

    /// Returns the captured acknowledgement from this exact source connection generation.
    pub const fn subscription_acknowledgement(&self) -> &KrakenSubscriptionAcknowledgementEvidence {
        &self.subscription_acknowledgement
    }

    /// Returns provider checksum/sequence/snapshot continuity semantics.
    pub const fn continuity(&self) -> KrakenMarketContinuity {
        self.continuity
    }

    /// Returns the already decoded provider-normalized observations.
    pub const fn batch(&self) -> &DecodedProviderBatch {
        &self.batch
    }

    /// Consumes the handoff into exact native bytes and typed observations for shared mapping.
    #[allow(
        clippy::type_complexity,
        reason = "the consuming boundary keeps every evidence axis explicit"
    )]
    pub fn into_parts(
        self,
    ) -> (
        CapturePayload,
        TransportFrameKind,
        Arc<KrakenConnectionBinding>,
        Arc<KrakenInstrumentBinding>,
        KrakenSubscriptionAcknowledgementEvidence,
        KrakenMarketContinuity,
        DecodedProviderBatch,
    ) {
        (
            self.native_frame.payload,
            self.native_frame.transport,
            self.connection,
            self.instrument_binding,
            self.subscription_acknowledgement,
            self.continuity,
            self.batch,
        )
    }
}

/// Authenticated order-level market batch plus its exact captured frame and acknowledgement.
#[derive(Debug)]
pub struct KrakenAuthenticatedLevel3MarketEventHandoff {
    native_frame: KrakenNativeFrame,
    evidence: DecoderEvidence,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    subscription_acknowledgement: KrakenSubscriptionAcknowledgementEvidence,
    continuity: KrakenMarketContinuity,
    batch: KrakenL3BookBatch,
}

impl KrakenAuthenticatedLevel3MarketEventHandoff {
    /// Returns exact captured provider bytes without reserialization.
    pub fn native_payload(&self) -> &[u8] {
        self.native_frame.payload()
    }

    /// Returns the exact transport kind.
    pub const fn transport(&self) -> TransportFrameKind {
        self.native_frame.transport
    }

    /// Returns exact source/revision/session/generation, receipt, frame, digest, and decoder rule.
    pub const fn evidence(&self) -> &DecoderEvidence {
        &self.evidence
    }

    /// Returns SHA-256 over the exact retained native payload.
    pub const fn native_payload_digest(&self) -> EvidenceDigest {
        self.evidence.payload_digest()
    }

    /// Returns provider, venue, product, channel, feed, depth, and request-contract evidence.
    pub fn connection(&self) -> &KrakenConnectionBinding {
        &self.connection
    }

    /// Returns provider-native symbol and externally supplied instrument binding.
    pub fn instrument_binding(&self) -> &KrakenInstrumentBinding {
        &self.instrument_binding
    }

    /// Returns the exact captured subscription acknowledgement for this product and generation.
    pub const fn subscription_acknowledgement(&self) -> &KrakenSubscriptionAcknowledgementEvidence {
        &self.subscription_acknowledgement
    }

    /// Returns checksum, local ordinal, absent provider sequence, and snapshot/update semantics.
    pub const fn continuity(&self) -> KrakenMarketContinuity {
        self.continuity
    }

    /// Returns the already decoded order-identity-preserving batch.
    pub const fn batch(&self) -> &KrakenL3BookBatch {
        &self.batch
    }

    /// Consumes the handoff into exact native bytes and the typed L3 batch for shared mapping.
    #[allow(
        clippy::type_complexity,
        reason = "the consuming boundary keeps every evidence axis explicit"
    )]
    pub fn into_parts(
        self,
    ) -> (
        CapturePayload,
        TransportFrameKind,
        DecoderEvidence,
        Arc<KrakenConnectionBinding>,
        Arc<KrakenInstrumentBinding>,
        KrakenSubscriptionAcknowledgementEvidence,
        KrakenMarketContinuity,
        KrakenL3BookBatch,
    ) {
        (
            self.native_frame.payload,
            self.native_frame.transport,
            self.evidence,
            self.connection,
            self.instrument_binding,
            self.subscription_acknowledgement,
            self.continuity,
            self.batch,
        )
    }
}

/// Control or discontinuity plus exact captured bytes. It never carries market freshness.
#[derive(Debug)]
pub struct KrakenControlOrDiscontinuityHandoff {
    native_frame: KrakenNativeFrame,
    evidence: DecoderEvidence,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Option<Arc<KrakenInstrumentBinding>>,
    subscription_acknowledgement: Option<KrakenSubscriptionAcknowledgementEvidence>,
    kind: KrakenControlOrDiscontinuityKind,
    provider_code: Option<SourceIdentifier>,
}

impl KrakenControlOrDiscontinuityHandoff {
    /// Returns exact captured provider bytes without reserialization.
    pub fn native_payload(&self) -> &[u8] {
        self.native_frame.payload()
    }

    /// Returns the exact transport kind.
    pub const fn transport(&self) -> TransportFrameKind {
        self.native_frame.transport
    }

    /// Returns exact source/revision/session/generation, receipt, frame, digest, and decoder rule.
    pub const fn evidence(&self) -> &DecoderEvidence {
        &self.evidence
    }

    /// Returns SHA-256 over the exact retained native payload.
    pub const fn native_payload_digest(&self) -> EvidenceDigest {
        self.evidence.payload_digest()
    }

    /// Returns connection-level provider/feed/subscription evidence.
    pub fn connection(&self) -> &KrakenConnectionBinding {
        &self.connection
    }

    /// Returns exact product binding only when the control or failure unambiguously identifies it.
    pub fn instrument_binding(&self) -> Option<&KrakenInstrumentBinding> {
        self.instrument_binding.as_deref()
    }

    /// Returns captured authenticated acknowledgement evidence when this result establishes it.
    pub const fn subscription_acknowledgement(
        &self,
    ) -> Option<&KrakenSubscriptionAcknowledgementEvidence> {
        self.subscription_acknowledgement.as_ref()
    }

    /// Returns the typed non-market disposition.
    pub const fn kind(&self) -> &KrakenControlOrDiscontinuityKind {
        &self.kind
    }

    /// Returns a bounded provider-defined public control/error code, when supplied.
    pub const fn provider_code(&self) -> Option<&SourceIdentifier> {
        self.provider_code.as_ref()
    }

    /// Consumes the non-market handoff without dropping any evidence field.
    #[allow(
        clippy::type_complexity,
        reason = "the consuming boundary keeps every evidence axis explicit"
    )]
    pub fn into_parts(
        self,
    ) -> (
        CapturePayload,
        TransportFrameKind,
        DecoderEvidence,
        Arc<KrakenConnectionBinding>,
        Option<Arc<KrakenInstrumentBinding>>,
        Option<KrakenSubscriptionAcknowledgementEvidence>,
        KrakenControlOrDiscontinuityKind,
        Option<SourceIdentifier>,
    ) {
        (
            self.native_frame.payload,
            self.native_frame.transport,
            self.evidence,
            self.connection,
            self.instrument_binding,
            self.subscription_acknowledgement,
            self.kind,
            self.provider_code,
        )
    }
}

/// Closed consuming Kraken result. No variant is `Clone` or serializable.
#[derive(Debug)]
pub enum KrakenMarketEventHandoff {
    /// Public price-level book or trade observations.
    Public(KrakenPublicMarketEventHandoff),
    /// Authenticated individual-order market batch.
    AuthenticatedLevel3(KrakenAuthenticatedLevel3MarketEventHandoff),
    /// Protocol control or explicit discontinuity; never market data.
    ControlOrDiscontinuity(KrakenControlOrDiscontinuityHandoff),
}

impl KrakenMarketEventHandoff {
    /// Returns exact source/revision/session/generation, receipt, frame, digest, and decoder rule.
    pub const fn evidence(&self) -> &DecoderEvidence {
        match self {
            Self::Public(handoff) => handoff.evidence(),
            Self::AuthenticatedLevel3(handoff) => handoff.evidence(),
            Self::ControlOrDiscontinuity(handoff) => handoff.evidence(),
        }
    }

    /// Returns exact captured provider bytes for every disposition.
    pub fn native_payload(&self) -> &[u8] {
        match self {
            Self::Public(handoff) => handoff.native_payload(),
            Self::AuthenticatedLevel3(handoff) => handoff.native_payload(),
            Self::ControlOrDiscontinuity(handoff) => handoff.native_payload(),
        }
    }

    /// Returns the exact transport kind for every disposition.
    pub const fn transport(&self) -> TransportFrameKind {
        match self {
            Self::Public(handoff) => handoff.transport(),
            Self::AuthenticatedLevel3(handoff) => handoff.transport(),
            Self::ControlOrDiscontinuity(handoff) => handoff.transport(),
        }
    }
}

pub(crate) fn from_public_outcome(
    native_frame: CapturePayload,
    transport: TransportFrameKind,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    subscription_acknowledgement: Option<KrakenSubscriptionAcknowledgementEvidence>,
    continuity: Option<KrakenMarketContinuity>,
    outcome: DecodeOutcome,
) -> KrakenMarketEventHandoff {
    let native_frame = KrakenNativeFrame {
        payload: native_frame,
        transport,
    };
    match outcome {
        DecodeOutcome::Data(batch) => {
            let subscription_acknowledgement =
                subscription_acknowledgement.filter(|acknowledgement| {
                    acknowledgement
                        .binding()
                        .shares_allocation_with(batch.evidence().binding())
                });
            let (Some(subscription_acknowledgement), Some(continuity)) =
                (subscription_acknowledgement, continuity)
            else {
                return control_handoff(
                    native_frame,
                    batch.evidence().clone(),
                    connection,
                    Some(instrument_binding),
                    KrakenControlOrDiscontinuityKind::PublicQuarantine(
                        QuarantineReason::ProtocolInvariantViolation,
                    ),
                    None,
                    None,
                );
            };
            KrakenMarketEventHandoff::Public(KrakenPublicMarketEventHandoff {
                native_frame,
                connection,
                instrument_binding,
                subscription_acknowledgement,
                continuity,
                batch,
            })
        }
        DecodeOutcome::Control(disposition) => control_handoff(
            native_frame,
            disposition.evidence().clone(),
            connection,
            Some(instrument_binding),
            KrakenControlOrDiscontinuityKind::PublicQuarantine(
                QuarantineReason::ProtocolInvariantViolation,
            ),
            disposition.provider_code().cloned(),
            subscription_acknowledgement,
        ),
        DecodeOutcome::Ignored(disposition) => control_handoff(
            native_frame,
            disposition.evidence().clone(),
            connection,
            Some(instrument_binding),
            KrakenControlOrDiscontinuityKind::PublicIgnored(disposition.reason()),
            disposition.provider_code().cloned(),
            subscription_acknowledgement,
        ),
        DecodeOutcome::Resynchronize(disposition) => control_handoff(
            native_frame,
            disposition.evidence().clone(),
            connection,
            Some(instrument_binding),
            KrakenControlOrDiscontinuityKind::PublicResynchronize(disposition.reason()),
            disposition.provider_code().cloned(),
            subscription_acknowledgement,
        ),
        DecodeOutcome::Quarantine(disposition) => control_handoff(
            native_frame,
            disposition.evidence().clone(),
            connection,
            Some(instrument_binding),
            KrakenControlOrDiscontinuityKind::PublicQuarantine(disposition.reason()),
            disposition.provider_code().cloned(),
            subscription_acknowledgement,
        ),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "public control evidence remains explicit at the consuming boundary"
)]
pub(crate) fn public_control_handoff(
    native_frame: CapturePayload,
    transport: TransportFrameKind,
    evidence: DecoderEvidence,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    subscription_acknowledgement: Option<KrakenSubscriptionAcknowledgementEvidence>,
    control: KrakenPublicControl,
) -> KrakenMarketEventHandoff {
    control_handoff(
        KrakenNativeFrame {
            payload: native_frame,
            transport,
        },
        evidence,
        connection,
        Some(instrument_binding),
        KrakenControlOrDiscontinuityKind::PublicControl(control),
        None,
        subscription_acknowledgement,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "public retirement evidence remains explicit at the consuming boundary"
)]
pub(crate) fn public_retirement_handoff(
    native_frame: CapturePayload,
    transport: TransportFrameKind,
    evidence: DecoderEvidence,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    subscription_acknowledgement: Option<KrakenSubscriptionAcknowledgementEvidence>,
    reason: KrakenGenerationRetirement,
) -> KrakenMarketEventHandoff {
    control_handoff(
        KrakenNativeFrame {
            payload: native_frame,
            transport,
        },
        evidence,
        connection,
        Some(instrument_binding),
        KrakenControlOrDiscontinuityKind::PublicGenerationRetired(reason),
        None,
        subscription_acknowledgement,
    )
}

pub(crate) fn authenticated_connection(
    metadata: &SourceMetadata,
    request_evidence: Option<Arc<KrakenL3SubscriptionRequestEvidence>>,
) -> Result<Arc<KrakenConnectionBinding>, DecodeInternalError> {
    let live = metadata
        .coverage()
        .live()
        .ok_or(DecodeInternalError::InvariantViolation)?;
    if metadata.provider().as_str() != PROVIDER
        || live.provider_product().as_source_identifier().as_str() != PRODUCT
        || live.provider_channel().as_source_identifier().as_str() != AUTHENTICATED_LEVEL3_CHANNEL
    {
        return Err(DecodeInternalError::InvariantViolation);
    }
    Ok(Arc::new(KrakenConnectionBinding {
        provider: metadata.provider().clone(),
        venue: VenueId::try_from(PROVIDER).map_err(|_| DecodeInternalError::InvariantViolation)?,
        provider_product: live.provider_product().clone(),
        provider_channel: live.provider_channel().clone(),
        feed: KrakenFeed::AuthenticatedSpotLevel3WebSocketV2,
        depth: Some(MarketDepth::OrderLevel),
        subscription_request: request_evidence.map(|request_evidence| {
            KrakenSubscriptionRequestEvidence::AuthenticatedSecretBearing { request_evidence }
        }),
    }))
}

#[allow(
    clippy::too_many_arguments,
    reason = "authenticated handoff relationships stay explicit"
)]
pub(crate) fn authenticated_market_handoff(
    native_frame: CapturePayload,
    transport: TransportFrameKind,
    evidence: DecoderEvidence,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    batch: KrakenL3BookBatch,
    acknowledgement: KrakenSubscriptionAcknowledgementEvidence,
) -> KrakenMarketEventHandoff {
    let transition = match batch.kind() {
        KrakenL3BatchKind::Snapshot => KrakenBookTransition::Snapshot,
        KrakenL3BatchKind::Update => KrakenBookTransition::Update,
    };
    let continuity = KrakenMarketContinuity::AuthenticatedLevel3 {
        transition,
        checksum: KrakenChecksumAvailability::Validated(batch.checksum()),
        local_generation_ordinal: batch.local_generation_ordinal(),
        sequence: KrakenSequenceAvailability::ProviderUnsupported,
    };
    KrakenMarketEventHandoff::AuthenticatedLevel3(KrakenAuthenticatedLevel3MarketEventHandoff {
        native_frame: KrakenNativeFrame {
            payload: native_frame,
            transport,
        },
        evidence,
        connection,
        instrument_binding,
        subscription_acknowledgement: acknowledgement,
        continuity,
        batch,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "authenticated discontinuity evidence stays explicit"
)]
pub(crate) fn authenticated_control_or_discontinuity(
    native_frame: CapturePayload,
    transport: TransportFrameKind,
    evidence: DecoderEvidence,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Option<Arc<KrakenInstrumentBinding>>,
    acknowledgement: Option<KrakenSubscriptionAcknowledgementEvidence>,
    kind: KrakenControlOrDiscontinuityKind,
) -> KrakenMarketEventHandoff {
    control_handoff(
        KrakenNativeFrame {
            payload: native_frame,
            transport,
        },
        evidence,
        connection,
        instrument_binding,
        kind,
        None,
        acknowledgement,
    )
}

pub(crate) fn captured_acknowledgement(
    evidence: &DecoderEvidence,
    request_id: Option<u64>,
    provider_request_received_at: Timestamp,
    provider_response_sent_at: Timestamp,
) -> KrakenSubscriptionAcknowledgementEvidence {
    KrakenSubscriptionAcknowledgementEvidence {
        request_id,
        provider_request_received_at,
        provider_response_sent_at,
        binding: evidence.binding().clone(),
        frame_id: evidence.frame_id(),
        received_at: evidence.received_at(),
        payload_digest: evidence.payload_digest(),
    }
}

pub(crate) fn instrument_binding(
    symbol: &str,
    instrument: InstrumentId,
) -> Result<Arc<KrakenInstrumentBinding>, DecodeInternalError> {
    Ok(Arc::new(KrakenInstrumentBinding {
        native_symbol: SourceIdentifier::try_from(symbol)
            .map_err(|_| DecodeInternalError::InvariantViolation)?,
        externally_resolved_instrument: instrument,
    }))
}

pub(crate) fn public_connection(
    metadata: &SourceMetadata,
    channel: KrakenChannel,
    subscription_request: Option<KrakenSubscriptionRequestEvidence>,
) -> Result<Arc<KrakenConnectionBinding>, DecodeInternalError> {
    let live = metadata
        .coverage()
        .live()
        .ok_or(DecodeInternalError::InvariantViolation)?;
    let (expected_channel, depth) = match channel {
        KrakenChannel::Book(_) => (PUBLIC_BOOK_CHANNEL, Some(MarketDepth::PriceLevel)),
        KrakenChannel::Trades => (PUBLIC_TRADE_CHANNEL, None),
    };
    if metadata.provider().as_str() != PROVIDER
        || live.provider_product().as_source_identifier().as_str() != PRODUCT
        || live.provider_channel().as_source_identifier().as_str() != expected_channel
    {
        return Err(DecodeInternalError::InvariantViolation);
    }
    Ok(Arc::new(KrakenConnectionBinding {
        provider: metadata.provider().clone(),
        venue: VenueId::try_from(PROVIDER).map_err(|_| DecodeInternalError::InvariantViolation)?,
        provider_product: live.provider_product().clone(),
        provider_channel: live.provider_channel().clone(),
        feed: KrakenFeed::PublicSpotWebSocketV2,
        depth,
        subscription_request,
    }))
}

pub(crate) fn public_continuity(
    batch: &DecodedProviderBatch,
    connection: &KrakenConnectionBinding,
    binding: &KrakenInstrumentBinding,
    channel: KrakenChannel,
) -> Result<KrakenMarketContinuity, DecodeInternalError> {
    let observations = batch.observations();
    if observations.is_empty()
        || observations.iter().any(|observation| {
            observation.venue() != connection.venue()
                || observation.instrument() != binding.externally_resolved_instrument()
                || !matches!(
                    observation.sequence(),
                    ProviderSequenceEvidence::Unsupported { .. }
                )
        })
    {
        return Err(DecodeInternalError::InvariantViolation);
    }
    match channel {
        KrakenChannel::Book(_) => {
            if observations.len() != 1 {
                return Err(DecodeInternalError::InvariantViolation);
            }
            let observation = &observations[0];
            let transition = match observation.event_class() {
                LiveEventClass::BookSnapshot => KrakenBookTransition::Snapshot,
                LiveEventClass::BookDelta => KrakenBookTransition::Update,
                _ => return Err(DecodeInternalError::InvariantViolation),
            };
            if observation.depth() != Some(MarketDepth::PriceLevel) {
                return Err(DecodeInternalError::InvariantViolation);
            }
            let ProviderChecksumEvidence::Provided { value, .. } = observation.checksum() else {
                return Err(DecodeInternalError::InvariantViolation);
            };
            let checksum = value
                .as_str()
                .parse::<u32>()
                .map_err(|_| DecodeInternalError::InvariantViolation)?;
            Ok(KrakenMarketContinuity::PriceLevelBook {
                transition,
                checksum: KrakenChecksumAvailability::Validated(checksum),
                sequence: KrakenSequenceAvailability::ProviderUnsupported,
            })
        }
        KrakenChannel::Trades => {
            if observations.iter().any(|observation| {
                observation.event_class() != LiveEventClass::Trade
                    || observation.depth().is_some()
                    || !matches!(
                        observation.checksum(),
                        ProviderChecksumEvidence::Unsupported { .. }
                    )
            }) {
                return Err(DecodeInternalError::InvariantViolation);
            }
            Ok(KrakenMarketContinuity::Trades {
                event_count: observations.len(),
                checksum: KrakenChecksumAvailability::Unsupported,
                sequence: KrakenSequenceAvailability::ProviderUnsupported,
            })
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "closed disposition construction stays explicit"
)]
fn control_handoff(
    native_frame: KrakenNativeFrame,
    evidence: DecoderEvidence,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Option<Arc<KrakenInstrumentBinding>>,
    kind: KrakenControlOrDiscontinuityKind,
    provider_code: Option<SourceIdentifier>,
    subscription_acknowledgement: Option<KrakenSubscriptionAcknowledgementEvidence>,
) -> KrakenMarketEventHandoff {
    KrakenMarketEventHandoff::ControlOrDiscontinuity(KrakenControlOrDiscontinuityHandoff {
        native_frame,
        evidence,
        connection,
        instrument_binding,
        subscription_acknowledgement,
        kind,
        provider_code,
    })
}
