//! Exact-lexeme, message-atomic Kraken decoder and book state.

use std::num::NonZeroU16;
use std::sync::Arc;

use chrono::DateTime;
use market_squawk_domain::{
    AggressorSide, IntegrityRule, MarketDepth, RawCaptureFrameView, RuleVersion, SourceIdentifier,
    Timestamp, TradeTakerOrderType,
};
use market_squawk_sources::{
    ControlFrameKind, DecodeError, DecodeInternalError, DecodeOutcome, DecodedControlFrame,
    DecodedIgnoredFrame, DecodedProviderBatch, DecodedQuarantineAction, DecodedRecoveryAction,
    DecoderEvidence, FrameSessionBinding, MAX_DECODED_EVENTS, MAX_RAW_FRAME_BYTES,
    ProviderAggressorEvidence, ProviderBookChange, ProviderBookLevel, ProviderBookSide,
    ProviderChecksumEvidence, ProviderDecimalLexeme, ProviderNormalizedObservation,
    ProviderObservationPayload, ProviderPrice, ProviderQuantity, ProviderSequenceEvidence,
    ProviderSnapshotEvidence, ProviderTimestampEvidence, QuarantineReason, ResynchronizationReason,
    SourceMetadata, SourceMetadataProvider, SourceProtocolProfile, TransportFrameKind,
    ValidatedRawMarketFrame, kraken_v2_crc32,
};
use rust_decimal::Decimal;

use crate::config::{KrakenChannel, KrakenDepth, KrakenNativeMarketCoordinates};
use crate::handoff::{
    KrakenConnectionBinding, KrakenControlOrDiscontinuityKind, KrakenGenerationRetirement,
    KrakenInstrumentBinding, KrakenMarketContinuity, KrakenMarketEventHandoff, KrakenProviderText,
    KrakenPublicControl, KrakenSubscriptionAcknowledgementEvidence,
    KrakenSubscriptionRequestEvidence, captured_acknowledgement, from_public_outcome,
    instrument_binding_from_coordinates, public_connection, public_continuity,
    public_control_handoff, public_retirement_handoff,
};
use crate::messages::{
    BookData, BookEnvelope, EnvelopeKind, Heartbeat, MAX_SUBSCRIPTION_ERROR_BYTES,
    PUBLIC_SUBSCRIPTION_REQUEST_ID, Pong, StatusEnvelope, SubscribeAck, TradeData, TradeEnvelope,
    WireLevel, bounded_trade_count, classify, exact_decimal, validate_warnings,
};
use crate::qualification::{KRAKEN_BOOK_SEQUENCE_RULE, KRAKEN_TRADE_SEQUENCE_RULE};
use crate::session::KrakenSentSubscriptionReceipt;

/// Decoder synchronization state for one connection generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenDecoderState {
    /// A fresh snapshot is required.
    AwaitingSnapshot,
    /// Snapshot and every accepted update passed the checksum rule.
    Healthy,
    /// The generation is isolated; only a new snapshot may recover state.
    Quarantined,
    /// Subscription/control authority is terminal; this allocation can never recover in place.
    Retired,
}

#[derive(Debug)]
pub(crate) struct KrakenCapturedFrame {
    native_payload: market_squawk_domain::CapturePayload,
    transport: TransportFrameKind,
    evidence: DecoderEvidence,
}

/// Source-metadata-bound implementation of the shared synchronous decoder contract.
#[derive(Debug)]
pub struct KrakenMarketDecoder {
    metadata: SourceMetadata,
    decoder: KrakenDecoder,
    connection: Arc<KrakenConnectionBinding>,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    active_binding: Option<FrameSessionBinding>,
    subscription_acknowledgement: Option<KrakenSubscriptionAcknowledgementEvidence>,
    retirement_reason: Option<KrakenGenerationRetirement>,
}

impl KrakenMarketDecoder {
    /// Constructs a generation-local decoder bound to immutable source metadata.
    ///
    /// # Errors
    ///
    /// Rejects metadata that is not the reviewed Kraken live protocol profile.
    pub fn try_new(
        metadata: SourceMetadata,
        coordinates: KrakenNativeMarketCoordinates,
        depth: KrakenDepth,
    ) -> Result<Self, DecodeError> {
        Self::try_for_channel(metadata, coordinates, KrakenChannel::Book(depth))
    }

    /// Constructs a generation-local trade decoder bound to checksum-unsupported metadata.
    pub fn try_trades(
        metadata: SourceMetadata,
        coordinates: KrakenNativeMarketCoordinates,
    ) -> Result<Self, DecodeError> {
        Self::try_for_channel(metadata, coordinates, KrakenChannel::Trades)
    }

    fn try_for_channel(
        metadata: SourceMetadata,
        coordinates: KrakenNativeMarketCoordinates,
        channel: KrakenChannel,
    ) -> Result<Self, DecodeError> {
        if !coordinates.matches_surface(&metadata, channel) {
            return Err(DecodeError::InvalidProviderEvidence);
        }
        let coordinates = Arc::new(coordinates);
        let connection = public_connection(&metadata, Arc::clone(&coordinates), None)
            .map_err(|_| DecodeError::InvalidProviderEvidence)?;
        let instrument_binding = instrument_binding_from_coordinates(&coordinates)
            .map_err(|_| DecodeError::InvalidProviderEvidence)?;
        Ok(Self {
            metadata,
            decoder: KrakenDecoder::try_for_channel(Arc::clone(&coordinates), channel)?,
            connection,
            instrument_binding,
            active_binding: None,
            subscription_acknowledgement: None,
            retirement_reason: None,
        })
    }

    /// Returns current generation synchronization state.
    pub const fn state(&self) -> KrakenDecoderState {
        self.decoder.state()
    }

    /// Returns the mandatory exact provider-native coordinates.
    pub fn native_coordinates(&self) -> &KrakenNativeMarketCoordinates {
        self.decoder.native_coordinates()
    }

    /// Consumes sender-minted proof of the exact public request before any provider frame is
    /// decoded.
    ///
    /// # Errors
    ///
    /// Rejects a receipt for another source, metadata revision, product, channel, request ID, or
    /// exact public wire payload. A decoder accepts exactly one receipt and one frame allocation.
    pub fn register_sent_subscription(
        &mut self,
        receipt: KrakenSentSubscriptionReceipt,
    ) -> Result<(), DecodeError> {
        if self.active_binding.is_some()
            || self.subscription_acknowledgement.is_some()
            || self.decoder.state == KrakenDecoderState::Retired
        {
            return self.reject_subscription_authority();
        }
        let (binding, request) = receipt.into_parts();
        if binding.source_id() != self.metadata.source_id()
            || binding.metadata_revision() != self.metadata.revision()
        {
            return self.reject_subscription_authority();
        }
        self.active_binding = Some(binding);
        let KrakenSubscriptionRequestEvidence::PublicExact {
            request_id,
            payload,
            instrument_binding,
            channel,
        } = &request
        else {
            return self.reject_subscription_authority();
        };
        let expected_payload = match crate::config::public_subscription_payload(
            self.decoder.native_coordinates().venue_symbol().as_str(),
            self.decoder.channel,
        ) {
            Ok(payload) => payload,
            Err(_) => return self.reject_subscription_authority(),
        };
        if *request_id != PUBLIC_SUBSCRIPTION_REQUEST_ID
            || instrument_binding.native_symbol() != self.instrument_binding.native_symbol()
            || instrument_binding.provider_identity_key()
                != self.instrument_binding.provider_identity_key()
            || instrument_binding.venue_mapping() != self.instrument_binding.venue_mapping()
            || instrument_binding.externally_resolved_instrument()
                != self.instrument_binding.externally_resolved_instrument()
            || *channel != self.decoder.channel
            || payload.as_bytes() != expected_payload.as_bytes()
        {
            return self.reject_subscription_authority();
        }
        self.connection = match public_connection(
            &self.metadata,
            Arc::clone(&self.decoder.native_coordinates),
            Some(request),
        ) {
            Ok(connection) => connection,
            Err(_) => return self.reject_subscription_authority(),
        };
        Ok(())
    }

    fn reject_subscription_authority(&mut self) -> Result<(), DecodeError> {
        self.retire(KrakenGenerationRetirement::SubscriptionAuthorityRejected);
        Err(DecodeError::InvalidProviderEvidence)
    }

    fn retire(&mut self, reason: KrakenGenerationRetirement) {
        self.decoder.state = KrakenDecoderState::Retired;
        self.decoder.retirement_reason = Some(reason);
        self.retirement_reason = Some(reason);
    }

    /// Decodes one current validated frame into the consuming provider-owned handoff.
    ///
    /// Exact captured bytes, frame/session evidence, configured native symbol, provider product,
    /// channel, depth, and truthful continuity remain bound to the already typed observations.
    ///
    /// # Errors
    ///
    /// Returns only internal invariant/allocation failures. Provider input failures are closed
    /// control or discontinuity variants.
    pub fn decode_captured(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<KrakenMarketEventHandoff, DecodeInternalError> {
        let captured = self.prepare_captured(frame)?;
        self.decode_admitted(captured)
    }

    pub(crate) fn prepare_captured(
        &self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<KrakenCapturedFrame, DecodeInternalError> {
        if frame.frame().source_id() != self.metadata.source_id()
            || frame.frame().metadata_revision() != self.metadata.revision()
            || !self
                .decoder
                .native_coordinates()
                .is_valid_at(frame.frame().received_at())
        {
            return Err(DecodeInternalError::InvariantViolation);
        }
        let SourceProtocolProfile::Live(profile) = self.metadata.protocol_profile() else {
            return Err(DecodeInternalError::InvariantViolation);
        };
        Ok(KrakenCapturedFrame {
            native_payload: frame.frame().capture_payload().clone(),
            transport: frame.frame().transport(),
            evidence: DecoderEvidence::from_validated_frame(frame, profile.decoder_rule().clone()),
        })
    }

    pub(crate) fn decode_admitted(
        &mut self,
        captured: KrakenCapturedFrame,
    ) -> Result<KrakenMarketEventHandoff, DecodeInternalError> {
        match &self.active_binding {
            Some(binding) if !binding.shares_allocation_with(captured.evidence.binding()) => {
                return Err(DecodeInternalError::InvariantViolation);
            }
            Some(_) => {}
            None => return Err(DecodeInternalError::InvariantViolation),
        }
        let KrakenCapturedFrame {
            native_payload,
            transport,
            evidence,
        } = captured;
        if self.decoder.state == KrakenDecoderState::Retired {
            return Ok(public_retirement_handoff(
                native_payload,
                transport,
                evidence,
                Arc::clone(&self.connection),
                Arc::clone(&self.instrument_binding),
                self.subscription_acknowledgement.clone(),
                KrakenGenerationRetirement::AlreadyRetired,
            ));
        }
        if transport != TransportFrameKind::Text {
            self.decoder.state = KrakenDecoderState::Quarantined;
            return Ok(from_public_outcome(
                native_payload,
                transport,
                Arc::clone(&self.connection),
                Arc::clone(&self.instrument_binding),
                self.subscription_acknowledgement.clone(),
                None,
                DecodeOutcome::Quarantine(DecodedQuarantineAction::new(
                    evidence,
                    QuarantineReason::SchemaViolation,
                    None,
                )),
            ));
        }
        match self.decoder.decode_payload(native_payload.as_bytes()) {
            Ok(KrakenDecodeOutcome::Market(observations)) => {
                match DecodedProviderBatch::try_new(evidence.clone(), observations) {
                    Ok(batch) => {
                        let acknowledgement_matches = self
                            .subscription_acknowledgement
                            .as_ref()
                            .is_some_and(|acknowledgement| {
                                acknowledgement
                                    .binding()
                                    .shares_allocation_with(batch.evidence().binding())
                            });
                        let continuity = public_continuity(
                            &batch,
                            &self.connection,
                            &self.instrument_binding,
                            self.decoder.channel,
                        );
                        let (outcome, continuity) = match (acknowledgement_matches, continuity) {
                            (true, Ok(continuity)) => {
                                (DecodeOutcome::Data(batch), Some(continuity))
                            }
                            (false, _) | (_, Err(_)) => {
                                self.decoder.state = KrakenDecoderState::Quarantined;
                                (
                                    DecodeOutcome::Quarantine(DecodedQuarantineAction::new(
                                        batch.evidence().clone(),
                                        QuarantineReason::ProtocolInvariantViolation,
                                        None,
                                    )),
                                    None,
                                )
                            }
                        };
                        Ok(from_public_outcome(
                            native_payload,
                            transport,
                            Arc::clone(&self.connection),
                            Arc::clone(&self.instrument_binding),
                            self.subscription_acknowledgement.clone(),
                            continuity,
                            outcome,
                        ))
                    }
                    Err(error) => {
                        self.decoder.state = KrakenDecoderState::Quarantined;
                        let outcome = decode_failure_outcome(error, evidence)?;
                        Ok(from_public_outcome(
                            native_payload,
                            transport,
                            Arc::clone(&self.connection),
                            Arc::clone(&self.instrument_binding),
                            self.subscription_acknowledgement.clone(),
                            None,
                            outcome,
                        ))
                    }
                }
            }
            Ok(KrakenDecodeOutcome::Control(control)) => {
                if let KrakenPublicControl::Subscribed {
                    request_id,
                    provider_request_received_at,
                    provider_response_sent_at,
                    ..
                } = &control
                {
                    if self.subscription_acknowledgement.is_some() {
                        self.retire(
                            KrakenGenerationRetirement::DuplicateSubscriptionAcknowledgement,
                        );
                        return Ok(public_retirement_handoff(
                            native_payload,
                            transport,
                            evidence,
                            Arc::clone(&self.connection),
                            Arc::clone(&self.instrument_binding),
                            self.subscription_acknowledgement.clone(),
                            KrakenGenerationRetirement::DuplicateSubscriptionAcknowledgement,
                        ));
                    }
                    self.subscription_acknowledgement = Some(captured_acknowledgement(
                        &evidence,
                        Some(*request_id),
                        *provider_request_received_at,
                        *provider_response_sent_at,
                    ));
                }
                match &control {
                    KrakenPublicControl::SubscriptionRefused { .. } => {
                        self.retire(KrakenGenerationRetirement::SubscriptionRefused);
                    }
                    KrakenPublicControl::ProviderReset { .. } => {
                        self.retire(KrakenGenerationRetirement::ProviderReset);
                    }
                    _ => {}
                }
                Ok(public_control_handoff(
                    native_payload,
                    transport,
                    evidence,
                    Arc::clone(&self.connection),
                    Arc::clone(&self.instrument_binding),
                    self.subscription_acknowledgement.clone(),
                    control,
                ))
            }
            Err(error) => {
                if self.decoder.state == KrakenDecoderState::Retired {
                    let reason = self
                        .retirement_reason
                        .or(self.decoder.retirement_reason)
                        .unwrap_or(KrakenGenerationRetirement::SubscriptionAuthorityRejected);
                    self.retire(reason);
                    return Ok(public_retirement_handoff(
                        native_payload,
                        transport,
                        evidence,
                        Arc::clone(&self.connection),
                        Arc::clone(&self.instrument_binding),
                        self.subscription_acknowledgement.clone(),
                        reason,
                    ));
                }
                let outcome = decode_failure_outcome(error, evidence)?;
                Ok(from_public_outcome(
                    native_payload,
                    transport,
                    Arc::clone(&self.connection),
                    Arc::clone(&self.instrument_binding),
                    self.subscription_acknowledgement.clone(),
                    None,
                    outcome,
                ))
            }
        }
    }
}

/// One-use result of a single stateful Kraken application decode.
#[derive(Debug)]
pub struct KrakenMarketDecodeHandoff {
    live: DecodeOutcome,
    publication: Option<KrakenPublicationDecodeOutcome>,
}

impl KrakenMarketDecodeHandoff {
    fn publishable(live: DecodeOutcome, publication: KrakenPublicationDecodeOutcome) -> Self {
        Self {
            live,
            publication: Some(publication),
        }
    }

    fn live_only(live: DecodeOutcome) -> Self {
        Self {
            live,
            publication: None,
        }
    }

    /// Consumes the handoff into the generic live result and optional exact publication input.
    pub fn into_parts(self) -> (DecodeOutcome, Option<KrakenPublicationDecodeOutcome>) {
        (self.live, self.publication)
    }

    pub(crate) fn try_from_socket_handoff(
        handoff: KrakenMarketEventHandoff,
    ) -> Result<Self, DecodeInternalError> {
        match handoff {
            KrakenMarketEventHandoff::Public(handoff) => {
                let (
                    _native_payload,
                    _transport,
                    connection,
                    native_coordinates,
                    instrument_binding,
                    subscription_acknowledgement,
                    continuity,
                    batch,
                ) = handoff.into_parts();
                let retained_bytes = batch.retained_bytes().map_err(|error| match error {
                    DecodeError::RetainedSizeOverflow => DecodeInternalError::RetainedSizeOverflow,
                    _ => DecodeInternalError::InvariantViolation,
                })?;
                let observations = batch.observations().to_vec();
                let context = KrakenPublicationContext {
                    connection,
                    native_coordinates,
                    instrument_binding,
                    subscription_acknowledgement,
                    continuity: Some(continuity),
                };
                Ok(Self::publishable(
                    DecodeOutcome::Data(batch),
                    KrakenPublicationDecodeOutcome::market(observations, retained_bytes, context),
                ))
            }
            KrakenMarketEventHandoff::ControlOrDiscontinuity(handoff) => {
                let (
                    _native_payload,
                    _transport,
                    evidence,
                    connection,
                    instrument_binding,
                    subscription_acknowledgement,
                    kind,
                    provider_code,
                ) = handoff.into_parts();
                match kind {
                    KrakenControlOrDiscontinuityKind::PublicControl(control) => {
                        let live = control_outcome(&control, evidence)?;
                        let native_coordinates = connection
                            .native_coordinates()
                            .cloned()
                            .ok_or(DecodeInternalError::InvariantViolation)?;
                        let publication = instrument_binding.zip(subscription_acknowledgement).map(
                            |(instrument_binding, subscription_acknowledgement)| {
                                KrakenPublicationDecodeOutcome::control(
                                    control,
                                    KrakenPublicationContext {
                                        connection,
                                        native_coordinates: Arc::new(native_coordinates),
                                        instrument_binding,
                                        subscription_acknowledgement,
                                        continuity: None,
                                    },
                                )
                            },
                        );
                        Ok(Self { live, publication })
                    }
                    KrakenControlOrDiscontinuityKind::PublicIgnored(reason) => {
                        Ok(Self::live_only(DecodeOutcome::Ignored(
                            DecodedIgnoredFrame::new(evidence, reason, provider_code),
                        )))
                    }
                    KrakenControlOrDiscontinuityKind::PublicResynchronize(reason) => {
                        Ok(Self::live_only(DecodeOutcome::Resynchronize(
                            DecodedRecoveryAction::new(evidence, reason, provider_code),
                        )))
                    }
                    KrakenControlOrDiscontinuityKind::PublicQuarantine(reason) => {
                        Ok(Self::live_only(DecodeOutcome::Quarantine(
                            DecodedQuarantineAction::new(evidence, reason, provider_code),
                        )))
                    }
                    KrakenControlOrDiscontinuityKind::PublicGenerationRetired(_) => Ok(
                        Self::live_only(DecodeOutcome::Resynchronize(DecodedRecoveryAction::new(
                            evidence,
                            ResynchronizationReason::DecoderStateDiscontinuity,
                            provider_code,
                        ))),
                    ),
                    KrakenControlOrDiscontinuityKind::AuthenticatedControl(_)
                    | KrakenControlOrDiscontinuityKind::AuthenticatedDiscontinuity(_) => {
                        Err(DecodeInternalError::InvariantViolation)
                    }
                }
            }
            KrakenMarketEventHandoff::AuthenticatedLevel3(_) => {
                Err(DecodeInternalError::InvariantViolation)
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct KrakenPublicationContext {
    connection: Arc<KrakenConnectionBinding>,
    native_coordinates: Arc<KrakenNativeMarketCoordinates>,
    instrument_binding: Arc<KrakenInstrumentBinding>,
    subscription_acknowledgement: KrakenSubscriptionAcknowledgementEvidence,
    continuity: Option<KrakenMarketContinuity>,
}

impl KrakenPublicationContext {
    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<KrakenConnectionBinding>,
        Arc<KrakenNativeMarketCoordinates>,
        Arc<KrakenInstrumentBinding>,
        KrakenSubscriptionAcknowledgementEvidence,
        Option<KrakenMarketContinuity>,
    ) {
        (
            self.connection,
            self.native_coordinates,
            self.instrument_binding,
            self.subscription_acknowledgement,
            self.continuity,
        )
    }
}

/// One-use typed publication input with the common decoder's exact retained-byte charge.
#[derive(Debug)]
pub struct KrakenPublicationDecodeOutcome {
    outcome: KrakenDecodeOutcome,
    decoded_retained_bytes: usize,
    context: KrakenPublicationContext,
}

impl KrakenPublicationDecodeOutcome {
    /// Returns the exact native identity and venue coordinates retained for publication lineage.
    pub fn native_coordinates(&self) -> &KrakenNativeMarketCoordinates {
        &self.context.native_coordinates
    }

    fn market(
        observations: Vec<ProviderNormalizedObservation>,
        retained_bytes: usize,
        context: KrakenPublicationContext,
    ) -> Self {
        Self {
            outcome: KrakenDecodeOutcome::Market(observations),
            decoded_retained_bytes: retained_bytes,
            context,
        }
    }

    fn control(control: KrakenPublicControl, context: KrakenPublicationContext) -> Self {
        Self {
            outcome: KrakenDecodeOutcome::Control(control),
            decoded_retained_bytes: 0,
            context,
        }
    }

    pub(crate) fn into_parts(self) -> (KrakenDecodeOutcome, usize, KrakenPublicationContext) {
        (self.outcome, self.decoded_retained_bytes, self.context)
    }
}

impl SourceMetadataProvider for KrakenMarketDecoder {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

fn control_outcome(
    control: &KrakenPublicControl,
    evidence: DecoderEvidence,
) -> Result<DecodeOutcome, DecodeInternalError> {
    let (kind, provider_code) = match control {
        KrakenPublicControl::Heartbeat => (ControlFrameKind::Heartbeat, None),
        KrakenPublicControl::Pong { .. } => (ControlFrameKind::Pong, None),
        KrakenPublicControl::Online => (ControlFrameKind::ProviderFlowControl, Some("online")),
        KrakenPublicControl::Subscribed {
            channel: KrakenChannel::Book(_),
            ..
        } => (ControlFrameKind::SubscriptionAcknowledgement, Some("book")),
        KrakenPublicControl::Subscribed {
            channel: KrakenChannel::Trades,
            ..
        } => (ControlFrameKind::SubscriptionAcknowledgement, Some("trade")),
        KrakenPublicControl::SubscriptionRefused { .. } => {
            let provider_code = SourceIdentifier::try_from("subscription_refused")
                .map_err(|_| DecodeInternalError::InvariantViolation)?;
            return Ok(DecodeOutcome::Resynchronize(DecodedRecoveryAction::new(
                evidence,
                ResynchronizationReason::ProviderRequestedReset,
                Some(provider_code),
            )));
        }
        KrakenPublicControl::ProviderReset { .. } => {
            let provider_code = SourceIdentifier::try_from("provider_reset")
                .map_err(|_| DecodeInternalError::InvariantViolation)?;
            return Ok(DecodeOutcome::Resynchronize(DecodedRecoveryAction::new(
                evidence,
                ResynchronizationReason::ProviderRequestedReset,
                Some(provider_code),
            )));
        }
    };
    let provider_code = provider_code
        .map(SourceIdentifier::try_from)
        .transpose()
        .map_err(|_| DecodeInternalError::InvariantViolation)?;
    Ok(DecodeOutcome::Control(DecodedControlFrame::new(
        evidence,
        kind,
        provider_code,
    )))
}

fn decode_failure_outcome(
    error: DecodeError,
    evidence: DecoderEvidence,
) -> Result<DecodeOutcome, DecodeInternalError> {
    let reason = match error {
        DecodeError::RetainedSizeOverflow => {
            return Err(DecodeInternalError::RetainedSizeOverflow);
        }
        DecodeError::ResynchronizationRequired => {
            return Ok(DecodeOutcome::Resynchronize(DecodedRecoveryAction::new(
                evidence,
                ResynchronizationReason::DecoderStateDiscontinuity,
                None,
            )));
        }
        DecodeError::MalformedPayload | DecodeError::EmptyBatch => {
            QuarantineReason::MalformedPayload
        }
        DecodeError::InexactValue => QuarantineReason::InexactNumericValue,
        DecodeError::TooManyEvents { .. } | DecodeError::TooManyNumericFields { .. } => {
            QuarantineReason::SchemaViolation
        }
        DecodeError::InvalidProviderEvidence => QuarantineReason::ProtocolInvariantViolation,
    };
    Ok(DecodeOutcome::Quarantine(DecodedQuarantineAction::new(
        evidence, reason, None,
    )))
}

/// Fully validated classification of one Kraken application message.
#[derive(Debug)]
pub enum KrakenDecodeOutcome {
    /// One or more market observations in provider wire order.
    Market(Vec<ProviderNormalizedObservation>),
    /// A valid connection/control-plane message that does not refresh market data.
    Control(KrakenPublicControl),
}

#[derive(Clone, Debug)]
struct Rules {
    timestamp: IntegrityRule,
    sequence: IntegrityRule,
    checksum: IntegrityRule,
    no_checksum: IntegrityRule,
    no_snapshot: IntegrityRule,
    aggressor: IntegrityRule,
}

impl Rules {
    fn try_new(channel: KrakenChannel) -> Result<Self, DecodeError> {
        let sequence_rule = match channel {
            KrakenChannel::Book(_) => KRAKEN_BOOK_SEQUENCE_RULE,
            KrakenChannel::Trades => KRAKEN_TRADE_SEQUENCE_RULE,
        };
        Ok(Self {
            timestamp: rule("kraken-ws-v2-rfc3339-timestamp-v1")?,
            sequence: rule(sequence_rule)?,
            checksum: rule("kraken-ws-v2-book-checksum-v1")?,
            no_checksum: rule("kraken-ws-v2-trade-checksum-unsupported-v1")?,
            no_snapshot: rule("kraken-ws-v2-trade-snapshot-na-v1")?,
            aggressor: rule("kraken-ws-v2-trade-taker-side-v1")?,
        })
    }
}

/// Stateful decoder for one Kraken symbol and one connection generation.
#[derive(Debug)]
pub struct KrakenDecoder {
    native_coordinates: Arc<KrakenNativeMarketCoordinates>,
    channel: KrakenChannel,
    state: KrakenDecoderState,
    retirement_reason: Option<KrakenGenerationRetirement>,
    bids: Vec<ProviderBookLevel>,
    asks: Vec<ProviderBookLevel>,
    last_checksum: Option<u32>,
    rules: Rules,
}

impl KrakenDecoder {
    /// Constructs an empty price-level decoder that requires an initializing snapshot.
    pub fn try_new(
        coordinates: KrakenNativeMarketCoordinates,
        depth: KrakenDepth,
    ) -> Result<Self, DecodeError> {
        Self::try_for_channel(Arc::new(coordinates), KrakenChannel::Book(depth))
    }

    /// Constructs an exact trade-channel decoder.
    pub fn try_trades(coordinates: KrakenNativeMarketCoordinates) -> Result<Self, DecodeError> {
        Self::try_for_channel(Arc::new(coordinates), KrakenChannel::Trades)
    }

    fn try_for_channel(
        native_coordinates: Arc<KrakenNativeMarketCoordinates>,
        channel: KrakenChannel,
    ) -> Result<Self, DecodeError> {
        if native_coordinates.channel() != channel {
            return Err(DecodeError::InvalidProviderEvidence);
        }
        Ok(Self {
            native_coordinates,
            channel,
            state: KrakenDecoderState::AwaitingSnapshot,
            retirement_reason: None,
            bids: Vec::new(),
            asks: Vec::new(),
            last_checksum: None,
            rules: Rules::try_new(channel)?,
        })
    }

    /// Returns the generation-local synchronization state.
    pub const fn state(&self) -> KrakenDecoderState {
        self.state
    }

    /// Returns the mandatory exact provider-native coordinates.
    pub fn native_coordinates(&self) -> &KrakenNativeMarketCoordinates {
        &self.native_coordinates
    }

    /// Returns the checksum of the last committed candidate.
    pub const fn last_checksum(&self) -> Option<u32> {
        self.last_checksum
    }

    /// Returns a stable digest of committed book state for atomicity assertions.
    pub const fn book_digest(&self) -> Option<u32> {
        self.last_checksum
    }

    /// Parses, validates, and atomically applies one bounded application message.
    ///
    /// Heartbeats, acknowledgements, status, and pong messages never update market freshness.
    ///
    /// # Errors
    ///
    /// Rejects malformed evidence, wrong symbols, unsupported state transitions, invalid exact
    /// numbers, crossed books, or checksum mismatches. Any market-message failure quarantines the
    /// generation and leaves committed state unchanged.
    pub fn decode_payload(&mut self, payload: &[u8]) -> Result<KrakenDecodeOutcome, DecodeError> {
        if self.state == KrakenDecoderState::Retired {
            return Err(DecodeError::ResynchronizationRequired);
        }
        if payload.len() > MAX_RAW_FRAME_BYTES {
            self.state = KrakenDecoderState::Retired;
            self.retirement_reason = Some(KrakenGenerationRetirement::ProtocolControlViolation);
            return Err(DecodeError::MalformedPayload);
        }
        let kind = match classify(payload) {
            Ok(kind) => kind,
            Err(_) => {
                self.state = KrakenDecoderState::Retired;
                self.retirement_reason = Some(KrakenGenerationRetirement::ProtocolControlViolation);
                return Err(DecodeError::MalformedPayload);
            }
        };
        let outcome = match kind {
            EnvelopeKind::Book => self.decode_book(payload),
            EnvelopeKind::Trade => self.decode_trades(payload),
            EnvelopeKind::Heartbeat => validate_heartbeat(payload),
            EnvelopeKind::Status => validate_status(payload),
            EnvelopeKind::SubscribeAck => validate_ack(
                payload,
                self.native_coordinates.venue_symbol().as_str(),
                self.channel,
            ),
            EnvelopeKind::Pong => validate_pong(payload),
        };
        match &outcome {
            Ok(KrakenDecodeOutcome::Control(KrakenPublicControl::SubscriptionRefused {
                ..
            })) => {
                self.state = KrakenDecoderState::Retired;
                self.retirement_reason = Some(KrakenGenerationRetirement::SubscriptionRefused);
            }
            Ok(KrakenDecodeOutcome::Control(KrakenPublicControl::ProviderReset { .. })) => {
                self.state = KrakenDecoderState::Retired;
                self.retirement_reason = Some(KrakenGenerationRetirement::ProviderReset);
            }
            _ => {}
        }
        if outcome.is_err() {
            self.state = if matches!(kind, EnvelopeKind::Book | EnvelopeKind::Trade) {
                KrakenDecoderState::Quarantined
            } else {
                KrakenDecoderState::Retired
            };
            if self.state == KrakenDecoderState::Retired {
                self.retirement_reason = Some(match kind {
                    EnvelopeKind::SubscribeAck => {
                        KrakenGenerationRetirement::SubscriptionAuthorityRejected
                    }
                    EnvelopeKind::Status => KrakenGenerationRetirement::ProtocolControlViolation,
                    EnvelopeKind::Heartbeat | EnvelopeKind::Pong => {
                        KrakenGenerationRetirement::ProtocolControlViolation
                    }
                    EnvelopeKind::Book | EnvelopeKind::Trade => unreachable!(),
                });
            }
        }
        outcome
    }

    fn decode_book(&mut self, payload: &[u8]) -> Result<KrakenDecodeOutcome, DecodeError> {
        let KrakenChannel::Book(depth) = self.channel else {
            return Err(DecodeError::MalformedPayload);
        };
        let envelope: BookEnvelope<'_> =
            serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
        if envelope.channel != "book" || envelope.data.len() != 1 {
            return Err(DecodeError::MalformedPayload);
        }
        let data = envelope.data.first().ok_or(DecodeError::MalformedPayload)?;
        if data.symbol != self.native_coordinates.venue_symbol().as_str() {
            return Err(DecodeError::MalformedPayload);
        }
        if data.bids.len() > depth.get().saturating_mul(4)
            || data.asks.len() > depth.get().saturating_mul(4)
        {
            return Err(DecodeError::TooManyNumericFields {
                max: depth.get().saturating_mul(8),
            });
        }
        let timestamp = parse_timestamp(data.timestamp)?;
        match envelope.kind {
            "snapshot" => self.apply_snapshot(data, timestamp),
            "update" if self.state == KrakenDecoderState::Healthy => {
                self.apply_update(data, timestamp)
            }
            "update" => Err(DecodeError::ResynchronizationRequired),
            _ => Err(DecodeError::MalformedPayload),
        }
    }

    fn apply_snapshot(
        &mut self,
        data: &BookData<'_>,
        timestamp: Timestamp,
    ) -> Result<KrakenDecodeOutcome, DecodeError> {
        let mut bids = parse_levels(&data.bids)?;
        let mut asks = parse_levels(&data.asks)?;
        validate_snapshot_side(&bids, false)?;
        validate_snapshot_side(&asks, true)?;
        let KrakenChannel::Book(depth) = self.channel else {
            return Err(DecodeError::MalformedPayload);
        };
        bids.truncate(depth.get());
        asks.truncate(depth.get());
        validate_book(&bids, &asks)?;
        let checksum = validate_checksum(data, &asks, &bids)?;
        let payload = ProviderObservationPayload::book_snapshot(
            MarketDepth::PriceLevel,
            bids.clone(),
            asks.clone(),
        )?;
        let observation = self.book_observation(
            data,
            timestamp,
            ProviderSnapshotEvidence::InitializingSnapshot {
                provider_reference: None,
            },
            payload,
        )?;
        self.bids = bids;
        self.asks = asks;
        self.last_checksum = Some(checksum);
        self.state = KrakenDecoderState::Healthy;
        Ok(KrakenDecodeOutcome::Market(vec![observation]))
    }

    fn apply_update(
        &mut self,
        data: &BookData<'_>,
        timestamp: Timestamp,
    ) -> Result<KrakenDecodeOutcome, DecodeError> {
        if data.bids.is_empty() && data.asks.is_empty() {
            return Err(DecodeError::MalformedPayload);
        }
        let bid_changes = parse_levels(&data.bids)?;
        let ask_changes = parse_levels(&data.asks)?;
        let mut candidate_bids = self.bids.clone();
        let mut candidate_asks = self.asks.clone();
        let KrakenChannel::Book(depth) = self.channel else {
            return Err(DecodeError::MalformedPayload);
        };
        apply_changes(&mut candidate_bids, &bid_changes, false, depth.get())?;
        apply_changes(&mut candidate_asks, &ask_changes, true, depth.get())?;
        validate_book(&candidate_bids, &candidate_asks)?;
        let checksum = validate_checksum(data, &candidate_asks, &candidate_bids)?;
        let changes = bid_changes
            .iter()
            .cloned()
            .map(|level| ProviderBookChange::new(ProviderBookSide::Bid, level))
            .chain(
                ask_changes
                    .iter()
                    .cloned()
                    .map(|level| ProviderBookChange::new(ProviderBookSide::Ask, level)),
            )
            .collect();
        let payload = ProviderObservationPayload::book_delta(MarketDepth::PriceLevel, changes)?;
        let observation = self.book_observation(
            data,
            timestamp,
            ProviderSnapshotEvidence::Delta {
                provider_snapshot_reference: None,
            },
            payload,
        )?;
        self.bids = candidate_bids;
        self.asks = candidate_asks;
        self.last_checksum = Some(checksum);
        Ok(KrakenDecodeOutcome::Market(vec![observation]))
    }

    fn book_observation(
        &self,
        data: &BookData<'_>,
        timestamp: Timestamp,
        snapshot: ProviderSnapshotEvidence,
        payload: ProviderObservationPayload,
    ) -> Result<ProviderNormalizedObservation, DecodeError> {
        ProviderNormalizedObservation::try_new(
            source_identifier(&format!(
                "book:{}:{}",
                self.native_coordinates.venue_symbol(),
                data.timestamp
            ))?,
            self.native_coordinates.venue().clone(),
            self.native_coordinates.instrument(),
            ProviderTimestampEvidence::Provided {
                value: timestamp,
                rule: self.rules.timestamp.clone(),
            },
            ProviderSequenceEvidence::Unsupported {
                rule: self.rules.sequence.clone(),
            },
            snapshot,
            ProviderChecksumEvidence::Provided {
                value: source_identifier(checksum_text(data.checksum)?)?,
                rule: self.rules.checksum.clone(),
            },
            payload,
        )
    }

    fn decode_trades(&mut self, payload: &[u8]) -> Result<KrakenDecodeOutcome, DecodeError> {
        if self.channel != KrakenChannel::Trades {
            return Err(DecodeError::MalformedPayload);
        }
        let envelope: TradeEnvelope<'_> =
            serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
        if envelope.channel != "trade" || !matches!(envelope.kind, "snapshot" | "update") {
            return Err(DecodeError::MalformedPayload);
        }
        let trade_count =
            bounded_trade_count(envelope.data).map_err(|_| DecodeError::MalformedPayload)?;
        if trade_count == 0 {
            return Err(DecodeError::EmptyBatch);
        }
        if trade_count > MAX_DECODED_EVENTS {
            return Err(DecodeError::TooManyEvents {
                max: MAX_DECODED_EVENTS,
            });
        }
        let trades: Vec<TradeData<'_>> =
            serde_json::from_str(envelope.data.get()).map_err(|_| DecodeError::MalformedPayload)?;
        if trades.len() != trade_count {
            return Err(DecodeError::MalformedPayload);
        }
        let mut observations = Vec::with_capacity(trade_count);
        for trade in trades {
            if trade.symbol != self.native_coordinates.venue_symbol().as_str() || trade.trade_id < 0
            {
                return Err(DecodeError::MalformedPayload);
            }
            let side = match trade.side {
                "buy" => AggressorSide::Buy,
                "sell" => AggressorSide::Sell,
                _ => return Err(DecodeError::MalformedPayload),
            };
            let trade_id = trade.trade_id.to_string();
            let taker_order_type = match trade.ord_type {
                "limit" => TradeTakerOrderType::Limit,
                "market" => TradeTakerOrderType::Market,
                _ => return Err(DecodeError::MalformedPayload),
            };
            observations.push(ProviderNormalizedObservation::try_new(
                source_identifier(&trade_id)?,
                self.native_coordinates.venue().clone(),
                self.native_coordinates.instrument(),
                ProviderTimestampEvidence::Provided {
                    value: parse_timestamp(trade.timestamp)?,
                    rule: self.rules.timestamp.clone(),
                },
                ProviderSequenceEvidence::Unsupported {
                    rule: self.rules.sequence.clone(),
                },
                ProviderSnapshotEvidence::NotApplicable(self.rules.no_snapshot.clone()),
                ProviderChecksumEvidence::Unsupported {
                    rule: self.rules.no_checksum.clone(),
                },
                ProviderObservationPayload::Trade {
                    trade_id: source_identifier(&trade_id)?,
                    price: parse_price(trade.price)?,
                    quantity: parse_positive_quantity(trade.qty)?,
                    aggressor: ProviderAggressorEvidence::new(
                        side,
                        Some(source_identifier(trade.side)?),
                        self.rules.aggressor.clone(),
                    ),
                    taker_order_type: Some(taker_order_type),
                },
            )?);
        }
        self.state = KrakenDecoderState::Healthy;
        Ok(KrakenDecodeOutcome::Market(observations))
    }
}

fn rule(name: &str) -> Result<IntegrityRule, DecodeError> {
    Ok(IntegrityRule::new(
        source_identifier(name)?,
        RuleVersion::new(1).map_err(|_| DecodeError::InvalidProviderEvidence)?,
    ))
}

fn source_identifier(value: &str) -> Result<SourceIdentifier, DecodeError> {
    SourceIdentifier::try_from(value).map_err(|_| DecodeError::MalformedPayload)
}

fn parse_level(level: &WireLevel<'_>) -> Result<ProviderBookLevel, DecodeError> {
    Ok(ProviderBookLevel::new(
        parse_price(level.price)?,
        parse_quantity(level.qty)?,
    ))
}

fn parse_levels(levels: &[WireLevel<'_>]) -> Result<Vec<ProviderBookLevel>, DecodeError> {
    levels.iter().map(parse_level).collect()
}

fn parse_price(value: &serde_json::value::RawValue) -> Result<ProviderPrice, DecodeError> {
    let lexeme = exact_decimal(value).map_err(|_| DecodeError::MalformedPayload)?;
    let value = ProviderDecimalLexeme::try_new(lexeme)?;
    if value.decimal() <= Decimal::ZERO {
        return Err(DecodeError::InexactValue);
    }
    Ok(ProviderPrice::new(value))
}

fn parse_quantity(value: &serde_json::value::RawValue) -> Result<ProviderQuantity, DecodeError> {
    let lexeme = exact_decimal(value).map_err(|_| DecodeError::MalformedPayload)?;
    let value = ProviderDecimalLexeme::try_new(lexeme)?;
    if value.decimal() < Decimal::ZERO {
        return Err(DecodeError::InexactValue);
    }
    Ok(ProviderQuantity::new(value))
}

fn parse_positive_quantity(
    value: &serde_json::value::RawValue,
) -> Result<ProviderQuantity, DecodeError> {
    let quantity = parse_quantity(value)?;
    if quantity.value().decimal() == Decimal::ZERO {
        return Err(DecodeError::InexactValue);
    }
    Ok(quantity)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, DecodeError> {
    let parsed = DateTime::parse_from_rfc3339(value).map_err(|_| DecodeError::InexactValue)?;
    let seconds = parsed.timestamp();
    let nanos = seconds
        .checked_mul(1_000_000_000)
        .and_then(|base| base.checked_add(i64::from(parsed.timestamp_subsec_nanos())))
        .ok_or(DecodeError::InexactValue)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn checksum_text(raw: &serde_json::value::RawValue) -> Result<&str, DecodeError> {
    let text = exact_decimal(raw).map_err(|_| DecodeError::MalformedPayload)?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DecodeError::MalformedPayload);
    }
    let _value = text
        .parse::<u32>()
        .map_err(|_| DecodeError::MalformedPayload)?;
    Ok(text)
}

fn validate_checksum(
    data: &BookData<'_>,
    asks: &[ProviderBookLevel],
    bids: &[ProviderBookLevel],
) -> Result<u32, DecodeError> {
    let expected = checksum_text(data.checksum)?
        .parse::<u32>()
        .map_err(|_| DecodeError::MalformedPayload)?;
    let level_count = NonZeroU16::new(10).ok_or(DecodeError::InvalidProviderEvidence)?;
    let computed = kraken_v2_crc32(asks, bids, level_count)
        .map_err(|_| DecodeError::InvalidProviderEvidence)?;
    if expected != computed {
        return Err(DecodeError::ResynchronizationRequired);
    }
    Ok(computed)
}

fn validate_snapshot_side(
    levels: &[ProviderBookLevel],
    ascending: bool,
) -> Result<(), DecodeError> {
    if levels.is_empty() {
        return Err(DecodeError::MalformedPayload);
    }
    let mut previous = None;
    for level in levels {
        let price = level.price().value().decimal();
        let quantity = level.quantity().value().decimal();
        if quantity <= Decimal::ZERO
            || previous.is_some_and(|prior| {
                if ascending {
                    prior >= price
                } else {
                    prior <= price
                }
            })
        {
            return Err(DecodeError::InvalidProviderEvidence);
        }
        previous = Some(price);
    }
    Ok(())
}

fn apply_changes(
    state: &mut Vec<ProviderBookLevel>,
    changes: &[ProviderBookLevel],
    ascending: bool,
    depth: usize,
) -> Result<(), DecodeError> {
    for change in changes {
        let price = change.price().value().decimal();
        let existing = state
            .iter()
            .position(|level| level.price().value().decimal() == price);
        if change.quantity().value().decimal() == Decimal::ZERO {
            if let Some(index) = existing {
                state.remove(index);
            }
        } else if let Some(index) = existing {
            state[index] = change.clone();
        } else {
            state
                .try_reserve(1)
                .map_err(|_| DecodeError::RetainedSizeOverflow)?;
            state.push(change.clone());
        }
    }
    state.sort_by(|left, right| {
        let order = left
            .price()
            .value()
            .decimal()
            .cmp(&right.price().value().decimal());
        if ascending { order } else { order.reverse() }
    });
    state.truncate(depth);
    Ok(())
}

fn validate_book(
    bids: &[ProviderBookLevel],
    asks: &[ProviderBookLevel],
) -> Result<(), DecodeError> {
    let bid = bids.first().ok_or(DecodeError::InvalidProviderEvidence)?;
    let ask = asks.first().ok_or(DecodeError::InvalidProviderEvidence)?;
    if bid.price().value().decimal() >= ask.price().value().decimal() {
        return Err(DecodeError::InvalidProviderEvidence);
    }
    Ok(())
}

fn validate_heartbeat(payload: &[u8]) -> Result<KrakenDecodeOutcome, DecodeError> {
    let heartbeat: Heartbeat<'_> =
        serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
    if heartbeat.channel != "heartbeat" {
        return Err(DecodeError::MalformedPayload);
    }
    Ok(KrakenDecodeOutcome::Control(KrakenPublicControl::Heartbeat))
}

fn validate_status(payload: &[u8]) -> Result<KrakenDecodeOutcome, DecodeError> {
    let status: StatusEnvelope<'_> =
        serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
    let value = status.data.first().ok_or(DecodeError::MalformedPayload)?;
    if status.channel != "status"
        || status.kind != "update"
        || status.data.len() != 1
        || value.api_version.is_empty()
        || value.version.is_empty()
        || value.connection_id == 0
    {
        return Err(DecodeError::ResynchronizationRequired);
    }
    if value.system == "online" {
        return Ok(KrakenDecodeOutcome::Control(KrakenPublicControl::Online));
    }
    let system = KrakenProviderText::try_new(value.system)
        .map_err(|_| DecodeError::ResynchronizationRequired)?;
    Ok(KrakenDecodeOutcome::Control(
        KrakenPublicControl::ProviderReset { system },
    ))
}

fn validate_ack(
    payload: &[u8],
    symbol: &str,
    channel: KrakenChannel,
) -> Result<KrakenDecodeOutcome, DecodeError> {
    let ack: SubscribeAck<'_> =
        serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
    if ack.method != "subscribe" || ack.req_id != Some(PUBLIC_SUBSCRIPTION_REQUEST_ID) {
        return Err(DecodeError::ResynchronizationRequired);
    }
    let provider_request_received_at = parse_timestamp(ack.time_in)?;
    let provider_response_sent_at = parse_timestamp(ack.time_out)?;
    if provider_response_sent_at < provider_request_received_at {
        return Err(DecodeError::ResynchronizationRequired);
    }
    if !ack.success {
        let error = ack.error.ok_or(DecodeError::ResynchronizationRequired)?;
        if error.is_empty() || error.len() > MAX_SUBSCRIPTION_ERROR_BYTES {
            return Err(DecodeError::ResynchronizationRequired);
        }
        if let Some(result) = ack.result.as_ref() {
            validate_subscription_result(result, symbol, channel)?;
        }
        let error = KrakenProviderText::try_new(error)
            .map_err(|_| DecodeError::ResynchronizationRequired)?;
        return Ok(KrakenDecodeOutcome::Control(
            KrakenPublicControl::SubscriptionRefused {
                request_id: ack.req_id,
                provider_request_received_at,
                provider_response_sent_at,
                error,
            },
        ));
    }
    if ack.error.is_some() {
        return Err(DecodeError::ResynchronizationRequired);
    }
    let result = ack.result.as_ref().ok_or(DecodeError::MalformedPayload)?;
    let acknowledged_channel = validate_subscription_result(result, symbol, channel)?;
    Ok(KrakenDecodeOutcome::Control(
        KrakenPublicControl::Subscribed {
            channel: acknowledged_channel,
            request_id: PUBLIC_SUBSCRIPTION_REQUEST_ID,
            provider_request_received_at,
            provider_response_sent_at,
        },
    ))
}

fn validate_subscription_result(
    result: &crate::messages::SubscribeResult<'_>,
    symbol: &str,
    channel: KrakenChannel,
) -> Result<KrakenChannel, DecodeError> {
    validate_warnings(result.warnings).map_err(|_| DecodeError::MalformedPayload)?;
    if !matches!(result.channel, "book" | "trade") || result.symbol != symbol {
        return Err(DecodeError::ResynchronizationRequired);
    }
    match channel {
        KrakenChannel::Book(depth)
            if result.channel == "book"
                && result.depth == Some(depth.get())
                && result.snapshot == Some(true) =>
        {
            Ok(channel)
        }
        KrakenChannel::Trades
            if result.channel == "trade"
                && result.depth.is_none()
                && result.snapshot == Some(true) =>
        {
            Ok(channel)
        }
        KrakenChannel::Book(_) | KrakenChannel::Trades => {
            Err(DecodeError::ResynchronizationRequired)
        }
    }
}

fn validate_pong(payload: &[u8]) -> Result<KrakenDecodeOutcome, DecodeError> {
    let pong: Pong<'_> =
        serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
    if pong.method != "pong" || pong.req_id == Some(0) {
        return Err(DecodeError::MalformedPayload);
    }
    let provider_request_received_at = parse_timestamp(pong.time_in)?;
    let provider_response_sent_at = parse_timestamp(pong.time_out)?;
    if provider_response_sent_at < provider_request_received_at {
        return Err(DecodeError::MalformedPayload);
    }
    Ok(KrakenDecodeOutcome::Control(KrakenPublicControl::Pong {
        request_id: pong.req_id,
        provider_request_received_at,
        provider_response_sent_at,
    }))
}
