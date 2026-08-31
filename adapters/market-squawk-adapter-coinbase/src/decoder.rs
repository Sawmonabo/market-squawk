use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::DateTime;
use market_squawk_domain::{
    AggressorSide, EvidenceDigest, IntegrityRule, MarketDepth, ProviderProduct,
    RawCaptureFrameView as _, SourceIdentifier, Timestamp,
};
use market_squawk_sources::{
    ControlFrameKind, DecodeInternalError, DecodeOutcome, DecodedControlFrame, DecodedIgnoredFrame,
    DecodedProviderBatch, DecodedQuarantineAction, DecodedRecoveryAction, DecoderEvidence,
    IgnoredFrameReason, ProviderAggressorEvidence, ProviderBookChange, ProviderBookLevel,
    ProviderBookSide, ProviderChecksumEvidence, ProviderDecimalLexeme,
    ProviderNativeInstrumentAttestation, ProviderNormalizedObservation, ProviderObservationPayload,
    ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
    ProviderTimestampEvidence, QuarantineReason, ResynchronizationReason, SourceMetadata,
    SourceMetadataProvider, TransportFrameKind, ValidatedRawMarketFrame,
};
use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::market_handoff::{
    CoinbaseMarketHandoffInput, CoinbaseMarketRawLineage, public_request_digests,
};
use crate::{
    CoinbaseChannel, CoinbaseConfigError, CoinbaseExchangeConfig, CoinbaseMarketChannel,
    CoinbaseMarketContinuity, CoinbaseMarketDecodeOutcome, CoinbaseMarketFeed,
    CoinbaseMarketHandoff,
};

const MAX_ENVELOPE_EVENTS: usize = market_squawk_sources::MAX_DECODED_EVENTS;
const MAX_L2_UPDATES: usize = market_squawk_sources::MAX_DECODED_BOOK_ITEMS;
const MAX_TRADES: usize = market_squawk_sources::MAX_DECODED_EVENTS;
const MAX_ACK_CHANNELS: usize = 3;
const MAX_ACK_PRODUCTS: usize = 100;
const MAX_HEARTBEAT_EVENTS: usize = 4;
const MAX_HEARTBEAT_TIME_BYTES: usize = 160;

/// Exact bounded decoder for the Coinbase Advanced Trade public market-data WebSocket.
#[derive(Clone, Debug)]
pub struct CoinbaseExchangeDecoder {
    metadata: SourceMetadata,
    instruments: BTreeMap<String, Arc<ProviderNativeInstrumentAttestation>>,
    decoder_rule: IntegrityRule,
    timestamp_rule: IntegrityRule,
    sequence_rule: IntegrityRule,
    checksum_rule: IntegrityRule,
    aggressor_rule: IntegrityRule,
    trade_snapshot_rule: IntegrityRule,
    expected_subscriptions: BTreeMap<String, BTreeSet<String>>,
    observed_subscriptions: BTreeMap<String, BTreeSet<String>>,
    acknowledgement_complete: bool,
    max_frame_bytes: usize,
    product: ProviderProduct,
    configured_instrument_attestation: Arc<ProviderNativeInstrumentAttestation>,
    request_set_digest: EvidenceDigest,
    subscription_digest: EvidenceDigest,
    last_market_coordinates: Option<CoinbasePublicMarketCoordinates>,
}

#[derive(Clone, Copy, Debug)]
struct CoinbasePublicMarketCoordinates {
    channel: CoinbaseMarketChannel,
    depth: Option<MarketDepth>,
    sequence: u64,
    provider_published_at: Timestamp,
}

impl CoinbaseExchangeDecoder {
    /// Constructs a decoder from the immutable, validated source configuration.
    ///
    /// # Errors
    ///
    /// Returns [`CoinbaseConfigError::InvalidProtocolProfile`] if the supplied metadata no longer
    /// exposes the live rules guaranteed by [`CoinbaseExchangeConfig`].
    pub fn try_new(config: &CoinbaseExchangeConfig) -> Result<Self, CoinbaseConfigError> {
        let mapping = config
            .mappings()
            .first()
            .ok_or(CoinbaseConfigError::InvalidMappingCount)?;
        let product = mapping.product().clone();
        let configured_instrument_attestation = Arc::clone(mapping.shared_instrument_attestation());
        let (request_set_digest, subscription_digest) = public_request_digests(config);
        let live = match config.metadata().protocol_profile() {
            market_squawk_sources::SourceProtocolProfile::Live(profile) => profile,
            market_squawk_sources::SourceProtocolProfile::NotLive => {
                return Err(CoinbaseConfigError::InvalidProtocolProfile);
            }
        };
        let sequence_rule = match live.sequence() {
            market_squawk_sources::SequenceValidationProfile::Unsupported { rule } => rule.clone(),
            market_squawk_sources::SequenceValidationProfile::Provided { rule, .. } => rule.clone(),
        };
        let checksum_rule = match live.checksum() {
            market_squawk_sources::ChecksumValidationProfile::Unsupported { rule } => rule.clone(),
            market_squawk_sources::ChecksumValidationProfile::Provided { rule, .. } => rule.clone(),
        };
        let trade_snapshot_rule = config
            .metadata()
            .coverage()
            .live()
            .and_then(|coverage| {
                coverage.rule_for(market_squawk_domain::LiveEventClass::Trade, None)
            })
            .and_then(|rule| match rule.snapshot_applicability() {
                market_squawk_domain::SnapshotApplicability::NotApplicable { metadata_rule } => {
                    Some(metadata_rule.clone())
                }
                market_squawk_domain::SnapshotApplicability::Required => None,
            })
            .ok_or(CoinbaseConfigError::InvalidProtocolProfile)?;
        let products = config
            .mappings()
            .iter()
            .map(|mapping| mapping.product().as_source_identifier().as_str().to_owned())
            .collect::<BTreeSet<_>>();
        let expected_subscriptions = config
            .channels()
            .iter()
            .map(|channel| {
                let channel_products = if *channel == CoinbaseChannel::Heartbeats {
                    BTreeSet::from(["heartbeats".to_owned()])
                } else {
                    products.clone()
                };
                (channel.as_str().to_owned(), channel_products)
            })
            .collect();
        Ok(Self {
            metadata: config.metadata().clone(),
            instruments: config
                .mappings()
                .iter()
                .map(|mapping| {
                    (
                        mapping.product().as_source_identifier().as_str().to_owned(),
                        Arc::clone(mapping.shared_instrument_attestation()),
                    )
                })
                .collect(),
            decoder_rule: live.decoder_rule().clone(),
            timestamp_rule: live.timestamp_rule().clone(),
            sequence_rule,
            checksum_rule,
            aggressor_rule: live.semantic_interpretation().aggressor_rule().clone(),
            trade_snapshot_rule,
            expected_subscriptions,
            observed_subscriptions: BTreeMap::new(),
            acknowledgement_complete: false,
            max_frame_bytes: config.transport_limits().max_frame_bytes(),
            product,
            configured_instrument_attestation,
            request_set_digest,
            subscription_digest,
            last_market_coordinates: None,
        })
    }

    fn decode_text(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
        evidence: DecoderEvidence,
    ) -> Result<DecodeOutcome, DecodeInternalError> {
        let payload = frame.frame().payload();
        if payload.len() > self.max_frame_bytes {
            return Ok(quarantine(
                evidence,
                QuarantineReason::SchemaViolation,
                None,
            ));
        }
        let probe = match serde_json::from_slice::<MessageProbe>(payload) {
            Ok(probe) => probe,
            Err(error) => {
                let reason = if error.is_syntax() || error.is_eof() {
                    QuarantineReason::MalformedPayload
                } else {
                    QuarantineReason::SchemaViolation
                };
                return Ok(quarantine(evidence, reason, None));
            }
        };
        if probe.kind.as_deref() == Some("error") {
            return Ok(self.decode_provider_error(payload, evidence));
        }
        let Some(channel) = probe.channel else {
            return Ok(match probe.kind {
                Some(kind) => match SourceIdentifier::try_from(kind) {
                    Ok(code) => DecodeOutcome::Ignored(DecodedIgnoredFrame::new(
                        evidence,
                        IgnoredFrameReason::DocumentedForwardCompatibleExtension,
                        Some(code),
                    )),
                    Err(_) => quarantine(evidence, QuarantineReason::SchemaViolation, None),
                },
                None => quarantine(evidence, QuarantineReason::SchemaViolation, None),
            });
        };
        let outcome = match channel.as_str() {
            "l2_data" => self.decode_l2(payload, evidence),
            "market_trades" => self.decode_market_trades(payload, evidence),
            "heartbeats" => self.decode_heartbeats(payload, evidence),
            "subscriptions" => self.decode_subscriptions(payload, evidence),
            _ => match SourceIdentifier::try_from(channel) {
                Ok(code) => DecodeOutcome::Ignored(DecodedIgnoredFrame::new(
                    evidence,
                    IgnoredFrameReason::DocumentedForwardCompatibleExtension,
                    Some(code),
                )),
                Err(_) => quarantine(evidence, QuarantineReason::SchemaViolation, None),
            },
        };
        Ok(outcome)
    }

    fn decode_l2(&mut self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<L2Envelope>(payload) {
            Ok(wire) => wire,
            Err(_) => return quarantine(evidence, QuarantineReason::SchemaViolation, None),
        };
        let envelope_at =
            match validate_header(&wire.channel, "l2_data", &wire.client_id, &wire.timestamp) {
                Ok(timestamp) => timestamp,
                Err(reason) => return quarantine(evidence, reason, None),
            };
        if wire.events.0.is_empty() {
            return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
        }
        let mut observations = Vec::new();
        if observations.try_reserve_exact(wire.events.0.len()).is_err() {
            return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
        }
        for (index, event) in wire.events.0.into_iter().enumerate() {
            let instrument = match self.instrument(&event.product_id) {
                Ok(instrument) => instrument,
                Err(reason) => {
                    return quarantine(evidence, reason, source_code(&event.product_id));
                }
            };
            if event.updates.0.is_empty() {
                return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
            }
            let payload = match event.kind.as_str() {
                "snapshot" => {
                    let mut bids = Vec::new();
                    let mut asks = Vec::new();
                    if bids.try_reserve_exact(event.updates.0.len()).is_err()
                        || asks.try_reserve_exact(event.updates.0.len()).is_err()
                    {
                        return quarantine(
                            evidence,
                            QuarantineReason::ProtocolInvariantViolation,
                            None,
                        );
                    }
                    for update in event.updates.0 {
                        let (side, level, _event_at) = match parse_l2_update(update, false) {
                            Ok(update) => update,
                            Err(reason) => return quarantine(evidence, reason, None),
                        };
                        match side {
                            ProviderBookSide::Bid => bids.push(level),
                            ProviderBookSide::Ask => asks.push(level),
                        }
                    }
                    match ProviderObservationPayload::book_snapshot(
                        MarketDepth::PriceLevel,
                        bids,
                        asks,
                    ) {
                        Ok(payload) => (
                            ProviderSnapshotEvidence::InitializingSnapshot {
                                provider_reference: None,
                            },
                            payload,
                        ),
                        Err(_) => {
                            return quarantine(evidence, QuarantineReason::SchemaViolation, None);
                        }
                    }
                }
                "update" => {
                    let mut changes = Vec::new();
                    if changes.try_reserve_exact(event.updates.0.len()).is_err() {
                        return quarantine(
                            evidence,
                            QuarantineReason::ProtocolInvariantViolation,
                            None,
                        );
                    }
                    for update in event.updates.0 {
                        let (side, level, _event_at) = match parse_l2_update(update, true) {
                            Ok(update) => update,
                            Err(reason) => return quarantine(evidence, reason, None),
                        };
                        changes.push(ProviderBookChange::new(side, level));
                    }
                    match ProviderObservationPayload::book_delta(MarketDepth::PriceLevel, changes) {
                        Ok(payload) => (
                            ProviderSnapshotEvidence::Delta {
                                provider_snapshot_reference: None,
                            },
                            payload,
                        ),
                        Err(_) => {
                            return quarantine(
                                evidence,
                                QuarantineReason::ProtocolInvariantViolation,
                                None,
                            );
                        }
                    }
                }
                _ => {
                    return quarantine(evidence, QuarantineReason::UnsupportedSemanticChange, None);
                }
            };
            // Coinbase documents `event_time` as the trading-engine time of each price-level
            // change and even shows epoch-zero values inside a current snapshot. It is validated
            // above as provider evidence, but it cannot represent freshness for the complete
            // snapshot/update observation. The envelope timestamp is the source publication time
            // for this message and therefore owns observation-level freshness.
            observations.push(observation_input(
                format!("l2-{}-{index}-{}", wire.sequence_num, event.product_id),
                instrument,
                ProviderTimestampEvidence::Provided {
                    value: envelope_at,
                    rule: self.timestamp_rule.clone(),
                },
                payload.0,
                payload.1,
            ));
        }
        let outcome = self.data(evidence, observations);
        if matches!(outcome, DecodeOutcome::Data(_)) {
            self.last_market_coordinates = Some(CoinbasePublicMarketCoordinates {
                channel: CoinbaseMarketChannel::Level2,
                depth: Some(MarketDepth::PriceLevel),
                sequence: wire.sequence_num,
                provider_published_at: envelope_at,
            });
        }
        outcome
    }

    fn decode_market_trades(&mut self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<MarketTradesEnvelope>(payload) {
            Ok(wire) => wire,
            Err(_) => return quarantine(evidence, QuarantineReason::SchemaViolation, None),
        };
        let envelope_at = match validate_header(
            &wire.channel,
            "market_trades",
            &wire.client_id,
            &wire.timestamp,
        ) {
            Ok(timestamp) => timestamp,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let mut observations = Vec::new();
        let mut trade_ids = BTreeSet::new();
        for event in wire.events.0 {
            if !matches!(event.kind.as_str(), "snapshot" | "update") {
                return quarantine(evidence, QuarantineReason::UnsupportedSemanticChange, None);
            }
            if observations.try_reserve(event.trades.0.len()).is_err() {
                return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
            }
            for trade in event.trades.0 {
                if !trade_ids.insert(trade.trade_id.clone()) {
                    return quarantine(
                        evidence,
                        QuarantineReason::ProtocolInvariantViolation,
                        None,
                    );
                }
                let instrument = match self.instrument(&trade.product_id) {
                    Ok(instrument) => instrument,
                    Err(reason) => {
                        return quarantine(evidence, reason, source_code(&trade.product_id));
                    }
                };
                let timestamp = match parse_timestamp(&trade.time) {
                    Ok(timestamp) => timestamp,
                    Err(reason) => return quarantine(evidence, reason, None),
                };
                let price = match parse_price(trade.price) {
                    Ok(price) => price,
                    Err(reason) => return quarantine(evidence, reason, None),
                };
                let quantity = match parse_quantity(trade.size, false) {
                    Ok(quantity) => quantity,
                    Err(reason) => return quarantine(evidence, reason, None),
                };
                let (aggressor, maker_code) = match trade.side.as_str() {
                    "SELL" => (AggressorSide::Buy, "maker:sell"),
                    "BUY" => (AggressorSide::Sell, "maker:buy"),
                    _ => {
                        return quarantine(
                            evidence,
                            QuarantineReason::UnsupportedSemanticChange,
                            None,
                        );
                    }
                };
                let maker_code = match SourceIdentifier::try_from(maker_code) {
                    Ok(code) => code,
                    Err(_) => {
                        return quarantine(
                            evidence,
                            QuarantineReason::ProtocolInvariantViolation,
                            None,
                        );
                    }
                };
                let trade_identifier = match SourceIdentifier::try_from(trade.trade_id.as_str()) {
                    Ok(identifier) => identifier,
                    Err(_) => {
                        return quarantine(
                            evidence,
                            QuarantineReason::ProtocolInvariantViolation,
                            None,
                        );
                    }
                };
                observations.push(observation_input(
                    trade.trade_id,
                    instrument,
                    ProviderTimestampEvidence::Provided {
                        value: timestamp,
                        rule: self.timestamp_rule.clone(),
                    },
                    ProviderSnapshotEvidence::NotApplicable(self.trade_snapshot_rule.clone()),
                    ProviderObservationPayload::Trade {
                        trade_id: trade_identifier,
                        price,
                        quantity,
                        aggressor: ProviderAggressorEvidence::new(
                            aggressor,
                            Some(maker_code),
                            self.aggressor_rule.clone(),
                        ),
                        taker_order_type: None,
                    },
                ));
            }
        }
        if observations.is_empty() {
            DecodeOutcome::Control(DecodedControlFrame::new(
                evidence,
                ControlFrameKind::ProviderFlowControl,
                source_code(&wire.sequence_num.to_string()),
            ))
        } else {
            let outcome = self.data(evidence, observations);
            if matches!(outcome, DecodeOutcome::Data(_)) {
                self.last_market_coordinates = Some(CoinbasePublicMarketCoordinates {
                    channel: CoinbaseMarketChannel::MarketTrades,
                    depth: None,
                    sequence: wire.sequence_num,
                    provider_published_at: envelope_at,
                });
            }
            outcome
        }
    }

    fn decode_heartbeats(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<HeartbeatsEnvelope>(payload) {
            Ok(wire) => wire,
            Err(_) => return quarantine(evidence, QuarantineReason::SchemaViolation, None),
        };
        if let Err(reason) = validate_header(
            &wire.channel,
            "heartbeats",
            &wire.client_id,
            &wire.timestamp,
        ) {
            return quarantine(evidence, reason, None);
        }
        if wire.events.0.is_empty() {
            return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
        }
        let _sequence_num = wire.sequence_num;
        let mut last_counter = None;
        for event in wire.events.0 {
            if event.current_time.is_empty() || event.current_time.len() > MAX_HEARTBEAT_TIME_BYTES
            {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
            last_counter = Some(event.heartbeat_counter.0);
        }
        DecodeOutcome::Control(DecodedControlFrame::new(
            evidence,
            ControlFrameKind::Heartbeat,
            last_counter.and_then(|counter| source_code(&counter.to_string())),
        ))
    }

    fn decode_subscriptions(&mut self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<SubscriptionsEnvelope>(payload) {
            Ok(wire) => wire,
            Err(_) => return quarantine(evidence, QuarantineReason::SchemaViolation, None),
        };
        if let Err(reason) = validate_header(
            &wire.channel,
            "subscriptions",
            &wire.client_id,
            &wire.timestamp,
        ) {
            return quarantine(evidence, reason, None);
        }
        if wire.events.0.len() != 1 {
            return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
        }
        let mut events = wire.events.0.into_iter();
        let Some(event) = events.next() else {
            return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
        };
        let actual = event
            .subscriptions
            .0
            .into_iter()
            .map(|(channel, products)| (channel, products.0.into_iter().collect()))
            .collect::<BTreeMap<String, BTreeSet<String>>>();
        if actual.is_empty() {
            return quarantine(evidence, QuarantineReason::WrongChannel, None);
        }
        for (channel, products) in &actual {
            let Some(expected_products) = self.expected_subscriptions.get(channel) else {
                return quarantine(
                    evidence,
                    QuarantineReason::WrongChannel,
                    source_code(channel),
                );
            };
            if products != expected_products {
                return quarantine(
                    evidence,
                    QuarantineReason::WrongProduct,
                    source_code(channel),
                );
            }
        }
        if self
            .observed_subscriptions
            .iter()
            .any(|(channel, products)| actual.get(channel) != Some(products))
        {
            return quarantine(evidence, QuarantineReason::WrongChannel, None);
        }
        if actual != self.observed_subscriptions {
            self.observed_subscriptions = actual;
        }
        if self.observed_subscriptions == self.expected_subscriptions
            && !self.acknowledgement_complete
        {
            self.acknowledgement_complete = true;
            DecodeOutcome::Control(DecodedControlFrame::new(
                evidence,
                ControlFrameKind::SubscriptionAcknowledgement,
                None,
            ))
        } else {
            DecodeOutcome::Control(DecodedControlFrame::new(
                evidence,
                ControlFrameKind::ProviderFlowControl,
                source_code(&wire.sequence_num.to_string()),
            ))
        }
    }

    fn decode_provider_error(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<ProviderErrorWire>(payload) {
            Ok(wire) if wire.kind == "error" => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        let _provider_message = wire.message.or(wire.reason);
        DecodeOutcome::Resynchronize(DecodedRecoveryAction::new(
            evidence,
            ResynchronizationReason::ProviderRequestedReset,
            source_code("coinbase-provider-error"),
        ))
    }

    fn instrument(
        &self,
        product: &str,
    ) -> Result<Arc<ProviderNativeInstrumentAttestation>, QuarantineReason> {
        self.instruments
            .get(product)
            .cloned()
            .ok_or(QuarantineReason::WrongProduct)
    }

    fn data(&self, evidence: DecoderEvidence, inputs: Vec<ObservationInput>) -> DecodeOutcome {
        let mut observations = Vec::new();
        if observations.try_reserve_exact(inputs.len()).is_err() {
            return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
        }
        for input in inputs {
            if input
                .instrument_attestation
                .validate_at(evidence.received_at())
                .is_err()
            {
                return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
            }
            let source_identifier = match SourceIdentifier::try_from(input.source_identifier) {
                Ok(identifier) => identifier,
                Err(_) => {
                    return quarantine(
                        evidence,
                        QuarantineReason::ProtocolInvariantViolation,
                        None,
                    );
                }
            };
            let observation = match ProviderNormalizedObservation::try_new(
                source_identifier,
                input.instrument_attestation.as_ref().clone(),
                input.timestamp,
                ProviderSequenceEvidence::Unsupported {
                    rule: self.sequence_rule.clone(),
                },
                input.snapshot,
                ProviderChecksumEvidence::Unsupported {
                    rule: self.checksum_rule.clone(),
                },
                input.payload,
            ) {
                Ok(observation) => observation,
                Err(_) => {
                    return quarantine(
                        evidence,
                        QuarantineReason::ProtocolInvariantViolation,
                        None,
                    );
                }
            };
            observations.push(observation);
        }
        match DecodedProviderBatch::try_new(evidence.clone(), observations) {
            Ok(batch) => DecodeOutcome::Data(batch),
            Err(_) => quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None),
        }
    }

    fn decode_with_market_coordinates(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<(DecodeOutcome, Option<CoinbasePublicMarketCoordinates>), DecodeInternalError> {
        self.last_market_coordinates = None;
        let evidence = DecoderEvidence::from_validated_frame(frame, self.decoder_rule.clone());
        let outcome = if frame.frame().transport() == TransportFrameKind::Text {
            self.decode_text(frame, evidence)?
        } else {
            quarantine(evidence, QuarantineReason::SchemaViolation, None)
        };
        Ok((outcome, self.last_market_coordinates.take()))
    }

    /// Decodes one public frame and, for book/trade data, consumes exact raw bytes and the typed
    /// batch into the provider-owned market handoff. Controls and fail-closed outcomes retain the
    /// ordinary decoder shape.
    pub fn decode_market_handoff(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<CoinbaseMarketDecodeOutcome, DecodeInternalError> {
        let (outcome, coordinates) = self.decode_with_market_coordinates(frame)?;
        match (outcome, coordinates) {
            (DecodeOutcome::Data(typed_batch), Some(coordinates)) => {
                let handoff = CoinbaseMarketHandoff::try_new(
                    CoinbaseMarketHandoffInput {
                        feed: CoinbaseMarketFeed::AdvancedTradePublic,
                        channel: coordinates.channel,
                        native_input_depth: coordinates.depth,
                        product: self.product.clone(),
                        instrument_attestation: Arc::clone(&self.configured_instrument_attestation),
                        request_set_digest: self.request_set_digest,
                        subscription_digest: self.subscription_digest,
                        subscription_acknowledgement: None,
                        continuity: CoinbaseMarketContinuity::ProviderCursorUnverified {
                            terminal: coordinates.sequence,
                        },
                        provider_published_at: coordinates.provider_published_at,
                        snapshot_provider_at: None,
                    },
                    CoinbaseMarketRawLineage::AdvancedTrade(
                        frame.frame().capture_payload().clone(),
                    ),
                    typed_batch,
                )
                .map_err(|_error| DecodeInternalError::InvariantViolation)?;
                Ok(CoinbaseMarketDecodeOutcome::Market(handoff))
            }
            (DecodeOutcome::Data(_), None) | (_, Some(_)) => {
                Err(DecodeInternalError::InvariantViolation)
            }
            (outcome, None) => Ok(CoinbaseMarketDecodeOutcome::Other(outcome)),
        }
    }
}

impl SourceMetadataProvider for CoinbaseExchangeDecoder {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

struct ObservationInput {
    source_identifier: String,
    instrument_attestation: Arc<ProviderNativeInstrumentAttestation>,
    timestamp: ProviderTimestampEvidence,
    snapshot: ProviderSnapshotEvidence,
    payload: ProviderObservationPayload,
}

fn observation_input(
    source_identifier: String,
    instrument_attestation: Arc<ProviderNativeInstrumentAttestation>,
    timestamp: ProviderTimestampEvidence,
    snapshot: ProviderSnapshotEvidence,
    payload: ProviderObservationPayload,
) -> ObservationInput {
    ObservationInput {
        source_identifier,
        instrument_attestation,
        timestamp,
        snapshot,
        payload,
    }
}

fn quarantine(
    evidence: DecoderEvidence,
    reason: QuarantineReason,
    provider_code: Option<SourceIdentifier>,
) -> DecodeOutcome {
    DecodeOutcome::Quarantine(DecodedQuarantineAction::new(
        evidence,
        reason,
        provider_code,
    ))
}

fn source_code(value: &str) -> Option<SourceIdentifier> {
    SourceIdentifier::try_from(value).ok()
}

fn validate_header(
    actual_channel: &str,
    expected_channel: &str,
    client_id: &str,
    timestamp: &str,
) -> Result<Timestamp, QuarantineReason> {
    if actual_channel != expected_channel || !client_id.is_empty() {
        return Err(QuarantineReason::WrongChannel);
    }
    parse_timestamp(timestamp)
}

fn parse_timestamp(value: &str) -> Result<Timestamp, QuarantineReason> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|timestamp| timestamp.timestamp_nanos_opt())
        .map(Timestamp::from_unix_nanos)
        .ok_or(QuarantineReason::InvalidTimestamp)
}

fn parse_price(value: String) -> Result<ProviderPrice, QuarantineReason> {
    let lexeme = ProviderDecimalLexeme::try_new(&value)
        .map_err(|_| QuarantineReason::InexactNumericValue)?;
    if lexeme.decimal().is_zero() || lexeme.decimal().is_sign_negative() {
        return Err(QuarantineReason::InexactNumericValue);
    }
    Ok(ProviderPrice::new(lexeme))
}

fn parse_quantity(value: String, allow_zero: bool) -> Result<ProviderQuantity, QuarantineReason> {
    let lexeme = ProviderDecimalLexeme::try_new(&value)
        .map_err(|_| QuarantineReason::InexactNumericValue)?;
    if lexeme.decimal().is_sign_negative() || (!allow_zero && lexeme.decimal().is_zero()) {
        return Err(QuarantineReason::NegativeQuantity);
    }
    Ok(ProviderQuantity::new(lexeme))
}

fn parse_l2_update(
    update: L2UpdateWire,
    allow_zero: bool,
) -> Result<(ProviderBookSide, ProviderBookLevel, Timestamp), QuarantineReason> {
    let side = match update.side.as_str() {
        "bid" => ProviderBookSide::Bid,
        "offer" => ProviderBookSide::Ask,
        _ => return Err(QuarantineReason::UnsupportedSemanticChange),
    };
    Ok((
        side,
        ProviderBookLevel::new(
            parse_price(update.price_level)?,
            parse_quantity(update.new_quantity, allow_zero)?,
        ),
        parse_timestamp(&update.event_time)?,
    ))
}

#[derive(Deserialize)]
struct MessageProbe {
    channel: Option<String>,
    #[serde(rename = "type")]
    kind: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct L2Envelope {
    channel: String,
    #[serde(default)]
    client_id: String,
    timestamp: String,
    sequence_num: u64,
    events: BoundedSequence<L2EventWire, MAX_ENVELOPE_EVENTS>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct L2EventWire {
    #[serde(rename = "type")]
    kind: String,
    product_id: String,
    updates: BoundedSequence<L2UpdateWire, MAX_L2_UPDATES>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct L2UpdateWire {
    side: String,
    event_time: String,
    price_level: String,
    new_quantity: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketTradesEnvelope {
    channel: String,
    #[serde(default)]
    client_id: String,
    timestamp: String,
    sequence_num: u64,
    events: BoundedSequence<MarketTradesEventWire, MAX_ENVELOPE_EVENTS>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketTradesEventWire {
    #[serde(rename = "type")]
    kind: String,
    trades: BoundedSequence<MarketTradeWire, MAX_TRADES>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketTradeWire {
    trade_id: String,
    product_id: String,
    price: String,
    size: String,
    side: String,
    time: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatsEnvelope {
    channel: String,
    #[serde(default)]
    client_id: String,
    timestamp: String,
    sequence_num: u64,
    events: BoundedSequence<HeartbeatEventWire, MAX_HEARTBEAT_EVENTS>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatEventWire {
    current_time: String,
    heartbeat_counter: HeartbeatCounterWire,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeartbeatCounterWire(u64);

impl<'de> Deserialize<'de> for HeartbeatCounterWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct HeartbeatCounterVisitor;

        impl Visitor<'_> for HeartbeatCounterVisitor {
            type Value = HeartbeatCounterWire;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a non-negative 64-bit heartbeat counter or decimal string")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(HeartbeatCounterWire(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.is_empty() || value.len() > 20 {
                    return Err(E::custom("heartbeat counter is outside its decimal bound"));
                }
                value
                    .parse::<u64>()
                    .map(HeartbeatCounterWire)
                    .map_err(|_error| E::custom("heartbeat counter is not an unsigned integer"))
            }
        }

        deserializer.deserialize_any(HeartbeatCounterVisitor)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionsEnvelope {
    channel: String,
    #[serde(default)]
    client_id: String,
    timestamp: String,
    sequence_num: u64,
    events: BoundedSequence<SubscriptionsEventWire, 1>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionsEventWire {
    subscriptions: BoundedMap<String, BoundedSequence<String, MAX_ACK_PRODUCTS>, MAX_ACK_CHANNELS>,
}

#[derive(Deserialize)]
struct ProviderErrorWire {
    #[serde(rename = "type")]
    kind: String,
    message: Option<String>,
    reason: Option<String>,
}

struct BoundedSequence<T, const N: usize>(Vec<T>);

impl<'de, T, const N: usize> Deserialize<'de> for BoundedSequence<T, N>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SequenceVisitor<T, const N: usize>(std::marker::PhantomData<T>);

        impl<'de, T, const N: usize> Visitor<'de> for SequenceVisitor<T, N>
        where
            T: Deserialize<'de>,
        {
            type Value = BoundedSequence<T, N>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "at most {N} sequence items")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    if values.len() == N {
                        return Err(A::Error::custom("bounded sequence capacity exceeded"));
                    }
                    values.push(value);
                }
                Ok(BoundedSequence(values))
            }
        }

        deserializer.deserialize_seq(SequenceVisitor(std::marker::PhantomData))
    }
}

struct BoundedMap<K, V, const N: usize>(BTreeMap<K, V>);

impl<'de, K, V, const N: usize> Deserialize<'de> for BoundedMap<K, V, N>
where
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MapVisitor<K, V, const N: usize>(std::marker::PhantomData<(K, V)>);

        impl<'de, K, V, const N: usize> Visitor<'de> for MapVisitor<K, V, N>
        where
            K: Deserialize<'de> + Ord,
            V: Deserialize<'de>,
        {
            type Value = BoundedMap<K, V, N>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "at most {N} unique map entries")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    if values.len() == N {
                        return Err(A::Error::custom("bounded map capacity exceeded"));
                    }
                    if values.insert(key, value).is_some() {
                        return Err(A::Error::custom("duplicate map key"));
                    }
                }
                Ok(BoundedMap(values))
            }
        }

        deserializer.deserialize_map(MapVisitor(std::marker::PhantomData))
    }
}
