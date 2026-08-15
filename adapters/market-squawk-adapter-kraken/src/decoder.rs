//! Exact-lexeme, message-atomic Kraken decoder and book state.

use std::num::NonZeroU16;

use chrono::DateTime;
use market_squawk_domain::{
    AggressorSide, InstrumentId, IntegrityRule, MarketDepth, RuleVersion, SourceIdentifier,
    Timestamp, TradeTakerOrderType, VenueId,
};
use market_squawk_sources::{
    ControlFrameKind, DecodeError, DecodeInternalError, DecodeOutcome, DecodedControlFrame,
    DecodedProviderBatch, DecodedQuarantineAction, DecodedRecoveryAction, DecoderEvidence,
    MAX_DECODED_EVENTS, MAX_RAW_FRAME_BYTES, MarketDecoder, ProviderAggressorEvidence,
    ProviderBookChange, ProviderBookLevel, ProviderBookSide, ProviderChecksumEvidence,
    ProviderDecimalLexeme, ProviderNormalizedObservation, ProviderObservationPayload,
    ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
    ProviderTimestampEvidence, QuarantineReason, ResolvedChecksumValidator,
    ResynchronizationReason, SourceMetadata, SourceMetadataProvider, SourceProtocolProfile,
    TransportFrameKind, ValidatedRawMarketFrame, kraken_v2_crc32,
};
use rust_decimal::Decimal;

use crate::config::{KrakenChannel, KrakenDepth};
use crate::messages::{
    BookData, BookEnvelope, EnvelopeKind, Heartbeat, MAX_SUBSCRIPTION_ERROR_BYTES,
    PUBLIC_SUBSCRIPTION_REQUEST_ID, Pong, StatusEnvelope, SubscribeAck, TradeData, TradeEnvelope,
    WireLevel, bounded_trade_count, classify, exact_decimal, validate_warnings,
};
use crate::qualification::{KRAKEN_BOOK_SEQUENCE_RULE, KRAKEN_TRADE_SEQUENCE_RULE};

const VENUE: &str = "kraken";

/// Decoder synchronization state for one connection generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenDecoderState {
    /// A fresh snapshot is required.
    AwaitingSnapshot,
    /// Snapshot and every accepted update passed the checksum rule.
    Healthy,
    /// The generation is isolated; only a new snapshot may recover state.
    Quarantined,
}

/// Source-metadata-bound implementation of the shared synchronous decoder contract.
#[derive(Debug)]
pub struct KrakenMarketDecoder {
    metadata: SourceMetadata,
    decoder: KrakenDecoder,
}

impl KrakenMarketDecoder {
    /// Constructs a generation-local decoder bound to immutable source metadata.
    ///
    /// # Errors
    ///
    /// Rejects metadata that is not the reviewed Kraken live protocol profile.
    pub fn try_new(
        metadata: SourceMetadata,
        symbol: impl Into<String>,
        instrument: InstrumentId,
        depth: KrakenDepth,
    ) -> Result<Self, DecodeError> {
        Self::try_for_channel(metadata, symbol, instrument, KrakenChannel::Book(depth))
    }

    /// Constructs a generation-local trade decoder bound to checksum-unsupported metadata.
    pub fn try_trades(
        metadata: SourceMetadata,
        symbol: impl Into<String>,
        instrument: InstrumentId,
    ) -> Result<Self, DecodeError> {
        Self::try_for_channel(metadata, symbol, instrument, KrakenChannel::Trades)
    }

    fn try_for_channel(
        metadata: SourceMetadata,
        symbol: impl Into<String>,
        instrument: InstrumentId,
        channel: KrakenChannel,
    ) -> Result<Self, DecodeError> {
        if metadata.provider().as_str() != VENUE
            || metadata.quality_ceiling() != market_squawk_domain::DataQuality::DirectUnverified
            || metadata.capabilities().sequence()
                != market_squawk_domain::SequenceCapability::Unsupported
        {
            return Err(DecodeError::InvalidProviderEvidence);
        }
        let SourceProtocolProfile::Live(profile) = metadata.protocol_profile() else {
            return Err(DecodeError::InvalidProviderEvidence);
        };
        match channel {
            KrakenChannel::Book(depth) => {
                ResolvedChecksumValidator::resolve(profile.checksum(), depth.get())
                    .map_err(|_| DecodeError::InvalidProviderEvidence)?;
            }
            KrakenChannel::Trades => {
                if !matches!(
                    profile.checksum(),
                    market_squawk_sources::ChecksumValidationProfile::Unsupported { .. }
                ) {
                    return Err(DecodeError::InvalidProviderEvidence);
                }
            }
        }
        Ok(Self {
            metadata,
            decoder: KrakenDecoder::try_for_channel(symbol, instrument, channel)?,
        })
    }

    /// Returns current generation synchronization state.
    pub const fn state(&self) -> KrakenDecoderState {
        self.decoder.state()
    }
}

impl SourceMetadataProvider for KrakenMarketDecoder {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl MarketDecoder for KrakenMarketDecoder {
    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<DecodeOutcome, DecodeInternalError> {
        let SourceProtocolProfile::Live(profile) = self.metadata.protocol_profile() else {
            return Err(DecodeInternalError::InvariantViolation);
        };
        let evidence = DecoderEvidence::from_validated_frame(frame, profile.decoder_rule().clone());
        if frame.frame().transport() != TransportFrameKind::Text {
            return Ok(DecodeOutcome::Quarantine(DecodedQuarantineAction::new(
                evidence,
                QuarantineReason::SchemaViolation,
                None,
            )));
        }
        match self.decoder.decode_payload(frame.frame().payload()) {
            Ok(KrakenDecodeOutcome::Market(observations)) => {
                match DecodedProviderBatch::try_new(evidence.clone(), observations) {
                    Ok(batch) => Ok(DecodeOutcome::Data(batch)),
                    Err(error) => decode_failure_outcome(error, evidence),
                }
            }
            Ok(KrakenDecodeOutcome::Control(control)) => control_outcome(control, evidence),
            Err(error) => decode_failure_outcome(error, evidence),
        }
    }
}

fn control_outcome(
    control: KrakenControl,
    evidence: DecoderEvidence,
) -> Result<DecodeOutcome, DecodeInternalError> {
    let (kind, provider_code) = match control {
        KrakenControl::Heartbeat => (ControlFrameKind::Heartbeat, None),
        KrakenControl::Pong => (ControlFrameKind::Pong, None),
        KrakenControl::Online => (ControlFrameKind::ProviderFlowControl, Some("online")),
        KrakenControl::Subscribed(KrakenSubscription::Book) => {
            (ControlFrameKind::SubscriptionAcknowledgement, Some("book"))
        }
        KrakenControl::Subscribed(KrakenSubscription::Trade) => {
            (ControlFrameKind::SubscriptionAcknowledgement, Some("trade"))
        }
        KrakenControl::SubscriptionRefused => {
            let provider_code = SourceIdentifier::try_from("subscription_refused")
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
    Control(KrakenControl),
}

/// Validated connection/control message classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenControl {
    /// Connection liveness only; never market freshness.
    Heartbeat,
    /// Application ping response; connection liveness only.
    Pong,
    /// Exchange engine reported `online`.
    Online,
    /// Successful subscription acknowledgement.
    Subscribed(KrakenSubscription),
    /// Structurally valid provider refusal of the exact subscription request.
    SubscriptionRefused,
}

/// Acknowledged Kraken channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KrakenSubscription {
    /// Price-level book subscription at the configured depth.
    Book,
    /// Trade subscription.
    Trade,
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
    symbol: String,
    instrument: InstrumentId,
    channel: KrakenChannel,
    state: KrakenDecoderState,
    bids: Vec<ProviderBookLevel>,
    asks: Vec<ProviderBookLevel>,
    last_checksum: Option<u32>,
    rules: Rules,
}

impl KrakenDecoder {
    /// Constructs an empty decoder that requires an initializing snapshot.
    ///
    /// # Errors
    ///
    /// Rejects an invalid provider symbol or an internal rule identity that cannot be represented.
    pub fn try_new(
        symbol: impl Into<String>,
        instrument: InstrumentId,
        depth: KrakenDepth,
    ) -> Result<Self, DecodeError> {
        Self::try_for_channel(symbol, instrument, KrakenChannel::Book(depth))
    }

    /// Constructs an exact trade-channel decoder.
    pub fn try_trades(
        symbol: impl Into<String>,
        instrument: InstrumentId,
    ) -> Result<Self, DecodeError> {
        Self::try_for_channel(symbol, instrument, KrakenChannel::Trades)
    }

    fn try_for_channel(
        symbol: impl Into<String>,
        instrument: InstrumentId,
        channel: KrakenChannel,
    ) -> Result<Self, DecodeError> {
        let symbol = symbol.into();
        if symbol.is_empty() || symbol.len() > 64 || !symbol.is_ascii() {
            return Err(DecodeError::MalformedPayload);
        }
        Ok(Self {
            symbol,
            instrument,
            channel,
            state: KrakenDecoderState::AwaitingSnapshot,
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
        if payload.len() > MAX_RAW_FRAME_BYTES {
            self.state = KrakenDecoderState::Quarantined;
            return Err(DecodeError::MalformedPayload);
        }
        let kind = match classify(payload) {
            Ok(kind) => kind,
            Err(_) => {
                self.state = KrakenDecoderState::Quarantined;
                return Err(DecodeError::MalformedPayload);
            }
        };
        let outcome = match kind {
            EnvelopeKind::Book => self.decode_book(payload),
            EnvelopeKind::Trade => self.decode_trades(payload),
            EnvelopeKind::Heartbeat => validate_heartbeat(payload),
            EnvelopeKind::Status => validate_status(payload),
            EnvelopeKind::SubscribeAck => validate_ack(payload, &self.symbol, self.channel),
            EnvelopeKind::Pong => validate_pong(payload),
        };
        if matches!(
            outcome,
            Ok(KrakenDecodeOutcome::Control(
                KrakenControl::SubscriptionRefused
            ))
        ) {
            self.state = KrakenDecoderState::Quarantined;
        }
        if outcome.is_err() {
            self.state = KrakenDecoderState::Quarantined;
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
        if data.symbol != self.symbol {
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
            source_identifier(&format!("book:{}:{}", self.symbol, data.timestamp))?,
            VenueId::try_from(VENUE).map_err(|_| DecodeError::MalformedPayload)?,
            self.instrument,
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
            if trade.symbol != self.symbol || trade.trade_id < 0 {
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
                VenueId::try_from(VENUE).map_err(|_| DecodeError::MalformedPayload)?,
                self.instrument,
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
    Ok(KrakenDecodeOutcome::Control(KrakenControl::Heartbeat))
}

fn validate_status(payload: &[u8]) -> Result<KrakenDecodeOutcome, DecodeError> {
    let status: StatusEnvelope<'_> =
        serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
    let value = status.data.first().ok_or(DecodeError::MalformedPayload)?;
    if status.channel != "status"
        || status.kind != "update"
        || status.data.len() != 1
        || value.system != "online"
        || value.api_version.is_empty()
        || value.version.is_empty()
        || value.connection_id == 0
    {
        return Err(DecodeError::ResynchronizationRequired);
    }
    Ok(KrakenDecodeOutcome::Control(KrakenControl::Online))
}

fn validate_ack(
    payload: &[u8],
    symbol: &str,
    channel: KrakenChannel,
) -> Result<KrakenDecodeOutcome, DecodeError> {
    let ack: SubscribeAck<'_> =
        serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
    if ack.method != "subscribe"
        || ack.time_in.is_empty()
        || ack.time_out.is_empty()
        || ack.req_id != Some(PUBLIC_SUBSCRIPTION_REQUEST_ID)
    {
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
        return Ok(KrakenDecodeOutcome::Control(
            KrakenControl::SubscriptionRefused,
        ));
    }
    if ack.error.is_some() {
        return Err(DecodeError::ResynchronizationRequired);
    }
    let result = ack.result.as_ref().ok_or(DecodeError::MalformedPayload)?;
    let subscription = validate_subscription_result(result, symbol, channel)?;
    Ok(KrakenDecodeOutcome::Control(KrakenControl::Subscribed(
        subscription,
    )))
}

fn validate_subscription_result(
    result: &crate::messages::SubscribeResult<'_>,
    symbol: &str,
    channel: KrakenChannel,
) -> Result<KrakenSubscription, DecodeError> {
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
            Ok(KrakenSubscription::Book)
        }
        KrakenChannel::Trades
            if result.channel == "trade"
                && result.depth.is_none()
                && result.snapshot == Some(true) =>
        {
            Ok(KrakenSubscription::Trade)
        }
        KrakenChannel::Book(_) | KrakenChannel::Trades => {
            Err(DecodeError::ResynchronizationRequired)
        }
    }
}

fn validate_pong(payload: &[u8]) -> Result<KrakenDecodeOutcome, DecodeError> {
    let pong: Pong<'_> =
        serde_json::from_slice(payload).map_err(|_| DecodeError::MalformedPayload)?;
    if pong.method != "pong"
        || pong.req_id == Some(0)
        || pong.time_in.is_empty()
        || pong.time_out.is_empty()
    {
        return Err(DecodeError::MalformedPayload);
    }
    Ok(KrakenDecodeOutcome::Control(KrakenControl::Pong))
}
