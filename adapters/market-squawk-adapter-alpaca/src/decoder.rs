use std::collections::{BTreeMap, BTreeSet};

use chrono::DateTime;
use market_squawk_domain::{
    AggressorSide, HaltTransition, InstrumentId, IntegrityRule, SourceIdentifier, Timestamp,
    VenueId,
};
use market_squawk_sources::{
    ControlFrameKind, DecodeInternalError, DecodeOutcome, DecodedControlFrame, DecodedIgnoredFrame,
    DecodedProviderBatch, DecodedQuarantineAction, DecodedRecoveryAction, DecoderEvidence,
    IgnoredFrameReason, MarketDecoder, ProviderAggressorEvidence, ProviderBookLevel,
    ProviderChecksumEvidence, ProviderDecimalLexeme, ProviderNormalizedObservation,
    ProviderObservationPayload, ProviderPrice, ProviderQuantity, ProviderSequenceEvidence,
    ProviderSnapshotEvidence, ProviderStatusEvidence, ProviderTimestampEvidence, QuarantineReason,
    ResynchronizationReason, SourceMetadata, SourceMetadataProvider, TransportFrameKind,
    ValidatedRawMarketFrame,
};
use serde::Deserialize;
use serde_json::{Number, Value};

use crate::config::{IEX_VENUE, INDICATIVE_OPTIONS_VENUE};
use crate::{AlpacaError, AlpacaIexLiveConfig, AlpacaOptionsLiveConfig};

const MAX_MESSAGES_PER_FRAME: usize = market_squawk_sources::MAX_DECODED_EVENTS;

/// Stateful JSON decoder for one Alpaca IEX connection generation.
#[derive(Clone, Debug)]
pub struct AlpacaIexDecoder(AlpacaDecoder);

impl AlpacaIexDecoder {
    /// Constructs a decoder whose expected subscription exactly matches the admitted 30-symbol
    /// configuration.
    pub fn try_new(config: &AlpacaIexLiveConfig) -> Result<Self, AlpacaError> {
        let symbols = config
            .mappings()
            .iter()
            .map(|mapping| (mapping.symbol().to_owned(), mapping.instrument()))
            .collect::<BTreeMap<_, _>>();
        Ok(Self(AlpacaDecoder::try_new(
            config.metadata(),
            symbols,
            VenueId::try_from(IEX_VENUE)?,
            DecoderSurface::Iex,
            config.transport_limits().max_frame_bytes(),
        )?))
    }
}

impl SourceMetadataProvider for AlpacaIexDecoder {
    fn metadata(&self) -> &SourceMetadata {
        &self.0.metadata
    }
}

impl MarketDecoder for AlpacaIexDecoder {
    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<DecodeOutcome, DecodeInternalError> {
        self.0.decode(frame, TransportFrameKind::Text)
    }
}

/// Stateful MessagePack decoder for one Alpaca indicative-options connection generation.
#[derive(Clone, Debug)]
pub struct AlpacaOptionsDecoder(AlpacaDecoder);

impl AlpacaOptionsDecoder {
    /// Constructs a decoder whose expected subscription excludes wildcards and exactly matches
    /// the admitted 200-symbol option set.
    pub fn try_new(config: &AlpacaOptionsLiveConfig) -> Result<Self, AlpacaError> {
        let symbols = config
            .mappings()
            .iter()
            .map(|mapping| (mapping.symbol().to_owned(), mapping.instrument()))
            .collect::<BTreeMap<_, _>>();
        Ok(Self(AlpacaDecoder::try_new(
            config.metadata(),
            symbols,
            VenueId::try_from(INDICATIVE_OPTIONS_VENUE)?,
            DecoderSurface::IndicativeOptions,
            config.transport_limits().max_frame_bytes(),
        )?))
    }
}

impl SourceMetadataProvider for AlpacaOptionsDecoder {
    fn metadata(&self) -> &SourceMetadata {
        &self.0.metadata
    }
}

impl MarketDecoder for AlpacaOptionsDecoder {
    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<DecodeOutcome, DecodeInternalError> {
        self.0.decode(frame, TransportFrameKind::Binary)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DecoderSurface {
    Iex,
    IndicativeOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionState {
    AwaitingConnected,
    AwaitingAuthenticated,
    AwaitingSubscription,
    Active,
}

#[derive(Clone, Debug)]
struct AlpacaDecoder {
    metadata: SourceMetadata,
    instruments: BTreeMap<String, InstrumentId>,
    expected_symbols: BTreeSet<String>,
    venue: VenueId,
    surface: DecoderSurface,
    state: SessionState,
    decoder_rule: IntegrityRule,
    timestamp_rule: IntegrityRule,
    sequence_rule: IntegrityRule,
    checksum_rule: IntegrityRule,
    aggressor_rule: IntegrityRule,
    status_rule: IntegrityRule,
    snapshot_rule: IntegrityRule,
    max_frame_bytes: usize,
}

impl AlpacaDecoder {
    fn try_new(
        metadata: &SourceMetadata,
        instruments: BTreeMap<String, InstrumentId>,
        venue: VenueId,
        surface: DecoderSurface,
        max_frame_bytes: usize,
    ) -> Result<Self, AlpacaError> {
        let live = match metadata.protocol_profile() {
            market_squawk_sources::SourceProtocolProfile::Live(profile) => profile,
            market_squawk_sources::SourceProtocolProfile::NotLive => {
                return Err(AlpacaError::Protocol);
            }
        };
        let sequence_rule = match live.sequence() {
            market_squawk_sources::SequenceValidationProfile::Unsupported { rule } => rule.clone(),
            market_squawk_sources::SequenceValidationProfile::Provided { .. } => {
                return Err(AlpacaError::Protocol);
            }
        };
        let checksum_rule = match live.checksum() {
            market_squawk_sources::ChecksumValidationProfile::Unsupported { rule } => rule.clone(),
            market_squawk_sources::ChecksumValidationProfile::Provided { .. } => {
                return Err(AlpacaError::Protocol);
            }
        };
        let snapshot_event = match surface {
            DecoderSurface::Iex => market_squawk_domain::LiveEventClass::Quote,
            DecoderSurface::IndicativeOptions => market_squawk_domain::LiveEventClass::Trade,
        };
        let snapshot_rule = metadata
            .coverage()
            .live()
            .and_then(|coverage| coverage.rule_for(snapshot_event, None))
            .and_then(|coverage| match coverage.snapshot_applicability() {
                market_squawk_domain::SnapshotApplicability::NotApplicable { metadata_rule } => {
                    Some(metadata_rule.clone())
                }
                market_squawk_domain::SnapshotApplicability::Required => None,
            })
            .ok_or(AlpacaError::Protocol)?;
        let expected_symbols = instruments.keys().cloned().collect();
        Ok(Self {
            metadata: metadata.clone(),
            instruments,
            expected_symbols,
            venue,
            surface,
            state: SessionState::AwaitingConnected,
            decoder_rule: live.decoder_rule().clone(),
            timestamp_rule: live.timestamp_rule().clone(),
            sequence_rule,
            checksum_rule,
            aggressor_rule: live.semantic_interpretation().aggressor_rule().clone(),
            status_rule: live.semantic_interpretation().trading_status_rule().clone(),
            snapshot_rule,
            max_frame_bytes,
        })
    }

    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
        expected_transport: TransportFrameKind,
    ) -> Result<DecodeOutcome, DecodeInternalError> {
        let evidence = DecoderEvidence::from_validated_frame(frame, self.decoder_rule.clone());
        if frame.frame().transport() != expected_transport
            || frame.frame().payload().len() > self.max_frame_bytes
        {
            return Ok(quarantine(
                evidence,
                QuarantineReason::SchemaViolation,
                None,
            ));
        }
        let messages = match self.surface {
            DecoderSurface::Iex => {
                serde_json::from_slice::<Vec<Value>>(frame.frame().payload()).map_err(|_| ())
            }
            DecoderSurface::IndicativeOptions => {
                rmp_serde::from_slice::<Vec<Value>>(frame.frame().payload()).map_err(|_| ())
            }
        };
        let messages = match messages {
            Ok(messages) if !messages.is_empty() && messages.len() <= MAX_MESSAGES_PER_FRAME => {
                messages
            }
            _ => {
                return Ok(quarantine(
                    evidence,
                    QuarantineReason::MalformedPayload,
                    None,
                ));
            }
        };
        let kinds = messages
            .iter()
            .map(message_kind)
            .collect::<Result<Vec<_>, _>>();
        let kinds = match kinds {
            Ok(kinds) => kinds,
            Err(reason) => return Ok(quarantine(evidence, reason, None)),
        };
        let control = kinds
            .iter()
            .all(|kind| matches!(kind.as_str(), "success" | "subscription" | "error"));
        if control {
            if messages.len() != 1 {
                return Ok(quarantine(
                    evidence,
                    QuarantineReason::ProtocolInvariantViolation,
                    None,
                ));
            }
            return Ok(self.decode_control(messages.into_iter().next(), evidence));
        }
        if kinds
            .iter()
            .any(|kind| matches!(kind.as_str(), "success" | "subscription" | "error"))
            || self.state != SessionState::Active
        {
            return Ok(quarantine(
                evidence,
                QuarantineReason::ProtocolInvariantViolation,
                None,
            ));
        }
        self.decode_data(messages, evidence)
    }

    fn decode_control(
        &mut self,
        message: Option<Value>,
        evidence: DecoderEvidence,
    ) -> DecodeOutcome {
        let Some(message) = message else {
            return quarantine(evidence, QuarantineReason::MalformedPayload, None);
        };
        let kind = match message_kind(&message) {
            Ok(kind) => kind,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        match kind.as_str() {
            "success" => {
                let wire = match serde_json::from_value::<SuccessWire>(message) {
                    Ok(wire) => wire,
                    Err(_) => {
                        return quarantine(evidence, QuarantineReason::SchemaViolation, None);
                    }
                };
                let valid = match (self.state, wire.message.as_str()) {
                    (SessionState::AwaitingConnected, "connected") => {
                        self.state = SessionState::AwaitingAuthenticated;
                        true
                    }
                    (SessionState::AwaitingAuthenticated, "authenticated") => {
                        self.state = SessionState::AwaitingSubscription;
                        true
                    }
                    _ => false,
                };
                if !valid {
                    return quarantine(
                        evidence,
                        QuarantineReason::ProtocolInvariantViolation,
                        None,
                    );
                }
                DecodeOutcome::Control(DecodedControlFrame::new(
                    evidence,
                    ControlFrameKind::ProviderFlowControl,
                    SourceIdentifier::try_from(wire.message).ok(),
                ))
            }
            "subscription" => {
                if self.state != SessionState::AwaitingSubscription {
                    return quarantine(
                        evidence,
                        QuarantineReason::ProtocolInvariantViolation,
                        None,
                    );
                }
                let wire = match serde_json::from_value::<SubscriptionWire>(message) {
                    Ok(wire) => wire,
                    Err(_) => {
                        return quarantine(evidence, QuarantineReason::SchemaViolation, None);
                    }
                };
                if !exact_symbols(&wire.trades, &self.expected_symbols)
                    || !exact_symbols(&wire.quotes, &self.expected_symbols)
                    || (self.surface == DecoderSurface::Iex
                        && !exact_symbols(&wire.statuses, &self.expected_symbols))
                    || (self.surface == DecoderSurface::IndicativeOptions
                        && !wire.statuses.is_empty())
                {
                    return quarantine(evidence, QuarantineReason::WrongProduct, None);
                }
                self.state = SessionState::Active;
                DecodeOutcome::Control(DecodedControlFrame::new(
                    evidence,
                    ControlFrameKind::SubscriptionAcknowledgement,
                    None,
                ))
            }
            "error" => {
                let wire = match serde_json::from_value::<ErrorWire>(message) {
                    Ok(wire) => wire,
                    Err(_) => {
                        return quarantine(evidence, QuarantineReason::SchemaViolation, None);
                    }
                };
                let code = SourceIdentifier::try_from(format!("alpaca-error-{}", wire.code)).ok();
                match wire.code {
                    405..=407 | 429 => DecodeOutcome::Resynchronize(DecodedRecoveryAction::new(
                        evidence,
                        ResynchronizationReason::ProviderRequestedReset,
                        code,
                    )),
                    412 | 413 => quarantine(evidence, QuarantineReason::WrongChannel, code),
                    _ => quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, code),
                }
            }
            _ => quarantine(evidence, QuarantineReason::SchemaViolation, None),
        }
    }

    fn decode_data(
        &self,
        messages: Vec<Value>,
        evidence: DecoderEvidence,
    ) -> Result<DecodeOutcome, DecodeInternalError> {
        let mut observations = Vec::new();
        observations
            .try_reserve_exact(messages.len())
            .map_err(|_| DecodeInternalError::Allocation)?;
        let mut ignored_code = None;
        for message in messages {
            let kind = match message_kind(&message) {
                Ok(kind) => kind,
                Err(reason) => return Ok(quarantine(evidence, reason, None)),
            };
            let input = match kind.as_str() {
                "t" => self.decode_trade(message),
                "q" => self.decode_quote(message),
                "s" if self.surface == DecoderSurface::Iex => self.decode_status(message),
                other => {
                    ignored_code = SourceIdentifier::try_from(other).ok();
                    Ok(None)
                }
            };
            let input = match input {
                Ok(input) => input,
                Err(reason) => return Ok(quarantine(evidence, reason, None)),
            };
            if let Some(input) = input {
                observations.push(
                    self.observation(input)
                        .map_err(|_| DecodeInternalError::InvariantViolation)?,
                );
            }
        }
        if observations.is_empty() {
            return Ok(DecodeOutcome::Ignored(DecodedIgnoredFrame::new(
                evidence,
                IgnoredFrameReason::DocumentedForwardCompatibleExtension,
                ignored_code,
            )));
        }
        if ignored_code.is_some() {
            return Ok(quarantine(
                evidence,
                QuarantineReason::UnsupportedSemanticChange,
                ignored_code,
            ));
        }
        let batch = DecodedProviderBatch::try_new(evidence, observations)
            .map_err(|_| DecodeInternalError::InvariantViolation)?;
        Ok(DecodeOutcome::Data(batch))
    }

    fn decode_trade(&self, message: Value) -> Result<Option<ObservationInput>, QuarantineReason> {
        let wire: TradeWire =
            serde_json::from_value(message).map_err(|_| QuarantineReason::SchemaViolation)?;
        let instrument = self.instrument(&wire.symbol)?;
        let timestamp = parse_timestamp(&wire.timestamp)?;
        let price = parse_price(wire.price)?;
        let quantity = parse_quantity(wire.size, false)?;
        let trade_identity = match wire.trade_id {
            Some(id) if id.as_u64().is_some() => id.to_string(),
            Some(_) => return Err(QuarantineReason::SchemaViolation),
            None => timestamp.unix_nanos().to_string(),
        };
        let source_identifier = format!(
            "alpaca:{}:trade:{}:{}:{}",
            surface_name(self.surface),
            wire.symbol,
            wire.exchange,
            trade_identity
        );
        Ok(Some(ObservationInput {
            source_identifier,
            instrument,
            timestamp,
            payload: ProviderObservationPayload::Trade {
                trade_id: SourceIdentifier::try_from(trade_identity)
                    .map_err(|_| QuarantineReason::SchemaViolation)?,
                price,
                quantity,
                aggressor: ProviderAggressorEvidence::new(
                    AggressorSide::Unknown,
                    None,
                    self.aggressor_rule.clone(),
                ),
            },
        }))
    }

    fn decode_quote(&self, message: Value) -> Result<Option<ObservationInput>, QuarantineReason> {
        let wire: QuoteWire =
            serde_json::from_value(message).map_err(|_| QuarantineReason::SchemaViolation)?;
        let instrument = self.instrument(&wire.symbol)?;
        let timestamp = parse_timestamp(&wire.timestamp)?;
        let bid = parse_quote_side(wire.bid_price, wire.bid_size)?;
        let ask = parse_quote_side(wire.ask_price, wire.ask_size)?;
        let payload = ProviderObservationPayload::quote(bid, ask)
            .map_err(|_| QuarantineReason::SchemaViolation)?;
        Ok(Some(ObservationInput {
            source_identifier: format!(
                "alpaca:{}:quote:{}:{}:{}:{}",
                surface_name(self.surface),
                wire.symbol,
                wire.bid_exchange,
                wire.ask_exchange,
                timestamp.unix_nanos()
            ),
            instrument,
            timestamp,
            payload,
        }))
    }

    fn decode_status(&self, message: Value) -> Result<Option<ObservationInput>, QuarantineReason> {
        let wire: StatusWire =
            serde_json::from_value(message).map_err(|_| QuarantineReason::SchemaViolation)?;
        let transition = match wire.status_code.as_str() {
            "2" | "H" | "P" => HaltTransition::Halted,
            "3" | "Q" | "T" => HaltTransition::Resumed,
            _ => return Ok(None),
        };
        let instrument = self.instrument(&wire.symbol)?;
        let timestamp = parse_timestamp(&wire.timestamp)?;
        let reason = if wire.reason_code.is_empty() {
            wire.status_code.clone()
        } else {
            wire.reason_code
        };
        Ok(Some(ObservationInput {
            source_identifier: format!(
                "alpaca:iex:status:{}:{}:{}",
                wire.symbol,
                wire.status_code,
                timestamp.unix_nanos()
            ),
            instrument,
            timestamp,
            payload: ProviderObservationPayload::TradingHalt {
                status: ProviderStatusEvidence::new(
                    SourceIdentifier::try_from(wire.status_code)
                        .map_err(|_| QuarantineReason::SchemaViolation)?,
                    self.status_rule.clone(),
                ),
                transition,
                reason: SourceIdentifier::try_from(reason)
                    .map_err(|_| QuarantineReason::SchemaViolation)?,
            },
        }))
    }

    fn observation(
        &self,
        input: ObservationInput,
    ) -> Result<ProviderNormalizedObservation, market_squawk_sources::DecodeError> {
        ProviderNormalizedObservation::try_new(
            SourceIdentifier::try_from(input.source_identifier)
                .map_err(|_| market_squawk_sources::DecodeError::InvalidProviderEvidence)?,
            self.venue.clone(),
            input.instrument,
            ProviderTimestampEvidence::Provided {
                value: input.timestamp,
                rule: self.timestamp_rule.clone(),
            },
            ProviderSequenceEvidence::Unsupported {
                rule: self.sequence_rule.clone(),
            },
            ProviderSnapshotEvidence::NotApplicable(self.snapshot_rule.clone()),
            ProviderChecksumEvidence::Unsupported {
                rule: self.checksum_rule.clone(),
            },
            input.payload,
        )
    }

    fn instrument(&self, symbol: &str) -> Result<InstrumentId, QuarantineReason> {
        self.instruments
            .get(symbol)
            .copied()
            .ok_or(QuarantineReason::WrongProduct)
    }
}

struct ObservationInput {
    source_identifier: String,
    instrument: InstrumentId,
    timestamp: Timestamp,
    payload: ProviderObservationPayload,
}

#[derive(Deserialize)]
struct ProbeWire {
    #[serde(rename = "T")]
    kind: String,
}

#[derive(Deserialize)]
struct SuccessWire {
    #[serde(rename = "T")]
    _kind: String,
    #[serde(rename = "msg")]
    message: String,
}

#[derive(Deserialize)]
struct ErrorWire {
    #[serde(rename = "T")]
    _kind: String,
    code: u16,
    #[serde(rename = "msg")]
    _message: String,
}

#[derive(Deserialize)]
struct SubscriptionWire {
    #[serde(rename = "T")]
    _kind: String,
    #[serde(default)]
    trades: Vec<String>,
    #[serde(default)]
    quotes: Vec<String>,
    #[serde(default)]
    statuses: Vec<String>,
}

#[derive(Deserialize)]
struct TradeWire {
    #[serde(rename = "T")]
    _kind: String,
    #[serde(rename = "S")]
    symbol: String,
    #[serde(rename = "i")]
    trade_id: Option<Number>,
    #[serde(rename = "x")]
    exchange: String,
    #[serde(rename = "p")]
    price: Number,
    #[serde(rename = "s")]
    size: Number,
    #[serde(rename = "t")]
    timestamp: String,
}

#[derive(Deserialize)]
struct QuoteWire {
    #[serde(rename = "T")]
    _kind: String,
    #[serde(rename = "S")]
    symbol: String,
    #[serde(rename = "bx")]
    bid_exchange: String,
    #[serde(rename = "bp")]
    bid_price: Number,
    #[serde(rename = "bs")]
    bid_size: Number,
    #[serde(rename = "ax")]
    ask_exchange: String,
    #[serde(rename = "ap")]
    ask_price: Number,
    #[serde(rename = "as")]
    ask_size: Number,
    #[serde(rename = "t")]
    timestamp: String,
}

#[derive(Deserialize)]
struct StatusWire {
    #[serde(rename = "T")]
    _kind: String,
    #[serde(rename = "S")]
    symbol: String,
    #[serde(rename = "sc")]
    status_code: String,
    #[serde(rename = "rc", default)]
    reason_code: String,
    #[serde(rename = "t")]
    timestamp: String,
}

fn message_kind(value: &Value) -> Result<String, QuarantineReason> {
    serde_json::from_value::<ProbeWire>(value.clone())
        .map(|wire| wire.kind)
        .map_err(|_| QuarantineReason::SchemaViolation)
}

fn exact_symbols(actual: &[String], expected: &BTreeSet<String>) -> bool {
    actual.len() == expected.len()
        && actual.iter().collect::<BTreeSet<_>>().len() == actual.len()
        && actual.iter().all(|symbol| expected.contains(symbol))
}

fn parse_timestamp(value: &str) -> Result<Timestamp, QuarantineReason> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|timestamp| timestamp.timestamp_nanos_opt())
        .map(Timestamp::from_unix_nanos)
        .ok_or(QuarantineReason::InvalidTimestamp)
}

fn parse_price(value: Number) -> Result<ProviderPrice, QuarantineReason> {
    let lexeme = parse_decimal(value)?;
    if lexeme.decimal().is_zero() || lexeme.decimal().is_sign_negative() {
        return Err(QuarantineReason::InexactNumericValue);
    }
    Ok(ProviderPrice::new(lexeme))
}

fn parse_quantity(value: Number, allow_zero: bool) -> Result<ProviderQuantity, QuarantineReason> {
    let lexeme = parse_decimal(value)?;
    if lexeme.decimal().is_sign_negative() || (!allow_zero && lexeme.decimal().is_zero()) {
        return Err(QuarantineReason::NegativeQuantity);
    }
    Ok(ProviderQuantity::new(lexeme))
}

fn parse_quote_side(
    price: Number,
    size: Number,
) -> Result<Option<ProviderBookLevel>, QuarantineReason> {
    let price = parse_decimal(price)?;
    let size = parse_decimal(size)?;
    if price.decimal().is_sign_negative() || size.decimal().is_sign_negative() {
        return Err(QuarantineReason::NegativeQuantity);
    }
    let price_zero = price.decimal().is_zero();
    let size_zero = size.decimal().is_zero();
    if price_zero && size_zero {
        return Ok(None);
    }
    if price_zero || size_zero {
        return Err(QuarantineReason::SchemaViolation);
    }
    Ok(Some(ProviderBookLevel::new(
        ProviderPrice::new(price),
        ProviderQuantity::new(size),
    )))
}

fn parse_decimal(value: Number) -> Result<ProviderDecimalLexeme, QuarantineReason> {
    ProviderDecimalLexeme::try_new(&value.to_string())
        .map_err(|_| QuarantineReason::InexactNumericValue)
}

const fn surface_name(surface: DecoderSurface) -> &'static str {
    match surface {
        DecoderSurface::Iex => "iex",
        DecoderSurface::IndicativeOptions => "indicative-options",
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
