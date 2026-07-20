use std::collections::{BTreeMap, BTreeSet};

use chrono::DateTime;
use market_squawk_domain::{
    AggressorSide, InstrumentId, IntegrityRule, MarketDepth, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    ControlFrameKind, DecodeInternalError, DecodeOutcome, DecodedControlFrame, DecodedIgnoredFrame,
    DecodedProviderBatch, DecodedQuarantineAction, DecodedRecoveryAction, DecoderEvidence,
    IgnoredFrameReason, MarketDecoder, ProviderAggressorEvidence, ProviderBookChange,
    ProviderBookLevel, ProviderBookSide, ProviderChecksumEvidence, ProviderDecimalLexeme,
    ProviderNormalizedObservation, ProviderObservationPayload, ProviderPrice, ProviderQuantity,
    ProviderSequenceEvidence, ProviderSnapshotEvidence, ProviderTimestampEvidence,
    QuarantineReason, ResynchronizationReason, SourceMetadata, SourceMetadataProvider,
    TransportFrameKind, ValidatedRawMarketFrame,
};
use serde::de::{Error as _, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use crate::{CoinbaseConfigError, CoinbaseExchangeConfig};

const MAX_LEVELS_PER_SIDE: usize = 10_000;
const MAX_CHANGES: usize = market_squawk_sources::MAX_DECODED_BOOK_ITEMS;
const MAX_ACK_CHANNELS: usize = 3;
const MAX_ACK_PRODUCTS: usize = 100;

/// Exact bounded decoder for Coinbase Exchange WebSocket v1.
#[derive(Clone, Debug)]
pub struct CoinbaseExchangeDecoder {
    metadata: SourceMetadata,
    instruments: BTreeMap<String, InstrumentId>,
    venue: VenueId,
    decoder_rule: IntegrityRule,
    timestamp_rule: IntegrityRule,
    sequence_rule: IntegrityRule,
    checksum_rule: IntegrityRule,
    aggressor_rule: IntegrityRule,
    trade_snapshot_rule: IntegrityRule,
    products: BTreeSet<String>,
    channels: BTreeSet<String>,
    max_frame_bytes: usize,
}

impl CoinbaseExchangeDecoder {
    /// Constructs a decoder from the immutable, validated source configuration.
    ///
    /// # Errors
    ///
    /// Returns [`CoinbaseConfigError::InvalidProtocolProfile`] if the supplied metadata no longer
    /// exposes the live rules guaranteed by [`CoinbaseExchangeConfig`].
    pub fn try_new(config: &CoinbaseExchangeConfig) -> Result<Self, CoinbaseConfigError> {
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
        Ok(Self {
            metadata: config.metadata().clone(),
            instruments: config
                .mappings()
                .iter()
                .map(|mapping| {
                    (
                        mapping.product().as_source_identifier().as_str().to_owned(),
                        mapping.instrument(),
                    )
                })
                .collect(),
            venue: VenueId::try_from("coinbase-exchange")?,
            decoder_rule: live.decoder_rule().clone(),
            timestamp_rule: live.timestamp_rule().clone(),
            sequence_rule,
            checksum_rule,
            aggressor_rule: live.semantic_interpretation().aggressor_rule().clone(),
            trade_snapshot_rule,
            products: config
                .mappings()
                .iter()
                .map(|mapping| mapping.product().as_source_identifier().as_str().to_owned())
                .collect(),
            channels: config
                .channels()
                .iter()
                .map(|channel| channel.as_str().to_owned())
                .collect(),
            max_frame_bytes: config.transport_limits().max_frame_bytes(),
        })
    }

    fn decode_text(
        &self,
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
        let provider_code = match SourceIdentifier::try_from(probe.kind.clone()) {
            Ok(code) => code,
            Err(_) => {
                return Ok(quarantine(
                    evidence,
                    QuarantineReason::SchemaViolation,
                    None,
                ));
            }
        };
        let outcome = match probe.kind.as_str() {
            "snapshot" => self.decode_snapshot(payload, evidence),
            "l2update" => self.decode_delta(payload, evidence),
            "match" | "last_match" => self.decode_trade(payload, evidence),
            "heartbeat" => self.decode_heartbeat(payload, evidence),
            "subscriptions" => self.decode_subscriptions(payload, evidence),
            "error" => self.decode_provider_error(payload, evidence),
            _ => DecodeOutcome::Ignored(DecodedIgnoredFrame::new(
                evidence,
                IgnoredFrameReason::DocumentedForwardCompatibleExtension,
                Some(provider_code),
            )),
        };
        Ok(outcome)
    }

    fn decode_snapshot(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<SnapshotWire>(payload) {
            Ok(wire) if wire.kind == "snapshot" => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        let instrument = match self.instrument(&wire.product_id) {
            Ok(instrument) => instrument,
            Err(reason) => return quarantine(evidence, reason, source_code(&wire.product_id)),
        };
        if wire.bids.0.is_empty() && wire.asks.0.is_empty() {
            return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
        }
        let bids = match wire
            .bids
            .0
            .into_iter()
            .map(|level| parse_level(level, false))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(levels) => levels,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let asks = match wire
            .asks
            .0
            .into_iter()
            .map(|level| parse_level(level, false))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(levels) => levels,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let provider_payload =
            match ProviderObservationPayload::book_snapshot(MarketDepth::PriceLevel, bids, asks) {
                Ok(value) => value,
                Err(_) => return quarantine(evidence, QuarantineReason::SchemaViolation, None),
            };
        self.data(
            evidence,
            observation_input(
                format!("snapshot:{}", wire.product_id),
                instrument,
                ProviderTimestampEvidence::AuthoritativelyAbsent(self.timestamp_rule.clone()),
                ProviderSnapshotEvidence::InitializingSnapshot {
                    provider_reference: None,
                },
                provider_payload,
            ),
        )
    }

    fn decode_delta(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<DeltaWire>(payload) {
            Ok(wire) if wire.kind == "l2update" => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        let instrument = match self.instrument(&wire.product_id) {
            Ok(instrument) => instrument,
            Err(reason) => return quarantine(evidence, reason, source_code(&wire.product_id)),
        };
        let timestamp = match parse_timestamp(&wire.time) {
            Ok(timestamp) => timestamp,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let changes = match wire
            .changes
            .0
            .into_iter()
            .map(parse_change)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(changes) => changes,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let provider_payload =
            match ProviderObservationPayload::book_delta(MarketDepth::PriceLevel, changes) {
                Ok(value) => value,
                Err(_) => {
                    return quarantine(
                        evidence,
                        QuarantineReason::ProtocolInvariantViolation,
                        None,
                    );
                }
            };
        self.data(
            evidence,
            observation_input(
                format!("l2update:{}", wire.product_id),
                instrument,
                ProviderTimestampEvidence::Provided {
                    value: timestamp,
                    rule: self.timestamp_rule.clone(),
                },
                ProviderSnapshotEvidence::Delta {
                    provider_snapshot_reference: None,
                },
                provider_payload,
            ),
        )
    }

    fn decode_trade(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<TradeWire>(payload) {
            Ok(wire) if matches!(wire.kind.as_str(), "match" | "last_match") => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        let instrument = match self.instrument(&wire.product_id) {
            Ok(instrument) => instrument,
            Err(reason) => return quarantine(evidence, reason, source_code(&wire.product_id)),
        };
        let timestamp = match parse_timestamp(&wire.time) {
            Ok(timestamp) => timestamp,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let _provider_sequence = wire.sequence;
        let _maker_order_id = &wire.maker_order_id;
        let _taker_order_id = &wire.taker_order_id;
        let price = match parse_price(wire.price) {
            Ok(price) => price,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let quantity = match parse_quantity(wire.size, false) {
            Ok(quantity) => quantity,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let (aggressor, maker_code) = match wire.side.as_str() {
            "sell" => (AggressorSide::Buy, "maker:sell"),
            "buy" => (AggressorSide::Sell, "maker:buy"),
            _ => return quarantine(evidence, QuarantineReason::UnsupportedSemanticChange, None),
        };
        let maker_code = match SourceIdentifier::try_from(maker_code) {
            Ok(code) => code,
            Err(_) => {
                return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
            }
        };
        let trade_id = wire.trade_id.to_string();
        let trade_identifier = match SourceIdentifier::try_from(trade_id.as_str()) {
            Ok(value) => value,
            Err(_) => {
                return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
            }
        };
        let provider_payload = ProviderObservationPayload::Trade {
            trade_id: trade_identifier,
            price,
            quantity,
            aggressor: ProviderAggressorEvidence::new(
                aggressor,
                Some(maker_code),
                self.aggressor_rule.clone(),
            ),
        };
        self.data(
            evidence,
            observation_input(
                format!("{}:{}", wire.kind, wire.trade_id),
                instrument,
                ProviderTimestampEvidence::Provided {
                    value: timestamp,
                    rule: self.timestamp_rule.clone(),
                },
                ProviderSnapshotEvidence::NotApplicable(self.trade_snapshot_rule.clone()),
                provider_payload,
            ),
        )
    }

    fn decode_heartbeat(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<HeartbeatWire>(payload) {
            Ok(wire) if wire.kind == "heartbeat" => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        if let Err(reason) = self.instrument(&wire.product_id) {
            return quarantine(evidence, reason, source_code(&wire.product_id));
        }
        if parse_timestamp(&wire.time).is_err() {
            return quarantine(evidence, QuarantineReason::InvalidTimestamp, None);
        }
        let _sequence = wire.sequence;
        let _last_trade_id = wire.last_trade_id;
        DecodeOutcome::Control(DecodedControlFrame::new(
            evidence,
            ControlFrameKind::Heartbeat,
            source_code(&wire.product_id),
        ))
    }

    fn decode_subscriptions(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<SubscriptionsWire>(payload) {
            Ok(wire) if wire.kind == "subscriptions" => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        let mut names = BTreeSet::new();
        for channel in wire.channels.0 {
            if !names.insert(channel.name.clone()) {
                return quarantine(
                    evidence,
                    QuarantineReason::WrongChannel,
                    source_code(&channel.name),
                );
            }
            let products = channel.product_ids.0.into_iter().collect::<BTreeSet<_>>();
            if products != self.products {
                return quarantine(
                    evidence,
                    QuarantineReason::WrongProduct,
                    source_code(&channel.name),
                );
            }
        }
        if names != self.channels {
            return quarantine(evidence, QuarantineReason::WrongChannel, None);
        }
        DecodeOutcome::Control(DecodedControlFrame::new(
            evidence,
            ControlFrameKind::SubscriptionAcknowledgement,
            None,
        ))
    }

    fn decode_provider_error(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<ProviderErrorWire>(payload) {
            Ok(wire) if wire.kind == "error" => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        let provider_code = source_code(&wire.message);
        DecodeOutcome::Resynchronize(DecodedRecoveryAction::new(
            evidence,
            ResynchronizationReason::ProviderRequestedReset,
            provider_code,
        ))
    }

    fn instrument(&self, product: &str) -> Result<InstrumentId, QuarantineReason> {
        self.instruments
            .get(product)
            .copied()
            .ok_or(QuarantineReason::WrongProduct)
    }

    fn data(&self, evidence: DecoderEvidence, input: ObservationInput) -> DecodeOutcome {
        let source_identifier = match SourceIdentifier::try_from(input.source_identifier) {
            Ok(identifier) => identifier,
            Err(_) => {
                return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
            }
        };
        let observation = match ProviderNormalizedObservation::try_new(
            source_identifier,
            self.venue.clone(),
            input.instrument,
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
                return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
            }
        };
        match DecodedProviderBatch::try_new(evidence.clone(), vec![observation]) {
            Ok(batch) => DecodeOutcome::Data(batch),
            Err(_) => quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None),
        }
    }
}

impl SourceMetadataProvider for CoinbaseExchangeDecoder {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl MarketDecoder for CoinbaseExchangeDecoder {
    fn decode(
        &mut self,
        frame: &ValidatedRawMarketFrame<'_>,
    ) -> Result<DecodeOutcome, DecodeInternalError> {
        let evidence = DecoderEvidence::from_validated_frame(frame, self.decoder_rule.clone());
        if frame.frame().transport() != TransportFrameKind::Text {
            return Ok(quarantine(
                evidence,
                QuarantineReason::SchemaViolation,
                None,
            ));
        }
        self.decode_text(frame, evidence)
    }
}

struct ObservationInput {
    source_identifier: String,
    instrument: InstrumentId,
    timestamp: ProviderTimestampEvidence,
    snapshot: ProviderSnapshotEvidence,
    payload: ProviderObservationPayload,
}

fn observation_input(
    source_identifier: String,
    instrument: InstrumentId,
    timestamp: ProviderTimestampEvidence,
    snapshot: ProviderSnapshotEvidence,
    payload: ProviderObservationPayload,
) -> ObservationInput {
    ObservationInput {
        source_identifier,
        instrument,
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

fn parse_level(
    level: [String; 2],
    allow_zero: bool,
) -> Result<ProviderBookLevel, QuarantineReason> {
    let [price, quantity] = level;
    Ok(ProviderBookLevel::new(
        parse_price(price)?,
        parse_quantity(quantity, allow_zero)?,
    ))
}

fn parse_change(change: [String; 3]) -> Result<ProviderBookChange, QuarantineReason> {
    let [side, price, quantity] = change;
    let side = match side.as_str() {
        "buy" => ProviderBookSide::Bid,
        "sell" => ProviderBookSide::Ask,
        _ => return Err(QuarantineReason::UnsupportedSemanticChange),
    };
    Ok(ProviderBookChange::new(
        side,
        ProviderBookLevel::new(parse_price(price)?, parse_quantity(quantity, true)?),
    ))
}

#[derive(Deserialize)]
struct MessageProbe {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotWire {
    #[serde(rename = "type")]
    kind: String,
    product_id: String,
    bids: BoundedSequence<[String; 2], MAX_LEVELS_PER_SIDE>,
    asks: BoundedSequence<[String; 2], MAX_LEVELS_PER_SIDE>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeltaWire {
    #[serde(rename = "type")]
    kind: String,
    product_id: String,
    time: String,
    changes: BoundedSequence<[String; 3], MAX_CHANGES>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TradeWire {
    #[serde(rename = "type")]
    kind: String,
    trade_id: u64,
    sequence: u64,
    maker_order_id: String,
    taker_order_id: String,
    time: String,
    product_id: String,
    size: String,
    price: String,
    side: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeartbeatWire {
    #[serde(rename = "type")]
    kind: String,
    sequence: u64,
    last_trade_id: u64,
    product_id: String,
    time: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionsWire {
    #[serde(rename = "type")]
    kind: String,
    channels: BoundedSequence<SubscriptionChannelWire, MAX_ACK_CHANNELS>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscriptionChannelWire {
    name: String,
    product_ids: BoundedSequence<String, MAX_ACK_PRODUCTS>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderErrorWire {
    #[serde(rename = "type")]
    kind: String,
    message: String,
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
