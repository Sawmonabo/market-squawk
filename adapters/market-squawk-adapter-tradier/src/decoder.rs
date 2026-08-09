use std::collections::BTreeMap;

use market_squawk_domain::{
    AggressorSide, InstrumentId, IntegrityRule, SourceIdentifier, Timestamp, VenueId,
};
use market_squawk_sources::{
    DecodeInternalError, DecodeOutcome, DecodedIgnoredFrame, DecodedProviderBatch,
    DecodedQuarantineAction, DecodedRecoveryAction, DecoderEvidence, IgnoredFrameReason,
    MarketDecoder, ProviderAggressorEvidence, ProviderBookLevel, ProviderChecksumEvidence,
    ProviderDecimalLexeme, ProviderNormalizedObservation, ProviderObservationPayload,
    ProviderPrice, ProviderQuantity, ProviderSequenceEvidence, ProviderSnapshotEvidence,
    ProviderTimestampEvidence, QuarantineReason, ResynchronizationReason, SourceMetadata,
    SourceMetadataProvider, SourceProtocolProfile, TransportFrameKind, ValidatedRawMarketFrame,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::config::{
    TRADIER_CONSOLIDATED_VENUE, TradierAccessSurface, TradierConfigError, TradierInstrumentKind,
    TradierSourceConfig,
};

const MAX_PROVIDER_ERROR_BYTES: usize = 1_024;

/// Exact decoder for one-object Tradier quote and `tradex` WebSocket frames.
#[derive(Clone, Debug)]
pub struct TradierMarketDecoder {
    metadata: SourceMetadata,
    mappings: BTreeMap<String, Mapping>,
    venue: VenueId,
    decoder_rule: IntegrityRule,
    timestamp_rule: IntegrityRule,
    sequence_rule: IntegrityRule,
    checksum_rule: IntegrityRule,
    aggressor_rule: IntegrityRule,
    nonbook_rule: IntegrityRule,
    max_frame_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
struct Mapping {
    instrument: InstrumentId,
    kind: TradierInstrumentKind,
}

impl TradierMarketDecoder {
    /// Constructs a decoder for a consolidated-securities source profile.
    ///
    /// # Errors
    ///
    /// Rejects a derived-index REST profile or metadata that no longer exposes the exact
    /// unsupported-sequence/checksum contract built by [`TradierSourceConfig`].
    pub fn try_new(config: &TradierSourceConfig) -> Result<Self, TradierConfigError> {
        if !config.profile().supports_streaming() {
            return Err(TradierConfigError::MixedLogicalProfile);
        }
        if config.access_surface() != TradierAccessSurface::Streaming {
            return Err(TradierConfigError::InvalidAccessSurface);
        }
        let live = match config.metadata().protocol_profile() {
            SourceProtocolProfile::Live(profile) => profile,
            SourceProtocolProfile::NotLive => return Err(TradierConfigError::InvalidRule),
        };
        let sequence_rule = match live.sequence() {
            market_squawk_sources::SequenceValidationProfile::Unsupported { rule } => rule.clone(),
            market_squawk_sources::SequenceValidationProfile::Provided { .. } => {
                return Err(TradierConfigError::InvalidRule);
            }
        };
        let checksum_rule = match live.checksum() {
            market_squawk_sources::ChecksumValidationProfile::Unsupported { rule } => rule.clone(),
            market_squawk_sources::ChecksumValidationProfile::Provided { .. } => {
                return Err(TradierConfigError::InvalidRule);
            }
        };
        let nonbook_rule = config
            .metadata()
            .coverage()
            .live()
            .and_then(|coverage| {
                coverage.rule_for(market_squawk_domain::LiveEventClass::Quote, None)
            })
            .and_then(|rule| match rule.snapshot_applicability() {
                market_squawk_domain::SnapshotApplicability::NotApplicable { metadata_rule } => {
                    Some(metadata_rule.clone())
                }
                market_squawk_domain::SnapshotApplicability::Required => None,
            })
            .ok_or(TradierConfigError::InvalidRule)?;
        let mut mappings = BTreeMap::new();
        for mapping in config.mappings() {
            let _previous = mappings.insert(
                mapping.symbol().as_str().to_owned(),
                Mapping {
                    instrument: mapping.instrument(),
                    kind: mapping.kind(),
                },
            );
        }
        Ok(Self {
            metadata: config.metadata().clone(),
            mappings,
            venue: VenueId::try_from(TRADIER_CONSOLIDATED_VENUE)?,
            decoder_rule: live.decoder_rule().clone(),
            timestamp_rule: live.timestamp_rule().clone(),
            sequence_rule,
            checksum_rule,
            aggressor_rule: live.semantic_interpretation().aggressor_rule().clone(),
            nonbook_rule,
            max_frame_bytes: config.transport_limits().max_frame_bytes(),
        })
    }

    fn decode_text(
        &self,
        frame: &ValidatedRawMarketFrame<'_>,
        evidence: DecoderEvidence,
    ) -> DecodeOutcome {
        let payload = frame.frame().payload();
        if payload.len() > self.max_frame_bytes {
            return quarantine(evidence, QuarantineReason::SchemaViolation, None);
        }
        let probe = match serde_json::from_slice::<EventProbe>(payload) {
            Ok(probe) => probe,
            Err(error) => {
                return quarantine(
                    evidence,
                    if error.is_syntax() || error.is_eof() {
                        QuarantineReason::MalformedPayload
                    } else {
                        QuarantineReason::SchemaViolation
                    },
                    None,
                );
            }
        };
        if let Some(error) = probe.error {
            if error.is_empty() || error.len() > MAX_PROVIDER_ERROR_BYTES {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
            return DecodeOutcome::Resynchronize(DecodedRecoveryAction::new(
                evidence,
                ResynchronizationReason::ProviderRequestedReset,
                source_code("tradier-provider-error"),
            ));
        }
        match probe.kind.as_deref() {
            Some("quote") => self.decode_quote(payload, evidence),
            Some("tradex") => self.decode_tradex(payload, evidence),
            Some(kind) => match SourceIdentifier::try_from(kind) {
                Ok(code) => DecodeOutcome::Ignored(DecodedIgnoredFrame::new(
                    evidence,
                    IgnoredFrameReason::DocumentedForwardCompatibleExtension,
                    Some(code),
                )),
                Err(_) => quarantine(evidence, QuarantineReason::SchemaViolation, None),
            },
            None => quarantine(evidence, QuarantineReason::SchemaViolation, None),
        }
    }

    fn decode_quote(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<QuoteWire>(payload) {
            Ok(wire) if wire.kind == "quote" => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        let mapping = match self.mappings.get(&wire.symbol) {
            Some(mapping) => *mapping,
            None => {
                return quarantine(
                    evidence,
                    QuarantineReason::WrongProduct,
                    source_code(&wire.symbol),
                );
            }
        };
        if SourceIdentifier::try_from(wire.bidexch.as_str()).is_err()
            || SourceIdentifier::try_from(wire.askexch.as_str()).is_err()
        {
            return quarantine(evidence, QuarantineReason::SchemaViolation, None);
        }
        let bid = match quote_side(
            wire.bid,
            wire.bidsz,
            mapping.kind.quote_quantity_multiplier(),
        ) {
            Ok(side) => side,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let ask = match quote_side(
            wire.ask,
            wire.asksz,
            mapping.kind.quote_quantity_multiplier(),
        ) {
            Ok(side) => side,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let event_at =
            match quote_timestamp(bid.is_some(), wire.biddate, ask.is_some(), wire.askdate) {
                Ok(timestamp) => timestamp,
                Err(reason) => return quarantine(evidence, reason, None),
            };
        let provider_payload = match ProviderObservationPayload::quote(bid, ask) {
            Ok(payload) => payload,
            Err(_) => return quarantine(evidence, QuarantineReason::SchemaViolation, None),
        };
        self.data(
            evidence,
            mapping.instrument,
            event_at,
            event_identifier("quote", payload),
            provider_payload,
        )
    }

    fn decode_tradex(&self, payload: &[u8], evidence: DecoderEvidence) -> DecodeOutcome {
        let wire = match serde_json::from_slice::<TradexWire>(payload) {
            Ok(wire) if wire.kind == "tradex" => wire,
            Ok(_) | Err(_) => {
                return quarantine(evidence, QuarantineReason::SchemaViolation, None);
            }
        };
        let mapping = match self.mappings.get(&wire.symbol) {
            Some(mapping) => *mapping,
            None => {
                return quarantine(
                    evidence,
                    QuarantineReason::WrongProduct,
                    source_code(&wire.symbol),
                );
            }
        };
        if SourceIdentifier::try_from(wire.exch.as_str()).is_err() {
            return quarantine(evidence, QuarantineReason::SchemaViolation, None);
        }
        let price = match positive_price(wire.price) {
            Ok(price) => price,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let last = match positive_price(wire.last) {
            Ok(price) => price,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        if price.value().decimal() != last.value().decimal() {
            return quarantine(evidence, QuarantineReason::ProtocolInvariantViolation, None);
        }
        let quantity = match positive_quantity(wire.size, 1) {
            Ok(quantity) => quantity,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        if nonnegative_decimal(wire.cvol).is_err() {
            return quarantine(evidence, QuarantineReason::InexactNumericValue, None);
        }
        let event_at = match epoch_millis(wire.date) {
            Ok(timestamp) => timestamp,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let identifier = match event_identifier("tradex", payload) {
            Ok(identifier) => identifier,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let trade_id = identifier.clone();
        self.data(
            evidence,
            mapping.instrument,
            event_at,
            Ok(identifier),
            ProviderObservationPayload::Trade {
                trade_id,
                price,
                quantity,
                aggressor: ProviderAggressorEvidence::new(
                    AggressorSide::Unknown,
                    None,
                    self.aggressor_rule.clone(),
                ),
            },
        )
    }

    fn data(
        &self,
        evidence: DecoderEvidence,
        instrument: InstrumentId,
        event_at: Timestamp,
        source_identifier: Result<SourceIdentifier, QuarantineReason>,
        payload: ProviderObservationPayload,
    ) -> DecodeOutcome {
        let source_identifier = match source_identifier {
            Ok(identifier) => identifier,
            Err(reason) => return quarantine(evidence, reason, None),
        };
        let observation = match ProviderNormalizedObservation::try_new(
            source_identifier,
            self.venue.clone(),
            instrument,
            ProviderTimestampEvidence::Provided {
                value: event_at,
                rule: self.timestamp_rule.clone(),
            },
            ProviderSequenceEvidence::Unsupported {
                rule: self.sequence_rule.clone(),
            },
            ProviderSnapshotEvidence::NotApplicable(self.nonbook_rule.clone()),
            ProviderChecksumEvidence::Unsupported {
                rule: self.checksum_rule.clone(),
            },
            payload,
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

impl SourceMetadataProvider for TradierMarketDecoder {
    fn metadata(&self) -> &SourceMetadata {
        &self.metadata
    }
}

impl MarketDecoder for TradierMarketDecoder {
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
        Ok(self.decode_text(frame, evidence))
    }
}

#[derive(Deserialize)]
struct EventProbe {
    #[serde(rename = "type")]
    kind: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QuoteWire {
    #[serde(rename = "type")]
    kind: String,
    symbol: String,
    bid: Option<ExactScalar>,
    bidsz: Option<ExactScalar>,
    bidexch: String,
    biddate: Option<ExactScalar>,
    ask: Option<ExactScalar>,
    asksz: Option<ExactScalar>,
    askexch: String,
    askdate: Option<ExactScalar>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TradexWire {
    #[serde(rename = "type")]
    kind: String,
    symbol: String,
    exch: String,
    price: ExactScalar,
    size: ExactScalar,
    cvol: ExactScalar,
    date: ExactScalar,
    last: ExactScalar,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExactScalar {
    Text(String),
    Number(serde_json::Number),
}

impl ExactScalar {
    fn into_string(self) -> String {
        match self {
            Self::Text(value) => value,
            Self::Number(value) => value.to_string(),
        }
    }
}

fn quote_side(
    price: Option<ExactScalar>,
    quantity: Option<ExactScalar>,
    multiplier: u32,
) -> Result<Option<ProviderBookLevel>, QuarantineReason> {
    match (price, quantity) {
        (None, None) => Ok(None),
        (Some(price), Some(quantity)) => {
            let price = decimal_lexeme(price)?;
            let quantity = decimal_lexeme(quantity)?;
            if price.decimal().is_zero() && quantity.decimal().is_zero() {
                return Ok(None);
            }
            if price.decimal().is_sign_negative()
                || price.decimal().is_zero()
                || quantity.decimal().is_sign_negative()
                || quantity.decimal().is_zero()
            {
                return Err(QuarantineReason::InexactNumericValue);
            }
            let quantity = scaled_quantity(quantity, multiplier)?;
            Ok(Some(ProviderBookLevel::new(
                ProviderPrice::new(price),
                quantity,
            )))
        }
        (None, Some(_)) | (Some(_), None) => Err(QuarantineReason::SchemaViolation),
    }
}

fn quote_timestamp(
    has_bid: bool,
    bid: Option<ExactScalar>,
    has_ask: bool,
    ask: Option<ExactScalar>,
) -> Result<Timestamp, QuarantineReason> {
    let bid = match (has_bid, bid) {
        (true, Some(value)) => Some(epoch_millis(value)?),
        (false, _) => None,
        (true, None) => return Err(QuarantineReason::InvalidTimestamp),
    };
    let ask = match (has_ask, ask) {
        (true, Some(value)) => Some(epoch_millis(value)?),
        (false, _) => None,
        (true, None) => return Err(QuarantineReason::InvalidTimestamp),
    };
    match (bid, ask) {
        (Some(bid), Some(ask)) => Ok(bid.min(ask)),
        (Some(timestamp), None) | (None, Some(timestamp)) => Ok(timestamp),
        (None, None) => Err(QuarantineReason::InvalidTimestamp),
    }
}

fn positive_price(value: ExactScalar) -> Result<ProviderPrice, QuarantineReason> {
    let lexeme = decimal_lexeme(value)?;
    if lexeme.decimal().is_sign_negative() || lexeme.decimal().is_zero() {
        return Err(QuarantineReason::InexactNumericValue);
    }
    Ok(ProviderPrice::new(lexeme))
}

fn positive_quantity(
    value: ExactScalar,
    multiplier: u32,
) -> Result<ProviderQuantity, QuarantineReason> {
    let lexeme = decimal_lexeme(value)?;
    if lexeme.decimal().is_sign_negative() || lexeme.decimal().is_zero() {
        return Err(QuarantineReason::NegativeQuantity);
    }
    scaled_quantity(lexeme, multiplier)
}

fn scaled_quantity(
    value: ProviderDecimalLexeme,
    multiplier: u32,
) -> Result<ProviderQuantity, QuarantineReason> {
    let scaled = value
        .decimal()
        .checked_mul(Decimal::from(multiplier))
        .ok_or(QuarantineReason::InexactNumericValue)?;
    let normalized = scaled.normalize().to_string();
    ProviderDecimalLexeme::try_new(&normalized)
        .map(ProviderQuantity::new)
        .map_err(|_| QuarantineReason::InexactNumericValue)
}

fn nonnegative_decimal(value: ExactScalar) -> Result<ProviderDecimalLexeme, QuarantineReason> {
    let value = decimal_lexeme(value)?;
    if value.decimal().is_sign_negative() {
        Err(QuarantineReason::NegativeQuantity)
    } else {
        Ok(value)
    }
}

fn decimal_lexeme(value: ExactScalar) -> Result<ProviderDecimalLexeme, QuarantineReason> {
    ProviderDecimalLexeme::try_new(&value.into_string())
        .map_err(|_| QuarantineReason::InexactNumericValue)
}

fn epoch_millis(value: ExactScalar) -> Result<Timestamp, QuarantineReason> {
    let value = value.into_string();
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(QuarantineReason::InvalidTimestamp);
    }
    let millis = value
        .parse::<i64>()
        .map_err(|_| QuarantineReason::InvalidTimestamp)?;
    millis
        .checked_mul(1_000_000)
        .map(Timestamp::from_unix_nanos)
        .ok_or(QuarantineReason::InvalidTimestamp)
}

fn event_identifier(prefix: &str, payload: &[u8]) -> Result<SourceIdentifier, QuarantineReason> {
    let digest: [u8; 32] = Sha256::digest(payload).into();
    let mut identity = String::with_capacity(prefix.len() + 73);
    identity.push_str("tradier-");
    identity.push_str(prefix);
    identity.push('-');
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        identity.push(char::from(HEX[usize::from(byte >> 4)]));
        identity.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SourceIdentifier::try_from(identity).map_err(|_| QuarantineReason::ProtocolInvariantViolation)
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

#[cfg(test)]
pub(crate) fn decode_wire_for_test(
    payload: &[u8],
    kind: TradierInstrumentKind,
) -> Result<(Timestamp, Decimal, Decimal), QuarantineReason> {
    let probe = serde_json::from_slice::<EventProbe>(payload)
        .map_err(|_| QuarantineReason::MalformedPayload)?;
    match probe.kind.as_deref() {
        Some("quote") => {
            let wire = serde_json::from_slice::<QuoteWire>(payload)
                .map_err(|_| QuarantineReason::SchemaViolation)?;
            let bid = quote_side(wire.bid, wire.bidsz, kind.quote_quantity_multiplier())?
                .ok_or(QuarantineReason::SchemaViolation)?;
            let ask = quote_side(wire.ask, wire.asksz, kind.quote_quantity_multiplier())?
                .ok_or(QuarantineReason::SchemaViolation)?;
            let timestamp = quote_timestamp(true, wire.biddate, true, wire.askdate)?;
            Ok((
                timestamp,
                bid.quantity().value().decimal(),
                ask.quantity().value().decimal(),
            ))
        }
        Some("tradex") => {
            let wire = serde_json::from_slice::<TradexWire>(payload)
                .map_err(|_| QuarantineReason::SchemaViolation)?;
            let timestamp = epoch_millis(wire.date)?;
            let price = positive_price(wire.price)?;
            let quantity = positive_quantity(wire.size, 1)?;
            Ok((
                timestamp,
                price.value().decimal(),
                quantity.value().decimal(),
            ))
        }
        _ => Err(QuarantineReason::UnsupportedSemanticChange),
    }
}
