//! Source-native Level-One prices, explicit size presence, and original quote clock.
use super::{
    SchwabCanonicalError, SchwabCanonicalField, SchwabCanonicalStreamerRecord,
    SchwabQuoteAbstention, SchwabQuoteCanonicalOutcome, SchwabStreamerSemanticField,
    millis_to_timestamp, parse_decimal,
};
use crate::{MarketDataService, NativeScalar, StreamerNativeValue};
use market_squawk_domain::{
    LiveProvenance, MarketDataQuoteEvent, MarketDataQuoteSide, MarketDataQuoteSize,
    MarketDataReference, MarketEvent, Money, Timestamp,
};
use rust_decimal::Decimal;

/// Promotes the exact dictionary-resolved Level-One record without execution scales or size units.
pub fn canonicalize_streamer_quote_record(
    record: &SchwabCanonicalStreamerRecord,
    reference: MarketDataReference,
    provenance: LiveProvenance,
) -> Result<SchwabQuoteCanonicalOutcome, SchwabCanonicalError> {
    if !matches!(
        record.service,
        MarketDataService::LevelOneEquities
            | MarketDataService::LevelOneOptions
            | MarketDataService::LevelOneFutures
            | MarketDataService::LevelOneFuturesOptions
            | MarketDataService::LevelOneForex
    ) {
        return Err(SchwabCanonicalError::UnsupportedCanonicalFamily);
    }
    if record.provider_identifier.as_str()
        != reference
            .provider_identity()
            .ok_or(SchwabCanonicalError::IdentityMismatch)?
            .provider_instrument_id()
            .as_str()
        || provenance.source_timestamp() != streamer_quote_source_timestamp(record)?
    {
        return Err(SchwabCanonicalError::IdentityMismatch);
    }
    let provider_instrument_id = reference
        .provider_identity()
        .ok_or(SchwabCanonicalError::IdentityMismatch)?
        .provider_instrument_id()
        .clone();
    let resolution_evidence = reference
        .provider_identity()
        .ok_or(SchwabCanonicalError::IdentityMismatch)?
        .evidence()
        .content_digest();
    let side = |price, size| -> Result<_, SchwabCanonicalError> {
        let price = decimal_field(record, price)?;
        let size = decimal_field(record, size)?;
        let SchwabCanonicalField::Value(price) = price else {
            return Ok(None);
        };
        let size = match size {
            SchwabCanonicalField::Absent => MarketDataQuoteSize::Absent,
            SchwabCanonicalField::Null => MarketDataQuoteSize::Null,
            SchwabCanonicalField::Value(value) => MarketDataQuoteSize::UnresolvedUnit(value),
        };
        Ok(Some(MarketDataQuoteSide::new(
            Money::new(price, reference.currency()),
            size,
        )))
    };
    let bid = side(
        SchwabStreamerSemanticField::BidPrice,
        SchwabStreamerSemanticField::BidSize,
    )?;
    let ask = side(
        SchwabStreamerSemanticField::AskPrice,
        SchwabStreamerSemanticField::AskSize,
    )?;
    if bid.is_none() && ask.is_none() {
        return Ok(SchwabQuoteCanonicalOutcome::Abstained {
            provider_instrument_id,
            resolution_evidence,
            reason: SchwabQuoteAbstention::NoQuotedSide,
        });
    }
    let event = MarketDataQuoteEvent::try_new(provenance, reference, bid, ask)
        .map(MarketEvent::MarketDataQuote)
        .map_err(|_| SchwabCanonicalError::DomainInvariant)?;
    Ok(SchwabQuoteCanonicalOutcome::Mapped {
        provider_instrument_id,
        resolution_evidence,
        event: Box::new(event),
    })
}

/// Reads only the native dictionary QuoteTime field. Missing/null never borrows envelope time.
pub fn streamer_quote_source_timestamp(
    record: &SchwabCanonicalStreamerRecord,
) -> Result<Option<Timestamp>, SchwabCanonicalError> {
    if !matches!(
        record.service,
        MarketDataService::LevelOneEquities
            | MarketDataService::LevelOneOptions
            | MarketDataService::LevelOneFutures
            | MarketDataService::LevelOneFuturesOptions
            | MarketDataService::LevelOneForex
    ) {
        return Err(SchwabCanonicalError::UnsupportedCanonicalFamily);
    }
    match field(record, SchwabStreamerSemanticField::QuoteTime)? {
        None | Some(StreamerNativeValue::Scalar(NativeScalar::Null)) => Ok(None),
        Some(StreamerNativeValue::Scalar(NativeScalar::Number(number))) => {
            let millis = number
                .as_str()
                .parse::<u64>()
                .map_err(|_| SchwabCanonicalError::SemanticTypeMismatch)?;
            millis_to_timestamp(millis).map(Some)
        }
        Some(_) => Err(SchwabCanonicalError::SemanticTypeMismatch),
    }
}

fn field(
    record: &SchwabCanonicalStreamerRecord,
    meaning: SchwabStreamerSemanticField,
) -> Result<Option<&StreamerNativeValue>, SchwabCanonicalError> {
    let mut values = record
        .fields
        .iter()
        .filter(|field| field.meaning == meaning);
    let first = values.next().map(|field| &field.value);
    if values.next().is_some() {
        return Err(SchwabCanonicalError::DictionaryInvalid);
    }
    Ok(first)
}

fn decimal_field(
    record: &SchwabCanonicalStreamerRecord,
    meaning: SchwabStreamerSemanticField,
) -> Result<SchwabCanonicalField<Decimal>, SchwabCanonicalError> {
    match field(record, meaning)? {
        None => Ok(SchwabCanonicalField::Absent),
        Some(StreamerNativeValue::Scalar(NativeScalar::Null)) => Ok(SchwabCanonicalField::Null),
        Some(StreamerNativeValue::Scalar(NativeScalar::Number(number))) => {
            parse_decimal(number).map(SchwabCanonicalField::Value)
        }
        Some(_) => Err(SchwabCanonicalError::SemanticTypeMismatch),
    }
}
