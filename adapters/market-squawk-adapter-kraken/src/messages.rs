//! Strict bounded Kraken WebSocket v2 wire parsing.

use serde::Deserialize;
use serde_json::value::RawValue;
use thiserror::Error;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BookEnvelope<'a> {
    pub(crate) channel: &'a str,
    #[serde(rename = "type")]
    pub(crate) kind: &'a str,
    #[serde(borrow)]
    pub(crate) data: Vec<BookData<'a>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BookData<'a> {
    pub(crate) symbol: &'a str,
    #[serde(default, borrow)]
    pub(crate) bids: Vec<WireLevel<'a>>,
    #[serde(default, borrow)]
    pub(crate) asks: Vec<WireLevel<'a>>,
    #[serde(borrow)]
    pub(crate) checksum: &'a RawValue,
    pub(crate) timestamp: &'a str,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WireLevel<'a> {
    #[serde(borrow)]
    pub(crate) price: &'a RawValue,
    #[serde(borrow)]
    pub(crate) qty: &'a RawValue,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TradeEnvelope<'a> {
    pub(crate) channel: &'a str,
    #[serde(rename = "type")]
    pub(crate) kind: &'a str,
    #[serde(borrow)]
    pub(crate) data: Vec<TradeData<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TradeData<'a> {
    pub(crate) symbol: &'a str,
    pub(crate) side: &'a str,
    #[serde(borrow)]
    pub(crate) price: &'a RawValue,
    #[serde(borrow)]
    pub(crate) qty: &'a RawValue,
    pub(crate) ord_type: &'a str,
    pub(crate) trade_id: i64,
    pub(crate) timestamp: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Heartbeat<'a> {
    pub(crate) channel: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StatusEnvelope<'a> {
    pub(crate) channel: &'a str,
    #[serde(rename = "type")]
    pub(crate) kind: &'a str,
    #[serde(borrow)]
    pub(crate) data: Vec<StatusData<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StatusData<'a> {
    pub(crate) system: &'a str,
    pub(crate) api_version: &'a str,
    pub(crate) connection_id: u64,
    pub(crate) version: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SubscribeAck<'a> {
    pub(crate) method: &'a str,
    pub(crate) success: bool,
    pub(crate) result: Option<SubscribeResult<'a>>,
    pub(crate) error: Option<&'a str>,
    pub(crate) req_id: Option<u64>,
    pub(crate) time_in: &'a str,
    pub(crate) time_out: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SubscribeResult<'a> {
    pub(crate) channel: &'a str,
    pub(crate) depth: Option<usize>,
    pub(crate) snapshot: Option<bool>,
    pub(crate) symbol: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Pong<'a> {
    pub(crate) method: &'a str,
    pub(crate) req_id: Option<u64>,
    pub(crate) time_in: &'a str,
    pub(crate) time_out: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EnvelopeKind {
    Book,
    Trade,
    Heartbeat,
    Status,
    SubscribeAck,
    Pong,
}

pub(crate) fn classify(payload: &[u8]) -> Result<EnvelopeKind, MessageError> {
    #[derive(Deserialize)]
    struct Discriminator<'a> {
        channel: Option<&'a str>,
        method: Option<&'a str>,
    }
    let discriminator: Discriminator<'_> =
        serde_json::from_slice(payload).map_err(|_| MessageError::Malformed)?;
    match (discriminator.channel, discriminator.method) {
        (Some("book"), None) => Ok(EnvelopeKind::Book),
        (Some("trade"), None) => Ok(EnvelopeKind::Trade),
        (Some("heartbeat"), None) => Ok(EnvelopeKind::Heartbeat),
        (Some("status"), None) => Ok(EnvelopeKind::Status),
        (None, Some("subscribe")) => Ok(EnvelopeKind::SubscribeAck),
        (None, Some("pong")) => Ok(EnvelopeKind::Pong),
        _ => Err(MessageError::Unsupported),
    }
}

pub(crate) fn exact_decimal(raw: &RawValue) -> Result<&str, MessageError> {
    let value = raw.get();
    if value.starts_with('"') {
        if value.len() < 2 || !value.ends_with('"') || value[1..value.len() - 1].contains('\\') {
            return Err(MessageError::Malformed);
        }
        Ok(&value[1..value.len() - 1])
    } else {
        Ok(value)
    }
}

/// Strict wire parse error.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum MessageError {
    #[error("Kraken message is malformed")]
    Malformed,
    #[error("Kraken message family is unsupported")]
    Unsupported,
}
