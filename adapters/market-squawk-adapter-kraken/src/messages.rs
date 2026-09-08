//! Strict bounded Kraken WebSocket v2 wire parsing.

use std::fmt;

use market_squawk_sources::MAX_DECODED_EVENTS;
use serde::de::{Error as _, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use thiserror::Error;

const MAX_SUBSCRIPTION_WARNINGS: usize = 16;
const MAX_WARNING_JSON_BYTES: usize = 512;
pub(crate) const MAX_SUBSCRIPTION_ERROR_BYTES: usize = 512;
pub(crate) const PUBLIC_SUBSCRIPTION_REQUEST_ID: u64 = 1;

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
    pub(crate) data: &'a RawValue,
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
    #[serde(default, borrow, deserialize_with = "deserialize_present_warnings")]
    pub(crate) warnings: Option<&'a RawValue>,
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

pub(crate) fn validate_warnings(raw: Option<&RawValue>) -> Result<(), MessageError> {
    let Some(raw) = raw else {
        return Ok(());
    };
    serde_json::from_str::<BoundedWarnings>(raw.get())
        .map(|_warnings| ())
        .map_err(|_| MessageError::Malformed)
}

pub(crate) fn bounded_trade_count(raw: &RawValue) -> Result<usize, MessageError> {
    serde_json::from_str::<BoundedTradeCount>(raw.get())
        .map(|count| count.0)
        .map_err(|_| MessageError::Malformed)
}

struct BoundedTradeCount(usize);

impl<'de> Deserialize<'de> for BoundedTradeCount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedTradeCountVisitor)
    }
}

struct BoundedTradeCountVisitor;

impl<'de> Visitor<'de> for BoundedTradeCountVisitor {
    type Value = BoundedTradeCount;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded array of trade objects")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut count = 0_usize;
        while sequence.next_element::<IgnoredAny>()?.is_some() {
            count = count.saturating_add(1).min(MAX_DECODED_EVENTS + 1);
        }
        Ok(BoundedTradeCount(count))
    }
}

fn deserialize_present_warnings<'de, D>(deserializer: D) -> Result<Option<&'de RawValue>, D::Error>
where
    D: Deserializer<'de>,
{
    <&RawValue>::deserialize(deserializer).map(Some)
}

struct BoundedWarnings;

impl<'de> Deserialize<'de> for BoundedWarnings {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedWarningsVisitor)
    }
}

struct BoundedWarningsVisitor;

impl<'de> Visitor<'de> for BoundedWarningsVisitor {
    type Value = BoundedWarnings;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded array of subscription warning strings")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut count = 0_usize;
        while let Some(raw_warning) = sequence.next_element::<&RawValue>()? {
            count = count
                .checked_add(1)
                .ok_or_else(|| A::Error::custom("subscription warning count overflow"))?;
            if count > MAX_SUBSCRIPTION_WARNINGS {
                return Err(A::Error::custom("too many subscription warnings"));
            }
            if raw_warning.get().len() > MAX_WARNING_JSON_BYTES {
                return Err(A::Error::custom("subscription warning is too large"));
            }
            let _warning =
                serde_json::from_str::<String>(raw_warning.get()).map_err(A::Error::custom)?;
        }
        Ok(BoundedWarnings)
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
