//! Strict bounded wire shapes for the authenticated `level3` channel.

use std::fmt;

use serde::de::{Error as _, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum EnvelopeKind {
    Level3,
    Heartbeat,
    Status,
    SubscribeAck,
    Pong,
}

pub(super) fn classify(payload: &[u8]) -> Result<EnvelopeKind, WireError> {
    #[derive(Deserialize)]
    struct Discriminator<'a> {
        channel: Option<&'a str>,
        method: Option<&'a str>,
    }

    let discriminator: Discriminator<'_> =
        serde_json::from_slice(payload).map_err(|_| WireError::Malformed)?;
    match (discriminator.channel, discriminator.method) {
        (Some("level3"), None) => Ok(EnvelopeKind::Level3),
        (Some("heartbeat"), None) => Ok(EnvelopeKind::Heartbeat),
        (Some("status"), None) => Ok(EnvelopeKind::Status),
        (None, Some("subscribe")) => Ok(EnvelopeKind::SubscribeAck),
        (None, Some("pong")) => Ok(EnvelopeKind::Pong),
        _ => Err(WireError::Unsupported),
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Level3Envelope<'a> {
    pub(super) channel: &'a str,
    #[serde(rename = "type")]
    pub(super) kind: &'a str,
    #[serde(borrow)]
    pub(super) data: &'a RawValue,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotData<'a> {
    pub(super) symbol: &'a str,
    #[serde(borrow)]
    pub(super) bids: &'a RawValue,
    #[serde(borrow)]
    pub(super) asks: &'a RawValue,
    #[serde(borrow)]
    pub(super) checksum: &'a RawValue,
    pub(super) timestamp: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateData<'a> {
    pub(super) symbol: &'a str,
    #[serde(default, borrow)]
    pub(super) bids: Option<&'a RawValue>,
    #[serde(default, borrow)]
    pub(super) asks: Option<&'a RawValue>,
    #[serde(borrow)]
    pub(super) checksum: &'a RawValue,
    pub(super) timestamp: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotOrder<'a> {
    pub(super) order_id: &'a str,
    #[serde(borrow)]
    pub(super) limit_price: &'a RawValue,
    #[serde(borrow)]
    pub(super) order_qty: &'a RawValue,
    pub(super) timestamp: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct UpdateOrder<'a> {
    pub(super) event: &'a str,
    pub(super) order_id: &'a str,
    #[serde(borrow)]
    pub(super) limit_price: &'a RawValue,
    #[serde(borrow)]
    pub(super) order_qty: &'a RawValue,
    pub(super) timestamp: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Heartbeat<'a> {
    pub(super) channel: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StatusEnvelope<'a> {
    pub(super) channel: &'a str,
    #[serde(rename = "type")]
    pub(super) kind: &'a str,
    #[serde(borrow)]
    pub(super) data: Vec<StatusData<'a>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StatusData<'a> {
    pub(super) system: &'a str,
    pub(super) api_version: &'a str,
    pub(super) connection_id: u64,
    pub(super) version: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SubscribeAck<'a> {
    pub(super) method: &'a str,
    pub(super) success: bool,
    pub(super) result: Option<SubscribeResult<'a>>,
    pub(super) error: Option<&'a str>,
    pub(super) req_id: Option<u64>,
    pub(super) time_in: &'a str,
    pub(super) time_out: &'a str,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SubscribeResult<'a> {
    pub(super) channel: &'a str,
    pub(super) depth: usize,
    pub(super) snapshot: bool,
    pub(super) symbol: &'a str,
    #[serde(default, borrow)]
    pub(super) warnings: Option<&'a RawValue>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Pong<'a> {
    pub(super) method: &'a str,
    pub(super) req_id: Option<u64>,
    pub(super) time_in: &'a str,
    pub(super) time_out: &'a str,
}

pub(super) fn exact_decimal(raw: &RawValue) -> Result<&str, WireError> {
    let value = raw.get();
    if value.starts_with('"') {
        if value.len() < 2 || !value.ends_with('"') || value[1..value.len() - 1].contains('\\') {
            return Err(WireError::Malformed);
        }
        Ok(&value[1..value.len() - 1])
    } else {
        Ok(value)
    }
}

pub(super) fn ensure_array_bound(raw: &RawValue, max: usize) -> Result<usize, WireError> {
    serde_json::from_str::<ArrayCount>(raw.get())
        .map_err(|_| WireError::Malformed)
        .and_then(|count| {
            if count.0 > max {
                Err(WireError::TooManyItems)
            } else {
                Ok(count.0)
            }
        })
}

struct ArrayCount(usize);

impl<'de> Deserialize<'de> for ArrayCount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(ArrayCountVisitor)
    }
}

struct ArrayCountVisitor;

impl<'de> Visitor<'de> for ArrayCountVisitor {
    type Value = ArrayCount;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded JSON array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut count = 0_usize;
        while sequence.next_element::<IgnoredAny>()?.is_some() {
            count = count
                .checked_add(1)
                .ok_or_else(|| A::Error::custom("array count overflow"))?;
        }
        Ok(ArrayCount(count))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WireError {
    Malformed,
    Unsupported,
    TooManyItems,
}
