//! Bounded compatibility wire for current and legacy raw-capture journals.

use std::{fmt, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as _, IgnoredAny, SeqAccess, Visitor},
};
use thiserror::Error;
use uuid::Uuid;

/// Maximum serialized JSON body accepted by the committed journal frame.
pub(crate) const MAX_SERIALIZED_RECORD_BYTES: usize = 64 * 1024 * 1024;
const MAX_COMPATIBILITY_PAYLOAD_BYTES: usize = (MAX_SERIALIZED_RECORD_BYTES - 2) / 2;

#[derive(Debug)]
struct BoundedString<const MAX: usize>(String);

impl<'de, const MAX: usize> Deserialize<'de> for BoundedString<MAX> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StringVisitor<const LIMIT: usize>;

        impl<const LIMIT: usize> Visitor<'_> for StringVisitor<LIMIT> {
            type Value = String;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a string no longer than {LIMIT} UTF-8 bytes")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() > LIMIT {
                    Err(E::custom(
                        "raw capture string exceeds its compatibility bound",
                    ))
                } else {
                    Ok(value.to_owned())
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() > LIMIT {
                    Err(E::custom(
                        "raw capture string exceeds its compatibility bound",
                    ))
                } else {
                    Ok(value)
                }
            }
        }

        deserializer
            .deserialize_string(StringVisitor::<MAX>)
            .map(Self)
    }
}

#[derive(Debug)]
struct BoundedBytes<const MAX: usize>(Vec<u8>);

impl<'de, const MAX: usize> Deserialize<'de> for BoundedBytes<MAX> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BytesVisitor<const LIMIT: usize>;

        impl<'de, const LIMIT: usize> Visitor<'de> for BytesVisitor<LIMIT> {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "at most {LIMIT} raw byte values")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let initial_capacity = sequence.size_hint().unwrap_or(0).min(LIMIT);
                let mut bytes = Vec::with_capacity(initial_capacity);
                while bytes.len() < LIMIT {
                    match sequence.next_element::<u8>()? {
                        Some(byte) => bytes.push(byte),
                        None => return Ok(bytes),
                    }
                }
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(A::Error::custom(
                        "raw capture payload exceeds its compatibility bound",
                    ));
                }
                Ok(bytes)
            }
        }

        deserializer.deserialize_seq(BytesVisitor::<MAX>).map(Self)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCaptureRecordWire {
    event_id: Uuid,
    source: BoundedString<MAX_SERIALIZED_RECORD_BYTES>,
    connection_id: Uuid,
    source_sequence: Option<u64>,
    exchange_at: Option<DateTime<Utc>>,
    received_at: DateTime<Utc>,
    payload: BoundedBytes<MAX_COMPATIBILITY_PAYLOAD_BYTES>,
}

#[derive(Serialize)]
struct RawCaptureRecordWireRef<'a> {
    event_id: Uuid,
    source: &'a str,
    connection_id: Uuid,
    source_sequence: Option<u64>,
    exchange_at: Option<DateTime<Utc>>,
    received_at: DateTime<Utc>,
    payload: &'a [u8],
}

/// Validation failure for a newly captured live frame.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RawCaptureRecordError {
    /// The domain receive timestamp could not be represented by the compatibility journal wire.
    #[error("raw capture receive timestamp is outside the compatibility wire range")]
    InvalidReceivedAt,
    /// The locally assigned event identifier must not be nil for new live capture.
    #[error("new live-capture event identifier must not be nil")]
    NilEventId,
    /// New live source labels are bounded, nonblank text without control characters.
    #[error("new live-capture source label is invalid")]
    InvalidSource,
    /// The live source connection identifier must not be nil.
    #[error("new live-capture connection identifier must not be nil")]
    NilConnectionId,
    /// The exact live frame exceeds the bounded payload size.
    #[error("new live-capture payload is {bytes} bytes; maximum is {max}")]
    PayloadTooLarge {
        /// Actual byte count.
        bytes: usize,
        /// Maximum byte count.
        max: usize,
    },
    /// Compatibility fields cannot fit within the committed serialized-record ceiling.
    #[error("raw capture compatibility field exceeds the committed journal bound")]
    CompatibilityBound,
}

/// Exact, invariant-preserving committed raw-envelope journal payload.
///
/// `MEJ1` and `MSJ1` historically accepted nil UUIDs and otherwise unconstrained field values as
/// long as the serialized JSON body fit the 64 MiB journal frame. Deserialization preserves that
/// compatibility. [`Self::try_new_live`] and live publication apply stricter authority-adjacent
/// validation to newly received frames; the authoritative generation key remains out of band.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawCaptureRecord {
    event_id: Uuid,
    source: Arc<str>,
    connection_id: Uuid,
    source_sequence: Option<u64>,
    exchange_at: Option<DateTime<Utc>>,
    received_at: DateTime<Utc>,
    payload: Bytes,
}

impl RawCaptureRecord {
    /// Maximum source-label bytes accepted for newly captured live records.
    pub const MAX_LIVE_SOURCE_BYTES: usize = 256;
    /// Maximum new live frame size.
    ///
    /// At four JSON characters per `255` byte plus bounded envelope overhead, this remains well
    /// below the committed 64 MiB serialized journal ceiling.
    pub const MAX_LIVE_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

    /// Constructs a checked newly received live record with explicit evidence.
    ///
    /// Adapters assign `received_at` at the socket boundary and pass exact frame bytes. This
    /// constructor performs no clock reads and synthesizes no identity or timestamp evidence.
    ///
    /// # Errors
    ///
    /// Returns [`RawCaptureRecordError`] for nil identities, an invalid source label, or an
    /// oversized new live frame.
    pub fn try_new_live(
        event_id: Uuid,
        source: Arc<str>,
        connection_id: Uuid,
        source_sequence: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
        payload: Bytes,
    ) -> Result<Self, RawCaptureRecordError> {
        if payload.len() > Self::MAX_LIVE_PAYLOAD_BYTES {
            return Err(RawCaptureRecordError::PayloadTooLarge {
                bytes: payload.len(),
                max: Self::MAX_LIVE_PAYLOAD_BYTES,
            });
        }
        // `Bytes::slice` may retain an arbitrarily larger allocation than its visible length.
        // Normalize at the trust boundary so the retained allocation is bounded by frame length.
        let payload = Bytes::copy_from_slice(&payload);
        let record = Self {
            event_id,
            source,
            connection_id,
            source_sequence,
            exchange_at,
            received_at,
            payload,
        };
        record.validate_live()?;
        Ok(record)
    }

    /// Constructs a bounded record using the historically permissive committed wire semantics.
    ///
    /// This exists for compatibility fixtures and controlled migrations. Passing the result to a
    /// live publisher still applies strict live-record validation and rejects nil identities or
    /// other values that are not execution-adjacent live-capture evidence.
    pub fn try_from_compatibility_parts(
        event_id: Uuid,
        source: String,
        connection_id: Uuid,
        source_sequence: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
        payload: Vec<u8>,
    ) -> Result<Self, RawCaptureRecordError> {
        let record = Self {
            event_id,
            source: Arc::from(source),
            connection_id,
            source_sequence,
            exchange_at,
            received_at,
            payload: Bytes::from(payload.into_boxed_slice()),
        };
        record.validate_compatibility()?;
        Ok(record)
    }

    pub(crate) fn validate_compatibility(&self) -> Result<(), RawCaptureRecordError> {
        if self.source.len() > MAX_SERIALIZED_RECORD_BYTES
            || self.payload.len() > MAX_COMPATIBILITY_PAYLOAD_BYTES
        {
            return Err(RawCaptureRecordError::CompatibilityBound);
        }
        Ok(())
    }

    pub(crate) fn validate_live(&self) -> Result<(), RawCaptureRecordError> {
        self.validate_compatibility()?;
        if self.event_id.is_nil() {
            return Err(RawCaptureRecordError::NilEventId);
        }
        if self.source.trim().is_empty()
            || self.source.len() > Self::MAX_LIVE_SOURCE_BYTES
            || self.source.chars().any(char::is_control)
        {
            return Err(RawCaptureRecordError::InvalidSource);
        }
        if self.connection_id.is_nil() {
            return Err(RawCaptureRecordError::NilConnectionId);
        }
        if self.payload.len() > Self::MAX_LIVE_PAYLOAD_BYTES {
            return Err(RawCaptureRecordError::PayloadTooLarge {
                bytes: self.payload.len(),
                max: Self::MAX_LIVE_PAYLOAD_BYTES,
            });
        }
        Ok(())
    }

    /// Returns the locally assigned event identity.
    pub const fn event_id(&self) -> Uuid {
        self.event_id
    }

    /// Returns the committed source label.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Returns the committed source connection identity.
    pub const fn connection_id(&self) -> Uuid {
        self.connection_id
    }

    /// Returns the optional provider sequence.
    pub const fn source_sequence(&self) -> Option<u64> {
        self.source_sequence
    }

    /// Returns the optional source-authored event time.
    pub const fn exchange_at(&self) -> Option<DateTime<Utc>> {
        self.exchange_at
    }

    /// Returns the socket-boundary receive time.
    pub const fn received_at(&self) -> DateTime<Utc> {
        self.received_at
    }

    /// Returns the exact source frame bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl Serialize for RawCaptureRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        RawCaptureRecordWireRef {
            event_id: self.event_id,
            source: &self.source,
            connection_id: self.connection_id,
            source_sequence: self.source_sequence,
            exchange_at: self.exchange_at,
            received_at: self.received_at,
            payload: &self.payload,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RawCaptureRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = RawCaptureRecordWire::deserialize(deserializer)?;
        let record = Self {
            event_id: wire.event_id,
            source: Arc::from(wire.source.0),
            connection_id: wire.connection_id,
            source_sequence: wire.source_sequence,
            exchange_at: wire.exchange_at,
            received_at: wire.received_at,
            payload: Bytes::from(wire.payload.0.into_boxed_slice()),
        };
        record.validate_compatibility().map_err(D::Error::custom)?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{Bytes, BytesMut};
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    use super::{BoundedBytes, RawCaptureRecord};

    #[test]
    fn bounded_byte_visitor_stops_at_the_first_excess_as_ignored_any() {
        let error = serde_json::from_str::<BoundedBytes<2>>(r#"[1,2,{"nested":"ignored"}]"#)
            .err()
            .map(|error| error.to_string());
        assert!(error.as_deref().is_some_and(|message| {
            message.starts_with("raw capture payload exceeds its compatibility bound")
        }));
    }

    #[test]
    fn live_record_clones_share_source_and_payload_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload = Bytes::from_static(b"shared-frame");
        let record = RawCaptureRecord::try_new_live(
            Uuid::from_u128(1),
            Arc::from("source-a"),
            Uuid::from_u128(2),
            None,
            None,
            Utc.timestamp_opt(1_752_607_200, 0)
                .single()
                .ok_or("invalid fixed test timestamp")?,
            payload,
        )?;
        let cloned = record.clone();

        assert!(std::ptr::eq(
            record.source().as_ptr(),
            cloned.source().as_ptr()
        ));
        assert!(std::ptr::eq(
            record.payload().as_ptr(),
            cloned.payload().as_ptr()
        ));
        Ok(())
    }

    #[test]
    fn live_record_normalizes_a_tiny_slice_of_a_large_backing_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut oversized_backing = BytesMut::with_capacity(16 * 1024 * 1024);
        oversized_backing.resize(16 * 1024 * 1024, 7);
        let visible_slice = oversized_backing.freeze().slice(..16);
        let record = RawCaptureRecord::try_new_live(
            Uuid::from_u128(1),
            Arc::from("source-a"),
            Uuid::from_u128(2),
            None,
            None,
            Utc.timestamp_opt(1_752_607_200, 0)
                .single()
                .ok_or("invalid fixed test timestamp")?,
            visible_slice,
        )?;
        let RawCaptureRecord { payload, .. } = record;
        let normalized = payload
            .try_into_mut()
            .map_err(|_payload| "normalized payload unexpectedly retained a shared owner")?;

        assert_eq!(normalized.len(), 16);
        assert!(normalized.capacity() <= RawCaptureRecord::MAX_LIVE_PAYLOAD_BYTES);
        Ok(())
    }
}
