//! Bounded compatibility wire for current and legacy raw-capture journals.

use std::{fmt, io, sync::Arc};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use market_squawk_domain::{
    CapturePayload, CapturePayloadError, CaptureRetainedComponent, CaptureRetainedSizeError,
    MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES, MAX_LIVE_CAPTURE_PAYLOAD_BYTES,
    checked_arc_str_allocation_bytes,
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error as _, IgnoredAny, SeqAccess, Visitor},
};
use thiserror::Error;
use uuid::Uuid;

/// Maximum serialized JSON body accepted by the committed journal frame.
pub(crate) const MAX_SERIALIZED_RECORD_BYTES: usize = 128 * 1024 * 1024;
const MAX_COMPATIBILITY_PAYLOAD_BYTES: usize = MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES;
const MAX_LIVE_WORST_CASE_SERIALIZED_BYTES: usize =
    MAX_LIVE_CAPTURE_PAYLOAD_BYTES * 4 + RawCaptureRecord::MAX_LIVE_SOURCE_BYTES * 6 + 4_096;
const _: () = assert!(MAX_LIVE_WORST_CASE_SERIALIZED_BYTES < MAX_SERIALIZED_RECORD_BYTES);
#[cfg(test)]
std::thread_local! {
    static COMPATIBILITY_VALIDATION_PASSES: std::cell::Cell<usize> = const {
        std::cell::Cell::new(0)
    };
}

#[derive(Debug)]
struct BoundedCountingWriter {
    bytes: usize,
    maximum: usize,
}

impl BoundedCountingWriter {
    const fn new(maximum: usize) -> Self {
        Self { bytes: 0, maximum }
    }
}

impl io::Write for BoundedCountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("serialized raw record length overflowed"))?;
        if next > self.maximum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "serialized raw record exceeds the committed journal bound",
            ));
        }
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

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

impl RawCaptureRecordWireRef<'_> {
    fn validate_serialized_bound(&self, maximum: usize) -> Result<usize, RawCaptureRecordError> {
        #[cfg(test)]
        COMPATIBILITY_VALIDATION_PASSES.with(|passes| passes.set(passes.get().saturating_add(1)));
        let mut counter = BoundedCountingWriter::new(maximum);
        serde_json::to_writer(&mut counter, self)
            .map_err(|_error| RawCaptureRecordError::CompatibilityBound)?;
        Ok(counter.bytes)
    }
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
    /// Generic conversion did not preserve the exact capture payload allocation.
    #[error("raw capture record did not preserve the exact payload allocation")]
    InvalidPayloadSharing,
    /// A complete retained-allocation formula failed closed.
    #[error(transparent)]
    RetainedSize(#[from] CaptureRetainedSizeError),
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
    payload: CapturePayload,
}

impl RawCaptureRecord {
    /// Maximum source-label bytes accepted for newly captured live records.
    pub const MAX_LIVE_SOURCE_BYTES: usize = 256;
    /// Maximum new live frame size.
    ///
    /// At four JSON characters per `255` byte plus bounded envelope overhead, this remains well
    /// below the committed 64 MiB serialized journal ceiling.
    pub const MAX_LIVE_PAYLOAD_BYTES: usize = MAX_LIVE_CAPTURE_PAYLOAD_BYTES;

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
        let payload_length = payload.len();
        let payload = CapturePayload::try_from_live(&payload).map_err(|error| match error {
            CapturePayloadError::TooLarge { .. } => RawCaptureRecordError::PayloadTooLarge {
                bytes: payload_length,
                max: Self::MAX_LIVE_PAYLOAD_BYTES,
            },
            CapturePayloadError::RetainedLayout(_) => {
                RawCaptureRecordError::RetainedSize(CaptureRetainedSizeError::Overflow {
                    component: CaptureRetainedComponent::Payload,
                })
            }
        })?;
        Self::try_new_live_payload(
            event_id,
            source,
            connection_id,
            source_sequence,
            exchange_at,
            received_at,
            payload,
        )
    }

    pub(crate) fn try_new_live_payload(
        event_id: Uuid,
        source: Arc<str>,
        connection_id: Uuid,
        source_sequence: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
        payload: CapturePayload,
    ) -> Result<Self, RawCaptureRecordError> {
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
        Self::try_from_compatibility_parts_with_bound(
            event_id,
            source,
            connection_id,
            source_sequence,
            exchange_at,
            received_at,
            payload,
            MAX_SERIALIZED_RECORD_BYTES,
        )
    }

    // This private seam mirrors the committed seven-field wire constructor and adds only the
    // injected byte ceiling needed to prove the exact boundary. Keeping the field list explicit
    // prevents a test-only wrapper from becoming an alternate production representation.
    #[allow(clippy::too_many_arguments)]
    fn try_from_compatibility_parts_with_bound(
        event_id: Uuid,
        source: String,
        connection_id: Uuid,
        source_sequence: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
        payload: Vec<u8>,
        serialized_maximum: usize,
    ) -> Result<Self, RawCaptureRecordError> {
        Self::wire_ref(
            event_id,
            &source,
            connection_id,
            source_sequence,
            exchange_at,
            received_at,
            &payload,
        )
        .validate_serialized_bound(serialized_maximum)?;
        let payload = CapturePayload::try_from_committed_wire(&payload)
            .map_err(|_error| RawCaptureRecordError::CompatibilityBound)?;
        Ok(Self {
            event_id,
            source: Arc::from(source),
            connection_id,
            source_sequence,
            exchange_at,
            received_at,
            payload,
        })
    }

    pub(crate) fn validate_compatibility(&self) -> Result<(), RawCaptureRecordError> {
        if self.source.len() > MAX_SERIALIZED_RECORD_BYTES
            || self.payload.as_bytes().len() > MAX_COMPATIBILITY_PAYLOAD_BYTES
        {
            return Err(RawCaptureRecordError::CompatibilityBound);
        }
        self.as_wire_ref()
            .validate_serialized_bound(MAX_SERIALIZED_RECORD_BYTES)?;
        Ok(())
    }

    fn wire_ref<'a>(
        event_id: Uuid,
        source: &'a str,
        connection_id: Uuid,
        source_sequence: Option<u64>,
        exchange_at: Option<DateTime<Utc>>,
        received_at: DateTime<Utc>,
        payload: &'a [u8],
    ) -> RawCaptureRecordWireRef<'a> {
        RawCaptureRecordWireRef {
            event_id,
            source,
            connection_id,
            source_sequence,
            exchange_at,
            received_at,
            payload,
        }
    }

    fn as_wire_ref(&self) -> RawCaptureRecordWireRef<'_> {
        Self::wire_ref(
            self.event_id,
            &self.source,
            self.connection_id,
            self.source_sequence,
            self.exchange_at,
            self.received_at,
            self.payload.as_bytes(),
        )
    }

    pub(crate) fn validate_live(&self) -> Result<(), RawCaptureRecordError> {
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
        if self.payload.as_bytes().len() > Self::MAX_LIVE_PAYLOAD_BYTES {
            return Err(RawCaptureRecordError::PayloadTooLarge {
                bytes: self.payload.as_bytes().len(),
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
        self.payload.as_bytes()
    }

    pub(crate) const fn capture_payload(&self) -> &CapturePayload {
        &self.payload
    }

    pub(crate) fn checked_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        std::mem::size_of::<Self>()
            .checked_add(self.checked_dynamic_retained_bytes()?)
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            })
    }

    pub(crate) fn checked_dynamic_retained_bytes(&self) -> Result<usize, CaptureRetainedSizeError> {
        let source_bytes = checked_arc_str_allocation_bytes(self.source.len()).map_err(|_| {
            CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            }
        })?;
        let payload_bytes = self.payload.checked_retained_allocation_bytes()?;
        source_bytes
            .checked_add(payload_bytes)
            .ok_or(CaptureRetainedSizeError::Overflow {
                component: CaptureRetainedComponent::Frame,
            })
    }

    pub(crate) fn shares_allocations_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.source, &other.source)
            && self.payload.shares_allocation_with(&other.payload)
    }
}

impl Serialize for RawCaptureRecord {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_wire_ref().serialize(serializer)
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
            payload: CapturePayload::try_from_committed_wire(&wire.payload.0)
                .map_err(D::Error::custom)?,
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
    use proptest::prelude::*;
    use uuid::Uuid;

    use market_squawk_domain::{
        MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES, MAX_LIVE_CAPTURE_PAYLOAD_BYTES,
    };

    use super::{
        BoundedBytes, COMPATIBILITY_VALIDATION_PASSES, MAX_SERIALIZED_RECORD_BYTES,
        RawCaptureRecord, RawCaptureRecordWireRef,
    };

    #[test]
    fn compatibility_field_read_ceiling_is_distinct_from_the_complete_record_ceiling() {
        const {
            assert!(MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES < MAX_SERIALIZED_RECORD_BYTES);
        }
    }

    #[test]
    fn compatibility_constructor_rejects_a_payload_whose_complete_encoding_is_too_large()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(
            RawCaptureRecord::try_from_compatibility_parts(
                Uuid::nil(),
                String::new(),
                Uuid::nil(),
                None,
                None,
                Utc.timestamp_opt(0, 0)
                    .single()
                    .ok_or("invalid fixture time")?,
                vec![255_u8; MAX_SERIALIZED_RECORD_BYTES / 4],
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn complete_json_bound_is_exact_for_digit_boundaries_and_maximal_escaping()
    -> Result<(), Box<dyn std::error::Error>> {
        let received_at = Utc
            .timestamp_opt(253_402_300_799, 999_999_999)
            .single()
            .ok_or("invalid maximum fixture time")?;
        let source = "\0\u{0008}\t\n\u{000c}\r\"\\maximally-escaped";

        for byte in [0_u8, 9, 10, 99, 100, 255] {
            let payload = [byte];
            let wire = RawCaptureRecordWireRef {
                event_id: Uuid::from_u128(u128::MAX),
                source,
                connection_id: Uuid::from_u128(u128::MAX),
                source_sequence: Some(u64::MAX),
                exchange_at: Some(received_at),
                received_at,
                payload: &payload,
            };
            let encoded = serde_json::to_vec(&wire)?;
            assert_eq!(
                wire.validate_serialized_bound(encoded.len())?,
                encoded.len()
            );
            assert!(wire.validate_serialized_bound(encoded.len() - 1).is_err());
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn complete_json_size_validation_matches_serde_for_arbitrary_bounded_records(
            event_id in any::<u128>(),
            source in proptest::collection::vec(any::<char>(), 0..128)
                .prop_map(|characters| characters.into_iter().collect::<String>()),
            connection_id in any::<u128>(),
            source_sequence in proptest::option::of(any::<u64>()),
            exchange_time in proptest::option::of((-1_000_000_i64..1_000_000_i64, 0_u32..1_000_000_000_u32)),
            received_seconds in -1_000_000_i64..1_000_000_i64,
            received_nanos in 0_u32..1_000_000_000_u32,
            payload in proptest::collection::vec(any::<u8>(), 0..1_024),
        ) {
            let exchange_at = exchange_time.and_then(|(seconds, nanos)| {
                Utc.timestamp_opt(seconds, nanos).single()
            });
            let received_at = Utc
                .timestamp_opt(received_seconds, received_nanos)
                .single()
                .ok_or_else(|| TestCaseError::fail("generated an invalid received timestamp"))?;
            let wire = RawCaptureRecordWireRef {
                event_id: Uuid::from_u128(event_id),
                source: &source,
                connection_id: Uuid::from_u128(connection_id),
                source_sequence,
                exchange_at,
                received_at,
                payload: &payload,
            };
            let encoded = serde_json::to_vec(&wire)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;

            prop_assert_eq!(
                wire.validate_serialized_bound(encoded.len()),
                Ok(encoded.len())
            );
            if !encoded.is_empty() {
                prop_assert_eq!(
                    wire.validate_serialized_bound(encoded.len() - 1),
                    Err(super::RawCaptureRecordError::CompatibilityBound)
                );
                prop_assert_eq!(
                    RawCaptureRecord::try_from_compatibility_parts_with_bound(
                        Uuid::from_u128(event_id),
                        source.clone(),
                        Uuid::from_u128(connection_id),
                        source_sequence,
                        exchange_at,
                        received_at,
                        payload.clone(),
                        encoded.len() - 1,
                    ),
                    Err(super::RawCaptureRecordError::CompatibilityBound)
                );
            }

            let record = RawCaptureRecord::try_from_compatibility_parts_with_bound(
                Uuid::from_u128(event_id),
                source,
                Uuid::from_u128(connection_id),
                source_sequence,
                exchange_at,
                received_at,
                payload,
                encoded.len(),
            )
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
            let actual = serde_json::to_vec(&record)
                .map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert_eq!(actual, encoded);
        }
    }

    #[test]
    fn complete_json_ceiling_accepts_exactly_the_last_byte_and_rejects_one_more()
    -> Result<(), Box<dyn std::error::Error>> {
        let event_id = Uuid::from_u128(u128::MAX);
        let connection_id = Uuid::from_u128(u128::MAX);
        let source_sequence = Some(u64::MAX);
        let exchange_at = Utc
            .timestamp_opt(253_402_300_799, 999_999_999)
            .single()
            .ok_or("invalid maximum exchange fixture time")?;
        let received_at = exchange_at;
        let empty_payload = [];
        let base = RawCaptureRecordWireRef {
            event_id,
            source: "",
            connection_id,
            source_sequence,
            exchange_at: Some(exchange_at),
            received_at,
            payload: &empty_payload,
        };
        let base_length = serde_json::to_vec(&base)?.len();
        let payload_length = (MAX_SERIALIZED_RECORD_BYTES - base_length).div_ceil(2);
        assert!(payload_length <= MAX_COMPATIBILITY_CAPTURE_PAYLOAD_BYTES);
        let payload_delta = payload_length
            .checked_mul(2)
            .and_then(|length| length.checked_sub(1))
            .ok_or("payload encoding length overflowed")?;
        let source_length = MAX_SERIALIZED_RECORD_BYTES
            .checked_sub(base_length + payload_delta)
            .ok_or("fixture payload exceeded serialized ceiling")?;
        let source = "a".repeat(source_length);

        let record = RawCaptureRecord::try_from_compatibility_parts(
            event_id,
            source.clone(),
            connection_id,
            source_sequence,
            Some(exchange_at),
            received_at,
            vec![0_u8; payload_length],
        )?;
        let encoded = serde_json::to_vec(&record)?;
        assert_eq!(encoded.len(), MAX_SERIALIZED_RECORD_BYTES);
        drop(encoded);
        drop(record);

        assert!(
            RawCaptureRecord::try_from_compatibility_parts(
                event_id,
                format!("{source}a"),
                connection_id,
                source_sequence,
                Some(exchange_at),
                received_at,
                vec![0_u8; payload_length],
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn every_constructible_compatibility_record_serializes_and_round_trips_within_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let record = RawCaptureRecord::try_from_compatibility_parts(
            Uuid::from_u128(u128::MAX),
            "\0\u{0008}\t\n\u{000c}\r\"\\maximally-escaped".to_owned(),
            Uuid::from_u128(u128::MAX),
            Some(u64::MAX),
            None,
            Utc.timestamp_opt(0, 0)
                .single()
                .ok_or("invalid fixture time")?,
            vec![0, 9, 10, 99, 100, 255],
        )?;
        let encoded = serde_json::to_vec(&record)?;
        assert!(encoded.len() <= MAX_SERIALIZED_RECORD_BYTES);
        let decoded: RawCaptureRecord = serde_json::from_slice(&encoded)?;
        assert_eq!(decoded, record);
        Ok(())
    }

    #[test]
    fn serialization_relies_on_the_immutable_constructor_invariant_without_a_second_counting_pass()
    -> Result<(), Box<dyn std::error::Error>> {
        let record = RawCaptureRecord::try_from_compatibility_parts(
            Uuid::from_u128(1),
            "compatibility-source".to_owned(),
            Uuid::from_u128(2),
            None,
            None,
            Utc.timestamp_opt(0, 0)
                .single()
                .ok_or("invalid fixture time")?,
            vec![0, 9, 10, 99, 100, 255],
        )?;
        let after_construction = COMPATIBILITY_VALIDATION_PASSES.with(std::cell::Cell::get);
        let encoded = serde_json::to_vec(&record)?;
        assert!(!encoded.is_empty());
        assert_eq!(
            COMPATIBILITY_VALIDATION_PASSES.with(std::cell::Cell::get),
            after_construction
        );
        Ok(())
    }

    #[test]
    fn live_validation_uses_the_constant_worst_case_proof_without_json_traversal()
    -> Result<(), Box<dyn std::error::Error>> {
        let before = COMPATIBILITY_VALIDATION_PASSES.with(std::cell::Cell::get);
        let record = RawCaptureRecord::try_new_live(
            Uuid::from_u128(1),
            Arc::from("direct-source"),
            Uuid::from_u128(2),
            Some(u64::MAX),
            None,
            Utc.timestamp_opt(0, 0)
                .single()
                .ok_or("invalid fixture time")?,
            Bytes::from(vec![255_u8; MAX_LIVE_CAPTURE_PAYLOAD_BYTES]),
        )?;
        assert_eq!(record.payload().len(), MAX_LIVE_CAPTURE_PAYLOAD_BYTES);
        assert_eq!(
            COMPATIBILITY_VALIDATION_PASSES.with(std::cell::Cell::get),
            before
        );
        Ok(())
    }

    #[test]
    fn historical_payload_above_live_limit_round_trips_through_serde()
    -> Result<(), Box<dyn std::error::Error>> {
        let payload_len = MAX_LIVE_CAPTURE_PAYLOAD_BYTES + 1;
        let original = RawCaptureRecord::try_from_compatibility_parts(
            Uuid::nil(),
            "historical-source".to_owned(),
            Uuid::nil(),
            None,
            None,
            Utc.timestamp_opt(0, 0)
                .single()
                .ok_or("invalid fixture time")?,
            vec![7_u8; payload_len],
        )?;
        let encoded = serde_json::to_vec(&original)?;
        let decoded: RawCaptureRecord = serde_json::from_slice(&encoded)?;

        assert_eq!(decoded, original);
        assert_eq!(decoded.payload().len(), payload_len);
        Ok(())
    }

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
        assert_eq!(record.payload().len(), 16);
        assert_eq!(
            record
                .capture_payload()
                .checked_retained_allocation_bytes()?,
            market_squawk_domain::checked_arc_bytes_allocation_bytes(16)?
        );
        Ok(())
    }
}
