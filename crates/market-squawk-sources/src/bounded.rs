use std::fmt;
use std::marker::PhantomData;

use bytes::Bytes;
use serde::de::{IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BoundExceeded {
    pub(crate) max: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub(crate) struct BoundedVec<T, const MAX: usize>(Vec<T>);

impl<T, const MAX: usize> BoundedVec<T, MAX> {
    pub(crate) const fn empty() -> Self {
        Self(Vec::new())
    }

    pub(crate) fn singleton(value: T) -> Self {
        Self(vec![value])
    }

    pub(crate) fn try_new(values: Vec<T>) -> Result<Self, BoundExceeded> {
        if values.len() > MAX {
            Err(BoundExceeded { max: MAX })
        } else {
            Ok(Self(values.into_boxed_slice().into_vec()))
        }
    }

    pub(crate) fn as_slice(&self) -> &[T] {
        &self.0
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn into_vec(self) -> Vec<T> {
        self.0
    }
}

struct BoundedVecVisitor<T, const MAX: usize>(PhantomData<T>);

impl<'de, T, const MAX: usize> Visitor<'de> for BoundedVecVisitor<T, MAX>
where
    T: Deserialize<'de>,
{
    type Value = BoundedVec<T, MAX>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a sequence containing at most {MAX} elements")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence.size_hint().is_some_and(|hint| hint > MAX) {
            return Err(serde::de::Error::custom(format_args!(
                "sequence exceeds maximum capacity {MAX}"
            )));
        }
        let initial_capacity = sequence.size_hint().unwrap_or(0).min(MAX).min(1_024);
        let mut values = Vec::with_capacity(initial_capacity);
        while values.len() < MAX {
            let Some(value) = sequence.next_element()? else {
                return Ok(BoundedVec(values));
            };
            values.push(value);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(format_args!(
                "sequence exceeds maximum capacity {MAX}"
            )))
        } else {
            Ok(BoundedVec(values))
        }
    }
}

impl<'de, T, const MAX: usize> Deserialize<'de> for BoundedVec<T, MAX>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(BoundedVecVisitor(PhantomData))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundedBytes<const MAX: usize>(Bytes);

impl<const MAX: usize> BoundedBytes<MAX> {
    pub(crate) fn try_from_bytes(bytes: Bytes) -> Result<Self, BoundExceeded> {
        if bytes.len() > MAX {
            Err(BoundExceeded { max: MAX })
        } else {
            // Always detach from caller-owned backing storage. A tiny `Bytes` slice can otherwise
            // retain an arbitrarily large allocation while satisfying the logical length bound.
            Ok(Self(Bytes::copy_from_slice(&bytes)))
        }
    }

    pub(crate) fn as_bytes(&self) -> &Bytes {
        &self.0
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.0.len()
    }
}

impl<const MAX: usize> Serialize for BoundedBytes<MAX> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

struct BoundedBytesVisitor<const MAX: usize>;

impl<'de, const MAX: usize> Visitor<'de> for BoundedBytesVisitor<MAX> {
    type Value = BoundedBytes<MAX>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a byte sequence no larger than {MAX} bytes")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value.len() > MAX {
            return Err(E::custom(format_args!(
                "byte sequence exceeds maximum capacity {MAX}"
            )));
        }
        Self::Value::try_from_bytes(Bytes::copy_from_slice(value)).map_err(E::custom)
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value.len() > MAX {
            return Err(E::custom(format_args!(
                "byte sequence exceeds maximum capacity {MAX}"
            )));
        }
        Self::Value::try_from_bytes(Bytes::from(value)).map_err(E::custom)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        if sequence.size_hint().is_some_and(|hint| hint > MAX) {
            return Err(serde::de::Error::custom(format_args!(
                "byte sequence exceeds maximum capacity {MAX}"
            )));
        }
        let initial_capacity = sequence.size_hint().unwrap_or(0).min(MAX).min(4_096);
        let mut bytes = Vec::with_capacity(initial_capacity);
        while bytes.len() < MAX {
            let Some(value) = sequence.next_element()? else {
                return Ok(BoundedBytes(Bytes::from(bytes)));
            };
            bytes.push(value);
        }
        if sequence.next_element::<IgnoredAny>()?.is_some() {
            Err(serde::de::Error::custom(format_args!(
                "byte sequence exceeds maximum capacity {MAX}"
            )))
        } else {
            Ok(BoundedBytes(Bytes::from(bytes)))
        }
    }
}

impl<'de, const MAX: usize> Deserialize<'de> for BoundedBytes<MAX> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Sequence decoding permits rejecting hostile length hints before a byte buffer is
        // allocated by the deserializer.
        deserializer.deserialize_seq(BoundedBytesVisitor)
    }
}

impl fmt::Display for BoundExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "value exceeds maximum capacity {}", self.max)
    }
}

impl std::error::Error for BoundExceeded {}
