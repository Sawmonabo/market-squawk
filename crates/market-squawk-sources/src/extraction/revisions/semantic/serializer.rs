//! Shared deterministic PIT-v1 tagged binary serializer.

use std::fmt;

use serde::Serialize;
use serde::ser::{
    Impossible, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use sha2::{Digest as _, Sha256};

const PIT_IDENTITY_SCHEMA_VERSION: u16 = 1;
const MAX_COLLECTION_ELEMENTS: usize = 4_096;

/// Cooperative control used while producing PIT-v1 canonical bytes.
#[doc(hidden)]
pub trait PitV1EncodingControl {
    /// Observes one bounded encoder operation.
    fn checkpoint(&mut self) -> Result<(), PitV1EncodingError>;
}

/// Failure to encode exact PIT-v1 canonical bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum PitV1EncodingError {
    /// A value cannot be represented by the canonical contract.
    Encoding,
    /// A bounded allocation failed.
    AllocationFailure,
    /// Checked byte accounting overflowed.
    AccountingOverflow,
    /// The configured byte or collection bound was exceeded.
    LimitExceeded,
    /// Encoding was cancelled.
    Cancelled,
    /// The encoding deadline elapsed.
    DeadlineExceeded,
}

impl fmt::Display for PitV1EncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Encoding => "canonical encoding failed",
            Self::AllocationFailure => "canonical encoding allocation failed",
            Self::AccountingOverflow => "canonical encoding accounting overflowed",
            Self::LimitExceeded => "canonical encoding limit exceeded",
            Self::Cancelled => "canonical encoding cancelled",
            Self::DeadlineExceeded => "canonical encoding deadline exceeded",
        })
    }
}

impl std::error::Error for PitV1EncodingError {}

impl serde::ser::Error for PitV1EncodingError {
    fn custom<T: fmt::Display>(_message: T) -> Self {
        Self::Encoding
    }
}

/// Exact PIT-v1 encoder shared by revision assignment and point-in-time selection.
#[doc(hidden)]
pub struct PitV1CanonicalEncoder<'control> {
    hasher: Sha256,
    bytes: Option<Vec<u8>>,
    encoded_len: usize,
    max_encoded_bytes: usize,
    control: &'control mut dyn PitV1EncodingControl,
}

impl fmt::Debug for PitV1CanonicalEncoder<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PitV1CanonicalEncoder")
            .field("collecting", &self.bytes.is_some())
            .field("encoded_len", &self.encoded_len)
            .field("max_encoded_bytes", &self.max_encoded_bytes)
            .finish_non_exhaustive()
    }
}

impl<'control> PitV1CanonicalEncoder<'control> {
    /// Constructs an uncollected encoder.
    pub fn new(
        domain: &str,
        control: &'control mut dyn PitV1EncodingControl,
    ) -> Result<Self, PitV1EncodingError> {
        Self::new_bounded(domain, usize::MAX, control)
    }

    pub(crate) fn new_bounded(
        domain: &str,
        max_encoded_bytes: usize,
        control: &'control mut dyn PitV1EncodingControl,
    ) -> Result<Self, PitV1EncodingError> {
        let mut value = Self {
            hasher: Sha256::new(),
            bytes: None,
            encoded_len: 0,
            max_encoded_bytes,
            control,
        };
        value.write_header(domain)?;
        Ok(value)
    }

    /// Constructs a collecting encoder with the exact measured capacity.
    pub fn collecting_exact(
        domain: &str,
        expected_len: usize,
        control: &'control mut dyn PitV1EncodingControl,
    ) -> Result<Self, PitV1EncodingError> {
        Self::collecting_exact_bounded(domain, expected_len, usize::MAX, control)
    }

    pub(crate) fn collecting_exact_bounded(
        domain: &str,
        expected_len: usize,
        max_encoded_bytes: usize,
        control: &'control mut dyn PitV1EncodingControl,
    ) -> Result<Self, PitV1EncodingError> {
        if expected_len > max_encoded_bytes {
            return Err(PitV1EncodingError::LimitExceeded);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(expected_len)
            .map_err(|_| PitV1EncodingError::AllocationFailure)?;
        if bytes.capacity() != expected_len {
            return Err(PitV1EncodingError::Encoding);
        }
        let mut value = Self {
            hasher: Sha256::new(),
            bytes: Some(bytes),
            encoded_len: 0,
            max_encoded_bytes,
            control,
        };
        value.write_header(domain)?;
        Ok(value)
    }

    fn write_header(&mut self, domain: &str) -> Result<(), PitV1EncodingError> {
        self.raw(b"MSQPIT")?;
        self.u16(PIT_IDENTITY_SCHEMA_VERSION)?;
        self.str(domain)
    }

    pub fn u8(&mut self, value: u8) -> Result<(), PitV1EncodingError> {
        self.raw(&[value])
    }

    pub fn u16(&mut self, value: u16) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn u32(&mut self, value: u32) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn u64(&mut self, value: u64) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn u128(&mut self, value: u128) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn i8(&mut self, value: i8) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn i16(&mut self, value: i16) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn i32(&mut self, value: i32) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn i64(&mut self, value: i64) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn i128(&mut self, value: i128) -> Result<(), PitV1EncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub fn bytes(&mut self, value: &[u8]) -> Result<(), PitV1EncodingError> {
        self.u64(u64::try_from(value.len()).map_err(|_| PitV1EncodingError::Encoding)?)?;
        self.raw(value)
    }

    pub fn str(&mut self, value: &str) -> Result<(), PitV1EncodingError> {
        self.bytes(value.as_bytes())
    }

    pub fn option_str(&mut self, value: Option<&str>) -> Result<(), PitV1EncodingError> {
        match value {
            Some(value) => {
                self.u8(1)?;
                self.str(value)
            }
            None => self.u8(0),
        }
    }

    pub fn serializable<T: Serialize>(&mut self, value: &T) -> Result<(), PitV1EncodingError> {
        value.serialize(CanonicalSerializer { encoder: self })
    }

    fn raw(&mut self, value: &[u8]) -> Result<(), PitV1EncodingError> {
        self.control.checkpoint()?;
        let encoded_len = self
            .encoded_len
            .checked_add(value.len())
            .ok_or(PitV1EncodingError::AccountingOverflow)?;
        if encoded_len > self.max_encoded_bytes {
            return Err(PitV1EncodingError::LimitExceeded);
        }
        if self
            .bytes
            .as_ref()
            .is_some_and(|bytes| encoded_len > bytes.capacity())
        {
            return Err(PitV1EncodingError::Encoding);
        }
        self.hasher.update(value);
        if let Some(bytes) = self.bytes.as_mut() {
            bytes.extend_from_slice(value);
        }
        self.encoded_len = encoded_len;
        Ok(())
    }

    pub fn finish(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }

    pub fn finish_with_len(self) -> ([u8; 32], usize) {
        (self.hasher.finalize().into(), self.encoded_len)
    }

    pub fn finish_with_bytes(self) -> Result<([u8; 32], Vec<u8>, usize), PitV1EncodingError> {
        let bytes = self.bytes.ok_or(PitV1EncodingError::Encoding)?;
        Ok((self.hasher.finalize().into(), bytes, self.encoded_len))
    }
}

struct CanonicalSerializer<'encoder, 'control> {
    encoder: &'encoder mut PitV1CanonicalEncoder<'control>,
}

struct Compound<'encoder, 'control> {
    encoder: &'encoder mut PitV1CanonicalEncoder<'control>,
    end_tag: u8,
    elements: usize,
}

impl<'encoder, 'control> serde::Serializer for CanonicalSerializer<'encoder, 'control> {
    type Ok = ();
    type Error = PitV1EncodingError;
    type SerializeSeq = Compound<'encoder, 'control>;
    type SerializeTuple = Compound<'encoder, 'control>;
    type SerializeTupleStruct = Compound<'encoder, 'control>;
    type SerializeTupleVariant = Compound<'encoder, 'control>;
    type SerializeMap = Impossible<(), PitV1EncodingError>;
    type SerializeStruct = Compound<'encoder, 'control>;
    type SerializeStructVariant = Compound<'encoder, 'control>;

    fn serialize_bool(self, value: bool) -> Result<(), Self::Error> {
        self.encoder.u8(1)?;
        self.encoder.u8(u8::from(value))
    }
    fn serialize_i8(self, value: i8) -> Result<(), Self::Error> {
        self.encoder.u8(2)?;
        self.encoder.i8(value)
    }
    fn serialize_i16(self, value: i16) -> Result<(), Self::Error> {
        self.encoder.u8(3)?;
        self.encoder.i16(value)
    }
    fn serialize_i32(self, value: i32) -> Result<(), Self::Error> {
        self.encoder.u8(4)?;
        self.encoder.i32(value)
    }
    fn serialize_i64(self, value: i64) -> Result<(), Self::Error> {
        self.encoder.u8(5)?;
        self.encoder.i64(value)
    }
    fn serialize_i128(self, value: i128) -> Result<(), Self::Error> {
        self.encoder.u8(6)?;
        self.encoder.i128(value)
    }
    fn serialize_u8(self, value: u8) -> Result<(), Self::Error> {
        self.encoder.u8(7)?;
        self.encoder.u8(value)
    }
    fn serialize_u16(self, value: u16) -> Result<(), Self::Error> {
        self.encoder.u8(8)?;
        self.encoder.u16(value)
    }
    fn serialize_u32(self, value: u32) -> Result<(), Self::Error> {
        self.encoder.u8(9)?;
        self.encoder.u32(value)
    }
    fn serialize_u64(self, value: u64) -> Result<(), Self::Error> {
        self.encoder.u8(10)?;
        self.encoder.u64(value)
    }
    fn serialize_u128(self, value: u128) -> Result<(), Self::Error> {
        self.encoder.u8(11)?;
        self.encoder.u128(value)
    }
    fn serialize_f32(self, value: f32) -> Result<(), Self::Error> {
        self.encoder.u8(12)?;
        self.encoder.u32(value.to_bits())
    }
    fn serialize_f64(self, value: f64) -> Result<(), Self::Error> {
        self.encoder.u8(13)?;
        self.encoder.u64(value.to_bits())
    }
    fn serialize_char(self, value: char) -> Result<(), Self::Error> {
        self.encoder.u8(14)?;
        self.encoder.u32(value.into())
    }
    fn serialize_str(self, value: &str) -> Result<(), Self::Error> {
        self.encoder.u8(15)?;
        self.encoder.str(value)
    }
    fn serialize_bytes(self, value: &[u8]) -> Result<(), Self::Error> {
        self.encoder.u8(16)?;
        self.encoder.bytes(value)
    }
    fn serialize_none(self) -> Result<(), Self::Error> {
        self.encoder.u8(17)
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), Self::Error> {
        self.encoder.u8(18)?;
        value.serialize(CanonicalSerializer {
            encoder: self.encoder,
        })
    }
    fn serialize_unit(self) -> Result<(), Self::Error> {
        self.encoder.u8(19)
    }
    fn serialize_unit_struct(self, name: &'static str) -> Result<(), Self::Error> {
        self.encoder.u8(20)?;
        self.encoder.str(name)
    }
    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<(), Self::Error> {
        self.encoder.u8(21)?;
        self.encoder.str(name)?;
        self.encoder.u32(variant_index)?;
        self.encoder.str(variant)
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.encoder.u8(22)?;
        self.encoder.str(name)?;
        value.serialize(CanonicalSerializer {
            encoder: self.encoder,
        })
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.encoder.u8(23)?;
        self.encoder.str(name)?;
        self.encoder.u32(variant_index)?;
        self.encoder.str(variant)?;
        value.serialize(CanonicalSerializer {
            encoder: self.encoder,
        })
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.encoder.u8(24)?;
        encode_collection_len(self.encoder, len)?;
        Ok(compound(self.encoder, 25))
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.encoder.u8(26)?;
        encode_collection_len(self.encoder, Some(len))?;
        Ok(compound(self.encoder, 27))
    }
    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.encoder.u8(28)?;
        self.encoder.str(name)?;
        encode_collection_len(self.encoder, Some(len))?;
        Ok(compound(self.encoder, 29))
    }
    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.encoder.u8(30)?;
        self.encoder.str(name)?;
        self.encoder.u32(variant_index)?;
        self.encoder.str(variant)?;
        encode_collection_len(self.encoder, Some(len))?;
        Ok(compound(self.encoder, 31))
    }
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(PitV1EncodingError::Encoding)
    }
    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.encoder.u8(32)?;
        self.encoder.str(name)?;
        encode_collection_len(self.encoder, Some(len))?;
        Ok(compound(self.encoder, 33))
    }
    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.encoder.u8(34)?;
        self.encoder.str(name)?;
        self.encoder.u32(variant_index)?;
        self.encoder.str(variant)?;
        encode_collection_len(self.encoder, Some(len))?;
        Ok(compound(self.encoder, 35))
    }
}

fn compound<'encoder, 'control>(
    encoder: &'encoder mut PitV1CanonicalEncoder<'control>,
    end_tag: u8,
) -> Compound<'encoder, 'control> {
    Compound {
        encoder,
        end_tag,
        elements: 0,
    }
}

fn encode_collection_len(
    encoder: &mut PitV1CanonicalEncoder<'_>,
    len: Option<usize>,
) -> Result<(), PitV1EncodingError> {
    if len.is_some_and(|len| len > MAX_COLLECTION_ELEMENTS) {
        return Err(PitV1EncodingError::LimitExceeded);
    }
    match len {
        Some(len) => {
            encoder.u8(1)?;
            encoder.u64(u64::try_from(len).map_err(|_| PitV1EncodingError::Encoding)?)
        }
        None => encoder.u8(0),
    }
}

impl Compound<'_, '_> {
    fn begin_element(&mut self, tag: u8) -> Result<(), PitV1EncodingError> {
        self.elements = self
            .elements
            .checked_add(1)
            .ok_or(PitV1EncodingError::AccountingOverflow)?;
        if self.elements > MAX_COLLECTION_ELEMENTS {
            return Err(PitV1EncodingError::LimitExceeded);
        }
        self.encoder.u8(tag)
    }
}

impl SerializeSeq for Compound<'_, '_> {
    type Ok = ();
    type Error = PitV1EncodingError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.begin_element(36)?;
        value.serialize(CanonicalSerializer {
            encoder: self.encoder,
        })
    }
    fn end(self) -> Result<(), Self::Error> {
        self.encoder.u8(self.end_tag)
    }
}

impl SerializeTuple for Compound<'_, '_> {
    type Ok = ();
    type Error = PitV1EncodingError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleStruct for Compound<'_, '_> {
    type Ok = ();
    type Error = PitV1EncodingError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleVariant for Compound<'_, '_> {
    type Ok = ();
    type Error = PitV1EncodingError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeStruct for Compound<'_, '_> {
    type Ok = ();
    type Error = PitV1EncodingError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.begin_element(37)?;
        self.encoder.str(key)?;
        value.serialize(CanonicalSerializer {
            encoder: self.encoder,
        })
    }
    fn end(self) -> Result<(), Self::Error> {
        self.encoder.u8(self.end_tag)
    }
}

impl SerializeStructVariant for Compound<'_, '_> {
    type Ok = ();
    type Error = PitV1EncodingError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        SerializeStruct::serialize_field(self, key, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        SerializeStruct::end(self)
    }
}
