//! Deterministic tagged binary serializer used for nested domain payloads.

use std::fmt;

use serde::Serialize;
use serde::ser::{
    Impossible, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
    SerializeTupleStruct, SerializeTupleVariant,
};
use sha2::{Digest as _, Sha256};

use super::super::POINT_IN_TIME_IDENTITY_SCHEMA_VERSION;
use super::super::PointInTimeError;
use super::super::retained::OperationControl;
use crate::Sha256Digest;

pub(crate) struct CanonicalEncoder<'control> {
    hasher: Sha256,
    bytes: Option<Vec<u8>>,
    encoded_len: usize,
    control: &'control mut OperationControl,
}

impl<'control> CanonicalEncoder<'control> {
    pub(crate) fn new(
        domain: &str,
        control: &'control mut OperationControl,
    ) -> Result<Self, CanonicalEncodingError> {
        let mut value = Self {
            hasher: Sha256::new(),
            bytes: None,
            encoded_len: 0,
            control,
        };
        value.write_header(domain)?;
        Ok(value)
    }

    pub(crate) fn collecting_exact(
        domain: &str,
        expected_len: usize,
        control: &'control mut OperationControl,
    ) -> Result<Self, CanonicalEncodingError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(expected_len)
            .map_err(|_| CanonicalEncodingError::AllocationFailure)?;
        if bytes.capacity() != expected_len {
            return Err(CanonicalEncodingError::Encoding);
        }
        let mut value = Self {
            hasher: Sha256::new(),
            bytes: Some(bytes),
            encoded_len: 0,
            control,
        };
        value.write_header(domain)?;
        Ok(value)
    }

    fn write_header(&mut self, domain: &str) -> Result<(), CanonicalEncodingError> {
        self.raw(b"MSQPIT")?;
        self.u16(POINT_IN_TIME_IDENTITY_SCHEMA_VERSION)?;
        self.str(domain)
    }

    pub(crate) fn u8(&mut self, value: u8) -> Result<(), CanonicalEncodingError> {
        self.raw(&[value])
    }

    pub(crate) fn u16(&mut self, value: u16) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn u32(&mut self, value: u32) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn u64(&mut self, value: u64) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn u128(&mut self, value: u128) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn i8(&mut self, value: i8) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn i16(&mut self, value: i16) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn i32(&mut self, value: i32) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn i64(&mut self, value: i64) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn i128(&mut self, value: i128) -> Result<(), CanonicalEncodingError> {
        self.raw(&value.to_le_bytes())
    }

    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<(), CanonicalEncodingError> {
        self.u64(u64::try_from(value.len()).map_err(|_| CanonicalEncodingError::Encoding)?)?;
        self.raw(value)
    }

    pub(crate) fn str(&mut self, value: &str) -> Result<(), CanonicalEncodingError> {
        self.bytes(value.as_bytes())
    }

    pub(crate) fn option_str(&mut self, value: Option<&str>) -> Result<(), CanonicalEncodingError> {
        match value {
            Some(value) => {
                self.u8(1)?;
                self.str(value)
            }
            None => self.u8(0),
        }
    }

    pub(crate) fn digest(&mut self, value: Sha256Digest) -> Result<(), CanonicalEncodingError> {
        self.bytes(&value.bytes())
    }

    pub(crate) fn serializable<T: Serialize>(
        &mut self,
        value: &T,
    ) -> Result<(), CanonicalEncodingError> {
        value.serialize(CanonicalSerializer { encoder: self })
    }

    fn raw(&mut self, value: &[u8]) -> Result<(), CanonicalEncodingError> {
        self.control.observe().map_err(control_error)?;
        let encoded_len = self
            .encoded_len
            .checked_add(value.len())
            .ok_or(CanonicalEncodingError::AccountingOverflow)?;
        if self
            .bytes
            .as_ref()
            .is_some_and(|bytes| encoded_len > bytes.capacity())
        {
            return Err(CanonicalEncodingError::Encoding);
        }
        self.hasher.update(value);
        if let Some(bytes) = self.bytes.as_mut() {
            bytes.extend_from_slice(value);
        }
        self.encoded_len = encoded_len;
        Ok(())
    }

    pub(crate) fn finish(self) -> Sha256Digest {
        Sha256Digest::new(self.hasher.finalize().into())
    }

    pub(crate) fn finish_with_len(self) -> (Sha256Digest, usize) {
        (
            Sha256Digest::new(self.hasher.finalize().into()),
            self.encoded_len,
        )
    }

    pub(crate) fn finish_with_bytes(
        self,
    ) -> Result<(Sha256Digest, Vec<u8>, usize), CanonicalEncodingError> {
        let bytes = self.bytes.ok_or(CanonicalEncodingError::Encoding)?;
        Ok((
            Sha256Digest::new(self.hasher.finalize().into()),
            bytes,
            self.encoded_len,
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CanonicalEncodingError {
    Encoding,
    AllocationFailure,
    AccountingOverflow,
    Cancelled,
    DeadlineExceeded,
}

impl fmt::Display for CanonicalEncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Encoding => "canonical encoding failed",
            Self::AllocationFailure => "canonical encoding allocation failed",
            Self::AccountingOverflow => "canonical encoding accounting overflowed",
            Self::Cancelled => "canonical encoding cancelled",
            Self::DeadlineExceeded => "canonical encoding deadline exceeded",
        })
    }
}

impl std::error::Error for CanonicalEncodingError {}

impl serde::ser::Error for CanonicalEncodingError {
    fn custom<T: fmt::Display>(_message: T) -> Self {
        Self::Encoding
    }
}

fn control_error(error: PointInTimeError<'_>) -> CanonicalEncodingError {
    match error {
        PointInTimeError::Cancelled => CanonicalEncodingError::Cancelled,
        PointInTimeError::DeadlineExceeded => CanonicalEncodingError::DeadlineExceeded,
        _ => CanonicalEncodingError::Encoding,
    }
}

struct CanonicalSerializer<'encoder, 'control> {
    encoder: &'encoder mut CanonicalEncoder<'control>,
}

struct Compound<'encoder, 'control> {
    encoder: &'encoder mut CanonicalEncoder<'control>,
    end_tag: u8,
}

impl<'encoder, 'control> serde::Serializer for CanonicalSerializer<'encoder, 'control> {
    type Ok = ();
    type Error = CanonicalEncodingError;
    type SerializeSeq = Compound<'encoder, 'control>;
    type SerializeTuple = Compound<'encoder, 'control>;
    type SerializeTupleStruct = Compound<'encoder, 'control>;
    type SerializeTupleVariant = Compound<'encoder, 'control>;
    type SerializeMap = Impossible<(), CanonicalEncodingError>;
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
        Ok(Compound {
            encoder: self.encoder,
            end_tag: 25,
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.encoder.u8(26)?;
        encode_collection_len(self.encoder, Some(len))?;
        Ok(Compound {
            encoder: self.encoder,
            end_tag: 27,
        })
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.encoder.u8(28)?;
        self.encoder.str(name)?;
        encode_collection_len(self.encoder, Some(len))?;
        Ok(Compound {
            encoder: self.encoder,
            end_tag: 29,
        })
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
        Ok(Compound {
            encoder: self.encoder,
            end_tag: 31,
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Err(CanonicalEncodingError::Encoding)
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.encoder.u8(32)?;
        self.encoder.str(name)?;
        encode_collection_len(self.encoder, Some(len))?;
        Ok(Compound {
            encoder: self.encoder,
            end_tag: 33,
        })
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
        Ok(Compound {
            encoder: self.encoder,
            end_tag: 35,
        })
    }
}

fn encode_collection_len(
    encoder: &mut CanonicalEncoder<'_>,
    len: Option<usize>,
) -> Result<(), CanonicalEncodingError> {
    match len {
        Some(len) => {
            encoder.u8(1)?;
            encoder.u64(u64::try_from(len).map_err(|_| CanonicalEncodingError::Encoding)?)
        }
        None => encoder.u8(0),
    }
}

impl SerializeSeq for Compound<'_, '_> {
    type Ok = ();
    type Error = CanonicalEncodingError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.encoder.u8(36)?;
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
    type Error = CanonicalEncodingError;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleStruct for Compound<'_, '_> {
    type Ok = ();
    type Error = CanonicalEncodingError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeTupleVariant for Compound<'_, '_> {
    type Ok = ();
    type Error = CanonicalEncodingError;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        SerializeSeq::end(self)
    }
}

impl SerializeStruct for Compound<'_, '_> {
    type Ok = ();
    type Error = CanonicalEncodingError;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.encoder.u8(37)?;
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
    type Error = CanonicalEncodingError;
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
