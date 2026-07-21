//! Fixed, versioned framing shared by the parent sink and journal helper.

use std::io::{self, Read, Write};

use sha2::{Digest, Sha256};

use crate::raw_record::MAX_SERIALIZED_RECORD_BYTES;

pub(super) const PROTOCOL_VERSION: u16 = 1;
pub(super) const MAX_PROTOCOL_PAYLOAD_BYTES: usize = MAX_SERIALIZED_RECORD_BYTES;
const MAGIC: [u8; 4] = *b"MSCP";
const HEADER_BYTES: usize = 4 + 2 + 1 + 1 + 8 + 4 + 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum MessageKind {
    Ready = 1,
    Append = 2,
    Flush = 3,
    Shutdown = 4,
    Acknowledged = 5,
    Rejected = 6,
}

impl TryFrom<u8> for MessageKind {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Ready),
            2 => Ok(Self::Append),
            3 => Ok(Self::Flush),
            4 => Ok(Self::Shutdown),
            5 => Ok(Self::Acknowledged),
            6 => Ok(Self::Rejected),
            _ => Err(ProtocolError::UnknownKind),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Header {
    pub(super) kind: MessageKind,
    pub(super) sequence: u64,
    pub(super) payload_bytes: u32,
    pub(super) digest: [u8; 32],
}

impl Header {
    pub(super) fn try_new(
        kind: MessageKind,
        sequence: u64,
        payload_bytes: usize,
        digest: [u8; 32],
    ) -> Result<Self, ProtocolError> {
        if payload_bytes > MAX_PROTOCOL_PAYLOAD_BYTES {
            return Err(ProtocolError::PayloadTooLarge);
        }
        Ok(Self {
            kind,
            sequence,
            payload_bytes: u32::try_from(payload_bytes)
                .map_err(|_error| ProtocolError::PayloadTooLarge)?,
            digest,
        })
    }

    pub(super) fn write_to(self, writer: &mut impl Write) -> Result<(), ProtocolError> {
        let mut bytes = [0_u8; HEADER_BYTES];
        bytes[0..4].copy_from_slice(&MAGIC);
        bytes[4..6].copy_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        bytes[6] = self.kind as u8;
        bytes[7] = 0;
        bytes[8..16].copy_from_slice(&self.sequence.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.payload_bytes.to_be_bytes());
        bytes[20..52].copy_from_slice(&self.digest);
        writer.write_all(&bytes).map_err(ProtocolError::Io)
    }

    pub(super) fn read_from(reader: &mut impl Read) -> Result<Self, ProtocolError> {
        let mut bytes = [0_u8; HEADER_BYTES];
        reader.read_exact(&mut bytes).map_err(ProtocolError::Io)?;
        if bytes[0..4] != MAGIC || bytes[7] != 0 {
            return Err(ProtocolError::InvalidHeader);
        }
        let version = u16::from_be_bytes(
            bytes[4..6]
                .try_into()
                .map_err(|_error| ProtocolError::InvalidHeader)?,
        );
        if version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion);
        }
        let kind = MessageKind::try_from(bytes[6])?;
        let sequence = u64::from_be_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_error| ProtocolError::InvalidHeader)?,
        );
        let payload_bytes = u32::from_be_bytes(
            bytes[16..20]
                .try_into()
                .map_err(|_error| ProtocolError::InvalidHeader)?,
        );
        let payload_length =
            usize::try_from(payload_bytes).map_err(|_error| ProtocolError::PayloadTooLarge)?;
        if payload_length > MAX_PROTOCOL_PAYLOAD_BYTES {
            return Err(ProtocolError::PayloadTooLarge);
        }
        let digest = bytes[20..52]
            .try_into()
            .map_err(|_error| ProtocolError::InvalidHeader)?;
        Ok(Self {
            kind,
            sequence,
            payload_bytes,
            digest,
        })
    }
}

pub(super) fn startup_digest(nonce: &[u8; 16]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/capture-helper-startup/v1\0");
    digest.update(nonce);
    digest.finalize().into()
}

pub(super) fn control_digest(kind: MessageKind, sequence: u64) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/capture-helper-control/v1\0");
    digest.update([kind as u8]);
    digest.update(sequence.to_be_bytes());
    digest.finalize().into()
}

#[derive(Debug)]
pub(super) struct CountingDigestWriter {
    bytes: usize,
    digest: Sha256,
}

impl CountingDigestWriter {
    pub(super) fn new() -> Self {
        Self {
            bytes: 0,
            digest: Sha256::new(),
        }
    }

    pub(super) fn finish(self) -> (usize, [u8; 32]) {
        (self.bytes, self.digest.finalize().into())
    }
}

impl Write for CountingDigestWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("capture helper payload length overflowed"))?;
        if next > MAX_PROTOCOL_PAYLOAD_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "capture helper payload exceeded its fixed bound",
            ));
        }
        self.digest.update(buffer);
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct VerifyingForwardWriter<'a, W> {
    writer: &'a mut W,
    expected_bytes: usize,
    bytes: usize,
    digest: Sha256,
}

impl<'a, W> VerifyingForwardWriter<'a, W> {
    pub(super) fn new(writer: &'a mut W, expected_bytes: usize) -> Self {
        Self {
            writer,
            expected_bytes,
            bytes: 0,
            digest: Sha256::new(),
        }
    }

    pub(super) fn finish(self) -> (usize, [u8; 32]) {
        (self.bytes, self.digest.finalize().into())
    }
}

impl<W: Write> Write for VerifyingForwardWriter<'_, W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("capture helper payload length overflowed"))?;
        if next > self.expected_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "capture helper payload exceeded its declared length",
            ));
        }
        self.writer.write_all(buffer)?;
        self.digest.update(buffer);
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[derive(Debug)]
pub(super) enum ProtocolError {
    Io(io::Error),
    InvalidHeader,
    UnsupportedVersion,
    UnknownKind,
    PayloadTooLarge,
}
