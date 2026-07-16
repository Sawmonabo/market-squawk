//! CRC-framed current and legacy raw-capture journals.
//!
//! `MEJ1` and `MSJ1` preserve source-faithful diagnostic bytes, raw connection identity, and
//! receive metadata. They do not persist the out-of-band [`crate::CaptureGenerationKey`] or live
//! authority state. Journal replay is therefore permanently execution-ineligible and cannot
//! reconstruct current capture, registry, sequence, checksum, freshness, or venue authority.

use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    num::TryFromIntError,
    path::{Path, PathBuf},
};

use crc32fast::Hasher;
use fs2::FileExt;
use thiserror::Error;

use crate::{RawCaptureRecord, RawCaptureRecordError, raw_record::MAX_SERIALIZED_RECORD_BYTES};

const CURRENT_MAGIC: &[u8; 4] = b"MSJ1";
const MAX_RECORD_BYTES: usize = MAX_SERIALIZED_RECORD_BYTES;
const DEFAULT_MAX_RECORDS: usize = 1_000_000;
const DEFAULT_MAX_AGGREGATE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JournalFormat {
    LegacyMej1,
    MarketSquawkMsj1,
}

/// Explicit authority limitation of every committed `MEJ1`/`MSJ1` replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalReplayAuthority {
    /// The format does not contain the out-of-band evidence needed for current live authority.
    UnavailableByFormat,
}

impl TryFrom<[u8; 4]> for JournalFormat {
    type Error = JournalError;

    fn try_from(value: [u8; 4]) -> Result<Self, Self::Error> {
        match &value {
            b"MEJ1" => Ok(Self::LegacyMej1),
            b"MSJ1" => Ok(Self::MarketSquawkMsj1),
            _ => Err(JournalError::UnsupportedMagic(value)),
        }
    }
}

/// Checked journal read/write failure.
#[derive(Debug, Error)]
pub enum JournalError {
    /// Header magic is neither the committed legacy nor current format.
    #[error("unsupported journal magic: {0:?}")]
    UnsupportedMagic([u8; 4]),
    /// Writers create and append only `.msj` files.
    #[error("new and writable journals must use the .msj extension")]
    InvalidWriterExtension,
    /// Source-derived journal filenames must be one validated portable component.
    #[error("journal source filename is invalid")]
    InvalidSourceFilename,
    /// A committed legacy journal is read-only.
    #[error("legacy journal is read-only; migrate to an MSJ1 journal before appending")]
    LegacyFormatReadOnly,
    /// Writable journal endpoints may not be symbolic links.
    #[error("journal endpoint must not be a symbolic link")]
    SymlinkNotAllowed,
    /// A second writer already owns the exclusive file lock.
    #[error("journal {path} already has an active writer")]
    AlreadyLocked {
        /// Locked journal path.
        path: PathBuf,
    },
    /// Bounded collection exceeded its record count.
    #[error("journal collection exceeds record limit {limit}")]
    RecordLimitExceeded {
        /// Configured record limit.
        limit: usize,
    },
    /// Bounded collection exceeded aggregate framed bytes.
    #[error("journal collection exceeds byte limit {limit}")]
    AggregateLimitExceeded {
        /// Configured aggregate byte limit.
        limit: u64,
    },
    /// Filesystem operation failed with path-safe context.
    #[error("{context}: {source}")]
    Io {
        /// Non-secret operation context.
        context: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// CRC, length, or framing is invalid.
    #[error("{0}")]
    InvalidRecord(String),
    /// JSON payload does not match the committed raw-envelope wire.
    #[error("invalid journal payload: {0}")]
    Json(#[from] serde_json::Error),
    /// An in-memory record violated the committed raw-envelope invariants.
    #[error("invalid raw capture record: {0}")]
    InvalidRawRecord(#[from] RawCaptureRecordError),
    /// Record length cannot be represented by the committed frame.
    #[error("journal record length overflow: {0}")]
    LengthOverflow(#[from] TryFromIntError),
    /// Serialized record exceeds the committed per-record bound.
    #[error("journal record is {bytes} bytes; maximum is {max}")]
    RecordTooLarge {
        /// Serialized byte count.
        bytes: usize,
        /// Maximum serialized byte count.
        max: usize,
    },
}

impl JournalError {
    pub(crate) fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

/// Single-owner writer for the current `MSJ1/.msj` format.
#[derive(Debug)]
pub struct JournalWriter {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl JournalWriter {
    pub(crate) fn from_open_file(
        path: PathBuf,
        file: File,
        parent_directory: Option<File>,
    ) -> Result<Self, JournalError> {
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("msj") {
            return Err(JournalError::InvalidWriterExtension);
        }
        if let Err(source) = FileExt::try_lock_exclusive(&file) {
            if source.kind() == std::io::ErrorKind::WouldBlock {
                return Err(JournalError::AlreadyLocked { path });
            }
            return Err(JournalError::io("failed to lock journal", source));
        }
        let is_new = file
            .metadata()
            .map_err(|source| JournalError::io("failed to inspect journal metadata", source))?
            .len()
            == 0;
        if !is_new && validate_existing_file(&file)? == JournalFormat::LegacyMej1 {
            return Err(JournalError::LegacyFormatReadOnly);
        }

        let mut writer = BufWriter::new(file);
        if is_new {
            writer
                .write_all(CURRENT_MAGIC)
                .map_err(|source| JournalError::io("failed to write journal header", source))?;
            writer
                .flush()
                .map_err(|source| JournalError::io("failed to flush journal header", source))?;
            writer.get_ref().sync_all().map_err(|source| {
                JournalError::io("failed to synchronize journal header", source)
            })?;
            parent_directory
                .ok_or_else(|| {
                    JournalError::InvalidRecord("journal path has no parent directory".to_owned())
                })?
                .sync_all()
                .map_err(|source| {
                    JournalError::io("failed to synchronize journal directory handle", source)
                })?;
        }
        Ok(Self { path, writer })
    }

    /// Appends one CRC-framed compatibility record to the buffer.
    pub fn append(&mut self, record: &RawCaptureRecord) -> Result<(), JournalError> {
        record.validate_compatibility()?;
        let payload = serde_json::to_vec(record)?;
        if payload.len() > MAX_RECORD_BYTES {
            return Err(JournalError::RecordTooLarge {
                bytes: payload.len(),
                max: MAX_RECORD_BYTES,
            });
        }
        let length = u32::try_from(payload.len())?;
        let crc = crc32fast::hash(&payload);
        self.writer
            .write_all(&length.to_le_bytes())
            .map_err(|source| JournalError::io("failed to write journal length", source))?;
        self.writer
            .write_all(&crc.to_le_bytes())
            .map_err(|source| JournalError::io("failed to write journal checksum", source))?;
        self.writer
            .write_all(&payload)
            .map_err(|source| JournalError::io("failed to write journal payload", source))?;
        Ok(())
    }

    /// Flushes buffered frames and synchronizes file data.
    pub fn flush(&mut self) -> Result<(), JournalError> {
        self.writer
            .flush()
            .map_err(|source| JournalError::io("failed to flush journal", source))?;
        self.writer
            .get_ref()
            .sync_data()
            .map_err(|source| JournalError::io("failed to synchronize journal", source))
    }

    /// Returns the owned journal path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Streaming reader for committed `MSJ1` and legacy `MEJ1` records.
#[derive(Debug)]
pub struct JournalReader<R = File> {
    reader: BufReader<R>,
    offset: u64,
    format: Option<JournalFormat>,
}

impl JournalReader<File> {
    /// Opens a journal and validates its header.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        let path = path.as_ref();
        let file = File::open(path).map_err(|source| {
            JournalError::io(format!("failed to open journal {}", path.display()), source)
        })?;
        let mut reader = Self::new(file);
        reader.ensure_format()?;
        Ok(reader)
    }
}

impl<R: Read> JournalReader<R> {
    /// Wraps a readable journal stream.
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            offset: 4,
            format: None,
        }
    }

    /// Returns the permanent execution-authority limitation of this diagnostic format.
    pub const fn replay_authority(&self) -> JournalReplayAuthority {
        JournalReplayAuthority::UnavailableByFormat
    }

    fn ensure_format(&mut self) -> Result<JournalFormat, JournalError> {
        if let Some(format) = self.format {
            return Ok(format);
        }
        let mut magic = [0_u8; 4];
        self.reader
            .read_exact(&mut magic)
            .map_err(|source| JournalError::io("truncated journal header", source))?;
        let format = JournalFormat::try_from(magic)?;
        self.format = Some(format);
        Ok(format)
    }

    /// Reads and validates the next record without collecting the complete journal.
    pub fn next_record(&mut self) -> Result<Option<RawCaptureRecord>, JournalError> {
        self.next_record_bounded(u64::MAX)
    }

    fn next_record_bounded(
        &mut self,
        max_framed_bytes: u64,
    ) -> Result<Option<RawCaptureRecord>, JournalError> {
        self.ensure_format()?;
        let mut length_bytes = [0_u8; 4];
        let first = self
            .reader
            .read(&mut length_bytes[..1])
            .map_err(|source| JournalError::io("failed to read journal record length", source))?;
        if first == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut length_bytes[1..])
            .map_err(|source| JournalError::io("truncated record length", source))?;
        let mut crc_bytes = [0_u8; 4];
        self.reader
            .read_exact(&mut crc_bytes)
            .map_err(|source| JournalError::io("truncated record checksum", source))?;
        let length = u32::from_le_bytes(length_bytes) as usize;
        if length > MAX_RECORD_BYTES {
            return Err(JournalError::InvalidRecord(format!(
                "journal record at offset {} is too large: {length}",
                self.offset
            )));
        }
        let framed_bytes = 8_u64.checked_add(u64::try_from(length)?).ok_or_else(|| {
            JournalError::InvalidRecord("journal frame length overflow".to_owned())
        })?;
        if framed_bytes > max_framed_bytes {
            return Err(JournalError::AggregateLimitExceeded {
                limit: max_framed_bytes,
            });
        }
        let mut payload = vec![0_u8; length];
        self.reader
            .read_exact(&mut payload)
            .map_err(|source| JournalError::io("truncated record payload", source))?;
        let expected_crc = u32::from_le_bytes(crc_bytes);
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        if actual_crc != expected_crc {
            return Err(JournalError::InvalidRecord(format!(
                "journal checksum mismatch at offset {}: expected={expected_crc}, actual={actual_crc}",
                self.offset
            )));
        }
        self.offset = self
            .offset
            .checked_add(framed_bytes)
            .ok_or_else(|| JournalError::InvalidRecord("journal offset overflow".to_owned()))?;
        Ok(Some(serde_json::from_slice(&payload)?))
    }

    /// Collects using conservative default record and byte limits.
    pub fn read_all(self) -> Result<Vec<RawCaptureRecord>, JournalError> {
        self.read_all_bounded(DEFAULT_MAX_RECORDS, DEFAULT_MAX_AGGREGATE_BYTES)
    }

    /// Collects under explicit record-count and aggregate framed-byte limits.
    pub fn read_all_bounded(
        mut self,
        max_records: usize,
        max_aggregate_bytes: u64,
    ) -> Result<Vec<RawCaptureRecord>, JournalError> {
        let mut records = Vec::new();
        loop {
            let has_record = !self
                .reader
                .fill_buf()
                .map_err(|source| JournalError::io("failed to inspect journal stream", source))?
                .is_empty();
            if !has_record {
                return Ok(records);
            }
            if records.len() >= max_records {
                return Err(JournalError::RecordLimitExceeded { limit: max_records });
            }
            let consumed = self.offset.saturating_sub(4);
            let remaining = max_aggregate_bytes.saturating_sub(consumed);
            let record = self.next_record_bounded(remaining)?.ok_or_else(|| {
                JournalError::InvalidRecord("journal stream changed while reading".to_owned())
            })?;
            records.push(record);
        }
    }
}

fn validate_existing_file(file: &File) -> Result<JournalFormat, JournalError> {
    let mut file = file
        .try_clone()
        .map_err(|source| JournalError::io("failed to clone journal handle", source))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| JournalError::io("failed to seek journal handle", source))?;
    let mut reader = JournalReader::new(file);
    validate_existing_reader(&mut reader)
}

fn validate_existing_reader<R: Read>(
    reader: &mut JournalReader<R>,
) -> Result<JournalFormat, JournalError> {
    let format = reader.ensure_format()?;
    let mut records = 0_usize;
    loop {
        let has_record = !reader
            .reader
            .fill_buf()
            .map_err(|source| JournalError::io("failed to inspect journal stream", source))?
            .is_empty();
        if !has_record {
            break;
        }
        let _record = reader.next_record()?.ok_or_else(|| {
            JournalError::InvalidRecord("journal stream changed while validating".to_owned())
        })?;
        records = records.checked_add(1).ok_or_else(|| {
            JournalError::InvalidRecord("journal record count overflow".to_owned())
        })?;
    }
    Ok(format)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{
        DEFAULT_MAX_AGGREGATE_BYTES, JournalFormat, JournalReader, validate_existing_reader,
    };

    fn valid_frame() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let payload = br#"{"event_id":"00000000-0000-0000-0000-000000000001","source":"fixture","connection_id":"00000000-0000-0000-0000-000000000002","source_sequence":1,"exchange_at":null,"received_at":"2026-07-15T20:00:01Z","payload":[]}"#;
        let mut frame = Vec::new();
        frame.extend_from_slice(&u32::try_from(payload.len())?.to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
        frame.extend_from_slice(payload);
        Ok(frame)
    }

    #[test]
    fn writer_startup_validation_streams_past_the_collection_byte_ceiling()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut reader = JournalReader::new(Cursor::new(valid_frame()?));
        reader.format = Some(JournalFormat::MarketSquawkMsj1);
        reader.offset = DEFAULT_MAX_AGGREGATE_BYTES
            .checked_add(1)
            .ok_or("invalid fixed test offset")?;

        let format = validate_existing_reader(&mut reader)?;

        assert_eq!(format, JournalFormat::MarketSquawkMsj1);
        assert!(reader.offset > DEFAULT_MAX_AGGREGATE_BYTES);
        Ok(())
    }

    #[test]
    fn writer_startup_validation_still_rejects_a_torn_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut frame = valid_frame()?;
        let _truncated_byte = frame.pop().ok_or("fixture frame was empty")?;
        let mut reader = JournalReader::new(Cursor::new(frame));
        reader.format = Some(JournalFormat::MarketSquawkMsj1);

        assert!(validate_existing_reader(&mut reader).is_err());
        Ok(())
    }
}
