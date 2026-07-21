//! Child-side journal ownership and bounded request processing.

use std::ffi::OsString;
use std::io::{BufReader, BufWriter, Read, Write};

use sha2::{Digest, Sha256};
use thiserror::Error;

use super::protocol::{
    Header, MAX_PROTOCOL_PAYLOAD_BYTES, MessageKind, ProtocolError, control_digest, startup_digest,
};
use crate::{LocalPaths, RawCaptureRecord};

#[cfg(all(feature = "capture-test", debug_assertions))]
const TEST_MODE_ENVIRONMENT: &str = "MARKET_SQUAWK_CAPTURE_HELPER_TEST_MODE";
#[cfg(all(feature = "capture-test", debug_assertions))]
const TEST_STALL_AFTER_APPEND: &str = "stall-after-append";

pub fn run_capture_helper(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<(), CaptureHelperError> {
    let mut arguments = arguments.into_iter();
    let root = arguments.next().ok_or(CaptureHelperError::Arguments)?;
    let source = arguments.next().ok_or(CaptureHelperError::Arguments)?;
    let nonce = arguments.next().ok_or(CaptureHelperError::Arguments)?;
    if arguments.next().is_some() {
        return Err(CaptureHelperError::Arguments);
    }
    let source = source
        .into_string()
        .map_err(|_source| CaptureHelperError::Arguments)?;
    let nonce = nonce
        .into_string()
        .map_err(|_nonce| CaptureHelperError::Arguments)?;
    let nonce = uuid::Uuid::parse_str(&nonce).map_err(|_error| CaptureHelperError::Arguments)?;
    let startup = startup_digest(nonce.as_bytes());
    let mut output = BufWriter::new(std::io::stdout().lock());
    let paths = match LocalPaths::prepare(std::path::PathBuf::from(root)) {
        Ok(paths) => paths,
        Err(_error) => {
            reject(&mut output, 0, startup)?;
            return Err(CaptureHelperError::Journal);
        }
    };
    let mut journal = match paths.open_journal_writer(&source) {
        Ok(journal) => journal,
        Err(_error) => {
            reject(&mut output, 0, startup)?;
            return Err(CaptureHelperError::Journal);
        }
    };
    acknowledge(&mut output, MessageKind::Ready, 0, startup)?;

    let mut input = BufReader::new(std::io::stdin().lock());
    let mut expected_sequence = 1_u64;
    loop {
        let header = Header::read_from(&mut input)?;
        if header.sequence != expected_sequence {
            reject(&mut output, header.sequence, header.digest)?;
            return Err(CaptureHelperError::Sequence);
        }
        match header.kind {
            MessageKind::Append => {
                if header.payload_bytes == 0 {
                    reject(&mut output, header.sequence, header.digest)?;
                    return Err(CaptureHelperError::Payload);
                }
                let length = usize::try_from(header.payload_bytes)
                    .map_err(|_error| CaptureHelperError::Payload)?;
                if length > MAX_PROTOCOL_PAYLOAD_BYTES {
                    return Err(CaptureHelperError::Payload);
                }
                let mut payload = Vec::new();
                payload
                    .try_reserve_exact(length)
                    .map_err(|_error| CaptureHelperError::Allocation)?;
                payload.resize(length, 0);
                input.read_exact(&mut payload)?;
                let observed_digest: [u8; 32] = Sha256::digest(&payload).into();
                if observed_digest != header.digest {
                    reject(&mut output, header.sequence, header.digest)?;
                    return Err(CaptureHelperError::Digest);
                }
                if should_stall_after_append() {
                    loop {
                        std::thread::park();
                    }
                }
                let record: RawCaptureRecord = serde_json::from_slice(&payload)
                    .map_err(|_error| CaptureHelperError::Payload)?;
                journal
                    .append(&record)
                    .map_err(|_error| CaptureHelperError::Journal)?;
                acknowledge(
                    &mut output,
                    MessageKind::Acknowledged,
                    header.sequence,
                    header.digest,
                )?;
            }
            MessageKind::Flush => {
                validate_control(&header, MessageKind::Flush)?;
                journal
                    .flush()
                    .map_err(|_error| CaptureHelperError::Journal)?;
                acknowledge(
                    &mut output,
                    MessageKind::Acknowledged,
                    header.sequence,
                    header.digest,
                )?;
            }
            MessageKind::Shutdown => {
                validate_control(&header, MessageKind::Shutdown)?;
                journal
                    .flush()
                    .map_err(|_error| CaptureHelperError::Journal)?;
                acknowledge(
                    &mut output,
                    MessageKind::Acknowledged,
                    header.sequence,
                    header.digest,
                )?;
                return Ok(());
            }
            MessageKind::Ready | MessageKind::Acknowledged | MessageKind::Rejected => {
                reject(&mut output, header.sequence, header.digest)?;
                return Err(CaptureHelperError::Protocol);
            }
        }
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(CaptureHelperError::Sequence)?;
    }
}

fn validate_control(header: &Header, kind: MessageKind) -> Result<(), CaptureHelperError> {
    if header.payload_bytes != 0
        || header.digest != control_digest(kind, header.sequence)
        || header.kind != kind
    {
        return Err(CaptureHelperError::Protocol);
    }
    Ok(())
}

fn acknowledge(
    output: &mut impl Write,
    kind: MessageKind,
    sequence: u64,
    digest: [u8; 32],
) -> Result<(), CaptureHelperError> {
    Header::try_new(kind, sequence, 0, digest)?.write_to(output)?;
    output.flush().map_err(CaptureHelperError::Io)
}

fn reject(
    output: &mut impl Write,
    sequence: u64,
    digest: [u8; 32],
) -> Result<(), CaptureHelperError> {
    acknowledge(output, MessageKind::Rejected, sequence, digest)
}

fn should_stall_after_append() -> bool {
    #[cfg(all(feature = "capture-test", debug_assertions))]
    {
        std::env::var(TEST_MODE_ENVIRONMENT).is_ok_and(|mode| mode == TEST_STALL_AFTER_APPEND)
    }
    #[cfg(not(all(feature = "capture-test", debug_assertions)))]
    {
        false
    }
}

#[cfg(all(feature = "capture-test", debug_assertions))]
pub(super) const fn test_mode_environment() -> &'static str {
    TEST_MODE_ENVIRONMENT
}

#[cfg(all(feature = "capture-test", debug_assertions))]
pub(super) const fn test_stall_after_append() -> &'static str {
    TEST_STALL_AFTER_APPEND
}

#[derive(Debug, Error)]
pub enum CaptureHelperError {
    #[error("capture helper arguments are invalid")]
    Arguments,
    #[error("capture helper bounded payload allocation failed")]
    Allocation,
    #[error("capture helper payload digest did not match its header")]
    Digest,
    #[error("capture helper protocol I/O failed")]
    Io(#[source] std::io::Error),
    #[error("capture helper journal operation failed")]
    Journal,
    #[error("capture helper payload is invalid")]
    Payload,
    #[error("capture helper protocol validation failed")]
    Protocol,
    #[error("capture helper sequence validation failed")]
    Sequence,
}

impl From<std::io::Error> for CaptureHelperError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for CaptureHelperError {
    fn from(error: ProtocolError) -> Self {
        match error {
            ProtocolError::Io(error) => Self::Io(error),
            ProtocolError::InvalidHeader
            | ProtocolError::UnsupportedVersion
            | ProtocolError::UnknownKind
            | ProtocolError::PayloadTooLarge => Self::Protocol,
        }
    }
}
