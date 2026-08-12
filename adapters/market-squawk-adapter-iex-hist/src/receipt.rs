use std::fmt;

use crc32fast::Hasher as Crc32;
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::model::Sha256Digest;
use crate::planning::ColdJobPlan;

const MAX_ETAG_BYTES: usize = 256;
const GZIP_HEADER_BYTES: usize = 10;
const GZIP_TRAILER_BYTES: usize = 8;

/// Exact metadata for the selected compressed-file response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CaptureResponseMetadata {
    /// Final response URL; redirects must already have been rejected by the transport owner.
    pub response_url: String,
    /// HTTP status, required to be `200`.
    pub status: u16,
    /// HTTP content length, required and equal to the catalog-advertised bytes.
    pub content_length: u64,
    /// HTTP content encoding; only absent or `identity` is admitted because the object is gzip.
    pub content_encoding: Option<String>,
    /// Optional bounded provider entity tag.
    pub etag: Option<String>,
    /// Local response-header time in Unix nanoseconds.
    pub response_started_at_unix_nanos: i64,
}

/// Byte-exact compressed-object and decompressed-PCAP receipt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PcapMaterializationReceipt {
    /// Parent cold plan identity.
    pub(crate) plan_sha256: Sha256Digest,
    /// Parent selected descriptor identity.
    pub(crate) selected_file_identity: Sha256Digest,
    /// SHA-256 of exact compressed `.pcap.gz` bytes.
    pub(crate) compressed_sha256: Sha256Digest,
    /// Exact compressed byte count, equal to catalog metadata.
    pub(crate) compressed_bytes: u64,
    /// SHA-256 of exact decompressed PCAP bytes.
    pub(crate) pcap_sha256: Sha256Digest,
    /// Exact decompressed PCAP byte count.
    pub(crate) pcap_bytes: u64,
    /// Gzip trailer CRC-32 verified against all streamed PCAP bytes.
    pub(crate) gzip_crc32: u32,
    /// Response entity tag when supplied.
    pub(crate) etag: Option<String>,
    /// Local response-header time.
    pub(crate) response_started_at_unix_nanos: i64,
    /// Local completion time after both streams and gzip trailer validation.
    pub(crate) completed_at_unix_nanos: i64,
    /// Composite content identity for downstream raw/canonical lineage.
    pub(crate) receipt_sha256: Sha256Digest,
}

impl PcapMaterializationReceipt {
    /// Returns the composite raw-capture receipt identity.
    #[must_use]
    pub const fn receipt_sha256(&self) -> Sha256Digest {
        self.receipt_sha256
    }

    /// Returns the exact compressed-object digest.
    #[must_use]
    pub const fn compressed_sha256(&self) -> Sha256Digest {
        self.compressed_sha256
    }

    /// Returns exact compressed bytes.
    #[must_use]
    pub const fn compressed_bytes(&self) -> u64 {
        self.compressed_bytes
    }

    /// Returns the exact decompressed PCAP digest.
    #[must_use]
    pub const fn pcap_sha256(&self) -> Sha256Digest {
        self.pcap_sha256
    }

    /// Returns exact decompressed PCAP bytes.
    #[must_use]
    pub const fn pcap_bytes(&self) -> u64 {
        self.pcap_bytes
    }

    /// Returns the gzip trailer CRC-32 verified against PCAP output.
    #[must_use]
    pub const fn gzip_crc32(&self) -> u32 {
        self.gzip_crc32
    }
}

/// Streaming checksum boundary between a bounded downloader/decompressor and the PCAP decoder.
///
/// Compressed bytes and decompressor output are supplied independently as they flow. The builder
/// retains only hash/checksum state, the fixed gzip header, and the last eight compressed bytes.
pub struct GzipPcapReceiptBuilder {
    plan_sha256: Sha256Digest,
    selected_file_identity: Sha256Digest,
    advertised_compressed_bytes: u64,
    max_pcap_bytes: u64,
    metadata: CaptureResponseMetadata,
    compressed_hasher: Sha256,
    pcap_hasher: Sha256,
    pcap_crc32: Crc32,
    compressed_bytes: u64,
    pcap_bytes: u64,
    gzip_header: [u8; GZIP_HEADER_BYTES],
    gzip_header_len: usize,
    gzip_tail: [u8; GZIP_TRAILER_BYTES],
    gzip_tail_len: usize,
    gzip_tail_cursor: usize,
    finished: bool,
}

impl fmt::Debug for GzipPcapReceiptBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GzipPcapReceiptBuilder")
            .field("plan_sha256", &self.plan_sha256)
            .field("compressed_bytes", &self.compressed_bytes)
            .field("pcap_bytes", &self.pcap_bytes)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl GzipPcapReceiptBuilder {
    /// Starts an exact response/capture receipt.
    ///
    /// # Errors
    ///
    /// Rejects URL, status, content length/encoding, ETag, or time metadata that does not match the
    /// selected plan before any response body is admitted.
    pub fn new(
        plan: &ColdJobPlan,
        metadata: CaptureResponseMetadata,
    ) -> Result<Self, CaptureError> {
        validate_response_metadata(plan, &metadata)?;
        Ok(Self {
            plan_sha256: plan.plan_sha256,
            selected_file_identity: plan.selected_file.identity(),
            advertised_compressed_bytes: plan.advertised_compressed_bytes,
            max_pcap_bytes: plan.max_pcap_bytes,
            metadata,
            compressed_hasher: Sha256::new(),
            pcap_hasher: Sha256::new(),
            pcap_crc32: Crc32::new(),
            compressed_bytes: 0,
            pcap_bytes: 0,
            gzip_header: [0; GZIP_HEADER_BYTES],
            gzip_header_len: 0,
            gzip_tail: [0; GZIP_TRAILER_BYTES],
            gzip_tail_len: 0,
            gzip_tail_cursor: 0,
            finished: false,
        })
    }

    /// Admits the next exact compressed response bytes.
    ///
    /// # Errors
    ///
    /// Rejects use after finish and any byte count beyond the catalog-advertised object size.
    pub fn push_compressed(&mut self, bytes: &[u8]) -> Result<(), CaptureError> {
        self.ensure_open()?;
        let increment = u64::try_from(bytes.len()).map_err(|_| CaptureError::CompressedTooLarge)?;
        let next = self
            .compressed_bytes
            .checked_add(increment)
            .ok_or(CaptureError::CompressedTooLarge)?;
        if next > self.advertised_compressed_bytes {
            return Err(CaptureError::CompressedTooLarge);
        }
        self.compressed_hasher.update(bytes);
        self.retain_gzip_edges(bytes);
        self.compressed_bytes = next;
        Ok(())
    }

    /// Admits the next exact PCAP bytes emitted by the streaming gzip decoder.
    ///
    /// # Errors
    ///
    /// Rejects use after finish and expansion beyond the pre-admitted PCAP ceiling.
    pub fn push_pcap(&mut self, bytes: &[u8]) -> Result<(), CaptureError> {
        self.ensure_open()?;
        let increment = u64::try_from(bytes.len()).map_err(|_| CaptureError::PcapTooLarge)?;
        let next = self
            .pcap_bytes
            .checked_add(increment)
            .ok_or(CaptureError::PcapTooLarge)?;
        if next > self.max_pcap_bytes {
            return Err(CaptureError::PcapTooLarge);
        }
        self.pcap_hasher.update(bytes);
        self.pcap_crc32.update(bytes);
        self.pcap_bytes = next;
        Ok(())
    }

    /// Finalizes only after exact length, gzip header/trailer CRC/ISIZE, and completion-time checks.
    ///
    /// # Errors
    ///
    /// Rejects incomplete or corrupt gzip/PCAP materialization and repeat finalization.
    pub fn finish(
        mut self,
        completed_at_unix_nanos: i64,
    ) -> Result<PcapMaterializationReceipt, CaptureError> {
        self.ensure_open()?;
        self.finished = true;
        if completed_at_unix_nanos < self.metadata.response_started_at_unix_nanos {
            return Err(CaptureError::InvalidResponseMetadata);
        }
        if self.compressed_bytes != self.advertised_compressed_bytes {
            return Err(CaptureError::CompressedLengthMismatch);
        }
        if self.pcap_bytes < 24
            || self.gzip_header_len != GZIP_HEADER_BYTES
            || self.gzip_tail_len != GZIP_TRAILER_BYTES
        {
            return Err(CaptureError::TruncatedGzip);
        }
        validate_gzip_header(&self.gzip_header)?;
        let trailer = self.ordered_gzip_tail();
        let expected_crc = u32::from_le_bytes(
            trailer[0..4]
                .try_into()
                .map_err(|_| CaptureError::TruncatedGzip)?,
        );
        let expected_size = u32::from_le_bytes(
            trailer[4..8]
                .try_into()
                .map_err(|_| CaptureError::TruncatedGzip)?,
        );
        let actual_crc = self.pcap_crc32.finalize();
        let actual_size = u32::try_from(self.pcap_bytes & u64::from(u32::MAX))
            .map_err(|_| CaptureError::GzipIntegrity)?;
        if expected_crc != actual_crc || expected_size != actual_size {
            return Err(CaptureError::GzipIntegrity);
        }
        let compressed_sha256 = Sha256Digest::from_bytes(self.compressed_hasher.finalize().into());
        let pcap_sha256 = Sha256Digest::from_bytes(self.pcap_hasher.finalize().into());
        let receipt_sha256 = crate::catalog::digest_fields(&[
            b"market-squawk/iex-hist-pcap-materialization/v1",
            self.plan_sha256.as_bytes(),
            self.selected_file_identity.as_bytes(),
            compressed_sha256.as_bytes(),
            &self.compressed_bytes.to_le_bytes(),
            pcap_sha256.as_bytes(),
            &self.pcap_bytes.to_le_bytes(),
            &actual_crc.to_le_bytes(),
            &completed_at_unix_nanos.to_le_bytes(),
        ]);
        Ok(PcapMaterializationReceipt {
            plan_sha256: self.plan_sha256,
            selected_file_identity: self.selected_file_identity,
            compressed_sha256,
            compressed_bytes: self.compressed_bytes,
            pcap_sha256,
            pcap_bytes: self.pcap_bytes,
            gzip_crc32: actual_crc,
            etag: self.metadata.etag,
            response_started_at_unix_nanos: self.metadata.response_started_at_unix_nanos,
            completed_at_unix_nanos,
            receipt_sha256,
        })
    }

    fn ensure_open(&self) -> Result<(), CaptureError> {
        if self.finished {
            Err(CaptureError::AlreadyFinished)
        } else {
            Ok(())
        }
    }

    fn retain_gzip_edges(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if self.gzip_header_len < GZIP_HEADER_BYTES {
                self.gzip_header[self.gzip_header_len] = byte;
                self.gzip_header_len += 1;
            }
            if self.gzip_tail_len < GZIP_TRAILER_BYTES {
                self.gzip_tail[self.gzip_tail_len] = byte;
                self.gzip_tail_len += 1;
            } else {
                self.gzip_tail[self.gzip_tail_cursor] = byte;
                self.gzip_tail_cursor = (self.gzip_tail_cursor + 1) % GZIP_TRAILER_BYTES;
            }
        }
    }

    fn ordered_gzip_tail(&self) -> [u8; GZIP_TRAILER_BYTES] {
        if self.gzip_tail_len < GZIP_TRAILER_BYTES || self.gzip_tail_cursor == 0 {
            return self.gzip_tail;
        }
        let mut ordered = [0_u8; GZIP_TRAILER_BYTES];
        let first = GZIP_TRAILER_BYTES - self.gzip_tail_cursor;
        ordered[..first].copy_from_slice(&self.gzip_tail[self.gzip_tail_cursor..]);
        ordered[first..].copy_from_slice(&self.gzip_tail[..self.gzip_tail_cursor]);
        ordered
    }
}

fn validate_response_metadata(
    plan: &ColdJobPlan,
    metadata: &CaptureResponseMetadata,
) -> Result<(), CaptureError> {
    let parsed =
        Url::parse(&metadata.response_url).map_err(|_| CaptureError::InvalidResponseMetadata)?;
    if parsed.as_str() != plan.selected_file.download_url
        || metadata.status != 200
        || metadata.content_length != plan.advertised_compressed_bytes
        || metadata.response_started_at_unix_nanos < 0
        || metadata.response_started_at_unix_nanos >= plan.deadline_unix_nanos
        || metadata
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case("identity"))
        || metadata.etag.as_ref().is_some_and(|etag| {
            etag.is_empty()
                || etag.len() > MAX_ETAG_BYTES
                || !etag
                    .bytes()
                    .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        })
    {
        return Err(CaptureError::InvalidResponseMetadata);
    }
    Ok(())
}

fn validate_gzip_header(header: &[u8; GZIP_HEADER_BYTES]) -> Result<(), CaptureError> {
    // CM=8 is DEFLATE. Reserved FLG bits 5..7 must be zero; optional fields are consumed by the
    // caller's streaming decoder and do not affect the fixed base header validation here.
    if header[0] != 0x1f || header[1] != 0x8b || header[2] != 8 || header[3] & 0xe0 != 0 {
        Err(CaptureError::InvalidGzipHeader)
    } else {
        Ok(())
    }
}

/// Capture/checksum receipt failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CaptureError {
    /// HTTP response metadata did not match the exact plan.
    #[error("IEX HIST capture response metadata is invalid")]
    InvalidResponseMetadata,
    /// Compressed bytes exceeded the exact advertised object size.
    #[error("IEX HIST compressed response exceeded its admitted size")]
    CompressedTooLarge,
    /// Decompressed PCAP bytes exceeded their pre-admitted ceiling.
    #[error("IEX HIST PCAP expansion exceeded its admitted size")]
    PcapTooLarge,
    /// Exact response bytes ended before the catalog-advertised size.
    #[error("IEX HIST compressed response length did not match its descriptor")]
    CompressedLengthMismatch,
    /// Gzip header or trailer was incomplete.
    #[error("IEX HIST gzip stream is truncated")]
    TruncatedGzip,
    /// Gzip header is not an admitted DEFLATE stream.
    #[error("IEX HIST gzip header is invalid")]
    InvalidGzipHeader,
    /// Gzip trailer CRC-32 or ISIZE did not match streamed PCAP output.
    #[error("IEX HIST gzip checksum or expanded size is invalid")]
    GzipIntegrity,
    /// Builder was used after terminal finalization.
    #[error("IEX HIST capture receipt was already finalized")]
    AlreadyFinished,
}
