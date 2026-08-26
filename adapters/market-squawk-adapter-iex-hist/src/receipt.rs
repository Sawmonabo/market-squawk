use std::fmt;

use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::model::{PcapObjectEncoding, Sha256Digest};
use crate::planning::{
    ColdJobPlan, IexHistExecutionAttempt, IexHistTrustedClockReading,
};

const MAX_ETAG_BYTES: usize = 256;
const GZIP_HEADER_BYTES: usize = 10;
const GZIP_TRAILER_BYTES: usize = 8;

/// Composition-bound metadata for the selected provider-object response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CaptureResponseMetadata {
    /// Final response URL; redirects must already have been rejected by the transport owner.
    pub(crate) response_url: String,
    /// HTTP status, required to be `200`.
    pub(crate) status: u16,
    /// HTTP content length, required and equal to the catalog-advertised bytes.
    pub(crate) content_length: u64,
    /// HTTP transfer content encoding; only absent or `identity` is admitted because provider
    /// object representation is already fixed by the selected descriptor.
    pub(crate) content_encoding: Option<String>,
    /// Optional bounded provider entity tag.
    pub(crate) etag: Option<String>,
    /// Trusted response-header clock/calendar reading from the application authority.
    pub(crate) response_started_clock: IexHistTrustedClockReading,
}

/// Explicit chronology disposition retained with complete content evidence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "disposition", content = "reason")]
pub enum CaptureChronologyDisposition {
    /// Retrieval chronology remained inside every immutable plan tolerance.
    Admitted,
    /// Bytes are complete evidence but may not proceed as a complete analytical generation.
    Quarantined(CaptureClockAnomaly),
}

/// Exact clock condition that quarantined an otherwise byte-complete capture.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureClockAnomaly {
    /// Response chronology predates the attempt admission beyond tolerance.
    ResponseBeforeAttempt,
    /// Response chronology predates its selected catalog receipt.
    ResponseBeforeCatalog,
    /// The trusted wall clock regressed beyond the immutable tolerance.
    WallClockRegression,
    /// Monotonic transfer duration exceeded the immutable tolerance.
    DownloadDurationExceeded,
    /// Trusted local calendar chronology predates catalog observation chronology.
    CalendarRegression,
}

/// Byte-exact provider-object and materialized-PCAP receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PcapMaterializationReceipt {
    /// Parent cold plan identity.
    pub(crate) plan_sha256: Sha256Digest,
    /// Parent selected descriptor identity.
    pub(crate) selected_file_identity: Sha256Digest,
    /// Exact expiring attempt that performed this materialization.
    pub(crate) attempt_sha256: Sha256Digest,
    /// Trusted wall time at which the shared authority admitted the producing attempt.
    pub(crate) attempt_admitted_at_unix_nanos: i64,
    /// Trusted attempt-admission UTC offset.
    pub(crate) attempt_admitted_utc_offset_seconds: i32,
    /// Trusted attempt-admission local calendar date.
    pub(crate) attempt_admitted_observed_date: String,
    /// Exact provider object representation selected by the catalog descriptor.
    pub(crate) object_encoding: PcapObjectEncoding,
    /// SHA-256 of the exact provider object (`.pcap.gz` or identity `.pcap`).
    pub(crate) compressed_sha256: Sha256Digest,
    /// Exact provider-object byte count, equal to catalog metadata.
    pub(crate) compressed_bytes: u64,
    /// SHA-256 of exact decompressed PCAP bytes.
    pub(crate) pcap_sha256: Sha256Digest,
    /// Exact decompressed PCAP byte count.
    pub(crate) pcap_bytes: u64,
    /// Gzip trailer CRC-32 verified against all streamed PCAP bytes.
    pub(crate) gzip_crc32: Option<u32>,
    /// Response entity tag when supplied.
    pub(crate) etag: Option<String>,
    /// Local response-header time.
    pub(crate) response_started_at_unix_nanos: i64,
    /// Trusted response-start UTC offset.
    pub(crate) response_started_utc_offset_seconds: i32,
    /// Trusted response-start local calendar date.
    pub(crate) response_started_observed_date: String,
    /// Local completion time after both streams and gzip trailer validation.
    pub(crate) completed_at_unix_nanos: i64,
    /// Trusted completion UTC offset.
    pub(crate) completed_utc_offset_seconds: i32,
    /// Trusted completion local calendar date.
    pub(crate) completed_observed_date: String,
    /// Monotonic duration independent of wall-clock movement.
    pub(crate) monotonic_duration_nanos: u64,
    /// Admitted or explicitly quarantined chronology.
    pub(crate) chronology_disposition: CaptureChronologyDisposition,
    /// Composite content identity for downstream raw/canonical lineage.
    pub(crate) receipt_sha256: Sha256Digest,
}

impl PcapMaterializationReceipt {
    /// Returns the composite raw-capture receipt identity.
    #[must_use]
    pub const fn receipt_sha256(&self) -> Sha256Digest {
        self.receipt_sha256
    }

    /// Returns the exact provider-object digest; DPLC identity objects are not compressed.
    #[must_use]
    pub const fn compressed_sha256(&self) -> Sha256Digest {
        self.compressed_sha256
    }

    /// Returns exact provider-object bytes; DPLC identity objects are not compressed.
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
    pub const fn gzip_crc32(&self) -> Option<u32> {
        self.gzip_crc32
    }

    /// Returns the exact provider object representation.
    #[must_use]
    pub const fn object_encoding(&self) -> PcapObjectEncoding {
        self.object_encoding
    }

    /// Returns the local response-header time retained by the receipt.
    #[must_use]
    pub const fn response_started_at_unix_nanos(&self) -> i64 {
        self.response_started_at_unix_nanos
    }

    /// Returns the local time at which full gzip and PCAP verification completed.
    #[must_use]
    pub const fn completed_at_unix_nanos(&self) -> i64 {
        self.completed_at_unix_nanos
    }

    /// Returns the bounded provider entity tag when one was supplied.
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Returns the attempt that produced these immutable bytes.
    #[must_use]
    pub const fn attempt_sha256(&self) -> Sha256Digest {
        self.attempt_sha256
    }

    /// Returns whether clock evidence is admitted or quarantined.
    #[must_use]
    pub const fn chronology_disposition(&self) -> CaptureChronologyDisposition {
        self.chronology_disposition
    }

    pub(crate) fn validate_against(&self, plan: &ColdJobPlan) -> Result<(), CaptureError> {
        if self.plan_sha256 != plan.plan_sha256
            || self.selected_file_identity != plan.selected_file.identity()
            || self.object_encoding != plan.object_encoding
            || self.compressed_bytes != plan.advertised_compressed_bytes
            || self.compressed_bytes == 0
            || self.pcap_bytes < 24
            || self.pcap_bytes > plan.max_pcap_bytes
            || self.response_started_at_unix_nanos < 0
            || self.completed_at_unix_nanos < 0
            || self.etag.as_ref().is_some_and(|etag| !valid_etag(etag))
        {
            return Err(CaptureError::ReceiptIdentityMismatch);
        }
        match self.object_encoding {
            PcapObjectEncoding::Gzip if self.gzip_crc32.is_none() => {
                return Err(CaptureError::ReceiptIdentityMismatch);
            }
            PcapObjectEncoding::Identity
                if self.gzip_crc32.is_some()
                    || self.compressed_bytes != self.pcap_bytes
                    || self.compressed_sha256 != self.pcap_sha256 =>
            {
                return Err(CaptureError::ReceiptIdentityMismatch);
            }
            PcapObjectEncoding::Gzip | PcapObjectEncoding::Identity => {}
        }
        let response_date = crate::model::TradeDate::parse(&self.response_started_observed_date)
            .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
        let completed_date = crate::model::TradeDate::parse(&self.completed_observed_date)
            .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
        let admitted_date = crate::model::TradeDate::parse(&self.attempt_admitted_observed_date)
            .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
        IexHistTrustedClockReading::try_new(
            self.attempt_admitted_at_unix_nanos,
            self.attempt_admitted_utc_offset_seconds,
            admitted_date,
        )
        .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
        IexHistTrustedClockReading::try_new(
            self.response_started_at_unix_nanos,
            self.response_started_utc_offset_seconds,
            response_date,
        )
        .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
        IexHistTrustedClockReading::try_new(
            self.completed_at_unix_nanos,
            self.completed_utc_offset_seconds,
            completed_date,
        )
        .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
        let expected_disposition = chronology_disposition(
            plan.selected_file.catalog_retrieved_at_unix_nanos(),
            plan.selected_file.catalog_observed_on(),
            plan.max_download_duration_nanos,
            plan.max_clock_regression_nanos,
            self.attempt_admitted_at_unix_nanos,
            self.response_started_at_unix_nanos,
            response_date,
            self.completed_at_unix_nanos,
            completed_date,
            self.monotonic_duration_nanos,
        );
        if expected_disposition != self.chronology_disposition {
            return Err(CaptureError::ReceiptIdentityMismatch);
        }
        let expected = materialization_identity(
            self.plan_sha256,
            self.selected_file_identity,
            self.attempt_sha256,
            self.attempt_admitted_at_unix_nanos,
            self.attempt_admitted_utc_offset_seconds,
            &self.attempt_admitted_observed_date,
            self.object_encoding,
            self.compressed_sha256,
            self.compressed_bytes,
            self.pcap_sha256,
            self.pcap_bytes,
            self.gzip_crc32,
            self.etag.as_deref(),
            self.response_started_at_unix_nanos,
            self.response_started_utc_offset_seconds,
            &self.response_started_observed_date,
            self.completed_at_unix_nanos,
            self.completed_utc_offset_seconds,
            &self.completed_observed_date,
            self.monotonic_duration_nanos,
            self.chronology_disposition,
        );
        if expected != self.receipt_sha256 {
            return Err(CaptureError::ReceiptIdentityMismatch);
        }
        Ok(())
    }
}

/// Streaming checksum boundary between a bounded downloader/decompressor and the PCAP decoder.
///
/// Provider-object and materialized-PCAP bytes are supplied independently as they flow. For gzip,
/// the builder retains the fixed header and final trailer; identity DPLC has no gzip state.
pub(crate) struct GzipPcapReceiptBuilder {
    plan_sha256: Sha256Digest,
    selected_file_identity: Sha256Digest,
    attempt_sha256: Sha256Digest,
    attempt_admitted_clock: IexHistTrustedClockReading,
    object_encoding: PcapObjectEncoding,
    advertised_compressed_bytes: u64,
    max_pcap_bytes: u64,
    attempt_deadline_unix_nanos: i64,
    catalog_retrieved_at_unix_nanos: i64,
    catalog_observed_on: crate::model::TradeDate,
    max_download_duration_nanos: u64,
    max_clock_regression_nanos: u64,
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
    pub(crate) fn new(
        plan: &ColdJobPlan,
        attempt: IexHistExecutionAttempt,
        metadata: CaptureResponseMetadata,
    ) -> Result<Self, CaptureError> {
        validate_response_metadata(plan, attempt, &metadata)?;
        Ok(Self {
            plan_sha256: plan.plan_sha256,
            selected_file_identity: plan.selected_file.identity(),
            attempt_sha256: attempt.attempt_sha256(),
            attempt_admitted_clock: attempt.admitted_clock(),
            object_encoding: plan.object_encoding,
            advertised_compressed_bytes: plan.advertised_compressed_bytes,
            max_pcap_bytes: plan.max_pcap_bytes,
            attempt_deadline_unix_nanos: attempt.deadline_unix_nanos(),
            catalog_retrieved_at_unix_nanos: plan
                .selected_file
                .catalog_retrieved_at_unix_nanos(),
            catalog_observed_on: plan.selected_file.catalog_observed_on(),
            max_download_duration_nanos: plan.max_download_duration_nanos,
            max_clock_regression_nanos: plan.max_clock_regression_nanos,
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

    /// Admits the next exact provider-object response bytes.
    ///
    /// # Errors
    ///
    /// Rejects use after finish and any byte count beyond the catalog-advertised object size.
    pub(crate) fn push_compressed(&mut self, bytes: &[u8]) -> Result<(), CaptureError> {
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
        if self.object_encoding == PcapObjectEncoding::Gzip {
            self.retain_gzip_edges(bytes);
        }
        self.compressed_bytes = next;
        Ok(())
    }

    /// Admits the next exact PCAP bytes emitted by the streaming gzip decoder.
    ///
    /// # Errors
    ///
    /// Rejects use after finish and expansion beyond the pre-admitted PCAP ceiling.
    pub(crate) fn push_pcap(&mut self, bytes: &[u8]) -> Result<(), CaptureError> {
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
    pub(crate) fn finish(
        mut self,
        completed_clock: IexHistTrustedClockReading,
        monotonic_duration_nanos: u64,
    ) -> Result<PcapMaterializationReceipt, CaptureError> {
        self.ensure_open()?;
        self.finished = true;
        let completed_at_unix_nanos = completed_clock.unix_nanos();
        if completed_at_unix_nanos >= self.attempt_deadline_unix_nanos {
            return Err(CaptureError::InvalidResponseMetadata);
        }
        if self.compressed_bytes != self.advertised_compressed_bytes {
            return Err(CaptureError::CompressedLengthMismatch);
        }
        if self.pcap_bytes < 24 {
            return Err(CaptureError::TruncatedObject);
        }
        let gzip_tail = self.ordered_gzip_tail();
        let actual_crc = self.pcap_crc32.finalize();
        let gzip_crc32 = match self.object_encoding {
            PcapObjectEncoding::Gzip => {
                if self.gzip_header_len != GZIP_HEADER_BYTES
                    || self.gzip_tail_len != GZIP_TRAILER_BYTES
                {
                    return Err(CaptureError::TruncatedObject);
                }
                validate_gzip_header(&self.gzip_header)?;
                let expected_crc = u32::from_le_bytes(
                    gzip_tail[0..4]
                        .try_into()
                        .map_err(|_| CaptureError::TruncatedObject)?,
                );
                let expected_size = u32::from_le_bytes(
                    gzip_tail[4..8]
                        .try_into()
                        .map_err(|_| CaptureError::TruncatedObject)?,
                );
                let actual_size = u32::try_from(self.pcap_bytes & u64::from(u32::MAX))
                    .map_err(|_| CaptureError::GzipIntegrity)?;
                if expected_crc != actual_crc || expected_size != actual_size {
                    return Err(CaptureError::GzipIntegrity);
                }
                Some(actual_crc)
            }
            PcapObjectEncoding::Identity => None,
        };
        let compressed_sha256 = Sha256Digest::from_bytes(self.compressed_hasher.finalize().into());
        let pcap_sha256 = Sha256Digest::from_bytes(self.pcap_hasher.finalize().into());
        if self.object_encoding == PcapObjectEncoding::Identity
            && (self.compressed_bytes != self.pcap_bytes || compressed_sha256 != pcap_sha256)
        {
            return Err(CaptureError::IdentityObjectMismatch);
        }
        let chronology_disposition = chronology_disposition(
            self.catalog_retrieved_at_unix_nanos,
            self.catalog_observed_on,
            self.max_download_duration_nanos,
            self.max_clock_regression_nanos,
            self.attempt_admitted_clock.unix_nanos(),
            self.metadata.response_started_clock.unix_nanos(),
            self.metadata.response_started_clock.observed_date(),
            completed_at_unix_nanos,
            completed_clock.observed_date(),
            monotonic_duration_nanos,
        );
        let receipt_sha256 = materialization_identity(
            self.plan_sha256,
            self.selected_file_identity,
            self.attempt_sha256,
            self.attempt_admitted_clock.unix_nanos(),
            self.attempt_admitted_clock.utc_offset_seconds(),
            &self.attempt_admitted_clock.observed_date().compact(),
            self.object_encoding,
            compressed_sha256,
            self.compressed_bytes,
            pcap_sha256,
            self.pcap_bytes,
            gzip_crc32,
            self.metadata.etag.as_deref(),
            self.metadata.response_started_clock.unix_nanos(),
            self.metadata.response_started_clock.utc_offset_seconds(),
            &self.metadata.response_started_clock.observed_date().compact(),
            completed_at_unix_nanos,
            completed_clock.utc_offset_seconds(),
            &completed_clock.observed_date().compact(),
            monotonic_duration_nanos,
            chronology_disposition,
        );
        Ok(PcapMaterializationReceipt {
            plan_sha256: self.plan_sha256,
            selected_file_identity: self.selected_file_identity,
            attempt_sha256: self.attempt_sha256,
            attempt_admitted_at_unix_nanos: self.attempt_admitted_clock.unix_nanos(),
            attempt_admitted_utc_offset_seconds: self.attempt_admitted_clock.utc_offset_seconds(),
            attempt_admitted_observed_date: self.attempt_admitted_clock.observed_date().compact(),
            object_encoding: self.object_encoding,
            compressed_sha256,
            compressed_bytes: self.compressed_bytes,
            pcap_sha256,
            pcap_bytes: self.pcap_bytes,
            gzip_crc32,
            etag: self.metadata.etag,
            response_started_at_unix_nanos: self.metadata.response_started_clock.unix_nanos(),
            response_started_utc_offset_seconds: self.metadata.response_started_clock.utc_offset_seconds(),
            response_started_observed_date: self.metadata.response_started_clock.observed_date().compact(),
            completed_at_unix_nanos,
            completed_utc_offset_seconds: completed_clock.utc_offset_seconds(),
            completed_observed_date: completed_clock.observed_date().compact(),
            monotonic_duration_nanos,
            chronology_disposition,
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
    attempt: IexHistExecutionAttempt,
    metadata: &CaptureResponseMetadata,
) -> Result<(), CaptureError> {
    let parsed =
        Url::parse(&metadata.response_url).map_err(|_| CaptureError::InvalidResponseMetadata)?;
    if parsed.as_str() != plan.selected_file.download_url
        || metadata.status != 200
        || metadata.content_length != plan.advertised_compressed_bytes
        || metadata.response_started_clock.unix_nanos() < 0
        || metadata.response_started_clock.unix_nanos() >= attempt.deadline_unix_nanos()
        || metadata
            .content_encoding
            .as_deref()
            .is_some_and(|value| !value.eq_ignore_ascii_case("identity"))
        || metadata.etag.as_ref().is_some_and(|etag| !valid_etag(etag))
    {
        return Err(CaptureError::InvalidResponseMetadata);
    }
    Ok(())
}

fn valid_etag(etag: &str) -> bool {
    !etag.is_empty()
        && etag.len() <= MAX_ETAG_BYTES
        && etag
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
}

#[allow(
    clippy::too_many_arguments,
    reason = "chronology classification compares the complete trusted clock boundary"
)]
fn chronology_disposition(
    catalog_retrieved_at_unix_nanos: i64,
    catalog_observed_on: crate::model::TradeDate,
    max_download_duration_nanos: u64,
    max_clock_regression_nanos: u64,
    attempt_admitted_at_unix_nanos: i64,
    response_started_at_unix_nanos: i64,
    response_started_observed_date: crate::model::TradeDate,
    completed_at_unix_nanos: i64,
    completed_observed_date: crate::model::TradeDate,
    monotonic_duration_nanos: u64,
) -> CaptureChronologyDisposition {
    let tolerated_response = response_started_at_unix_nanos
        .checked_add(i64::try_from(max_clock_regression_nanos).unwrap_or(i64::MAX))
        .unwrap_or(i64::MAX);
    if tolerated_response < attempt_admitted_at_unix_nanos {
        return CaptureChronologyDisposition::Quarantined(
            CaptureClockAnomaly::ResponseBeforeAttempt,
        );
    }
    if response_started_at_unix_nanos < catalog_retrieved_at_unix_nanos {
        return CaptureChronologyDisposition::Quarantined(
            CaptureClockAnomaly::ResponseBeforeCatalog,
        );
    }
    if response_started_observed_date < catalog_observed_on
        || completed_observed_date < response_started_observed_date
    {
        return CaptureChronologyDisposition::Quarantined(
            CaptureClockAnomaly::CalendarRegression,
        );
    }
    let tolerated_completion = completed_at_unix_nanos
        .checked_add(i64::try_from(max_clock_regression_nanos).unwrap_or(i64::MAX))
        .unwrap_or(i64::MAX);
    if tolerated_completion < response_started_at_unix_nanos {
        return CaptureChronologyDisposition::Quarantined(
            CaptureClockAnomaly::WallClockRegression,
        );
    }
    if monotonic_duration_nanos > max_download_duration_nanos {
        return CaptureChronologyDisposition::Quarantined(
            CaptureClockAnomaly::DownloadDurationExceeded,
        );
    }
    CaptureChronologyDisposition::Admitted
}

const fn chronology_tag(disposition: CaptureChronologyDisposition) -> &'static [u8] {
    match disposition {
        CaptureChronologyDisposition::Admitted => b"admitted",
        CaptureChronologyDisposition::Quarantined(CaptureClockAnomaly::ResponseBeforeAttempt) => {
            b"quarantined_response_before_attempt"
        }
        CaptureChronologyDisposition::Quarantined(CaptureClockAnomaly::ResponseBeforeCatalog) => {
            b"quarantined_response_before_catalog"
        }
        CaptureChronologyDisposition::Quarantined(CaptureClockAnomaly::WallClockRegression) => {
            b"quarantined_wall_clock_regression"
        }
        CaptureChronologyDisposition::Quarantined(
            CaptureClockAnomaly::DownloadDurationExceeded,
        ) => b"quarantined_download_duration_exceeded",
        CaptureChronologyDisposition::Quarantined(CaptureClockAnomaly::CalendarRegression) => {
            b"quarantined_calendar_regression"
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the materialization identity intentionally commits every terminal receipt field"
)]
fn materialization_identity(
    plan_sha256: Sha256Digest,
    selected_file_identity: Sha256Digest,
    attempt_sha256: Sha256Digest,
    attempt_admitted_at_unix_nanos: i64,
    attempt_admitted_utc_offset_seconds: i32,
    attempt_admitted_observed_date: &str,
    object_encoding: PcapObjectEncoding,
    compressed_sha256: Sha256Digest,
    compressed_bytes: u64,
    pcap_sha256: Sha256Digest,
    pcap_bytes: u64,
    gzip_crc32: Option<u32>,
    etag: Option<&str>,
    response_started_at_unix_nanos: i64,
    response_started_utc_offset_seconds: i32,
    response_started_observed_date: &str,
    completed_at_unix_nanos: i64,
    completed_utc_offset_seconds: i32,
    completed_observed_date: &str,
    monotonic_duration_nanos: u64,
    chronology_disposition: CaptureChronologyDisposition,
) -> Sha256Digest {
    let etag_present = [u8::from(etag.is_some())];
    let etag_bytes = etag.unwrap_or_default().as_bytes();
    let gzip_crc_present = [u8::from(gzip_crc32.is_some())];
    let gzip_crc = gzip_crc32.unwrap_or_default().to_le_bytes();
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-pcap-materialization/v3",
        plan_sha256.as_bytes(),
        selected_file_identity.as_bytes(),
        attempt_sha256.as_bytes(),
        &attempt_admitted_at_unix_nanos.to_le_bytes(),
        &attempt_admitted_utc_offset_seconds.to_le_bytes(),
        attempt_admitted_observed_date.as_bytes(),
        object_encoding.identity_value().as_bytes(),
        compressed_sha256.as_bytes(),
        &compressed_bytes.to_le_bytes(),
        pcap_sha256.as_bytes(),
        &pcap_bytes.to_le_bytes(),
        &gzip_crc_present,
        &gzip_crc,
        &etag_present,
        etag_bytes,
        &response_started_at_unix_nanos.to_le_bytes(),
        &response_started_utc_offset_seconds.to_le_bytes(),
        response_started_observed_date.as_bytes(),
        &completed_at_unix_nanos.to_le_bytes(),
        &completed_utc_offset_seconds.to_le_bytes(),
        completed_observed_date.as_bytes(),
        &monotonic_duration_nanos.to_le_bytes(),
        chronology_tag(chronology_disposition),
    ])
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
    /// Provider object, PCAP, or required gzip framing was incomplete.
    #[error("IEX HIST provider object or PCAP materialization is truncated")]
    TruncatedObject,
    /// Gzip header is not an admitted DEFLATE stream.
    #[error("IEX HIST gzip header is invalid")]
    InvalidGzipHeader,
    /// Gzip trailer CRC-32 or ISIZE did not match streamed PCAP output.
    #[error("IEX HIST gzip checksum or expanded size is invalid")]
    GzipIntegrity,
    /// Identity-encoded provider object did not equal the staged PCAP byte-for-byte.
    #[error("IEX HIST identity object does not equal its PCAP materialization")]
    IdentityObjectMismatch,
    /// Builder was used after terminal finalization.
    #[error("IEX HIST capture receipt was already finalized")]
    AlreadyFinished,
    /// A restored or caller-supplied receipt did not commit the exact admitted terminal fields.
    #[error("IEX HIST capture receipt identity does not match its admitted materialization")]
    ReceiptIdentityMismatch,
}
