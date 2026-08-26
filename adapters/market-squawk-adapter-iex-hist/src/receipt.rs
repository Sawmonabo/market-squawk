use std::fmt;

use crc32fast::Hasher as Crc32;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::durable::{IexHistResumeClaim, valid_strong_etag};
use crate::model::{PcapObjectEncoding, Sha256Digest};
use crate::planning::{ColdJobPlan, IexHistExecutionAttempt, IexHistTrustedClockReading};
use crate::transport::IexHistResumeAdoptionReceipt;

const MAX_ETAG_BYTES: usize = 256;
const GZIP_HEADER_BYTES: usize = 10;
const GZIP_TRAILER_BYTES: usize = 8;

/// Composition-bound metadata for the selected provider-object response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CaptureResponseMetadata {
    /// Final response URL; redirects must already have been rejected by the transport owner.
    pub(crate) response_url: String,
    /// HTTP status, required to be `200` from byte zero or `206` for an exact resume range.
    pub(crate) status: u16,
    /// First response byte: zero for a complete response, or the exact admitted resume prefix.
    pub(crate) range_start: u64,
    /// HTTP content length, equal to the exact bytes remaining from `range_start`.
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
    /// Complete expiring authority attempt that performed this materialization.
    pub(crate) attempt: IexHistExecutionAttempt,
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
    /// Exact shared-physical adoption rejoined before a validated range response.
    pub(crate) resume_adoption: Option<IexHistResumeAdoptionReceipt>,
    /// Gzip trailer CRC-32 verified against all streamed PCAP bytes.
    pub(crate) gzip_crc32: Option<u32>,
    /// Response entity tag when supplied.
    pub(crate) etag: Option<String>,
    /// Exact terminal response status (`200` for byte zero, `206` for a validated range).
    pub(crate) response_status: u16,
    /// Exact first provider-object byte represented by the terminal response.
    pub(crate) response_range_start: u64,
    /// Exact terminal response body length.
    pub(crate) response_content_length: u64,
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
    /// Monotonic duration of the final response segment.
    pub(crate) segment_monotonic_duration_nanos: u64,
    /// Cumulative monotonic duration across every retained resume segment.
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

    /// Returns the exact shared-physical adoption used for byte-range completion.
    #[must_use]
    pub const fn resume_adoption(&self) -> Option<&IexHistResumeAdoptionReceipt> {
        self.resume_adoption.as_ref()
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

    /// Returns the exact full/range response status retained by the receipt.
    #[must_use]
    pub const fn response_status(&self) -> u16 {
        self.response_status
    }

    /// Returns the exact first provider-object byte supplied by the terminal response.
    #[must_use]
    pub const fn response_range_start(&self) -> u64 {
        self.response_range_start
    }

    /// Returns the exact terminal response body length.
    #[must_use]
    pub const fn response_content_length(&self) -> u64 {
        self.response_content_length
    }

    /// Returns the complete authority attempt that produced these immutable bytes.
    #[must_use]
    pub const fn attempt(&self) -> IexHistExecutionAttempt {
        self.attempt
    }

    /// Returns whether clock evidence is admitted or quarantined.
    #[must_use]
    pub const fn chronology_disposition(&self) -> CaptureChronologyDisposition {
        self.chronology_disposition
    }

    pub(crate) fn validate_against(&self, plan: &ColdJobPlan) -> Result<(), CaptureError> {
        self.attempt
            .validate()
            .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
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
        let response_clock = IexHistTrustedClockReading::try_new(
            self.response_started_at_unix_nanos,
            self.response_started_utc_offset_seconds,
            response_date,
        )
        .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
        let completed_clock = IexHistTrustedClockReading::try_new(
            self.completed_at_unix_nanos,
            self.completed_utc_offset_seconds,
            completed_date,
        )
        .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
        if self.response_started_at_unix_nanos >= self.attempt.deadline_unix_nanos()
            || self.completed_at_unix_nanos >= self.attempt.deadline_unix_nanos()
        {
            return Err(CaptureError::ReceiptIdentityMismatch);
        }
        let prior_claim = self
            .resume_adoption
            .as_ref()
            .map(IexHistResumeAdoptionReceipt::claim);
        let prior_disposition = if let Some(adoption) = self.resume_adoption.as_ref() {
            adoption
                .validate_against(plan)
                .map_err(|_| CaptureError::ReceiptIdentityMismatch)?;
            let claim = adoption.claim();
            if self.etag.as_deref() != Some(claim.strong_etag())
                || self.attempt.attempt_sha256() == claim.segment_attempt().attempt_sha256()
                || self.response_status != 206
                || self.response_range_start != claim.prefix_bytes()
                || plan
                    .advertised_compressed_bytes
                    .checked_sub(claim.prefix_bytes())
                    != Some(self.response_content_length)
                || claim
                    .cumulative_monotonic_duration_nanos()
                    .checked_add(self.segment_monotonic_duration_nanos)
                    != Some(self.monotonic_duration_nanos)
            {
                return Err(CaptureError::ReceiptIdentityMismatch);
            }
            Some(claim.chronology_disposition())
        } else {
            if self.response_status != 200
                || self.response_range_start != 0
                || self.response_content_length != plan.advertised_compressed_bytes
                || self.segment_monotonic_duration_nanos != self.monotonic_duration_nanos
            {
                return Err(CaptureError::ReceiptIdentityMismatch);
            }
            None
        };
        let segment_disposition = chronology_disposition(
            plan.selected_file.catalog_retrieved_at_unix_nanos(),
            plan.selected_file.catalog_observed_on(),
            plan.max_download_duration_nanos,
            plan.max_clock_regression_nanos,
            self.attempt.admitted_clock().unix_nanos(),
            self.response_started_at_unix_nanos,
            response_date,
            self.completed_at_unix_nanos,
            completed_date,
            self.segment_monotonic_duration_nanos,
        );
        let boundary_disposition = resumed_boundary_disposition(
            prior_claim,
            self.attempt.admitted_clock(),
            response_clock,
            completed_clock,
            plan.max_clock_regression_nanos,
        );
        let expected_disposition = combine_chronology(
            prior_disposition,
            boundary_disposition,
            segment_disposition,
            self.monotonic_duration_nanos,
            plan.max_download_duration_nanos,
        );
        if expected_disposition != self.chronology_disposition {
            return Err(CaptureError::ReceiptIdentityMismatch);
        }
        let expected = materialization_identity(
            self.plan_sha256,
            self.selected_file_identity,
            self.attempt,
            self.object_encoding,
            self.compressed_sha256,
            self.compressed_bytes,
            self.pcap_sha256,
            self.pcap_bytes,
            self.resume_adoption.as_ref(),
            self.gzip_crc32,
            self.etag.as_deref(),
            self.response_status,
            self.response_range_start,
            self.response_content_length,
            self.response_started_at_unix_nanos,
            self.response_started_utc_offset_seconds,
            &self.response_started_observed_date,
            self.completed_at_unix_nanos,
            self.completed_utc_offset_seconds,
            &self.completed_observed_date,
            self.segment_monotonic_duration_nanos,
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
    attempt: IexHistExecutionAttempt,
    object_encoding: PcapObjectEncoding,
    advertised_compressed_bytes: u64,
    max_pcap_bytes: u64,
    catalog_retrieved_at_unix_nanos: i64,
    catalog_observed_on: crate::model::TradeDate,
    max_download_duration_nanos: u64,
    max_clock_regression_nanos: u64,
    metadata: CaptureResponseMetadata,
    resume_adoption: Option<IexHistResumeAdoptionReceipt>,
    segment_start_bytes: u64,
    resume_prefix_verified: bool,
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
    pub(crate) const fn compressed_bytes(&self) -> u64 {
        self.compressed_bytes
    }

    pub(crate) const fn segment_start_bytes(&self) -> u64 {
        self.segment_start_bytes
    }

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
        validate_response_metadata(plan, attempt, &metadata, None)?;
        Self::new_inner(plan, attempt, metadata, None)
    }

    /// Starts a range-response receipt after validating the exact prefix claim and validator.
    pub(crate) fn resume(
        plan: &ColdJobPlan,
        attempt: IexHistExecutionAttempt,
        metadata: CaptureResponseMetadata,
        resume_adoption: IexHistResumeAdoptionReceipt,
    ) -> Result<Self, CaptureError> {
        resume_adoption
            .validate_against(plan)
            .map_err(|_| CaptureError::InvalidResumeClaim)?;
        validate_response_metadata(plan, attempt, &metadata, Some(resume_adoption.claim()))?;
        Self::new_inner(plan, attempt, metadata, Some(resume_adoption))
    }

    fn new_inner(
        plan: &ColdJobPlan,
        attempt: IexHistExecutionAttempt,
        metadata: CaptureResponseMetadata,
        resume_adoption: Option<IexHistResumeAdoptionReceipt>,
    ) -> Result<Self, CaptureError> {
        let segment_start_bytes = resume_adoption
            .as_ref()
            .map_or(0, |adoption| adoption.claim().prefix_bytes());
        Ok(Self {
            plan_sha256: plan.plan_sha256,
            selected_file_identity: plan.selected_file.identity(),
            attempt,
            object_encoding: plan.object_encoding,
            advertised_compressed_bytes: plan.advertised_compressed_bytes,
            max_pcap_bytes: plan.max_pcap_bytes,
            catalog_retrieved_at_unix_nanos: plan.selected_file.catalog_retrieved_at_unix_nanos(),
            catalog_observed_on: plan.selected_file.catalog_observed_on(),
            max_download_duration_nanos: plan.max_download_duration_nanos,
            max_clock_regression_nanos: plan.max_clock_regression_nanos,
            metadata,
            resume_adoption,
            segment_start_bytes,
            resume_prefix_verified: segment_start_bytes == 0,
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
        if !self.resume_prefix_verified && next > self.segment_start_bytes {
            return Err(CaptureError::ResumePrefixMismatch);
        }
        self.compressed_hasher.update(bytes);
        if self.object_encoding == PcapObjectEncoding::Gzip {
            self.retain_gzip_edges(bytes);
        }
        self.compressed_bytes = next;
        Ok(())
    }

    /// Confirms that re-read controlled prefix bytes exactly match the admitted resume claim.
    pub(crate) fn verify_resume_prefix(&mut self) -> Result<(), CaptureError> {
        self.ensure_open()?;
        let Some(claim) = self
            .resume_adoption
            .as_ref()
            .map(IexHistResumeAdoptionReceipt::claim)
        else {
            return Err(CaptureError::InvalidResumeClaim);
        };
        let prefix_sha256 =
            Sha256Digest::from_bytes(self.compressed_hasher.clone().finalize().into());
        if self.compressed_bytes != claim.prefix_bytes() || prefix_sha256 != claim.prefix_sha256() {
            return Err(CaptureError::ResumePrefixMismatch);
        }
        self.resume_prefix_verified = true;
        Ok(())
    }

    /// Produces an incomplete-prefix claim for adoption by the shared physical lifecycle.
    pub(crate) fn checkpoint(
        &self,
        plan: &ColdJobPlan,
        checkpoint_clock: IexHistTrustedClockReading,
        segment_monotonic_duration_nanos: u64,
    ) -> Result<IexHistResumeClaim, CaptureError> {
        self.ensure_open()?;
        if !self.resume_prefix_verified
            || self.compressed_bytes <= self.segment_start_bytes
            || self.compressed_bytes >= self.advertised_compressed_bytes
        {
            return Err(CaptureError::ResumePrefixMismatch);
        }
        let strong_etag = self
            .metadata
            .etag
            .as_ref()
            .filter(|value| valid_strong_etag(value))
            .ok_or(CaptureError::ResumeValidatorUnavailable)?;
        let cumulative_monotonic_duration_nanos = self
            .resume_adoption
            .as_ref()
            .map(IexHistResumeAdoptionReceipt::claim)
            .map_or(Ok(segment_monotonic_duration_nanos), |claim| {
                claim
                    .cumulative_monotonic_duration_nanos()
                    .checked_add(segment_monotonic_duration_nanos)
                    .ok_or(CaptureError::ResumeDurationOverflow)
            })?;
        let current = chronology_disposition(
            self.catalog_retrieved_at_unix_nanos,
            self.catalog_observed_on,
            self.max_download_duration_nanos,
            self.max_clock_regression_nanos,
            self.attempt.admitted_clock().unix_nanos(),
            self.metadata.response_started_clock.unix_nanos(),
            self.metadata.response_started_clock.observed_date(),
            checkpoint_clock.unix_nanos(),
            checkpoint_clock.observed_date(),
            segment_monotonic_duration_nanos,
        );
        let prior_claim = self
            .resume_adoption
            .as_ref()
            .map(IexHistResumeAdoptionReceipt::claim);
        let boundary = resumed_boundary_disposition(
            prior_claim,
            self.attempt.admitted_clock(),
            self.metadata.response_started_clock,
            checkpoint_clock,
            self.max_clock_regression_nanos,
        );
        let chronology_disposition = combine_chronology(
            prior_claim.map(IexHistResumeClaim::chronology_disposition),
            boundary,
            current,
            cumulative_monotonic_duration_nanos,
            self.max_download_duration_nanos,
        );
        IexHistResumeClaim::try_new(
            plan,
            self.segment_start_bytes,
            self.compressed_bytes,
            Sha256Digest::from_bytes(self.compressed_hasher.clone().finalize().into()),
            strong_etag.clone(),
            self.attempt,
            self.metadata.response_started_clock,
            checkpoint_clock,
            cumulative_monotonic_duration_nanos,
            chronology_disposition,
            self.resume_adoption
                .as_ref()
                .map(IexHistResumeAdoptionReceipt::claim),
        )
        .map_err(|_| CaptureError::InvalidResumeClaim)
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
        if completed_at_unix_nanos >= self.attempt.deadline_unix_nanos() {
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
        if !self.resume_prefix_verified {
            return Err(CaptureError::ResumePrefixMismatch);
        }
        let cumulative_monotonic_duration_nanos = self
            .resume_adoption
            .as_ref()
            .map(IexHistResumeAdoptionReceipt::claim)
            .map_or(Ok(monotonic_duration_nanos), |claim| {
                claim
                    .cumulative_monotonic_duration_nanos()
                    .checked_add(monotonic_duration_nanos)
                    .ok_or(CaptureError::ResumeDurationOverflow)
            })?;
        let segment_disposition = chronology_disposition(
            self.catalog_retrieved_at_unix_nanos,
            self.catalog_observed_on,
            self.max_download_duration_nanos,
            self.max_clock_regression_nanos,
            self.attempt.admitted_clock().unix_nanos(),
            self.metadata.response_started_clock.unix_nanos(),
            self.metadata.response_started_clock.observed_date(),
            completed_at_unix_nanos,
            completed_clock.observed_date(),
            monotonic_duration_nanos,
        );
        let prior_claim = self
            .resume_adoption
            .as_ref()
            .map(IexHistResumeAdoptionReceipt::claim);
        let boundary_disposition = resumed_boundary_disposition(
            prior_claim,
            self.attempt.admitted_clock(),
            self.metadata.response_started_clock,
            completed_clock,
            self.max_clock_regression_nanos,
        );
        let chronology_disposition = combine_chronology(
            prior_claim.map(IexHistResumeClaim::chronology_disposition),
            boundary_disposition,
            segment_disposition,
            cumulative_monotonic_duration_nanos,
            self.max_download_duration_nanos,
        );
        let receipt_sha256 = materialization_identity(
            self.plan_sha256,
            self.selected_file_identity,
            self.attempt,
            self.object_encoding,
            compressed_sha256,
            self.compressed_bytes,
            pcap_sha256,
            self.pcap_bytes,
            self.resume_adoption.as_ref(),
            gzip_crc32,
            self.metadata.etag.as_deref(),
            self.metadata.status,
            self.metadata.range_start,
            self.metadata.content_length,
            self.metadata.response_started_clock.unix_nanos(),
            self.metadata.response_started_clock.utc_offset_seconds(),
            &self
                .metadata
                .response_started_clock
                .observed_date()
                .compact(),
            completed_at_unix_nanos,
            completed_clock.utc_offset_seconds(),
            &completed_clock.observed_date().compact(),
            monotonic_duration_nanos,
            cumulative_monotonic_duration_nanos,
            chronology_disposition,
        );
        Ok(PcapMaterializationReceipt {
            plan_sha256: self.plan_sha256,
            selected_file_identity: self.selected_file_identity,
            attempt: self.attempt,
            object_encoding: self.object_encoding,
            compressed_sha256,
            compressed_bytes: self.compressed_bytes,
            pcap_sha256,
            pcap_bytes: self.pcap_bytes,
            resume_adoption: self.resume_adoption,
            gzip_crc32,
            etag: self.metadata.etag,
            response_status: self.metadata.status,
            response_range_start: self.metadata.range_start,
            response_content_length: self.metadata.content_length,
            response_started_at_unix_nanos: self.metadata.response_started_clock.unix_nanos(),
            response_started_utc_offset_seconds: self
                .metadata
                .response_started_clock
                .utc_offset_seconds(),
            response_started_observed_date: self
                .metadata
                .response_started_clock
                .observed_date()
                .compact(),
            completed_at_unix_nanos,
            completed_utc_offset_seconds: completed_clock.utc_offset_seconds(),
            completed_observed_date: completed_clock.observed_date().compact(),
            segment_monotonic_duration_nanos: monotonic_duration_nanos,
            monotonic_duration_nanos: cumulative_monotonic_duration_nanos,
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
    resume_claim: Option<&IexHistResumeClaim>,
) -> Result<(), CaptureError> {
    let parsed =
        Url::parse(&metadata.response_url).map_err(|_| CaptureError::InvalidResponseMetadata)?;
    let expected_range_start = resume_claim.map_or(0, IexHistResumeClaim::prefix_bytes);
    let expected_content_length = plan
        .advertised_compressed_bytes
        .checked_sub(expected_range_start)
        .ok_or(CaptureError::InvalidResponseMetadata)?;
    let response_shape_matches = match resume_claim {
        None => metadata.status == 200,
        Some(claim) => {
            metadata.status == 206
                && metadata.etag.as_deref() == Some(claim.strong_etag())
                && valid_strong_etag(claim.strong_etag())
                && attempt.attempt_sha256() != claim.segment_attempt().attempt_sha256()
        }
    };
    if parsed.as_str() != plan.selected_file.download_url
        || !response_shape_matches
        || metadata.range_start != expected_range_start
        || metadata.content_length != expected_content_length
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

const fn combine_chronology(
    prior: Option<CaptureChronologyDisposition>,
    resumed_boundary: CaptureChronologyDisposition,
    current: CaptureChronologyDisposition,
    cumulative_monotonic_duration_nanos: u64,
    max_download_duration_nanos: u64,
) -> CaptureChronologyDisposition {
    if let Some(CaptureChronologyDisposition::Quarantined(reason)) = prior {
        return CaptureChronologyDisposition::Quarantined(reason);
    }
    if let CaptureChronologyDisposition::Quarantined(reason) = resumed_boundary {
        return CaptureChronologyDisposition::Quarantined(reason);
    }
    if let CaptureChronologyDisposition::Quarantined(reason) = current {
        return CaptureChronologyDisposition::Quarantined(reason);
    }
    if cumulative_monotonic_duration_nanos > max_download_duration_nanos {
        return CaptureChronologyDisposition::Quarantined(
            CaptureClockAnomaly::DownloadDurationExceeded,
        );
    }
    CaptureChronologyDisposition::Admitted
}

/// Classifies exact clock continuity from a physically adopted prefix into its successor attempt.
///
/// The new attempt's admission, response, and terminal readings must each remain at or after the
/// prior physical checkpoint within the immutable regression tolerance. This is separate from the
/// segment-local classifier because every segment can be locally ordered while the authority clock
/// still regresses between attempts.
fn resumed_boundary_disposition(
    prior_claim: Option<&IexHistResumeClaim>,
    attempt_admitted_clock: IexHistTrustedClockReading,
    response_started_clock: IexHistTrustedClockReading,
    terminal_clock: IexHistTrustedClockReading,
    max_clock_regression_nanos: u64,
) -> CaptureChronologyDisposition {
    let Some(prior_checkpoint_clock) = prior_claim.map(IexHistResumeClaim::checkpoint_clock) else {
        return CaptureChronologyDisposition::Admitted;
    };
    if attempt_admitted_clock.observed_date() < prior_checkpoint_clock.observed_date()
        || response_started_clock.observed_date() < prior_checkpoint_clock.observed_date()
        || terminal_clock.observed_date() < prior_checkpoint_clock.observed_date()
    {
        return CaptureChronologyDisposition::Quarantined(CaptureClockAnomaly::CalendarRegression);
    }
    let tolerance = i64::try_from(max_clock_regression_nanos).unwrap_or(i64::MAX);
    if [
        attempt_admitted_clock,
        response_started_clock,
        terminal_clock,
    ]
    .into_iter()
    .any(|current| {
        current
            .unix_nanos()
            .checked_add(tolerance)
            .unwrap_or(i64::MAX)
            < prior_checkpoint_clock.unix_nanos()
    }) {
        return CaptureChronologyDisposition::Quarantined(CaptureClockAnomaly::WallClockRegression);
    }
    CaptureChronologyDisposition::Admitted
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
        return CaptureChronologyDisposition::Quarantined(CaptureClockAnomaly::CalendarRegression);
    }
    let tolerated_completion = completed_at_unix_nanos
        .checked_add(i64::try_from(max_clock_regression_nanos).unwrap_or(i64::MAX))
        .unwrap_or(i64::MAX);
    if tolerated_completion < response_started_at_unix_nanos {
        return CaptureChronologyDisposition::Quarantined(CaptureClockAnomaly::WallClockRegression);
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
    attempt: IexHistExecutionAttempt,
    object_encoding: PcapObjectEncoding,
    compressed_sha256: Sha256Digest,
    compressed_bytes: u64,
    pcap_sha256: Sha256Digest,
    pcap_bytes: u64,
    resume_adoption: Option<&IexHistResumeAdoptionReceipt>,
    gzip_crc32: Option<u32>,
    etag: Option<&str>,
    response_status: u16,
    response_range_start: u64,
    response_content_length: u64,
    response_started_at_unix_nanos: i64,
    response_started_utc_offset_seconds: i32,
    response_started_observed_date: &str,
    completed_at_unix_nanos: i64,
    completed_utc_offset_seconds: i32,
    completed_observed_date: &str,
    segment_monotonic_duration_nanos: u64,
    cumulative_monotonic_duration_nanos: u64,
    chronology_disposition: CaptureChronologyDisposition,
) -> Sha256Digest {
    let etag_present = [u8::from(etag.is_some())];
    let etag_bytes = etag.unwrap_or_default().as_bytes();
    let gzip_crc_present = [u8::from(gzip_crc32.is_some())];
    let gzip_crc = gzip_crc32.unwrap_or_default().to_le_bytes();
    let resume_present = [u8::from(resume_adoption.is_some())];
    let resume_identity = resume_adoption.map_or_else(
        || Sha256Digest::of(b"no-iex-hist-resume-adoption"),
        IexHistResumeAdoptionReceipt::receipt_sha256,
    );
    crate::catalog::digest_fields(&[
        b"market-squawk/iex-hist-pcap-materialization/v6",
        plan_sha256.as_bytes(),
        selected_file_identity.as_bytes(),
        attempt.request_sha256().as_bytes(),
        attempt.reservation_sha256().as_bytes(),
        &attempt.authority_generation().to_le_bytes(),
        attempt.storage_root_sha256().as_bytes(),
        &attempt.admitted_clock().unix_nanos().to_le_bytes(),
        &attempt.admitted_clock().utc_offset_seconds().to_le_bytes(),
        attempt
            .admitted_clock()
            .observed_date()
            .compact()
            .as_bytes(),
        &attempt.deadline_unix_nanos().to_le_bytes(),
        attempt.attempt_sha256().as_bytes(),
        object_encoding.identity_value().as_bytes(),
        compressed_sha256.as_bytes(),
        &compressed_bytes.to_le_bytes(),
        pcap_sha256.as_bytes(),
        &pcap_bytes.to_le_bytes(),
        &resume_present,
        resume_identity.as_bytes(),
        &gzip_crc_present,
        &gzip_crc,
        &etag_present,
        etag_bytes,
        &response_status.to_le_bytes(),
        &response_range_start.to_le_bytes(),
        &response_content_length.to_le_bytes(),
        &response_started_at_unix_nanos.to_le_bytes(),
        &response_started_utc_offset_seconds.to_le_bytes(),
        response_started_observed_date.as_bytes(),
        &completed_at_unix_nanos.to_le_bytes(),
        &completed_utc_offset_seconds.to_le_bytes(),
        completed_observed_date.as_bytes(),
        &segment_monotonic_duration_nanos.to_le_bytes(),
        &cumulative_monotonic_duration_nanos.to_le_bytes(),
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
    /// A serialized resume claim did not match the exact selected-file plan and chain.
    #[error("IEX HIST resumable-prefix claim is invalid")]
    InvalidResumeClaim,
    /// Re-read controlled prefix bytes did not match the claim length and digest.
    #[error("IEX HIST resumable-prefix bytes do not match their claim")]
    ResumePrefixMismatch,
    /// An incomplete response had no matching strong ETag and cannot be resumed safely.
    #[error("IEX HIST response has no strong validator for safe resume")]
    ResumeValidatorUnavailable,
    /// Cumulative monotonic time across resume segments overflowed.
    #[error("IEX HIST resumable-transfer duration overflowed")]
    ResumeDurationOverflow,
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
