//! Bounded exact-body capture for unpaginated HTTP responses.

use std::io::{self, Read};

use bytes::Bytes;
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

const MAX_CAPTURE_URL_BYTES: usize = 2_048;
const MAX_SEGMENTED_HTTP_BODY_BYTES: u64 = 256 * 1024 * 1024;
const MAX_SEGMENTED_HTTP_SEGMENTS: usize = 64;

/// Closed HTTP request method identity retained by a capture receipt.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum HttpCaptureMethod {
    /// Idempotent resource retrieval.
    Get,
}

/// Digest and length of one exact bounded response-body segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpResponseSegmentReceipt {
    ordinal: u32,
    body_length: u64,
    body_digest: EvidenceDigest,
}

impl HttpResponseSegmentReceipt {
    /// Returns the zero-based segment ordinal.
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }

    /// Returns the exact segment byte length.
    pub const fn body_length(self) -> u64 {
        self.body_length
    }

    /// Returns SHA-256 over the exact segment bytes.
    pub const fn body_digest(self) -> EvidenceDigest {
        self.body_digest
    }
}

/// One receipt binding response metadata, the complete body, and every retained segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentedHttpResponseReceipt {
    method: HttpCaptureMethod,
    final_url: Box<str>,
    status: u16,
    declared_body_length: Option<u64>,
    body_length: u64,
    body_digest: EvidenceDigest,
    segments: Box<[HttpResponseSegmentReceipt]>,
}

impl SegmentedHttpResponseReceipt {
    /// Returns the exact request method.
    pub const fn method(&self) -> HttpCaptureMethod {
        self.method
    }

    /// Returns the admitted final response URL after redirect handling.
    pub fn final_url(&self) -> &str {
        &self.final_url
    }

    /// Returns the exact HTTP response status.
    pub const fn status(&self) -> u16 {
        self.status
    }

    /// Returns the provider-declared body length, when supplied.
    pub const fn declared_body_length(&self) -> Option<u64> {
        self.declared_body_length
    }

    /// Returns the exact observed body byte length.
    pub const fn body_length(&self) -> u64 {
        self.body_length
    }

    /// Returns SHA-256 over the complete body in segment order.
    pub const fn body_digest(&self) -> EvidenceDigest {
        self.body_digest
    }

    /// Returns exact per-segment receipts in body order.
    pub fn segments(&self) -> &[HttpResponseSegmentReceipt] {
        &self.segments
    }
}

#[derive(Clone, Debug)]
struct CapturedHttpSegment {
    bytes: Bytes,
}

/// Exact segmented response bytes plus their immutable receipt.
#[derive(Clone, Debug)]
pub struct SegmentedHttpResponseCapture {
    receipt: SegmentedHttpResponseReceipt,
    segments: Box<[CapturedHttpSegment]>,
}

impl SegmentedHttpResponseCapture {
    /// Returns the complete response receipt.
    pub const fn receipt(&self) -> &SegmentedHttpResponseReceipt {
        &self.receipt
    }

    /// Returns the exact complete body length.
    pub const fn body_length(&self) -> u64 {
        self.receipt.body_length
    }

    /// Returns a zero-copy segmented reader suitable for streaming decoders.
    pub fn reader(&self) -> SegmentedHttpResponseReader<'_> {
        SegmentedHttpResponseReader {
            segments: &self.segments,
            segment_index: 0,
            byte_index: 0,
        }
    }
}

/// `Read` view over exact captured segments without constructing one oversized contiguous frame.
#[derive(Debug)]
pub struct SegmentedHttpResponseReader<'a> {
    segments: &'a [CapturedHttpSegment],
    segment_index: usize,
    byte_index: usize,
}

impl Read for SegmentedHttpResponseReader<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        let mut written = 0_usize;
        while written < output.len() {
            let Some(segment) = self.segments.get(self.segment_index) else {
                break;
            };
            let remaining = segment
                .bytes
                .get(self.byte_index..)
                .ok_or_else(|| io::Error::other("segmented HTTP reader invariant failed"))?;
            if remaining.is_empty() {
                self.segment_index += 1;
                self.byte_index = 0;
                continue;
            }
            let count = remaining.len().min(output.len() - written);
            output[written..written + count].copy_from_slice(&remaining[..count]);
            written += count;
            self.byte_index += count;
            if self.byte_index == segment.bytes.len() {
                self.segment_index += 1;
                self.byte_index = 0;
            }
        }
        Ok(written)
    }
}

/// Single-use bounded response capture builder.
#[derive(Debug)]
pub struct SegmentedHttpResponseBuilder {
    method: HttpCaptureMethod,
    final_url: Box<str>,
    status: u16,
    declared_body_length: Option<u64>,
    max_body_bytes: u64,
    max_segments: usize,
    body_length: u64,
    body_hasher: Sha256,
    segments: Vec<CapturedHttpSegment>,
    segment_receipts: Vec<HttpResponseSegmentReceipt>,
    failed: bool,
}

impl SegmentedHttpResponseBuilder {
    /// Starts one exact response capture under explicit body and segment limits.
    ///
    /// # Errors
    ///
    /// Rejects an insecure/ambiguous final URL, invalid status, or excessive limits.
    #[allow(
        clippy::too_many_arguments,
        reason = "HTTP response identity and all boundedness inputs remain explicit"
    )]
    pub fn try_new(
        method: HttpCaptureMethod,
        final_url: &str,
        status: u16,
        declared_body_length: Option<u64>,
        max_body_bytes: u64,
        max_segments: usize,
    ) -> Result<Self, SegmentedHttpCaptureError> {
        validate_final_url(final_url)?;
        if !(100..=599).contains(&status)
            || max_body_bytes == 0
            || max_body_bytes > MAX_SEGMENTED_HTTP_BODY_BYTES
            || max_segments == 0
            || max_segments > MAX_SEGMENTED_HTTP_SEGMENTS
            || declared_body_length.is_some_and(|length| length > max_body_bytes)
        {
            return Err(SegmentedHttpCaptureError::InvalidBounds);
        }
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(max_segments)
            .map_err(|_| SegmentedHttpCaptureError::Allocation)?;
        let mut segment_receipts = Vec::new();
        segment_receipts
            .try_reserve_exact(max_segments)
            .map_err(|_| SegmentedHttpCaptureError::Allocation)?;
        Ok(Self {
            method,
            final_url: final_url.to_owned().into_boxed_str(),
            status,
            declared_body_length,
            max_body_bytes,
            max_segments,
            body_length: 0,
            body_hasher: Sha256::new(),
            segments,
            segment_receipts,
            failed: false,
        })
    }

    /// Appends one nonempty exact segment no larger than the global raw-frame ceiling.
    ///
    /// Any rejection poisons the builder so a partial response cannot later be completed.
    pub fn try_push_segment(&mut self, bytes: Bytes) -> Result<(), SegmentedHttpCaptureError> {
        let outcome = (|| {
            if self.failed {
                return Err(SegmentedHttpCaptureError::Incomplete);
            }
            if bytes.is_empty() || bytes.len() > crate::MAX_RAW_FRAME_BYTES {
                return Err(SegmentedHttpCaptureError::InvalidSegment);
            }
            if self.segments.len() == self.max_segments {
                return Err(SegmentedHttpCaptureError::SegmentCountExceeded);
            }
            let segment_length =
                u64::try_from(bytes.len()).map_err(|_| SegmentedHttpCaptureError::BodyTooLarge)?;
            let next_length = self
                .body_length
                .checked_add(segment_length)
                .ok_or(SegmentedHttpCaptureError::BodyTooLarge)?;
            if next_length > self.max_body_bytes
                || self
                    .declared_body_length
                    .is_some_and(|declared| next_length > declared)
            {
                return Err(SegmentedHttpCaptureError::BodyTooLarge);
            }
            let ordinal = u32::try_from(self.segments.len())
                .map_err(|_| SegmentedHttpCaptureError::SegmentCountExceeded)?;
            let digest: [u8; 32] = Sha256::digest(&bytes).into();
            self.body_hasher.update(&bytes);
            self.segments.push(CapturedHttpSegment { bytes });
            self.segment_receipts.push(HttpResponseSegmentReceipt {
                ordinal,
                body_length: segment_length,
                body_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
            });
            self.body_length = next_length;
            Ok(())
        })();
        if outcome.is_err() {
            self.failed = true;
        }
        outcome
    }

    /// Finishes a complete body and verifies any declared content length exactly.
    pub fn finish(self) -> Result<SegmentedHttpResponseCapture, SegmentedHttpCaptureError> {
        if self.failed
            || self.segments.is_empty()
            || self
                .declared_body_length
                .is_some_and(|declared| declared != self.body_length)
        {
            return Err(SegmentedHttpCaptureError::Incomplete);
        }
        let body_digest: [u8; 32] = self.body_hasher.finalize().into();
        Ok(SegmentedHttpResponseCapture {
            receipt: SegmentedHttpResponseReceipt {
                method: self.method,
                final_url: self.final_url,
                status: self.status,
                declared_body_length: self.declared_body_length,
                body_length: self.body_length,
                body_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, body_digest),
                segments: self.segment_receipts.into_boxed_slice(),
            },
            segments: self.segments.into_boxed_slice(),
        })
    }
}

fn validate_final_url(value: &str) -> Result<(), SegmentedHttpCaptureError> {
    if value.is_empty() || value.len() > MAX_CAPTURE_URL_BYTES {
        return Err(SegmentedHttpCaptureError::InvalidFinalUrl);
    }
    let url = Url::parse(value).map_err(|_| SegmentedHttpCaptureError::InvalidFinalUrl)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(SegmentedHttpCaptureError::InvalidFinalUrl);
    }
    Ok(())
}

/// Bounded segmented-response capture failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SegmentedHttpCaptureError {
    /// The admitted final URL is invalid or unsafe.
    #[error("segmented HTTP capture final URL is invalid")]
    InvalidFinalUrl,
    /// Status, body, or segment limits are invalid.
    #[error("segmented HTTP capture bounds are invalid")]
    InvalidBounds,
    /// A segment was empty or exceeded the single-frame byte ceiling.
    #[error("segmented HTTP capture segment is invalid")]
    InvalidSegment,
    /// The configured segment count was exceeded.
    #[error("segmented HTTP capture segment count was exceeded")]
    SegmentCountExceeded,
    /// The observed or declared body exceeded the configured byte ceiling.
    #[error("segmented HTTP capture body is too large")]
    BodyTooLarge,
    /// Exact bounded storage could not be reserved.
    #[error("segmented HTTP capture allocation failed")]
    Allocation,
    /// Capture was poisoned, empty, truncated, or disagreed with the declared length.
    #[error("segmented HTTP capture is incomplete")]
    Incomplete,
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;

    use bytes::Bytes;
    use sha2::{Digest as _, Sha256};

    use super::{HttpCaptureMethod, SegmentedHttpResponseBuilder};
    use crate::MAX_RAW_FRAME_BYTES;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn one_receipt_binds_a_snapshot_larger_than_the_single_frame_ceiling() -> TestResult {
        let tail_bytes = 17_usize;
        let total = MAX_RAW_FRAME_BYTES
            .checked_add(tail_bytes)
            .ok_or("fixture length overflow")?;
        let mut builder = SegmentedHttpResponseBuilder::try_new(
            HttpCaptureMethod::Get,
            "https://api.exchange.coinbase.com/products/BTC-USD/book?level=3",
            200,
            Some(u64::try_from(total)?),
            u64::try_from(total)?,
            2,
        )?;
        builder.try_push_segment(Bytes::from(vec![b'a'; MAX_RAW_FRAME_BYTES]))?;
        builder.try_push_segment(Bytes::from(vec![b'b'; tail_bytes]))?;
        let capture = builder.finish()?;

        assert!(capture.body_length() > u64::try_from(MAX_RAW_FRAME_BYTES)?);
        assert_eq!(capture.receipt().segments().len(), 2);
        assert_eq!(
            capture.receipt().segments()[0].body_length(),
            u64::try_from(MAX_RAW_FRAME_BYTES)?
        );
        assert_eq!(
            capture.receipt().segments()[1].body_length(),
            u64::try_from(tail_bytes)?
        );
        assert_eq!(capture.receipt().method(), HttpCaptureMethod::Get);
        assert_eq!(capture.receipt().status(), 200);
        assert_eq!(
            capture.receipt().final_url(),
            "https://api.exchange.coinbase.com/products/BTC-USD/book?level=3"
        );

        let mut body = Vec::new();
        capture.reader().read_to_end(&mut body)?;
        assert_eq!(body.len(), total);
        assert_eq!(
            capture.receipt().body_digest().bytes(),
            <[u8; 32]>::from(Sha256::digest(&body))
        );
        Ok(())
    }
}
