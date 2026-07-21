//! Bounded page retention and local BLS producer state.

use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

use bytes::Bytes;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_sources::{MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, SourceError};

use super::normalize::CanonicalBlsRecord;
use crate::client::RetrievedBlsPage;

const CACHE_ENTRY_OVERHEAD_BYTES: usize = 512;

#[derive(Debug)]
pub(super) struct PageCache {
    pub(super) retained_bytes: u64,
    pub(super) pages: BTreeMap<String, CachedBlsPage>,
    limit: u64,
}

#[derive(Clone, Debug)]
pub(super) struct CachedBlsPage {
    pub(super) bytes: Arc<[u8]>,
    pub(super) received_at: Timestamp,
    pub(super) sha256_hex: String,
}

impl PageCache {
    pub(super) fn new() -> Self {
        Self::with_limit(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES)
    }

    pub(super) fn with_limit(limit: u64) -> Self {
        Self {
            limit,
            retained_bytes: 0,
            pages: BTreeMap::new(),
        }
    }

    pub(super) fn insert(
        &mut self,
        object_id: &SourceIdentifier,
        page: &RetrievedBlsPage,
    ) -> Result<bool, SourceError> {
        if self.pages.contains_key(object_id.as_str()) {
            return Ok(true);
        }
        let bytes = Self::retained_charge(object_id, page)?;
        let next = self
            .retained_bytes
            .checked_add(bytes)
            .ok_or(SourceError::FrameTooLarge {
                max: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
            })?;
        if next > self.limit {
            return Ok(false);
        }
        self.retained_bytes = next;
        self.pages.insert(
            object_id.as_str().to_owned(),
            CachedBlsPage {
                bytes: Arc::from(page.bytes.as_ref()),
                received_at: page.received_at,
                sha256_hex: page.sha256_hex.clone(),
            },
        );
        Ok(true)
    }

    pub(super) fn retained_charge(
        object_id: &SourceIdentifier,
        page: &RetrievedBlsPage,
    ) -> Result<u64, SourceError> {
        let charge = page
            .bytes
            .len()
            .checked_add(object_id.as_str().len())
            .and_then(|bytes| bytes.checked_add(page.sha256_hex.len()))
            .and_then(|bytes| bytes.checked_add(size_of::<CachedBlsPage>()))
            .and_then(|bytes| bytes.checked_add(CACHE_ENTRY_OVERHEAD_BYTES))
            .ok_or(SourceError::FrameTooLarge {
                max: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
            })?;
        u64::try_from(charge).map_err(|_| SourceError::FrameTooLarge {
            max: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
        })
    }
}

impl std::fmt::Debug for RetrievedBlsPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetrievedBlsPage")
            .field("bytes", &self.bytes.len())
            .field("received_at", &self.received_at)
            .field("sha256_hex", &self.sha256_hex)
            .finish_non_exhaustive()
    }
}

/// A normalized BLS response page retaining local availability and exact source evidence.
#[derive(Clone, Debug)]
pub struct BlsNormalizedPage {
    pub(super) received_at: Timestamp,
    pub(super) source_payload_sha256: String,
    pub(super) exact_payload: Bytes,
    pub(super) payloads: Vec<Bytes>,
    pub(super) records: Vec<CanonicalBlsRecord>,
}

impl BlsNormalizedPage {
    /// Returns the process-local first-observation time for this exact source response.
    pub const fn received_at(&self) -> Timestamp {
        self.received_at
    }

    /// Returns the lowercase SHA-256 identity of the exact provider response.
    pub fn source_payload_sha256(&self) -> &str {
        &self.source_payload_sha256
    }

    /// Returns exact provider bytes for durable raw-source persistence and audit.
    pub const fn exact_payload(&self) -> &Bytes {
        &self.exact_payload
    }

    /// Returns normalized research-v3 payloads without fabricated temporal precision.
    pub fn payloads(&self) -> &[Bytes] {
        &self.payloads
    }
}

/// Stable local health state for the bounded BLS research producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlsSourceHealth {
    pub(super) last_attempt_at: Option<Timestamp>,
    pub(super) last_success_at: Option<Timestamp>,
    pub(super) last_payload_digest: Option<[u8; 32]>,
    pub(super) consecutive_failures: u32,
}

impl BlsSourceHealth {
    pub(super) const fn new() -> Self {
        Self {
            last_attempt_at: None,
            last_success_at: None,
            last_payload_digest: None,
            consecutive_failures: 0,
        }
    }

    /// Returns the most recent local provider-request attempt.
    pub const fn last_attempt_at(self) -> Option<Timestamp> {
        self.last_attempt_at
    }

    /// Returns the provider receipt time for the most recent validated response.
    pub const fn last_success_at(self) -> Option<Timestamp> {
        self.last_success_at
    }

    /// Returns the SHA-256 digest of the most recent validated provider payload.
    pub const fn last_payload_digest(self) -> Option<[u8; 32]> {
        self.last_payload_digest
    }

    /// Returns saturating consecutive failures since the most recent success.
    pub const fn consecutive_failures(self) -> u32 {
        self.consecutive_failures
    }
}
