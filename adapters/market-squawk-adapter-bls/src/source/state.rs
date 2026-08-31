//! Bounded page retention and local BLS producer state.

use std::collections::BTreeSet;
use std::mem::size_of;

use bytes::Bytes;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_sources::{MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES, SourceError};

use super::normalize::CanonicalBlsRecord;
use crate::client::{RetainedBlsPage, RetrievedBlsPage};

const CACHE_ENTRY_OVERHEAD_BYTES: usize = 512;

#[derive(Clone, Debug)]
pub(super) struct PageRetentionBudget {
    pub(super) retained_bytes: u64,
    pub(super) receipts: BTreeSet<String>,
    limit: u64,
}

impl PageRetentionBudget {
    pub(super) fn new() -> Self {
        Self::with_limit(MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES)
    }

    pub(super) fn with_limit(limit: u64) -> Self {
        Self {
            limit,
            retained_bytes: 0,
            receipts: BTreeSet::new(),
        }
    }

    pub(super) fn insert(
        &mut self,
        retention_key: &str,
        object_id: &SourceIdentifier,
        page: &RetainedBlsPage,
    ) -> Result<bool, SourceError> {
        if self.receipts.contains(retention_key) {
            return Err(SourceError::InvalidProtocolState);
        }
        // Every provider response owns a separately received allocation until the corresponding
        // sealed discovery admission consumes it. Equal payload digests do not prove shared
        // allocation ownership, so repeated responses must each consume the retention budget.
        let bytes = Self::retained_charge(retention_key, object_id, page)?;
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
        self.receipts.insert(retention_key.to_owned());
        Ok(true)
    }

    pub(super) fn retained_charge(
        retention_key: &str,
        object_id: &SourceIdentifier,
        page: &RetainedBlsPage,
    ) -> Result<u64, SourceError> {
        let occurrence = retention_key
            .len()
            .checked_add(object_id.retained_bytes())
            .and_then(|bytes| bytes.checked_add(page.sha256_hex.len()))
            .and_then(|bytes| bytes.checked_add(size_of::<RetainedBlsPage>()))
            .and_then(|bytes| bytes.checked_add(CACHE_ENTRY_OVERHEAD_BYTES))
            .ok_or(SourceError::FrameTooLarge {
                max: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
            })?;
        let payload = page
            .bytes
            .len()
            .checked_add(page.sha256_hex.len())
            .and_then(|bytes| bytes.checked_add(size_of::<Bytes>()))
            .and_then(|bytes| bytes.checked_add(CACHE_ENTRY_OVERHEAD_BYTES))
            .ok_or(SourceError::FrameTooLarge {
                max: MAX_IN_MEMORY_EXTRACTION_BATCH_BYTES as usize,
            })?;
        let charge = occurrence
            .checked_add(payload)
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
            .field("response_received_at", &self.response_received_at)
            .field("locally_available_at", &self.locally_available_at)
            .field("sha256_hex", &self.sha256_hex)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for RetainedBlsPage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedBlsPage")
            .field("bytes", &self.bytes.len())
            .field("response_received_at", &self.response_received_at)
            .field("locally_available_at", &self.locally_available_at)
            .field("sha256_hex", &self.sha256_hex)
            .finish_non_exhaustive()
    }
}

/// A normalized BLS response page retaining local availability and exact source evidence.
#[derive(Debug)]
pub struct BlsNormalizedPage {
    pub(super) locally_available_at: Timestamp,
    pub(super) response_received_at: Timestamp,
    pub(super) source_payload_sha256: String,
    pub(super) exact_payload: Bytes,
    pub(super) payloads: Vec<Bytes>,
    pub(super) records: Vec<CanonicalBlsRecord>,
    pub(super) response: crate::BlsResponse,
    pub(super) canonical_ingested_at: Timestamp,
}

impl BlsNormalizedPage {
    /// Returns when the exact bounded response became completely available locally.
    pub const fn locally_available_at(&self) -> Timestamp {
        self.locally_available_at
    }

    /// Returns when provider response headers first became available to the transport.
    pub const fn response_received_at(&self) -> Timestamp {
        self.response_received_at
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
