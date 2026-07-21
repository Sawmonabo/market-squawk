//! Raw row decoding through current bounded source contracts.

use market_squawk_domain::{RevisionNumber, Timestamp};
use market_squawk_sources::{
    ObservedRevisionError, ObservedSemanticPayload, ObservedVersionEvidence, ObservedVersionKind,
};
use rusqlite::Row;

use super::canonical::{decode_provider_order, require_digest};
use super::{PAYLOAD_EVIDENCE_VERSION, StoredObservedRevision, VERSION_EVIDENCE_VERSION};

#[derive(Debug)]
pub(super) struct StoredVersionRow {
    revision: i64,
    version_kind: String,
    version_algorithm: i64,
    version_digest: Vec<u8>,
    version_evidence_version: i64,
    version_evidence: Vec<u8>,
    payload_algorithm: i64,
    payload_digest: Vec<u8>,
    payload_evidence_version: i64,
    payload_evidence: Vec<u8>,
    provider_order_evidence_version: Option<i64>,
    provider_coordinate_json: Option<String>,
    provider_tie_breaker: Option<Vec<u8>>,
    assigned_at_ns: i64,
}

impl StoredVersionRow {
    pub(super) fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            revision: row.get(0)?,
            version_kind: row.get(1)?,
            version_algorithm: row.get(2)?,
            version_digest: row.get(3)?,
            version_evidence_version: row.get(4)?,
            version_evidence: row.get(5)?,
            payload_algorithm: row.get(6)?,
            payload_digest: row.get(7)?,
            payload_evidence_version: row.get(8)?,
            payload_evidence: row.get(9)?,
            provider_order_evidence_version: row.get(10)?,
            provider_coordinate_json: row.get(11)?,
            provider_tie_breaker: row.get(12)?,
            assigned_at_ns: row.get(13)?,
        })
    }

    pub(super) fn retained_components(&self) -> [usize; 7] {
        [
            self.version_kind.len(),
            self.version_digest.len(),
            self.version_evidence.len(),
            self.payload_digest.len(),
            self.payload_evidence.len(),
            self.provider_coordinate_json
                .as_ref()
                .map_or(0, String::len),
            self.provider_tie_breaker.as_ref().map_or(0, Vec::len),
        ]
    }

    pub(super) fn decode(self) -> Result<StoredObservedRevision, ObservedRevisionError> {
        let revision = u32::try_from(self.revision)
            .ok()
            .and_then(|value| RevisionNumber::new(value).ok())
            .ok_or(ObservedRevisionError::CorruptAuthorityState)?;
        if self.version_evidence_version != VERSION_EVIDENCE_VERSION
            || self.payload_evidence_version != PAYLOAD_EVIDENCE_VERSION
        {
            return Err(ObservedRevisionError::CorruptAuthorityState);
        }
        let semantic_payload = ObservedSemanticPayload::try_from_bytes(&self.payload_evidence)
            .map_err(|_| ObservedRevisionError::CorruptAuthorityState)?;
        require_digest(
            self.payload_algorithm,
            &self.payload_digest,
            semantic_payload.identity(),
        )?;
        let version = match self.version_kind.as_str() {
            "provider_supplied" => {
                ObservedVersionEvidence::provider_supplied(&self.version_evidence)
            }
            "locally_observed_content" => {
                if self.version_evidence != self.payload_evidence {
                    return Err(ObservedRevisionError::CorruptAuthorityState);
                }
                ObservedVersionEvidence::locally_observed_content(&semantic_payload)
            }
            _ => return Err(ObservedRevisionError::CorruptAuthorityState),
        }
        .map_err(|_| ObservedRevisionError::CorruptAuthorityState)?;
        require_digest(
            self.version_algorithm,
            &self.version_digest,
            version.identity(),
        )?;
        let provider_order = decode_provider_order(
            self.provider_order_evidence_version,
            self.provider_coordinate_json,
            self.provider_tie_breaker,
        )?;
        if version.kind() == ObservedVersionKind::LocallyObservedContent && provider_order.is_some()
        {
            return Err(ObservedRevisionError::CorruptAuthorityState);
        }
        Ok(StoredObservedRevision {
            revision,
            version,
            semantic_payload,
            provider_order,
            assigned_at: Timestamp::from_unix_nanos(self.assigned_at_ns),
        })
    }
}
