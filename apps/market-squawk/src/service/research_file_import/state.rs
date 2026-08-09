//! Crash-safe state for guided, workspace-controlled research-file imports.

use std::collections::BTreeSet;

use market_squawk_platform::LocalAuthorityStateStore;
use market_squawk_services::ServiceError;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

pub(super) const MAXIMUM_IMPORTS: usize = 256;
pub(super) const MAXIMUM_JOB_START_ATTEMPTS: u16 = 8;
pub(super) const PENDING_PREVIEW_LIFETIME_NANOS: i64 = 60 * 60 * 1_000_000_000;
const SCHEMA_VERSION: u16 = 2;

pub(super) struct ImportAuthority {
    store: LocalAuthorityStateStore,
    manifest: ImportManifest,
}

impl ImportAuthority {
    pub(super) fn open(store: LocalAuthorityStateStore) -> Result<Self, ServiceError> {
        let manifest = store
            .load()
            .map_err(|_error| ServiceError::Unavailable)?
            .map_or_else(
                || Ok(ImportManifest::empty()),
                |bytes| ImportManifest::decode(&bytes),
            )?;
        Ok(Self { store, manifest })
    }

    pub(super) fn entries(&self) -> &[ImportEntry] {
        &self.manifest.entries
    }

    pub(super) fn entry(&self, preview_id: &str) -> Option<&ImportEntry> {
        self.manifest
            .entries
            .iter()
            .find(|entry| entry.preview_id == preview_id)
    }

    pub(super) fn append_pending(
        &mut self,
        entry: ImportEntry,
        now_unix_nanos: i64,
    ) -> Result<Vec<ImportEntry>, ServiceError> {
        if entry.phase != ImportPhase::Pending
            || entry.pending_expired(now_unix_nanos)
            || self.entry(&entry.preview_id).is_some()
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut candidate = self.manifest.clone();
        let mut removed = Vec::new();
        candidate.entries.retain(|retained| {
            let replace = retained.phase == ImportPhase::Pending
                && (retained.same_owner(&entry) || retained.pending_expired(now_unix_nanos));
            if replace {
                removed.push(retained.clone());
            }
            !replace
        });
        if candidate.entries.len() >= MAXIMUM_IMPORTS {
            return Err(ServiceError::ResourceExhausted);
        }
        candidate.entries.push(entry);
        candidate
            .entries
            .sort_by(|left, right| left.preview_id.cmp(&right.preview_id));
        self.persist(candidate)?;
        Ok(removed)
    }

    pub(super) fn replace(&mut self, entry: ImportEntry) -> Result<(), ServiceError> {
        let mut candidate = self.manifest.clone();
        let retained = candidate
            .entries
            .iter_mut()
            .find(|candidate| candidate.preview_id == entry.preview_id)
            .ok_or(ServiceError::NotFound)?;
        *retained = entry;
        self.persist(candidate)
    }

    pub(super) fn remove(&mut self, preview_id: &str) -> Result<ImportEntry, ServiceError> {
        let mut candidate = self.manifest.clone();
        let index = candidate
            .entries
            .iter()
            .position(|entry| entry.preview_id == preview_id)
            .ok_or(ServiceError::NotFound)?;
        let removed = candidate.entries.remove(index);
        self.persist(candidate)?;
        Ok(removed)
    }

    pub(super) fn remove_expired_pending(
        &mut self,
        now_unix_nanos: i64,
    ) -> Result<Vec<ImportEntry>, ServiceError> {
        self.remove_pending_where(|entry| entry.pending_expired(now_unix_nanos))
    }

    pub(super) fn remove_all_pending(&mut self) -> Result<Vec<ImportEntry>, ServiceError> {
        self.remove_pending_where(|_entry| true)
    }

    fn remove_pending_where(
        &mut self,
        remove: impl Fn(&ImportEntry) -> bool,
    ) -> Result<Vec<ImportEntry>, ServiceError> {
        let mut candidate = self.manifest.clone();
        let mut removed = Vec::new();
        candidate.entries.retain(|entry| {
            let discard = entry.phase == ImportPhase::Pending && remove(entry);
            if discard {
                removed.push(entry.clone());
            }
            !discard
        });
        if removed.is_empty() {
            return Ok(removed);
        }
        self.persist(candidate)?;
        Ok(removed)
    }

    fn persist(&mut self, candidate: ImportManifest) -> Result<(), ServiceError> {
        candidate.validate()?;
        let bytes = serde_json::to_vec(&candidate).map_err(|_error| ServiceError::Internal)?;
        self.store
            .store(&bytes)
            .map_err(|_error| ServiceError::Unavailable)?;
        self.manifest = candidate;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ImportManifest {
    schema_version: u16,
    entries: Vec<ImportEntry>,
}

impl ImportManifest {
    const fn empty() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            entries: Vec::new(),
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, ServiceError> {
        let manifest: Self =
            serde_json::from_slice(bytes).map_err(|_error| ServiceError::Internal)?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), ServiceError> {
        if self.schema_version != SCHEMA_VERSION || self.entries.len() > MAXIMUM_IMPORTS {
            return Err(ServiceError::Internal);
        }
        let mut preview_ids = BTreeSet::new();
        let mut pending_owners = BTreeSet::new();
        for entry in &self.entries {
            entry.validate()?;
            if !preview_ids.insert(entry.preview_id.as_str()) {
                return Err(ServiceError::Internal);
            }
            if entry.phase == ImportPhase::Pending
                && !pending_owners.insert((entry.workspace_id.as_str(), entry.client_id.as_str()))
            {
                return Err(ServiceError::Internal);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ImportEntry {
    pub(super) preview_id: String,
    pub(super) ticket_id: String,
    pub(super) workspace_id: String,
    pub(super) client_id: String,
    pub(super) source_sha256: String,
    pub(super) source_bytes: u64,
    pub(super) object_reference: String,
    pub(super) format: StoredFileFormat,
    pub(super) admitted_at_unix_nanos: i64,
    pub(super) phase: ImportPhase,
    pub(super) mapping: Option<ResearchFileMapping>,
    pub(super) activation: Option<ActivationRecord>,
    pub(super) job_start: Option<StoredJobStart>,
    pub(super) job_receipt: Option<StoredJobReceipt>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(in crate::service) struct StoredJobStart {
    attempt: u16,
    job_id: String,
    request_id: String,
    admitted_at_unix_nanos: i64,
}

impl StoredJobStart {
    pub(super) fn first(
        entry: &ImportEntry,
        admitted_at_unix_nanos: i64,
    ) -> Result<Self, ServiceError> {
        Self::for_attempt(entry, 1, admitted_at_unix_nanos)
    }

    pub(super) fn next(
        &self,
        entry: &ImportEntry,
        admitted_at_unix_nanos: i64,
    ) -> Result<Option<Self>, ServiceError> {
        if self.attempt >= MAXIMUM_JOB_START_ATTEMPTS {
            return Ok(None);
        }
        let attempt = self.attempt.checked_add(1).ok_or(ServiceError::Internal)?;
        Self::for_attempt(entry, attempt, admitted_at_unix_nanos).map(Some)
    }

    fn for_attempt(
        entry: &ImportEntry,
        attempt: u16,
        admitted_at_unix_nanos: i64,
    ) -> Result<Self, ServiceError> {
        if attempt == 0 || attempt > MAXIMUM_JOB_START_ATTEMPTS || admitted_at_unix_nanos <= 0 {
            return Err(ServiceError::Internal);
        }
        let job_id = deterministic_job_id(entry, attempt)?;
        Ok(Self {
            attempt,
            job_id: job_id.hyphenated().to_string(),
            request_id: format!("research-file-import-{}-{attempt}", entry.preview_id),
            admitted_at_unix_nanos,
        })
    }

    pub(in crate::service) fn job_id(&self) -> &str {
        &self.job_id
    }

    pub(in crate::service) fn request_id(&self) -> &str {
        &self.request_id
    }

    pub(in crate::service) const fn admitted_at_unix_nanos(&self) -> i64 {
        self.admitted_at_unix_nanos
    }

    fn validate_for(&self, entry: &ImportEntry) -> Result<(), ServiceError> {
        let expected = Self::for_attempt(entry, self.attempt, self.admitted_at_unix_nanos)?;
        if self != &expected
            || entry.activation.as_ref().is_none_or(|activation| {
                activation.admitted_at_unix_nanos != self.admitted_at_unix_nanos
            })
        {
            return Err(ServiceError::Internal);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(in crate::service) struct StoredJobReceipt {
    job_id: String,
    generation: u64,
    sequence: u64,
    state: StoredJobReceiptState,
}

impl StoredJobReceipt {
    pub(in crate::service) fn queued(job_id: &str) -> Result<Self, ServiceError> {
        let receipt = Self {
            job_id: job_id.to_owned(),
            generation: 1,
            sequence: 0,
            state: StoredJobReceiptState::Queued,
        };
        receipt.validate()?;
        Ok(receipt)
    }

    pub(super) fn decode(value: serde_json::Value) -> Result<Self, ServiceError> {
        let receipt: Self =
            serde_json::from_value(value).map_err(|_error| ServiceError::Internal)?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub(super) fn encode(&self) -> Result<serde_json::Value, ServiceError> {
        serde_json::to_value(self).map_err(|_error| ServiceError::Internal)
    }

    pub(in crate::service) fn job_id(&self) -> &str {
        &self.job_id
    }

    pub(in crate::service) const fn generation(&self) -> u64 {
        self.generation
    }

    fn validate(&self) -> Result<(), ServiceError> {
        if Uuid::parse_str(&self.job_id).is_err()
            || self.generation != 1
            || self.sequence != 0
            || self.state != StoredJobReceiptState::Queued
        {
            return Err(ServiceError::Internal);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum StoredJobReceiptState {
    Queued,
}

impl ImportEntry {
    pub(super) fn same_owner(&self, other: &Self) -> bool {
        self.workspace_id == other.workspace_id && self.client_id == other.client_id
    }

    pub(super) fn pending_expired(&self, now_unix_nanos: i64) -> bool {
        self.phase == ImportPhase::Pending
            && self
                .admitted_at_unix_nanos
                .checked_add(PENDING_PREVIEW_LIFETIME_NANOS)
                .is_none_or(|expires_at| expires_at <= now_unix_nanos)
    }

    fn validate(&self) -> Result<(), ServiceError> {
        if !is_sha256(&self.preview_id)
            || !is_sha256(&self.source_sha256)
            || self.source_bytes == 0
            || self.source_bytes > super::MAXIMUM_RESEARCH_FILE_BYTES
            || Uuid::parse_str(&self.ticket_id).is_err()
            || Uuid::parse_str(&self.workspace_id).is_err()
            || Uuid::parse_str(&self.client_id).is_err()
            || self.object_reference
                != format!("objects/{}.{}", self.source_sha256, self.format.extension())
        {
            return Err(ServiceError::Internal);
        }
        let valid_phase = match self.phase {
            ImportPhase::Pending => {
                self.mapping.is_none()
                    && self.activation.is_none()
                    && self.job_start.is_none()
                    && self.job_receipt.is_none()
            }
            ImportPhase::Promoting => {
                self.mapping.is_some()
                    && self.activation.is_some()
                    && self.job_start.is_some()
                    && self.job_receipt.is_none()
            }
            ImportPhase::Committed => {
                self.mapping.is_some()
                    && self.activation.is_some()
                    && self.job_start.is_some()
                    && self.job_receipt.is_some()
            }
        };
        if !valid_phase {
            return Err(ServiceError::Internal);
        }
        if let Some(activation) = &self.activation {
            activation.validate()?;
        }
        if let Some(start) = &self.job_start {
            start.validate_for(self)?;
        }
        if let Some(receipt) = &self.job_receipt {
            receipt.validate()?;
            if self
                .job_start
                .as_ref()
                .is_none_or(|start| start.job_id != receipt.job_id)
            {
                return Err(ServiceError::Internal);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ImportPhase {
    Pending,
    Promoting,
    Committed,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum StoredFileFormat {
    Csv,
    Json,
    Ndjson,
    Parquet,
}

impl StoredFileFormat {
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Ndjson => "ndjson",
            Self::Parquet => "parquet",
        }
    }

    pub(super) const fn extension(self) -> &'static str {
        self.name()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ResearchFileMapping {
    pub(super) dataset: String,
    pub(super) identity_field: String,
    pub(super) fields: Vec<ResearchValueMapping>,
    pub(super) effective_at: String,
    pub(super) published_at: Option<String>,
    pub(super) effective_field: Option<String>,
    pub(super) published_field: Option<String>,
    pub(super) available_field: Option<String>,
    pub(super) revision_field: Option<String>,
    pub(super) revision_number_field: Option<String>,
    pub(super) superseded_field: Option<String>,
    pub(super) instrument_id: Option<Uuid>,
    pub(super) universe: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(super) struct ResearchValueMapping {
    pub(super) source: String,
    pub(super) field: String,
    pub(super) decimal_scale: u32,
    pub(super) unit: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ActivationRecord {
    pub(super) root_reference: String,
    pub(super) manifest_reference: String,
    pub(super) manifest_sha256: String,
    pub(super) admitted_input_set_sha256: String,
    pub(super) local_admission_evidence_sha256: String,
    pub(super) workspace_receipt_evidence_sha256: String,
    pub(super) import_receipt_evidence_sha256: String,
    pub(super) admitted_at_unix_nanos: i64,
}

impl ActivationRecord {
    fn validate(&self) -> Result<(), ServiceError> {
        if self.root_reference.is_empty()
            || self.root_reference.len() > 256
            || self.manifest_reference.is_empty()
            || self.manifest_reference.len() > 512
            || [
                self.manifest_sha256.as_str(),
                self.admitted_input_set_sha256.as_str(),
                self.local_admission_evidence_sha256.as_str(),
                self.workspace_receipt_evidence_sha256.as_str(),
                self.import_receipt_evidence_sha256.as_str(),
            ]
            .into_iter()
            .any(|value| !is_sha256(value))
        {
            return Err(ServiceError::Internal);
        }
        Ok(())
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        && value.bytes().any(|byte| byte != b'0')
}

fn deterministic_job_id(entry: &ImportEntry, attempt: u16) -> Result<Uuid, ServiceError> {
    let workspace_id =
        Uuid::parse_str(&entry.workspace_id).map_err(|_error| ServiceError::Internal)?;
    let client_id = Uuid::parse_str(&entry.client_id).map_err(|_error| ServiceError::Internal)?;
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/research-file-import/job-id/v1\0");
    digest.update(workspace_id.as_bytes());
    digest.update(client_id.as_bytes());
    digest.update(entry.preview_id.as_bytes());
    digest.update(attempt.to_be_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let id = Uuid::from_bytes(bytes);
    if id.is_nil() {
        Err(ServiceError::Internal)
    } else {
        Ok(id)
    }
}
