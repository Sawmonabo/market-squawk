//! Bounded SQLite staging for one-pass official option-reference publication streams.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use market_squawk_domain::{EvidenceDigest, SourceIdentifier};
use rusqlite::{Connection, OptionalExtension as _, Transaction, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::CatalogAuthority;
use super::official_options_reference::{
    MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS, MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS,
    OfficialOptionsReferenceAliasAssertionSetBuilder,
    OfficialOptionsReferenceAliasAssertionSetEvidence,
    OfficialOptionsReferenceAliasResolutionInput, OfficialOptionsReferenceConflictInput,
    OfficialOptionsReferenceConflictSetDigestBuilder, OfficialOptionsReferenceConflictSetEvidence,
    OfficialOptionsReferenceError, OfficialOptionsReferenceRecordInput,
    OfficialOptionsReferenceRecordSetDigestBuilder, OfficialOptionsReferenceRecordSetEvidence,
    OfficialOptionsReferenceRequestBinding, OfficialOptionsReferenceResolutionSetDigestBuilder,
    OfficialOptionsReferenceResolutionSetEvidence, alias_key_json, conflict_sort_key,
    record_sort_key,
};
use super::storage::{append_audit, parse_digest, trusted_catalog_now};

/// Maximum values accepted by one application push.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_BATCH_ROWS: usize = 4_096;
/// Maximum encoded bytes retained in one in-memory application push or replay page.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_BATCH_BYTES: usize = 4 * 1024 * 1024;
/// Maximum encoded typed-stage bytes retained beside the separately sealed raw closure.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_TOTAL_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Maximum concurrently recoverable official option-reference stages in one local catalog.
pub const MAX_OFFICIAL_OPTIONS_REFERENCE_STAGES: u64 = 16;

/// One open, provider-neutral append capability for an official option-reference request.
#[derive(Clone)]
pub struct OfficialOptionsReferenceStageCapability {
    authority: Arc<Mutex<CatalogAuthority>>,
    catalog_id: Uuid,
    dataset: SourceIdentifier,
    stage_id: SourceIdentifier,
    request_binding: OfficialOptionsReferenceRequestBinding,
}

/// Exact persisted cursors for resuming one open stage after interruption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceStageProgress {
    records: u64,
    resolutions: u64,
    conflicts: u64,
    encoded_bytes: u64,
}

impl OfficialOptionsReferenceStageProgress {
    /// Returns the number of complete typed records retained atomically.
    pub const fn records(self) -> u64 {
        self.records
    }

    /// Returns the number of complete alias resolutions retained atomically.
    pub const fn resolutions(self) -> u64 {
        self.resolutions
    }

    /// Returns the number of complete conflicts retained atomically.
    pub const fn conflicts(self) -> u64 {
        self.conflicts
    }

    /// Returns exact staged sort-key plus JSON bytes retained so far.
    pub const fn encoded_bytes(self) -> u64 {
        self.encoded_bytes
    }
}

impl std::fmt::Debug for OfficialOptionsReferenceStageCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OfficialOptionsReferenceStageCapability")
            .field("dataset", &self.dataset)
            .field("stage_id", &self.stage_id)
            .field("request_id", self.request_binding.request_id())
            .field("authority", &"[SEALED CATALOG AUTHORITY]")
            .finish()
    }
}

/// Immutable evidence for one complete typed stage that can be replayed after restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OfficialOptionsReferenceSealedStage {
    catalog_id: Uuid,
    dataset: SourceIdentifier,
    stage_id: SourceIdentifier,
    request_binding: OfficialOptionsReferenceRequestBinding,
    alias_assertions: OfficialOptionsReferenceAliasAssertionSetEvidence,
    records: OfficialOptionsReferenceRecordSetEvidence,
    resolutions: OfficialOptionsReferenceResolutionSetEvidence,
    conflicts: OfficialOptionsReferenceConflictSetEvidence,
    encoded_bytes: u64,
}

/// Restart disposition for one exact typed stage after its process response may have been lost.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OfficialOptionsReferenceStageRestartDisposition {
    /// The sealed stage has not committed a canonical generation and remains replayable.
    Sealed(OfficialOptionsReferenceSealedStage),
    /// The canonical transaction committed; only explicit acknowledgement may remove the tombstone.
    AlreadyPublished {
        dataset: SourceIdentifier,
        request_id: SourceIdentifier,
        generation_digest: EvidenceDigest,
    },
}

impl OfficialOptionsReferenceStageRestartDisposition {
    /// Returns the exact provider-neutral dataset coordinate.
    pub const fn dataset(&self) -> &SourceIdentifier {
        match self {
            Self::Sealed(stage) => stage.dataset(),
            Self::AlreadyPublished { dataset, .. } => dataset,
        }
    }

    /// Returns the exact acquisition request represented by the stage.
    pub const fn request_id(&self) -> &SourceIdentifier {
        match self {
            Self::Sealed(stage) => stage.request_id(),
            Self::AlreadyPublished { request_id, .. } => request_id,
        }
    }

    /// Returns the committed canonical generation when publication already completed.
    pub const fn generation_digest(&self) -> Option<EvidenceDigest> {
        match self {
            Self::Sealed(_) => None,
            Self::AlreadyPublished {
                generation_digest, ..
            } => Some(*generation_digest),
        }
    }
}

impl OfficialOptionsReferenceSealedStage {
    /// Reopens and re-verifies one sealed typed stage from the current catalog.
    pub fn try_reopen(
        authority: Arc<Mutex<CatalogAuthority>>,
        stage_id: SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<Self>, OfficialOptionsReferenceError> {
        check_operation(deadline, cancellation)?;
        let guard = authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        let catalog_id = guard.session_id();
        let connection = &guard.catalog().connection;
        let Some((
            dataset,
            request_id,
            request_binding_json,
            request_binding_digest,
            state,
            encoded_bytes,
        )) = connection
            .query_row(
                "SELECT dataset_id, request_id, request_binding_json, request_binding_digest,
                        state, encoded_bytes
                 FROM official_options_reference_stages WHERE stage_id=?1",
                [stage_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()?
        else {
            return Ok(None);
        };
        if state != "sealed" {
            return Ok(None);
        }
        let dataset = parse_identifier(dataset)?;
        let request_id = parse_identifier(request_id)?;
        let request_binding: OfficialOptionsReferenceRequestBinding =
            serde_json::from_str(&request_binding_json)?;
        request_binding.validate()?;
        if request_binding.request_id() != &request_id
            || request_binding.digest().bytes() != request_binding_digest.as_slice()
            || serde_json::to_string(&request_binding)? != request_binding_json
        {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        let encoded_bytes = parse_stage_bytes(encoded_bytes)?;
        let verified =
            verify_stage_streams(connection, &stage_id, &request_id, deadline, cancellation)?;
        verify_retained_evidence(connection, &stage_id, &verified, encoded_bytes)?;
        Ok(Some(Self {
            catalog_id,
            dataset,
            stage_id,
            request_binding,
            alias_assertions: verified.alias_assertions,
            records: verified.records,
            resolutions: verified.resolutions,
            conflicts: verified.conflicts,
            encoded_bytes,
        }))
    }

    /// Returns the exact provider-neutral dataset coordinate.
    pub const fn dataset(&self) -> &SourceIdentifier {
        &self.dataset
    }

    /// Returns the restart-stable typed-stage identity.
    pub const fn stage_id(&self) -> &SourceIdentifier {
        &self.stage_id
    }

    /// Returns the exact acquisition request represented by the stage.
    pub const fn request_id(&self) -> &SourceIdentifier {
        self.request_binding.request_id()
    }

    /// Returns the exact clocks and ordered surface request graph that admitted the stage.
    pub const fn request_binding(&self) -> &OfficialOptionsReferenceRequestBinding {
        &self.request_binding
    }

    /// Returns the exact multiset commitment derived from staged provider rows.
    pub const fn alias_assertions(&self) -> &OfficialOptionsReferenceAliasAssertionSetEvidence {
        &self.alias_assertions
    }

    /// Returns the exact ordered record-set evidence.
    pub const fn records(&self) -> OfficialOptionsReferenceRecordSetEvidence {
        self.records
    }

    /// Returns the exact ordered alias-resolution-set evidence.
    pub const fn resolutions(&self) -> OfficialOptionsReferenceResolutionSetEvidence {
        self.resolutions
    }

    /// Returns the exact ordered conflict-set evidence.
    pub const fn conflicts(&self) -> OfficialOptionsReferenceConflictSetEvidence {
        self.conflicts
    }

    /// Returns exact encoded typed-stage bytes, excluding separately sealed raw bytes.
    pub const fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    pub(super) const fn catalog_id(&self) -> Uuid {
        self.catalog_id
    }
}

pub(super) fn try_restart_stage(
    authority: Arc<Mutex<CatalogAuthority>>,
    stage_id: SourceIdentifier,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<Option<OfficialOptionsReferenceStageRestartDisposition>, OfficialOptionsReferenceError>
{
    check_operation(deadline, cancellation)?;
    let guard = authority
        .try_lock()
        .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
    let connection = &guard.catalog().connection;
    let Some((
        dataset,
        request_id,
        request_binding_json,
        request_binding_digest,
        state,
        record_count,
        resolution_count,
        conflict_count,
        record_set_digest,
        resolution_set_digest,
        conflict_set_digest,
        consumed_generation_digest,
    )) = connection
        .query_row(
            "SELECT dataset_id, request_id, request_binding_json, request_binding_digest,
                    state, record_count, resolution_count, conflict_count, record_set_digest,
                    resolution_set_digest, conflict_set_digest, consumed_generation_digest
             FROM official_options_reference_stages WHERE stage_id=?1",
            [stage_id.as_str()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, Option<Vec<u8>>>(8)?,
                    row.get::<_, Option<Vec<u8>>>(9)?,
                    row.get::<_, Option<Vec<u8>>>(10)?,
                    row.get::<_, Option<Vec<u8>>>(11)?,
                ))
            },
        )
        .optional()?
    else {
        return Ok(None);
    };
    if matches!(state.as_str(), "open" | "abandoning") {
        return Ok(None);
    }
    if state == "sealed" {
        drop(guard);
        return OfficialOptionsReferenceSealedStage::try_reopen(
            authority,
            stage_id,
            deadline,
            cancellation,
        )
        .map(|stage| stage.map(OfficialOptionsReferenceStageRestartDisposition::Sealed));
    }
    if state != "consumed" {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }

    let dataset = parse_identifier(dataset)?;
    let request_id = parse_identifier(request_id)?;
    let request_binding: OfficialOptionsReferenceRequestBinding =
        serde_json::from_str(&request_binding_json)?;
    request_binding.validate()?;
    if request_binding.request_id() != &request_id
        || request_binding.digest().bytes() != request_binding_digest.as_slice()
        || serde_json::to_string(&request_binding)? != request_binding_json
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    let record_count = parse_stage_count(record_count, MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS)?;
    let resolution_count = parse_stage_count(
        resolution_count,
        super::official_options_reference::MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS,
    )?;
    let conflict_count =
        parse_stage_count(conflict_count, MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS)?;
    let record_set_digest = parse_digest(
        1,
        record_set_digest
            .as_deref()
            .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?,
    )?;
    let resolution_set_digest = parse_digest(
        1,
        resolution_set_digest
            .as_deref()
            .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?,
    )?;
    let conflict_set_digest = parse_digest(
        1,
        conflict_set_digest
            .as_deref()
            .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?,
    )?;
    let generation_digest = parse_digest(
        1,
        consumed_generation_digest
            .as_deref()
            .ok_or(OfficialOptionsReferenceError::CorruptCatalog)?,
    )?;
    let generation: Option<(i64, i64, i64, i64, i64, Vec<u8>, Vec<u8>, Vec<u8>)> = connection
        .query_row(
            "SELECT requested_at_ns, request_deadline_ns, record_count,
                    alias_resolution_count, conflict_count, record_set_digest,
                    alias_resolution_set_digest, conflict_set_digest
             FROM official_options_reference_generations
             WHERE generation_digest=?1 AND dataset_id=?2 AND request_id=?3",
            params![
                generation_digest.bytes().as_slice(),
                dataset.as_str(),
                request_id.as_str()
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()?;
    let Some(generation) = generation else {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    };
    if generation.0 != request_binding.requested_at().unix_nanos()
        || generation.1 != request_binding.request_deadline().unix_nanos()
        || parse_stage_count(generation.2, MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS)? != record_count
        || parse_stage_count(
            generation.3,
            super::official_options_reference::MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS,
        )? != resolution_count
        || parse_stage_count(generation.4, MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS)?
            != conflict_count
        || parse_digest(1, &generation.5)? != record_set_digest
        || parse_digest(1, &generation.6)? != resolution_set_digest
        || parse_digest(1, &generation.7)? != conflict_set_digest
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    Ok(Some(
        OfficialOptionsReferenceStageRestartDisposition::AlreadyPublished {
            dataset,
            request_id,
            generation_digest,
        },
    ))
}

impl OfficialOptionsReferenceStageCapability {
    /// Creates or resumes one open stage with exact dataset and request coordinates.
    pub fn try_open(
        authority: Arc<Mutex<CatalogAuthority>>,
        dataset: SourceIdentifier,
        stage_id: SourceIdentifier,
        request_binding: OfficialOptionsReferenceRequestBinding,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, OfficialOptionsReferenceError> {
        check_operation(deadline, cancellation)?;
        let guard = authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        let catalog_id = guard.session_id();
        let connection = &guard.catalog().connection;
        request_binding.validate()?;
        let request_binding_json = serde_json::to_string(&request_binding)?;
        let request_id = request_binding.request_id();
        let transaction = connection.unchecked_transaction()?;
        let now = trusted_catalog_now(&transaction)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let existing: bool = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM official_options_reference_stages WHERE stage_id=?1
             )",
            [stage_id.as_str()],
            |row| row.get(0),
        )?;
        if !existing {
            let stages: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM official_options_reference_stages
                 WHERE state IN ('open','sealed')",
                [],
                |row| row.get(0),
            )?;
            if parse_stage_count(stages, MAX_OFFICIAL_OPTIONS_REFERENCE_STAGES)?
                >= MAX_OFFICIAL_OPTIONS_REFERENCE_STAGES
            {
                return Err(OfficialOptionsReferenceError::CapacityExceeded);
            }
        }
        transaction.execute(
            "INSERT OR IGNORE INTO official_options_reference_stages
             (stage_id, dataset_id, request_id, request_binding_json, request_binding_digest,
              state, record_count, resolution_count,
              conflict_count, encoded_bytes, created_at_ns, sealed_at_ns,
              record_set_digest, resolution_set_digest, conflict_set_digest,
              consumed_generation_digest)
             VALUES (?1, ?2, ?3, ?4, ?5, 'open', 0, 0, 0, 0, ?6, NULL, NULL, NULL, NULL, NULL)",
            params![
                stage_id.as_str(),
                dataset.as_str(),
                request_id.as_str(),
                request_binding_json,
                request_binding.digest().bytes().as_slice(),
                now.unix_nanos()
            ],
        )?;
        let retained: (String, String, String, String, Vec<u8>, String) = transaction.query_row(
            "SELECT stage_id, dataset_id, request_id, request_binding_json,
                    request_binding_digest, state
             FROM official_options_reference_stages WHERE dataset_id=?1 AND request_id=?2",
            params![dataset.as_str(), request_id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )?;
        if retained
            != (
                stage_id.as_str().to_owned(),
                dataset.as_str().to_owned(),
                request_id.as_str().to_owned(),
                serde_json::to_string(&request_binding)?,
                request_binding.digest().bytes().to_vec(),
                "open".into(),
            )
        {
            return Err(OfficialOptionsReferenceError::PositionConflict);
        }
        transaction.commit()?;
        drop(guard);
        Ok(Self {
            authority,
            catalog_id,
            dataset,
            stage_id,
            request_binding,
        })
    }

    /// Reads restart-stable cursors for resuming the three independently appended streams.
    pub fn progress(
        &self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferenceStageProgress, OfficialOptionsReferenceError> {
        check_operation(deadline, cancellation)?;
        let guard = self
            .authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        if guard.session_id() != self.catalog_id {
            return Err(OfficialOptionsReferenceError::SourceUnavailable);
        }
        let retained: (i64, i64, i64, i64) = guard.catalog().connection.query_row(
            "SELECT record_count, resolution_count, conflict_count, encoded_bytes
             FROM official_options_reference_stages
             WHERE stage_id=?1 AND dataset_id=?2 AND request_id=?3 AND state='open'",
            params![
                self.stage_id.as_str(),
                self.dataset.as_str(),
                self.request_binding.request_id().as_str()
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        Ok(OfficialOptionsReferenceStageProgress {
            records: parse_stage_count(retained.0, MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS)?,
            resolutions: parse_stage_count(
                retained.1,
                super::official_options_reference::MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS,
            )?,
            conflicts: parse_stage_count(retained.2, MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS)?,
            encoded_bytes: parse_stage_bytes(retained.3)?,
        })
    }

    /// Discards this exact recoverable stage when its acquisition is intentionally abandoned.
    pub fn discard(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), OfficialOptionsReferenceError> {
        check_operation(deadline, cancellation)?;
        transition_stage_to_abandoning(
            &self.authority,
            self.catalog_id,
            &self.dataset,
            &self.stage_id,
            self.request_binding.request_id(),
        )?;
        remove_stage_bounded(
            &self.authority,
            self.catalog_id,
            &self.stage_id,
            "abandoning",
            true,
            deadline,
            cancellation,
        )
    }

    /// Appends a bounded batch of mapped provider rows in arbitrary arrival order.
    pub fn push_records(
        &self,
        values: &[OfficialOptionsReferenceRecordInput],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), OfficialOptionsReferenceError> {
        self.append(
            StreamKind::Records,
            values,
            record_sort_key,
            deadline,
            cancellation,
        )
    }

    /// Appends a bounded batch of terminal alias resolutions in arbitrary arrival order.
    pub fn push_resolutions(
        &self,
        values: &[OfficialOptionsReferenceAliasResolutionInput],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), OfficialOptionsReferenceError> {
        self.append(
            StreamKind::Resolutions,
            values,
            |value| Ok(alias_key_json(value.key())?.into_bytes()),
            deadline,
            cancellation,
        )
    }

    /// Appends a bounded batch of terminal ambiguity conflicts in arbitrary arrival order.
    pub fn push_conflicts(
        &self,
        values: &[OfficialOptionsReferenceConflictInput],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), OfficialOptionsReferenceError> {
        self.append(
            StreamKind::Conflicts,
            values,
            conflict_sort_key,
            deadline,
            cancellation,
        )
    }

    fn append<T, F>(
        &self,
        kind: StreamKind,
        values: &[T],
        mut sort_key: F,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<(), OfficialOptionsReferenceError>
    where
        T: Serialize,
        F: FnMut(&T) -> Result<Vec<u8>, OfficialOptionsReferenceError>,
    {
        check_operation(deadline, cancellation)?;
        if values.is_empty() || values.len() > MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_BATCH_ROWS {
            return Err(OfficialOptionsReferenceError::InvalidInput);
        }
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(values.len())
            .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)?;
        let mut batch_bytes = 0_usize;
        for value in values {
            let json = serde_json::to_string(value)?;
            if json.is_empty() || json.len() > 64 * 1024 {
                return Err(OfficialOptionsReferenceError::InvalidInput);
            }
            let key = sort_key(value)?;
            if key.is_empty() || key.len() > 64 * 1024 {
                return Err(OfficialOptionsReferenceError::InvalidInput);
            }
            batch_bytes = batch_bytes
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(json.len()))
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            if batch_bytes > MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_BATCH_BYTES {
                return Err(OfficialOptionsReferenceError::CapacityExceeded);
            }
            encoded.push((key, json));
        }
        let guard = self
            .authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        if guard.session_id() != self.catalog_id {
            return Err(OfficialOptionsReferenceError::SourceUnavailable);
        }
        let transaction = guard.catalog().connection.unchecked_transaction()?;
        let (state, count, retained_bytes): (String, i64, i64) = transaction.query_row(
            kind.stage_counter_query(),
            [self.stage_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        if state != "open" {
            return Err(OfficialOptionsReferenceError::PositionConflict);
        }
        let count =
            u64::try_from(count).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let retained_bytes = parse_stage_bytes(retained_bytes)?;
        let mut inserted = 0_u64;
        let mut inserted_bytes = 0_u64;
        for (key, json) in encoded {
            let ordinal = count
                .checked_add(inserted)
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            let row_bytes = u64::try_from(key.len())
                .ok()
                .and_then(|bytes| u64::try_from(json.len()).ok()?.checked_add(bytes))
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            let changed = transaction.execute(
                kind.insert_sql(),
                params![
                    self.stage_id.as_str(),
                    to_i64(ordinal)?,
                    key.as_slice(),
                    json
                ],
            )?;
            if changed == 0 {
                let existing: Option<String> = transaction
                    .query_row(
                        kind.existing_value_sql(),
                        params![self.stage_id.as_str(), key.as_slice()],
                        |row| row.get(0),
                    )
                    .optional()?;
                if existing.as_deref() != Some(json.as_str()) {
                    return Err(OfficialOptionsReferenceError::PositionConflict);
                }
                continue;
            }
            if changed != 1 {
                return Err(OfficialOptionsReferenceError::CorruptCatalog);
            }
            inserted = inserted
                .checked_add(1)
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            inserted_bytes = inserted_bytes
                .checked_add(row_bytes)
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        }
        let next_count = count
            .checked_add(inserted)
            .filter(|count| *count <= kind.maximum_count())
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        let next_bytes = retained_bytes
            .checked_add(inserted_bytes)
            .filter(|bytes| *bytes <= MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_TOTAL_BYTES)
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        transaction.execute(
            kind.update_counter_sql(),
            params![
                self.stage_id.as_str(),
                to_i64(next_count)?,
                to_i64(next_bytes)?
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Seals all staged streams after recomputing their exact ordered evidence.
    pub fn seal(
        self,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<OfficialOptionsReferenceSealedStage, OfficialOptionsReferenceError> {
        check_operation(deadline, cancellation)?;
        let guard = self
            .authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        if guard.session_id() != self.catalog_id {
            return Err(OfficialOptionsReferenceError::SourceUnavailable);
        }
        let connection = &guard.catalog().connection;
        let verified = verify_stage_streams(
            connection,
            &self.stage_id,
            self.request_binding.request_id(),
            deadline,
            cancellation,
        )?;
        let encoded_bytes: i64 = connection.query_row(
            "SELECT encoded_bytes FROM official_options_reference_stages
             WHERE stage_id=?1 AND state='open'",
            [self.stage_id.as_str()],
            |row| row.get(0),
        )?;
        let encoded_bytes = parse_stage_bytes(encoded_bytes)?;
        if encoded_bytes != verified.encoded_bytes {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        let transaction = connection.unchecked_transaction()?;
        let now = trusted_catalog_now(&transaction)
            .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        let changed = transaction.execute(
            "UPDATE official_options_reference_stages
             SET state='sealed', sealed_at_ns=?2, record_set_digest=?3,
                 resolution_set_digest=?4, conflict_set_digest=?5
             WHERE stage_id=?1 AND state='open'",
            params![
                self.stage_id.as_str(),
                now.unix_nanos(),
                verified.records.digest().bytes().as_slice(),
                verified.resolutions.digest().bytes().as_slice(),
                verified.conflicts.digest().bytes().as_slice(),
            ],
        )?;
        if changed != 1 {
            return Err(OfficialOptionsReferenceError::PositionConflict);
        }
        append_audit(
            &transaction,
            "official-options-reference.stage-sealed",
            self.stage_id.as_str(),
            verified.records.digest().bytes(),
            now,
        )
        .map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)?;
        transaction.commit()?;
        Ok(OfficialOptionsReferenceSealedStage {
            catalog_id: self.catalog_id,
            dataset: self.dataset,
            stage_id: self.stage_id,
            request_binding: self.request_binding,
            alias_assertions: verified.alias_assertions,
            records: verified.records,
            resolutions: verified.resolutions,
            conflicts: verified.conflicts,
            encoded_bytes,
        })
    }
}

struct VerifiedStageStreams {
    alias_assertions: OfficialOptionsReferenceAliasAssertionSetEvidence,
    records: OfficialOptionsReferenceRecordSetEvidence,
    resolutions: OfficialOptionsReferenceResolutionSetEvidence,
    conflicts: OfficialOptionsReferenceConflictSetEvidence,
    encoded_bytes: u64,
}

fn verify_stage_streams(
    connection: &Connection,
    stage_id: &SourceIdentifier,
    request_id: &SourceIdentifier,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<VerifiedStageStreams, OfficialOptionsReferenceError> {
    let mut record_builder = OfficialOptionsReferenceRecordSetDigestBuilder::new();
    let mut assertion_builder =
        OfficialOptionsReferenceAliasAssertionSetBuilder::new(request_id.clone());
    let record_bytes = scan::<OfficialOptionsReferenceRecordInput, _, _>(
        connection,
        StreamKind::Records,
        stage_id,
        deadline,
        cancellation,
        record_sort_key,
        |record| {
            assertion_builder.try_observe(&record)?;
            record_builder.try_observe(&record)
        },
    )?;
    let mut resolution_builder = OfficialOptionsReferenceResolutionSetDigestBuilder::new();
    let mut observations = 0_u64;
    let mut conflict_references = 0_u64;
    let resolution_bytes = scan::<OfficialOptionsReferenceAliasResolutionInput, _, _>(
        connection,
        StreamKind::Resolutions,
        stage_id,
        deadline,
        cancellation,
        |resolution| Ok(alias_key_json(resolution.key())?.into_bytes()),
        |resolution| {
            observations = observations
                .checked_add(resolution.observations())
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            conflict_references = conflict_references
                .checked_add(u64::from(resolution.conflicts()))
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            resolution_builder.try_observe(&resolution)
        },
    )?;
    let mut conflict_builder = OfficialOptionsReferenceConflictSetDigestBuilder::new();
    let conflict_bytes = scan::<OfficialOptionsReferenceConflictInput, _, _>(
        connection,
        StreamKind::Conflicts,
        stage_id,
        deadline,
        cancellation,
        conflict_sort_key,
        |conflict| conflict_builder.try_observe(&conflict),
    )?;
    let alias_assertions = assertion_builder.finish();
    let records = record_builder.finish();
    let resolutions = resolution_builder.finish();
    let conflicts = conflict_builder.finish();
    if records.count() == 0
        || resolutions.count() == 0
        || observations != alias_assertions.assertions()
        || conflict_references != conflicts.count()
    {
        return Err(OfficialOptionsReferenceError::IncompleteStream);
    }
    let encoded_bytes = record_bytes
        .checked_add(resolution_bytes)
        .and_then(|bytes| bytes.checked_add(conflict_bytes))
        .filter(|bytes| *bytes <= MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_TOTAL_BYTES)
        .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
    Ok(VerifiedStageStreams {
        alias_assertions,
        records,
        resolutions,
        conflicts,
        encoded_bytes,
    })
}

fn verify_retained_evidence(
    connection: &Connection,
    stage_id: &SourceIdentifier,
    verified: &VerifiedStageStreams,
    encoded_bytes: u64,
) -> Result<(), OfficialOptionsReferenceError> {
    let retained: (i64, i64, i64, i64, Vec<u8>, Vec<u8>, Vec<u8>) = connection.query_row(
        "SELECT record_count, resolution_count, conflict_count, encoded_bytes,
                record_set_digest, resolution_set_digest, conflict_set_digest
         FROM official_options_reference_stages WHERE stage_id=?1 AND state='sealed'",
        [stage_id.as_str()],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        },
    )?;
    if u64::try_from(retained.0).ok() != Some(verified.records.count())
        || u64::try_from(retained.1).ok() != Some(verified.resolutions.count())
        || u64::try_from(retained.2).ok() != Some(verified.conflicts.count())
        || parse_stage_bytes(retained.3)? != encoded_bytes
        || encoded_bytes != verified.encoded_bytes
        || retained.4.as_slice() != verified.records.digest().bytes()
        || retained.5.as_slice() != verified.resolutions.digest().bytes()
        || retained.6.as_slice() != verified.conflicts.digest().bytes()
    {
        return Err(OfficialOptionsReferenceError::CorruptCatalog);
    }
    Ok(())
}

fn scan<T, K, F>(
    connection: &Connection,
    kind: StreamKind,
    stage_id: &SourceIdentifier,
    deadline: Instant,
    cancellation: &CancellationToken,
    mut expected_sort_key: K,
    mut sink: F,
) -> Result<u64, OfficialOptionsReferenceError>
where
    T: DeserializeOwned,
    K: FnMut(&T) -> Result<Vec<u8>, OfficialOptionsReferenceError>,
    F: FnMut(T) -> Result<(), OfficialOptionsReferenceError>,
{
    let mut statement = connection.prepare(kind.select_all_sql())?;
    let mut rows = statement.query([stage_id.as_str()])?;
    let mut encoded_bytes = 0_u64;
    while let Some(row) = rows.next()? {
        check_operation(deadline, cancellation)?;
        let sort_key: Vec<u8> = row.get(0)?;
        let json: String = row.get(1)?;
        let value = serde_json::from_str(&json)?;
        if expected_sort_key(&value)? != sort_key {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        encoded_bytes = encoded_bytes
            .checked_add(
                u64::try_from(sort_key.len())
                    .map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)?,
            )
            .and_then(|bytes| u64::try_from(json.len()).ok()?.checked_add(bytes))
            .filter(|bytes| *bytes <= MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_TOTAL_BYTES)
            .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
        sink(value)?;
    }
    Ok(encoded_bytes)
}

#[derive(Clone, Copy)]
enum StreamKind {
    Records,
    Resolutions,
    Conflicts,
}

impl StreamKind {
    const fn maximum_count(self) -> u64 {
        match self {
            Self::Records => MAX_OFFICIAL_OPTIONS_REFERENCE_RECORDS,
            Self::Resolutions => {
                super::official_options_reference::MAX_OFFICIAL_OPTIONS_REFERENCE_ALIAS_RESOLUTIONS
            }
            Self::Conflicts => MAX_OFFICIAL_OPTIONS_REFERENCE_CONFLICTS,
        }
    }

    const fn insert_sql(self) -> &'static str {
        match self {
            Self::Records => {
                "INSERT OR IGNORE INTO official_options_reference_stage_records
                (stage_id, stream_ordinal, sort_key, value_json) VALUES (?1, ?2, ?3, ?4)"
            }
            Self::Resolutions => {
                "INSERT OR IGNORE INTO official_options_reference_stage_resolutions
                (stage_id, stream_ordinal, sort_key, value_json) VALUES (?1, ?2, ?3, ?4)"
            }
            Self::Conflicts => {
                "INSERT OR IGNORE INTO official_options_reference_stage_conflicts
                (stage_id, stream_ordinal, sort_key, value_json) VALUES (?1, ?2, ?3, ?4)"
            }
        }
    }

    const fn existing_value_sql(self) -> &'static str {
        match self {
            Self::Records => {
                "SELECT value_json FROM official_options_reference_stage_records
                WHERE stage_id=?1 AND sort_key=?2"
            }
            Self::Resolutions => {
                "SELECT value_json FROM official_options_reference_stage_resolutions
                WHERE stage_id=?1 AND sort_key=?2"
            }
            Self::Conflicts => {
                "SELECT value_json FROM official_options_reference_stage_conflicts
                WHERE stage_id=?1 AND sort_key=?2"
            }
        }
    }

    const fn select_all_sql(self) -> &'static str {
        match self {
            Self::Records => {
                "SELECT sort_key, value_json FROM official_options_reference_stage_records
                WHERE stage_id=?1 ORDER BY sort_key"
            }
            Self::Resolutions => {
                "SELECT sort_key, value_json FROM official_options_reference_stage_resolutions
                WHERE stage_id=?1 ORDER BY sort_key"
            }
            Self::Conflicts => {
                "SELECT sort_key, value_json FROM official_options_reference_stage_conflicts
                WHERE stage_id=?1 ORDER BY sort_key"
            }
        }
    }

    const fn select_page_sql(self) -> &'static str {
        match self {
            Self::Records => {
                "SELECT sort_key, value_json
                FROM official_options_reference_stage_records
                WHERE stage_id=?1 AND (?2 IS NULL OR sort_key>?2)
                ORDER BY sort_key LIMIT 4096"
            }
            Self::Resolutions => {
                "SELECT sort_key, value_json
                FROM official_options_reference_stage_resolutions
                WHERE stage_id=?1 AND (?2 IS NULL OR sort_key>?2)
                ORDER BY sort_key LIMIT 4096"
            }
            Self::Conflicts => {
                "SELECT sort_key, value_json
                FROM official_options_reference_stage_conflicts
                WHERE stage_id=?1 AND (?2 IS NULL OR sort_key>?2)
                ORDER BY sort_key LIMIT 4096"
            }
        }
    }

    const fn stage_counter_query(self) -> &'static str {
        match self {
            Self::Records => {
                "SELECT state, record_count, encoded_bytes
                FROM official_options_reference_stages WHERE stage_id=?1"
            }
            Self::Resolutions => {
                "SELECT state, resolution_count, encoded_bytes
                FROM official_options_reference_stages WHERE stage_id=?1"
            }
            Self::Conflicts => {
                "SELECT state, conflict_count, encoded_bytes
                FROM official_options_reference_stages WHERE stage_id=?1"
            }
        }
    }

    const fn update_counter_sql(self) -> &'static str {
        match self {
            Self::Records => {
                "UPDATE official_options_reference_stages
                SET record_count=?2, encoded_bytes=?3 WHERE stage_id=?1 AND state='open'"
            }
            Self::Resolutions => {
                "UPDATE official_options_reference_stages
                SET resolution_count=?2, encoded_bytes=?3 WHERE stage_id=?1 AND state='open'"
            }
            Self::Conflicts => {
                "UPDATE official_options_reference_stages
                SET conflict_count=?2, encoded_bytes=?3 WHERE stage_id=?1 AND state='open'"
            }
        }
    }
}

pub(super) struct OfficialOptionsReferenceStageReplay<T> {
    stage_id: SourceIdentifier,
    kind: StreamKind,
    after_sort_key: Option<Vec<u8>>,
    buffered: VecDeque<T>,
    complete: bool,
}

impl<T> OfficialOptionsReferenceStageReplay<T>
where
    T: DeserializeOwned,
{
    fn new(stage_id: SourceIdentifier, kind: StreamKind) -> Self {
        Self {
            stage_id,
            kind,
            after_sort_key: None,
            buffered: VecDeque::new(),
            complete: false,
        }
    }

    pub(super) fn next(
        &mut self,
        connection: &Connection,
    ) -> Result<Option<T>, OfficialOptionsReferenceError> {
        if let Some(value) = self.buffered.pop_front() {
            return Ok(Some(value));
        }
        if self.complete {
            return Ok(None);
        }
        let mut statement = connection.prepare(self.kind.select_page_sql())?;
        let mut rows = statement.query(params![
            self.stage_id.as_str(),
            self.after_sort_key.as_deref()
        ])?;
        let mut bytes = 0_usize;
        let mut loaded = 0_usize;
        while let Some(row) = rows.next()? {
            let key: Vec<u8> = row.get(0)?;
            let json: String = row.get(1)?;
            let row_bytes = key
                .len()
                .checked_add(json.len())
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            if loaded > 0
                && bytes
                    .checked_add(row_bytes)
                    .is_none_or(|next| next > MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_BATCH_BYTES)
            {
                break;
            }
            bytes = bytes
                .checked_add(row_bytes)
                .ok_or(OfficialOptionsReferenceError::CapacityExceeded)?;
            self.buffered.push_back(serde_json::from_str(&json)?);
            self.after_sort_key = Some(key);
            loaded += 1;
        }
        if loaded == 0 {
            self.complete = true;
            return Ok(None);
        }
        Ok(self.buffered.pop_front())
    }
}

pub(super) fn record_replay(
    stage: &OfficialOptionsReferenceSealedStage,
) -> OfficialOptionsReferenceStageReplay<OfficialOptionsReferenceRecordInput> {
    OfficialOptionsReferenceStageReplay::new(stage.stage_id.clone(), StreamKind::Records)
}

pub(super) fn resolution_replay(
    stage: &OfficialOptionsReferenceSealedStage,
) -> OfficialOptionsReferenceStageReplay<OfficialOptionsReferenceAliasResolutionInput> {
    OfficialOptionsReferenceStageReplay::new(stage.stage_id.clone(), StreamKind::Resolutions)
}

pub(super) fn conflict_replay(
    stage: &OfficialOptionsReferenceSealedStage,
) -> OfficialOptionsReferenceStageReplay<OfficialOptionsReferenceConflictInput> {
    OfficialOptionsReferenceStageReplay::new(stage.stage_id.clone(), StreamKind::Conflicts)
}

fn transition_stage_to_abandoning(
    authority: &Arc<Mutex<CatalogAuthority>>,
    catalog_id: Uuid,
    dataset: &SourceIdentifier,
    stage_id: &SourceIdentifier,
    request_id: &SourceIdentifier,
) -> Result<(), OfficialOptionsReferenceError> {
    let guard = authority
        .try_lock()
        .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
    if guard.session_id() != catalog_id {
        return Err(OfficialOptionsReferenceError::SourceUnavailable);
    }
    let changed = guard.catalog().connection.execute(
        "UPDATE official_options_reference_stages SET state='abandoning'
         WHERE stage_id=?1 AND dataset_id=?2 AND request_id=?3 AND state='open'",
        params![stage_id.as_str(), dataset.as_str(), request_id.as_str()],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::PositionConflict)
    }
}

pub(super) fn prune_consumed_stage(
    authority: &Arc<Mutex<CatalogAuthority>>,
    stage: &OfficialOptionsReferenceSealedStage,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError> {
    remove_stage_bounded(
        authority,
        stage.catalog_id,
        &stage.stage_id,
        "consumed",
        false,
        deadline,
        cancellation,
    )
}

pub(super) fn mark_stage_consumed(
    transaction: &Transaction<'_>,
    stage: &OfficialOptionsReferenceSealedStage,
    generation_digest: market_squawk_domain::EvidenceDigest,
) -> Result<(), OfficialOptionsReferenceError> {
    let changed = transaction.execute(
        "UPDATE official_options_reference_stages
         SET state='consumed', consumed_generation_digest=?2
         WHERE stage_id=?1 AND dataset_id=?3 AND request_id=?4 AND state='sealed'
           AND request_binding_digest=?5 AND record_set_digest=?6
           AND resolution_set_digest=?7 AND conflict_set_digest=?8",
        params![
            stage.stage_id.as_str(),
            generation_digest.bytes().as_slice(),
            stage.dataset.as_str(),
            stage.request_id().as_str(),
            stage.request_binding.digest().bytes().as_slice(),
            stage.records.digest().bytes().as_slice(),
            stage.resolutions.digest().bytes().as_slice(),
            stage.conflicts.digest().bytes().as_slice(),
        ],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::PositionConflict)
    }
}

pub(super) fn acknowledge_published_stage(
    authority: &Arc<Mutex<CatalogAuthority>>,
    dataset: &SourceIdentifier,
    stage_id: &SourceIdentifier,
    generation_digest: EvidenceDigest,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError> {
    check_operation(deadline, cancellation)?;
    let catalog_id = authority
        .try_lock()
        .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?
        .session_id();
    let matches_tombstone = {
        let guard = authority
            .try_lock()
            .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
        if guard.session_id() != catalog_id {
            return Err(OfficialOptionsReferenceError::SourceUnavailable);
        }
        guard.catalog().connection.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM official_options_reference_stages
                 WHERE stage_id=?1 AND dataset_id=?2 AND state='consumed'
                   AND consumed_generation_digest=?3
             )",
            params![
                stage_id.as_str(),
                dataset.as_str(),
                generation_digest.bytes().as_slice()
            ],
            |row| row.get::<_, bool>(0),
        )?
    };
    if !matches_tombstone {
        return Err(OfficialOptionsReferenceError::PositionConflict);
    }
    remove_stage_bounded(
        authority,
        catalog_id,
        stage_id,
        "consumed",
        false,
        deadline,
        cancellation,
    )?;
    check_operation(deadline, cancellation)?;
    let guard = authority
        .try_lock()
        .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
    if guard.session_id() != catalog_id {
        return Err(OfficialOptionsReferenceError::SourceUnavailable);
    }
    let changed = guard.catalog().connection.execute(
        "DELETE FROM official_options_reference_stages
         WHERE stage_id=?1 AND dataset_id=?2 AND state='consumed'
           AND consumed_generation_digest=?3",
        params![
            stage_id.as_str(),
            dataset.as_str(),
            generation_digest.bytes().as_slice()
        ],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::PositionConflict)
    }
}

pub(super) fn cleanup_reclaimable_stages(
    authority: &Arc<Mutex<CatalogAuthority>>,
    dataset: &SourceIdentifier,
    maximum_stages: u64,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<u64, OfficialOptionsReferenceError> {
    if maximum_stages == 0 || maximum_stages > MAX_OFFICIAL_OPTIONS_REFERENCE_STAGES {
        return Err(OfficialOptionsReferenceError::InvalidLimit);
    }
    let catalog_id = authority
        .try_lock()
        .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?
        .session_id();
    let mut processed = 0_u64;
    while processed < maximum_stages {
        check_operation(deadline, cancellation)?;
        let stage = {
            let guard = authority
                .try_lock()
                .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
            if guard.session_id() != catalog_id {
                return Err(OfficialOptionsReferenceError::SourceUnavailable);
            }
            guard
                .catalog()
                .connection
                .query_row(
                    "SELECT stage_id, state FROM official_options_reference_stages
                     WHERE dataset_id=?1 AND (
                         state='abandoning'
                         OR (state='consumed' AND (
                             EXISTS (
                                 SELECT 1 FROM official_options_reference_stage_records AS record
                                 WHERE record.stage_id=official_options_reference_stages.stage_id
                             )
                             OR EXISTS (
                                 SELECT 1 FROM official_options_reference_stage_resolutions AS resolution
                                 WHERE resolution.stage_id=official_options_reference_stages.stage_id
                             )
                             OR EXISTS (
                                 SELECT 1 FROM official_options_reference_stage_conflicts AS conflict
                                 WHERE conflict.stage_id=official_options_reference_stages.stage_id
                             )
                         ))
                     )
                     ORDER BY created_at_ns, stage_id
                     LIMIT 1",
                    [dataset.as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?
        };
        let Some((stage_id, state)) = stage else {
            break;
        };
        if !matches!(state.as_str(), "abandoning" | "consumed") {
            return Err(OfficialOptionsReferenceError::CorruptCatalog);
        }
        remove_stage_bounded(
            authority,
            catalog_id,
            &parse_identifier(stage_id)?,
            if state == "abandoning" {
                "abandoning"
            } else {
                "consumed"
            },
            state == "abandoning",
            deadline,
            cancellation,
        )?;
        processed += 1;
    }
    Ok(processed)
}

fn remove_stage_bounded(
    authority: &Arc<Mutex<CatalogAuthority>>,
    catalog_id: Uuid,
    stage_id: &SourceIdentifier,
    required_state: &'static str,
    delete_parent: bool,
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError> {
    if !matches!(required_state, "abandoning" | "consumed") {
        return Err(OfficialOptionsReferenceError::InvalidInput);
    }
    for (table, ordinal) in [
        (
            "official_options_reference_stage_conflicts",
            "stream_ordinal",
        ),
        (
            "official_options_reference_stage_resolutions",
            "stream_ordinal",
        ),
        ("official_options_reference_stage_records", "stream_ordinal"),
    ] {
        loop {
            check_operation(deadline, cancellation)?;
            let guard = authority
                .try_lock()
                .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
            if guard.session_id() != catalog_id {
                return Err(OfficialOptionsReferenceError::SourceUnavailable);
            }
            let transaction = guard.catalog().connection.unchecked_transaction()?;
            let state: Option<String> = transaction
                .query_row(
                    "SELECT state FROM official_options_reference_stages WHERE stage_id=?1",
                    [stage_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;
            if state.as_deref() != Some(required_state) {
                return Err(OfficialOptionsReferenceError::PositionConflict);
            }
            let sql = format!(
                "DELETE FROM {table} WHERE stage_id=?1 AND {ordinal} IN
                 (SELECT {ordinal} FROM {table} WHERE stage_id=?1
                  ORDER BY {ordinal} LIMIT 4096)"
            );
            let changed = transaction.execute(&sql, [stage_id.as_str()])?;
            transaction.commit()?;
            if changed == 0 {
                break;
            }
        }
    }
    if !delete_parent {
        return Ok(());
    }
    check_operation(deadline, cancellation)?;
    let guard = authority
        .try_lock()
        .map_err(|_| OfficialOptionsReferenceError::AuthorityUnavailable)?;
    if guard.session_id() != catalog_id {
        return Err(OfficialOptionsReferenceError::SourceUnavailable);
    }
    let changed = guard.catalog().connection.execute(
        "DELETE FROM official_options_reference_stages WHERE stage_id=?1 AND state=?2",
        params![stage_id.as_str(), required_state],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(OfficialOptionsReferenceError::PositionConflict)
    }
}

fn parse_identifier(value: String) -> Result<SourceIdentifier, OfficialOptionsReferenceError> {
    SourceIdentifier::try_from(value).map_err(|_| OfficialOptionsReferenceError::CorruptCatalog)
}

fn parse_stage_bytes(value: i64) -> Result<u64, OfficialOptionsReferenceError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value <= MAX_OFFICIAL_OPTIONS_REFERENCE_STAGE_TOTAL_BYTES)
        .ok_or(OfficialOptionsReferenceError::CorruptCatalog)
}

fn parse_stage_count(value: i64, maximum: u64) -> Result<u64, OfficialOptionsReferenceError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value <= maximum)
        .ok_or(OfficialOptionsReferenceError::CorruptCatalog)
}

fn to_i64(value: u64) -> Result<i64, OfficialOptionsReferenceError> {
    i64::try_from(value).map_err(|_| OfficialOptionsReferenceError::CapacityExceeded)
}

fn check_operation(
    deadline: Instant,
    cancellation: &CancellationToken,
) -> Result<(), OfficialOptionsReferenceError> {
    if cancellation.is_cancelled() {
        Err(OfficialOptionsReferenceError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(OfficialOptionsReferenceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU64};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use market_squawk_domain::{
        DigestAlgorithm, EvidenceDigest, OccOptionIdentity, ProviderInstrumentId, SourceIdentifier,
        Timestamp, VenueId,
    };
    use market_squawk_platform::LocalPaths;
    use tokio_util::sync::CancellationToken;

    use super::{OfficialOptionsReferenceSealedStage, OfficialOptionsReferenceStageCapability};
    use crate::catalog::official_options_reference::{
        OfficialOptionsReferenceAliasKey, OfficialOptionsReferenceAliasResolutionInput,
        OfficialOptionsReferenceAliasResolutionState, OfficialOptionsReferenceCboeSeries,
        OfficialOptionsReferenceConflictInput, OfficialOptionsReferenceConflictKind,
        OfficialOptionsReferenceLifecycleEventEvidence,
        OfficialOptionsReferenceOccExchangeListingEvidence,
        OfficialOptionsReferenceOccPositionLimit, OfficialOptionsReferenceOccProduct,
        OfficialOptionsReferenceOccProductType, OfficialOptionsReferenceRecordInput,
        OfficialOptionsReferenceRecordValue, OfficialOptionsReferenceRequestBinding,
        OfficialOptionsReferenceSurface, OfficialOptionsReferenceValidityEvidence,
    };
    use crate::{CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn occ_and_cboe_stage_reopens_with_exact_evidence_after_restart() -> TestResult {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("market-squawk"))?;
        let config = || -> Result<CatalogConfig, Box<dyn std::error::Error>> {
            Ok(CatalogConfig::try_new(
                paths.catalog()?.clone(),
                Duration::from_millis(750),
                CatalogLimit::new(32)?,
                CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
            )?)
        };
        let authority = Arc::new(Mutex::new(CatalogAuthority::open(config()?)?));
        let dataset = SourceIdentifier::try_from("official-options-reference")?;
        let stage_id = SourceIdentifier::try_from("stage-occ-cboe-1")?;
        let request_id = SourceIdentifier::try_from("request-occ-cboe-1")?;
        let venue = VenueId::try_from("c1")?;
        let request_binding = OfficialOptionsReferenceRequestBinding::try_new(
            request_id,
            Timestamp::from_unix_nanos(1),
            Timestamp::from_unix_nanos(2),
            vec![
                OfficialOptionsReferenceSurface::CboeAllSeries {
                    venue: venue.clone(),
                },
                OfficialOptionsReferenceSurface::OccDlpSelectedText,
            ],
        )?;
        let expected_request_binding = request_binding.clone();
        let deadline = Instant::now() + Duration::from_secs(5);
        let cancellation = CancellationToken::new();
        let stage = OfficialOptionsReferenceStageCapability::try_open(
            Arc::clone(&authority),
            dataset.clone(),
            stage_id.clone(),
            request_binding.clone(),
            deadline,
            &cancellation,
        )?;
        let cboe_binding = EvidenceDigest::new(DigestAlgorithm::Sha256, [1; 32]);
        let occ_binding = EvidenceDigest::new(DigestAlgorithm::Sha256, [2; 32]);
        let osi = OccOptionIdentity::try_from("SPX   260320C05000000")?;
        let second_osi = OccOptionIdentity::try_from("SPX   260320P05000000")?;
        let cboe_symbol = "SPX001";
        let records = [
            OfficialOptionsReferenceRecordInput::try_new(
                0,
                cboe_binding,
                1,
                SourceIdentifier::try_from("cboe-row-1")?,
                OfficialOptionsReferenceRecordValue::CboeSeries(
                    OfficialOptionsReferenceCboeSeries::try_new(
                        venue.clone(),
                        cboe_symbol,
                        osi.clone(),
                        ProviderInstrumentId::try_from("SPX")?,
                        NonZeroU16::new(1).ok_or("missing matching unit")?,
                        false,
                    )?,
                ),
            )?,
            OfficialOptionsReferenceRecordInput::try_new(
                0,
                cboe_binding,
                2,
                SourceIdentifier::try_from("cboe-row-2")?,
                OfficialOptionsReferenceRecordValue::CboeSeries(
                    OfficialOptionsReferenceCboeSeries::try_new(
                        venue.clone(),
                        cboe_symbol,
                        second_osi.clone(),
                        ProviderInstrumentId::try_from("SPX")?,
                        NonZeroU16::new(1).ok_or("missing matching unit")?,
                        true,
                    )?,
                ),
            )?,
            OfficialOptionsReferenceRecordInput::try_new(
                1,
                occ_binding,
                1,
                SourceIdentifier::try_from("occ-row-1")?,
                OfficialOptionsReferenceRecordValue::OccProduct(
                    OfficialOptionsReferenceOccProduct::try_new(
                        ProviderInstrumentId::try_from("SPX")?,
                        ProviderInstrumentId::try_from("SPX")?,
                        "S&P 500 Index",
                        "C",
                        OfficialOptionsReferenceOccExchangeListingEvidence::Reported,
                        OfficialOptionsReferenceOccPositionLimit::EquityReported(
                            NonZeroU64::new(1).ok_or("missing position limit")?,
                        ),
                        OfficialOptionsReferenceOccProductType::EquityUnderlying,
                    )?,
                ),
            )?,
        ];
        let resolutions = [
            OfficialOptionsReferenceAliasResolutionInput::try_new(
                OfficialOptionsReferenceAliasKey::CboeSymbol {
                    symbol: cboe_symbol.to_owned(),
                },
                OfficialOptionsReferenceAliasResolutionState::Ambiguous,
                2,
                1,
            )?,
            OfficialOptionsReferenceAliasResolutionInput::try_new(
                OfficialOptionsReferenceAliasKey::CboeOsi { osi },
                OfficialOptionsReferenceAliasResolutionState::Exact,
                1,
                0,
            )?,
            OfficialOptionsReferenceAliasResolutionInput::try_new(
                OfficialOptionsReferenceAliasKey::CboeOsi { osi: second_osi },
                OfficialOptionsReferenceAliasResolutionState::Exact,
                1,
                0,
            )?,
            OfficialOptionsReferenceAliasResolutionInput::try_new(
                OfficialOptionsReferenceAliasKey::CboeVenueSymbol {
                    venue,
                    symbol: cboe_symbol.to_owned(),
                },
                OfficialOptionsReferenceAliasResolutionState::Exact,
                2,
                0,
            )?,
            OfficialOptionsReferenceAliasResolutionInput::try_new(
                OfficialOptionsReferenceAliasKey::OccProduct {
                    options_symbol: ProviderInstrumentId::try_from("SPX")?,
                    product_type: OfficialOptionsReferenceOccProductType::EquityUnderlying,
                },
                OfficialOptionsReferenceAliasResolutionState::Exact,
                1,
                0,
            )?,
        ];
        let conflicts = [OfficialOptionsReferenceConflictInput::try_new(
            OfficialOptionsReferenceAliasKey::CboeSymbol {
                symbol: cboe_symbol.to_owned(),
            },
            OfficialOptionsReferenceConflictKind::CboeSymbolMapsMultipleOsi,
            SourceIdentifier::try_from("cboe-row-1")?,
            SourceIdentifier::try_from("cboe-row-2")?,
        )?];
        for record in &records {
            let lifecycle = match record.value() {
                OfficialOptionsReferenceRecordValue::CboeSeries(value) => value.lifecycle(),
                OfficialOptionsReferenceRecordValue::OccProduct(value) => value.lifecycle(),
            };
            assert_eq!(
                lifecycle.validity(),
                OfficialOptionsReferenceValidityEvidence::PresentInExactSourceSnapshotOnly
            );
            assert_eq!(
                lifecycle.successor(),
                OfficialOptionsReferenceLifecycleEventEvidence::NotEstablishedBySelectedSource
            );
            assert_eq!(
                lifecycle.delisting(),
                OfficialOptionsReferenceLifecycleEventEvidence::NotEstablishedBySelectedSource
            );
        }
        stage.push_records(&records, deadline, &cancellation)?;
        stage.push_records(&records, deadline, &cancellation)?;
        let partial = stage.progress(deadline, &cancellation)?;
        assert_eq!(partial.records(), 3);
        assert_eq!(partial.resolutions(), 0);
        drop(stage);
        drop(authority);

        let resumed_authority = Arc::new(Mutex::new(CatalogAuthority::open(config()?)?));
        let resume_deadline = Instant::now() + Duration::from_secs(5);
        let stage = OfficialOptionsReferenceStageCapability::try_open(
            Arc::clone(&resumed_authority),
            dataset.clone(),
            stage_id.clone(),
            request_binding,
            resume_deadline,
            &cancellation,
        )?;
        assert_eq!(stage.progress(resume_deadline, &cancellation)?, partial);
        stage.push_resolutions(&resolutions, resume_deadline, &cancellation)?;
        stage.push_resolutions(&resolutions, resume_deadline, &cancellation)?;
        stage.push_conflicts(&conflicts, resume_deadline, &cancellation)?;
        stage.push_conflicts(&conflicts, resume_deadline, &cancellation)?;
        let progress = stage.progress(resume_deadline, &cancellation)?;
        assert_eq!(progress.records(), 3);
        assert_eq!(progress.resolutions(), 5);
        assert_eq!(progress.conflicts(), 1);
        assert!(progress.encoded_bytes() > 0);
        let sealed = stage.seal(resume_deadline, &cancellation)?;
        let expected = sealed.clone();
        drop(sealed);
        drop(resumed_authority);

        let reopened_authority = Arc::new(Mutex::new(CatalogAuthority::open(config()?)?));
        let reopen_deadline = Instant::now() + Duration::from_secs(5);
        let reopened = OfficialOptionsReferenceSealedStage::try_reopen(
            reopened_authority,
            stage_id,
            reopen_deadline,
            &cancellation,
        )?
        .ok_or("sealed stage was not recovered")?;
        assert_eq!(reopened.dataset(), &dataset);
        assert_eq!(reopened.request_binding(), &expected_request_binding);
        assert_eq!(reopened.alias_assertions(), expected.alias_assertions());
        assert_eq!(reopened.records(), expected.records());
        assert_eq!(reopened.resolutions(), expected.resolutions());
        assert_eq!(reopened.conflicts(), expected.conflicts());
        assert_eq!(reopened.encoded_bytes(), expected.encoded_bytes());
        Ok(())
    }
}
