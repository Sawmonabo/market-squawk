//! Provider-neutral durable staging and restart evidence for multi-response macro plans.

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_sources::ProviderCaptureTerminalDisposition;
use rusqlite::{Connection, OptionalExtension as _, Transaction, TransactionBehavior, params};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::provider_capture::{
    PersistedProviderCaptureBindingEvidence, PreparedProviderCaptureBinding,
    ProviderMacroPlanCompletionCapture, load_provider_capture_binding_evidence,
    retain_provider_macro_plan_completion_capture, retain_staged_provider_capture_binding,
};
use super::storage::{append_audit, parse_digest, trusted_catalog_now};
use super::{Catalog, CatalogError};
use crate::{DatasetId, DatasetManifestRef};

pub(crate) const MAX_PROVIDER_MACRO_PLAN_BINDINGS: usize = 4_096;
pub(crate) const MAX_PROVIDER_MACRO_PLAN_RESPONSES: usize = 1_024;
pub(crate) const MAX_PROVIDER_MACRO_PLAN_DATA_PAGES: usize = 1_023;
pub(crate) const MAX_PROVIDER_MACRO_PLAN_CHECKPOINT_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_PROVIDER_MACRO_PLAN_SEMANTICS_BYTES: usize = 64 * 1024 * 1024;

const REQUEST_SET_DOMAIN: &[u8] = b"market-squawk/provider-macro-plan/request-set/v1";
const PUBLICATION_DOMAIN: &[u8] = b"market-squawk/provider-macro-plan/publication/v1";
const CATALOG_RECEIPT_DOMAIN: &[u8] = b"market-squawk/provider-macro-plan/catalog-receipt/v1";
const PUBLICATION_AUDIT_EVENT: &str = "provider-macro-plan.published";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderMacroPlanSessionKey {
    session_id: Uuid,
    analytical_dataset: DatasetId,
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    provider_dataset: SourceIdentifier,
    source_generation_digest: EvidenceDigest,
}

impl ProviderMacroPlanSessionKey {
    pub(crate) fn try_new(
        session_id: Uuid,
        analytical_dataset: DatasetId,
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        provider_dataset: SourceIdentifier,
        source_generation_digest: EvidenceDigest,
    ) -> Result<Self, CatalogError> {
        require_digest(source_generation_digest)?;
        if session_id.is_nil() {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            session_id,
            analytical_dataset,
            source_id,
            metadata_revision,
            provider_dataset,
            source_generation_digest,
        })
    }

    pub(crate) const fn session_id(&self) -> Uuid {
        self.session_id
    }
    pub(crate) const fn analytical_dataset(&self) -> &DatasetId {
        &self.analytical_dataset
    }
    pub(crate) const fn source_id(&self) -> &SourceId {
        &self.source_id
    }
    pub(crate) const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }
    pub(crate) const fn provider_dataset(&self) -> &SourceIdentifier {
        &self.provider_dataset
    }
    pub(crate) const fn source_generation_digest(&self) -> EvidenceDigest {
        self.source_generation_digest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProviderMacroPlanStageCoordinate {
    session_id: Uuid,
    state_version: u16,
    checkpoint_digest: EvidenceDigest,
}

impl ProviderMacroPlanStageCoordinate {
    pub(crate) const fn session_id(self) -> Uuid {
        self.session_id
    }
    pub(crate) const fn state_version(self) -> u16 {
        self.state_version
    }
    pub(crate) const fn checkpoint_digest(self) -> EvidenceDigest {
        self.checkpoint_digest
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ProviderMacroPlanSemanticsEvidence {
    schema: SourceIdentifier,
    schema_requirement_digest: EvidenceDigest,
    semantic_digest: EvidenceDigest,
    payload: Box<[u8]>,
    payload_digest: EvidenceDigest,
}

impl ProviderMacroPlanSemanticsEvidence {
    pub(crate) fn try_new(
        schema: SourceIdentifier,
        schema_requirement_digest: EvidenceDigest,
        semantic_digest: EvidenceDigest,
        payload: Box<[u8]>,
    ) -> Result<Self, CatalogError> {
        require_digest(schema_requirement_digest)?;
        require_digest(semantic_digest)?;
        if payload.is_empty() || payload.len() > MAX_PROVIDER_MACRO_PLAN_CHECKPOINT_BYTES {
            return Err(CatalogError::InvalidRecord);
        }
        let payload_digest = sha256_evidence(&payload);
        Ok(Self {
            schema,
            schema_requirement_digest,
            semantic_digest,
            payload,
            payload_digest,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ProviderMacroPlanStagedPageInput {
    page_ordinal: u16,
    candidate_digest: EvidenceDigest,
    semantics: ProviderMacroPlanSemanticsEvidence,
    binding: PreparedProviderCaptureBinding,
}

impl ProviderMacroPlanStagedPageInput {
    pub(crate) fn try_new(
        page_ordinal: u16,
        candidate_digest: EvidenceDigest,
        semantics: ProviderMacroPlanSemanticsEvidence,
        binding: PreparedProviderCaptureBinding,
    ) -> Result<Self, CatalogError> {
        require_digest(candidate_digest)?;
        if usize::from(page_ordinal) >= MAX_PROVIDER_MACRO_PLAN_DATA_PAGES
            || binding.capture().terminal()
                != ProviderCaptureTerminalDisposition::StandaloneResponse
            || binding.capture().pages().len() != 1
        {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        Ok(Self {
            page_ordinal,
            candidate_digest,
            semantics,
            binding,
        })
    }
}

#[derive(Debug)]
pub(crate) struct ProviderMacroPlanTerminalInput {
    response_ordinal: u16,
    adapter_completion_digest: EvidenceDigest,
    completion: ProviderMacroPlanCompletionCapture,
}

impl ProviderMacroPlanTerminalInput {
    pub(crate) fn try_new(
        response_ordinal: u16,
        adapter_completion_digest: EvidenceDigest,
        completion: ProviderMacroPlanCompletionCapture,
    ) -> Result<Self, CatalogError> {
        require_digest(adapter_completion_digest)?;
        if response_ordinal == 0
            || usize::from(response_ordinal) > MAX_PROVIDER_MACRO_PLAN_DATA_PAGES
        {
            return Err(CatalogError::InvalidRecord);
        }
        Ok(Self {
            response_ordinal,
            adapter_completion_digest,
            completion,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CompletedProviderMacroPlanSession {
    key: ProviderMacroPlanSessionKey,
    coordinate: ProviderMacroPlanStageCoordinate,
    checkpoint: Box<[u8]>,
    request_set_identity: EvidenceDigest,
    adapter_completion_digest: EvidenceDigest,
    publication_digest: EvidenceDigest,
    terminal_capture_observation_digest: EvidenceDigest,
    terminal_seal_digest: EvidenceDigest,
    terminal_raw_claim_digest: EvidenceDigest,
    terminal_physical_receipt_digest: EvidenceDigest,
    response_count: u16,
    data_page_count: u16,
    analytical_row_count: u64,
}

impl CompletedProviderMacroPlanSession {
    pub(crate) const fn key(&self) -> &ProviderMacroPlanSessionKey {
        &self.key
    }
    pub(crate) const fn coordinate(&self) -> ProviderMacroPlanStageCoordinate {
        self.coordinate
    }
    pub(crate) fn checkpoint(&self) -> &[u8] {
        &self.checkpoint
    }
    pub(crate) const fn request_set_identity(&self) -> EvidenceDigest {
        self.request_set_identity
    }
    pub(crate) const fn adapter_completion_digest(&self) -> EvidenceDigest {
        self.adapter_completion_digest
    }
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }
    pub(crate) const fn terminal_seal_digest(&self) -> EvidenceDigest {
        self.terminal_seal_digest
    }
    pub(crate) const fn terminal_capture_observation_digest(&self) -> EvidenceDigest {
        self.terminal_capture_observation_digest
    }
    pub(crate) const fn terminal_raw_claim_digest(&self) -> EvidenceDigest {
        self.terminal_raw_claim_digest
    }
    pub(crate) const fn terminal_physical_receipt_digest(&self) -> EvidenceDigest {
        self.terminal_physical_receipt_digest
    }
    pub(crate) const fn response_count(&self) -> u16 {
        self.response_count
    }
    pub(crate) const fn data_page_count(&self) -> u16 {
        self.data_page_count
    }
    pub(crate) const fn analytical_row_count(&self) -> u64 {
        self.analytical_row_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderMacroPlanPublishedHead {
    publication_digest: EvidenceDigest,
    manifest_dataset: DatasetId,
    manifest_version: u64,
    checkpoint_version: u16,
    checkpoint_digest: EvidenceDigest,
}

impl ProviderMacroPlanPublishedHead {
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }
    pub(crate) const fn manifest_dataset(&self) -> &DatasetId {
        &self.manifest_dataset
    }
    pub(crate) const fn manifest_version(&self) -> u64 {
        self.manifest_version
    }
    pub(crate) const fn checkpoint_version(&self) -> u16 {
        self.checkpoint_version
    }
    pub(crate) const fn checkpoint_digest(&self) -> EvidenceDigest {
        self.checkpoint_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderMacroPlanPublicationCommit {
    session: ProviderMacroPlanStageCoordinate,
    publication_digest: EvidenceDigest,
    expected_head: Option<ProviderMacroPlanPublishedHead>,
}

impl ProviderMacroPlanPublicationCommit {
    pub(crate) fn try_new(
        session: ProviderMacroPlanStageCoordinate,
        publication_digest: EvidenceDigest,
        expected_head: Option<ProviderMacroPlanPublishedHead>,
    ) -> Result<Self, CatalogError> {
        require_digest(publication_digest)?;
        Ok(Self {
            session,
            publication_digest,
            expected_head,
        })
    }

    pub(crate) const fn session_id(&self) -> Uuid {
        self.session.session_id
    }
    pub(crate) const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderMacroPlanRestartProjection {
    manifest: DatasetManifestRef,
    generation_sequence: u64,
    anchor_manifest_id: Uuid,
    run_id: Uuid,
    completed: CompletedProviderMacroPlanSession,
    catalog_receipt_digest: EvidenceDigest,
    predecessor: Option<ProviderMacroPlanPublishedHead>,
    successor: ProviderMacroPlanPublishedHead,
}

impl ProviderMacroPlanRestartProjection {
    pub(crate) const fn manifest(&self) -> &DatasetManifestRef {
        &self.manifest
    }
    pub(crate) const fn generation_sequence(&self) -> u64 {
        self.generation_sequence
    }
    pub(crate) const fn anchor_manifest_id(&self) -> Uuid {
        self.anchor_manifest_id
    }
    pub(crate) const fn run_id(&self) -> Uuid {
        self.run_id
    }
    pub(crate) const fn completed(&self) -> &CompletedProviderMacroPlanSession {
        &self.completed
    }
    pub(crate) const fn catalog_receipt_digest(&self) -> EvidenceDigest {
        self.catalog_receipt_digest
    }
    pub(crate) const fn predecessor(&self) -> Option<&ProviderMacroPlanPublishedHead> {
        self.predecessor.as_ref()
    }
    pub(crate) const fn successor(&self) -> &ProviderMacroPlanPublishedHead {
        &self.successor
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderMacroPlanSessionRecovery {
    key: ProviderMacroPlanSessionKey,
    coordinate: ProviderMacroPlanStageCoordinate,
    checkpoint: Box<[u8]>,
    complete: bool,
    response_count: u16,
    data_page_count: u16,
    analytical_row_count: u64,
}

impl ProviderMacroPlanSessionRecovery {
    pub(crate) const fn key(&self) -> &ProviderMacroPlanSessionKey {
        &self.key
    }
    pub(crate) const fn coordinate(&self) -> ProviderMacroPlanStageCoordinate {
        self.coordinate
    }
    pub(crate) fn checkpoint(&self) -> &[u8] {
        &self.checkpoint
    }
    pub(crate) const fn is_complete(&self) -> bool {
        self.complete
    }
    pub(crate) const fn response_count(&self) -> u16 {
        self.response_count
    }
    pub(crate) const fn data_page_count(&self) -> u16 {
        self.data_page_count
    }
    pub(crate) const fn analytical_row_count(&self) -> u64 {
        self.analytical_row_count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProviderMacroPlanReplayPageEvidence {
    page_ordinal: u16,
    candidate_digest: EvidenceDigest,
    semantics_schema: SourceIdentifier,
    semantics_schema_requirement_digest: EvidenceDigest,
    semantics_digest: EvidenceDigest,
    semantics_payload: Box<[u8]>,
    semantics_payload_digest: EvidenceDigest,
    binding: PersistedProviderCaptureBindingEvidence,
}

impl ProviderMacroPlanReplayPageEvidence {
    pub(crate) const fn page_ordinal(&self) -> u16 {
        self.page_ordinal
    }
    pub(crate) const fn candidate_digest(&self) -> EvidenceDigest {
        self.candidate_digest
    }
    pub(crate) const fn semantics_schema(&self) -> &SourceIdentifier {
        &self.semantics_schema
    }
    pub(crate) const fn semantics_schema_requirement_digest(&self) -> EvidenceDigest {
        self.semantics_schema_requirement_digest
    }
    pub(crate) const fn semantics_digest(&self) -> EvidenceDigest {
        self.semantics_digest
    }
    pub(crate) fn semantics_payload(&self) -> &[u8] {
        &self.semantics_payload
    }
    pub(crate) const fn semantics_payload_digest(&self) -> EvidenceDigest {
        self.semantics_payload_digest
    }
    pub(crate) const fn binding(&self) -> &PersistedProviderCaptureBindingEvidence {
        &self.binding
    }
}

impl Catalog {
    pub(crate) fn begin_provider_macro_plan_session(
        &self,
        key: ProviderMacroPlanSessionKey,
        checkpoint: Box<[u8]>,
    ) -> Result<ProviderMacroPlanStageCoordinate, CatalogError> {
        validate_checkpoint(&checkpoint)?;
        let checkpoint_digest = sha256_evidence(&checkpoint);
        let transaction =
            Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let now = trusted_catalog_now(&transaction)?;
        let source_matches: bool = transaction.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sources AS source
                 JOIN source_revisions AS revision
                   ON revision.source_id=source.source_id
                  AND revision.revision_digest=source.current_revision_digest
                 WHERE source.source_id=?1
                   AND json_extract(revision.metadata_json,
                       '$.revision_evidence.metadata_revision')=?2)",
            params![
                key.source_id.as_str(),
                key.metadata_revision.as_source_identifier().as_str()
            ],
            |row| row.get(0),
        )?;
        if !source_matches {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        transaction.execute(
            "INSERT INTO provider_macro_plan_sessions
             (session_id, analytical_dataset, source_id, metadata_revision, provider_dataset,
              source_generation_digest, state, state_version, checkpoint_digest,
              checkpoint_bytes, response_count, data_page_count, analytical_row_count,
              semantics_bytes, created_at_ns, updated_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'acquiring', 0, ?7, ?8, 0, 0, 0, 0, ?9, ?9)",
            params![
                key.session_id.to_string(),
                key.analytical_dataset.as_str(),
                key.source_id.as_str(),
                key.metadata_revision.as_source_identifier().as_str(),
                key.provider_dataset.as_str(),
                digest_bytes(key.source_generation_digest),
                digest_bytes(checkpoint_digest),
                checkpoint.as_ref(),
                now.unix_nanos(),
            ],
        )?;
        transaction.commit()?;
        Ok(ProviderMacroPlanStageCoordinate {
            session_id: key.session_id,
            state_version: 0,
            checkpoint_digest,
        })
    }

    pub(crate) fn stage_provider_macro_plan_page(
        &self,
        expected: ProviderMacroPlanStageCoordinate,
        successor_checkpoint: Box<[u8]>,
        input: ProviderMacroPlanStagedPageInput,
    ) -> Result<ProviderMacroPlanStageCoordinate, CatalogError> {
        validate_checkpoint(&successor_checkpoint)?;
        if input.page_ordinal != expected.state_version {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        let successor_digest = sha256_evidence(&successor_checkpoint);
        let transaction =
            Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let session = load_session(&transaction, expected.session_id)?;
        require_exact_acquiring(&session, expected)?;
        if input.binding.source_id() != session.key.source_id()
            || input.binding.capture().metadata_revision() != session.key.metadata_revision()
            || input.binding.capture().dataset() != session.key.provider_dataset()
            || usize::from(session.data_page_count) >= MAX_PROVIDER_MACRO_PLAN_BINDINGS
            || usize::from(session.response_count) >= MAX_PROVIDER_MACRO_PLAN_DATA_PAGES
            || usize::try_from(session.semantics_bytes)
                .ok()
                .and_then(|value| value.checked_add(input.semantics.payload.len()))
                .is_none_or(|value| value > MAX_PROVIDER_MACRO_PLAN_SEMANTICS_BYTES)
        {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let now = trusted_catalog_now(&transaction)?;
        retain_staged_provider_capture_binding(&transaction, &input.binding, now)?;
        transaction.execute(
            "INSERT INTO provider_macro_plan_staged_pages
             (session_id, page_ordinal, candidate_digest, binding_digest,
              capture_observation_digest, canonical_record_count, semantics_schema,
              semantics_schema_requirement_digest, semantics_digest, semantics_payload,
              semantics_payload_digest, staged_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                expected.session_id.to_string(),
                i64::from(input.page_ordinal),
                digest_bytes(input.candidate_digest),
                digest_bytes(input.binding.binding_digest()),
                digest_bytes(input.binding.capture_observation_digest()),
                to_i64(input.binding.record_count())?,
                input.semantics.schema.as_str(),
                digest_bytes(input.semantics.schema_requirement_digest),
                digest_bytes(input.semantics.semantic_digest),
                input.semantics.payload.as_ref(),
                digest_bytes(input.semantics.payload_digest),
                now.unix_nanos(),
            ],
        )?;
        let next_version = expected
            .state_version
            .checked_add(1)
            .ok_or(CatalogError::ProviderCaptureConflict)?;
        let updated = transaction.execute(
            "UPDATE provider_macro_plan_sessions
             SET state_version=?1, checkpoint_digest=?2, checkpoint_bytes=?3,
                 response_count=response_count+1, data_page_count=data_page_count+1,
                 analytical_row_count=analytical_row_count+?4,
                 semantics_bytes=semantics_bytes+?5, updated_at_ns=?6
             WHERE session_id=?7 AND state='acquiring' AND state_version=?8
               AND checkpoint_digest=?9",
            params![
                i64::from(next_version),
                digest_bytes(successor_digest),
                successor_checkpoint.as_ref(),
                to_i64(input.binding.record_count())?,
                to_i64(input.semantics.payload.len())?,
                now.unix_nanos(),
                expected.session_id.to_string(),
                i64::from(expected.state_version),
                digest_bytes(expected.checkpoint_digest),
            ],
        )?;
        if updated != 1 {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        transaction.commit()?;
        Ok(ProviderMacroPlanStageCoordinate {
            session_id: expected.session_id,
            state_version: next_version,
            checkpoint_digest: successor_digest,
        })
    }

    pub(crate) fn complete_provider_macro_plan_session(
        &self,
        expected: ProviderMacroPlanStageCoordinate,
        successor_checkpoint: Box<[u8]>,
        input: ProviderMacroPlanTerminalInput,
    ) -> Result<CompletedProviderMacroPlanSession, CatalogError> {
        validate_checkpoint(&successor_checkpoint)?;
        let successor_digest = sha256_evidence(&successor_checkpoint);
        let transaction =
            Transaction::new_unchecked(&self.connection, TransactionBehavior::Immediate)?;
        let session = load_session(&transaction, expected.session_id)?;
        require_exact_acquiring(&session, expected)?;
        let capture = input.completion.capture();
        if session.data_page_count == 0
            || input.response_ordinal != session.response_count
            || capture.source_id() != session.key.source_id()
            || capture.metadata_revision() != session.key.metadata_revision()
            || capture.dataset() != session.key.provider_dataset()
        {
            return Err(CatalogError::ProviderCaptureMismatch);
        }
        let now = trusted_catalog_now(&transaction)?;
        retain_provider_macro_plan_completion_capture(&transaction, &input.completion, now)?;
        transaction.execute(
            "INSERT INTO provider_macro_plan_terminal_completions
             (session_id, response_ordinal, adapter_completion_digest,
              capture_observation_digest, sealed_capture_receipt_digest, raw_claim_digest,
              physical_receipt_digest, completed_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                expected.session_id.to_string(),
                i64::from(input.response_ordinal),
                digest_bytes(input.adapter_completion_digest),
                digest_bytes(capture.observation_digest()),
                digest_bytes(input.completion.sealed_capture_receipt_digest()),
                digest_bytes(input.completion.physical_claim().raw_claim_digest()),
                digest_bytes(
                    input
                        .completion
                        .physical_claim()
                        .claim()
                        .physical_receipt_digest()
                ),
                now.unix_nanos(),
            ],
        )?;
        let next_version = expected
            .state_version
            .checked_add(1)
            .ok_or(CatalogError::ProviderCaptureConflict)?;
        let updated = transaction.execute(
            "UPDATE provider_macro_plan_sessions
             SET state='complete', state_version=?1, checkpoint_digest=?2,
                 checkpoint_bytes=?3, response_count=response_count+1, updated_at_ns=?4
             WHERE session_id=?5 AND state='acquiring' AND state_version=?6
               AND checkpoint_digest=?7",
            params![
                i64::from(next_version),
                digest_bytes(successor_digest),
                successor_checkpoint.as_ref(),
                now.unix_nanos(),
                expected.session_id.to_string(),
                i64::from(expected.state_version),
                digest_bytes(expected.checkpoint_digest),
            ],
        )?;
        if updated != 1 {
            return Err(CatalogError::ProviderCaptureConflict);
        }
        let completed = load_completed_session(&transaction, expected.session_id)?;
        transaction.commit()?;
        Ok(completed)
    }

    pub(crate) fn provider_macro_plan_completed_session(
        &self,
        session_id: Uuid,
    ) -> Result<CompletedProviderMacroPlanSession, CatalogError> {
        load_completed_session(&self.connection, session_id)
    }

    pub(crate) fn provider_macro_plan_session_recovery(
        &self,
        session_id: Uuid,
    ) -> Result<ProviderMacroPlanSessionRecovery, CatalogError> {
        let session = load_session(&self.connection, session_id)?;
        Ok(ProviderMacroPlanSessionRecovery {
            key: session.key,
            coordinate: session.coordinate,
            checkpoint: session.checkpoint,
            complete: session.state == "complete",
            response_count: session.response_count,
            data_page_count: session.data_page_count,
            analytical_row_count: session.analytical_row_count,
        })
    }

    pub(crate) fn provider_macro_plan_replay_page(
        &self,
        session_id: Uuid,
        page_ordinal: u16,
    ) -> Result<Option<ProviderMacroPlanReplayPageEvidence>, CatalogError> {
        type Row = (Vec<u8>, Vec<u8>, String, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);
        let row: Option<Row> = self
            .connection
            .query_row(
                "SELECT candidate_digest, binding_digest, semantics_schema,
                    semantics_schema_requirement_digest, semantics_digest,
                    semantics_payload, semantics_payload_digest
             FROM provider_macro_plan_staged_pages
             WHERE session_id=?1 AND page_ordinal=?2",
                params![session_id.to_string(), i64::from(page_ordinal)],
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
            )
            .optional()?;
        row.map(|row| {
            let payload_digest = parse_sha256(&row.6)?;
            if row.5.is_empty()
                || row.5.len() > MAX_PROVIDER_MACRO_PLAN_CHECKPOINT_BYTES
                || sha256_evidence(&row.5) != payload_digest
            {
                return Err(CatalogError::CorruptCatalog);
            }
            let binding_digest = parse_sha256(&row.1)?;
            let binding = load_provider_capture_binding_evidence(&self.connection, binding_digest)?
                .ok_or(CatalogError::CorruptCatalog)?;
            Ok(ProviderMacroPlanReplayPageEvidence {
                page_ordinal,
                candidate_digest: parse_sha256(&row.0)?,
                semantics_schema: SourceIdentifier::try_from(row.2.as_str())
                    .map_err(|_| CatalogError::CorruptCatalog)?,
                semantics_schema_requirement_digest: parse_sha256(&row.3)?,
                semantics_digest: parse_sha256(&row.4)?,
                semantics_payload: row.5.into_boxed_slice(),
                semantics_payload_digest: payload_digest,
                binding,
            })
        })
        .transpose()
    }
}

#[derive(Debug)]
struct StoredSession {
    key: ProviderMacroPlanSessionKey,
    state: String,
    coordinate: ProviderMacroPlanStageCoordinate,
    checkpoint: Box<[u8]>,
    response_count: u16,
    data_page_count: u16,
    analytical_row_count: u64,
    semantics_bytes: u64,
}

fn load_session(connection: &Connection, session_id: Uuid) -> Result<StoredSession, CatalogError> {
    type Row = (
        String,
        String,
        String,
        String,
        Vec<u8>,
        String,
        i64,
        Vec<u8>,
        Vec<u8>,
        i64,
        i64,
        i64,
        i64,
    );
    let row: Row = connection
        .query_row(
            "SELECT analytical_dataset, source_id, metadata_revision, provider_dataset,
                    source_generation_digest, state, state_version, checkpoint_digest,
                    checkpoint_bytes, response_count, data_page_count, analytical_row_count,
                    semantics_bytes
             FROM provider_macro_plan_sessions WHERE session_id=?1",
            [session_id.to_string()],
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
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                ))
            },
        )
        .optional()?
        .ok_or(CatalogError::ProviderCaptureConflict)?;
    if row.8.is_empty() || row.8.len() > MAX_PROVIDER_MACRO_PLAN_CHECKPOINT_BYTES {
        return Err(CatalogError::CorruptCatalog);
    }
    let checkpoint_digest = parse_sha256(&row.7)?;
    if sha256_evidence(&row.8) != checkpoint_digest {
        return Err(CatalogError::CorruptCatalog);
    }
    let state_version = parse_u16(row.6)?;
    Ok(StoredSession {
        key: ProviderMacroPlanSessionKey::try_new(
            session_id,
            DatasetId::try_from(row.0.as_str()).map_err(|_| CatalogError::CorruptCatalog)?,
            SourceId::try_from(row.1.as_str()).map_err(|_| CatalogError::CorruptCatalog)?,
            MetadataRevision::new(
                SourceIdentifier::try_from(row.2.as_str())
                    .map_err(|_| CatalogError::CorruptCatalog)?,
            ),
            SourceIdentifier::try_from(row.3.as_str()).map_err(|_| CatalogError::CorruptCatalog)?,
            parse_sha256(&row.4)?,
        )?,
        state: row.5,
        coordinate: ProviderMacroPlanStageCoordinate {
            session_id,
            state_version,
            checkpoint_digest,
        },
        checkpoint: row.8.into_boxed_slice(),
        response_count: parse_u16(row.9)?,
        data_page_count: parse_u16(row.10)?,
        analytical_row_count: u64::try_from(row.11).map_err(|_| CatalogError::CorruptCatalog)?,
        semantics_bytes: u64::try_from(row.12).map_err(|_| CatalogError::CorruptCatalog)?,
    })
}

fn require_exact_acquiring(
    session: &StoredSession,
    expected: ProviderMacroPlanStageCoordinate,
) -> Result<(), CatalogError> {
    if session.state != "acquiring" || session.coordinate != expected {
        Err(CatalogError::ProviderCaptureConflict)
    } else {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct StoredPageIdentity {
    ordinal: u16,
    candidate_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
    capture_request_set_identity: EvidenceDigest,
    capture_content_digest: EvidenceDigest,
    capture_observation_digest: EvidenceDigest,
    sealed_capture_receipt_digest: EvidenceDigest,
    extraction_content_digest: EvidenceDigest,
    native_batch_digest: EvidenceDigest,
    canonical_record_count: u64,
    semantics_schema: String,
    semantics_schema_requirement_digest: EvidenceDigest,
    semantics_digest: EvidenceDigest,
    semantics_payload_digest: EvidenceDigest,
}

fn load_page_identities(
    connection: &Connection,
    session_id: Uuid,
) -> Result<Vec<StoredPageIdentity>, CatalogError> {
    let mut statement = connection.prepare(
        "SELECT page.page_ordinal, page.candidate_digest, page.binding_digest,
                capture.request_set_identity, capture.capture_content_digest,
                capture.capture_observation_digest, binding.sealed_capture_receipt_digest,
                binding.extraction_content_digest, native.batch_digest,
                page.canonical_record_count, page.semantics_schema,
                page.semantics_schema_requirement_digest, page.semantics_digest,
                page.semantics_payload, page.semantics_payload_digest
         FROM provider_macro_plan_staged_pages AS page
         JOIN provider_capture_bindings AS binding ON binding.binding_digest=page.binding_digest
         JOIN provider_raw_observations AS capture
           ON capture.capture_observation_digest=page.capture_observation_digest
         JOIN provider_capture_binding_native_lineage AS native
           ON native.binding_digest=page.binding_digest
         WHERE page.session_id=?1 ORDER BY page.page_ordinal LIMIT ?2",
    )?;
    let mut rows = statement.query(params![
        session_id.to_string(),
        i64::try_from(MAX_PROVIDER_MACRO_PLAN_DATA_PAGES + 1)
            .map_err(|_| CatalogError::InvalidRecord)?,
    ])?;
    let mut pages = Vec::new();
    pages
        .try_reserve_exact(MAX_PROVIDER_MACRO_PLAN_DATA_PAGES)
        .map_err(|_| CatalogError::Allocation)?;
    let mut semantics_bytes = 0usize;
    while let Some(row) = rows.next()? {
        if pages.len() == MAX_PROVIDER_MACRO_PLAN_DATA_PAGES {
            return Err(CatalogError::CorruptCatalog);
        }
        let ordinal = parse_u16(row.get(0)?)?;
        let payload: Vec<u8> = row.get(13)?;
        let payload_digest = parse_sha256(&row.get::<_, Vec<u8>>(14)?)?;
        semantics_bytes = semantics_bytes
            .checked_add(payload.len())
            .ok_or(CatalogError::CorruptCatalog)?;
        if ordinal != u16::try_from(pages.len()).map_err(|_| CatalogError::CorruptCatalog)?
            || payload.is_empty()
            || semantics_bytes > MAX_PROVIDER_MACRO_PLAN_SEMANTICS_BYTES
            || sha256_evidence(&payload) != payload_digest
        {
            return Err(CatalogError::CorruptCatalog);
        }
        pages.push(StoredPageIdentity {
            ordinal,
            candidate_digest: parse_sha256(&row.get::<_, Vec<u8>>(1)?)?,
            binding_digest: parse_sha256(&row.get::<_, Vec<u8>>(2)?)?,
            capture_request_set_identity: parse_sha256(&row.get::<_, Vec<u8>>(3)?)?,
            capture_content_digest: parse_sha256(&row.get::<_, Vec<u8>>(4)?)?,
            capture_observation_digest: parse_sha256(&row.get::<_, Vec<u8>>(5)?)?,
            sealed_capture_receipt_digest: parse_sha256(&row.get::<_, Vec<u8>>(6)?)?,
            extraction_content_digest: parse_sha256(&row.get::<_, Vec<u8>>(7)?)?,
            native_batch_digest: parse_sha256(&row.get::<_, Vec<u8>>(8)?)?,
            canonical_record_count: u64::try_from(row.get::<_, i64>(9)?)
                .map_err(|_| CatalogError::CorruptCatalog)?,
            semantics_schema: row.get(10)?,
            semantics_schema_requirement_digest: parse_sha256(&row.get::<_, Vec<u8>>(11)?)?,
            semantics_digest: parse_sha256(&row.get::<_, Vec<u8>>(12)?)?,
            semantics_payload_digest: payload_digest,
        });
    }
    Ok(pages)
}

fn load_completed_session(
    connection: &Connection,
    session_id: Uuid,
) -> Result<CompletedProviderMacroPlanSession, CatalogError> {
    let session = load_session(connection, session_id)?;
    if session.state != "complete"
        || session.response_count < 2
        || usize::from(session.response_count) > MAX_PROVIDER_MACRO_PLAN_RESPONSES
        || session.data_page_count + 1 != session.response_count
        || session.coordinate.state_version != session.response_count
    {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    type Terminal = (i64, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>);
    let terminal: Terminal = connection.query_row(
        "SELECT response_ordinal, adapter_completion_digest, capture_observation_digest,
                sealed_capture_receipt_digest, raw_claim_digest, physical_receipt_digest
         FROM provider_macro_plan_terminal_completions WHERE session_id=?1",
        [session_id.to_string()],
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
    if parse_u16(terminal.0)? != session.data_page_count {
        return Err(CatalogError::CorruptCatalog);
    }
    let pages = load_page_identities(connection, session_id)?;
    let total_rows = pages.iter().try_fold(0_u64, |total, page| {
        total
            .checked_add(page.canonical_record_count)
            .ok_or(CatalogError::CorruptCatalog)
    })?;
    if pages.len() != usize::from(session.data_page_count)
        || total_rows != session.analytical_row_count
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let adapter_completion_digest = parse_sha256(&terminal.1)?;
    let terminal_capture_observation_digest = parse_sha256(&terminal.2)?;
    let terminal_seal_digest = parse_sha256(&terminal.3)?;
    let terminal_raw_claim_digest = parse_sha256(&terminal.4)?;
    let terminal_physical_receipt_digest = parse_sha256(&terminal.5)?;
    let terminal_request_set_identity: Vec<u8> = connection.query_row(
        "SELECT request_set_identity FROM provider_raw_observations
         WHERE capture_observation_digest=?1",
        [digest_bytes(terminal_capture_observation_digest)],
        |row| row.get(0),
    )?;
    let request_set_identity = request_set_identity(
        &pages,
        parse_sha256(&terminal_request_set_identity)?,
        terminal_capture_observation_digest,
        terminal_seal_digest,
    )?;
    let publication_digest = publication_digest(
        &session,
        &pages,
        request_set_identity,
        adapter_completion_digest,
        terminal_capture_observation_digest,
        terminal_seal_digest,
    )?;
    Ok(CompletedProviderMacroPlanSession {
        key: session.key,
        coordinate: session.coordinate,
        checkpoint: session.checkpoint,
        request_set_identity,
        adapter_completion_digest,
        publication_digest,
        terminal_capture_observation_digest,
        terminal_seal_digest,
        terminal_raw_claim_digest,
        terminal_physical_receipt_digest,
        response_count: session.response_count,
        data_page_count: session.data_page_count,
        analytical_row_count: session.analytical_row_count,
    })
}

fn request_set_identity(
    pages: &[StoredPageIdentity],
    terminal_request_set_identity: EvidenceDigest,
    terminal_observation_digest: EvidenceDigest,
    terminal_seal_digest: EvidenceDigest,
) -> Result<EvidenceDigest, CatalogError> {
    let mut hash = Sha256::new();
    hash.update(REQUEST_SET_DOMAIN);
    hash.update(to_u16(pages.len())?.to_be_bytes());
    for page in pages {
        hash.update(page.ordinal.to_be_bytes());
        hash.update(page.capture_request_set_identity.bytes());
        hash.update(page.capture_content_digest.bytes());
        hash.update(page.capture_observation_digest.bytes());
        hash.update(page.sealed_capture_receipt_digest.bytes());
    }
    hash.update(to_u16(pages.len())?.to_be_bytes());
    hash.update(terminal_request_set_identity.bytes());
    hash.update(terminal_observation_digest.bytes());
    hash.update(terminal_seal_digest.bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn publication_digest(
    session: &StoredSession,
    pages: &[StoredPageIdentity],
    request_set_identity: EvidenceDigest,
    adapter_completion_digest: EvidenceDigest,
    terminal_observation_digest: EvidenceDigest,
    terminal_seal_digest: EvidenceDigest,
) -> Result<EvidenceDigest, CatalogError> {
    let mut hash = Sha256::new();
    hash.update(PUBLICATION_DOMAIN);
    hash_text(&mut hash, session.key.analytical_dataset.as_str())?;
    hash_text(&mut hash, session.key.source_id.as_str())?;
    hash_text(
        &mut hash,
        session
            .key
            .metadata_revision
            .as_source_identifier()
            .as_str(),
    )?;
    hash_text(&mut hash, session.key.provider_dataset.as_str())?;
    hash.update(session.key.source_generation_digest.bytes());
    hash.update(request_set_identity.bytes());
    hash.update(adapter_completion_digest.bytes());
    hash.update(session.response_count.to_be_bytes());
    hash.update(session.data_page_count.to_be_bytes());
    hash.update(session.analytical_row_count.to_be_bytes());
    for page in pages {
        hash.update(page.ordinal.to_be_bytes());
        hash.update(page.candidate_digest.bytes());
        hash.update(page.binding_digest.bytes());
        hash.update(page.canonical_record_count.to_be_bytes());
        hash.update(page.extraction_content_digest.bytes());
        hash.update(page.native_batch_digest.bytes());
        hash_text(&mut hash, &page.semantics_schema)?;
        hash.update(page.semantics_schema_requirement_digest.bytes());
        hash.update(page.semantics_digest.bytes());
        hash.update(page.semantics_payload_digest.bytes());
    }
    hash.update(terminal_observation_digest.bytes());
    hash.update(terminal_seal_digest.bytes());
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

pub(crate) fn retain_completed_provider_macro_plan_for_run(
    transaction: &Transaction<'_>,
    run_id: Uuid,
    commit: &ProviderMacroPlanPublicationCommit,
    recorded_at: Timestamp,
) -> Result<(), CatalogError> {
    let completed = load_completed_session(transaction, commit.session.session_id)?;
    if completed.coordinate != commit.session
        || completed.publication_digest != commit.publication_digest
    {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    let run_source: String = transaction.query_row(
        "SELECT source_id FROM ingest_runs
         WHERE run_id=?1 AND state='reserved' AND operation='persist' AND payload_digest=?2",
        params![run_id.to_string(), digest_bytes(commit.publication_digest)],
        |row| row.get(0),
    )?;
    if run_source != completed.key.source_id.as_str() {
        return Err(CatalogError::ProviderCaptureMismatch);
    }
    let pages = load_page_identities(transaction, commit.session.session_id)?;
    if transaction.query_row(
        "SELECT COUNT(*) FROM ingest_run_provider_capture_bindings WHERE run_id=?1",
        [run_id.to_string()],
        |row| row.get::<_, i64>(0),
    )? != 0
    {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    for page in &pages {
        if load_provider_capture_binding_evidence(transaction, page.binding_digest)?.is_none() {
            return Err(CatalogError::CorruptCatalog);
        }
        transaction.execute(
            "INSERT INTO ingest_run_provider_capture_bindings
             (run_id, input_ordinal, binding_digest, source_id) VALUES (?1, ?2, ?3, ?4)",
            params![
                run_id.to_string(),
                i64::from(page.ordinal),
                digest_bytes(page.binding_digest),
                completed.key.source_id.as_str(),
            ],
        )?;
    }
    append_audit(
        transaction,
        "provider-macro-plan.staged-inputs-retained",
        &run_id.to_string(),
        commit.publication_digest.bytes(),
        recorded_at,
    )
}

pub(crate) fn load_provider_macro_plan_head(
    connection: &Connection,
    analytical_dataset: &DatasetId,
    source_id: &SourceId,
    provider_dataset: &SourceIdentifier,
) -> Result<Option<ProviderMacroPlanPublishedHead>, CatalogError> {
    type Row = (Vec<u8>, i64, i64, Vec<u8>);
    connection
        .query_row(
            "SELECT publication_digest, manifest_version, completed_checkpoint_version,
                    completed_checkpoint_digest
             FROM provider_macro_plan_published_heads
             WHERE analytical_dataset=?1 AND source_id=?2 AND provider_dataset=?3",
            params![
                analytical_dataset.as_str(),
                source_id.as_str(),
                provider_dataset.as_str()
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?
        .map(|row: Row| {
            Ok(ProviderMacroPlanPublishedHead {
                publication_digest: parse_sha256(&row.0)?,
                manifest_dataset: analytical_dataset.clone(),
                manifest_version: u64::try_from(row.1).map_err(|_| CatalogError::CorruptCatalog)?,
                checkpoint_version: parse_u16(row.2)?,
                checkpoint_digest: parse_sha256(&row.3)?,
            })
        })
        .transpose()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn publish_provider_macro_plan_record(
    transaction: &Transaction<'_>,
    commit: &ProviderMacroPlanPublicationCommit,
    manifest: &DatasetManifestRef,
    anchor_manifest_id: Uuid,
    run_id: Uuid,
    generation_sequence: i64,
    published_at: Timestamp,
) -> Result<EvidenceDigest, CatalogError> {
    let completed = load_completed_session(transaction, commit.session.session_id)?;
    if completed.coordinate != commit.session
        || completed.publication_digest != commit.publication_digest
        || completed.key.analytical_dataset() != manifest.dataset_id()
    {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    let current_head = load_provider_macro_plan_head(
        transaction,
        completed.key.analytical_dataset(),
        completed.key.source_id(),
        completed.key.provider_dataset(),
    )?;
    if current_head != commit.expected_head {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    let receipt = catalog_receipt_digest(
        manifest,
        anchor_manifest_id,
        run_id,
        &completed,
        current_head.as_ref(),
    )?;
    transaction.execute(
        "INSERT INTO provider_macro_plan_publications
         (publication_digest, generation_sequence, session_id, manifest_dataset_id,
          manifest_version, manifest_schema_name, manifest_schema_version,
          manifest_schema_fingerprint, manifest_content_hash, anchor_manifest_id, run_id,
          source_id, metadata_revision, provider_dataset, source_generation_digest,
          request_set_identity, adapter_completion_digest, terminal_seal_digest,
          catalog_receipt_digest, response_count, data_page_count, analytical_row_count,
          completed_checkpoint_version, completed_checkpoint_digest,
          predecessor_publication_digest, predecessor_manifest_dataset_id,
          predecessor_manifest_version, predecessor_checkpoint_version,
          predecessor_checkpoint_digest, published_at_ns)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
                 ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27,
                 ?28, ?29, ?30)",
        params![
            digest_bytes(completed.publication_digest),
            generation_sequence,
            completed.key.session_id.to_string(),
            manifest.dataset_id().as_str(),
            to_i64(manifest.manifest_version())?,
            manifest.schema().name(),
            i64::from(manifest.schema().version().get()),
            manifest.schema().fingerprint().as_slice(),
            manifest.content_hash().bytes(),
            anchor_manifest_id.to_string(),
            run_id.to_string(),
            completed.key.source_id.as_str(),
            completed
                .key
                .metadata_revision
                .as_source_identifier()
                .as_str(),
            completed.key.provider_dataset.as_str(),
            digest_bytes(completed.key.source_generation_digest),
            digest_bytes(completed.request_set_identity),
            digest_bytes(completed.adapter_completion_digest),
            digest_bytes(completed.terminal_seal_digest),
            digest_bytes(receipt),
            i64::from(completed.response_count),
            i64::from(completed.data_page_count),
            to_i64(completed.analytical_row_count)?,
            i64::from(completed.coordinate.state_version),
            digest_bytes(completed.coordinate.checkpoint_digest),
            current_head
                .as_ref()
                .map(|head| digest_bytes(head.publication_digest)),
            current_head
                .as_ref()
                .map(|head| head.manifest_dataset.as_str()),
            current_head
                .as_ref()
                .map(|head| head.manifest_version)
                .map(to_i64)
                .transpose()?,
            current_head
                .as_ref()
                .map(|head| i64::from(head.checkpoint_version)),
            current_head
                .as_ref()
                .map(|head| digest_bytes(head.checkpoint_digest)),
            published_at.unix_nanos(),
        ],
    )?;
    let advanced = if let Some(head) = current_head {
        transaction.execute(
            "UPDATE provider_macro_plan_published_heads
             SET publication_digest=?1, session_id=?2, generation_sequence=?3,
                 manifest_version=?4, completed_checkpoint_version=?5,
                 completed_checkpoint_digest=?6, advanced_at_ns=?7
             WHERE analytical_dataset=?8 AND source_id=?9 AND provider_dataset=?10
               AND publication_digest=?11 AND manifest_version=?12
               AND completed_checkpoint_version=?13 AND completed_checkpoint_digest=?14",
            params![
                digest_bytes(completed.publication_digest),
                completed.key.session_id.to_string(),
                generation_sequence,
                to_i64(manifest.manifest_version())?,
                i64::from(completed.coordinate.state_version),
                digest_bytes(completed.coordinate.checkpoint_digest),
                published_at.unix_nanos(),
                completed.key.analytical_dataset.as_str(),
                completed.key.source_id.as_str(),
                completed.key.provider_dataset.as_str(),
                digest_bytes(head.publication_digest),
                to_i64(head.manifest_version)?,
                i64::from(head.checkpoint_version),
                digest_bytes(head.checkpoint_digest),
            ],
        )?
    } else {
        transaction.execute(
            "INSERT INTO provider_macro_plan_published_heads
             (analytical_dataset, source_id, provider_dataset, publication_digest, session_id,
              generation_sequence, manifest_version, completed_checkpoint_version,
              completed_checkpoint_digest, advanced_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                completed.key.analytical_dataset.as_str(),
                completed.key.source_id.as_str(),
                completed.key.provider_dataset.as_str(),
                digest_bytes(completed.publication_digest),
                completed.key.session_id.to_string(),
                generation_sequence,
                to_i64(manifest.manifest_version())?,
                i64::from(completed.coordinate.state_version),
                digest_bytes(completed.coordinate.checkpoint_digest),
                published_at.unix_nanos(),
            ],
        )?
    };
    if advanced != 1 {
        return Err(CatalogError::ProviderCaptureConflict);
    }
    append_audit(
        transaction,
        PUBLICATION_AUDIT_EVENT,
        &anchor_manifest_id.to_string(),
        receipt.bytes(),
        published_at,
    )?;
    Ok(receipt)
}

pub(crate) fn reconstruct_provider_macro_plan_projection(
    connection: &Connection,
    manifest: &DatasetManifestRef,
) -> Result<ProviderMacroPlanRestartProjection, CatalogError> {
    type Row = (
        i64,
        Vec<u8>,
        String,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        i64,
        i64,
        i64,
        Vec<u8>,
        Option<Vec<u8>>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<Vec<u8>>,
        String,
    );
    let row: Row = connection
        .query_row(
            "SELECT publication.generation_sequence, publication.publication_digest,
                    publication.session_id, publication.anchor_manifest_id,
                    publication.request_set_identity, publication.adapter_completion_digest,
                    publication.terminal_seal_digest, publication.catalog_receipt_digest,
                    publication.response_count, publication.data_page_count,
                    publication.analytical_row_count, publication.completed_checkpoint_version,
                    publication.completed_checkpoint_digest,
                    publication.predecessor_publication_digest,
                    publication.predecessor_manifest_dataset_id,
                    publication.predecessor_manifest_version,
                    publication.predecessor_checkpoint_version,
                    publication.predecessor_checkpoint_digest, publication.run_id
             FROM analytical_generations AS generation
             JOIN provider_macro_plan_publications AS publication
               ON publication.generation_sequence=generation.generation_sequence
             WHERE generation.dataset_id=?1 AND generation.manifest_version=?2
               AND generation.schema_name=?3 AND generation.schema_version=?4
               AND generation.schema_fingerprint=?5 AND generation.content_hash=?6
               AND publication.manifest_dataset_id=generation.dataset_id
               AND publication.manifest_version=generation.manifest_version",
            params![
                manifest.dataset_id().as_str(),
                to_i64(manifest.manifest_version())?,
                manifest.schema().name(),
                i64::from(manifest.schema().version().get()),
                manifest.schema().fingerprint().as_slice(),
                manifest.content_hash().bytes(),
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
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                    row.get(16)?,
                    row.get(17)?,
                    row.get(18)?,
                ))
            },
        )
        .optional()?
        .ok_or(CatalogError::ProviderCaptureConflict)?;
    let session_id = Uuid::parse_str(&row.2).map_err(|_| CatalogError::CorruptCatalog)?;
    let completed = load_completed_session(connection, session_id)?;
    let publication_digest = parse_sha256(&row.1)?;
    let catalog_receipt = parse_sha256(&row.7)?;
    if publication_digest != completed.publication_digest
        || parse_sha256(&row.4)? != completed.request_set_identity
        || parse_sha256(&row.5)? != completed.adapter_completion_digest
        || parse_sha256(&row.6)? != completed.terminal_seal_digest
        || parse_u16(row.8)? != completed.response_count
        || parse_u16(row.9)? != completed.data_page_count
        || u64::try_from(row.10).ok() != Some(completed.analytical_row_count)
        || parse_u16(row.11)? != completed.coordinate.state_version
        || parse_sha256(&row.12)? != completed.coordinate.checkpoint_digest
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let predecessor = match (&row.13, &row.14, row.15, row.16, &row.17) {
        (
            Some(publication),
            Some(dataset),
            Some(version),
            Some(checkpoint_version),
            Some(checkpoint),
        ) => Some(ProviderMacroPlanPublishedHead {
            publication_digest: parse_sha256(publication)?,
            manifest_dataset: DatasetId::try_from(dataset.as_str())
                .map_err(|_| CatalogError::CorruptCatalog)?,
            manifest_version: u64::try_from(version).map_err(|_| CatalogError::CorruptCatalog)?,
            checkpoint_version: parse_u16(checkpoint_version)?,
            checkpoint_digest: parse_sha256(checkpoint)?,
        }),
        (None, None, None, None, None) => None,
        _ => return Err(CatalogError::CorruptCatalog),
    };
    let anchor = Uuid::parse_str(&row.3).map_err(|_| CatalogError::CorruptCatalog)?;
    let run_id = Uuid::parse_str(&row.18).map_err(|_| CatalogError::CorruptCatalog)?;
    let generation_sequence = u64::try_from(row.0).map_err(|_| CatalogError::CorruptCatalog)?;
    if catalog_receipt_digest(manifest, anchor, run_id, &completed, predecessor.as_ref())?
        != catalog_receipt
    {
        return Err(CatalogError::CorruptCatalog);
    }
    let pages = load_page_identities(connection, session_id)?;
    let mut statement = connection.prepare(
        "SELECT input_ordinal, binding_digest
         FROM analytical_generation_provider_capture_bindings
         WHERE generation_sequence=?1 AND run_id=?2 ORDER BY input_ordinal LIMIT ?3",
    )?;
    let mut inputs = statement.query(params![
        row.0,
        run_id.to_string(),
        i64::try_from(MAX_PROVIDER_MACRO_PLAN_DATA_PAGES + 1)
            .map_err(|_| CatalogError::InvalidRecord)?,
    ])?;
    let mut ordinal = 0usize;
    while let Some(input) = inputs.next()? {
        if ordinal >= pages.len()
            || input.get::<_, i64>(0)?
                != i64::try_from(ordinal).map_err(|_| CatalogError::CorruptCatalog)?
            || parse_sha256(&input.get::<_, Vec<u8>>(1)?)? != pages[ordinal].binding_digest
        {
            return Err(CatalogError::CorruptCatalog);
        }
        ordinal += 1;
    }
    if ordinal != pages.len() {
        return Err(CatalogError::CorruptCatalog);
    }
    let successor = ProviderMacroPlanPublishedHead {
        publication_digest,
        manifest_dataset: manifest.dataset_id().clone(),
        manifest_version: manifest.manifest_version(),
        checkpoint_version: completed.coordinate.state_version,
        checkpoint_digest: completed.coordinate.checkpoint_digest,
    };
    Ok(ProviderMacroPlanRestartProjection {
        manifest: manifest.clone(),
        generation_sequence,
        anchor_manifest_id: anchor,
        run_id,
        completed,
        catalog_receipt_digest: catalog_receipt,
        predecessor,
        successor,
    })
}

fn catalog_receipt_digest(
    manifest: &DatasetManifestRef,
    anchor_manifest_id: Uuid,
    run_id: Uuid,
    completed: &CompletedProviderMacroPlanSession,
    predecessor: Option<&ProviderMacroPlanPublishedHead>,
) -> Result<EvidenceDigest, CatalogError> {
    let mut hash = Sha256::new();
    hash.update(CATALOG_RECEIPT_DOMAIN);
    hash_text(&mut hash, manifest.dataset_id().as_str())?;
    hash.update(manifest.manifest_version().to_be_bytes());
    hash_text(&mut hash, manifest.schema().name())?;
    hash.update(manifest.schema().version().get().to_be_bytes());
    hash.update(manifest.schema().fingerprint());
    hash.update(manifest.content_hash().bytes());
    hash.update(anchor_manifest_id.as_bytes());
    hash.update(run_id.as_bytes());
    hash.update(completed.key.session_id.as_bytes());
    hash.update(completed.publication_digest.bytes());
    hash.update(completed.request_set_identity.bytes());
    hash.update(completed.adapter_completion_digest.bytes());
    hash.update(completed.terminal_capture_observation_digest.bytes());
    hash.update(completed.terminal_seal_digest.bytes());
    hash.update(completed.terminal_raw_claim_digest.bytes());
    hash.update(completed.terminal_physical_receipt_digest.bytes());
    hash.update(completed.response_count.to_be_bytes());
    hash.update(completed.data_page_count.to_be_bytes());
    hash.update(completed.analytical_row_count.to_be_bytes());
    hash.update(completed.coordinate.state_version.to_be_bytes());
    hash.update(completed.coordinate.checkpoint_digest.bytes());
    if let Some(predecessor) = predecessor {
        hash.update([1]);
        hash.update(predecessor.publication_digest.bytes());
        hash_text(&mut hash, predecessor.manifest_dataset.as_str())?;
        hash.update(predecessor.manifest_version.to_be_bytes());
        hash.update(predecessor.checkpoint_version.to_be_bytes());
        hash.update(predecessor.checkpoint_digest.bytes());
    } else {
        hash.update([0]);
    }
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hash.finalize().into(),
    ))
}

fn validate_checkpoint(checkpoint: &[u8]) -> Result<(), CatalogError> {
    if checkpoint.is_empty() || checkpoint.len() > MAX_PROVIDER_MACRO_PLAN_CHECKPOINT_BYTES {
        Err(CatalogError::InvalidRecord)
    } else {
        Ok(())
    }
}

fn require_digest(digest: EvidenceDigest) -> Result<(), CatalogError> {
    if digest.algorithm() != DigestAlgorithm::Sha256 || digest.bytes() == [0; 32] {
        Err(CatalogError::InvalidRecord)
    } else {
        Ok(())
    }
}

fn parse_sha256(value: &[u8]) -> Result<EvidenceDigest, CatalogError> {
    let digest = parse_digest(1, value)?;
    require_digest(digest)?;
    Ok(digest)
}

fn sha256_evidence(value: &[u8]) -> EvidenceDigest {
    EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(value).into())
}

fn digest_bytes(digest: EvidenceDigest) -> [u8; 32] {
    digest.bytes()
}

fn hash_text(hash: &mut Sha256, value: &str) -> Result<(), CatalogError> {
    hash.update(to_i64(value.len())?.to_be_bytes());
    hash.update(value.as_bytes());
    Ok(())
}

fn parse_u16(value: i64) -> Result<u16, CatalogError> {
    u16::try_from(value).map_err(|_| CatalogError::CorruptCatalog)
}

fn to_u16(value: usize) -> Result<u16, CatalogError> {
    u16::try_from(value).map_err(|_| CatalogError::InvalidRecord)
}

fn to_i64<T>(value: T) -> Result<i64, CatalogError>
where
    i64: TryFrom<T>,
{
    i64::try_from(value).map_err(|_| CatalogError::InvalidRecord)
}

#[cfg(test)]
mod tests {
    use rusqlite::{Connection, params};

    use crate::migrations::MIGRATIONS;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn macro_plan_checkpoint_requires_the_exact_predecessor() -> TestResult {
        let connection = Connection::open_in_memory()?;
        for migration in MIGRATIONS {
            connection.execute_batch(migration.sql)?;
        }
        let source_revision = [3_u8; 32];
        let source = connection.unchecked_transaction()?;
        source.execute(
            "INSERT INTO sources
             (source_id, current_revision_digest, current_registered_at_ns,
              first_registered_at_ns) VALUES (?1, ?2, 1, 1)",
            params!["macro-source", source_revision],
        )?;
        source.execute(
            "INSERT INTO source_revisions
             (source_id, revision_digest, metadata_json, registered_at_ns)
             VALUES (?1, ?2, '{}', 1)",
            params!["macro-source", source_revision],
        )?;
        source.commit()?;
        let original = [1_u8; 32];
        let successor = [2_u8; 32];
        connection.execute(
            "INSERT INTO provider_macro_plan_sessions
             (session_id, analytical_dataset, source_id, metadata_revision, provider_dataset,
              source_generation_digest, state, state_version, checkpoint_digest,
              checkpoint_bytes, response_count, data_page_count, analytical_row_count,
              semantics_bytes, created_at_ns, updated_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'acquiring', 0, ?7, ?8, 0, 0, 0, 0, 1, 1)",
            params![
                "00000000-0000-4000-8000-000000000001",
                "macro-observations",
                "macro-source",
                "revision-1",
                "provider-dataset",
                [4_u8; 32],
                original,
                b"first",
            ],
        )?;

        let without_page = connection.execute(
            "UPDATE provider_macro_plan_sessions
             SET state_version=1, checkpoint_digest=?1, checkpoint_bytes=?2,
                 response_count=1, data_page_count=1, analytical_row_count=1,
                 semantics_bytes=1, updated_at_ns=2
             WHERE session_id=?3 AND state='acquiring' AND state_version=0
               AND checkpoint_digest=?4",
            params![
                successor,
                b"second",
                "00000000-0000-4000-8000-000000000001",
                original
            ],
        );
        assert!(without_page.is_err());

        let stale = connection.execute(
            "UPDATE provider_macro_plan_sessions
             SET checkpoint_digest=?1, checkpoint_bytes=?2
             WHERE session_id=?3 AND state='acquiring' AND state_version=1
               AND checkpoint_digest=?4",
            params![
                successor,
                b"second",
                "00000000-0000-4000-8000-000000000001",
                original
            ],
        )?;
        assert_eq!(stale, 0);
        Ok(())
    }
}
