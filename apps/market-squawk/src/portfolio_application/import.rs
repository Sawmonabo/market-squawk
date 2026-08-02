//! Controlled extraction-artifact import and crash-safe revision publication.

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};

use market_squawk_adapter_portfolio::{
    AccountObservation, HoldingObservation, PortfolioExtractionSource, PortfolioImport,
    PortfolioImportLimits, PortfolioTransaction,
};
use market_squawk_data::{DatasetId, DatasetManifestRef, Sha256Digest, extraction_batch_digest};
use market_squawk_domain::{
    AccountId, DataQuality, DigestAlgorithm, MetadataRevision, Money, NormalizedPortfolioLotMethod,
    NormalizedPortfolioTransactionClass, RevisionNumber, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::{ArtifactPathError, ArtifactRoot, LocalAuthorityStateStore};
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, LotSelection, PortfolioLedger,
    PortfolioLimitInput, PortfolioLimits, PriceEvidence, RevisionEvidence, Task10EconomicKind,
    Task10TransactionInstruction, TradeSide, TransactionRevision, ValuationSet,
};
use market_squawk_services::{RequestContext, TypedToolRequest, TypedToolResult};
use market_squawk_sources::ExtractionBatch;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};

use super::model::{
    AccountHistory, PortfolioReadImage, PublicationEntry, PublicationManifest, PublishedRevision,
    SourceKey,
};
use super::read;
use super::{PortfolioApplicationLimits, PortfolioApplicationServiceError, Runtime, ensure_live};

const RAW_ARCHIVE_NAMESPACE: &str = "portfolio/sources";
const PUBLICATION_AUTHORITY_DIRECTORY: &str = "portfolio/publication";
const IMMUTABLE_IMPORT_NAMESPACE: &str = "portfolio/imports";
const IMMUTABLE_RECEIPT_NAMESPACE: &str = "portfolio/import-receipts";
const IMMUTABLE_RESOLUTION_PLAN_NAMESPACE: &str = "portfolio/import-resolution-plans";
const PENDING_IMPORT_AUTHORITY_DIRECTORY: &str = "portfolio/import-previews";
const IMMUTABLE_PREVIEW_NAMESPACE: &str = "portfolio/import-preview-inputs";
const PENDING_IMPORT_SCHEMA_VERSION: u16 = 1;
const MAX_PENDING_IMPORTS: usize = 4_096;

/// Server-held, canonicalized result of one staged portfolio input.
///
/// The desktop receives only this bounded projection. The exact staged bytes, raw source
/// evidence, and authority binding remain under the local portfolio authority until commit.
#[derive(Clone, Debug)]
pub(crate) struct PortfolioImportPreview {
    pub(crate) preview_id: String,
    pub(crate) preview_digest: [u8; 32],
    pub(crate) result: Value,
}

/// One ambiguity-only interpretation submitted at the governed commit boundary.
///
/// The caller cannot provide a quantity, amount, source reference, account, actor, source rule,
/// or input ticket. Those values are recovered from the server-held preview and governance
/// authority respectively.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PortfolioImportInterpretation {
    pub(crate) record_id: String,
    pub(crate) interpretation: String,
    pub(crate) rationale: String,
}

/// Governance facts derived by trusted native authority after consuming the one-use handle.
///
/// This is deliberately not deserialized from a client request. The transport/native boundary
/// may construct it only after it has validated the authorization capability and derived its
/// local actor, time, and governing rule identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GovernedImportCommitReceipt {
    pub(crate) actor_id: String,
    pub(crate) committed_at_unix_nanos: String,
    pub(crate) governing_rule_id: String,
    pub(crate) authorization_handle_digest: [u8; 32],
}

/// Server-held governed resolution material for one prepared import.
///
/// The native/governance composition owns construction of this value after consuming
/// `PortfolioImportResolution` authorization tickets. In particular, specific lot identifiers
/// and corporate-action plans never arrive from desktop request JSON.
#[derive(Clone, Debug)]
pub(crate) struct ServerHeldPortfolioImportResolution {
    pub(crate) receipt: GovernedImportCommitReceipt,
    pub(crate) corporate_action_plan: Option<market_squawk_data::CorporateActionPlan>,
    pub(crate) specific_lot_ids: BTreeMap<String, Vec<SourceIdentifier>>,
}

impl GovernedImportCommitReceipt {
    pub(crate) fn validate(&self) -> Result<(), PortfolioApplicationServiceError> {
        if self.actor_id.is_empty()
            || self.actor_id.len() > 512
            || self.governing_rule_id.is_empty()
            || self.governing_rule_id.len() > 512
            || self.committed_at_unix_nanos.parse::<i128>().is_err()
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingImportManifest {
    schema_version: u16,
    entries: Vec<PendingImport>,
}

impl PendingImportManifest {
    const fn empty() -> Self {
        Self {
            schema_version: PENDING_IMPORT_SCHEMA_VERSION,
            entries: Vec::new(),
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, PortfolioApplicationServiceError> {
        let pending: Self = serde_json::from_slice(bytes)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
        if pending.schema_version != PENDING_IMPORT_SCHEMA_VERSION
            || pending.entries.len() > MAX_PENDING_IMPORTS
        {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        Ok(pending)
    }

    fn encode(&self) -> Result<Vec<u8>, PortfolioApplicationServiceError> {
        serde_json::to_vec(self).map_err(|_| PortfolioApplicationServiceError::Publication)
    }
}

/// Durable state machine for an import that has consumed a native staged-input ticket.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingImport {
    preview_id: String,
    preview_digest: [u8; 32],
    account_id: AccountId,
    input_ticket_id: String,
    artifact_reference: String,
    artifact_sha256: [u8; 32],
    phase: PendingImportPhase,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
enum PendingImportPhase {
    /// Preview exists; no raw archive or public revision can have changed.
    Prepared,
    /// Governance passed and the adapter/publication transition must be completed on recovery.
    Promoting {
        interpretations: Vec<PortfolioImportInterpretation>,
        receipt: GovernedImportCommitReceipt,
        specific_lot_ids: BTreeMap<String, Vec<String>>,
        corporate_action_plan_reference: Option<String>,
        corporate_action_plan_sha256: Option<[u8; 32]>,
        corporate_action_content_digest: Option<[u8; 32]>,
        corporate_action_audit_digest: Option<[u8; 32]>,
    },
}

#[derive(Clone, Debug)]
pub(super) struct ImportRequest {
    account_id: AccountId,
    artifact_id: String,
    requested_maximum_bytes: usize,
}

impl ImportRequest {
    pub(super) fn from_request(
        request: &TypedToolRequest,
    ) -> Result<Self, PortfolioApplicationServiceError> {
        let account_id = request
            .arguments()
            .get("accountId")
            .and_then(Value::as_str)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
            .parse()
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        let artifact_id = request
            .arguments()
            .get("artifactId")
            .and_then(Value::as_str)
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?
            .to_owned();
        let requested_maximum_bytes = request
            .arguments()
            .get("resultLimits")
            .and_then(Value::as_object)
            .and_then(|limits| limits.get("maximumBytes"))
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        Ok(Self {
            account_id,
            artifact_id,
            requested_maximum_bytes,
        })
    }
}

pub(super) struct ImportPublication {
    pub(super) image: PortfolioReadImage,
    pub(super) result: TypedToolResult,
}

pub(super) struct ImportAuthority {
    control_root: PathBuf,
    publication: LocalAuthorityStateStore,
    pending: LocalAuthorityStateStore,
    sources: BTreeMap<SourceKey, PortfolioExtractionSource>,
    accounts: BTreeMap<AccountId, AccountHistory>,
    manifest: PublicationManifest,
    pending_manifest: PendingImportManifest,
    limits: PortfolioApplicationLimits,
    portfolio_limits: PortfolioLimits,
}

impl ImportAuthority {
    pub(super) fn restore(
        artifacts: ArtifactRoot,
        control_root: &Path,
        limits: PortfolioApplicationLimits,
    ) -> Result<(Self, PortfolioReadImage), PortfolioApplicationServiceError> {
        let publication =
            LocalAuthorityStateStore::try_open(control_root.join(PUBLICATION_AUTHORITY_DIRECTORY))
                .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let pending = LocalAuthorityStateStore::try_open(
            control_root.join(PENDING_IMPORT_AUTHORITY_DIRECTORY),
        )
        .map_err(|_| PortfolioApplicationServiceError::Authority)?;
        let manifest = publication
            .load()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?
            .map_or_else(
                || Ok(PublicationManifest::empty()),
                |bytes| PublicationManifest::decode(&bytes),
            )?;
        if manifest.entries.len()
            > limits
                .max_accounts
                .saturating_mul(limits.max_history_per_account)
        {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let pending_manifest = pending
            .load()
            .map_err(|_| PortfolioApplicationServiceError::Authority)?
            .map_or_else(
                || Ok(PendingImportManifest::empty()),
                |bytes| PendingImportManifest::decode(&bytes),
            )?;
        let portfolio_limits = portfolio_limits(limits)?;
        let mut authority = Self {
            control_root: control_root.to_path_buf(),
            publication,
            pending,
            sources: BTreeMap::new(),
            accounts: BTreeMap::new(),
            manifest: PublicationManifest::empty(),
            pending_manifest,
            limits,
            portfolio_limits,
        };
        let mut seen_publications = BTreeSet::new();
        for entry in &manifest.entries {
            let canonical_reference = format!(
                "{IMMUTABLE_IMPORT_NAMESPACE}/{}.json",
                hex(&entry.artifact_sha256)
            );
            if entry.artifact_reference != canonical_reference
                || !seen_publications.insert((entry.account_id, entry.artifact_sha256))
            {
                return Err(PortfolioApplicationServiceError::CorruptPublication);
            }
            let bytes = read_artifact(
                &artifacts,
                &entry.artifact_reference,
                limits.max_artifact_bytes,
            )?;
            if Sha256::digest(&bytes).as_slice() != entry.artifact_sha256 {
                return Err(PortfolioApplicationServiceError::CorruptPublication);
            }
            match (
                entry.governance_receipt_reference.as_deref(),
                entry.governance_receipt_sha256,
            ) {
                (Some(reference), Some(digest)) => {
                    let expected = format!("{IMMUTABLE_RECEIPT_NAMESPACE}/{}.json", hex(&digest));
                    if reference != expected
                        || Sha256::digest(&read_artifact(
                            &artifacts,
                            reference,
                            limits.max_artifact_bytes,
                        )?)
                        .as_slice()
                            != digest
                    {
                        return Err(PortfolioApplicationServiceError::CorruptPublication);
                    }
                }
                (None, None) => {}
                _ => return Err(PortfolioApplicationServiceError::CorruptPublication),
            }
            let batch: ExtractionBatch = serde_json::from_slice(&bytes)
                .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
            authority.apply_batch(entry.account_id, &batch, entry.artifact_sha256)?;
            authority.manifest.entries.push(entry.clone());
        }
        let image =
            PortfolioReadImage::try_from_accounts(authority.accounts.clone(), authority.limits)?;
        Ok((authority, image))
    }

    pub(super) fn import(
        &mut self,
        artifacts: &ArtifactRoot,
        request: ImportRequest,
        context: &RequestContext,
        runtime: &Runtime,
    ) -> Result<ImportPublication, PortfolioApplicationServiceError> {
        ensure_live(runtime, context)?;
        let bytes = read_artifact(
            artifacts,
            &request.artifact_id,
            self.limits.max_artifact_bytes,
        )?;
        ensure_live(runtime, context)?;
        let artifact_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        let batch: ExtractionBatch = serde_json::from_slice(&bytes)
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        let canonical_reference = format!(
            "{IMMUTABLE_IMPORT_NAMESPACE}/{}.json",
            hex(&artifact_sha256)
        );

        if let Some(entry_index) = self.manifest.entries.iter().position(|entry| {
            entry.account_id == request.account_id && entry.artifact_sha256 == artifact_sha256
        }) {
            persist_immutable(artifacts, &canonical_reference, &bytes)?;
            let image = PortfolioReadImage::try_from_accounts(self.accounts.clone(), self.limits)?;
            let revision_index = self.manifest.entries[..=entry_index]
                .iter()
                .filter(|entry| entry.account_id == request.account_id)
                .count()
                .checked_sub(1)
                .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
            let revision = image
                .accounts
                .get(&request.account_id)
                .and_then(|history| history.revisions.get(revision_index))
                .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
            let result =
                import_result(revision, "replay", context, request.requested_maximum_bytes)?;
            return Ok(ImportPublication { image, result });
        }

        match self.accounts.get(&request.account_id) {
            Some(history) if history.revisions.len() >= self.limits.max_history_per_account => {
                return Err(PortfolioApplicationServiceError::ResourceExhausted);
            }
            None if self.accounts.len() >= self.limits.max_accounts => {
                return Err(PortfolioApplicationServiceError::ResourceExhausted);
            }
            _ => {}
        }
        let mut candidate_accounts = self.accounts.clone();
        let published = self.build_revision(
            request.account_id,
            &batch,
            artifact_sha256,
            &candidate_accounts,
        )?;
        let history = candidate_accounts.entry(request.account_id).or_default();
        if history.revisions.len() >= self.limits.max_history_per_account {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        history.revisions.push(published);
        let image = PortfolioReadImage::try_from_accounts(candidate_accounts.clone(), self.limits)?;
        let mut candidate_manifest = self.manifest.clone();
        candidate_manifest.entries.push(PublicationEntry {
            account_id: request.account_id,
            artifact_reference: canonical_reference.clone(),
            artifact_sha256,
            governance_receipt_reference: None,
            governance_receipt_sha256: None,
        });
        let encoded = candidate_manifest.encode()?;
        let revision = image
            .accounts
            .get(&request.account_id)
            .and_then(|history| history.revisions.last())
            .ok_or(PortfolioApplicationServiceError::Publication)?;
        let result = import_result(
            revision,
            "applied",
            context,
            request.requested_maximum_bytes,
        )?;
        ensure_live(runtime, context)?;
        persist_immutable(artifacts, &canonical_reference, &bytes)?;
        ensure_live(runtime, context)?;
        self.publication
            .store(&encoded)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        self.accounts = candidate_accounts;
        self.manifest = candidate_manifest;
        Ok(ImportPublication { image, result })
    }

    /// Consumes already-verified native staged bytes into an immutable, server-held preview.
    ///
    /// This method deliberately does not call `import_batch`: its only durable effects are the
    /// immutable input copy and an authority-private prepared record. Raw source/archive state,
    /// active source lineage, portfolio revisions, and the public read image remain unchanged.
    pub(super) fn prepare_staged_import(
        &mut self,
        artifacts: &ArtifactRoot,
        account_id: AccountId,
        input_ticket_id: String,
        bytes: &[u8],
    ) -> Result<PortfolioImportPreview, PortfolioApplicationServiceError> {
        if input_ticket_id.is_empty() || input_ticket_id.len() > 512 {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        if bytes.is_empty() {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        if bytes.len() > self.limits.max_artifact_bytes {
            return Err(PortfolioApplicationServiceError::ResourceExhausted);
        }
        let artifact_sha256: [u8; 32] = Sha256::digest(bytes).into();
        let batch: ExtractionBatch = serde_json::from_slice(bytes)
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
        let imported = self.preview_batch(&batch)?;
        validate_account_binding(account_id, &imported)?;
        let preview_body = preview_projection(
            account_id,
            &imported,
            self.accounts
                .get(&account_id)
                .and_then(|history| history.revisions.last()),
        )?;
        let preview_digest: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&json!({
                "accountId": account_id.to_string(),
                "artifactSha256": hex(&artifact_sha256),
                "preview": preview_body,
            }))
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        )
        .into();
        let preview_id = hex(&preview_digest);
        let artifact_reference = format!(
            "{IMMUTABLE_PREVIEW_NAMESPACE}/{}.json",
            hex(&artifact_sha256)
        );
        let entry = PendingImport {
            preview_id: preview_id.clone(),
            preview_digest,
            account_id,
            input_ticket_id,
            artifact_reference: artifact_reference.clone(),
            artifact_sha256,
            phase: PendingImportPhase::Prepared,
        };
        match self
            .pending_manifest
            .entries
            .iter()
            .find(|existing| existing.preview_id == preview_id)
        {
            Some(existing)
                if existing.account_id == entry.account_id
                    && existing.preview_digest == entry.preview_digest
                    && existing.artifact_sha256 == entry.artifact_sha256 => {}
            Some(_) => return Err(PortfolioApplicationServiceError::CorruptPublication),
            None => {
                if self.pending_manifest.entries.len() >= MAX_PENDING_IMPORTS {
                    return Err(PortfolioApplicationServiceError::ResourceExhausted);
                }
                let mut candidate = self.pending_manifest.clone();
                candidate.entries.push(entry);
                let encoded = candidate.encode()?;
                // The immutable bytes are durable before their authority reference becomes live.
                persist_immutable(artifacts, &artifact_reference, bytes)?;
                self.pending
                    .store(&encoded)
                    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
                self.pending_manifest = candidate;
            }
        }
        Ok(PortfolioImportPreview {
            preview_id,
            preview_digest,
            result: json!({
                "previewId": hex(&preview_digest),
                "digest": hex(&preview_digest),
                "preview": preview_body,
            }),
        })
    }

    fn preview_batch(
        &mut self,
        batch: &ExtractionBatch,
    ) -> Result<PortfolioImport, PortfolioApplicationServiceError> {
        let source = self.source_for_batch(batch)?;
        source
            .preview_batch(batch)
            .map_err(|_| PortfolioApplicationServiceError::Import)
    }

    fn source_for_batch(
        &mut self,
        batch: &ExtractionBatch,
    ) -> Result<&mut PortfolioExtractionSource, PortfolioApplicationServiceError> {
        let object = batch.request().object();
        let key = SourceKey {
            source_id: object.source_id().clone(),
            metadata_revision: object
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .to_owned(),
        };
        match self.sources.entry(key.clone()) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let namespace = source_namespace(&key);
                let store = LocalAuthorityStateStore::try_open(
                    self.control_root
                        .join(RAW_ARCHIVE_NAMESPACE)
                        .join(namespace),
                )
                .map_err(|_| PortfolioApplicationServiceError::Authority)?;
                let source = PortfolioExtractionSource::try_new(
                    key.source_id.clone(),
                    MetadataRevision::new(
                        SourceIdentifier::try_from(key.metadata_revision.clone())
                            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?,
                    ),
                    DataQuality::DirectUnverified,
                    store,
                    None,
                    PortfolioImportLimits::standard(),
                )
                .map_err(|_| PortfolioApplicationServiceError::Import)?;
                Ok(entry.insert(source))
            }
        }
    }

    /// Promotes exactly one server-held preview after the native governance boundary has
    /// consumed its authorization capability and supplied server-derived resolution bindings.
    ///
    /// The pending state is made durable before raw source promotion. Therefore an interruption
    /// can never expose a new public revision without an exact prepared input and retained
    /// governance/interpretation receipt; recovery may resume only by re-presenting the same
    /// server-held resolution material.
    pub(super) fn commit_prepared_import(
        &mut self,
        artifacts: &ArtifactRoot,
        preview_id: &str,
        interpretations: &[PortfolioImportInterpretation],
        resolution: &ServerHeldPortfolioImportResolution,
    ) -> Result<PortfolioReadImage, PortfolioApplicationServiceError> {
        resolution.receipt.validate()?;
        let pending_index = self
            .pending_manifest
            .entries
            .iter()
            .position(|entry| entry.preview_id == preview_id)
            .ok_or(PortfolioApplicationServiceError::NotFound)?;
        let pending = self
            .pending_manifest
            .entries
            .get(pending_index)
            .cloned()
            .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
        let bytes = read_artifact(
            artifacts,
            &pending.artifact_reference,
            self.limits.max_artifact_bytes,
        )?;
        if Sha256::digest(&bytes).as_slice() != pending.artifact_sha256 {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let batch: ExtractionBatch = serde_json::from_slice(&bytes)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
        let preview = self.preview_batch(&batch)?;
        validate_account_binding(pending.account_id, &preview)?;
        let recomputed = preview_projection(
            pending.account_id,
            &preview,
            self.accounts
                .get(&pending.account_id)
                .and_then(|history| history.revisions.last()),
        )?;
        let digest: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&json!({
                "accountId": pending.account_id.to_string(),
                "artifactSha256": hex(&pending.artifact_sha256),
                "preview": recomputed,
            }))
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        )
        .into();
        if digest != pending.preview_digest {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let instructions = governed_instructions(
            &preview,
            self.accounts
                .get(&pending.account_id)
                .and_then(|history| history.revisions.last()),
            interpretations,
            resolution,
        )?;
        let has_corporate_action = preview.transaction_evidence().iter().any(|record| {
            record.classification() == NormalizedPortfolioTransactionClass::CorporateAction
        });
        if has_corporate_action && resolution.corporate_action_plan.is_none() {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let (corporate_action_content_digest, corporate_action_audit_digest) = resolution
            .corporate_action_plan
            .as_ref()
            .map(|plan| (plan.content_hash().bytes(), plan.audit_hash().bytes()))
            .map_or((None, None), |(content, audit)| {
                (Some(content), Some(audit))
            });
        let specific_lot_ids = persisted_specific_lot_ids(resolution);
        let (corporate_action_plan_reference, corporate_action_plan_sha256, plan_bytes) =
            persisted_plan_material(resolution)?;
        if let PendingImportPhase::Promoting {
            interpretations: retained_interpretations,
            receipt,
            specific_lot_ids: retained_lot_ids,
            corporate_action_plan_reference: retained_plan_reference,
            corporate_action_plan_sha256: retained_plan_sha256,
            corporate_action_content_digest: retained_content_digest,
            corporate_action_audit_digest: retained_audit_digest,
        } = &pending.phase
        {
            if retained_interpretations.as_slice() != interpretations
                || receipt != &resolution.receipt
                || retained_lot_ids != &specific_lot_ids
                || retained_plan_reference != &corporate_action_plan_reference
                || retained_plan_sha256 != &corporate_action_plan_sha256
                || retained_content_digest != &corporate_action_content_digest
                || retained_audit_digest != &corporate_action_audit_digest
            {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
        }
        let mut pending_candidate = self.pending_manifest.clone();
        let candidate = pending_candidate
            .entries
            .get_mut(pending_index)
            .ok_or(PortfolioApplicationServiceError::CorruptPublication)?;
        if candidate.preview_digest != digest {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        candidate.phase = PendingImportPhase::Promoting {
            interpretations: interpretations.to_vec(),
            receipt: resolution.receipt.clone(),
            specific_lot_ids,
            corporate_action_plan_reference,
            corporate_action_plan_sha256,
            corporate_action_content_digest,
            corporate_action_audit_digest,
        };
        if let (Some(reference), Some(bytes)) = (
            candidate_corporate_plan_reference(candidate),
            plan_bytes.as_deref(),
        ) {
            persist_immutable(artifacts, reference, bytes)?;
        }
        self.pending
            .store(&pending_candidate.encode()?)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        self.pending_manifest = pending_candidate;

        if self.manifest.entries.iter().any(|entry| {
            entry.account_id == pending.account_id
                && entry.artifact_sha256 == pending.artifact_sha256
        }) {
            return self.complete_pending_publication(pending_index);
        }
        self.apply_governed_batch(
            artifacts,
            pending_index,
            pending.account_id,
            &batch,
            pending.artifact_sha256,
            &instructions,
            resolution.corporate_action_plan.as_ref(),
        )
    }

    /// Completes an interrupted `Promoting` transition using the exact server-held governance
    /// material that was durably bound before raw source promotion. Public reads remain on the
    /// prior image until this completes; a prepared (not promoting) preview is never recoverable
    /// as a mutation.
    pub(super) fn recover_promoting_import(
        &mut self,
        artifacts: &ArtifactRoot,
        preview_id: &str,
        resolution: &ServerHeldPortfolioImportResolution,
    ) -> Result<PortfolioReadImage, PortfolioApplicationServiceError> {
        let pending_index = self
            .pending_manifest
            .entries
            .iter()
            .position(|entry| entry.preview_id == preview_id)
            .ok_or(PortfolioApplicationServiceError::NotFound)?;
        let pending = self.pending_manifest.entries[pending_index].clone();
        let PendingImportPhase::Promoting {
            interpretations,
            receipt,
            specific_lot_ids,
            corporate_action_plan_reference,
            corporate_action_plan_sha256,
            corporate_action_content_digest,
            corporate_action_audit_digest,
        } = &pending.phase
        else {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        };
        resolution.receipt.validate()?;
        let candidate_content = resolution
            .corporate_action_plan
            .as_ref()
            .map(|plan| plan.content_hash().bytes());
        let candidate_audit = resolution
            .corporate_action_plan
            .as_ref()
            .map(|plan| plan.audit_hash().bytes());
        let (candidate_plan_reference, candidate_plan_sha256, _) =
            persisted_plan_material(resolution)?;
        if receipt != &resolution.receipt
            || specific_lot_ids != &persisted_specific_lot_ids(resolution)
            || corporate_action_plan_reference != &candidate_plan_reference
            || corporate_action_plan_sha256 != &candidate_plan_sha256
            || corporate_action_content_digest != &candidate_content
            || corporate_action_audit_digest != &candidate_audit
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let bytes = read_artifact(
            artifacts,
            &pending.artifact_reference,
            self.limits.max_artifact_bytes,
        )?;
        if Sha256::digest(&bytes).as_slice() != pending.artifact_sha256 {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        let batch: ExtractionBatch = serde_json::from_slice(&bytes)
            .map_err(|_| PortfolioApplicationServiceError::CorruptPublication)?;
        let preview = self.preview_batch(&batch)?;
        validate_account_binding(pending.account_id, &preview)?;
        let instructions = governed_instructions(
            &preview,
            self.accounts
                .get(&pending.account_id)
                .and_then(|history| history.revisions.last()),
            interpretations,
            resolution,
        )?;
        if self.manifest.entries.iter().any(|entry| {
            entry.account_id == pending.account_id
                && entry.artifact_sha256 == pending.artifact_sha256
        }) {
            return self.complete_pending_publication(pending_index);
        }
        self.apply_governed_batch(
            artifacts,
            pending_index,
            pending.account_id,
            &batch,
            pending.artifact_sha256,
            &instructions,
            resolution.corporate_action_plan.as_ref(),
        )
    }

    fn apply_governed_batch(
        &mut self,
        artifacts: &ArtifactRoot,
        pending_index: usize,
        account_id: AccountId,
        batch: &ExtractionBatch,
        artifact_sha256: [u8; 32],
        instructions: &[Task10TransactionInstruction],
        corporate_action_plan: Option<&market_squawk_data::CorporateActionPlan>,
    ) -> Result<PortfolioReadImage, PortfolioApplicationServiceError> {
        match self.accounts.get(&account_id) {
            Some(history) if history.revisions.len() >= self.limits.max_history_per_account => {
                return Err(PortfolioApplicationServiceError::ResourceExhausted);
            }
            None if self.accounts.len() >= self.limits.max_accounts => {
                return Err(PortfolioApplicationServiceError::ResourceExhausted);
            }
            _ => {}
        }
        let imported = {
            let source = self.source_for_batch(batch)?;
            source
                .import_batch(batch)
                .map_err(|_| PortfolioApplicationServiceError::Import)?
        };
        validate_account_binding(account_id, &imported)?;
        let prior = self
            .accounts
            .get(&account_id)
            .and_then(|history| history.revisions.last());
        let account = imported
            .accounts()
            .first()
            .cloned()
            .or_else(|| prior.map(|revision| revision.account.clone()))
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let holdings = merge_holdings(prior, imported.holdings());
        let transactions = merge_transactions(prior, imported.transactions());
        let core = build_governed_core_revision(
            prior,
            &account,
            &holdings,
            batch,
            artifact_sha256,
            self.portfolio_limits,
            self.accounts
                .get(&account_id)
                .map_or(1, |history| history.revisions.len().saturating_add(1)),
            imported.transaction_evidence(),
            instructions,
            corporate_action_plan,
        )?;
        let object = batch.request().object();
        let key = SourceKey {
            source_id: object.source_id().clone(),
            metadata_revision: object
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .to_owned(),
        };
        let mut source_coverage =
            prior.map_or_else(Vec::new, |revision| revision.source_coverage.clone());
        if !source_coverage.contains(&key.source_id) {
            source_coverage.push(key.source_id.clone());
            source_coverage.sort_unstable();
        }
        let published = PublishedRevision {
            core,
            account,
            holdings,
            transactions,
            discrepancies: imported.discrepancies().to_vec(),
            source_id: object.source_id().clone(),
            source_coverage,
            effective_at: maximum_effective(batch)?,
            available_at: maximum_conservative_availability(batch),
            artifact_sha256,
        };
        let mut candidate_accounts = self.accounts.clone();
        candidate_accounts
            .entry(account_id)
            .or_default()
            .revisions
            .push(published);
        let image = PortfolioReadImage::try_from_accounts(candidate_accounts.clone(), self.limits)?;
        let canonical_reference = format!(
            "{IMMUTABLE_IMPORT_NAMESPACE}/{}.json",
            hex(&artifact_sha256)
        );
        let mut candidate_manifest = self.manifest.clone();
        let receipt = governed_receipt_bytes(
            self.pending_manifest
                .entries
                .get(pending_index)
                .ok_or(PortfolioApplicationServiceError::CorruptPublication)?,
        )?;
        let receipt_sha256: [u8; 32] = Sha256::digest(&receipt).into();
        let receipt_reference = format!(
            "{IMMUTABLE_RECEIPT_NAMESPACE}/{}.json",
            hex(&receipt_sha256)
        );
        candidate_manifest.entries.push(PublicationEntry {
            account_id,
            artifact_reference: canonical_reference.clone(),
            artifact_sha256,
            governance_receipt_reference: Some(receipt_reference.clone()),
            governance_receipt_sha256: Some(receipt_sha256),
        });
        persist_immutable(
            artifacts,
            &canonical_reference,
            &read_artifact(
                artifacts,
                &self.pending_manifest.entries[pending_index].artifact_reference,
                self.limits.max_artifact_bytes,
            )?,
        )?;
        persist_immutable(artifacts, &receipt_reference, &receipt)?;
        self.publication
            .store(&candidate_manifest.encode()?)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        self.accounts = candidate_accounts;
        self.manifest = candidate_manifest;
        self.complete_pending_publication(pending_index)
            .map(|_| image)
    }

    fn complete_pending_publication(
        &mut self,
        pending_index: usize,
    ) -> Result<PortfolioReadImage, PortfolioApplicationServiceError> {
        let mut candidate = self.pending_manifest.clone();
        if pending_index >= candidate.entries.len() {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        candidate.entries.remove(pending_index);
        self.pending
            .store(&candidate.encode()?)
            .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        self.pending_manifest = candidate;
        PortfolioReadImage::try_from_accounts(self.accounts.clone(), self.limits)
    }

    fn apply_batch(
        &mut self,
        account_id: AccountId,
        batch: &ExtractionBatch,
        artifact_sha256: [u8; 32],
    ) -> Result<(), PortfolioApplicationServiceError> {
        let current_accounts = self.accounts.clone();
        let published =
            self.build_revision(account_id, batch, artifact_sha256, &current_accounts)?;
        let history = self.accounts.entry(account_id).or_default();
        if history.revisions.len() >= self.limits.max_history_per_account {
            return Err(PortfolioApplicationServiceError::CorruptPublication);
        }
        history.revisions.push(published);
        Ok(())
    }

    fn build_revision(
        &mut self,
        account_id: AccountId,
        batch: &ExtractionBatch,
        artifact_sha256: [u8; 32],
        accounts: &BTreeMap<AccountId, AccountHistory>,
    ) -> Result<PublishedRevision, PortfolioApplicationServiceError> {
        let object = batch.request().object();
        let key = SourceKey {
            source_id: object.source_id().clone(),
            metadata_revision: object
                .metadata_revision()
                .as_source_identifier()
                .as_str()
                .to_owned(),
        };
        let source = match self.sources.entry(key.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let namespace = source_namespace(&key);
                let store = LocalAuthorityStateStore::try_open(
                    self.control_root
                        .join(RAW_ARCHIVE_NAMESPACE)
                        .join(namespace),
                )
                .map_err(|_| PortfolioApplicationServiceError::Authority)?;
                entry.insert(
                    PortfolioExtractionSource::try_new(
                        key.source_id.clone(),
                        MetadataRevision::new(
                            SourceIdentifier::try_from(key.metadata_revision.clone()).map_err(
                                |_| PortfolioApplicationServiceError::CorruptPublication,
                            )?,
                        ),
                        DataQuality::DirectUnverified,
                        store,
                        None,
                        PortfolioImportLimits::standard(),
                    )
                    .map_err(|_| PortfolioApplicationServiceError::Import)?,
                )
            }
        };
        let imported = source
            .import_batch(batch)
            .map_err(|_| PortfolioApplicationServiceError::Import)?;
        let prior = accounts
            .get(&account_id)
            .and_then(|history| history.revisions.last());
        validate_account_binding(account_id, &imported)?;
        let account = imported
            .accounts()
            .first()
            .cloned()
            .or_else(|| prior.map(|revision| revision.account.clone()))
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        if account.account_id() != account_id {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let holdings = merge_holdings(prior, imported.holdings());
        let transactions = merge_transactions(prior, imported.transactions());
        let effective_at = maximum_effective(batch)?;
        let available_at = maximum_conservative_availability(batch);
        let knowledge_at = available_at
            .filter(|available| *available > effective_at)
            .unwrap_or(effective_at);
        let mut source_coverage =
            prior.map_or_else(Vec::new, |revision| revision.source_coverage.clone());
        if !source_coverage.contains(&key.source_id) {
            source_coverage.push(key.source_id.clone());
            source_coverage.sort_unstable();
        }
        let core = build_core_revision(
            prior,
            &account,
            knowledge_at,
            object.source_id(),
            batch,
            artifact_sha256,
            self.portfolio_limits,
            accounts
                .get(&account_id)
                .map_or(1, |history| history.revisions.len().saturating_add(1)),
        )?;
        Ok(PublishedRevision {
            core,
            account,
            holdings,
            transactions,
            discrepancies: imported.discrepancies().to_vec(),
            source_id: object.source_id().clone(),
            source_coverage,
            effective_at,
            available_at,
            artifact_sha256,
        })
    }
}

fn validate_account_binding(
    account_id: AccountId,
    imported: &PortfolioImport,
) -> Result<(), PortfolioApplicationServiceError> {
    if imported.accounts().len() > 1
        || imported
            .accounts()
            .iter()
            .any(|value| value.account_id() != account_id)
        || imported
            .holdings()
            .iter()
            .any(|value| value.account_id() != account_id)
        || imported
            .transactions()
            .iter()
            .any(|value| value.account_id() != account_id)
        || imported
            .supplied_totals()
            .iter()
            .any(|value| value.account_id() != account_id)
    {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    Ok(())
}

fn merge_holdings(
    prior: Option<&PublishedRevision>,
    imported: &[HoldingObservation],
) -> Vec<HoldingObservation> {
    let mut holdings = prior.map_or_else(Vec::new, |revision| revision.holdings.clone());
    for holding in imported {
        if let Some(index) = holdings.iter().position(|existing| {
            existing.account_id() == holding.account_id()
                && existing.instrument_id() == holding.instrument_id()
        }) {
            holdings[index] = holding.clone();
        } else {
            holdings.push(holding.clone());
        }
    }
    holdings.sort_unstable_by_key(HoldingObservation::instrument_id);
    holdings
}

fn merge_transactions(
    prior: Option<&PublishedRevision>,
    imported: &[PortfolioTransaction],
) -> Vec<PortfolioTransaction> {
    let mut transactions = prior.map_or_else(Vec::new, |revision| revision.transactions.clone());
    for transaction in imported {
        if let Some(index) = transactions.iter().position(|existing| {
            existing.account_id() == transaction.account_id()
                && existing.broker_transaction_id() == transaction.broker_transaction_id()
        }) {
            transactions[index] = transaction.clone();
        } else {
            transactions.push(transaction.clone());
        }
    }
    transactions.sort_unstable_by(|left, right| {
        left.occurred_at().cmp(&right.occurred_at()).then_with(|| {
            left.broker_transaction_id()
                .cmp(right.broker_transaction_id())
        })
    });
    transactions
}

fn governed_instructions(
    imported: &PortfolioImport,
    prior: Option<&PublishedRevision>,
    interpretations: &[PortfolioImportInterpretation],
    resolution: &ServerHeldPortfolioImportResolution,
) -> Result<Vec<Task10TransactionInstruction>, PortfolioApplicationServiceError> {
    let expected = imported
        .transaction_evidence()
        .iter()
        .filter(|record| {
            matches!(
                record.classification(),
                NormalizedPortfolioTransactionClass::Trade
                    | NormalizedPortfolioTransactionClass::Income
            )
        })
        .map(|record| (record.logical_record_id().as_str(), record))
        .collect::<BTreeMap<_, _>>();
    if interpretations.len() != expected.len() {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let mut instructions = Vec::new();
    instructions
        .try_reserve_exact(interpretations.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    let mut seen = BTreeSet::new();
    for submitted in interpretations {
        if submitted.record_id.is_empty()
            || submitted.record_id.len() > 512
            || submitted.rationale.trim().is_empty()
            || submitted.rationale.len() > 4_096
            || !seen.insert(submitted.record_id.as_str())
        {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let evidence = expected
            .get(submitted.record_id.as_str())
            .copied()
            .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
        let economic_kind = match evidence.classification() {
            NormalizedPortfolioTransactionClass::Income => {
                match submitted.interpretation.as_str() {
                    "dividend" => Task10EconomicKind::Dividend,
                    "interest" => Task10EconomicKind::Interest,
                    "withholding" => Task10EconomicKind::Withholding,
                    _ => return Err(PortfolioApplicationServiceError::InvalidRequest),
                }
            }
            NormalizedPortfolioTransactionClass::Trade => {
                let quantity_is_positive = evidence
                    .quantity()
                    .is_some_and(|quantity| quantity.is_sign_positive());
                let (side, lot_selection) = trade_resolution(
                    &submitted.interpretation,
                    quantity_is_positive,
                    evidence.lot_method(),
                    &submitted.record_id,
                    prior,
                    resolution,
                    evidence,
                )?;
                Task10EconomicKind::Trade {
                    side,
                    lot_selection,
                }
            }
            _ => return Err(PortfolioApplicationServiceError::InvalidRequest),
        };
        instructions.push(
            Task10TransactionInstruction::try_new(
                evidence.broker_transaction_id().clone(),
                economic_kind,
            )
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?,
        );
    }
    Ok(instructions)
}

fn persisted_specific_lot_ids(
    resolution: &ServerHeldPortfolioImportResolution,
) -> BTreeMap<String, Vec<String>> {
    resolution
        .specific_lot_ids
        .iter()
        .map(|(record_id, lot_ids)| {
            (
                record_id.clone(),
                lot_ids
                    .iter()
                    .map(|lot_id| lot_id.as_str().to_owned())
                    .collect(),
            )
        })
        .collect()
}

fn persisted_plan_material(
    resolution: &ServerHeldPortfolioImportResolution,
) -> Result<(Option<String>, Option<[u8; 32]>, Option<Vec<u8>>), PortfolioApplicationServiceError> {
    let Some(plan) = resolution.corporate_action_plan.as_ref() else {
        return Ok((None, None, None));
    };
    let bytes = plan
        .encode_recovery_material()
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let digest: [u8; 32] = Sha256::digest(&bytes).into();
    Ok((
        Some(format!(
            "{IMMUTABLE_RESOLUTION_PLAN_NAMESPACE}/{}.json",
            hex(&digest)
        )),
        Some(digest),
        Some(bytes),
    ))
}

fn candidate_corporate_plan_reference(pending: &PendingImport) -> Option<&str> {
    match &pending.phase {
        PendingImportPhase::Promoting {
            corporate_action_plan_reference,
            ..
        } => corporate_action_plan_reference.as_deref(),
        PendingImportPhase::Prepared => None,
    }
}

fn governed_receipt_bytes(
    pending: &PendingImport,
) -> Result<Vec<u8>, PortfolioApplicationServiceError> {
    let PendingImportPhase::Promoting {
        interpretations,
        receipt,
        specific_lot_ids,
        corporate_action_plan_reference,
        corporate_action_plan_sha256,
        corporate_action_content_digest,
        corporate_action_audit_digest,
    } = &pending.phase
    else {
        return Err(PortfolioApplicationServiceError::CorruptPublication);
    };
    serde_json::to_vec(&json!({
        "schemaVersion": 1_u16,
        "previewId": pending.preview_id,
        "previewDigest": hex(&pending.preview_digest),
        "accountId": pending.account_id.to_string(),
        "governance": receipt,
        "interpretations": interpretations,
        "specificLotIds": specific_lot_ids,
        "corporateActionPlanReference": corporate_action_plan_reference,
        "corporateActionPlanSha256": corporate_action_plan_sha256.map(|value| hex(&value)),
        "corporateActionContentDigest": corporate_action_content_digest.map(|value| hex(&value)),
        "corporateActionAuditDigest": corporate_action_audit_digest.map(|value| hex(&value)),
    }))
    .map_err(|_| PortfolioApplicationServiceError::Publication)
}

#[allow(
    clippy::too_many_arguments,
    reason = "all evidence is checked at the governed boundary"
)]
fn trade_resolution(
    interpretation: &str,
    quantity_is_positive: bool,
    method: Option<NormalizedPortfolioLotMethod>,
    record_id: &str,
    prior: Option<&PublishedRevision>,
    resolution: &ServerHeldPortfolioImportResolution,
    evidence: &market_squawk_domain::NormalizedPortfolioTransactionEvidence,
) -> Result<(TradeSide, LotSelection), PortfolioApplicationServiceError> {
    let (side, expected_method, specific) = match interpretation {
        "buy" => (TradeSide::Buy, NormalizedPortfolioLotMethod::Fifo, false),
        "buy_to_cover" => (
            TradeSide::BuyToCover,
            NormalizedPortfolioLotMethod::Fifo,
            false,
        ),
        "sell" => (TradeSide::Sell, NormalizedPortfolioLotMethod::Fifo, false),
        "sell_short" => (
            TradeSide::SellShort,
            NormalizedPortfolioLotMethod::Fifo,
            false,
        ),
        "buy_lifo" => (TradeSide::Buy, NormalizedPortfolioLotMethod::Lifo, false),
        "buy_to_cover_lifo" => (
            TradeSide::BuyToCover,
            NormalizedPortfolioLotMethod::Lifo,
            false,
        ),
        "sell_lifo" => (TradeSide::Sell, NormalizedPortfolioLotMethod::Lifo, false),
        "sell_short_lifo" => (
            TradeSide::SellShort,
            NormalizedPortfolioLotMethod::Lifo,
            false,
        ),
        "buy_average_cost" => (
            TradeSide::Buy,
            NormalizedPortfolioLotMethod::AverageCost,
            false,
        ),
        "buy_to_cover_average_cost" => (
            TradeSide::BuyToCover,
            NormalizedPortfolioLotMethod::AverageCost,
            false,
        ),
        "sell_average_cost" => (
            TradeSide::Sell,
            NormalizedPortfolioLotMethod::AverageCost,
            false,
        ),
        "sell_short_average_cost" => (
            TradeSide::SellShort,
            NormalizedPortfolioLotMethod::AverageCost,
            false,
        ),
        "buy_specific_identification" => (
            TradeSide::Buy,
            NormalizedPortfolioLotMethod::SpecificIdentification,
            true,
        ),
        "buy_to_cover_specific_identification" => (
            TradeSide::BuyToCover,
            NormalizedPortfolioLotMethod::SpecificIdentification,
            true,
        ),
        "sell_specific_identification" => (
            TradeSide::Sell,
            NormalizedPortfolioLotMethod::SpecificIdentification,
            true,
        ),
        "sell_short_specific_identification" => (
            TradeSide::SellShort,
            NormalizedPortfolioLotMethod::SpecificIdentification,
            true,
        ),
        _ => return Err(PortfolioApplicationServiceError::InvalidRequest),
    };
    let sign_matches = if quantity_is_positive {
        matches!(side, TradeSide::Buy | TradeSide::BuyToCover)
    } else {
        matches!(side, TradeSide::Sell | TradeSide::SellShort)
    };
    if !sign_matches || method != Some(expected_method) {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let lot_selection = match expected_method {
        NormalizedPortfolioLotMethod::Fifo => LotSelection::Fifo,
        NormalizedPortfolioLotMethod::Lifo => LotSelection::Lifo,
        NormalizedPortfolioLotMethod::AverageCost => LotSelection::AverageCost,
        NormalizedPortfolioLotMethod::SpecificIdentification if specific => {
            if matches!(side, TradeSide::Buy | TradeSide::SellShort) {
                // No existing inventory is disposed. The source method remains bound by the
                // normalized evidence, while Fifo is an inert internal placeholder rather than
                // an invented selected lot.
                return Ok((side, LotSelection::Fifo));
            }
            let selected = resolution
                .specific_lot_ids
                .get(record_id)
                .ok_or(PortfolioApplicationServiceError::InvalidRequest)?;
            if selected.is_empty() || selected.len() > 100_000 {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
            let eligible = eligible_lot_ids(prior, evidence)
                .into_iter()
                .collect::<BTreeSet<_>>();
            if selected
                .iter()
                .any(|lot_id| !eligible.contains(lot_id.as_str()))
            {
                return Err(PortfolioApplicationServiceError::InvalidRequest);
            }
            LotSelection::SpecificIdentification(selected.clone())
        }
        NormalizedPortfolioLotMethod::SpecificIdentification => {
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
    };
    Ok((side, lot_selection))
}

#[allow(
    clippy::too_many_arguments,
    reason = "all source, valuation, and governance evidence binds one revision"
)]
fn build_governed_core_revision(
    prior: Option<&PublishedRevision>,
    account: &AccountObservation,
    holdings: &[HoldingObservation],
    batch: &ExtractionBatch,
    artifact_sha256: [u8; 32],
    limits: PortfolioLimits,
    manifest_version: usize,
    transactions: &[market_squawk_domain::NormalizedPortfolioTransactionEvidence],
    instructions: &[Task10TransactionInstruction],
    corporate_action_plan: Option<&market_squawk_data::CorporateActionPlan>,
) -> Result<market_squawk_portfolio::PortfolioRevision, PortfolioApplicationServiceError> {
    let as_of = account.as_of();
    if transactions
        .iter()
        .any(|record| record.occurred_at() > as_of)
    {
        return Err(PortfolioApplicationServiceError::InvalidRequest);
    }
    let mut prices = Vec::new();
    prices
        .try_reserve_exact(holdings.len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for holding in holdings {
        if holding.as_of() != as_of || holding.currency() != account.currency() {
            // No live valuation or FX inference is available at portfolio import. A mixed-time or
            // mixed-currency snapshot must bring separately governed exact evidence.
            return Err(PortfolioApplicationServiceError::InvalidRequest);
        }
        let quantity = holding.quantity().as_decimal().abs();
        let market_value = holding.market_value().amount().abs();
        let unit_price = market_value
            .checked_div(quantity)
            .ok_or(PortfolioApplicationServiceError::Publication)?;
        prices.push(
            PriceEvidence::try_new(
                holding.instrument_id(),
                Money::new(unit_price, account.currency()),
                as_of,
                holding.source_reference().clone(),
            )
            .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?,
        );
    }
    let digest = extraction_batch_digest(batch)
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    if digest.algorithm() != DigestAlgorithm::Sha256 {
        return Err(PortfolioApplicationServiceError::Publication);
    }
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("portfolio-import")
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        u64::try_from(manifest_version)?,
        market_squawk_data::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        Sha256Digest::new(digest.bytes()),
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let source = SourceIdentifier::try_from(batch.request().object().source_id().as_str())
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let evidence = RevisionEvidence::try_new(
        as_of,
        dataset.clone(),
        Sha256Digest::new(digest.bytes()),
        Sha256Digest::new(artifact_sha256),
        vec![source],
        Vec::new(),
        corporate_action_plan.map(market_squawk_portfolio::CorporateActionBinding::from_plan),
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let valuation = ValuationSet::try_new(
        account.currency(),
        as_of,
        dataset,
        Sha256Digest::new(digest.bytes()),
        prices,
        Vec::new(),
        limits,
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let mut ledger = prior
        .map_or_else(
            || PortfolioLedger::try_new(account.account_id(), account.currency(), limits),
            |revision| revision.core.clone().into_ledger(),
        )
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    ledger
        .try_apply_import(
            transactions,
            instructions,
            corporate_action_plan,
            valuation,
            evidence,
        )
        .map_err(|_| PortfolioApplicationServiceError::Publication)
}

/// Produces the bounded visible portion of an exact server-held preview.
///
/// The projection is intentionally evidence-first: all user-selectable values are keys into
/// retained normalized evidence, never caller-reported amounts or source references. Exact raw
/// payloads remain private, but their immutable reference/digest are visible for audit.
fn preview_projection(
    account_id: AccountId,
    imported: &PortfolioImport,
    prior: Option<&PublishedRevision>,
) -> Result<Value, PortfolioApplicationServiceError> {
    let mut records = Vec::new();
    records
        .try_reserve_exact(imported.transaction_evidence().len())
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    for evidence in imported.transaction_evidence() {
        let eligible_lots = eligible_lot_ids(prior, evidence);
        let allowed_interpretations = allowed_interpretations(evidence, &eligible_lots);
        records.push(json!({
            "recordId": evidence.logical_record_id().as_str(),
            "brokerTransactionId": evidence.broker_transaction_id().as_str(),
            "sourceReference": evidence.raw_source_reference().as_str(),
            "rawPayloadDigest": {
                "algorithm": match evidence.raw_payload_digest().algorithm() {
                    DigestAlgorithm::Sha256 => "sha256",
                    DigestAlgorithm::Blake3 => "blake3",
                },
                "value": hex(&evidence.raw_payload_digest().bytes()),
            },
            "sourceRevision": evidence.source_revision().as_str(),
            "supersedesSourceRevision": evidence
                .supersedes_source_revision()
                .map(|value| value.as_str()),
            "classification": classification_name(evidence.classification()),
            "amount": {
                "value": evidence.amount().amount().to_string(),
                "currency": evidence.amount().currency().as_str(),
            },
            "quantity": evidence.quantity().map(|value| value.to_string()),
            "occurredAtUnixNanos": evidence.occurred_at().unix_nanos().to_string(),
            "lotMethod": evidence.lot_method().map(lot_method_name),
            "allowedInterpretations": allowed_interpretations,
            "eligibleOpeningLotIds": eligible_lots,
        }));
    }
    let raw_records = imported
        .raw_records()
        .iter()
        .map(|record| {
            let digest = record.payload_hash();
            json!({
                "sourceReference": record.source_reference().as_str(),
                "payloadDigest": {
                    "algorithm": match digest.algorithm() {
                        DigestAlgorithm::Sha256 => "sha256",
                        DigestAlgorithm::Blake3 => "blake3",
                    },
                    "value": hex(&digest.bytes()),
                },
            })
        })
        .collect::<Vec<_>>();
    let corporate_actions = imported
        .transaction_evidence()
        .iter()
        .filter(|record| {
            record.classification() == NormalizedPortfolioTransactionClass::CorporateAction
        })
        .map(|record| {
            json!({
                "recordId": record.logical_record_id().as_str(),
                "sourceReference": record.raw_source_reference().as_str(),
                "instrumentId": record.instrument_id().map(|value| value.to_string()),
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "accountId": account_id.to_string(),
        "disposition": match imported.disposition() {
            market_squawk_adapter_portfolio::ImportDisposition::Applied => "applied",
            market_squawk_adapter_portfolio::ImportDisposition::Replay => "replay",
        },
        "rawRecords": raw_records,
        "transactions": records,
        "reconciliationDiscrepancies": imported.discrepancies(),
        "corporateActionRequirements": corporate_actions,
        "resolutionRequirements": {
            "requiresGovernedInterpretation": imported.transaction_evidence().iter().any(|record| {
                matches!(
                    record.classification(),
                    NormalizedPortfolioTransactionClass::Trade
                        | NormalizedPortfolioTransactionClass::Income
                )
            }),
            "requiresServerHeldCorporateActionPlan": imported.transaction_evidence().iter().any(|record| {
                record.classification() == NormalizedPortfolioTransactionClass::CorporateAction
            }),
            "specificIdentificationUsesOnlyServerEnumeratedLots": true,
        },
    }))
}

fn eligible_lot_ids(
    prior: Option<&PublishedRevision>,
    evidence: &market_squawk_domain::NormalizedPortfolioTransactionEvidence,
) -> Vec<String> {
    let Some(prior) = prior else {
        return Vec::new();
    };
    let Some(instrument_id) = evidence.instrument_id() else {
        return Vec::new();
    };
    let Some(quantity) = evidence.quantity() else {
        return Vec::new();
    };
    let wanted_direction = if quantity.is_sign_negative() {
        market_squawk_portfolio::LotDirection::Long
    } else {
        market_squawk_portfolio::LotDirection::Short
    };
    prior
        .core
        .position(instrument_id)
        .map(|position| {
            position
                .lots()
                .iter()
                .filter(|lot| lot.direction() == wanted_direction)
                .map(|lot| lot.id().as_str().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

fn allowed_interpretations(
    evidence: &market_squawk_domain::NormalizedPortfolioTransactionEvidence,
    eligible_lot_ids: &[String],
) -> Vec<&'static str> {
    match evidence.classification() {
        NormalizedPortfolioTransactionClass::Trade => match (
            evidence
                .quantity()
                .is_some_and(|value| value.is_sign_positive()),
            evidence.lot_method(),
        ) {
            (true, Some(NormalizedPortfolioLotMethod::Fifo)) => vec!["buy", "buy_to_cover"],
            (false, Some(NormalizedPortfolioLotMethod::Fifo)) => vec!["sell", "sell_short"],
            (false, Some(NormalizedPortfolioLotMethod::SpecificIdentification)) => {
                if eligible_lot_ids.is_empty() {
                    vec!["sell_short_specific_identification"]
                } else {
                    vec![
                        "sell_specific_identification",
                        "sell_short_specific_identification",
                    ]
                }
            }
            (true, Some(NormalizedPortfolioLotMethod::SpecificIdentification)) => {
                if eligible_lot_ids.is_empty() {
                    vec!["buy_specific_identification"]
                } else {
                    vec![
                        "buy_specific_identification",
                        "buy_to_cover_specific_identification",
                    ]
                }
            }
            (true, Some(NormalizedPortfolioLotMethod::Lifo)) => {
                vec!["buy_lifo", "buy_to_cover_lifo"]
            }
            (false, Some(NormalizedPortfolioLotMethod::Lifo)) => {
                vec!["sell_lifo", "sell_short_lifo"]
            }
            (true, Some(NormalizedPortfolioLotMethod::AverageCost)) => {
                vec!["buy_average_cost", "buy_to_cover_average_cost"]
            }
            (false, Some(NormalizedPortfolioLotMethod::AverageCost)) => {
                vec!["sell_average_cost", "sell_short_average_cost"]
            }
            _ => Vec::new(),
        },
        NormalizedPortfolioTransactionClass::Income => {
            vec!["dividend", "interest", "withholding"]
        }
        NormalizedPortfolioTransactionClass::CashTransfer
        | NormalizedPortfolioTransactionClass::Fee
        | NormalizedPortfolioTransactionClass::CorporateAction => Vec::new(),
    }
}

const fn classification_name(value: NormalizedPortfolioTransactionClass) -> &'static str {
    match value {
        NormalizedPortfolioTransactionClass::Trade => "trade",
        NormalizedPortfolioTransactionClass::CashTransfer => "cash_transfer",
        NormalizedPortfolioTransactionClass::Income => "income",
        NormalizedPortfolioTransactionClass::Fee => "fee",
        NormalizedPortfolioTransactionClass::CorporateAction => "corporate_action",
    }
}

const fn lot_method_name(value: NormalizedPortfolioLotMethod) -> &'static str {
    match value {
        NormalizedPortfolioLotMethod::Fifo => "fifo",
        NormalizedPortfolioLotMethod::Lifo => "lifo",
        NormalizedPortfolioLotMethod::SpecificIdentification => "specific_identification",
        NormalizedPortfolioLotMethod::AverageCost => "average_cost",
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "revision publication binds all source and point-in-time evidence explicitly"
)]
fn build_core_revision(
    prior: Option<&PublishedRevision>,
    account: &AccountObservation,
    as_of: Timestamp,
    source_id: &SourceId,
    batch: &ExtractionBatch,
    artifact_sha256: [u8; 32],
    limits: PortfolioLimits,
    manifest_version: usize,
) -> Result<market_squawk_portfolio::PortfolioRevision, PortfolioApplicationServiceError> {
    let mut ledger = prior
        .map_or_else(
            || PortfolioLedger::try_new(account.account_id(), account.currency(), limits),
            |revision| revision.core.clone().into_ledger(),
        )
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let prior_cash = prior.map_or(Decimal::ZERO, |revision| {
        revision.account.cash_balance().amount()
    });
    let delta = account
        .cash_balance()
        .amount()
        .checked_sub(prior_cash)
        .ok_or(PortfolioApplicationServiceError::Publication)?;
    let mut entries = Vec::new();
    if !delta.is_zero() {
        let kind = if delta.is_sign_positive() {
            CashFlowKind::Deposit
        } else {
            CashFlowKind::Withdrawal
        };
        let transaction_id =
            SourceIdentifier::try_from(format!("account-snapshot-{}", hex(&artifact_sha256)))
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
        entries.push(
            LedgerEntry::try_new(
                account.account_id(),
                TransactionRevision::try_new(
                    transaction_id,
                    RevisionNumber::new(1)
                        .map_err(|_| PortfolioApplicationServiceError::Publication)?,
                    None,
                )
                .map_err(|_| PortfolioApplicationServiceError::Publication)?,
                account.as_of(),
                account.source_reference().clone(),
                LedgerEntryKind::CashFlow(
                    CashFlow::try_new(kind, Money::new(delta.abs(), account.currency()), None)
                        .map_err(|_| PortfolioApplicationServiceError::Publication)?,
                ),
            )
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        );
    }
    let digest = extraction_batch_digest(batch)
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    if digest.algorithm() != DigestAlgorithm::Sha256 {
        return Err(PortfolioApplicationServiceError::Publication);
    }
    let dataset = DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("portfolio-import")
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        u64::try_from(manifest_version)?,
        market_squawk_data::DatasetSchemaRegistry::local()
            .canonical_research_observations()
            .map_err(|_| PortfolioApplicationServiceError::Publication)?,
        Sha256Digest::new(digest.bytes()),
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let source = SourceIdentifier::try_from(source_id.as_str())
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let evidence = RevisionEvidence::try_new(
        as_of,
        dataset.clone(),
        Sha256Digest::new(digest.bytes()),
        Sha256Digest::new(artifact_sha256),
        vec![source],
        Vec::new(),
        None,
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    let valuation = ValuationSet::try_new(
        account.currency(),
        as_of,
        dataset,
        Sha256Digest::new(digest.bytes()),
        Vec::new(),
        Vec::new(),
        limits,
    )
    .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    ledger
        .try_apply(entries, None, valuation, evidence)
        .map_err(|_| PortfolioApplicationServiceError::Publication)
}

fn portfolio_limits(
    limits: PortfolioApplicationLimits,
) -> Result<PortfolioLimits, PortfolioApplicationServiceError> {
    PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: limits.max_accounts,
        max_instruments: limits.max_result_items,
        max_lots: limits.max_result_items,
        max_transactions: limits.max_result_items,
        max_factors: limits.max_result_items.min(16_384),
        max_scenarios: limits.max_result_items.min(16_384),
        max_history: limits.max_history_per_account,
        max_results: limits.max_result_items,
        max_retained_bytes: limits.max_retained_bytes,
    })
    .map_err(|_| PortfolioApplicationServiceError::InvalidLimits)
}

fn maximum_effective(
    batch: &ExtractionBatch,
) -> Result<Timestamp, PortfolioApplicationServiceError> {
    batch
        .records()
        .iter()
        .filter_map(|record| record.effective_time().exact_timestamp())
        .max()
        .ok_or(PortfolioApplicationServiceError::InvalidRequest)
}

fn maximum_conservative_availability(batch: &ExtractionBatch) -> Option<Timestamp> {
    if batch
        .records()
        .iter()
        .any(|record| record.available_at().is_none())
    {
        None
    } else {
        batch
            .records()
            .iter()
            .filter_map(|record| record.available_at())
            .max()
    }
}

fn import_result(
    revision: &PublishedRevision,
    disposition: &str,
    context: &RequestContext,
    requested_maximum_bytes: usize,
) -> Result<TypedToolResult, PortfolioApplicationServiceError> {
    read::mutation_result(
        json!({
            "accountId": revision.account.account_id().to_string(),
            "revisionId": hex(&revision.token().bytes()),
            "disposition": disposition,
            "sourceId": revision.source_id.as_str(),
            "effectiveAtUnixNanos": revision.effective_at.unix_nanos().to_string(),
            "availableAtUnixNanos": revision.available_at.map(|value| value.unix_nanos().to_string()),
            "artifactSha256": hex(&revision.artifact_sha256),
            "rawEvidenceRetained": true,
            "reconciliationDiscrepancies": revision.discrepancies.len()
        }),
        context,
        requested_maximum_bytes,
    )
}

fn source_namespace(key: &SourceKey) -> String {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/portfolio-source-namespace/v1");
    digest.update(key.source_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(key.metadata_revision.as_bytes());
    hex(&digest.finalize().into())
}

fn read_artifact(
    artifacts: &ArtifactRoot,
    reference: &str,
    maximum_bytes: usize,
) -> Result<Vec<u8>, PortfolioApplicationServiceError> {
    if maximum_bytes == 0 {
        return Err(PortfolioApplicationServiceError::InvalidLimits);
    }
    let file = artifacts
        .resolve(reference)
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?
        .open_read()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    let metadata = file
        .metadata()
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    if usize::try_from(metadata.len()).map_or(true, |bytes| bytes > maximum_bytes) {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    let maximum = u64::try_from(maximum_bytes)?
        .checked_add(1)
        .ok_or(PortfolioApplicationServiceError::ResourceExhausted)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(usize::try_from(metadata.len())?)
        .map_err(|_| PortfolioApplicationServiceError::ResourceExhausted)?;
    file.take(maximum)
        .read_to_end(&mut bytes)
        .map_err(|_| PortfolioApplicationServiceError::InvalidRequest)?;
    if bytes.len() > maximum_bytes {
        return Err(PortfolioApplicationServiceError::ResourceExhausted);
    }
    Ok(bytes)
}

fn persist_immutable(
    artifacts: &ArtifactRoot,
    reference: &str,
    bytes: &[u8],
) -> Result<(), PortfolioApplicationServiceError> {
    let resolved = artifacts
        .resolve(reference)
        .map_err(|_| PortfolioApplicationServiceError::Publication)?;
    match resolved.create_new() {
        Ok(mut file) => {
            file.write_all(bytes)
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
            file.sync_all()
                .map_err(|_| PortfolioApplicationServiceError::Publication)?;
            Ok(())
        }
        Err(ArtifactPathError::Io { source })
            if source.kind() == std::io::ErrorKind::AlreadyExists =>
        {
            let existing = read_artifact(artifacts, reference, bytes.len())?;
            if existing == bytes {
                Ok(())
            } else {
                Err(PortfolioApplicationServiceError::CorruptPublication)
            }
        }
        Err(_) => Err(PortfolioApplicationServiceError::Publication),
    }
}

pub(super) fn hex(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}
