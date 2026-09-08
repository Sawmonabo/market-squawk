//! Installed staged-input and approval boundary for governed portfolio imports.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_platform::{LocalAuthorityStateStore, LocalPaths};
use market_squawk_runtime::{ClientId, InputStager, InputTicketId, RuntimeIdentity};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::portfolio_application::{
    GovernedImportCommitReceipt, PortfolioApplicationService, PortfolioApplicationServiceError,
    PortfolioImportInterpretation, PortfolioImportPreview, ServerHeldPortfolioImportResolution,
};

const PREVIEW_IMPORT: &str = "Portfolio.PreviewStagedImport";
const APPROVE_IMPORT: &str = "Portfolio.ApproveStagedImport";
const COMMIT_IMPORT: &str = "Portfolio.CommitStagedImport";
const DISCARD_IMPORT: &str = "Portfolio.DiscardStagedImport";
const PORTFOLIO_IMPORT_MEDIA_TYPE: &str = "market-squawk.portfolio-extraction-batch.v1";
const APPROVAL_AUTHORITY_DIRECTORY: &str = "portfolio/import-approvals";
const APPROVAL_SCHEMA_VERSION: u16 = 1;
const GOVERNING_RULE_ID: &str = "market-squawk.portfolio-import-resolution.v1";
const MAX_APPROVALS: usize = 4_096;
const MAX_INTERPRETATIONS: usize = 100_000;
const MAX_RATIONALE_BYTES: usize = 4_096;

/// Installed transport-neutral adapter over one staged-input and portfolio authority pair.
#[derive(Clone)]
pub(super) struct InstalledPortfolioImportOperations {
    portfolio: Arc<PortfolioApplicationService>,
    inputs: Arc<InputStager>,
    runtime: RuntimeIdentity,
    approvals: Arc<Mutex<ApprovalAuthority>>,
    product_tokens: Arc<Mutex<ProductImportTokens>>,
}

impl InstalledPortfolioImportOperations {
    pub(super) fn try_new(
        paths: &LocalPaths,
        portfolio: Arc<PortfolioApplicationService>,
        inputs: Arc<InputStager>,
        runtime: RuntimeIdentity,
    ) -> Result<Self, ServiceError> {
        let control = paths
            .control_root()
            .map_err(|_error| ServiceError::Unavailable)?;
        control
            .try_clone_directory()
            .map_err(|_error| ServiceError::Unavailable)?
            .create_dir_all("portfolio")
            .map_err(|_error| ServiceError::Unavailable)?;
        let store =
            LocalAuthorityStateStore::try_open(control.root().join(APPROVAL_AUTHORITY_DIRECTORY))
                .map_err(|_error| ServiceError::Unavailable)?;
        let manifest = store
            .load()
            .map_err(|_error| ServiceError::Unavailable)?
            .map_or_else(
                || Ok(ApprovalManifest::empty()),
                |bytes| ApprovalManifest::decode(&bytes),
            )?;
        Ok(Self {
            portfolio,
            inputs,
            runtime,
            approvals: Arc::new(Mutex::new(ApprovalAuthority { store, manifest })),
            product_tokens: Arc::new(Mutex::new(ProductImportTokens::default())),
        })
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            PREVIEW_IMPORT | APPROVE_IMPORT | COMMIT_IMPORT | DISCARD_IMPORT
        )
    }

    /// Completes only transitions that had durably entered `Promoting` before restart.
    pub(super) fn recover_promoting(&self, context: &RequestContext) -> Result<(), ServiceError> {
        self.authorize(context)?;
        let pending = {
            let approvals = self
                .approvals
                .lock()
                .map_err(|_error| ServiceError::Unavailable)?;
            approvals
                .manifest
                .entries
                .iter()
                .filter(|entry| entry.phase == ApprovalPhase::Promoting)
                .cloned()
                .collect::<Vec<_>>()
        };
        for approval in pending {
            let recovery = self.portfolio.recover_promoting_import(
                &approval.preview_id,
                &approval.resolution()?,
                context,
            );
            match recovery {
                Ok(()) => {}
                Err(
                    PortfolioApplicationServiceError::InvalidRequest
                    | PortfolioApplicationServiceError::NotFound,
                ) => self
                    .portfolio
                    .resume_approved_import(
                        &approval.preview_id,
                        &approval.interpretations,
                        &approval.resolution()?,
                        context,
                    )
                    .map_err(|error| error.as_service_error())?,
                Err(error) => return Err(error.as_service_error()),
            }
            self.remove_approval(&approval.approval_id)?;
        }
        Ok(())
    }

    pub(super) async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.call_blocking(&request, &context))
            .await
            .map_err(|_error| ServiceError::Internal)?
    }

    fn call_blocking(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        self.authorize(context)?;
        ensure_live(context)?;
        let result = match request.name() {
            PREVIEW_IMPORT => self.preview(request, context)?,
            APPROVE_IMPORT => self.approve(request, context)?,
            COMMIT_IMPORT => self.commit(request, context)?,
            DISCARD_IMPORT => self.discard(request, context)?,
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            result,
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    fn preview(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let input: PreviewRequest = decode(request.arguments())?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let client = ClientId::try_from_uuid(origin.client_id())
            .map_err(|_error| ServiceError::Unauthorized)?;
        let ticket_id = InputTicketId::try_from_uuid(input.input_ticket_id)
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let media_type = SourceIdentifier::try_from(PORTFOLIO_IMPORT_MEDIA_TYPE)
            .map_err(|_error| ServiceError::Unavailable)?;
        let now = current_timestamp()?;
        let claimed = self
            .inputs
            .claim(ticket_id, client, &media_type, now)
            .map_err(|_error| ServiceError::Unauthorized)?;
        let bytes = claimed
            .read_verified(
                u64::try_from(self.portfolio.maximum_staged_import_bytes())
                    .map_err(|_error| ServiceError::Unavailable)?,
            )
            .map_err(|_error| ServiceError::InvalidRequest)?;
        let mut preview = self
            .portfolio
            .prepare_staged_import(
                input.account_id,
                claimed.ticket().id().as_uuid().to_string(),
                &bytes,
                context,
            )
            .map_err(|error| error.as_service_error())?;
        self.product_tokens
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .productize_preview(&mut preview)?;
        Ok(preview.result)
    }

    fn approve(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let input: ApproveRequest = decode(request.arguments())?;
        let binding = self
            .product_tokens
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .preview(input.review_token)?
            .clone();
        let preview = self
            .portfolio
            .prepared_import_preview(&binding.preview_id, context)
            .map_err(|error| error.as_service_error())?;
        if preview.preview_id != binding.preview_id
            || preview.preview_digest != binding.preview_digest
        {
            return Err(ServiceError::InvalidRequest);
        }
        let ResolvedImportInterpretations {
            interpretations,
            specific_lot_ids,
        } = resolve_interpretations(&preview, &binding, input.interpretations)?;
        if requires_corporate_action_plan(&preview)? {
            // Corporate-action resolution is owned by the server's point-in-time plan authority;
            // this adapter never accepts a client-authored plan or raw plan bytes.
            return Err(ServiceError::Unavailable);
        }
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let actor_id = origin.client_id().to_string();
        let workspace_id = origin.workspace_id().to_string();
        let approved_at = current_timestamp()?;
        let mut approvals = self
            .approvals
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        if let Some(existing) = approvals
            .manifest
            .entries
            .iter()
            .find(|entry| entry.preview_id == binding.preview_id)
        {
            if existing.workspace_id == workspace_id
                && existing.actor_id == actor_id
                && existing.interpretations == interpretations
                && existing.specific_lot_ids == specific_lot_ids
            {
                let approval_token = self
                    .product_tokens
                    .lock()
                    .map_err(|_error| ServiceError::Unavailable)?
                    .approval_token(&existing.approval_id, binding.review_token)?;
                return Ok(approval_value(
                    approval_token,
                    binding.review_token,
                    existing,
                ));
            }
            return Err(ServiceError::InvalidRequest);
        }
        if approvals.manifest.entries.len() >= MAX_APPROVALS {
            return Err(ServiceError::ResourceExhausted);
        }
        let approval_id = Uuid::new_v4().to_string();
        let authorization_handle_digest = authorization_digest(
            &approval_id,
            &binding.preview_id,
            &workspace_id,
            &actor_id,
            approved_at.unix_nanos(),
            &interpretations,
            &specific_lot_ids,
        )?;
        let entry = ApprovalEntry {
            approval_id,
            preview_id: binding.preview_id,
            preview_digest: preview.preview_digest,
            workspace_id,
            actor_id: actor_id.clone(),
            interpretations,
            specific_lot_ids,
            receipt: GovernedImportCommitReceipt {
                actor_id,
                committed_at_unix_nanos: approved_at.unix_nanos().to_string(),
                governing_rule_id: GOVERNING_RULE_ID.to_owned(),
                authorization_handle_digest,
            },
            phase: ApprovalPhase::Approved,
        };
        approvals.append(entry.clone())?;
        let approval_token = self
            .product_tokens
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .approval_token(&entry.approval_id, binding.review_token)?;
        Ok(approval_value(approval_token, binding.review_token, &entry))
    }

    fn commit(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let input: ApprovalRequest = decode(request.arguments())?;
        let product_approval = self
            .product_tokens
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .approval(input.approval_token)?
            .clone();
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let approval = {
            let mut approvals = self
                .approvals
                .lock()
                .map_err(|_error| ServiceError::Unavailable)?;
            let entry = approvals
                .manifest
                .entries
                .iter()
                .find(|entry| entry.approval_id == product_approval.approval_id)
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if entry.workspace_id != origin.workspace_id().to_string()
                || entry.actor_id != origin.client_id().to_string()
            {
                return Err(ServiceError::Unauthorized);
            }
            approvals.mark_promoting(&entry.approval_id)?
        };
        let resolution = approval.resolution()?;
        let commit = self.portfolio.commit_prepared_import(
            &approval.preview_id,
            &approval.interpretations,
            &resolution,
            context,
        );
        match commit {
            Ok(()) => {}
            Err(PortfolioApplicationServiceError::NotFound) => self
                .portfolio
                .resume_approved_import(
                    &approval.preview_id,
                    &approval.interpretations,
                    &resolution,
                    context,
                )
                .map_err(|error| error.as_service_error())?,
            Err(error) => return Err(error.as_service_error()),
        }
        self.remove_approval(&approval.approval_id)?;
        self.product_tokens
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .remove_completed(
                product_approval.approval_token,
                product_approval.review_token,
            );
        Ok(json!({"accepted": true}))
    }

    fn discard(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Value, ServiceError> {
        let input: PreviewIdentityRequest = decode(request.arguments())?;
        let binding = self
            .product_tokens
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .preview(input.review_token)?
            .clone();
        let approvals = self
            .approvals
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?;
        if approvals
            .manifest
            .entries
            .iter()
            .any(|entry| entry.preview_id == binding.preview_id)
        {
            return Err(ServiceError::InvalidRequest);
        }
        drop(approvals);
        self.portfolio
            .discard_prepared_import(&binding.preview_id, context)
            .map_err(|error| error.as_service_error())?;
        self.product_tokens
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .remove_preview(binding.review_token);
        Ok(json!({"reviewToken": binding.review_token, "status": "discarded"}))
    }

    fn authorize(&self, context: &RequestContext) -> Result<(), ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        if origin.workspace_id() != self.runtime.workspace_id().as_uuid() {
            return Err(ServiceError::Unauthorized);
        }
        Ok(())
    }

    fn remove_approval(&self, approval_id: &str) -> Result<(), ServiceError> {
        self.approvals
            .lock()
            .map_err(|_error| ServiceError::Unavailable)?
            .remove(approval_id)
    }
}

impl fmt::Debug for InstalledPortfolioImportOperations {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledPortfolioImportOperations")
            .field("portfolio", &"[PORTFOLIO AUTHORITY]")
            .field("inputs", &"[ONE-SHOT INPUT STAGER]")
            .field("runtime", &self.runtime)
            .field("approvals", &"[DURABLE APPROVAL AUTHORITY]")
            .field("product_tokens", &"[SESSION PRODUCT TOKENS]")
            .finish()
    }
}

struct ApprovalAuthority {
    store: LocalAuthorityStateStore,
    manifest: ApprovalManifest,
}

#[derive(Default)]
struct ProductImportTokens {
    previews: Vec<ProductPreviewBinding>,
    approvals: Vec<ProductApprovalBinding>,
}

#[derive(Clone)]
struct ProductPreviewBinding {
    review_token: Uuid,
    preview_id: String,
    preview_digest: [u8; 32],
    records: Vec<ProductRecordBinding>,
}

#[derive(Clone)]
struct ProductRecordBinding {
    record_token: Uuid,
    record_id: String,
}

#[derive(Clone)]
struct ProductApprovalBinding {
    approval_token: Uuid,
    approval_id: String,
    review_token: Uuid,
}

impl ProductImportTokens {
    fn productize_preview(
        &mut self,
        preview: &mut PortfolioImportPreview,
    ) -> Result<(), ServiceError> {
        if let Some(binding) = self
            .previews
            .iter()
            .find(|binding| binding.preview_id == preview.preview_id)
            .cloned()
        {
            return apply_product_preview_tokens(preview, &binding);
        }
        if self.previews.len() >= MAX_APPROVALS {
            return Err(ServiceError::ResourceExhausted);
        }
        let result_transactions = preview
            .result
            .get("transactions")
            .and_then(Value::as_array)
            .ok_or(ServiceError::Internal)?;
        let review_transactions = preview
            .review
            .get("transactions")
            .and_then(Value::as_array)
            .ok_or(ServiceError::Internal)?;
        if result_transactions.len() != review_transactions.len()
            || review_transactions.len() > MAX_INTERPRETATIONS
        {
            return Err(ServiceError::Internal);
        }
        let mut records = Vec::new();
        records
            .try_reserve_exact(review_transactions.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for transaction in review_transactions {
            let record_id = transaction
                .get("recordId")
                .and_then(Value::as_str)
                .ok_or(ServiceError::Internal)?;
            records.push(ProductRecordBinding {
                record_token: Uuid::new_v4(),
                record_id: record_id.to_owned(),
            });
        }
        let binding = ProductPreviewBinding {
            review_token: Uuid::new_v4(),
            preview_id: preview.preview_id.clone(),
            preview_digest: preview.preview_digest,
            records,
        };
        apply_product_preview_tokens(preview, &binding)?;
        self.previews
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        self.previews.push(binding);
        Ok(())
    }

    fn preview(&self, token: Uuid) -> Result<&ProductPreviewBinding, ServiceError> {
        self.previews
            .iter()
            .find(|binding| binding.review_token == token)
            .ok_or(ServiceError::NotFound)
    }

    fn approval_token(
        &mut self,
        approval_id: &str,
        review_token: Uuid,
    ) -> Result<Uuid, ServiceError> {
        if let Some(binding) = self
            .approvals
            .iter()
            .find(|binding| binding.approval_id == approval_id)
        {
            return Ok(binding.approval_token);
        }
        if self.approvals.len() >= MAX_APPROVALS {
            return Err(ServiceError::ResourceExhausted);
        }
        let approval_token = Uuid::new_v4();
        self.approvals
            .try_reserve(1)
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        self.approvals.push(ProductApprovalBinding {
            approval_token,
            approval_id: approval_id.to_owned(),
            review_token,
        });
        Ok(approval_token)
    }

    fn approval(&self, token: Uuid) -> Result<&ProductApprovalBinding, ServiceError> {
        self.approvals
            .iter()
            .find(|binding| binding.approval_token == token)
            .ok_or(ServiceError::NotFound)
    }

    fn remove_completed(&mut self, approval_token: Uuid, review_token: Uuid) {
        self.approvals
            .retain(|binding| binding.approval_token != approval_token);
        self.remove_preview(review_token);
    }

    fn remove_preview(&mut self, review_token: Uuid) {
        self.previews
            .retain(|binding| binding.review_token != review_token);
    }
}

fn apply_product_preview_tokens(
    preview: &mut PortfolioImportPreview,
    binding: &ProductPreviewBinding,
) -> Result<(), ServiceError> {
    preview
        .result
        .as_object_mut()
        .ok_or(ServiceError::Internal)?
        .insert("reviewToken".to_owned(), json!(binding.review_token));
    let result_transactions = preview
        .result
        .get_mut("transactions")
        .and_then(Value::as_array_mut)
        .ok_or(ServiceError::Internal)?;
    let review_transactions = preview
        .review
        .get_mut("transactions")
        .and_then(Value::as_array_mut)
        .ok_or(ServiceError::Internal)?;
    if result_transactions.len() != binding.records.len()
        || review_transactions.len() != binding.records.len()
    {
        return Err(ServiceError::Internal);
    }
    for ((result, review), record) in result_transactions
        .iter_mut()
        .zip(review_transactions.iter_mut())
        .zip(&binding.records)
    {
        let review_record_id = review
            .get("recordId")
            .and_then(Value::as_str)
            .ok_or(ServiceError::Internal)?;
        if review_record_id != record.record_id {
            return Err(ServiceError::Internal);
        }
        result
            .as_object_mut()
            .ok_or(ServiceError::Internal)?
            .insert("recordToken".to_owned(), json!(record.record_token));
        review
            .as_object_mut()
            .ok_or(ServiceError::Internal)?
            .insert("recordToken".to_owned(), json!(record.record_token));
    }
    Ok(())
}

impl ApprovalAuthority {
    fn append(&mut self, entry: ApprovalEntry) -> Result<(), ServiceError> {
        if self
            .manifest
            .entries
            .iter()
            .any(|existing| existing.approval_id == entry.approval_id)
        {
            return Err(ServiceError::InvalidRequest);
        }
        let mut candidate = self.manifest.clone();
        candidate.entries.push(entry);
        candidate
            .entries
            .sort_by(|left, right| left.approval_id.cmp(&right.approval_id));
        self.persist(candidate)
    }

    fn mark_promoting(&mut self, approval_id: &str) -> Result<ApprovalEntry, ServiceError> {
        let mut candidate = self.manifest.clone();
        let entry = candidate
            .entries
            .iter_mut()
            .find(|entry| entry.approval_id == approval_id)
            .ok_or(ServiceError::NotFound)?;
        entry.phase = ApprovalPhase::Promoting;
        let retained = entry.clone();
        self.persist(candidate)?;
        Ok(retained)
    }

    fn remove(&mut self, approval_id: &str) -> Result<(), ServiceError> {
        let mut candidate = self.manifest.clone();
        let original = candidate.entries.len();
        candidate
            .entries
            .retain(|entry| entry.approval_id != approval_id);
        if candidate.entries.len() == original {
            return Err(ServiceError::NotFound);
        }
        self.persist(candidate)
    }

    fn persist(&mut self, candidate: ApprovalManifest) -> Result<(), ServiceError> {
        let bytes = candidate.encode()?;
        self.store
            .store(&bytes)
            .map_err(|_error| ServiceError::Unavailable)?;
        self.manifest = candidate;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ApprovalManifest {
    schema_version: u16,
    entries: Vec<ApprovalEntry>,
}

impl ApprovalManifest {
    const fn empty() -> Self {
        Self {
            schema_version: APPROVAL_SCHEMA_VERSION,
            entries: Vec::new(),
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self, ServiceError> {
        let manifest: Self =
            serde_json::from_slice(bytes).map_err(|_error| ServiceError::Internal)?;
        if manifest.schema_version != APPROVAL_SCHEMA_VERSION
            || manifest.entries.len() > MAX_APPROVALS
        {
            return Err(ServiceError::Internal);
        }
        let mut ids = BTreeSet::new();
        for entry in &manifest.entries {
            entry.validate()?;
            if !ids.insert(entry.approval_id.as_str()) {
                return Err(ServiceError::Internal);
            }
        }
        Ok(manifest)
    }

    fn encode(&self) -> Result<Vec<u8>, ServiceError> {
        serde_json::to_vec(self).map_err(|_error| ServiceError::Internal)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ApprovalEntry {
    approval_id: String,
    preview_id: String,
    preview_digest: [u8; 32],
    workspace_id: String,
    actor_id: String,
    interpretations: Vec<PortfolioImportInterpretation>,
    specific_lot_ids: BTreeMap<String, Vec<String>>,
    receipt: GovernedImportCommitReceipt,
    phase: ApprovalPhase,
}

impl ApprovalEntry {
    fn validate(&self) -> Result<(), ServiceError> {
        Uuid::parse_str(&self.approval_id).map_err(|_error| ServiceError::Internal)?;
        Uuid::parse_str(&self.workspace_id).map_err(|_error| ServiceError::Internal)?;
        Uuid::parse_str(&self.actor_id).map_err(|_error| ServiceError::Internal)?;
        if self.preview_id != hex(&self.preview_digest)
            || self.interpretations.len() > MAX_INTERPRETATIONS
        {
            return Err(ServiceError::Internal);
        }
        self.receipt
            .validate()
            .map_err(|_error| ServiceError::Internal)?;
        let expected = authorization_digest(
            &self.approval_id,
            &self.preview_id,
            &self.workspace_id,
            &self.actor_id,
            self.receipt
                .committed_at_unix_nanos
                .parse()
                .map_err(|_error| ServiceError::Internal)?,
            &self.interpretations,
            &self.specific_lot_ids,
        )?;
        if expected != self.receipt.authorization_handle_digest
            || self.receipt.actor_id != self.actor_id
            || self.receipt.governing_rule_id != GOVERNING_RULE_ID
        {
            return Err(ServiceError::Internal);
        }
        Ok(())
    }

    fn resolution(&self) -> Result<ServerHeldPortfolioImportResolution, ServiceError> {
        let specific_lot_ids = self
            .specific_lot_ids
            .iter()
            .map(|(record_id, lot_ids)| {
                let lot_ids = lot_ids
                    .iter()
                    .cloned()
                    .map(SourceIdentifier::try_from)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_error| ServiceError::Internal)?;
                Ok((record_id.clone(), lot_ids))
            })
            .collect::<Result<BTreeMap<_, _>, ServiceError>>()?;
        Ok(ServerHeldPortfolioImportResolution {
            receipt: self.receipt.clone(),
            corporate_action_plan: None,
            specific_lot_ids,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ApprovalPhase {
    Approved,
    Promoting,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PreviewRequest {
    account_id: market_squawk_domain::AccountId,
    input_ticket_id: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ApproveRequest {
    review_token: Uuid,
    interpretations: Vec<InterpretationSelection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InterpretationSelection {
    record_token: Uuid,
    interpretation: String,
    rationale: String,
    #[serde(default)]
    selected_lot_indexes: Vec<usize>,
}

struct ResolvedImportInterpretations {
    interpretations: Vec<PortfolioImportInterpretation>,
    specific_lot_ids: BTreeMap<String, Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ApprovalRequest {
    approval_token: Uuid,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PreviewIdentityRequest {
    review_token: Uuid,
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(super::business_arguments(arguments)))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn resolve_interpretations(
    preview: &PortfolioImportPreview,
    binding: &ProductPreviewBinding,
    selected: Vec<InterpretationSelection>,
) -> Result<ResolvedImportInterpretations, ServiceError> {
    let transactions = preview
        .review
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or(ServiceError::Internal)?;
    let resolvable = transactions
        .iter()
        .filter(|transaction| {
            matches!(
                transaction.get("classification").and_then(Value::as_str),
                Some("trade" | "income")
            )
        })
        .collect::<Vec<_>>();
    if selected.len() != resolvable.len() || selected.len() > MAX_INTERPRETATIONS {
        return Err(ServiceError::InvalidRequest);
    }
    let mut selections = selected
        .into_iter()
        .map(|selection| (selection.record_token, selection))
        .collect::<BTreeMap<_, _>>();
    if selections.len() != resolvable.len() {
        return Err(ServiceError::InvalidRequest);
    }
    let mut interpretations = Vec::new();
    let mut specific_lot_ids = BTreeMap::new();
    interpretations
        .try_reserve_exact(resolvable.len())
        .map_err(|_error| ServiceError::ResourceExhausted)?;
    for transaction in resolvable {
        let record_id = transaction
            .get("recordId")
            .and_then(Value::as_str)
            .ok_or(ServiceError::Internal)?;
        let record_token = binding
            .records
            .iter()
            .find(|binding| binding.record_id == record_id)
            .map(|binding| binding.record_token)
            .ok_or(ServiceError::Internal)?;
        let selection = selections
            .remove(&record_token)
            .ok_or(ServiceError::InvalidRequest)?;
        if selection.interpretation.is_empty()
            || selection.interpretation.len() > 512
            || selection.rationale.trim().is_empty()
            || selection.rationale.len() > MAX_RATIONALE_BYTES
            || !transaction
                .get("allowedInterpretations")
                .and_then(Value::as_array)
                .is_some_and(|allowed| {
                    allowed
                        .iter()
                        .any(|value| value.as_str() == Some(selection.interpretation.as_str()))
                })
        {
            return Err(ServiceError::InvalidRequest);
        }
        let eligible = transaction
            .get("eligibleOpeningLotIds")
            .and_then(Value::as_array)
            .ok_or(ServiceError::Internal)?;
        if selection.selected_lot_indexes.len() > eligible.len() {
            return Err(ServiceError::InvalidRequest);
        }
        let mut seen = BTreeSet::new();
        let lots = selection
            .selected_lot_indexes
            .iter()
            .map(|index| {
                if !seen.insert(*index) {
                    return Err(ServiceError::InvalidRequest);
                }
                eligible
                    .get(*index)
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .ok_or(ServiceError::InvalidRequest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let disposes_existing_lots = matches!(
            selection.interpretation.as_str(),
            "sell_specific_identification" | "buy_to_cover_specific_identification"
        );
        if disposes_existing_lots != !lots.is_empty()
            || (!selection
                .interpretation
                .ends_with("specific_identification")
                && !lots.is_empty())
        {
            return Err(ServiceError::InvalidRequest);
        }
        if !lots.is_empty() {
            specific_lot_ids.insert(record_id.to_owned(), lots);
        }
        interpretations.push(PortfolioImportInterpretation {
            record_id: record_id.to_owned(),
            interpretation: selection.interpretation,
            rationale: selection.rationale,
        });
    }
    Ok(ResolvedImportInterpretations {
        interpretations,
        specific_lot_ids,
    })
}

fn requires_corporate_action_plan(preview: &PortfolioImportPreview) -> Result<bool, ServiceError> {
    preview
        .review
        .get("resolutionRequirements")
        .and_then(|value| value.get("requiresServerHeldCorporateActionPlan"))
        .and_then(Value::as_bool)
        .ok_or(ServiceError::Internal)
}

fn authorization_digest(
    approval_id: &str,
    preview_id: &str,
    workspace_id: &str,
    actor_id: &str,
    approved_at_unix_nanos: i64,
    interpretations: &[PortfolioImportInterpretation],
    specific_lot_ids: &BTreeMap<String, Vec<String>>,
) -> Result<[u8; 32], ServiceError> {
    let canonical = serde_json::to_vec(&json!({
        "approvalId": approval_id,
        "previewId": preview_id,
        "workspaceId": workspace_id,
        "actorId": actor_id,
        "approvedAtUnixNanos": approved_at_unix_nanos.to_string(),
        "governingRuleId": GOVERNING_RULE_ID,
        "interpretations": interpretations,
        "specificLotIds": specific_lot_ids,
    }))
    .map_err(|_error| ServiceError::Internal)?;
    let mut hasher = Sha256::new();
    hasher.update(b"market-squawk/portfolio-import-authorization/v1\0");
    hasher.update(canonical);
    Ok(hasher.finalize().into())
}

fn approval_value(approval_token: Uuid, review_token: Uuid, entry: &ApprovalEntry) -> Value {
    json!({
        "approvalToken": approval_token,
        "reviewToken": review_token,
        "status": match entry.phase {
            ApprovalPhase::Approved => "approved",
            ApprovalPhase::Promoting => "promoting",
        },
    })
}

fn current_timestamp() -> Result<Timestamp, ServiceError> {
    super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if std::time::Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn hex(bytes: &[u8; 32]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(TABLE[usize::from(byte >> 4)]));
        output.push(char::from(TABLE[usize::from(byte & 0x0f)]));
    }
    output
}
