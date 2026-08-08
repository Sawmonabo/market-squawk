//! Closed service surface for application-owned candidate-dossier assembly.

use std::sync::Arc;

use market_squawk_decisions::CandidateId;
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::application::decision::{
    DecisionApplication, DossierEvidenceInventory, DossierEvidenceSelection,
    DossierPreparationDraft, DossierPreparationError, DossierPreparationFence,
    DossierPreparationReceipt, PreparedDossierPreview,
};

use super::append_outcome_value;

pub(super) const GET_DOSSIER_PREPARATION: &str = "Decision.GetDossierPreparation";
pub(super) const PREPARE_DOSSIER: &str = "Decision.PrepareDossier";
pub(super) const CREATE_DOSSIER: &str = "Decision.CreateDossier";

/// Runtime-fenced dossier preparation over the sole durable decision authority.
pub(super) struct DossierPreparationOperations {
    decisions: Arc<DecisionApplication>,
    runtime: RuntimeIdentity,
}

impl DossierPreparationOperations {
    pub(super) const fn new(decisions: Arc<DecisionApplication>, runtime: RuntimeIdentity) -> Self {
        Self { decisions, runtime }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            GET_DOSSIER_PREPARATION | PREPARE_DOSSIER | CREATE_DOSSIER
        )
    }

    pub(super) fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let now = super::super::runtime::current_timestamp()
            .map_err(|_error| ServiceError::Unavailable)?;
        let arguments = mutation_arguments(request.arguments());
        let content = match request.name() {
            GET_DOSSIER_PREPARATION => {
                let input: CandidateRequest = decode(&arguments)?;
                let inventory = self
                    .decisions
                    .dossier_evidence_inventory(
                        &CandidateId::try_new(input.candidate_id)
                            .map_err(|_error| ServiceError::InvalidRequest)?,
                    )
                    .map_err(map_preparation)?;
                inventory_value(&inventory)
            }
            PREPARE_DOSSIER => {
                let input: PrepareRequest = decode(&arguments)?;
                let preview = self
                    .decisions
                    .prepare_dossier(self.fence(context)?, input.draft.decode()?, now)
                    .map_err(map_preparation)?;
                preview_value(&preview)
            }
            CREATE_DOSSIER => {
                let input: CommitRequest = decode(&arguments)?;
                let receipt =
                    DossierPreparationReceipt::parse(&input.receipt_id).map_err(map_preparation)?;
                let outcome = self
                    .decisions
                    .consume_dossier_preparation(receipt, self.fence(context)?, now)
                    .map_err(map_preparation)?;
                append_outcome_value(outcome)
            }
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            1,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    fn fence(&self, context: &RequestContext) -> Result<DossierPreparationFence, ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        DossierPreparationFence::try_new(
            origin,
            self.runtime.workspace_id(),
            self.runtime.service_generation(),
        )
        .map_err(map_preparation)
    }
}

impl std::fmt::Debug for DossierPreparationOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DossierPreparationOperations")
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CandidateRequest {
    candidate_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareRequest {
    draft: DossierDraftInput,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommitRequest {
    receipt_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DossierDraftInput {
    candidate_id: String,
    evidence: Vec<DossierEvidenceSelectionInput>,
}

impl DossierDraftInput {
    fn decode(self) -> Result<DossierPreparationDraft, ServiceError> {
        Ok(DossierPreparationDraft {
            candidate_id: CandidateId::try_new(self.candidate_id)
                .map_err(|_error| ServiceError::InvalidRequest)?,
            evidence: self.evidence.into_iter().map(Into::into).collect(),
        })
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DossierEvidenceSelectionInput {
    Candidate,
    Dataset,
    Universe,
    PortfolioImpact,
}

impl From<DossierEvidenceSelectionInput> for DossierEvidenceSelection {
    fn from(value: DossierEvidenceSelectionInput) -> Self {
        match value {
            DossierEvidenceSelectionInput::Candidate => Self::Candidate,
            DossierEvidenceSelectionInput::Dataset => Self::Dataset,
            DossierEvidenceSelectionInput::Universe => Self::Universe,
            DossierEvidenceSelectionInput::PortfolioImpact => Self::PortfolioImpact,
        }
    }
}

fn inventory_value(value: &DossierEvidenceInventory) -> Value {
    json!({
        "candidateId": value.candidate_id.as_str(),
        "screenRunId": value.screen_run_id.as_str(),
        "instrumentId": value.instrument_id,
        "selectedAt": value.selected_at,
        "requiredEvidence": ["candidate", "dataset", "universe"],
        "portfolioImpactAvailable": value.portfolio_impact_available,
    })
}

fn preview_value(value: &PreparedDossierPreview) -> Value {
    json!({
        "receiptId": value.receipt.to_string(),
        "dossierId": value.dossier_id.as_str(),
        "candidateId": value.candidate_id.as_str(),
        "screenRunId": value.screen_run_id.as_str(),
        "instrumentId": value.instrument_id,
        "evidence": value.evidence.iter().copied().map(evidence_name).collect::<Vec<_>>(),
        "assembledAt": value.assembled_at,
        "receiptExpiresAt": value.receipt_expires_at,
    })
}

const fn evidence_name(value: DossierEvidenceSelection) -> &'static str {
    match value {
        DossierEvidenceSelection::Candidate => "candidate",
        DossierEvidenceSelection::Dataset => "dataset",
        DossierEvidenceSelection::Universe => "universe",
        DossierEvidenceSelection::PortfolioImpact => "portfolio_impact",
    }
}

fn mutation_arguments(arguments: &Map<String, Value>) -> Map<String, Value> {
    let mut arguments = arguments.clone();
    arguments.remove("confirmation");
    arguments
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
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

fn map_preparation(error: DossierPreparationError) -> ServiceError {
    match error {
        DossierPreparationError::InvalidRequest
        | DossierPreparationError::Conflict
        | DossierPreparationError::Expired => ServiceError::InvalidRequest,
        DossierPreparationError::NotFound => ServiceError::NotFound,
        DossierPreparationError::FenceMismatch => ServiceError::Unauthorized,
        DossierPreparationError::Capacity => ServiceError::ResourceExhausted,
        DossierPreparationError::Application(error) => super::map_application(error),
    }
}
