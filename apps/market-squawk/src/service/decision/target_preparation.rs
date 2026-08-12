//! Closed transport adapter for server-owned investment-target preparation.

use std::sync::Arc;

use market_squawk_decisions::DossierId;
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::application::decision::{
    DecisionApplication,
    target_preparation::{
        PreparedTargetPreview, TargetAssumptionDraft, TargetAssumptionEvidenceSelection,
        TargetEvidenceInventory, TargetEvidenceSelection, TargetHorizon, TargetIntent,
        TargetPreparationCommitKind, TargetPreparationDraft, TargetPreparationError,
        TargetPreparationFence, TargetPreparationOperation, TargetPreparationReceipt,
        TargetPriceDraft, TargetReferenceMarkSelector,
    },
};
use crate::portfolio_application::PortfolioFairValueReadCapability;

use super::{MoneyInput, TargetMethodInput, append_outcome_value, money_value, target_id};

pub(super) const GET_TARGET_PREPARATION: &str = "Decision.GetTargetPreparation";
pub(super) const PREPARE_TARGET: &str = "Decision.PrepareTargetSet";
pub(super) const CREATE_TARGET: &str = "Decision.CreateTargetSet";
pub(super) const REEVALUATE_TARGET: &str = "Decision.ReevaluateTargetSet";

/// Runtime-fenced target preparation over the sole durable decision authority.
pub(super) struct TargetPreparationOperations {
    decisions: Arc<DecisionApplication>,
    portfolio: PortfolioFairValueReadCapability,
    runtime: RuntimeIdentity,
}

impl TargetPreparationOperations {
    pub(super) const fn new(
        decisions: Arc<DecisionApplication>,
        portfolio: PortfolioFairValueReadCapability,
        runtime: RuntimeIdentity,
    ) -> Self {
        Self {
            decisions,
            portfolio,
            runtime,
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            GET_TARGET_PREPARATION | PREPARE_TARGET | CREATE_TARGET | REEVALUATE_TARGET
        )
    }

    pub(super) fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let arguments = super::super::business_arguments(request.arguments());
        let now = super::super::runtime::current_timestamp()
            .map_err(|_error| ServiceError::Unavailable)?;
        if matches!(request.name(), GET_TARGET_PREPARATION | PREPARE_TARGET) {
            self.refresh_reference_marks(context, now)?;
        }
        let (content, item_count) = match request.name() {
            GET_TARGET_PREPARATION => {
                let input: InventoryRequest = decode(&arguments)?;
                let inventory = self
                    .decisions
                    .target_evidence_inventory(
                        &DossierId::try_new(input.dossier_id)
                            .map_err(|_error| ServiceError::InvalidRequest)?,
                        now,
                    )
                    .map_err(map_preparation)?;
                let item_count = inventory.reference_marks.len();
                (inventory_value(&inventory), item_count)
            }
            PREPARE_TARGET => {
                let input: PrepareRequest = decode(&arguments)?;
                let fence = self.fence(context)?;
                let preview = self
                    .decisions
                    .prepare_target(fence, input.draft.decode()?, now)
                    .map_err(map_preparation)?;
                (preview_value(&preview), 1)
            }
            CREATE_TARGET | REEVALUATE_TARGET => {
                let input: CommitRequest = decode(&arguments)?;
                let receipt =
                    TargetPreparationReceipt::parse(&input.receipt_id).map_err(map_preparation)?;
                let expected = if request.name() == CREATE_TARGET {
                    TargetPreparationCommitKind::Create
                } else {
                    TargetPreparationCommitKind::Reevaluate
                };
                let outcome = self
                    .decisions
                    .consume_target_preparation(receipt, self.fence(context)?, expected, now)
                    .map_err(map_preparation)?;
                (append_outcome_value(outcome), 1)
            }
            _ => return Err(ServiceError::NotFound),
        };
        ensure_live(context)?;
        TypedToolResult::try_new(
            content,
            item_count,
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }

    fn fence(&self, context: &RequestContext) -> Result<TargetPreparationFence, ServiceError> {
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        TargetPreparationFence::try_new(
            origin,
            self.runtime.workspace_id(),
            self.runtime.service_generation(),
        )
        .map_err(map_preparation)
    }

    fn refresh_reference_marks(
        &self,
        context: &RequestContext,
        now: market_squawk_domain::Timestamp,
    ) -> Result<(), ServiceError> {
        let prices = self
            .portfolio
            .current_price_evidence(context.deadline(), context.cancellation())
            .map_err(|error| error.as_service_error())?;
        for price in &prices {
            self.decisions
                .admit_target_reference_mark(
                    price,
                    market_squawk_domain::DataQuality::Indicative,
                    now,
                )
                .map_err(map_preparation)?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for TargetPreparationOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TargetPreparationOperations")
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .field("portfolio", &"[IMMUTABLE PORTFOLIO EVIDENCE]")
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct InventoryRequest {
    dossier_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PrepareRequest {
    draft: TargetDraftInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CommitRequest {
    receipt_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum TargetOperationInput {
    Create,
    Reevaluate { target_id: String },
}

impl TargetOperationInput {
    fn decode(self) -> Result<TargetPreparationOperation, ServiceError> {
        match self {
            Self::Create => Ok(TargetPreparationOperation::Create),
            Self::Reevaluate { target_id: value } => Ok(TargetPreparationOperation::Reevaluate {
                target_id: target_id(&value)?,
            }),
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TargetIntentInput {
    Buy,
    Sell,
    Hold,
}

impl From<TargetIntentInput> for TargetIntent {
    fn from(value: TargetIntentInput) -> Self {
        match value {
            TargetIntentInput::Buy => Self::Buy,
            TargetIntentInput::Sell => Self::Sell,
            TargetIntentInput::Hold => Self::Hold,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TargetHorizonInput {
    Quarter,
    Year,
    ThreeYears,
}

impl From<TargetHorizonInput> for TargetHorizon {
    fn from(value: TargetHorizonInput) -> Self {
        match value {
            TargetHorizonInput::Quarter => Self::Quarter,
            TargetHorizonInput::Year => Self::Year,
            TargetHorizonInput::ThreeYears => Self::ThreeYears,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TargetPriceInput {
    downside: MoneyInput,
    add: MoneyInput,
    entry_lower: MoneyInput,
    entry_upper: MoneyInput,
    base: MoneyInput,
    trim_lower: MoneyInput,
    trim_upper: MoneyInput,
    exit_lower: MoneyInput,
    exit_upper: MoneyInput,
    upside: MoneyInput,
}

impl TargetPriceInput {
    fn decode(self) -> Result<TargetPriceDraft, ServiceError> {
        Ok(TargetPriceDraft {
            downside: self.downside.decode()?,
            add: self.add.decode()?,
            entry_lower: self.entry_lower.decode()?,
            entry_upper: self.entry_upper.decode()?,
            base: self.base.decode()?,
            trim_lower: self.trim_lower.decode()?,
            trim_upper: self.trim_upper.decode()?,
            exit_lower: self.exit_lower.decode()?,
            exit_upper: self.exit_upper.decode()?,
            upside: self.upside.decode()?,
        })
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
enum AssumptionEvidenceInput {
    Dossier,
    DossierReference { index: usize },
    Forecast,
    FairValue,
    Portfolio,
    ReferenceMark,
}

impl From<AssumptionEvidenceInput> for TargetAssumptionEvidenceSelection {
    fn from(value: AssumptionEvidenceInput) -> Self {
        match value {
            AssumptionEvidenceInput::Dossier => Self::Dossier,
            AssumptionEvidenceInput::DossierReference { index } => Self::DossierReference { index },
            AssumptionEvidenceInput::Forecast => Self::Forecast,
            AssumptionEvidenceInput::FairValue => Self::FairValue,
            AssumptionEvidenceInput::Portfolio => Self::Portfolio,
            AssumptionEvidenceInput::ReferenceMark => Self::ReferenceMark,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssumptionDraftInput {
    text: String,
    evidence: AssumptionEvidenceInput,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TargetEvidenceInput {
    reference_mark: String,
    forecast_reference: Option<usize>,
    use_fair_value: bool,
    use_portfolio: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TargetDraftInput {
    operation: TargetOperationInput,
    dossier_id: String,
    intent: TargetIntentInput,
    horizon: TargetHorizonInput,
    prices: TargetPriceInput,
    method: TargetMethodInput,
    assumptions: Vec<AssumptionDraftInput>,
    thesis: String,
    risks: Vec<String>,
    invalidation_conditions: Vec<String>,
    evidence: TargetEvidenceInput,
}

impl TargetDraftInput {
    fn decode(self) -> Result<TargetPreparationDraft, ServiceError> {
        Ok(TargetPreparationDraft {
            operation: self.operation.decode()?,
            dossier_id: DossierId::try_new(self.dossier_id)
                .map_err(|_error| ServiceError::InvalidRequest)?,
            intent: self.intent.into(),
            horizon: self.horizon.into(),
            prices: self.prices.decode()?,
            method: self.method.into(),
            assumptions: self
                .assumptions
                .into_iter()
                .map(|assumption| TargetAssumptionDraft {
                    text: assumption.text.into_boxed_str(),
                    evidence: assumption.evidence.into(),
                })
                .collect(),
            thesis: self.thesis.into_boxed_str(),
            risks: boxed_strings(self.risks),
            invalidation_conditions: boxed_strings(self.invalidation_conditions),
            evidence: TargetEvidenceSelection {
                reference_mark: TargetReferenceMarkSelector::parse(&self.evidence.reference_mark)
                    .map_err(map_preparation)?,
                forecast_reference: self.evidence.forecast_reference,
                use_fair_value: self.evidence.use_fair_value,
                use_portfolio: self.evidence.use_portfolio,
            },
        })
    }
}

fn inventory_value(inventory: &TargetEvidenceInventory) -> Value {
    json!({
        "dossierId": inventory.dossier_id.as_str(),
        "instrumentId": inventory.instrument_id,
        "assembledAt": inventory.assembled_at,
        "forecastOptions": inventory.forecast.iter().map(|option| json!({"index": option.index})).collect::<Vec<_>>(),
        "fairValueAvailable": inventory.fair_value_available,
        "portfolioAvailable": inventory.portfolio_available,
        "referenceMarks": inventory.reference_marks.iter().map(|option| json!({
            "selector": option.selector.to_string(),
            "price": money_value(option.price),
            "observedAt": option.observed_at,
            "quality": option.quality,
            "source": option.source,
        })).collect::<Vec<_>>(),
    })
}

fn preview_value(preview: &PreparedTargetPreview) -> Value {
    json!({
        "receiptId": preview.receipt.to_string(),
        "receiptExpiresAt": preview.receipt_expires_at,
        "targetId": preview.target_id.as_str(),
        "revision": preview.revision.get(),
        "dossierId": preview.dossier_id.as_str(),
        "instrumentId": preview.instrument_id,
        "intent": intent_name(preview.intent),
        "referenceMark": money_value(preview.reference_mark),
        "referenceMarkObservedAt": preview.reference_mark_observed_at,
        "referenceMarkQuality": preview.reference_mark_quality,
        "referenceMarkSource": preview.reference_mark_source,
        "prices": {
            "downside": money_value(preview.prices.downside),
            "add": money_value(preview.prices.add),
            "entryLower": money_value(preview.prices.entry_lower),
            "entryUpper": money_value(preview.prices.entry_upper),
            "base": money_value(preview.prices.base),
            "trimLower": money_value(preview.prices.trim_lower),
            "trimUpper": money_value(preview.prices.trim_upper),
            "exitLower": money_value(preview.prices.exit_lower),
            "exitUpper": money_value(preview.prices.exit_upper),
            "upside": money_value(preview.prices.upside),
        },
        "method": super::target_method_name(preview.method),
        "assumptions": preview.assumptions.iter().map(|assumption| json!({
            "text": assumption.text,
            "evidence": assumption_evidence_value(assumption.evidence),
        })).collect::<Vec<_>>(),
        "thesis": preview.thesis,
        "risks": preview.risks,
        "invalidationConditions": preview.invalidation_conditions,
        "createdAt": preview.created_at,
        "horizonAt": preview.horizon_at,
        "expiresAt": preview.expires_at,
        "reviewDueAt": preview.review_due_at,
        "author": preview.author.as_str(),
        "rulesetVersion": preview.ruleset_version.get(),
        "forecastSelected": preview.forecast_selected,
        "fairValueSelected": preview.fair_value_selected,
        "portfolioSelected": preview.portfolio_selected,
    })
}

fn assumption_evidence_value(value: TargetAssumptionEvidenceSelection) -> Value {
    match value {
        TargetAssumptionEvidenceSelection::Dossier => json!({"kind": "dossier"}),
        TargetAssumptionEvidenceSelection::DossierReference { index } => {
            json!({"kind": "dossier_reference", "index": index})
        }
        TargetAssumptionEvidenceSelection::Forecast => json!({"kind": "forecast"}),
        TargetAssumptionEvidenceSelection::FairValue => json!({"kind": "fair_value"}),
        TargetAssumptionEvidenceSelection::Portfolio => json!({"kind": "portfolio"}),
        TargetAssumptionEvidenceSelection::ReferenceMark => {
            json!({"kind": "reference_mark"})
        }
    }
}

const fn intent_name(value: TargetIntent) -> &'static str {
    match value {
        TargetIntent::Buy => "buy",
        TargetIntent::Sell => "sell",
        TargetIntent::Hold => "hold",
    }
}

fn boxed_strings(values: Vec<String>) -> Vec<Box<str>> {
    values.into_iter().map(String::into_boxed_str).collect()
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

fn map_preparation(error: TargetPreparationError) -> ServiceError {
    match error {
        TargetPreparationError::InvalidRequest => ServiceError::InvalidRequest,
        TargetPreparationError::NotFound => ServiceError::NotFound,
        TargetPreparationError::Conflict => ServiceError::InvalidRequest,
        TargetPreparationError::Expired => ServiceError::InvalidRequest,
        TargetPreparationError::FenceMismatch => ServiceError::Unauthorized,
        TargetPreparationError::Capacity => ServiceError::ResourceExhausted,
        TargetPreparationError::Application(error) => super::map_application(error),
    }
}
