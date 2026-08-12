//! Closed service surface for application-owned candidate-dossier assembly.

use std::{str::FromStr as _, sync::Arc};

use chrono::DateTime;
use market_squawk_decisions::{CandidateId, DecisionContentDigest};
use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, InstrumentId, Timestamp};
use market_squawk_modeling::BundleId;
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceError, ServiceLimits, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::application::{
    Application,
    decision::{
        DecisionApplication, DossierEvidenceInventory, DossierEvidenceSelection,
        DossierFairValueEvidence, DossierForecastEvidence, DossierPreparationDraft,
        DossierPreparationError, DossierPreparationFence, DossierPreparationReceipt,
        PreparedDossierPreview,
    },
};

use super::append_outcome_value;

pub(super) const GET_DOSSIER_PREPARATION: &str = "Decision.GetDossierPreparation";
pub(super) const PREPARE_DOSSIER: &str = "Decision.PrepareDossier";
pub(super) const CREATE_DOSSIER: &str = "Decision.CreateDossier";
const LIST_FORECASTS: &str = "Model.ListForecasts";
const LIST_MEASUREMENTS: &str = "FairValue.ListMeasurements";
const GET_CLASSIFICATION: &str = "FairValue.GetClassification";
const FORECAST_SELECTOR_PREFIX: &str = "forecast:";
const FAIR_VALUE_SELECTOR_PREFIX: &str = "fair-value:";
const MAXIMUM_FORECAST_OPTIONS: usize = 64;
const MAXIMUM_FAIR_VALUE_OPTIONS: usize = 64;
const MAXIMUM_EVIDENCE_SCAN_ITEMS: usize = 4_096;
const MAXIMUM_EVIDENCE_RESULT_BYTES: usize = 8 * 1024 * 1024;

/// Runtime-fenced dossier preparation over the sole durable decision authority.
pub(super) struct DossierPreparationOperations {
    decisions: Arc<DecisionApplication>,
    application: Arc<Application>,
    runtime: RuntimeIdentity,
}

impl DossierPreparationOperations {
    pub(super) const fn new(
        decisions: Arc<DecisionApplication>,
        application: Arc<Application>,
        runtime: RuntimeIdentity,
    ) -> Self {
        Self {
            decisions,
            application,
            runtime,
        }
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(
            operation,
            GET_DOSSIER_PREPARATION | PREPARE_DOSSIER | CREATE_DOSSIER
        )
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let now = super::super::runtime::current_timestamp()
            .map_err(|_error| ServiceError::Unavailable)?;
        let arguments = super::super::business_arguments(request.arguments());
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
                let catalog = self.evidence_catalog(&inventory, now, context).await?;
                inventory_value(&inventory, &catalog)
            }
            PREPARE_DOSSIER => {
                let input: PrepareRequest = decode(&arguments)?;
                let candidate_id = CandidateId::try_new(input.draft.candidate_id.as_str())
                    .map_err(|_error| ServiceError::InvalidRequest)?;
                let inventory = self
                    .decisions
                    .dossier_evidence_inventory(&candidate_id)
                    .map_err(map_preparation)?;
                let catalog = self.evidence_catalog(&inventory, now, context).await?;
                let draft = input.draft.decode(&catalog)?;
                let preview = self
                    .decisions
                    .prepare_dossier(self.fence(context)?, draft, now)
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

    async fn evidence_catalog(
        &self,
        inventory: &DossierEvidenceInventory,
        now: Timestamp,
        context: &RequestContext,
    ) -> Result<DossierEvidenceCatalog, ServiceError> {
        let forecast_context = evidence_context(context, MAXIMUM_EVIDENCE_SCAN_ITEMS)?;
        let forecast_result = self
            .application
            .invoke(LIST_FORECASTS, Map::new(), forecast_context)
            .await?;
        let forecast_list: ForecastList =
            serde_json::from_value(forecast_result.structured_content().clone())
                .map_err(|_error| ServiceError::InvalidResult)?;
        if forecast_list.available < forecast_list.forecasts.len()
            || forecast_list.truncated != (forecast_list.forecasts.len() < forecast_list.available)
        {
            return Err(ServiceError::InvalidResult);
        }
        let mut forecasts = Vec::new();
        forecasts
            .try_reserve_exact(forecast_list.forecasts.len().min(MAXIMUM_FORECAST_OPTIONS))
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for record in forecast_list.forecasts {
            if let Some(option) = forecast_option(record, inventory, now)? {
                forecasts.push(option);
                if forecasts.len() == MAXIMUM_FORECAST_OPTIONS {
                    break;
                }
            }
        }
        if forecasts.iter().enumerate().any(|(index, option)| {
            forecasts[index + 1..]
                .iter()
                .any(|other| option.selector == other.selector)
        }) {
            return Err(ServiceError::InvalidResult);
        }

        let fair_value_context = evidence_context(context, MAXIMUM_EVIDENCE_SCAN_ITEMS)?;
        let measurement_result = self
            .application
            .invoke(LIST_MEASUREMENTS, Map::new(), fair_value_context)
            .await?;
        let measurements: MeasurementList =
            serde_json::from_value(measurement_result.structured_content().clone())
                .map_err(|_error| ServiceError::InvalidResult)?;
        let mut compatible_measurements = Vec::new();
        compatible_measurements
            .try_reserve_exact(
                measurements
                    .measurements
                    .len()
                    .min(MAXIMUM_FAIR_VALUE_OPTIONS),
            )
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for record in measurements.measurements {
            if let Some(option) = FairValueMeasurementOption::try_from_record(record, inventory)? {
                compatible_measurements.push(option);
            }
        }
        compatible_measurements.sort_unstable_by(|left, right| {
            right
                .measurement_at
                .cmp(&left.measurement_at)
                .then_with(|| left.measurement_id.cmp(&right.measurement_id))
        });
        compatible_measurements.truncate(MAXIMUM_FAIR_VALUE_OPTIONS);
        let mut fair_values = Vec::new();
        fair_values
            .try_reserve_exact(compatible_measurements.len())
            .map_err(|_error| ServiceError::ResourceExhausted)?;
        for measurement in compatible_measurements {
            ensure_live(context)?;
            let mut arguments = Map::new();
            arguments.insert(
                "measurementId".to_owned(),
                Value::String(measurement.measurement_id.clone()),
            );
            let classification_context = evidence_context(context, 1)?;
            match self
                .application
                .invoke(GET_CLASSIFICATION, arguments, classification_context)
                .await
            {
                Ok(result) => {
                    let classified: ClassifiedMeasurement =
                        serde_json::from_value(result.structured_content().clone())
                            .map_err(|_error| ServiceError::InvalidResult)?;
                    fair_values.push(measurement.finish(classified)?);
                }
                Err(ServiceError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        if fair_values.iter().enumerate().any(|(index, option)| {
            fair_values[index + 1..]
                .iter()
                .any(|other| option.selector == other.selector)
        }) {
            return Err(ServiceError::InvalidResult);
        }
        Ok(DossierEvidenceCatalog {
            forecasts,
            fair_values,
        })
    }
}

impl std::fmt::Debug for DossierPreparationOperations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DossierPreparationOperations")
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .field("application", &"[INSTALLED EVIDENCE AUTHORITIES]")
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
    forecast_selector: Option<String>,
    fair_value_selector: Option<String>,
}

impl DossierDraftInput {
    fn decode(
        self,
        catalog: &DossierEvidenceCatalog,
    ) -> Result<DossierPreparationDraft, ServiceError> {
        let mut evidence = self
            .evidence
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let forecast = self
            .forecast_selector
            .as_deref()
            .map(|selector| catalog.resolve_forecast(selector))
            .transpose()?;
        if forecast.is_some() {
            evidence.push(DossierEvidenceSelection::Forecast);
        }
        let fair_value = self
            .fair_value_selector
            .as_deref()
            .map(|selector| catalog.resolve_fair_value(selector))
            .transpose()?;
        if fair_value.is_some() {
            evidence.push(DossierEvidenceSelection::FairValue);
        }
        Ok(DossierPreparationDraft {
            candidate_id: CandidateId::try_new(self.candidate_id)
                .map_err(|_error| ServiceError::InvalidRequest)?,
            evidence,
            forecast,
            fair_value,
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

fn inventory_value(value: &DossierEvidenceInventory, catalog: &DossierEvidenceCatalog) -> Value {
    json!({
        "candidateId": value.candidate_id.as_str(),
        "screenRunId": value.screen_run_id.as_str(),
        "instrumentId": value.instrument_id,
        "selectedAt": value.selected_at,
        "requiredEvidence": ["candidate", "dataset", "universe"],
        "portfolioImpactAvailable": value.portfolio_impact_available,
        "forecastOptions": catalog.forecasts.iter().map(ForecastEvidenceOption::value).collect::<Vec<_>>(),
        "fairValueOptions": catalog.fair_values.iter().map(FairValueEvidenceOption::value).collect::<Vec<_>>(),
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
        "forecastSelector": value.forecast_selector,
        "fairValueSelector": value.fair_value_selector,
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
        DossierEvidenceSelection::Forecast => "forecast",
        DossierEvidenceSelection::FairValue => "fair_value",
    }
}

#[derive(Debug)]
struct DossierEvidenceCatalog {
    forecasts: Vec<ForecastEvidenceOption>,
    fair_values: Vec<FairValueEvidenceOption>,
}

impl DossierEvidenceCatalog {
    fn resolve_forecast(&self, selector: &str) -> Result<DossierForecastEvidence, ServiceError> {
        if !valid_selector(selector, FORECAST_SELECTOR_PREFIX) {
            return Err(ServiceError::InvalidRequest);
        }
        self.forecasts
            .iter()
            .find(|option| option.selector.as_ref() == selector)
            .map(|option| option.evidence.clone())
            .ok_or(ServiceError::NotFound)
    }

    fn resolve_fair_value(&self, selector: &str) -> Result<DossierFairValueEvidence, ServiceError> {
        if !valid_selector(selector, FAIR_VALUE_SELECTOR_PREFIX) {
            return Err(ServiceError::InvalidRequest);
        }
        self.fair_values
            .iter()
            .find(|option| option.selector.as_ref() == selector)
            .map(|option| option.evidence.clone())
            .ok_or(ServiceError::NotFound)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastList {
    forecasts: Vec<ForecastSummary>,
    available: usize,
    truncated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastSummary {
    vintage_id: String,
    request_hash: String,
    instrument_id: InstrumentId,
    model_id: String,
    bundle_id: String,
    bundle_version: u64,
    observed_through_unix_nanos: i64,
    created_at_unix_nanos: i64,
    expires_at_unix_nanos: i64,
    horizon_points: u16,
    horizon_step_nanos: u64,
    has_calibrated_intervals: bool,
    quality: String,
    unavailable_reason: Option<String>,
    controlled_artifact: Value,
}

#[derive(Clone, Debug)]
struct ForecastEvidenceOption {
    selector: Box<str>,
    model_id: Box<str>,
    bundle_id: Box<str>,
    bundle_version: u64,
    observed_through: Timestamp,
    created_at: Timestamp,
    expires_at: Timestamp,
    horizon_points: u16,
    horizon_step_nanos: u64,
    calibrated: bool,
    quality: Box<str>,
    evidence: DossierForecastEvidence,
}

impl ForecastEvidenceOption {
    fn value(&self) -> Value {
        json!({
            "selector": self.selector,
            "modelId": self.model_id,
            "bundleId": self.bundle_id,
            "bundleVersion": self.bundle_version,
            "observedThrough": self.observed_through,
            "createdAt": self.created_at,
            "expiresAt": self.expires_at,
            "horizonPoints": self.horizon_points,
            "horizonStepNanos": self.horizon_step_nanos,
            "calibrated": self.calibrated,
            "quality": self.quality,
        })
    }
}

fn forecast_option(
    record: ForecastSummary,
    inventory: &DossierEvidenceInventory,
    now: Timestamp,
) -> Result<Option<ForecastEvidenceOption>, ServiceError> {
    let observed_through = Timestamp::from_unix_nanos(record.observed_through_unix_nanos);
    let created_at = Timestamp::from_unix_nanos(record.created_at_unix_nanos);
    let expires_at = Timestamp::from_unix_nanos(record.expires_at_unix_nanos);
    if record.instrument_id != inventory.instrument_id
        || observed_through > inventory.selected_at
        || created_at > inventory.selected_at
        || expires_at <= now
        || record.horizon_points == 0
        || record.horizon_step_nanos == 0
        || record.quality != "modeled"
        || record.unavailable_reason.is_some()
    {
        return Ok(None);
    }
    validate_hex_digest(&record.request_hash)?;
    validate_controlled_artifact(&record.controlled_artifact)?;
    let content_identity = decision_digest_from_hex(&record.vintage_id)?;
    let selector = format!("{FORECAST_SELECTOR_PREFIX}{}", record.vintage_id).into_boxed_str();
    let bundle = BundleId::try_new(record.bundle_id.as_str())
        .map_err(|_error| ServiceError::InvalidResult)?;
    let evidence = DossierForecastEvidence::try_new(selector.clone(), content_identity, bundle)
        .map_err(map_preparation)?;
    Ok(Some(ForecastEvidenceOption {
        selector,
        model_id: record.model_id.into_boxed_str(),
        bundle_id: record.bundle_id.into_boxed_str(),
        bundle_version: record.bundle_version,
        observed_through,
        created_at,
        expires_at,
        horizon_points: record.horizon_points,
        horizon_step_nanos: record.horizon_step_nanos,
        calibrated: record.has_calibrated_intervals,
        quality: record.quality.into_boxed_str(),
        evidence,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MeasurementList {
    measurements: Vec<FairValueMeasurementRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FairValueMeasurementRecord {
    measurement_id: String,
    evidence_hash: String,
    account_id: String,
    instrument_id: InstrumentId,
    amount: FairValueAmount,
    measurement_at: String,
    prepared_at: String,
    prepared_by: String,
    method: String,
    input_count: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct FairValueAmount {
    amount: String,
    currency: String,
    scale: u32,
    #[serde(rename = "amountBasis")]
    amount_basis: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassifiedMeasurement {
    measurement: FairValueMeasurementRecord,
    classification: FairValueClassificationRecord,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FairValueClassificationRecord {
    decision_id: String,
    measurement_id: String,
    evidence_hash: String,
    ruleset_version: u32,
    ruleset_hash: String,
    hierarchy: String,
    basis: Value,
    truth_table_item_count: usize,
    reason_count: usize,
}

#[derive(Debug)]
struct FairValueMeasurementOption {
    measurement_id: String,
    record: FairValueMeasurementRecord,
    measurement_at: Timestamp,
    prepared_at: Timestamp,
}

impl FairValueMeasurementOption {
    fn try_from_record(
        record: FairValueMeasurementRecord,
        inventory: &DossierEvidenceInventory,
    ) -> Result<Option<Self>, ServiceError> {
        let measurement_at = timestamp(&record.measurement_at)?;
        let prepared_at = timestamp(&record.prepared_at)?;
        if record.prepared_by.is_empty()
            || record.input_count == 0
            || !matches!(
                record.amount.amount_basis.as_str(),
                "per_instrument_unit" | "reporting_entity_total" | "position_total"
            )
            || !matches!(
                record.method.as_str(),
                "quoted_market_price" | "market_approach" | "income_approach" | "cost_approach"
            )
        {
            return Err(ServiceError::InvalidResult);
        }
        if record.instrument_id != inventory.instrument_id
            || measurement_at > inventory.selected_at
            || prepared_at > inventory.selected_at
        {
            return Ok(None);
        }
        validate_hex_digest(&record.measurement_id)?;
        validate_hex_digest(&record.evidence_hash)?;
        Ok(Some(Self {
            measurement_id: record.measurement_id.clone(),
            record,
            measurement_at,
            prepared_at,
        }))
    }

    fn finish(
        self,
        classified: ClassifiedMeasurement,
    ) -> Result<FairValueEvidenceOption, ServiceError> {
        if classified.measurement != self.record
            || classified.classification.measurement_id != self.measurement_id
            || classified.classification.evidence_hash != self.record.evidence_hash
            || classified.classification.ruleset_version == 0
            || classified.classification.truth_table_item_count == 0
            || classified.classification.hierarchy.is_empty()
            || !matches!(
                classified.classification.hierarchy.as_str(),
                "level_1" | "level_2" | "level_3" | "unclassified"
            )
        {
            return Err(ServiceError::InvalidResult);
        }
        validate_hex_digest(&classified.classification.ruleset_hash)?;
        if !classified.classification.basis.is_object() {
            return Err(ServiceError::InvalidResult);
        }
        let decision_id =
            market_squawk_valuation::DecisionId::from_str(&classified.classification.decision_id)
                .map_err(|_error| ServiceError::InvalidResult)?;
        let selector = format!("{FAIR_VALUE_SELECTOR_PREFIX}{decision_id}").into_boxed_str();
        let content_identity = DecisionContentDigest::try_new(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            decision_id.bytes(),
        ))
        .map_err(|_error| ServiceError::InvalidResult)?;
        let evidence =
            DossierFairValueEvidence::try_new(selector.clone(), content_identity, decision_id)
                .map_err(map_preparation)?;
        Ok(FairValueEvidenceOption {
            selector,
            measurement_id: self.measurement_id.into_boxed_str(),
            account_id: self.record.account_id.into_boxed_str(),
            amount: self.record.amount.amount.into_boxed_str(),
            currency: self.record.amount.currency.into_boxed_str(),
            scale: self.record.amount.scale,
            amount_basis: self.record.amount.amount_basis.into_boxed_str(),
            measurement_at: self.measurement_at,
            prepared_at: self.prepared_at,
            method: self.record.method.into_boxed_str(),
            hierarchy: classified.classification.hierarchy.into_boxed_str(),
            ruleset_version: classified.classification.ruleset_version,
            reason_count: classified.classification.reason_count,
            evidence,
        })
    }
}

#[derive(Clone, Debug)]
struct FairValueEvidenceOption {
    selector: Box<str>,
    measurement_id: Box<str>,
    account_id: Box<str>,
    amount: Box<str>,
    currency: Box<str>,
    scale: u32,
    amount_basis: Box<str>,
    measurement_at: Timestamp,
    prepared_at: Timestamp,
    method: Box<str>,
    hierarchy: Box<str>,
    ruleset_version: u32,
    reason_count: usize,
    evidence: DossierFairValueEvidence,
}

impl FairValueEvidenceOption {
    fn value(&self) -> Value {
        json!({
            "selector": self.selector,
            "measurementId": self.measurement_id,
            "accountId": self.account_id,
            "amount": {
                "amount": self.amount,
                "currency": self.currency,
                "scale": self.scale,
                "amountBasis": self.amount_basis,
            },
            "measurementAt": self.measurement_at,
            "preparedAt": self.prepared_at,
            "method": self.method,
            "hierarchy": self.hierarchy,
            "rulesetVersion": self.ruleset_version,
            "reasonCount": self.reason_count,
        })
    }
}

fn evidence_context(
    context: &RequestContext,
    maximum_items: usize,
) -> Result<RequestContext, ServiceError> {
    let parent = context.limits();
    let maximum_result_items = maximum_items.max(1);
    let maximum_inline_items = maximum_result_items;
    let maximum_result_bytes = MAXIMUM_EVIDENCE_RESULT_BYTES;
    let maximum_inline_bytes = maximum_result_bytes;
    let limits = ServiceLimits::try_new(
        maximum_inline_bytes,
        maximum_inline_items,
        maximum_result_bytes,
        maximum_result_items,
        parent.result_structure(),
    )
    .map_err(|_error| ServiceError::Internal)?;
    let mut child = RequestContext::new(
        context.request_id().clone(),
        context.cancellation().clone(),
        context.deadline(),
        limits,
    );
    if let Some(origin) = context.origin() {
        child = child.with_origin(origin);
    }
    Ok(child)
}

fn timestamp(value: &str) -> Result<Timestamp, ServiceError> {
    let parsed =
        DateTime::parse_from_rfc3339(value).map_err(|_error| ServiceError::InvalidResult)?;
    let nanos = parsed
        .timestamp_nanos_opt()
        .ok_or(ServiceError::InvalidResult)?;
    Ok(Timestamp::from_unix_nanos(nanos))
}

fn decision_digest_from_hex(value: &str) -> Result<DecisionContentDigest, ServiceError> {
    let bytes = decode_hex_digest(value)?;
    DecisionContentDigest::try_new(EvidenceDigest::new(DigestAlgorithm::Sha256, bytes))
        .map_err(|_error| ServiceError::InvalidResult)
}

fn validate_hex_digest(value: &str) -> Result<(), ServiceError> {
    decode_hex_digest(value).map(|_bytes| ())
}

fn decode_hex_digest(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ServiceError::InvalidResult);
    }
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        let offset = index.checked_mul(2).ok_or(ServiceError::InvalidResult)?;
        *byte = u8::from_str_radix(
            value
                .get(offset..offset + 2)
                .ok_or(ServiceError::InvalidResult)?,
            16,
        )
        .map_err(|_error| ServiceError::InvalidResult)?;
    }
    if output == [0; 32] {
        return Err(ServiceError::InvalidResult);
    }
    Ok(output)
}

fn valid_selector(value: &str, prefix: &str) -> bool {
    value.len() == prefix.len() + 64
        && value
            .strip_prefix(prefix)
            .is_some_and(|digest| decode_hex_digest(digest).is_ok())
}

fn validate_controlled_artifact(value: &Value) -> Result<(), ServiceError> {
    let object = value.as_object().ok_or(ServiceError::InvalidResult)?;
    let artifact_id = object
        .get("artifactId")
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidResult)?;
    if object.len() != 4
        || artifact_id.is_empty()
        || artifact_id.len() > 160
        || !artifact_id
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        || !artifact_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        || object
            .get("sha256")
            .and_then(Value::as_str)
            .map(validate_hex_digest)
            .transpose()?
            .is_none()
        || object
            .get("byteCount")
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
        || object.get("mediaType").and_then(Value::as_str) != Some("application/json")
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
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
