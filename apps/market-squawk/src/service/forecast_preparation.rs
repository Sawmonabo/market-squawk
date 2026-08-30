//! Installed-service adapter for evidence-derived forecast preparation.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU16,
    sync::Arc,
};

use market_squawk_data::InstrumentDefinitionReadCapability;
use market_squawk_domain::{InstrumentDefinition, InstrumentId};
use market_squawk_modeling::{ForecastHorizon, ModelOutputSemantics};
use market_squawk_runtime::RuntimeIdentity;
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceError, ToolResultMetadata, TypedToolRequest,
    TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use crate::{
    LocalProduct,
    application::{
        AnalyticalForecastEvidenceReader, internal_forecast_generation_descriptor,
        lifecycle::WorkspaceRuntimeIdentity,
        model::forecast_preparation::{
            ForecastEvidenceDataset, ForecastEvidencePolicy, ForecastInstrumentAvailability,
            ForecastModelSummary, ForecastPreparationAuthority, ForecastPreparationCatalog,
            ForecastPreparationError, ForecastPreparationLimits, ForecastPreparationPreview,
            ForecastPreparationSelection, PreparedForecast,
        },
        opaque_product_token,
    },
};

pub(super) const GET_FORECAST_PREPARATION: &str = "Model.GetForecastPreparation";
pub(super) const PREPARE_FORECAST: &str = "Model.PrepareForecast";
pub(super) const START_PREPARED_FORECAST: &str = "Model.StartPreparedForecast";

const MAXIMUM_CATALOG_INSTRUMENTS: usize = 4_096;

/// One process-owned preparation authority, absent only when no model runtime is admitted.
pub(super) struct InstalledForecastPreparation {
    authority: Option<Arc<ForecastPreparationAuthority>>,
    instruments: InstrumentDefinitionReadCapability,
    runtime: RuntimeIdentity,
}

impl InstalledForecastPreparation {
    pub(super) fn try_new(
        product: &LocalProduct,
        _capabilities: &ServiceCapabilities,
        runtime: RuntimeIdentity,
    ) -> Result<Self, ServiceError> {
        let authority = product
            .model_runtime()
            .map(|model_runtime| {
                let descriptor = internal_forecast_generation_descriptor()
                    .map_err(|_error| ServiceError::Unavailable)?;
                let evidence = Arc::new(AnalyticalForecastEvidenceReader::new(
                    product.research().analytical_reader(),
                    Some(product.macro_context_read_capability()),
                ));
                ForecastPreparationAuthority::try_new(
                    model_runtime,
                    evidence,
                    descriptor,
                    ForecastPreparationLimits::standard().map_err(map_preparation)?,
                )
                .map(Arc::new)
                .map_err(map_preparation)
            })
            .transpose()?;
        Ok(Self {
            authority,
            instruments: product.research().instrument_definitions(),
            runtime,
        })
    }

    pub(super) fn owns(operation: &str) -> bool {
        matches!(operation, GET_FORECAST_PREPARATION | PREPARE_FORECAST)
    }

    pub(super) async fn call(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        ensure_live(context)?;
        let authority = self.authority.as_ref().ok_or(ServiceError::Unavailable)?;
        let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
        let workspace = self.workspace()?;
        let (content, item_count) = match request.name() {
            GET_FORECAST_PREPARATION => {
                let catalog = authority
                    .catalog(
                        origin,
                        workspace,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_preparation)?;
                let labels = self.labels_for_catalog(&catalog, context)?;
                catalog_value(&catalog, &labels)
            }
            PREPARE_FORECAST => {
                let input: ForecastPreparationRequest =
                    decode(&super::business_arguments(request.arguments()))?;
                let catalog = authority
                    .catalog(
                        origin,
                        workspace,
                        context.deadline(),
                        context.cancellation().child_token(),
                    )
                    .await
                    .map_err(map_preparation)?;
                let selection = resolve_selection(&catalog, input.selection)?;
                let prepared = authority
                    .prepare(
                        origin,
                        workspace,
                        selection,
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_preparation)?;
                let label = self.instrument_label(prepared.preview().instrument_id(), context)?;
                (prepared_value(&prepared, &label), 1)
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
        .map_err(ServiceError::from)
    }

    pub(super) async fn consume(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolRequest, ServiceError> {
        ensure_live(context)?;
        let authority = self.authority.as_ref().ok_or(ServiceError::Unavailable)?;
        let input: PreparedForecastStart = decode(&super::business_arguments(request.arguments()))?;
        authority
            .consume_token(
                context.origin().ok_or(ServiceError::Unauthorized)?,
                self.workspace()?,
                input.confirmation_token,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(map_preparation)
    }

    fn workspace(&self) -> Result<WorkspaceRuntimeIdentity, ServiceError> {
        WorkspaceRuntimeIdentity::try_from_runtime(self.runtime)
            .map_err(|_error| ServiceError::Unavailable)
    }

    fn labels_for_catalog(
        &self,
        catalog: &ForecastPreparationCatalog,
        context: &RequestContext,
    ) -> Result<BTreeMap<InstrumentId, String>, ServiceError> {
        let ids = catalog
            .evidence()
            .datasets()
            .iter()
            .flat_map(|dataset| dataset.instruments().iter())
            .map(ForecastInstrumentAvailability::instrument_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if ids.len() > MAXIMUM_CATALOG_INSTRUMENTS {
            return Err(ServiceError::ResourceExhausted);
        }
        let definitions = self
            .instruments
            .latest(
                &ids,
                MAXIMUM_CATALOG_INSTRUMENTS,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|_error| ServiceError::Unavailable)?;
        Ok(definitions
            .iter()
            .map(|definition| (definition.instrument_id(), definition_label(definition)))
            .collect())
    }

    fn instrument_label(
        &self,
        instrument_id: InstrumentId,
        context: &RequestContext,
    ) -> Result<String, ServiceError> {
        self.instruments
            .latest(
                &[instrument_id],
                1,
                context.deadline(),
                context.cancellation(),
            )
            .map_err(|_error| ServiceError::Unavailable)?
            .first()
            .map(definition_label)
            .ok_or(ServiceError::NotFound)
    }
}

impl std::fmt::Debug for InstalledForecastPreparation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledForecastPreparation")
            .field(
                "authority",
                &self.authority.as_ref().map(|_| "[FORECAST AUTHORITY]"),
            )
            .field("instruments", &self.instruments)
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastPreparationRequest {
    selection: ForecastSelectionWire,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastSelectionWire {
    model_token: Uuid,
    history_token: Uuid,
    investment_token: InstrumentId,
    policy_token: Uuid,
    horizon_points: u16,
    validity_nanos: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedForecastStart {
    confirmation_token: Uuid,
}

fn resolve_selection(
    catalog: &ForecastPreparationCatalog,
    selection: ForecastSelectionWire,
) -> Result<ForecastPreparationSelection, ServiceError> {
    let mut models = catalog
        .models()
        .iter()
        .filter(|model| model_token(model) == selection.model_token);
    let model = models.next().ok_or(ServiceError::InvalidRequest)?;
    if models.next().is_some() {
        return Err(ServiceError::InvalidRequest);
    }
    let mut histories = catalog.evidence().datasets().iter().filter(|dataset| {
        dataset_matches_model(dataset, model) && history_token(dataset) == selection.history_token
    });
    let history = histories.next().ok_or(ServiceError::InvalidRequest)?;
    if histories.next().is_some()
        || !history
            .instruments()
            .iter()
            .any(|instrument| instrument.instrument_id() == selection.investment_token)
    {
        return Err(ServiceError::InvalidRequest);
    }
    let mut policies = history
        .policies()
        .iter()
        .copied()
        .filter(|policy| policy_token(*policy) == selection.policy_token);
    let policy = policies.next().ok_or(ServiceError::InvalidRequest)?;
    if policies.next().is_some() {
        return Err(ServiceError::InvalidRequest);
    }
    ForecastPreparationSelection::try_new(
        model.model_id(),
        model.bundle_id().clone(),
        model.bundle_version(),
        history.dataset().manifest().clone(),
        history.analysis_manifest().clone(),
        selection.investment_token,
        ForecastHorizon::try_new(
            NonZeroU16::new(selection.horizon_points).ok_or(ServiceError::InvalidRequest)?,
            policy.horizon_step_nanos(),
        )
        .map_err(|_| ServiceError::InvalidRequest)?,
        parse_u64(&selection.validity_nanos)?,
    )
    .map_err(map_preparation)
}

fn model_token(model: &ForecastModelSummary) -> Uuid {
    let model_id = model.model_id().as_uuid();
    let bundle_version = model.bundle_version().get().to_be_bytes();
    let components: [&[u8]; 3] = [
        model_id.as_bytes(),
        model.bundle_id().as_str().as_bytes(),
        &bundle_version,
    ];
    opaque_product_token(b"market-squawk/forecast-model-choice/v1\0", &components)
}

fn history_token(dataset: &ForecastEvidenceDataset) -> Uuid {
    let training = dataset.dataset().manifest();
    let analysis = dataset.analysis_manifest();
    let training_manifest_version = training.manifest_version().to_be_bytes();
    let training_schema_version = training.schema().version().get().to_be_bytes();
    let training_schema_fingerprint = training.schema().fingerprint();
    let training_content_hash = training.content_hash().bytes();
    let analysis_manifest_version = analysis.manifest_version().to_be_bytes();
    let analysis_schema_version = analysis.schema().version().get().to_be_bytes();
    let analysis_schema_fingerprint = analysis.schema().fingerprint();
    let analysis_content_hash = analysis.content_hash().bytes();
    let components: [&[u8]; 12] = [
        training.dataset_id().as_str().as_bytes(),
        &training_manifest_version,
        training.schema().name().as_bytes(),
        &training_schema_version,
        &training_schema_fingerprint,
        &training_content_hash,
        analysis.dataset_id().as_str().as_bytes(),
        &analysis_manifest_version,
        analysis.schema().name().as_bytes(),
        &analysis_schema_version,
        &analysis_schema_fingerprint,
        &analysis_content_hash,
    ];
    opaque_product_token(b"market-squawk/forecast-history-choice/v1\0", &components)
}

fn policy_token(policy: ForecastEvidencePolicy) -> Uuid {
    let maximum_horizon_points = policy.maximum_horizon_points().get().to_be_bytes();
    let horizon_step_nanos = policy.horizon_step_nanos().get().to_be_bytes();
    let maximum_validity_nanos = policy.maximum_validity_nanos().get().to_be_bytes();
    let minimum_observed_points = policy.minimum_observed_points().get().to_be_bytes();
    let components: [&[u8]; 4] = [
        &maximum_horizon_points,
        &horizon_step_nanos,
        &maximum_validity_nanos,
        &minimum_observed_points,
    ];
    opaque_product_token(b"market-squawk/forecast-policy-choice/v1\0", &components)
}

fn catalog_value(
    catalog: &ForecastPreparationCatalog,
    labels: &BTreeMap<InstrumentId, String>,
) -> (Value, usize) {
    let models = catalog
        .models()
        .iter()
        .filter_map(|model| {
            let datasets = catalog
                .evidence()
                .datasets()
                .iter()
                .filter(|dataset| dataset_matches_model(dataset, model))
                .map(|dataset| dataset_value(dataset, labels))
                .collect::<Vec<_>>();
            (!datasets.is_empty()).then(|| model_value(model, Some(datasets)))
        })
        .collect::<Vec<_>>();
    let item_count = models.len();
    (json!({ "models": models }), item_count)
}

fn dataset_matches_model(dataset: &ForecastEvidenceDataset, model: &ForecastModelSummary) -> bool {
    dataset.model_id() == model.model_id()
        && dataset.bundle_id() == model.bundle_id()
        && dataset.bundle_version() == model.bundle_version()
        && dataset.dataset().manifest() == model.dataset_manifest()
}

fn dataset_value(
    dataset: &ForecastEvidenceDataset,
    labels: &BTreeMap<InstrumentId, String>,
) -> Value {
    json!({
        "historyToken": history_token(dataset),
        "instruments": dataset.instruments().iter().map(|instrument| json!({
            "investmentToken": instrument.instrument_id().to_string(),
            "label": labels.get(&instrument.instrument_id()).cloned().unwrap_or_else(|| instrument.instrument_id().to_string()),
            "observedFromUnixNanos": instrument.observed_from().unix_nanos().to_string(),
            "observedThroughUnixNanos": instrument.observed_through().unix_nanos().to_string(),
            "availableAtUnixNanos": instrument.available_at().unix_nanos().to_string(),
            "observedPoints": instrument.observed_points().get(),
        })).collect::<Vec<_>>(),
        "policies": dataset.policies().iter().map(|policy| json!({
            "policyToken": policy_token(*policy),
            "maximumHorizonPoints": policy.maximum_horizon_points().get(),
            "horizonStepNanos": policy.horizon_step_nanos().get().to_string(),
            "maximumValidityNanos": policy.maximum_validity_nanos().get().to_string(),
            "minimumObservedPoints": policy.minimum_observed_points().get(),
        })).collect::<Vec<_>>(),
    })
}

fn prepared_value(prepared: &PreparedForecast, instrument_label: &str) -> Value {
    let receipt = prepared.receipt();
    let preview = prepared.preview();
    json!({
        "receipt": {
            "confirmationToken": receipt.receipt_id(),
            "expiresAtUnixNanos": receipt.expires_at().unix_nanos().to_string(),
        },
        "preview": preview_value(preview, instrument_label),
    })
}

fn preview_value(preview: &ForecastPreparationPreview, instrument_label: &str) -> Value {
    json!({
        "model": model_value(preview.model(), None),
        "investmentToken": preview.instrument_id().to_string(),
        "instrumentLabel": instrument_label,
        "observedFromUnixNanos": preview.observed_from().unix_nanos().to_string(),
        "observedThroughUnixNanos": preview.observed_through().unix_nanos().to_string(),
        "availableAtUnixNanos": preview.available_at().unix_nanos().to_string(),
        "observedPoints": preview.observed_points(),
        "horizonPoints": preview.horizon().points().get(),
        "horizonStepNanos": preview.horizon().step_nanos().get().to_string(),
        "validityNanos": preview.validity_nanos().to_string(),
        "evidenceState": if preview.model().has_calibrated_intervals() { "calibrated" } else { "limited" },
        "analysisOnly": true,
    })
}

fn model_value(model: &ForecastModelSummary, datasets: Option<Vec<Value>>) -> Value {
    let (name, objective) = match model.output_semantics() {
        ModelOutputSemantics::Regression => ("Numeric outcome forecast", "numeric_outcome"),
        ModelOutputSemantics::BinaryProbability => ("Likelihood estimate", "likelihood"),
    };
    let mut value = serde_json::Map::from_iter([
        ("modelToken".to_owned(), json!(model_token(model))),
        ("name".to_owned(), json!(name)),
        ("objective".to_owned(), json!(objective)),
        ("intendedUse".to_owned(), json!(model.intended_use())),
        ("limitations".to_owned(), json!(model.limitations())),
        (
            "evidenceState".to_owned(),
            json!(if model.has_calibrated_intervals() {
                "calibrated"
            } else {
                "limited"
            }),
        ),
        ("unavailableBehavior".to_owned(), json!("no_action")),
    ]);
    if let Some(datasets) = datasets {
        value.insert("histories".to_owned(), Value::Array(datasets));
    }
    Value::Object(value)
}

fn definition_label(definition: &InstrumentDefinition) -> String {
    let mut mappings = definition
        .venue_mappings()
        .iter()
        .map(|mapping| mapping.venue_symbol().to_string())
        .collect::<Vec<_>>();
    mappings.sort_unstable();
    mappings
        .into_iter()
        .next()
        .unwrap_or_else(|| definition.instrument_id().to_string())
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn parse_u64(value: &str) -> Result<u64, ServiceError> {
    value.parse().map_err(|_error| ServiceError::InvalidRequest)
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

fn map_preparation(error: ForecastPreparationError) -> ServiceError {
    match error {
        ForecastPreparationError::InvalidLimits
        | ForecastPreparationError::InvalidDescriptor
        | ForecastPreparationError::InvalidSelection
        | ForecastPreparationError::IncompatibleSelection
        | ForecastPreparationError::InvalidEvidence => ServiceError::InvalidRequest,
        ForecastPreparationError::ModelUnavailable
        | ForecastPreparationError::ReceiptUnavailable => ServiceError::NotFound,
        ForecastPreparationError::ReceiptMismatch => ServiceError::Unauthorized,
        ForecastPreparationError::Capacity => ServiceError::ResourceExhausted,
        ForecastPreparationError::Cancelled => ServiceError::Cancelled,
        ForecastPreparationError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ForecastPreparationError::TimeUnavailable | ForecastPreparationError::Unavailable => {
            ServiceError::Unavailable
        }
    }
}
