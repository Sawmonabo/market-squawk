//! Installed-service adapter for evidence-derived forecast preparation.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use market_squawk_domain::{AssetClass, InstrumentId, Timestamp};
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
        AnalyticalForecastEvidenceReader, InstrumentContext, InstrumentContextOutcome,
        InstrumentContextReadCapability, InstrumentContextRequest,
        internal_forecast_generation_descriptor,
        lifecycle::WorkspaceRuntimeIdentity,
        model::forecast::{ForecastProductHorizon, ForecastProductIdentity, ForecastProductTarget},
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
    instruments: Option<InstrumentContextReadCapability>,
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
            instruments: product.instrument_context_read_capability(),
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
                let identities = self.identities_for_catalog(&catalog, context)?;
                catalog_value(&catalog, &identities)?
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
                let instruments = self.instruments.as_ref().ok_or(ServiceError::Unavailable)?;
                let prepared = authority
                    .prepare(
                        origin,
                        workspace,
                        selection.selection.clone(),
                        |instrument_id, knowledge_at, effective_at| {
                            resolve_product_identity(
                                instruments,
                                instrument_id,
                                knowledge_at,
                                effective_at,
                                context,
                            )
                            .map_err(|_| ForecastPreparationError::Unavailable)
                        },
                        context.deadline(),
                        context.cancellation().clone(),
                    )
                    .await
                    .map_err(map_preparation)?;
                (prepared_value(&prepared, &selection)?, 1)
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

    fn identities_for_catalog(
        &self,
        catalog: &ForecastPreparationCatalog,
        context: &RequestContext,
    ) -> Result<BTreeMap<(InstrumentId, i64, i64), ForecastProductIdentity>, ServiceError> {
        let coordinates = catalog
            .evidence()
            .datasets()
            .iter()
            .flat_map(|dataset| dataset.instruments().iter())
            .map(|instrument| {
                (
                    instrument.instrument_id(),
                    instrument.available_at().unix_nanos(),
                    instrument.observed_through().unix_nanos(),
                )
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if coordinates.len() > MAXIMUM_CATALOG_INSTRUMENTS {
            return Err(ServiceError::ResourceExhausted);
        }
        let instruments = self.instruments.as_ref().ok_or(ServiceError::Unavailable)?;
        coordinates
            .into_iter()
            .map(|coordinate @ (instrument_id, knowledge_at, effective_at)| {
                resolve_product_identity(
                    instruments,
                    instrument_id,
                    Timestamp::from_unix_nanos(knowledge_at),
                    Timestamp::from_unix_nanos(effective_at),
                    context,
                )
                .map(|identity| (coordinate, identity))
            })
            .collect()
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
    investment_token: Uuid,
    horizon_token: Uuid,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedForecastStart {
    confirmation_token: Uuid,
}

#[derive(Clone)]
struct ResolvedForecastSelection {
    selection: ForecastPreparationSelection,
    investment_token: Uuid,
    horizon_label: String,
    horizon_description: String,
}

fn resolve_selection(
    catalog: &ForecastPreparationCatalog,
    selection: ForecastSelectionWire,
) -> Result<ResolvedForecastSelection, ServiceError> {
    let retained_investment_token = selection.investment_token;
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
    if histories.next().is_some() {
        return Err(ServiceError::InvalidRequest);
    }
    let mut investments = history.instruments().iter().filter(|investment| {
        investment_token(history, investment.instrument_id()) == selection.investment_token
    });
    let investment = investments.next().ok_or(ServiceError::InvalidRequest)?;
    if investments.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    let mut policies = history
        .policies()
        .iter()
        .copied()
        .filter(|policy| horizon_token(history, *policy) == selection.horizon_token);
    let policy = policies.next().ok_or(ServiceError::InvalidRequest)?;
    if policies.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    let horizon =
        ForecastHorizon::try_new(policy.maximum_horizon_points(), policy.horizon_step_nanos())
            .map_err(|_| ServiceError::InvalidResult)?;
    let product_horizon = ForecastProductHorizon::try_from_horizon(horizon)
        .map_err(|_| ServiceError::InvalidResult)?;
    let horizon_label = product_horizon.label().to_owned();
    let horizon_description = product_horizon.description().to_owned();
    let selection = ForecastPreparationSelection::try_new(
        model.model_id(),
        model.bundle_id().clone(),
        model.bundle_version(),
        history.dataset().manifest().clone(),
        history.analysis_manifest().clone(),
        investment.instrument_id(),
        horizon,
        policy.maximum_validity_nanos().get(),
    )
    .map_err(map_preparation)?;
    Ok(ResolvedForecastSelection {
        selection,
        investment_token: retained_investment_token,
        horizon_label,
        horizon_description,
    })
}

fn model_token(model: &ForecastModelSummary) -> Uuid {
    model.product_evidence().model_token()
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

fn investment_token(dataset: &ForecastEvidenceDataset, instrument_id: InstrumentId) -> Uuid {
    let history_token = history_token(dataset);
    opaque_product_token(
        b"market-squawk/forecast-investment-choice/v1\0",
        &[history_token.as_bytes(), instrument_id.as_uuid().as_bytes()],
    )
}

fn horizon_token(dataset: &ForecastEvidenceDataset, policy: ForecastEvidencePolicy) -> Uuid {
    let history_token = history_token(dataset);
    let maximum_horizon_points = policy.maximum_horizon_points().get().to_be_bytes();
    let horizon_step_nanos = policy.horizon_step_nanos().get().to_be_bytes();
    let maximum_validity_nanos = policy.maximum_validity_nanos().get().to_be_bytes();
    let minimum_observed_points = policy.minimum_observed_points().get().to_be_bytes();
    let components: [&[u8]; 5] = [
        history_token.as_bytes(),
        &maximum_horizon_points,
        &horizon_step_nanos,
        &maximum_validity_nanos,
        &minimum_observed_points,
    ];
    opaque_product_token(b"market-squawk/forecast-horizon-choice/v1\0", &components)
}

fn catalog_value(
    catalog: &ForecastPreparationCatalog,
    identities: &BTreeMap<(InstrumentId, i64, i64), ForecastProductIdentity>,
) -> Result<(Value, usize), ServiceError> {
    let models = catalog
        .models()
        .iter()
        .map(|model| {
            let datasets = catalog
                .evidence()
                .datasets()
                .iter()
                .filter(|dataset| dataset_matches_model(dataset, model))
                .map(|dataset| dataset_value(dataset, identities))
                .collect::<Result<Vec<_>, _>>()?;
            if datasets.is_empty() {
                Ok(None)
            } else {
                Ok(model_value(model, Some(datasets)))
            }
        })
        .collect::<Result<Vec<_>, ServiceError>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let item_count = models.len();
    Ok((json!({ "models": models }), item_count))
}

fn dataset_matches_model(dataset: &ForecastEvidenceDataset, model: &ForecastModelSummary) -> bool {
    dataset.model_id() == model.model_id()
        && dataset.bundle_id() == model.bundle_id()
        && dataset.bundle_version() == model.bundle_version()
        && dataset.dataset().manifest() == model.dataset_manifest()
}

fn dataset_value(
    dataset: &ForecastEvidenceDataset,
    identities: &BTreeMap<(InstrumentId, i64, i64), ForecastProductIdentity>,
) -> Result<Value, ServiceError> {
    let investments = dataset
        .instruments()
        .iter()
        .map(|instrument| {
            let identity = identities
                .get(&(
                    instrument.instrument_id(),
                    instrument.available_at().unix_nanos(),
                    instrument.observed_through().unix_nanos(),
                ))
                .ok_or(ServiceError::InvalidResult)?;
            Ok(json!({
                "investmentToken": investment_token(dataset, instrument.instrument_id()),
                "label": identity.display_name(),
                "observedFromUnixNanos": instrument.observed_from().unix_nanos().to_string(),
                "observedThroughUnixNanos": instrument.observed_through().unix_nanos().to_string(),
                "availableAtUnixNanos": instrument.available_at().unix_nanos().to_string(),
                "observationCount": instrument.observed_points().get(),
            }))
        })
        .collect::<Result<Vec<_>, ServiceError>>()?;
    Ok(json!({
        "historyToken": history_token(dataset),
        "label": "Verified point-in-time investment history",
        "investments": investments,
        "horizons": dataset.policies().iter().filter_map(|policy| {
            let horizon = ForecastHorizon::try_new(
                policy.maximum_horizon_points(),
                policy.horizon_step_nanos(),
            ).ok()?;
            let product = ForecastProductHorizon::try_from_horizon(horizon).ok()?;
            Some(json!({
                "horizonToken": horizon_token(dataset, *policy),
                "label": product.label(),
                "description": product.description(),
            }))
        }).collect::<Vec<_>>(),
    }))
}

fn prepared_value(
    prepared: &PreparedForecast,
    resolved: &ResolvedForecastSelection,
) -> Result<Value, ServiceError> {
    let receipt = prepared.receipt();
    let preview = prepared.preview();
    let limitations = preview_limitations(preview);
    Ok(json!({
        "confirmationToken": receipt.receipt_id(),
        "expiresAtUnixNanos": receipt.expires_at().unix_nanos().to_string(),
        "model": model_value(preview.model(), None).ok_or(ServiceError::InvalidResult)?,
        "investmentToken": resolved.investment_token,
        "instrumentLabel": prepared.product_identity().display_name(),
        "observedFromUnixNanos": preview.observed_from().unix_nanos().to_string(),
        "observedThroughUnixNanos": preview.observed_through().unix_nanos().to_string(),
        "availableAtUnixNanos": preview.available_at().unix_nanos().to_string(),
        "observationCount": preview.observed_points(),
        "horizon": {
            "label": resolved.horizon_label,
            "description": resolved.horizon_description,
        },
        "limitations": limitations,
        "analysisOnly": true,
    }))
}

fn preview_limitations(preview: &ForecastPreparationPreview) -> Vec<String> {
    let mut limitations = preview
        .model()
        .limitations()
        .iter()
        .map(|limitation| limitation.to_string())
        .collect::<Vec<_>>();
    if !preview.model().has_calibrated_intervals() {
        limitations.push(
            "Calibrated forecast ranges are unavailable, so this forecast must be treated as limited evidence."
                .to_owned(),
        );
    }
    limitations.push(
        "A forecast is uncertain investment research, not a promise of profit or permission to trade."
            .to_owned(),
    );
    limitations.sort_unstable();
    limitations.dedup();
    limitations
}

fn model_value(model: &ForecastModelSummary, datasets: Option<Vec<Value>>) -> Option<Value> {
    let (name, objective) = match model.output_semantics() {
        ModelOutputSemantics::Regression => ("Numeric outcome forecast", "numeric_outcome"),
        ModelOutputSemantics::BinaryProbability => ("Likelihood estimate", "likelihood"),
    };
    let mut value = serde_json::Map::from_iter([
        ("modelToken".to_owned(), json!(model_token(model))),
        ("name".to_owned(), json!(name)),
        ("objective".to_owned(), json!(objective)),
        ("target".to_owned(), target_value(model)?),
        (
            "modelEvidence".to_owned(),
            model.product_evidence().product_value(),
        ),
        ("intendedUse".to_owned(), json!(model.intended_use())),
        ("limitations".to_owned(), json!(model.limitations())),
        ("unavailableBehavior".to_owned(), json!("no_action")),
    ]);
    if let Some(datasets) = datasets {
        value.insert("histories".to_owned(), Value::Array(datasets));
    }
    Some(Value::Object(value))
}

fn resolve_product_identity(
    instruments: &InstrumentContextReadCapability,
    instrument_id: InstrumentId,
    knowledge_at: Timestamp,
    effective_at: Timestamp,
    context: &RequestContext,
) -> Result<ForecastProductIdentity, ServiceError> {
    let request = InstrumentContextRequest::try_new(instrument_id, knowledge_at, effective_at)
        .map_err(|_| ServiceError::InvalidResult)?;
    let read = instruments
        .read(request, context.deadline(), context.cancellation())
        .map_err(|_| ServiceError::Unavailable)?;
    let InstrumentContextOutcome::Exact(identity) = read.outcome() else {
        return Err(ServiceError::Unavailable);
    };
    ForecastProductIdentity::try_new(
        identity.display_name(),
        Some(identity.listed_symbol()),
        investment_description(identity),
        identity.quote_currency(),
        knowledge_at,
        effective_at,
    )
    .map_err(|_| ServiceError::InvalidResult)
}

fn investment_description(identity: &InstrumentContext) -> &'static str {
    if identity.exchange_traded_fund() {
        return "Exchange-traded fund with point-in-time verified listing identity.";
    }
    match identity.asset_class() {
        AssetClass::Equity => "Listed company investment with point-in-time verified identity.",
        AssetClass::FixedIncome => "Fixed-income investment with point-in-time verified identity.",
        AssetClass::Option => "Listed option with point-in-time verified identity.",
        AssetClass::Future => "Futures investment with point-in-time verified identity.",
        AssetClass::ForeignExchange => {
            "Foreign-exchange investment with point-in-time verified identity."
        }
        AssetClass::Crypto => "Crypto investment with point-in-time verified identity.",
        AssetClass::Commodity => "Commodity investment with point-in-time verified identity.",
        AssetClass::Fund => "Fund investment with point-in-time verified identity.",
        AssetClass::Index => "Market index with point-in-time verified identity.",
        AssetClass::Cash => "Cash investment with point-in-time verified identity.",
    }
}

fn target_value(model: &ForecastModelSummary) -> Option<Value> {
    let target = ForecastProductTarget::try_from_binding(model.output_binding()).ok()?;
    Some(json!({
        "label": target.label(),
        "meaning": target.meaning(),
        "valueKind": target.value_kind(),
        "unitLabel": target.unit_label(),
        "currencyCode": target.currency_code(),
    }))
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
