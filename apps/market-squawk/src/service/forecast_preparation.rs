//! Installed-service adapter for evidence-derived forecast preparation.

use std::{
    collections::{BTreeMap, BTreeSet},
    num::{NonZeroU16, NonZeroU64},
    sync::Arc,
};

use market_squawk_data::{
    DatasetId, DatasetManifestRef, DatasetSchemaRef, InstrumentDefinitionReadCapability,
    Sha256Digest,
};
use market_squawk_domain::{InstrumentDefinition, InstrumentId, SchemaVersion, Timestamp};
use market_squawk_modeling::{ForecastHorizon, ModelFormat, ModelOutputSemantics};
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
        AnalyticalForecastEvidenceReader,
        lifecycle::WorkspaceRuntimeIdentity,
        model::forecast_preparation::{
            ForecastEvidenceDataset, ForecastInstrumentAvailability, ForecastModelSummary,
            ForecastPreparationAuthority, ForecastPreparationCatalog, ForecastPreparationError,
            ForecastPreparationLimits, ForecastPreparationPreview, ForecastPreparationReceipt,
            ForecastPreparationSelection, PreparedForecast,
        },
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
        capabilities: &ServiceCapabilities,
        runtime: RuntimeIdentity,
    ) -> Result<Self, ServiceError> {
        let authority = product
            .model_runtime()
            .map(|model_runtime| {
                let descriptor = capabilities
                    .find("Model.GenerateForecast")
                    .cloned()
                    .ok_or(ServiceError::Unavailable)?;
                let evidence = Arc::new(AnalyticalForecastEvidenceReader::new(
                    product.research().analytical_reader(),
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
                let item_count = catalog.models().len().max(1);
                (catalog_value(&catalog, &labels), item_count)
            }
            PREPARE_FORECAST => {
                let input: ForecastPreparationRequest =
                    decode(&super::business_arguments(request.arguments()))?;
                let selection = input.selection.try_into_domain()?;
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
        let receipt = input.receipt.try_into_domain()?;
        authority
            .consume(
                context.origin().ok_or(ServiceError::Unauthorized)?,
                self.workspace()?,
                receipt,
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
    model_id: market_squawk_domain::ModelId,
    bundle_id: String,
    bundle_version: u64,
    dataset_manifest: ManifestWire,
    instrument_id: InstrumentId,
    horizon_points: u16,
    horizon_step_nanos: String,
    validity_nanos: String,
}

impl ForecastSelectionWire {
    fn try_into_domain(self) -> Result<ForecastPreparationSelection, ServiceError> {
        ForecastPreparationSelection::try_new(
            self.model_id,
            market_squawk_modeling::BundleId::try_new(self.bundle_id)
                .map_err(|_error| ServiceError::InvalidRequest)?,
            NonZeroU64::new(self.bundle_version).ok_or(ServiceError::InvalidRequest)?,
            self.dataset_manifest.try_into_domain()?,
            self.instrument_id,
            ForecastHorizon::try_new(
                NonZeroU16::new(self.horizon_points).ok_or(ServiceError::InvalidRequest)?,
                NonZeroU64::new(parse_u64(&self.horizon_step_nanos)?)
                    .ok_or(ServiceError::InvalidRequest)?,
            )
            .map_err(|_error| ServiceError::InvalidRequest)?,
            parse_u64(&self.validity_nanos)?,
        )
        .map_err(map_preparation)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestWire {
    dataset: String,
    manifest_version: u64,
    schema: SchemaWire,
    content_hash: String,
}

impl ManifestWire {
    fn try_into_domain(self) -> Result<DatasetManifestRef, ServiceError> {
        let schema = DatasetSchemaRef::try_new(
            self.schema.name,
            SchemaVersion::new(self.schema.version).map_err(|_| ServiceError::InvalidRequest)?,
            parse_sha256(&self.schema.fingerprint)?,
        )
        .map_err(|_error| ServiceError::InvalidRequest)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset.as_str())
                .map_err(|_error| ServiceError::InvalidRequest)?,
            self.manifest_version,
            schema,
            Sha256Digest::new(parse_sha256(&self.content_hash)?),
        )
        .map_err(|_error| ServiceError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SchemaWire {
    name: String,
    version: u16,
    fingerprint: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedForecastStart {
    receipt: ForecastReceiptWire,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForecastReceiptWire {
    receipt_id: Uuid,
    receipt_sha256: String,
    expires_at_unix_nanos: String,
}

impl ForecastReceiptWire {
    fn try_into_domain(self) -> Result<ForecastPreparationReceipt, ServiceError> {
        ForecastPreparationReceipt::try_from_wire(
            self.receipt_id,
            parse_sha256(&self.receipt_sha256)?,
            Timestamp::from_unix_nanos(parse_i64(&self.expires_at_unix_nanos)?),
        )
        .map_err(map_preparation)
    }
}

fn catalog_value(
    catalog: &ForecastPreparationCatalog,
    labels: &BTreeMap<InstrumentId, String>,
) -> Value {
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
    json!({
        "runtimeGenerationSha256": hex(catalog.runtime_generation_sha256()),
        "models": models,
    })
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
    let manifest = dataset.dataset().manifest();
    json!({
        "manifest": manifest_value(manifest),
        "label": manifest.dataset_id().as_str(),
        "instruments": dataset.instruments().iter().map(|instrument| json!({
            "instrumentId": instrument.instrument_id().to_string(),
            "label": labels.get(&instrument.instrument_id()).cloned().unwrap_or_else(|| instrument.instrument_id().to_string()),
            "observedFromUnixNanos": instrument.observed_from().unix_nanos().to_string(),
            "observedThroughUnixNanos": instrument.observed_through().unix_nanos().to_string(),
            "availableAtUnixNanos": instrument.available_at().unix_nanos().to_string(),
            "observedPoints": instrument.observed_points().get(),
            "decimalScale": instrument.decimal_scale(),
        })).collect::<Vec<_>>(),
        "policies": dataset.policies().iter().map(|policy| json!({
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
            "receiptId": receipt.receipt_id(),
            "receiptSha256": hex(receipt.receipt_sha256()),
            "expiresAtUnixNanos": receipt.expires_at().unix_nanos().to_string(),
        },
        "preview": preview_value(preview, instrument_label),
    })
}

fn preview_value(preview: &ForecastPreparationPreview, instrument_label: &str) -> Value {
    json!({
        "model": model_value(preview.model(), None),
        "instrumentId": preview.instrument_id().to_string(),
        "instrumentLabel": instrument_label,
        "observedFromUnixNanos": preview.observed_from().unix_nanos().to_string(),
        "observedThroughUnixNanos": preview.observed_through().unix_nanos().to_string(),
        "availableAtUnixNanos": preview.available_at().unix_nanos().to_string(),
        "observedPoints": preview.observed_points(),
        "horizonPoints": preview.horizon().points().get(),
        "horizonStepNanos": preview.horizon().step_nanos().get().to_string(),
        "validityNanos": preview.validity_nanos().to_string(),
        "evidenceSha256": hex(preview.evidence_sha256()),
        "requestSha256": hex(preview.request_sha256()),
        "runtimeGenerationSha256": hex(preview.runtime_generation_sha256()),
    })
}

fn model_value(model: &ForecastModelSummary, datasets: Option<Vec<Value>>) -> Value {
    let mut value = Map::from_iter([
        ("modelId".to_owned(), json!(model.model_id().to_string())),
        ("bundleId".to_owned(), json!(model.bundle_id().as_str())),
        (
            "bundleVersion".to_owned(),
            json!(model.bundle_version().get()),
        ),
        (
            "metadataSha256".to_owned(),
            json!(hex(model.metadata_sha256())),
        ),
        (
            "artifactSha256".to_owned(),
            json!(hex(model.artifact_sha256())),
        ),
        (
            "datasetExportSha256".to_owned(),
            json!(hex(model.dataset_export_sha256())),
        ),
        (
            "datasetPolicySha256".to_owned(),
            json!(hex(model.dataset_policy_sha256())),
        ),
        ("featureCount".to_owned(), json!(model.feature_count())),
        (
            "hasCalibratedIntervals".to_owned(),
            json!(model.has_calibrated_intervals()),
        ),
        ("format".to_owned(), json!(format_name(model.format()))),
        (
            "outputSemantics".to_owned(),
            json!(semantics_name(model.output_semantics())),
        ),
        ("intendedUse".to_owned(), json!(model.intended_use())),
        ("limitations".to_owned(), json!(model.limitations())),
        ("fallbackReason".to_owned(), json!(model.fallback_reason())),
    ]);
    if let Some(datasets) = datasets {
        value.insert("datasets".to_owned(), Value::Array(datasets));
    }
    Value::Object(value)
}

fn manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({
        "dataset": manifest.dataset_id().as_str(),
        "manifestVersion": manifest.manifest_version(),
        "schema": {
            "name": manifest.schema().name(),
            "version": manifest.schema().version().get(),
            "fingerprint": encode_bytes(manifest.schema().fingerprint()),
        },
        "contentHash": hex(manifest.content_hash()),
    })
}

fn definition_label(definition: &InstrumentDefinition) -> String {
    let mut mappings = definition
        .venue_mappings()
        .iter()
        .map(|mapping| format!("{} · {}", mapping.venue_symbol(), mapping.venue_id()))
        .collect::<Vec<_>>();
    mappings.sort_unstable();
    mappings
        .into_iter()
        .next()
        .unwrap_or_else(|| definition.instrument_id().to_string())
}

const fn format_name(value: ModelFormat) -> &'static str {
    match value {
        ModelFormat::NativeLinear => "native_linear",
        ModelFormat::NativeLogistic => "native_logistic",
        ModelFormat::Onnx => "onnx",
    }
}

const fn semantics_name(value: ModelOutputSemantics) -> &'static str {
    match value {
        ModelOutputSemantics::Regression => "regression",
        ModelOutputSemantics::BinaryProbability => "binary_probability",
    }
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(arguments.clone()))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn parse_u64(value: &str) -> Result<u64, ServiceError> {
    value.parse().map_err(|_error| ServiceError::InvalidRequest)
}

fn parse_i64(value: &str) -> Result<i64, ServiceError> {
    value.parse().map_err(|_error| ServiceError::InvalidRequest)
}

fn parse_sha256(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64 {
        return Err(ServiceError::InvalidRequest);
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(bytes)
}

fn hex_nibble(value: u8) -> Result<u8, ServiceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(ServiceError::InvalidRequest),
    }
}

fn hex(value: Sha256Digest) -> String {
    encode_bytes(value.bytes())
}

fn encode_bytes(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
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
