//! Native financial preparation through the same selected model and durable forecast job.

use market_squawk_data::{
    DatasetBuildPurpose, FeatureDatasetInputEpochOutput, FeatureLabelMeasurement,
    ForecastFeatureValue,
};
use market_squawk_domain::HistoricalStudyBasis;
use market_squawk_modeling::{ForecastMeasurement, ForecastTargetMeaning};

use super::*;
use crate::application::model::{
    forecast::{
        ForecastServingEvidence, financial_analysis_evidence, financial_coordinate_index,
        financial_feature_values, forecast_recovery_coordinates,
    },
    runtime::ModelAdmissionReceipt,
};

impl ForecastPreparationAuthority {
    /// Creates the existing job input from an actually completed training job and an exact
    /// source-authenticated, explicitly retrospective StudyInputs generation. The caller supplies no financial scalars.
    #[allow(
        clippy::too_many_arguments,
        reason = "independent owner and source identities remain explicit"
    )]
    pub(crate) async fn prepare_financial_job(
        &self,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        admitted_model: &ModelAdmissionReceipt,
        manifest: &DatasetManifestRef,
        example_id: &str,
        product_identity: ForecastProductIdentity,
        financial_profile_digest: [u8; 32],
        validity_nanos: NonZeroU64,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedForecastJobInput, ForecastPreparationError> {
        validate_origin(origin, workspace)?;
        check_control(deadline, &cancellation)?;
        if financial_profile_digest == [0; 32]
            || validity_nanos.get() > MAXIMUM_FORECAST_VALIDITY_NANOS
        {
            return Err(ForecastPreparationError::InvalidSelection);
        }
        let retained = self.runtime.retain_forecast_runtime()?;
        let bundle = retained
            .image
            .registry
            .selection(
                admitted_model.bundle_id(),
                admitted_model.bundle_version(),
                deadline,
                &cancellation,
            )?
            .ok_or(ForecastPreparationError::ModelUnavailable)?;
        let model = model_requirement(&retained, &bundle)?;
        drop(bundle);
        let metadata = model.metadata();
        if metadata.model_id() != admitted_model.model_id()
            || metadata.metadata_hash() != admitted_model.metadata_sha256()
            || metadata.artifact_hash() != admitted_model.artifact_sha256()
            || metadata.training_run_hash() != admitted_model.training_run_sha256()
            || metadata.dataset().selection_digest() != admitted_model.dataset_selection_sha256()
        {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        let output = self
            .evidence
            .financial_input(manifest, deadline, cancellation.child_token())
            .await?;
        let mut indices = output
            .epochs()
            .iter()
            .enumerate()
            .filter(|(_, epoch)| epoch.example_id() == example_id);
        let (index, epoch) = indices
            .next()
            .ok_or(ForecastPreparationError::InvalidSelection)?;
        if indices.next().is_some()
            || epoch.basis() != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
            || epoch.purpose() != DatasetBuildPurpose::StudyInputs
            || epoch.source_selection_as_of() > wall_now()?
            || product_identity.knowledge_at() != epoch.source_selection_as_of()
            || product_identity.effective_at() != epoch.source_selection_as_of()
        {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        let identity = json!({
            "displayName": product_identity.display_name(),
            "canonicalSymbol": product_identity.canonical_symbol(),
            "description": product_identity.description(),
            "quoteCurrency": product_identity.quote_currency().as_str(),
            "knowledgeAtUnixNanos": product_identity.knowledge_at().unix_nanos(),
            // This is the real identity selection timestamp, never a fiscal period converted
            // into an invented midnight. The fiscal target stays exclusively in its native epoch.
            "effectiveAtUnixNanos": product_identity.effective_at().unix_nanos(),
        });
        let (arguments, pairing) =
            financial_arguments(&model, &output, index, identity, validity_nanos)?;
        let request = self
            .generate_descriptor
            .admit(arguments)
            .map_err(|_| ForecastPreparationError::InvalidDescriptor)?;
        forecast_recovery_coordinates(&request)
            .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        let (request_sha256, bytes) = request_digest(&request)?;
        if bytes > MAXIMUM_SINGLE_REQUEST_BYTES {
            return Err(ForecastPreparationError::Capacity);
        }
        check_control(deadline, &cancellation)?;
        self.runtime
            .validate_forecast_runtime_generation(retained.generation_sha256)?;
        PreparedForecastJobInput::from_financial(
            request,
            origin,
            workspace,
            retained.generation_sha256,
            pairing,
            request_sha256,
            financial_profile_digest,
        )
    }

    /// Reopens the original native generation and rebuilds the entire retained request. It never
    /// substitutes current facts or retrains a model during recovery.
    pub(super) async fn revalidate_financial_job(
        &self,
        input: &PreparedForecastJobInput,
        authority_identity: Sha256Digest,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ForecastPreparationError> {
        let coordinates = forecast_recovery_coordinates(input.request())
            .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        let retained = self.runtime.retain_forecast_runtime()?;
        let bundle = retained
            .image
            .registry
            .selection(
                &coordinates.bundle_id,
                coordinates.bundle_version,
                deadline,
                &cancellation,
            )?
            .ok_or(ForecastPreparationError::ModelUnavailable)?;
        let model = model_requirement(&retained, &bundle)?;
        drop(bundle);
        if model.metadata().model_id() != coordinates.model_id {
            return Err(ForecastPreparationError::ModelUnavailable);
        }
        let output = self
            .evidence
            .financial_input(
                coordinates.serving_evidence.manifest(),
                deadline,
                cancellation.child_token(),
            )
            .await?;
        let index = financial_coordinate_index(&output, &coordinates.serving_evidence)
            .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        let original_identity = input
            .request()
            .arguments()
            .get("request")
            .and_then(Value::as_object)
            .and_then(|request| request.get("productIdentity"))
            .cloned()
            .ok_or(ForecastPreparationError::InvalidEvidence)?;
        let (arguments, pairing) = financial_arguments(
            &model,
            &output,
            index,
            original_identity,
            coordinates.validity_nanos,
        )?;
        if &arguments != input.request().arguments() || pairing != authority_identity {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        check_control(deadline, &cancellation)
    }
}

fn financial_arguments(
    model: &ForecastModelRequirement,
    output: &FeatureDatasetInputEpochOutput,
    index: usize,
    product_identity: Value,
    validity: NonZeroU64,
) -> Result<(Map<String, Value>, Sha256Digest), ForecastPreparationError> {
    let coordinate = output
        .coordinate(index)
        .ok_or(ForecastPreparationError::InvalidEvidence)?;
    let epoch = coordinate.epoch();
    let period = epoch
        .financial_period()
        .ok_or(ForecastPreparationError::InvalidEvidence)?;
    let metadata = model.metadata();
    let distance = period
        .target_ordinal()
        .checked_sub(period.observed_ordinal())
        .and_then(|value| u16::try_from(value).ok())
        .and_then(NonZeroU16::new)
        .ok_or(ForecastPreparationError::InvalidEvidence)?;
    let horizon = ForecastHorizon::try_fiscal(period.cadence(), distance)
        .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
    let FeatureLabelMeasurement::FinancialAmount {
        currency,
        role,
        basis,
        share_convention,
    } = epoch
        .financial_measurement()
        .ok_or(ForecastPreparationError::InvalidEvidence)?
    else {
        return Err(ForecastPreparationError::InvalidEvidence);
    };
    if metadata.output_binding().measurement()
        != (ForecastMeasurement::FinancialAmount {
            currency,
            role,
            basis,
            share_convention,
        })
        || metadata.output_binding().target()
            != (ForecastTargetMeaning::FinancialPeriod {
                cadence: period.cadence(),
                periods_ahead: distance,
            })
        || metadata.dataset().universe_digest() != output.dataset().universe_digest()
        || metadata.dataset().selection_as_of() > epoch.source_selection_as_of()
        || epoch.basis() != HistoricalStudyBasis::RetrospectiveFrozenSnapshot
        || epoch.purpose() != DatasetBuildPurpose::StudyInputs
        || product_identity
            .get("quoteCurrency")
            .and_then(Value::as_str)
            != Some(currency.as_str())
        || product_identity
            .get("knowledgeAtUnixNanos")
            .and_then(Value::as_i64)
            != Some(epoch.source_selection_as_of().unix_nanos())
        || product_identity
            .get("effectiveAtUnixNanos")
            .and_then(Value::as_i64)
            != Some(epoch.source_selection_as_of().unix_nanos())
    {
        return Err(ForecastPreparationError::IncompatibleSelection);
    }
    let selected_model = model.bind_selected_horizon(horizon)?;
    if selected_model.product_evidence().overall() == ForecastModelEvidenceState::Unavailable {
        return Err(ForecastPreparationError::ModelUnavailable);
    }
    let values = financial_feature_values(metadata, coordinate)
        .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
    let decimal_scale = match coordinate.rows().first().map(|row| row.value()) {
        Some(ForecastFeatureValue::Decimal { scale, .. })
            if *scale <= market_squawk_modeling::MAX_FORECAST_DECIMAL_SCALE =>
        {
            *scale
        }
        _ => return Err(ForecastPreparationError::InvalidEvidence),
    };
    let serving = ForecastServingEvidence::from_financial_output(output, index)
        .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
    let analysis = financial_analysis_evidence(metadata, output.dataset())
        .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
    let pairing = analysis.pairing_sha256();
    let arguments = Map::from_iter([
        ("confirm".into(), Value::Bool(true)),
        ("modelId".into(), json!(metadata.model_id().to_string())),
        (
            "resultLimits".into(),
            json!({"maximumItems":market_squawk_modeling::MAX_FORECAST_POINTS,
            "maximumBytes":4*1024*1024}),
        ),
        (
            "request".into(),
            json!({
                "instrumentId":epoch.instrument_id().to_string(),
                "productIdentity":product_identity,
                "modelEvidence":selected_model.product_evidence().product_value(),
                "bundleId":metadata.bundle_id().as_str(),
                "bundleVersion":metadata.bundle_version().get(),
                "observedThroughUnixNanos":null,
                "availableAtUnixNanos":epoch.source_selection_as_of().unix_nanos(),
                "horizonPoints":1,"horizonStepNanos":null,
                "fiscalHorizon":{"cadence":period.cadence(),"periodsAhead":distance.get()},
                "decimalScale":decimal_scale,"validityNanos":validity.get(),
                "observedHistory":[],"inputs":[values],
                "analysisEvidence":{
                    "manifest":manifest_value(analysis.manifest()),
                    "productionIdentitySha256":hex(analysis.production_identity_sha256()),
                    "productionReceiptSha256":hex(analysis.production_receipt_sha256()),
                    "pairingSha256":hex(pairing),
                },
                "servingEvidence":{
                    "manifest":manifest_value(serving.manifest()),
                    "parentManifests":serving.parent_manifests().iter().map(manifest_value).collect::<Vec<_>>(),
                    "sourceId":serving.source_id().as_str(),
                    "objectGraphSha256":hex(serving.object_graph_sha256()),
                    "selectionSha256":hex(serving.selection_sha256()),
                    "resultSha256":hex(serving.result_sha256()),
                    "knowledgeCutoffUnixNanos":serving.knowledge_cutoff().unix_nanos(),
                    "priorObservedAtUnixNanos":null,"observedThroughUnixNanos":null,
                    "featureSha256":hex(serving.feature_sha256()),"originBar":null,
                    "financialInput":serving.financial_input(),
                    "currentPriceInput":null,
                }
            }),
        ),
    ]);
    Ok((arguments, pairing))
}

fn manifest_value(manifest: &DatasetManifestRef) -> Value {
    json!({"dataset":manifest.dataset_id().as_str(),
        "manifestVersion":manifest.manifest_version(),
        "schema":{"name":manifest.schema().name(),"version":manifest.schema_version().get(),
            "fingerprint":hex(Sha256Digest::new(manifest.schema().fingerprint()))},
        "contentHash":hex(manifest.content_hash())})
}
