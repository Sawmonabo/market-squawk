//! Closed durable recipe for the exact authority-minted forecast request.

use market_squawk_services::{JsonStructureLimits, ServiceLimits};
use serde::{Deserialize, Serialize};

use super::*;
use crate::application::model::forecast::forecast_recovery_coordinates;

const INPUT_REVISION: u16 = 1;
pub(crate) const MAXIMUM_FORECAST_JOB_INPUT_BYTES: usize = MAXIMUM_SINGLE_REQUEST_BYTES + 16_384;

/// Created only by consuming an owner-bound preparation receipt or checking its controlled bytes.
#[derive(Debug)]
pub(crate) struct PreparedForecastJobInput {
    request: TypedToolRequest,
    recipe: RetainedRecipe,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RetainedRecipe {
    revision: u16,
    operation: String,
    descriptor_version: String,
    arguments: Map<String, Value>,
    owner_workspace: Uuid,
    owner_client: Uuid,
    workspace: WorkspaceRuntimeIdentity,
    runtime_generation_sha256: [u8; 32],
    authority_generation_sha256: [u8; 32],
    evidence_sha256: [u8; 32],
    request_sha256: [u8; 32],
    financial_profile_digest: Option<[u8; 32]>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct RetainedInput {
    recipe: RetainedRecipe,
    // Four result ceilings followed by the four exact JSON structural ceilings.
    limits: [usize; 8],
}

impl PreparedForecastJobInput {
    pub(super) fn from_stored(
        stored: StoredForecastPreparation,
    ) -> Result<Self, ForecastPreparationError> {
        let expected = stored
            .revalidation
            .ok_or(ForecastPreparationError::InvalidEvidence)?;
        let (request_sha256, _) = request_digest(&stored.request)?;
        let recipe = RetainedRecipe {
            revision: INPUT_REVISION,
            operation: stored.request.name().to_owned(),
            descriptor_version: stored.request.version().to_owned(),
            arguments: stored.request.arguments().clone(),
            owner_workspace: stored.owner.workspace_id(),
            owner_client: stored.owner.client_id(),
            workspace: stored.workspace,
            runtime_generation_sha256: stored.runtime_generation_sha256.bytes(),
            authority_generation_sha256: expected.request.authority_generation_sha256.bytes(),
            evidence_sha256: expected.evidence_sha256.bytes(),
            request_sha256: request_sha256.bytes(),
            financial_profile_digest: stored.financial_profile_digest,
        };
        Ok(Self {
            request: stored.request,
            recipe,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "native request retains independent owner and runtime fences"
    )]
    pub(super) fn from_financial(
        request: TypedToolRequest,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        runtime_generation: Sha256Digest,
        pairing: Sha256Digest,
        request_sha256: Sha256Digest,
        financial_profile_digest: [u8; 32],
    ) -> Result<Self, ForecastPreparationError> {
        validate_origin(origin, workspace)?;
        if request_digest(&request)?.0 != request_sha256
            || financial_profile_digest == [0; 32]
            || [runtime_generation, pairing, request_sha256]
                .iter()
                .any(|value| value.bytes() == [0; 32])
        {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        let recipe = RetainedRecipe {
            revision: INPUT_REVISION,
            operation: request.name().to_owned(),
            descriptor_version: request.version().to_owned(),
            arguments: request.arguments().clone(),
            owner_workspace: origin.workspace_id(),
            owner_client: origin.client_id(),
            workspace,
            runtime_generation_sha256: runtime_generation.bytes(),
            authority_generation_sha256: pairing.bytes(),
            evidence_sha256: pairing.bytes(),
            request_sha256: request_sha256.bytes(),
            financial_profile_digest: Some(financial_profile_digest),
        };
        Ok(Self { request, recipe })
    }

    pub(crate) const fn request(&self) -> &TypedToolRequest {
        &self.request
    }

    pub(crate) fn origin(&self) -> Result<RequestOrigin, ForecastPreparationError> {
        RequestOrigin::try_new(self.recipe.owner_workspace, self.recipe.owner_client)
            .map_err(|_| ForecastPreparationError::InvalidEvidence)
    }

    pub(crate) const fn request_sha256(&self) -> [u8; 32] {
        self.recipe.request_sha256
    }
    pub(crate) const fn financial_profile_digest(&self) -> Option<[u8; 32]> {
        self.recipe.financial_profile_digest
    }

    pub(crate) fn into_bytes(
        self,
        limits: ServiceLimits,
    ) -> Result<Vec<u8>, ForecastPreparationError> {
        let structure = limits.result_structure();
        let retained = RetainedInput {
            recipe: self.recipe,
            limits: [
                limits.maximum_inline_bytes(),
                limits.maximum_inline_items(),
                limits.maximum_result_bytes(),
                limits.maximum_result_items(),
                structure.maximum_depth(),
                structure.maximum_string_bytes(),
                structure.maximum_array_items(),
                structure.maximum_map_entries(),
            ],
        };
        let bytes =
            serde_json::to_vec(&retained).map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        if bytes.len() > MAXIMUM_FORECAST_JOB_INPUT_BYTES {
            return Err(ForecastPreparationError::Capacity);
        }
        Ok(bytes)
    }
}

impl ForecastPreparationAuthority {
    /// Checks the durable recipe without substituting a fresh preparation or current input values.
    /// The caller must first resolve and verify the job-owned controlled input artifact.
    pub(crate) fn restore_job_input(
        &self,
        bytes: &[u8],
        owner: RequestOrigin,
    ) -> Result<(PreparedForecastJobInput, ServiceLimits), ForecastPreparationError> {
        if bytes.len() > MAXIMUM_FORECAST_JOB_INPUT_BYTES {
            return Err(ForecastPreparationError::Capacity);
        }
        let stored: RetainedInput =
            serde_json::from_slice(bytes).map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        let recipe = stored.recipe;
        if recipe.revision != INPUT_REVISION
            || recipe.operation != self.generate_descriptor.name()
            || recipe.descriptor_version != self.generate_descriptor.version()
            || recipe.owner_workspace != owner.workspace_id()
            || recipe.owner_client != owner.client_id()
            || recipe.workspace.workspace_id().as_uuid() != owner.workspace_id()
            || recipe.financial_profile_digest == Some([0; 32])
            || [
                recipe.runtime_generation_sha256,
                recipe.authority_generation_sha256,
                recipe.evidence_sha256,
                recipe.request_sha256,
            ]
            .contains(&[0; 32])
        {
            return Err(ForecastPreparationError::ReceiptMismatch);
        }
        // The old service generation is retained as evidence, not reused as current transport authority.
        let request = self
            .generate_descriptor
            .admit(recipe.arguments.clone())
            .map_err(|_| ForecastPreparationError::InvalidDescriptor)?;
        if request_digest(&request)?.0.bytes() != recipe.request_sha256 {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        forecast_recovery_coordinates(&request)
            .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        let [
            inline_bytes,
            inline_items,
            result_bytes,
            result_items,
            depth,
            strings,
            arrays,
            maps,
        ] = stored.limits;
        let structure = JsonStructureLimits::try_new(depth, strings, arrays, maps)
            .map_err(|_| ForecastPreparationError::InvalidLimits)?;
        let limits = ServiceLimits::try_new(
            inline_bytes,
            inline_items,
            result_bytes,
            result_items,
            structure,
        )
        .map_err(|_| ForecastPreparationError::InvalidLimits)?;
        Ok((PreparedForecastJobInput { request, recipe }, limits))
    }

    /// Rebuilds only the original exact model/data fences and verifies the retained input bytes.
    pub(crate) async fn revalidate_job_input(
        &self,
        input: &PreparedForecastJobInput,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<(), ForecastPreparationError> {
        check_control(deadline, &cancellation)?;
        self.runtime
            .validate_forecast_runtime_generation(Sha256Digest::new(
                input.recipe.runtime_generation_sha256,
            ))?;
        let coordinates = forecast_recovery_coordinates(input.request())
            .map_err(|_| ForecastPreparationError::InvalidEvidence)?;
        if coordinates.horizon.fiscal_periods().is_some() {
            if input.recipe.authority_generation_sha256 != input.recipe.evidence_sha256 {
                return Err(ForecastPreparationError::InvalidEvidence);
            }
            return self
                .revalidate_financial_job(
                    input,
                    Sha256Digest::new(input.recipe.authority_generation_sha256),
                    deadline,
                    cancellation,
                )
                .await;
        }
        let retained = self.runtime.retain_forecast_runtime()?;
        let backup = self.runtime.retain_backup()?;
        let current_selector =
            if let Some(current) = coordinates.serving_evidence.current_price_input() {
                let mut selector = ForecastCurrentFeatureInputSelection::try_new(
                    coordinates.serving_evidence.manifest().clone(),
                    &current.example_id,
                )?;
                if let Some(reference) =
                    crate::application::model::forecast::current_price_cohort_reference(current)
                        .map_err(|_| ForecastPreparationError::InvalidEvidence)?
                {
                    selector = selector.with_session_cohort(reference);
                }
                Some(selector)
            } else {
                None
            };
        let catalog_request = catalog_request(
            &retained,
            &backup,
            coordinates.serving_evidence.knowledge_cutoff(),
            current_selector.clone(),
        )?;
        let model = catalog_request
            .models
            .iter()
            .find(|model| {
                let metadata = model.metadata();
                metadata.model_id() == coordinates.model_id
                    && metadata.bundle_id() == &coordinates.bundle_id
                    && metadata.bundle_version() == coordinates.bundle_version
            })
            .cloned()
            .ok_or(ForecastPreparationError::ModelUnavailable)?;
        let mut selection = ForecastPreparationSelection::try_new(
            coordinates.model_id,
            coordinates.bundle_id,
            coordinates.bundle_version,
            model.metadata().dataset().manifest().clone(),
            coordinates.analysis_evidence.manifest().clone(),
            coordinates.instrument_id,
            coordinates.horizon,
            coordinates.validity_nanos.get(),
        )?;
        if let Some(selector) = current_selector {
            selection = selection.with_current_feature_input(selector);
        }
        let catalog = self
            .evidence
            .catalog(
                catalog_request.clone(),
                deadline,
                cancellation.child_token(),
            )
            .await?;
        validate_catalog(&catalog_request, &catalog)?;
        let option = compatible_option(&catalog, &selection)?;
        if option.pairing.pairing_sha256() != coordinates.analysis_evidence.pairing_sha256()
            || option.pairing.analysis_production_identity()
                != coordinates.analysis_evidence.production_identity_sha256()
            || option.pairing.analysis_production_receipt_sha256()
                != coordinates.analysis_evidence.production_receipt_sha256()
        {
            return Err(ForecastPreparationError::InvalidEvidence);
        }
        let serving = coordinates.serving_evidence;
        let knowledge_cutoff = serving.knowledge_cutoff();
        let serving_input = if let Some(current) = serving.current_price_input() {
            ForecastServingInputFence {
                manifest: serving.manifest().clone(),
                parent_manifests: serving.parent_manifests().into(),
                source_id: serving.source_id().clone(),
                object_graph_sha256: serving.object_graph_sha256(),
                selection_sha256: serving.selection_sha256(),
                result_sha256: serving.result_sha256(),
                knowledge_cutoff,
                prior_observed_at: None,
                observed_through: serving
                    .observed_through()
                    .ok_or(ForecastPreparationError::InvalidEvidence)?,
                feature_sha256: serving.feature_sha256(),
                origin_bar: serving.origin_bar().cloned(),
                current_price_input: Some(current.clone()),
            }
        } else {
            ForecastServingInputFence::try_new(
                serving.manifest().clone(),
                serving.source_id().clone(),
                serving.object_graph_sha256(),
                serving.selection_sha256(),
                serving.result_sha256(),
                knowledge_cutoff,
                serving
                    .prior_observed_at()
                    .ok_or(ForecastPreparationError::InvalidEvidence)?,
                serving
                    .observed_through()
                    .ok_or(ForecastPreparationError::InvalidEvidence)?,
                serving.feature_sha256(),
            )?
            .with_parent_manifests(serving.parent_manifests().to_vec())?
            .with_origin_bar(serving.origin_bar().cloned())?
        };
        let expected = ForecastEvidenceRevalidation {
            request: ForecastEvidenceMaterializationRequest {
                model: model.bind_selected_horizon(coordinates.horizon)?,
                selection,
                pairing: option.pairing.clone(),
                authority_generation_sha256: Sha256Digest::new(
                    input.recipe.authority_generation_sha256,
                ),
                knowledge_cutoff,
                macro_effective_date_cutoff: knowledge_cutoff
                    .utc_calendar_date()
                    .map_err(|_| ForecastPreparationError::TimeUnavailable)?,
            },
            serving_input,
            evidence_sha256: Sha256Digest::new(input.recipe.evidence_sha256),
        };
        self.evidence
            .revalidate(&expected, deadline, cancellation)
            .await?;
        Ok(())
    }
}
