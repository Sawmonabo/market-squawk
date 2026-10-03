//! Exact completed-dataset job to the existing product-owned training admission.

use market_squawk_data::{
    FeatureDatasetProductContract, PythonDatasetSelection, PythonDatasetVerificationLimits,
    Sha256Digest, verify_python_dataset,
};
use market_squawk_domain::Timestamp;
use market_squawk_jobs::{
    JobGeneration, JobId, JobRepository, JobSnapshot, JobState, SqliteJobRepository,
};
use market_squawk_platform::LocalPaths;
use market_squawk_services::{
    ArtifactReadContext, ArtifactReadRequest, ArtifactRepository, RequestContext, ServiceError,
    ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::json;
use std::{num::NonZeroUsize, sync::Arc};

use crate::{
    LocalProduct,
    application::{
        analytical_profile::{AnalyticalProfileResolution, revalidate},
        job::JobReceipt,
    },
    jobs::{InstalledJobAuthority, PreparedProductTraining, TrainingJobRunner},
};

pub(crate) const START_PREPARED_TRAINING: &str = "Model.StartPreparedTraining";
pub(crate) const GET_TRAINING_RESULT: &str = "Model.GetTrainingJobResult";
pub(crate) const START_INVESTMENT_DATASET: &str = "Analysis.StartInvestmentDataset";
pub(crate) const GET_DATASET_RESULT: &str = "Analysis.GetPreparedDatasetJobResult";
const RESULT_AUTHORITY: &str = "analysis.phase-one-feature-derived-generation.v1";
const DATASET_KIND: &str = "analysis.phase-one-feature-derived-generation-job.v1";
const MAXIMUM_DATASET_RESULT_BYTES: usize = 16 * 1024;

/// Cloned handles to the existing owners; this contains no registry or pending state.
#[derive(Clone)]
pub(crate) struct InstalledProductTraining {
    paths: LocalPaths,
    research: Arc<crate::ResearchService>,
    instruments: Option<Arc<crate::application::InstrumentContextReadCapability>>,
    artifacts: Arc<dyn ArtifactRepository>,
    jobs: Arc<SqliteJobRepository>,
}
impl InstalledProductTraining {
    pub(crate) fn new(product: &LocalProduct, jobs: &InstalledJobAuthority) -> Self {
        Self {
            instruments: product.instrument_context_read_capability().map(Arc::new),
            paths: product.paths().clone(),
            research: product.research(),
            artifacts: product.artifacts(),
            jobs: jobs.repository(),
        }
    }
    pub(crate) async fn snapshot(
        &self,
        id: &str,
        generation: u64,
        context: &RequestContext,
    ) -> Result<JobSnapshot, ServiceError> {
        super::ensure_live(context)?;
        let id = JobId::try_from_str(id).map_err(|_| ServiceError::InvalidRequest)?;
        let generation =
            JobGeneration::try_new(generation).map_err(|_| ServiceError::InvalidRequest)?;
        let snapshot = tokio::select! { biased;
            _=context.cancellation().cancelled()=>return Err(ServiceError::Cancelled),
            _=tokio::time::sleep_until(context.deadline().into())=>return Err(ServiceError::DeadlineExceeded),
            value=self.jobs.get(id,generation)=>value.map_err(|_|ServiceError::Unavailable)?,
        };
        check_origin(&snapshot, context)?;
        Ok(snapshot)
    }
    pub(super) async fn prepare(
        &self,
        runner: &TrainingJobRunner,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<PreparedProductTraining, ServiceError> {
        let input: StartInput = super::decode(request.arguments())?;
        let profile = revalidate(&input.financial_profile, None).map_err(ServiceError::from)?;
        let snapshot = self
            .snapshot(&input.dataset_job_id, input.dataset_job_generation, context)
            .await?;
        let selection = self.reopen_training_dataset(&snapshot, context).await?;
        super::ensure_live(context)?;
        runner
            .prepare_product(&selection, selection.identity().manifest(), &profile, None)
            .map_err(super::map_training_admission)
    }

    pub(crate) async fn completed_model(
        &self,
        runner: &TrainingJobRunner,
        job_id: &str,
        generation: u64,
        context: &RequestContext,
    ) -> Result<
        (
            crate::application::model::runtime::ModelAdmissionReceipt,
            uuid::Uuid,
        ),
        ServiceError,
    > {
        let snapshot = self.snapshot(job_id, generation, context).await?;
        let receipt = runner
            .resolve_completed(&snapshot)
            .map_err(super::map_training_admission)?;
        let token = runner
            .completed_model_token(&receipt)
            .map_err(super::map_training_admission)?;
        super::ensure_live(context)?;
        Ok((receipt, token))
    }
    pub(super) async fn prepare_dataset(
        &self,
        authority: &super::super::research_dataset::InstalledResearchDatasetPreparation,
        runtime: market_squawk_runtime::RuntimeIdentity,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<crate::application::PreparedFeatureDatasetBuild, ServiceError> {
        let input: DatasetStartInput = super::decode(request.arguments())?;
        let cutoff = input
            .source_cutoff_unix_nanos
            .parse::<i64>()
            .map_err(|_| ServiceError::InvalidRequest)?;
        if cutoff.to_string() != input.source_cutoff_unix_nanos {
            return Err(ServiceError::InvalidRequest);
        }
        let cutoff = Timestamp::from_unix_nanos(cutoff);
        let profile = revalidate(&input.financial_profile, None).map_err(ServiceError::from)?;
        let identities = self.instruments.as_ref().ok_or(ServiceError::Unavailable)?;
        let population = crate::application::prepare_fixed_current_population(
            &self.research,
            Arc::clone(identities),
            vec![input.instrument_id],
            digest(&profile.resolution().configuration_digest)?,
            cutoff,
            context.deadline(),
            context.cancellation(),
        )
        .await?;
        let [member] = population.members() else {
            return Err(ServiceError::InvalidResult);
        };
        if !profile.admits_investment(
            member.canonical_record().definition().asset_class(),
            member.listing_record().is_etf(),
        ) {
            return Err(ServiceError::InvalidRequest);
        }
        authority
            .prepare_investment_dataset(
                input.instrument_id,
                cutoff,
                input.source_action_reference,
                &profile,
                population,
                input.intended_use,
                context.origin().ok_or(ServiceError::Unauthorized)?,
                crate::application::lifecycle::WorkspaceRuntimeIdentity::try_from_runtime(runtime)
                    .map_err(|_| ServiceError::Unavailable)?,
                super::super::runtime::current_timestamp()
                    .map_err(|_| ServiceError::Unavailable)?,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(Into::into)
    }
    pub(crate) async fn reopen_training_dataset(
        &self,
        snapshot: &JobSnapshot,
        context: &RequestContext,
    ) -> Result<PythonDatasetSelection, ServiceError> {
        let selection = self
            .reopen_prepared_dataset(snapshot, None, context)
            .await?;
        if !matches!(selection.product_contract(),FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1
            |FeatureDatasetProductContract::FinancialAmountFiscalPeriodsTrainingV1
            |FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1
            |FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1
            |FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1) {return Err(ServiceError::InvalidRequest);}
        Ok(selection)
    }

    /// The sole reusable dataset-job decoder for ordinary, fiscal, and fold workflows.
    /// It names no latest dataset and accepts no caller export digest, recipe, or source cutoff.
    pub(crate) async fn reopen_prepared_dataset(
        &self,
        snapshot: &JobSnapshot,
        expected_contract: Option<FeatureDatasetProductContract>,
        context: &RequestContext,
    ) -> Result<PythonDatasetSelection, ServiceError> {
        self.read_prepared_dataset(snapshot, expected_contract, context)
            .await
            .map(|(selection, _)| selection)
    }

    /// Verifies exact exports on the existing owned I/O lane under the original request budget.
    pub(crate) async fn verify_dataset_export(
        &self,
        export: Sha256Digest,
        contract: FeatureDatasetProductContract,
        as_of: Timestamp,
        context: &RequestContext,
    ) -> Result<PythonDatasetSelection, ServiceError> {
        super::ensure_live(context)?;
        let paths = self.paths.clone();
        let deadline = context.deadline();
        let limits = PythonDatasetVerificationLimits::try_new(100_000, 256 * 1024 * 1024)
            .map_err(|_| ServiceError::Internal)?;
        let selection = self
            .research
            .run_owned_research_io(deadline, context.cancellation(), move |cancellation| {
                verify_python_dataset(
                    paths.root(),
                    export,
                    contract,
                    as_of,
                    limits,
                    deadline,
                    &cancellation,
                )
            })
            .await
            .map_err(crate::application::map_source_research_error)?
            .map_err(|error| match error {
                market_squawk_data::PythonDatasetCatalogError::Cancelled => ServiceError::Cancelled,
                market_squawk_data::PythonDatasetCatalogError::DeadlineExceeded => {
                    ServiceError::DeadlineExceeded
                }
                _ => ServiceError::InvalidResult,
            })?;
        super::ensure_live(context)?;
        Ok(selection)
    }

    async fn read_prepared_dataset(
        &self,
        snapshot: &JobSnapshot,
        expected_contract: Option<FeatureDatasetProductContract>,
        context: &RequestContext,
    ) -> Result<(PythonDatasetSelection, DatasetResultWire), ServiceError> {
        super::ensure_live(context)?;
        check_origin(snapshot, context)?;
        let spec = snapshot.spec();
        let result = snapshot
            .terminal_result()
            .ok_or(ServiceError::InvalidRequest)?;
        if snapshot.state() != JobState::Completed
            || spec.kind().as_str() != DATASET_KIND
            || spec.input().authority().as_str()
                != "research.phase-one-derived-generation-request.v1"
            || spec.authority().authority().as_str() != RESULT_AUTHORITY
            || spec.authority().identity().as_str() != RESULT_AUTHORITY
            || result.authority().as_str() != RESULT_AUTHORITY
            || result.artifacts().len() != 1
        {
            return Err(ServiceError::InvalidRequest);
        }
        let reference = &result.artifacts()[0];
        if reference.media_type() != "application/json" {
            return Err(ServiceError::InvalidResult);
        }
        let content = self
            .artifacts
            .read(
                ArtifactReadRequest::try_new(
                    reference.clone(),
                    NonZeroUsize::new(MAXIMUM_DATASET_RESULT_BYTES)
                        .ok_or(ServiceError::Internal)?,
                )
                .map_err(|_| ServiceError::InvalidResult)?,
                ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
            )
            .await
            .map_err(|error| match error {
                market_squawk_services::ArtifactError::Cancelled => ServiceError::Cancelled,
                market_squawk_services::ArtifactError::DeadlineExceeded => {
                    ServiceError::DeadlineExceeded
                }
                _ => ServiceError::InvalidResult,
            })?;
        let wire: DatasetResultWire =
            serde_json::from_slice(content.content()).map_err(|_| ServiceError::InvalidResult)?;
        let contract = FeatureDatasetProductContract::from_identity(&wire.product_contract)
            .ok_or(ServiceError::InvalidResult)?;
        if expected_contract.is_some_and(|expected| expected != contract)
            || wire.job_binding.job_id != snapshot.id().as_uuid()
            || wire.job_binding.generation != snapshot.generation().get()
            || digest(&wire.job_binding.input_sha256)?.bytes() != spec.input().digest().bytes()
            || wire.publication_stage != "phase_one_derived_generation"
            || wire.product_admission != "admitted_by_product_recipe_at_completion"
            || digest(&wire.manifest.content_sha256)?.bytes() != result.evidence_digest().bytes()
            || result.identity().as_str()
                != format!(
                    "phase-one-derived-generation-{}",
                    wire.manifest.content_sha256
                )
            || digest(&wire.build_spec_sha256)?.bytes() != spec.input().digest().bytes()
            || spec.input().identity().as_str()
                != format!(
                    "phase-one-build-v1:{}:{}",
                    contract.identity(),
                    wire.manifest.dataset
                )
        {
            return Err(ServiceError::InvalidResult);
        }
        // Native verification resolves the exact catalog production receipt, reopens all
        // immutable objects, and checks current Train rights and the complete row selection.
        let selection = self
            .verify_dataset_export(
                digest(&wire.phase_one_descriptor_sha256)?,
                contract,
                Timestamp::from_unix_nanos(wire.selection_as_of_unix_nanos),
                context,
            )
            .await?;
        let identity = selection.identity();
        let manifest = identity.manifest();
        let training=matches!(contract,FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonForwardReturnTrainingV1
            |FeatureDatasetProductContract::FinancialAmountFiscalPeriodsTrainingV1
            |FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonPriceHigherTrainingV1
            |FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonBenchmarkOutperformanceTrainingV1
            |FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonProfitAfterCostsTrainingV1);
        let study_inputs = matches!(
            contract,
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1
                | FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1
        );
        let expected_rows = wire
            .split_examples
            .train
            .checked_add(wire.split_examples.validation)
            .and_then(|v| v.checked_add(wire.split_examples.test))
            .and_then(|v| {
                v.checked_mul(usize::from(!study_inputs) + 1 + contract.macro_components().len())
            })
            .ok_or(ServiceError::InvalidResult)?;
        if manifest.dataset_id().as_str() != wire.manifest.dataset
            || manifest.manifest_version() != wire.manifest.version
            || manifest.schema().name() != wire.manifest.schema
            || manifest.schema().version().get() != wire.manifest.schema_version
            || manifest.schema().fingerprint()
                != digest(&wire.manifest.schema_fingerprint_sha256)?.bytes()
            || manifest.content_hash() != digest(&wire.manifest.content_sha256)?
            || identity.build_spec_digest().digest() != digest(&wire.build_spec_sha256)?
            || identity.policy_digest() != digest(&wire.policy_sha256)?
            || identity.universe_digest() != digest(&wire.universe_sha256)?
            || selection.selected_rows() != expected_rows
            || (training
                && (wire.split_examples.train == 0
                    || wire.split_examples.validation == 0
                    || wire.split_examples.test == 0))
            || selection
                .study_policy()
                .is_none_or(|study| study.snapshot_as_of() != selection.as_of())
        {
            return Err(ServiceError::InvalidResult);
        }
        super::ensure_live(context)?;
        Ok((selection, wire))
    }
    pub(super) async fn read_dataset_result(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let input: ResultInput = super::decode(request.arguments())?;
        let snapshot = self
            .snapshot(&input.job_id, input.generation, context)
            .await?;
        let (selection, wire) = self.read_prepared_dataset(&snapshot, None, context).await?;
        super::ensure_live(context)?;
        TypedToolResult::try_new(json!({"job":JobReceipt::from_snapshot(&snapshot),"dataset":{
            "manifest":wire.manifest,"productContract":selection.product_contract().identity(),
            "selectionAsOfUnixNanos":selection.as_of().unix_nanos().to_string(),"buildSpecSha256":wire.build_spec_sha256,
            "policySha256":wire.policy_sha256,"universeSha256":wire.universe_sha256,
            "phaseOneDescriptorSha256":wire.phase_one_descriptor_sha256,"splitExamples":wire.split_examples
        }}),1,ToolResultMetadata::complete_not_applicable(),context.limits()).map_err(Into::into)
    }
    pub(super) async fn read_result(
        &self,
        runner: &TrainingJobRunner,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        let input: ResultInput = super::decode(request.arguments())?;
        let snapshot = self
            .snapshot(&input.job_id, input.generation, context)
            .await?;
        let receipt = runner
            .resolve_completed(&snapshot)
            .map_err(super::map_training_admission)?;
        let model_token = runner
            .completed_model_token(&receipt)
            .map_err(super::map_training_admission)?;
        super::ensure_live(context)?;
        TypedToolResult::try_new(json!({"job":JobReceipt::from_snapshot(&snapshot),"model":{
            "modelToken":model_token,"modelId":receipt.model_id(),"bundleId":receipt.bundle_id().as_str(),
            "bundleVersion":receipt.bundle_version().get(),"metadataSha256":hex(receipt.metadata_sha256()),
            "artifactSha256":hex(receipt.artifact_sha256()),"trainingRunSha256":hex(receipt.training_run_sha256()),
            "authoritySha256":hex(receipt.authority_sha256()),"datasetSelectionSha256":hex(receipt.dataset_selection_sha256())
        }}),1,ToolResultMetadata::complete_not_applicable(),context.limits()).map_err(Into::into)
    }
}
fn check_origin(snapshot: &JobSnapshot, context: &RequestContext) -> Result<(), ServiceError> {
    let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
    if snapshot.spec().origin().workspace().as_str() != origin.workspace_id().to_string()
        || snapshot.spec().origin().client().as_str() != origin.client_id().to_string()
    {
        return Err(ServiceError::Unauthorized);
    }
    Ok(())
}
fn digest(value: &str) -> Result<Sha256Digest, ServiceError> {
    super::super::jobs::parse_sha256(value).map(|value| Sha256Digest::new(value.bytes()))
}
fn hex(value: Sha256Digest) -> String {
    crate::application::model::forecast_preparation::hex(value)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StartInput {
    dataset_job_id: String,
    dataset_job_generation: u64,
    financial_profile: AnalyticalProfileResolution,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResultInput {
    job_id: String,
    generation: u64,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetResultWire {
    job_binding: DatasetJobBindingWire,
    publication_stage: String,
    product_admission: String,
    product_contract: String,
    selection_as_of_unix_nanos: i64,
    manifest: ManifestWire,
    build_spec_sha256: String,
    policy_sha256: String,
    universe_sha256: String,
    phase_one_descriptor_sha256: String,
    split_examples: SplitWire,
}
#[derive(Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestWire {
    dataset: String,
    version: u64,
    schema: String,
    schema_version: u16,
    schema_fingerprint_sha256: String,
    content_sha256: String,
}
#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct SplitWire {
    train: usize,
    validation: usize,
    test: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetStartInput {
    source_action_reference: crate::application::SourceAppliedCorporateActionPlanReference,
    instrument_id: market_squawk_domain::InstrumentId,
    source_cutoff_unix_nanos: String,
    financial_profile: AnalyticalProfileResolution,
    intended_use: crate::application::DatasetPreparationUse,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DatasetJobBindingWire {
    job_id: uuid::Uuid,
    generation: u64,
    input_sha256: String,
}
