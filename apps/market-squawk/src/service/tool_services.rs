//! One installed tool surface shared by native and MCP transports.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use market_squawk_domain::{SourceIdentifier, Timestamp};
use market_squawk_jobs::{
    JobAuthority, JobAuthorityError, JobGeneration, JobId, JobOrigin, JobRepository,
    JobRepositoryError, JobSnapshot, JobState, SqliteJobRepository,
};
use market_squawk_runtime::{ClientId, InputStager, InputTicketId, RuntimeIdentity};
use market_squawk_services::{
    RequestContext, RequestId, ServiceCapabilities, ServiceError, ToolResultMetadata, ToolServices,
    TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    LocalProduct,
    application::{
        Application, DatasetPreparationPreviewRequest, DatasetPreparationReceipt,
        DatasetPreparationSelection, lifecycle::WorkspaceRuntimeIdentity,
        operations::OperationsApplicationServices,
    },
    jobs::{InstalledJobAuthority, InstalledJobRunners},
};

use super::{
    analysis::InstalledAnalysisOperations,
    backtest_preparation::{InstalledBacktestPreparation, START_PREPARED_BACKTEST},
    decision::{InstalledDecisionOperations, RUN_SCREEN},
    forecast_preparation::{InstalledForecastPreparation, START_PREPARED_FORECAST},
    jobs::InstalledJobOperations,
    operations::InstalledOperations,
    portfolio_import::InstalledPortfolioImportOperations,
    research_dataset::InstalledResearchDatasetPreparation,
    research_file_import::{InstalledResearchFileImportOperations, PreparedResearchFileCommit},
};

const START_INGEST: &str = "Research.StartIngestSource";
const START_EXPORT: &str = "Research.StartExport";
const START_DATASET: &str = "Research.StartDatasetBuild";
const START_FEATURE_DATASET: &str = "Analysis.StartFeatureDatasetBuild";
const GET_FEATURE_DATASET_PREPARATION: &str = "Analysis.GetFeatureDatasetPreparationOptions";
const PREVIEW_FEATURE_DATASET: &str = "Analysis.PreviewFeatureDatasetBuild";
const START_PREPARED_FEATURE_DATASET: &str = "Analysis.StartPreparedFeatureDatasetBuild";
const START_SCENARIO: &str = "Analysis.StartScenarioBatch";
const START_BACKTEST: &str = "Analysis.StartBacktest";
const START_TRAINING: &str = "Model.StartTraining";
const START_FORECAST: &str = "Model.StartForecast";
const TRAINING_CONFIG_MEDIA_TYPE: &str = "market-squawk.training-config.v1";
const TRAINING_AUTHORITY_MEDIA_TYPE: &str = "market-squawk.model-authority.v1";
const RESEARCH_INGEST_JOB_KIND: &str = "research.ingest-source.v1";
const RESEARCH_INGEST_INPUT_AUTHORITY: &str = "research.ingest-request.v1";
const RESEARCH_INGEST_RESULT_AUTHORITY: &str = "research.dataset-publication.v1";

/// Sole transport-neutral installed-service composition.
pub(super) struct InstalledToolServices {
    application: Arc<Application>,
    jobs: InstalledJobOperations,
    runners: Arc<InstalledJobRunners>,
    inputs: Arc<InputStager>,
    runtime: RuntimeIdentity,
    dataset_preparation: InstalledResearchDatasetPreparation,
    backtest_preparation: InstalledBacktestPreparation,
    forecast_preparation: InstalledForecastPreparation,
    analysis: InstalledAnalysisOperations,
    decisions: InstalledDecisionOperations,
    operations: InstalledOperations,
    portfolio_import: InstalledPortfolioImportOperations,
    research_file_import: InstalledResearchFileImportOperations,
    research_file_job_repository: Arc<SqliteJobRepository>,
    research_file_job_authority: Arc<JobAuthority<SqliteJobRepository>>,
}

/// Application authorities required to compose the installed tool surface.
pub(super) struct InstalledToolServiceAuthorities<'a> {
    application: Arc<Application>,
    operations: Arc<OperationsApplicationServices>,
    product: &'a LocalProduct,
    jobs: &'a InstalledJobAuthority,
}

impl<'a> InstalledToolServiceAuthorities<'a> {
    pub(super) fn new(
        application: Arc<Application>,
        operations: Arc<OperationsApplicationServices>,
        product: &'a LocalProduct,
        jobs: &'a InstalledJobAuthority,
    ) -> Self {
        Self {
            application,
            operations,
            product,
            jobs,
        }
    }
}

/// Runtime-owned resources required to compose the installed tool surface.
pub(super) struct InstalledToolServiceRuntime {
    runners: Arc<InstalledJobRunners>,
    inputs: Arc<InputStager>,
    runtime: RuntimeIdentity,
    portfolio_import: InstalledPortfolioImportOperations,
    research_file_import: InstalledResearchFileImportOperations,
}

impl InstalledToolServiceRuntime {
    pub(super) fn new(
        runners: Arc<InstalledJobRunners>,
        inputs: Arc<InputStager>,
        runtime: RuntimeIdentity,
        portfolio_import: InstalledPortfolioImportOperations,
        research_file_import: InstalledResearchFileImportOperations,
    ) -> Self {
        Self {
            runners,
            inputs,
            runtime,
            portfolio_import,
            research_file_import,
        }
    }
}

impl InstalledToolServices {
    pub(super) fn try_new(
        authorities: InstalledToolServiceAuthorities<'_>,
        runtime_resources: InstalledToolServiceRuntime,
    ) -> Result<Self, ServiceError> {
        let InstalledToolServiceAuthorities {
            application,
            operations,
            product,
            jobs,
        } = authorities;
        let InstalledToolServiceRuntime {
            runners,
            inputs,
            runtime,
            portfolio_import,
            research_file_import,
        } = runtime_resources;
        let forecast_preparation =
            InstalledForecastPreparation::try_new(product, &application.capabilities(), runtime)?;
        let installed_operations = InstalledOperations::new(
            operations,
            jobs,
            Arc::clone(runners.backup()),
            Arc::clone(runners.recovery()),
            Arc::clone(runners.update()),
        );
        Ok(Self {
            application: Arc::clone(&application),
            jobs: InstalledJobOperations::new(jobs),
            runners,
            inputs: Arc::clone(&inputs),
            runtime,
            dataset_preparation: InstalledResearchDatasetPreparation::new(product.research()),
            backtest_preparation: InstalledBacktestPreparation::try_new(
                product.research().analytical_reader(),
                runtime,
            )?,
            forecast_preparation,
            analysis: InstalledAnalysisOperations::new(product, jobs),
            decisions: InstalledDecisionOperations::try_new(
                Arc::clone(&application),
                product.decisions(),
                product.research().analytical_reader(),
                product.portfolio().fair_value_reader(),
                runtime,
            )?,
            operations: installed_operations,
            portfolio_import,
            research_file_import,
            research_file_job_repository: jobs.repository(),
            research_file_job_authority: jobs.authority(),
        })
    }

    pub(super) fn recover_promoting_portfolio_imports(
        &self,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        self.portfolio_import.recover_promoting(context)
    }

    pub(super) async fn recover_promoting_research_file_imports(
        &self,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        let committed = self.research_file_import.committed_jobs()?;
        self.research_file_import.discard_pending_after_restart()?;
        for preview_id in self.research_file_import.recovery_ids()? {
            self.restart_research_file_import(&preview_id, context)
                .await?;
        }
        for (preview_id, receipt) in committed {
            let view = self.jobs.view(receipt.job_id()).await?;
            if view.generation().get() != receipt.generation()
                || view.kind().as_str() != RESEARCH_INGEST_JOB_KIND
            {
                return Err(ServiceError::InvalidResult);
            }
            if research_file_import_requires_restart(&view)? {
                if self
                    .research_file_import
                    .reopen_committed_job(&preview_id, receipt.job_id())?
                {
                    self.restart_research_file_import(&preview_id, context)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn restart_research_file_import(
        &self,
        preview_id: &str,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        loop {
            let mut prepared = self
                .research_file_import
                .prepare_recovery(preview_id, context)
                .await?;
            let job_id = prepared.job_start().job_id().to_owned();
            let (result, reconciled_existing) = self
                .start_research_file_import_job(&mut prepared, context)
                .await?;
            self.research_file_import
                .complete_commit(prepared.preview_id(), &result)?;
            drop(prepared);
            if !reconciled_existing {
                return Ok(());
            }
            let view = self.jobs.view(&job_id).await?;
            if !research_file_import_requires_restart(&view)?
                || !self
                    .research_file_import
                    .reopen_committed_job(preview_id, &job_id)?
            {
                return Ok(());
            }
        }
    }

    async fn start_job(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
    ) -> Result<Option<TypedToolResult>, ServiceError> {
        let captured_at =
            super::runtime::current_timestamp().map_err(|_error| ServiceError::Unavailable)?;
        let limits = context.limits();
        let (admission, revoke) = match request.name() {
            START_INGEST => {
                let terminal = self.terminal_request(request, "Research.IngestSource")?;
                let admission = self
                    .runners
                    .ingest()
                    .admit(terminal, limits, captured_at)
                    .map_err(map_research_admission)?;
                (admission, JobAdmissionOwner::Ingest)
            }
            START_EXPORT => {
                let terminal = self.terminal_request(request, "Research.GetHistory")?;
                let admission = self
                    .runners
                    .export()
                    .admit(terminal, limits, captured_at)
                    .map_err(map_research_admission)?;
                (admission, JobAdmissionOwner::Export)
            }
            START_DATASET | START_FEATURE_DATASET => {
                let registration = request
                    .arguments()
                    .get("registration")
                    .and_then(serde_json::Value::as_object)
                    .ok_or(ServiceError::InvalidRequest)?;
                let build = crate::local_product::cli_dataset::admit_inline_dataset_registration(
                    registration,
                )
                .map_err(map_dataset_admission)?;
                if request.name() == START_DATASET {
                    let admission = self
                        .runners
                        .dataset()
                        .admit(build, captured_at)
                        .map_err(map_research_admission)?;
                    (admission, JobAdmissionOwner::Dataset)
                } else {
                    let admission = self
                        .runners
                        .feature()
                        .admit(build, captured_at)
                        .map_err(map_research_admission)?;
                    (admission, JobAdmissionOwner::Feature)
                }
            }
            START_PREPARED_FEATURE_DATASET => {
                let input: PreparedFeatureDatasetStart = decode(request.arguments())?;
                let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
                let workspace = WorkspaceRuntimeIdentity::try_from_runtime(self.runtime)
                    .map_err(|_error| ServiceError::Unavailable)?;
                let build = self
                    .dataset_preparation
                    .consume(
                        input.receipt,
                        origin,
                        workspace,
                        Instant::now(),
                        context.deadline(),
                        context.cancellation(),
                    )
                    .map_err(ServiceError::from)?;
                let admission = self
                    .runners
                    .feature()
                    .admit(build, captured_at)
                    .map_err(map_research_admission)?;
                (admission, JobAdmissionOwner::Feature)
            }
            START_SCENARIO => {
                let terminal = self.terminal_request(request, "Analysis.GetScenarios")?;
                let admission = self
                    .runners
                    .scenario()
                    .admit(terminal, limits, captured_at)
                    .map_err(map_research_admission)?;
                (admission, JobAdmissionOwner::Scenario)
            }
            START_BACKTEST => {
                let registration = request
                    .arguments()
                    .get("registration")
                    .and_then(serde_json::Value::as_object)
                    .ok_or(ServiceError::InvalidRequest)?;
                let admission = self
                    .runners
                    .backtest()
                    .admit_registration(
                        self.runners.backtest_registrar().as_ref(),
                        registration,
                        context.cancellation().clone(),
                        context.deadline(),
                        captured_at,
                    )
                    .await
                    .map_err(map_backtest_admission)?;
                (admission, JobAdmissionOwner::Backtest)
            }
            START_PREPARED_BACKTEST => {
                let input = self.backtest_preparation.consume(request, context).await?;
                let admission = self
                    .runners
                    .backtest()
                    .admit_prepared(
                        self.runners.backtest_registrar().as_ref(),
                        input,
                        context.cancellation().clone(),
                        context.deadline(),
                        captured_at,
                    )
                    .await
                    .map_err(map_backtest_admission)?;
                (admission, JobAdmissionOwner::Backtest)
            }
            START_TRAINING => {
                let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
                let client = ClientId::try_from_uuid(origin.client_id())
                    .map_err(|_error| ServiceError::Unauthorized)?;
                let config = self.claim_input(
                    request,
                    "configTicketId",
                    client,
                    TRAINING_CONFIG_MEDIA_TYPE,
                    captured_at,
                )?;
                let authority = self.claim_input(
                    request,
                    "authorityTicketId",
                    client,
                    TRAINING_AUTHORITY_MEDIA_TYPE,
                    captured_at,
                )?;
                let runner = self.runners.training().ok_or(ServiceError::Unavailable)?;
                let admission = runner
                    .admit_staged(config, authority, captured_at)
                    .map_err(map_training_admission)?;
                (admission, JobAdmissionOwner::Training)
            }
            START_FORECAST => {
                let terminal = self.terminal_request(request, "Model.GenerateForecast")?;
                let admission = self
                    .runners
                    .forecast()
                    .admit(terminal, limits, captured_at)
                    .map_err(map_forecast_admission)?;
                (admission, JobAdmissionOwner::Forecast)
            }
            START_PREPARED_FORECAST => {
                let terminal = self.forecast_preparation.consume(request, context).await?;
                let admission = self
                    .runners
                    .forecast()
                    .admit(terminal, limits, captured_at)
                    .map_err(map_forecast_admission)?;
                (admission, JobAdmissionOwner::Forecast)
            }
            RUN_SCREEN => {
                let prepared = self
                    .decisions
                    .prepare_screen_job(request, context, captured_at)
                    .await?;
                let admission = self
                    .runners
                    .screen()
                    .admit(crate::jobs::ScreenJobCommand::new(prepared), captured_at)
                    .map_err(map_screen_admission)?;
                (admission, JobAdmissionOwner::Screen)
            }
            _ => return Ok(None),
        };
        let retained = admission.clone();
        let metadata = job_receipt_metadata(request)?;
        match self.jobs.start(admission, context, metadata).await {
            Ok(result) => Ok(Some(result)),
            Err(error) => {
                self.revoke(revoke, &retained);
                Err(error)
            }
        }
    }

    async fn start_research_file_import_job(
        &self,
        prepared: &mut super::research_file_import::PreparedStart,
        context: &RequestContext,
    ) -> Result<(TypedToolResult, bool), ServiceError> {
        ensure_live(context)?;
        let start = prepared.job_start();
        let job_id =
            JobId::try_from_str(start.job_id()).map_err(|_error| ServiceError::Internal)?;
        let generation = JobGeneration::try_new(1).map_err(|_error| ServiceError::Internal)?;
        let admitted_at = Timestamp::from_unix_nanos(start.admitted_at_unix_nanos());
        let request_id = RequestId::try_string(start.request_id().to_owned())
            .map_err(|_error| ServiceError::Internal)?;
        let workspace = SourceIdentifier::try_from(prepared.workspace_id())
            .map_err(|_error| ServiceError::Internal)?;
        let client = SourceIdentifier::try_from(prepared.client_id())
            .map_err(|_error| ServiceError::Internal)?;
        let origin = JobOrigin::new(workspace, client);

        match self
            .research_file_job_repository
            .get(job_id, generation)
            .await
        {
            Ok(snapshot) => {
                prepared.mark_job_admission_may_exist();
                validate_research_file_job_binding(
                    &snapshot,
                    job_id,
                    &origin,
                    &request_id,
                    admitted_at,
                )?;
                ensure_live(context)?;
                return Ok((prepared.queued_result(), true));
            }
            Err(JobRepositoryError::NotFound) => {}
            Err(_error) => {
                prepared.mark_job_admission_may_exist();
                return Err(ServiceError::Unavailable);
            }
        }

        let admission = self
            .runners
            .ingest()
            .admit(prepared.request().clone(), context.limits(), admitted_at)
            .map_err(map_research_admission)?;
        let spec = admission
            .clone()
            .into_spec(job_id, origin.clone(), request_id.clone(), admitted_at)
            .map_err(|_error| ServiceError::Internal)?;
        prepared.mark_job_admission_may_exist();
        match self.research_file_job_authority.start(&spec).await {
            Ok(snapshot) => {
                if snapshot.spec() != &spec
                    || snapshot.state() != JobState::Queued
                    || snapshot.sequence().get() != 0
                {
                    return Err(ServiceError::InvalidResult);
                }
                ensure_live(context)?;
                Ok((prepared.queued_result(), false))
            }
            Err(error) => {
                match self
                    .research_file_job_repository
                    .get(job_id, generation)
                    .await
                {
                    Ok(snapshot) => {
                        if snapshot.spec() != &spec {
                            return Err(ServiceError::InvalidResult);
                        }
                        Ok((prepared.queued_result(), false))
                    }
                    Err(JobRepositoryError::NotFound) => {
                        self.revoke(JobAdmissionOwner::Ingest, &admission);
                        prepared.mark_job_not_admitted();
                        Err(map_research_file_job_authority(error))
                    }
                    Err(_error) => Err(ServiceError::Unavailable),
                }
            }
        }
    }

    fn claim_input(
        &self,
        request: &TypedToolRequest,
        argument: &str,
        client: ClientId,
        media_type: &str,
        now: market_squawk_domain::Timestamp,
    ) -> Result<market_squawk_runtime::ClaimedInput, ServiceError> {
        let id = request
            .arguments()
            .get(argument)
            .and_then(serde_json::Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
            .and_then(|value| InputTicketId::try_from_uuid(value).ok())
            .ok_or(ServiceError::InvalidRequest)?;
        let media_type = market_squawk_domain::SourceIdentifier::try_from(media_type)
            .map_err(|_error| ServiceError::Unavailable)?;
        self.inputs
            .claim(id, client, &media_type, now)
            .map_err(|_error| ServiceError::Unauthorized)
    }

    fn terminal_request(
        &self,
        start: &TypedToolRequest,
        terminal_name: &str,
    ) -> Result<TypedToolRequest, ServiceError> {
        let terminal = self
            .application
            .capabilities()
            .find(terminal_name)
            .cloned()
            .ok_or(ServiceError::Unavailable)?;
        crate::application::job::terminal_request_for_start(
            start,
            start.name(),
            &terminal,
            terminal_name,
        )
        .map_err(|_error| ServiceError::InvalidRequest)
    }

    fn revoke(&self, owner: JobAdmissionOwner, admission: &crate::application::job::JobAdmission) {
        match owner {
            JobAdmissionOwner::Ingest => {
                let _result = self.runners.ingest().revoke(admission);
            }
            JobAdmissionOwner::Export => {
                let _result = self.runners.export().revoke(admission);
            }
            JobAdmissionOwner::Dataset => {
                let _result = self.runners.dataset().revoke(admission);
            }
            JobAdmissionOwner::Feature => {
                let _result = self.runners.feature().revoke(admission);
            }
            JobAdmissionOwner::Scenario => {
                let _result = self.runners.scenario().revoke(admission);
            }
            JobAdmissionOwner::Backtest => {
                let _result = self.runners.backtest().revoke(admission);
            }
            JobAdmissionOwner::Training => {
                if let Some(runner) = self.runners.training() {
                    let _result = runner.revoke(admission);
                }
            }
            JobAdmissionOwner::Forecast => {
                let _result = self.runners.forecast().revoke(admission);
            }
            JobAdmissionOwner::Screen => {
                let _result = self.runners.screen().revoke(admission);
            }
        }
    }
}

fn research_file_import_requires_restart(
    view: &crate::application::job::JobView,
) -> Result<bool, ServiceError> {
    match view.state() {
        JobState::Completed | JobState::Cancelled => Ok(false),
        JobState::Queued | JobState::Preparing | JobState::Running | JobState::Recovering => {
            Ok(true)
        }
        JobState::Interrupted => Ok(!view.cancellation_requested()),
        JobState::Failed => Ok(view.failure().is_some_and(|failure| {
            failure.class().as_str() == "recovery"
                && failure.diagnostic().as_str() == "runner-recovery-failed"
        })),
        JobState::AwaitingConfirmation | JobState::Cancelling => Err(ServiceError::InvalidResult),
    }
}

fn validate_research_file_job_binding(
    snapshot: &JobSnapshot,
    job_id: JobId,
    origin: &JobOrigin,
    request_id: &RequestId,
    admitted_at: Timestamp,
) -> Result<(), ServiceError> {
    let spec = snapshot.spec();
    if snapshot.id() != job_id
        || snapshot.generation().get() != 1
        || spec.id() != job_id
        || spec.generation().get() != 1
        || spec.kind().as_str() != RESEARCH_INGEST_JOB_KIND
        || spec.origin() != origin
        || spec.request_id() != request_id
        || spec.input().authority().as_str() != RESEARCH_INGEST_INPUT_AUTHORITY
        || spec.authority().authority().as_str() != RESEARCH_INGEST_RESULT_AUTHORITY
        || spec.authority().identity().as_str() != RESEARCH_INGEST_RESULT_AUTHORITY
        || spec.authority().captured_at() != admitted_at
        || spec.attempt_limit().get() != 1
        || spec.admitted_at() != admitted_at
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(())
}

fn map_research_file_job_authority(error: JobAuthorityError) -> ServiceError {
    match error {
        JobAuthorityError::Capacity => ServiceError::ResourceExhausted,
        JobAuthorityError::UnknownKind
        | JobAuthorityError::Repository
        | JobAuthorityError::Contract
        | JobAuthorityError::ShutdownIncomplete => ServiceError::Unavailable,
    }
}

fn job_receipt_metadata(request: &TypedToolRequest) -> Result<ToolResultMetadata, ServiceError> {
    if request.name() != START_INGEST {
        return Ok(ToolResultMetadata::complete_not_applicable());
    }
    let provider = required_argument(request, "provider")?;
    let dataset = required_argument(request, "dataset")?;
    let object = required_argument(request, "object")?;
    ToolResultMetadata::try_complete(
        serde_json::json!({
            "provider": provider,
            "dataset": dataset,
        }),
        serde_json::json!({
            "sourceObject": object,
            "discoveryReceiptBound": true,
            "executionEligible": false,
        }),
    )
    .map_err(Into::into)
}

fn required_argument<'a>(
    request: &'a TypedToolRequest,
    name: &str,
) -> Result<&'a str, ServiceError> {
    request
        .arguments()
        .get(name)
        .and_then(Value::as_str)
        .ok_or(ServiceError::InvalidRequest)
}

#[derive(Clone, Copy)]
enum JobAdmissionOwner {
    Ingest,
    Export,
    Dataset,
    Feature,
    Scenario,
    Backtest,
    Training,
    Forecast,
    Screen,
}

impl std::fmt::Debug for InstalledToolServices {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledToolServices")
            .field("application", &"[APPLICATION AUTHORITY]")
            .field("jobs", &self.jobs)
            .field("runners", &self.runners)
            .field("inputs", &"[ONE-SHOT NATIVE INPUT STAGER]")
            .field("runtime", &self.runtime)
            .field(
                "dataset_preparation",
                &"[ONE-USE DATASET PREPARATION AUTHORITY]",
            )
            .field(
                "backtest_preparation",
                &"[ONE-USE BACKTEST PREPARATION AUTHORITY]",
            )
            .field(
                "forecast_preparation",
                &"[ONE-USE FORECAST PREPARATION AUTHORITY]",
            )
            .field("analysis", &self.analysis)
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
            .field("operations", &self.operations)
            .field("portfolio_import", &self.portfolio_import)
            .field("research_file_import", &self.research_file_import)
            .finish()
    }
}

#[async_trait]
impl ToolServices for InstalledToolServices {
    fn capabilities(&self) -> ServiceCapabilities {
        self.application.capabilities()
    }

    async fn call(
        &self,
        request: TypedToolRequest,
        context: RequestContext,
    ) -> Result<TypedToolResult, ServiceError> {
        if InstalledResearchFileImportOperations::owns_commit(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            return match self
                .research_file_import
                .prepare_commit(&request, &context)
                .await?
            {
                PreparedResearchFileCommit::Existing(result) => {
                    result
                        .validate_for(&descriptor)
                        .map_err(ServiceError::from)?;
                    Ok(result)
                }
                PreparedResearchFileCommit::Ready(mut prepared) => {
                    let (result, _reconciled_existing) = self
                        .start_research_file_import_job(&mut prepared, &context)
                        .await?;
                    result
                        .validate_for(&descriptor)
                        .map_err(ServiceError::from)?;
                    self.research_file_import
                        .complete_commit(prepared.preview_id(), &result)?;
                    Ok(result)
                }
            };
        }
        if let Some(result) = self.start_job(&request, &context).await? {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if InstalledJobOperations::owns(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let result = self.jobs.call(&request, &context).await?;
            result
                .validate_against(context.limits())
                .map_err(ServiceError::from)?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if matches!(
            request.name(),
            GET_FEATURE_DATASET_PREPARATION | PREVIEW_FEATURE_DATASET
        ) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            ensure_live(&context)?;
            let (content, item_count) = match request.name() {
                GET_FEATURE_DATASET_PREPARATION => {
                    let options = self
                        .dataset_preparation
                        .options(context.deadline(), context.cancellation().clone())
                        .await
                        .map_err(|error| {
                            tracing::warn!(
                                operation = GET_FEATURE_DATASET_PREPARATION,
                                error = ?error,
                                "guided feature-dataset preparation failed"
                            );
                            ServiceError::from(error)
                        })?;
                    let item_count = options.datasets.len().max(1);
                    (encode(&options)?, item_count)
                }
                PREVIEW_FEATURE_DATASET => {
                    let selection: DatasetPreparationSelection = decode(request.arguments())?;
                    let origin = context.origin().ok_or(ServiceError::Unauthorized)?;
                    let workspace = WorkspaceRuntimeIdentity::try_from_runtime(self.runtime)
                        .map_err(|_error| ServiceError::Unavailable)?;
                    let observed_at = super::runtime::current_timestamp()
                        .map_err(|_error| ServiceError::Unavailable)?;
                    let preview = self
                        .dataset_preparation
                        .preview(DatasetPreparationPreviewRequest {
                            selection,
                            origin,
                            workspace,
                            now: Instant::now(),
                            observed_at,
                            deadline: context.deadline(),
                            cancellation: context.cancellation().clone(),
                        })
                        .await
                        .map_err(ServiceError::from)?;
                    (encode(&preview)?, 1)
                }
                _ => return Err(ServiceError::NotFound),
            };
            ensure_live(&context)?;
            let result = TypedToolResult::try_new(
                content,
                item_count,
                ToolResultMetadata::complete_not_applicable(),
                context.limits(),
            )
            .map_err(ServiceError::from)?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if InstalledForecastPreparation::owns(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let result = self.forecast_preparation.call(&request, &context).await?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if InstalledBacktestPreparation::owns(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let result = self.backtest_preparation.call(&request, &context).await?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if InstalledDecisionOperations::owns(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let result = self.decisions.call(&request, &context).await?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if InstalledPortfolioImportOperations::owns(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let result = self.portfolio_import.call(request, context).await?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if InstalledResearchFileImportOperations::owns_direct(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let result = self.research_file_import.call(request, context).await?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if InstalledAnalysisOperations::owns(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let result = self.analysis.call(&request, &context).await?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        if InstalledOperations::owns(request.name()) {
            let descriptor = self
                .application
                .capabilities()
                .find(request.name())
                .cloned()
                .ok_or(ServiceError::NotFound)?;
            if descriptor.version() != request.version()
                || descriptor.contract() != request.contract()
            {
                return Err(ServiceError::InvalidRequest);
            }
            let result = self.operations.call(&request, &context).await?;
            result
                .validate_for(&descriptor)
                .map_err(ServiceError::from)?;
            return Ok(result);
        }
        self.application.call(request, context).await
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PreparedFeatureDatasetStart {
    receipt: DatasetPreparationReceipt,
}

fn decode<T: for<'de> Deserialize<'de>>(arguments: &Map<String, Value>) -> Result<T, ServiceError> {
    serde_json::from_value(Value::Object(super::business_arguments(arguments)))
        .map_err(|_error| ServiceError::InvalidRequest)
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Value, ServiceError> {
    serde_json::to_value(value).map_err(|_error| ServiceError::Internal)
}

fn ensure_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        Err(ServiceError::Cancelled)
    } else if Instant::now() >= context.deadline() {
        Err(ServiceError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

fn map_research_admission(error: crate::jobs::ResearchJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::ResearchJobRunnerError::InvalidLimits
        | crate::jobs::ResearchJobRunnerError::InvalidRequest
        | crate::jobs::ResearchJobRunnerError::Conflict => ServiceError::InvalidRequest,
        crate::jobs::ResearchJobRunnerError::Capacity => ServiceError::ResourceExhausted,
        crate::jobs::ResearchJobRunnerError::Unavailable => ServiceError::Unavailable,
    }
}

fn map_backtest_admission(error: crate::jobs::BacktestJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::BacktestJobRunnerError::InvalidLimits
        | crate::jobs::BacktestJobRunnerError::InvalidCommand
        | crate::jobs::BacktestJobRunnerError::Conflict => ServiceError::InvalidRequest,
        crate::jobs::BacktestJobRunnerError::Capacity => ServiceError::ResourceExhausted,
        crate::jobs::BacktestJobRunnerError::Unavailable => ServiceError::Unavailable,
    }
}

fn map_dataset_admission(error: crate::local_product::CliDatasetError) -> ServiceError {
    match error {
        crate::local_product::CliDatasetError::InvalidRequest
        | crate::local_product::CliDatasetError::RequestJson
        | crate::local_product::CliDatasetError::ConfirmationRequired => {
            ServiceError::InvalidRequest
        }
        crate::local_product::CliDatasetError::RequestFile => ServiceError::Unauthorized,
        crate::local_product::CliDatasetError::Build(_)
        | crate::local_product::CliDatasetError::PythonExport(_) => ServiceError::Unavailable,
    }
}

fn map_training_admission(error: crate::jobs::TrainingJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::TrainingJobRunnerError::InvalidLimits
        | crate::jobs::TrainingJobRunnerError::InvalidInput
        | crate::jobs::TrainingJobRunnerError::InputChanged
        | crate::jobs::TrainingJobRunnerError::Conflict
        | crate::jobs::TrainingJobRunnerError::StagedInput(_) => ServiceError::InvalidRequest,
        crate::jobs::TrainingJobRunnerError::Capacity => ServiceError::ResourceExhausted,
        crate::jobs::TrainingJobRunnerError::Unavailable
        | crate::jobs::TrainingJobRunnerError::WorkerUnavailable
        | crate::jobs::TrainingJobRunnerError::StagingConflict
        | crate::jobs::TrainingJobRunnerError::InvalidCandidate
        | crate::jobs::TrainingJobRunnerError::Cleanup
        | crate::jobs::TrainingJobRunnerError::Input(_)
        | crate::jobs::TrainingJobRunnerError::Path
        | crate::jobs::TrainingJobRunnerError::Artifact
        | crate::jobs::TrainingJobRunnerError::Program(_) => ServiceError::Unavailable,
    }
}

fn map_forecast_admission(error: crate::jobs::ForecastJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::ForecastJobRunnerError::InvalidLimits
        | crate::jobs::ForecastJobRunnerError::InvalidRequest
        | crate::jobs::ForecastJobRunnerError::Conflict => ServiceError::InvalidRequest,
        crate::jobs::ForecastJobRunnerError::Capacity => ServiceError::ResourceExhausted,
        crate::jobs::ForecastJobRunnerError::Unavailable => ServiceError::Unavailable,
    }
}

fn map_screen_admission(error: crate::jobs::ScreenJobRunnerError) -> ServiceError {
    match error {
        crate::jobs::ScreenJobRunnerError::Conflict => ServiceError::InvalidRequest,
        crate::jobs::ScreenJobRunnerError::InvalidConfiguration => ServiceError::Unavailable,
    }
}
