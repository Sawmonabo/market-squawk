//! One installed tool surface shared by native and MCP transports.

use std::sync::Arc;

use async_trait::async_trait;
use market_squawk_runtime::{ClientId, InputStager, InputTicketId};
use market_squawk_services::{
    RequestContext, ServiceCapabilities, ServiceError, ToolServices, TypedToolRequest,
    TypedToolResult,
};
use uuid::Uuid;

use crate::{
    LocalProduct,
    application::Application,
    jobs::{InstalledJobAuthority, InstalledJobRunners},
};

use super::{
    analysis::InstalledAnalysisOperations, decision::InstalledDecisionOperations,
    jobs::InstalledJobOperations,
};

const START_INGEST: &str = "Research.StartIngestSource";
const START_EXPORT: &str = "Research.StartExport";
const START_DATASET: &str = "Research.StartDatasetBuild";
const START_FEATURE_DATASET: &str = "Analysis.StartFeatureDatasetBuild";
const START_SCENARIO: &str = "Analysis.StartScenarioBatch";
const START_BACKTEST: &str = "Analysis.StartBacktest";
const START_TRAINING: &str = "Model.StartTraining";
const START_FORECAST: &str = "Model.StartForecast";
const TRAINING_CONFIG_MEDIA_TYPE: &str = "market-squawk.training-config.v1";
const TRAINING_AUTHORITY_MEDIA_TYPE: &str = "market-squawk.model-authority.v1";

/// Sole transport-neutral installed-service composition.
pub(super) struct InstalledToolServices {
    application: Arc<Application>,
    jobs: InstalledJobOperations,
    runners: Arc<InstalledJobRunners>,
    inputs: Arc<InputStager>,
    analysis: InstalledAnalysisOperations,
    decisions: InstalledDecisionOperations,
}

impl InstalledToolServices {
    pub(super) fn try_new(
        application: Arc<Application>,
        product: &LocalProduct,
        jobs: &InstalledJobAuthority,
        runners: Arc<InstalledJobRunners>,
        inputs: Arc<InputStager>,
    ) -> Result<Self, ServiceError> {
        Ok(Self {
            application,
            jobs: InstalledJobOperations::new(jobs),
            runners,
            inputs,
            analysis: InstalledAnalysisOperations::new(product, jobs),
            decisions: InstalledDecisionOperations::try_new(product.decisions())?,
        })
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
            _ => return Ok(None),
        };
        let retained = admission.clone();
        match self.jobs.start(admission, context).await {
            Ok(result) => Ok(Some(result)),
            Err(error) => {
                self.revoke(revoke, &retained);
                Err(error)
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
        }
    }
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
}

impl std::fmt::Debug for InstalledToolServices {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InstalledToolServices")
            .field("application", &"[APPLICATION AUTHORITY]")
            .field("jobs", &self.jobs)
            .field("runners", &self.runners)
            .field("inputs", &"[ONE-SHOT NATIVE INPUT STAGER]")
            .field("analysis", &self.analysis)
            .field("decisions", &"[DURABLE DECISION AUTHORITY]")
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
            let result = self.decisions.call(&request, &context)?;
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
        self.application.call(request, context).await
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
